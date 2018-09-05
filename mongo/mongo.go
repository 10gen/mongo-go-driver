// Copyright (C) MongoDB, Inc. 2017-present.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

package mongo

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"reflect"
	"strings"

	"github.com/mongodb/mongo-go-driver/bson"
	"github.com/mongodb/mongo-go-driver/bson/objectid"
	"github.com/mongodb/mongo-go-driver/mongo/countopt"
)

// Dialer is used to make network connections.
type Dialer interface {
	DialContext(ctx context.Context, network, address string) (net.Conn, error)
}

// BSONAppender is an interface implemented by types that can marshal a
// provided type into BSON bytes and append those bytes to the provided []byte.
// The AppendBSON can return a non-nil error and non-nil []byte. The AppendBSON
// method may also write incomplete BSON to the []byte.
type BSONAppender interface {
	AppendBSON([]byte, interface{}) ([]byte, error)
}

// BSONAppenderFunc is an adapter function that allows any function that
// satisfies the AppendBSON method signature to be used where a BSONAppender is
// used.
type BSONAppenderFunc func([]byte, interface{}) ([]byte, error)

// AppendBSON implements the BSONAppender interface
func (baf BSONAppenderFunc) AppendBSON(dst []byte, val interface{}) ([]byte, error) {
	return baf(dst, val)
}

// TransformDocument handles transforming a document of an allowable type into
// a *bson.Document. This method is called directly after most methods that
// have one or more parameters that are documents.
//
// The supported types for document are:
//
//  bson.Marshaler
//  bson.DocumentMarshaler
//  bson.Reader
//  []byte (must be a valid BSON document)
//  io.Reader (only 1 BSON document will be read)
//  A custom struct type
//
func TransformDocument(document interface{}) (*bson.Document, error) {
	switch d := document.(type) {
	case nil:
		return bson.NewDocument(), nil
	case *bson.Document:
		return d, nil
	case bson.Marshaler, bson.Reader, []byte, io.Reader:
		return bson.NewDocumentEncoder().EncodeDocument(document)
	case bson.DocumentMarshaler:
		return d.MarshalBSONDocument()
	default:
		var kind reflect.Kind
		if t := reflect.TypeOf(document); t.Kind() == reflect.Ptr {
			kind = t.Elem().Kind()
		}
		if reflect.ValueOf(document).Kind() == reflect.Struct || kind == reflect.Struct {
			return bson.NewDocumentEncoder().EncodeDocument(document)
		}
		if reflect.ValueOf(document).Kind() == reflect.Map &&
			reflect.TypeOf(document).Key().Kind() == reflect.String {
			return bson.NewDocumentEncoder().EncodeDocument(document)
		}

		return nil, fmt.Errorf("cannot transform type %s to a *bson.Document", reflect.TypeOf(document))
	}
}

func ensureID(d *bson.Document) (interface{}, error) {
	var id interface{}

	elem, err := d.LookupElementErr("_id")
	switch {
	case err == bson.ErrElementNotFound:
		oid := objectid.New()
		d.Append(bson.EC.ObjectID("_id", oid))
		id = oid
	case err != nil:
		return nil, err
	default:
		id = elem
	}
	return id, nil
}

func ensureDollarKey(doc *bson.Document) error {
	if elem, ok := doc.ElementAtOK(0); !ok || !strings.HasPrefix(elem.Key(), "$") {
		return errors.New("update document must contain key beginning with '$'")
	}
	return nil
}

func transformAggregatePipeline(pipeline interface{}) (*bson.Array, error) {
	var pipelineArr *bson.Array
	switch t := pipeline.(type) {
	case *bson.Array:
		pipelineArr = t
	case []*bson.Document:
		pipelineArr = bson.NewArray()

		for _, doc := range t {
			pipelineArr.Append(bson.VC.Document(doc))
		}
	case []interface{}:
		pipelineArr = bson.NewArray()

		for _, val := range t {
			doc, err := TransformDocument(val)
			if err != nil {
				return nil, err
			}

			pipelineArr.Append(bson.VC.Document(doc))
		}
	default:
		p, err := TransformDocument(pipeline)
		if err != nil {
			return nil, err
		}

		pipelineArr = bson.ArrayFromDocument(p)
	}

	return pipelineArr, nil
}

// Build the aggregation pipeline for the CountDocument command.
func countDocumentsAggregatePipeline(filter interface{}, opts ...countopt.Count) (*bson.Array, error) {
	pipeline := bson.NewArray()
	filterDoc, err := TransformDocument(filter)

	if err != nil {
		return nil, err
	}
	pipeline.Append(bson.VC.Document(bson.NewDocument(bson.EC.SubDocument("$match", filterDoc))))
	for _, opt := range opts {
		switch t := opt.(type) {
		case countopt.OptSkip:
			skip := int64(t)
			pipeline.Append(bson.VC.Document(bson.NewDocument(bson.EC.Int64("$skip", skip))))
		case countopt.OptLimit:
			limit := int64(t)
			pipeline.Append(bson.VC.Document(bson.NewDocument(bson.EC.Int64("$limit", limit))))
		}
	}
	pipeline.Append(bson.VC.Document(bson.NewDocument(
		bson.EC.SubDocument("$group", bson.NewDocument(
			bson.EC.Null("_id"),
			bson.EC.SubDocument("n", bson.NewDocument(
				bson.EC.Int32("$sum", 1)),
			)),
		)),
	))

	return pipeline, nil
}
