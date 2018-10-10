// Copyright (C) MongoDB, Inc. 2017-present.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

package mongo

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	"github.com/mongodb/mongo-go-driver/bson"
	"github.com/mongodb/mongo-go-driver/core/command"
	"github.com/mongodb/mongo-go-driver/core/dispatch"
	"github.com/mongodb/mongo-go-driver/options"
)

// ErrInvalidIndexValue indicates that the index Keys document has a value that isn't either a number or a string.
var ErrInvalidIndexValue = errors.New("invalid index value")

// ErrNonStringIndexName indicates that the index name specified in the options is not a string.
var ErrNonStringIndexName = errors.New("index name must be a string")

// ErrMultipleIndexDrop indicates that multiple indexes would be dropped from a call to IndexView.DropOne.
var ErrMultipleIndexDrop = errors.New("multiple indexes would be dropped")

// IndexView is used to create, drop, and list indexes on a given collection.
type IndexView struct {
	coll *Collection
}

// IndexModel contains information about an index.
type IndexModel struct {
	Keys    *bson.Document
	Options *bson.Document
}

// List returns a cursor iterating over all the indexes in the collection.
func (iv IndexView) List(ctx context.Context, opts ...*options.ListIndexesOptions) (Cursor, error) {
	sess := sessionFromContext(ctx)

	err := iv.coll.client.ValidSession(sess)
	if err != nil {
		return nil, err
	}

	listCmd := command.ListIndexes{
		NS:      iv.coll.namespace(),
		Session: sess,
		Clock:   iv.coll.client.clock,
	}

	return dispatch.ListIndexes(
		ctx, listCmd,
		iv.coll.client.topology,
		iv.coll.writeSelector,
		iv.coll.client.id,
		iv.coll.client.topology.SessionPool,
		opts...,
	)
}

// CreateOne creates a single index in the collection specified by the model.
func (iv IndexView) CreateOne(ctx context.Context, model IndexModel, opts ...*options.CreateIndexesOptions) (string, error) {
	names, err := iv.CreateMany(ctx, []IndexModel{model}, opts...)
	if err != nil {
		return "", err
	}

	return names[0], nil
}

// CreateMany creates multiple indexes in the collection specified by the models. The names of the
// creates indexes are returned.
func (iv IndexView) CreateMany(ctx context.Context, models []IndexModel, opts ...*options.CreateIndexesOptions) ([]string, error) {
	names := make([]string, 0, len(models))
	indexes := bson.NewArray()

	for _, model := range models {
		if model.Keys == nil {
			return nil, fmt.Errorf("index model keys cannot be nil")
		}

		name, err := getOrGenerateIndexName(model)
		if err != nil {
			return nil, err
		}

		names = append(names, name)

		index := bson.NewDocument(
			bson.EC.SubDocument("key", model.Keys),
		)
		if model.Options != nil {
			err = index.Concat(model.Options)
			if err != nil {
				return nil, err
			}
		}
		index.Set(bson.EC.String("name", name))

		indexes.Append(bson.VC.Document(index))
	}

	sess := sessionFromContext(ctx)

	err := iv.coll.client.ValidSession(sess)
	if err != nil {
		return nil, err
	}

	cmd := command.CreateIndexes{
		NS:      iv.coll.namespace(),
		Indexes: indexes,
		Session: sess,
		Clock:   iv.coll.client.clock,
	}

	_, err = dispatch.CreateIndexes(
		ctx, cmd,
		iv.coll.client.topology,
		iv.coll.writeSelector,
		iv.coll.client.id,
		iv.coll.client.topology.SessionPool,
		opts...,
	)
	if err != nil {
		return nil, err
	}

	return names, nil
}

// DropOne drops the index with the given name from the collection.
func (iv IndexView) DropOne(ctx context.Context, name string, opts ...*options.DropIndexesOptions) (bson.Reader, error) {
	if name == "*" {
		return nil, ErrMultipleIndexDrop
	}

	sess := sessionFromContext(ctx)

	err := iv.coll.client.ValidSession(sess)
	if err != nil {
		return nil, err
	}

	cmd := command.DropIndexes{
		NS:      iv.coll.namespace(),
		Index:   name,
		Session: sess,
		Clock:   iv.coll.client.clock,
	}

	return dispatch.DropIndexes(
		ctx, cmd,
		iv.coll.client.topology,
		iv.coll.writeSelector,
		iv.coll.client.id,
		iv.coll.client.topology.SessionPool,
		opts...,
	)
}

// DropAll drops all indexes in the collection.
func (iv IndexView) DropAll(ctx context.Context, opts ...*options.DropIndexesOptions) (bson.Reader, error) {
	sess := sessionFromContext(ctx)

	err := iv.coll.client.ValidSession(sess)
	if err != nil {
		return nil, err
	}

	cmd := command.DropIndexes{
		NS:      iv.coll.namespace(),
		Index:   "*",
		Session: sess,
		Clock:   iv.coll.client.clock,
	}

	return dispatch.DropIndexes(
		ctx, cmd,
		iv.coll.client.topology,
		iv.coll.writeSelector,
		iv.coll.client.id,
		iv.coll.client.topology.SessionPool,
		opts...,
	)
}

func getOrGenerateIndexName(model IndexModel) (string, error) {
	if model.Options != nil {
		nameVal, err := model.Options.LookupErr("name")

		switch err {
		case bson.ErrElementNotFound:
			break
		case nil:
			if nameVal.Type() != bson.TypeString {
				return "", ErrNonStringIndexName
			}

			return nameVal.StringValue(), nil
		default:
			return "", err
		}
	}

	name := bytes.NewBufferString("")
	itr := model.Keys.Iterator()
	first := true

	for itr.Next() {
		if !first {
			_, err := name.WriteRune('_')
			if err != nil {
				return "", err
			}
		}

		elem := itr.Element()
		_, err := name.WriteString(elem.Key())
		if err != nil {
			return "", err
		}

		_, err = name.WriteRune('_')
		if err != nil {
			return "", err
		}

		var value string

		switch elem.Value().Type() {
		case bson.TypeInt32:
			value = fmt.Sprintf("%d", elem.Value().Int32())
		case bson.TypeInt64:
			value = fmt.Sprintf("%d", elem.Value().Int64())
		case bson.TypeString:
			value = elem.Value().StringValue()
		default:
			return "", ErrInvalidIndexValue
		}

		_, err = name.WriteString(value)
		if err != nil {
			return "", err
		}

		first = false
	}
	if err := itr.Err(); err != nil {
		return "", err
	}

	return name.String(), nil
}
