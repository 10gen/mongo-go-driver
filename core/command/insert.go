// Copyright (C) MongoDB, Inc. 2017-present.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

package command

import (
	"context"

	"github.com/mongodb/mongo-go-driver/bson"
	"github.com/mongodb/mongo-go-driver/core/description"
	"github.com/mongodb/mongo-go-driver/core/option"
	"github.com/mongodb/mongo-go-driver/core/result"
	"github.com/mongodb/mongo-go-driver/core/wiremessage"
	"github.com/mongodb/mongo-go-driver/core/writeconcern"
)

// this is the amount of reserved buffer space in a message that the
// driver reserves for command overhead.
const reservedCommandBufferBytes = 16 * 10 * 10 * 10

// Insert represents the insert command.
//
// The insert command inserts a set of documents into the database.
//
// Since the Insert command does not return any value other than ok or
// an error, this type has no Err method.
type Insert struct {
	NS           Namespace
	Docs         []*bson.Document
	Opts         []option.InsertOptioner
	WriteConcern *writeconcern.WriteConcern

	result          result.Insert
	err             error
	continueOnError bool
}

func (i *Insert) split(maxCount, targetBatchSize int) ([][]*bson.Document, error) {
	batches := [][]*bson.Document{}

	if targetBatchSize > reservedCommandBufferBytes {
		targetBatchSize -= reservedCommandBufferBytes
	}

	if maxCount <= 0 {
		maxCount = 1
	}

	startAt := 0
splitInserts:
	for {
		size := 0
		batch := []*bson.Document{}
	assembleBatch:
		for idx := startAt; idx < len(i.Docs); idx++ {
			itsize, err := i.Docs[idx].Validate()
			if err != nil {
				return nil, err
			}

			if size+int(itsize) > targetBatchSize {
				break assembleBatch
			}

			size += int(itsize)
			batch = append(batch, i.Docs[idx])
			startAt++
			if len(batch) == maxCount {
				break assembleBatch
			}
		}
		batches = append(batches, batch)
		if startAt == len(i.Docs) {
			break splitInserts
		}
	}

	return batches, nil
}

func (i *Insert) encodeBatch(docs []*bson.Document, desc description.SelectedServer) (wiremessage.WireMessage, error) {

	command := bson.NewDocument(bson.EC.String("insert", i.NS.Collection))

	vals := make([]*bson.Value, 0, len(docs))
	for _, doc := range docs {
		vals = append(vals, bson.VC.Document(doc))
	}
	command.Append(bson.EC.ArrayFromElements("documents", vals...))

	for _, opt := range i.Opts {
		if opt == nil {
			continue
		}

		if ordered, ok := opt.(option.OptOrdered); ok {
			if !ordered {
				i.continueOnError = true
			}
		}

		err := opt.Option(command)
		if err != nil {
			return nil, err
		}
	}

	return (&Write{
		DB:           i.NS.DB,
		Command:      command,
		WriteConcern: i.WriteConcern,
	}).Encode(desc)
}

// Encode will encode this command into a wire message for the given server description.
func (i *Insert) Encode(desc description.SelectedServer) ([]wiremessage.WireMessage, error) {
	out := []wiremessage.WireMessage{}
	batches, err := i.split(int(desc.MaxBatchCount), int(desc.MaxDocumentSize))
	if err != nil {
		return nil, err
	}

	for _, docs := range batches {
		cmd, err := i.encodeBatch(docs, desc)
		if err != nil {
			return nil, err
		}

		out = append(out, cmd)
	}

	return out, nil
}

// Decode will decode the wire message using the provided server description. Errors during decoding
// are deferred until either the Result or Err methods are called.
func (i *Insert) Decode(desc description.SelectedServer, wm wiremessage.WireMessage) *Insert {
	rdr, err := (&Write{}).Decode(desc, wm).Result()
	if err != nil {
		i.err = err
		return i
	}

	i.err = bson.Unmarshal(rdr, &i.result)
	return i
}

// Result returns the result of a decoded wire message and server description.
func (i *Insert) Result() (result.Insert, error) {
	if i.err != nil {
		return result.Insert{}, i.err
	}
	return i.result, nil
}

// Err returns the error set on this command.
func (i *Insert) Err() error { return i.err }

// send a series of wire messages to the server for this command
// closes the ReadWriter after all messages have been sent and responses have been parsed
func (i *Insert) sendMessages(ctx context.Context, desc description.SelectedServer, rw wiremessage.ReadWriter, wms []wiremessage.WireMessage, acknowledged bool) (result.Insert, error) {

	var err error
	res := result.Insert{}

	for _, wm := range wms {
		err = rw.WriteWireMessage(ctx, wm)
		if err != nil {
			return res, err
		}

		// if this is an acknowledged write, wait for a response
		// if this is an unacknowledged write, only wait for a response if using OP_QUERY, not OP_MSG
		_, isQuery := wm.(wiremessage.Query)

		if acknowledged || isQuery {
			wm, err = rw.ReadWireMessage(ctx)
			r, err := i.Decode(desc, wm).Result()
			if err != nil {
				return res, err
			}

			res.WriteErrors = append(res.WriteErrors, r.WriteErrors...)

			if r.WriteConcernError != nil {
				res.WriteConcernError = r.WriteConcernError
			}

			res.N += r.N
		}

		if !i.continueOnError && len(res.WriteErrors) > 0 {
			return res, nil
		}
	}

	return res, nil
}

// RoundTrip handles the execution of this command using the provided wiremessage.ReadWriter.
func (i *Insert) RoundTrip(ctx context.Context, desc description.SelectedServer, rw wiremessage.ReadWriter) (result.Insert, error) {
	res := result.Insert{}

	wms, err := i.Encode(desc)
	if err != nil {
		return res, err
	}

	if !writeconcern.AckWrite(i.WriteConcern) {
		go func() {
			defer func() { _ = recover() }()

			_, _ = i.sendMessages(ctx, desc, rw, wms, false)
		}()

		return result.Insert{}, ErrUnacknowledgedWrite
	}

	return i.sendMessages(ctx, desc, rw, wms, true)
}
