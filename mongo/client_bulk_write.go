// Copyright (C) MongoDB, Inc. 2024-present.
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
	"strconv"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/internal/driverutil"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/writeconcern"
	"go.mongodb.org/mongo-driver/v2/x/bsonx/bsoncore"
	"go.mongodb.org/mongo-driver/v2/x/mongo/driver"
	"go.mongodb.org/mongo-driver/v2/x/mongo/driver/description"
	"go.mongodb.org/mongo-driver/v2/x/mongo/driver/session"
	"go.mongodb.org/mongo-driver/v2/x/mongo/driver/wiremessage"
)

const (
	database = "admin"
)

type clientBulkWritePair struct {
	namespace string
	model     interface{}
}

type clientBulkWrite struct {
	writePairs               []clientBulkWritePair
	errorsOnly               bool
	ordered                  *bool
	bypassDocumentValidation *bool
	comment                  interface{}
	let                      interface{}
	session                  *session.Client
	client                   *Client
	selector                 description.ServerSelector
	writeConcern             *writeconcern.WriteConcern

	result ClientBulkWriteResult
}

func (bw *clientBulkWrite) execute(ctx context.Context) error {
	if len(bw.writePairs) == 0 {
		return fmt.Errorf("error from writes: %w", ErrEmptySlice)
	}
	for i, m := range bw.writePairs {
		if m.model == nil {
			return fmt.Errorf("error from model at index %d: %w", i, ErrNilDocument)
		}
	}
	batches := &modelBatches{
		session:    bw.session,
		client:     bw.client,
		ordered:    bw.ordered == nil || *bw.ordered,
		writePairs: bw.writePairs,
		result:     &bw.result,
		retryMode:  driver.RetryOnce,
	}
	err := driver.Operation{
		CommandFn:         bw.newCommand(),
		ProcessResponseFn: batches.processResponse,
		Client:            bw.session,
		Clock:             bw.client.clock,
		RetryMode:         &batches.retryMode,
		Type:              driver.Write,
		Batches:           batches,
		CommandMonitor:    bw.client.monitor,
		Database:          database,
		Deployment:        bw.client.deployment,
		Selector:          bw.selector,
		WriteConcern:      bw.writeConcern,
		Crypt:             bw.client.cryptFLE,
		ServerAPI:         bw.client.serverAPI,
		Timeout:           bw.client.timeout,
		Logger:            bw.client.logger,
		Authenticator:     bw.client.authenticator,
		Name:              driverutil.BulkWriteOp,
	}.Execute(ctx)
	var exception *ClientBulkWriteException

	var ce CommandError
	if errors.As(err, &ce) {
		exception = &ClientBulkWriteException{
			WriteError: &WriteError{
				Code:    int(ce.Code),
				Message: ce.Message,
				Raw:     ce.Raw,
			},
		}
	}
	if len(batches.writeConcernErrors) > 0 || len(batches.writeErrors) > 0 {
		if exception == nil {
			exception = new(ClientBulkWriteException)
		}
		exception.WriteConcernErrors = batches.writeConcernErrors
		exception.WriteErrors = batches.writeErrors
	}
	if exception != nil {
		var hasSuccess bool
		if batches.ordered {
			_, ok := batches.writeErrors[0]
			hasSuccess = !ok
		} else {
			hasSuccess = len(batches.writeErrors) < len(bw.writePairs)
		}
		if hasSuccess {
			exception.PartialResult = batches.result
		}
		return *exception
	}
	return err
}

func (bw *clientBulkWrite) newCommand() func([]byte, description.SelectedServer) ([]byte, error) {
	return func(dst []byte, desc description.SelectedServer) ([]byte, error) {
		dst = bsoncore.AppendInt32Element(dst, "bulkWrite", 1)

		dst = bsoncore.AppendBooleanElement(dst, "errorsOnly", bw.errorsOnly)
		if bw.bypassDocumentValidation != nil && (desc.WireVersion != nil && driverutil.VersionRangeIncludes(*desc.WireVersion, 4)) {
			dst = bsoncore.AppendBooleanElement(dst, "bypassDocumentValidation", *bw.bypassDocumentValidation)
		}
		if bw.comment != nil {
			comment, err := marshalValue(bw.comment, bw.client.bsonOpts, bw.client.registry)
			if err != nil {
				return nil, err
			}
			dst = bsoncore.AppendValueElement(dst, "comment", comment)
		}
		dst = bsoncore.AppendBooleanElement(dst, "ordered", bw.ordered == nil || *bw.ordered)
		if bw.let != nil {
			let, err := marshal(bw.let, bw.client.bsonOpts, bw.client.registry)
			if err != nil {
				return nil, err
			}
			dst = bsoncore.AppendDocumentElement(dst, "let", let)
		}
		return dst, nil
	}
}

type cursorInfo struct {
	Ok        bool
	Idx       int32
	Code      *int32
	Errmsg    *string
	ErrInfo   bson.Raw
	N         int32
	NModified *int32
	Upserted  *struct {
		ID interface{} `bson:"_id"`
	}
}

func (cur *cursorInfo) extractError() *WriteError {
	if cur.Ok {
		return nil
	}
	err := &WriteError{
		Index:   int(cur.Idx),
		Details: cur.ErrInfo,
	}
	if cur.Code != nil {
		err.Code = int(*cur.Code)
	}
	if cur.Errmsg != nil {
		err.Message = *cur.Errmsg
	}
	return err
}

type modelBatches struct {
	session *session.Client
	client  *Client

	ordered    bool
	writePairs []clientBulkWritePair

	offset int

	retryMode      driver.RetryMode // RetryNone by default
	cursorHandlers []func(*cursorInfo, bson.Raw) bool
	newIDMap       map[int]interface{}

	result             *ClientBulkWriteResult
	writeConcernErrors []WriteConcernError
	writeErrors        map[int]WriteError
}

var _ driver.OperationBatches = &modelBatches{}

func (mb *modelBatches) IsOrdered() *bool {
	return &mb.ordered
}

func (mb *modelBatches) AdvanceBatches(n int) {
	mb.offset += n
	if mb.offset > len(mb.writePairs) {
		mb.offset = len(mb.writePairs)
	}
}

func (mb *modelBatches) Size() int {
	if mb.offset > len(mb.writePairs) {
		return 0
	}
	return len(mb.writePairs) - mb.offset
}

func (mb *modelBatches) AppendBatchSequence(dst []byte, maxCount, totalSize int) (int, []byte, error) {
	fn := functionSet{
		appendStart: func(dst []byte, identifier string) (int32, []byte) {
			var idx int32
			dst = wiremessage.AppendMsgSectionType(dst, wiremessage.DocumentSequence)
			idx, dst = bsoncore.ReserveLength(dst)
			dst = append(dst, identifier...)
			dst = append(dst, 0x00)
			return idx, dst
		},
		appendDocument: func(dst []byte, _ string, doc []byte) []byte {
			dst = append(dst, doc...)
			return dst
		},
		updateLength: func(dst []byte, idx, length int32) []byte {
			dst = bsoncore.UpdateLength(dst, idx, length)
			return dst
		},
	}
	return mb.appendBatches(fn, dst, maxCount, totalSize)
}

func (mb *modelBatches) AppendBatchArray(dst []byte, maxCount, totalSize int) (int, []byte, error) {
	fn := functionSet{
		appendStart:    bsoncore.AppendArrayElementStart,
		appendDocument: bsoncore.AppendDocumentElement,
		updateLength: func(dst []byte, idx, _ int32) []byte {
			dst, _ = bsoncore.AppendArrayEnd(dst, idx)
			return dst
		},
	}
	return mb.appendBatches(fn, dst, maxCount, totalSize)
}

type functionSet struct {
	appendStart    func([]byte, string) (int32, []byte)
	appendDocument func([]byte, string, []byte) []byte
	updateLength   func([]byte, int32, int32) []byte
}

func (mb *modelBatches) appendBatches(fn functionSet, dst []byte, maxCount, totalSize int) (int, []byte, error) {
	if mb.Size() == 0 {
		return 0, dst, io.EOF
	}

	mb.cursorHandlers = mb.cursorHandlers[:0]
	mb.newIDMap = make(map[int]interface{})

	nsMap := make(map[string]int)
	getNsIndex := func(namespace string) (int, bool) {
		v, ok := nsMap[namespace]
		if ok {
			return v, ok
		}
		nsIdx := len(nsMap)
		nsMap[namespace] = nsIdx
		return nsIdx, ok
	}

	canRetry := true
	l := len(dst)

	opsIdx, dst := fn.appendStart(dst, "ops")
	nsIdx, nsDst := fn.appendStart(nil, "nsInfo")

	totalSize -= 1000
	size := len(dst) + len(nsDst)
	var n int
	for i := mb.offset; i < len(mb.writePairs); i++ {
		if n == maxCount {
			break
		}

		ns := mb.writePairs[i].namespace
		nsIdx, exists := getNsIndex(ns)

		var doc bsoncore.Document
		var err error
		switch model := mb.writePairs[i].model.(type) {
		case *ClientInsertOneModel:
			mb.cursorHandlers = append(mb.cursorHandlers, mb.appendInsertResult)
			var id interface{}
			id, doc, err = (&clientInsertDoc{
				namespace: nsIdx,
				document:  model.Document,
			}).marshal(mb.client.bsonOpts, mb.client.registry)
			if err != nil {
				break
			}
			mb.newIDMap[i] = id
		case *ClientUpdateOneModel:
			mb.cursorHandlers = append(mb.cursorHandlers, mb.appendUpdateResult)
			doc, err = (&clientUpdateDoc{
				namespace:      nsIdx,
				filter:         model.Filter,
				update:         model.Update,
				hint:           model.Hint,
				arrayFilters:   model.ArrayFilters,
				collation:      model.Collation,
				upsert:         model.Upsert,
				sort:           model.Sort,
				multi:          false,
				checkDollarKey: true,
			}).marshal(mb.client.bsonOpts, mb.client.registry)
		case *ClientUpdateManyModel:
			canRetry = false
			mb.cursorHandlers = append(mb.cursorHandlers, mb.appendUpdateResult)
			doc, err = (&clientUpdateDoc{
				namespace:      nsIdx,
				filter:         model.Filter,
				update:         model.Update,
				hint:           model.Hint,
				arrayFilters:   model.ArrayFilters,
				collation:      model.Collation,
				upsert:         model.Upsert,
				multi:          true,
				checkDollarKey: true,
			}).marshal(mb.client.bsonOpts, mb.client.registry)
		case *ClientReplaceOneModel:
			mb.cursorHandlers = append(mb.cursorHandlers, mb.appendUpdateResult)
			doc, err = (&clientUpdateDoc{
				namespace:      nsIdx,
				filter:         model.Filter,
				update:         model.Replacement,
				hint:           model.Hint,
				arrayFilters:   nil,
				collation:      model.Collation,
				upsert:         model.Upsert,
				sort:           model.Sort,
				multi:          false,
				checkDollarKey: false,
			}).marshal(mb.client.bsonOpts, mb.client.registry)
		case *ClientDeleteOneModel:
			mb.cursorHandlers = append(mb.cursorHandlers, mb.appendDeleteResult)
			doc, err = (&clientDeleteDoc{
				namespace: nsIdx,
				filter:    model.Filter,
				collation: model.Collation,
				hint:      model.Hint,
				multi:     false,
			}).marshal(mb.client.bsonOpts, mb.client.registry)
		case *ClientDeleteManyModel:
			canRetry = false
			mb.cursorHandlers = append(mb.cursorHandlers, mb.appendDeleteResult)
			doc, err = (&clientDeleteDoc{
				namespace: nsIdx,
				filter:    model.Filter,
				collation: model.Collation,
				hint:      model.Hint,
				multi:     true,
			}).marshal(mb.client.bsonOpts, mb.client.registry)
		default:
			mb.cursorHandlers = append(mb.cursorHandlers, nil)
		}
		if err != nil {
			return 0, nil, err
		}
		length := len(doc)
		if !exists {
			length += len(ns)
		}
		size += length
		if size >= totalSize {
			break
		}

		dst = fn.appendDocument(dst, strconv.Itoa(n), doc)
		if !exists {
			idx, doc := bsoncore.AppendDocumentStart(nil)
			doc = bsoncore.AppendStringElement(doc, "ns", ns)
			doc, _ = bsoncore.AppendDocumentEnd(doc, idx)
			nsDst = fn.appendDocument(nsDst, strconv.Itoa(n), doc)
		}
		n++
	}
	if n == 0 {
		return 0, dst[:l], nil
	}

	dst = fn.updateLength(dst, opsIdx, int32(len(dst[opsIdx:])))
	nsDst = fn.updateLength(nsDst, nsIdx, int32(len(nsDst[nsIdx:])))
	dst = append(dst, nsDst...)

	mb.retryMode = driver.RetryNone
	if mb.client.retryWrites && canRetry {
		mb.retryMode = driver.RetryOnce
	}
	return n, dst, nil
}

func (mb *modelBatches) processResponse(ctx context.Context, resp bsoncore.Document, info driver.ResponseInfo) error {
	var writeCmdErr driver.WriteCommandError
	if errors.As(info.Error, &writeCmdErr) && writeCmdErr.WriteConcernError != nil {
		wce := convertDriverWriteConcernError(writeCmdErr.WriteConcernError)
		if wce != nil {
			mb.writeConcernErrors = append(mb.writeConcernErrors, *wce)
		}
	}
	if len(resp) == 0 {
		return nil
	}
	var res struct {
		Ok        bool
		Cursor    bsoncore.Document
		NDeleted  int32
		NInserted int32
		NMatched  int32
		NModified int32
		NUpserted int32
		NErrors   int32
		Code      int32
		Errmsg    string
	}
	err := bson.Unmarshal(resp, &res)
	if err != nil {
		return err
	}
	if !res.Ok {
		return ClientBulkWriteException{
			WriteError: &WriteError{
				Code:    int(res.Code),
				Message: res.Errmsg,
				Raw:     bson.Raw(resp),
			},
			WriteConcernErrors: mb.writeConcernErrors,
			WriteErrors:        mb.writeErrors,
			PartialResult:      mb.result,
		}
	}

	if mb.result.Acknowledged {
		mb.result.DeletedCount += int64(res.NDeleted)
		mb.result.InsertedCount += int64(res.NInserted)
		mb.result.MatchedCount += int64(res.NMatched)
		mb.result.ModifiedCount += int64(res.NModified)
		mb.result.UpsertedCount += int64(res.NUpserted)
	}

	var cursorRes driver.CursorResponse
	cursorRes, err = driver.NewCursorResponse(res.Cursor, info)
	if err != nil {
		return err
	}
	var bCursor *driver.BatchCursor
	bCursor, err = driver.NewBatchCursor(cursorRes, mb.session, mb.client.clock,
		driver.CursorOptions{
			CommandMonitor:        mb.client.monitor,
			Crypt:                 mb.client.cryptFLE,
			ServerAPI:             mb.client.serverAPI,
			MarshalValueEncoderFn: newEncoderFn(mb.client.bsonOpts, mb.client.registry),
		},
	)
	if err != nil {
		return err
	}
	var cursor *Cursor
	cursor, err = newCursor(bCursor, mb.client.bsonOpts, mb.client.registry)
	if err != nil {
		return err
	}
	defer cursor.Close(ctx)

	ok := true
	for cursor.Next(ctx) {
		var cur cursorInfo
		err = cursor.Decode(&cur)
		if err != nil {
			return err
		}
		if int(cur.Idx) >= len(mb.cursorHandlers) {
			continue
		}
		ok = mb.cursorHandlers[int(cur.Idx)](&cur, cursor.Current) && ok
	}
	err = cursor.Err()
	if err != nil {
		return err
	}
	if mb.ordered && (writeCmdErr.WriteConcernError != nil || !ok || !res.Ok || res.NErrors > 0) {
		return ClientBulkWriteException{
			WriteConcernErrors: mb.writeConcernErrors,
			WriteErrors:        mb.writeErrors,
			PartialResult:      mb.result,
		}
	}
	return nil
}

func (mb *modelBatches) appendDeleteResult(cur *cursorInfo, raw bson.Raw) bool {
	idx := int(cur.Idx) + mb.offset
	if err := cur.extractError(); err != nil {
		err.Raw = raw
		if mb.writeErrors == nil {
			mb.writeErrors = make(map[int]WriteError)
		}
		mb.writeErrors[idx] = *err
		return false
	}

	if mb.result.Acknowledged {
		if mb.result.DeleteResults == nil {
			mb.result.DeleteResults = make(map[int]ClientBulkWriteDeleteResult)
		}
		mb.result.DeleteResults[idx] = ClientBulkWriteDeleteResult{int64(cur.N)}
	}

	return true
}

func (mb *modelBatches) appendInsertResult(cur *cursorInfo, raw bson.Raw) bool {
	idx := int(cur.Idx) + mb.offset
	if err := cur.extractError(); err != nil {
		err.Raw = raw
		if mb.writeErrors == nil {
			mb.writeErrors = make(map[int]WriteError)
		}
		mb.writeErrors[idx] = *err
		return false
	}

	if mb.result.Acknowledged {
		if mb.result.InsertResults == nil {
			mb.result.InsertResults = make(map[int]ClientBulkWriteInsertResult)
		}
		mb.result.InsertResults[idx] = ClientBulkWriteInsertResult{mb.newIDMap[idx]}
	}

	return true
}

func (mb *modelBatches) appendUpdateResult(cur *cursorInfo, raw bson.Raw) bool {
	idx := int(cur.Idx) + mb.offset
	if err := cur.extractError(); err != nil {
		err.Raw = raw
		if mb.writeErrors == nil {
			mb.writeErrors = make(map[int]WriteError)
		}
		mb.writeErrors[idx] = *err
		return false
	}

	if mb.result.Acknowledged {
		if mb.result.UpdateResults == nil {
			mb.result.UpdateResults = make(map[int]ClientBulkWriteUpdateResult)
		}
		result := ClientBulkWriteUpdateResult{
			MatchedCount: int64(cur.N),
		}
		if cur.NModified != nil {
			result.ModifiedCount = int64(*cur.NModified)
		}
		if cur.Upserted != nil {
			result.UpsertedID = cur.Upserted.ID
		}
		mb.result.UpdateResults[idx] = result
	}

	return true
}

type clientInsertDoc struct {
	namespace int
	document  interface{}
}

func (d *clientInsertDoc) marshal(bsonOpts *options.BSONOptions, registry *bson.Registry) (interface{}, bsoncore.Document, error) {
	uidx, doc := bsoncore.AppendDocumentStart(nil)

	doc = bsoncore.AppendInt32Element(doc, "insert", int32(d.namespace))
	f, err := marshal(d.document, bsonOpts, registry)
	if err != nil {
		return nil, nil, err
	}
	var id interface{}
	f, id, err = ensureID(f, bson.NilObjectID, bsonOpts, registry)
	if err != nil {
		return nil, nil, err
	}
	doc = bsoncore.AppendDocumentElement(doc, "document", f)
	doc, err = bsoncore.AppendDocumentEnd(doc, uidx)
	return id, doc, err
}

type clientUpdateDoc struct {
	namespace      int
	filter         interface{}
	update         interface{}
	hint           interface{}
	arrayFilters   []interface{}
	collation      *options.Collation
	sort           interface{}
	upsert         *bool
	multi          bool
	checkDollarKey bool
}

func (d *clientUpdateDoc) marshal(bsonOpts *options.BSONOptions, registry *bson.Registry) (bsoncore.Document, error) {
	uidx, doc := bsoncore.AppendDocumentStart(nil)

	doc = bsoncore.AppendInt32Element(doc, "update", int32(d.namespace))

	if d.filter == nil {
		return nil, fmt.Errorf("update filter cannot be nil")
	}
	f, err := marshal(d.filter, bsonOpts, registry)
	if err != nil {
		return nil, err
	}
	doc = bsoncore.AppendDocumentElement(doc, "filter", f)

	u, err := marshalUpdateValue(d.update, bsonOpts, registry, d.checkDollarKey)
	if err != nil {
		return nil, err
	}
	doc = bsoncore.AppendValueElement(doc, "updateMods", u)
	doc = bsoncore.AppendBooleanElement(doc, "multi", d.multi)

	if d.arrayFilters != nil {
		reg := registry
		arr, err := marshalValue(d.arrayFilters, bsonOpts, reg)
		if err != nil {
			return nil, err
		}
		doc = bsoncore.AppendArrayElement(doc, "arrayFilters", arr.Data)
	}

	if d.collation != nil {
		doc = bsoncore.AppendDocumentElement(doc, "collation", toDocument(d.collation))
	}

	if d.upsert != nil {
		doc = bsoncore.AppendBooleanElement(doc, "upsert", *d.upsert)
	}

	if d.hint != nil {
		if isUnorderedMap(d.hint) {
			return nil, ErrMapForOrderedArgument{"hint"}
		}
		hintVal, err := marshalValue(d.hint, bsonOpts, registry)
		if err != nil {
			return nil, err
		}
		doc = bsoncore.AppendValueElement(doc, "hint", hintVal)
	}

	if d.sort != nil {
		if isUnorderedMap(d.sort) {
			return nil, ErrMapForOrderedArgument{"sort"}
		}
		sortVal, err := marshalValue(d.sort, bsonOpts, registry)
		if err != nil {
			return nil, err
		}
		doc = bsoncore.AppendValueElement(doc, "sort", sortVal)
	}

	return bsoncore.AppendDocumentEnd(doc, uidx)
}

type clientDeleteDoc struct {
	namespace int
	filter    interface{}
	collation *options.Collation
	hint      interface{}
	multi     bool
}

func (d *clientDeleteDoc) marshal(bsonOpts *options.BSONOptions, registry *bson.Registry) (bsoncore.Document, error) {
	didx, doc := bsoncore.AppendDocumentStart(nil)

	doc = bsoncore.AppendInt32Element(doc, "delete", int32(d.namespace))

	if d.filter == nil {
		return nil, fmt.Errorf("delete filter cannot be nil")
	}
	f, err := marshal(d.filter, bsonOpts, registry)
	if err != nil {
		return nil, err
	}
	doc = bsoncore.AppendDocumentElement(doc, "filter", f)
	doc = bsoncore.AppendBooleanElement(doc, "multi", d.multi)

	if d.collation != nil {
		doc = bsoncore.AppendDocumentElement(doc, "collation", toDocument(d.collation))
	}
	if d.hint != nil {
		if isUnorderedMap(d.hint) {
			return nil, ErrMapForOrderedArgument{"hint"}
		}
		hintVal, err := marshalValue(d.hint, bsonOpts, registry)
		if err != nil {
			return nil, err
		}
		doc = bsoncore.AppendValueElement(doc, "hint", hintVal)
	}
	return bsoncore.AppendDocumentEnd(doc, didx)
}
