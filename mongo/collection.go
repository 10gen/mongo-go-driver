// Copyright (C) MongoDB, Inc. 2017-present.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

package mongo

import (
	"context"
	"errors"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/bsoncodec"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readconcern"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"go.mongodb.org/mongo-driver/mongo/writeconcern"
	"go.mongodb.org/mongo-driver/x/bsonx"
	"go.mongodb.org/mongo-driver/x/bsonx/bsoncore"
	"go.mongodb.org/mongo-driver/x/mongo/driver"
	"go.mongodb.org/mongo-driver/x/mongo/driver/description"
	"go.mongodb.org/mongo-driver/x/mongo/driver/operation"
	"go.mongodb.org/mongo-driver/x/mongo/driver/session"
	"go.mongodb.org/mongo-driver/x/mongo/driverlegacy"
	"go.mongodb.org/mongo-driver/x/network/command"
)

// Collection performs operations on a given collection.
type Collection struct {
	client         *Client
	db             *Database
	name           string
	readConcern    *readconcern.ReadConcern
	writeConcern   *writeconcern.WriteConcern
	readPreference *readpref.ReadPref
	readSelector   description.ServerSelector
	writeSelector  description.ServerSelector
	registry       *bsoncodec.Registry
}

func closeImplicitSession(sess *session.Client) {
	if sess != nil && sess.SessionType == session.Implicit {
		sess.EndSession()
	}
}

func newCollection(db *Database, name string, opts ...*options.CollectionOptions) *Collection {
	collOpt := options.MergeCollectionOptions(opts...)

	rc := db.readConcern
	if collOpt.ReadConcern != nil {
		rc = collOpt.ReadConcern
	}

	wc := db.writeConcern
	if collOpt.WriteConcern != nil {
		wc = collOpt.WriteConcern
	}

	rp := db.readPreference
	if collOpt.ReadPreference != nil {
		rp = collOpt.ReadPreference
	}

	reg := db.registry
	if collOpt.Registry != nil {
		reg = collOpt.Registry
	}

	readSelector := description.CompositeSelector([]description.ServerSelector{
		description.ReadPrefSelector(rp),
		description.LatencySelector(db.client.localThreshold),
	})

	writeSelector := description.CompositeSelector([]description.ServerSelector{
		description.WriteSelector(),
		description.LatencySelector(db.client.localThreshold),
	})

	coll := &Collection{
		client:         db.client,
		db:             db,
		name:           name,
		readPreference: rp,
		readConcern:    rc,
		writeConcern:   wc,
		readSelector:   readSelector,
		writeSelector:  writeSelector,
		registry:       reg,
	}

	return coll
}

func (coll *Collection) copy() *Collection {
	return &Collection{
		client:         coll.client,
		db:             coll.db,
		name:           coll.name,
		readConcern:    coll.readConcern,
		writeConcern:   coll.writeConcern,
		readPreference: coll.readPreference,
		readSelector:   coll.readSelector,
		writeSelector:  coll.writeSelector,
		registry:       coll.registry,
	}
}

// Clone creates a copy of this collection with updated options, if any are given.
func (coll *Collection) Clone(opts ...*options.CollectionOptions) (*Collection, error) {
	copyColl := coll.copy()
	optsColl := options.MergeCollectionOptions(opts...)

	if optsColl.ReadConcern != nil {
		copyColl.readConcern = optsColl.ReadConcern
	}

	if optsColl.WriteConcern != nil {
		copyColl.writeConcern = optsColl.WriteConcern
	}

	if optsColl.ReadPreference != nil {
		copyColl.readPreference = optsColl.ReadPreference
	}

	if optsColl.Registry != nil {
		copyColl.registry = optsColl.Registry
	}

	copyColl.readSelector = description.CompositeSelector([]description.ServerSelector{
		description.ReadPrefSelector(copyColl.readPreference),
		description.LatencySelector(copyColl.client.localThreshold),
	})

	return copyColl, nil
}

// Name provides access to the name of the collection.
func (coll *Collection) Name() string {
	return coll.name
}

// namespace returns the namespace of the collection.
func (coll *Collection) namespace() command.Namespace {
	return command.NewNamespace(coll.db.name, coll.name)
}

// Database provides access to the database that contains the collection.
func (coll *Collection) Database() *Database {
	return coll.db
}

// BulkWrite performs a bulk write operation.
//
// See https://docs.mongodb.com/manual/core/bulk-write-operations/.
func (coll *Collection) BulkWrite(ctx context.Context, models []WriteModel,
	opts ...*options.BulkWriteOptions) (*BulkWriteResult, error) {

	if len(models) == 0 {
		return nil, ErrEmptySlice
	}

	if ctx == nil {
		ctx = context.Background()
	}

	sess := sessionFromContext(ctx)

	err := coll.client.validSession(sess)
	if err != nil {
		return nil, err
	}

	dispatchModels := make([]driverlegacy.WriteModel, len(models))
	for i, model := range models {
		if model == nil {
			return nil, ErrNilDocument
		}
		dispatchModels[i] = model.convertModel()
	}

	res, err := driverlegacy.BulkWrite(
		ctx,
		coll.namespace(),
		dispatchModels,
		coll.client.topology,
		coll.writeSelector,
		coll.client.id,
		coll.client.topology.SessionPool,
		coll.client.retryWrites,
		sess,
		coll.writeConcern,
		coll.client.clock,
		coll.registry,
		opts...,
	)
	result := BulkWriteResult{
		InsertedCount: res.InsertedCount,
		MatchedCount:  res.MatchedCount,
		ModifiedCount: res.ModifiedCount,
		DeletedCount:  res.DeletedCount,
		UpsertedCount: res.UpsertedCount,
		UpsertedIDs:   res.UpsertedIDs,
	}

	return &result, replaceErrors(err)
}

// InsertOne inserts a single document into the collection.
func (coll *Collection) InsertOne(ctx context.Context, document interface{},
	opts ...*options.InsertOneOptions) (*InsertOneResult, error) {

	if ctx == nil {
		ctx = context.Background()
	}

	doc, insertedID, err := transformAndEnsureIDv2(coll.registry, document)
	if err != nil {
		return nil, err
	}

	sess := sessionFromContext(ctx)
	if sess == nil && coll.client.topology.SessionPool != nil {
		sess, err = session.NewClientSession(coll.client.topology.SessionPool, coll.client.id, session.Implicit)
		if err != nil {
			return nil, err
		}
		defer sess.EndSession()
	}

	err = coll.client.validSession(sess)
	if err != nil {
		return nil, err
	}

	wc := coll.writeConcern
	if sess.TransactionRunning() {
		wc = nil
	}
	if !writeconcern.AckWrite(wc) {
		sess = nil
	}

	selector := coll.writeSelector
	if sess != nil && sess.PinnedServer != nil {
		selector = sess.PinnedServer
	}

	option := options.MergeInsertOneOptions(opts...)

	op := operation.NewInsert(doc).
		Session(sess).WriteConcern(wc).CommandMonitor(coll.client.monitor).
		ServerSelector(selector).ClusterClock(coll.client.clock).
		Database(coll.db.name).Collection(coll.name).
		Deployment(coll.client.topology)

	if option.BypassDocumentValidation != nil {
		op = op.BypassDocumentValidation(*option.BypassDocumentValidation)
	}
	retry := driver.RetryNone
	if coll.client.retryWrites {
		retry = driver.RetryOncePerCommand
	}
	op = op.Retry(retry)
	err = op.Execute(ctx)

	rr, err := processWriteError(nil, nil, err)
	if rr&rrOne == 0 {
		return nil, err
	}

	return &InsertOneResult{InsertedID: insertedID}, err
}

// InsertMany inserts the provided documents.
func (coll *Collection) InsertMany(ctx context.Context, documents []interface{},
	opts ...*options.InsertManyOptions) (*InsertManyResult, error) {

	if ctx == nil {
		ctx = context.Background()
	}

	if len(documents) == 0 {
		return nil, ErrEmptySlice
	}

	result := make([]interface{}, len(documents))
	docs := make([]bsonx.Doc, len(documents))

	for i, doc := range documents {
		if doc == nil {
			return nil, ErrNilDocument
		}
		bdoc, insertedID, err := transformAndEnsureID(coll.registry, doc)
		if err != nil {
			return nil, err
		}

		docs[i] = bdoc
		result[i] = insertedID
	}

	sess := sessionFromContext(ctx)

	err := coll.client.validSession(sess)
	if err != nil {
		return nil, err
	}

	wc := coll.writeConcern
	if sess.TransactionRunning() {
		wc = nil
	}

	oldns := coll.namespace()
	cmd := command.Insert{
		NS:           command.Namespace{DB: oldns.DB, Collection: oldns.Collection},
		Docs:         docs,
		WriteConcern: wc,
		Session:      sess,
		Clock:        coll.client.clock,
	}

	res, err := driverlegacy.Insert(
		ctx, cmd,
		coll.client.topology,
		coll.writeSelector,
		coll.client.id,
		coll.client.topology.SessionPool,
		coll.client.retryWrites,
		opts...,
	)

	switch err {
	case nil:
	case command.ErrUnacknowledgedWrite:
		return &InsertManyResult{InsertedIDs: result}, ErrUnacknowledgedWrite
	default:
		return nil, replaceErrors(err)
	}
	if len(res.WriteErrors) > 0 || res.WriteConcernError != nil {
		bwErrors := make([]BulkWriteError, 0, len(res.WriteErrors))
		for _, we := range res.WriteErrors {
			bwErrors = append(bwErrors, BulkWriteError{
				WriteError{
					Index:   we.Index,
					Code:    we.Code,
					Message: we.ErrMsg,
				},
				nil,
			})
		}

		err = BulkWriteException{
			WriteErrors:       bwErrors,
			WriteConcernError: convertWriteConcernError(res.WriteConcernError),
		}
	}

	return &InsertManyResult{InsertedIDs: result}, err
}

// DeleteOne deletes a single document from the collection.
func (coll *Collection) DeleteOne(ctx context.Context, filter interface{},
	opts ...*options.DeleteOptions) (*DeleteResult, error) {

	if ctx == nil {
		ctx = context.Background()
	}

	f, err := transformDocument(coll.registry, filter)
	if err != nil {
		return nil, err
	}
	deleteDocs := []bsonx.Doc{
		{
			{"q", bsonx.Document(f)},
			{"limit", bsonx.Int32(1)},
		},
	}

	sess := sessionFromContext(ctx)

	err = coll.client.validSession(sess)
	if err != nil {
		return nil, err
	}

	wc := coll.writeConcern
	if sess.TransactionRunning() {
		wc = nil
	}

	oldns := coll.namespace()
	cmd := command.Delete{
		NS:           command.Namespace{DB: oldns.DB, Collection: oldns.Collection},
		Deletes:      deleteDocs,
		WriteConcern: wc,
		Session:      sess,
		Clock:        coll.client.clock,
	}

	res, err := driverlegacy.Delete(
		ctx, cmd,
		coll.client.topology,
		coll.writeSelector,
		coll.client.id,
		coll.client.topology.SessionPool,
		coll.client.retryWrites,
		opts...,
	)

	rr, err := processWriteError(res.WriteConcernError, res.WriteErrors, err)
	if rr&rrOne == 0 {
		return nil, err
	}
	return &DeleteResult{DeletedCount: int64(res.N)}, err
}

// DeleteMany deletes multiple documents from the collection.
func (coll *Collection) DeleteMany(ctx context.Context, filter interface{},
	opts ...*options.DeleteOptions) (*DeleteResult, error) {

	if ctx == nil {
		ctx = context.Background()
	}

	f, err := transformDocument(coll.registry, filter)
	if err != nil {
		return nil, err
	}
	deleteDocs := []bsonx.Doc{{{"q", bsonx.Document(f)}, {"limit", bsonx.Int32(0)}}}

	sess := sessionFromContext(ctx)

	err = coll.client.validSession(sess)
	if err != nil {
		return nil, err
	}

	wc := coll.writeConcern
	if sess.TransactionRunning() {
		wc = nil
	}

	oldns := coll.namespace()
	cmd := command.Delete{
		NS:           command.Namespace{DB: oldns.DB, Collection: oldns.Collection},
		Deletes:      deleteDocs,
		WriteConcern: wc,
		Session:      sess,
		Clock:        coll.client.clock,
	}

	res, err := driverlegacy.Delete(
		ctx, cmd,
		coll.client.topology,
		coll.writeSelector,
		coll.client.id,
		coll.client.topology.SessionPool,
		false,
		opts...,
	)

	rr, err := processWriteError(res.WriteConcernError, res.WriteErrors, err)
	if rr&rrMany == 0 {
		return nil, err
	}
	return &DeleteResult{DeletedCount: int64(res.N)}, err
}

func (coll *Collection) updateOrReplaceOne(ctx context.Context, filter,
	update bsonx.Doc, sess *session.Client, opts ...*options.UpdateOptions) (*UpdateResult, error) {

	// TODO: should session be taken from ctx or left as argument?
	if ctx == nil {
		ctx = context.Background()
	}

	updateDocs := []bsonx.Doc{
		{
			{"q", bsonx.Document(filter)},
			{"u", bsonx.Document(update)},
			{"multi", bsonx.Boolean(false)},
		},
	}

	wc := coll.writeConcern
	if sess.TransactionRunning() {
		wc = nil
	}

	oldns := coll.namespace()
	cmd := command.Update{
		NS:           command.Namespace{DB: oldns.DB, Collection: oldns.Collection},
		Docs:         updateDocs,
		WriteConcern: wc,
		Session:      sess,
		Clock:        coll.client.clock,
	}

	r, err := driverlegacy.Update(
		ctx, cmd,
		coll.client.topology,
		coll.writeSelector,
		coll.client.id,
		coll.client.topology.SessionPool,
		coll.client.retryWrites,
		opts...,
	)
	if err != nil && err != command.ErrUnacknowledgedWrite {
		return nil, replaceErrors(err)
	}

	res := &UpdateResult{
		MatchedCount:  r.MatchedCount,
		ModifiedCount: r.ModifiedCount,
		UpsertedCount: int64(len(r.Upserted)),
	}
	if len(r.Upserted) > 0 {
		res.UpsertedID = r.Upserted[0].ID
		res.MatchedCount--
	}

	rr, err := processWriteError(r.WriteConcernError, r.WriteErrors, err)
	if rr&rrOne == 0 {
		return nil, err
	}
	return res, err
}

// UpdateOne updates a single document in the collection.
func (coll *Collection) UpdateOne(ctx context.Context, filter interface{}, update interface{},
	opts ...*options.UpdateOptions) (*UpdateResult, error) {

	if ctx == nil {
		ctx = context.Background()
	}

	f, err := transformDocument(coll.registry, filter)
	if err != nil {
		return nil, err
	}

	u, err := transformDocument(coll.registry, update)
	if err != nil {
		return nil, err
	}

	if err := ensureDollarKey(u); err != nil {
		return nil, err
	}

	sess := sessionFromContext(ctx)

	err = coll.client.validSession(sess)
	if err != nil {
		return nil, err
	}

	return coll.updateOrReplaceOne(ctx, f, u, sess, opts...)
}

// UpdateMany updates multiple documents in the collection.
func (coll *Collection) UpdateMany(ctx context.Context, filter interface{}, update interface{},
	opts ...*options.UpdateOptions) (*UpdateResult, error) {

	if ctx == nil {
		ctx = context.Background()
	}

	f, err := transformDocument(coll.registry, filter)
	if err != nil {
		return nil, err
	}

	u, err := transformDocument(coll.registry, update)
	if err != nil {
		return nil, err
	}

	if err = ensureDollarKey(u); err != nil {
		return nil, err
	}

	updateDocs := []bsonx.Doc{
		{
			{"q", bsonx.Document(f)},
			{"u", bsonx.Document(u)},
			{"multi", bsonx.Boolean(true)},
		},
	}

	sess := sessionFromContext(ctx)

	err = coll.client.validSession(sess)
	if err != nil {
		return nil, err
	}

	wc := coll.writeConcern
	if sess.TransactionRunning() {
		wc = nil
	}

	oldns := coll.namespace()
	cmd := command.Update{
		NS:           command.Namespace{DB: oldns.DB, Collection: oldns.Collection},
		Docs:         updateDocs,
		WriteConcern: wc,
		Session:      sess,
		Clock:        coll.client.clock,
	}

	r, err := driverlegacy.Update(
		ctx, cmd,
		coll.client.topology,
		coll.writeSelector,
		coll.client.id,
		coll.client.topology.SessionPool,
		false,
		opts...,
	)
	if err != nil && err != command.ErrUnacknowledgedWrite {
		return nil, replaceErrors(err)
	}
	res := &UpdateResult{
		MatchedCount:  r.MatchedCount,
		ModifiedCount: r.ModifiedCount,
		UpsertedCount: int64(len(r.Upserted)),
	}
	// TODO(skriptble): Is this correct? Do we only return the first upserted ID for an UpdateMany?
	if len(r.Upserted) > 0 {
		res.UpsertedID = r.Upserted[0].ID
		res.MatchedCount--
	}

	rr, err := processWriteError(r.WriteConcernError, r.WriteErrors, err)
	if rr&rrMany == 0 {
		return nil, err
	}
	return res, err
}

// ReplaceOne replaces a single document in the collection.
func (coll *Collection) ReplaceOne(ctx context.Context, filter interface{},
	replacement interface{}, opts ...*options.ReplaceOptions) (*UpdateResult, error) {

	if ctx == nil {
		ctx = context.Background()
	}

	f, err := transformDocument(coll.registry, filter)
	if err != nil {
		return nil, err
	}

	r, err := transformDocument(coll.registry, replacement)
	if err != nil {
		return nil, err
	}

	if len(r) > 0 && strings.HasPrefix(r[0].Key, "$") {
		return nil, errors.New("replacement document cannot contains keys beginning with '$")
	}

	sess := sessionFromContext(ctx)

	err = coll.client.validSession(sess)
	if err != nil {
		return nil, err
	}

	updateOptions := make([]*options.UpdateOptions, 0, len(opts))
	for _, opt := range opts {
		uOpts := options.Update()
		uOpts.BypassDocumentValidation = opt.BypassDocumentValidation
		uOpts.Collation = opt.Collation
		uOpts.Upsert = opt.Upsert
		updateOptions = append(updateOptions, uOpts)
	}

	return coll.updateOrReplaceOne(ctx, f, r, sess, updateOptions...)
}

// Aggregate runs an aggregation framework pipeline.
//
// See https://docs.mongodb.com/manual/aggregation/.
func (coll *Collection) Aggregate(ctx context.Context, pipeline interface{},
	opts ...*options.AggregateOptions) (*Cursor, error) {

	if ctx == nil {
		ctx = context.Background()
	}

	pipelineArr, hasDollarOut, err := transformAggregatePipelinev2(coll.registry, pipeline)
	if err != nil {
		return nil, err
	}

	sess := sessionFromContext(ctx)
	if sess == nil && coll.client.topology.SessionPool != nil {
		sess, err = session.NewClientSession(coll.client.topology.SessionPool, coll.client.id, session.Implicit)
		if err != nil {
			return nil, err
		}
	}
	if err = coll.client.validSession(sess); err != nil {
		return nil, err
	}

	var wc *writeconcern.WriteConcern
	if hasDollarOut {
		wc = coll.writeConcern
	}
	rc := coll.readConcern
	if sess.TransactionRunning() {
		wc = nil
		rc = nil
	}
	if !writeconcern.AckWrite(wc) {
		closeImplicitSession(sess)
		sess = nil
	}

	selector := coll.readSelector
	if hasDollarOut {
		selector = coll.writeSelector
	}
	if sess != nil && sess.PinnedServer != nil {
		selector = sess.PinnedServer
	}

	ao := options.MergeAggregateOptions(opts...)
	cursorOpts := driver.CursorOptions{
		CommandMonitor: coll.client.monitor,
	}

	op := operation.NewAggregate(pipelineArr).Session(sess).WriteConcern(wc).ReadConcern(rc).ReadPreference(coll.readPreference).CommandMonitor(coll.client.monitor).
		ServerSelector(selector).ClusterClock(coll.client.clock).Database(coll.db.name).Collection(coll.name).Deployment(coll.client.topology)
	if ao.AllowDiskUse != nil {
		op.AllowDiskUse(*ao.AllowDiskUse)
	}
	// ignore batchSize of 0 with $out
	if ao.BatchSize != nil && !(*ao.BatchSize == 0 && hasDollarOut) {
		op.BatchSize(*ao.BatchSize)
		cursorOpts.BatchSize = *ao.BatchSize
	}
	if ao.BypassDocumentValidation != nil {
		op.BypassDocumentValidation(*ao.BypassDocumentValidation)
	}
	if ao.Collation != nil {
		op.Collation(bsoncore.Document(ao.Collation.ToDocument()))
	}
	if ao.MaxTime != nil {
		op.MaxTimeMS(int64(*ao.MaxTime / time.Millisecond))
	}
	if ao.MaxAwaitTime != nil {
		cursorOpts.MaxTimeMS = int64(*ao.MaxAwaitTime / time.Millisecond)
	}
	if ao.Comment != nil {
		op.Comment(*ao.Comment)
	}
	if ao.Hint != nil {
		hintVal, err := transformValue(coll.registry, ao.Hint)
		if err != nil {
			closeImplicitSession(sess)
			return nil, err
		}
		op.Hint(hintVal)
	}

	err = op.Execute(ctx)
	if err != nil {
		closeImplicitSession(sess)
		if wce, ok := err.(driver.WriteCommandError); ok && wce.WriteConcernError != nil {
			return nil, *convertDriverWriteConcernError(wce.WriteConcernError)
		}
		return nil, replaceErrors(err)
	}

	bc, err := op.Result(cursorOpts)
	if err != nil {
		closeImplicitSession(sess)
		return nil, replaceErrors(err)
	}
	cursor, err := newCursorWithSession(bc, coll.registry, sess)
	return cursor, replaceErrors(err)
}

// CountDocuments gets the number of documents matching the filter.
func (coll *Collection) CountDocuments(ctx context.Context, filter interface{},
	opts ...*options.CountOptions) (int64, error) {

	if ctx == nil {
		ctx = context.Background()
	}

	countOpts := options.MergeCountOptions(opts...)

	pipelineArr, err := countDocumentsAggregatePipeline(coll.registry, filter, countOpts)
	if err != nil {
		return 0, err
	}

	sess := sessionFromContext(ctx)

	err = coll.client.validSession(sess)
	if err != nil {
		return 0, err
	}

	rc := coll.readConcern
	if sess.TransactionRunning() {
		rc = nil
	}

	oldns := coll.namespace()
	cmd := command.CountDocuments{
		NS:          command.Namespace{DB: oldns.DB, Collection: oldns.Collection},
		Pipeline:    pipelineArr,
		ReadPref:    coll.readPreference,
		ReadConcern: rc,
		Session:     sess,
		Clock:       coll.client.clock,
	}

	count, err := driverlegacy.CountDocuments(
		ctx, cmd,
		coll.client.topology,
		coll.readSelector,
		coll.client.id,
		coll.client.topology.SessionPool,
		coll.registry,
		countOpts,
	)

	return count, replaceErrors(err)
}

// EstimatedDocumentCount gets an estimate of the count of documents in a collection using collection metadata.
func (coll *Collection) EstimatedDocumentCount(ctx context.Context,
	opts ...*options.EstimatedDocumentCountOptions) (int64, error) {

	if ctx == nil {
		ctx = context.Background()
	}

	sess := sessionFromContext(ctx)

	err := coll.client.validSession(sess)
	if err != nil {
		return 0, err
	}

	rc := coll.readConcern
	if sess != nil && (sess.TransactionInProgress()) {
		rc = nil
	}

	oldns := coll.namespace()
	cmd := command.Count{
		NS:          command.Namespace{DB: oldns.DB, Collection: oldns.Collection},
		Query:       bsonx.Doc{},
		ReadPref:    coll.readPreference,
		ReadConcern: rc,
		Session:     sess,
		Clock:       coll.client.clock,
	}

	countOpts := options.Count()
	if len(opts) >= 1 {
		countOpts = countOpts.SetMaxTime(*opts[len(opts)-1].MaxTime)
	}

	count, err := driverlegacy.Count(
		ctx, cmd,
		coll.client.topology,
		coll.readSelector,
		coll.client.id,
		coll.client.topology.SessionPool,
		coll.registry,
		countOpts,
	)

	return count, replaceErrors(err)
}

// Distinct finds the distinct values for a specified field across a single
// collection.
func (coll *Collection) Distinct(ctx context.Context, fieldName string, filter interface{},
	opts ...*options.DistinctOptions) ([]interface{}, error) {

	if ctx == nil {
		ctx = context.Background()
	}

	f, err := transformDocument(coll.registry, filter)
	if err != nil {
		return nil, err
	}

	sess := sessionFromContext(ctx)

	err = coll.client.validSession(sess)
	if err != nil {
		return nil, err
	}

	rc := coll.readConcern
	if sess.TransactionRunning() {
		rc = nil
	}

	oldns := coll.namespace()
	cmd := command.Distinct{
		NS:          command.Namespace{DB: oldns.DB, Collection: oldns.Collection},
		Field:       fieldName,
		Query:       f,
		ReadPref:    coll.readPreference,
		ReadConcern: rc,
		Session:     sess,
		Clock:       coll.client.clock,
	}

	res, err := driverlegacy.Distinct(
		ctx, cmd,
		coll.client.topology,
		coll.readSelector,
		coll.client.id,
		coll.client.topology.SessionPool,
		opts...,
	)
	if err != nil {
		return nil, replaceErrors(err)
	}

	return res.Values, nil
}

// Find finds the documents matching a model.
func (coll *Collection) Find(ctx context.Context, filter interface{},
	opts ...*options.FindOptions) (*Cursor, error) {

	if ctx == nil {
		ctx = context.Background()
	}

	f, err := transformDocument(coll.registry, filter)
	if err != nil {
		return nil, err
	}

	sess := sessionFromContext(ctx)

	err = coll.client.validSession(sess)
	if err != nil {
		return nil, err
	}

	rc := coll.readConcern
	if sess.TransactionRunning() {
		rc = nil
	}

	oldns := coll.namespace()
	cmd := command.Find{
		NS:          command.Namespace{DB: oldns.DB, Collection: oldns.Collection},
		Filter:      f,
		ReadPref:    coll.readPreference,
		ReadConcern: rc,
		Session:     sess,
		Clock:       coll.client.clock,
	}

	batchCursor, err := driverlegacy.Find(
		ctx, cmd,
		coll.client.topology,
		coll.readSelector,
		coll.client.id,
		coll.client.topology.SessionPool,
		coll.registry,
		opts...,
	)
	if err != nil {
		return nil, replaceErrors(err)
	}

	cursor, err := newCursor(batchCursor, coll.registry)
	return cursor, replaceErrors(err)
}

// FindOne returns up to one document that matches the model.
func (coll *Collection) FindOne(ctx context.Context, filter interface{},
	opts ...*options.FindOneOptions) *SingleResult {

	if ctx == nil {
		ctx = context.Background()
	}

	f, err := transformDocument(coll.registry, filter)
	if err != nil {
		return &SingleResult{err: err}
	}

	sess := sessionFromContext(ctx)

	err = coll.client.validSession(sess)
	if err != nil {
		return &SingleResult{err: err}
	}

	rc := coll.readConcern
	if sess.TransactionRunning() {
		rc = nil
	}

	oldns := coll.namespace()
	cmd := command.Find{
		NS:          command.Namespace{DB: oldns.DB, Collection: oldns.Collection},
		Filter:      f,
		ReadPref:    coll.readPreference,
		ReadConcern: rc,
		Session:     sess,
		Clock:       coll.client.clock,
	}

	findOpts := make([]*options.FindOptions, len(opts))
	for i, opt := range opts {
		findOpts[i] = &options.FindOptions{
			AllowPartialResults: opt.AllowPartialResults,
			BatchSize:           opt.BatchSize,
			Collation:           opt.Collation,
			Comment:             opt.Comment,
			CursorType:          opt.CursorType,
			Hint:                opt.Hint,
			Max:                 opt.Max,
			MaxAwaitTime:        opt.MaxAwaitTime,
			Min:                 opt.Min,
			NoCursorTimeout:     opt.NoCursorTimeout,
			OplogReplay:         opt.OplogReplay,
			Projection:          opt.Projection,
			ReturnKey:           opt.ReturnKey,
			ShowRecordID:        opt.ShowRecordID,
			Skip:                opt.Skip,
			Snapshot:            opt.Snapshot,
			Sort:                opt.Sort,
		}
	}

	batchCursor, err := driverlegacy.Find(
		ctx, cmd,
		coll.client.topology,
		coll.readSelector,
		coll.client.id,
		coll.client.topology.SessionPool,
		coll.registry,
		findOpts...,
	)
	if err != nil {
		return &SingleResult{err: replaceErrors(err)}
	}

	cursor, err := newCursor(batchCursor, coll.registry)
	return &SingleResult{cur: cursor, reg: coll.registry, err: replaceErrors(err)}
}

func (coll *Collection) findAndModify(ctx context.Context, op *operation.FindAndModify) *SingleResult {
	if ctx == nil {
		ctx = context.Background()
	}

	sess := sessionFromContext(ctx)
	var err error
	if sess == nil && coll.client.topology.SessionPool != nil {
		sess, err = session.NewClientSession(coll.client.topology.SessionPool, coll.client.id, session.Implicit)
		if err != nil {
			return &SingleResult{err: err}
		}
		defer sess.EndSession()
	}

	err = coll.client.validSession(sess)
	if err != nil {
		return &SingleResult{err: err}
	}

	wc := coll.writeConcern
	if sess.TransactionRunning() {
		wc = nil
	}
	if !writeconcern.AckWrite(wc) {
		sess = nil
	}

	selector := coll.writeSelector
	if sess != nil && sess.PinnedServer != nil {
		selector = sess.PinnedServer
	}

	retry := driver.RetryNone
	if coll.client.retryWrites {
		retry = driver.RetryOnce
	}

	op = op.Session(sess).
		WriteConcern(wc).
		CommandMonitor(coll.client.monitor).
		ServerSelector(selector).
		ClusterClock(coll.client.clock).
		Database(coll.db.name).
		Collection(coll.name).
		Deployment(coll.client.topology).
		Retry(retry)

	_, err = processWriteError(nil, nil, op.Execute(ctx))
	if err != nil {
		return &SingleResult{err: err}
	}

	return &SingleResult{rdr: bson.Raw(op.Result().Value), reg: coll.registry}
}

// FindOneAndDelete find a single document and deletes it, returning the
// original in result.
func (coll *Collection) FindOneAndDelete(ctx context.Context, filter interface{},
	opts ...*options.FindOneAndDeleteOptions) *SingleResult {

	f, err := transformBsoncoreDocument(coll.registry, filter)
	if err != nil {
		return &SingleResult{err: err}
	}
	fod := options.MergeFindOneAndDeleteOptions(opts...)
	op := operation.NewFindAndModify(f).Remove(true)
	if fod.Collation != nil {
		op = op.Collation(bsoncore.Document(fod.Collation.ToDocument()))
	}
	if fod.MaxTime != nil {
		op = op.MaxTimeMS(int64(*fod.MaxTime / time.Millisecond))
	}
	if fod.Projection != nil {
		proj, err := transformBsoncoreDocument(coll.registry, fod.Projection)
		if err != nil {
			return &SingleResult{err: err}
		}
		op = op.Fields(proj)
	}
	if fod.Sort != nil {
		sort, err := transformBsoncoreDocument(coll.registry, fod.Sort)
		if err != nil {
			return &SingleResult{err: err}
		}
		op = op.Sort(sort)
	}

	return coll.findAndModify(ctx, op)
}

// FindOneAndReplace finds a single document and replaces it, returning either
// the original or the replaced document.
func (coll *Collection) FindOneAndReplace(ctx context.Context, filter interface{},
	replacement interface{}, opts ...*options.FindOneAndReplaceOptions) *SingleResult {

	f, err := transformBsoncoreDocument(coll.registry, filter)
	if err != nil {
		return &SingleResult{err: err}
	}
	r, err := transformBsoncoreDocument(coll.registry, replacement)
	if err != nil {
		return &SingleResult{err: err}
	}
	if firstElem, err := r.IndexErr(0); err == nil && strings.HasPrefix(firstElem.Key(), "$") {
		return &SingleResult{err: errors.New("replacement document cannot contain keys beginning with '$'")}
	}

	fo := options.MergeFindOneAndReplaceOptions(opts...)
	op := operation.NewFindAndModify(f).Update(r)
	if fo.BypassDocumentValidation != nil {
		op = op.BypassDocumentValidation(*fo.BypassDocumentValidation)
	}
	if fo.Collation != nil {
		op = op.Collation(bsoncore.Document(fo.Collation.ToDocument()))
	}
	if fo.MaxTime != nil {
		op = op.MaxTimeMS(int64(*fo.MaxTime / time.Millisecond))
	}
	if fo.Projection != nil {
		proj, err := transformBsoncoreDocument(coll.registry, fo.Projection)
		if err != nil {
			return &SingleResult{err: err}
		}
		op = op.Fields(proj)
	}
	if fo.ReturnDocument != nil {
		op = op.NewDocument(*fo.ReturnDocument == options.After)
	}
	if fo.Sort != nil {
		sort, err := transformBsoncoreDocument(coll.registry, fo.Sort)
		if err != nil {
			return &SingleResult{err: err}
		}
		op = op.Sort(sort)
	}
	if fo.Upsert != nil {
		op = op.Upsert(*fo.Upsert)
	}

	return coll.findAndModify(ctx, op)
}

// FindOneAndUpdate finds a single document and updates it, returning either
// the original or the updated.
func (coll *Collection) FindOneAndUpdate(ctx context.Context, filter interface{},
	update interface{}, opts ...*options.FindOneAndUpdateOptions) *SingleResult {

	if ctx == nil {
		ctx = context.Background()
	}

	f, err := transformBsoncoreDocument(coll.registry, filter)
	if err != nil {
		return &SingleResult{err: err}
	}
	u, err := transformBsoncoreDocument(coll.registry, update)
	if err != nil {
		return &SingleResult{err: err}
	}
	err = ensureDollarKeyv2(u)
	if err != nil {
		return &SingleResult{
			err: err,
		}
	}

	fo := options.MergeFindOneAndUpdateOptions(opts...)
	op := operation.NewFindAndModify(f).Update(u)

	if fo.ArrayFilters != nil {
		filtersDoc, err := fo.ArrayFilters.ToArrayDocument()
		if err != nil {
			return &SingleResult{err: err}
		}
		op = op.ArrayFilters(bsoncore.Document(filtersDoc))
	}
	if fo.BypassDocumentValidation != nil {
		op = op.BypassDocumentValidation(*fo.BypassDocumentValidation)
	}
	if fo.Collation != nil {
		op = op.Collation(bsoncore.Document(fo.Collation.ToDocument()))
	}
	if fo.MaxTime != nil {
		op = op.MaxTimeMS(int64(*fo.MaxTime / time.Millisecond))
	}
	if fo.Projection != nil {
		proj, err := transformBsoncoreDocument(coll.registry, fo.Projection)
		if err != nil {
			return &SingleResult{err: err}
		}
		op = op.Fields(proj)
	}
	if fo.ReturnDocument != nil {
		op = op.NewDocument(*fo.ReturnDocument == options.After)
	}
	if fo.Sort != nil {
		sort, err := transformBsoncoreDocument(coll.registry, fo.Sort)
		if err != nil {
			return &SingleResult{err: err}
		}
		op = op.Sort(sort)
	}
	if fo.Upsert != nil {
		op = op.Upsert(*fo.Upsert)
	}

	return coll.findAndModify(ctx, op)
}

// Watch returns a change stream cursor used to receive notifications of changes to the collection.
//
// This method is preferred to running a raw aggregation with a $changeStream stage because it
// supports resumability in the case of some errors. The collection must have read concern majority or no read concern
// for a change stream to be created successfully.
func (coll *Collection) Watch(ctx context.Context, pipeline interface{},
	opts ...*options.ChangeStreamOptions) (*ChangeStream, error) {
	return newChangeStream(ctx, coll, pipeline, opts...)
}

// Indexes returns the index view for this collection.
func (coll *Collection) Indexes() IndexView {
	return IndexView{coll: coll}
}

// Drop drops this collection from database.
func (coll *Collection) Drop(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}

	sess := sessionFromContext(ctx)

	err := coll.client.validSession(sess)
	if err != nil {
		return err
	}

	wc := coll.writeConcern
	if sess.TransactionRunning() {
		wc = nil
	}

	cmd := command.DropCollection{
		DB:           coll.db.name,
		Collection:   coll.name,
		WriteConcern: wc,
		Session:      sess,
		Clock:        coll.client.clock,
	}
	_, err = driverlegacy.DropCollection(
		ctx, cmd,
		coll.client.topology,
		coll.writeSelector,
		coll.client.id,
		coll.client.topology.SessionPool,
	)
	if err != nil && !command.IsNotFound(err) {
		return replaceErrors(err)
	}
	return nil
}
