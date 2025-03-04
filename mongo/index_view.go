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
	"strconv"

	"go.mongodb.org/mongo-driver/v2/internal/mongoutil"
	"go.mongodb.org/mongo-driver/v2/internal/serverselector"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"
	"go.mongodb.org/mongo-driver/v2/x/bsonx/bsoncore"
	"go.mongodb.org/mongo-driver/v2/x/mongo/driver"
	"go.mongodb.org/mongo-driver/v2/x/mongo/driver/description"
	"go.mongodb.org/mongo-driver/v2/x/mongo/driver/operation"
	"go.mongodb.org/mongo-driver/v2/x/mongo/driver/session"
)

// ErrInvalidIndexValue is returned if an index is created with a keys document that has a value that is not a number
// or string.
var ErrInvalidIndexValue = errors.New("invalid index value")

// ErrNonStringIndexName is returned if an index is created with a name that is not a string.
var ErrNonStringIndexName = errors.New("index name must be a string")

// ErrMultipleIndexDrop is returned if multiple indexes would be dropped from a call to IndexView.DropOne.
var ErrMultipleIndexDrop = errors.New("multiple indexes would be dropped")

// IndexView is a type that can be used to create, drop, and list indexes on a collection. An IndexView for a collection
// can be created by a call to Collection.Indexes().
type IndexView struct {
	coll *Collection
}

// IndexModel represents a new index to be created.
type IndexModel struct {
	// A document describing which keys should be used for the index. It cannot be nil. This must be an order-preserving
	// type such as bson.D. Map types such as bson.M are not valid. See https://www.mongodb.com/docs/manual/indexes/#indexes
	// for examples of valid documents.
	Keys interface{}

	// The options to use to create the index.
	Options *options.IndexOptionsBuilder
}

func isNamespaceNotFoundError(err error) bool {
	var de driver.Error
	if errors.As(err, &de) {
		return de.Code == 26
	}
	return false
}

// List executes a listIndexes command and returns a cursor over the indexes in the collection.
//
// The opts parameter can be used to specify options for this operation (see the options.ListIndexesOptions
// documentation).
//
// For more information about the command, see https://www.mongodb.com/docs/manual/reference/command/listIndexes/.
func (iv IndexView) List(ctx context.Context, opts ...options.Lister[options.ListIndexesOptions]) (*Cursor, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	sess := sessionFromContext(ctx)
	if sess == nil && iv.coll.client.sessionPool != nil {
		sess = session.NewImplicitClientSession(iv.coll.client.sessionPool, iv.coll.client.id)
	}

	err := iv.coll.client.validSession(sess)
	if err != nil {
		closeImplicitSession(sess)
		return nil, err
	}
	var selector description.ServerSelector

	selector = &serverselector.Composite{
		Selectors: []description.ServerSelector{
			&serverselector.ReadPref{ReadPref: readpref.Primary()},
			&serverselector.Latency{Latency: iv.coll.client.localThreshold},
		},
	}

	selector = makeReadPrefSelector(sess, selector, iv.coll.client.localThreshold)
	op := operation.NewListIndexes().
		Session(sess).CommandMonitor(iv.coll.client.monitor).
		ServerSelector(selector).ClusterClock(iv.coll.client.clock).
		Database(iv.coll.db.name).Collection(iv.coll.name).
		Deployment(iv.coll.client.deployment).ServerAPI(iv.coll.client.serverAPI).
		Timeout(iv.coll.client.timeout).Crypt(iv.coll.client.cryptFLE).Authenticator(iv.coll.client.authenticator)

	cursorOpts := iv.coll.client.createBaseCursorOptions()

	cursorOpts.MarshalValueEncoderFn = newEncoderFn(iv.coll.bsonOpts, iv.coll.registry)

	args, err := mongoutil.NewOptions[options.ListIndexesOptions](opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to construct options from builder: %w", err)
	}

	if args.BatchSize != nil {
		op = op.BatchSize(*args.BatchSize)
		cursorOpts.BatchSize = *args.BatchSize
	}

	retry := driver.RetryNone
	if iv.coll.client.retryReads {
		retry = driver.RetryOncePerCommand
	}
	op.Retry(retry)

	err = op.Execute(ctx)
	if err != nil {
		// for namespaceNotFound errors, return an empty cursor and do not throw an error
		closeImplicitSession(sess)
		if isNamespaceNotFoundError(err) {
			return newEmptyCursor(), nil
		}

		return nil, replaceErrors(err)
	}

	bc, err := op.Result(cursorOpts)
	if err != nil {
		closeImplicitSession(sess)
		return nil, replaceErrors(err)
	}
	cursor, err := newCursorWithSession(bc, iv.coll.bsonOpts, iv.coll.registry, sess)
	return cursor, replaceErrors(err)
}

// ListSpecifications executes a List command and returns a slice of returned IndexSpecifications
func (iv IndexView) ListSpecifications(
	ctx context.Context,
	opts ...options.Lister[options.ListIndexesOptions],
) ([]IndexSpecification, error) {
	cursor, err := iv.List(ctx, opts...)
	if err != nil {
		return nil, err
	}

	var resp []indexListSpecificationResponse

	if err := cursor.All(ctx, &resp); err != nil {
		return nil, err
	}

	namespace := iv.coll.db.Name() + "." + iv.coll.Name()

	specs := make([]IndexSpecification, len(resp))
	for idx, spec := range resp {
		specs[idx] = IndexSpecification(spec)
		specs[idx].Namespace = namespace
	}

	return specs, nil
}

// CreateOne executes a createIndexes command to create an index on the collection and returns the name of the new
// index. See the IndexView.CreateMany documentation for more information and an example.
func (iv IndexView) CreateOne(
	ctx context.Context,
	model IndexModel,
	opts ...options.Lister[options.CreateIndexesOptions],
) (string, error) {
	names, err := iv.CreateMany(ctx, []IndexModel{model}, opts...)
	if err != nil {
		return "", err
	}

	return names[0], nil
}

// CreateMany executes a createIndexes command to create multiple indexes on the collection and returns the names of
// the new indexes.
//
// For each IndexModel in the models parameter, the index name can be specified via the Options field. If a name is not
// given, it will be generated from the Keys document.
//
// The opts parameter can be used to specify options for this operation (see the options.CreateIndexesOptions
// documentation).
//
// For more information about the command, see https://www.mongodb.com/docs/manual/reference/command/createIndexes/.
func (iv IndexView) CreateMany(
	ctx context.Context,
	models []IndexModel,
	opts ...options.Lister[options.CreateIndexesOptions],
) ([]string, error) {
	names := make([]string, 0, len(models))

	var indexes bsoncore.Document
	aidx, indexes := bsoncore.AppendArrayStart(indexes)

	for i, model := range models {
		if model.Keys == nil {
			return nil, fmt.Errorf("index model keys cannot be nil")
		}

		if isUnorderedMap(model.Keys) {
			return nil, ErrMapForOrderedArgument{"keys"}
		}

		keys, err := marshal(model.Keys, iv.coll.bsonOpts, iv.coll.registry)
		if err != nil {
			return nil, err
		}

		name, err := getOrGenerateIndexName(keys, model)
		if err != nil {
			return nil, err
		}

		names = append(names, name)

		var iidx int32
		iidx, indexes = bsoncore.AppendDocumentElementStart(indexes, strconv.Itoa(i))
		indexes = bsoncore.AppendDocumentElement(indexes, "key", keys)

		if model.Options == nil {
			model.Options = options.Index()
		}
		model.Options.SetName(name)

		optsDoc, err := iv.createOptionsDoc(model.Options)
		if err != nil {
			return nil, err
		}

		indexes = bsoncore.AppendDocument(indexes, optsDoc)

		indexes, err = bsoncore.AppendDocumentEnd(indexes, iidx)
		if err != nil {
			return nil, err
		}
	}

	indexes, err := bsoncore.AppendArrayEnd(indexes, aidx)
	if err != nil {
		return nil, err
	}

	sess := sessionFromContext(ctx)

	if sess == nil && iv.coll.client.sessionPool != nil {
		sess = session.NewImplicitClientSession(iv.coll.client.sessionPool, iv.coll.client.id)
		defer sess.EndSession()
	}

	err = iv.coll.client.validSession(sess)
	if err != nil {
		return nil, err
	}

	wc := iv.coll.writeConcern
	if sess.TransactionRunning() {
		wc = nil
	}
	if !wc.Acknowledged() {
		sess = nil
	}

	selector := makePinnedSelector(sess, iv.coll.writeSelector)

	args, err := mongoutil.NewOptions[options.CreateIndexesOptions](opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to construct options from builder: %w", err)
	}

	op := operation.NewCreateIndexes(indexes).
		Session(sess).WriteConcern(wc).ClusterClock(iv.coll.client.clock).
		Database(iv.coll.db.name).Collection(iv.coll.name).CommandMonitor(iv.coll.client.monitor).
		Deployment(iv.coll.client.deployment).ServerSelector(selector).ServerAPI(iv.coll.client.serverAPI).
		Timeout(iv.coll.client.timeout).Crypt(iv.coll.client.cryptFLE).Authenticator(iv.coll.client.authenticator)
	if args.CommitQuorum != nil {
		commitQuorum, err := marshalValue(args.CommitQuorum, iv.coll.bsonOpts, iv.coll.registry)
		if err != nil {
			return nil, err
		}

		op.CommitQuorum(commitQuorum)
	}

	_, err = processWriteError(op.Execute(ctx))
	if err != nil {
		return nil, err
	}

	return names, nil
}

func (iv IndexView) createOptionsDoc(opts options.Lister[options.IndexOptions]) (bsoncore.Document, error) {
	args, err := mongoutil.NewOptions[options.IndexOptions](opts)
	if err != nil {
		return nil, fmt.Errorf("failed to construct options from builder: %w", err)
	}

	optsDoc := bsoncore.Document{}
	if args.ExpireAfterSeconds != nil {
		optsDoc = bsoncore.AppendInt32Element(optsDoc, "expireAfterSeconds", *args.ExpireAfterSeconds)
	}
	if args.Name != nil {
		optsDoc = bsoncore.AppendStringElement(optsDoc, "name", *args.Name)
	}
	if args.Sparse != nil {
		optsDoc = bsoncore.AppendBooleanElement(optsDoc, "sparse", *args.Sparse)
	}
	if args.StorageEngine != nil {
		doc, err := marshal(args.StorageEngine, iv.coll.bsonOpts, iv.coll.registry)
		if err != nil {
			return nil, err
		}

		optsDoc = bsoncore.AppendDocumentElement(optsDoc, "storageEngine", doc)
	}
	if args.Unique != nil {
		optsDoc = bsoncore.AppendBooleanElement(optsDoc, "unique", *args.Unique)
	}
	if args.Version != nil {
		optsDoc = bsoncore.AppendInt32Element(optsDoc, "v", *args.Version)
	}
	if args.DefaultLanguage != nil {
		optsDoc = bsoncore.AppendStringElement(optsDoc, "default_language", *args.DefaultLanguage)
	}
	if args.LanguageOverride != nil {
		optsDoc = bsoncore.AppendStringElement(optsDoc, "language_override", *args.LanguageOverride)
	}
	if args.TextVersion != nil {
		optsDoc = bsoncore.AppendInt32Element(optsDoc, "textIndexVersion", *args.TextVersion)
	}
	if args.Weights != nil {
		doc, err := marshal(args.Weights, iv.coll.bsonOpts, iv.coll.registry)
		if err != nil {
			return nil, err
		}

		optsDoc = bsoncore.AppendDocumentElement(optsDoc, "weights", doc)
	}
	if args.SphereVersion != nil {
		optsDoc = bsoncore.AppendInt32Element(optsDoc, "2dsphereIndexVersion", *args.SphereVersion)
	}
	if args.Bits != nil {
		optsDoc = bsoncore.AppendInt32Element(optsDoc, "bits", *args.Bits)
	}
	if args.Max != nil {
		optsDoc = bsoncore.AppendDoubleElement(optsDoc, "max", *args.Max)
	}
	if args.Min != nil {
		optsDoc = bsoncore.AppendDoubleElement(optsDoc, "min", *args.Min)
	}
	if args.BucketSize != nil {
		optsDoc = bsoncore.AppendInt32Element(optsDoc, "bucketSize", *args.BucketSize)
	}
	if args.PartialFilterExpression != nil {
		doc, err := marshal(args.PartialFilterExpression, iv.coll.bsonOpts, iv.coll.registry)
		if err != nil {
			return nil, err
		}

		optsDoc = bsoncore.AppendDocumentElement(optsDoc, "partialFilterExpression", doc)
	}
	if args.Collation != nil {
		optsDoc = bsoncore.AppendDocumentElement(optsDoc, "collation", bsoncore.Document(toDocument(args.Collation)))
	}
	if args.WildcardProjection != nil {
		doc, err := marshal(args.WildcardProjection, iv.coll.bsonOpts, iv.coll.registry)
		if err != nil {
			return nil, err
		}

		optsDoc = bsoncore.AppendDocumentElement(optsDoc, "wildcardProjection", doc)
	}
	if args.Hidden != nil {
		optsDoc = bsoncore.AppendBooleanElement(optsDoc, "hidden", *args.Hidden)
	}

	return optsDoc, nil
}

func (iv IndexView) drop(ctx context.Context, index any, _ ...options.Lister[options.DropIndexesOptions]) error {
	if ctx == nil {
		ctx = context.Background()
	}

	sess := sessionFromContext(ctx)
	if sess == nil && iv.coll.client.sessionPool != nil {
		sess = session.NewImplicitClientSession(iv.coll.client.sessionPool, iv.coll.client.id)
		defer sess.EndSession()
	}

	err := iv.coll.client.validSession(sess)
	if err != nil {
		return err
	}

	wc := iv.coll.writeConcern
	if sess.TransactionRunning() {
		wc = nil
	}
	if !wc.Acknowledged() {
		sess = nil
	}

	selector := makePinnedSelector(sess, iv.coll.writeSelector)

	op := operation.NewDropIndexes(index).Session(sess).WriteConcern(wc).CommandMonitor(iv.coll.client.monitor).
		ServerSelector(selector).ClusterClock(iv.coll.client.clock).
		Database(iv.coll.db.name).Collection(iv.coll.name).
		Deployment(iv.coll.client.deployment).ServerAPI(iv.coll.client.serverAPI).
		Timeout(iv.coll.client.timeout).Crypt(iv.coll.client.cryptFLE).Authenticator(iv.coll.client.authenticator)

	err = op.Execute(ctx)
	if err != nil {
		return replaceErrors(err)
	}

	return nil
}

// DropOne executes a dropIndexes operation to drop an index on the collection.
//
// The name parameter should be the name of the index to drop. If the name is
// "*", ErrMultipleIndexDrop will be returned without running the command
// because doing so would drop all indexes.
//
// The opts parameter can be used to specify options for this operation (see the
// options.DropIndexesOptions documentation).
//
// For more information about the command, see https://www.mongodb.com/docs/manual/reference/command/dropIndexes/.
func (iv IndexView) DropOne(
	ctx context.Context,
	name string,
	opts ...options.Lister[options.DropIndexesOptions],
) error {
	// For more information about the command, see
	// https://www.mongodb.com/docs/manual/reference/command/dropIndexes/.
	if name == "*" {
		return ErrMultipleIndexDrop
	}

	return iv.drop(ctx, name, opts...)
}

// DropWithKey drops a collection index by key using the dropIndexes operation.
//
// This function is useful to drop an index using its key specification instead of its name.
func (iv IndexView) DropWithKey(ctx context.Context, keySpecDocument interface{}, opts ...options.Lister[options.DropIndexesOptions]) error {
	doc, err := marshal(keySpecDocument, iv.coll.bsonOpts, iv.coll.registry)
	if err != nil {
		return err
	}

	return iv.drop(ctx, doc, opts...)
}

// DropAll executes a dropIndexes operation to drop all indexes on the collection.
//
// The opts parameter can be used to specify options for this operation (see the
// options.DropIndexesOptions documentation).
//
// For more information about the command, see
// https://www.mongodb.com/docs/manual/reference/command/dropIndexes/.
func (iv IndexView) DropAll(
	ctx context.Context,
	opts ...options.Lister[options.DropIndexesOptions],
) error {
	return iv.drop(ctx, "*", opts...)
}

func getOrGenerateIndexName(keySpecDocument bsoncore.Document, model IndexModel) (string, error) {
	args, err := mongoutil.NewOptions[options.IndexOptions](model.Options)
	if err != nil {
		return "", fmt.Errorf("failed to construct options from builder: %w", err)
	}

	if args != nil && args.Name != nil {
		return *args.Name, nil
	}

	name := bytes.NewBufferString("")
	first := true

	elems, err := keySpecDocument.Elements()
	if err != nil {
		return "", err
	}
	for _, elem := range elems {
		if !first {
			_, err := name.WriteRune('_')
			if err != nil {
				return "", err
			}
		}

		_, err := name.WriteString(elem.Key())
		if err != nil {
			return "", err
		}

		_, err = name.WriteRune('_')
		if err != nil {
			return "", err
		}

		var value string

		bsonValue := elem.Value()
		switch bsonValue.Type {
		case bsoncore.TypeInt32:
			value = fmt.Sprintf("%d", bsonValue.Int32())
		case bsoncore.TypeInt64:
			value = fmt.Sprintf("%d", bsonValue.Int64())
		case bsoncore.TypeString:
			value = bsonValue.StringValue()
		default:
			return "", ErrInvalidIndexValue
		}

		_, err = name.WriteString(value)
		if err != nil {
			return "", err
		}

		first = false
	}

	return name.String(), nil
}
