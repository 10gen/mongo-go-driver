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
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/internal/csfle"
	"go.mongodb.org/mongo-driver/v2/internal/csot"
	"go.mongodb.org/mongo-driver/v2/internal/mongoutil"
	"go.mongodb.org/mongo-driver/v2/internal/serverselector"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readconcern"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"
	"go.mongodb.org/mongo-driver/v2/mongo/writeconcern"
	"go.mongodb.org/mongo-driver/v2/x/bsonx/bsoncore"
	"go.mongodb.org/mongo-driver/v2/x/mongo/driver"
	"go.mongodb.org/mongo-driver/v2/x/mongo/driver/description"
	"go.mongodb.org/mongo-driver/v2/x/mongo/driver/operation"
	"go.mongodb.org/mongo-driver/v2/x/mongo/driver/session"
)

var (
	defaultRunCmdOpts = []options.Lister[options.RunCmdOptions]{options.RunCmd().SetReadPreference(readpref.Primary())}
)

// Database is a handle to a MongoDB database. It is safe for concurrent use by multiple goroutines.
type Database struct {
	client         *Client
	name           string
	readConcern    *readconcern.ReadConcern
	writeConcern   *writeconcern.WriteConcern
	readPreference *readpref.ReadPref
	readSelector   description.ServerSelector
	writeSelector  description.ServerSelector
	bsonOpts       *options.BSONOptions
	registry       *bson.Registry
}

func newDatabase(client *Client, name string, opts ...options.Lister[options.DatabaseOptions]) *Database {
	args, _ := mongoutil.NewOptions[options.DatabaseOptions](opts...)

	rc := client.readConcern
	if args.ReadConcern != nil {
		rc = args.ReadConcern
	}

	rp := client.readPreference
	if args.ReadPreference != nil {
		rp = args.ReadPreference
	}

	wc := client.writeConcern
	if args.WriteConcern != nil {
		wc = args.WriteConcern
	}

	bsonOpts := client.bsonOpts
	if args.BSONOptions != nil {
		bsonOpts = args.BSONOptions
	}

	reg := client.registry
	if args.Registry != nil {
		reg = args.Registry
	}

	db := &Database{
		client:         client,
		name:           name,
		readPreference: rp,
		readConcern:    rc,
		writeConcern:   wc,
		bsonOpts:       bsonOpts,
		registry:       reg,
	}

	db.readSelector = &serverselector.Composite{
		Selectors: []description.ServerSelector{
			&serverselector.ReadPref{ReadPref: db.readPreference},
			&serverselector.Latency{Latency: db.client.localThreshold},
		},
	}

	db.writeSelector = &serverselector.Composite{
		Selectors: []description.ServerSelector{
			&serverselector.Write{},
			&serverselector.Latency{Latency: db.client.localThreshold},
		},
	}

	return db
}

// Client returns the Client the Database was created from.
func (db *Database) Client() *Client {
	return db.client
}

// Name returns the name of the database.
func (db *Database) Name() string {
	return db.name
}

// Collection returns a handle for a collection with the given name and options.
//
// If the collection does not exist on the server, it will be created when a
// write operation is performed.
func (db *Database) Collection(name string, opts ...options.Lister[options.CollectionOptions]) *Collection {
	return newCollection(db, name, opts...)
}

// Aggregate executes an aggregate command the database.
//
// The pipeline parameter must be a slice of documents, each representing an aggregation stage. The pipeline
// cannot be nil but can be empty. The stage documents must all be non-nil. For a pipeline of bson.D documents, the
// mongo.Pipeline type can be used. See
// https://www.mongodb.com/docs/manual/reference/operator/aggregation-pipeline/#db-aggregate-stages for a list of valid
// stages in database-level aggregations.
//
// The opts parameter can be used to specify options for this operation (see the options.AggregateOptions documentation).
//
// For more information about the command, see https://www.mongodb.com/docs/manual/reference/command/aggregate/.
func (db *Database) Aggregate(
	ctx context.Context,
	pipeline interface{},
	opts ...options.Lister[options.AggregateOptions],
) (*Cursor, error) {
	a := aggregateParams{
		ctx:            ctx,
		pipeline:       pipeline,
		client:         db.client,
		registry:       db.registry,
		readConcern:    db.readConcern,
		writeConcern:   db.writeConcern,
		retryRead:      db.client.retryReads,
		db:             db.name,
		readSelector:   db.readSelector,
		writeSelector:  db.writeSelector,
		readPreference: db.readPreference,
	}

	return aggregate(a, opts...)
}

func (db *Database) processRunCommand(
	ctx context.Context,
	cmd interface{},
	cursorCommand bool,
	opts ...options.Lister[options.RunCmdOptions],
) (*operation.Command, *session.Client, error) {
	args, err := mongoutil.NewOptions[options.RunCmdOptions](append(defaultRunCmdOpts, opts...)...)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to construct options from builder: %w", err)
	}

	sess := sessionFromContext(ctx)
	if sess == nil && db.client.sessionPool != nil {
		sess = session.NewImplicitClientSession(db.client.sessionPool, db.client.id)
	}

	if err := db.client.validSession(sess); err != nil {
		return nil, sess, err
	}

	if sess != nil && sess.TransactionRunning() && args.ReadPreference != nil && args.ReadPreference.Mode() != readpref.PrimaryMode {
		return nil, sess, errors.New("read preference in a transaction must be primary")
	}

	if isUnorderedMap(cmd) {
		return nil, sess, ErrMapForOrderedArgument{"cmd"}
	}

	runCmdDoc, err := marshal(cmd, db.bsonOpts, db.registry)
	if err != nil {
		return nil, sess, err
	}

	var readSelect description.ServerSelector

	readSelect = &serverselector.Composite{
		Selectors: []description.ServerSelector{
			&serverselector.ReadPref{ReadPref: args.ReadPreference},
			&serverselector.Latency{Latency: db.client.localThreshold},
		},
	}

	if sess != nil && sess.PinnedServerAddr != nil {
		readSelect = makePinnedSelector(sess, readSelect)
	}

	var op *operation.Command
	switch cursorCommand {
	case true:
		cursorOpts := db.client.createBaseCursorOptions()

		cursorOpts.MarshalValueEncoderFn = newEncoderFn(db.bsonOpts, db.registry)

		op = operation.NewCursorCommand(runCmdDoc, cursorOpts)
	default:
		op = operation.NewCommand(runCmdDoc)
	}

	return op.Session(sess).CommandMonitor(db.client.monitor).
		ServerSelector(readSelect).ClusterClock(db.client.clock).
		Database(db.name).Deployment(db.client.deployment).
		Crypt(db.client.cryptFLE).ReadPreference(args.ReadPreference).ServerAPI(db.client.serverAPI).
		Timeout(db.client.timeout).Logger(db.client.logger).Authenticator(db.client.authenticator), sess, nil
}

// RunCommand executes the given command against the database.
//
// This function does not obey the Database's readPreference. To specify a read
// preference, the RunCmdOptions.ReadPreference option must be used.
//
// This function does not obey the Database's readConcern or writeConcern. A
// user must supply these values manually in the user-provided runCommand
// parameter.
//
// The runCommand parameter must be a document for the command to be executed. It cannot be nil.
// This must be an order-preserving type such as bson.D. Map types such as bson.M are not valid.
//
// The opts parameter can be used to specify options for this operation (see the options.RunCmdOptions documentation).
//
// The behavior of RunCommand is undefined if the command document contains any of the following:
// - A session ID or any transaction-specific fields
// - API versioning options when an API version is already declared on the Client
// - maxTimeMS when Timeout is set on the Client
func (db *Database) RunCommand(
	ctx context.Context,
	runCommand interface{},
	opts ...options.Lister[options.RunCmdOptions],
) *SingleResult {
	if ctx == nil {
		ctx = context.Background()
	}

	op, sess, err := db.processRunCommand(ctx, runCommand, false, opts...)
	defer closeImplicitSession(sess)
	if err != nil {
		return &SingleResult{err: err}
	}

	err = op.Execute(ctx)
	// RunCommand can be used to run a write, thus execute may return a write error
	rr, convErr := processWriteError(err)
	return &SingleResult{
		ctx:          ctx,
		err:          convErr,
		rdr:          bson.Raw(op.Result()),
		bsonOpts:     db.bsonOpts,
		reg:          db.registry,
		Acknowledged: rr.isAcknowledged(),
	}
}

// RunCommandCursor executes the given command against the database and parses the response as a cursor. If the command
// being executed does not return a cursor (e.g. insert), the command will be executed on the server and an error will
// be returned because the server response cannot be parsed as a cursor. This function does not obey the Database's read
// preference. To specify a read preference, the RunCmdOptions.ReadPreference option must be used.
//
// The runCommand parameter must be a document for the command to be executed. It cannot be nil.
// This must be an order-preserving type such as bson.D. Map types such as bson.M are not valid.
//
// The opts parameter can be used to specify options for this operation (see the options.RunCmdOptions documentation).
//
// The behavior of RunCommandCursor is undefined if the command document contains any of the following:
// - A session ID or any transaction-specific fields
// - API versioning options when an API version is already declared on the Client
// - maxTimeMS when Timeout is set on the Client
func (db *Database) RunCommandCursor(
	ctx context.Context,
	runCommand interface{},
	opts ...options.Lister[options.RunCmdOptions],
) (*Cursor, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	op, sess, err := db.processRunCommand(ctx, runCommand, true, opts...)
	if err != nil {
		closeImplicitSession(sess)
		return nil, replaceErrors(err)
	}

	if err = op.Execute(ctx); err != nil {
		closeImplicitSession(sess)
		if errors.Is(err, driver.ErrNoCursor) {
			return nil, errors.New(
				"database response does not contain a cursor; try using RunCommand instead")
		}
		return nil, replaceErrors(err)
	}

	bc, err := op.ResultCursor()
	if err != nil {
		closeImplicitSession(sess)
		return nil, replaceErrors(err)
	}
	cursor, err := newCursorWithSession(bc, db.bsonOpts, db.registry, sess)
	return cursor, replaceErrors(err)
}

// Drop drops the database on the server. This method ignores "namespace not found" errors so it is safe to drop
// a database that does not exist on the server.
func (db *Database) Drop(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}

	sess := sessionFromContext(ctx)
	if sess == nil && db.client.sessionPool != nil {
		sess = session.NewImplicitClientSession(db.client.sessionPool, db.client.id)
		defer sess.EndSession()
	}

	err := db.client.validSession(sess)
	if err != nil {
		return err
	}

	wc := db.writeConcern
	if sess.TransactionRunning() {
		wc = nil
	}
	if !wc.Acknowledged() {
		sess = nil
	}

	selector := makePinnedSelector(sess, db.writeSelector)

	op := operation.NewDropDatabase().
		Session(sess).WriteConcern(wc).CommandMonitor(db.client.monitor).
		ServerSelector(selector).ClusterClock(db.client.clock).
		Database(db.name).Deployment(db.client.deployment).Crypt(db.client.cryptFLE).
		ServerAPI(db.client.serverAPI).Authenticator(db.client.authenticator)

	err = op.Execute(ctx)

	var driverErr driver.Error
	if err != nil && (!errors.As(err, &driverErr) || !driverErr.NamespaceNotFound()) {
		return replaceErrors(err)
	}
	return nil
}

// ListCollectionSpecifications executes a listCollections command and returns a slice of CollectionSpecification
// instances representing the collections in the database.
//
// The filter parameter must be a document containing query operators and can be used to select which collections
// are included in the result. It cannot be nil. An empty document (e.g. bson.D{}) should be used to include all
// collections.
//
// The opts parameter can be used to specify options for the operation (see the options.ListCollectionsOptions
// documentation).
//
// For more information about the command, see https://www.mongodb.com/docs/manual/reference/command/listCollections/.
func (db *Database) ListCollectionSpecifications(
	ctx context.Context,
	filter interface{},
	opts ...options.Lister[options.ListCollectionsOptions],
) ([]CollectionSpecification, error) {
	cursor, err := db.ListCollections(ctx, filter, opts...)
	if err != nil {
		return nil, err
	}

	var resp []struct {
		Name string `bson:"name"`
		Type string `bson:"type"`
		Info *struct {
			ReadOnly bool         `bson:"readOnly"`
			UUID     *bson.Binary `bson:"uuid"`
		} `bson:"info"`
		Options bson.Raw                       `bson:"options"`
		IDIndex indexListSpecificationResponse `bson:"idIndex"`
	}

	err = cursor.All(ctx, &resp)
	if err != nil {
		return nil, err
	}

	specs := make([]CollectionSpecification, len(resp))
	for idx, spec := range resp {
		specs[idx] = CollectionSpecification{
			Name:    spec.Name,
			Type:    spec.Type,
			Options: spec.Options,
			IDIndex: IndexSpecification(spec.IDIndex),
		}

		if spec.Info != nil {
			specs[idx].ReadOnly = spec.Info.ReadOnly
			specs[idx].UUID = spec.Info.UUID
		}

		// Pre-4.4 servers report a namespace in their responses, so we only set Namespace manually if it was not in
		// the response.
		if specs[idx].IDIndex.Namespace == "" {
			specs[idx].IDIndex.Namespace = db.name + "." + specs[idx].Name
		}
	}

	return specs, nil
}

// ListCollections executes a listCollections command and returns a cursor over the collections in the database.
//
// The filter parameter must be a document containing query operators and can be used to select which collections
// are included in the result. It cannot be nil. An empty document (e.g. bson.D{}) should be used to include all
// collections.
//
// The opts parameter can be used to specify options for the operation (see the options.ListCollectionsOptions
// documentation).
//
// For more information about the command, see https://www.mongodb.com/docs/manual/reference/command/listCollections/.
//
// BUG(benjirewis): ListCollections prevents listing more than 100 collections per database when running against
// MongoDB version 2.6.
func (db *Database) ListCollections(
	ctx context.Context,
	filter interface{},
	opts ...options.Lister[options.ListCollectionsOptions],
) (*Cursor, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	args, err := mongoutil.NewOptions[options.ListCollectionsOptions](opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to construct options from builder: %w", err)
	}

	filterDoc, err := marshal(filter, db.bsonOpts, db.registry)
	if err != nil {
		return nil, err
	}

	sess := sessionFromContext(ctx)
	if sess == nil && db.client.sessionPool != nil {
		sess = session.NewImplicitClientSession(db.client.sessionPool, db.client.id)
	}

	err = db.client.validSession(sess)
	if err != nil {
		closeImplicitSession(sess)
		return nil, err
	}

	var selector description.ServerSelector

	selector = &serverselector.Composite{
		Selectors: []description.ServerSelector{
			&serverselector.ReadPref{ReadPref: readpref.Primary()},
			&serverselector.Latency{Latency: db.client.localThreshold},
		},
	}

	selector = makeReadPrefSelector(sess, selector, db.client.localThreshold)

	op := operation.NewListCollections(filterDoc).
		Session(sess).ReadPreference(db.readPreference).CommandMonitor(db.client.monitor).
		ServerSelector(selector).ClusterClock(db.client.clock).
		Database(db.name).Deployment(db.client.deployment).Crypt(db.client.cryptFLE).
		ServerAPI(db.client.serverAPI).Timeout(db.client.timeout).Authenticator(db.client.authenticator)

	cursorOpts := db.client.createBaseCursorOptions()

	cursorOpts.MarshalValueEncoderFn = newEncoderFn(db.bsonOpts, db.registry)

	if args.NameOnly != nil {
		op = op.NameOnly(*args.NameOnly)
	}
	if args.BatchSize != nil {
		cursorOpts.BatchSize = *args.BatchSize
		op = op.BatchSize(*args.BatchSize)
	}
	if args.AuthorizedCollections != nil {
		op = op.AuthorizedCollections(*args.AuthorizedCollections)
	}

	retry := driver.RetryNone
	if db.client.retryReads {
		retry = driver.RetryOncePerCommand
	}
	op = op.Retry(retry)

	err = op.Execute(ctx)
	if err != nil {
		closeImplicitSession(sess)
		return nil, replaceErrors(err)
	}

	bc, err := op.Result(cursorOpts)
	if err != nil {
		closeImplicitSession(sess)
		return nil, replaceErrors(err)
	}
	cursor, err := newCursorWithSession(bc, db.bsonOpts, db.registry, sess)
	return cursor, replaceErrors(err)
}

// ListCollectionNames executes a listCollections command and returns a slice containing the names of the collections
// in the database. This method requires driver version >= 1.1.0.
//
// The filter parameter must be a document containing query operators and can be used to select which collections
// are included in the result. It cannot be nil. An empty document (e.g. bson.D{}) should be used to include all
// collections.
//
// The opts parameter can be used to specify options for the operation (see the options.ListCollectionsOptions
// documentation).
//
// For more information about the command, see https://www.mongodb.com/docs/manual/reference/command/listCollections/.
//
// BUG(benjirewis): ListCollectionNames prevents listing more than 100 collections per database when running against
// MongoDB version 2.6.
func (db *Database) ListCollectionNames(
	ctx context.Context,
	filter interface{},
	opts ...options.Lister[options.ListCollectionsOptions],
) ([]string, error) {
	opts = append(opts, options.ListCollections().SetNameOnly(true))

	res, err := db.ListCollections(ctx, filter, opts...)
	if err != nil {
		return nil, err
	}

	defer res.Close(ctx)

	names := make([]string, 0)
	for res.Next(ctx) {
		elem, err := res.Current.LookupErr("name")
		if err != nil {
			return nil, err
		}

		if elem.Type != bson.TypeString {
			return nil, fmt.Errorf("incorrect type for 'name'. got %v. want %v", elem.Type, bson.TypeString)
		}

		elemName := elem.StringValue()
		names = append(names, elemName)
	}

	res.Close(ctx)
	return names, nil
}

// Watch returns a change stream for all changes to the corresponding database. See
// https://www.mongodb.com/docs/manual/changeStreams/ for more information about change streams.
//
// The Database must be configured with read concern majority or no read concern for a change stream to be created
// successfully.
//
// The pipeline parameter must be a slice of documents, each representing a pipeline stage. The pipeline cannot be
// nil but can be empty. The stage documents must all be non-nil. See https://www.mongodb.com/docs/manual/changeStreams/ for
// a list of pipeline stages that can be used with change streams. For a pipeline of bson.D documents, the
// mongo.Pipeline{} type can be used.
//
// The opts parameter can be used to specify options for change stream creation (see the options.ChangeStreamOptions
// documentation).
func (db *Database) Watch(ctx context.Context, pipeline interface{},
	opts ...options.Lister[options.ChangeStreamOptions]) (*ChangeStream, error) {

	csConfig := changeStreamConfig{
		readConcern:    db.readConcern,
		readPreference: db.readPreference,
		client:         db.client,
		bsonOpts:       db.bsonOpts,
		registry:       db.registry,
		streamType:     DatabaseStream,
		databaseName:   db.Name(),
		crypt:          db.client.cryptFLE,
	}
	return newChangeStream(ctx, csConfig, pipeline, opts...)
}

// CreateCollection creates a new collection on the server with the specified
// name and options.
//
// MongoDB versions < 7.0 will return an error if the collection already exists.
// MongoDB versions >= 7.0 will not return an error if an existing collection
// created with the same name and options already exists.
//
// For more information about the command, see
// https://www.mongodb.com/docs/manual/reference/command/create/.
func (db *Database) CreateCollection(ctx context.Context, name string, opts ...options.Lister[options.CreateCollectionOptions]) error {
	args, err := mongoutil.NewOptions(opts...)
	if err != nil {
		return fmt.Errorf("failed to construct options from builder: %w", err)
	}

	// Follow In-Use Encryption specification to check for encryptedFields.
	// Check for encryptedFields from create options.
	ef := args.EncryptedFields
	// Check for encryptedFields from the client EncryptedFieldsMap.
	if ef == nil {
		ef = db.getEncryptedFieldsFromMap(name)
	}
	if ef != nil {
		return db.createCollectionWithEncryptedFields(ctx, name, ef, opts...)
	}

	return db.createCollection(ctx, name, opts...)
}

// getEncryptedFieldsFromServer tries to get an "encryptedFields" document associated with collectionName by running the "listCollections" command.
// Returns nil and no error if the listCollections command succeeds, but "encryptedFields" is not present.
func (db *Database) getEncryptedFieldsFromServer(ctx context.Context, collectionName string) (interface{}, error) {
	// Check if collection has an EncryptedFields configured server-side.
	collSpecs, err := db.ListCollectionSpecifications(ctx, bson.D{{"name", collectionName}})
	if err != nil {
		return nil, err
	}
	if len(collSpecs) == 0 {
		return nil, nil
	}
	if len(collSpecs) > 1 {
		return nil, fmt.Errorf("expected 1 or 0 results from listCollections, got %v", len(collSpecs))
	}
	collSpec := collSpecs[0]
	rawValue, err := collSpec.Options.LookupErr("encryptedFields")
	if errors.Is(err, bsoncore.ErrElementNotFound) {
		return nil, nil
	} else if err != nil {
		return nil, err
	}

	encryptedFields, ok := rawValue.DocumentOK()
	if !ok {
		return nil, fmt.Errorf("expected encryptedFields of %v to be document, got %v", collectionName, rawValue.Type)
	}

	return encryptedFields, nil
}

// getEncryptedFieldsFromMap tries to get an "encryptedFields" document associated with collectionName by checking the client EncryptedFieldsMap.
// Returns nil and no error if an EncryptedFieldsMap is not configured, or does not contain an entry for collectionName.
func (db *Database) getEncryptedFieldsFromMap(collectionName string) interface{} {
	// Check the EncryptedFieldsMap
	efMap := db.client.encryptedFieldsMap
	if efMap == nil {
		return nil
	}

	namespace := db.name + "." + collectionName

	ef, ok := efMap[namespace]
	if ok {
		return ef
	}
	return nil
}

// createCollectionWithEncryptedFields creates a collection with an EncryptedFields.
func (db *Database) createCollectionWithEncryptedFields(
	ctx context.Context,
	name string,
	ef interface{},
	opts ...options.Lister[options.CreateCollectionOptions],
) error {
	efBSON, err := marshal(ef, db.bsonOpts, db.registry)
	if err != nil {
		return fmt.Errorf("error transforming document: %w", err)
	}

	// Check the wire version to ensure server is 7.0.0 or newer.
	// After the wire version check, and before creating the collections, it is possible the server state changes.
	// That is OK. This wire version check is a best effort to inform users earlier if using a QEv2 driver with a QEv1 server.
	{
		const QEv2WireVersion = 21
		ctx, cancel := csot.WithServerSelectionTimeout(ctx, db.client.deployment.GetServerSelectionTimeout())
		defer cancel()

		server, err := db.client.deployment.SelectServer(ctx, &serverselector.Write{})
		if err != nil {
			return fmt.Errorf("error selecting server to check maxWireVersion: %w", err)
		}

		conn, err := server.Connection(ctx)
		if err != nil {
			return fmt.Errorf("error getting connection to check maxWireVersion: %w", err)
		}
		defer conn.Close()
		wireVersionRange := conn.Description().WireVersion
		if wireVersionRange.Max < QEv2WireVersion {
			return fmt.Errorf("Driver support of Queryable Encryption is incompatible with server. Upgrade server to use Queryable Encryption. Got maxWireVersion %v but need maxWireVersion >= %v", wireVersionRange.Max, QEv2WireVersion)
		}
	}

	// Create the two encryption-related, associated collections: `escCollection` and `ecocCollection`.

	stateCollectionOpts := options.CreateCollection().
		SetClusteredIndex(bson.D{{"key", bson.D{{"_id", 1}}}, {"unique", true}})
	// Create ESCCollection.
	escCollection, err := csfle.GetEncryptedStateCollectionName(efBSON, name, csfle.EncryptedStateCollection)
	if err != nil {
		return err
	}

	if err := db.createCollection(ctx, escCollection, stateCollectionOpts); err != nil {
		return err
	}

	// Create ECOCCollection.
	ecocCollection, err := csfle.GetEncryptedStateCollectionName(efBSON, name, csfle.EncryptedCompactionCollection)
	if err != nil {
		return err
	}

	if err := db.createCollection(ctx, ecocCollection, stateCollectionOpts); err != nil {
		return err
	}

	// Create a data collection with the 'encryptedFields' option.
	op, err := db.createCollectionOperation(name, opts...)
	if err != nil {
		return err
	}

	op.EncryptedFields(efBSON)
	if err := db.executeCreateOperation(ctx, op); err != nil {
		return err
	}

	// Create an index on the __safeContent__ field in the collection @collectionName.
	if _, err := db.Collection(name).Indexes().CreateOne(ctx, IndexModel{Keys: bson.D{{"__safeContent__", 1}}}); err != nil {
		return fmt.Errorf("error creating safeContent index: %w", err)
	}

	return nil
}

// createCollection creates a collection without EncryptedFields.
func (db *Database) createCollection(
	ctx context.Context,
	name string,
	opts ...options.Lister[options.CreateCollectionOptions],
) error {
	op, err := db.createCollectionOperation(name, opts...)
	if err != nil {
		return err
	}
	return db.executeCreateOperation(ctx, op)
}

func (db *Database) createCollectionOperation(
	name string,
	opts ...options.Lister[options.CreateCollectionOptions],
) (*operation.Create, error) {
	args, err := mongoutil.NewOptions[options.CreateCollectionOptions](opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to construct options from builder: %w", err)
	}

	op := operation.NewCreate(name).ServerAPI(db.client.serverAPI).Authenticator(db.client.authenticator)

	if args.Capped != nil {
		op.Capped(*args.Capped)
	}
	if args.Collation != nil {
		op.Collation(bsoncore.Document(toDocument(args.Collation)))
	}
	if args.ChangeStreamPreAndPostImages != nil {
		csppi, err := marshal(args.ChangeStreamPreAndPostImages, db.bsonOpts, db.registry)
		if err != nil {
			return nil, err
		}
		op.ChangeStreamPreAndPostImages(csppi)
	}
	if args.DefaultIndexOptions != nil {
		defaultIndexArgs, err := mongoutil.NewOptions[options.DefaultIndexOptions](args.DefaultIndexOptions)
		if err != nil {
			return nil, fmt.Errorf("failed to construct DefaultIndexArgs from options: %w", err)
		}

		idx, doc := bsoncore.AppendDocumentStart(nil)
		if defaultIndexArgs.StorageEngine != nil {
			storageEngine, err := marshal(defaultIndexArgs.StorageEngine, db.bsonOpts, db.registry)
			if err != nil {
				return nil, err
			}

			doc = bsoncore.AppendDocumentElement(doc, "storageEngine", storageEngine)
		}

		doc, err = bsoncore.AppendDocumentEnd(doc, idx)
		if err != nil {
			return nil, err
		}

		op.IndexOptionDefaults(doc)
	}
	if args.MaxDocuments != nil {
		op.Max(*args.MaxDocuments)
	}
	if args.SizeInBytes != nil {
		op.Size(*args.SizeInBytes)
	}
	if args.StorageEngine != nil {
		storageEngine, err := marshal(args.StorageEngine, db.bsonOpts, db.registry)
		if err != nil {
			return nil, err
		}
		op.StorageEngine(storageEngine)
	}
	if args.ValidationAction != nil {
		op.ValidationAction(*args.ValidationAction)
	}
	if args.ValidationLevel != nil {
		op.ValidationLevel(*args.ValidationLevel)
	}
	if args.Validator != nil {
		validator, err := marshal(args.Validator, db.bsonOpts, db.registry)
		if err != nil {
			return nil, err
		}
		op.Validator(validator)
	}
	if args.ExpireAfterSeconds != nil {
		op.ExpireAfterSeconds(*args.ExpireAfterSeconds)
	}
	if args.TimeSeriesOptions != nil {
		timeSeriesArgs, err := mongoutil.NewOptions[options.TimeSeriesOptions](args.TimeSeriesOptions)
		if err != nil {
			return nil, fmt.Errorf("failed to construct DefaultIndexArgs from options: %w", err)
		}

		idx, doc := bsoncore.AppendDocumentStart(nil)
		doc = bsoncore.AppendStringElement(doc, "timeField", timeSeriesArgs.TimeField)

		if timeSeriesArgs.MetaField != nil {
			doc = bsoncore.AppendStringElement(doc, "metaField", *timeSeriesArgs.MetaField)
		}
		if timeSeriesArgs.Granularity != nil {
			doc = bsoncore.AppendStringElement(doc, "granularity", *timeSeriesArgs.Granularity)
		}

		if timeSeriesArgs.BucketMaxSpan != nil {
			bmss := int64(*timeSeriesArgs.BucketMaxSpan / time.Second)

			doc = bsoncore.AppendInt64Element(doc, "bucketMaxSpanSeconds", bmss)
		}

		if timeSeriesArgs.BucketRounding != nil {
			brs := int64(*timeSeriesArgs.BucketRounding / time.Second)

			doc = bsoncore.AppendInt64Element(doc, "bucketRoundingSeconds", brs)
		}

		doc, err = bsoncore.AppendDocumentEnd(doc, idx)
		if err != nil {
			return nil, err
		}

		op.TimeSeries(doc)
	}
	if args.ClusteredIndex != nil {
		clusteredIndex, err := marshal(args.ClusteredIndex, db.bsonOpts, db.registry)
		if err != nil {
			return nil, err
		}
		op.ClusteredIndex(clusteredIndex)
	}

	return op, nil
}

// CreateView creates a view on the server.
//
// The viewName parameter specifies the name of the view to create. The viewOn
// parameter specifies the name of the collection or view on which this view
// will be created. The pipeline parameter specifies an aggregation pipeline
// that will be exececuted against the source collection or view to create this
// view.
//
// MongoDB versions < 7.0 will return an error if the view already exists.
// MongoDB versions >= 7.0 will not return an error if an existing view created
// with the same name and options already exists.
//
// See https://www.mongodb.com/docs/manual/core/views/ for more information
// about views.
func (db *Database) CreateView(ctx context.Context, viewName, viewOn string, pipeline interface{},
	opts ...options.Lister[options.CreateViewOptions]) error {

	pipelineArray, _, err := marshalAggregatePipeline(pipeline, db.bsonOpts, db.registry)
	if err != nil {
		return err
	}

	op := operation.NewCreate(viewName).
		ViewOn(viewOn).
		Pipeline(pipelineArray).
		ServerAPI(db.client.serverAPI).Authenticator(db.client.authenticator)
	args, err := mongoutil.NewOptions(opts...)
	if err != nil {
		return fmt.Errorf("failed to construct options from builder: %w", err)
	}

	if args.Collation != nil {
		op.Collation(bsoncore.Document(toDocument(args.Collation)))
	}

	return db.executeCreateOperation(ctx, op)
}

func (db *Database) executeCreateOperation(ctx context.Context, op *operation.Create) error {
	sess := sessionFromContext(ctx)
	if sess == nil && db.client.sessionPool != nil {
		sess = session.NewImplicitClientSession(db.client.sessionPool, db.client.id)
		defer sess.EndSession()
	}

	err := db.client.validSession(sess)
	if err != nil {
		return err
	}

	wc := db.writeConcern
	if sess.TransactionRunning() {
		wc = nil
	}
	if !wc.Acknowledged() {
		sess = nil
	}

	selector := makePinnedSelector(sess, db.writeSelector)
	op = op.Session(sess).
		WriteConcern(wc).
		CommandMonitor(db.client.monitor).
		ServerSelector(selector).
		ClusterClock(db.client.clock).
		Database(db.name).
		Deployment(db.client.deployment).
		Crypt(db.client.cryptFLE)

	return replaceErrors(op.Execute(ctx))
}

// GridFSBucket is used to construct a GridFS bucket which can be used as a
// container for files.
func (db *Database) GridFSBucket(opts ...options.Lister[options.BucketOptions]) *GridFSBucket {
	b := &GridFSBucket{
		name:      "fs",
		chunkSize: DefaultGridFSChunkSize,
		db:        db,
	}

	bo, _ := mongoutil.NewOptions[options.BucketOptions](opts...)
	if bo.Name != nil {
		b.name = *bo.Name
	}
	if bo.ChunkSizeBytes != nil {
		b.chunkSize = *bo.ChunkSizeBytes
	}
	if bo.WriteConcern != nil {
		b.wc = bo.WriteConcern
	}
	if bo.ReadConcern != nil {
		b.rc = bo.ReadConcern
	}
	if bo.ReadPreference != nil {
		b.rp = bo.ReadPreference
	}

	var collOpts = options.Collection().SetWriteConcern(b.wc).SetReadConcern(b.rc).SetReadPreference(b.rp)

	b.chunksColl = db.Collection(b.name+".chunks", collOpts)
	b.filesColl = db.Collection(b.name+".files", collOpts)
	b.readBuf = make([]byte, b.chunkSize)
	b.writeBuf = make([]byte, b.chunkSize)

	return b
}
