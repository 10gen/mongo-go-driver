// Copyright (C) MongoDB, Inc. 2017-present.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

package integration

import (
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"reflect"
	"sync"
	"testing"
	"time"
	"unsafe"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/bsoncodec"
	"go.mongodb.org/mongo-driver/bson/bsonrw"
	"go.mongodb.org/mongo-driver/bson/bsontype"
	"go.mongodb.org/mongo-driver/event"
	"go.mongodb.org/mongo-driver/internal/assert"
	"go.mongodb.org/mongo-driver/internal/bsonutil"
	"go.mongodb.org/mongo-driver/internal/integtest"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/address"
	"go.mongodb.org/mongo-driver/mongo/gridfs"
	"go.mongodb.org/mongo-driver/mongo/integration/mtest"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readconcern"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"go.mongodb.org/mongo-driver/x/mongo/driver/session"
	"go.mongodb.org/mongo-driver/x/mongo/driver/topology"
)

const (
	gridFSFiles            = "fs.files"
	gridFSChunks           = "fs.chunks"
	spec1403SkipReason     = "servers less than 4.2 do not have mongocryptd; see SPEC-1403"
	godriver2466SkipReason = "test has not been updated; see GODRIVER-2466"
	godriver2413SkipReason = "encryptedFields argument is not supported on Collection.Drop; see GODRIVER-2413"
)

var (
	defaultHeartbeatInterval = 50 * time.Millisecond
	skippedTestDescriptions  = map[string]string{
		// SPEC-1403: This test checks to see if the correct error is thrown when auto encrypting with a server < 4.2.
		// Currently, the test will fail because a server < 4.2 wouldn't have mongocryptd, so Client construction
		// would fail with a mongocryptd spawn error.
		"operation fails with maxWireVersion < 8": spec1403SkipReason,
		// GODRIVER-2466: The test below has not been updated as required.
		"Network error on minPoolSize background creation": godriver2466SkipReason,
		"CreateCollection from encryptedFields.":           godriver2413SkipReason,
		"DropCollection from encryptedFields":              godriver2413SkipReason,
		"DropCollection from remote encryptedFields":       godriver2413SkipReason,
	}
)

type testFile struct {
	RunOn           []mtest.RunOnBlock `bson:"runOn"`
	DatabaseName    string             `bson:"database_name"`
	CollectionName  string             `bson:"collection_name"`
	BucketName      string             `bson:"bucket_name"`
	Data            testData           `bson:"data"`
	JSONSchema      bson.Raw           `bson:"json_schema"`
	KeyVaultData    []bson.Raw         `bson:"key_vault_data"`
	Tests           []*testCase        `bson:"tests"`
	EncryptedFields bson.Raw           `bson:"encrypted_fields"`
}

type testData struct {
	Documents  []bson.Raw
	GridFSData struct {
		Files  []bson.Raw `bson:"fs.files"`
		Chunks []bson.Raw `bson:"fs.chunks"`
	}
}

// custom decoder for testData type
func decodeTestData(dc bsoncodec.DecodeContext, vr bsonrw.ValueReader, val reflect.Value) error {
	switch vr.Type() {
	case bsontype.Array:
		docsVal := val.FieldByName("Documents")
		decoder, err := dc.Registry.LookupDecoder(docsVal.Type())
		if err != nil {
			return err
		}

		return decoder.DecodeValue(dc, vr, docsVal)
	case bsontype.EmbeddedDocument:
		gridfsDataVal := val.FieldByName("GridFSData")
		decoder, err := dc.Registry.LookupDecoder(gridfsDataVal.Type())
		if err != nil {
			return err
		}

		return decoder.DecodeValue(dc, vr, gridfsDataVal)
	}
	return nil
}

type testCase struct {
	Description         string          `bson:"description"`
	SkipReason          string          `bson:"skipReason"`
	FailPoint           *bson.Raw       `bson:"failPoint"`
	ClientOptions       bson.Raw        `bson:"clientOptions"`
	SessionOptions      bson.Raw        `bson:"sessionOptions"`
	Operations          []*operation    `bson:"operations"`
	Expectations        *[]*expectation `bson:"expectations"`
	UseMultipleMongoses bool            `bson:"useMultipleMongoses"`
	Outcome             *outcome        `bson:"outcome"`

	// set in code if the test is a GridFS test
	chunkSize int32
	bucket    *gridfs.Bucket

	// set in code to track test context
	testTopology    *topology.Topology
	recordedPrimary address.Address
	monitor         *unifiedRunnerEventMonitor
	routinesMap     sync.Map // maps thread name to *backgroundRoutine
}

type operation struct {
	Name              string      `bson:"name"`
	Object            string      `bson:"object"`
	CollectionOptions bson.Raw    `bson:"collectionOptions"`
	DatabaseOptions   bson.Raw    `bson:"databaseOptions"`
	Result            interface{} `bson:"result"`
	Arguments         bson.Raw    `bson:"arguments"`
	Error             bool        `bson:"error"`
	CommandName       string      `bson:"command_name"`

	// set in code after determining whether or not result represents an error
	opError *operationError
}

type expectation struct {
	CommandStartedEvent *struct {
		CommandName  string                 `bson:"command_name"`
		DatabaseName string                 `bson:"database_name"`
		Command      bson.Raw               `bson:"command"`
		Extra        map[string]interface{} `bson:",inline"`
	} `bson:"command_started_event"`
	CommandSucceededEvent *struct {
		CommandName string                 `bson:"command_name"`
		Reply       bson.Raw               `bson:"reply"`
		Extra       map[string]interface{} `bson:",inline"`
	} `bson:"command_succeeded_event"`
	CommandFailedEvent *struct {
		CommandName string                 `bson:"command_name"`
		Extra       map[string]interface{} `bson:",inline"`
	} `bson:"command_failed_event"`
}

type outcome struct {
	Collection *outcomeCollection `bson:"collection"`
}

type outcomeCollection struct {
	Name string      `bson:"name"`
	Data interface{} `bson:"data"`
}

type operationError struct {
	ErrorContains      *string  `bson:"errorContains"`
	ErrorCodeName      *string  `bson:"errorCodeName"`
	ErrorLabelsContain []string `bson:"errorLabelsContain"`
	ErrorLabelsOmit    []string `bson:"errorLabelsOmit"`
	IsTimeoutError     *bool    `bson:"isTimeoutError"`
}

const dataPath string = "../../testdata/"

var directories = []string{
	"transactions/legacy",
	"convenient-transactions",
	"retryable-reads",
	"read-write-concern/operation",
	"server-discovery-and-monitoring/integration",
	"atlas-data-lake-testing",
}

var checkOutcomeOpts = options.Collection().SetReadPreference(readpref.Primary()).SetReadConcern(readconcern.Local())
var specTestRegistry = bson.NewRegistryBuilder().
	RegisterTypeMapEntry(bson.TypeEmbeddedDocument, reflect.TypeOf(bson.Raw{})).
	RegisterTypeDecoder(reflect.TypeOf(testData{}), bsoncodec.ValueDecoderFunc(decodeTestData)).Build()

func TestUnifiedSpecs(t *testing.T) {
	for _, specDir := range directories {
		t.Run(specDir, func(t *testing.T) {
			for _, fileName := range jsonFilesInDir(t, path.Join(dataPath, specDir)) {
				t.Run(fileName, func(t *testing.T) {
					runSpecTestFile(t, specDir, fileName)
				})
			}
		})
	}
}

// specDir: name of directory for a spec in the data/ folder
// fileName: name of test file in specDir
func runSpecTestFile(t *testing.T, specDir, fileName string) {
	filePath := path.Join(dataPath, specDir, fileName)
	content, err := ioutil.ReadFile(filePath)
	assert.Nil(t, err, "unable to read spec test file %v: %v", filePath, err)

	var testFile testFile
	err = bson.UnmarshalExtJSONWithRegistry(specTestRegistry, content, false, &testFile)
	assert.Nil(t, err, "unable to unmarshal spec test file at %v: %v", filePath, err)

	// create mtest wrapper and skip if needed
	mtOpts := mtest.NewOptions().
		RunOn(testFile.RunOn...).
		CreateClient(false)
	if specDir == "atlas-data-lake-testing" {
		mtOpts.AtlasDataLake(true)
	}
	mt := mtest.New(t, mtOpts)

	for _, test := range testFile.Tests {
		runSpecTestCase(mt, test, testFile)
	}
}

func runSpecTestCase(mt *mtest.T, test *testCase, testFile testFile) {
	opts := mtest.NewOptions().DatabaseName(testFile.DatabaseName).CollectionName(testFile.CollectionName)
	if mtest.ClusterTopologyKind() == mtest.Sharded && !test.UseMultipleMongoses {
		// pin to a single mongos
		opts = opts.ClientType(mtest.Pinned)
	}

	cco := options.CreateCollection()
	if len(testFile.JSONSchema) > 0 {
		validator := bson.D{
			{"$jsonSchema", testFile.JSONSchema},
		}
		cco.SetValidator(validator)
	}

	if len(testFile.EncryptedFields) > 0 {
		cco.SetEncryptedFields(testFile.EncryptedFields)
	}

	opts.CollectionCreateOptions(cco)

	// Start the test without setting client options so the setup will be done with a default client.
	mt.RunOpts(test.Description, opts, func(mt *mtest.T) {
		if len(test.SkipReason) > 0 {
			mt.Skip(test.SkipReason)
		}
		if skipReason, ok := skippedTestDescriptions[test.Description]; ok {
			mt.Skipf("skipping due to known failure: %v", skipReason)
		}

		// work around for SERVER-39704: run a non-transactional distinct against each shard in a sharded cluster
		if mtest.ClusterTopologyKind() == mtest.Sharded && test.Description == "distinct" {
			err := runCommandOnAllServers(func(mongosClient *mongo.Client) error {
				coll := mongosClient.Database(mt.DB.Name()).Collection(mt.Coll.Name())
				_, err := coll.Distinct(context.Background(), "x", bson.D{})
				return err
			})
			assert.Nil(mt, err, "error running distinct against all mongoses: %v", err)
		}

		// Defer killSessions to ensure it runs regardless of the state of the test because the client has already
		// been created and the collection drop in mongotest will hang for transactions to be aborted (60 seconds)
		// in error cases.
		defer killSessions(mt)

		// Test setup: create collections that are tracked by mtest, insert test data, and set the failpoint.
		setupTest(mt, &testFile, test)
		if test.FailPoint != nil {
			mt.SetFailPointFromDocument(*test.FailPoint)
		}

		// Reset the client using the client options specified in the test.
		testClientOpts := createClientOptions(mt, test.ClientOptions)

		// If AutoEncryptionOptions is set and AutoEncryption isn't disabled (neither
		// bypassAutoEncryption nor bypassQueryAnalysis are true), then add extra options to load
		// the crypt_shared library.
		if testClientOpts.AutoEncryptionOptions != nil {
			bypassAutoEncryption := testClientOpts.AutoEncryptionOptions.BypassAutoEncryption != nil &&
				*testClientOpts.AutoEncryptionOptions.BypassAutoEncryption
			bypassQueryAnalysis := testClientOpts.AutoEncryptionOptions.BypassQueryAnalysis != nil &&
				*testClientOpts.AutoEncryptionOptions.BypassQueryAnalysis
			if !bypassAutoEncryption && !bypassQueryAnalysis {
				if testClientOpts.AutoEncryptionOptions.ExtraOptions == nil {
					testClientOpts.AutoEncryptionOptions.ExtraOptions = make(map[string]interface{})
				}

				for k, v := range getCryptSharedLibExtraOptions() {
					testClientOpts.AutoEncryptionOptions.ExtraOptions[k] = v
				}
			}
		}

		test.monitor = newUnifiedRunnerEventMonitor()
		testClientOpts.SetPoolMonitor(&event.PoolMonitor{
			Event: test.monitor.handlePoolEvent,
		})
		testClientOpts.SetServerMonitor(test.monitor.sdamMonitor)
		if testClientOpts.HeartbeatInterval == nil {
			// If one isn't specified in the test, use a low heartbeat frequency so the Client will quickly recover when
			// using failpoints that cause SDAM state changes.
			testClientOpts.SetHeartbeatInterval(defaultHeartbeatInterval)
		}
		mt.ResetClient(testClientOpts)

		// Record the underlying topology for the test's Client.
		test.testTopology = getTopologyFromClient(mt.Client)

		// Create the GridFS bucket and sessions after resetting the client so it will be created with a connected
		// client.
		createBucket(mt, testFile, test)
		sess0, sess1 := setupSessions(mt, test)
		if sess0 != nil {
			defer func() {
				sess0.EndSession(context.Background())
				sess1.EndSession(context.Background())
			}()
		}

		// run operations
		mt.ClearEvents()
		for idx, op := range test.Operations {
			err := runOperation(mt, test, op, sess0, sess1)
			assert.Nil(mt, err, "error running operation %q at index %d: %v", op.Name, idx, err)
		}

		// Needs to be done here (in spite of defer) because some tests
		// require end session to be called before we check expectation
		sess0.EndSession(context.Background())
		sess1.EndSession(context.Background())
		mt.ClearFailPoints()

		checkExpectations(mt, test.Expectations, sess0.ID(), sess1.ID())

		if test.Outcome != nil {
			verifyTestOutcome(mt, test.Outcome.Collection)
		}
	})
}

func createBucket(mt *mtest.T, testFile testFile, testCase *testCase) {
	if testFile.BucketName == "" {
		return
	}

	bucketOpts := options.GridFSBucket()
	if testFile.BucketName != "" {
		bucketOpts.SetName(testFile.BucketName)
	}
	chunkSize := testCase.chunkSize
	if chunkSize == 0 {
		chunkSize = gridfs.DefaultChunkSize
	}
	bucketOpts.SetChunkSizeBytes(chunkSize)

	var err error
	testCase.bucket, err = gridfs.NewBucket(mt.DB, bucketOpts)
	assert.Nil(mt, err, "NewBucket error: %v", err)
}

func runOperation(mt *mtest.T, testCase *testCase, op *operation, sess0, sess1 mongo.Session) error {
	if op.Name == "count" {
		mt.Skip("count has been deprecated")
	}

	var sess mongo.Session
	if sessVal, err := op.Arguments.LookupErr("session"); err == nil {
		sessStr := sessVal.StringValue()
		switch sessStr {
		case "session0":
			sess = sess0
		case "session1":
			sess = sess1
		default:
			return fmt.Errorf("unrecognized session identifier: %v", sessStr)
		}
	}

	if op.Object == "testRunner" {
		return executeTestRunnerOperation(mt, testCase, op, sess)
	}

	if op.DatabaseOptions != nil {
		mt.CloneDatabase(createDatabaseOptions(mt, op.DatabaseOptions))
	}
	if op.CollectionOptions != nil {
		mt.CloneCollection(createCollectionOptions(mt, op.CollectionOptions))
	}

	// execute the command on the given object
	var err error
	switch op.Object {
	case "session0":
		err = executeSessionOperation(mt, op, sess0)
	case "session1":
		err = executeSessionOperation(mt, op, sess1)
	case "", "collection":
		// object defaults to "collection" if not specified
		err = executeCollectionOperation(mt, op, sess)
	case "database":
		err = executeDatabaseOperation(mt, op, sess)
	case "gridfsbucket":
		err = executeGridFSOperation(mt, testCase.bucket, op)
	case "client":
		err = executeClientOperation(mt, op, sess)
	default:
		return fmt.Errorf("unrecognized operation object: %v", op.Object)
	}

	op.opError = errorFromResult(mt, op.Result)
	// Some tests (e.g. crud/v2) only specify that an error should occur via the op.Error field but do not specify
	// which error via the op.Result field. In this case, pass in an empty non-nil operationError so verifyError will
	// make the right assertions.
	if op.Error && op.Result == nil {
		op.opError = &operationError{}
	}
	return verifyError(op.opError, err)
}

func executeGridFSOperation(mt *mtest.T, bucket *gridfs.Bucket, op *operation) error {
	// no results for GridFS operations
	assert.Nil(mt, op.Result, "unexpected result for GridFS operation")

	switch op.Name {
	case "download":
		_, err := executeGridFSDownload(mt, bucket, op.Arguments)
		return err
	case "download_by_name":
		_, err := executeGridFSDownloadByName(mt, bucket, op.Arguments)
		return err
	default:
		mt.Fatalf("unrecognized gridfs operation: %v", op.Name)
	}
	return nil
}

func executeTestRunnerOperation(mt *mtest.T, testCase *testCase, op *operation, sess mongo.Session) error {
	var clientSession *session.Client
	if sess != nil {
		xsess, ok := sess.(mongo.XSession)
		if !ok {
			return fmt.Errorf("expected session type %T to implement mongo.XSession", sess)
		}
		clientSession = xsess.ClientSession()
	}

	switch op.Name {
	case "targetedFailPoint":
		fpDoc := op.Arguments.Lookup("failPoint")

		var fp mtest.FailPoint
		if err := bson.Unmarshal(fpDoc.Document(), &fp); err != nil {
			return fmt.Errorf("Unmarshal error: %w", err)
		}

		if clientSession == nil {
			return errors.New("expected valid session, got nil")
		}
		targetHost := clientSession.PinnedServer.Addr.String()
		opts := options.Client().ApplyURI(mtest.ClusterURI()).SetHosts([]string{targetHost})
		integtest.AddTestServerAPIVersion(opts)
		client, err := mongo.Connect(context.Background(), opts)
		if err != nil {
			return fmt.Errorf("Connect error for targeted client: %w", err)
		}

		if err = client.Database("admin").RunCommand(context.Background(), fp).Err(); err != nil {
			return fmt.Errorf("error setting targeted fail point: %w", err)
		}
		mt.TrackFailPoint(fp.ConfigureFailPoint, client)
	case "configureFailPoint":
		fp, err := op.Arguments.LookupErr("failPoint")
		if err != nil {
			return fmt.Errorf("unable to find 'failPoint' in arguments: %w", err)
		}
		mt.SetFailPointFromDocument(fp.Document())
	case "assertSessionTransactionState":
		stateVal, err := op.Arguments.LookupErr("state")
		if err != nil {
			return fmt.Errorf("unable to find 'state' in arguments: %w", err)
		}
		expectedState, ok := stateVal.StringValueOK()
		if !ok {
			return errors.New("expected 'state' argument to be string")
		}

		if clientSession == nil {
			return errors.New("expected valid session, got nil")
		}
		actualState := clientSession.TransactionState.String()

		// actualState should match expectedState, but "in progress" is the same as
		// "in_progress".
		stateMatch := actualState == expectedState ||
			actualState == "in progress" && expectedState == "in_progress"
		if !stateMatch {
			return fmt.Errorf("expected transaction state %v, got %v", expectedState, actualState)
		}
	case "assertSessionPinned":
		if clientSession == nil {
			return errors.New("expected valid session, got nil")
		}
		if clientSession.PinnedServer == nil {
			return errors.New("expected pinned server, got nil")
		}
	case "assertSessionUnpinned":
		if clientSession == nil {
			return errors.New("expected valid session, got nil")
		}
		// We don't use a combined helper for assertSessionPinned and assertSessionUnpinned because the unpinned
		// case provides the pinned server address in the error msg for debugging.
		if clientSession.PinnedServer != nil {
			return fmt.Errorf("expected pinned server to be nil but got %q", clientSession.PinnedServer.Addr)
		}
	case "assertSameLsidOnLastTwoCommands":
		first, second := lastTwoIDs(mt)
		if !first.Equal(second) {
			return fmt.Errorf("expected last two lsids to be equal but got %v and %v", first, second)
		}
	case "assertDifferentLsidOnLastTwoCommands":
		first, second := lastTwoIDs(mt)
		if first.Equal(second) {
			return fmt.Errorf("expected last two lsids to be not equal but both were %v", first)
		}
	case "assertCollectionExists":
		return verifyCollectionState(op, true)
	case "assertCollectionNotExists":
		return verifyCollectionState(op, false)
	case "assertIndexExists":
		return verifyIndexState(op, true)
	case "assertIndexNotExists":
		return verifyIndexState(op, false)
	case "wait":
		time.Sleep(convertValueToMilliseconds(mt, op.Arguments.Lookup("ms")))
	case "waitForEvent":
		waitForEvent(mt, testCase, op)
	case "assertEventCount":
		assertEventCount(mt, testCase, op)
	case "recordPrimary":
		recordPrimary(mt, testCase)
	case "runAdminCommand":
		executeAdminCommand(mt, op)
	case "waitForPrimaryChange":
		waitForPrimaryChange(mt, testCase, op)
	case "startThread":
		startThread(mt, testCase, op)
	case "runOnThread":
		runOnThread(mt, testCase, op)
	case "waitForThread":
		waitForThread(mt, testCase, op)
	default:
		return fmt.Errorf("unrecognized testRunner operation %v", op.Name)
	}

	return nil
}

func verifyIndexState(op *operation, shouldExist bool) error {
	db := op.Arguments.Lookup("database").StringValue()
	coll := op.Arguments.Lookup("collection").StringValue()
	index := op.Arguments.Lookup("index").StringValue()

	exists, err := indexExists(db, coll, index)
	if err != nil {
		return err
	}
	if exists != shouldExist {
		return fmt.Errorf("index state mismatch for index %s in namespace %s.%s; should exist: %v, exists: %v",
			index, db, coll, shouldExist, exists)
	}
	return nil
}

func indexExists(dbName, collName, indexName string) (bool, error) {
	// Use global client because listIndexes cannot be executed inside a transaction.
	iv := mtest.GlobalClient().Database(dbName).Collection(collName).Indexes()
	cursor, err := iv.List(context.Background())
	if err != nil {
		return false, fmt.Errorf("IndexView.List error: %w", err)
	}
	defer cursor.Close(context.Background())

	for cursor.Next(context.Background()) {
		if cursor.Current.Lookup("name").StringValue() == indexName {
			return true, nil
		}
	}
	return false, cursor.Err()
}

func verifyCollectionState(op *operation, shouldExist bool) error {
	db := op.Arguments.Lookup("database").StringValue()
	coll := op.Arguments.Lookup("collection").StringValue()

	exists, err := collectionExists(db, coll)
	if err != nil {
		return err
	}
	if exists != shouldExist {
		return fmt.Errorf("collection state mismatch for %s.%s; should exist %v, exists: %v", db, coll, shouldExist,
			exists)
	}
	return nil
}

func collectionExists(dbName, collName string) (bool, error) {
	filter := bson.D{
		{"name", collName},
	}

	// Use global client because listCollections cannot be executed inside a transaction.
	collections, err := mtest.GlobalClient().Database(dbName).ListCollectionNames(context.Background(), filter)
	if err != nil {
		return false, fmt.Errorf("ListCollectionNames error: %w", err)
	}

	return len(collections) > 0, nil
}

func lastTwoIDs(mt *mtest.T) (bson.RawValue, bson.RawValue) {
	events := mt.GetAllStartedEvents()
	lastTwoEvents := events[len(events)-2:]

	first := lastTwoEvents[0].Command.Lookup("lsid")
	second := lastTwoEvents[1].Command.Lookup("lsid")
	return first, second
}

func executeSessionOperation(mt *mtest.T, op *operation, sess mongo.Session) error {
	switch op.Name {
	case "startTransaction":
		var txnOpts *options.TransactionOptions
		if opts, err := op.Arguments.LookupErr("options"); err == nil {
			txnOpts = createTransactionOptions(mt, opts.Document())
		}
		return sess.StartTransaction(txnOpts)
	case "commitTransaction":
		return sess.CommitTransaction(context.Background())
	case "abortTransaction":
		return sess.AbortTransaction(context.Background())
	case "withTransaction":
		return executeWithTransaction(mt, sess, op.Arguments)
	default:
		return fmt.Errorf("unrecognized session operation: %v", op.Name)
	}
}

func executeCollectionOperation(mt *mtest.T, op *operation, sess mongo.Session) error {
	switch op.Name {
	case "countDocuments":
		// no results to verify with count
		res, err := executeCountDocuments(mt, sess, op.Arguments)
		if op.opError == nil && err == nil {
			verifyCountResult(mt, res, op.Result)
		}
		return err
	case "distinct":
		res, err := executeDistinct(mt, sess, op.Arguments)
		if op.opError == nil && err == nil {
			verifyDistinctResult(mt, res, op.Result)
		}
		return err
	case "insertOne":
		res, err := executeInsertOne(mt, sess, op.Arguments)
		if op.opError == nil && err == nil {
			verifyInsertOneResult(mt, res, op.Result)
		}
		return err
	case "insertMany":
		res, err := executeInsertMany(mt, sess, op.Arguments)
		if op.opError == nil && err == nil {
			verifyInsertManyResult(mt, res, op.Result)
		}
		return err
	case "find":
		cursor, err := executeFind(mt, sess, op.Arguments)
		if op.opError == nil && err == nil {
			verifyCursorResult(mt, cursor, op.Result)
			_ = cursor.Close(context.Background())
		}
		return err
	case "findOneAndDelete":
		res := executeFindOneAndDelete(mt, sess, op.Arguments)
		if op.opError == nil && res.Err() == nil {
			verifySingleResult(mt, res, op.Result)
		}
		return res.Err()
	case "findOneAndUpdate":
		res := executeFindOneAndUpdate(mt, sess, op.Arguments)
		if op.opError == nil && res.Err() == nil {
			verifySingleResult(mt, res, op.Result)
		}
		return res.Err()
	case "findOneAndReplace":
		res := executeFindOneAndReplace(mt, sess, op.Arguments)
		if op.opError == nil && res.Err() == nil {
			verifySingleResult(mt, res, op.Result)
		}
		return res.Err()
	case "deleteOne":
		res, err := executeDeleteOne(mt, sess, op.Arguments)
		if op.opError == nil && err == nil {
			verifyDeleteResult(mt, res, op.Result)
		}
		return err
	case "deleteMany":
		res, err := executeDeleteMany(mt, sess, op.Arguments)
		if op.opError == nil && err == nil {
			verifyDeleteResult(mt, res, op.Result)
		}
		return err
	case "updateOne":
		res, err := executeUpdateOne(mt, sess, op.Arguments)
		if op.opError == nil && err == nil {
			verifyUpdateResult(mt, res, op.Result)
		}
		return err
	case "updateMany":
		res, err := executeUpdateMany(mt, sess, op.Arguments)
		if op.opError == nil && err == nil {
			verifyUpdateResult(mt, res, op.Result)
		}
		return err
	case "replaceOne":
		res, err := executeReplaceOne(mt, sess, op.Arguments)
		if op.opError == nil && err == nil {
			verifyUpdateResult(mt, res, op.Result)
		}
		return err
	case "aggregate":
		cursor, err := executeAggregate(mt, mt.Coll, sess, op.Arguments)
		if op.opError == nil && err == nil {
			verifyCursorResult(mt, cursor, op.Result)
			_ = cursor.Close(context.Background())
		}
		return err
	case "bulkWrite":
		res, err := executeBulkWrite(mt, sess, op.Arguments)
		if op.opError == nil && err == nil {
			verifyBulkWriteResult(mt, res, op.Result)
		}
		return err
	case "estimatedDocumentCount":
		res, err := executeEstimatedDocumentCount(mt, sess, op.Arguments)
		if op.opError == nil && err == nil {
			verifyCountResult(mt, res, op.Result)
		}
		return err
	case "findOne":
		res := executeFindOne(mt, sess, op.Arguments)
		if op.opError == nil && res.Err() == nil {
			verifySingleResult(mt, res, op.Result)
		}
		return res.Err()
	case "listIndexes":
		cursor, err := executeListIndexes(mt, sess, op.Arguments)
		if op.opError == nil && err == nil {
			verifyCursorResult(mt, cursor, op.Result)
			_ = cursor.Close(context.Background())
		}
		return err
	case "watch":
		stream, err := executeWatch(mt, mt.Coll, sess, op.Arguments)
		if op.opError == nil && err == nil {
			assert.Nil(mt, op.Result, "unexpected result for watch: %v", op.Result)
			_ = stream.Close(context.Background())
		}
		return err
	case "createIndex":
		indexName, err := executeCreateIndex(mt, sess, op.Arguments)
		if op.opError == nil && err == nil {
			assert.Nil(mt, op.Result, "unexpected result for createIndex: %v", op.Result)
			assert.True(mt, len(indexName) > 0, "expected valid index name, got empty string")
			assert.True(mt, len(indexName) > 0, "created index has empty name")
		}
		return err
	case "dropIndex":
		res, err := executeDropIndex(mt, sess, op.Arguments)
		if op.opError == nil && err == nil {
			assert.Nil(mt, op.Result, "unexpected result for dropIndex: %v", op.Result)
			assert.NotNil(mt, res, "expected result from dropIndex operation, got nil")
		}
		return err
	case "listIndexNames", "mapReduce":
		mt.Skipf("operation %v not implemented", op.Name)
	default:
		mt.Fatalf("unrecognized collection operation: %v", op.Name)
	}
	return nil
}

func executeDatabaseOperation(mt *mtest.T, op *operation, sess mongo.Session) error {
	switch op.Name {
	case "runCommand":
		res := executeRunCommand(mt, sess, op.Arguments)
		if op.opError == nil && res.Err() == nil {
			verifySingleResult(mt, res, op.Result)
		}
		return res.Err()
	case "aggregate":
		cursor, err := executeAggregate(mt, mt.DB, sess, op.Arguments)
		if op.opError == nil && err == nil {
			verifyCursorResult(mt, cursor, op.Result)
			_ = cursor.Close(context.Background())
		}
		return err
	case "listCollections":
		cursor, err := executeListCollections(mt, sess, op.Arguments)
		if op.opError == nil && err == nil {
			assert.Nil(mt, op.Result, "unexpected result for listCollections: %v", op.Result)
			_ = cursor.Close(context.Background())
		}
		return err
	case "listCollectionNames":
		_, err := executeListCollectionNames(mt, sess, op.Arguments)
		if op.opError == nil && err == nil {
			assert.Nil(mt, op.Result, "unexpected result for listCollectionNames: %v", op.Result)
		}
		return err
	case "watch":
		stream, err := executeWatch(mt, mt.DB, sess, op.Arguments)
		if op.opError == nil && err == nil {
			assert.Nil(mt, op.Result, "unexpected result for watch: %v", op.Result)
			_ = stream.Close(context.Background())
		}
		return err
	case "dropCollection":
		err := executeDropCollection(mt, sess, op.Arguments)
		if op.opError == nil && err == nil {
			assert.Nil(mt, op.Result, "unexpected result for dropCollection: %v", op.Result)
		}
		return err
	case "createCollection":
		err := executeCreateCollection(mt, sess, op.Arguments)
		if op.opError == nil && err == nil {
			assert.Nil(mt, op.Result, "unexpected result for createCollection: %v", op.Result)
		}
		return err
	case "listCollectionObjects":
		mt.Skipf("operation %v not implemented", op.Name)
	default:
		mt.Fatalf("unrecognized database operation: %v", op.Name)
	}
	return nil
}

func executeClientOperation(mt *mtest.T, op *operation, sess mongo.Session) error {
	switch op.Name {
	case "listDatabaseNames":
		_, err := executeListDatabaseNames(mt, sess, op.Arguments)
		if op.opError == nil && err == nil {
			assert.Nil(mt, op.Result, "unexpected result for countDocuments: %v", op.Result)
		}
		return err
	case "listDatabases":
		res, err := executeListDatabases(mt, sess, op.Arguments)
		if op.opError == nil && err == nil {
			verifyListDatabasesResult(mt, res, op.Result)
		}
		return err
	case "watch":
		stream, err := executeWatch(mt, mt.Client, sess, op.Arguments)
		if op.opError == nil && err == nil {
			assert.Nil(mt, op.Result, "unexpected result for watch: %v", op.Result)
			_ = stream.Close(context.Background())
		}
		return err
	case "listDatabaseObjects":
		mt.Skipf("operation %v not implemented", op.Name)
	default:
		mt.Fatalf("unrecognized client operation: %v", op.Name)
	}
	return nil
}

func setupSessions(mt *mtest.T, test *testCase) (mongo.Session, mongo.Session) {
	mt.Helper()

	var sess0Opts, sess1Opts *options.SessionOptions
	if opts, err := test.SessionOptions.LookupErr("session0"); err == nil {
		sess0Opts = createSessionOptions(mt, opts.Document())
	}
	if opts, err := test.SessionOptions.LookupErr("session1"); err == nil {
		sess1Opts = createSessionOptions(mt, opts.Document())
	}

	sess0, err := mt.Client.StartSession(sess0Opts)
	assert.Nil(mt, err, "error creating session0: %v", err)
	sess1, err := mt.Client.StartSession(sess1Opts)
	assert.Nil(mt, err, "error creating session1: %v", err)

	return sess0, sess1
}

func insertDocuments(mt *mtest.T, coll *mongo.Collection, rawDocs []bson.Raw) {
	mt.Helper()

	docsToInsert := bsonutil.RawToInterfaces(rawDocs...)
	if len(docsToInsert) == 0 {
		return
	}

	_, err := coll.InsertMany(context.Background(), docsToInsert)
	assert.Nil(mt, err, "InsertMany error for collection %v: %v", coll.Name(), err)
}

// load initial data into appropriate collections and set chunkSize for the test case if necessary
func setupTest(mt *mtest.T, testFile *testFile, testCase *testCase) {
	mt.Helper()

	// key vault data
	if len(testFile.KeyVaultData) > 0 {
		// Drop the key vault collection in case it exists from a prior test run.
		err := mt.Client.Database("keyvault").Collection("datakeys").Drop(context.Background())
		assert.Nil(mt, err, "error dropping key vault collection")

		keyVaultColl := mt.CreateCollection(mtest.Collection{
			Name: "datakeys",
			DB:   "keyvault",
		}, false)

		insertDocuments(mt, keyVaultColl, testFile.KeyVaultData)
	}

	// regular documents
	if testFile.Data.Documents != nil {
		insertDocuments(mt, mt.Coll, testFile.Data.Documents)
		return
	}

	// GridFS data
	gfsData := testFile.Data.GridFSData

	if gfsData.Chunks != nil {
		chunks := mt.CreateCollection(mtest.Collection{
			Name: gridFSChunks,
		}, false)
		insertDocuments(mt, chunks, gfsData.Chunks)
	}
	if gfsData.Files != nil {
		files := mt.CreateCollection(mtest.Collection{
			Name: gridFSFiles,
		}, false)
		insertDocuments(mt, files, gfsData.Files)

		csVal, err := gfsData.Files[0].LookupErr("chunkSize")
		if err == nil {
			testCase.chunkSize = csVal.Int32()
		}
	}
}

func verifyTestOutcome(mt *mtest.T, outcomeColl *outcomeCollection) {
	// Outcome needs to be verified using the global client instead of the test client because certain client
	// configurations will cause outcome checking to fail. For example, a client configured with auto encryption
	// will decrypt results, causing comparisons to fail.

	collName := mt.Coll.Name()
	if outcomeColl.Name != "" {
		collName = outcomeColl.Name
	}
	coll := mtest.GlobalClient().Database(mt.DB.Name()).Collection(collName, checkOutcomeOpts)

	findOpts := options.Find().
		SetSort(bson.M{"_id": 1})
	cursor, err := coll.Find(context.Background(), bson.D{}, findOpts)
	assert.Nil(mt, err, "Find error: %v", err)
	verifyCursorResult(mt, cursor, outcomeColl.Data)
}

func getTopologyFromClient(client *mongo.Client) *topology.Topology {
	clientElem := reflect.ValueOf(client).Elem()
	deploymentField := clientElem.FieldByName("deployment")
	deploymentField = reflect.NewAt(deploymentField.Type(), unsafe.Pointer(deploymentField.UnsafeAddr())).Elem()
	return deploymentField.Interface().(*topology.Topology)
}

// getCryptSharedLibExtraOptions returns an AutoEncryption extra options map with crypt_shared
// library path information if the CRYPT_SHARED_LIB_PATH environment variable is set.
func getCryptSharedLibExtraOptions() map[string]interface{} {
	path := os.Getenv("CRYPT_SHARED_LIB_PATH")
	if path == "" {
		return nil
	}
	return map[string]interface{}{
		"cryptSharedLibRequired": true,
		"cryptSharedLibPath":     path,
	}
}
