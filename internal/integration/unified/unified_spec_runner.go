// Copyright (C) MongoDB, Inc. 2017-present.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

package unified

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"strings"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/internal/assert"
	"go.mongodb.org/mongo-driver/v2/internal/integration/mtest"
	"go.mongodb.org/mongo-driver/v2/internal/spectest"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

var (
	skippedTests = map[string]string{
		// GODRIVER-1773: This test runs a "find" with limit=4 and batchSize=3. It expects batchSize values of three for
		// the "find" and one for the "getMore", but we send three for both.
		"A successful find event with a getmore and the server kills the cursor (<= 4.4)": "See GODRIVER-1773",

		// GODRIVER-2577: The following spec tests require canceling ops immediately, but the current logic clears pools
		// and cancels in-progress ops after two the heartbeat failures.
		"Connection pool clear uses interruptInUseConnections=true after monitor timeout":                      "Godriver clears after multiple timeout",
		"Error returned from connection pool clear with interruptInUseConnections=true is retryable":           "Godriver clears after multiple timeout",
		"Error returned from connection pool clear with interruptInUseConnections=true is retryable for write": "Godriver clears after multiple timeout",

		// TODO(GODRIVER-2843): Fix and unskip these test cases.
		"Find operation with snapshot":                                      "Test fails frequently. See GODRIVER-2843",
		"Write commands with snapshot session do not affect snapshot reads": "Test fails frequently. See GODRIVER-2843",

		// TODO(GODRIVER-3043): Avoid Appending Write/Read Concern in Atlas Search
		// Index Helper Commands.
		"dropSearchIndex ignores read and write concern":       "Sync GODRIVER-3074, but skip testing bug GODRIVER-3043",
		"listSearchIndexes ignores read and write concern":     "Sync GODRIVER-3074, but skip testing bug GODRIVER-3043",
		"updateSearchIndex ignores the read and write concern": "Sync GODRIVER-3074, but skip testing bug GODRIVER-3043",

		// TODO(DRIVERS-2829): Create CSOT Legacy Timeout Analogues and Compatibility Field
		"Reset server and pool after network timeout error during authentication": "Uses unsupported socketTimeoutMS",
		"Ignore network timeout error on find":                                    "Uses unsupported socketTimeoutMS",
		"A successful find with options":                                          "Uses unsupported maxTimeMS",
		"estimatedDocumentCount with maxTimeMS":                                   "Uses unsupported maxTimeMS",
		"supports configuring getMore maxTimeMS":                                  "Uses unsupported maxTimeMS",

		// TODO(GODRIVER-3137): Implement Gossip cluster time"
		"unpin after TransientTransactionError error on commit": "Implement GODRIVER-3137",

		// TODO(GODRIVER-3034): Drivers should unpin connections when ending a session
		"unpin on successful abort":                                   "Implement GODRIVER-3034",
		"unpin after non-transient error on abort":                    "Implement GODRIVER-3034",
		"unpin after TransientTransactionError error on abort":        "Implement GODRIVER-3034",
		"unpin when a new transaction is started":                     "Implement GODRIVER-3034",
		"unpin when a non-transaction write operation uses a session": "Implement GODRIVER-3034",
		"unpin when a non-transaction read operation uses a session":  "Implement GODRIVER-3034",

		// DRIVERS-2722: Setting "maxTimeMS" on a command that creates a cursor
		// also limits the lifetime of the cursor. That may be surprising to
		// users, so omit "maxTimeMS" from operations that return user-managed
		// cursors.
		"timeoutMS can be overridden for a find":                                               "maxTimeMS is disabled on find and aggregate. See DRIVERS-2722.",
		"timeoutMS can be configured for an operation - find on collection":                    "maxTimeMS is disabled on find and aggregate. See DRIVERS-2722.",
		"timeoutMS can be configured for an operation - aggregate on collection":               "maxTimeMS is disabled on find and aggregate. See DRIVERS-2722.",
		"timeoutMS can be configured for an operation - aggregate on database":                 "maxTimeMS is disabled on find and aggregate. See DRIVERS-2722.",
		"timeoutMS can be configured on a MongoClient - find on collection":                    "maxTimeMS is disabled on find and aggregate. See DRIVERS-2722.",
		"timeoutMS can be configured on a MongoClient - aggregate on collection":               "maxTimeMS is disabled on find and aggregate. See DRIVERS-2722.",
		"timeoutMS can be configured on a MongoClient - aggregate on database":                 "maxTimeMS is disabled on find and aggregate. See DRIVERS-2722.",
		"operation is retried multiple times for non-zero timeoutMS - find on collection":      "maxTimeMS is disabled on find and aggregate. See DRIVERS-2722.",
		"operation is retried multiple times for non-zero timeoutMS - aggregate on collection": "maxTimeMS is disabled on find and aggregate. See DRIVERS-2722.",
		"operation is retried multiple times for non-zero timeoutMS - aggregate on database":   "maxTimeMS is disabled on find and aggregate. See DRIVERS-2722.",
		"timeoutMS applied to find command":                                                    "maxTimeMS is disabled on find and aggregate. See DRIVERS-2722.",
		"timeoutMS applied to find":                                                            "maxTimeMS is disabled on find and aggregate. See DRIVERS-2722.",
		"timeoutMS is refreshed for getMore if maxAwaitTimeMS is not set":                      "maxTimeMS is disabled on find and aggregate. See DRIVERS-2722.",
		"timeoutMS is refreshed for getMore if maxAwaitTimeMS is set":                          "maxTimeMS is disabled on find and aggregate. See DRIVERS-2722.",
		"timeoutMS is refreshed for getMore - failure":                                         "maxTimeMS is disabled on find and aggregate. See DRIVERS-2722.",

		// GODRIVER-3473: the implementation of DRIVERS-2868 makes it clear that the
		// Go Driver does not correctly implement the following validation for
		// tailable awaitData cursors:
		//
		//     Drivers MUST error if this option is set, timeoutMS is set to a
		//     non-zero value, and maxAwaitTimeMS is greater than or equal to
		//     timeoutMS.
		//
		// Once GODRIVER-3473 is completed, we can continue running these tests.
		"error if maxAwaitTimeMS is equal to timeoutMS":         "Go Driver does not implement this behavior. See GODRIVER-3473",
		"error if maxAwaitTimeMS is greater than timeoutMS":     "Go Driver does not implement this behavior. See GODRIVER-3473",
		"apply remaining timeoutMS if less than maxAwaitTimeMS": "Go Driver does not implement this behavior. See GODRIVER-3473",
		"apply maxAwaitTimeMS if less than remaining timeout":   "Go Driver does not implement this behavior. See GODRIVER-3473",

		// DRIVERS-2953: This test requires that the driver sends a "getMore"
		// with "maxTimeMS" set. However, "getMore" can only include "maxTimeMS"
		// for tailable awaitData cursors. Including "maxTimeMS" on "getMore"
		// for any other cursor type results in a server error:
		//
		//  (BadValue) cannot set maxTimeMS on getMore command for a non-awaitData cursor
		//
		"Non-tailable cursor lifetime remaining timeoutMS applied to getMore if timeoutMode is unset": "maxTimeMS can't be set on a getMore. See DRIVERS-2953",
	}

	logMessageValidatorTimeout = 10 * time.Millisecond
	lowHeartbeatFrequency      = 500 * time.Millisecond
)

// TestCase holds and runs a unified spec test case
type TestCase struct {
	Description       string               `bson:"description"`
	RunOnRequirements []mtest.RunOnBlock   `bson:"runOnRequirements"`
	SkipReason        *string              `bson:"skipReason"`
	Operations        []*operation         `bson:"operations"`
	ExpectedEvents    []*expectedEvents    `bson:"expectEvents"`
	ExpectLogMessages []*clientLogMessages `bson:"expectLogMessages"`
	Outcome           []*collectionData    `bson:"outcome"`

	initialData     []*collectionData
	createEntities  []map[string]*entityOptions
	killAllSessions bool
	schemaVersion   string

	entities *EntityMap
	loopDone chan struct{}
}

func (tc *TestCase) performsDistinct() bool {
	return tc.performsOperation("distinct")
}

func (tc *TestCase) setsFailPoint() bool {
	return tc.performsOperation("failPoint")
}

func (tc *TestCase) startsTransaction() bool {
	return tc.performsOperation("startTransaction")
}

func (tc *TestCase) performsOperation(name string) bool {
	for _, op := range tc.Operations {
		if op.Name == name {
			return true
		}
	}
	return false
}

// TestFile holds the contents of a unified spec test file
type TestFile struct {
	Description       string                      `bson:"description"`
	SchemaVersion     string                      `bson:"schemaVersion"`
	RunOnRequirements []mtest.RunOnBlock          `bson:"runOnRequirements"`
	CreateEntities    []map[string]*entityOptions `bson:"createEntities"`
	InitialData       []*collectionData           `bson:"initialData"`
	TestCases         []*TestCase                 `bson:"tests"`
}

// runTestDirectory runs the files in the given directory, which must be in the unified spec format, with
// expectValidFail determining whether the tests should expect to pass or fail
func runTestDirectory(t *testing.T, directoryPath string, expectValidFail bool) {
	for _, filename := range spectest.FindJSONFilesInDir(t, directoryPath) {
		t.Run(filename, func(t *testing.T) {
			runTestFile(t, path.Join(directoryPath, filename), expectValidFail)
		})
	}
}

// runTestFile runs the tests in the given file, with expectValidFail determining whether the tests should expect to pass or fail
func runTestFile(t *testing.T, filepath string, expectValidFail bool, opts ...*Options) {
	content, err := ioutil.ReadFile(filepath)
	assert.Nil(t, err, "ReadFile error for file %q: %v", filepath, err)

	fileReqs, testCases := ParseTestFile(t, content, expectValidFail, opts...)

	mtOpts := mtest.NewOptions().
		RunOn(fileReqs...).
		CreateClient(false)
	mt := mtest.New(t, mtOpts)

	for _, testCase := range testCases {
		mtOpts := mtest.NewOptions().
			RunOn(testCase.RunOnRequirements...).
			CreateClient(false)

		mt.RunOpts(testCase.Description, mtOpts, func(mt *mtest.T) {
			// Skip CSOT spec tests when SKIP_CSOT_TESTS=true. In Evergreen, we
			// typically set that environment variable on Windows and macOS
			// because the CSOT spec tests are unreliable on those hosts.
			if os.Getenv("SKIP_CSOT_TESTS") == "true" && strings.Contains(filepath, "client-side-operations-timeout") {
				mt.Skip("Skipping CSOT spec test because SKIP_CSOT_TESTS=true")
			}

			defer func() {
				// catch panics from looking up elements and fail if it's unexpected
				if r := recover(); r != nil {
					if !expectValidFail {
						mt.Fatal(r)
					}
				}
			}()
			err := testCase.Run(mt)
			if expectValidFail {
				if err != nil {
					return
				}
				mt.Fatalf("expected test to error, got nil")
			}
			if err != nil {
				mt.Fatal(err)
			}
		})
	}
}

func parseTestFile(testJSON []byte, opts ...*Options) ([]mtest.RunOnBlock, []*TestCase, error) {
	var testFile TestFile
	if err := bson.UnmarshalExtJSON(testJSON, false, &testFile); err != nil {
		return nil, nil, err
	}

	op := NewOptions()
	for _, opt := range opts {
		if opt == nil {
			continue
		}
		if opt.RunKillAllSessions != nil {
			op.RunKillAllSessions = opt.RunKillAllSessions
		}
	}

	for _, testCase := range testFile.TestCases {
		testCase.initialData = testFile.InitialData
		testCase.createEntities = testFile.CreateEntities
		testCase.schemaVersion = testFile.SchemaVersion
		testCase.entities = newEntityMap()
		testCase.loopDone = make(chan struct{})
		testCase.killAllSessions = *op.RunKillAllSessions
	}

	return testFile.RunOnRequirements, testFile.TestCases, nil
}

// ParseTestFile create an array of TestCases from the testJSON json blob
func ParseTestFile(t *testing.T, testJSON []byte, expectValidFail bool, opts ...*Options) ([]mtest.RunOnBlock, []*TestCase) {
	t.Helper()

	runOnRequirements, testCases, err := parseTestFile(testJSON, opts...)

	if !expectValidFail {
		assert.NoError(t, err, "error parsing test file")
	}

	return runOnRequirements, testCases
}

// GetEntities returns a pointer to the EntityMap for the TestCase. This should not be called until after
// the test is run
func (tc *TestCase) GetEntities() *EntityMap {
	return tc.entities
}

// EndLoop will cause the runner to stop a loop operation if one is included in the test. If the test has finished
// running, this will panic
func (tc *TestCase) EndLoop() {
	tc.loopDone <- struct{}{}
}

// LoggerSkipper is passed to TestCase.Run to allow it to perform logging and skipping operations
type LoggerSkipper interface {
	Log(args ...interface{})
	Logf(format string, args ...interface{})
	Skip(args ...interface{})
	Skipf(format string, args ...interface{})
}

// skipTestError indicates that a test must be skipped because the runner cannot execute it (e.g. the test requires
// an operation or option that the driver does not support).
type skipTestError struct {
	reason string
}

// Error implements the error interface.
func (s skipTestError) Error() string {
	return fmt.Sprintf("test must be skipped: %q", s.reason)
}

func newSkipTestError(reason string) error {
	return &skipTestError{reason}
}

func isSkipTestError(err error) bool {
	return err != nil && strings.Contains(err.Error(), "test must be skipped")
}

// Run runs the TestCase and returns an error if it fails
func (tc *TestCase) Run(ls LoggerSkipper) error {
	if tc.SkipReason != nil {
		ls.Skipf("skipping for reason: %q", *tc.SkipReason)
	}
	if skipReason, ok := skippedTests[tc.Description]; ok {
		ls.Skipf("skipping due to known failure: %q", skipReason)
	}

	// Validate that we support the schema declared by the test file before attempting to use its contents.
	if err := checkSchemaVersion(tc.schemaVersion); err != nil {
		return fmt.Errorf("schema version %q not supported: %v", tc.schemaVersion, err)
	}

	testCtx := newTestContext(context.Background(), tc.entities, tc.ExpectLogMessages, tc.setsFailPoint())

	defer func() {
		// If anything fails while doing test cleanup, we only log the error because the actual test may have already
		// failed and that failure should be preserved.

		for _, err := range disableUntargetedFailPoints(testCtx) {
			ls.Log(err)
		}
		for _, err := range disableTargetedFailPoints(testCtx) {
			ls.Log(err)
		}
		for _, err := range entities(testCtx).close(testCtx) {
			ls.Log(err)
		}
		// Tests that started a transaction should terminate any sessions left open on the server. This is required even
		// if the test attempted to commit/abort the transaction because an abortTransaction command can fail if it's
		// sent to a mongos that isn't aware of the transaction.
		if tc.startsTransaction() && tc.killAllSessions {
			if err := terminateOpenSessions(context.Background()); err != nil {
				ls.Logf("error terminating open transactions after failed test: %v", err)
			}
		}

		close(tc.loopDone)
	}()

	// Set up collections based on the file-level initialData field.
	for _, collData := range tc.initialData {
		if err := collData.createCollection(testCtx); err != nil {
			return fmt.Errorf("error setting up collection %q: %v", collData.namespace(), err)
		}
	}

	// Set up entities based on the file-level createEntities field. For client entities, if the test will configure
	// a fail point, set a low heartbeatFrequencyMS value into the URI options map if one is not already present.
	// This speeds up recovery time for the client if the fail point forces the server to return a state change
	// error.
	for idx, entity := range tc.createEntities {
		for entityType, entityOptions := range entity {
			if entityType == "client" && hasOperationalFailpoint(testCtx) {
				entityOptions.setHeartbeatFrequencyMS(lowHeartbeatFrequency)
			}

			if err := tc.entities.addEntity(testCtx, entityType, entityOptions); err != nil {
				if isSkipTestError(err) {
					ls.Skip(err)
				}

				return fmt.Errorf("error creating entity at index %d: %v", idx, err)
			}
		}
	}

	// Work around SERVER-39704.
	if mtest.ClusterTopologyKind() == mtest.Sharded && tc.performsDistinct() {
		if err := performDistinctWorkaround(testCtx); err != nil {
			return fmt.Errorf("error performing \"distinct\" workaround: %v", err)
		}
	}

	for idx, operation := range tc.Operations {
		if err := operation.execute(testCtx, tc.loopDone); err != nil {
			if isSkipTestError(err) {
				ls.Skip(err)
			}

			return fmt.Errorf("error running operation %q at index %d: %v", operation.Name, idx, err)
		}
	}

	// Create a validator for log messages and start the workers that will
	// observe log messages as they occur operationally.
	logMessageValidator := newLogMessageValidator(tc)
	go startLogValidators(testCtx, logMessageValidator)

	for _, client := range tc.entities.clients() {
		client.stopListeningForEvents()
	}

	// One of the bulkWrite spec tests expects update and updateMany to be grouped together into a single batch,
	// but this isn't the case because of GODRIVER-1157. To work around this, we skip event verification for this test.
	// This guard should be removed when GODRIVER-1157 is done.
	if tc.Description != "BulkWrite on server that doesn't support arrayFilters with arrayFilters on second op" {
		for idx, expectedEvents := range tc.ExpectedEvents {
			if err := verifyEvents(testCtx, expectedEvents); err != nil {
				return fmt.Errorf("events verification failed at index %d: %v", idx, err)
			}
		}
	}

	for idx, collData := range tc.Outcome {
		if err := collData.verifyContents(testCtx); err != nil {
			return fmt.Errorf("error verifying outcome for collection %q at index %d: %v",
				collData.namespace(), idx, err)
		}
	}

	{
		// Create a context with a deadline to use for log message
		// validation. This will prevent any blocking from test cases
		// with N messages where only N - K (0 < K < N) messages are
		// observed.
		ctx, cancel := context.WithTimeout(testCtx, logMessageValidatorTimeout)
		defer cancel()

		// For each client, verify that all expected log messages were
		// received.
		if err := stopLogValidators(ctx, logMessageValidator); err != nil {
			return fmt.Errorf("error verifying log messages: %w", err)
		}
	}

	return nil
}

func disableUntargetedFailPoints(ctx context.Context) []error {
	var errs []error
	for fpName, client := range failPoints(ctx) {
		if err := disableFailPointWithClient(ctx, fpName, client); err != nil {
			errs = append(errs, fmt.Errorf("error disabling fail point %q: %v", fpName, err))
		}
	}
	return errs
}

func disableTargetedFailPoints(ctx context.Context) []error {
	var errs []error
	for fpName, host := range targetedFailPoints(ctx) {
		commandFn := func(ctx context.Context, client *mongo.Client) error {
			return disableFailPointWithClient(ctx, fpName, client)
		}
		if err := runCommandOnHost(ctx, host, commandFn); err != nil {
			errs = append(errs, fmt.Errorf("error disabling targeted fail point %q on host %q: %v", fpName, host, err))
		}
	}
	return errs
}

func disableFailPointWithClient(ctx context.Context, fpName string, client *mongo.Client) error {
	cmd := bson.D{
		{"configureFailPoint", fpName},
		{"mode", "off"},
	}
	return client.Database("admin").RunCommand(ctx, cmd).Err()
}
