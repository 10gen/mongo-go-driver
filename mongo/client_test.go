// Copyright (C) MongoDB, Inc. 2017-present.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

package mongo

import (
	"context"
	"errors"
	"math"
	"os"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/event"
	"go.mongodb.org/mongo-driver/v2/internal/assert"
	"go.mongodb.org/mongo-driver/v2/internal/integtest"
	"go.mongodb.org/mongo-driver/v2/internal/require"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readconcern"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"
	"go.mongodb.org/mongo-driver/v2/mongo/writeconcern"
	"go.mongodb.org/mongo-driver/v2/tag"
	"go.mongodb.org/mongo-driver/v2/x/mongo/driver/mongocrypt"
	"go.mongodb.org/mongo-driver/v2/x/mongo/driver/topology"
)

var bgCtx = context.Background()

func setupClient(opts ...*options.ClientOptions) *Client {
	if len(opts) == 0 {
		clientOpts := options.Client().ApplyURI("mongodb://localhost:27017")
		integtest.AddTestServerAPIVersion(clientOpts)
		opts = append(opts, clientOpts)
	}
	client, _ := Connect(opts...)
	return client
}

func TestClient(t *testing.T) {
	t.Run("new client", func(t *testing.T) {
		client := setupClient()
		assert.NotNil(t, client.deployment, "expected valid deployment, got nil")
	})
	t.Run("database", func(t *testing.T) {
		dbName := "foo"
		client := setupClient()
		db := client.Database(dbName)
		assert.Equal(t, dbName, db.Name(), "expected db name %v, got %v", dbName, db.Name())
		assert.Equal(t, client, db.Client(), "expected client %v, got %v", client, db.Client())
	})
	t.Run("replaceErrors for disconnected topology", func(t *testing.T) {
		client := setupClient()

		topo, ok := client.deployment.(*topology.Topology)
		require.True(t, ok, "client deployment is not a topology")

		err := topo.Disconnect(context.Background())
		require.NoError(t, err)

		_, err = client.ListDatabases(bgCtx, bson.D{})
		assert.Equal(t, ErrClientDisconnected, err, "expected error %v, got %v", ErrClientDisconnected, err)

		err = client.Ping(bgCtx, nil)
		assert.Equal(t, ErrClientDisconnected, err, "expected error %v, got %v", ErrClientDisconnected, err)

		err = client.Disconnect(bgCtx)
		assert.Equal(t, ErrClientDisconnected, err, "expected error %v, got %v", ErrClientDisconnected, err)

		_, err = client.Watch(bgCtx, []bson.D{})
		assert.Equal(t, ErrClientDisconnected, err, "expected error %v, got %v", ErrClientDisconnected, err)
	})
	t.Run("nil document error", func(t *testing.T) {
		client := setupClient()

		_, err := client.Watch(bgCtx, nil)
		watchErr := errors.New("can only marshal slices and arrays into aggregation pipelines, but got invalid")
		assert.Equal(t, watchErr, err, "expected error %v, got %v", watchErr, err)

		_, err = client.ListDatabases(bgCtx, nil)
		assert.True(t, errors.Is(err, ErrNilDocument), "expected error %v, got %v", ErrNilDocument, err)

		_, err = client.ListDatabaseNames(bgCtx, nil)
		assert.True(t, errors.Is(err, ErrNilDocument), "expected error %v, got %v", ErrNilDocument, err)
	})
	t.Run("read preference", func(t *testing.T) {
		t.Run("absent", func(t *testing.T) {
			client := setupClient()
			gotMode := client.readPreference.Mode()
			wantMode := readpref.PrimaryMode
			assert.Equal(t, gotMode, wantMode, "expected mode %v, got %v", wantMode, gotMode)
			_, flag := client.readPreference.MaxStaleness()
			assert.False(t, flag, "expected max staleness to not be set but was")
		})
		t.Run("specified", func(t *testing.T) {
			tags := []tag.Set{
				{
					tag.Tag{
						Name:  "one",
						Value: "1",
					},
				},
				{
					tag.Tag{
						Name:  "two",
						Value: "2",
					},
				},
			}
			cs := "mongodb://localhost:27017/"
			cs += "?readpreference=secondary&readPreferenceTags=one:1&readPreferenceTags=two:2&maxStaleness=5"

			client := setupClient(options.Client().ApplyURI(cs))
			gotMode := client.readPreference.Mode()
			assert.Equal(t, gotMode, readpref.SecondaryMode, "expected mode %v, got %v", readpref.SecondaryMode, gotMode)
			gotTags := client.readPreference.TagSets()
			assert.Equal(t, gotTags, tags, "expected tags %v, got %v", tags, gotTags)
			gotStaleness, flag := client.readPreference.MaxStaleness()
			assert.True(t, flag, "expected max staleness to be set but was not")
			wantStaleness := time.Duration(5) * time.Second
			assert.Equal(t, gotStaleness, wantStaleness, "expected staleness %v, got %v", wantStaleness, gotStaleness)
		})
	})
	t.Run("localThreshold", func(t *testing.T) {
		testCases := []struct {
			name              string
			opts              *options.ClientOptions
			expectedThreshold time.Duration
		}{
			{"default", options.Client(), defaultLocalThreshold},
			{"custom", options.Client().SetLocalThreshold(10 * time.Second), 10 * time.Second},
		}
		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				client := setupClient(tc.opts)
				assert.Equal(t, tc.expectedThreshold, client.localThreshold,
					"expected localThreshold %v, got %v", tc.expectedThreshold, client.localThreshold)
			})
		}
	})
	t.Run("read concern", func(t *testing.T) {
		rc := readconcern.Majority()
		client := setupClient(options.Client().SetReadConcern(rc))
		assert.Equal(t, rc, client.readConcern, "expected read concern %v, got %v", rc, client.readConcern)
	})
	t.Run("min pool size from Set*PoolSize()", func(t *testing.T) {
		testCases := []struct {
			name string
			opts *options.ClientOptions
			err  error
		}{
			{
				name: "minPoolSize < default maxPoolSize",
				opts: options.Client().SetMinPoolSize(64),
				err:  nil,
			},
			{
				name: "minPoolSize > default maxPoolSize",
				opts: options.Client().SetMinPoolSize(128),
				err:  errors.New("minPoolSize must be less than or equal to maxPoolSize, got minPoolSize=128 maxPoolSize=100"),
			},
			{
				name: "minPoolSize < maxPoolSize",
				opts: options.Client().SetMinPoolSize(128).SetMaxPoolSize(256),
				err:  nil,
			},
			{
				name: "minPoolSize == maxPoolSize",
				opts: options.Client().SetMinPoolSize(128).SetMaxPoolSize(128),
				err:  nil,
			},
			{
				name: "minPoolSize > maxPoolSize",
				opts: options.Client().SetMinPoolSize(64).SetMaxPoolSize(32),
				err:  errors.New("minPoolSize must be less than or equal to maxPoolSize, got minPoolSize=64 maxPoolSize=32"),
			},
			{
				name: "maxPoolSize == 0",
				opts: options.Client().SetMinPoolSize(128).SetMaxPoolSize(0),
				err:  nil,
			},
		}
		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				_, err := newClient(tc.opts)
				assert.Equal(t, tc.err, err, "expected error %v, got %v", tc.err, err)
			})
		}
	})
	t.Run("min pool size from ApplyURI()", func(t *testing.T) {
		testCases := []struct {
			name string
			opts *options.ClientOptions
			err  error
		}{
			{
				name: "minPoolSize < default maxPoolSize",
				opts: options.Client().ApplyURI("mongodb://localhost:27017/?minPoolSize=64"),
				err:  nil,
			},
			{
				name: "minPoolSize > default maxPoolSize",
				opts: options.Client().ApplyURI("mongodb://localhost:27017/?minPoolSize=128"),
				err:  errors.New("minPoolSize must be less than or equal to maxPoolSize, got minPoolSize=128 maxPoolSize=100"),
			},
			{
				name: "minPoolSize < maxPoolSize",
				opts: options.Client().ApplyURI("mongodb://localhost:27017/?minPoolSize=128&maxPoolSize=256"),
				err:  nil,
			},
			{
				name: "minPoolSize == maxPoolSize",
				opts: options.Client().ApplyURI("mongodb://localhost:27017/?minPoolSize=128&maxPoolSize=128"),
				err:  nil,
			},
			{
				name: "minPoolSize > maxPoolSize",
				opts: options.Client().ApplyURI("mongodb://localhost:27017/?minPoolSize=64&maxPoolSize=32"),
				err:  errors.New("minPoolSize must be less than or equal to maxPoolSize, got minPoolSize=64 maxPoolSize=32"),
			},
			{
				name: "maxPoolSize == 0",
				opts: options.Client().ApplyURI("mongodb://localhost:27017/?minPoolSize=128&maxPoolSize=0"),
				err:  nil,
			},
		}
		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				_, err := newClient(tc.opts)
				assert.Equal(t, tc.err, err, "expected error %v, got %v", tc.err, err)
			})
		}
	})
	t.Run("retry writes", func(t *testing.T) {
		retryWritesURI := "mongodb://localhost:27017/?retryWrites=false"
		retryWritesErrorURI := "mongodb://localhost:27017/?retryWrites=foobar"

		testCases := []struct {
			name          string
			opts          *options.ClientOptions
			expectErr     bool
			expectedRetry bool
		}{
			{"default", options.Client(), false, true},
			{"custom options", options.Client().SetRetryWrites(false), false, false},
			{"custom URI", options.Client().ApplyURI(retryWritesURI), false, false},
			{"custom URI error", options.Client().ApplyURI(retryWritesErrorURI), true, false},
		}
		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				client, err := newClient(tc.opts)
				if tc.expectErr {
					assert.NotNil(t, err, "expected error, got nil")
					return
				}
				assert.Nil(t, err, "configuration error: %v", err)
				assert.Equal(t, tc.expectedRetry, client.retryWrites, "expected retryWrites %v, got %v",
					tc.expectedRetry, client.retryWrites)
			})
		}
	})
	t.Run("retry reads", func(t *testing.T) {
		retryReadsURI := "mongodb://localhost:27017/?retryReads=false"
		retryReadsErrorURI := "mongodb://localhost:27017/?retryReads=foobar"

		testCases := []struct {
			name          string
			opts          *options.ClientOptions
			expectErr     bool
			expectedRetry bool
		}{
			{"default", options.Client(), false, true},
			{"custom options", options.Client().SetRetryReads(false), false, false},
			{"custom URI", options.Client().ApplyURI(retryReadsURI), false, false},
			{"custom URI error", options.Client().ApplyURI(retryReadsErrorURI), true, false},
		}
		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				client, err := newClient(tc.opts)
				if tc.expectErr {
					assert.NotNil(t, err, "expected error, got nil")
					return
				}
				assert.Nil(t, err, "configuration error: %v", err)
				assert.Equal(t, tc.expectedRetry, client.retryReads, "expected retryReads %v, got %v",
					tc.expectedRetry, client.retryReads)
			})
		}
	})
	t.Run("write concern", func(t *testing.T) {
		wc := writeconcern.Majority()
		client := setupClient(options.Client().SetWriteConcern(wc))
		assert.Equal(t, wc, client.writeConcern, "mismatch; expected write concern %v, got %v", wc, client.writeConcern)
	})
	t.Run("server monitor", func(t *testing.T) {
		monitor := &event.ServerMonitor{}
		client := setupClient(options.Client().SetServerMonitor(monitor))
		assert.Equal(t, monitor, client.serverMonitor, "expected sdam monitor %v, got %v", monitor, client.serverMonitor)
	})
	t.Run("GetURI", func(t *testing.T) {
		t.Run("ApplyURI not called", func(t *testing.T) {
			opts := options.Client().SetHosts([]string{"localhost:27017"})
			uri := opts.GetURI()
			assert.Equal(t, "", uri, "expected GetURI to return empty string, got %v", uri)
		})
		t.Run("ApplyURI called with empty string", func(t *testing.T) {
			opts := options.Client().ApplyURI("")

			uri := opts.GetURI()
			assert.Equal(t, "", uri, "expected GetURI to return empty string, got %v", uri)
		})
		t.Run("ApplyURI called with non-empty string", func(t *testing.T) {
			uri := "mongodb://localhost:27017/foobar"
			opts := options.Client().ApplyURI(uri)

			got := opts.GetURI()

			assert.Equal(t, uri, got, "expected GetURI to return %v, got %v", uri, got)
		})
	})
	t.Run("endSessions", func(t *testing.T) {
		cs := integtest.ConnString(t)
		originalBatchSize := endSessionsBatchSize
		endSessionsBatchSize = 2
		defer func() {
			endSessionsBatchSize = originalBatchSize
		}()

		testCases := []struct {
			name            string
			numSessions     int
			eventBatchSizes []int
		}{
			{"number of sessions divides evenly", endSessionsBatchSize * 2, []int{endSessionsBatchSize, endSessionsBatchSize}},
			{"number of sessions does not divide evenly", endSessionsBatchSize + 1, []int{endSessionsBatchSize, 1}},
		}
		for _, tc := range testCases {
			if testing.Short() {
				t.Skip("skipping integration test in short mode")
			}
			if os.Getenv("DOCKER_RUNNING") != "" {
				t.Skip("skipping test in docker environment")
			}

			t.Run(tc.name, func(t *testing.T) {
				// Setup a client and skip the test based on server version.
				var started []*event.CommandStartedEvent
				var failureReasons []error
				cmdMonitor := &event.CommandMonitor{
					Started: func(_ context.Context, evt *event.CommandStartedEvent) {
						if evt.CommandName == "endSessions" {
							started = append(started, evt)
						}
					},
					Failed: func(_ context.Context, evt *event.CommandFailedEvent) {
						if evt.CommandName == "endSessions" {
							failureReasons = append(failureReasons, evt.Failure)
						}
					},
				}
				clientOpts := options.Client().ApplyURI(cs.Original).SetReadPreference(readpref.Primary()).
					SetWriteConcern(writeconcern.Majority()).SetMonitor(cmdMonitor)
				integtest.AddTestServerAPIVersion(clientOpts)
				client, err := Connect(clientOpts)
				assert.Nil(t, err, "Connect error: %v", err)
				defer func() {
					_ = client.Disconnect(bgCtx)
				}()

				serverVersion, err := getServerVersion(client.Database("admin"))
				assert.Nil(t, err, "getServerVersion error: %v", err)
				if compareVersions(serverVersion, "3.6.0") < 1 {
					t.Skip("skipping server version < 3.6")
				}

				coll := client.Database("foo").Collection("bar")
				defer func() {
					_ = coll.Drop(bgCtx)
				}()

				// Do an application operation and create the number of sessions specified by the test.
				_, err = coll.CountDocuments(bgCtx, bson.D{})
				assert.Nil(t, err, "CountDocuments error: %v", err)
				var sessions []*Session
				for i := 0; i < tc.numSessions; i++ {
					sess, err := client.StartSession()
					assert.Nil(t, err, "StartSession error at index %d: %v", i, err)
					sessions = append(sessions, sess)
				}
				for _, sess := range sessions {
					sess.EndSession(bgCtx)
				}

				client.endSessions(bgCtx)
				divisionResult := float64(tc.numSessions) / float64(endSessionsBatchSize)
				numEventsExpected := int(math.Ceil(divisionResult))
				assert.Equal(t, len(started), numEventsExpected, "expected %d started events, got %d", numEventsExpected,
					len(started))
				assert.Equal(t, len(failureReasons), 0, "endSessions errors: %v", failureReasons)

				for i := 0; i < numEventsExpected; i++ {
					sentArray := started[i].Command.Lookup("endSessions").Array()
					values, _ := sentArray.Values()
					expectedNumValues := tc.eventBatchSizes[i]
					assert.Equal(t, len(values), expectedNumValues,
						"batch size mismatch at index %d; expected %d sessions in batch, got %d", i, expectedNumValues,
						len(values))
				}
			})
		}
	})
	t.Run("serverAPI version", func(t *testing.T) {
		getServerAPIOptions := func() *options.ServerAPIOptions {
			return options.ServerAPI(options.ServerAPIVersion1).
				SetStrict(false).SetDeprecationErrors(false)
		}

		t.Run("success with all options", func(t *testing.T) {
			serverAPIOptions := getServerAPIOptions()
			client, err := newClient(options.Client().SetServerAPIOptions(serverAPIOptions))
			assert.Nil(t, err, "unexpected error from NewClient: %v", err)
			convertedAPIOptions := topology.ConvertToDriverAPIOptions(serverAPIOptions)
			assert.Equal(t, convertedAPIOptions, client.serverAPI,
				"mismatch in serverAPI; expected %v, got %v", convertedAPIOptions, client.serverAPI)
		})
		t.Run("failure with unsupported version", func(t *testing.T) {
			serverAPIOptions := options.ServerAPI("badVersion")
			_, err := newClient(options.Client().SetServerAPIOptions(serverAPIOptions))
			assert.NotNil(t, err, "expected error from NewClient, got nil")
			errmsg := `api version "badVersion" not supported; this driver version only supports API version "1"`
			assert.Equal(t, errmsg, err.Error(), "expected error %v, got %v", errmsg, err.Error())
		})
		t.Run("cannot modify options after client creation", func(t *testing.T) {
			serverAPIOptions := getServerAPIOptions()
			client, err := newClient(options.Client().SetServerAPIOptions(serverAPIOptions))
			assert.Nil(t, err, "unexpected error from NewClient: %v", err)

			expectedServerAPIOptions := getServerAPIOptions()
			// modify passed-in options
			serverAPIOptions.SetStrict(true).SetDeprecationErrors(true)
			convertedAPIOptions := topology.ConvertToDriverAPIOptions(expectedServerAPIOptions)
			assert.Equal(t, convertedAPIOptions, client.serverAPI,
				"unexpected modification to serverAPI; expected %v, got %v", convertedAPIOptions, client.serverAPI)
		})
	})
	t.Run("mongocryptd or crypt_shared", func(t *testing.T) {
		cryptSharedLibPath := os.Getenv("CRYPT_SHARED_LIB_PATH")
		if cryptSharedLibPath == "" {
			t.Skip("CRYPT_SHARED_LIB_PATH not set, skipping")
		}
		if len(mongocrypt.Version()) == 0 {
			t.Skip("Not built with cse flag")
		}

		testCases := []struct {
			description       string
			useCryptSharedLib bool
		}{
			{
				description:       "when crypt_shared is loaded, should not attempt to spawn mongocryptd",
				useCryptSharedLib: true,
			},
			{
				description:       "when crypt_shared is not loaded, should attempt to spawn mongocryptd",
				useCryptSharedLib: false,
			},
		}
		for _, tc := range testCases {
			t.Run(tc.description, func(t *testing.T) {
				extraOptions := map[string]interface{}{
					// Set a mongocryptd path that does not exist. If Connect() attempts to start
					// mongocryptd, it will cause an error.
					"mongocryptdPath": "/does/not/exist",
				}

				// If we're using the crypt_shared library, set the "cryptSharedLibRequired" option
				// to true and the "cryptSharedLibPath" option to the crypt_shared library path from
				// the CRYPT_SHARED_LIB_PATH environment variable. If we're not using the
				// crypt_shared library, explicitly disable loading the crypt_shared library.
				if tc.useCryptSharedLib {
					extraOptions["cryptSharedLibRequired"] = true
					extraOptions["cryptSharedLibPath"] = cryptSharedLibPath
				} else {
					extraOptions["__cryptSharedLibDisabledForTestOnly"] = true
				}

				_, err := newClient(options.Client().
					SetAutoEncryptionOptions(options.AutoEncryption().
						SetKmsProviders(map[string]map[string]interface{}{
							"local": {"key": make([]byte, 96)},
						}).
						SetExtraOptions(extraOptions)))

				// If we're using the crypt_shared library, expect that Connect() doesn't attempt to spawn
				// mongocryptd and no error is returned. If we're not using the crypt_shared library,
				// expect that Connect() tries to spawn mongocryptd and returns an error.
				if tc.useCryptSharedLib {
					assert.Nil(t, err, "Connect() error: %v", err)
				} else {
					assert.NotNil(t, err, "expected Connect() error, but got nil")
				}
			})
		}
	})
	t.Run("negative timeout will err", func(t *testing.T) {
		t.Parallel()

		copts := options.Client().SetTimeout(-1 * time.Second)
		_, err := Connect(copts)

		errmsg := `invalid value "-1s" for "Timeout": value must be positive`
		assert.Equal(t, errmsg, err.Error(), "expected error %v, got %v", errmsg, err.Error())
	})
}
