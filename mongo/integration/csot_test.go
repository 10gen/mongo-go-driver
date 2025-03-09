// Copyright (C) MongoDB, Inc. 2024-present.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

package integration

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/event"
	"go.mongodb.org/mongo-driver/internal/assert"
	"go.mongodb.org/mongo-driver/internal/eventtest"
	"go.mongodb.org/mongo-driver/internal/ptrutil"
	"go.mongodb.org/mongo-driver/internal/require"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/integration/mtest"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/x/bsonx/bsoncore"
	"go.mongodb.org/mongo-driver/x/mongo/driver"
)

// Test automatic "maxTimeMS" appending and connection closing behavior when
// CSOT is disabled and enabled.
func TestCSOT_maxTimeMS(t *testing.T) {
	// Skip CSOT tests when SKIP_CSOT_TESTS=true. In Evergreen, we typically set
	// that environment variable on Windows and macOS because the CSOT spec
	// tests are unreliable on those hosts.
	if os.Getenv("SKIP_CSOT_TESTS") == "true" {
		t.Skip("Skipping CSOT test because SKIP_CSOT_TESTS=true")
	}

	mt := mtest.New(t, mtest.NewOptions().CreateClient(false))

	testCases := []struct {
		desc        string
		commandName string
		operation   func(ctx context.Context, coll *mongo.Collection) error
		topologies  []mtest.TopologyKind

		// sendsMaxTimeMSWithTimeoutMS specifies whether the driver
		// automatically adds "maxTimeMS" to the command-under-test when
		// "timeoutMS" is set but no context deadline is provided.
		sendsMaxTimeMSWithTimeoutMS bool

		// sendsMaxTimeMSWithContextDeadline specifies whether the driver
		// automatically adds "maxTimeMS" to the command-under-test when
		// "timeoutMS" is set and a context deadline is provided.
		sendsMaxTimeMSWithContextDeadline bool

		// preventsConnClosureWithTimeoutMS specifies whether the driver
		// attempts to prevent closing connections when "timeoutMS" is set for
		// the command-under-test.
		preventsConnClosureWithTimeoutMS bool
	}{
		{
			desc:        "FindOne",
			commandName: "find",
			operation: func(ctx context.Context, coll *mongo.Collection) error {
				return coll.FindOne(ctx, bson.D{}).Err()
			},
			sendsMaxTimeMSWithTimeoutMS:       true,
			sendsMaxTimeMSWithContextDeadline: true,
			preventsConnClosureWithTimeoutMS:  true,
		},
		{
			desc:        "Find",
			commandName: "find",
			operation: func(ctx context.Context, coll *mongo.Collection) error {
				_, err := coll.Find(ctx, bson.D{})
				return err
			},
			sendsMaxTimeMSWithTimeoutMS:       true,
			sendsMaxTimeMSWithContextDeadline: false,
			preventsConnClosureWithTimeoutMS:  true,
		},
		{
			desc:        "FindOneAndDelete",
			commandName: "findAndModify",
			operation: func(ctx context.Context, coll *mongo.Collection) error {
				return coll.FindOneAndDelete(ctx, bson.D{}).Err()
			},
			sendsMaxTimeMSWithTimeoutMS:       true,
			sendsMaxTimeMSWithContextDeadline: true,
			preventsConnClosureWithTimeoutMS:  true,
		},
		{
			desc:        "FindOneAndUpdate",
			commandName: "findAndModify",
			operation: func(ctx context.Context, coll *mongo.Collection) error {
				return coll.FindOneAndUpdate(ctx, bson.D{}, bson.M{"$set": bson.M{"key": "value"}}).Err()
			},
			sendsMaxTimeMSWithTimeoutMS:       true,
			sendsMaxTimeMSWithContextDeadline: true,
			preventsConnClosureWithTimeoutMS:  true,
		},
		{
			desc:        "FindOneAndReplace",
			commandName: "findAndModify",
			operation: func(ctx context.Context, coll *mongo.Collection) error {
				return coll.FindOneAndReplace(ctx, bson.D{}, bson.D{}).Err()
			},
			sendsMaxTimeMSWithTimeoutMS:       true,
			sendsMaxTimeMSWithContextDeadline: true,
			preventsConnClosureWithTimeoutMS:  true,
		},
		{
			desc:        "InsertOne",
			commandName: "insert",
			operation: func(ctx context.Context, coll *mongo.Collection) error {
				_, err := coll.InsertOne(ctx, bson.D{})
				return err
			},
			sendsMaxTimeMSWithTimeoutMS:       true,
			sendsMaxTimeMSWithContextDeadline: true,
			preventsConnClosureWithTimeoutMS:  true,
		},
		{
			desc:        "InsertMany",
			commandName: "insert",
			operation: func(ctx context.Context, coll *mongo.Collection) error {
				_, err := coll.InsertMany(ctx, []interface{}{bson.D{}})
				return err
			},
			sendsMaxTimeMSWithTimeoutMS:       true,
			sendsMaxTimeMSWithContextDeadline: true,
			preventsConnClosureWithTimeoutMS:  true,
		},
		{
			desc:        "UpdateOne",
			commandName: "update",
			operation: func(ctx context.Context, coll *mongo.Collection) error {
				_, err := coll.UpdateOne(ctx, bson.D{}, bson.M{"$set": bson.M{"key": "value"}})
				return err
			},
			sendsMaxTimeMSWithTimeoutMS:       true,
			sendsMaxTimeMSWithContextDeadline: true,
			preventsConnClosureWithTimeoutMS:  true,
		},
		{
			desc:        "UpdateMany",
			commandName: "update",
			operation: func(ctx context.Context, coll *mongo.Collection) error {
				_, err := coll.UpdateMany(ctx, bson.D{}, bson.M{"$set": bson.M{"key": "value"}})
				return err
			},
			sendsMaxTimeMSWithTimeoutMS:       true,
			sendsMaxTimeMSWithContextDeadline: true,
			preventsConnClosureWithTimeoutMS:  true,
		},
		{
			desc:        "ReplaceOne",
			commandName: "update",
			operation: func(ctx context.Context, coll *mongo.Collection) error {
				_, err := coll.ReplaceOne(ctx, bson.D{}, bson.D{})
				return err
			},
			sendsMaxTimeMSWithTimeoutMS:       true,
			sendsMaxTimeMSWithContextDeadline: true,
			preventsConnClosureWithTimeoutMS:  true,
		},
		{
			desc:        "DeleteOne",
			commandName: "delete",
			operation: func(ctx context.Context, coll *mongo.Collection) error {
				_, err := coll.DeleteOne(ctx, bson.D{})
				return err
			},
			sendsMaxTimeMSWithTimeoutMS:       true,
			sendsMaxTimeMSWithContextDeadline: true,
			preventsConnClosureWithTimeoutMS:  true,
		},
		{
			desc:        "DeleteMany",
			commandName: "delete",
			operation: func(ctx context.Context, coll *mongo.Collection) error {
				_, err := coll.DeleteMany(ctx, bson.D{})
				return err
			},
			sendsMaxTimeMSWithTimeoutMS:       true,
			sendsMaxTimeMSWithContextDeadline: true,
			preventsConnClosureWithTimeoutMS:  true,
		},
		{
			desc:        "Distinct",
			commandName: "distinct",
			operation: func(ctx context.Context, coll *mongo.Collection) error {
				_, err := coll.Distinct(ctx, "name", bson.D{})
				return err
			},
			sendsMaxTimeMSWithTimeoutMS:       true,
			sendsMaxTimeMSWithContextDeadline: true,
			preventsConnClosureWithTimeoutMS:  true,
		},
		{
			desc:        "Aggregate",
			commandName: "aggregate",
			operation: func(ctx context.Context, coll *mongo.Collection) error {
				_, err := coll.Aggregate(ctx, mongo.Pipeline{})
				return err
			},
			sendsMaxTimeMSWithTimeoutMS:       true,
			sendsMaxTimeMSWithContextDeadline: false,
			preventsConnClosureWithTimeoutMS:  true,
		},
		{
			desc:        "Watch",
			commandName: "aggregate",
			operation: func(ctx context.Context, coll *mongo.Collection) error {
				cs, err := coll.Watch(ctx, mongo.Pipeline{})
				if cs != nil {
					cs.Close(context.Background())
				}
				return err
			},
			sendsMaxTimeMSWithTimeoutMS:       true,
			sendsMaxTimeMSWithContextDeadline: true,
			preventsConnClosureWithTimeoutMS:  true,
			// Change Streams aren't supported on standalone topologies.
			topologies: []mtest.TopologyKind{
				mtest.ReplicaSet,
				mtest.Sharded,
			},
		},
		{
			desc:        "Cursor getMore",
			commandName: "getMore",
			operation: func(ctx context.Context, coll *mongo.Collection) error {
				cursor, err := coll.Find(ctx, bson.D{}, options.Find().SetBatchSize(1))
				if err != nil {
					return err
				}
				var res []bson.D
				return cursor.All(ctx, &res)
			},
			sendsMaxTimeMSWithTimeoutMS:       false,
			sendsMaxTimeMSWithContextDeadline: false,
			preventsConnClosureWithTimeoutMS:  false,
		},
	}

	// insertTwoDocuments inserts two documents in the test collection.
	insertTwoDocuments := func(mt *mtest.T) {
		mt.Helper()

		_, err := mt.Coll.InsertMany(context.Background(), []interface{}{bson.D{}, bson.D{}})
		require.NoError(mt, err, "InsertMany error")
	}

	// getStartedEvent returns the first command started event that matches the
	// specified command name.
	getStartedEvent := func(mt *mtest.T, command string) *event.CommandStartedEvent {
		for {
			evt := mt.GetStartedEvent()
			if evt == nil {
				break
			}
			_, err := evt.Command.LookupErr(command)
			if errors.Is(err, bsoncore.ErrElementNotFound) {
				continue
			}
			return evt
		}

		mt.Errorf("could not find command started event for command %q", command)
		mt.FailNow()
		return nil
	}

	// getMaxTimeMS asserts that "maxTimeMS" is set on the command document for
	// the given command name and returns the value.
	getMaxTimeMS := func(mt *mtest.T, command string) int64 {
		mt.Helper()

		evt := getStartedEvent(mt, command)
		maxTimeVal := evt.Command.Lookup("maxTimeMS")
		if len(maxTimeVal.Value) == 0 {
			return -1
		}

		require.Greater(mt,
			len(maxTimeVal.Value),
			0,
			"expected maxTimeMS BSON value to be non-empty")
		require.Equal(mt,
			maxTimeVal.Type,
			bson.TypeInt64,
			"expected maxTimeMS BSON value to be type Int64")
		assert.Greater(mt,
			maxTimeVal.Int64(),
			int64(0),
			"expected maxTimeMS value to be greater than 0")

		return maxTimeVal.Int64()
	}

	// assertMaxTimeMSIsSet asserts that "maxTimeMS" is not set on the given
	// command document.
	assertMaxTimeMSNotSet := func(mt *mtest.T, command string) {
		mt.Helper()

		evt := getStartedEvent(mt, command)

		_, err := evt.Command.LookupErr("maxTimeMS")
		assert.ErrorIs(mt,
			err,
			bsoncore.ErrElementNotFound,
			"expected maxTimeMS BSON value to be missing, but is present")
	}

	for _, tc := range testCases {
		mt.RunOpts(tc.desc, mtest.NewOptions().Topologies(tc.topologies...), func(mt *mtest.T) {
			mt.Run("maxTimeMS", func(mt *mtest.T) {
				mt.Run("timeoutMS not set", func(mt *mtest.T) {
					// Insert some documents so the collection isn't empty.
					insertTwoDocuments(mt)

					err := tc.operation(context.Background(), mt.Coll)
					require.NoError(mt, err)
					assertMaxTimeMSNotSet(mt, tc.commandName)
				})

				csotOpts := mtest.NewOptions().ClientOptions(options.Client().SetTimeout(10 * time.Second))
				mt.RunOpts("timeoutMS and context.Background", csotOpts, func(mt *mtest.T) {
					// Insert some documents so the collection isn't empty.
					insertTwoDocuments(mt)

					err := tc.operation(context.Background(), mt.Coll)
					require.NoError(mt, err)

					if !tc.sendsMaxTimeMSWithTimeoutMS {
						assertMaxTimeMSNotSet(mt, tc.commandName)
						return
					}

					maxTimeMS := getMaxTimeMS(mt, tc.commandName)
					assert.Greater(mt, maxTimeMS, int64(0), "expected maxTimeMS to be greater than 0")
				})

				mt.RunOpts("timeoutMS and Context with deadline", csotOpts, func(mt *mtest.T) {
					// Insert some documents so the collection isn't empty.
					insertTwoDocuments(mt)

					ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
					defer cancel()

					err := tc.operation(ctx, mt.Coll)
					require.NoError(mt, err)

					if !tc.sendsMaxTimeMSWithContextDeadline {
						assertMaxTimeMSNotSet(mt, tc.commandName)
						return
					}

					maxTimeMS := getMaxTimeMS(mt, tc.commandName)
					assert.Greater(mt, maxTimeMS, int64(0), "expected maxTimeMS to be greater than 0")
				})
			})

			if tc.preventsConnClosureWithTimeoutMS {
				opts := mtest.NewOptions().
					// Blocking failpoints don't work on pre-4.2 and sharded clusters.
					Topologies(mtest.Single, mtest.ReplicaSet).
					MinServerVersion("4.2")
				mt.RunOpts("prevents connection closure with timeoutMS", opts, func(mt *mtest.T) {
					// Insert some documents so the collection isn't empty.
					insertTwoDocuments(mt)

					mt.SetFailPoint(mtest.FailPoint{
						ConfigureFailPoint: "failCommand",
						Mode:               "alwaysOn",
						Data: mtest.FailPointData{
							FailCommands:    []string{tc.commandName},
							BlockConnection: true,
							BlockTimeMS:     500,
						},
					})

					tpm := eventtest.NewTestPoolMonitor()
					mt.ResetClient(options.Client().
						SetPoolMonitor(tpm.PoolMonitor))

					// Run 5 operations that time out with CSOT disabled, then
					// assert that at least 1 connection was closed during those
					// timeouts.
					for i := 0; i < 5; i++ {
						ctx, cancel := context.WithTimeout(context.Background(), 15*time.Millisecond)
						err := tc.operation(ctx, mt.Coll)
						cancel()

						if !mongo.IsTimeout(err) {
							t.Errorf("CSOT-disabled operation %d returned a non-timeout error: %v", i, err)
						}
					}

					closedEvents := tpm.Events(func(pe *event.PoolEvent) bool {
						return pe.Type == event.ConnectionClosed
					})
					assert.Greater(mt,
						len(closedEvents),
						0,
						"expected more than 0 connection closed events")

					tpm = eventtest.NewTestPoolMonitor()
					mt.ResetClient(options.Client().
						SetPoolMonitor(tpm.PoolMonitor).
						SetTimeout(10 * time.Second))

					// Run 5 operations that time out with CSOT enabled, then
					// assert that no connections were closed.
					for i := 0; i < 5; i++ {
						ctx, cancel := context.WithTimeout(context.Background(), 15*time.Millisecond)
						err := tc.operation(ctx, mt.Coll)
						cancel()

						if !mongo.IsTimeout(err) {
							t.Errorf("CSOT-enabled operation %d returned a non-timeout error: %v", i, err)
						}
					}

					closedEvents = tpm.Events(func(pe *event.PoolEvent) bool {
						return pe.Type == event.ConnectionClosed
					})
					assert.Len(mt, closedEvents, 0, "expected no connection closed event")
				})
			}
		})
	}

	mt.Run("maxTimeMS is omitted for values greater than 2147483647ms", func(mt *mtest.T) {
		// Set a client-level timeoutMS value.
		mt.ResetClient(options.Client().SetTimeout(10 * time.Second))

		ctx, cancel := context.WithTimeout(context.Background(), (2147483647+1000)*time.Millisecond)
		defer cancel()
		_, err := mt.Coll.InsertOne(ctx, bson.D{})
		require.NoError(t, err)

		evt := mt.GetStartedEvent()
		_, err = evt.Command.LookupErr("maxTimeMS")
		assert.ErrorIs(mt,
			err,
			bsoncore.ErrElementNotFound,
			"expected maxTimeMS BSON value to be missing, but is present")
	})

	// Deprecated MaxTime option tests.
	mt.Run("Find uses MaxTime option when no other timeouts are set", func(mt *mtest.T) {
		// Insert some documents so the collection isn't empty.
		insertTwoDocuments(mt)

		// Set a 5-second MaxTime value.
		opts := options.Find().SetMaxTime(5 * time.Second)

		cursor, err := mt.Coll.Find(context.Background(), bson.D{}, opts)
		require.NoError(mt, err, "Find error")
		err = cursor.Close(context.Background())
		require.NoError(mt, err, "Cursor.Close error")

		// Assert that maxTimeMS is set and that it's equal to the MaxTime
		// value.
		maxTimeMS := getMaxTimeMS(mt, "find")
		assert.Equal(mt,
			int64(5_000),
			maxTimeMS,
			"expected maxTimeMS to be equal to the MaxTime value")
	})
	mt.Run("Find ignores MaxTime option when timeoutMS is set", func(mt *mtest.T) {
		// Insert some documents so the collection isn't empty.
		insertTwoDocuments(mt)

		// Set a 10-second client-level timeoutMS value .
		mt.ResetClient(options.Client().SetTimeout(10 * time.Second))

		// Set a 5-second MaxTime value.
		opts := options.Find().SetMaxTime(5 * time.Second)

		cursor, err := mt.Coll.Find(context.Background(), bson.D{}, opts)
		require.NoError(mt, err, "Find error")
		err = cursor.Close(context.Background())
		require.NoError(mt, err, "Cursor.Close error")

		// Assert that maxTimeMS is set and that it's greater than the
		// MaxTime value.
		maxTimeMS := getMaxTimeMS(mt, "find")
		assert.Greater(mt,
			maxTimeMS,
			int64(5_000),
			"expected maxTimeMS to be greater than the MaxTime value")
	})
	// TODO(GODRIVER-2944): Remove this test once the "timeoutMode" option is
	// supported.
	mt.Run("Find uses MaxTime option when timeoutMS and Context with deadline are set", func(mt *mtest.T) {
		// Insert some documents so the collection isn't empty.
		insertTwoDocuments(mt)

		// Set a 10-second client-level timeoutMS value .
		mt.ResetClient(options.Client().SetTimeout(10 * time.Second))

		// Set a 10-second operation-level Context timeout.
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		// Set a 5-second MaxTime value.
		opts := options.Find().SetMaxTime(5 * time.Second)

		cursor, err := mt.Coll.Find(ctx, bson.D{}, opts)
		require.NoError(mt, err, "Find error")
		err = cursor.Close(context.Background())
		require.NoError(mt, err, "Cursor.Close error")

		// Assert that maxTimeMS is set and that it's equal to the MaxTime
		// value.
		maxTimeMS := getMaxTimeMS(mt, "find")
		assert.Equal(mt,
			int64(5_000),
			maxTimeMS,
			"expected maxTimeMS to be equal to the MaxTime value")
	})
	mt.Run("Aggregate uses MaxTime option when no other timeouts are set", func(mt *mtest.T) {
		// Insert some documents so the collection isn't empty.
		insertTwoDocuments(mt)

		// Set a 5-second MaxTime value.
		opts := options.Aggregate().SetMaxTime(5 * time.Second)

		cursor, err := mt.Coll.Aggregate(context.Background(), bson.D{}, opts)
		require.NoError(mt, err, "Aggregate error")
		err = cursor.Close(context.Background())
		require.NoError(mt, err, "Cursor.Close error")

		// Assert that maxTimeMS is set and that it's equal to the MaxTime
		// value.
		maxTimeMS := getMaxTimeMS(mt, "aggregate")
		assert.Equal(mt,
			int64(5_000),
			maxTimeMS,
			"expected maxTimeMS to be equal to the MaxTime value")
	})
	mt.Run("Aggregate ignores MaxTime option when timeoutMS is set", func(mt *mtest.T) {
		// Insert some documents so the collection isn't empty.
		insertTwoDocuments(mt)

		// Set a 10-second client-level timeoutMS value .
		mt.ResetClient(options.Client().SetTimeout(10 * time.Second))

		// Set a 5-second MaxTime value.
		opts := options.Aggregate().SetMaxTime(5 * time.Second)

		cursor, err := mt.Coll.Aggregate(context.Background(), bson.D{}, opts)
		require.NoError(mt, err, "Aggregate error")
		err = cursor.Close(context.Background())
		require.NoError(mt, err, "Cursor.Close error")

		// Assert that maxTimeMS is set and that it's greater than the
		// MaxTime value.
		maxTimeMS := getMaxTimeMS(mt, "aggregate")
		assert.Greater(mt,
			maxTimeMS,
			int64(5_000),
			"expected maxTimeMS to be greater than the MaxTime value")
	})
	// TODO(GODRIVER-2944): Remove this test once the "timeoutMode" option is
	// supported.
	mt.Run("Aggregate uses MaxTime option when timeoutMS and Context with deadline are set", func(mt *mtest.T) {
		// Insert some documents so the collection isn't empty.
		insertTwoDocuments(mt)

		// Set a 10-second client-level timeoutMS value .
		mt.ResetClient(options.Client().SetTimeout(10 * time.Second))

		// Set a 10-second operation-level Context timeout.
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		// Set a 5-second MaxTime value.
		opts := options.Aggregate().SetMaxTime(5 * time.Second)

		cursor, err := mt.Coll.Aggregate(ctx, bson.D{}, opts)
		require.NoError(mt, err, "Aggregate error")
		err = cursor.Close(context.Background())
		require.NoError(mt, err, "Cursor.Close error")

		// Assert that maxTimeMS is set and that it's equal to the MaxTime
		// value.
		maxTimeMS := getMaxTimeMS(mt, "aggregate")
		assert.Equal(mt,
			int64(5_000),
			maxTimeMS,
			"expected maxTimeMS to be equal to the MaxTime value")
	})

	mt.Run("UnsafeAllowSeperateMaxTimeMSWithCSOT", func(mt *mtest.T) {
		ops := []struct {
			name        string
			commandName string
			fn          func(ctx context.Context, coll *mongo.Collection, maxTime *time.Duration) error
			cursorOp    bool
		}{
			{
				name:        "FindOne",
				commandName: "find",
				fn: func(ctx context.Context, coll *mongo.Collection, maxTime *time.Duration) error {
					opts := options.FindOne()
					opts.UnsafeAllowSeperateMaxTimeMS = true
					if maxTime != nil {
						opts.SetMaxTime(*maxTime)
					}
					res := coll.FindOne(ctx, bson.D{}, opts)
					return res.Err()
				},
				cursorOp: false,
			},
			//{
			//	name:        "Find",
			//	commandName: "find",
			//	fn: func(ctx context.Context, coll *mongo.Collection, maxTime *time.Duration) error {
			//		opts := options.Find()
			//		if maxTime != nil {
			//			opts.SetMaxTime(*maxTime)
			//		}
			//		_, err := coll.Find(ctx, bson.D{}, opts)
			//		return err
			//	},
			//	cursorOp: true,
			// },
			//{
			//	name:        "FindOneAndUpdate",
			//	commandName: "findAndModify",
			//	fn: func(ctx context.Context, coll *mongo.Collection, maxTime *time.Duration) error {
			//		opts := options.FindOneAndUpdate()
			//		if maxTime != nil {
			//			opts.SetMaxTime(*maxTime)
			//		}
			//		res := coll.FindOneAndUpdate(ctx, bson.D{}, bson.M{"$set": bson.M{"key": "value"}}, opts)
			//		return res.Err()
			//	},
			//	cursorOp: false,
			// },
			//{
			//	name:        "Aggregate",
			//	commandName: "aggregate",
			//	fn: func(ctx context.Context, coll *mongo.Collection, maxTime *time.Duration) error {
			//		opts := options.Aggregate()
			//		if maxTime != nil {
			//			opts.SetMaxTime(*maxTime)
			//		}
			//		_, err := coll.Aggregate(ctx, bson.D{}, opts)
			//		return err
			//	},
			//	cursorOp: true,
			// },
		}

		for _, op := range ops {
			mt.Run(op.name, func(mt *mtest.T) {
				testCases := []struct {
					name       string
					ctxTimeout *time.Duration
					maxTime    *time.Duration
					wantMS     int
					wantDelta  float64
				}{
					{
						name:       "CSOT with context deadline with maxTime",
						ctxTimeout: ptrutil.Ptr(10 * time.Second),
						maxTime:    ptrutil.Ptr(5 * time.Second),
						wantMS:     5_000,
						wantDelta:  0,
					},
					{
						name:       "CSOT with context deadline without maxTime",
						ctxTimeout: ptrutil.Ptr(10 * time.Second),
						maxTime:    nil,
						wantMS:     10_000,
						wantDelta:  500,
					},
					{
						name:       "CSOT without context deadline with maxTime",
						ctxTimeout: nil,
						maxTime:    ptrutil.Ptr(5 * time.Second),
						wantMS:     5_000,
						wantDelta:  0,
					},
					{
						name:       "CSOT without context deadline with maxTime",
						ctxTimeout: nil,
						maxTime:    nil,
						wantMS:     15_000,
						wantDelta:  500,
					},
				}

				for _, tc := range testCases {
					mt.Run(tc.name, func(mt *mtest.T) {
						// driver.UnsafeAllowSeperateMaxTimeMSWithCSOT = true
						// defer func() { driver.UnsafeAllowSeperateMaxTimeMSWithCSOT = false }()

						// Enable CSOT
						mt.ResetClient(options.Client().SetTimeout(15 * time.Second))

						var hasDeadline bool
						ctx := context.Background()
						if tc.ctxTimeout != nil {
							var cancel context.CancelFunc

							ctx, cancel = context.WithTimeout(ctx, *tc.ctxTimeout)
							defer cancel()

							hasDeadline = true
						}

						// Insert some documents so the collection isn't empty.
						insertTwoDocuments(mt)

						err := op.fn(ctx, mt.Coll, tc.maxTime)
						require.NoError(mt, err)

						// Assert that maxTimeMS is set and that it's equal to the MaxTime
						// value.
						maxTimeMS := getMaxTimeMS(mt, op.commandName)
						if op.cursorOp && tc.maxTime == nil && hasDeadline {
							assert.Equal(mt, int64(-1), maxTimeMS)
						} else {
							assert.InDelta(mt,
								tc.wantMS,
								maxTimeMS,
								tc.wantDelta,
								"expected maxTimeMS to be equal to the MaxTime value")
						}
					})
				}
			})
		}
	})
}

func TestCSOT_errors(t *testing.T) {
	// Skip CSOT tests when SKIP_CSOT_TESTS=true. In Evergreen, we typically set
	// that environment variable on Windows and macOS because the CSOT spec
	// tests are unreliable on those hosts.
	if os.Getenv("SKIP_CSOT_TESTS") == "true" {
		t.Skip("Skipping CSOT test because SKIP_CSOT_TESTS=true")
	}

	mt := mtest.New(t, mtest.NewOptions().
		CreateClient(false).
		// Blocking failpoints don't work on pre-4.2 and sharded clusters.
		Topologies(mtest.Single, mtest.ReplicaSet).
		MinServerVersion("4.2").
		// Enable CSOT.
		ClientOptions(options.Client().SetTimeout(10*time.Second)))

	// Test that, when CSOT is enabled, the error returned when the database
	// returns a MaxTimeMSExceeded error (error code 50) wraps
	// "context.DeadlineExceeded".
	mt.Run("MaxTimeMSExceeded wraps context.DeadlineExceeded", func(mt *mtest.T) {
		_, err := mt.Coll.InsertOne(context.Background(), bson.D{})
		require.NoError(mt, err, "InsertOne error")

		mt.SetFailPoint(mtest.FailPoint{
			ConfigureFailPoint: "failCommand",
			Mode: mtest.FailPointMode{
				Times: 1,
			},
			Data: mtest.FailPointData{
				FailCommands: []string{"find"},
				ErrorCode:    50, // MaxTimeMSExceeded
			},
		})

		err = mt.Coll.FindOne(context.Background(), bson.D{}).Err()

		assert.True(mt,
			errors.Is(err, context.DeadlineExceeded),
			"expected error %[1]T(%[1]q) to wrap context.DeadlineExceeded",
			err)
		assert.True(mt,
			mongo.IsTimeout(err),
			"expected error %[1]T(%[1]q) to be a timeout error",
			err)
	})

	// Test that, when CSOT is enabled, the error returned when a context
	// deadline is exceeded during a network operation wraps
	// "context.DeadlineExceeded".
	mt.Run("Context timeout wraps context.DeadlineExceeded", func(mt *mtest.T) {
		_, err := mt.Coll.InsertOne(context.Background(), bson.D{})
		require.NoError(mt, err, "InsertOne error")

		mt.SetFailPoint(mtest.FailPoint{
			ConfigureFailPoint: "failCommand",
			Mode: mtest.FailPointMode{
				Times: 1,
			},
			Data: mtest.FailPointData{
				FailCommands:    []string{"find"},
				BlockConnection: true,
				BlockTimeMS:     500,
			},
		})

		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Millisecond)
		defer cancel()
		err = mt.Coll.FindOne(ctx, bson.D{}).Err()

		assert.False(mt,
			errors.Is(err, driver.ErrDeadlineWouldBeExceeded),
			"expected error %[1]T(%[1]q) to not wrap driver.ErrDeadlineWouldBeExceeded",
			err)
		assert.True(mt,
			errors.Is(err, context.DeadlineExceeded),
			"expected error %[1]T(%[1]q) to wrap context.DeadlineExceeded",
			err)
		assert.True(mt,
			mongo.IsTimeout(err),
			"expected error %[1]T(%[1]q) to be a timeout error",
			err)
	})

	mt.Run("timeoutMS timeout wraps context.DeadlineExceeded", func(mt *mtest.T) {
		_, err := mt.Coll.InsertOne(context.Background(), bson.D{})
		require.NoError(mt, err, "InsertOne error")

		mt.SetFailPoint(mtest.FailPoint{
			ConfigureFailPoint: "failCommand",
			Mode: mtest.FailPointMode{
				Times: 1,
			},
			Data: mtest.FailPointData{
				FailCommands:    []string{"find"},
				BlockConnection: true,
				BlockTimeMS:     100,
			},
		})

		// Set timeoutMS=10 to run the FindOne, then unset it so the mtest
		// cleanup operations pass successfully (e.g. unsetting failpoints).
		mt.ResetClient(options.Client().SetTimeout(10 * time.Millisecond))
		defer mt.ResetClient(options.Client())
		err = mt.Coll.FindOne(context.Background(), bson.D{}).Err()

		assert.False(mt,
			errors.Is(err, driver.ErrDeadlineWouldBeExceeded),
			"expected error %[1]T(%[1]q) to not wrap driver.ErrDeadlineWouldBeExceeded",
			err)
		assert.True(mt,
			errors.Is(err, context.DeadlineExceeded),
			"expected error %[1]T(%[1]q) to wrap context.DeadlineExceeded",
			err)
		assert.True(mt,
			mongo.IsTimeout(err),
			"expected error %[1]T(%[1]q) to be a timeout error",
			err)
	})
}
