// Copyright (C) MongoDB, Inc. 2022-present.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

package driver

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/internal/assert"
	"go.mongodb.org/mongo-driver/v2/internal/csot"
	"go.mongodb.org/mongo-driver/v2/internal/handshake"
	"go.mongodb.org/mongo-driver/v2/internal/require"
	"go.mongodb.org/mongo-driver/v2/internal/uuid"
	"go.mongodb.org/mongo-driver/v2/mongo/address"
	"go.mongodb.org/mongo-driver/v2/mongo/readconcern"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"
	"go.mongodb.org/mongo-driver/v2/mongo/writeconcern"
	"go.mongodb.org/mongo-driver/v2/x/bsonx/bsoncore"
	"go.mongodb.org/mongo-driver/v2/x/mongo/driver/description"
	"go.mongodb.org/mongo-driver/v2/x/mongo/driver/mnet"
	"go.mongodb.org/mongo-driver/v2/x/mongo/driver/session"
	"go.mongodb.org/mongo-driver/v2/x/mongo/driver/wiremessage"
)

func noerr(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
		t.FailNow()
	}
}

func compareErrors(err1, err2 error) bool {
	if err1 == nil && err2 == nil {
		return true
	}

	if err1 == nil || err2 == nil {
		return false
	}

	if err1.Error() != err2.Error() {
		return false
	}

	return true
}

func TestOperation(t *testing.T) {
	int64ToPtr := func(i64 int64) *int64 { return &i64 }

	t.Run("selectServer", func(t *testing.T) {
		t.Run("returns validation error", func(t *testing.T) {
			op := &Operation{}
			_, err := op.selectServer(context.Background(), 1, nil)
			if err == nil {
				t.Error("Expected a validation error from selectServer, but got <nil>")
			}
		})
		t.Run("uses specified server selector", func(t *testing.T) {
			want := new(mockServerSelector)
			d := new(mockDeployment)
			op := &Operation{
				CommandFn:  func([]byte, description.SelectedServer) ([]byte, error) { return nil, nil },
				Deployment: d,
				Database:   "testing",
				Selector:   want,
			}
			_, err := op.selectServer(context.Background(), 1, nil)
			noerr(t, err)

			// Assert the selector is an operation selector wrapper.
			oss, ok := d.params.selector.(*opServerSelector)
			require.True(t, ok)

			if !cmp.Equal(oss.selector, want) {
				t.Errorf("Did not get expected server selector. got %v; want %v", oss.selector, want)
			}
		})
		t.Run("uses a default server selector", func(t *testing.T) {
			d := new(mockDeployment)
			op := &Operation{
				CommandFn:  func([]byte, description.SelectedServer) ([]byte, error) { return nil, nil },
				Deployment: d,
				Database:   "testing",
			}
			_, err := op.selectServer(context.Background(), 1, nil)
			noerr(t, err)
			if d.params.selector == nil {
				t.Error("The selectServer method should use a default selector when not specified on Operation, but it passed <nil>.")
			}
		})
	})
	t.Run("Validate", func(t *testing.T) {
		cmdFn := func([]byte, description.SelectedServer) ([]byte, error) { return nil, nil }
		d := new(mockDeployment)
		testCases := []struct {
			name string
			op   *Operation
			err  error
		}{
			{"CommandFn", &Operation{}, InvalidOperationError{MissingField: "CommandFn"}},
			{"Deployment", &Operation{CommandFn: cmdFn}, InvalidOperationError{MissingField: "Deployment"}},
			{"Database", &Operation{CommandFn: cmdFn, Deployment: d}, errDatabaseNameEmpty},
			{"<nil>", &Operation{CommandFn: cmdFn, Deployment: d, Database: "test"}, nil},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				if tc.op == nil {
					t.Fatal("op cannot be <nil>")
				}
				want := tc.err
				got := tc.op.Validate()
				if !cmp.Equal(got, want, cmp.Comparer(compareErrors)) {
					t.Errorf("Did not validate properly. got %v; want %v", got, want)
				}
			})
		}
	})
	t.Run("retryableWrite", func(t *testing.T) {
		sessPool := session.NewPool(nil)
		id, err := uuid.New()
		noerr(t, err)

		sess, err := session.NewClientSession(sessPool, id)
		noerr(t, err)

		sessStartingTransaction, err := session.NewClientSession(sessPool, id)
		noerr(t, err)
		err = sessStartingTransaction.StartTransaction(nil)
		noerr(t, err)

		sessInProgressTransaction, err := session.NewClientSession(sessPool, id)
		noerr(t, err)
		err = sessInProgressTransaction.StartTransaction(nil)
		noerr(t, err)
		err = sessInProgressTransaction.ApplyCommand(description.Server{})
		noerr(t, err)

		wcAck := writeconcern.Majority()
		wcUnack := writeconcern.Unacknowledged()

		descRetryable := description.Server{
			WireVersion:           &description.VersionRange{Min: 6, Max: 21},
			SessionTimeoutMinutes: int64ToPtr(1),
		}

		descNotRetryableWireVersion := description.Server{
			WireVersion:           &description.VersionRange{Min: 6, Max: 21},
			SessionTimeoutMinutes: int64ToPtr(1),
		}

		descNotRetryableStandalone := description.Server{
			WireVersion:           &description.VersionRange{Min: 6, Max: 21},
			SessionTimeoutMinutes: int64ToPtr(1),
			Kind:                  description.ServerKindStandalone,
		}

		testCases := []struct {
			name string
			op   Operation
			desc description.Server
			want Type
		}{
			{"deployment doesn't support", Operation{}, description.Server{}, Type(0)},
			{"wire version too low", Operation{Client: sess, WriteConcern: wcAck}, descNotRetryableWireVersion, Type(0)},
			{"standalone not supported", Operation{Client: sess, WriteConcern: wcAck}, descNotRetryableStandalone, Type(0)},
			{
				"transaction in progress",
				Operation{Client: sessInProgressTransaction, WriteConcern: wcAck},
				descRetryable, Type(0),
			},
			{
				"transaction starting",
				Operation{Client: sessStartingTransaction, WriteConcern: wcAck},
				descRetryable, Type(0),
			},
			{"unacknowledged write concern", Operation{Client: sess, WriteConcern: wcUnack}, descRetryable, Type(0)},
			{
				"acknowledged write concern",
				Operation{Client: sess, WriteConcern: wcAck, Type: Write},
				descRetryable, Write,
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				got := tc.op.retryable(tc.desc)
				if got != (tc.want != Type(0)) {
					t.Errorf("Did not receive expected Type. got %v; want %v", got, tc.want)
				}
			})
		}
	})
	t.Run("addReadConcern", func(t *testing.T) {
		majorityRc := bsoncore.AppendDocumentElement(nil, "readConcern", bsoncore.BuildDocument(nil,
			bsoncore.AppendStringElement(nil, "level", "majority"),
		))

		testCases := []struct {
			name string
			rc   *readconcern.ReadConcern
			want bsoncore.Document
		}{
			{"nil", nil, nil},
			{"empty", &readconcern.ReadConcern{}, nil},
			{"non-empty", readconcern.Majority(), majorityRc},
		}

		for _, tc := range testCases {
			got, err := Operation{ReadConcern: tc.rc}.addReadConcern(nil, description.SelectedServer{})
			noerr(t, err)
			if !bytes.Equal(got, tc.want) {
				t.Errorf("ReadConcern elements do not match. got %v; want %v", got, tc.want)
			}
		}
	})
	t.Run("addWriteConcern", func(t *testing.T) {
		want := bsoncore.AppendDocumentElement(nil, "writeConcern", bsoncore.BuildDocumentFromElements(
			nil, bsoncore.AppendStringElement(nil, "w", "majority"),
		))
		got, err := Operation{WriteConcern: writeconcern.Majority()}.
			addWriteConcern(context.Background(), nil, description.SelectedServer{})
		noerr(t, err)
		if !bytes.Equal(got, want) {
			t.Errorf("WriteConcern elements do not match. got %v; want %v", got, want)
		}
	})
	t.Run("addSession", func(t *testing.T) { t.Skip("These tests should be covered by spec tests.") })
	t.Run("addClusterTime", func(t *testing.T) {
		t.Run("adds max cluster time", func(t *testing.T) {
			want := bsoncore.AppendDocumentElement(nil, "$clusterTime", bsoncore.BuildDocumentFromElements(nil,
				bsoncore.AppendTimestampElement(nil, "clusterTime", 1234, 5678),
			))
			newer := bsoncore.BuildDocumentFromElements(nil, want)
			older := bsoncore.BuildDocumentFromElements(nil,
				bsoncore.AppendDocumentElement(nil, "$clusterTime", bsoncore.BuildDocumentFromElements(nil,
					bsoncore.AppendTimestampElement(nil, "clusterTime", 1234, 5670),
				)),
			)

			clusterClock := new(session.ClusterClock)
			clusterClock.AdvanceClusterTime(newer)
			sessPool := session.NewPool(nil)
			id, err := uuid.New()
			noerr(t, err)

			sess, err := session.NewClientSession(sessPool, id)
			noerr(t, err)
			err = sess.AdvanceClusterTime(older)
			noerr(t, err)

			got := Operation{Client: sess, Clock: clusterClock}.addClusterTime(nil, description.SelectedServer{
				Server: description.Server{WireVersion: &description.VersionRange{Min: 6, Max: 21}},
			})
			if !bytes.Equal(got, want) {
				t.Errorf("ClusterTimes do not match. got %v; want %v", got, want)
			}
		})
	})
	t.Run("calculateMaxTimeMS", func(t *testing.T) {
		var (
			timeout  = 5 * time.Second
			shortRTT = 50 * time.Millisecond
			longRTT  = 10 * time.Second
		)

		timeoutCtx, cancel := csot.WithTimeout(context.Background(), &timeout)
		defer cancel()

		testCases := []struct {
			name     string
			op       Operation
			ctx      context.Context
			rtt      RTTMonitor
			rttMin   time.Duration
			rttStats string
			want     int64
			err      error
		}{
			{
				name:     "uses context deadline and rtt90 with timeout",
				ctx:      timeoutCtx,
				rttMin:   shortRTT,
				rttStats: "",
				want:     5000,
				err:      nil,
			},
			{
				name:     "sub millisecond rtt should round up",
				ctx:      context.Background(),
				rttMin:   longRTT,
				rttStats: "",
				want:     1,
				err:      nil,
			},
		}
		for _, tc := range testCases {
			// Capture test-case for parallel sub-test.
			tc := tc
			t.Run(tc.name, func(t *testing.T) {
				t.Parallel()

				got, err := tc.op.calculateMaxTimeMS(tc.ctx, tc.rttMin, tc.rttStats)

				// Assert that the calculated maxTimeMS is less than or equal to the expected value. A few
				// milliseconds will have elapsed toward the context deadline, and (remainingTimeout
				// - rtt90) will be slightly smaller than the expected value.
				if got > tc.want {
					t.Errorf("maxTimeMS value higher than expected. got %v; wanted at most %v", got, tc.want)
				}
				if !errors.Is(err, tc.err) {
					t.Errorf("error values do not match. got %v; want %v", err, tc.err)
				}
			})
		}
	})
	t.Run("updateClusterTimes", func(t *testing.T) {
		clustertime := bsoncore.BuildDocumentFromElements(nil,
			bsoncore.AppendDocumentElement(nil, "$clusterTime", bsoncore.BuildDocumentFromElements(nil,
				bsoncore.AppendTimestampElement(nil, "clusterTime", 1234, 5678),
			)),
		)

		clusterClock := new(session.ClusterClock)
		sessPool := session.NewPool(nil)
		id, err := uuid.New()
		noerr(t, err)

		sess, err := session.NewClientSession(sessPool, id)
		noerr(t, err)
		Operation{Client: sess, Clock: clusterClock}.updateClusterTimes(clustertime)

		got := sess.ClusterTime
		if !bytes.Equal(got, clustertime) {
			t.Errorf("ClusterTimes do not match. got %v; want %v", got, clustertime)
		}
		got = clusterClock.GetClusterTime()
		if !bytes.Equal(got, clustertime) {
			t.Errorf("ClusterTimes do not match. got %v; want %v", got, clustertime)
		}

		Operation{}.updateClusterTimes(bsoncore.BuildDocumentFromElements(nil)) // should do nothing
	})
	t.Run("updateOperationTime", func(t *testing.T) {
		want := bson.Timestamp{T: 1234, I: 4567}

		sessPool := session.NewPool(nil)
		id, err := uuid.New()
		noerr(t, err)

		sess, err := session.NewClientSession(sessPool, id)
		noerr(t, err)
		if sess.OperationTime != nil {
			t.Fatal("OperationTime should not be set on new session.")
		}
		response := bsoncore.BuildDocumentFromElements(nil, bsoncore.AppendTimestampElement(nil, "operationTime", want.T, want.I))
		Operation{Client: sess}.updateOperationTime(response)
		got := sess.OperationTime
		if got.T != want.T || got.I != want.I {
			t.Errorf("OperationTimes do not match. got %v; want %v", got, want)
		}

		response = bsoncore.BuildDocumentFromElements(nil)
		Operation{Client: sess}.updateOperationTime(response)
		got = sess.OperationTime
		if got.T != want.T || got.I != want.I {
			t.Errorf("OperationTimes do not match. got %v; want %v", got, want)
		}

		Operation{}.updateOperationTime(response) // should do nothing
	})
	t.Run("createReadPref", func(t *testing.T) {
		rpWithTags := bsoncore.BuildDocumentFromElements(nil,
			bsoncore.AppendStringElement(nil, "mode", "secondaryPreferred"),
			bsoncore.BuildArrayElement(nil, "tags",
				bsoncore.Value{Type: bsoncore.TypeEmbeddedDocument,
					Data: bsoncore.BuildDocumentFromElements(nil,
						bsoncore.AppendStringElement(nil, "disk", "ssd"),
						bsoncore.AppendStringElement(nil, "use", "reporting"),
					),
				},
			),
		)
		rpWithMaxStaleness := bsoncore.BuildDocumentFromElements(nil,
			bsoncore.AppendStringElement(nil, "mode", "secondaryPreferred"),
			bsoncore.AppendInt32Element(nil, "maxStalenessSeconds", 25),
		)
		// Hedged read preference: {mode: "secondaryPreferred", hedge: {enabled: true}}
		rpWithHedge := bsoncore.BuildDocumentFromElements(nil,
			bsoncore.AppendStringElement(nil, "mode", "secondaryPreferred"),
			bsoncore.AppendDocumentElement(nil, "hedge", bsoncore.BuildDocumentFromElements(nil,
				bsoncore.AppendBooleanElement(nil, "enabled", true),
			)),
		)
		rpWithAllOptions := bsoncore.BuildDocumentFromElements(nil,
			bsoncore.AppendStringElement(nil, "mode", "secondaryPreferred"),
			bsoncore.BuildArrayElement(nil, "tags",
				bsoncore.Value{Type: bsoncore.TypeEmbeddedDocument,
					Data: bsoncore.BuildDocumentFromElements(nil,
						bsoncore.AppendStringElement(nil, "disk", "ssd"),
						bsoncore.AppendStringElement(nil, "use", "reporting"),
					),
				},
			),
			bsoncore.AppendInt32Element(nil, "maxStalenessSeconds", 25),
			bsoncore.AppendDocumentElement(nil, "hedge", bsoncore.BuildDocumentFromElements(nil,
				bsoncore.AppendBooleanElement(nil, "enabled", false),
			)),
		)

		rpPrimaryPreferred := bsoncore.BuildDocumentFromElements(nil, bsoncore.AppendStringElement(nil, "mode", "primaryPreferred"))
		rpSecondaryPreferred := bsoncore.BuildDocumentFromElements(nil, bsoncore.AppendStringElement(nil, "mode", "secondaryPreferred"))
		rpSecondary := bsoncore.BuildDocumentFromElements(nil, bsoncore.AppendStringElement(nil, "mode", "secondary"))
		rpNearest := bsoncore.BuildDocumentFromElements(nil, bsoncore.AppendStringElement(nil, "mode", "nearest"))

		testCases := []struct {
			name       string
			rp         *readpref.ReadPref
			serverKind description.ServerKind
			topoKind   description.TopologyKind
			opQuery    bool
			want       bsoncore.Document
		}{
			{"nil/single/mongos", nil, description.ServerKindMongos, description.TopologyKindSingle, false, nil},
			{"nil/single/secondary", nil, description.ServerKindRSSecondary, description.TopologyKindSingle, false, rpPrimaryPreferred},
			{"primary/mongos", readpref.Primary(), description.ServerKindMongos, description.TopologyKindSharded, false, nil},
			{"primary/single", readpref.Primary(), description.ServerKindRSPrimary, description.TopologyKindSingle, false, rpPrimaryPreferred},
			{"primary/primary", readpref.Primary(), description.ServerKindRSPrimary, description.TopologyKindReplicaSet, false, nil},
			{"primaryPreferred", &readpref.ReadPref{Mode: readpref.PrimaryPreferredMode}, description.ServerKindRSSecondary, description.TopologyKindReplicaSet, false, rpPrimaryPreferred},
			{"secondaryPreferred/mongos/opquery", &readpref.ReadPref{Mode: readpref.SecondaryPreferredMode}, description.ServerKindMongos, description.TopologyKindSharded, true, nil},
			{"secondaryPreferred", &readpref.ReadPref{Mode: readpref.SecondaryPreferredMode}, description.ServerKindRSSecondary, description.TopologyKindReplicaSet, false, rpSecondaryPreferred},
			{"secondary", &readpref.ReadPref{Mode: readpref.SecondaryMode}, description.ServerKindRSSecondary, description.TopologyKindReplicaSet, false, rpSecondary},
			{"nearest", &readpref.ReadPref{Mode: readpref.NearestMode}, description.ServerKindRSSecondary, description.TopologyKindReplicaSet, false, rpNearest},
			{
				"secondaryPreferred/withTags",
				func() *readpref.ReadPref {
					rpOpts := readpref.Options()

					tagSet, err := readpref.NewTagSet("disk", "ssd", "use", "reporting")
					assert.NoError(t, err)

					rpOpts.SetTagSets([]readpref.TagSet{tagSet})

					rp, _ := readpref.New(readpref.SecondaryPreferredMode, rpOpts)

					return rp
				}(),
				description.ServerKindRSSecondary, description.TopologyKindReplicaSet, false, rpWithTags,
			},
			// GODRIVER-2205: Ensure empty tag sets are written as an empty document in the read
			// preference document. Empty tag sets match any server and are used as a fallback when
			// no other tag sets match any servers.
			{
				"secondaryPreferred/withTags/emptyTagSet",
				func() *readpref.ReadPref {
					rpOpts := readpref.Options()

					rpOpts.SetTagSets([]readpref.TagSet{
						readpref.TagSet{{Name: "disk", Value: "ssd"}},
						readpref.TagSet{},
					})

					rp, _ := readpref.New(readpref.SecondaryPreferredMode, rpOpts)

					return rp
				}(),
				description.ServerKindRSSecondary,
				description.TopologyKindReplicaSet,
				false,
				bsoncore.NewDocumentBuilder().
					AppendString("mode", "secondaryPreferred").
					AppendArray("tags", bsoncore.NewArrayBuilder().
						AppendDocument(bsoncore.NewDocumentBuilder().AppendString("disk", "ssd").Build()).
						AppendDocument(bsoncore.NewDocumentBuilder().Build()).
						Build()).
					Build(),
			},
			{
				"secondaryPreferred/withMaxStaleness",
				func() *readpref.ReadPref {
					rpOpts := readpref.Options()

					maxStaleness := 25 * time.Second
					rpOpts.SetMaxStaleness(maxStaleness)

					rp, _ := readpref.New(readpref.SecondaryPreferredMode, rpOpts)

					return rp
				}(),
				description.ServerKindRSSecondary, description.TopologyKindReplicaSet, false, rpWithMaxStaleness,
			},
			{
				// A read preference document is generated for SecondaryPreferred if the hedge document is non-nil.
				"secondaryPreferred with hedge to mongos using OP_QUERY",
				func() *readpref.ReadPref {
					rpOpts := readpref.Options()

					he := true
					rpOpts.SetHedgeEnabled(he)

					rp, _ := readpref.New(readpref.SecondaryPreferredMode, rpOpts)

					return rp
				}(),
				description.ServerKindMongos,
				description.TopologyKindSharded,
				true,
				rpWithHedge,
			},
			{
				"secondaryPreferred with all options",
				func() *readpref.ReadPref {
					tagSet, err := readpref.NewTagSet("disk", "ssd", "use", "reporting")
					assert.NoError(t, err)

					rpOpts := readpref.Options()

					rpOpts.SetTagSets([]readpref.TagSet{tagSet})

					maxStaleness := 25 * time.Second
					rpOpts.SetMaxStaleness(maxStaleness)

					he := false
					rpOpts.SetHedgeEnabled(he)

					rp, _ := readpref.New(readpref.SecondaryPreferredMode, rpOpts)

					return rp
				}(),
				description.ServerKindRSSecondary,
				description.TopologyKindReplicaSet,
				false,
				rpWithAllOptions,
			},
		}

		for _, tc := range testCases {
			tc := tc
			t.Run(tc.name, func(t *testing.T) {
				desc := description.SelectedServer{Kind: tc.topoKind, Server: description.Server{Kind: tc.serverKind}}
				got, err := Operation{ReadPreference: tc.rp}.createReadPref(desc, tc.opQuery)
				if err != nil {
					t.Fatalf("error creating read pref: %v", err)
				}
				if !bytes.Equal(got, tc.want) {
					t.Errorf("Returned documents do not match. got %v; want %v", got, tc.want)
				}
			})
		}
	})
	t.Run("secondaryOK", func(t *testing.T) {
		t.Run("description.SelectedServer", func(t *testing.T) {
			want := wiremessage.SecondaryOK
			desc := description.SelectedServer{
				Kind:   description.TopologyKindSingle,
				Server: description.Server{Kind: description.ServerKindRSSecondary},
			}
			got := Operation{}.secondaryOK(desc)
			if got != want {
				t.Errorf("Did not receive expected query flags. got %v; want %v", got, want)
			}
		})
		t.Run("readPreference", func(t *testing.T) {
			want := wiremessage.SecondaryOK
			got := Operation{ReadPreference: &readpref.ReadPref{Mode: readpref.SecondaryMode}}.secondaryOK(description.SelectedServer{})
			if got != want {
				t.Errorf("Did not receive expected query flags. got %v; want %v", got, want)
			}
		})
		t.Run("not secondaryOK", func(t *testing.T) {
			var want wiremessage.QueryFlag
			got := Operation{}.secondaryOK(description.SelectedServer{})
			if got != want {
				t.Errorf("Did not receive expected query flags. got %v; want %v", got, want)
			}
		})
	})
	t.Run("ExecuteExhaust", func(t *testing.T) {
		t.Run("errors if connection is not streaming", func(t *testing.T) {
			conn := mnet.NewConnection(&mockConnection{
				rStreaming: false,
			})

			err := Operation{}.ExecuteExhaust(context.TODO(), conn)
			assert.NotNil(t, err, "expected error, got nil")
		})
	})
	t.Run("exhaustAllowed and moreToCome", func(t *testing.T) {
		// Test the interaction between exhaustAllowed and moreToCome on requests/responses when using the Execute
		// and ExecuteExhaust methods.

		// Create a server response wire message that has moreToCome=false.
		serverResponseDoc := bsoncore.BuildDocumentFromElements(nil,
			bsoncore.AppendInt32Element(nil, "ok", 1),
		)
		nonStreamingResponse := createExhaustServerResponse(serverResponseDoc, false)

		// Create a connection that reports that it cannot stream messages.
		conn := &mockConnection{
			rDesc: description.Server{
				WireVersion: &description.VersionRange{
					Max: 6,
				},
			},
			rReadWM:    nonStreamingResponse,
			rCanStream: false,
		}

		mnetconn := mnet.NewConnection(conn)

		op := Operation{
			CommandFn: func(dst []byte, _ description.SelectedServer) ([]byte, error) {
				return bsoncore.AppendInt32Element(dst, handshake.LegacyHello, 1), nil
			},
			Database:   "admin",
			Deployment: SingleConnectionDeployment{C: mnetconn},
		}
		err := op.Execute(context.TODO())
		assert.Nil(t, err, "Execute error: %v", err)

		// The wire message sent to the server should not have exhaustAllowed=true. After execution, the connection
		// should not be in a streaming state.
		assertExhaustAllowedSet(t, conn.pWriteWM, false)
		assert.False(t, conn.CurrentlyStreaming(), "expected CurrentlyStreaming to be false")

		// Modify the connection to report that it can stream and create a new server response with moreToCome=true.
		streamingResponse := createExhaustServerResponse(serverResponseDoc, true)
		conn.rReadWM = streamingResponse
		conn.rCanStream = true
		err = op.Execute(context.TODO())
		assert.Nil(t, err, "Execute error: %v", err)
		assertExhaustAllowedSet(t, conn.pWriteWM, true)
		assert.True(t, conn.CurrentlyStreaming(), "expected CurrentlyStreaming to be true")

		// Reset the server response and go through ExecuteExhaust to mimic streaming the next response. After
		// execution, the connection should still be in a streaming state.
		conn.rReadWM = streamingResponse
		err = op.ExecuteExhaust(context.TODO(), mnetconn)
		assert.Nil(t, err, "ExecuteExhaust error: %v", err)
		assert.True(t, conn.CurrentlyStreaming(), "expected CurrentlyStreaming to be true")
	})
	t.Run("context deadline exceeded not marked as TransientTransactionError", func(t *testing.T) {
		conn := mnet.NewConnection(&mockConnection{})

		// Create a context that's already timed out.
		ctx, cancel := context.WithDeadline(context.Background(), time.Unix(893934480, 0))
		defer cancel()

		op := Operation{
			Database:   "foobar",
			Deployment: SingleConnectionDeployment{C: conn},
			CommandFn: func(dst []byte, _ description.SelectedServer) ([]byte, error) {
				dst = bsoncore.AppendInt32Element(dst, "ping", 1)
				return dst, nil
			},
		}

		err := op.Execute(ctx)
		assert.NotNil(t, err, "expected an error from Execute(), got nil")
		// Assert that error is just context deadline exceeded and is therefore not a driver.Error marked
		// with the TransientTransactionError label.
		assert.True(t, errors.Is(err, context.DeadlineExceeded))
	})
	t.Run("canceled context not marked as TransientTransactionError", func(t *testing.T) {
		conn := mnet.NewConnection(&mockConnection{})

		// Create a context and cancel it immediately.
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		op := Operation{
			Database:   "foobar",
			Deployment: SingleConnectionDeployment{C: conn},
			CommandFn: func(dst []byte, _ description.SelectedServer) ([]byte, error) {
				dst = bsoncore.AppendInt32Element(dst, "ping", 1)
				return dst, nil
			},
		}

		err := op.Execute(ctx)
		assert.NotNil(t, err, "expected an error from Execute(), got nil")
		// Assert that error is just context canceled and is therefore not a driver.Error marked with
		// the TransientTransactionError label.
		assert.Equal(t, err, context.Canceled, "expected context.Canceled error, got %v", err)
	})
	t.Run("ErrDeadlineWouldBeExceeded wraps context.DeadlineExceeded", func(t *testing.T) {
		// Create a deployment that returns a server that reports a 90th
		// percentile RTT of 1 minute.
		d := new(mockDeployment)
		d.returns.server = mockServer{
			conn:       mnet.NewConnection(&mockConnection{}),
			rttMonitor: mockRTTMonitor{min: 1 * time.Minute},
		}

		// Create an operation with a Timeout specified to enable CSOT behavior.
		var dur time.Duration
		op := Operation{
			Database:   "foobar",
			Deployment: d,
			CommandFn: func(dst []byte, _ description.SelectedServer) ([]byte, error) {
				return dst, nil
			},
			Timeout: &dur,
		}

		// Call the operation with a context with a deadline less than the 90th
		// percentile RTT configured above.
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()
		err := op.Execute(ctx)

		assert.ErrorIs(t, err, ErrDeadlineWouldBeExceeded)
		assert.ErrorIs(t, err, context.DeadlineExceeded)
	})
}

func createExhaustServerResponse(response bsoncore.Document, moreToCome bool) []byte {
	const psuedoRequestID = 1
	idx, wm := wiremessage.AppendHeaderStart(nil, 0, psuedoRequestID, wiremessage.OpMsg)
	var flags wiremessage.MsgFlag
	if moreToCome {
		flags = wiremessage.MoreToCome
	}
	wm = wiremessage.AppendMsgFlags(wm, flags)
	wm = wiremessage.AppendMsgSectionType(wm, wiremessage.SingleDocument)
	wm = bsoncore.AppendDocument(wm, response)
	return bsoncore.UpdateLength(wm, idx, int32(len(wm)))
}

func assertExhaustAllowedSet(t *testing.T, wm []byte, expected bool) {
	t.Helper()
	_, _, _, _, wm, ok := wiremessage.ReadHeader(wm)
	if !ok {
		t.Fatal("could not read wm header")
	}
	flags, wm, ok := wiremessage.ReadMsgFlags(wm)
	if !ok {
		t.Fatal("could not read wm flags")
	}

	actual := flags&wiremessage.ExhaustAllowed > 0
	assert.Equal(t, expected, actual, "expected exhaustAllowed set %v, got %v", expected, actual)
}

type mockDeployment struct {
	params struct {
		selector description.ServerSelector
	}
	returns struct {
		server                 Server
		err                    error
		retry                  bool
		kind                   description.TopologyKind
		serverSelectionTimeout time.Duration
	}
}

func (m *mockDeployment) SelectServer(_ context.Context, desc description.ServerSelector) (Server, error) {
	m.params.selector = desc

	return m.returns.server, m.returns.err
}

func (m *mockDeployment) GetServerSelectionTimeout() time.Duration {
	return m.returns.serverSelectionTimeout
}

func (m *mockDeployment) Kind() description.TopologyKind { return m.returns.kind }

type mockServerSelector struct{}

func (m *mockServerSelector) SelectServer(description.Topology, []description.Server) ([]description.Server, error) {
	panic("not implemented")
}

func (m *mockServerSelector) String() string {
	panic("not implemented")
}

type mockServer struct {
	conn       *mnet.Connection
	err        error
	rttMonitor RTTMonitor
}

func (ms mockServer) Connection(context.Context) (*mnet.Connection, error) { return ms.conn, ms.err }
func (ms mockServer) RTTMonitor() RTTMonitor                               { return ms.rttMonitor }

type mockRTTMonitor struct {
	ewma  time.Duration
	min   time.Duration
	stats string
}

func (mrm mockRTTMonitor) EWMA() time.Duration { return mrm.ewma }
func (mrm mockRTTMonitor) Min() time.Duration  { return mrm.min }
func (mrm mockRTTMonitor) Stats() string       { return mrm.stats }

type mockConnection struct {
	// parameters
	pWriteWM []byte

	// returns
	rWriteErr     error
	rReadWM       []byte
	rReadErr      error
	rDesc         description.Server
	rCloseErr     error
	rID           string
	rServerConnID *int64
	rAddr         address.Address
	rCanStream    bool
	rStreaming    bool
}

func (m *mockConnection) Description() description.Server { return m.rDesc }
func (m *mockConnection) Close() error                    { return m.rCloseErr }
func (m *mockConnection) ID() string                      { return m.rID }
func (m *mockConnection) ServerConnectionID() *int64      { return m.rServerConnID }
func (m *mockConnection) Address() address.Address        { return m.rAddr }
func (m *mockConnection) SupportsStreaming() bool         { return m.rCanStream }
func (m *mockConnection) CurrentlyStreaming() bool        { return m.rStreaming }
func (m *mockConnection) SetStreaming(streaming bool)     { m.rStreaming = streaming }
func (m *mockConnection) Stale() bool                     { return false }
func (m *mockConnection) OIDCTokenGenID() uint64          { return 0 }
func (m *mockConnection) SetOIDCTokenGenID(uint64)        {}

func (m *mockConnection) DriverConnectionID() int64 { return 0 }

func (m *mockConnection) Write(_ context.Context, wm []byte) error {
	m.pWriteWM = wm
	return m.rWriteErr
}

func (m *mockConnection) Read(_ context.Context) ([]byte, error) {
	return m.rReadWM, m.rReadErr
}

type retryableError struct {
	error
}

func (retryableError) Retryable() bool { return true }

var _ RetryablePoolError = retryableError{}

// mockRetryServer is used to test retry of connection checkout. Returns a retryable error from
// Connection().
type mockRetryServer struct {
	numCallsToConnection int
}

// Connection records the number of calls and returns retryable errors until the provided context
// times out or is cancelled, then returns the context error.
func (ms *mockRetryServer) Connection(ctx context.Context) (*mnet.Connection, error) {
	ms.numCallsToConnection++

	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	time.Sleep(1 * time.Millisecond)
	return nil, retryableError{error: errors.New("test error")}
}

func (ms *mockRetryServer) RTTMonitor() RTTMonitor {
	return &csot.ZeroRTTMonitor{}
}

func TestRetry(t *testing.T) {
	t.Run("retries multiple times with RetryContext", func(t *testing.T) {
		d := new(mockDeployment)
		ms := new(mockRetryServer)
		d.returns.server = ms

		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()

		retry := RetryContext
		err := Operation{
			CommandFn:  func([]byte, description.SelectedServer) ([]byte, error) { return nil, nil },
			Deployment: d,
			Database:   "testing",
			RetryMode:  &retry,
			Type:       Read,
		}.Execute(ctx)
		assert.NotNil(t, err, "expected an error from Execute()")

		// Expect Connection() to be called at least 3 times. The first call is the initial attempt
		// to run the operation and the second is the retry. The third indicates that we retried
		// more than once, which is the behavior we want to assert.
		assert.True(t,
			ms.numCallsToConnection >= 3,
			"expected Connection() to be called at least 3 times")

		deadline, _ := ctx.Deadline()
		assert.True(t,
			time.Now().After(deadline),
			"expected operation to complete only after the context deadline is exceeded")
	})
}

func TestDecodeOpReply(t *testing.T) {
	t.Parallel()

	// GODRIVER-2869: Prevent infinite loop caused by malformatted wiremessage with length of 0.
	t.Run("malformatted wiremessage with length of 0", func(t *testing.T) {
		t.Parallel()

		var wm []byte
		wm = wiremessage.AppendReplyFlags(wm, 0)
		wm = wiremessage.AppendReplyCursorID(wm, int64(0))
		wm = wiremessage.AppendReplyStartingFrom(wm, 0)
		wm = wiremessage.AppendReplyNumberReturned(wm, 0)
		idx, wm := bsoncore.ReserveLength(wm)
		wm = bsoncore.UpdateLength(wm, idx, 0)
		reply := Operation{}.decodeOpReply(wm)
		assert.Equal(t, []bsoncore.Document(nil), reply.documents)
	})
}

func TestFilterDeprioritizedServers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		deprioritized []description.Server
		candidates    []description.Server
		want          []description.Server
	}{
		{
			name:       "empty",
			candidates: []description.Server{},
			want:       []description.Server{},
		},
		{
			name:       "nil candidates",
			candidates: nil,
			want:       []description.Server{},
		},
		{
			name: "nil deprioritized server list",
			candidates: []description.Server{
				{
					Addr: address.Address("mongodb://localhost:27017"),
				},
			},
			want: []description.Server{
				{
					Addr: address.Address("mongodb://localhost:27017"),
				},
			},
		},
		{
			name: "deprioritize single server candidate list",
			candidates: []description.Server{
				{
					Addr: address.Address("mongodb://localhost:27017"),
				},
			},
			deprioritized: []description.Server{
				{
					Addr: address.Address("mongodb://localhost:27017"),
				},
			},
			want: []description.Server{
				// Since all available servers were deprioritized, then the selector
				// should return all candidates.
				{
					Addr: address.Address("mongodb://localhost:27017"),
				},
			},
		},
		{
			name: "depriotirize one server in multi server candidate list",
			candidates: []description.Server{
				{
					Addr: address.Address("mongodb://localhost:27017"),
				},
				{
					Addr: address.Address("mongodb://localhost:27018"),
				},
				{
					Addr: address.Address("mongodb://localhost:27019"),
				},
			},
			deprioritized: []description.Server{
				{
					Addr: address.Address("mongodb://localhost:27017"),
				},
			},
			want: []description.Server{
				{
					Addr: address.Address("mongodb://localhost:27018"),
				},
				{
					Addr: address.Address("mongodb://localhost:27019"),
				},
			},
		},
		{
			name: "depriotirize multiple servers in multi server candidate list",
			deprioritized: []description.Server{
				{
					Addr: address.Address("mongodb://localhost:27017"),
				},
				{
					Addr: address.Address("mongodb://localhost:27018"),
				},
			},
			candidates: []description.Server{
				{
					Addr: address.Address("mongodb://localhost:27017"),
				},
				{
					Addr: address.Address("mongodb://localhost:27018"),
				},
				{
					Addr: address.Address("mongodb://localhost:27019"),
				},
			},
			want: []description.Server{
				{
					Addr: address.Address("mongodb://localhost:27019"),
				},
			},
		},
	}

	for _, tc := range tests {
		tc := tc // Capture the range variable.

		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got := filterDeprioritizedServers(tc.candidates, tc.deprioritized)
			assert.ElementsMatch(t, got, tc.want)
		})
	}
}

func TestMarshalBSONWriteConcern(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		writeConcern writeconcern.WriteConcern
		wantBSONType bson.Type
		wtimeout     time.Duration
		want         bson.D
		wantErr      string
	}{
		{
			name:         "empty",
			writeConcern: writeconcern.WriteConcern{},
			wantBSONType: 0x0,
			want:         nil,
			wtimeout:     0,
			wantErr:      "a write concern must have at least one field set",
		},
		{
			name:         "journal only",
			writeConcern: *writeconcern.Journaled(),
			wantBSONType: bson.TypeEmbeddedDocument,
			want:         bson.D{{"j", true}},
			wtimeout:     0,
			wantErr:      "a write concern must have at least one field set",
		},
		{
			name:         "journal and wtimout",
			writeConcern: *writeconcern.Journaled(),
			wtimeout:     10 * time.Millisecond,
			wantBSONType: bson.TypeEmbeddedDocument,
			want:         bson.D{{"j", true}, {"wtimeout", int64(10 * time.Millisecond / time.Millisecond)}},
			wantErr:      "a write concern must have at least one field set",
		},
	}

	for _, test := range tests {
		test := test // Capture the range variable

		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			gotBSONType, gotBSON, gotErr := MarshalBSONWriteConcern(&test.writeConcern, test.wtimeout)
			assert.Equal(t, test.wantBSONType, gotBSONType)

			wantBSON := []byte(nil)

			if test.want != nil {
				var err error

				wantBSON, err = bson.Marshal(test.want)
				assert.NoError(t, err)
			}

			assert.Equal(t, wantBSON, gotBSON)

			if gotErr != nil {
				assert.EqualError(t, gotErr, test.wantErr)
			}
		})
	}
}
