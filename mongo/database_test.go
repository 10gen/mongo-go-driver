// Copyright (C) MongoDB, Inc. 2017-present.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

package mongo

import (
	"context"
	"errors"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/internal/assert"
	"go.mongodb.org/mongo-driver/v2/internal/require"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readconcern"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"
	"go.mongodb.org/mongo-driver/v2/mongo/writeconcern"
	"go.mongodb.org/mongo-driver/v2/x/mongo/driver/topology"
)

func setupDb(name string, opts ...options.Lister[options.DatabaseOptions]) *Database {
	client := setupClient()
	return client.Database(name, opts...)
}

func compareDbs(t *testing.T, expected, got *Database) {
	t.Helper()
	assert.Equal(t, expected.readPreference, got.readPreference,
		"expected read preference %v, got %v", expected.readPreference, got.readPreference)
	assert.Equal(t, expected.readConcern, got.readConcern,
		"expected read concern %v, got %v", expected.readConcern, got.readConcern)
	assert.Equal(t, expected.writeConcern, got.writeConcern,
		"expected write concern %v, got %v", expected.writeConcern, got.writeConcern)
	assert.Equal(t, expected.registry, got.registry,
		"expected write concern %v, got %v", expected.registry, got.registry)
}

func TestDatabase(t *testing.T) {
	t.Run("initialize", func(t *testing.T) {
		name := "foo"
		db := setupDb(name)
		assert.Equal(t, name, db.Name(), "expected db name %v, got %v", name, db.Name())
		assert.NotNil(t, db.Client(), "expected valid client, got nil")
	})
	t.Run("options", func(t *testing.T) {
		t.Run("custom", func(t *testing.T) {
			rpPrimary := readpref.Primary()
			rpSecondary := readpref.Secondary()
			wc1 := &writeconcern.WriteConcern{W: 5}
			wc2 := &writeconcern.WriteConcern{W: 10}
			rcLocal := readconcern.Local()
			rcMajority := readconcern.Majority()
			reg := bson.NewRegistry()

			opts := options.Database().SetReadPreference(rpPrimary).SetReadConcern(rcLocal).SetWriteConcern(wc1).
				SetReadPreference(rpSecondary).SetReadConcern(rcMajority).SetWriteConcern(wc2).SetRegistry(reg)
			expected := &Database{
				readPreference: rpSecondary,
				readConcern:    rcMajority,
				writeConcern:   wc2,
				registry:       reg,
			}
			got := setupDb("foo", opts)
			compareDbs(t, expected, got)
		})
		t.Run("inherit", func(t *testing.T) {
			rpPrimary := readpref.Primary()
			rcLocal := readconcern.Local()
			wc1 := &writeconcern.WriteConcern{W: 10}
			reg := bson.NewRegistry()

			client := setupClient(options.Client().SetReadPreference(rpPrimary).SetReadConcern(rcLocal).SetRegistry(reg))
			got := client.Database("foo", options.Database().SetWriteConcern(wc1))
			expected := &Database{
				readPreference: rpPrimary,
				readConcern:    rcLocal,
				writeConcern:   wc1,
				registry:       reg,
			}
			compareDbs(t, expected, got)
		})
	})
	t.Run("replaceErrors for disconnected topology", func(t *testing.T) {
		db := setupDb("foo")

		topo, ok := db.client.deployment.(*topology.Topology)
		require.True(t, ok, "client deployment is not a topology")

		err := topo.Disconnect(context.Background())
		require.NoError(t, err)

		err = db.RunCommand(bgCtx, bson.D{{"x", 1}}).Err()
		assert.Equal(t, ErrClientDisconnected, err, "expected error %v, got %v", ErrClientDisconnected, err)

		err = db.Drop(bgCtx)
		assert.Equal(t, ErrClientDisconnected, err, "expected error %v, got %v", ErrClientDisconnected, err)

		_, err = db.ListCollections(bgCtx, bson.D{})
		assert.Equal(t, ErrClientDisconnected, err, "expected error %v, got %v", ErrClientDisconnected, err)
	})
	t.Run("TransientTransactionError label", func(t *testing.T) {
		client := setupClient(options.Client().ApplyURI("mongodb://nonexistent").SetServerSelectionTimeout(3 * time.Second))
		defer func() { _ = client.Disconnect(bgCtx) }()

		t.Run("negative case of non-transaction", func(t *testing.T) {
			var sse topology.ServerSelectionError
			var le LabeledError

			err := client.Ping(bgCtx, nil)
			assert.NotNil(t, err, "expected error, got nil")
			assert.True(t, errors.As(err, &sse), `expected error to be a "topology.ServerSelectionError"`)
			if errors.As(err, &le) {
				assert.False(t, le.HasErrorLabel("TransientTransactionError"), `expected error not to include the "TransientTransactionError" label`)
			}
		})

		t.Run("positive case of transaction", func(t *testing.T) {
			var sse topology.ServerSelectionError
			var le LabeledError

			sess, err := client.StartSession()
			assert.Nil(t, err, "expected nil, got %v", err)
			defer sess.EndSession(bgCtx)

			sessCtx := NewSessionContext(bgCtx, sess)
			err = sess.StartTransaction()
			assert.Nil(t, err, "expected nil, got %v", err)

			err = client.Ping(sessCtx, nil)
			assert.NotNil(t, err, "expected error, got nil")
			assert.True(t, errors.As(err, &sse), `expected error to be a "topology.ServerSelectionError"`)
			assert.True(t, errors.As(err, &le), `expected error to implement the "LabeledError" interface`)
			assert.True(t, le.HasErrorLabel("TransientTransactionError"), `expected error to include the "TransientTransactionError" label`)
		})
	})
	t.Run("nil document error", func(t *testing.T) {
		db := setupDb("foo")

		err := db.RunCommand(bgCtx, nil).Err()
		assert.True(t, errors.Is(err, ErrNilDocument), "expected error %v, got %v", ErrNilDocument, err)

		_, err = db.Watch(context.Background(), nil)
		watchErr := errors.New("can only marshal slices and arrays into aggregation pipelines, but got invalid")
		assert.Equal(t, watchErr, err, "expected error %v, got %v", watchErr, err)

		_, err = db.ListCollections(context.Background(), nil)
		assert.True(t, errors.Is(err, ErrNilDocument), "expected error %v, got %v", ErrNilDocument, err)

		_, err = db.ListCollectionNames(context.Background(), nil)
		assert.True(t, errors.Is(err, ErrNilDocument), "expected error %v, got %v", ErrNilDocument, err)
	})
}
