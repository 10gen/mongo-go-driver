// Copyright (C) MongoDB, Inc. 2017-present.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

package mongo

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/internal/testutil"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func shouldSkipMongosPinningTests(t *testing.T, serverVersion string) bool {
	return os.Getenv("TOPOLOGY") != "sharded_cluster" || compareVersions(t, serverVersion, "4.1.6") < 0
}

func TestMongosPinning(t *testing.T) {
	dbName := "admin"
	dbAdmin := createTestDatabase(t, &dbName)
	version, err := getServerVersion(dbAdmin)
	require.NoError(t, err)

	mongodbURI := testutil.ConnString(t)
	opts := options.Client().ApplyURI(mongodbURI.String()).SetLocalThreshold(time.Second)
	hosts := opts.Hosts

	if shouldSkipMongosPinningTests(t, version) || len(hosts) < 2 {
		t.Skip("Not enough mongoses")
	}

	client, err := Connect(ctx, opts)
	require.NoError(t, err)
	defer func() { _ = client.Disconnect(ctx) }()
	db := client.Database("TestMongosPinning")

	t.Run("unpinForNextTransaction", func(t *testing.T) {
		collName := "unpinForNextTransaction"
		db.RunCommand(
			context.Background(),
			bson.D{{"drop", collName}},
		)

		err = db.RunCommand(
			context.Background(),
			bson.D{{"create", collName}},
		).Err()
		coll := db.Collection(collName)

		addresses := map[string]struct{}{}
		err = client.UseSession(ctx, func(sctx SessionContext) error {
			err := sctx.StartTransaction(options.Transaction())
			if err != nil {
				return err
			}

			_, err = coll.InsertOne(sctx, bson.D{{"x", 1}})
			if err != nil {
				return err
			}

			err = sctx.CommitTransaction(sctx)
			if err != nil {
				return err
			}

			for i := 0; i < 50; i++ {
				err = sctx.StartTransaction(options.Transaction())
				if err != nil {
					return err
				}

				cursor, err := coll.Find(context.Background(), bson.D{})
				if err != nil {
					return err
				}
				require.True(t, cursor.Next(context.Background()))
				addresses[cursor.bc.Server().Description().Addr.String()] = struct{}{}

				err = sctx.CommitTransaction(sctx)
				if err != nil {
					return err
				}
			}
			return nil
		})
		require.NoError(t, err)
		require.True(t, len(addresses) > 1)
	})
	t.Run("unpinForNonTransactionOperation", func(t *testing.T) {
		collName := "unpinForNonTransaction"
		db.RunCommand(
			context.Background(),
			bson.D{{"drop", collName}},
		)

		err = db.RunCommand(
			context.Background(),
			bson.D{{"create", collName}},
		).Err()
		coll := db.Collection(collName)

		addresses := map[string]bool{}
		err = client.UseSession(ctx, func(sctx SessionContext) error {
			err := sctx.StartTransaction(options.Transaction())
			if err != nil {
				return err
			}

			_, err = coll.InsertOne(sctx, bson.D{{"x", 1}})
			if err != nil {
				return err
			}

			err = sctx.CommitTransaction(sctx)
			if err != nil {
				return err
			}

			for i := 0; i < 50; i++ {
				cursor, err := coll.Find(context.Background(), bson.D{})
				if err != nil {
					return err
				}
				require.True(t, cursor.Next(context.Background()))
				addresses[cursor.bc.Server().Description().Addr.String()] = true
			}
			return nil
		})
		require.NoError(t, err)
		require.True(t, len(addresses) > 1)
	})
}
