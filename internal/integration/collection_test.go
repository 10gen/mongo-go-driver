// Copyright (C) MongoDB, Inc. 2017-present.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

package integration

import (
	"context"
	"errors"
	"strings"
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/event"
	"go.mongodb.org/mongo-driver/v2/internal/assert"
	"go.mongodb.org/mongo-driver/v2/internal/integration/mtest"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/writeconcern"
	"go.mongodb.org/mongo-driver/v2/x/bsonx/bsoncore"
	"go.mongodb.org/mongo-driver/v2/x/mongo/driver/drivertest"
)

const (
	errorDuplicateKey           = 11000
	errorCappedCollDeleteLegacy = 10101
	errorCappedCollDelete       = 20
	errorModifiedIDLegacy       = 16837
	errorModifiedID             = 66
)

var (
	// impossibleWc is a write concern that can't be satisfied and is used to test write concern errors
	// for various operations. It includes a timeout because legacy servers will wait for all W nodes to respond,
	// causing tests to hang.
	impossibleWc = &writeconcern.WriteConcern{
		W: 30,
	}
)

func TestCollection(t *testing.T) {
	mt := mtest.New(t, mtest.NewOptions().CreateClient(false))

	mt.RunOpts("insert one", noClientOpts, func(mt *mtest.T) {
		mt.Run("success", func(mt *mtest.T) {
			id := bson.NewObjectID()
			doc := bson.D{{"_id", id}, {"x", 1}}
			res, err := mt.Coll.InsertOne(context.Background(), doc)
			assert.Nil(mt, err, "InsertOne error: %v", err)
			assert.Equal(mt, id, res.InsertedID, "expected inserted ID %v, got %v", id, res.InsertedID)
		})
		mt.Run("write error", func(mt *mtest.T) {
			doc := bson.D{{"_id", 1}}
			_, err := mt.Coll.InsertOne(context.Background(), doc)
			assert.Nil(mt, err, "InsertOne error: %v", err)

			_, err = mt.Coll.InsertOne(context.Background(), doc)
			we, ok := err.(mongo.WriteException)
			assert.True(mt, ok, "expected error type %T, got %T", mongo.WriteException{}, err)
			assert.Equal(mt, 1, len(we.WriteErrors), "expected 1 write error, got %v", len(we.WriteErrors))
			writeErr := we.WriteErrors[0]
			assert.Equal(mt, errorDuplicateKey, writeErr.Code, "expected code %v, got %v", errorDuplicateKey, writeErr.Code)
		})

		wcCollOpts := options.Collection().SetWriteConcern(impossibleWc)
		wcTestOpts := mtest.NewOptions().CollectionOptions(wcCollOpts).Topologies(mtest.ReplicaSet)
		mt.RunOpts("write concern error", wcTestOpts, func(mt *mtest.T) {
			doc := bson.D{{"_id", 1}}
			_, err := mt.Coll.InsertOne(context.Background(), doc)
			we, ok := err.(mongo.WriteException)
			assert.True(mt, ok, "expected error type %v, got %v", mongo.WriteException{}, err)
			assert.NotNil(mt, we.WriteConcernError, "expected write concern error, got %+v", we)
		})

		// Require 3.2 servers for bypassDocumentValidation support.
		convertedOptsOpts := mtest.NewOptions().MinServerVersion("3.2")
		mt.RunOpts("options are converted", convertedOptsOpts, func(mt *mtest.T) {
			nilOptsTestCases := []struct {
				name            string
				opts            []options.Lister[options.InsertOneOptions]
				expectOptionSet bool
			}{
				{
					"only nil is passed",
					[]options.Lister[options.InsertOneOptions]{nil},
					false,
				},
				{
					"non-nil options is passed before nil",
					[]options.Lister[options.InsertOneOptions]{options.InsertOne().SetBypassDocumentValidation(true), nil},
					true,
				},
				{
					"non-nil options is passed after nil",
					[]options.Lister[options.InsertOneOptions]{nil, options.InsertOne().SetBypassDocumentValidation(true)},
					true,
				},
			}

			for _, testCase := range nilOptsTestCases {
				mt.Run(testCase.name, func(mt *mtest.T) {
					doc := bson.D{{"x", 1}}
					_, err := mt.Coll.InsertOne(context.Background(), doc, testCase.opts...)
					assert.Nil(mt, err, "InsertOne error: %v", err)
					optName := "bypassDocumentValidation"
					evt := mt.GetStartedEvent()

					val, err := evt.Command.LookupErr(optName)
					if testCase.expectOptionSet {
						assert.Nil(mt, err, "expected %v to be set but got: %v", optName, err)
						assert.True(mt, val.Boolean(), "expected %v to be true but got: %v", optName, val.Boolean())
						return
					}
					assert.NotNil(mt, err, "expected %v to be unset but got nil", optName)
				})
			}
		})
	})
	mt.RunOpts("insert many", noClientOpts, func(mt *mtest.T) {
		mt.Parallel()

		mt.Run("success", func(mt *mtest.T) {
			mt.Parallel()

			want1 := int32(11)
			want2 := int32(12)
			docs := []interface{}{
				bson.D{{"_id", want1}},
				bson.D{{"x", 6}},
				bson.D{{"_id", want2}},
			}

			res, err := mt.Coll.InsertMany(context.Background(), docs)
			assert.Nil(mt, err, "InsertMany error: %v", err)
			assert.Equal(mt, 3, len(res.InsertedIDs), "expected 3 inserted IDs, got %v", len(res.InsertedIDs))
			assert.Equal(mt, want1, res.InsertedIDs[0], "expected inserted ID %v, got %v", want1, res.InsertedIDs[0])
			assert.NotNil(mt, res.InsertedIDs[1], "expected ID but got nil")
			assert.Equal(mt, want2, res.InsertedIDs[2], "expected inserted ID %v, got %v", want2, res.InsertedIDs[2])
		})
		mt.Run("batches", func(mt *mtest.T) {
			mt.Parallel()

			const (
				megabyte = 10 * 10 * 10 * 10 * 10 * 10
				numDocs  = 700000
			)
			var docs []interface{}
			total := uint32(0)
			expectedDocSize := uint32(26)
			for i := 0; i < numDocs; i++ {
				d := bson.D{
					{"a", int32(i)},
					{"b", int32(i * 2)},
					{"c", int32(i * 3)},
				}
				b, _ := bson.Marshal(d)
				assert.Equal(mt, int(expectedDocSize), len(b), "expected doc len %v, got %v", expectedDocSize, len(b))
				docs = append(docs, d)
				total += uint32(len(b))
			}
			assert.True(mt, total > 16*megabyte, "expected total greater than 16mb but got %v", total)
			res, err := mt.Coll.InsertMany(context.Background(), docs)
			assert.Nil(mt, err, "InsertMany error: %v", err)
			assert.Equal(mt, numDocs, len(res.InsertedIDs), "expected %v inserted IDs, got %v", numDocs, len(res.InsertedIDs))
		})
		mt.Run("large document batches", func(mt *mtest.T) {
			mt.Parallel()

			docs := []interface{}{create16MBDocument(mt), create16MBDocument(mt), create16MBDocument(mt)}
			_, err := mt.Coll.InsertMany(context.Background(), docs)
			assert.Nil(mt, err, "InsertMany error: %v", err)
			evt := mt.GetStartedEvent()
			assert.Equal(mt, "insert", evt.CommandName, "expected 'insert' event, got '%v'", evt.CommandName)
			evt = mt.GetStartedEvent()
			assert.Equal(mt, "insert", evt.CommandName, "expected 'insert' event, got '%v'", evt.CommandName)
		})
		mt.RunOpts("write error", noClientOpts, func(mt *mtest.T) {
			mt.Parallel()

			docs := []interface{}{
				bson.D{{"_id", bson.NewObjectID()}},
				bson.D{{"_id", bson.NewObjectID()}},
				bson.D{{"_id", bson.NewObjectID()}},
			}

			testCases := []struct {
				name      string
				ordered   bool
				numErrors int
			}{
				{"unordered", false, 3},
				{"ordered", true, 1},
			}
			for _, tc := range testCases {
				mt.Run(tc.name, func(mt *mtest.T) {
					_, err := mt.Coll.InsertMany(context.Background(), docs)
					assert.Nil(mt, err, "InsertMany error: %v", err)
					_, err = mt.Coll.InsertMany(context.Background(), docs, options.InsertMany().SetOrdered(tc.ordered))

					we, ok := err.(mongo.BulkWriteException)
					assert.True(mt, ok, "expected error type %T, got %T", mongo.BulkWriteException{}, err)
					numErrors := len(we.WriteErrors)
					assert.Equal(mt, tc.numErrors, numErrors, "expected %v write errors, got %v", tc.numErrors, numErrors)
					gotCode := we.WriteErrors[0].Code
					assert.Equal(mt, errorDuplicateKey, gotCode, "expected error code %v, got %v", errorDuplicateKey, gotCode)
				})
			}
		})
		mt.Run("return only inserted ids", func(mt *mtest.T) {
			mt.Parallel()

			id := int32(15)
			docs := []interface{}{
				bson.D{{"_id", id}},
				bson.D{{"_id", id}},
				bson.D{{"x", 6}},
				bson.D{{"_id", id}},
			}

			testCases := []struct {
				name        string
				ordered     bool
				numInserted int
				numErrors   int
			}{
				{"unordered", false, 2, 2},
				{"ordered", true, 1, 1},
			}
			for _, tc := range testCases {
				mt.Run(tc.name, func(mt *mtest.T) {
					res, err := mt.Coll.InsertMany(context.Background(), docs, options.InsertMany().SetOrdered(tc.ordered))

					assert.Equal(mt, tc.numInserted, len(res.InsertedIDs), "expected %v inserted IDs, got %v", tc.numInserted, len(res.InsertedIDs))
					assert.Equal(mt, id, res.InsertedIDs[0], "expected inserted ID %v, got %v", id, res.InsertedIDs[0])
					if tc.numInserted > 1 {
						assert.NotNil(mt, res.InsertedIDs[1], "expected ID but got nil")
					}

					we, ok := err.(mongo.BulkWriteException)
					assert.True(mt, ok, "expected error type %T, got %T", mongo.BulkWriteException{}, err)
					numErrors := len(we.WriteErrors)
					assert.Equal(mt, tc.numErrors, numErrors, "expected %v write errors, got %v", tc.numErrors, numErrors)
					gotCode := we.WriteErrors[0].Code
					assert.Equal(mt, errorDuplicateKey, gotCode, "expected error code %v, got %v", errorDuplicateKey, gotCode)
				})
			}
		})
		mt.Run("writeError index", func(mt *mtest.T) {
			mt.Parallel()

			// force multiple batches
			numDocs := 700000
			var docs []interface{}
			for i := 0; i < numDocs; i++ {
				d := bson.D{
					{"a", int32(i)},
					{"b", int32(i * 2)},
					{"c", int32(i * 3)},
				}
				docs = append(docs, d)
			}
			repeated := bson.D{{"_id", int32(11)}}
			docs = append(docs, repeated, repeated)

			_, err := mt.Coll.InsertMany(context.Background(), docs)
			assert.NotNil(mt, err, "expected InsertMany error, got nil")

			we, ok := err.(mongo.BulkWriteException)
			assert.True(mt, ok, "expected error type %T, got %T", mongo.BulkWriteException{}, err)
			numErrors := len(we.WriteErrors)
			assert.Equal(mt, 1, numErrors, "expected 1 write error, got %v", numErrors)
			gotIndex := we.WriteErrors[0].Index
			assert.Equal(mt, numDocs+1, gotIndex, "expected index %v, got %v", numDocs+1, gotIndex)
		})
		wcCollOpts := options.Collection().SetWriteConcern(impossibleWc)
		wcTestOpts := mtest.NewOptions().CollectionOptions(wcCollOpts).Topologies(mtest.ReplicaSet)
		mt.RunOpts("write concern error", wcTestOpts, func(mt *mtest.T) {
			_, err := mt.Coll.InsertMany(context.Background(), []interface{}{bson.D{{"_id", 1}}})
			we, ok := err.(mongo.BulkWriteException)
			assert.True(mt, ok, "expected error type %v, got %v", mongo.BulkWriteException{}, err)
			assert.NotNil(mt, we.WriteConcernError, "expected write concern error, got %+v", err)
		})
	})
	mt.RunOpts("delete one", noClientOpts, func(mt *mtest.T) {
		mt.Run("found", func(mt *mtest.T) {
			initCollection(mt, mt.Coll)
			res, err := mt.Coll.DeleteOne(context.Background(), bson.D{{"x", 1}})
			assert.Nil(mt, err, "DeleteOne error: %v", err)
			assert.Equal(mt, int64(1), res.DeletedCount, "expected DeletedCount 1, got %v", res.DeletedCount)
		})
		mt.Run("not found", func(mt *mtest.T) {
			initCollection(mt, mt.Coll)
			res, err := mt.Coll.DeleteOne(context.Background(), bson.D{{"x", 0}})
			assert.Nil(mt, err, "DeleteOne error: %v", err)
			assert.Equal(mt, int64(0), res.DeletedCount, "expected DeletedCount 0, got %v", res.DeletedCount)
		})
		mt.RunOpts("not found with options", mtest.NewOptions().MinServerVersion("3.4"), func(mt *mtest.T) {
			initCollection(mt, mt.Coll)
			opts := options.DeleteOne().SetCollation(&options.Collation{Locale: "en_US"})
			res, err := mt.Coll.DeleteOne(context.Background(), bson.D{{"x", 0}}, opts)
			assert.Nil(mt, err, "DeleteOne error: %v", err)
			assert.Equal(mt, int64(0), res.DeletedCount, "expected DeletedCount 0, got %v", res.DeletedCount)
		})
		mt.RunOpts("write error", mtest.NewOptions().MaxServerVersion("5.0.7"), func(mt *mtest.T) {
			// Deletes are not allowed on capped collections on MongoDB 5.0.6-. We use this
			// behavior to test the processing of write errors.
			cappedOpts := options.CreateCollection().SetCapped(true).SetSizeInBytes(64 * 1024)
			capped := mt.CreateCollection(mtest.Collection{
				Name:       "deleteOne_capped",
				CreateOpts: cappedOpts,
			}, true)
			_, err := capped.DeleteOne(context.Background(), bson.D{{"x", 1}})

			we, ok := err.(mongo.WriteException)
			assert.True(mt, ok, "expected error type %T, got %T", mongo.WriteException{}, err)
			numWriteErrors := len(we.WriteErrors)
			assert.Equal(mt, 1, numWriteErrors, "expected 1 write error, got %v", numWriteErrors)
			gotCode := we.WriteErrors[0].Code
			assert.True(mt, gotCode == errorCappedCollDeleteLegacy || gotCode == errorCappedCollDelete,
				"expected error code %v or %v, got %v", errorCappedCollDeleteLegacy, errorCappedCollDelete, gotCode)
		})
		mt.RunOpts("write concern error", mtest.NewOptions().Topologies(mtest.ReplicaSet), func(mt *mtest.T) {
			// 2.6 returns right away if the document doesn't exist
			filter := bson.D{{"x", 1}}
			_, err := mt.Coll.InsertOne(context.Background(), filter)
			assert.Nil(mt, err, "InsertOne error: %v", err)

			mt.CloneCollection(options.Collection().SetWriteConcern(impossibleWc))
			_, err = mt.Coll.DeleteOne(context.Background(), filter)
			we, ok := err.(mongo.WriteException)
			assert.True(mt, ok, "expected error type %T, got %T", mongo.WriteException{}, err)
			assert.NotNil(mt, we.WriteConcernError, "expected write concern error, got nil")
		})
		mt.RunOpts("single key map index", mtest.NewOptions().MinServerVersion("4.4"), func(mt *mtest.T) {
			initCollection(mt, mt.Coll)
			indexView := mt.Coll.Indexes()
			_, err := indexView.CreateOne(context.Background(), mongo.IndexModel{
				Keys: bson.D{{"x", 1}},
			})
			assert.Nil(mt, err, "CreateOne error: %v", err)

			opts := options.DeleteOne().SetHint(bson.M{"x": 1})
			res, err := mt.Coll.DeleteOne(context.Background(), bson.D{{"x", 1}}, opts)
			assert.Nil(mt, err, "DeleteOne error: %v", err)
			assert.Equal(mt, int64(1), res.DeletedCount, "expected DeletedCount 1, got %v", res.DeletedCount)
		})
		mt.RunOpts("multikey map index", mtest.NewOptions().MinServerVersion("4.4"), func(mt *mtest.T) {
			opts := options.DeleteOne().SetHint(bson.M{"x": 1, "y": 1})
			_, err := mt.Coll.DeleteOne(context.Background(), bson.D{{"x", 0}}, opts)
			assert.Equal(mt, mongo.ErrMapForOrderedArgument{"hint"}, err, "expected error %v, got %v", mongo.ErrMapForOrderedArgument{"hint"}, err)
		})
	})
	mt.RunOpts("delete many", noClientOpts, func(mt *mtest.T) {
		mt.Run("found", func(mt *mtest.T) {
			initCollection(mt, mt.Coll)
			res, err := mt.Coll.DeleteMany(context.Background(), bson.D{{"x", bson.D{{"$gte", 3}}}})
			assert.Nil(mt, err, "DeleteMany error: %v", err)
			assert.Equal(mt, int64(3), res.DeletedCount, "expected DeletedCount 3, got %v", res.DeletedCount)
		})
		mt.Run("not found", func(mt *mtest.T) {
			initCollection(mt, mt.Coll)
			res, err := mt.Coll.DeleteMany(context.Background(), bson.D{{"x", bson.D{{"$lt", 1}}}})
			assert.Nil(mt, err, "DeleteMany error: %v", err)
			assert.Equal(mt, int64(0), res.DeletedCount, "expected DeletedCount 0, got %v", res.DeletedCount)
		})
		mt.RunOpts("not found with options", mtest.NewOptions().MinServerVersion("3.4"), func(mt *mtest.T) {
			initCollection(mt, mt.Coll)
			opts := options.DeleteMany().SetCollation(&options.Collation{Locale: "en_US"})
			res, err := mt.Coll.DeleteMany(context.Background(), bson.D{{"x", bson.D{{"$lt", 1}}}}, opts)
			assert.Nil(mt, err, "DeleteMany error: %v", err)
			assert.Equal(mt, int64(0), res.DeletedCount, "expected DeletedCount 0, got %v", res.DeletedCount)
		})
		mt.RunOpts("write error", mtest.NewOptions().MaxServerVersion("5.0.7"), func(mt *mtest.T) {
			// Deletes are not allowed on capped collections on MongoDB 5.0.6-. We use this
			// behavior to test the processing of write errors.
			cappedOpts := options.CreateCollection().SetCapped(true).SetSizeInBytes(64 * 1024)
			capped := mt.CreateCollection(mtest.Collection{
				Name:       "deleteMany_capped",
				CreateOpts: cappedOpts,
			}, true)
			_, err := capped.DeleteMany(context.Background(), bson.D{{"x", 1}})

			we, ok := err.(mongo.WriteException)
			assert.True(mt, ok, "expected error type %v, got %v", mongo.WriteException{}, err)
			numWriteErrors := len(we.WriteErrors)
			assert.Equal(mt, 1, len(we.WriteErrors), "expected 1 write error, got %v", numWriteErrors)
			gotCode := we.WriteErrors[0].Code
			assert.True(mt, gotCode == errorCappedCollDeleteLegacy || gotCode == errorCappedCollDelete,
				"expected error code %v or %v, got %v", errorCappedCollDeleteLegacy, errorCappedCollDelete, gotCode)
		})
		mt.RunOpts("write concern error", mtest.NewOptions().Topologies(mtest.ReplicaSet), func(mt *mtest.T) {
			// 2.6 server returns right away if the document doesn't exist
			filter := bson.D{{"x", 1}}
			_, err := mt.Coll.InsertOne(context.Background(), filter)
			assert.Nil(mt, err, "InsertOne error: %v", err)

			mt.CloneCollection(options.Collection().SetWriteConcern(impossibleWc))
			_, err = mt.Coll.DeleteMany(context.Background(), filter)
			we, ok := err.(mongo.WriteException)
			assert.True(mt, ok, "expected error type %v, got %v", mongo.WriteException{}, err)
			assert.NotNil(mt, we.WriteConcernError, "expected write concern error, got %+v", err)
		})
		mt.RunOpts("single key map index", mtest.NewOptions().MinServerVersion("4.4"), func(mt *mtest.T) {
			initCollection(mt, mt.Coll)
			indexView := mt.Coll.Indexes()
			_, err := indexView.CreateOne(context.Background(), mongo.IndexModel{
				Keys: bson.D{{"x", 1}},
			})
			assert.Nil(mt, err, "index CreateOne error: %v", err)

			opts := options.DeleteOne().SetHint(bson.M{"x": 1})
			res, err := mt.Coll.DeleteOne(context.Background(), bson.D{{"x", 1}}, opts)
			assert.Nil(mt, err, "DeleteOne error: %v", err)
			assert.Equal(mt, int64(1), res.DeletedCount, "expected DeletedCount 1, got %v", res.DeletedCount)
		})
		mt.RunOpts("multikey map index", mtest.NewOptions().MinServerVersion("4.4"), func(mt *mtest.T) {
			opts := options.DeleteMany().SetHint(bson.M{"x": 1, "y": 1})
			_, err := mt.Coll.DeleteMany(context.Background(), bson.D{{"x", 0}}, opts)
			assert.Equal(mt, mongo.ErrMapForOrderedArgument{"hint"}, err, "expected error %v, got %v", mongo.ErrMapForOrderedArgument{"hint"}, err)
		})
	})
	mt.RunOpts("update one", noClientOpts, func(mt *mtest.T) {
		mt.Run("empty update", func(mt *mtest.T) {
			_, err := mt.Coll.UpdateOne(context.Background(), bson.D{}, bson.D{})
			assert.NotNil(mt, err, "expected error, got nil")
		})
		mt.Run("found", func(mt *mtest.T) {
			initCollection(mt, mt.Coll)
			filter := bson.D{{"x", 1}}
			update := bson.D{{"$inc", bson.D{{"x", 1}}}}

			res, err := mt.Coll.UpdateOne(context.Background(), filter, update)
			assert.Nil(mt, err, "UpdateOne error: %v", err)
			assert.Equal(mt, int64(1), res.MatchedCount, "expected matched count 1, got %v", res.MatchedCount)
			assert.Equal(mt, int64(1), res.ModifiedCount, "expected matched count 1, got %v", res.ModifiedCount)
			assert.Nil(mt, res.UpsertedID, "expected upserted ID nil, got %v", res.UpsertedID)
		})
		mt.Run("not found", func(mt *mtest.T) {
			initCollection(mt, mt.Coll)
			filter := bson.D{{"x", 0}}
			update := bson.D{{"$inc", bson.D{{"x", 1}}}}

			res, err := mt.Coll.UpdateOne(context.Background(), filter, update)
			assert.Nil(mt, err, "UpdateOne error: %v", err)
			assert.Equal(mt, int64(0), res.MatchedCount, "expected matched count 0, got %v", res.MatchedCount)
			assert.Equal(mt, int64(0), res.ModifiedCount, "expected matched count 0, got %v", res.ModifiedCount)
			assert.Nil(mt, res.UpsertedID, "expected upserted ID nil, got %v", res.UpsertedID)
		})
		mt.Run("upsert", func(mt *mtest.T) {
			initCollection(mt, mt.Coll)
			filter := bson.D{{"x", 0}}
			update := bson.D{{"$inc", bson.D{{"x", 1}}}}

			res, err := mt.Coll.UpdateOne(context.Background(), filter, update, options.UpdateOne().SetUpsert(true))
			assert.Nil(mt, err, "UpdateOne error: %v", err)
			assert.Equal(mt, int64(0), res.MatchedCount, "expected matched count 0, got %v", res.MatchedCount)
			assert.Equal(mt, int64(0), res.ModifiedCount, "expected matched count 0, got %v", res.ModifiedCount)
			assert.NotNil(mt, res.UpsertedID, "expected upserted ID, got nil")
		})
		mt.Run("write error", func(mt *mtest.T) {
			filter := bson.D{{"_id", "foo"}}
			update := bson.D{{"$set", bson.D{{"_id", 3.14159}}}}
			_, err := mt.Coll.InsertOne(context.Background(), filter)
			assert.Nil(mt, err, "InsertOne error: %v", err)

			_, err = mt.Coll.UpdateOne(context.Background(), filter, update)
			we, ok := err.(mongo.WriteException)
			assert.True(mt, ok, "expected error type %v, got %v", mongo.WriteException{}, err)
			numWriteErrors := len(we.WriteErrors)
			assert.Equal(mt, 1, numWriteErrors, "expected 1 write error, got %v", numWriteErrors)
			gotCode := we.WriteErrors[0].Code
			assert.Equal(mt, errorModifiedID, gotCode, "expected error code %v, got %v", errorModifiedID, gotCode)
		})
		mt.RunOpts("write concern error", mtest.NewOptions().Topologies(mtest.ReplicaSet), func(mt *mtest.T) {
			// 2.6 returns right away if the document doesn't exist
			filter := bson.D{{"_id", "foo"}}
			update := bson.D{{"$set", bson.D{{"pi", 3.14159}}}}
			_, err := mt.Coll.InsertOne(context.Background(), filter)
			assert.Nil(mt, err, "InsertOne error: %v", err)

			mt.CloneCollection(options.Collection().SetWriteConcern(impossibleWc))
			_, err = mt.Coll.UpdateOne(context.Background(), filter, update)
			we, ok := err.(mongo.WriteException)
			assert.True(mt, ok, "expected error type %v, got %v", mongo.WriteException{}, err)
			assert.NotNil(mt, we.WriteConcernError, "expected write concern error, got %+v", err)
		})
		mt.RunOpts("special slice types", noClientOpts, func(mt *mtest.T) {
			// test special types that should be converted to a document for updates even though the underlying type is
			// a slice/array
			doc := bson.D{{"$set", bson.D{{"x", 2}}}}
			docBytes, err := bson.Marshal(doc)
			assert.Nil(mt, err, "Marshal error: %v", err)

			testCases := []struct {
				name   string
				update interface{}
			}{
				{"bson Document", bsoncore.Document(docBytes)},
				{"bson Raw", bson.Raw(docBytes)},
				{"bson D", doc},
				{"byte slice", docBytes},
			}
			for _, tc := range testCases {
				mt.Run(tc.name, func(mt *mtest.T) {
					filter := bson.D{{"x", 1}}
					_, err := mt.Coll.InsertOne(context.Background(), filter)
					assert.Nil(mt, err, "InsertOne error: %v", err)

					res, err := mt.Coll.UpdateOne(context.Background(), filter, tc.update)
					assert.Nil(mt, err, "UpdateOne error: %v", err)
					assert.Equal(mt, int64(1), res.MatchedCount, "expected matched count 1, got %v", res.MatchedCount)
					assert.Equal(mt, int64(1), res.ModifiedCount, "expected modified count 1, got %v", res.ModifiedCount)
				})
			}
		})
	})
	mt.RunOpts("update by id", noClientOpts, func(mt *mtest.T) {
		mt.Run("empty update", func(mt *mtest.T) {
			_, err := mt.Coll.UpdateByID(context.Background(), "foo", bson.D{})
			assert.NotNil(mt, err, "expected error, got nil")
		})
		mt.Run("nil id", func(mt *mtest.T) {
			_, err := mt.Coll.UpdateByID(context.Background(), nil, bson.D{{"$inc", bson.D{{"x", 1}}}})
			assert.True(mt, errors.Is(err, mongo.ErrNilValue), "expected %v, got %v", mongo.ErrNilValue, err)
		})
		mt.RunOpts("found", noClientOpts, func(mt *mtest.T) {
			testCases := []struct {
				name string
				id   interface{}
			}{
				{"objectID", bson.NewObjectID()},
				{"string", "foo"},
				{"int", 11},
			}
			for _, tc := range testCases {
				mt.Run(tc.name, func(mt *mtest.T) {
					doc := bson.D{{"_id", tc.id}, {"x", 1}}
					_, err := mt.Coll.InsertOne(context.Background(), doc)
					assert.Nil(mt, err, "InsertOne error: %v", err)

					update := bson.D{{"$inc", bson.D{{"x", 1}}}}

					res, err := mt.Coll.UpdateByID(context.Background(), tc.id, update)
					assert.Nil(mt, err, "UpdateByID error: %v", err)
					assert.Equal(mt, int64(1), res.MatchedCount, "expected matched count 1, got %v", res.MatchedCount)
					assert.Equal(mt, int64(1), res.ModifiedCount, "expected modified count 1, got %v", res.ModifiedCount)
					assert.Nil(mt, res.UpsertedID, "expected upserted ID nil, got %v", res.UpsertedID)
				})
			}
		})
		mt.Run("not found", func(mt *mtest.T) {
			id := bson.NewObjectID()
			doc := bson.D{{"_id", id}, {"x", 1}}
			_, err := mt.Coll.InsertOne(context.Background(), doc)
			assert.Nil(mt, err, "InsertOne error: %v", err)

			update := bson.D{{"$inc", bson.D{{"x", 1}}}}

			res, err := mt.Coll.UpdateByID(context.Background(), 0, update)
			assert.Nil(mt, err, "UpdateByID error: %v", err)
			assert.Equal(mt, int64(0), res.MatchedCount, "expected matched count 0, got %v", res.MatchedCount)
			assert.Equal(mt, int64(0), res.ModifiedCount, "expected modified count 0, got %v", res.ModifiedCount)
			assert.Nil(mt, res.UpsertedID, "expected upserted ID nil, got %v", res.UpsertedID)
		})
		mt.Run("upsert", func(mt *mtest.T) {
			doc := bson.D{{"_id", bson.NewObjectID()}, {"x", 1}}
			_, err := mt.Coll.InsertOne(context.Background(), doc)
			assert.Nil(mt, err, "InsertOne error: %v", err)

			update := bson.D{{"$inc", bson.D{{"x", 1}}}}

			id := "blah"
			res, err := mt.Coll.UpdateByID(context.Background(), id, update, options.UpdateOne().SetUpsert(true))
			assert.Nil(mt, err, "UpdateByID error: %v", err)
			assert.Equal(mt, int64(0), res.MatchedCount, "expected matched count 0, got %v", res.MatchedCount)
			assert.Equal(mt, int64(0), res.ModifiedCount, "expected modified count 0, got %v", res.ModifiedCount)
			assert.Equal(mt, res.UpsertedID, id, "expected upserted ID %v, got %v", id, res.UpsertedID)
		})
		mt.Run("write error", func(mt *mtest.T) {
			id := "foo"
			update := bson.D{{"$set", bson.D{{"_id", 3.14159}}}}
			_, err := mt.Coll.InsertOne(context.Background(), bson.D{{"_id", id}})
			assert.Nil(mt, err, "InsertOne error: %v", err)

			_, err = mt.Coll.UpdateByID(context.Background(), id, update)
			se, ok := err.(mongo.ServerError)
			assert.True(mt, ok, "expected ServerError, got %v", err)
			assert.True(mt, se.HasErrorCode(errorModifiedID), "expected error code %v, got %v", errorModifiedID, err)
		})
		mt.RunOpts("write concern error", mtest.NewOptions().Topologies(mtest.ReplicaSet), func(mt *mtest.T) {
			// 2.6 returns right away if the document doesn't exist
			id := "foo"
			update := bson.D{{"$set", bson.D{{"pi", 3.14159}}}}
			_, err := mt.Coll.InsertOne(context.Background(), bson.D{{"_id", id}})
			assert.Nil(mt, err, "InsertOne error: %v", err)

			mt.CloneCollection(options.Collection().SetWriteConcern(impossibleWc))
			_, err = mt.Coll.UpdateByID(context.Background(), id, update)
			we, ok := err.(mongo.WriteException)
			assert.True(mt, ok, "expected error type %v, got %v", mongo.WriteException{}, err)
			assert.NotNil(mt, we.WriteConcernError, "expected write concern error, got %+v", we)
		})
	})
	mt.RunOpts("update many", noClientOpts, func(mt *mtest.T) {
		mt.Run("empty update", func(mt *mtest.T) {
			_, err := mt.Coll.UpdateMany(context.Background(), bson.D{}, bson.D{})
			assert.NotNil(mt, err, "expected error, got nil")
		})
		mt.Run("found", func(mt *mtest.T) {
			initCollection(mt, mt.Coll)
			filter := bson.D{{"x", bson.D{{"$gte", 3}}}}
			update := bson.D{{"$inc", bson.D{{"x", 1}}}}

			res, err := mt.Coll.UpdateMany(context.Background(), filter, update)
			assert.Nil(mt, err, "UpdateMany error: %v", err)
			assert.Equal(mt, int64(3), res.MatchedCount, "expected matched count 3, got %v", res.MatchedCount)
			assert.Equal(mt, int64(3), res.ModifiedCount, "expected modified count 3, got %v", res.ModifiedCount)
			assert.Nil(mt, res.UpsertedID, "expected upserted ID nil, got %v", res.UpsertedID)
		})
		mt.Run("not found", func(mt *mtest.T) {
			initCollection(mt, mt.Coll)
			filter := bson.D{{"x", bson.D{{"$lt", 1}}}}
			update := bson.D{{"$inc", bson.D{{"x", 1}}}}

			res, err := mt.Coll.UpdateMany(context.Background(), filter, update)
			assert.Nil(mt, err, "UpdateMany error: %v", err)
			assert.Equal(mt, int64(0), res.MatchedCount, "expected matched count 0, got %v", res.MatchedCount)
			assert.Equal(mt, int64(0), res.ModifiedCount, "expected modified count 0, got %v", res.ModifiedCount)
			assert.Nil(mt, res.UpsertedID, "expected upserted ID nil, got %v", res.UpsertedID)
		})
		mt.Run("upsert", func(mt *mtest.T) {
			initCollection(mt, mt.Coll)
			filter := bson.D{{"x", bson.D{{"$lt", 1}}}}
			update := bson.D{{"$inc", bson.D{{"x", 1}}}}

			res, err := mt.Coll.UpdateMany(context.Background(), filter, update, options.UpdateMany().SetUpsert(true))
			assert.Nil(mt, err, "UpdateMany error: %v", err)
			assert.Equal(mt, int64(0), res.MatchedCount, "expected matched count 0, got %v", res.MatchedCount)
			assert.Equal(mt, int64(0), res.ModifiedCount, "expected modified count 0, got %v", res.ModifiedCount)
			assert.NotNil(mt, res.UpsertedID, "expected upserted ID, got nil")
		})
		mt.Run("write error", func(mt *mtest.T) {
			filter := bson.D{{"_id", "foo"}}
			update := bson.D{{"$set", bson.D{{"_id", 3.14159}}}}
			_, err := mt.Coll.InsertOne(context.Background(), filter)
			assert.Nil(mt, err, "InsertOne error: %v", err)

			_, err = mt.Coll.UpdateMany(context.Background(), filter, update)
			we, ok := err.(mongo.WriteException)
			assert.True(mt, ok, "expected error type %v, got %v", mongo.WriteException{}, err)
			numWriteErrors := len(we.WriteErrors)
			assert.Equal(mt, 1, numWriteErrors, "expected 1 write error, got %v", numWriteErrors)
			gotCode := we.WriteErrors[0].Code
			assert.Equal(mt, errorModifiedID, gotCode, "expected error code %v, got %v", errorModifiedID, gotCode)
		})
		mt.RunOpts("write concern error", mtest.NewOptions().Topologies(mtest.ReplicaSet), func(mt *mtest.T) {
			// 2.6 returns right away if the document doesn't exist
			filter := bson.D{{"_id", "foo"}}
			update := bson.D{{"$set", bson.D{{"pi", 3.14159}}}}
			_, err := mt.Coll.InsertOne(context.Background(), filter)
			assert.Nil(mt, err, "InsertOne error: %v", err)

			mt.CloneCollection(options.Collection().SetWriteConcern(impossibleWc))
			_, err = mt.Coll.UpdateMany(context.Background(), filter, update)
			we, ok := err.(mongo.WriteException)
			assert.True(mt, ok, "expected error type %v, got %v", mongo.WriteException{}, err)
			assert.NotNil(mt, we.WriteConcernError, "expected write concern error, got %+v", we)
		})
	})
	mt.RunOpts("replace one", noClientOpts, func(mt *mtest.T) {
		mt.Run("found", func(mt *mtest.T) {
			initCollection(mt, mt.Coll)
			filter := bson.D{{"x", 1}}
			replacement := bson.D{{"y", 1}}

			res, err := mt.Coll.ReplaceOne(context.Background(), filter, replacement)
			assert.Nil(mt, err, "ReplaceOne error: %v", err)
			assert.Equal(mt, int64(1), res.MatchedCount, "expected matched count 1, got %v", res.MatchedCount)
			assert.Equal(mt, int64(1), res.ModifiedCount, "expected modified count 1, got %v", res.ModifiedCount)
			assert.Nil(mt, res.UpsertedID, "expected upserted ID nil, got %v", res.UpsertedID)
		})
		mt.Run("not found", func(mt *mtest.T) {
			initCollection(mt, mt.Coll)
			filter := bson.D{{"x", 0}}
			replacement := bson.D{{"y", 1}}

			res, err := mt.Coll.ReplaceOne(context.Background(), filter, replacement)
			assert.Nil(mt, err, "ReplaceOne error: %v", err)
			assert.Equal(mt, int64(0), res.MatchedCount, "expected matched count 0, got %v", res.MatchedCount)
			assert.Equal(mt, int64(0), res.ModifiedCount, "expected modified count 0, got %v", res.ModifiedCount)
			assert.Nil(mt, res.UpsertedID, "expected upserted ID nil, got %v", res.UpsertedID)
		})
		mt.Run("upsert", func(mt *mtest.T) {
			initCollection(mt, mt.Coll)
			filter := bson.D{{"x", 0}}
			replacement := bson.D{{"y", 1}}

			res, err := mt.Coll.ReplaceOne(context.Background(), filter, replacement, options.Replace().SetUpsert(true))
			assert.Nil(mt, err, "ReplaceOne error: %v", err)
			assert.Equal(mt, int64(0), res.MatchedCount, "expected matched count 0, got %v", res.MatchedCount)
			assert.Equal(mt, int64(0), res.ModifiedCount, "expected modified count 0, got %v", res.ModifiedCount)
			assert.NotNil(mt, res.UpsertedID, "expected upserted ID, got nil")
		})
		mt.Run("write error", func(mt *mtest.T) {
			filter := bson.D{{"_id", "foo"}}
			replacement := bson.D{{"_id", 3.14159}}
			_, err := mt.Coll.InsertOne(context.Background(), filter)
			assert.Nil(mt, err, "InsertOne error: %v", err)

			_, err = mt.Coll.ReplaceOne(context.Background(), filter, replacement)
			we, ok := err.(mongo.WriteException)
			assert.True(mt, ok, "expected error type %v, got %v", mongo.WriteException{}, err)
			numWriteErrors := len(we.WriteErrors)
			assert.Equal(mt, 1, numWriteErrors, "expected 1 write error, got %v", numWriteErrors)
			gotCode := we.WriteErrors[0].Code
			assert.True(mt, gotCode == errorModifiedID || gotCode == errorModifiedIDLegacy,
				"expected error code %v or %v, got %v", errorModifiedID, errorModifiedIDLegacy, gotCode)
		})
		mt.RunOpts("write concern error", mtest.NewOptions().Topologies(mtest.ReplicaSet), func(mt *mtest.T) {
			// 2.6 returns right away if document doesn't exist
			filter := bson.D{{"_id", "foo"}}
			replacement := bson.D{{"pi", 3.14159}}
			_, err := mt.Coll.InsertOne(context.Background(), filter)
			assert.Nil(mt, err, "InsertOne error: %v", err)

			mt.CloneCollection(options.Collection().SetWriteConcern(impossibleWc))
			_, err = mt.Coll.ReplaceOne(context.Background(), filter, replacement)
			we, ok := err.(mongo.WriteException)
			assert.True(mt, ok, "expected error type %v, got %v", mongo.WriteException{}, err)
			assert.NotNil(mt, we.WriteConcernError, "expected write concern error, got nil")
		})
	})
	mt.RunOpts("aggregate", noClientOpts, func(mt *mtest.T) {
		mt.Run("success", func(mt *mtest.T) {
			initCollection(mt, mt.Coll)
			pipeline := bson.A{
				bson.D{{"$match", bson.D{{"x", bson.D{{"$gte", 2}}}}}},
				bson.D{{"$project", bson.D{
					{"_id", 0},
					{"x", 1},
				}}},
				bson.D{{"$sort", bson.D{{"x", 1}}}},
			}
			cursor, err := mt.Coll.Aggregate(context.Background(), pipeline)
			assert.Nil(mt, err, "Aggregate error: %v", err)

			for i := 2; i < 5; i++ {
				assert.True(mt, cursor.Next(context.Background()), "expected Next true, got false (i=%v)", i)
				elems, _ := cursor.Current.Elements()
				assert.Equal(mt, 1, len(elems), "expected doc with 1 element, got %v", cursor.Current)

				num, err := cursor.Current.LookupErr("x")
				assert.Nil(mt, err, "x not found in document %v", cursor.Current)
				assert.Equal(mt, bson.TypeInt32, num.Type, "expected 'x' type %v, got %v", bson.TypeInt32, num.Type)
				assert.Equal(mt, int32(i), num.Int32(), "expected x value %v, got %v", i, num.Int32())
			}
		})
		mt.RunOpts("index hint", mtest.NewOptions().MinServerVersion("3.6"), func(mt *mtest.T) {
			hint := bson.D{{"x", 1}}
			testAggregateWithOptions(mt, true, options.Aggregate().SetHint(hint))
		})
		mt.Run("options", func(mt *mtest.T) {
			testAggregateWithOptions(mt, false, options.Aggregate().SetAllowDiskUse(true))
		})
		mt.RunOpts("single key map hint", mtest.NewOptions().MinServerVersion("3.6"), func(mt *mtest.T) {
			hint := bson.M{"x": 1}
			testAggregateWithOptions(mt, true, options.Aggregate().SetHint(hint))
		})
		mt.RunOpts("multikey map hint", mtest.NewOptions().MinServerVersion("3.6"), func(mt *mtest.T) {
			pipeline := mongo.Pipeline{bson.D{{"$out", mt.Coll.Name()}}}
			cursor, err := mt.Coll.Aggregate(context.Background(), pipeline, options.Aggregate().SetHint(bson.M{"x": 1, "y": 1}))
			assert.Nil(mt, cursor, "expected cursor nil, got %v", cursor)
			assert.Equal(mt, mongo.ErrMapForOrderedArgument{"hint"}, err, "expected error %v, got %v", mongo.ErrMapForOrderedArgument{"hint"}, err)
		})
		wcCollOpts := options.Collection().SetWriteConcern(impossibleWc)
		wcTestOpts := mtest.NewOptions().Topologies(mtest.ReplicaSet).MinServerVersion("3.6").CollectionOptions(wcCollOpts)
		mt.RunOpts("write concern error", wcTestOpts, func(mt *mtest.T) {
			pipeline := mongo.Pipeline{{{"$out", mt.Coll.Name()}}}
			cursor, err := mt.Coll.Aggregate(context.Background(), pipeline)
			assert.Nil(mt, cursor, "expected cursor nil, got %v", cursor)
			_, ok := err.(mongo.WriteConcernError)
			assert.True(mt, ok, "expected error type %v, got %v", mongo.WriteConcernError{}, err)
		})
		mt.Run("getMore commands are monitored", func(mt *mtest.T) {
			initCollection(mt, mt.Coll)
			assertGetMoreCommandsAreMonitored(mt, "aggregate", func() (*mongo.Cursor, error) {
				return mt.Coll.Aggregate(context.Background(), mongo.Pipeline{}, options.Aggregate().SetBatchSize(3))
			})
		})
		mt.Run("killCursors commands are monitored", func(mt *mtest.T) {
			initCollection(mt, mt.Coll)
			assertKillCursorsCommandsAreMonitored(mt, "aggregate", func() (*mongo.Cursor, error) {
				return mt.Coll.Aggregate(context.Background(), mongo.Pipeline{}, options.Aggregate().SetBatchSize(3))
			})
		})
		mt.Run("Custom", func(mt *mtest.T) {
			// Custom options should be a BSON map of option names to Marshalable option values.
			// We use "allowDiskUse" as an example.
			customOpts := bson.M{"allowDiskUse": true}
			opts := options.Aggregate().SetCustom(customOpts)

			// Run aggregate with custom options set.
			mt.ClearEvents()
			_, err := mt.Coll.Aggregate(context.Background(), mongo.Pipeline{}, opts)
			assert.Nil(mt, err, "Aggregate error: %v", err)

			// Assert that custom option is passed to the aggregate expression.
			evt := mt.GetStartedEvent()
			assert.Equal(mt, "aggregate", evt.CommandName, "expected command 'aggregate' got, %q", evt.CommandName)

			aduVal, err := evt.Command.LookupErr("allowDiskUse")
			assert.Nil(mt, err, "expected field 'allowDiskUse' in started command not found")
			adu, ok := aduVal.BooleanOK()
			assert.True(mt, ok, "expected field 'allowDiskUse' to be boolean, got %v", aduVal.Type.String())
			assert.True(mt, adu, "expected field 'allowDiskUse' to be true, got false")
		})
	})
	mt.RunOpts("count documents", noClientOpts, func(mt *mtest.T) {
		mt.Run("success", func(mt *mtest.T) {
			testCases := []struct {
				name     string
				filter   bson.D
				opts     *options.CountOptionsBuilder
				count    int64
				testOpts *mtest.Options
			}{
				{"no filter", bson.D{}, nil, 5, mtest.NewOptions()},
				{"filter", bson.D{{"x", bson.D{{"$gt", 2}}}}, nil, 3, mtest.NewOptions()},
				{"limit", bson.D{}, options.Count().SetLimit(3), 3, mtest.NewOptions()},
				{"skip", bson.D{}, options.Count().SetSkip(3), 2, mtest.NewOptions()},
				{"single key map hint", bson.D{}, options.Count().SetHint(bson.M{"x": 1}), 5,
					mtest.NewOptions().MinServerVersion("3.6")},
			}
			for _, tc := range testCases {
				mt.RunOpts(tc.name, tc.testOpts, func(mt *mtest.T) {
					initCollection(mt, mt.Coll)
					indexView := mt.Coll.Indexes()
					_, err := indexView.CreateOne(context.Background(), mongo.IndexModel{
						Keys: bson.D{{"x", 1}},
					})
					assert.Nil(mt, err, "CreateOne error: %v", err)

					count, err := mt.Coll.CountDocuments(context.Background(), tc.filter, tc.opts)
					assert.Nil(mt, err, "CountDocuments error: %v", err)
					assert.Equal(mt, tc.count, count, "expected count %v, got %v", tc.count, count)
				})
			}
		})
		mt.Run("multikey map hint", func(mt *mtest.T) {
			opts := options.Count().SetHint(bson.M{"x": 1, "y": 1})
			_, err := mt.Coll.CountDocuments(context.Background(), bson.D{}, opts)
			assert.Equal(mt, mongo.ErrMapForOrderedArgument{"hint"}, err, "expected error %v, got %v", mongo.ErrMapForOrderedArgument{"hint"}, err)
		})
	})
	mt.RunOpts("estimated document count", noClientOpts, func(mt *mtest.T) {
		testCases := []struct {
			name  string
			opts  *options.EstimatedDocumentCountOptionsBuilder
			count int64
		}{
			{"no options", nil, 5},
			{"options", options.EstimatedDocumentCount().SetComment("1"), 5},
		}
		for _, tc := range testCases {
			mt.Run(tc.name, func(mt *mtest.T) {
				initCollection(mt, mt.Coll)
				count, err := mt.Coll.EstimatedDocumentCount(context.Background(), tc.opts)
				assert.Nil(mt, err, "EstimatedDocumentCount error: %v", err)
				assert.Equal(mt, tc.count, count, "expected count %v, got %v", tc.count, count)
			})
		}
	})
	mt.RunOpts("distinct", noClientOpts, func(mt *mtest.T) {
		all := []int32{1, 2, 3, 4, 5}

		testCases := []struct {
			name   string
			filter bson.D
			opts   *options.DistinctOptionsBuilder
			want   []int32
		}{
			{"no options", bson.D{}, nil, all},
			{"filter", bson.D{{"x", bson.D{{"$gt", 2}}}}, nil, all[2:]},
			{"options", bson.D{}, options.Distinct().SetComment("1"), all},
		}
		for _, tc := range testCases {
			mt.Run(tc.name, func(mt *mtest.T) {
				initCollection(mt, mt.Coll)
				res := mt.Coll.Distinct(context.Background(), "x", tc.filter, tc.opts)
				assert.Nil(mt, res.Err(), "Distinct error: %v", res.Err())

				var got []int32
				err := res.Decode(&got)
				assert.NoError(t, err)

				assert.EqualValues(mt, tc.want, got, "expected result %v, got %v", tc.want, got)
			})
		}

		collOpts := options.Collection().SetBSONOptions(&options.BSONOptions{AllowTruncatingDoubles: true})
		opts := mtest.NewOptions().CollectionOptions(collOpts)

		mt.RunOpts("distinct with bson options", opts, func(mt *mtest.T) {
			_, err := mt.Coll.InsertOne(context.Background(), bson.D{{"y", 1.7}})
			assert.NoError(mt, err, "failed to insert double")

			filter := bson.D{{"y", bson.D{{"$gt", 1}}}}

			res := mt.Coll.Distinct(context.Background(), "y", filter)
			assert.Nil(mt, res.Err(), "Distinct error: %v", res.Err())

			var got []int32
			assert.NoError(t, res.Decode(&got))

			assert.EqualValues(mt, []int32{1}, got)
		})
	})
	mt.RunOpts("find", noClientOpts, func(mt *mtest.T) {
		mt.Run("found", func(mt *mtest.T) {
			initCollection(mt, mt.Coll)
			cursor, err := mt.Coll.Find(context.Background(), bson.D{}, options.Find().SetSort(bson.D{{"x", 1}}))
			assert.Nil(mt, err, "Find error: %v", err)

			results := make([]int, 0, 5)
			for cursor.Next(context.Background()) {
				x, err := cursor.Current.LookupErr("x")
				assert.Nil(mt, err, "x not found in document %v", cursor.Current)
				assert.Equal(mt, bson.TypeInt32, x.Type, "expected x type %v, got %v", bson.TypeInt32, x.Type)
				results = append(results, int(x.Int32()))
			}
			assert.Equal(mt, 5, len(results), "expected 5 results, got %v", len(results))
			expected := []int{1, 2, 3, 4, 5}
			assert.Equal(mt, expected, results, "expected results %v, got %v", expected, results)
		})
		mt.Run("limit and batch size", func(mt *mtest.T) {
			initCollection(mt, mt.Coll)
			for _, batchSize := range []int32{2, 3, 4} {
				cursor, err := mt.Coll.Find(context.Background(), bson.D{}, options.Find().SetLimit(3).SetBatchSize(batchSize))
				assert.Nil(mt, err, "Find error: %v", err)

				numReceived := 0
				for cursor.Next(context.Background()) {
					numReceived++
				}
				err = cursor.Err()
				assert.Nil(mt, err, "cursor error: %v", err)
				assert.Equal(mt, 3, numReceived, "expected 3 results, got %v", numReceived)
			}
		})
		mt.Run("not found", func(mt *mtest.T) {
			initCollection(mt, mt.Coll)
			cursor, err := mt.Coll.Find(context.Background(), bson.D{{"x", 6}})
			assert.Nil(mt, err, "Find error: %v", err)
			assert.False(mt, cursor.Next(context.Background()), "expected no documents, found %v", cursor.Current)
		})
		mt.Run("invalid identifier error", func(mt *mtest.T) {
			cursor, err := mt.Coll.Find(context.Background(), bson.D{{"$foo", 1}})
			assert.NotNil(mt, err, "expected error for invalid identifier, got nil")
			assert.Nil(mt, cursor, "expected nil cursor, got %v", cursor)
		})
		mt.Run("negative limit", func(mt *mtest.T) {
			initCollection(mt, mt.Coll)
			c, err := mt.Coll.Find(context.Background(), bson.D{}, options.Find().SetLimit(-2))
			assert.Nil(mt, err, "Find error: %v", err)
			// single batch returned so cursor should have ID 0
			assert.Equal(mt, int64(0), c.ID(), "expected cursor ID 0, got %v", c.ID())

			var numDocs int
			for c.Next(context.Background()) {
				numDocs++
			}
			assert.Equal(mt, 2, numDocs, "expected 2 documents, got %v", numDocs)
		})
		mt.Run("exhaust cursor", func(mt *mtest.T) {
			initCollection(mt, mt.Coll)
			c, err := mt.Coll.Find(context.Background(), bson.D{})
			assert.Nil(mt, err, "Find error: %v", err)

			var numDocs int
			for c.Next(context.Background()) {
				numDocs++
			}
			assert.Equal(mt, 5, numDocs, "expected 5 documents, got %v", numDocs)
			err = c.Close(context.Background())
			assert.Nil(mt, err, "Close error: %v", err)
		})
		mt.Run("hint", func(mt *mtest.T) {
			_, err := mt.Coll.Find(context.Background(), bson.D{}, options.Find().SetHint("_id_"))
			assert.Nil(mt, err, "Find error with string hint: %v", err)

			_, err = mt.Coll.Find(context.Background(), bson.D{}, options.Find().SetHint(bson.D{{"_id", 1}}))
			assert.Nil(mt, err, "Find error with document hint: %v", err)

			_, err = mt.Coll.Find(context.Background(), bson.D{}, options.Find().SetHint("foobar"))
			_, ok := err.(mongo.CommandError)
			assert.True(mt, ok, "expected error type %v, got %v", mongo.CommandError{}, err)

			_, err = mt.Coll.Find(context.Background(), bson.D{}, options.Find().SetHint(bson.M{"_id": 1}))
			assert.Nil(mt, err, "Find error with single key map hint: %v", err)

			_, err = mt.Coll.Find(context.Background(), bson.D{}, options.Find().SetHint(bson.M{"_id": 1, "x": 1}))
			assert.Equal(mt, mongo.ErrMapForOrderedArgument{"hint"}, err, "expected error %v, got %v", mongo.ErrMapForOrderedArgument{"hint"}, err)
		})
		mt.Run("sort", func(mt *mtest.T) {
			_, err := mt.Coll.Find(context.Background(), bson.D{}, options.Find().SetSort(bson.M{"_id": 1}))
			assert.Nil(mt, err, "Find error with single key map sort: %v", err)
			_, err = mt.Coll.Find(context.Background(), bson.D{}, options.Find().SetSort(bson.M{"_id": 1, "x": 1}))
			assert.Equal(mt, mongo.ErrMapForOrderedArgument{"sort"}, err, "expected error %v, got %v", mongo.ErrMapForOrderedArgument{"sort"}, err)
		})
		mt.Run("limit and batch size and skip", func(mt *mtest.T) {
			testCases := []struct {
				limit     int64
				batchSize int32
				skip      int64
				name      string
			}{
				{
					99, 100, 10,
					"case 1",
				},
				{
					100, 100, 20,
					"case 2",
				},
				{
					80, 20, 90,
					"case 3",
				},
				{
					201, 201, 0,
					"case 4",
				},
				{
					100, 200, 120,
					"case 5",
				},
			}
			for _, tc := range testCases {
				mt.Run(tc.name, func(mt *mtest.T) {
					var insertDocs []interface{}
					for i := 1; i <= 201; i++ {
						insertDocs = append(insertDocs, bson.D{{"x", int32(i)}})
					}

					_, err := mt.Coll.InsertMany(context.Background(), insertDocs)
					assert.Nil(mt, err, "InsertMany error for initial data: %v", err)

					findOptions := options.Find().SetLimit(tc.limit).SetBatchSize(tc.batchSize).
						SetSkip(tc.skip)
					cursor, err := mt.Coll.Find(context.Background(), bson.D{}, findOptions)
					assert.Nil(mt, err, "Find error: %v", err)

					var docs []interface{}
					err = cursor.All(context.Background(), &docs)
					assert.Nil(mt, err, "All error: %v", err)
					if (201 - tc.skip) < tc.limit {
						assert.Equal(mt, int(201-tc.skip), len(docs), "expected number of docs to be %v, got %v", int(201-tc.skip), len(docs))
					} else {
						assert.Equal(mt, int(tc.limit), len(docs), "expected number of docs to be %v, got %v", tc.limit, len(docs))
					}
				})
			}
		})
		mt.Run("unset batch size does not surpass limit", func(mt *mtest.T) {
			testCases := []struct {
				limit int64
				name  string
			}{
				{
					99,
					"99",
				},
				{
					100,
					"100",
				},
				{
					101,
					"101",
				},
				{
					200,
					"200",
				},
			}
			for _, tc := range testCases {
				mt.Run(tc.name, func(mt *mtest.T) {
					var insertDocs []interface{}
					for i := 1; i <= 201; i++ {
						insertDocs = append(insertDocs, bson.D{{"x", int32(i)}})
					}

					_, err := mt.Coll.InsertMany(context.Background(), insertDocs)
					assert.Nil(mt, err, "InsertMany error for initial data: %v", err)
					opts := options.Find().SetSkip(0).SetLimit(tc.limit)
					cursor, err := mt.Coll.Find(context.Background(), bson.D{}, opts)
					assert.Nil(mt, err, "Find error with limit %v: %v", tc.limit, err)

					var docs []interface{}
					err = cursor.All(context.Background(), &docs)
					assert.Nil(mt, err, "All error with limit %v: %v", tc.limit, err)

					assert.Equal(mt, int(tc.limit), len(docs), "expected number of docs to be %v, got %v", tc.limit, len(docs))
				})
			}
		})
		mt.Run("getMore commands are monitored", func(mt *mtest.T) {
			initCollection(mt, mt.Coll)
			assertGetMoreCommandsAreMonitored(mt, "find", func() (*mongo.Cursor, error) {
				return mt.Coll.Find(context.Background(), bson.D{}, options.Find().SetBatchSize(3))
			})
		})
		mt.Run("killCursors commands are monitored", func(mt *mtest.T) {
			initCollection(mt, mt.Coll)
			assertKillCursorsCommandsAreMonitored(mt, "find", func() (*mongo.Cursor, error) {
				return mt.Coll.Find(context.Background(), bson.D{}, options.Find().SetBatchSize(3))
			})
		})
	})
	mt.RunOpts("find one", noClientOpts, func(mt *mtest.T) {
		mt.Run("limit", func(mt *mtest.T) {
			err := mt.Coll.FindOne(context.Background(), bson.D{}).Err()
			assert.Equal(mt, mongo.ErrNoDocuments, err, "expected error %v, got %v", mongo.ErrNoDocuments, err)

			started := mt.GetStartedEvent()
			assert.NotNil(mt, started, "expected CommandStartedEvent, got nil")
			limitVal, err := started.Command.LookupErr("limit")
			assert.Nil(mt, err, "limit not found in command")
			limit := limitVal.Int64()
			assert.Equal(mt, int64(1), limit, "expected limit 1, got %v", limit)
		})
		mt.Run("found", func(mt *mtest.T) {
			initCollection(mt, mt.Coll)
			res, err := mt.Coll.FindOne(context.Background(), bson.D{{"x", 1}}).Raw()
			assert.Nil(mt, err, "FindOne error: %v", err)

			x, err := res.LookupErr("x")
			assert.Nil(mt, err, "x not found in document %v", res)
			assert.Equal(mt, bson.TypeInt32, x.Type, "expected x type %v, got %v", bson.TypeInt32, x.Type)
			got := x.Int32()
			assert.Equal(mt, int32(1), got, "expected x value 1, got %v", got)
		})
		mt.RunOpts("options", mtest.NewOptions().MinServerVersion("3.4"), func(mt *mtest.T) {
			initCollection(mt, mt.Coll)

			// Create an index for Hint, Max, and Min options
			indexView := mt.Coll.Indexes()
			indexName, err := indexView.CreateOne(context.Background(), mongo.IndexModel{
				Keys: bson.D{{"x", 1}},
			})
			assert.Nil(mt, err, "CreateOne error: %v", err)

			mt.ClearEvents()
			expectedComment := "here's a query for ya"
			// SetCursorType is excluded because tailable cursors can't be used with limit -1,
			// which all FindOne operations set, and the nontailable setting isn't passed to the
			// operation layer
			// SetMaxAwaitTime affects the cursor and not the server command, so it can't be checked
			// SetCursorTime and setMaxAwaitTime will be deprecated in GODRIVER-1775
			opts := options.FindOne().
				SetAllowPartialResults(true).
				SetCollation(&options.Collation{Locale: "en_US"}).
				SetComment(expectedComment).
				SetHint(indexName).
				SetMax(bson.D{{"x", int32(5)}}).
				SetMin(bson.D{{"x", int32(0)}}).
				SetOplogReplay(false).
				SetProjection(bson.D{{"x", int32(1)}}).
				SetReturnKey(false).
				SetShowRecordID(false).
				SetSkip(0).
				SetSort(bson.D{{"x", int32(1)}})
			res, err := mt.Coll.FindOne(context.Background(), bson.D{}, opts).Raw()
			assert.Nil(mt, err, "FindOne error: %v", err)

			x, err := res.LookupErr("x")
			assert.Nil(mt, err, "x not found in document %v", res)
			assert.Equal(mt, bson.TypeInt32, x.Type, "expected x type %v, got %v", bson.TypeInt32, x.Type)
			got := x.Int32()
			assert.Equal(mt, int32(1), got, "expected x value 1, got %v", got)

			optionsDoc := bsoncore.NewDocumentBuilder().
				AppendBoolean("allowPartialResults", true).
				StartDocument("collation").AppendString("locale", "en_US").FinishDocument().
				AppendString("comment", expectedComment).
				AppendString("hint", indexName).
				StartDocument("max").AppendInt32("x", 5).FinishDocument().
				StartDocument("min").AppendInt32("x", 0).FinishDocument().
				AppendBoolean("oplogReplay", false).
				StartDocument("projection").AppendInt32("x", 1).FinishDocument().
				AppendBoolean("returnKey", false).
				AppendBoolean("showRecordId", false).
				AppendInt64("skip", 0).
				StartDocument("sort").AppendInt32("x", 1).FinishDocument().
				Build()

			started := mt.GetStartedEvent()
			assert.NotNil(mt, started, "expected CommandStartedEvent, got nil")

			if err := compareDocs(mt, bson.Raw(optionsDoc), started.Command); err != nil {
				mt.Fatalf("options mismatch: %v", err)
			}

		})
		mt.Run("not found", func(mt *mtest.T) {
			initCollection(mt, mt.Coll)
			err := mt.Coll.FindOne(context.Background(), bson.D{{"x", 6}}).Err()
			assert.Equal(mt, mongo.ErrNoDocuments, err, "expected error %v, got %v", mongo.ErrNoDocuments, err)
		})
		mt.RunOpts("maps for sorted opts", noClientOpts, func(mt *mtest.T) {
			testCases := []struct {
				name     string
				opts     *options.FindOneOptionsBuilder
				errParam string
			}{
				{"single key hint", options.FindOne().SetHint(bson.M{"x": 1}), ""},
				{"multikey hint", options.FindOne().SetHint(bson.M{"x": 1, "y": 1}), "hint"},
				{"single key sort", options.FindOne().SetSort(bson.M{"x": 1}), ""},
				{"multikey sort", options.FindOne().SetSort(bson.M{"x": 1, "y": 1}), "sort"},
			}
			for _, tc := range testCases {
				mt.Run(tc.name, func(mt *mtest.T) {
					initCollection(mt, mt.Coll)
					indexView := mt.Coll.Indexes()
					_, err := indexView.CreateOne(context.Background(), mongo.IndexModel{
						Keys: bson.D{{"x", 1}},
					})
					assert.Nil(mt, err, "CreateOne error: %v", err)

					res, err := mt.Coll.FindOne(context.Background(), bson.D{{"x", 1}}, tc.opts).Raw()

					if tc.errParam != "" {
						expErr := mongo.ErrMapForOrderedArgument{tc.errParam}
						assert.Equal(mt, expErr, err, "expected error %v, got %v", expErr, err)
						return
					}

					assert.Nil(mt, err, "FindOne error: %v", err)
					x, err := res.LookupErr("x")
					assert.Nil(mt, err, "x not found in document %v", res)
					got, ok := x.Int32OK()
					assert.True(mt, ok, "expected x type int32, got %v", x.Type)
					assert.Equal(mt, int32(1), got, "expected x value 1, got %v", got)
				})
			}
		})
	})
	mt.RunOpts("find one and delete", noClientOpts, func(mt *mtest.T) {
		mt.Run("found", func(mt *mtest.T) {
			initCollection(mt, mt.Coll)
			res, err := mt.Coll.FindOneAndDelete(context.Background(), bson.D{{"x", 3}}).Raw()
			assert.Nil(mt, err, "FindOneAndDelete error: %v", err)

			elem, err := res.LookupErr("x")
			assert.Nil(mt, err, "x not found in result %v", res)
			assert.Equal(mt, bson.TypeInt32, elem.Type, "expected x type %v, got %v", bson.TypeInt32, elem.Type)
			x := elem.Int32()
			assert.Equal(mt, int32(3), x, "expected x value 3, got %v", x)
		})
		mt.Run("found ignore result", func(mt *mtest.T) {
			initCollection(mt, mt.Coll)
			err := mt.Coll.FindOneAndDelete(context.Background(), bson.D{{"x", 3}}).Err()
			assert.Nil(mt, err, "FindOneAndDelete error: %v", err)
		})
		mt.Run("not found", func(mt *mtest.T) {
			initCollection(mt, mt.Coll)
			err := mt.Coll.FindOneAndDelete(context.Background(), bson.D{{"x", 6}}).Err()
			assert.Equal(mt, mongo.ErrNoDocuments, err, "expected error %v, got %v", mongo.ErrNoDocuments, err)
		})
		mt.RunOpts("maps for sorted opts", noClientOpts, func(mt *mtest.T) {
			testCases := []struct {
				name     string
				opts     *options.FindOneAndDeleteOptionsBuilder
				errParam string
			}{
				{"single key hint", options.FindOneAndDelete().SetHint(bson.M{"x": 1}), ""},
				{"multikey hint", options.FindOneAndDelete().SetHint(bson.M{"x": 1, "y": 1}), "hint"},
				{"single key sort", options.FindOneAndDelete().SetSort(bson.M{"x": 1}), ""},
				{"multikey sort", options.FindOneAndDelete().SetSort(bson.M{"x": 1, "y": 1}), "sort"},
			}
			for _, tc := range testCases {
				mt.RunOpts(tc.name, mtest.NewOptions().MinServerVersion("4.4"), func(mt *mtest.T) {
					initCollection(mt, mt.Coll)
					indexView := mt.Coll.Indexes()
					_, err := indexView.CreateOne(context.Background(), mongo.IndexModel{
						Keys: bson.D{{"x", 1}},
					})
					assert.Nil(mt, err, "CreateOne error: %v", err)

					res, err := mt.Coll.FindOneAndDelete(context.Background(), bson.D{{"x", 1}}, tc.opts).Raw()

					if tc.errParam != "" {
						expErr := mongo.ErrMapForOrderedArgument{tc.errParam}
						assert.Equal(mt, expErr, err, "expected error %v, got %v", expErr, err)
						return
					}

					assert.Nil(mt, err, "FindOneAndDelete error: %v", err)
					x, err := res.LookupErr("x")
					assert.Nil(mt, err, "x not found in document %v", res)
					got, ok := x.Int32OK()
					assert.True(mt, ok, "expected x type int32, got %v", x.Type)
					assert.Equal(mt, int32(1), got, "expected x value 1, got %v", got)
				})
			}
		})
		wcCollOpts := options.Collection().SetWriteConcern(impossibleWc)
		wcTestOpts := mtest.NewOptions().CollectionOptions(wcCollOpts).Topologies(mtest.ReplicaSet).MinServerVersion("3.2")
		mt.RunOpts("write concern error", wcTestOpts, func(mt *mtest.T) {
			err := mt.Coll.FindOneAndDelete(context.Background(), bson.D{{"x", 3}}).Err()
			we, ok := err.(mongo.WriteException)
			assert.True(mt, ok, "expected error type %v, got %v", mongo.WriteException{}, err)
			assert.NotNil(mt, we.WriteConcernError, "expected write concern error, got %v", err)
		})
	})
	mt.RunOpts("find one and replace", noClientOpts, func(mt *mtest.T) {
		mt.Run("found", func(mt *mtest.T) {
			initCollection(mt, mt.Coll)
			filter := bson.D{{"x", 3}}
			replacement := bson.D{{"y", 3}}

			res, err := mt.Coll.FindOneAndReplace(context.Background(), filter, replacement).Raw()
			assert.Nil(mt, err, "FindOneAndReplace error: %v", err)
			elem, err := res.LookupErr("x")
			assert.Nil(mt, err, "x not found in result %v", res)
			assert.Equal(mt, bson.TypeInt32, elem.Type, "expected x type %v, got %v", bson.TypeInt32, elem.Type)
			x := elem.Int32()
			assert.Equal(mt, int32(3), x, "expected x value 3, got %v", x)
		})
		mt.Run("found ignore result", func(mt *mtest.T) {
			initCollection(mt, mt.Coll)
			filter := bson.D{{"x", 3}}
			replacement := bson.D{{"y", 3}}

			err := mt.Coll.FindOneAndReplace(context.Background(), filter, replacement).Err()
			assert.Nil(mt, err, "FindOneAndReplace error: %v", err)
		})
		mt.Run("not found", func(mt *mtest.T) {
			initCollection(mt, mt.Coll)
			filter := bson.D{{"x", 6}}
			replacement := bson.D{{"y", 6}}

			err := mt.Coll.FindOneAndReplace(context.Background(), filter, replacement).Err()
			assert.Equal(mt, mongo.ErrNoDocuments, err, "expected error %v, got %v", mongo.ErrNoDocuments, err)
		})
		mt.RunOpts("maps for sorted opts", noClientOpts, func(mt *mtest.T) {
			testCases := []struct {
				name     string
				opts     *options.FindOneAndReplaceOptionsBuilder
				errParam string
			}{
				{"single key hint", options.FindOneAndReplace().SetHint(bson.M{"x": 1}), ""},
				{"multikey hint", options.FindOneAndReplace().SetHint(bson.M{"x": 1, "y": 1}), "hint"},
				{"single key sort", options.FindOneAndReplace().SetSort(bson.M{"x": 1}), ""},
				{"multikey sort", options.FindOneAndReplace().SetSort(bson.M{"x": 1, "y": 1}), "sort"},
			}
			for _, tc := range testCases {
				mt.RunOpts(tc.name, mtest.NewOptions().MinServerVersion("4.4"), func(mt *mtest.T) {
					initCollection(mt, mt.Coll)
					indexView := mt.Coll.Indexes()
					_, err := indexView.CreateOne(context.Background(), mongo.IndexModel{
						Keys: bson.D{{"x", 1}},
					})
					assert.Nil(mt, err, "CreateOne error: %v", err)

					res, err := mt.Coll.FindOneAndReplace(context.Background(), bson.D{{"x", 1}}, bson.D{{"y", 3}}, tc.opts).Raw()

					if tc.errParam != "" {
						expErr := mongo.ErrMapForOrderedArgument{tc.errParam}
						assert.Equal(mt, expErr, err, "expected error %v, got %v", expErr, err)
						return
					}

					assert.Nil(mt, err, "FindOneAndReplace error: %v", err)
					x, err := res.LookupErr("x")
					assert.Nil(mt, err, "x not found in document %v", res)
					got, ok := x.Int32OK()
					assert.True(mt, ok, "expected x type int32, got %v", x.Type)
					assert.Equal(mt, int32(1), got, "expected x value 1, got %v", got)
				})
			}
		})
		wcCollOpts := options.Collection().SetWriteConcern(impossibleWc)
		wcTestOpts := mtest.NewOptions().CollectionOptions(wcCollOpts).Topologies(mtest.ReplicaSet).MinServerVersion("3.2")
		mt.RunOpts("write concern error", wcTestOpts, func(mt *mtest.T) {
			filter := bson.D{{"x", 3}}
			replacement := bson.D{{"y", 3}}
			err := mt.Coll.FindOneAndReplace(context.Background(), filter, replacement).Err()
			we, ok := err.(mongo.WriteException)
			assert.True(mt, ok, "expected error type %v, got %v", mongo.WriteException{}, err)
			assert.NotNil(mt, we.WriteConcernError, "expected write concern error, got %v", err)
		})
	})
	mt.RunOpts("find one and update", noClientOpts, func(mt *mtest.T) {
		mt.Run("found", func(mt *mtest.T) {
			initCollection(mt, mt.Coll)
			filter := bson.D{{"x", 3}}
			update := bson.D{{"$set", bson.D{{"x", 6}}}}

			res, err := mt.Coll.FindOneAndUpdate(context.Background(), filter, update).Raw()
			assert.Nil(mt, err, "FindOneAndUpdate error: %v", err)
			elem, err := res.LookupErr("x")
			assert.Nil(mt, err, "x not found in result %v", res)
			assert.Equal(mt, bson.TypeInt32, elem.Type, "expected x type %v, got %v", bson.TypeInt32, elem.Type)
			x := elem.Int32()
			assert.Equal(mt, int32(3), x, "expected x value 3, got %v", x)
		})
		mt.Run("empty update", func(mt *mtest.T) {
			err := mt.Coll.FindOneAndUpdate(context.Background(), bson.D{}, bson.D{})
			assert.NotNil(mt, err, "expected error, got nil")
		})
		mt.Run("found ignore result", func(mt *mtest.T) {
			initCollection(mt, mt.Coll)
			filter := bson.D{{"x", 3}}
			update := bson.D{{"$set", bson.D{{"x", 6}}}}

			err := mt.Coll.FindOneAndUpdate(context.Background(), filter, update).Err()
			assert.Nil(mt, err, "FindOneAndUpdate error: %v", err)
		})
		mt.Run("not found", func(mt *mtest.T) {
			initCollection(mt, mt.Coll)
			filter := bson.D{{"x", 6}}
			update := bson.D{{"$set", bson.D{{"y", 6}}}}

			err := mt.Coll.FindOneAndUpdate(context.Background(), filter, update).Err()
			assert.Equal(mt, mongo.ErrNoDocuments, err, "expected error %v, got %v", mongo.ErrNoDocuments, err)
		})
		mt.RunOpts("maps for sorted opts", noClientOpts, func(mt *mtest.T) {
			testCases := []struct {
				name     string
				opts     *options.FindOneAndUpdateOptionsBuilder
				errParam string
			}{
				{"single key hint", options.FindOneAndUpdate().SetHint(bson.M{"x": 1}), ""},
				{"multikey hint", options.FindOneAndUpdate().SetHint(bson.M{"x": 1, "y": 1}), "hint"},
				{"single key sort", options.FindOneAndUpdate().SetSort(bson.M{"x": 1}), ""},
				{"multikey sort", options.FindOneAndUpdate().SetSort(bson.M{"x": 1, "y": 1}), "sort"},
			}
			for _, tc := range testCases {
				mt.RunOpts(tc.name, mtest.NewOptions().MinServerVersion("4.4"), func(mt *mtest.T) {
					initCollection(mt, mt.Coll)
					indexView := mt.Coll.Indexes()
					_, err := indexView.CreateOne(context.Background(), mongo.IndexModel{
						Keys: bson.D{{"x", 1}},
					})
					assert.Nil(mt, err, "CreateOne error: %v", err)

					res, err := mt.Coll.FindOneAndUpdate(context.Background(), bson.D{{"x", 1}}, bson.D{{"$set", bson.D{{"x", 6}}}}, tc.opts).Raw()

					if tc.errParam != "" {
						expErr := mongo.ErrMapForOrderedArgument{tc.errParam}
						assert.Equal(mt, expErr, err, "expected error %v, got %v", expErr, err)
						return
					}

					assert.Nil(mt, err, "FindOneAndUpdate error: %v", err)
					x, err := res.LookupErr("x")
					assert.Nil(mt, err, "x not found in document %v", res)
					got, ok := x.Int32OK()
					assert.True(mt, ok, "expected x type int32, got %v", x.Type)
					assert.Equal(mt, int32(1), got, "expected x value 1, got %v", got)
				})
			}
		})
		wcCollOpts := options.Collection().SetWriteConcern(impossibleWc)
		wcTestOpts := mtest.NewOptions().CollectionOptions(wcCollOpts).Topologies(mtest.ReplicaSet).MinServerVersion("3.2")
		mt.RunOpts("write concern error", wcTestOpts, func(mt *mtest.T) {
			filter := bson.D{{"x", 3}}
			update := bson.D{{"$set", bson.D{{"x", 6}}}}
			err := mt.Coll.FindOneAndUpdate(context.Background(), filter, update).Err()
			we, ok := err.(mongo.WriteException)
			assert.True(mt, ok, "expected error type %v, got %v", mongo.WriteException{}, err)
			assert.NotNil(mt, we.WriteConcernError, "expected write concern error, got %v", err)
		})
	})

	unackClientOpts := options.Client().
		SetWriteConcern(writeconcern.Unacknowledged())
	unackMtOpts := mtest.NewOptions().
		ClientOptions(unackClientOpts).
		MinServerVersion("3.6")
	mt.RunOpts("unacknowledged writes", unackMtOpts, func(mt *mtest.T) {
		mt.Run("bulk write", func(mt *mtest.T) {
			models := []mongo.WriteModel{
				mongo.NewInsertOneModel().SetDocument(bson.D{{"x", 1}}),
			}

			res, err := mt.Coll.BulkWrite(context.Background(), models)

			assert.NoError(mt, err)
			assert.False(mt, res.Acknowledged)
		})

		mt.Run("insert one", func(mt *mtest.T) {
			res, err := mt.Coll.InsertOne(context.Background(), bson.D{{"x", 1}})

			assert.NoError(mt, err)
			assert.False(mt, res.Acknowledged)
		})

		mt.Run("insert many", func(mt *mtest.T) {
			docs := []interface{}{
				bson.D{{"x", 1}},
				bson.D{{"y", 1}},
			}

			res, err := mt.Coll.InsertMany(context.Background(), docs)

			assert.NoError(mt, err)
			assert.False(mt, res.Acknowledged)
		})

		mt.Run("delete", func(mt *mtest.T) {
			res, err := mt.Coll.DeleteOne(context.Background(), bson.D{{"x", 1}})

			assert.NoError(mt, err)
			assert.False(mt, res.Acknowledged)
		})

		mt.Run("update", func(mt *mtest.T) {
			res, err := mt.Coll.UpdateOne(context.Background(), bson.D{{"x", 1}}, bson.D{{"$set", bson.D{{"x", "2"}}}})

			assert.NoError(mt, err)
			assert.False(mt, res.Acknowledged)
		})

		mt.Run("find and modify", func(mt *mtest.T) {
			res := mt.Coll.FindOneAndDelete(context.Background(), bson.D{{"x", 1}})

			assert.ErrorIs(mt, res.Err(), mongo.ErrNoDocuments)
			assert.False(mt, res.Acknowledged)

		})

		mt.Run("dropping a collection", func(mt *mtest.T) {
			err := mt.Coll.Drop(context.Background())
			assert.NoError(mt, err)
		})

		mt.Run("creating a collection", func(mt *mtest.T) {
			err := mt.DB.CreateCollection(context.Background(), "test coll")
			assert.NoError(mt, err)
		})

		mt.Run("creating an index", func(mt *mtest.T) {
			indexModel := mongo.IndexModel{
				Keys:    bson.M{"username": 1},
				Options: options.Index().SetUnique(true),
			}

			_, err := mt.Coll.Indexes().CreateMany(context.Background(), []mongo.IndexModel{indexModel})
			assert.NoError(mt, err)
		})

		mt.Run("creating an index view", func(mt *mtest.T) {
			projectStage := bson.D{
				{"$project", bson.D{
					{"_id", 0},
					{"fullName", bson.D{
						{"$concat", []string{"$firstName", " ", "$lastName"}},
					}},
				}},
			}

			pipeline := mongo.Pipeline{projectStage}

			err := mt.DB.CreateView(context.Background(), "testview", "coll", pipeline, nil)
			assert.NoError(mt, err)
		})

		mt.Run("dropping a database", func(mt *mtest.T) {
			db := mt.Client.Database("bd7b09e4-7d12-4bcb-9fc6-9852ad93715a")

			err := db.Drop(context.Background())
			assert.NoError(mt, err)
		})

		mt.Run("dropping an index", func(mt *mtest.T) {
			indexModel := mongo.IndexModel{
				Keys:    bson.M{"username": 1},
				Options: options.Index().SetUnique(true).SetName("username_1"),
			}

			_, err := mt.Coll.Indexes().CreateOne(context.TODO(), indexModel)
			assert.NoError(mt, err, "failed to create index")

			_ = mt.Coll.Indexes().DropOne(context.Background(), "username_1")
		})
	})

	mt.RunOpts("bulk write", noClientOpts, func(mt *mtest.T) {
		wcCollOpts := options.Collection().SetWriteConcern(impossibleWc)
		wcTestOpts := mtest.NewOptions().CollectionOptions(wcCollOpts).Topologies(mtest.ReplicaSet).CreateClient(false)
		mt.RunOpts("write concern error", wcTestOpts, func(mt *mtest.T) {
			filter := bson.D{{"foo", "bar"}}
			update := bson.D{{"$set", bson.D{{"foo", 10}}}}
			insertModel := mongo.NewInsertOneModel().SetDocument(bson.D{{"foo", 1}})
			updateModel := mongo.NewUpdateOneModel().SetFilter(filter).SetUpdate(update)
			deleteModel := mongo.NewDeleteOneModel().SetFilter(filter)

			testCases := []struct {
				name   string
				models []mongo.WriteModel
			}{
				{"insert", []mongo.WriteModel{insertModel}},
				{"update", []mongo.WriteModel{updateModel}},
				{"delete", []mongo.WriteModel{deleteModel}},
			}
			for _, tc := range testCases {
				mt.Run(tc.name, func(mt *mtest.T) {
					_, err := mt.Coll.BulkWrite(context.Background(), tc.models)
					bwe, ok := err.(mongo.BulkWriteException)
					assert.True(mt, ok, "expected error type %v, got %v", mongo.BulkWriteException{}, err)
					numWriteErrors := len(bwe.WriteErrors)
					assert.Equal(mt, 0, numWriteErrors, "expected 0 write errors, got %v", numWriteErrors)
					assert.NotNil(mt, bwe.WriteConcernError, "expected write concern error, got %v", err)
				})
			}
		})

		mt.RunOpts("insert write errors", noClientOpts, func(mt *mtest.T) {
			doc1 := mongo.NewInsertOneModel().SetDocument(bson.D{{"_id", "x"}})
			doc2 := mongo.NewInsertOneModel().SetDocument(bson.D{{"_id", "y"}})
			models := []mongo.WriteModel{doc1, doc1, doc2, doc2}

			testCases := []struct {
				name           string
				ordered        bool
				insertedCount  int64
				numWriteErrors int
			}{
				{"ordered", true, 1, 1},
				{"unordered", false, 2, 2},
			}
			for _, tc := range testCases {
				mt.Run(tc.name, func(mt *mtest.T) {
					res, err := mt.Coll.BulkWrite(context.Background(), models, options.BulkWrite().SetOrdered(tc.ordered))
					assert.Equal(mt, tc.insertedCount, res.InsertedCount,
						"expected inserted count %v, got %v", tc.insertedCount, res.InsertedCount)

					bwe, ok := err.(mongo.BulkWriteException)
					assert.True(mt, ok, "expected error type %v, got %v", mongo.BulkWriteException{}, err)
					numWriteErrors := len(bwe.WriteErrors)
					assert.Equal(mt, tc.numWriteErrors, numWriteErrors,
						"expected %v write errors, got %v", tc.numWriteErrors, numWriteErrors)
					gotCode := bwe.WriteErrors[0].Code
					assert.Equal(mt, errorDuplicateKey, gotCode, "expected error code %v, got %v", errorDuplicateKey, gotCode)
				})
			}
		})
		mt.RunOpts("delete write errors", mtest.NewOptions().MaxServerVersion("5.0.7"), func(mt *mtest.T) {
			// Deletes are not allowed on capped collections on MongoDB 5.0.6-. We use this
			// behavior to test the processing of write errors.
			doc := mongo.NewDeleteOneModel().SetFilter(bson.D{{"x", 1}})
			models := []mongo.WriteModel{doc, doc}
			cappedOpts := options.CreateCollection().SetCapped(true).SetSizeInBytes(64 * 1024)
			capped := mt.CreateCollection(mtest.Collection{
				Name:       "delete_write_errors",
				CreateOpts: cappedOpts,
			}, true)

			testCases := []struct {
				name           string
				ordered        bool
				numWriteErrors int
			}{
				{"ordered", true, 1},
				{"unordered", false, 2},
			}
			for _, tc := range testCases {
				mt.Run(tc.name, func(mt *mtest.T) {
					_, err := capped.BulkWrite(context.Background(), models, options.BulkWrite().SetOrdered(tc.ordered))
					bwe, ok := err.(mongo.BulkWriteException)
					assert.True(mt, ok, "expected error type %v, got %v", mongo.BulkWriteException{}, err)
					numWriteErrors := len(bwe.WriteErrors)
					assert.Equal(mt, tc.numWriteErrors, numWriteErrors,
						"expected %v write errors, got %v", tc.numWriteErrors, numWriteErrors)
					gotCode := bwe.WriteErrors[0].Code
					assert.True(mt, gotCode == errorCappedCollDeleteLegacy || gotCode == errorCappedCollDelete,
						"expected error code %v or %v, got %v", errorCappedCollDeleteLegacy, errorCappedCollDelete, gotCode)
				})
			}
		})
		mt.RunOpts("update write errors", noClientOpts, func(mt *mtest.T) {
			filter := bson.D{{"_id", "foo"}}
			doc1 := mongo.NewUpdateOneModel().SetFilter(filter).SetUpdate(bson.D{{"$set", bson.D{{"_id", 3.14159}}}})
			doc2 := mongo.NewUpdateOneModel().SetFilter(filter).SetUpdate(bson.D{{"$set", bson.D{{"x", "fa"}}}})
			models := []mongo.WriteModel{doc1, doc1, doc2}

			testCases := []struct {
				name           string
				ordered        bool
				modifiedCount  int64
				numWriteErrors int
			}{
				{"ordered", true, 0, 1},
				{"unordered", false, 1, 2},
			}
			for _, tc := range testCases {
				mt.Run(tc.name, func(mt *mtest.T) {
					_, err := mt.Coll.InsertOne(context.Background(), filter)
					assert.Nil(mt, err, "InsertOne error: %v", err)
					res, err := mt.Coll.BulkWrite(context.Background(), models, options.BulkWrite().SetOrdered(tc.ordered))
					assert.Equal(mt, tc.modifiedCount, res.ModifiedCount,
						"expected modified count %v, got %v", tc.modifiedCount, res.ModifiedCount)

					bwe, ok := err.(mongo.BulkWriteException)
					assert.True(mt, ok, "expected error type %v, got %v", mongo.BulkWriteException{}, err)
					numWriteErrors := len(bwe.WriteErrors)
					assert.Equal(mt, tc.numWriteErrors, numWriteErrors,
						"expected %v write errors, got %v", tc.numWriteErrors, numWriteErrors)
					gotCode := bwe.WriteErrors[0].Code
					assert.Equal(mt, errorModifiedID, gotCode, "expected error code %v, got %v", errorModifiedID, gotCode)
				})
			}
		})
		mt.Run("error on nil filter", func(mt *mtest.T) {
			mt.Parallel()

			testCases := []struct {
				name        string
				model       mongo.WriteModel
				errorString string
			}{
				{
					name:        "DeleteOne",
					model:       mongo.NewDeleteOneModel(),
					errorString: "delete filter cannot be nil",
				},
				{
					name:        "DeleteMany",
					model:       mongo.NewDeleteManyModel(),
					errorString: "delete filter cannot be nil",
				},
				{
					name:        "UpdateOne",
					model:       mongo.NewUpdateOneModel().SetUpdate(bson.D{{"$set", bson.D{{"x", 1}}}}),
					errorString: "update filter cannot be nil",
				},
				{
					name:        "UpdateMany",
					model:       mongo.NewUpdateManyModel().SetUpdate(bson.D{{"$set", bson.D{{"x", 1}}}}),
					errorString: "update filter cannot be nil",
				},
			}
			for _, tc := range testCases {
				tc := tc

				mt.Run(tc.name, func(mt *mtest.T) {
					mt.Parallel()

					_, err := mt.Coll.BulkWrite(context.Background(), []mongo.WriteModel{tc.model})
					assert.EqualError(mt, err, tc.errorString)
				})
			}
		})
		mt.Run("correct model in errors", func(mt *mtest.T) {
			models := []mongo.WriteModel{
				mongo.NewUpdateOneModel().SetFilter(bson.M{}).SetUpdate(bson.M{
					"$set": bson.M{
						"x": 1,
					},
				}),
				mongo.NewInsertOneModel().SetDocument(bson.M{
					"_id": "notduplicate",
				}),
				mongo.NewInsertOneModel().SetDocument(bson.M{
					"_id": "duplicate1",
				}),
				mongo.NewInsertOneModel().SetDocument(bson.M{
					"_id": "duplicate1",
				}),
				mongo.NewInsertOneModel().SetDocument(bson.M{
					"_id": "duplicate2",
				}),
				mongo.NewInsertOneModel().SetDocument(bson.M{
					"_id": "duplicate2",
				}),
			}

			_, err := mt.Coll.BulkWrite(context.Background(), models)
			bwException, ok := err.(mongo.BulkWriteException)
			assert.True(mt, ok, "expected error of type %T, got %T", mongo.BulkWriteException{}, err)

			expectedModel := models[3]
			actualModel := bwException.WriteErrors[0].Request
			assert.Equal(mt, expectedModel, actualModel, "expected model %v in BulkWriteException, got %v",
				expectedModel, actualModel)
		})
		mt.RunOpts("unordered writeError index", mtest.NewOptions().MaxServerVersion("5.0.7"), func(mt *mtest.T) {
			// Deletes are not allowed on capped collections on MongoDB 5.0.6-. We use this
			// behavior to test the processing of write errors.
			cappedOpts := options.CreateCollection().SetCapped(true).SetSizeInBytes(64 * 1024)
			capped := mt.CreateCollection(mtest.Collection{
				Name:       "deleteOne_capped",
				CreateOpts: cappedOpts,
			}, true)
			models := []mongo.WriteModel{
				mongo.NewInsertOneModel().SetDocument(bson.D{{"_id", "id1"}}),
				mongo.NewInsertOneModel().SetDocument(bson.D{{"_id", "id3"}}),
			}
			_, err := capped.BulkWrite(context.Background(), models, options.BulkWrite())
			assert.Nil(t, err, "BulkWrite error: %v", err)

			// UpdateOne and ReplaceOne models are batched together, so they each appear once
			models = []mongo.WriteModel{
				mongo.NewDeleteOneModel().SetFilter(bson.D{{"_id", "id0"}}),
				mongo.NewDeleteManyModel().SetFilter(bson.D{{"_id", "id0"}}),
				mongo.NewUpdateOneModel().SetFilter(bson.D{{"_id", "id3"}}).SetUpdate(bson.D{{"$set", bson.D{{"_id", 3.14159}}}}),
				mongo.NewInsertOneModel().SetDocument(bson.D{{"_id", "id1"}}),
				mongo.NewDeleteManyModel().SetFilter(bson.D{{"_id", "id0"}}),
				mongo.NewUpdateManyModel().SetFilter(bson.D{{"_id", "id3"}}).SetUpdate(bson.D{{"$set", bson.D{{"_id", 3.14159}}}}),
				mongo.NewDeleteOneModel().SetFilter(bson.D{{"_id", "id0"}}),
				mongo.NewInsertOneModel().SetDocument(bson.D{{"_id", "id1"}}),
				mongo.NewReplaceOneModel().SetFilter(bson.D{{"_id", "id3"}}).SetReplacement(bson.D{{"_id", 3.14159}}),
				mongo.NewUpdateManyModel().SetFilter(bson.D{{"_id", "id3"}}).SetUpdate(bson.D{{"$set", bson.D{{"_id", 3.14159}}}}),
			}
			_, err = capped.BulkWrite(context.Background(), models, options.BulkWrite().SetOrdered(false))
			bwException, ok := err.(mongo.BulkWriteException)
			assert.True(mt, ok, "expected error of type %T, got %T", mongo.BulkWriteException{}, err)

			assert.Equal(mt, len(bwException.WriteErrors), 10, "expected 10 writeErrors, got %v", len(bwException.WriteErrors))
			for _, writeErr := range bwException.WriteErrors {
				switch writeErr.Request.(type) {
				case *mongo.DeleteOneModel:
					assert.True(mt, writeErr.Index == 0 || writeErr.Index == 6,
						"expected index 0 or 6, got %v", writeErr.Index)
				case *mongo.DeleteManyModel:
					assert.True(mt, writeErr.Index == 1 || writeErr.Index == 4,
						"expected index 1 or 4, got %v", writeErr.Index)
				case *mongo.UpdateManyModel:
					assert.True(mt, writeErr.Index == 5 || writeErr.Index == 9,
						"expected index 5 or 9, got %v", writeErr.Index)
				case *mongo.InsertOneModel:
					assert.True(mt, writeErr.Index == 3 || writeErr.Index == 7,
						"expected index 3 or 7, got %v", writeErr.Index)
				case *mongo.UpdateOneModel:
					assert.Equal(mt, writeErr.Index, 2, "expected index 2, got %v", writeErr.Index)
				case *mongo.ReplaceOneModel:
					assert.Equal(mt, writeErr.Index, 8, "expected index 8, got %v", writeErr.Index)
				}

			}
		})
		mt.Run("unordered upsertID index", func(mt *mtest.T) {
			id1 := "id1"
			id3 := "id3"
			models := []mongo.WriteModel{
				mongo.NewDeleteOneModel().SetFilter(bson.D{{"_id", "id0"}}),
				mongo.NewReplaceOneModel().SetFilter(bson.D{{"_id", id1}}).SetReplacement(bson.D{{"_id", id1}}).SetUpsert(true),
				mongo.NewDeleteOneModel().SetFilter(bson.D{{"_id", "id2"}}),
				mongo.NewReplaceOneModel().SetFilter(bson.D{{"_id", id3}}).SetReplacement(bson.D{{"_id", id3}}).SetUpsert(true),
				mongo.NewDeleteOneModel().SetFilter(bson.D{{"_id", "id4"}}),
			}
			res, err := mt.Coll.BulkWrite(context.Background(), models, options.BulkWrite().SetOrdered(false))
			assert.Nil(mt, err, "bulkwrite error: %v", err)

			assert.Equal(mt, len(res.UpsertedIDs), 2, "expected 2 UpsertedIDs, got %v", len(res.UpsertedIDs))
			assert.Equal(mt, res.UpsertedIDs[1].(string), id1, "expected UpsertedIDs[1] to be %v, got %v", id1, res.UpsertedIDs[1])
			assert.Equal(mt, res.UpsertedIDs[3].(string), id3, "expected UpsertedIDs[3] to be %v, got %v", id3, res.UpsertedIDs[3])
		})
		mt.RunOpts("insert and delete with batches", mtest.NewOptions().ClientType(mtest.Mock), func(mt *mtest.T) {
			// grouped together because delete requires the documents to be inserted
			maxBatchCount := int(drivertest.MockDescription.MaxBatchCount)
			numDocs := maxBatchCount + 50
			var insertModels []mongo.WriteModel
			var deleteModels []mongo.WriteModel
			for i := 0; i < numDocs; i++ {
				d := bson.D{
					{"a", int32(i)},
					{"b", int32(i * 2)},
					{"c", int32(i * 3)},
				}
				insertModels = append(insertModels, mongo.NewInsertOneModel().SetDocument(d))
				deleteModels = append(deleteModels, mongo.NewDeleteOneModel().SetFilter(bson.D{}))
			}

			// Seed mock responses. Both insert and delete responses look like {ok: 1, n: <inserted/deleted count>}.
			// This loop only creates one set of responses, but the sets for insert and delete should be equivalent,
			// so we can duplicate the generated set before calling mt.AddMockResponses().
			var responses []bson.D
			for i := numDocs; i > 0; i -= maxBatchCount {
				count := maxBatchCount
				if i < maxBatchCount {
					count = i
				}
				res := mtest.CreateSuccessResponse(bson.E{"n", count})
				responses = append(responses, res)
			}
			mt.AddMockResponses(append(responses, responses...)...)

			mt.ClearEvents()
			res, err := mt.Coll.BulkWrite(context.Background(), insertModels)
			assert.Nil(mt, err, "BulkWrite error: %v", err)
			assert.Equal(mt, int64(numDocs), res.InsertedCount, "expected %v inserted documents, got %v", numDocs, res.InsertedCount)
			mt.FilterStartedEvents(func(evt *event.CommandStartedEvent) bool {
				return evt.CommandName == "insert"
			})
			// MaxWriteBatchSize changed between 3.4 and 3.6, so there isn't a given number of batches that this will be split into
			inserts := len(mt.GetAllStartedEvents())
			assert.True(mt, inserts > 1, "expected multiple batches, got %v", inserts)

			mt.ClearEvents()
			res, err = mt.Coll.BulkWrite(context.Background(), deleteModels)
			assert.Nil(mt, err, "BulkWrite error: %v", err)
			assert.Equal(mt, int64(numDocs), res.DeletedCount, "expected %v deleted documents, got %v", numDocs, res.DeletedCount)
			mt.FilterStartedEvents(func(evt *event.CommandStartedEvent) bool {
				return evt.CommandName == "delete"
			})
			// MaxWriteBatchSize changed between 3.4 and 3.6, so there isn't a given number of batches that this will be split into
			deletes := len(mt.GetAllStartedEvents())
			assert.True(mt, deletes > 1, "expected multiple batches, got %v", deletes)
		})
		mt.RunOpts("update with batches", mtest.NewOptions().ClientType(mtest.Mock), func(mt *mtest.T) {
			maxBatchCount := int(drivertest.MockDescription.MaxBatchCount)
			numModels := maxBatchCount + 50
			var models []mongo.WriteModel

			// It's significantly faster to upsert the first model and only modify the rest than to upsert all of them.
			for i := 0; i < numModels-1; i++ {
				update := bson.D{
					{"$set", bson.D{
						{"a", int32(i + 1)},
						{"b", int32(i * 2)},
						{"c", int32(i * 3)},
					}},
				}
				model := mongo.NewUpdateOneModel().
					SetFilter(bson.D{{"a", int32(i)}}).
					SetUpdate(update).SetUpsert(true)
				models = append(models, model)
			}
			// Add one last upsert for second batch.
			models = append(models, mongo.NewUpdateOneModel().
				SetFilter(bson.D{{"x", int32(1)}}).
				SetUpdate(bson.D{{"$set", bson.D{{"x", int32(1)}}}}).
				SetUpsert(true),
			)

			// Seed mock responses for the BulkWrite.
			//
			// The response from the first batch should look like:
			// {ok: 1, n: 100000, nModified: 99999, upserted: [{index: 0, _id: <id>}]}
			firstBatchUpserted := bson.A{bson.D{{"index", 0}, {"_id", bson.NewObjectID()}}}
			firstBatchResponse := mtest.CreateSuccessResponse(
				bson.E{"n", 100000},
				bson.E{"nModified", 99999},
				bson.E{"upserted", firstBatchUpserted},
			)
			// The response from the second batch should look like:
			// {ok: 1, n: 50, nModified: 49, upserted: [{index: 49, _id: <id>}]}
			secondBatchUpserted := bson.A{bson.D{{"index", 49}, {"_id", bson.NewObjectID()}}}
			secondBatchResponse := mtest.CreateSuccessResponse(
				bson.E{"n", 50},
				bson.E{"nModified", 49},
				bson.E{"upserted", secondBatchUpserted},
			)
			mt.AddMockResponses([]bson.D{firstBatchResponse, secondBatchResponse}...)

			mt.ClearEvents()
			res, err := mt.Coll.BulkWrite(context.Background(), models)
			assert.Nil(mt, err, "BulkWrite error: %v", err)

			mt.FilterStartedEvents(func(evt *event.CommandStartedEvent) bool {
				return evt.CommandName == "update"
			})
			// MaxWriteBatchSize changed between 3.4 and 3.6, so there isn't a given number of batches that
			// this will be split into.
			updates := len(mt.GetAllStartedEvents())
			assert.True(mt, updates > 1, "expected multiple batches, got %v", updates)

			assert.Equal(mt, int64(numModels-2), res.ModifiedCount, "expected %v modified documents, got %v", numModels-2, res.ModifiedCount)
			assert.Equal(mt, int64(numModels-2), res.MatchedCount, "expected %v matched documents, got %v", numModels-2, res.ModifiedCount)
			assert.Equal(mt, int64(2), res.UpsertedCount, "expected %v upserted documents, got %v", 2, res.UpsertedCount)
			assert.Equal(mt, 2, len(res.UpsertedIDs), "expected %v upserted ids, got %v", 2, len(res.UpsertedIDs))

			// Check that IDs exist in result for upserted documents.
			_, ok := res.UpsertedIDs[0]
			assert.True(mt, ok, "expected id at key 0")
			_, ok = res.UpsertedIDs[int64(numModels-1)]
			assert.True(mt, ok, "expected id at key %v", numModels-1)
		})
		mt.RunOpts("map hint", noClientOpts, func(mt *mtest.T) {
			filter := bson.D{{"_id", "foo"}}
			testCases := []struct {
				name     string
				models   []mongo.WriteModel
				errParam string
			}{
				{"updateOne/multi key", []mongo.WriteModel{mongo.NewUpdateOneModel().SetFilter(filter).SetUpdate(bson.D{{"$set", bson.D{{"x", "fa"}}}}).SetHint(bson.M{"_id": 1, "x": 1})}, "hint"},
				{"updateMany/multi key", []mongo.WriteModel{mongo.NewUpdateManyModel().SetFilter(filter).SetUpdate(bson.D{{"$set", bson.D{{"x", "fa"}}}}).SetHint(bson.M{"_id": 1, "x": 1})}, "hint"},
				{"replaceOne/multi key", []mongo.WriteModel{mongo.NewReplaceOneModel().SetFilter(filter).SetReplacement(bson.D{{"x", "bar"}}).SetHint(bson.M{"_id": 1, "x": 1})}, "hint"},
				{"deleteOne/multi key", []mongo.WriteModel{mongo.NewDeleteOneModel().SetFilter(filter).SetHint(bson.M{"_id": 1, "x": 1})}, "hint"},
				{"deleteMany/multi key", []mongo.WriteModel{mongo.NewDeleteManyModel().SetFilter(filter).SetHint(bson.M{"_id": 1, "x": 1})}, "hint"},

				{"updateOne/one key", []mongo.WriteModel{mongo.NewUpdateOneModel().SetFilter(filter).SetUpdate(bson.D{{"$set", bson.D{{"x", "fa"}}}}).SetHint(bson.M{"_id": 1})}, ""},
				{"updateMany/one key", []mongo.WriteModel{mongo.NewUpdateManyModel().SetFilter(filter).SetUpdate(bson.D{{"$set", bson.D{{"x", "fa"}}}}).SetHint(bson.M{"_id": 1})}, ""},
				{"replaceOne/one key", []mongo.WriteModel{mongo.NewReplaceOneModel().SetFilter(filter).SetReplacement(bson.D{{"x", "bar"}}).SetHint(bson.M{"_id": 1})}, ""},
				{"deleteOne/one key", []mongo.WriteModel{mongo.NewDeleteOneModel().SetFilter(filter).SetHint(bson.M{"_id": 1})}, ""},
				{"deleteMany/one key", []mongo.WriteModel{mongo.NewDeleteManyModel().SetFilter(filter).SetHint(bson.M{"_id": 1})}, ""},
			}
			for _, tc := range testCases {
				mt.RunOpts(tc.name, mtest.NewOptions().MinServerVersion("4.4"), func(mt *mtest.T) {
					_, err := mt.Coll.InsertOne(context.Background(), filter)
					assert.Nil(mt, err, "InsertOne error: %v", err)
					_, err = mt.Coll.BulkWrite(context.Background(), tc.models)
					if tc.errParam == "" {
						assert.Nil(mt, err, "expected nil error, got %v", err)
						return
					}
					expErr := mongo.ErrMapForOrderedArgument{tc.errParam}
					assert.Equal(mt, expErr, err, "expected error %v, got %v", expErr, err)
				})
			}
		})
	})
}

func initCollection(mt *mtest.T, coll *mongo.Collection) {
	mt.Helper()

	var docs []interface{}
	for i := 1; i <= 5; i++ {
		docs = append(docs, bson.D{{"x", int32(i)}})
	}

	_, err := coll.InsertMany(context.Background(), docs)
	assert.Nil(mt, err, "InsertMany error for initial data: %v", err)
}

func testAggregateWithOptions(mt *mtest.T, createIndex bool, opts options.Lister[options.AggregateOptions]) {
	mt.Helper()
	initCollection(mt, mt.Coll)

	if createIndex {
		indexView := mt.Coll.Indexes()
		_, err := indexView.CreateOne(context.Background(), mongo.IndexModel{
			Keys: bson.D{{"x", 1}},
		})
		assert.Nil(mt, err, "CreateOne error: %v", err)
	}

	pipeline := mongo.Pipeline{
		{{"$match", bson.D{{"x", bson.D{{"$gte", 2}}}}}},
		{{"$project", bson.D{{"_id", 0}, {"x", 1}}}},
		{{"$sort", bson.D{{"x", 1}}}},
	}

	cursor, err := mt.Coll.Aggregate(context.Background(), pipeline, opts)
	assert.Nil(mt, err, "Aggregate error: %v", err)

	for i := 2; i < 5; i++ {
		assert.True(mt, cursor.Next(context.Background()), "expected Next true, got false")
		elems, _ := cursor.Current.Elements()
		assert.Equal(mt, 1, len(elems), "expected doc with 1 element, got %v", cursor.Current)

		num, err := cursor.Current.LookupErr("x")
		assert.Nil(mt, err, "x not found in document %v", cursor.Current)
		assert.Equal(mt, bson.TypeInt32, num.Type, "expected 'x' type %v, got %v", bson.TypeInt32, num.Type)
		assert.Equal(mt, int32(i), num.Int32(), "expected x value %v, got %v", i, num.Int32())
	}
}

func create16MBDocument(mt *mtest.T) bsoncore.Document {
	// 4 bytes = document length
	// 1 byte = element type (ObjectID = \x07)
	// 4 bytes = key name ("_id" + \x00)
	// 12 bytes = ObjectID value
	// 1 byte = element type (string = \x02)
	// 4 bytes = key name ("key" + \x00)
	// 4 bytes = string length
	// X bytes = string of length X bytes
	// 1 byte = \x00
	// 1 byte = \x00
	//
	// Therefore the string length should be: 1024*1024*16 - 32

	targetDocSize := 1024 * 1024 * 16
	strSize := targetDocSize - 32
	var b strings.Builder
	b.Grow(strSize)
	for i := 0; i < strSize; i++ {
		b.WriteByte('A')
	}

	idx, doc := bsoncore.AppendDocumentStart(nil)
	doc = bsoncore.AppendObjectIDElement(doc, "_id", bson.NewObjectID())
	doc = bsoncore.AppendStringElement(doc, "key", b.String())
	doc, _ = bsoncore.AppendDocumentEnd(doc, idx)
	assert.Equal(mt, targetDocSize, len(doc), "expected document length %v, got %v", targetDocSize, len(doc))
	return doc
}

// This is a helper function to ensure that sending getMore commands for a cursor results in command monitoring events
// being published. The cursorFn parameter should be a function that yields a cursor which is open on the server and
// requires at least one getMore to be fully iterated.
func assertGetMoreCommandsAreMonitored(mt *mtest.T, cmdName string, cursorFn func() (*mongo.Cursor, error)) {
	mt.Helper()
	mt.ClearEvents()

	cursor, err := cursorFn()
	assert.Nil(mt, err, "error creating cursor: %v", err)
	var docs []bson.D
	err = cursor.All(context.Background(), &docs)
	assert.Nil(mt, err, "All error: %v", err)

	// Only assert that the initial command and at least one getMore were sent. The exact number of getMore's required
	// is not important.
	evt := mt.GetStartedEvent()
	assert.Equal(mt, cmdName, evt.CommandName, "expected command %q, got %q", cmdName, evt.CommandName)
	evt = mt.GetStartedEvent()
	assert.Equal(mt, "getMore", evt.CommandName, "expected command 'getMore', got %q", evt.CommandName)
}

// This is a helper function to ensure that sending killCursors commands for a cursor results in command monitoring
// events being published. The cursorFn parameter should be a function that yields a cursor which is open on the server.
func assertKillCursorsCommandsAreMonitored(mt *mtest.T, cmdName string, cursorFn func() (*mongo.Cursor, error)) {
	mt.Helper()
	mt.ClearEvents()

	cursor, err := cursorFn()
	assert.Nil(mt, err, "error creating cursor: %v", err)
	err = cursor.Close(context.Background())
	assert.Nil(mt, err, "Close error: %v", err)

	evt := mt.GetStartedEvent()
	assert.Equal(mt, cmdName, evt.CommandName, "expected command %q, got %q", cmdName, evt.CommandName)
	evt = mt.GetStartedEvent()
	assert.Equal(mt, "killCursors", evt.CommandName, "expected command 'killCursors', got %q", evt.CommandName)
}
