// Copyright (C) MongoDB, Inc. 2017-present.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

package integration

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/internal/assert"
	"go.mongodb.org/mongo-driver/v2/internal/handshake"
	"go.mongodb.org/mongo-driver/v2/internal/integration/mtest"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"
	"go.mongodb.org/mongo-driver/v2/x/bsonx/bsoncore"
)

const (
	listCollCapped   = "listcoll_capped"
	listCollUncapped = "listcoll_uncapped"
)

var (
	interfaceAsMapRegistry = func() *bson.Registry {
		reg := bson.NewRegistry()
		reg.RegisterTypeMapEntry(bson.TypeEmbeddedDocument, reflect.TypeOf(bson.M{}))
		return reg
	}()
)

func TestDatabase(t *testing.T) {
	mt := mtest.New(t, mtest.NewOptions().CreateClient(false))

	mt.RunOpts("run command", noClientOpts, func(mt *mtest.T) {
		mt.Run("decode raw", func(mt *mtest.T) {
			res, err := mt.DB.RunCommand(context.Background(), bson.D{{handshake.LegacyHello, 1}}).Raw()
			assert.Nil(mt, err, "RunCommand error: %v", err)

			ok, err := res.LookupErr("ok")
			assert.Nil(mt, err, "ok field not found in result")
			assert.Equal(mt, bson.TypeDouble, ok.Type, "expected ok type %v, got %v", bson.TypeDouble, ok.Type)
			assert.Equal(mt, 1.0, ok.Double(), "expected ok value 1.0, got %v", ok.Double())

			hello, err := res.LookupErr(handshake.LegacyHelloLowercase)
			assert.Nil(mt, err, "legacy hello response field not found in result")
			assert.Equal(mt, bson.TypeBoolean, hello.Type, "expected hello type %v, got %v", bson.TypeBoolean, hello.Type)
			assert.True(mt, hello.Boolean(), "expected hello value true, got false")
		})
		mt.Run("decode struct", func(mt *mtest.T) {
			result := struct {
				Ok float64 `bson:"ok"`
			}{}
			err := mt.DB.RunCommand(context.Background(), bson.D{{"ping", 1}}).Decode(&result)
			assert.Nil(mt, err, "RunCommand error: %v", err)
			assert.Equal(mt, 1.0, result.Ok, "expected ok value 1.0, got %v", result.Ok)
		})

		// We set min server version 3.6 because pre-3.6 servers use OP_QUERY, so the command document will look like
		// {$query: {...}, $readPreference: {...}}. Per the command monitoring spec, the $query subdocument is unwrapped
		// and the $readPreference is dropped for monitoring purposes, so we can't examine it.
		readPrefOpts := mtest.NewOptions().
			Topologies(mtest.Sharded).
			MinServerVersion("3.6")
		mt.RunOpts("read pref passed to mongos", readPrefOpts, func(mt *mtest.T) {
			// When communicating with a mongos, the supplied read preference should be passed down to the operations
			// layer, which should add a top-level $readPreference field to the command.

			runCmdOpts := options.RunCmd().
				SetReadPreference(&readpref.ReadPref{Mode: readpref.SecondaryPreferredMode})
			err := mt.DB.RunCommand(context.Background(), bson.D{{handshake.LegacyHello, 1}}, runCmdOpts).Err()
			assert.Nil(mt, err, "RunCommand error: %v", err)

			expected := bson.Raw(bsoncore.NewDocumentBuilder().
				AppendString("mode", "secondaryPreferred").
				Build())
			evt := mt.GetStartedEvent()
			assert.Equal(mt, handshake.LegacyHello, evt.CommandName, "expected legacy hello command to be sent, got %q", evt.CommandName)
			actual, ok := evt.Command.Lookup("$readPreference").DocumentOK()
			assert.True(mt, ok, "expected command %v to contain a $readPreference document", evt.Command)
			assert.Equal(mt, expected, actual, "expected $readPreference document %v, got %v", expected, actual)
		})
		failpointOpts := mtest.NewOptions().MinServerVersion("4.0").Topologies(mtest.ReplicaSet)
		mt.RunOpts("gets result and error", failpointOpts, func(mt *mtest.T) {
			mt.SetFailPoint(mtest.FailPoint{
				ConfigureFailPoint: "failCommand",
				Mode: mtest.FailPointMode{
					Times: 1,
				},
				Data: mtest.FailPointData{
					FailCommands: []string{"insert"},
					WriteConcernError: &mtest.WriteConcernErrorData{
						Code: 100,
					},
				},
			})
			cmd := bson.D{
				{"insert", "test"},
				{"documents", bson.A{bson.D{{"a", 1}}}},
			}
			res, gotErr := mt.DB.RunCommand(context.Background(), cmd).Raw()

			n, ok := res.Lookup("n").Int32OK()
			assert.True(mt, ok, "expected n in response")
			assert.Equal(mt, int32(1), n, "expected n value 1, got %v", n)

			writeExcept, ok := gotErr.(mongo.WriteException)
			assert.True(mt, ok, "expected WriteCommandError, got %T", gotErr)
			assert.NotNil(mt, writeExcept.WriteConcernError, "expected WriteConcernError to be non-nil")
			assert.Equal(mt, writeExcept.WriteConcernError.Code, 100, "expected error code 100, got %v", writeExcept.WriteConcernError.Code)
		})
		mt.Run("multi key map command", func(mt *mtest.T) {
			err := mt.DB.RunCommand(context.Background(), bson.M{"insert": "test", "documents": bson.A{bson.D{{"a", 1}}}}).Err()
			assert.Equal(mt, mongo.ErrMapForOrderedArgument{"cmd"}, err, "expected error %v, got %v", mongo.ErrMapForOrderedArgument{"cmd"}, err)
		})
	})

	dropOpts := mtest.NewOptions().DatabaseName("dropDb")
	mt.RunOpts("drop", dropOpts, func(mt *mtest.T) {
		err := mt.DB.Drop(context.Background())
		assert.Nil(mt, err, "Drop error: %v", err)

		list, err := mt.Client.ListDatabaseNames(context.Background(), bson.D{})
		assert.Nil(mt, err, "ListDatabaseNames error: %v", err)
		for _, db := range list {
			if db == "dropDb" {
				mt.Fatal("dropped database 'dropDb' found in database names")
			}
		}
	})

	lcNamesOpts := mtest.NewOptions().MinServerVersion("4.0")
	mt.RunOpts("list collection names", lcNamesOpts, func(mt *mtest.T) {
		collName := "lcNamesCollection"
		mt.CreateCollection(mtest.Collection{Name: collName}, true)

		testCases := []struct {
			name   string
			filter bson.D
			found  bool
		}{
			{"no filter", bson.D{}, true},
			{"filter", bson.D{{"name", "lcNamesCollection"}}, true},
			{"filter not found", bson.D{{"name", "123"}}, false},
		}
		for _, tc := range testCases {
			mt.Run(tc.name, func(mt *mtest.T) {
				colls, err := mt.DB.ListCollectionNames(context.Background(), tc.filter)
				assert.Nil(mt, err, "ListCollectionNames error: %v", err)

				var found bool
				for _, coll := range colls {
					if coll == collName {
						found = true
						break
					}
				}

				assert.Equal(mt, tc.found, found, "expected to find collection: %v, found collection: %v", tc.found, found)
			})
		}
	})

	mt.RunOpts("list collections", noClientOpts, func(mt *mtest.T) {
		createCollections := func(mt *mtest.T, numCollections int) {
			mt.Helper()

			for i := 0; i < numCollections; i++ {
				mt.CreateCollection(mtest.Collection{
					Name: fmt.Sprintf("list-collections-test-%d", i),
				}, true)
			}
		}

		mt.RunOpts("verify results", noClientOpts, func(mt *mtest.T) {
			testCases := []struct {
				name             string
				expectedTopology mtest.TopologyKind
				cappedOnly       bool
			}{
				{"standalone no filter", mtest.Single, false},
				{"standalone filter", mtest.Single, true},
				{"replica set no filter", mtest.ReplicaSet, false},
				{"replica set filter", mtest.ReplicaSet, true},
				{"sharded no filter", mtest.Sharded, false},
				{"sharded filter", mtest.Sharded, true},
			}
			for _, tc := range testCases {
				tcOpts := mtest.NewOptions().Topologies(tc.expectedTopology)
				mt.RunOpts(tc.name, tcOpts, func(mt *mtest.T) {
					mt.CreateCollection(mtest.Collection{Name: listCollUncapped}, true)
					mt.CreateCollection(mtest.Collection{
						Name:       listCollCapped,
						CreateOpts: options.CreateCollection().SetCapped(true).SetSizeInBytes(64 * 1024),
					}, true)

					filter := bson.D{}
					if tc.cappedOnly {
						filter = bson.D{{"options.capped", true}}
					}

					var err error
					for i := 0; i < 1; i++ {
						cursor, err := mt.DB.ListCollections(context.Background(), filter)
						assert.Nil(mt, err, "ListCollections error (iteration %v): %v", i, err)

						err = verifyListCollections(cursor, tc.cappedOnly)
						if err == nil {
							return
						}
					}
					mt.Fatalf("error verifying list collections result: %v", err)
				})
			}
		})

		// For server versions below 3.0, we internally execute ListCollections() as a legacy
		// OP_QUERY against the system.namespaces collection. Command monitoring upconversions
		// translate this to a "find" command rather than "listCollections".
		cmdMonitoringCmdName := "listCollections"
		if mtest.CompareServerVersions(mtest.ServerVersion(), "3.0") < 0 {
			cmdMonitoringCmdName = "find"
		}
		mt.Run("batch size", func(mt *mtest.T) {
			// Create two new collections so there will be three total.
			createCollections(mt, 2)

			mt.ClearEvents()
			lcOpts := options.ListCollections().SetBatchSize(2)
			_, err := mt.DB.ListCollectionNames(context.Background(), bson.D{}, lcOpts)
			assert.Nil(mt, err, "ListCollectionNames error: %v", err)

			evt := mt.GetStartedEvent()
			assert.Equal(
				mt,
				cmdMonitoringCmdName,
				evt.CommandName,
				"expected %q command to be sent, got %q",
				cmdMonitoringCmdName,
				evt.CommandName)
			_, err = evt.Command.LookupErr("cursor", "batchSize")
			assert.Nil(mt, err, "expected command %s to contain key 'batchSize'", evt.Command)
		})
		mt.RunOpts("authorizedCollections", mtest.NewOptions().MinServerVersion("4.0"), func(mt *mtest.T) {
			mt.ClearEvents()
			lcOpts := options.ListCollections().SetAuthorizedCollections(true)
			_, err := mt.DB.ListCollections(context.Background(), bson.D{}, lcOpts)
			assert.Nil(mt, err, "ListCollections error: %v", err)

			evt := mt.GetStartedEvent()
			assert.Equal(mt, "listCollections", evt.CommandName,
				"expected 'listCollections' command to be sent, got %q", evt.CommandName)
			_, err = evt.Command.LookupErr("authorizedCollections")
			assert.Nil(mt, err, "expected command to contain key 'authorizedCollections'")

		})
		mt.Run("getMore commands are monitored", func(mt *mtest.T) {
			createCollections(mt, 2)
			assertGetMoreCommandsAreMonitored(mt, cmdMonitoringCmdName, func() (*mongo.Cursor, error) {
				return mt.DB.ListCollections(context.Background(), bson.D{}, options.ListCollections().SetBatchSize(2))
			})
		})
		mt.Run("killCursors commands are monitored", func(mt *mtest.T) {
			createCollections(mt, 2)
			assertKillCursorsCommandsAreMonitored(mt, cmdMonitoringCmdName, func() (*mongo.Cursor, error) {
				return mt.DB.ListCollections(context.Background(), bson.D{}, options.ListCollections().SetBatchSize(2))
			})
		})
	})

	mt.RunOpts("list collection specifications", noClientOpts, func(mt *mtest.T) {
		mt.Run("filter passed to listCollections", func(mt *mtest.T) {
			// Test that ListCollectionSpecifications correctly uses the supplied filter.
			cappedName := "list-collection-specs-capped"
			mt.CreateCollection(mtest.Collection{
				Name:       cappedName,
				CreateOpts: options.CreateCollection().SetCapped(true).SetSizeInBytes(4096),
			}, true)

			filter := bson.M{
				"options.capped": true,
			}
			cursor, err := mt.DB.ListCollections(context.Background(), filter)
			assert.Nil(mt, err, "ListCollections error: %v", err)
			defer cursor.Close(context.Background())
			assert.True(mt, cursor.Next(context.Background()), "expected Next to return true, got false; cursor error: %v",
				cursor.Err())

			optionsDoc := bsoncore.NewDocumentBuilder().
				AppendBoolean("capped", true).
				AppendInt32("size", 4096).
				Build()

			expectedSpec := mongo.CollectionSpecification{
				Name:     cappedName,
				Type:     "collection",
				ReadOnly: false,
				Options:  bson.Raw(optionsDoc),
			}
			if mtest.CompareServerVersions(mtest.ServerVersion(), "3.6") >= 0 {
				uuidSubtype, uuidData := cursor.Current.Lookup("info", "uuid").Binary()
				expectedSpec.UUID = &bson.Binary{Subtype: uuidSubtype, Data: uuidData}
			}
			if mtest.CompareServerVersions(mtest.ServerVersion(), "3.4") >= 0 {
				keysDoc := bsoncore.NewDocumentBuilder().
					AppendInt32("_id", 1).
					Build()
				expectedSpec.IDIndex = mongo.IndexSpecification{
					Name:         "_id_",
					Namespace:    mt.DB.Name() + "." + cappedName,
					KeysDocument: bson.Raw(keysDoc),
					Version:      2,
				}
			}

			specs, err := mt.DB.ListCollectionSpecifications(context.Background(), filter)
			assert.Nil(mt, err, "ListCollectionSpecifications error: %v", err)
			assert.Equal(mt, 1, len(specs), "expected 1 CollectionSpecification, got %d", len(specs))
			assert.Equal(mt, expectedSpec, specs[0], "expected specification %v, got %v", expectedSpec, specs[0])
		})

		mt.RunOpts("options passed to listCollections", mtest.NewOptions().MinServerVersion("3.0"), func(mt *mtest.T) {
			// Test that ListCollectionSpecifications correctly uses the supplied options.

			opts := options.ListCollections().SetNameOnly(true)
			_, err := mt.DB.ListCollectionSpecifications(context.Background(), bson.D{}, opts)
			assert.Nil(mt, err, "ListCollectionSpecifications error: %v", err)

			evt := mt.GetStartedEvent()
			assert.Equal(mt, "listCollections", evt.CommandName, "expected %q command to be sent, got %q",
				"listCollections", evt.CommandName)
			nameOnly, ok := evt.Command.Lookup("nameOnly").BooleanOK()
			assert.True(mt, ok, "expected command %v to contain %q field", evt.Command, "nameOnly")
			assert.True(mt, nameOnly, "expected nameOnly value to be true, got false")
		})
	})

	mt.RunOpts("run command cursor", noClientOpts, func(mt *mtest.T) {
		var data []interface{}
		for i := 0; i < 5; i++ {
			data = append(data, bson.D{{"x", i}})
		}
		findCollName := "runcommandcursor_find"
		findCmd := bson.D{{"find", findCollName}}
		aggCollName := "runcommandcursor_agg"
		aggCmd := bson.D{
			{"aggregate", aggCollName},
			{"pipeline", bson.A{}},
			{"cursor", bson.D{}},
		}
		pingCmd := bson.D{{"ping", 1}}
		pingErr := errors.New("database response does not contain a cursor; try using RunCommand instead")

		testCases := []struct {
			name        string
			collName    string
			cmd         interface{}
			toInsert    []interface{}
			expectedErr error
			numExpected int
			minVersion  string
		}{
			{"success find", findCollName, findCmd, data, nil, 5, "3.2"},
			{"success aggregate", aggCollName, aggCmd, data, nil, 5, ""},
			{"failures", "runcommandcursor_ping", pingCmd, nil, pingErr, 0, ""},
		}
		for _, tc := range testCases {
			tcOpts := mtest.NewOptions().CollectionName(tc.collName)
			if tc.minVersion != "" {
				tcOpts.MinServerVersion(tc.minVersion)
			}

			mt.RunOpts(tc.name, tcOpts, func(mt *mtest.T) {
				if len(tc.toInsert) > 0 {
					_, err := mt.Coll.InsertMany(context.Background(), tc.toInsert)
					assert.Nil(mt, err, "InsertMany error: %v", err)
				}

				cursor, err := mt.DB.RunCommandCursor(context.Background(), tc.cmd)
				assert.Equal(mt, tc.expectedErr, err, "expected error %v, got %v", tc.expectedErr, err)
				if tc.expectedErr != nil {
					return
				}

				var count int
				for cursor.Next(context.Background()) {
					count++
				}
				assert.Equal(mt, tc.numExpected, count, "expected document count %v, got %v", tc.numExpected, count)
			})
		}

		// The find command does not exist on server versions below 3.2.
		cmdMonitoringMtOpts := mtest.NewOptions().MinServerVersion("3.2")
		mt.RunOpts("getMore commands are monitored", cmdMonitoringMtOpts, func(mt *mtest.T) {
			initCollection(mt, mt.Coll)
			assertGetMoreCommandsAreMonitored(mt, "find", func() (*mongo.Cursor, error) {
				findCmd := bson.D{
					{"find", mt.Coll.Name()},
					{"batchSize", 2},
				}
				return mt.DB.RunCommandCursor(context.Background(), findCmd)
			})
		})
		mt.RunOpts("killCursors commands are monitored", cmdMonitoringMtOpts, func(mt *mtest.T) {
			initCollection(mt, mt.Coll)
			assertKillCursorsCommandsAreMonitored(mt, "find", func() (*mongo.Cursor, error) {
				findCmd := bson.D{
					{"find", mt.Coll.Name()},
					{"batchSize", 2},
				}
				return mt.DB.RunCommandCursor(context.Background(), findCmd)
			})
		})
	})

	mt.RunOpts("create collection", noClientOpts, func(mt *mtest.T) {
		collectionName := "create-collection-test"

		mt.RunOpts("options", noClientOpts, func(mt *mtest.T) {
			// Tests for various options combinations. The test creates a collection with some options and then verifies
			// the result using the options document reported by listCollections.

			// All possible options except collation and changeStreamPreAndPostImages. The collation is omitted here and tested below because the
			// collation document reported by listCollections fills in extra fields and includes a "version" field that's not described in
			// https://www.mongodb.com/docs/manual/reference/collation/. changeStreamPreAndPostImages is omitted here and tested in another testcase
			// because it is only an available option on 6.0+.
			storageEngine := bson.M{
				"wiredTiger": bson.M{
					"configString": "block_compressor=zlib",
				},
			}
			defaultIndexOpts := options.DefaultIndex().SetStorageEngine(storageEngine)
			validator := bson.M{
				"$or": bson.A{
					bson.M{
						"phone": bson.M{"$type": "string"},
					},
					bson.M{
						"email": bson.M{"$type": "string"},
					},
				},
			}
			nonCollationOpts := options.CreateCollection().
				SetCapped(true).
				SetDefaultIndexOptions(defaultIndexOpts).
				SetMaxDocuments(100).
				SetSizeInBytes(1024).
				SetStorageEngine(storageEngine).
				SetValidator(validator).
				SetValidationAction("warn").
				SetValidationLevel("moderate")
			nonCollationExpected := bson.M{
				"capped": true,
				"indexOptionDefaults": bson.M{
					"storageEngine": storageEngine,
				},
				"max":              int32(100),
				"size":             int32(1024),
				"storageEngine":    storageEngine,
				"validator":        validator,
				"validationAction": "warn",
				"validationLevel":  "moderate",
			}

			csppiOpts := options.CreateCollection().SetChangeStreamPreAndPostImages(bson.M{"enabled": true})
			csppiExpected := bson.M{
				"changeStreamPreAndPostImages": bson.M{
					"enabled": true,
				},
			}

			testCases := []struct {
				name             string
				minServerVersion string
				maxServerVersion string
				createOpts       *options.CreateCollectionOptionsBuilder
				expectedOpts     bson.M
			}{
				{"all options except collation and csppi", "3.2", "", nonCollationOpts, nonCollationExpected},
				{"changeStreamPreAndPostImages", "6.0", "", csppiOpts, csppiExpected},
			}

			for _, tc := range testCases {
				mtOpts := mtest.NewOptions().
					MinServerVersion(tc.minServerVersion).
					MaxServerVersion(tc.maxServerVersion)

				mt.RunOpts(tc.name, mtOpts, func(mt *mtest.T) {
					mt.CreateCollection(mtest.Collection{
						Name: collectionName,
					}, false)

					err := mt.DB.CreateCollection(context.Background(), collectionName, tc.createOpts)
					assert.Nil(mt, err, "CreateCollection error: %v", err)

					actualOpts := getCollectionOptions(mt, collectionName)
					assert.Equal(mt, tc.expectedOpts, actualOpts, "options mismatch; expected %v, got %v",
						tc.expectedOpts, actualOpts)
				})
			}
		})
		mt.RunOpts("collation", mtest.NewOptions().MinServerVersion("3.4"), func(mt *mtest.T) {
			mt.CreateCollection(mtest.Collection{
				Name: collectionName,
			}, false)

			locale := "en_US"
			createOpts := options.CreateCollection().SetCollation(&options.Collation{
				Locale: locale,
			})
			err := mt.DB.CreateCollection(context.Background(), collectionName, createOpts)
			assert.Nil(mt, err, "CreateCollection error: %v", err)

			actualOpts := getCollectionOptions(mt, collectionName)
			collationVal, ok := actualOpts["collation"]
			assert.True(mt, ok, "expected key 'collation' in collection options %v", actualOpts)
			collation := collationVal.(bson.M)
			assert.Equal(mt, locale, collation["locale"], "expected locale %v, got %v", locale, collation["locale"])
		})
		mt.Run("write concern", func(mt *mtest.T) {
			mt.CreateCollection(mtest.Collection{
				Name: collectionName,
			}, false)

			err := mt.DB.CreateCollection(context.Background(), collectionName)
			assert.Nil(mt, err, "CreateCollection error: %v", err)

			evt := mt.GetStartedEvent()
			assert.Equal(mt, evt.CommandName, "create", "expected event for 'create', got '%v'", evt.CommandName)
			_, err = evt.Command.LookupErr("writeConcern")
			assert.Nil(mt, err, "expected write concern to be included in command %v", evt.Command)
		})
	})

	mt.RunOpts("create view", mtest.NewOptions().CreateClient(false).MinServerVersion("3.4"), func(mt *mtest.T) {
		sourceCollectionName := "create-view-test-collection"
		viewName := "create-view-test-view"
		projectStage := bson.M{
			"$project": bson.M{
				"projectedField": "foo",
			},
		}
		pipeline := []bson.M{projectStage}

		mt.Run("function parameters are translated into options", func(mt *mtest.T) {
			mt.CreateCollection(mtest.Collection{
				Name: viewName,
			}, false)

			err := mt.DB.CreateView(context.Background(), viewName, sourceCollectionName, pipeline)
			assert.Nil(mt, err, "CreateView error: %v", err)

			expectedOpts := bson.M{
				"viewOn":   sourceCollectionName,
				"pipeline": bson.A{projectStage},
			}
			actualOpts := getCollectionOptions(mt, viewName)
			assert.Equal(mt, expectedOpts, actualOpts, "options mismatch; expected %v, got %v", expectedOpts,
				actualOpts)
		})
		mt.Run("collation", func(mt *mtest.T) {
			mt.CreateCollection(mtest.Collection{
				Name: viewName,
			}, false)

			locale := "en_US"
			viewOpts := options.CreateView().SetCollation(&options.Collation{
				Locale: locale,
			})
			err := mt.DB.CreateView(context.Background(), viewName, sourceCollectionName, mongo.Pipeline{}, viewOpts)
			assert.Nil(mt, err, "CreateView error: %v", err)

			actualOpts := getCollectionOptions(mt, viewName)
			collationVal, ok := actualOpts["collation"]
			assert.True(mt, ok, "expected key 'collation' in view options %v", actualOpts)
			collation := collationVal.(bson.M)
			assert.Equal(mt, locale, collation["locale"], "expected locale %v, got %v", locale, collation["locale"])
		})
	})
}

func getCollectionOptions(mt *mtest.T, collectionName string) bson.M {
	mt.Helper()

	filter := bson.M{
		"name": collectionName,
	}
	cursor, err := mt.DB.ListCollections(context.Background(), filter)
	assert.Nil(mt, err, "ListCollections error: %v", err)
	defer cursor.Close(context.Background())
	assert.True(mt, cursor.Next(context.Background()), "expected Next to return true, got false")

	var actualOpts bson.M
	docBytes := cursor.Current.Lookup("options").Document()
	dec := bson.NewDecoder(bson.NewDocumentReader(bytes.NewReader(docBytes)))
	dec.SetRegistry(interfaceAsMapRegistry)
	err = dec.Decode(&actualOpts)
	assert.Nil(mt, err, "UnmarshalWithRegistry error: %v", err)

	return actualOpts
}

func verifyListCollections(cursor *mongo.Cursor, cappedOnly bool) error {
	var cappedFound, uncappedFound bool

	for cursor.Next(context.Background()) {
		nameElem, err := cursor.Current.LookupErr("name")
		if err != nil {
			return fmt.Errorf("name element not found in document %v", cursor.Current)
		}
		if nameElem.Type != bson.TypeString {
			return fmt.Errorf("expected name type %v, got %v", bson.TypeString, nameElem.Type)
		}

		name := nameElem.StringValue()
		// legacy servers can return an indexes collection that shouldn't be considered here
		if name != listCollUncapped && name != listCollCapped {
			continue
		}

		if name == listCollUncapped && !uncappedFound {
			if cappedOnly {
				return fmt.Errorf("found uncapped collection %v but expected only capped collections", listCollUncapped)
			}

			uncappedFound = true
			continue
		}
		if name == listCollCapped && !cappedFound {
			cappedFound = true
			continue
		}

		// duplicate found
		return fmt.Errorf("found duplicate collection %v", name)
	}

	if !cappedFound {
		return fmt.Errorf("capped collection %v not found", listCollCapped)
	}
	if !cappedOnly && !uncappedFound {
		return fmt.Errorf("uncapped collection %v not found", listCollUncapped)
	}
	return nil
}
