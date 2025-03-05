// Copyright (C) MongoDB, Inc. 2017-present.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

//go:build cse
// +build cse

package integration

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/event"
	"go.mongodb.org/mongo-driver/v2/internal/assert"
	"go.mongodb.org/mongo-driver/v2/internal/handshake"
	"go.mongodb.org/mongo-driver/v2/internal/integration/mtest"
	"go.mongodb.org/mongo-driver/v2/internal/integtest"
	"go.mongodb.org/mongo-driver/v2/internal/require"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/writeconcern"
	"go.mongodb.org/mongo-driver/v2/x/bsonx/bsoncore"
	"go.mongodb.org/mongo-driver/v2/x/mongo/driver/mongocrypt"
	mongocryptopts "go.mongodb.org/mongo-driver/v2/x/mongo/driver/mongocrypt/options"
)

var (
	localMasterKey = []byte("2x44+xduTaBBkY16Er5DuADaghvS4vwdkg8tpPp3tz6gV01A1CwbD9itQ2HFDgPWOp8eMaC1Oi766JzXZBdBdbdMurdonJ1d")
)

const (
	clientEncryptionProseDir      = "../../testdata/client-side-encryption-prose"
	deterministicAlgorithm        = "AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic"
	randomAlgorithm               = "AEAD_AES_256_CBC_HMAC_SHA_512-Random"
	kvNamespace                   = "keyvault.datakeys" // default namespace for the key vault collection
	keySubtype               byte = 4                   // expected subtype for data keys
	encryptedValueSubtype    byte = 6                   // expected subtypes for encrypted values
	cryptMaxBatchSizeBytes        = 2097152             // max bytes in write batch when auto encryption is enabled
	maxBsonObjSize                = 16777216            // max bytes in BSON object
)

func TestClientSideEncryptionProse(t *testing.T) {
	t.Parallel()

	verifyClientSideEncryptionVarsSet(t)
	mt := mtest.New(t, mtest.NewOptions().MinServerVersion("4.2").Enterprise(true).CreateClient(false))

	defaultKvClientOptions := options.Client().ApplyURI(mtest.ClusterURI())
	integtest.AddTestServerAPIVersion(defaultKvClientOptions)
	fullKmsProvidersMap := map[string]map[string]interface{}{
		"aws": {
			"accessKeyId":     awsAccessKeyID,
			"secretAccessKey": awsSecretAccessKey,
		},
		"azure": {
			"tenantId":     azureTenantID,
			"clientId":     azureClientID,
			"clientSecret": azureClientSecret,
		},
		"gcp": {
			"email":      gcpEmail,
			"privateKey": gcpPrivateKey,
		},
		"local": {"key": localMasterKey},
		"kmip": {
			"endpoint": "localhost:5698",
		},
	}

	mt.Run("1. custom key material test", func(mt *mtest.T) {
		const (
			dkCollection = "datakeys"
			idKey        = "_id"
			kvDatabase   = "keyvault"
		)

		// Create a ClientEncryption object (referred to as client_encryption) with client set as the keyVaultClient.
		// Using client, drop the collection keyvault.datakeys.
		cse := setup(mt, nil, defaultKvClientOptions, options.ClientEncryption().
			SetKmsProviders(fullKmsProvidersMap).
			SetKeyVaultNamespace(kvNamespace))

		err := cse.kvClient.Database(kvDatabase).Collection(dkCollection).Drop(context.Background())
		assert.Nil(mt, err, "error dropping %q namespace: %v", kvNamespace, err)

		// Using client_encryption, create a data key with a local KMS provider and the declared b64 custom key material
		// (given as base64).
		const b641 = `xPTAjBRG5JiPm+d3fj6XLi2q5DMXUS/f1f+SMAlhhwkhDRL0kr8r9GDLIGTAGlvC+HVjSIgdL+RKwZCvpXSyxTICWSXT` +
			`UYsWYPyu3IoHbuBZdmw2faM3WhcRIgbMReU5`

		// Decode the base64-encoded keyMaterial string.
		km, err := base64.StdEncoding.DecodeString(b641)
		assert.Nil(mt, err, "error decoding b64: %v", err)

		_, err = cse.clientEnc.CreateDataKey(context.Background(), "local", options.DataKey().SetKeyMaterial(km))
		assert.Nil(mt, err, "error creating data key: %v", err)

		// Find the resulting key document in keyvault.datakeys, save a copy of the key document, then remove the key
		// document from the collection.
		coll := cse.kvClient.Database(kvDatabase).Collection(dkCollection)

		keydoc, err := coll.FindOne(context.Background(), bson.D{}).Raw()
		assert.Nil(mt, err, "error in decoding bytes: %v", err)

		// Remove the key document from the collection.
		id, err := keydoc.LookupErr(idKey)
		assert.Nil(mt, err, "error looking up %s: %v", idKey, err)

		_, err = coll.DeleteOne(context.Background(), bson.D{{idKey, id}})
		assert.Nil(mt, err, "error deleting key document: %v", err)

		// Replace the _id field in the copied key document with a UUID with base64 value AAAAAAAAAAAAAAAAAAAAAA== (16
		// bytes all equal to 0x00) and insert the modified key document into keyvault.datakeys with majority write
		// concern.
		cidx, alteredKeydoc := bsoncore.AppendDocumentStart(nil)
		docElems, _ := keydoc.Elements()
		for _, element := range docElems {
			if key := element.Key(); key != idKey {
				alteredKeydoc = bsoncore.AppendValueElement(alteredKeydoc, key, rawValueToCoreValue(element.Value()))
			}
		}
		empty := [16]byte{}
		uuidSubtype, _ := keydoc.Lookup(idKey).Binary()
		alteredKeydoc = bsoncore.AppendBinaryElement(alteredKeydoc, idKey, uuidSubtype, empty[:])
		alteredKeydoc, _ = bsoncore.AppendDocumentEnd(alteredKeydoc, cidx)

		// Insert the copied key document into keyvault.datakeys with majority write concern.
		wcMajority := writeconcern.Majority()
		wcMajorityCollectionOpts := options.Collection().SetWriteConcern(wcMajority)
		wcmColl := cse.kvClient.Database(kvDatabase).Collection(dkCollection, wcMajorityCollectionOpts)
		_, err = wcmColl.InsertOne(context.Background(), alteredKeydoc)
		assert.Nil(mt, err, "error inserting altered key document: %v", err)

		// Using client_encryption, encrypt the string "test" with the modified data key using the
		// AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic algorithm and assert the resulting value is equal to the
		// declared b64 constant.
		const b642 = `AQAAAAAAAAAAAAAAAAAAAAACz0ZOLuuhEYi807ZXTdhbqhLaS2/t9wLifJnnNYwiw79d75QYIZ6M/aYC1h9nCzCjZ7pG` +
			`UpAuNnkUhnIXM3PjrA==`

		empty = [16]byte{}
		keyid := bson.Binary{Subtype: 0x04, Data: empty[:]}

		encOpts := options.Encrypt().SetAlgorithm(deterministicAlgorithm).SetKeyID(keyid)
		testVal := bson.RawValue{
			Type:  bson.TypeString,
			Value: bsoncore.AppendString(nil, "test"),
		}
		actual, err := cse.clientEnc.Encrypt(context.Background(), testVal, encOpts)
		assert.Nil(mt, err, "error encrypting data: %v", err)

		expected := bson.Binary{Subtype: 0x06}
		expected.Data, _ = base64.StdEncoding.DecodeString(b642)

		assert.Equal(t, actual, expected, "expected: %v, got: %v", actual, expected)
	})
	mt.RunOpts("2. data key and double encryption", noClientOpts, func(mt *mtest.T) {
		// set up options structs
		schema := bson.D{
			{"bsonType", "object"},
			{"properties", bson.D{
				{"encrypted_placeholder", bson.D{
					{"encrypt", bson.D{
						{"keyId", "/placeholder"},
						{"bsonType", "string"},
						{"algorithm", "AEAD_AES_256_CBC_HMAC_SHA_512-Random"},
					}},
				}},
			}},
		}
		schemaMap := map[string]interface{}{"db.coll": schema}
		tlsConfig := make(map[string]*tls.Config)
		if tlsCAFileKMIP != "" && tlsClientCertificateKeyFileKMIP != "" {
			tlsOpts := map[string]interface{}{
				"tlsCertificateKeyFile": tlsClientCertificateKeyFileKMIP,
				"tlsCAFile":             tlsCAFileKMIP,
			}
			kmipConfig, err := options.BuildTLSConfig(tlsOpts)
			assert.Nil(mt, err, "BuildTLSConfig error: %v", err)
			tlsConfig["kmip"] = kmipConfig
		}

		aeo := options.AutoEncryption().
			SetKmsProviders(fullKmsProvidersMap).
			SetKeyVaultNamespace(kvNamespace).
			SetSchemaMap(schemaMap).
			SetTLSConfig(tlsConfig).
			SetExtraOptions(getCryptSharedLibExtraOptions())

		ceo := options.ClientEncryption().
			SetKmsProviders(fullKmsProvidersMap).
			SetKeyVaultNamespace(kvNamespace).
			SetTLSConfig(tlsConfig)

		awsMasterKey := bson.D{
			{"region", "us-east-1"},
			{"key", "arn:aws:kms:us-east-1:579766882180:key/89fcc2c4-08b0-4bd9-9f25-e30687b580d0"},
		}
		azureMasterKey := bson.D{
			{"keyVaultEndpoint", "key-vault-csfle.vault.azure.net"},
			{"keyName", "key-name-csfle"},
		}
		gcpMasterKey := bson.D{
			{"projectId", "devprod-drivers"},
			{"location", "global"},
			{"keyRing", "key-ring-csfle"},
			{"keyName", "key-name-csfle"},
		}
		kmipMasterKey := bson.D{}
		testCases := []struct {
			provider  string
			masterKey interface{}
		}{
			{"local", nil},
			{"aws", awsMasterKey},
			{"azure", azureMasterKey},
			{"gcp", gcpMasterKey},
			{"kmip", kmipMasterKey},
		}
		for _, tc := range testCases {
			mt.Run(tc.provider, func(mt *mtest.T) {
				if tc.provider == "kmip" && "" == os.Getenv("KMS_MOCK_SERVERS_RUNNING") {
					mt.Skipf("Skipping test as KMS_MOCK_SERVERS_RUNNING is not set")
				}
				var startedEvents []*event.CommandStartedEvent
				monitor := &event.CommandMonitor{
					Started: func(_ context.Context, evt *event.CommandStartedEvent) {
						startedEvents = append(startedEvents, evt)
					},
				}
				kvClientOpts := options.Client().ApplyURI(mtest.ClusterURI()).SetMonitor(monitor)
				integtest.AddTestServerAPIVersion(kvClientOpts)
				cpt := setup(mt, aeo, kvClientOpts, ceo)
				defer cpt.teardown(mt)

				// create data key
				keyAltName := fmt.Sprintf("%s_altname", tc.provider)
				dataKeyOpts := options.DataKey().SetKeyAltNames([]string{keyAltName})
				if tc.masterKey != nil {
					dataKeyOpts.SetMasterKey(tc.masterKey)
				}
				dataKeyID, err := cpt.clientEnc.CreateDataKey(context.Background(), tc.provider, dataKeyOpts)
				assert.Nil(mt, err, "CreateDataKey error: %v", err)
				assert.Equal(mt, keySubtype, dataKeyID.Subtype,
					"expected data key subtype %v, got %v", keySubtype, dataKeyID.Subtype)

				// assert that the key exists in the key vault
				cursor, err := cpt.keyVaultColl.Find(context.Background(), bson.D{{"_id", dataKeyID}})
				assert.Nil(mt, err, "key vault Find error: %v", err)
				assert.True(mt, cursor.Next(context.Background()), "no keys found in key vault")
				provider := cursor.Current.Lookup("masterKey", "provider").StringValue()
				assert.Equal(mt, tc.provider, provider, "expected provider %v, got %v", tc.provider, provider)
				assert.False(mt, cursor.Next(context.Background()), "unexpected document in key vault: %v", cursor.Current)

				// verify that the key was inserted using write concern majority
				assert.Equal(mt, 1, len(startedEvents), "expected 1 CommandStartedEvent, got %v", len(startedEvents))
				evt := startedEvents[0]
				assert.Equal(mt, "insert", evt.CommandName, "expected command 'insert', got '%v'", evt.CommandName)
				writeConcernVal, err := evt.Command.LookupErr("writeConcern")
				assert.Nil(mt, err, "expected writeConcern in command %s", evt.Command)
				wString := writeConcernVal.Document().Lookup("w").StringValue()
				assert.Equal(mt, "majority", wString, "expected write concern 'majority', got %v", wString)

				// encrypt a value with the new key by ID
				valueToEncrypt := fmt.Sprintf("hello %s", tc.provider)
				rawVal := bson.RawValue{Type: bson.TypeString, Value: bsoncore.AppendString(nil, valueToEncrypt)}
				encrypted, err := cpt.clientEnc.Encrypt(context.Background(), rawVal,
					options.Encrypt().SetAlgorithm(deterministicAlgorithm).SetKeyID(dataKeyID))
				assert.Nil(mt, err, "Encrypt error while encrypting value by ID: %v", err)
				assert.Equal(mt, encryptedValueSubtype, encrypted.Subtype,
					"expected encrypted value subtype %v, got %v", encryptedValueSubtype, encrypted.Subtype)

				// insert an encrypted value. the value shouldn't be encrypted again because it's not in the schema.
				_, err = cpt.cseColl.InsertOne(context.Background(), bson.D{{"_id", tc.provider}, {"value", encrypted}})
				assert.Nil(mt, err, "InsertOne error: %v", err)

				// find the inserted document. the value should be decrypted automatically
				resBytes, err := cpt.cseColl.FindOne(context.Background(), bson.D{{"_id", tc.provider}}).Raw()
				assert.Nil(mt, err, "Find error: %v", err)
				foundVal := resBytes.Lookup("value").StringValue()
				assert.Equal(mt, valueToEncrypt, foundVal, "expected value %v, got %v", valueToEncrypt, foundVal)

				// encrypt a value with an alternate name for the new key
				altEncrypted, err := cpt.clientEnc.Encrypt(context.Background(), rawVal,
					options.Encrypt().SetAlgorithm(deterministicAlgorithm).SetKeyAltName(keyAltName))
				assert.Nil(mt, err, "Encrypt error while encrypting value by alt key name: %v", err)
				assert.Equal(mt, encryptedValueSubtype, altEncrypted.Subtype,
					"expected encrypted value subtype %v, got %v", encryptedValueSubtype, altEncrypted.Subtype)
				assert.Equal(mt, encrypted.Data, altEncrypted.Data,
					"expected data %v, got %v", encrypted.Data, altEncrypted.Data)

				// insert an encrypted value for an auto-encrypted field
				_, err = cpt.cseColl.InsertOne(context.Background(), bson.D{{"encrypted_placeholder", encrypted}})
				assert.NotNil(mt, err, "expected InsertOne error, got nil")
			})
		}
	})
	mt.RunOpts("3. external key vault", noClientOpts, func(mt *mtest.T) {
		testCases := []struct {
			name          string
			externalVault bool
		}{
			{"with external vault", true},
			{"without external vault", false},
		}

		for _, tc := range testCases {
			mt.Run(tc.name, func(mt *mtest.T) {
				// setup options structs
				kmsProviders := map[string]map[string]interface{}{
					"local": {
						"key": localMasterKey,
					},
				}
				schemaMap := map[string]interface{}{"db.coll": readJSONFile(mt, "external-schema.json")}
				aeo := options.AutoEncryption().
					SetKmsProviders(kmsProviders).
					SetKeyVaultNamespace(kvNamespace).
					SetSchemaMap(schemaMap).
					SetExtraOptions(getCryptSharedLibExtraOptions())
				ceo := options.ClientEncryption().
					SetKmsProviders(kmsProviders).
					SetKeyVaultNamespace(kvNamespace)
				kvClientOpts := defaultKvClientOptions

				if tc.externalVault {
					externalKvOpts := options.Client().ApplyURI(mtest.ClusterURI()).SetAuth(options.Credential{
						Username: "fake-user",
						Password: "fake-password",
					})
					integtest.AddTestServerAPIVersion(externalKvOpts)
					aeo.SetKeyVaultClientOptions(externalKvOpts)
					kvClientOpts = externalKvOpts
				}
				cpt := setup(mt, aeo, kvClientOpts, ceo)
				defer cpt.teardown(mt)

				// manually insert data key
				key := readJSONFile(mt, "external-key.json")
				_, err := cpt.keyVaultColl.InsertOne(context.Background(), key)
				assert.Nil(mt, err, "InsertOne error for data key: %v", err)
				subtype, data := key.Lookup("_id").Binary()
				dataKeyID := bson.Binary{Subtype: subtype, Data: data}

				doc := bson.D{{"encrypted", "test"}}
				_, insertErr := cpt.cseClient.Database("db").Collection("coll").InsertOne(context.Background(), doc)
				rawVal := bson.RawValue{Type: bson.TypeString, Value: bsoncore.AppendString(nil, "test")}
				_, encErr := cpt.clientEnc.Encrypt(context.Background(), rawVal,
					options.Encrypt().SetKeyID(dataKeyID).SetAlgorithm(deterministicAlgorithm))

				if tc.externalVault {
					assert.NotNil(mt, insertErr, "expected InsertOne auth error, got nil")
					assert.NotNil(mt, encErr, "expected Encrypt auth error, got nil")
					assert.True(mt, strings.Contains(insertErr.Error(), "auth error"),
						"expected InsertOne auth error, got %v", insertErr)
					assert.True(mt, strings.Contains(encErr.Error(), "auth error"),
						"expected Encrypt auth error, got %v", encErr)
					return
				}
				assert.Nil(mt, insertErr, "InsertOne error: %v", insertErr)
				assert.Nil(mt, encErr, "Encrypt error: %v", err)
			})
		}
	})
	mt.Run("4. bson size limits", func(mt *mtest.T) {
		kmsProviders := map[string]map[string]interface{}{
			"local": {
				"key": localMasterKey,
			},
		}
		aeo := options.AutoEncryption().
			SetKmsProviders(kmsProviders).
			SetKeyVaultNamespace(kvNamespace).
			SetExtraOptions(getCryptSharedLibExtraOptions())
		cpt := setup(mt, aeo, nil, nil)
		defer cpt.teardown(mt)

		// create coll with JSON schema
		err := mt.Client.Database("db").RunCommand(context.Background(), bson.D{
			{"create", "coll"},
			{"validator", bson.D{
				{"$jsonSchema", readJSONFile(mt, "limits-schema.json")},
			}},
		}).Err()
		assert.Nil(mt, err, "create error with validator: %v", err)

		// insert key
		key := readJSONFile(mt, "limits-key.json")
		_, err = cpt.keyVaultColl.InsertOne(context.Background(), key)
		assert.Nil(mt, err, "InsertOne error for key: %v", err)

		var builder2mb, builder16mb strings.Builder
		for i := 0; i < cryptMaxBatchSizeBytes; i++ {
			builder2mb.WriteByte('a')
		}
		for i := 0; i < maxBsonObjSize; i++ {
			builder16mb.WriteByte('a')
		}
		complete2mbStr := builder2mb.String()
		complete16mbStr := builder16mb.String()

		// insert a document over 2MiB
		doc := bson.D{{"over_2mib_under_16mib", complete2mbStr}}
		_, err = cpt.cseColl.InsertOne(context.Background(), doc)
		assert.Nil(mt, err, "InsertOne error for 2MiB document: %v", err)

		str := complete2mbStr[:cryptMaxBatchSizeBytes-2000] // remove last 2000 bytes
		limitsDoc := readJSONFile(mt, "limits-doc.json")

		// insert a doc smaller than 2MiB that is bigger than 2MiB after encryption
		var extendedLimitsDoc []byte
		extendedLimitsDoc = append(extendedLimitsDoc, limitsDoc...)
		extendedLimitsDoc = extendedLimitsDoc[:len(extendedLimitsDoc)-1] // remove last byte to add new fields
		extendedLimitsDoc = bsoncore.AppendStringElement(extendedLimitsDoc, "_id", "encryption_exceeds_2mib")
		extendedLimitsDoc = bsoncore.AppendStringElement(extendedLimitsDoc, "unencrypted", str)
		extendedLimitsDoc, _ = bsoncore.AppendDocumentEnd(extendedLimitsDoc, 0)
		_, err = cpt.cseColl.InsertOne(context.Background(), extendedLimitsDoc)
		assert.Nil(mt, err, "error inserting extended limits document: %v", err)

		// bulk insert two 2MiB documents, each over 2 MiB
		// each document should be split into its own batch because the documents are bigger than 2MiB but smaller
		// than 16MiB
		cpt.cseStarted = cpt.cseStarted[:0]
		firstDoc := bson.D{{"_id", "over_2mib_1"}, {"unencrypted", complete2mbStr}}
		secondDoc := bson.D{{"_id", "over_2mib_2"}, {"unencrypted", complete2mbStr}}
		_, err = cpt.cseColl.InsertMany(context.Background(), []interface{}{firstDoc, secondDoc})
		assert.Nil(mt, err, "InsertMany error for small documents: %v", err)
		assert.Equal(mt, 2, len(cpt.cseStarted), "expected 2 insert events, got %d", len(cpt.cseStarted))

		// bulk insert two documents
		str = complete2mbStr[:cryptMaxBatchSizeBytes-20000]
		firstBulkDoc := make([]byte, len(limitsDoc))
		copy(firstBulkDoc, limitsDoc)
		firstBulkDoc = firstBulkDoc[:len(firstBulkDoc)-1] // remove last byte to append new fields
		firstBulkDoc = bsoncore.AppendStringElement(firstBulkDoc, "_id", "encryption_exceeds_2mib_1")
		firstBulkDoc = bsoncore.AppendStringElement(firstBulkDoc, "unencrypted", string(str))
		firstBulkDoc, _ = bsoncore.AppendDocumentEnd(firstBulkDoc, 0)

		secondBulkDoc := make([]byte, len(limitsDoc))
		copy(secondBulkDoc, limitsDoc)
		secondBulkDoc = secondBulkDoc[:len(secondBulkDoc)-1] // remove last byte to append new fields
		secondBulkDoc = bsoncore.AppendStringElement(secondBulkDoc, "_id", "encryption_exceeds_2mib_2")
		secondBulkDoc = bsoncore.AppendStringElement(secondBulkDoc, "unencrypted", string(str))
		secondBulkDoc, _ = bsoncore.AppendDocumentEnd(secondBulkDoc, 0)

		cpt.cseStarted = cpt.cseStarted[:0]
		_, err = cpt.cseColl.InsertMany(context.Background(), []interface{}{firstBulkDoc, secondBulkDoc})
		assert.Nil(mt, err, "InsertMany error for large documents: %v", err)
		assert.Equal(mt, 2, len(cpt.cseStarted), "expected 2 insert events, got %d", len(cpt.cseStarted))

		// insert a document slightly smaller than 16MiB and expect the operation to succeed
		doc = bson.D{{"_id", "under_16mib"}, {"unencrypted", complete16mbStr[:maxBsonObjSize-2000]}}
		_, err = cpt.cseColl.InsertOne(context.Background(), doc)
		assert.Nil(mt, err, "InsertOne error: %v", err)

		// insert a document over 16MiB and expect the operation to fail
		var over16mb []byte
		over16mb = append(over16mb, limitsDoc...)
		over16mb = over16mb[:len(over16mb)-1] // remove last byte
		over16mb = bsoncore.AppendStringElement(over16mb, "_id", "encryption_exceeds_16mib")
		over16mb = bsoncore.AppendStringElement(over16mb, "unencrypted", complete16mbStr[:maxBsonObjSize-2000])
		over16mb, _ = bsoncore.AppendDocumentEnd(over16mb, 0)
		_, err = cpt.cseColl.InsertOne(context.Background(), over16mb)
		assert.NotNil(mt, err, "expected InsertOne error for document over 16MiB, got nil")
	})
	mt.Run("5. views are prohibited", func(mt *mtest.T) {
		mt.Parallel()

		kmsProviders := map[string]map[string]interface{}{
			"local": {
				"key": localMasterKey,
			},
		}
		aeo := options.AutoEncryption().
			SetKmsProviders(kmsProviders).
			SetKeyVaultNamespace(kvNamespace).
			SetExtraOptions(getCryptSharedLibExtraOptions())
		cpt := setup(mt, aeo, nil, nil)
		defer cpt.teardown(mt)

		// create view on db.coll
		mt.CreateCollection(mtest.Collection{
			Name:         "view",
			DB:           cpt.cseColl.Database().Name(),
			ViewOn:       "coll",
			ViewPipeline: mongo.Pipeline{},
		}, true)

		view := cpt.cseColl.Database().Collection("view")
		_, err := view.InsertOne(context.Background(), bson.D{{"_id", "insert_on_view"}})
		assert.NotNil(mt, err, "expected InsertOne error on view, got nil")
		errStr := strings.ToLower(err.Error())
		viewErrSubstr := "cannot auto encrypt a view"
		assert.True(mt, strings.Contains(errStr, viewErrSubstr),
			"expected error '%v' to contain substring '%v'", errStr, viewErrSubstr)
	})
	mt.RunOpts("6. corpus test", noClientOpts, func(mt *mtest.T) {
		if "" == os.Getenv("KMS_MOCK_SERVERS_RUNNING") {
			mt.Skipf("Skipping test as KMS_MOCK_SERVERS_RUNNING is not set")
		}
		corpusSchema := readJSONFile(mt, "corpus-schema.json")
		localSchemaMap := map[string]interface{}{
			"db.coll": corpusSchema,
		}

		tlsConfig := make(map[string]*tls.Config)
		if tlsCAFileKMIP != "" && tlsClientCertificateKeyFileKMIP != "" {
			tlsOpts := map[string]interface{}{
				"tlsCertificateKeyFile": tlsClientCertificateKeyFileKMIP,
				"tlsCAFile":             tlsCAFileKMIP,
			}
			kmipConfig, err := options.BuildTLSConfig(tlsOpts)
			assert.Nil(mt, err, "BuildTLSConfig error: %v", err)
			tlsConfig["kmip"] = kmipConfig
		}

		getBaseAutoEncryptionOpts := func() *options.AutoEncryptionOptions {
			return options.AutoEncryption().
				SetKmsProviders(fullKmsProvidersMap).
				SetKeyVaultNamespace(kvNamespace).
				SetTLSConfig(tlsConfig).
				SetExtraOptions(getCryptSharedLibExtraOptions())
		}

		testCases := []struct {
			name   string
			aeo    *options.AutoEncryptionOptions
			schema bson.Raw // the schema to create the collection. if nil, the collection won't be explicitly created
		}{
			{"remote schema", getBaseAutoEncryptionOpts(), corpusSchema},
			{"local schema", getBaseAutoEncryptionOpts().SetSchemaMap(localSchemaMap), nil},
		}

		for _, tc := range testCases {
			mt.Run(tc.name, func(mt *mtest.T) {
				ceo := options.ClientEncryption().
					SetKmsProviders(fullKmsProvidersMap).
					SetKeyVaultNamespace(kvNamespace).
					SetTLSConfig(tlsConfig)

				cpt := setup(mt, tc.aeo, defaultKvClientOptions, ceo)
				defer cpt.teardown(mt)

				// create collection with JSON schema
				if tc.schema != nil {
					db := cpt.coll.Database()
					err := db.RunCommand(context.Background(), bson.D{
						{"create", "coll"},
						{"validator", bson.D{
							{"$jsonSchema", readJSONFile(mt, "corpus-schema.json")},
						}},
					}).Err()
					assert.Nil(mt, err, "create error with validator: %v", err)
				}

				// Manually insert keys for each KMS provider into the key vault.
				_, err := cpt.keyVaultColl.InsertMany(context.Background(), []interface{}{
					readJSONFile(mt, "corpus-key-local.json"),
					readJSONFile(mt, "corpus-key-aws.json"),
					readJSONFile(mt, "corpus-key-azure.json"),
					readJSONFile(mt, "corpus-key-gcp.json"),
					readJSONFile(mt, "corpus-key-kmip.json"),
				})
				assert.Nil(mt, err, "InsertMany error for key vault: %v", err)

				// read original corpus and recursively copy over each value to new corpus, encrypting certain values
				// when needed
				corpus := readJSONFile(mt, "corpus.json")
				cidx, copied := bsoncore.AppendDocumentStart(nil)
				elems, _ := corpus.Elements()

				// Keys for top-level non-document elements that should be copied directly.
				copiedKeys := map[string]struct{}{
					"_id":           {},
					"altname_aws":   {},
					"altname_local": {},
					"altname_azure": {},
					"altname_gcp":   {},
					"altname_kmip":  {},
				}

				for _, elem := range elems {
					key := elem.Key()
					val := elem.Value()

					if _, ok := copiedKeys[key]; ok {
						copied = bsoncore.AppendStringElement(copied, key, val.StringValue())
						continue
					}

					doc := val.Document()
					switch method := doc.Lookup("method").StringValue(); method {
					case "auto":
						// Copy the value directly because it will be auto-encrypted later.
						copied = bsoncore.AppendDocumentElement(copied, key, doc)
						continue
					case "explicit":
						// Handled below.
					default:
						mt.Fatalf("unrecognized 'method' value %q", method)
					}

					// explicitly encrypt value
					algorithm := deterministicAlgorithm
					if doc.Lookup("algo").StringValue() == "rand" {
						algorithm = randomAlgorithm
					}
					eo := options.Encrypt().SetAlgorithm(algorithm)

					identifier := doc.Lookup("identifier").StringValue()
					kms := doc.Lookup("kms").StringValue()
					switch identifier {
					case "id":
						var keyID string
						switch kms {
						case "local":
							keyID = "LOCALAAAAAAAAAAAAAAAAA=="
						case "aws":
							keyID = "AWSAAAAAAAAAAAAAAAAAAA=="
						case "azure":
							keyID = "AZUREAAAAAAAAAAAAAAAAA=="
						case "gcp":
							keyID = "GCPAAAAAAAAAAAAAAAAAAA=="
						case "kmip":
							keyID = "KMIPAAAAAAAAAAAAAAAAAA=="
						default:
							mt.Fatalf("unrecognized KMS provider %q", kms)
						}

						keyIDBytes, err := base64.StdEncoding.DecodeString(keyID)
						assert.Nil(mt, err, "base64 DecodeString error: %v", err)
						eo.SetKeyID(bson.Binary{Subtype: 4, Data: keyIDBytes})
					case "altname":
						eo.SetKeyAltName(kms) // alt name for a key is the same as the KMS name
					default:
						mt.Fatalf("unrecognized identifier: %v", identifier)
					}

					// iterate over all elements in the document. copy elements directly, except for ones that need to
					// be encrypted, which should be copied after encryption.
					var nestedIdx int32
					nestedIdx, copied = bsoncore.AppendDocumentElementStart(copied, key)
					docElems, _ := doc.Elements()
					for _, de := range docElems {
						deKey := de.Key()
						deVal := de.Value()

						// element to encrypt has key "value"
						if deKey != "value" {
							copied = bsoncore.AppendValueElement(copied, deKey, rawValueToCoreValue(deVal))
							continue
						}

						encrypted, err := cpt.clientEnc.Encrypt(context.Background(), deVal, eo)
						if !doc.Lookup("allowed").Boolean() {
							// if allowed is false, encryption should error. in this case, the unencrypted value should be
							// copied over
							assert.NotNil(mt, err, "expected error encrypting value for key %v, got nil", key)
							copied = bsoncore.AppendValueElement(copied, deKey, rawValueToCoreValue(deVal))
							continue
						}

						// copy encrypted value
						assert.Nil(mt, err, "Encrypt error for key %v: %v", key, err)
						copied = bsoncore.AppendBinaryElement(copied, deKey, encrypted.Subtype, encrypted.Data)
					}
					copied, _ = bsoncore.AppendDocumentEnd(copied, nestedIdx)
				}
				copied, _ = bsoncore.AppendDocumentEnd(copied, cidx)

				// insert document with encrypted values
				_, err = cpt.cseColl.InsertOne(context.Background(), copied)
				assert.Nil(mt, err, "InsertOne error for corpus document: %v", err)

				// find document using client with encryption and assert it matches original
				decryptedDoc, err := cpt.cseColl.FindOne(context.Background(), bson.D{}).Raw()
				assert.Nil(mt, err, "Find error with encrypted client: %v", err)
				assert.Equal(mt, corpus, decryptedDoc, "expected document %v, got %v", corpus, decryptedDoc)

				// find document using a client without encryption enabled and assert fields remain encrypted
				corpusEncrypted := readJSONFile(mt, "corpus-encrypted.json")
				foundDoc, err := cpt.coll.FindOne(context.Background(), bson.D{}).Raw()
				assert.Nil(mt, err, "Find error with unencrypted client: %v", err)

				encryptedElems, _ := corpusEncrypted.Elements()
				for _, encryptedElem := range encryptedElems {
					// skip non-document fields
					encryptedDoc, ok := encryptedElem.Value().DocumentOK()
					if !ok {
						continue
					}

					allowed := encryptedDoc.Lookup("allowed").Boolean()
					expectedKey := encryptedElem.Key()
					expectedVal := encryptedDoc.Lookup("value")
					foundVal := foundDoc.Lookup(expectedKey).Document().Lookup("value")

					// for deterministic encryption, the value should be exactly equal
					// for random encryption, the value should not be equal if allowed is true
					algo := encryptedDoc.Lookup("algo").StringValue()
					switch algo {
					case "det":
						assert.True(mt, expectedVal.Equal(foundVal),
							"expected value %v for key %v, got %v", expectedVal, expectedKey, foundVal)
					case "rand":
						if allowed {
							assert.False(mt, expectedVal.Equal(foundVal),
								"expected values for key %v to be different but were %v", expectedKey, expectedVal)
						}
					}

					// if allowed is true, decrypt both values with clientEnc and validate equality
					if allowed {
						sub, data := expectedVal.Binary()
						expectedDecrypted, err := cpt.clientEnc.Decrypt(context.Background(), bson.Binary{Subtype: sub, Data: data})
						assert.Nil(mt, err, "Decrypt error: %v", err)
						sub, data = foundVal.Binary()
						actualDecrypted, err := cpt.clientEnc.Decrypt(context.Background(), bson.Binary{Subtype: sub, Data: data})
						assert.Nil(mt, err, "Decrypt error: %v", err)

						assert.True(mt, expectedDecrypted.Equal(actualDecrypted),
							"expected decrypted value %v for key %v, got %v", expectedDecrypted, expectedKey, actualDecrypted)
						continue
					}

					// if allowed is false, validate found value equals the original value in corpus
					corpusVal := corpus.Lookup(expectedKey).Document().Lookup("value")
					assert.True(mt, corpusVal.Equal(foundVal),
						"expected value %v for key %v, got %v", corpusVal, expectedKey, foundVal)
				}
			})
		}
	})
	mt.Run("7. custom endpoint", func(mt *mtest.T) {
		validKmsProviders := map[string]map[string]interface{}{
			"aws": {
				"accessKeyId":     awsAccessKeyID,
				"secretAccessKey": awsSecretAccessKey,
			},
			"azure": {
				"tenantId":                 azureTenantID,
				"clientId":                 azureClientID,
				"clientSecret":             azureClientSecret,
				"identityPlatformEndpoint": "login.microsoftonline.com:443",
			},
			"gcp": {
				"email":      gcpEmail,
				"privateKey": gcpPrivateKey,
				"endpoint":   "oauth2.googleapis.com:443",
			},
			"kmip": {
				"endpoint": "localhost:5698",
			},
		}

		tlsConfig := make(map[string]*tls.Config)
		if tlsCAFileKMIP != "" && tlsClientCertificateKeyFileKMIP != "" {
			tlsOpts := map[string]interface{}{
				"tlsCertificateKeyFile": tlsClientCertificateKeyFileKMIP,
				"tlsCAFile":             tlsCAFileKMIP,
			}
			kmipConfig, err := options.BuildTLSConfig(tlsOpts)
			assert.Nil(mt, err, "BuildTLSConfig error: %v", err)
			tlsConfig["kmip"] = kmipConfig
		}

		validClientEncryptionOptions := options.ClientEncryption().
			SetKmsProviders(validKmsProviders).
			SetKeyVaultNamespace(kvNamespace).
			SetTLSConfig(tlsConfig)

		invalidKmsProviders := map[string]map[string]interface{}{
			"azure": {
				"tenantId":                 azureTenantID,
				"clientId":                 azureClientID,
				"clientSecret":             azureClientSecret,
				"identityPlatformEndpoint": "doesnotexist.invalid:443",
			},
			"gcp": {
				"email":      gcpEmail,
				"privateKey": gcpPrivateKey,
				"endpoint":   "doesnotexist.invalid:443",
			},
			"kmip": {
				"endpoint": "doesnotexist.local:5698",
			},
		}

		invalidClientEncryptionOptions := options.ClientEncryption().
			SetKmsProviders(invalidKmsProviders).
			SetKeyVaultNamespace(kvNamespace).
			SetTLSConfig(tlsConfig)

		awsSuccessWithoutEndpoint := map[string]interface{}{
			"region": "us-east-1",
			"key":    "arn:aws:kms:us-east-1:579766882180:key/89fcc2c4-08b0-4bd9-9f25-e30687b580d0",
		}
		awsSuccessWithEndpoint := map[string]interface{}{
			"region":   "us-east-1",
			"key":      "arn:aws:kms:us-east-1:579766882180:key/89fcc2c4-08b0-4bd9-9f25-e30687b580d0",
			"endpoint": "kms.us-east-1.amazonaws.com",
		}
		awsSuccessWithHTTPSEndpoint := map[string]interface{}{
			"region":   "us-east-1",
			"key":      "arn:aws:kms:us-east-1:579766882180:key/89fcc2c4-08b0-4bd9-9f25-e30687b580d0",
			"endpoint": "kms.us-east-1.amazonaws.com:443",
		}
		awsFailureConnectionError := map[string]interface{}{
			"region":   "us-east-1",
			"key":      "arn:aws:kms:us-east-1:579766882180:key/89fcc2c4-08b0-4bd9-9f25-e30687b580d0",
			"endpoint": "kms.us-east-1.amazonaws.com:12345",
		}
		awsFailureInvalidEndpoint := map[string]interface{}{
			"region":   "us-east-1",
			"key":      "arn:aws:kms:us-east-1:579766882180:key/89fcc2c4-08b0-4bd9-9f25-e30687b580d0",
			"endpoint": "kms.us-east-2.amazonaws.com",
		}
		awsFailureParseError := map[string]interface{}{
			"region":   "us-east-1",
			"key":      "arn:aws:kms:us-east-1:579766882180:key/89fcc2c4-08b0-4bd9-9f25-e30687b580d0",
			"endpoint": "doesnotexist.invalid",
		}
		azure := map[string]interface{}{
			"keyVaultEndpoint": "key-vault-csfle.vault.azure.net",
			"keyName":          "key-name-csfle",
		}
		gcpSuccess := map[string]interface{}{
			"projectId": "devprod-drivers",
			"location":  "global",
			"keyRing":   "key-ring-csfle",
			"keyName":   "key-name-csfle",
			"endpoint":  "cloudkms.googleapis.com:443",
		}
		gcpFailure := map[string]interface{}{
			"projectId": "devprod-drivers",
			"location":  "global",
			"keyRing":   "key-ring-csfle",
			"keyName":   "key-name-csfle",
			"endpoint":  "doesnotexist.invalid:443",
		}
		kmipSuccessWithoutEndpoint := map[string]interface{}{
			"keyId": "1",
		}
		kmipSuccessWithEndpoint := map[string]interface{}{
			"keyId":    "1",
			"endpoint": "localhost:5698",
		}
		kmipFailureInvalidEndpoint := map[string]interface{}{
			"keyId":    "1",
			"endpoint": "doesnotexist.local:5698",
		}

		const (
			errConnectionRefused           = "connection refused"
			errInvalidKMSResponse          = "Invalid KMS response"
			errMongocryptError             = "mongocrypt error"
			errNoSuchHost                  = "no such host"
			errServerMisbehaving           = "server misbehaving"
			errWindowsTLSConnectionRefused = "No connection could be made because the target machine actively refused it"
		)

		testCases := []struct {
			name                                  string
			provider                              string
			masterKey                             interface{}
			errorSubstring                        []string
			testInvalidClientEncryption           bool
			invalidClientEncryptionErrorSubstring []string
		}{
			{
				name:                                  "Case 1: aws success without endpoint",
				provider:                              "aws",
				masterKey:                             awsSuccessWithoutEndpoint,
				errorSubstring:                        []string{},
				testInvalidClientEncryption:           false,
				invalidClientEncryptionErrorSubstring: []string{},
			},
			{
				name:                                  "Case 2: aws success with endpoint",
				provider:                              "aws",
				masterKey:                             awsSuccessWithEndpoint,
				errorSubstring:                        []string{},
				testInvalidClientEncryption:           false,
				invalidClientEncryptionErrorSubstring: []string{},
			},
			{
				name:                                  "Case 3: aws success with https endpoint",
				provider:                              "aws",
				masterKey:                             awsSuccessWithHTTPSEndpoint,
				errorSubstring:                        []string{},
				testInvalidClientEncryption:           false,
				invalidClientEncryptionErrorSubstring: []string{},
			},
			{
				name:                                  "Case 4: aws failure with connection error",
				provider:                              "aws",
				masterKey:                             awsFailureConnectionError,
				errorSubstring:                        []string{errConnectionRefused, errWindowsTLSConnectionRefused},
				testInvalidClientEncryption:           false,
				invalidClientEncryptionErrorSubstring: []string{},
			},
			{
				name:                                  "Case 5: aws failure with wrong endpoint",
				provider:                              "aws",
				masterKey:                             awsFailureInvalidEndpoint,
				errorSubstring:                        []string{errMongocryptError},
				testInvalidClientEncryption:           false,
				invalidClientEncryptionErrorSubstring: []string{},
			},
			{
				name:                                  "Case 6: aws failure with parse error",
				provider:                              "aws",
				masterKey:                             awsFailureParseError,
				errorSubstring:                        []string{errNoSuchHost, errServerMisbehaving},
				testInvalidClientEncryption:           false,
				invalidClientEncryptionErrorSubstring: []string{},
			},
			{
				name:                                  "Case 7: azure success",
				provider:                              "azure",
				masterKey:                             azure,
				errorSubstring:                        []string{},
				testInvalidClientEncryption:           true,
				invalidClientEncryptionErrorSubstring: []string{errNoSuchHost, errServerMisbehaving},
			},
			{
				name:                                  "Case 8: gcp success",
				provider:                              "gcp",
				masterKey:                             gcpSuccess,
				errorSubstring:                        []string{},
				testInvalidClientEncryption:           true,
				invalidClientEncryptionErrorSubstring: []string{errNoSuchHost, errServerMisbehaving},
			},
			{
				name:                                  "Case 9: gcp failure",
				provider:                              "gcp",
				masterKey:                             gcpFailure,
				errorSubstring:                        []string{errInvalidKMSResponse},
				testInvalidClientEncryption:           false,
				invalidClientEncryptionErrorSubstring: []string{},
			},
			{
				name:                                  "Case 10: kmip success without endpoint",
				provider:                              "kmip",
				masterKey:                             kmipSuccessWithoutEndpoint,
				errorSubstring:                        []string{},
				testInvalidClientEncryption:           true,
				invalidClientEncryptionErrorSubstring: []string{errNoSuchHost, errServerMisbehaving},
			},
			{
				name:                                  "Case 11: kmip success with endpoint",
				provider:                              "kmip",
				masterKey:                             kmipSuccessWithEndpoint,
				errorSubstring:                        []string{},
				testInvalidClientEncryption:           false,
				invalidClientEncryptionErrorSubstring: []string{},
			},
			{
				name:                                  "Case 12: kmip failure with invalid endpoint",
				provider:                              "kmip",
				masterKey:                             kmipFailureInvalidEndpoint,
				errorSubstring:                        []string{errNoSuchHost, errServerMisbehaving},
				testInvalidClientEncryption:           false,
				invalidClientEncryptionErrorSubstring: []string{},
			},
		}
		for _, tc := range testCases {
			mt.Run(tc.name, func(mt *mtest.T) {
				if strings.Contains(tc.name, "kmip") && "" == os.Getenv("KMS_MOCK_SERVERS_RUNNING") {
					mt.Skipf("Skipping test as KMS_MOCK_SERVERS_RUNNING is not set")
				}
				cpt := setup(mt, nil, defaultKvClientOptions, validClientEncryptionOptions)
				defer cpt.teardown(mt)

				dkOpts := options.DataKey().SetMasterKey(tc.masterKey)
				createdKey, err := cpt.clientEnc.CreateDataKey(context.Background(), tc.provider, dkOpts)
				if len(tc.errorSubstring) > 0 {
					assert.NotNil(mt, err, "expected error, got nil")

					assert.True(t, containsPattern(tc.errorSubstring, err.Error()),
						"expected tc.errorSubstring=%v to contain %v, but it didn't", tc.errorSubstring, err.Error())

					return
				}
				assert.Nil(mt, err, "CreateDataKey error: %v", err)

				encOpts := options.Encrypt().SetKeyID(createdKey).SetAlgorithm(deterministicAlgorithm)
				testVal := bson.RawValue{
					Type:  bson.TypeString,
					Value: bsoncore.AppendString(nil, "test"),
				}
				encrypted, err := cpt.clientEnc.Encrypt(context.Background(), testVal, encOpts)
				assert.Nil(mt, err, "Encrypt error: %v", err)
				decrypted, err := cpt.clientEnc.Decrypt(context.Background(), encrypted)
				assert.Nil(mt, err, "Decrypt error: %v", err)
				assert.Equal(mt, testVal, decrypted, "expected value %s, got %s", testVal, decrypted)

				if !tc.testInvalidClientEncryption {
					return
				}

				invalidClientEncryption, err := mongo.NewClientEncryption(cpt.kvClient, invalidClientEncryptionOptions)
				assert.Nil(mt, err, "error creating invalidClientEncryption object: %v", err)
				defer invalidClientEncryption.Close(context.Background())

				invalidKeyOpts := options.DataKey().SetMasterKey(tc.masterKey)
				_, err = invalidClientEncryption.CreateDataKey(context.Background(), tc.provider, invalidKeyOpts)
				assert.NotNil(mt, err, "expected CreateDataKey error, got nil")

				assert.True(t, containsPattern(tc.invalidClientEncryptionErrorSubstring, err.Error()),
					"expected tc.invalidClientEncryptionErrorSubstring=%v to contain %v, but it didn't",
					tc.invalidClientEncryptionErrorSubstring, err.Error())
			})
		}
	})
	mt.RunOpts("8. bypass mongocryptd spawning", noClientOpts, func(mt *mtest.T) {
		mt.Parallel()

		kmsProviders := map[string]map[string]interface{}{
			"local": {
				"key": localMasterKey,
			},
		}
		schemaMap := map[string]interface{}{
			"db.coll": readJSONFile(mt, "external-schema.json"),
		}

		// All mongocryptd options use port 27021 instead of the default 27020 to avoid interference
		// with mongocryptd instances spawned by previous tests. Explicitly disable loading the
		// crypt_shared library to make sure we're testing mongocryptd spawning behavior that is not
		// influenced by loading the crypt_shared library.
		mongocryptdBypassSpawnTrue := map[string]interface{}{
			"mongocryptdBypassSpawn":              true,
			"mongocryptdURI":                      "mongodb://localhost:27021/db?serverSelectionTimeoutMS=1000",
			"mongocryptdSpawnArgs":                []string{"--pidfilepath=bypass-spawning-mongocryptd.pid", "--port=27021"},
			"__cryptSharedLibDisabledForTestOnly": true, // Disable loading the crypt_shared library.
		}
		mongocryptdBypassSpawnFalse := map[string]interface{}{
			"mongocryptdBypassSpawn":              false,
			"mongocryptdSpawnArgs":                []string{"--pidfilepath=bypass-spawning-mongocryptd.pid", "--port=27021"},
			"__cryptSharedLibDisabledForTestOnly": true, // Disable loading the crypt_shared library.
		}
		mongocryptdBypassSpawnNotSet := map[string]interface{}{
			"mongocryptdSpawnArgs":                []string{"--pidfilepath=bypass-spawning-mongocryptd.pid", "--port=27021"},
			"__cryptSharedLibDisabledForTestOnly": true, // Disable loading the crypt_shared library.
		}

		testCases := []struct {
			name                    string
			mongocryptdOpts         map[string]interface{}
			setBypassAutoEncryption bool
			bypassAutoEncryption    bool
			bypassQueryAnalysis     bool
			useSharedLib            bool
		}{
			{
				name:            "mongocryptdBypassSpawn only",
				mongocryptdOpts: mongocryptdBypassSpawnTrue,
			},
			{
				name:                    "bypassAutoEncryption only",
				mongocryptdOpts:         mongocryptdBypassSpawnNotSet,
				setBypassAutoEncryption: true,
				bypassAutoEncryption:    true,
			},
			{
				name:                    "mongocryptdBypassSpawn false, bypassAutoEncryption true",
				mongocryptdOpts:         mongocryptdBypassSpawnFalse,
				setBypassAutoEncryption: true,
				bypassAutoEncryption:    true,
			},
			{
				name:                    "mongocryptdBypassSpawn true, bypassAutoEncryption false",
				mongocryptdOpts:         mongocryptdBypassSpawnTrue,
				setBypassAutoEncryption: true,
				bypassAutoEncryption:    false,
			},
			{
				name:                "bypassQueryAnalysis only",
				mongocryptdOpts:     mongocryptdBypassSpawnNotSet,
				bypassQueryAnalysis: true,
			},
			{
				name:         "use shared library",
				useSharedLib: true,
				mongocryptdOpts: map[string]interface{}{
					"mongocryptdURI":       "mongodb://localhost:27021/db?serverSelectionTimeoutMS=1000",
					"mongocryptdSpawnArgs": []string{"--pidfilepath=bypass-spawning-mongocryptd.pid", "--port=27021"},
					"cryptSharedLibPath":   os.Getenv("CRYPT_SHARED_LIB_PATH"),
					"cryptSharedRequired":  true,
				},
			},
		}
		for _, tc := range testCases {
			mt.Run(tc.name, func(mt *mtest.T) {
				if tc.useSharedLib && os.Getenv("CRYPT_SHARED_LIB_PATH") == "" {
					mt.Skip("CRYPT_SHARED_LIB_PATH not set, skipping")
					return
				}
				aeo := options.AutoEncryption().
					SetKmsProviders(kmsProviders).
					SetKeyVaultNamespace(kvNamespace).
					SetSchemaMap(schemaMap).
					SetExtraOptions(tc.mongocryptdOpts)
				if tc.setBypassAutoEncryption {
					aeo.SetBypassAutoEncryption(tc.bypassAutoEncryption)
				}
				aeo.SetBypassQueryAnalysis(tc.bypassQueryAnalysis)
				cpt := setup(mt, aeo, nil, nil)
				defer cpt.teardown(mt)

				_, err := cpt.cseColl.InsertOne(context.Background(), bson.D{{"unencrypted", "test"}})

				// Check for mongocryptd server selection error if auto encryption needed mongocryptd.
				if !(tc.setBypassAutoEncryption && tc.bypassAutoEncryption) && !tc.bypassQueryAnalysis && !tc.useSharedLib {
					assert.NotNil(mt, err, "expected InsertOne error, got nil")
					mcryptErr, ok := err.(mongo.MongocryptdError)
					assert.True(mt, ok, "expected error type %T, got %v of type %T", mongo.MongocryptdError{}, err, err)
					assert.True(mt, strings.Contains(mcryptErr.Error(), "server selection error"),
						"expected mongocryptd server selection error, got %v", err)
					return
				}

				// If mongocryptd was not needed, the command should succeed. Create a new client to connect to
				// mongocryptd and verify it is not running.
				assert.Nil(mt, err, "InsertOne error: %v", err)

				mcryptOpts := options.Client().ApplyURI("mongodb://localhost:27021").
					SetServerSelectionTimeout(1 * time.Second)
				integtest.AddTestServerAPIVersion(mcryptOpts)
				mcryptClient, err := mongo.Connect(mcryptOpts)
				assert.Nil(mt, err, "mongocryptd Connect error: %v", err)

				err = mcryptClient.Database("admin").RunCommand(context.Background(), bson.D{{handshake.LegacyHelloLowercase, 1}}).Err()
				assert.NotNil(mt, err, "expected mongocryptd legacy hello error, got nil")
				assert.True(mt, strings.Contains(err.Error(), "server selection error"),
					"expected mongocryptd server selection error, got %v", err)
			})
		}
	})
	changeStreamOpts := mtest.NewOptions().
		CreateClient(false).
		Topologies(mtest.ReplicaSet)
	mt.RunOpts("change streams", changeStreamOpts, func(mt *mtest.T) {
		// Change streams can't easily fit into the spec test format because of their tailable nature, so there are two
		// prose tests for them instead:
		//
		// 1. Auto-encryption errors for Watch operations. Collection-level change streams error because the
		// $changeStream aggregation stage is not valid for encryption. Client and database-level streams error because
		// only collection-level operations are valid for encryption.
		//
		// 2. Events are automatically decrypted: If the Watch() is done with BypassAutoEncryption=true, the Watch
		// should succeed and subsequent getMore calls should decrypt documents when necessary.

		var testConfig struct {
			JSONSchema        bson.Raw   `bson:"json_schema"`
			KeyVaultData      []bson.Raw `bson:"key_vault_data"`
			EncryptedDocument bson.Raw   `bson:"encrypted_document"`
			DecryptedDocument bson.Raw   `bson:"decrytped_document"`
		}
		decodeJSONFile(mt, "change-streams-test.json", &testConfig)

		schemaMap := map[string]interface{}{
			"db.coll": testConfig.JSONSchema,
		}
		kmsProviders := map[string]map[string]interface{}{
			"aws": {
				"accessKeyId":     awsAccessKeyID,
				"secretAccessKey": awsSecretAccessKey,
			},
		}

		testCases := []struct {
			name       string
			streamType mongo.StreamType
		}{
			{"client", mongo.ClientStream},
			{"database", mongo.DatabaseStream},
			{"collection", mongo.CollectionStream},
		}
		mt.RunOpts("auto encryption errors", noClientOpts, func(mt *mtest.T) {
			mt.Parallel()

			for _, tc := range testCases {
				mt.Run(tc.name, func(mt *mtest.T) {
					autoEncryptionOpts := options.AutoEncryption().
						SetKmsProviders(kmsProviders).
						SetKeyVaultNamespace(kvNamespace).
						SetSchemaMap(schemaMap).
						SetExtraOptions(getCryptSharedLibExtraOptions())
					cpt := setup(mt, autoEncryptionOpts, nil, nil)
					defer cpt.teardown(mt)

					_, err := getWatcher(mt, tc.streamType, cpt).Watch(context.Background(), mongo.Pipeline{})
					assert.NotNil(mt, err, "expected Watch error: %v", err)
				})
			}
		})
		mt.RunOpts("events are automatically decrypted", noClientOpts, func(mt *mtest.T) {
			for _, tc := range testCases {
				mt.Run(tc.name, func(mt *mtest.T) {
					autoEncryptionOpts := options.AutoEncryption().
						SetKmsProviders(kmsProviders).
						SetKeyVaultNamespace(kvNamespace).
						SetSchemaMap(schemaMap).
						SetBypassAutoEncryption(true)
					cpt := setup(mt, autoEncryptionOpts, nil, nil)
					defer cpt.teardown(mt)

					// Insert key vault data so the key can be accessed when starting the change stream.
					insertDocuments(mt, cpt.keyVaultColl, testConfig.KeyVaultData)

					stream, err := getWatcher(mt, tc.streamType, cpt).Watch(context.Background(), mongo.Pipeline{})
					assert.Nil(mt, err, "Watch error: %v", err)
					defer stream.Close(context.Background())

					// Insert already encrypted data and verify that it is automatically decrypted by Next().
					insertDocuments(mt, cpt.coll, []bson.Raw{testConfig.EncryptedDocument})
					assert.True(mt, stream.Next(context.Background()), "expected Next to return true, got false")
					gotValue, err := stream.Current.LookupErr("fullDocument")
					assert.Nil(mt, err, "did not find fullDocument in stream.Current: %v", stream.Current)
					gotDocument := gotValue.Document()
					err = compareDocs(mt, testConfig.DecryptedDocument, gotDocument)
					assert.Nil(mt, err, "compareDocs error: %v", err)
				})
			}
		})
	})
	mt.RunOpts("9. deadlock tests", noClientOpts, func(mt *mtest.T) {
		testcases := []struct {
			description                            string
			maxPoolSize                            uint64
			bypassAutoEncryption                   bool
			keyVaultClientSet                      bool
			clientEncryptedTopologyOpeningExpected int
			clientEncryptedCommandStartedExpected  []startedEvent
			clientKeyVaultCommandStartedExpected   []startedEvent
		}{
			// In the following comments, "special auto encryption options" refers to the "bypassAutoEncryption" and
			// "keyVaultClient" options
			{
				// If the client has a limited maxPoolSize, and no special auto-encryption options are set, the
				// driver should create an internal Client for metadata/keyVault operations.
				"deadlock case 1", 1, false, false, 2,
				[]startedEvent{{"listCollections", "db"}, {"find", "keyvault"}, {"insert", "db"}, {"find", "db"}},
				nil,
			},
			{
				// If the client has a limited maxPoolSize, and a keyVaultClient is set, the driver should create
				// an internal Client for metadata operations.
				"deadlock case 2", 1, false, true, 2,
				[]startedEvent{{"listCollections", "db"}, {"insert", "db"}, {"find", "db"}},
				[]startedEvent{{"find", "keyvault"}},
			},
			{
				// If the client has a limited maxPoolSize, and a bypassAutomaticEncryption=true, the driver should
				// create an internal Client for keyVault operations.
				"deadlock case 3", 1, true, false, 2,
				[]startedEvent{{"find", "db"}, {"find", "keyvault"}},
				nil,
			},
			{
				// If the client has a limited maxPoolSize, bypassAutomaticEncryption=true, and a keyVaultClient is set,
				// the driver should not create an internal Client.
				"deadlock case 4", 1, true, true, 1,
				[]startedEvent{{"find", "db"}},
				[]startedEvent{{"find", "keyvault"}},
			},
			{
				// If the client has an unlimited maxPoolSize, and no special auto-encryption options are set,  the
				// driver should reuse the client for metadata/keyVault operations
				"deadlock case 5", 0, false, false, 1,
				[]startedEvent{{"listCollections", "db"}, {"listCollections", "keyvault"}, {"find", "keyvault"}, {"insert", "db"}, {"find", "db"}},
				nil,
			},
			{
				// If the client has an unlimited maxPoolSize, and a keyVaultClient is set, the driver should reuse the
				// client for metadata operations.
				"deadlock case 6", 0, false, true, 1,
				[]startedEvent{{"listCollections", "db"}, {"insert", "db"}, {"find", "db"}},
				[]startedEvent{{"find", "keyvault"}},
			},
			{
				// If the client has an unlimited maxPoolSize, and bypassAutomaticEncryption=true, the driver should
				// reuse the client for keyVault operations
				"deadlock case 7", 0, true, false, 1,
				[]startedEvent{{"find", "db"}, {"find", "keyvault"}},
				nil,
			},
			{
				// If the client has an unlimited maxPoolSize, bypassAutomaticEncryption=true, and a keyVaultClient is
				// set, the driver should not create an internal Client.
				"deadlock case 8", 0, true, true, 1,
				[]startedEvent{{"find", "db"}},
				[]startedEvent{{"find", "keyvault"}},
			},
		}

		for _, tc := range testcases {
			mt.Run(tc.description, func(mt *mtest.T) {
				var clientEncryptedEvents []startedEvent
				var clientEncryptedTopologyOpening int

				d := newDeadlockTest(mt)
				defer d.disconnect(mt)

				kmsProviders := map[string]map[string]interface{}{
					"local": {"key": localMasterKey},
				}
				aeOpts := options.AutoEncryption()
				aeOpts.SetKeyVaultNamespace("keyvault.datakeys").
					SetKmsProviders(kmsProviders).
					SetBypassAutoEncryption(tc.bypassAutoEncryption)

				// Only set the crypt_shared library extra options if bypassAutoEncryption isn't
				// true because it's invalid to set cryptSharedLibRequired=true and
				// bypassAutoEncryption=true together.
				if !tc.bypassAutoEncryption {
					aeOpts.SetExtraOptions(getCryptSharedLibExtraOptions())
				}

				if tc.keyVaultClientSet {
					integtest.AddTestServerAPIVersion(d.clientKeyVaultOpts)
					aeOpts.SetKeyVaultClientOptions(d.clientKeyVaultOpts)
				}

				ceOpts := options.Client().ApplyURI(mtest.ClusterURI()).
					SetMonitor(&event.CommandMonitor{
						Started: func(ctx context.Context, event *event.CommandStartedEvent) {
							clientEncryptedEvents = append(clientEncryptedEvents, startedEvent{event.CommandName, event.DatabaseName})
						},
					}).
					SetServerMonitor(&event.ServerMonitor{
						TopologyOpening: func(event *event.TopologyOpeningEvent) {
							clientEncryptedTopologyOpening++
						},
					}).
					SetMaxPoolSize(tc.maxPoolSize).
					SetAutoEncryptionOptions(aeOpts)

				integtest.AddTestServerAPIVersion(ceOpts)
				clientEncrypted, err := mongo.Connect(ceOpts)
				assert.Nil(mt, err, "Connect error: %v", err)
				defer clientEncrypted.Disconnect(context.Background())

				coll := clientEncrypted.Database("db").Collection("coll")
				if !tc.bypassAutoEncryption {
					_, err = coll.InsertOne(context.Background(), bson.M{"_id": 0, "encrypted": "string0"})
				} else {
					unencryptedColl := d.clientTest.Database("db").Collection("coll")
					_, err = unencryptedColl.InsertOne(context.Background(), bson.M{"_id": 0, "encrypted": d.ciphertext})
				}
				assert.Nil(mt, err, "InsertOne error: %v", err)

				raw, err := coll.FindOne(context.Background(), bson.M{"_id": 0}).Raw()
				assert.Nil(mt, err, "FindOne error: %v", err)

				expected := bsoncore.NewDocumentBuilder().
					AppendInt32("_id", 0).
					AppendString("encrypted", "string0").
					Build()
				assert.Equal(mt, bson.Raw(expected), raw, "returned value unequal, expected: %v, got: %v", expected, raw)

				assert.Equal(mt, clientEncryptedEvents, tc.clientEncryptedCommandStartedExpected, "mismatched events for clientEncrypted. Expected %v, got %v", clientEncryptedEvents, tc.clientEncryptedCommandStartedExpected)
				assert.Equal(mt, d.clientKeyVaultEvents, tc.clientKeyVaultCommandStartedExpected, "mismatched events for clientKeyVault. Expected %v, got %v", d.clientKeyVaultEvents, tc.clientKeyVaultCommandStartedExpected)
				assert.Equal(mt, clientEncryptedTopologyOpening, tc.clientEncryptedTopologyOpeningExpected, "wrong number of TopologyOpening events. Expected %v, got %v", tc.clientEncryptedTopologyOpeningExpected, clientEncryptedTopologyOpening)
			})
		}
	})

	// These tests only run when 3 KMS HTTP servers and 1 KMS KMIP server are
	// running. See specification for port numbers and necessary arguments:
	// https://github.com/mongodb/specifications/blob/master/source/client-side-encryption/tests/README.md#10-kms-tls-tests
	mt.RunOpts("10. kms tls tests", noClientOpts, func(mt *mtest.T) {
		kmsTlsTestcase := os.Getenv("KMS_TLS_TESTCASE")
		if kmsTlsTestcase == "" {
			mt.Skipf("Skipping test as KMS_TLS_TESTCASE is not set")
		}

		testcases := []struct {
			name       string
			port       int
			envValue   string
			errMessage string
		}{
			{
				"invalid certificate",
				9000,
				"INVALID_CERT",
				"expired",
			},
			{
				"invalid hostname",
				9001,
				"INVALID_HOSTNAME",
				"SANs",
			},
		}

		for _, tc := range testcases {
			mt.Run(tc.name, func(mt *mtest.T) {
				// Only run test if correct KMS mock server is running.
				if kmsTlsTestcase != tc.envValue {
					mt.Skipf("Skipping test as KMS_TLS_TESTCASE is set to %q, expected %v", kmsTlsTestcase, tc.envValue)
				}

				ceo := options.ClientEncryption().
					SetKmsProviders(fullKmsProvidersMap).
					SetKeyVaultNamespace(kvNamespace)
				cpt := setup(mt, nil, nil, ceo)
				defer cpt.teardown(mt)

				_, err := cpt.clientEnc.CreateDataKey(context.Background(), "aws", options.DataKey().SetMasterKey(
					bson.D{
						{"region", "us-east-1"},
						{"key", "arn:aws:kms:us-east-1:579766882180:key/89fcc2c4-08b0-4bd9-9f25-e30687b580d0"},
						{"endpoint", fmt.Sprintf("127.0.0.1:%d", tc.port)},
					},
				))
				assert.NotNil(mt, err, "expected CreateDataKey error, got nil")
				assert.True(mt, strings.Contains(err.Error(), tc.errMessage),
					"expected CreateDataKey error to contain %v, got %v", tc.errMessage, err.Error())
			})
		}
	})

	// These tests only run when 3 KMS HTTP servers and 1 KMS KMIP server are
	// running. See specification for port numbers and necessary arguments:
	// https://github.com/mongodb/specifications/blob/master/source/client-side-encryption/tests/README.md#11-kms-tls-options-tests
	mt.RunOpts("11. kms tls options tests", noClientOpts, func(mt *mtest.T) {
		if os.Getenv("KMS_MOCK_SERVERS_RUNNING") == "" {
			mt.Skipf("Skipping test as KMS_MOCK_SERVERS_RUNNING is not set")
		}
		if tlsCAFileKMIP == "" || tlsClientCertificateKeyFileKMIP == "" {
			mt.Fatal("Env vars CSFLE_TLS_CA_FILE and CSFLE_TLS_CLIENT_CERT_FILE must be set")
		}

		validKmsProviders := map[string]map[string]interface{}{
			"aws": {
				"accessKeyId":     awsAccessKeyID,
				"secretAccessKey": awsSecretAccessKey,
			},
			"azure": {
				"tenantId":                 azureTenantID,
				"clientId":                 azureClientID,
				"clientSecret":             azureClientSecret,
				"identityPlatformEndpoint": "127.0.0.1:9002",
			},
			"gcp": {
				"email":      gcpEmail,
				"privateKey": gcpPrivateKey,
				"endpoint":   "127.0.0.1:9002",
			},
			"kmip": {
				"endpoint": "127.0.0.1:5698",
			},
		}

		expiredKmsProviders := map[string]map[string]interface{}{
			"aws": {
				"accessKeyId":     awsAccessKeyID,
				"secretAccessKey": awsSecretAccessKey,
			},
			"azure": {
				"tenantId":                 azureTenantID,
				"clientId":                 azureClientID,
				"clientSecret":             azureClientSecret,
				"identityPlatformEndpoint": "127.0.0.1:9000",
			},
			"gcp": {
				"email":      gcpEmail,
				"privateKey": gcpPrivateKey,
				"endpoint":   "127.0.0.1:9000",
			},
			"kmip": {
				"endpoint": "127.0.0.1:9000",
			},
		}

		invalidKmsProviders := map[string]map[string]interface{}{
			"aws": {
				"accessKeyId":     awsAccessKeyID,
				"secretAccessKey": awsSecretAccessKey,
			},
			"azure": {
				"tenantId":                 azureTenantID,
				"clientId":                 azureClientID,
				"clientSecret":             azureClientSecret,
				"identityPlatformEndpoint": "127.0.0.1:9001",
			},
			"gcp": {
				"email":      gcpEmail,
				"privateKey": gcpPrivateKey,
				"endpoint":   "127.0.0.1:9001",
			},
			"kmip": {
				"endpoint": "127.0.0.1:9001",
			},
		}

		// create valid Client Encryption options without a client certificate
		validClientEncryptionOptionsWithoutClientCert := options.ClientEncryption().
			SetKmsProviders(validKmsProviders).
			SetKeyVaultNamespace(kvNamespace)

		// make TLS opts containing client certificate and CA file
		clientAndCATLSConfig, err := options.BuildTLSConfig(map[string]interface{}{
			"tlsCertificateKeyFile": tlsClientCertificateKeyFileKMIP,
			"tlsCAFile":             tlsCAFileKMIP,
		})
		assert.Nil(mt, err, "BuildTLSConfig error: %v", err)

		// create valid Client Encryption options and set valid TLS options
		validClientEncryptionOptionsWithTLS := options.ClientEncryption().
			SetKmsProviders(validKmsProviders).
			SetKeyVaultNamespace(kvNamespace).
			SetTLSConfig(map[string]*tls.Config{
				"aws":   clientAndCATLSConfig,
				"azure": clientAndCATLSConfig,
				"gcp":   clientAndCATLSConfig,
				"kmip":  clientAndCATLSConfig,
			})

		// make TLS opts containing only CA file
		caTLSConfig, err := options.BuildTLSConfig(map[string]interface{}{
			"tlsCAFile": tlsCAFileKMIP,
		})
		assert.Nil(mt, err, "BuildTLSConfig error: %v", err)

		// create invalid Client Encryption options with expired credentials
		expiredClientEncryptionOptions := options.ClientEncryption().
			SetKmsProviders(expiredKmsProviders).
			SetKeyVaultNamespace(kvNamespace).
			SetTLSConfig(map[string]*tls.Config{
				"aws":   caTLSConfig,
				"azure": caTLSConfig,
				"gcp":   caTLSConfig,
				"kmip":  caTLSConfig,
			})

		// create invalid Client Encryption options with invalid hostnames
		invalidHostnameClientEncryptionOptions := options.ClientEncryption().
			SetKmsProviders(invalidKmsProviders).
			SetKeyVaultNamespace(kvNamespace).
			SetTLSConfig(map[string]*tls.Config{
				"aws":   caTLSConfig,
				"azure": caTLSConfig,
				"gcp":   caTLSConfig,
				"kmip":  caTLSConfig,
			})

		awsMasterKeyNoClientCert := map[string]interface{}{
			"region":   "us-east-1",
			"key":      "arn:aws:kms:us-east-1:579766882180:key/89fcc2c4-08b0-4bd9-9f25-e30687b580d0",
			"endpoint": "127.0.0.1:9002",
		}
		awsMasterKeyWithTLS := map[string]interface{}{
			"region":   "us-east-1",
			"key":      "arn:aws:kms:us-east-1:579766882180:key/89fcc2c4-08b0-4bd9-9f25-e30687b580d0",
			"endpoint": "127.0.0.1:9002",
		}
		awsMasterKeyExpired := map[string]interface{}{
			"region":   "us-east-1",
			"key":      "arn:aws:kms:us-east-1:579766882180:key/89fcc2c4-08b0-4bd9-9f25-e30687b580d0",
			"endpoint": "127.0.0.1:9000",
		}
		awsMasterKeyInvalidHostname := map[string]interface{}{
			"region":   "us-east-1",
			"key":      "arn:aws:kms:us-east-1:579766882180:key/89fcc2c4-08b0-4bd9-9f25-e30687b580d0",
			"endpoint": "127.0.0.1:9001",
		}
		azureMasterKey := map[string]interface{}{
			"keyVaultEndpoint": "doesnotexist.local",
			"keyName":          "foo",
		}
		gcpMasterKey := map[string]interface{}{
			"projectId": "foo",
			"location":  "bar",
			"keyRing":   "baz",
			"keyName":   "foo",
		}
		kmipMasterKey := map[string]interface{}{}

		testCases := []struct {
			name                     string
			masterKeyNoClientCert    interface{}
			masterKeyWithTLS         interface{}
			masterKeyExpired         interface{}
			masterKeyInvalidHostname interface{}
			tlsError                 string
			expiredError             string
			invalidHostnameError     string
		}{
			{"aws", awsMasterKeyNoClientCert, awsMasterKeyWithTLS, awsMasterKeyExpired, awsMasterKeyInvalidHostname, "parse error", "certificate has expired", "cannot validate certificate"},
			{"azure", azureMasterKey, azureMasterKey, azureMasterKey, azureMasterKey, "HTTP status=404", "certificate has expired", "cannot validate certificate"},
			{"gcp", gcpMasterKey, gcpMasterKey, gcpMasterKey, gcpMasterKey, "HTTP status=404", "certificate has expired", "cannot validate certificate"},
			{"kmip", kmipMasterKey, kmipMasterKey, kmipMasterKey, kmipMasterKey, "", "certificate has expired", "cannot validate certificate"},
		}

		for _, tc := range testCases {
			mt.Run(tc.name, func(mt *mtest.T) {
				if tc.name == "kmip" && "" == os.Getenv("KMS_MOCK_SERVERS_RUNNING") {
					mt.Skipf("Skipping test as KMS_MOCK_SERVERS_RUNNING is not set")
				}
				// call CreateDataKey with CEO no TLS with each provider and corresponding master key
				cpt := setup(mt, nil, defaultKvClientOptions, validClientEncryptionOptionsWithoutClientCert)
				defer cpt.teardown(mt)

				dkOpts := options.DataKey().SetMasterKey(tc.masterKeyNoClientCert)
				_, err := cpt.clientEnc.CreateDataKey(context.Background(), tc.name, dkOpts)

				assert.NotNil(mt, err, "expected error, got nil")

				possibleErrors := []string{
					"x509: certificate signed by unknown authority",                   // Windows
					"x509: “valid.testing.golang.invalid” certificate is not trusted", // macOS
					"x509: “server” certificate is not standards compliant",           // macOS
					"x509: certificate is not authorized to sign other certificates",  // All others
				}

				assert.True(t, containsPattern(possibleErrors, err.Error()),
					"expected possibleErrors=%v to contain %v, but it didn't",
					possibleErrors, err.Error())

				// call CreateDataKey with CEO & TLS with each provider and corresponding master key
				cpt = setup(mt, nil, defaultKvClientOptions, validClientEncryptionOptionsWithTLS)

				dkOpts = options.DataKey().SetMasterKey(tc.masterKeyWithTLS)
				_, err = cpt.clientEnc.CreateDataKey(context.Background(), tc.name, dkOpts)
				// check if current test case is KMIP, which should pass
				if tc.name == "kmip" {
					assert.Nil(mt, err, "expected no error, got err: %v", err)
				} else {
					assert.NotNil(mt, err, "expected error, got nil")
					assert.True(mt, strings.Contains(err.Error(), tc.tlsError),
						"expected error '%s' to contain '%s'", err.Error(), tc.tlsError)
				}

				// call CreateDataKey with expired CEO each provider and same masterKey
				cpt = setup(mt, nil, defaultKvClientOptions, expiredClientEncryptionOptions)

				dkOpts = options.DataKey().SetMasterKey(tc.masterKeyExpired)
				_, err = cpt.clientEnc.CreateDataKey(context.Background(), tc.name, dkOpts)
				assert.NotNil(mt, err, "expected error, got nil")
				assert.True(mt, strings.Contains(err.Error(), tc.expiredError),
					"expected error '%s' to contain '%s'", err.Error(), tc.expiredError)

				// call CreateDataKey with invalid hostname CEO with each provider and same masterKey
				cpt = setup(mt, nil, defaultKvClientOptions, invalidHostnameClientEncryptionOptions)

				dkOpts = options.DataKey().SetMasterKey(tc.masterKeyInvalidHostname)
				_, err = cpt.clientEnc.CreateDataKey(context.Background(), tc.name, dkOpts)
				assert.NotNil(mt, err, "expected error, got nil")
				assert.True(mt, strings.Contains(err.Error(), tc.invalidHostnameError),
					"expected error '%s' to contain '%s'", err.Error(), tc.invalidHostnameError)
			})
		}
	})
	runOpts := mtest.NewOptions().MinServerVersion("7.0").Topologies(mtest.ReplicaSet, mtest.LoadBalanced, mtest.ShardedReplicaSet)
	// Only test MongoDB Server 7.0+. MongoDB Server 7.0 introduced a backwards breaking change to the Queryable Encryption (QE) protocol: QEv2.
	// libmongocrypt is configured to use the QEv2 protocol.
	mt.RunOpts("12. explicit encryption", runOpts, func(mt *mtest.T) {
		// Test Setup ... begin
		encryptedFields := readJSONFile(mt, "encrypted-fields.json")
		key1Document := readJSONFile(mt, "key1-document.json")
		var key1ID bson.Binary
		{
			subtype, data := key1Document.Lookup("_id").Binary()
			key1ID = bson.Binary{Subtype: subtype, Data: data}
		}

		testSetup := func() (*mongo.Client, *mongo.ClientEncryption) {
			mtest.DropEncryptedCollection(mt, mt.Client.Database("db").Collection("explicit_encryption"), encryptedFields)
			cco := options.CreateCollection().SetEncryptedFields(encryptedFields)
			err := mt.Client.Database("db").CreateCollection(context.Background(), "explicit_encryption", cco)
			assert.Nil(mt, err, "error on CreateCollection: %v", err)
			err = mt.Client.Database("keyvault").Collection("datakeys").Drop(context.Background())
			assert.Nil(mt, err, "error on Drop: %v", err)
			keyVaultClient, err := mongo.Connect(options.Client().ApplyURI(mtest.ClusterURI()))
			assert.Nil(mt, err, "error on Connect: %v", err)
			datakeysColl := keyVaultClient.Database("keyvault").Collection("datakeys", options.Collection().SetWriteConcern(mtest.MajorityWc))
			_, err = datakeysColl.InsertOne(context.Background(), key1Document)
			assert.Nil(mt, err, "error on InsertOne: %v", err)
			// Create a ClientEncryption.
			ceo := options.ClientEncryption().
				SetKeyVaultNamespace("keyvault.datakeys").
				SetKmsProviders(fullKmsProvidersMap)
			clientEncryption, err := mongo.NewClientEncryption(keyVaultClient, ceo)
			assert.Nil(mt, err, "error on NewClientEncryption: %v", err)

			// Create a MongoClient with AutoEncryptionOpts and bypassQueryAnalysis=true.
			aeo := options.AutoEncryption().
				SetKeyVaultNamespace("keyvault.datakeys").
				SetKmsProviders(fullKmsProvidersMap).
				SetBypassQueryAnalysis(true)
			co := options.Client().SetAutoEncryptionOptions(aeo).ApplyURI(mtest.ClusterURI())
			encryptedClient, err := mongo.Connect(co)
			assert.Nil(mt, err, "error on Connect: %v", err)
			return encryptedClient, clientEncryption
		}
		// Test Setup ... end

		mt.Run("case 1: can insert encrypted indexed and find", func(mt *mtest.T) {
			encryptedClient, clientEncryption := testSetup()
			defer clientEncryption.Close(context.Background())
			defer encryptedClient.Disconnect(context.Background())

			// Explicit encrypt the value "encrypted indexed value" with algorithm: "Indexed".
			eo := options.Encrypt().SetAlgorithm("Indexed").SetKeyID(key1ID).SetContentionFactor(0)
			valueToEncrypt := "encrypted indexed value"
			rawVal := bson.RawValue{Type: bson.TypeString, Value: bsoncore.AppendString(nil, valueToEncrypt)}
			insertPayload, err := clientEncryption.Encrypt(context.Background(), rawVal, eo)
			assert.Nil(mt, err, "error in Encrypt: %v", err)
			// Insert.
			coll := encryptedClient.Database("db").Collection("explicit_encryption")
			_, err = coll.InsertOne(context.Background(), bson.D{{"_id", 1}, {"encryptedIndexed", insertPayload}})
			assert.Nil(mt, err, "Error in InsertOne: %v", err)
			// Explicit encrypt an indexed value to find.
			eo = options.Encrypt().SetAlgorithm("Indexed").SetKeyID(key1ID).SetQueryType(options.QueryTypeEquality).SetContentionFactor(0)
			findPayload, err := clientEncryption.Encrypt(context.Background(), rawVal, eo)
			assert.Nil(mt, err, "error in Encrypt: %v", err)
			// Find.
			res := coll.FindOne(context.Background(), bson.D{{"encryptedIndexed", findPayload}})
			assert.Nil(mt, res.Err(), "Error in FindOne: %v", res.Err())
			got, err := res.Raw()
			assert.Nil(mt, err, "error in Raw: %v", err)
			gotValue, err := got.LookupErr("encryptedIndexed")
			assert.Nil(mt, err, "error in LookupErr: %v", err)
			assert.Equal(mt, gotValue.StringValue(), valueToEncrypt, "expected %q, got %q", valueToEncrypt, gotValue.StringValue())
		})
		mt.Run("case 2: can insert encrypted indexed and find with non-zero contention", func(mt *mtest.T) {
			encryptedClient, clientEncryption := testSetup()
			defer clientEncryption.Close(context.Background())
			defer encryptedClient.Disconnect(context.Background())

			coll := encryptedClient.Database("db").Collection("explicit_encryption")
			valueToEncrypt := "encrypted indexed value"
			rawVal := bson.RawValue{Type: bson.TypeString, Value: bsoncore.AppendString(nil, valueToEncrypt)}

			for i := 0; i < 10; i++ {
				// Explicit encrypt the value "encrypted indexed value" with algorithm: "Indexed".
				eo := options.Encrypt().SetAlgorithm("Indexed").SetKeyID(key1ID).SetContentionFactor(10)
				insertPayload, err := clientEncryption.Encrypt(context.Background(), rawVal, eo)
				assert.Nil(mt, err, "error in Encrypt: %v", err)
				// Insert.
				_, err = coll.InsertOne(context.Background(), bson.D{{"_id", i}, {"encryptedIndexed", insertPayload}})
				assert.Nil(mt, err, "Error in InsertOne: %v", err)
			}

			// Explicit encrypt an indexed value to find with default contentionFactor 0.
			{
				eo := options.Encrypt().SetAlgorithm("Indexed").SetKeyID(key1ID).SetQueryType(options.QueryTypeEquality).SetContentionFactor(0)
				findPayload, err := clientEncryption.Encrypt(context.Background(), rawVal, eo)
				assert.Nil(mt, err, "error in Encrypt: %v", err)
				// Find with contentionFactor=0.
				cursor, err := coll.Find(context.Background(), bson.D{{"encryptedIndexed", findPayload}})
				assert.Nil(mt, err, "error in Find: %v", err)
				var got []bson.Raw
				err = cursor.All(context.Background(), &got)
				assert.Nil(mt, err, "error in All: %v", err)
				assert.True(mt, len(got) < 10, "expected len(got) < 10, got: %v", len(got))
				for _, doc := range got {
					gotValue, err := doc.LookupErr("encryptedIndexed")
					assert.Nil(mt, err, "error in LookupErr: %v", err)
					assert.Equal(mt, gotValue.StringValue(), valueToEncrypt, "expected %q, got %q", valueToEncrypt, gotValue.StringValue())
				}
			}

			// Explicit encrypt an indexed value to find with contentionFactor 10.
			{
				eo := options.Encrypt().SetAlgorithm("Indexed").SetKeyID(key1ID).SetQueryType(options.QueryTypeEquality).SetContentionFactor(10)
				findPayload2, err := clientEncryption.Encrypt(context.Background(), rawVal, eo)
				assert.Nil(mt, err, "error in Encrypt: %v", err)
				// Find with contentionFactor=10.
				cursor, err := coll.Find(context.Background(), bson.D{{"encryptedIndexed", findPayload2}})
				assert.Nil(mt, err, "error in Find: %v", err)
				var got []bson.Raw
				err = cursor.All(context.Background(), &got)
				assert.Nil(mt, err, "error in All: %v", err)
				assert.True(mt, len(got) == 10, "expected len(got) == 10, got: %v", len(got))
				for _, doc := range got {
					gotValue, err := doc.LookupErr("encryptedIndexed")
					assert.Nil(mt, err, "error in LookupErr: %v", err)
					assert.Equal(mt, gotValue.StringValue(), valueToEncrypt, "expected %q, got %q", valueToEncrypt, gotValue.StringValue())
				}
			}
		})
		mt.Run("case 3: can insert encrypted unindexed", func(mt *mtest.T) {
			encryptedClient, clientEncryption := testSetup()
			defer clientEncryption.Close(context.Background())
			defer encryptedClient.Disconnect(context.Background())

			// Explicit encrypt the value "encrypted indexed value" with algorithm: "Indexed".
			eo := options.Encrypt().SetAlgorithm("Unindexed").SetKeyID(key1ID)
			valueToEncrypt := "encrypted unindexed value"
			rawVal := bson.RawValue{Type: bson.TypeString, Value: bsoncore.AppendString(nil, valueToEncrypt)}
			insertPayload, err := clientEncryption.Encrypt(context.Background(), rawVal, eo)
			assert.Nil(mt, err, "error in Encrypt: %v", err)
			// Insert.
			coll := encryptedClient.Database("db").Collection("explicit_encryption")
			_, err = coll.InsertOne(context.Background(), bson.D{{"_id", 1}, {"encryptedUnindexed", insertPayload}})
			assert.Nil(mt, err, "Error in InsertOne: %v", err)
			// Find.
			res := coll.FindOne(context.Background(), bson.D{{"_id", 1}})
			assert.Nil(mt, res.Err(), "Error in FindOne: %v", res.Err())
			got, err := res.Raw()
			assert.Nil(mt, err, "error in Raw: %v", err)
			gotValue, err := got.LookupErr("encryptedUnindexed")
			assert.Nil(mt, err, "error in LookupErr: %v", err)
			assert.Equal(mt, gotValue.StringValue(), valueToEncrypt, "expected %q, got %q", valueToEncrypt, gotValue.StringValue())
		})
		mt.Run("case 4: can roundtrip encrypted indexed", func(mt *mtest.T) {
			encryptedClient, clientEncryption := testSetup()
			defer clientEncryption.Close(context.Background())
			defer encryptedClient.Disconnect(context.Background())

			// Explicit encrypt the value "encrypted indexed value" with algorithm: "Indexed".
			eo := options.Encrypt().SetAlgorithm("Indexed").SetKeyID(key1ID).SetContentionFactor(0)
			valueToEncrypt := "encrypted indexed value"
			rawVal := bson.RawValue{Type: bson.TypeString, Value: bsoncore.AppendString(nil, valueToEncrypt)}
			payload, err := clientEncryption.Encrypt(context.Background(), rawVal, eo)
			assert.Nil(mt, err, "error in Encrypt: %v", err)
			gotValue, err := clientEncryption.Decrypt(context.Background(), payload)
			assert.Nil(mt, err, "error in Decrypt: %v", err)
			assert.Equal(mt, gotValue.StringValue(), valueToEncrypt, "expected %q, got %q", valueToEncrypt, gotValue.StringValue())
		})
		mt.Run("case 5: can roundtrip encrypted unindexed", func(mt *mtest.T) {
			encryptedClient, clientEncryption := testSetup()
			defer clientEncryption.Close(context.Background())
			defer encryptedClient.Disconnect(context.Background())

			// Explicit encrypt the value "encrypted indexed value" with algorithm: "Indexed".
			eo := options.Encrypt().SetAlgorithm("Unindexed").SetKeyID(key1ID)
			valueToEncrypt := "encrypted unindexed value"
			rawVal := bson.RawValue{Type: bson.TypeString, Value: bsoncore.AppendString(nil, valueToEncrypt)}
			payload, err := clientEncryption.Encrypt(context.Background(), rawVal, eo)
			assert.Nil(mt, err, "error in Encrypt: %v", err)
			gotValue, err := clientEncryption.Decrypt(context.Background(), payload)
			assert.Nil(mt, err, "error in Decrypt: %v", err)
			assert.Equal(mt, gotValue.StringValue(), valueToEncrypt, "expected %q, got %q", valueToEncrypt, gotValue.StringValue())
		})
	})

	runOpts = mtest.NewOptions().MinServerVersion("4.2").
		Topologies(mtest.ReplicaSet, mtest.LoadBalanced, mtest.ShardedReplicaSet)
	mt.RunOpts("13. Unique Index on keyAltNames", runOpts, func(mt *mtest.T) {
		const (
			dkCollection  = "datakeys"
			idKey         = "_id"
			kvDatabase    = "keyvault"
			defKeyAltName = "def"
			abcKeyAltName = "abc"
		)

		var cse *cseProseTest

		var initialize = func() bson.Binary {
			// Create a ClientEncryption object (referred to as client_encryption) with client set as the keyVaultClient.
			// Using client, drop the collection keyvault.datakeys.
			cse = setup(mt, nil, defaultKvClientOptions, options.ClientEncryption().
				SetKmsProviders(fullKmsProvidersMap).
				SetKeyVaultNamespace(kvNamespace))

			err := cse.kvClient.Database(kvDatabase).Collection(dkCollection).Drop(context.Background())
			assert.Nil(mt, err, "error dropping %q namespace: %v", kvNamespace, err)

			// Using client, create a unique index on keyAltNames with a partial index filter for only documents where
			// keyAltNames exists using writeConcern "majority".
			keyVaultIndex := mongo.IndexModel{
				Keys: bson.D{{"keyAltNames", 1}},
				Options: options.Index().
					SetUnique(true).
					SetName("keyAltNames_1").
					SetPartialFilterExpression(bson.D{
						{"keyAltNames", bson.D{
							{"$exists", true},
						}},
					}),
			}

			wcMajority := writeconcern.Majority()
			wcMajorityCollectionOpts := options.Collection().SetWriteConcern(wcMajority)
			wcmColl := cse.kvClient.Database(kvDatabase).Collection(dkCollection, wcMajorityCollectionOpts)
			_, err = wcmColl.Indexes().CreateOne(context.Background(), keyVaultIndex)
			assert.Nil(mt, err, "error creating keyAltNames index: %v", err)

			// Using client_encryption, create a data key with a local KMS provider and the keyAltName "def".
			opts := options.DataKey().SetKeyAltNames([]string{defKeyAltName})
			defKeyID, err := cse.clientEnc.CreateDataKey(context.Background(), "local", opts)
			assert.Nil(mt, err, "error creating %q data key: %v", defKeyAltName, err)
			return defKeyID
		}

		var validateAddKeyAltName = func(mt *mtest.T, cse *cseProseTest, res *mongo.SingleResult, expected ...string) {
			assert.Nil(mt, res.Err(), "error adding key alt name: %v", res.Err())

			resbytes, err := res.Raw()
			assert.Nil(mt, err, "error decoding result bytes: %v", err)

			idsubtype, iddata := bson.RawValue{Type: bson.TypeEmbeddedDocument, Value: resbytes}.
				Document().Lookup("_id").Binary()
			filter := bsoncore.NewDocumentBuilder().AppendBinary("_id", idsubtype, iddata).Build()

			ctx := context.Background()
			updatedData, err := cse.keyVaultColl.FindOne(ctx, filter).Raw()
			assert.Nil(mt, err, "error decoding result bytes: %v", err)

			updated := bson.RawValue{Type: bson.TypeEmbeddedDocument, Value: updatedData}
			updatedKeyAltNames, err := updated.Document().Lookup("keyAltNames").Array().Values()
			assert.Nil(mt, err, "error looking up raw keyAltNames: %v", err)
			assert.Equal(mt, len(updatedKeyAltNames), len(expected), "expected raw keyAltNames length to be 1")

			for idx, keyAltName := range updatedKeyAltNames {
				str := keyAltName.StringValue()
				assert.Equal(mt, str, expected[idx], "expected keyAltName to be %q, got: %q", expected[idx], str)
			}
		}

		mt.Run("case 1: createKey()", func(mt *mtest.T) {
			initialize()

			// Use client_encryption to create a new local data key with a keyAltName "abc" and assert the operation
			// does not fail.
			opts := options.DataKey().SetKeyAltNames([]string{abcKeyAltName})
			_, err := cse.clientEnc.CreateDataKey(context.Background(), "local", opts)
			assert.Nil(mt, err, "error creating %q data key: %v", abcKeyAltName, err)

			// Repeat Step 1 and assert the operation fails due to a duplicate key server error (error code 11000).
			opts = options.DataKey().SetKeyAltNames([]string{abcKeyAltName})
			_, err = cse.clientEnc.CreateDataKey(context.Background(), "local", opts)
			assert.NotNil(mt, err, "duplicate %q key did not propagate expected error", abcKeyAltName)

			e110000 := "E11000 duplicate key"
			correctError := strings.Contains(err.Error(), e110000)
			assert.True(t, correctError, "expected error to contain %q, got: %v", e110000, err)

			// Use client_encryption to create a new local data key with a keyAltName "def" and assert the operation
			// fails due to a duplicate key server error (error code 11000).
			opts = options.DataKey().SetKeyAltNames([]string{defKeyAltName})
			_, err = cse.clientEnc.CreateDataKey(context.Background(), "local", opts)
			assert.NotNil(mt, err, "duplicate %q key did not propagate expected error", defKeyAltName)

			e110000 = "E11000 duplicate key"
			correctError = strings.Contains(err.Error(), e110000)
			assert.True(t, correctError, "expected error to contain %q, got: %v", e110000, err)
		})

		mt.Run("case 2: addKeyAltName()", func(mt *mtest.T) {
			defKeyID := initialize()

			var someNewKeyID bson.Binary
			// Use client_encryption to create a new local data key and assert the operation does not fail.
			var err error
			someNewKeyID, err = cse.clientEnc.CreateDataKey(context.Background(), "local")
			assert.Nil(mt, err, "error creating data key: %v", err)

			// Use client_encryption to add a keyAltName "abc" to the key created in Step 1 and assert the operation
			// does not fail.
			res := cse.clientEnc.AddKeyAltName(context.Background(), someNewKeyID, abcKeyAltName)
			validateAddKeyAltName(mt, cse, res, abcKeyAltName)

			// Repeat Step 2, assert the operation does not fail, and assert the returned key document contains the
			// keyAltName "abc" added in Step 2.
			res = cse.clientEnc.AddKeyAltName(context.Background(), someNewKeyID, abcKeyAltName)
			validateAddKeyAltName(mt, cse, res, abcKeyAltName)

			// Use client_encryption to add a keyAltName "def" to the key created in Step 1 and assert the operation
			// fails due to a duplicate key server error (error code 11000).
			res = cse.clientEnc.AddKeyAltName(context.Background(), someNewKeyID, defKeyAltName)
			assert.NotNil(mt, res.Err(), "duplicate %q key did not propagate expected error", defKeyAltName)

			e110000 := "E11000 duplicate key"
			correctError := strings.Contains(res.Err().Error(), e110000)
			assert.True(t, correctError, "expected error to contain %q, got: %v", e110000, res.Err())

			// Use client_encryption to add a keyAltName "def" to the existing key, assert the operation does not fail,
			// and assert the returned key document contains the keyAltName "def" added during Setup.
			res = cse.clientEnc.AddKeyAltName(context.Background(), defKeyID, defKeyAltName)
			validateAddKeyAltName(mt, cse, res, defKeyAltName)
		})

	})

	mt.RunOpts("16. Rewrap", runOpts, func(mt *mtest.T) {
		mt.Run("Case 1: Rewrap with separate ClientEncryption", func(mt *mtest.T) {
			dataKeyMap := map[string]bson.M{
				"aws": {
					"region": "us-east-1",
					"key":    "arn:aws:kms:us-east-1:579766882180:key/89fcc2c4-08b0-4bd9-9f25-e30687b580d0",
				},
				"azure": {
					"keyVaultEndpoint": "key-vault-csfle.vault.azure.net",
					"keyName":          "key-name-csfle",
				},
				"gcp": {
					"projectId": "devprod-drivers",
					"location":  "global",
					"keyRing":   "key-ring-csfle",
					"keyName":   "key-name-csfle",
				},
				"kmip": {},
			}

			tlsConfig := make(map[string]*tls.Config)
			if tlsCAFileKMIP != "" && tlsClientCertificateKeyFileKMIP != "" {
				tlsOpts := map[string]interface{}{
					"tlsCertificateKeyFile": tlsClientCertificateKeyFileKMIP,
					"tlsCAFile":             tlsCAFileKMIP,
				}
				kmipConfig, err := options.BuildTLSConfig(tlsOpts)
				assert.Nil(mt, err, "BuildTLSConfig error: %v", err)
				tlsConfig["kmip"] = kmipConfig
			}

			kmsProviders := []string{"local", "aws", "gcp", "azure", "kmip"}
			for _, srcProvider := range kmsProviders {
				for _, dstProvider := range kmsProviders {
					mt.Run(fmt.Sprintf("%s to %s", srcProvider, dstProvider), func(mt *mtest.T) {
						var err error
						// Drop the collection ``keyvault.datakeys``.
						{
							err = mt.Client.Database("keyvault").Collection("datakeys").Drop(context.Background())
							assert.Nil(mt, err, "error on Drop: %v", err)
						}

						// Create a ``ClientEncryption`` object named ``clientEncryption1``.
						var clientEncryption1 *mongo.ClientEncryption
						{
							var keyVaultClient *mongo.Client
							{
								co := options.Client().ApplyURI(mtest.ClusterURI())
								keyVaultClient, err = mongo.Connect(co)
								defer keyVaultClient.Disconnect(context.Background())
								integtest.AddTestServerAPIVersion(co)
								assert.Nil(mt, err, "error on Connect: %v", err)
							}
							ceOpts := options.ClientEncryption().
								SetKeyVaultNamespace("keyvault.datakeys").
								SetKmsProviders(fullKmsProvidersMap).
								SetTLSConfig(tlsConfig)
							clientEncryption1, err = mongo.NewClientEncryption(keyVaultClient, ceOpts)
							assert.Nil(mt, err, "error in NewClientEncryption: %v", err)
							defer clientEncryption1.Close(context.Background())
						}

						// Call ``clientEncryption1.createDataKey``.
						var keyID bson.Binary
						{
							dkOpts := options.DataKey()
							if val, ok := dataKeyMap[srcProvider]; ok {
								dkOpts.SetMasterKey(val)
							}
							keyID, err = clientEncryption1.CreateDataKey(context.Background(), srcProvider, dkOpts)
							assert.Nil(mt, err, "error in CreateDataKey: %v", err)
						}

						// Call ``clientEncryption1.encrypt`` with the value "test".
						var ciphertext bson.Binary
						{
							t, value, err := bson.MarshalValue("test")
							assert.Nil(mt, err, "error in MarshalValue: %v", err)
							plaintext := bson.RawValue{Type: t, Value: value}
							eOpts := options.Encrypt().SetKeyID(keyID).SetAlgorithm("AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic")
							ciphertext, err = clientEncryption1.Encrypt(context.Background(), plaintext, eOpts)
							assert.Nil(mt, err, "error in Encrypt: %v", err)
						}

						// Create a ``ClientEncryption`` object named ``clientEncryption2``.
						var clientEncryption2 *mongo.ClientEncryption
						{
							var keyVaultClient *mongo.Client
							{
								co := options.Client().ApplyURI(mtest.ClusterURI())
								keyVaultClient, err = mongo.Connect(co)
								defer keyVaultClient.Disconnect(context.Background())
								integtest.AddTestServerAPIVersion(co)
								assert.Nil(mt, err, "error on Connect: %v", err)
							}
							ceOpts := options.ClientEncryption().
								SetKeyVaultNamespace("keyvault.datakeys").
								SetKmsProviders(fullKmsProvidersMap).
								SetTLSConfig(tlsConfig)
							clientEncryption2, err = mongo.NewClientEncryption(keyVaultClient, ceOpts)
							assert.Nil(mt, err, "error in NewClientEncryption: %v", err)
							defer clientEncryption2.Close(context.Background())
						}

						// Call ``clientEncryption2.rewrapManyDataKey`` with an empty ``filter``.
						{
							rwOpts := options.RewrapManyDataKey().SetProvider(dstProvider)
							if val, ok := dataKeyMap[dstProvider]; ok {
								rwOpts.SetMasterKey(val)
							}
							res, err := clientEncryption2.RewrapManyDataKey(context.Background(), bson.D{}, rwOpts)
							assert.Nil(mt, err, "error in RewrapManyDataKey: %v", err)
							assert.Equal(mt, res.BulkWriteResult.ModifiedCount, int64(1), "expected ModifiedCount of 1, got %v", res.BulkWriteResult.ModifiedCount)
						}

						// Call ``clientEncryption1.decrypt`` with the ``ciphertext``.
						{
							plaintext, err := clientEncryption1.Decrypt(context.Background(), ciphertext)
							assert.Nil(mt, err, "error in Decrypt: %v", err)
							assert.Equal(mt, plaintext.StringValue(), "test", "expected plaintext 'test', got %q", plaintext.StringValue())
						}

						// Call ``clientEncryption2.decrypt`` with the ``ciphertext``.
						{
							plaintext, err := clientEncryption2.Decrypt(context.Background(), ciphertext)
							assert.Nil(mt, err, "error in Decrypt: %v", err)
							assert.Equal(mt, plaintext.StringValue(), "test", "expected plaintext 'test', got %q", plaintext.StringValue())
						}
					})
				}
			}
		})

		mt.Run("Case 2: RewrapManyDataKeyOpts.provider is not optional", func(mt *mtest.T) {
			var err error
			var clientEncryption *mongo.ClientEncryption
			{
				var keyVaultClient *mongo.Client
				{
					co := options.Client().ApplyURI(mtest.ClusterURI())
					keyVaultClient, err = mongo.Connect(co)
					defer keyVaultClient.Disconnect(context.Background())
					integtest.AddTestServerAPIVersion(co)
					assert.Nil(mt, err, "error on Connect: %v", err)
				}
				ceOpts := options.ClientEncryption().
					SetKeyVaultNamespace("keyvault.datakeys").
					SetKmsProviders(fullKmsProvidersMap)
				clientEncryption, err = mongo.NewClientEncryption(keyVaultClient, ceOpts)
				assert.Nil(mt, err, "error in NewClientEncryption: %v", err)
				defer clientEncryption.Close(context.Background())
			}

			_, err = clientEncryption.RewrapManyDataKey(context.Background(), bson.D{}, options.RewrapManyDataKey().SetMasterKey(bson.D{}))
			assert.ErrorContains(mt, err, "expected 'Provider' to be set to identify type of 'MasterKey'")
		})
	})

	mt.RunOpts("18. Azure IMDS Credentials", noClientOpts, func(mt *mtest.T) {
		buf := new(bytes.Buffer)
		kmsProvidersMap := map[string]map[string]interface{}{
			"azure": {},
		}
		vw := bson.NewDocumentWriter(buf)
		err := bson.NewEncoder(vw).Encode(kmsProvidersMap)
		assert.Nil(mt, err, "error in Encode: %v", err)

		getClient := func(header http.Header) *http.Client {
			lt := &localTransport{
				header: header,
				rt:     http.DefaultTransport,
			}
			return &http.Client{
				Timeout:   30 * time.Second,
				Transport: lt,
			}
		}

		mt.Run("Case 1: Success", func(mt *mtest.T) {
			opts := &mongocryptopts.MongoCryptOptions{
				KmsProviders: buf.Bytes(),
				HTTPClient:   getClient(nil),
			}
			crypt, err := mongocrypt.NewMongoCrypt(opts)
			assert.Nil(mt, err, "error in NewMongoCrypt: %v", err)
			doc, err := crypt.GetKmsProviders(context.Background())
			assert.Nil(mt, err, "error in GetKmsProviders: %v", err)
			val, err := doc.LookupErr("azure")
			assert.Nil(mt, err, "error in LookupErr: %v", err)
			assert.Equal(mt, `{"accessToken": "magic-cookie"}`, val.String(), "expected accessToken, got %s", val.String())
		})
		mt.Run("Case 2: Empty JSON", func(mt *mtest.T) {
			header := make(http.Header)
			header.Set("X-MongoDB-HTTP-TestParams", "case=empty-json")
			opts := &mongocryptopts.MongoCryptOptions{
				KmsProviders: buf.Bytes(),
				HTTPClient:   getClient(header),
			}
			crypt, err := mongocrypt.NewMongoCrypt(opts)
			assert.Nil(mt, err, "error in NewMongoCrypt: %v", err)
			_, err = crypt.GetKmsProviders(context.Background())
			assert.ErrorContains(mt, err, "got unexpected empty accessToken")
		})
		mt.Run("Case 3: Bad JSON", func(mt *mtest.T) {
			header := make(http.Header)
			header.Set("X-MongoDB-HTTP-TestParams", "case=bad-json")
			opts := &mongocryptopts.MongoCryptOptions{
				KmsProviders: buf.Bytes(),
				HTTPClient:   getClient(header),
			}
			crypt, err := mongocrypt.NewMongoCrypt(opts)
			assert.Nil(mt, err, "error in NewMongoCrypt: %v", err)
			_, err = crypt.GetKmsProviders(context.Background())
			assert.ErrorContains(mt, err, "error reading body JSON")
		})
		mt.Run("Case 4: HTTP 404", func(mt *mtest.T) {
			header := make(http.Header)
			header.Set("X-MongoDB-HTTP-TestParams", "case=404")
			opts := &mongocryptopts.MongoCryptOptions{
				KmsProviders: buf.Bytes(),
				HTTPClient:   getClient(header),
			}
			crypt, err := mongocrypt.NewMongoCrypt(opts)
			assert.Nil(mt, err, "error in NewMongoCrypt: %v", err)
			_, err = crypt.GetKmsProviders(context.Background())
			assert.ErrorContains(mt, err, "got StatusCode: 404")
		})
		mt.Run("Case 5: HTTP 500", func(mt *mtest.T) {
			header := make(http.Header)
			header.Set("X-MongoDB-HTTP-TestParams", "case=500")
			opts := &mongocryptopts.MongoCryptOptions{
				KmsProviders: buf.Bytes(),
				HTTPClient:   getClient(header),
			}
			crypt, err := mongocrypt.NewMongoCrypt(opts)
			assert.Nil(mt, err, "error in NewMongoCrypt: %v", err)
			_, err = crypt.GetKmsProviders(context.Background())
			assert.ErrorContains(mt, err, "got StatusCode: 500")
		})
		mt.Run("Case 6: Slow Response", func(mt *mtest.T) {
			header := make(http.Header)
			header.Set("X-MongoDB-HTTP-TestParams", "case=slow")
			opts := &mongocryptopts.MongoCryptOptions{
				KmsProviders: buf.Bytes(),
				HTTPClient:   getClient(header),
			}
			crypt, err := mongocrypt.NewMongoCrypt(opts)
			assert.Nil(mt, err, "error in NewMongoCrypt: %v", err)
			_, err = crypt.GetKmsProviders(context.Background())

			possibleErrors := []string{
				"error reading response body: context deadline exceeded",    // <= 1.19 + RHEL & macOS
				"Client.Timeout or context cancellation while reading body", // > 1.20 on all OS
			}

			assert.True(t, containsPattern(possibleErrors, err.Error()),
				"expected possibleErrors=%v to contain %v, but it didn't",
				possibleErrors, err.Error())
		})
	})

	mt.RunOpts("20. Bypass creating mongocryptd client when shared library is loaded",
		noClientOpts, func(mt *mtest.T) {
			cryptSharedLibPath := os.Getenv("CRYPT_SHARED_LIB_PATH")
			if cryptSharedLibPath == "" {
				mt.Skip("CRYPT_SHARED_LIB_PATH not set, skipping")
				return
			}

			kmsProviders := map[string]map[string]interface{}{
				"local": {
					"key": localMasterKey,
				},
			}

			// Listen for connections in a separate goroutine.
			listener := listenForConnections(t, func(c net.Conn) {
				mt.Fatalf("Unexpected connection created to mock mongocryptd goroutine")
				c.Close()
			})
			defer listener.Close()

			uri := "mongodb://" + listener.Addr().String()
			mongocryptdSpawnArgs := map[string]interface{}{
				"mongocryptdURI":     uri,
				"cryptSharedLibPath": cryptSharedLibPath,
			}

			aeo := options.AutoEncryption().SetKmsProviders(kmsProviders).
				SetExtraOptions(mongocryptdSpawnArgs)
			cliOpts := options.Client().ApplyURI(mtest.ClusterURI()).SetAutoEncryptionOptions(aeo)
			integtest.AddTestServerAPIVersion(cliOpts)
			encClient, err := mongo.Connect(cliOpts)
			assert.Nil(mt, err, "Connect error: %v", err)
			defer func() {
				err = encClient.Disconnect(context.Background())
				assert.Nil(mt, err, "encrypted client Disconnect error: %v", err)
			}()

			// Run an Insert through the encrypted client to make sure mongocryptd is started ('insert' uses the
			// mongocryptd and will wait for it to be active).
			_, err = encClient.Database("db").Collection("coll").InsertOne(context.Background(), bson.D{{"unencrypted", "test"}})
			assert.Nil(mt, err, "InsertOne error: %v", err)
		})

	// qeRunOpts are requirements for Queryable Encryption.
	qeRunOpts := mtest.NewOptions().MinServerVersion("7.0").Topologies(mtest.ReplicaSet, mtest.Sharded, mtest.LoadBalanced, mtest.ShardedReplicaSet)
	// Only test MongoDB Server 7.0+. MongoDB Server 7.0 introduced a backwards breaking change to the Queryable Encryption (QE) protocol: QEv2.
	// libmongocrypt is configured to use the QEv2 protocol.
	mt.RunOpts("21. automatic data encryption keys", qeRunOpts, func(mt *mtest.T) {
		setup := func() (*mongo.Client, *mongo.ClientEncryption, error) {
			opts := options.Client().ApplyURI(mtest.ClusterURI())
			client, err := mongo.Connect(opts)
			if err != nil {
				return nil, nil, err
			}
			client.Database("keyvault").Collection("datakeys").Drop(context.Background())
			client.Database("db").Drop(context.Background())
			ceo := options.ClientEncryption().
				SetKmsProviders(fullKmsProvidersMap).
				SetKeyVaultNamespace(kvNamespace)
			clientEnc, err := mongo.NewClientEncryption(client, ceo)
			if err != nil {
				return nil, nil, err
			}
			return client, clientEnc, nil
		}

		type KMSProviderTestcase struct {
			kmsProvider string
			masterKey   *bson.D
		}

		testcases := []KMSProviderTestcase{
			{
				kmsProvider: "local",
				masterKey:   nil,
			},
			{
				kmsProvider: "aws",
				masterKey: &bson.D{
					{"region", "us-east-1"},
					{"key", "arn:aws:kms:us-east-1:579766882180:key/89fcc2c4-08b0-4bd9-9f25-e30687b580d0"},
				},
			},
		}

		for _, tc := range testcases {
			mt.Run(tc.kmsProvider, func(mt *mtest.T) {

				mt.Run("case 1: simple creation and validation", func(mt *mtest.T) {
					client, clientEnc, err := setup()
					assert.Nil(mt, err, "setup error: %v", err)
					defer func() {
						err := clientEnc.Close(context.Background())
						assert.Nil(mt, err, "error in Close")
					}()

					var encryptedFields bson.Raw
					err = bson.UnmarshalExtJSON([]byte(`{
				"fields": [{
					"path": "ssn",
					"bsonType": "string",
					"keyId": null
				}]
			}`), true /* canonical */, &encryptedFields)
					assert.Nil(mt, err, "Unmarshal error: %v", err)

					coll, _, err := clientEnc.CreateEncryptedCollection(
						context.Background(),
						client.Database("db"),
						"testing1", options.CreateCollection().SetEncryptedFields(encryptedFields),
						"local", nil,
					)
					assert.Nil(mt, err, "CreateCollection error: %v", err)

					_, err = coll.InsertOne(context.Background(), bson.D{{"ssn", "123-45-6789"}})
					assert.ErrorContains(mt, err, "Document failed validation")
				})
				mt.Run("case 2: missing encryptedFields", func(mt *mtest.T) {
					client, clientEnc, err := setup()
					assert.Nil(mt, err, "setup error: %v", err)
					defer func() {
						err := clientEnc.Close(context.Background())
						assert.Nil(mt, err, "error in Close")
					}()

					coll, _, err := clientEnc.CreateEncryptedCollection(
						context.Background(),
						client.Database("db"),
						"testing1", options.CreateCollection(),
						"local", nil,
					)
					assert.Nil(mt, coll, "expect nil collection")
					assert.EqualError(mt, err, "no EncryptedFields defined for the collection")
				})
				mt.Run("case 3: invalid keyId", func(mt *mtest.T) {
					client, clientEnc, err := setup()
					assert.Nil(mt, err, "setup error: %v", err)
					defer func() {
						err := clientEnc.Close(context.Background())
						assert.Nil(mt, err, "error in Close")
					}()

					var encryptedFields bson.Raw
					err = bson.UnmarshalExtJSON([]byte(`{
				"fields": [{
					"path": "ssn",
					"bsonType": "string",
					"keyId": false
				}]
			}`), true /* canonical */, &encryptedFields)
					assert.Nil(mt, err, "Unmarshal error: %v", err)

					_, _, err = clientEnc.CreateEncryptedCollection(
						context.Background(),
						client.Database("db"),
						"testing1", options.CreateCollection().SetEncryptedFields(encryptedFields),
						"local", nil,
					)
					assert.ErrorContains(mt, err, "BSON field 'create.encryptedFields.fields.keyId' is the wrong type 'bool', expected type 'binData'")
				})
				mt.Run("case 4: insert encrypted value", func(mt *mtest.T) {
					client, clientEnc, err := setup()
					assert.Nil(mt, err, "setup error: %v", err)
					defer func() {
						err := clientEnc.Close(context.Background())
						assert.Nil(mt, err, "error in Close")
					}()

					var encryptedFields bson.Raw
					err = bson.UnmarshalExtJSON([]byte(`{
				"fields": [{
					"path": "ssn",
					"bsonType": "string",
					"keyId": null
				}]
			}`), true /* canonical */, &encryptedFields)
					assert.Nil(mt, err, "Unmarshal error: %v", err)

					coll, ef, err := clientEnc.CreateEncryptedCollection(
						context.Background(),
						client.Database("db"),
						"testing1", options.CreateCollection().SetEncryptedFields(encryptedFields),
						"local", nil,
					)
					assert.Nil(mt, err, "CreateCollection error: %v", err)

					keyid := ef["fields"].(bson.A)[0].(bson.M)["keyId"].(bson.Binary)
					rawValueType, rawValueData, err := bson.MarshalValue("123-45-6789")
					assert.Nil(mt, err, "MarshalValue error: %v", err)
					rawValue := bson.RawValue{Type: rawValueType, Value: rawValueData}
					encryptionOpts := options.Encrypt().
						SetAlgorithm("Unindexed").
						SetKeyID(keyid)
					encryptedField, err := clientEnc.Encrypt(
						context.Background(),
						rawValue,
						encryptionOpts)
					assert.Nil(mt, err, "Encrypt error: %v", err)

					_, err = coll.InsertOne(context.Background(), bson.D{{"ssn", encryptedField}})
					assert.Nil(mt, err, "InsertOne error: %v", err)
				})
			})
		}
	})

	qeRunOpts22 := qeRunOpts.MinServerVersion("8.0")
	mt.RunOpts("22. range explicit encryption", qeRunOpts22, func(mt *mtest.T) {
		type testcase struct {
			typeStr       string
			field         string
			typeBson      bson.Type
			rangeOpts     *options.RangeOptionsBuilder
			zero          bson.RawValue
			six           bson.RawValue
			thirty        bson.RawValue
			twoHundred    bson.RawValue
			twoHundredOne bson.RawValue
		}

		trimFactor := int32(1)
		sparsity := int64(1)
		precision := int32(2)

		d128_0, err := bson.ParseDecimal128("0")
		assert.Nil(mt, err)
		d128_0h, d128_0l := d128_0.GetBytes()

		d128_6, err := bson.ParseDecimal128("6")
		assert.Nil(mt, err)
		d128_6h, d128_6l := d128_6.GetBytes()

		d128_30, err := bson.ParseDecimal128("30")
		assert.Nil(mt, err)
		d128_30h, d128_30l := d128_30.GetBytes()

		d128_200, err := bson.ParseDecimal128("200")
		assert.Nil(mt, err)
		d128_200h, d128_200l := d128_200.GetBytes()

		d128_201, err := bson.ParseDecimal128("201")
		assert.Nil(mt, err)
		d128_201h, d128_201l := d128_201.GetBytes()

		tests := []testcase{
			{
				typeStr:       "DecimalNoPrecision",
				field:         "encryptedDecimalNoPrecision",
				typeBson:      bson.TypeDecimal128,
				rangeOpts:     options.Range().SetTrimFactor(trimFactor).SetSparsity(sparsity),
				zero:          bson.RawValue{Type: bson.TypeDecimal128, Value: bsoncore.AppendDecimal128(nil, d128_0h, d128_0l)},
				six:           bson.RawValue{Type: bson.TypeDecimal128, Value: bsoncore.AppendDecimal128(nil, d128_6h, d128_6l)},
				thirty:        bson.RawValue{Type: bson.TypeDecimal128, Value: bsoncore.AppendDecimal128(nil, d128_30h, d128_30l)},
				twoHundred:    bson.RawValue{Type: bson.TypeDecimal128, Value: bsoncore.AppendDecimal128(nil, d128_200h, d128_200l)},
				twoHundredOne: bson.RawValue{Type: bson.TypeDecimal128, Value: bsoncore.AppendDecimal128(nil, d128_201h, d128_201l)},
			},
			{
				typeStr:  "DecimalPrecision",
				field:    "encryptedDecimalPrecision",
				typeBson: bson.TypeDecimal128,
				rangeOpts: options.Range().
					SetMin(bson.RawValue{Type: bson.TypeDecimal128, Value: bsoncore.AppendDecimal128(nil, d128_0h, d128_0l)}).
					SetMax(bson.RawValue{Type: bson.TypeDecimal128, Value: bsoncore.AppendDecimal128(nil, d128_200h, d128_200l)}).
					SetTrimFactor(trimFactor).
					SetSparsity(sparsity).
					SetPrecision(precision),
				zero:          bson.RawValue{Type: bson.TypeDecimal128, Value: bsoncore.AppendDecimal128(nil, d128_0h, d128_0l)},
				six:           bson.RawValue{Type: bson.TypeDecimal128, Value: bsoncore.AppendDecimal128(nil, d128_6h, d128_6l)},
				thirty:        bson.RawValue{Type: bson.TypeDecimal128, Value: bsoncore.AppendDecimal128(nil, d128_30h, d128_30l)},
				twoHundred:    bson.RawValue{Type: bson.TypeDecimal128, Value: bsoncore.AppendDecimal128(nil, d128_200h, d128_200l)},
				twoHundredOne: bson.RawValue{Type: bson.TypeDecimal128, Value: bsoncore.AppendDecimal128(nil, d128_201h, d128_201l)},
			},
			{
				typeStr:       "DoubleNoPrecision",
				field:         "encryptedDoubleNoPrecision",
				typeBson:      bson.TypeDouble,
				rangeOpts:     options.Range().SetTrimFactor(trimFactor).SetSparsity(sparsity),
				zero:          bson.RawValue{Type: bson.TypeDouble, Value: bsoncore.AppendDouble(nil, 0)},
				six:           bson.RawValue{Type: bson.TypeDouble, Value: bsoncore.AppendDouble(nil, 6)},
				thirty:        bson.RawValue{Type: bson.TypeDouble, Value: bsoncore.AppendDouble(nil, 30)},
				twoHundred:    bson.RawValue{Type: bson.TypeDouble, Value: bsoncore.AppendDouble(nil, 200)},
				twoHundredOne: bson.RawValue{Type: bson.TypeDouble, Value: bsoncore.AppendDouble(nil, 201)},
			},
			{
				typeStr:  "DoublePrecision",
				field:    "encryptedDoublePrecision",
				typeBson: bson.TypeDouble,
				rangeOpts: options.Range().
					SetMin(bson.RawValue{Type: bson.TypeDouble, Value: bsoncore.AppendDouble(nil, 0)}).
					SetMax(bson.RawValue{Type: bson.TypeDouble, Value: bsoncore.AppendDouble(nil, 200)}).
					SetTrimFactor(trimFactor).
					SetSparsity(sparsity).
					SetPrecision(precision),
				zero:          bson.RawValue{Type: bson.TypeDouble, Value: bsoncore.AppendDouble(nil, 0)},
				six:           bson.RawValue{Type: bson.TypeDouble, Value: bsoncore.AppendDouble(nil, 6)},
				thirty:        bson.RawValue{Type: bson.TypeDouble, Value: bsoncore.AppendDouble(nil, 30)},
				twoHundred:    bson.RawValue{Type: bson.TypeDouble, Value: bsoncore.AppendDouble(nil, 200)},
				twoHundredOne: bson.RawValue{Type: bson.TypeDouble, Value: bsoncore.AppendDouble(nil, 201)},
			},
			{
				typeStr:  "Date",
				field:    "encryptedDate",
				typeBson: bson.TypeDateTime,
				rangeOpts: options.Range().
					SetMin(bson.RawValue{Type: bson.TypeDateTime, Value: bsoncore.AppendDateTime(nil, 0)}).
					SetMax(bson.RawValue{Type: bson.TypeDateTime, Value: bsoncore.AppendDateTime(nil, 200)}).
					SetTrimFactor(trimFactor).
					SetSparsity(sparsity),
				zero:          bson.RawValue{Type: bson.TypeDateTime, Value: bsoncore.AppendDateTime(nil, 0)},
				six:           bson.RawValue{Type: bson.TypeDateTime, Value: bsoncore.AppendDateTime(nil, 6)},
				thirty:        bson.RawValue{Type: bson.TypeDateTime, Value: bsoncore.AppendDateTime(nil, 30)},
				twoHundred:    bson.RawValue{Type: bson.TypeDateTime, Value: bsoncore.AppendDateTime(nil, 200)},
				twoHundredOne: bson.RawValue{Type: bson.TypeDateTime, Value: bsoncore.AppendDateTime(nil, 201)},
			},
			{
				typeStr:  "Int",
				field:    "encryptedInt",
				typeBson: bson.TypeInt32,
				rangeOpts: options.Range().
					SetMin(bson.RawValue{Type: bson.TypeInt32, Value: bsoncore.AppendInt32(nil, 0)}).
					SetMax(bson.RawValue{Type: bson.TypeInt32, Value: bsoncore.AppendInt32(nil, 200)}).
					SetTrimFactor(trimFactor).
					SetSparsity(sparsity),
				zero:          bson.RawValue{Type: bson.TypeInt32, Value: bsoncore.AppendInt32(nil, 0)},
				six:           bson.RawValue{Type: bson.TypeInt32, Value: bsoncore.AppendInt32(nil, 6)},
				thirty:        bson.RawValue{Type: bson.TypeInt32, Value: bsoncore.AppendInt32(nil, 30)},
				twoHundred:    bson.RawValue{Type: bson.TypeInt32, Value: bsoncore.AppendInt32(nil, 200)},
				twoHundredOne: bson.RawValue{Type: bson.TypeInt32, Value: bsoncore.AppendInt32(nil, 201)},
			},
			{
				typeStr:  "Long",
				field:    "encryptedLong",
				typeBson: bson.TypeInt64,
				rangeOpts: options.Range().
					SetMin(bson.RawValue{Type: bson.TypeInt64, Value: bsoncore.AppendInt64(nil, 0)}).
					SetMax(bson.RawValue{Type: bson.TypeInt64, Value: bsoncore.AppendInt64(nil, 200)}).
					SetTrimFactor(trimFactor).
					SetSparsity(sparsity),
				zero:          bson.RawValue{Type: bson.TypeInt64, Value: bsoncore.AppendInt64(nil, 0)},
				six:           bson.RawValue{Type: bson.TypeInt64, Value: bsoncore.AppendInt64(nil, 6)},
				thirty:        bson.RawValue{Type: bson.TypeInt64, Value: bsoncore.AppendInt64(nil, 30)},
				twoHundred:    bson.RawValue{Type: bson.TypeInt64, Value: bsoncore.AppendInt64(nil, 200)},
				twoHundredOne: bson.RawValue{Type: bson.TypeInt64, Value: bsoncore.AppendInt64(nil, 201)},
			},
		}

		for _, test := range tests {
			mt.Run(test.typeStr, func(mt *mtest.T) {
				if test.typeStr == "DecimalNoPrecision" && mtest.ClusterTopologyKind() != mtest.ReplicaSet {
					mt.Skipf("Skipping DecimalNoPrecision tests on a non ReplicaSet topology. DecimalNoPrecision queries are expected to take a long time and may exceed the default mongos timeout")
				}

				// Test Setup ... begin
				encryptedFields := readJSONFile(mt, fmt.Sprintf("range-encryptedFields-%v.json", test.typeStr))
				key1Document := readJSONFile(mt, "key1-document.json")
				var key1ID bson.Binary
				{
					subtype, data := key1Document.Lookup("_id").Binary()
					key1ID = bson.Binary{Subtype: subtype, Data: data}
				}

				testSetup := func() (*mongo.Client, *mongo.ClientEncryption) {
					mtest.DropEncryptedCollection(mt, mt.Client.Database("db").Collection("explicit_encryption"), encryptedFields)
					cco := options.CreateCollection().SetEncryptedFields(encryptedFields)
					err := mt.Client.Database("db").CreateCollection(context.Background(), "explicit_encryption", cco)
					assert.Nil(mt, err, "error on CreateCollection: %v", err)
					err = mt.Client.Database("keyvault").Collection("datakeys").Drop(context.Background())
					assert.Nil(mt, err, "error on Drop: %v", err)
					keyVaultClient, err := mongo.Connect(options.Client().ApplyURI(mtest.ClusterURI()))
					assert.Nil(mt, err, "error on Connect: %v", err)
					datakeysColl := keyVaultClient.Database("keyvault").Collection("datakeys", options.Collection().SetWriteConcern(mtest.MajorityWc))
					_, err = datakeysColl.InsertOne(context.Background(), key1Document)
					assert.Nil(mt, err, "error on InsertOne: %v", err)
					// Create a ClientEncryption.
					ceo := options.ClientEncryption().
						SetKeyVaultNamespace("keyvault.datakeys").
						SetKmsProviders(fullKmsProvidersMap)
					clientEncryption, err := mongo.NewClientEncryption(keyVaultClient, ceo)
					assert.Nil(mt, err, "error on NewClientEncryption: %v", err)

					// Create a MongoClient with AutoEncryptionOpts and bypassQueryAnalysis=true.
					aeo := options.AutoEncryption().
						SetKeyVaultNamespace("keyvault.datakeys").
						SetKmsProviders(fullKmsProvidersMap).
						SetBypassQueryAnalysis(true)
					co := options.Client().SetAutoEncryptionOptions(aeo).ApplyURI(mtest.ClusterURI())
					encryptedClient, err := mongo.Connect(co)
					assert.Nil(mt, err, "error on Connect: %v", err)

					// Insert 0, 6, 30, and 200.
					coll := encryptedClient.Database("db").Collection("explicit_encryption")
					eo := options.Encrypt().
						SetAlgorithm("Range").
						SetKeyID(key1ID).
						SetContentionFactor(0).
						SetRangeOptions(test.rangeOpts)
					// Insert 0.
					insertPayloadZero, err := clientEncryption.Encrypt(context.Background(), test.zero, eo)
					assert.Nil(mt, err, "error in Encrypt: %v", err)
					_, err = coll.InsertOne(context.Background(), bson.D{{"_id", 0}, {test.field, insertPayloadZero}})
					assert.Nil(mt, err, "error in InsertOne: %v", err)
					// Insert 6.
					insertPayloadSix, err := clientEncryption.Encrypt(context.Background(), test.six, eo)
					assert.Nil(mt, err, "error in Encrypt: %v", err)
					_, err = coll.InsertOne(context.Background(), bson.D{{"_id", 1}, {test.field, insertPayloadSix}})
					assert.Nil(mt, err, "error in InsertOne: %v", err)
					// Insert 30.
					insertPayloadThirty, err := clientEncryption.Encrypt(context.Background(), test.thirty, eo)
					assert.Nil(mt, err, "error in Encrypt: %v", err)
					_, err = coll.InsertOne(context.Background(), bson.D{{"_id", 2}, {test.field, insertPayloadThirty}})
					assert.Nil(mt, err, "error in InsertOne: %v", err)
					// Insert 200.
					insertPayloadTwoHundred, err := clientEncryption.Encrypt(context.Background(), test.twoHundred, eo)
					assert.Nil(mt, err, "error in Encrypt: %v", err)
					_, err = coll.InsertOne(context.Background(), bson.D{{"_id", 3}, {test.field, insertPayloadTwoHundred}})
					assert.Nil(mt, err, "error in InsertOne: %v", err)

					return encryptedClient, clientEncryption
				}
				// Test Setup ... end

				// checkCursorResults checks documents returned by a cursor.
				// Expects document i to have field `field` with value `values[i]`.
				// Expects exactly len(values) documents to be returned.
				checkCursorResults := func(cursor *mongo.Cursor, field string, values ...bson.RawValue) {
					for i, v := range values {
						assert.True(mt, cursor.Next(context.Background()), "expected Next true, got false. Expected document %v with value: %v", i, v)
						got, err := cursor.Current.LookupErr(test.field)
						assert.Nil(mt, err, "%v not found in document %v: %v", test.field, i, cursor.Current)
						assert.Equal(mt, v, got, "expected %v, got %v in document %v", v, got, i)
					}
					assert.False(mt, cursor.Next(context.Background()), "expected Next false, got true with document: %v", cursor.Current)
				}

				mt.Run("Case 1: can decrypt a payload", func(mt *mtest.T) {
					encryptedClient, clientEncryption := testSetup()
					defer clientEncryption.Close(context.Background())
					defer encryptedClient.Disconnect(context.Background())
					eo := options.Encrypt().
						SetAlgorithm("Range").
						SetKeyID(key1ID).
						SetContentionFactor(0).
						SetRangeOptions(test.rangeOpts)
					insertPayloadSix, err := clientEncryption.Encrypt(context.Background(), test.six, eo)
					assert.Nil(mt, err, "error in Encrypt: %v", err)
					got, err := clientEncryption.Decrypt(context.Background(), insertPayloadSix)
					assert.Nil(mt, err, "error in Decrypt: %v", err)
					assert.Equal(mt, test.six, got, "expected %v, got %v", test.six, got)
				})

				mt.Run("Case 2: can find encrypted range and return the maximum", func(mt *mtest.T) {
					encryptedClient, clientEncryption := testSetup()
					defer clientEncryption.Close(context.Background())
					defer encryptedClient.Disconnect(context.Background())
					eo := options.Encrypt().
						SetAlgorithm("Range").
						SetKeyID(key1ID).
						SetContentionFactor(0).
						SetQueryType("range").
						SetRangeOptions(test.rangeOpts)

					expr := bson.M{
						"$and": bson.A{
							bson.M{
								test.field: bson.M{
									"$gte": test.six,
								},
							},
							bson.M{
								test.field: bson.M{
									"$lte": test.twoHundred,
								},
							},
						},
					}

					// Encrypt.
					var encryptedExpr bson.Raw
					{
						err := clientEncryption.EncryptExpression(context.Background(), expr, &encryptedExpr, eo)
						assert.Nil(mt, err, "error in EncryptExpression: %v", err)
					}

					coll := encryptedClient.Database("db").Collection("explicit_encryption")
					opts := options.Find().SetSort(bson.D{{"_id", 1}})
					cursor, err := coll.Find(context.Background(), encryptedExpr, opts)
					assert.Nil(mt, err, "error in coll.Find: %v", err)
					defer cursor.Close(context.Background())

					checkCursorResults(cursor, test.field, test.six, test.thirty, test.twoHundred)
				})

				mt.Run("Case 3: can find encrypted range and return the minimum", func(mt *mtest.T) {
					encryptedClient, clientEncryption := testSetup()
					defer clientEncryption.Close(context.Background())
					defer encryptedClient.Disconnect(context.Background())
					eo := options.Encrypt().
						SetAlgorithm("Range").
						SetKeyID(key1ID).
						SetContentionFactor(0).
						SetQueryType("range").
						SetRangeOptions(test.rangeOpts)

					expr := bson.M{
						"$and": bson.A{
							bson.M{
								test.field: bson.M{
									"$gte": test.zero,
								},
							},
							bson.M{
								test.field: bson.M{
									"$lte": test.six,
								},
							},
						},
					}

					// Encrypt.
					var encryptedExpr bson.Raw
					{
						err := clientEncryption.EncryptExpression(context.Background(), expr, &encryptedExpr, eo)
						assert.Nil(mt, err, "error in EncryptExpression: %v", err)
					}

					coll := encryptedClient.Database("db").Collection("explicit_encryption")
					opts := options.Find().SetSort(bson.D{{"_id", 1}})
					cursor, err := coll.Find(context.Background(), encryptedExpr, opts)
					assert.Nil(mt, err, "error in coll.Find: %v", err)
					defer cursor.Close(context.Background())

					checkCursorResults(cursor, test.field, test.zero, test.six)
				})

				mt.Run("Case 4: can find encrypted range with an open range query", func(mt *mtest.T) {
					encryptedClient, clientEncryption := testSetup()
					defer clientEncryption.Close(context.Background())
					defer encryptedClient.Disconnect(context.Background())
					eo := options.Encrypt().
						SetAlgorithm("Range").
						SetKeyID(key1ID).
						SetContentionFactor(0).
						SetQueryType("range").
						SetRangeOptions(test.rangeOpts)

					expr := bson.M{
						"$and": bson.A{
							bson.M{
								test.field: bson.M{
									"$gt": test.thirty,
								},
							},
						},
					}

					// Encrypt.
					var encryptedExpr bson.Raw
					{
						err := clientEncryption.EncryptExpression(context.Background(), expr, &encryptedExpr, eo)
						assert.Nil(mt, err, "error in EncryptExpression: %v", err)
					}

					coll := encryptedClient.Database("db").Collection("explicit_encryption")
					opts := options.Find().SetSort(bson.D{{"_id", 1}})
					cursor, err := coll.Find(context.Background(), encryptedExpr, opts)
					assert.Nil(mt, err, "error in coll.Find: %v", err)
					defer cursor.Close(context.Background())

					checkCursorResults(cursor, test.field, test.twoHundred)
				})

				mt.Run("Case 5: can run an aggregation expression inside $expr", func(mt *mtest.T) {
					encryptedClient, clientEncryption := testSetup()
					defer clientEncryption.Close(context.Background())
					defer encryptedClient.Disconnect(context.Background())
					eo := options.Encrypt().
						SetAlgorithm("Range").
						SetKeyID(key1ID).
						SetContentionFactor(0).
						SetQueryType("range").
						SetRangeOptions(test.rangeOpts)

					expr := bson.M{
						"$and": bson.A{
							bson.M{
								"$lt": bson.A{
									fmt.Sprintf("$%v", test.field),
									test.thirty,
								},
							},
						},
					}

					// Encrypt.
					var encryptedExpr bson.Raw
					{
						err := clientEncryption.EncryptExpression(context.Background(), expr, &encryptedExpr, eo)
						assert.Nil(mt, err, "error in EncryptExpression: %v", err)
					}

					coll := encryptedClient.Database("db").Collection("explicit_encryption")
					opts := options.Find().SetSort(bson.D{{"_id", 1}})
					cursor, err := coll.Find(context.Background(), bson.M{"$expr": encryptedExpr}, opts)
					assert.Nil(mt, err, "error in coll.Find: %v", err)
					defer cursor.Close(context.Background())

					checkCursorResults(cursor, test.field, test.zero, test.six)
				})

				if test.field != "encryptedDoubleNoPrecision" && test.field != "encryptedDecimalNoPrecision" {
					mt.Run("Case 6: encrypting a document greater than the maximum errors", func(mt *mtest.T) {
						encryptedClient, clientEncryption := testSetup()
						defer clientEncryption.Close(context.Background())
						defer encryptedClient.Disconnect(context.Background())
						eo := options.Encrypt().
							SetAlgorithm("Range").
							SetKeyID(key1ID).
							SetContentionFactor(0).
							SetRangeOptions(test.rangeOpts)

						_, err := clientEncryption.Encrypt(context.Background(), test.twoHundredOne, eo)
						assert.NotNil(mt, err, "expected error, but got none")
					})

					mt.Run("Case 7: encrypting a document of a different type errors", func(mt *mtest.T) {
						encryptedClient, clientEncryption := testSetup()
						defer clientEncryption.Close(context.Background())
						defer encryptedClient.Disconnect(context.Background())
						eo := options.Encrypt().
							SetAlgorithm("Range").
							SetKeyID(key1ID).
							SetContentionFactor(0).
							SetRangeOptions(test.rangeOpts)

						var val bson.RawValue
						if test.field == "encryptedInt" {
							val = bson.RawValue{Type: bson.TypeDouble, Value: bsoncore.AppendDouble(nil, 6)}
						} else {
							val = bson.RawValue{Type: bson.TypeInt32, Value: bsoncore.AppendInt32(nil, 6)}
						}

						_, err := clientEncryption.Encrypt(context.Background(), val, eo)
						assert.NotNil(mt, err, "expected error, but got none")
					})
				}

				if test.field != "encryptedDoubleNoPrecision" && test.field != "encryptedDoublePrecision" && test.field != "encryptedDecimalNoPrecision" && test.field != "encryptedDecimalPrecision" {
					mt.Run("Case 8: setting precision errors if the type is not a double", func(mt *mtest.T) {
						encryptedClient, clientEncryption := testSetup()
						defer clientEncryption.Close(context.Background())
						defer encryptedClient.Disconnect(context.Background())

						// Copy rangeOpts and set precision.
						ro := test.rangeOpts
						ro.SetPrecision(2)
						eo := options.Encrypt().
							SetAlgorithm("Range").
							SetKeyID(key1ID).
							SetContentionFactor(0).
							SetRangeOptions(ro)

						_, err := clientEncryption.Encrypt(context.Background(), test.six, eo)
						assert.NotNil(mt, err, "expected error, but got none")
					})
				}
			})
		}
	})

	mt.RunOpts("23. range explicit encryption applies defaults", qeRunOpts22, func(mt *mtest.T) {
		err := mt.Client.Database("keyvault").Collection("datakeys").Drop(context.Background())
		assert.Nil(mt, err, "error on Drop: %v", err)

		testVal := bson.RawValue{Type: bson.TypeInt32, Value: bsoncore.AppendInt32(nil, 123)}

		keyVaultClient, err := mongo.Connect(options.Client().ApplyURI(mtest.ClusterURI()))
		assert.Nil(mt, err, "error on Connect: %v", err)

		ceo := options.ClientEncryption().
			SetKeyVaultNamespace("keyvault.datakeys").
			SetKmsProviders(fullKmsProvidersMap)
		clientEncryption, err := mongo.NewClientEncryption(keyVaultClient, ceo)
		assert.Nil(mt, err, "error on NewClientEncryption: %v", err)

		dkOpts := options.DataKey()
		keyID, err := clientEncryption.CreateDataKey(context.Background(), "local", dkOpts)
		assert.Nil(mt, err, "error in CreateDataKey: %v", err)

		eo := options.Encrypt().
			SetAlgorithm("Range").
			SetKeyID(keyID).
			SetContentionFactor(0).
			SetRangeOptions(options.Range().
				SetMin(bson.RawValue{Type: bson.TypeInt32, Value: bsoncore.AppendInt32(nil, 0)}).
				SetMax(bson.RawValue{Type: bson.TypeInt32, Value: bsoncore.AppendInt32(nil, 1000)}))
		payloadDefaults, err := clientEncryption.Encrypt(context.Background(), testVal, eo)
		assert.Nil(mt, err, "error in Encrypt: %v", err)

		mt.Run("Case 1: uses libmongocrypt defaults", func(mt *mtest.T) {
			trimFactor := int32(6)
			sparsity := int64(2)
			eo := options.Encrypt().
				SetAlgorithm("Range").
				SetKeyID(keyID).
				SetContentionFactor(0).
				SetRangeOptions(options.Range().
					SetMin(bson.RawValue{Type: bson.TypeInt32, Value: bsoncore.AppendInt32(nil, 0)}).
					SetMax(bson.RawValue{Type: bson.TypeInt32, Value: bsoncore.AppendInt32(nil, 1000)}).
					SetTrimFactor(trimFactor).
					SetSparsity(sparsity))
			payload, err := clientEncryption.Encrypt(context.Background(), testVal, eo)
			assert.Nil(mt, err, "error in Encrypt: %v", err)
			assert.Equalf(mt, len(payload.Data), len(payloadDefaults.Data), "the returned payload size is expected to be %d", len(payloadDefaults.Data))
		})

		mt.Run("Case 2: accepts trimFactor 0", func(mt *mtest.T) {
			trimFactor := int32(0)
			eo := options.Encrypt().
				SetAlgorithm("Range").
				SetKeyID(keyID).
				SetContentionFactor(0).
				SetRangeOptions(options.Range().
					SetMin(bson.RawValue{Type: bson.TypeInt32, Value: bsoncore.AppendInt32(nil, 0)}).
					SetMax(bson.RawValue{Type: bson.TypeInt32, Value: bsoncore.AppendInt32(nil, 1000)}).
					SetTrimFactor(trimFactor))
			payload, err := clientEncryption.Encrypt(context.Background(), testVal, eo)
			assert.Nil(mt, err, "error in Encrypt: %v", err)
			assert.Greater(t, len(payload.Data), len(payloadDefaults.Data), "the returned payload size is expected to be greater than %d", len(payloadDefaults.Data))
		})
	})

	mt.RunOpts("24. kms retry tests", noClientOpts, func(mt *mtest.T) {
		kmsTlsTestcase := os.Getenv("KMS_FAILPOINT_SERVER_RUNNING")
		if kmsTlsTestcase == "" {
			mt.Skipf("Skipping test as KMS_FAILPOINT_SERVER_RUNNING is not set")
		}

		mt.Parallel()

		tlsCAFile := os.Getenv("KMS_FAILPOINT_CA_FILE")
		require.NotEqual(mt, tlsCAFile, "", "failed to load CA file")

		clientAndCATlsMap := map[string]interface{}{
			"tlsCAFile": tlsCAFile,
		}
		tlsCfg, err := options.BuildTLSConfig(clientAndCATlsMap)
		require.NoError(mt, err, "BuildTLSConfig error: %v", err)

		setFailPoint := func(failure string, count int) error {
			url := fmt.Sprintf("https://localhost:9003/set_failpoint/%s", failure)
			var payloadBuf bytes.Buffer
			body := map[string]int{"count": count}
			json.NewEncoder(&payloadBuf).Encode(body)
			req, err := http.NewRequest(http.MethodPost, url, &payloadBuf)
			if err != nil {
				return err
			}

			client := &http.Client{
				Transport: &http.Transport{TLSClientConfig: tlsCfg},
			}
			res, err := client.Do(req)
			if err != nil {
				return err
			}
			return res.Body.Close()
		}

		kmsProviders := map[string]map[string]interface{}{
			"aws": {
				"accessKeyId":     awsAccessKeyID,
				"secretAccessKey": awsSecretAccessKey,
			},
			"azure": {
				"tenantId":                 azureTenantID,
				"clientId":                 azureClientID,
				"clientSecret":             azureClientSecret,
				"identityPlatformEndpoint": "127.0.0.1:9003",
			},
			"gcp": {
				"email":      gcpEmail,
				"privateKey": gcpPrivateKey,
				"endpoint":   "127.0.0.1:9003",
			},
		}

		dataKeys := []struct {
			provider  string
			masterKey interface{}
		}{
			{"aws", bson.D{
				{"region", "foo"},
				{"key", "bar"},
				{"endpoint", "127.0.0.1:9003"},
			}},
			{"azure", bson.D{
				{"keyVaultEndpoint", "127.0.0.1:9003"},
				{"keyName", "foo"},
			}},
			{"gcp", bson.D{
				{"projectId", "foo"},
				{"location", "bar"},
				{"keyRing", "baz"},
				{"keyName", "qux"},
				{"endpoint", "127.0.0.1:9003"},
			}},
		}

		testCases := []struct {
			name    string
			failure string
		}{
			{"Case 1: createDataKey and encrypt with TCP retry", "network"},
			{"Case 2: createDataKey and encrypt with HTTP retry", "http"},
		}

		for _, tc := range testCases {
			for _, dataKey := range dataKeys {
				mt.Run(fmt.Sprintf("%s_%s", tc.name, dataKey.provider), func(mt *mtest.T) {
					keyVaultClient, err := mongo.Connect(options.Client().ApplyURI(mtest.ClusterURI()))
					require.NoError(mt, err, "error on Connect: %v", err)

					ceo := options.ClientEncryption().
						SetKeyVaultNamespace(kvNamespace).
						SetKmsProviders(kmsProviders).
						SetTLSConfig(map[string]*tls.Config{dataKey.provider: tlsCfg})
					clientEncryption, err := mongo.NewClientEncryption(keyVaultClient, ceo)
					require.NoError(mt, err, "error on NewClientEncryption: %v", err)

					err = setFailPoint(tc.failure, 1)
					require.NoError(mt, err, "mock server error: %v", err)

					dkOpts := options.DataKey().SetMasterKey(dataKey.masterKey)
					var keyID bson.Binary
					keyID, err = clientEncryption.CreateDataKey(context.Background(), dataKey.provider, dkOpts)
					require.NoError(mt, err, "error in CreateDataKey: %v", err)

					err = setFailPoint(tc.failure, 1)
					require.NoError(mt, err, "mock server error: %v", err)

					testVal := bson.RawValue{Type: bson.TypeInt32, Value: bsoncore.AppendInt32(nil, 123)}
					eo := options.Encrypt().
						SetKeyID(keyID).
						SetAlgorithm("AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic")
					_, err = clientEncryption.Encrypt(context.Background(), testVal, eo)
					require.NoError(mt, err, "error in Encrypt: %v", err)
				})
			}
		}

		for _, dataKey := range dataKeys {
			mt.Run(fmt.Sprintf("Case 3: createDataKey fails after too many retries_%s", dataKey.provider), func(mt *mtest.T) {
				keyVaultClient, err := mongo.Connect(options.Client().ApplyURI(mtest.ClusterURI()))
				require.NoError(mt, err, "error on Connect: %v", err)

				ceo := options.ClientEncryption().
					SetKeyVaultNamespace(kvNamespace).
					SetKmsProviders(kmsProviders).
					SetTLSConfig(map[string]*tls.Config{dataKey.provider: tlsCfg})
				clientEncryption, err := mongo.NewClientEncryption(keyVaultClient, ceo)
				require.NoError(mt, err, "error on NewClientEncryption: %v", err)

				err = setFailPoint("network", 4)
				require.NoError(mt, err, "mock server error: %v", err)

				dkOpts := options.DataKey().SetMasterKey(dataKey.masterKey)
				_, err = clientEncryption.CreateDataKey(context.Background(), dataKey.provider, dkOpts)
				require.ErrorContains(mt, err, "KMS request failed after 3 retries due to a network error")
			})
		}
	})
}

func getWatcher(mt *mtest.T, streamType mongo.StreamType, cpt *cseProseTest) watcher {
	mt.Helper()

	switch streamType {
	case mongo.ClientStream:
		return cpt.cseClient
	case mongo.DatabaseStream:
		return cpt.cseColl.Database()
	case mongo.CollectionStream:
		return cpt.cseColl
	default:
		mt.Fatalf("unknown stream type %v", streamType)
	}
	return nil
}

type cseProseTest struct {
	coll         *mongo.Collection // collection db.coll
	kvClient     *mongo.Client
	keyVaultColl *mongo.Collection
	cseClient    *mongo.Client     // encrypted client
	cseColl      *mongo.Collection // db.coll with encrypted client
	clientEnc    *mongo.ClientEncryption
	cseStarted   []*event.CommandStartedEvent
}

func setup(mt *mtest.T, aeo *options.AutoEncryptionOptions, kvClientOpts *options.ClientOptions,
	ceo options.Lister[options.ClientEncryptionOptions]) *cseProseTest {
	mt.Helper()
	var cpt cseProseTest
	var err error
	cpt.coll = mt.CreateCollection(mtest.Collection{
		Name: "coll",
		DB:   "db",
		Opts: options.Collection().SetWriteConcern(mtest.MajorityWc),
	}, false)
	cpt.keyVaultColl = mt.CreateCollection(mtest.Collection{
		Name: "datakeys",
		DB:   "keyvault",
		Opts: options.Collection().SetWriteConcern(mtest.MajorityWc),
	}, false)

	if aeo != nil {
		cseMonitor := &event.CommandMonitor{
			Started: func(_ context.Context, evt *event.CommandStartedEvent) {
				cpt.cseStarted = append(cpt.cseStarted, evt)
			},
		}
		opts := options.Client().ApplyURI(mtest.ClusterURI()).SetWriteConcern(mtest.MajorityWc).
			SetReadPreference(mtest.PrimaryRp).SetAutoEncryptionOptions(aeo).SetMonitor(cseMonitor)
		integtest.AddTestServerAPIVersion(opts)
		cpt.cseClient, err = mongo.Connect(opts)
		assert.Nil(mt, err, "Connect error for encrypted client: %v", err)
		cpt.cseColl = cpt.cseClient.Database("db").Collection("coll")
	}
	if ceo != nil {
		cpt.kvClient, err = mongo.Connect(kvClientOpts)
		assert.Nil(mt, err, "Connect error for ClientEncryption key vault client: %v", err)
		cpt.clientEnc, err = mongo.NewClientEncryption(cpt.kvClient, ceo)
		assert.Nil(mt, err, "NewClientEncryption error: %v", err)
	}
	return &cpt
}

func (cpt *cseProseTest) teardown(mt *mtest.T) {
	mt.Helper()

	if cpt.cseClient != nil {
		_ = cpt.cseClient.Disconnect(context.Background())
	}
	if cpt.clientEnc != nil {
		_ = cpt.clientEnc.Close(context.Background())
	}
}

func readJSONFile(mt *mtest.T, file string) bson.Raw {
	mt.Helper()

	content, err := ioutil.ReadFile(filepath.Join(clientEncryptionProseDir, file))
	assert.Nil(mt, err, "ReadFile error for %v: %v", file, err)

	var doc bson.Raw
	err = bson.UnmarshalExtJSON(content, true, &doc)
	assert.Nil(mt, err, "UnmarshalExtJSON error for file %v: %v", file, err)
	return doc
}

func decodeJSONFile(mt *mtest.T, file string, val interface{}) bson.Raw {
	mt.Helper()

	content, err := ioutil.ReadFile(filepath.Join(clientEncryptionProseDir, file))
	assert.Nil(mt, err, "ReadFile error for %v: %v", file, err)

	var doc bson.Raw
	err = bson.UnmarshalExtJSON(content, true, val)
	assert.Nil(mt, err, "UnmarshalExtJSON error for file %v: %v", file, err)
	return doc
}

func rawValueToCoreValue(rv bson.RawValue) bsoncore.Value {
	return bsoncore.Value{Type: bsoncore.Type(rv.Type), Data: rv.Value}
}

type deadlockTest struct {
	clientTest           *mongo.Client
	clientKeyVaultOpts   *options.ClientOptions
	clientKeyVaultEvents []startedEvent
	clientEncryption     *mongo.ClientEncryption
	ciphertext           bson.Binary
}

type startedEvent struct {
	Command  string
	Database string
}

func newDeadlockTest(mt *mtest.T) *deadlockTest {
	mt.Helper()

	var d deadlockTest
	var err error

	clientTestOpts := options.Client().ApplyURI(mtest.ClusterURI()).SetWriteConcern(mtest.MajorityWc)
	integtest.AddTestServerAPIVersion(clientTestOpts)
	if d.clientTest, err = mongo.Connect(clientTestOpts); err != nil {
		mt.Fatalf("Connect error: %v", err)
	}

	clientKeyVaultMonitor := &event.CommandMonitor{
		Started: func(ctx context.Context, event *event.CommandStartedEvent) {
			startedEvent := startedEvent{event.CommandName, event.DatabaseName}
			d.clientKeyVaultEvents = append(d.clientKeyVaultEvents, startedEvent)
		},
	}

	d.clientKeyVaultOpts = options.Client().ApplyURI(mtest.ClusterURI()).
		SetMaxPoolSize(1).SetMonitor(clientKeyVaultMonitor)

	keyvaultColl := d.clientTest.Database("keyvault").Collection("datakeys")
	dataColl := d.clientTest.Database("db").Collection("coll")
	err = dataColl.Drop(context.Background())
	assert.Nil(mt, err, "Drop error for collection db.coll: %v", err)

	err = keyvaultColl.Drop(context.Background())
	assert.Nil(mt, err, "Drop error for key vault collection: %v", err)

	keyDoc := readJSONFile(mt, "external-key.json")
	_, err = keyvaultColl.InsertOne(context.Background(), keyDoc)
	assert.Nil(mt, err, "InsertOne error into key vault collection: %v", err)

	schemaDoc := readJSONFile(mt, "external-schema.json")
	createOpts := options.CreateCollection().SetValidator(bson.M{"$jsonSchema": schemaDoc})
	err = d.clientTest.Database("db").CreateCollection(context.Background(), "coll", createOpts)
	assert.Nil(mt, err, "CreateCollection error: %v", err)

	kmsProviders := map[string]map[string]interface{}{
		"local": {"key": localMasterKey},
	}
	ceOpts := options.ClientEncryption().SetKmsProviders(kmsProviders).SetKeyVaultNamespace("keyvault.datakeys")
	d.clientEncryption, err = mongo.NewClientEncryption(d.clientTest, ceOpts)
	assert.Nil(mt, err, "NewClientEncryption error: %v", err)

	t, value, err := bson.MarshalValue("string0")
	assert.Nil(mt, err, "MarshalValue error: %v", err)
	in := bson.RawValue{Type: t, Value: value}
	eopts := options.Encrypt().SetAlgorithm("AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic").SetKeyAltName("local")
	d.ciphertext, err = d.clientEncryption.Encrypt(context.Background(), in, eopts)
	assert.Nil(mt, err, "Encrypt error: %v", err)

	return &d
}

func (d *deadlockTest) disconnect(mt *mtest.T) {
	mt.Helper()
	err := d.clientEncryption.Close(context.Background())
	assert.Nil(mt, err, "clientEncryption Close error: %v", err)
	d.clientTest.Disconnect(context.Background())
	assert.Nil(mt, err, "clientTest Disconnect error: %v", err)
}

// listenForConnections creates a listener that will listen for connections.
// The user provided run function will be called with the accepted
// connection. The user is responsible for calling Close on the returned listener.
func listenForConnections(t *testing.T, run func(net.Conn)) net.Listener {
	t.Helper()

	l, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Errorf("Could not set up a listener: %v", err)
		t.FailNow()
	}
	go func() {
		for {
			// Ignore errors due to closing the listener connection.
			c, _ := l.Accept()
			if c == nil {
				break
			}
			go run(c)
		}
	}()
	return l
}

type localTransport struct {
	rt http.RoundTripper

	header http.Header
}

func (t *localTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	r := req.Clone(req.Context())
	r.URL.Host = "localhost:8080"
	for key, vals := range t.header {
		for _, val := range vals {
			r.Header.Add(key, val)
		}
	}
	return t.rt.RoundTrip(r)
}
