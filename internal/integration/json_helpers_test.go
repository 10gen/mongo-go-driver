// Copyright (C) MongoDB, Inc. 2017-present.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

package integration

import (
	"crypto/tls"
	"errors"
	"fmt"
	"io/ioutil"
	"math"
	"os"
	"path"
	"strings"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/internal/assert"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readconcern"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"
	"go.mongodb.org/mongo-driver/v2/mongo/writeconcern"
)

var (
	awsAccessKeyID                  = os.Getenv("FLE_AWS_KEY")
	awsSecretAccessKey              = os.Getenv("FLE_AWS_SECRET")
	awsTempAccessKeyID              = os.Getenv("CSFLE_AWS_TEMP_ACCESS_KEY_ID")
	awsTempSecretAccessKey          = os.Getenv("CSFLE_AWS_TEMP_SECRET_ACCESS_KEY")
	awsTempSessionToken             = os.Getenv("CSFLE_AWS_TEMP_SESSION_TOKEN")
	azureTenantID                   = os.Getenv("FLE_AZURE_TENANTID")
	azureClientID                   = os.Getenv("FLE_AZURE_CLIENTID")
	azureClientSecret               = os.Getenv("FLE_AZURE_CLIENTSECRET")
	gcpEmail                        = os.Getenv("FLE_GCP_EMAIL")
	gcpPrivateKey                   = os.Getenv("FLE_GCP_PRIVATEKEY")
	tlsCAFileKMIP                   = os.Getenv("CSFLE_TLS_CA_FILE")
	tlsClientCertificateKeyFileKMIP = os.Getenv("CSFLE_TLS_CLIENT_CERT_FILE")
)

// Helper functions to do read JSON spec test files and convert JSON objects into the appropriate driver types.
// Functions in this file should take testing.TB rather than testing.T/mtest.T for generality because they
// do not do any database communication.

// generate a slice of all JSON file names in a directory
func jsonFilesInDir(t testing.TB, dir string) []string {
	t.Helper()

	files := make([]string, 0)

	entries, err := ioutil.ReadDir(dir)
	assert.Nil(t, err, "unable to read json file: %v", err)

	for _, entry := range entries {
		if entry.IsDir() || path.Ext(entry.Name()) != ".json" {
			continue
		}

		files = append(files, entry.Name())
	}

	return files
}

// create client options from a map
func createClientOptions(t testing.TB, opts bson.Raw) *options.ClientOptionsBuilder {
	t.Helper()

	clientOpts := options.Client()
	elems, _ := opts.Elements()
	for _, elem := range elems {
		name := elem.Key()
		opt := elem.Value()

		switch name {
		case "retryWrites":
			clientOpts.SetRetryWrites(opt.Boolean())
		case "w":
			switch opt.Type {
			case bson.TypeInt32:
				w := int(opt.Int32())
				clientOpts.SetWriteConcern(&writeconcern.WriteConcern{W: w})
			case bson.TypeDouble:
				w := int(opt.Double())
				clientOpts.SetWriteConcern(&writeconcern.WriteConcern{W: w})
			case bson.TypeString:
				clientOpts.SetWriteConcern(writeconcern.Majority())
			default:
				t.Fatalf("unrecognized type for w client option: %v", opt.Type)
			}
		case "readConcernLevel":
			clientOpts.SetReadConcern(&readconcern.ReadConcern{Level: opt.StringValue()})
		case "readPreference":
			clientOpts.SetReadPreference(readPrefFromString(opt.StringValue()))
		case "heartbeatFrequencyMS":
			hf := convertValueToMilliseconds(t, opt)
			clientOpts.SetHeartbeatInterval(hf)
		case "retryReads":
			clientOpts.SetRetryReads(opt.Boolean())
		case "autoEncryptOpts":
			clientOpts.SetAutoEncryptionOptions(createAutoEncryptionOptions(t, opt.Document()))
		case "appname":
			clientOpts.SetAppName(opt.StringValue())
		case "connectTimeoutMS":
			ct := convertValueToMilliseconds(t, opt)
			clientOpts.SetConnectTimeout(ct)
		case "serverSelectionTimeoutMS":
			sst := convertValueToMilliseconds(t, opt)
			clientOpts.SetServerSelectionTimeout(sst)
		case "minPoolSize":
			clientOpts.SetMinPoolSize(uint64(opt.AsInt64()))
		case "maxPoolSize":
			clientOpts.SetMaxPoolSize(uint64(opt.AsInt64()))
		case "directConnection":
			clientOpts.SetDirect(opt.Boolean())
		default:
			t.Fatalf("unrecognized client option: %v", name)
		}
	}

	return clientOpts
}

func createAutoEncryptionOptions(t testing.TB, opts bson.Raw) *options.AutoEncryptionOptionsBuilder {
	t.Helper()

	aeo := options.AutoEncryption()
	var kvnsFound bool
	elems, _ := opts.Elements()

	for _, elem := range elems {
		name := elem.Key()
		opt := elem.Value()

		switch name {
		case "kmsProviders":
			tlsConfigs := createTLSOptsMap(t, opt.Document())
			aeo.SetKmsProviders(createKmsProvidersMap(t, opt.Document())).SetTLSConfig(tlsConfigs)
		case "schemaMap":
			var schemaMap map[string]interface{}
			err := bson.Unmarshal(opt.Document(), &schemaMap)
			if err != nil {
				t.Fatalf("error creating schema map: %v", err)
			}

			aeo.SetSchemaMap(schemaMap)
		case "keyVaultNamespace":
			kvnsFound = true
			aeo.SetKeyVaultNamespace(opt.StringValue())
		case "bypassAutoEncryption":
			aeo.SetBypassAutoEncryption(opt.Boolean())
		case "encryptedFieldsMap":
			var encryptedFieldsMap map[string]interface{}
			err := bson.Unmarshal(opt.Document(), &encryptedFieldsMap)
			if err != nil {
				t.Fatalf("error creating encryptedFieldsMap: %v", err)
			}
			aeo.SetEncryptedFieldsMap(encryptedFieldsMap)
		case "bypassQueryAnalysis":
			aeo.SetBypassQueryAnalysis(opt.Boolean())
		default:
			t.Fatalf("unrecognized auto encryption option: %v", name)
		}
	}
	if !kvnsFound {
		aeo.SetKeyVaultNamespace("keyvault.datakeys")
	}

	return aeo
}

func createTLSOptsMap(t testing.TB, opts bson.Raw) map[string]*tls.Config {
	t.Helper()

	tlsMap := make(map[string]*tls.Config)
	elems, _ := opts.Elements()

	for _, elem := range elems {
		provider := elem.Key()

		if provider == "kmip" {
			tlsOptsMap := map[string]interface{}{
				"tlsCertificateKeyFile": tlsClientCertificateKeyFileKMIP,
				"tlsCAFile":             tlsCAFileKMIP,
			}

			cfg, err := options.BuildTLSConfig(tlsOptsMap)
			if err != nil {
				t.Fatalf("error building TLS config map: %v", err)
			}

			tlsMap["kmip"] = cfg
		}
	}
	return tlsMap
}

func createKmsProvidersMap(t testing.TB, opts bson.Raw) map[string]map[string]interface{} {
	t.Helper()

	kmsMap := make(map[string]map[string]interface{})
	elems, _ := opts.Elements()

	for _, elem := range elems {
		provider := elem.Key()
		providerOpt := elem.Value()

		switch provider {
		case "aws":
			awsMap := map[string]interface{}{
				"accessKeyId":     awsAccessKeyID,
				"secretAccessKey": awsSecretAccessKey,
			}
			kmsMap["aws"] = awsMap
		case "azure":
			kmsMap["azure"] = map[string]interface{}{
				"tenantId":     azureTenantID,
				"clientId":     azureClientID,
				"clientSecret": azureClientSecret,
			}
		case "gcp":
			kmsMap["gcp"] = map[string]interface{}{
				"email":      gcpEmail,
				"privateKey": gcpPrivateKey,
			}
		case "local":
			_, key := providerOpt.Document().Lookup("key").Binary()
			localMap := map[string]interface{}{
				"key": key,
			}
			kmsMap["local"] = localMap
		case "awsTemporary":
			if awsTempAccessKeyID == "" {
				t.Fatal("AWS temp access key ID not set")
			}
			if awsTempSecretAccessKey == "" {
				t.Fatal("AWS temp secret access key not set")
			}
			if awsTempSessionToken == "" {
				t.Fatal("AWS temp session token not set")
			}
			awsMap := map[string]interface{}{
				"accessKeyId":     awsTempAccessKeyID,
				"secretAccessKey": awsTempSecretAccessKey,
				"sessionToken":    awsTempSessionToken,
			}
			kmsMap["aws"] = awsMap
		case "awsTemporaryNoSessionToken":
			if awsTempAccessKeyID == "" {
				t.Fatal("AWS temp access key ID not set")
			}
			if awsTempSecretAccessKey == "" {
				t.Fatal("AWS temp secret access key not set")
			}
			awsMap := map[string]interface{}{
				"accessKeyId":     awsTempAccessKeyID,
				"secretAccessKey": awsTempSecretAccessKey,
			}
			kmsMap["aws"] = awsMap
		case "kmip":
			kmipMap := map[string]interface{}{
				"endpoint": "localhost:5698",
			}
			kmsMap["kmip"] = kmipMap
		default:
			t.Fatalf("unrecognized KMS provider: %v", provider)
		}
	}

	return kmsMap
}

// create session options from a map
func createSessionOptions(t testing.TB, opts bson.Raw) *options.SessionOptionsBuilder {
	t.Helper()

	sessOpts := options.Session()
	elems, _ := opts.Elements()
	for _, elem := range elems {
		name := elem.Key()
		opt := elem.Value()

		switch name {
		case "causalConsistency":
			sessOpts = sessOpts.SetCausalConsistency(opt.Boolean())
		case "defaultTransactionOptions":
			sessOpts.SetDefaultTransactionOptions(createTransactionOptions(t, opt.Document()))
		default:
			t.Fatalf("unrecognized session option: %v", name)
		}
	}

	return sessOpts
}

// create database options from a BSON document.
func createDatabaseOptions(t testing.TB, opts bson.Raw) *options.DatabaseOptionsBuilder {
	t.Helper()

	do := options.Database()
	elems, _ := opts.Elements()
	for _, elem := range elems {
		name := elem.Key()
		opt := elem.Value()

		switch name {
		case "readConcern":
			do.SetReadConcern(createReadConcern(opt))
		case "writeConcern":
			do.SetWriteConcern(createWriteConcern(t, opt))
		default:
			t.Fatalf("unrecognized database option: %v", name)
		}
	}

	return do
}

// create collection options from a map
func createCollectionOptions(t testing.TB, opts bson.Raw) *options.CollectionOptionsBuilder {
	t.Helper()

	co := options.Collection()
	elems, _ := opts.Elements()
	for _, elem := range elems {
		name := elem.Key()
		opt := elem.Value()

		switch name {
		case "readConcern":
			co.SetReadConcern(createReadConcern(opt))
		case "writeConcern":
			co.SetWriteConcern(createWriteConcern(t, opt))
		case "readPreference":
			co.SetReadPreference(createReadPref(opt))
		default:
			t.Fatalf("unrecognized collection option: %v", name)
		}
	}

	return co
}

// create transaction options from a map
func createTransactionOptions(t testing.TB, opts bson.Raw) *options.TransactionOptionsBuilder {
	t.Helper()

	txnOpts := options.Transaction()
	elems, _ := opts.Elements()
	for _, elem := range elems {
		name := elem.Key()
		opt := elem.Value()

		switch name {
		case "writeConcern":
			txnOpts.SetWriteConcern(createWriteConcern(t, opt))
		case "readPreference":
			txnOpts.SetReadPreference(createReadPref(opt))
		case "readConcern":
			txnOpts.SetReadConcern(createReadConcern(opt))
		case "maxCommitTimeMS":
			t.Skip("GODRIVER-2348: maxCommitTimeMS is deprecated")
		default:
			t.Fatalf("unrecognized transaction option: %v", opt)
		}
	}
	return txnOpts
}

// create a read concern from a map
func createReadConcern(opt bson.RawValue) *readconcern.ReadConcern {
	return &readconcern.ReadConcern{Level: opt.Document().Lookup("level").StringValue()}
}

// create a read concern from a map
func createWriteConcern(t testing.TB, opt bson.RawValue) *writeconcern.WriteConcern {
	wcDoc, ok := opt.DocumentOK()
	if !ok {
		return nil
	}

	wc := &writeconcern.WriteConcern{}
	elems, _ := wcDoc.Elements()
	for _, elem := range elems {
		key := elem.Key()
		val := elem.Value()

		switch key {
		case "j":
			j := val.Boolean()
			wc.Journal = &j
		case "w":
			switch val.Type {
			case bson.TypeString:
				if val.StringValue() != "majority" {
					break
				}
				wc.W = "majority"
			case bson.TypeInt32:
				w := int(val.Int32())
				wc.W = w
			default:
				t.Fatalf("unrecognized type for w: %v", val.Type)
			}
		default:
			t.Fatalf("unrecognized write concern option: %v", key)
		}
	}
	return wc
}

// create a read preference from a string.
// returns readpref.Primary() if the string doesn't match any known read preference modes.
func readPrefFromString(s string) *readpref.ReadPref {
	switch strings.ToLower(s) {
	case "primary":
		return readpref.Primary()
	case "primarypreferred":
		return &readpref.ReadPref{Mode: readpref.PrimaryPreferredMode}
	case "secondary":
		return &readpref.ReadPref{Mode: readpref.SecondaryMode}
	case "secondarypreferred":
		return &readpref.ReadPref{Mode: readpref.SecondaryPreferredMode}
	case "nearest":
		return &readpref.ReadPref{Mode: readpref.NearestMode}
	}
	return readpref.Primary()
}

// create a read preference from a map.
func createReadPref(opt bson.RawValue) *readpref.ReadPref {
	mode := opt.Document().Lookup("mode").StringValue()
	return readPrefFromString(mode)
}

// retrieve the error associated with a result.
func errorFromResult(t testing.TB, result interface{}) *operationError {
	t.Helper()

	// embedded doc will be unmarshalled as Raw
	raw, ok := result.(bson.Raw)
	if !ok {
		return nil
	}

	var expected operationError
	err := bson.Unmarshal(raw, &expected)
	if err != nil {
		return nil
	}
	if expected.ErrorCodeName == nil && expected.ErrorContains == nil && len(expected.ErrorLabelsOmit) == 0 &&
		len(expected.ErrorLabelsContain) == 0 {
		return nil
	}

	return &expected
}

// errorDetails is a helper type that holds information that can be returned by driver functions in different error
// types.
type errorDetails struct {
	name   string
	labels []string
}

// extractErrorDetails creates an errorDetails instance based on the provided error. It returns the details and an "ok"
// value which is true if the provided error is of a known type that can be processed.
func extractErrorDetails(err error) (errorDetails, bool) {
	var details errorDetails

	switch converted := err.(type) {
	case mongo.CommandError:
		details.name = converted.Name
		details.labels = converted.Labels
	case mongo.WriteException:
		if converted.WriteConcernError != nil {
			details.name = converted.WriteConcernError.Name
		}
		details.labels = converted.Labels
	case mongo.BulkWriteException:
		if converted.WriteConcernError != nil {
			details.name = converted.WriteConcernError.Name
		}
		details.labels = converted.Labels
	default:
		return errorDetails{}, false
	}

	return details, true
}

// verify that an error returned by an operation matches the expected error.
func verifyError(expected *operationError, actual error) error {
	// The spec test format doesn't treat ErrNoDocuments as an errors, so set actual to nil
	// to indicate that no error occurred.
	if errors.Is(actual, mongo.ErrNoDocuments) {
		actual = nil
	}

	if expected == nil && actual != nil {
		return fmt.Errorf("did not expect error but got %w", actual)
	}
	if expected != nil && actual == nil {
		return fmt.Errorf("expected error but got nil")
	}
	if expected == nil {
		return nil
	}

	// check ErrorContains for all error types
	if expected.ErrorContains != nil {
		emsg := strings.ToLower(*expected.ErrorContains)
		amsg := strings.ToLower(actual.Error())
		if !strings.Contains(amsg, emsg) {
			return fmt.Errorf("expected error message %q to contain %q", amsg, emsg)
		}
	}

	// Get an errorDetails instance for the error. If this fails but the test has expectations about the error name or
	// labels, fail because we can't verify them.
	details, ok := extractErrorDetails(actual)
	if !ok {
		if expected.ErrorCodeName != nil || len(expected.ErrorLabelsContain) > 0 || len(expected.ErrorLabelsOmit) > 0 {
			return fmt.Errorf("failed to extract details from error %v of type %T", actual, actual)
		}
		return nil
	}

	if expected.ErrorCodeName != nil {
		if *expected.ErrorCodeName != details.name {
			return fmt.Errorf("expected error name %v, got %v", *expected.ErrorCodeName, details.name)
		}
	}
	for _, label := range expected.ErrorLabelsContain {
		if !stringSliceContains(details.labels, label) {
			return fmt.Errorf("expected error %w to contain label %q", actual, label)
		}
	}
	for _, label := range expected.ErrorLabelsOmit {
		if stringSliceContains(details.labels, label) {
			return fmt.Errorf("expected error %w to not contain label %q", actual, label)
		}
	}
	return nil
}

// get the underlying value of i as an int64. returns nil if i is not an int, int32, or int64 type.
func getIntFromInterface(i interface{}) *int64 {
	var out int64

	switch v := i.(type) {
	case int:
		out = int64(v)
	case int32:
		out = int64(v)
	case int64:
		out = v
	case float32:
		f := float64(v)
		if math.Floor(f) != f || f > float64(math.MaxInt64) {
			break
		}

		out = int64(f)
	case float64:
		if math.Floor(v) != v || v > float64(math.MaxInt64) {
			break
		}

		out = int64(v)
	default:
		return nil
	}

	return &out
}

func createCollation(t testing.TB, m bson.Raw) *options.Collation {
	var collation options.Collation
	elems, _ := m.Elements()

	for _, elem := range elems {
		switch elem.Key() {
		case "locale":
			collation.Locale = elem.Value().StringValue()
		case "caseLevel":
			collation.CaseLevel = elem.Value().Boolean()
		case "caseFirst":
			collation.CaseFirst = elem.Value().StringValue()
		case "strength":
			collation.Strength = int(elem.Value().Int32())
		case "numericOrdering":
			collation.NumericOrdering = elem.Value().Boolean()
		case "alternate":
			collation.Alternate = elem.Value().StringValue()
		case "maxVariable":
			collation.MaxVariable = elem.Value().StringValue()
		case "normalization":
			collation.Normalization = elem.Value().Boolean()
		case "backwards":
			collation.Backwards = elem.Value().Boolean()
		default:
			t.Fatalf("unrecognized collation option: %v", elem.Key())
		}
	}
	return &collation
}

func convertValueToMilliseconds(t testing.TB, val bson.RawValue) time.Duration {
	t.Helper()

	int32Val, ok := val.Int32OK()
	if !ok {
		t.Fatalf("failed to convert value of type %s to int32", val.Type)
	}
	return time.Duration(int32Val) * time.Millisecond
}

func stringSliceContains(stringSlice []string, target string) bool {
	for _, str := range stringSlice {
		if str == target {
			return true
		}
	}
	return false
}
