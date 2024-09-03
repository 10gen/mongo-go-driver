// Copyright (C) MongoDB, Inc. 2022-present.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

package options

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/event"
	"go.mongodb.org/mongo-driver/v2/internal/assert"
	"go.mongodb.org/mongo-driver/v2/internal/httputil"
	"go.mongodb.org/mongo-driver/v2/internal/ptrutil"
	"go.mongodb.org/mongo-driver/v2/mongo/readconcern"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"
	"go.mongodb.org/mongo-driver/v2/mongo/writeconcern"
	"go.mongodb.org/mongo-driver/v2/x/mongo/driver/connstring"
)

var tClientOptions = reflect.TypeOf(&ClientOptionsBuilder{})
var tClientopts = reflect.TypeOf(&ClientOptions{})

func TestClientOptions(t *testing.T) {
	t.Run("ApplyURI/doesn't overwrite previous errors", func(t *testing.T) {
		uri := "not-mongo-db-uri://"
		want := fmt.Errorf(
			"error parsing uri: %w",
			errors.New(`scheme must be "mongodb" or "mongodb+srv"`))
		co := Client().ApplyURI(uri).ApplyURI("mongodb://localhost/")
		got := co.Validate()
		if !cmp.Equal(got, want, cmp.Comparer(compareErrors)) {
			t.Errorf("Did not received expected error. got %v; want %v", got, want)
		}
	})
	t.Run("Set", func(t *testing.T) {
		testCases := []struct {
			name        string
			fn          interface{} // method to be run
			arg         interface{} // argument for method
			field       string      // field to be set
			dereference bool        // Should we compare a pointer or the field
		}{
			{"AppName", (*ClientOptionsBuilder).SetAppName, "example-application", "AppName", true},
			{"Auth", (*ClientOptionsBuilder).SetAuth, Credential{Username: "foo", Password: "bar"}, "Auth", true},
			{"Compressors", (*ClientOptionsBuilder).SetCompressors, []string{"zstd", "snappy", "zlib"}, "Compressors", true},
			{"ConnectTimeout", (*ClientOptionsBuilder).SetConnectTimeout, 5 * time.Second, "ConnectTimeout", true},
			{"Dialer", (*ClientOptionsBuilder).SetDialer, testDialer{Num: 12345}, "Dialer", true},
			{"HeartbeatInterval", (*ClientOptionsBuilder).SetHeartbeatInterval, 5 * time.Second, "HeartbeatInterval", true},
			{"Hosts", (*ClientOptionsBuilder).SetHosts, []string{"localhost:27017", "localhost:27018", "localhost:27019"}, "Hosts", true},
			{"LocalThreshold", (*ClientOptionsBuilder).SetLocalThreshold, 5 * time.Second, "LocalThreshold", true},
			{"MaxConnIdleTime", (*ClientOptionsBuilder).SetMaxConnIdleTime, 5 * time.Second, "MaxConnIdleTime", true},
			{"MaxPoolSize", (*ClientOptionsBuilder).SetMaxPoolSize, uint64(250), "MaxPoolSize", true},
			{"MinPoolSize", (*ClientOptionsBuilder).SetMinPoolSize, uint64(10), "MinPoolSize", true},
			{"MaxConnecting", (*ClientOptionsBuilder).SetMaxConnecting, uint64(10), "MaxConnecting", true},
			{"PoolMonitor", (*ClientOptionsBuilder).SetPoolMonitor, &event.PoolMonitor{}, "PoolMonitor", false},
			{"Monitor", (*ClientOptionsBuilder).SetMonitor, &event.CommandMonitor{}, "Monitor", false},
			{"ReadConcern", (*ClientOptionsBuilder).SetReadConcern, readconcern.Majority(), "ReadConcern", false},
			{"ReadPreference", (*ClientOptionsBuilder).SetReadPreference, readpref.SecondaryPreferred(), "ReadPreference", false},
			{"Registry", (*ClientOptionsBuilder).SetRegistry, bson.NewRegistry(), "Registry", false},
			{"ReplicaSet", (*ClientOptionsBuilder).SetReplicaSet, "example-replicaset", "ReplicaSet", true},
			{"RetryWrites", (*ClientOptionsBuilder).SetRetryWrites, true, "RetryWrites", true},
			{"ServerSelectionTimeout", (*ClientOptionsBuilder).SetServerSelectionTimeout, 5 * time.Second, "ServerSelectionTimeout", true},
			{"Direct", (*ClientOptionsBuilder).SetDirect, true, "Direct", true},
			{"TLSConfig", (*ClientOptionsBuilder).SetTLSConfig, &tls.Config{}, "TLSConfig", false},
			{"WriteConcern", (*ClientOptionsBuilder).SetWriteConcern, writeconcern.Majority(), "WriteConcern", false},
			{"ZlibLevel", (*ClientOptionsBuilder).SetZlibLevel, 6, "ZlibLevel", true},
			{"DisableOCSPEndpointCheck", (*ClientOptionsBuilder).SetDisableOCSPEndpointCheck, true, "DisableOCSPEndpointCheck", true},
			{"LoadBalanced", (*ClientOptionsBuilder).SetLoadBalanced, true, "LoadBalanced", true},
		}

		opt1, opt2, optResult := Client(), Client(), Client()
		for idx, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				fn := reflect.ValueOf(tc.fn)
				if fn.Kind() != reflect.Func {
					t.Fatal("fn argument must be a function")
				}
				if fn.Type().NumIn() < 2 || fn.Type().In(0) != tClientOptions {
					t.Fatal("fn argument must have a *ClientOptions as the first argument and one other argument")
				}
				if _, exists := tClientopts.Elem().FieldByName(tc.field); !exists {
					t.Fatalf("field (%s) does not exist in ClientOptions", tc.field)
				}
				opts := make([]reflect.Value, 2)
				opts[0] = reflect.New(tClientOptions.Elem())
				want := reflect.ValueOf(tc.arg)
				opts[1] = want

				if !want.IsValid() || !want.CanInterface() {
					t.Fatal("arg property of test case must be valid")
				}

				_ = fn.Call(opts)

				// To avoid duplication we're piggybacking on the Set* tests to make the
				// MergeClientOptions test simpler and more thorough.
				// To do this we set the odd numbered test cases to the first opt, the even and
				// divisible by three test cases to the second, and the result of merging the two to
				// the result option. This gives us coverage of options set by the first option, by
				// the second, and by both.
				if idx%2 != 0 {
					opts[0] = reflect.ValueOf(opt1)
					_ = fn.Call(opts)
				}
				if idx%2 == 0 || idx%3 == 0 {
					opts[0] = reflect.ValueOf(opt2)
					_ = fn.Call(opts)
				}
				opts[0] = reflect.ValueOf(optResult)
				_ = fn.Call(opts)

				optsValue := opts[0].Elem().FieldByName("Opts")

				// Ensure the value is a slice
				if optsValue.Kind() != reflect.Slice {
					t.Fatalf("expected the options to be a slice")
				}

				setters := make([]func(*ClientOptions) error, optsValue.Len())

				// Iterate over the reflect.Value and extract each function
				for i := 0; i < optsValue.Len(); i++ {
					elem := optsValue.Index(i)
					if elem.Kind() != reflect.Func {
						t.Fatalf("expected all elements of opts to be functions")
					}

					setters[i] = elem.Interface().(func(*ClientOptions) error)
				}

				clientopts := &ClientOptions{}
				for _, set := range setters {
					err := set(clientopts)
					assert.NoError(t, err)
				}

				got := reflect.ValueOf(clientopts).Elem().FieldByName(tc.field)
				if !got.IsValid() || !got.CanInterface() {
					t.Fatal("cannot create concrete instance from retrieved field")
				}

				if got.Kind() == reflect.Ptr && tc.dereference {
					got = got.Elem()
				}

				if !cmp.Equal(
					got.Interface(), want.Interface(),
					cmp.AllowUnexported(readconcern.ReadConcern{}, writeconcern.WriteConcern{}, readpref.ReadPref{}),
					cmp.Comparer(func(r1, r2 *bson.Registry) bool { return r1 == r2 }),
					cmp.Comparer(func(cfg1, cfg2 *tls.Config) bool { return cfg1 == cfg2 }),
					cmp.Comparer(func(fp1, fp2 *event.PoolMonitor) bool { return fp1 == fp2 }),
				) {
					t.Errorf("Field not set properly. got %v; want %v", got.Interface(), want.Interface())
				}
			})
		}
	})
	t.Run("direct connection validation", func(t *testing.T) {
		t.Run("multiple hosts", func(t *testing.T) {
			expectedErr := errors.New("a direct connection cannot be made if multiple hosts are specified")

			testCases := []struct {
				name string
				opts *ClientOptionsBuilder
			}{
				{"hosts in URI", Client().ApplyURI("mongodb://localhost,localhost2")},
				{"hosts in options", Client().SetHosts([]string{"localhost", "localhost2"})},
			}
			for _, tc := range testCases {
				t.Run(tc.name, func(t *testing.T) {
					err := tc.opts.SetDirect(true).Validate()
					assert.NotNil(t, err, "expected error, got nil")
					assert.Equal(t, expectedErr.Error(), err.Error(), "expected error %v, got %v", expectedErr, err)
				})
			}
		})
		t.Run("srv", func(t *testing.T) {
			expectedErr := errors.New("a direct connection cannot be made if an SRV URI is used")
			// Use a non-SRV URI and manually set the scheme because using an SRV URI would force an SRV lookup.
			optsBldr := Client().ApplyURI("mongodb://localhost:27017")

			args, err := getOptions[ClientOptions](optsBldr)
			assert.NoError(t, err)

			args.connString.Scheme = connstring.SchemeMongoDBSRV

			newOpts := &ClientOptionsBuilder{}
			newOpts.Opts = append(newOpts.Opts, func(ca *ClientOptions) error {
				*ca = *args

				return nil
			})

			err = newOpts.SetDirect(true).Validate()
			assert.NotNil(t, err, "expected error, got nil")
			assert.Equal(t, expectedErr.Error(), err.Error(), "expected error %v, got %v", expectedErr, err)
		})
	})
	t.Run("loadBalanced validation", func(t *testing.T) {
		testCases := []struct {
			name string
			opts *ClientOptionsBuilder
			err  error
		}{
			{"multiple hosts in URI", Client().ApplyURI("mongodb://foo,bar"), connstring.ErrLoadBalancedWithMultipleHosts},
			{"multiple hosts in options", Client().SetHosts([]string{"foo", "bar"}), connstring.ErrLoadBalancedWithMultipleHosts},
			{"replica set name", Client().SetReplicaSet("foo"), connstring.ErrLoadBalancedWithReplicaSet},
			{"directConnection=true", Client().SetDirect(true), connstring.ErrLoadBalancedWithDirectConnection},
		}
		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				// The loadBalanced option should not be validated if it is unset or false.
				err := tc.opts.Validate()
				assert.Nil(t, err, "Validate error when loadBalanced is unset: %v", err)

				tc.opts.SetLoadBalanced(false)
				err = tc.opts.Validate()
				assert.Nil(t, err, "Validate error when loadBalanced=false: %v", err)

				tc.opts.SetLoadBalanced(true)
				err = tc.opts.Validate()
				assert.Equal(t, tc.err, err, "expected error %v when loadBalanced=true, got %v", tc.err, err)
			})
		}
	})
	t.Run("minPoolSize validation", func(t *testing.T) {
		testCases := []struct {
			name string
			opts *ClientOptionsBuilder
			err  error
		}{
			{
				"minPoolSize < maxPoolSize",
				Client().SetMinPoolSize(128).SetMaxPoolSize(256),
				nil,
			},
			{
				"minPoolSize == maxPoolSize",
				Client().SetMinPoolSize(128).SetMaxPoolSize(128),
				nil,
			},
			{
				"minPoolSize > maxPoolSize",
				Client().SetMinPoolSize(64).SetMaxPoolSize(32),
				errors.New("minPoolSize must be less than or equal to maxPoolSize, got minPoolSize=64 maxPoolSize=32"),
			},
			{
				"maxPoolSize == 0",
				Client().SetMinPoolSize(128).SetMaxPoolSize(0),
				nil,
			},
		}
		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				err := tc.opts.Validate()
				assert.Equal(t, tc.err, err, "expected error %v, got %v", tc.err, err)
			})
		}
	})
	t.Run("srvMaxHosts validation", func(t *testing.T) {
		testCases := []struct {
			name string
			opts *ClientOptionsBuilder
			err  error
		}{
			{"replica set name", Client().SetReplicaSet("foo"), connstring.ErrSRVMaxHostsWithReplicaSet},
			{"loadBalanced=true", Client().SetLoadBalanced(true), connstring.ErrSRVMaxHostsWithLoadBalanced},
			{"loadBalanced=false", Client().SetLoadBalanced(false), nil},
		}
		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				err := tc.opts.Validate()
				assert.Nil(t, err, "Validate error when srvMxaHosts is unset: %v", err)

				tc.opts.SetSRVMaxHosts(0)
				err = tc.opts.Validate()
				assert.Nil(t, err, "Validate error when srvMaxHosts is 0: %v", err)

				tc.opts.SetSRVMaxHosts(2)
				err = tc.opts.Validate()
				assert.Equal(t, tc.err, err, "expected error %v when srvMaxHosts > 0, got %v", tc.err, err)
			})
		}
	})
	t.Run("srvMaxHosts validation", func(t *testing.T) {
		t.Parallel()

		testCases := []struct {
			name string
			opts *ClientOptionsBuilder
			err  error
		}{
			{
				name: "valid ServerAPI",
				opts: Client().SetServerAPIOptions(ServerAPI(ServerAPIVersion1)),
				err:  nil,
			},
			{
				name: "invalid ServerAPI",
				opts: Client().SetServerAPIOptions(ServerAPI("nope")),
				err:  errors.New(`api version "nope" not supported; this driver version only supports API version "1"`),
			},
			{
				name: "invalid ServerAPI with other invalid options",
				opts: Client().SetServerAPIOptions(ServerAPI("nope")).SetSRVMaxHosts(1).SetReplicaSet("foo"),
				err:  errors.New(`api version "nope" not supported; this driver version only supports API version "1"`),
			},
		}
		for _, tc := range testCases {
			tc := tc // Capture range variable.

			t.Run(tc.name, func(t *testing.T) {
				t.Parallel()

				err := tc.opts.Validate()
				assert.Equal(t, tc.err, err, "want error %v, got error %v", tc.err, err)
			})
		}
	})
	t.Run("server monitoring mode validation", func(t *testing.T) {
		t.Parallel()

		testCases := []struct {
			name string
			opts *ClientOptionsBuilder
			err  error
		}{
			{
				name: "undefined",
				opts: Client(),
				err:  nil,
			},
			{
				name: "auto",
				opts: Client().SetServerMonitoringMode(ServerMonitoringModeAuto),
				err:  nil,
			},
			{
				name: "poll",
				opts: Client().SetServerMonitoringMode(ServerMonitoringModePoll),
				err:  nil,
			},
			{
				name: "stream",
				opts: Client().SetServerMonitoringMode(ServerMonitoringModeStream),
				err:  nil,
			},
			{
				name: "invalid",
				opts: Client().SetServerMonitoringMode("invalid"),
				err:  errors.New("invalid server monitoring mode: \"invalid\""),
			},
		}

		for _, tc := range testCases {
			tc := tc // Capture the range variable

			t.Run(tc.name, func(t *testing.T) {
				t.Parallel()

				err := tc.opts.Validate()
				assert.Equal(t, tc.err, err, "expected error %v, got %v", tc.err, err)
			})
		}
	})
	t.Run("OIDC auth configuration validation", func(t *testing.T) {
		t.Parallel()

		emptyCb := func(_ context.Context, _ *OIDCArgs) (*OIDCCredential, error) {
			return nil, nil
		}

		testCases := []struct {
			name string
			opts *ClientOptionsBuilder
			err  error
		}{
			{
				name: "password must not be set",
				opts: Client().SetAuth(Credential{AuthMechanism: "MONGODB-OIDC", Password: "password"}),
				err:  fmt.Errorf("password must not be set for the MONGODB-OIDC auth mechanism"),
			},
			{
				name: "cannot set both OIDCMachineCallback and OIDCHumanCallback simultaneously",
				opts: Client().SetAuth(Credential{AuthMechanism: "MONGODB-OIDC",
					OIDCMachineCallback: emptyCb, OIDCHumanCallback: emptyCb}),
				err: fmt.Errorf("cannot set both OIDCMachineCallback and OIDCHumanCallback, only one may be specified"),
			},
			{
				name: "cannot set ALLOWED_HOSTS without OIDCHumanCallback",
				opts: Client().SetAuth(Credential{AuthMechanism: "MONGODB-OIDC",
					OIDCMachineCallback:     emptyCb,
					AuthMechanismProperties: map[string]string{"ALLOWED_HOSTS": "www.example.com"},
				}),
				err: fmt.Errorf("Cannot specify ALLOWED_HOSTS without an OIDCHumanCallback"),
			},
			{
				name: "cannot set OIDCMachineCallback in GCP Environment",
				opts: Client().SetAuth(Credential{
					AuthMechanism:           "MONGODB-OIDC",
					OIDCMachineCallback:     emptyCb,
					AuthMechanismProperties: map[string]string{"ENVIRONMENT": "gcp"},
				}),
				err: fmt.Errorf(`OIDCMachineCallback cannot be specified with the gcp "ENVIRONMENT"`),
			},
			{
				name: "cannot set OIDCMachineCallback in AZURE Environment",
				opts: Client().SetAuth(Credential{
					AuthMechanism:           "MONGODB-OIDC",
					OIDCMachineCallback:     emptyCb,
					AuthMechanismProperties: map[string]string{"ENVIRONMENT": "azure"},
				}),
				err: fmt.Errorf(`OIDCMachineCallback cannot be specified with the azure "ENVIRONMENT"`),
			},
			{
				name: "TOKEN_RESOURCE must be set in GCP Environment",
				opts: Client().SetAuth(Credential{
					AuthMechanism:           "MONGODB-OIDC",
					AuthMechanismProperties: map[string]string{"ENVIRONMENT": "gcp"},
				}),
				err: fmt.Errorf(`"TOKEN_RESOURCE" must be set for the gcp "ENVIRONMENT"`),
			},
			{
				name: "TOKEN_RESOURCE must be set in AZURE Environment",
				opts: Client().SetAuth(Credential{
					AuthMechanism:           "MONGODB-OIDC",
					AuthMechanismProperties: map[string]string{"ENVIRONMENT": "azure"},
				}),
				err: fmt.Errorf(`"TOKEN_RESOURCE" must be set for the azure "ENVIRONMENT"`),
			},
			{
				name: "TOKEN_RESOURCE must not be set in TEST Environment",
				opts: Client().SetAuth(Credential{
					AuthMechanism:           "MONGODB-OIDC",
					AuthMechanismProperties: map[string]string{"ENVIRONMENT": "test", "TOKEN_RESOURCE": "stuff"},
				}),
				err: fmt.Errorf(`"TOKEN_RESOURCE" must not be set for the test "ENVIRONMENT"`),
			},
			{
				name: "TOKEN_RESOURCE must not be set in any other Environment",
				opts: Client().SetAuth(Credential{
					AuthMechanism:           "MONGODB-OIDC",
					AuthMechanismProperties: map[string]string{"ENVIRONMENT": "random env!", "TOKEN_RESOURCE": "stuff"},
				}),
				err: fmt.Errorf(`"TOKEN_RESOURCE" must not be set for the random env! "ENVIRONMENT"`),
			},
		}
		for _, tc := range testCases {
			tc := tc // Capture range variable.

			t.Run(tc.name, func(t *testing.T) {
				t.Parallel()

				err := tc.opts.Validate()
				assert.Equal(t, tc.err, err, "want error %v, got error %v", tc.err, err)
			})
		}
	})
	t.Run("OIDC auth configuration validation", func(t *testing.T) {
		t.Parallel()

		emptyCb := func(_ context.Context, _ *OIDCArgs) (*OIDCCredential, error) {
			return nil, nil
		}

		testCases := []struct {
			name string
			opts *ClientOptionsBuilder
			err  error
		}{
			{
				name: "password must not be set",
				opts: Client().SetAuth(Credential{AuthMechanism: "MONGODB-OIDC", Password: "password"}),
				err:  fmt.Errorf("password must not be set for the MONGODB-OIDC auth mechanism"),
			},
			{
				name: "cannot set both OIDCMachineCallback and OIDCHumanCallback simultaneously",
				opts: Client().SetAuth(Credential{AuthMechanism: "MONGODB-OIDC",
					OIDCMachineCallback: emptyCb, OIDCHumanCallback: emptyCb}),
				err: fmt.Errorf("cannot set both OIDCMachineCallback and OIDCHumanCallback, only one may be specified"),
			},
			{
				name: "cannot set ALLOWED_HOSTS without OIDCHumanCallback",
				opts: Client().SetAuth(Credential{AuthMechanism: "MONGODB-OIDC",
					OIDCMachineCallback:     emptyCb,
					AuthMechanismProperties: map[string]string{"ALLOWED_HOSTS": "www.example.com"},
				}),
				err: fmt.Errorf("Cannot specify ALLOWED_HOSTS without an OIDCHumanCallback"),
			},
			{
				name: "cannot set OIDCMachineCallback in GCP Environment",
				opts: Client().SetAuth(Credential{
					AuthMechanism:           "MONGODB-OIDC",
					OIDCMachineCallback:     emptyCb,
					AuthMechanismProperties: map[string]string{"ENVIRONMENT": "gcp"},
				}),
				err: fmt.Errorf(`OIDCMachineCallback cannot be specified with the gcp "ENVIRONMENT"`),
			},
			{
				name: "cannot set OIDCMachineCallback in AZURE Environment",
				opts: Client().SetAuth(Credential{
					AuthMechanism:           "MONGODB-OIDC",
					OIDCMachineCallback:     emptyCb,
					AuthMechanismProperties: map[string]string{"ENVIRONMENT": "azure"},
				}),
				err: fmt.Errorf(`OIDCMachineCallback cannot be specified with the azure "ENVIRONMENT"`),
			},
			{
				name: "TOKEN_RESOURCE must be set in GCP Environment",
				opts: Client().SetAuth(Credential{
					AuthMechanism:           "MONGODB-OIDC",
					AuthMechanismProperties: map[string]string{"ENVIRONMENT": "gcp"},
				}),
				err: fmt.Errorf(`"TOKEN_RESOURCE" must be set for the gcp "ENVIRONMENT"`),
			},
			{
				name: "TOKEN_RESOURCE must be set in AZURE Environment",
				opts: Client().SetAuth(Credential{
					AuthMechanism:           "MONGODB-OIDC",
					AuthMechanismProperties: map[string]string{"ENVIRONMENT": "azure"},
				}),
				err: fmt.Errorf(`"TOKEN_RESOURCE" must be set for the azure "ENVIRONMENT"`),
			},
			{
				name: "TOKEN_RESOURCE must not be set in TEST Environment",
				opts: Client().SetAuth(Credential{
					AuthMechanism:           "MONGODB-OIDC",
					AuthMechanismProperties: map[string]string{"ENVIRONMENT": "test", "TOKEN_RESOURCE": "stuff"},
				}),
				err: fmt.Errorf(`"TOKEN_RESOURCE" must not be set for the test "ENVIRONMENT"`),
			},
			{
				name: "TOKEN_RESOURCE must not be set in any other Environment",
				opts: Client().SetAuth(Credential{
					AuthMechanism:           "MONGODB-OIDC",
					AuthMechanismProperties: map[string]string{"ENVIRONMENT": "random env!", "TOKEN_RESOURCE": "stuff"},
				}),
				err: fmt.Errorf(`"TOKEN_RESOURCE" must not be set for the random env! "ENVIRONMENT"`),
			},
		}
		for _, tc := range testCases {
			tc := tc // Capture range variable.

			t.Run(tc.name, func(t *testing.T) {
				t.Parallel()

				err := tc.opts.Validate()
				assert.Equal(t, tc.err, err, "want error %v, got error %v", tc.err, err)
			})
		}
	})
}

func createCertPool(t *testing.T, paths ...string) *x509.CertPool {
	t.Helper()

	pool := x509.NewCertPool()
	for _, path := range paths {
		pool.AddCert(loadCert(t, path))
	}
	return pool
}

func loadCert(t *testing.T, file string) *x509.Certificate {
	t.Helper()

	data := readFile(t, file)
	block, _ := pem.Decode(data)
	cert, err := x509.ParseCertificate(block.Bytes)
	assert.Nil(t, err, "ParseCertificate error for %s: %v", file, err)
	return cert
}

func readFile(t *testing.T, path string) []byte {
	data, err := os.ReadFile(path)
	assert.Nil(t, err, "ReadFile error for %s: %v", path, err)
	return data
}

type testDialer struct {
	Num int
}

func (testDialer) DialContext(context.Context, string, string) (net.Conn, error) {
	return nil, nil
}

func compareTLSConfig(cfg1, cfg2 *tls.Config) bool {
	if cfg1 == nil && cfg2 == nil {
		return true
	}

	if cfg1 == nil || cfg2 == nil {
		return true
	}

	if (cfg1.RootCAs == nil && cfg1.RootCAs != nil) || (cfg1.RootCAs != nil && cfg1.RootCAs == nil) {
		return false
	}

	if cfg1.RootCAs != nil {
		cfg1Subjects := cfg1.RootCAs.Subjects()
		cfg2Subjects := cfg2.RootCAs.Subjects()
		if len(cfg1Subjects) != len(cfg2Subjects) {
			return false
		}

		for idx, firstSubject := range cfg1Subjects {
			if !bytes.Equal(firstSubject, cfg2Subjects[idx]) {
				return false
			}
		}
	}

	if len(cfg1.Certificates) != len(cfg2.Certificates) {
		return false
	}

	if cfg1.InsecureSkipVerify != cfg2.InsecureSkipVerify {
		return false
	}

	return true
}

func compareErrors(err1, err2 error) bool {
	if err1 == nil && err2 == nil {
		return true
	}

	if err1 == nil || err2 == nil {
		return false
	}

	var ospe1, ospe2 *os.PathError
	if errors.As(err1, &ospe1) && errors.As(err2, &ospe2) {
		return ospe1.Op == ospe2.Op && ospe1.Path == ospe2.Path
	}

	if err1.Error() != err2.Error() {
		return false
	}

	return true
}

func TestSetURIopts(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name     string
		uri      string
		wantopts *ClientOptions

		// A list of possible errors that can be returned, required to account for
		// OS-specific errors.
		wantErrs []error
	}{
		{
			name:     "ParseError",
			uri:      "not-mongo-db-uri://",
			wantopts: &ClientOptions{},
			wantErrs: []error{
				fmt.Errorf(
					"error parsing uri: %w",
					errors.New(`scheme must be "mongodb" or "mongodb+srv"`)),
			},
		},
		{
			name: "ReadPreference Invalid Mode",
			uri:  "mongodb://localhost/?maxStaleness=200",
			wantopts: &ClientOptions{
				Hosts: []string{"localhost"},
			},
			wantErrs: []error{
				fmt.Errorf("unknown read preference %v", ""),
			},
		},
		{
			name: "ReadPreference Primary With Options",
			uri:  "mongodb://localhost/?readPreference=Primary&maxStaleness=200",
			wantopts: &ClientOptions{
				Hosts: []string{"localhost"},
			},
			wantErrs: []error{
				errors.New("can not specify tags, max staleness, or hedge with mode primary"),
			},
		},
		{
			name: "TLS addCertFromFile error",
			uri:  "mongodb://localhost/?ssl=true&sslCertificateAuthorityFile=testdata/doesntexist",
			wantopts: &ClientOptions{
				Hosts: []string{"localhost"},
			},
			wantErrs: []error{
				&os.PathError{
					Op:   "open",
					Path: "testdata/doesntexist",
					Err:  errors.New("no such file or directory"),
				},
				&os.PathError{
					Op:   "open",
					Path: "testdata/doesntexist",
					// Windows error
					Err: errors.New("The system cannot find the file specified."), //nolint:revive
				},
			},
		},
		{
			name: "TLS ClientCertificateKey",
			uri:  "mongodb://localhost/?ssl=true&sslClientCertificateKeyFile=testdata/doesntexist",
			wantopts: &ClientOptions{
				Hosts: []string{"localhost"},
			},
			wantErrs: []error{
				&os.PathError{
					Op:   "open",
					Path: "testdata/doesntexist",
					Err:  errors.New("no such file or directory"),
				},
				&os.PathError{
					Op:   "open",
					Path: "testdata/doesntexist",
					// Windows error
					Err: errors.New("The system cannot find the file specified."), //nolint:revive
				},
			},
		},
		{
			name: "AppName",
			uri:  "mongodb://localhost/?appName=awesome-example-application",
			wantopts: &ClientOptions{
				Hosts:   []string{"localhost"},
				AppName: ptrutil.Ptr[string]("awesome-example-application"),
			},
			wantErrs: nil,
		},
		{
			name: "AuthMechanism",
			uri:  "mongodb://localhost/?authMechanism=mongodb-x509",
			wantopts: &ClientOptions{
				Hosts: []string{"localhost"},
				Auth:  &Credential{AuthSource: "$external", AuthMechanism: "mongodb-x509"},
			},
			wantErrs: nil,
		},
		{
			name: "AuthMechanismProperties",
			uri:  "mongodb://foo@localhost/?authMechanism=gssapi&authMechanismProperties=SERVICE_NAME:mongodb-fake",
			wantopts: &ClientOptions{
				Hosts: []string{"localhost"},
				Auth: &Credential{
					AuthSource:              "$external",
					AuthMechanism:           "gssapi",
					AuthMechanismProperties: map[string]string{"SERVICE_NAME": "mongodb-fake"},
					Username:                "foo",
				},
			},
			wantErrs: nil,
		},
		{
			name: "AuthSource",
			uri:  "mongodb://foo@localhost/?authSource=random-database-example",
			wantopts: &ClientOptions{
				Hosts: []string{"localhost"},
				Auth:  &Credential{AuthSource: "random-database-example", Username: "foo"},
			},
			wantErrs: nil,
		},
		{
			name: "Username",
			uri:  "mongodb://foo@localhost/",
			wantopts: &ClientOptions{
				Hosts: []string{"localhost"},
				Auth:  &Credential{AuthSource: "admin", Username: "foo"},
			},
			wantErrs: nil,
		},
		{
			name:     "Unescaped slash in username",
			uri:      "mongodb:///:pwd@localhost",
			wantopts: &ClientOptions{},
			wantErrs: []error{
				fmt.Errorf("error parsing uri: %w", errors.New("unescaped slash in username")),
			},
		},
		{
			name: "Password",
			uri:  "mongodb://foo:bar@localhost/",
			wantopts: &ClientOptions{
				Hosts: []string{"localhost"},
				Auth: &Credential{
					AuthSource: "admin", Username: "foo",
					Password: "bar", PasswordSet: true,
				},
			},
			wantErrs: nil,
		},
		{
			name: "Single character username and password",
			uri:  "mongodb://f:b@localhost/",
			wantopts: &ClientOptions{
				Hosts: []string{"localhost"},
				Auth: &Credential{
					AuthSource: "admin", Username: "f",
					Password: "b", PasswordSet: true,
				},
			},
			wantErrs: nil,
		},
		{
			name: "Connect",
			uri:  "mongodb://localhost/?connect=direct",
			wantopts: &ClientOptions{
				Hosts:  []string{"localhost"},
				Direct: ptrutil.Ptr[bool](true),
			},
			wantErrs: nil,
		},
		{
			name: "ConnectTimeout",
			uri:  "mongodb://localhost/?connectTimeoutms=5000",
			wantopts: &ClientOptions{
				Hosts:          []string{"localhost"},
				ConnectTimeout: ptrutil.Ptr[time.Duration](5 * time.Second),
			},
			wantErrs: nil,
		},
		{
			name: "Compressors",
			uri:  "mongodb://localhost/?compressors=zlib,snappy",
			wantopts: &ClientOptions{
				Hosts:       []string{"localhost"},
				Compressors: []string{"zlib", "snappy"},
				ZlibLevel:   ptrutil.Ptr[int](6),
			},
			wantErrs: nil,
		},
		{
			name: "DatabaseNoAuth",
			uri:  "mongodb://localhost/example-database",
			wantopts: &ClientOptions{
				Hosts: []string{"localhost"},
			},
			wantErrs: nil,
		},
		{
			name: "DatabaseAsDefault",
			uri:  "mongodb://foo@localhost/example-database",
			wantopts: &ClientOptions{
				Hosts: []string{"localhost"},
				Auth:  &Credential{AuthSource: "example-database", Username: "foo"},
			},
			wantErrs: nil,
		},
		{
			name: "HeartbeatInterval",
			uri:  "mongodb://localhost/?heartbeatIntervalms=12000",
			wantopts: &ClientOptions{
				Hosts:             []string{"localhost"},
				HeartbeatInterval: ptrutil.Ptr[time.Duration](12 * time.Second),
			},
			wantErrs: nil,
		},
		{
			name: "Hosts",
			uri:  "mongodb://localhost:27017,localhost:27018,localhost:27019/",
			wantopts: &ClientOptions{
				Hosts: []string{"localhost:27017", "localhost:27018", "localhost:27019"},
			},
			wantErrs: nil,
		},
		{
			name: "LocalThreshold",
			uri:  "mongodb://localhost/?localThresholdMS=200",
			wantopts: &ClientOptions{
				Hosts:          []string{"localhost"},
				LocalThreshold: ptrutil.Ptr[time.Duration](200 * time.Millisecond),
			},
			wantErrs: nil,
		},
		{
			name: "MaxConnIdleTime",
			uri:  "mongodb://localhost/?maxIdleTimeMS=300000",
			wantopts: &ClientOptions{
				Hosts:           []string{"localhost"},
				MaxConnIdleTime: ptrutil.Ptr[time.Duration](5 * time.Minute),
			},
			wantErrs: nil,
		},
		{
			name: "MaxPoolSize",
			uri:  "mongodb://localhost/?maxPoolSize=256",
			wantopts: &ClientOptions{
				Hosts:       []string{"localhost"},
				MaxPoolSize: ptrutil.Ptr[uint64](256),
			},
			wantErrs: nil,
		},
		{
			name: "MinPoolSize",
			uri:  "mongodb://localhost/?minPoolSize=256",
			wantopts: &ClientOptions{
				Hosts:       []string{"localhost"},
				MinPoolSize: ptrutil.Ptr[uint64](256),
			},
			wantErrs: nil,
		},
		{
			name: "MaxConnecting",
			uri:  "mongodb://localhost/?maxConnecting=10",
			wantopts: &ClientOptions{
				Hosts:         []string{"localhost"},
				MaxConnecting: ptrutil.Ptr[uint64](10),
			},
			wantErrs: nil,
		},
		{
			name: "ReadConcern",
			uri:  "mongodb://localhost/?readConcernLevel=linearizable",
			wantopts: &ClientOptions{
				Hosts:       []string{"localhost"},
				ReadConcern: readconcern.Linearizable(),
			},
			wantErrs: nil,
		},
		{
			name: "ReadPreference",
			uri:  "mongodb://localhost/?readPreference=secondaryPreferred",
			wantopts: &ClientOptions{
				Hosts:          []string{"localhost"},
				ReadPreference: readpref.SecondaryPreferred(),
			},
			wantErrs: nil,
		},
		{
			name: "ReadPreferenceTagSets",
			uri:  "mongodb://localhost/?readPreference=secondaryPreferred&readPreferenceTags=foo:bar",
			wantopts: &ClientOptions{
				Hosts: []string{"localhost"},
				ReadPreference: func() *readpref.ReadPref {
					tagSet, _ := readpref.NewTagSet("foo", "bar")
					return readpref.SecondaryPreferred(readpref.Options().SetTagSets([]readpref.TagSet{tagSet}))
				}(),
			},
			wantErrs: nil,
		},
		{
			name: "MaxStaleness",
			uri:  "mongodb://localhost/?readPreference=secondaryPreferred&maxStaleness=250",
			wantopts: &ClientOptions{
				Hosts:          []string{"localhost"},
				ReadPreference: readpref.SecondaryPreferred(readpref.Options().SetMaxStaleness(250 * time.Second)),
			},
			wantErrs: nil,
		},
		{
			name: "RetryWrites",
			uri:  "mongodb://localhost/?retryWrites=true",
			wantopts: &ClientOptions{
				Hosts:       []string{"localhost"},
				RetryWrites: ptrutil.Ptr[bool](true),
			},
			wantErrs: nil,
		},
		{
			name: "ReplicaSet",
			uri:  "mongodb://localhost/?replicaSet=rs01",
			wantopts: &ClientOptions{
				Hosts:      []string{"localhost"},
				ReplicaSet: ptrutil.Ptr[string]("rs01"),
			},
			wantErrs: nil,
		},
		{
			name: "ServerSelectionTimeout",
			uri:  "mongodb://localhost/?serverSelectionTimeoutMS=45000",
			wantopts: &ClientOptions{
				Hosts:                  []string{"localhost"},
				ServerSelectionTimeout: ptrutil.Ptr[time.Duration](45 * time.Second),
			},
			wantErrs: nil,
		},
		{
			name: "SocketTimeout",
			uri:  "mongodb://localhost/?socketTimeoutMS=15000",
			wantopts: &ClientOptions{
				Hosts: []string{"localhost"},
			},
			wantErrs: nil,
		},
		{
			name: "TLS CACertificate",
			uri:  "mongodb://localhost/?ssl=true&sslCertificateAuthorityFile=testdata/ca.pem",
			wantopts: &ClientOptions{
				Hosts: []string{"localhost"},
				TLSConfig: &tls.Config{
					RootCAs: createCertPool(t, "testdata/ca.pem"),
				},
			},
			wantErrs: nil,
		},
		{
			name: "TLS Insecure",
			uri:  "mongodb://localhost/?ssl=true&sslInsecure=true",
			wantopts: &ClientOptions{
				Hosts: []string{"localhost"},
				TLSConfig: &tls.Config{
					InsecureSkipVerify: true,
				},
			},
			wantErrs: nil,
		},
		{
			name: "TLS ClientCertificateKey",
			uri:  "mongodb://localhost/?ssl=true&sslClientCertificateKeyFile=testdata/nopass/certificate.pem",
			wantopts: &ClientOptions{
				Hosts: []string{"localhost"},
				TLSConfig: &tls.Config{
					Certificates: make([]tls.Certificate, 1),
				},
			},
			wantErrs: nil,
		},
		{
			name: "TLS ClientCertificateKey with password",
			uri:  "mongodb://localhost/?ssl=true&sslClientCertificateKeyFile=testdata/certificate.pem&sslClientCertificateKeyPassword=passphrase",
			wantopts: &ClientOptions{
				Hosts: []string{"localhost"},
				TLSConfig: &tls.Config{
					Certificates: make([]tls.Certificate, 1),
				},
			},
			wantErrs: nil,
		},
		{
			name: "TLS Username",
			uri:  "mongodb://localhost/?ssl=true&authMechanism=mongodb-x509&sslClientCertificateKeyFile=testdata/nopass/certificate.pem",
			wantopts: &ClientOptions{
				Hosts: []string{"localhost"},
				Auth: &Credential{
					AuthMechanism: "mongodb-x509", AuthSource: "$external",
					Username: `C=US,ST=New York,L=New York City, Inc,O=MongoDB\,OU=WWW`,
				},
			},
			wantErrs: nil,
		},
		{
			name: "WriteConcern J",
			uri:  "mongodb://localhost/?journal=true",
			wantopts: &ClientOptions{
				Hosts:        []string{"localhost"},
				WriteConcern: writeconcern.Journaled(),
			},
			wantErrs: nil,
		},
		{
			name: "WriteConcern WString",
			uri:  "mongodb://localhost/?w=majority",
			wantopts: &ClientOptions{
				Hosts:        []string{"localhost"},
				WriteConcern: writeconcern.Majority(),
			},
			wantErrs: nil,
		},
		{
			name: "WriteConcern W",
			uri:  "mongodb://localhost/?w=3",
			wantopts: &ClientOptions{
				Hosts:        []string{"localhost"},
				WriteConcern: &writeconcern.WriteConcern{W: 3},
			},
			wantErrs: nil,
		},
		{
			name: "WriteConcern WTimeout",
			uri:  "mongodb://localhost/?wTimeoutMS=45000",
			wantopts: &ClientOptions{
				Hosts: []string{"localhost"},
			},
			wantErrs: nil,
		},
		{
			name: "ZLibLevel",
			uri:  "mongodb://localhost/?zlibCompressionLevel=4",
			wantopts: &ClientOptions{
				Hosts:     []string{"localhost"},
				ZlibLevel: ptrutil.Ptr[int](4),
			},
			wantErrs: nil,
		},
		{
			name: "TLS tlsCertificateFile and tlsPrivateKeyFile",
			uri:  "mongodb://localhost/?tlsCertificateFile=testdata/nopass/cert.pem&tlsPrivateKeyFile=testdata/nopass/key.pem",
			wantopts: &ClientOptions{
				Hosts: []string{"localhost"},
				TLSConfig: &tls.Config{
					Certificates: make([]tls.Certificate, 1),
				},
			},
			wantErrs: nil,
		},
		{
			name:     "TLS only tlsCertificateFile",
			uri:      "mongodb://localhost/?tlsCertificateFile=testdata/nopass/cert.pem",
			wantopts: &ClientOptions{},
			wantErrs: []error{
				fmt.Errorf(
					"error validating uri: %w",
					errors.New("the tlsPrivateKeyFile URI option must be provided if the tlsCertificateFile option is specified")),
			},
		},
		{
			name:     "TLS only tlsPrivateKeyFile",
			uri:      "mongodb://localhost/?tlsPrivateKeyFile=testdata/nopass/key.pem",
			wantopts: &ClientOptions{},
			wantErrs: []error{
				fmt.Errorf(
					"error validating uri: %w",
					errors.New("the tlsCertificateFile URI option must be provided if the tlsPrivateKeyFile option is specified")),
			},
		},
		{
			name:     "TLS tlsCertificateFile and tlsPrivateKeyFile and tlsCertificateKeyFile",
			uri:      "mongodb://localhost/?tlsCertificateFile=testdata/nopass/cert.pem&tlsPrivateKeyFile=testdata/nopass/key.pem&tlsCertificateKeyFile=testdata/nopass/certificate.pem",
			wantopts: &ClientOptions{},
			wantErrs: []error{
				fmt.Errorf(
					"error validating uri: %w",
					errors.New("the sslClientCertificateKeyFile/tlsCertificateKeyFile URI option cannot be provided "+
						"along with tlsCertificateFile or tlsPrivateKeyFile")),
			},
		},
		{
			name: "disable OCSP endpoint check",
			uri:  "mongodb://localhost/?tlsDisableOCSPEndpointCheck=true",
			wantopts: &ClientOptions{
				Hosts:                    []string{"localhost"},
				DisableOCSPEndpointCheck: ptrutil.Ptr[bool](true),
			},
			wantErrs: nil,
		},
		{
			name: "directConnection",
			uri:  "mongodb://localhost/?directConnection=true",
			wantopts: &ClientOptions{
				Hosts:  []string{"localhost"},
				Direct: ptrutil.Ptr[bool](true),
			},
			wantErrs: nil,
		},
		{
			name: "TLS CA file with multiple certificiates",
			uri:  "mongodb://localhost/?tlsCAFile=testdata/ca-with-intermediates.pem",
			wantopts: &ClientOptions{
				Hosts: []string{"localhost"},
				TLSConfig: &tls.Config{
					RootCAs: createCertPool(t, "testdata/ca-with-intermediates-first.pem",
						"testdata/ca-with-intermediates-second.pem", "testdata/ca-with-intermediates-third.pem"),
				},
			},
			wantErrs: nil,
		},
		{
			name: "TLS empty CA file",
			uri:  "mongodb://localhost/?tlsCAFile=testdata/empty-ca.pem",
			wantopts: &ClientOptions{
				Hosts: []string{"localhost"},
			},
			wantErrs: []error{
				errors.New("the specified CA file does not contain any valid certificates"),
			},
		},
		{
			name: "TLS CA file with no certificates",
			uri:  "mongodb://localhost/?tlsCAFile=testdata/ca-key.pem",
			wantopts: &ClientOptions{
				Hosts: []string{"localhost"},
			},
			wantErrs: []error{
				errors.New("the specified CA file does not contain any valid certificates"),
			},
		},
		{
			name: "TLS malformed CA file",
			uri:  "mongodb://localhost/?tlsCAFile=testdata/malformed-ca.pem",
			wantopts: &ClientOptions{
				Hosts: []string{"localhost"},
			},
			wantErrs: []error{
				errors.New("the specified CA file does not contain any valid certificates"),
			},
		},
		{
			name: "loadBalanced=true",
			uri:  "mongodb://localhost/?loadBalanced=true",
			wantopts: &ClientOptions{
				Hosts:        []string{"localhost"},
				LoadBalanced: ptrutil.Ptr[bool](true),
			},
			wantErrs: nil,
		},
		{
			name: "loadBalanced=false",
			uri:  "mongodb://localhost/?loadBalanced=false",
			wantopts: &ClientOptions{
				Hosts:        []string{"localhost"},
				LoadBalanced: ptrutil.Ptr[bool](false),
			},
			wantErrs: nil,
		},
		{
			name: "srvServiceName",
			uri:  "mongodb+srv://test22.test.build.10gen.cc/?srvServiceName=customname",
			wantopts: &ClientOptions{
				Hosts:          []string{"localhost.test.build.10gen.cc:27017", "localhost.test.build.10gen.cc:27018"},
				SRVServiceName: ptrutil.Ptr[string]("customname"),
			},
			wantErrs: nil,
		},
		{
			name: "srvMaxHosts",
			uri:  "mongodb+srv://test1.test.build.10gen.cc/?srvMaxHosts=2",
			wantopts: &ClientOptions{
				Hosts:       []string{"localhost.test.build.10gen.cc:27017", "localhost.test.build.10gen.cc:27018"},
				SRVMaxHosts: ptrutil.Ptr[int](2),
			},
			wantErrs: nil,
		},
		{
			name: "GODRIVER-2263 regression test",
			uri:  "mongodb://localhost/?tlsCertificateKeyFile=testdata/one-pk-multiple-certs.pem",
			wantopts: &ClientOptions{
				Hosts:     []string{"localhost"},
				TLSConfig: &tls.Config{Certificates: make([]tls.Certificate, 1)},
			},
			wantErrs: nil,
		},
		{
			name: "GODRIVER-2650 X509 certificate",
			uri:  "mongodb://localhost/?ssl=true&authMechanism=mongodb-x509&sslClientCertificateKeyFile=testdata/one-pk-multiple-certs.pem",
			wantopts: &ClientOptions{
				Hosts: []string{"localhost"},
				Auth: &Credential{
					AuthMechanism: "mongodb-x509", AuthSource: "$external",
					// Subject name in the first certificate is used as the username for X509 auth.
					Username: `C=US,ST=New York,L=New York City,O=MongoDB,OU=Drivers,CN=localhost`,
				},
				TLSConfig: &tls.Config{Certificates: make([]tls.Certificate, 1)},
			},
			wantErrs: nil,
		},
		{
			name: "ALLOWED_HOSTS cannot be specified in URI connection",
			uri:  "mongodb://localhost/?authMechanism=MONGODB-OIDC&authMechanismProperties=ALLOWED_HOSTS:example.com",
			wantopts: &ClientOptions{
				HTTPClient: httputil.DefaultHTTPClient,
			},
			wantErrs: []error{
				errors.New(`error validating uri: ALLOWED_HOSTS cannot be specified in the URI connection string for the "MONGODB-OIDC" auth mechanism, it must be specified through the ClientOptions directly`),
			},
		},
		{
			name: "colon in TOKEN_RESOURCE works as expected",
			uri:  "mongodb://example.com/?authMechanism=MONGODB-OIDC&authMechanismProperties=TOKEN_RESOURCE:mongodb://test-cluster",
			wantopts: &ClientOptions{
				Hosts:      []string{"example.com"},
				Auth:       &Credential{AuthMechanism: "MONGODB-OIDC", AuthSource: "$external", AuthMechanismProperties: map[string]string{"TOKEN_RESOURCE": "mongodb://test-cluster"}},
				HTTPClient: httputil.DefaultHTTPClient,
			},
			wantErrs: nil,
		},
		{
			name: "tmp",
			uri:  "mongodb://example.com/?authMechanism=MONGODB-OIDC&authMechanismProperties=TOKEN_RESOURCE:mongodb://test-cluster,ENVIRONMENT:azureManagedIdentities",
			wantopts: &ClientOptions{
				Hosts: []string{"example.com"},
				Auth: &Credential{AuthMechanism: "MONGODB-OIDC", AuthSource: "$external", AuthMechanismProperties: map[string]string{
					"ENVIRONMENT":    "azureManagedIdentities",
					"TOKEN_RESOURCE": "mongodb://test-cluster"}},
				HTTPClient: httputil.DefaultHTTPClient,
			},
			wantErrs: nil,
		},
		{
			name: "comma in key:value pair causes error",
			uri:  "mongodb://example.com/?authMechanismProperties=TOKEN_RESOURCE:mongodb://host1%2Chost2",
			wantopts: &ClientOptions{
				HTTPClient: httputil.DefaultHTTPClient,
			},
			wantErrs: []error{
				errors.New(`error parsing uri: invalid authMechanism property`),
			},
		},
	}

	for _, test := range testCases {
		test := test

		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			// Manually add the URI and ConnString to the test expectations to avoid
			// adding them in each test definition. The ConnString should only be
			// recorded if there was no error while parsing.
			connString, err := connstring.ParseAndValidate(test.uri)
			if err == nil {
				test.wantopts.connString = connString
			}

			// Also manually add the default HTTP client if one does not exist.
			if test.wantopts.HTTPClient == nil {
				test.wantopts.HTTPClient = http.DefaultClient
			}

			// Use the setURIopts to just test that a correct error is returned.
			if gotErr := setURIOpts(test.uri, &ClientOptions{}); test.wantErrs != nil {
				var foundError bool

				for _, err := range test.wantErrs {
					if err.Error() == gotErr.Error() {
						foundError = true

						break
					}
				}

				assert.True(t, foundError, "expected error to be one of %v, got: %v", test.wantErrs, gotErr)
			}

			// Run this test through the client.ApplyURI method to ensure that it
			// remains a naive wrapper.
			opts := Client().ApplyURI(test.uri)

			gotopts := &ClientOptions{}
			for _, setter := range opts.Opts {
				_ = setter(gotopts)
			}

			// We have to sort string slices in comparison, as Hosts resolved from SRV
			// URIs do not have a set order.
			stringLess := func(a, b string) bool { return a < b }
			if diff := cmp.Diff(
				test.wantopts, gotopts,
				cmp.AllowUnexported(ClientOptions{}, readconcern.ReadConcern{}, writeconcern.WriteConcern{}, readpref.ReadPref{}),
				// cmp.Comparer(func(r1, r2 *bsoncodec.Registry) bool { return r1 == r2 }),
				cmp.Comparer(compareTLSConfig),
				cmp.Comparer(compareErrors),
				cmpopts.SortSlices(stringLess),
				cmpopts.IgnoreFields(connstring.ConnString{}, "SSLClientCertificateKeyPassword"),
				cmpopts.IgnoreFields(http.Client{}, "Transport"),
			); diff != "" {
				t.Errorf("URI did not apply correctly: (-want +got)\n%s", diff)
			}
		})
	}
}
