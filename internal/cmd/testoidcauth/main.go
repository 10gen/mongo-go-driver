// Copyright (C) MongoDB, Inc. 2022-present.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"path"
	"reflect"
	"strings"
	"sync"
	"time"
	"unsafe"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/event"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/x/mongo/driver/auth"
)

var uriAdmin = os.Getenv("MONGODB_URI")
var uriSingle = os.Getenv("MONGODB_URI_SINGLE")
var uriMulti = os.Getenv("MONGODB_URI_MULTI")
var oidcTokenDir = os.Getenv("OIDC_TOKEN_DIR")

var oidcDomain = os.Getenv("OIDC_DOMAIN")

func explicitUser(user string) string {
	return fmt.Sprintf("%s@%s", user, oidcDomain)
}

func tokenFile(user string) string {
	return path.Join(oidcTokenDir, user)
}

func connectAdminClient() (*mongo.Client, error) {
	return mongo.Connect(options.Client().ApplyURI(uriAdmin))
}

func connectWithMachineCB(uri string, cb options.OIDCCallback) (*mongo.Client, error) {
	cred := options.Credential{
		AuthMechanism:       "MONGODB-OIDC",
		OIDCMachineCallback: cb,
	}
	optsBuilder := options.Client().ApplyURI(uri).SetAuth(cred)
	return mongo.Connect(optsBuilder)
}

func connectWithHumanCB(uri string, cb options.OIDCCallback) (*mongo.Client, error) {
	cred := options.Credential{
		AuthMechanism:     "MONGODB-OIDC",
		OIDCHumanCallback: cb,
	}
	opts := options.Client().ApplyURI(uri).SetAuth(cred)
	return mongo.Connect(opts)
}

func connectWithHumanCBAndUser(uri string, principal string, cb options.OIDCCallback) (*mongo.Client, error) {
	username := principal
	switch principal {
	case "test_user1", "test_user2":
		username = explicitUser(principal)
	}
	cred := options.Credential{
		AuthMechanism:       "MONGODB-OIDC",
		OIDCMachineCallback: cb,
		Username:            username,
	}
	opts := options.Client().ApplyURI(uri).SetAuth(cred)
	return mongo.Connect(opts)
}

func connectWithHumanCBAndMonitor(uri string, cb options.OIDCCallback, m *event.CommandMonitor) (*mongo.Client, error) {
	cred := options.Credential{
		AuthMechanism:     "MONGODB-OIDC",
		OIDCHumanCallback: cb,
	}
	opts := options.Client().ApplyURI(uri).SetMonitor(m).SetAuth(cred)
	return mongo.Connect(opts)
}

func connectWithMachineCBAndProperties(uri string, cb options.OIDCCallback, props map[string]string) (*mongo.Client, error) {
	cred := options.Credential{
		AuthMechanism:           "MONGODB-OIDC",
		OIDCMachineCallback:     cb,
		AuthMechanismProperties: props,
	}
	optsBuilder := options.Client().ApplyURI(uri).SetAuth(cred)
	return mongo.Connect(optsBuilder)
}

func main() {
	// be quiet linter
	_ = tokenFile("test_user2")

	hasError := false
	aux := func(test_name string, f func() error) {
		fmt.Printf("%s...\n", test_name)
		err := f()
		if err != nil {
			fmt.Println("Test Error: ", err)
			fmt.Println("...Failed")
			hasError = true
		} else {
			fmt.Println("...Ok")
		}
	}
	env := os.Getenv("OIDC_ENV")
	switch env {
	case "":
		aux("machine_1_1_callbackIsCalled", machine11callbackIsCalled)
		aux("machine_1_2_callbackIsCalledOnlyOneForMultipleConnections", machine12callbackIsCalledOnlyOneForMultipleConnections)
		aux("machine_2_1_validCallbackInputs", machine21validCallbackInputs)
		aux("machine_2_3_oidcCallbackReturnMissingData", machine23oidcCallbackReturnMissingData)
		aux("machine_2_4_invalidClientConfigurationWithCallback", machine24invalidClientConfigurationWithCallback)
		aux("machine_3_1_failureWithCachedTokensFetchANewTokenAndRetryAuth", machine31failureWithCachedTokensFetchANewTokenAndRetryAuth)
		aux("machine_3_2_authFailuresWithoutCachedTokensReturnsAnError", machine32authFailuresWithoutCachedTokensReturnsAnError)
		aux("machine_3_3_UnexpectedErrorCodeDoesNotClearTheCache", machine33UnexpectedErrorCodeDoesNotClearTheCache)
		aux("machine_4_1_reauthenticationSucceeds", machine41ReauthenticationSucceeds)
		aux("machine_4_2_readCommandsFailIfReauthenticationFails", machine42ReadCommandsFailIfReauthenticationFails)
		aux("machine_4_3_writeCommandsFailIfReauthenticationFails", machine43WriteCommandsFailIfReauthenticationFails)
		aux("human_1_1_singlePrincipalImplictUsername", human11singlePrincipalImplictUsername)
		aux("human_1_2_singlePrincipalExplicitUsername", human12singlePrincipalExplicitUsername)
		aux("human_1_3_mulitplePrincipalUser1", human13mulitplePrincipalUser1)
		aux("human_1_4_mulitplePrincipalUser2", human14mulitplePrincipalUser2)
		aux("human_1_5_multiplPrincipalNoUser", human15mulitplePrincipalNoUser)
		aux("human_1_6_allowedHostsBlocked", human16allowedHostsBlocked)
		aux("human_1_7_allowedHostsInConnectionStringIgnored", human17AllowedHostsInConnectionStringIgnored)
		aux("human_1_8_machineIDPHumanCallback", human18MachineIDPHumanCallback)
		aux("human_2_1_validCallbackInputs", human21validCallbackInputs)
		aux("human_2_2_CallbackReturnsMissingData", human22CallbackReturnsMissingData)
		aux("human_2_3_RefreshTokenIsPassedToCallback", human23RefreshTokenIsPassedToCallback)
		aux("human_3_1_usesSpeculativeAuth", human31usesSpeculativeAuth)
		aux("human_3_2_doesNotUseSpecualtiveAuth", human32doesNotUseSpecualtiveAuth)
		aux("human_4_1_reauthenticationSucceeds", human41ReauthenticationSucceeds)
		aux("human_4_2_reauthenticationSucceedsNoRefresh", human42ReauthenticationSucceedsNoRefreshToken)
		aux("human_4_3_reauthenticationSucceedsAfterRefreshFails", human43ReauthenticationSucceedsAfterRefreshFails)
		aux("human_4_4_reauthenticationFails", human44ReauthenticationFails)
	case "azure":
		aux("machine_2_5_InvalidUseofAllowedHosts", machine25InvalidUseofAllowedHosts)
		aux("machine_5_1_azureWithNoUsername", machine51azureWithNoUsername)
		aux("machine_5_2_azureWithNoUsername", machine52azureWithBadUsername)
	case "gcp":
		aux("machine_6_1_gcpWithNoUsername", machine61gcpWithNoUsername)
	case "k8s":
		aux("machine_k8s", machinek8s)
	default:
		log.Fatal("Unknown OIDC_ENV: ", env)
	}
	if hasError {
		log.Fatal("One or more tests failed")
	}
}

func machine11callbackIsCalled() error {
	callbackCount := 0
	var callbackFailed error
	countMutex := sync.Mutex{}

	client, err := connectWithMachineCB(uriSingle, func(context.Context, *options.OIDCArgs) (*options.OIDCCredential, error) {
		countMutex.Lock()
		defer countMutex.Unlock()
		callbackCount++
		t := time.Now().Add(time.Hour)
		tokenFile := tokenFile("test_user1")
		accessToken, err := os.ReadFile(tokenFile)
		if err != nil {
			callbackFailed = fmt.Errorf("machine_1_1: failed reading token file: %v", err)
		}
		return &options.OIDCCredential{
			AccessToken:  string(accessToken),
			ExpiresAt:    &t,
			RefreshToken: nil,
		}, nil
	})

	defer func() { _ = client.Disconnect(context.Background()) }()

	if err != nil {
		return fmt.Errorf("machine_1_1: failed connecting client: %v", err)
	}

	coll := client.Database("test").Collection("test")

	_, err = coll.Find(context.Background(), bson.D{})
	if err != nil {
		return fmt.Errorf("machine_1_1: failed executing Find: %v", err)
	}
	countMutex.Lock()
	defer countMutex.Unlock()
	if callbackCount != 1 {
		return fmt.Errorf("machine_1_1: expected callback count to be 1, got %d", callbackCount)
	}
	return callbackFailed
}

func machine12callbackIsCalledOnlyOneForMultipleConnections() error {
	callbackCount := 0
	var callbackFailed error
	countMutex := sync.Mutex{}

	client, err := connectWithMachineCB(uriSingle, func(context.Context, *options.OIDCArgs) (*options.OIDCCredential, error) {
		countMutex.Lock()
		defer countMutex.Unlock()
		callbackCount++
		t := time.Now().Add(time.Hour)
		tokenFile := tokenFile("test_user1")
		accessToken, err := os.ReadFile(tokenFile)
		if err != nil {
			callbackFailed = fmt.Errorf("machine_1_2: failed reading token file: %v", err)
		}
		return &options.OIDCCredential{
			AccessToken:  string(accessToken),
			ExpiresAt:    &t,
			RefreshToken: nil,
		}, nil
	})

	defer func() { _ = client.Disconnect(context.Background()) }()

	if err != nil {
		return fmt.Errorf("machine_1_2: failed connecting client: %v", err)
	}

	var wg sync.WaitGroup

	var findFailed error
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			coll := client.Database("test").Collection("test")
			_, err := coll.Find(context.Background(), bson.D{})
			if err != nil {
				findFailed = fmt.Errorf("machine_1_2: failed executing Find: %v", err)
			}
		}()
	}

	wg.Wait()
	countMutex.Lock()
	defer countMutex.Unlock()
	if callbackCount != 1 {
		return fmt.Errorf("machine_1_2: expected callback count to be 1, got %d", callbackCount)
	}
	if callbackFailed != nil {
		return callbackFailed
	}
	return findFailed
}

func machine21validCallbackInputs() error {
	callbackCount := 0
	var callbackFailed error
	countMutex := sync.Mutex{}

	client, err := connectWithMachineCB(uriSingle, func(ctx context.Context, args *options.OIDCArgs) (*options.OIDCCredential, error) {
		if args.RefreshToken != nil {
			callbackFailed = fmt.Errorf("machine_2_1: expected RefreshToken to be nil, got %v", args.RefreshToken)
		}
		timeout, ok := ctx.Deadline()
		if !ok {
			callbackFailed = fmt.Errorf("machine_2_1: expected context to have deadline, got %v", ctx)
		}
		if timeout.Before(time.Now()) {
			callbackFailed = fmt.Errorf("machine_2_1: expected timeout to be in the future, got %v", timeout)
		}
		if args.Version < 1 {
			callbackFailed = fmt.Errorf("machine_2_1: expected Version to be at least 1, got %d", args.Version)
		}
		if args.IDPInfo != nil {
			callbackFailed = fmt.Errorf("machine_2_1: expected IdpID to be nil for Machine flow, got %v", args.IDPInfo)
		}
		countMutex.Lock()
		defer countMutex.Unlock()
		callbackCount++
		t := time.Now().Add(time.Hour)
		tokenFile := tokenFile("test_user1")
		accessToken, err := os.ReadFile(tokenFile)
		if err != nil {
			return nil, fmt.Errorf("machine_2_1: failed reading token file: %w", err)
		}
		return &options.OIDCCredential{
			AccessToken:  string(accessToken),
			ExpiresAt:    &t,
			RefreshToken: nil,
		}, nil
	})

	defer func() { _ = client.Disconnect(context.Background()) }()

	if err != nil {
		return fmt.Errorf("machine_2_1: failed connecting client: %v", err)
	}

	coll := client.Database("test").Collection("test")

	_, err = coll.Find(context.Background(), bson.D{})
	if err != nil {
		return fmt.Errorf("machine_2_1: failed executing Find: %v", err)
	}
	countMutex.Lock()
	defer countMutex.Unlock()
	if callbackCount != 1 {
		return fmt.Errorf("machine_2_1: expected callback count to be 1, got %d", callbackCount)
	}
	return callbackFailed
}

func machine23oidcCallbackReturnMissingData() error {
	callbackCount := 0
	countMutex := sync.Mutex{}

	client, err := connectWithMachineCB(uriSingle, func(context.Context, *options.OIDCArgs) (*options.OIDCCredential, error) {
		countMutex.Lock()
		defer countMutex.Unlock()
		callbackCount++
		t := time.Now().Add(time.Hour)
		return &options.OIDCCredential{
			AccessToken:  "",
			ExpiresAt:    &t,
			RefreshToken: nil,
		}, nil
	})

	defer func() { _ = client.Disconnect(context.Background()) }()

	if err != nil {
		return fmt.Errorf("machine_2_3: failed connecting client: %v", err)
	}

	coll := client.Database("test").Collection("test")

	_, err = coll.Find(context.Background(), bson.D{})
	if err == nil {
		return fmt.Errorf("machine_2_3: should have failed to executed Find, but succeeded")
	}
	countMutex.Lock()
	defer countMutex.Unlock()
	if callbackCount != 1 {
		return fmt.Errorf("machine_2_3: expected callback count to be 1, got %d", callbackCount)
	}
	return nil
}

func machine24invalidClientConfigurationWithCallback() error {
	_, err := connectWithMachineCBAndProperties(uriSingle, func(context.Context, *options.OIDCArgs) (*options.OIDCCredential, error) {
		t := time.Now().Add(time.Hour)
		return &options.OIDCCredential{
			AccessToken:  "",
			ExpiresAt:    &t,
			RefreshToken: nil,
		}, nil
	},
		map[string]string{"ENVIRONMENT": "test"},
	)
	if err == nil {
		return fmt.Errorf("machine_2_4: succeeded building client when it should fail")
	}
	return nil
}

func machine25InvalidUseofAllowedHosts() error {
	_, err := connectWithMachineCBAndProperties(uriSingle, func(_ context.Context, _ *options.OIDCArgs) (*options.OIDCCredential, error) {
		t := time.Now().Add(time.Hour)
		return &options.OIDCCredential{
			AccessToken:  "",
			ExpiresAt:    &t,
			RefreshToken: nil,
		}, nil
	},
		map[string]string{
			"ENVIRONMENT":   "azure",
			"ALLOWED_HOSTS": "",
		},
	)
	if err == nil {
		return fmt.Errorf("machine_2_5: succeeded building client when it should fail")
	}
	return nil
}

func machine31failureWithCachedTokensFetchANewTokenAndRetryAuth() error {
	callbackCount := 0
	var callbackFailed error
	countMutex := sync.Mutex{}

	client, err := connectWithMachineCB(uriSingle, func(context.Context, *options.OIDCArgs) (*options.OIDCCredential, error) {
		countMutex.Lock()
		defer countMutex.Unlock()
		callbackCount++
		t := time.Now().Add(time.Hour)
		tokenFile := tokenFile("test_user1")
		accessToken, err := os.ReadFile(tokenFile)
		if err != nil {
			callbackFailed = fmt.Errorf("machine_3_1: failed reading token file: %v", err)
		}
		return &options.OIDCCredential{
			AccessToken:  string(accessToken),
			ExpiresAt:    &t,
			RefreshToken: nil,
		}, nil
	})

	defer func() { _ = client.Disconnect(context.Background()) }()

	if err != nil {
		return fmt.Errorf("machine_3_1: failed connecting client: %v", err)
	}

	// Poison the cache with a random token
	clientElem := reflect.ValueOf(client).Elem()
	authenticatorField := clientElem.FieldByName("authenticator")
	authenticatorField = reflect.NewAt(
		authenticatorField.Type(),
		unsafe.Pointer(authenticatorField.UnsafeAddr())).Elem()
	// this is the only usage of the x packages in the test, showing the public interface is
	// correct.
	authenticatorField.Interface().(*auth.OIDCAuthenticator).SetAccessToken("some random happy sunshine string")

	coll := client.Database("test").Collection("test")

	_, err = coll.Find(context.Background(), bson.D{})
	if err != nil {
		return fmt.Errorf("machine_3_1: failed executing Find: %v", err)
	}
	countMutex.Lock()
	defer countMutex.Unlock()
	if callbackCount != 1 {
		return fmt.Errorf("machine_3_1: expected callback count to be 1, got %d", callbackCount)
	}
	return callbackFailed
}

func machine32authFailuresWithoutCachedTokensReturnsAnError() error {
	callbackCount := 0
	var callbackFailed error
	countMutex := sync.Mutex{}

	client, err := connectWithMachineCB(uriSingle, func(context.Context, *options.OIDCArgs) (*options.OIDCCredential, error) {
		countMutex.Lock()
		defer countMutex.Unlock()
		callbackCount++
		t := time.Now().Add(time.Hour)
		return &options.OIDCCredential{
			AccessToken:  "this is a bad, bad token",
			ExpiresAt:    &t,
			RefreshToken: nil,
		}, nil
	})

	defer func() { _ = client.Disconnect(context.Background()) }()

	if err != nil {
		return fmt.Errorf("machine_3_2: failed connecting client: %v", err)
	}

	coll := client.Database("test").Collection("test")
	_, err = coll.Find(context.Background(), bson.D{})
	if err == nil {
		return fmt.Errorf("machine_3_2: Find ucceeded when it should fail")
	}
	countMutex.Lock()
	defer countMutex.Unlock()
	if callbackCount != 1 {
		return fmt.Errorf("machine_3_2: expected callback count to be 1, got %d", callbackCount)
	}
	return callbackFailed
}

func machine33UnexpectedErrorCodeDoesNotClearTheCache() error {
	callbackCount := 0
	var callbackFailed error
	countMutex := sync.Mutex{}

	adminClient, err := connectAdminClient()

	defer func() { _ = adminClient.Disconnect(context.Background()) }()

	if err != nil {
		return fmt.Errorf("machine_3_3: failed connecting admin client: %v", err)
	}

	client, err := connectWithMachineCB(uriSingle, func(context.Context, *options.OIDCArgs) (*options.OIDCCredential, error) {
		countMutex.Lock()
		defer countMutex.Unlock()
		callbackCount++
		t := time.Now().Add(time.Hour)
		tokenFile := tokenFile("test_user1")
		accessToken, err := os.ReadFile(tokenFile)
		if err != nil {
			callbackFailed = fmt.Errorf("machine_3_3: failed reading token file: %v", err)
		}
		return &options.OIDCCredential{
			AccessToken:  string(accessToken),
			ExpiresAt:    &t,
			RefreshToken: nil,
		}, nil
	})

	defer func() { _ = client.Disconnect(context.Background()) }()

	if err != nil {
		return fmt.Errorf("machine_3_3: failed connecting client: %v", err)
	}

	coll := client.Database("test").Collection("test")

	res := adminClient.Database("admin").RunCommand(context.Background(), bson.D{
		{Key: "configureFailPoint", Value: "failCommand"},
		{Key: "mode", Value: bson.D{
			{Key: "times", Value: 1},
		}},
		{Key: "data", Value: bson.D{
			{Key: "failCommands", Value: bson.A{
				"saslStart",
			}},
			{Key: "errorCode", Value: 20},
		}},
	})

	if res.Err() != nil {
		return fmt.Errorf("machine_3_3: failed setting failpoint: %v", res.Err())
	}

	_, err = coll.Find(context.Background(), bson.D{})
	if err == nil {
		return fmt.Errorf("machine_3_3: Find succeeded when it should fail")
	}

	countMutex.Lock()
	defer countMutex.Unlock()
	if callbackCount != 1 {
		return fmt.Errorf("machine_3_3: expected callback count to be 1, got %d", callbackCount)
	}

	_, err = coll.Find(context.Background(), bson.D{})
	if err != nil {
		return fmt.Errorf("machine_3_3: failed executing Find: %v", err)
	}
	if callbackCount != 1 {
		return fmt.Errorf("machine_3_3: expected callback count to be 1, got %d", callbackCount)
	}
	return callbackFailed
}

func machine41ReauthenticationSucceeds() error {
	callbackCount := 0
	var callbackFailed error
	countMutex := sync.Mutex{}

	adminClient, err := connectAdminClient()
	defer func() { _ = adminClient.Disconnect(context.Background()) }()

	if err != nil {
		return fmt.Errorf("machine_4_1: failed connecting admin client: %v", err)
	}
	defer func() { _ = adminClient.Disconnect(context.Background()) }()

	client, err := connectWithMachineCB(uriSingle, func(context.Context, *options.OIDCArgs) (*options.OIDCCredential, error) {
		countMutex.Lock()
		defer countMutex.Unlock()
		callbackCount++
		t := time.Now().Add(time.Hour)
		tokenFile := tokenFile("test_user1")
		accessToken, err := os.ReadFile(tokenFile)
		if err != nil {
			callbackFailed = fmt.Errorf("machine_4_1: failed reading token file: %v", err)
		}
		return &options.OIDCCredential{
			AccessToken:  string(accessToken),
			ExpiresAt:    &t,
			RefreshToken: nil,
		}, nil
	})

	defer func() { _ = client.Disconnect(context.Background()) }()

	if err != nil {
		return fmt.Errorf("machine_4_1: failed connecting client: %v", err)
	}

	coll := client.Database("test").Collection("test")
	res := adminClient.Database("admin").RunCommand(context.Background(), bson.D{
		{Key: "configureFailPoint", Value: "failCommand"},
		{Key: "mode", Value: bson.D{
			{Key: "times", Value: 1},
		}},
		{Key: "data", Value: bson.D{
			{Key: "failCommands", Value: bson.A{
				"find",
			}},
			{Key: "errorCode", Value: 391},
		}},
	})

	if res.Err() != nil {
		return fmt.Errorf("machine_4_1: failed setting failpoint: %v", res.Err())
	}

	_, err = coll.Find(context.Background(), bson.D{})
	if err != nil {
		return fmt.Errorf("machine_4_1: failed executing Find: %v", err)
	}
	countMutex.Lock()
	defer countMutex.Unlock()
	if callbackCount != 2 {
		return fmt.Errorf("machine_4_1: expected callback count to be 2, got %d", callbackCount)
	}
	return callbackFailed
}

func machine42ReadCommandsFailIfReauthenticationFails() error {
	callbackCount := 0
	var callbackFailed error
	firstCall := true
	countMutex := sync.Mutex{}

	adminClient, err := connectAdminClient()
	defer func() { _ = adminClient.Disconnect(context.Background()) }()

	if err != nil {
		return fmt.Errorf("machine_4_2: failed connecting admin client: %v", err)
	}
	defer func() { _ = adminClient.Disconnect(context.Background()) }()

	client, err := connectWithMachineCB(uriSingle, func(context.Context, *options.OIDCArgs) (*options.OIDCCredential, error) {
		countMutex.Lock()
		defer countMutex.Unlock()
		callbackCount++
		t := time.Now().Add(time.Hour)
		if firstCall {
			firstCall = false
			tokenFile := tokenFile("test_user1")
			accessToken, err := os.ReadFile(tokenFile)
			if err != nil {
				callbackFailed = fmt.Errorf("machine_4_2: failed reading token file: %v", err)
			}
			return &options.OIDCCredential{
				AccessToken:  string(accessToken),
				ExpiresAt:    &t,
				RefreshToken: nil,
			}, nil
		}
		return &options.OIDCCredential{
			AccessToken:  "this is a bad, bad token",
			ExpiresAt:    &t,
			RefreshToken: nil,
		}, nil

	})

	defer func() { _ = client.Disconnect(context.Background()) }()

	if err != nil {
		return fmt.Errorf("machine_4_2: failed connecting client: %v", err)
	}

	coll := client.Database("test").Collection("test")
	_, err = coll.Find(context.Background(), bson.D{})
	if err != nil {
		return fmt.Errorf("machine_4_2: failed executing Find: %v", err)
	}

	res := adminClient.Database("admin").RunCommand(context.Background(), bson.D{
		{Key: "configureFailPoint", Value: "failCommand"},
		{Key: "mode", Value: bson.D{
			{Key: "times", Value: 1},
		}},
		{Key: "data", Value: bson.D{
			{Key: "failCommands", Value: bson.A{
				"find",
			}},
			{Key: "errorCode", Value: 391},
		}},
	})

	if res.Err() != nil {
		return fmt.Errorf("machine_4_2: failed setting failpoint: %v", res.Err())
	}

	_, err = coll.Find(context.Background(), bson.D{})
	if err == nil {
		return fmt.Errorf("machine_4_2: Find succeeded when it should fail")
	}

	countMutex.Lock()
	defer countMutex.Unlock()
	if callbackCount != 2 {
		return fmt.Errorf("machine_4_2: expected callback count to be 2, got %d", callbackCount)
	}
	return callbackFailed
}

func machine43WriteCommandsFailIfReauthenticationFails() error {
	callbackCount := 0
	var callbackFailed error
	firstCall := true
	countMutex := sync.Mutex{}

	adminClient, err := connectAdminClient()
	defer func() { _ = adminClient.Disconnect(context.Background()) }()

	if err != nil {
		return fmt.Errorf("machine_4_3: failed connecting admin client: %v", err)
	}
	defer func() { _ = adminClient.Disconnect(context.Background()) }()

	client, err := connectWithMachineCB(uriSingle, func(context.Context, *options.OIDCArgs) (*options.OIDCCredential, error) {
		countMutex.Lock()
		defer countMutex.Unlock()
		callbackCount++
		t := time.Now().Add(time.Hour)
		if firstCall {
			firstCall = false
			tokenFile := tokenFile("test_user1")
			accessToken, err := os.ReadFile(tokenFile)
			if err != nil {
				callbackFailed = fmt.Errorf("machine_4_3: failed reading token file: %v", err)
			}
			return &options.OIDCCredential{
				AccessToken:  string(accessToken),
				ExpiresAt:    &t,
				RefreshToken: nil,
			}, nil
		}
		return &options.OIDCCredential{
			AccessToken:  "this is a bad, bad token",
			ExpiresAt:    &t,
			RefreshToken: nil,
		}, nil
	})

	defer func() { _ = client.Disconnect(context.Background()) }()

	if err != nil {
		return fmt.Errorf("machine_4_3: failed connecting client: %v", err)
	}

	coll := client.Database("test").Collection("test")
	_, err = coll.InsertOne(context.Background(), bson.D{})
	if err != nil {
		return fmt.Errorf("machine_4_3: failed executing Insert: %v", err)
	}

	res := adminClient.Database("admin").RunCommand(context.Background(), bson.D{
		{Key: "configureFailPoint", Value: "failCommand"},
		{Key: "mode", Value: bson.D{
			{Key: "times", Value: 1},
		}},
		{Key: "data", Value: bson.D{
			{Key: "failCommands", Value: bson.A{
				"insert",
			}},
			{Key: "errorCode", Value: 391},
		}},
	})

	if res.Err() != nil {
		return fmt.Errorf("machine_4_3: failed setting failpoint: %v", res.Err())
	}

	_, err = coll.InsertOne(context.Background(), bson.D{})
	if err == nil {
		return fmt.Errorf("machine_4_3: Insert succeeded when it should fail")
	}

	countMutex.Lock()
	defer countMutex.Unlock()
	if callbackCount != 2 {
		return fmt.Errorf("machine_4_3: expected callback count to be 2, got %d", callbackCount)
	}
	return callbackFailed
}

func human11singlePrincipalImplictUsername() error {
	callbackCount := 0
	var callbackFailed error
	countMutex := sync.Mutex{}

	client, err := connectWithHumanCB(uriSingle, func(context.Context, *options.OIDCArgs) (*options.OIDCCredential, error) {
		countMutex.Lock()
		defer countMutex.Unlock()
		callbackCount++
		t := time.Now().Add(time.Hour)
		tokenFile := tokenFile("test_user1")
		accessToken, err := os.ReadFile(tokenFile)
		if err != nil {
			callbackFailed = fmt.Errorf("human_1_1: failed reading token file: %v", err)
		}
		return &options.OIDCCredential{
			AccessToken:  string(accessToken),
			ExpiresAt:    &t,
			RefreshToken: nil,
		}, nil
	})

	defer func() { _ = client.Disconnect(context.Background()) }()

	if err != nil {
		return fmt.Errorf("human_1_1: failed connecting client: %v", err)
	}

	coll := client.Database("test").Collection("test")

	_, err = coll.Find(context.Background(), bson.D{})
	if err != nil {
		return fmt.Errorf("human_1_1: failed executing Find: %v", err)
	}
	countMutex.Lock()
	defer countMutex.Unlock()
	if callbackCount != 1 {
		return fmt.Errorf("human_1_1: expected callback count to be 1, got %d", callbackCount)
	}
	return callbackFailed
}

func human12singlePrincipalExplicitUsername() error {
	callbackCount := 0
	var callbackFailed error
	countMutex := sync.Mutex{}

	client, err := connectWithHumanCBAndUser(uriSingle, "test_user1", func(_ context.Context, _ *options.OIDCArgs) (*options.OIDCCredential, error) {
		countMutex.Lock()
		defer countMutex.Unlock()
		callbackCount++
		t := time.Now().Add(time.Hour)
		tokenFile := tokenFile("test_user1")
		accessToken, err := os.ReadFile(tokenFile)
		if err != nil {
			callbackFailed = fmt.Errorf("human_1_2: failed reading token file: %v", err)
		}
		return &options.OIDCCredential{
			AccessToken:  string(accessToken),
			ExpiresAt:    &t,
			RefreshToken: nil,
		}, nil
	})
	if err != nil {
		return fmt.Errorf("human_1_2: failed connecting client: %v", err)
	}
	defer func() { _ = client.Disconnect(context.Background()) }()

	coll := client.Database("test").Collection("test")

	_, err = coll.Find(context.Background(), bson.D{})
	if err != nil {
		return fmt.Errorf("human_1_2: failed executing Find: %v", err)
	}
	countMutex.Lock()
	defer countMutex.Unlock()
	if callbackCount != 1 {
		return fmt.Errorf("human_1_2: expected callback count to be 1, got %d", callbackCount)
	}
	return callbackFailed
}

func human13mulitplePrincipalUser1() error {
	callbackCount := 0
	var callbackFailed error
	countMutex := sync.Mutex{}
	cb := func(context.Context, *options.OIDCArgs) (*options.OIDCCredential, error) {
		countMutex.Lock()
		defer countMutex.Unlock()
		callbackCount++
		t := time.Now().Add(time.Hour)
		tokenFile := tokenFile("test_user1")
		accessToken, err := os.ReadFile(tokenFile)
		if err != nil {
			callbackFailed = fmt.Errorf("human_1_3: failed reading token file: %v", err)
		}
		return &options.OIDCCredential{
			AccessToken:  string(accessToken),
			ExpiresAt:    &t,
			RefreshToken: nil,
		}, nil
	}
	cred := options.Credential{
		AuthMechanism:     "MONGODB-OIDC",
		OIDCHumanCallback: cb,
		Username:          explicitUser("test_user1"),
	}
	opts := options.Client().ApplyURI(uriMulti).SetAuth(cred)
	client, err := mongo.Connect(opts)
	if err != nil {
		return fmt.Errorf("human_1_3: failed connecting client: %v", err)
	}
	defer func() { _ = client.Disconnect(context.Background()) }()

	coll := client.Database("test").Collection("test")

	_, err = coll.Find(context.Background(), bson.D{})
	if err != nil {
		return fmt.Errorf("human_1_3: failed executing Find: %v", err)
	}
	countMutex.Lock()
	defer countMutex.Unlock()
	if callbackCount != 1 {
		return fmt.Errorf("human_1_3: expected callback count to be 1, got %d", callbackCount)
	}
	return callbackFailed
}

func human14mulitplePrincipalUser2() error {
	callbackCount := 0
	var callbackFailed error
	countMutex := sync.Mutex{}
	cb := func(context.Context, *options.OIDCArgs) (*options.OIDCCredential, error) {
		countMutex.Lock()
		defer countMutex.Unlock()
		callbackCount++
		t := time.Now().Add(time.Hour)
		tokenFile := tokenFile("test_user2")
		accessToken, err := os.ReadFile(tokenFile)
		if err != nil {
			callbackFailed = fmt.Errorf("human_1_4: failed reading token file: %v", err)
		}
		return &options.OIDCCredential{
			AccessToken:  string(accessToken),
			ExpiresAt:    &t,
			RefreshToken: nil,
		}, nil
	}
	cred := options.Credential{
		AuthMechanism:     "MONGODB-OIDC",
		OIDCHumanCallback: cb,
		Username:          explicitUser("test_user2"),
	}
	opts := options.Client().ApplyURI(uriMulti).SetAuth(cred)
	client, err := mongo.Connect(opts)
	if err != nil {
		return fmt.Errorf("human_1_4: failed connecting client: %v", err)
	}
	defer func() { _ = client.Disconnect(context.Background()) }()

	coll := client.Database("test").Collection("test")

	_, err = coll.Find(context.Background(), bson.D{})
	if err != nil {
		return fmt.Errorf("human_1_4: failed executing Find: %v", err)
	}
	countMutex.Lock()
	defer countMutex.Unlock()
	if callbackCount != 1 {
		return fmt.Errorf("human_1_4: expected callback count to be 1, got %d", callbackCount)
	}
	return callbackFailed
}

func human15mulitplePrincipalNoUser() error {
	callbackCount := 0
	var callbackFailed error
	countMutex := sync.Mutex{}

	client, err := connectWithHumanCB(uriMulti, func(context.Context, *options.OIDCArgs) (*options.OIDCCredential, error) {
		countMutex.Lock()
		defer countMutex.Unlock()
		callbackCount++
		t := time.Now().Add(time.Hour)
		tokenFile := tokenFile("test_user1")
		accessToken, err := os.ReadFile(tokenFile)
		if err != nil {
			callbackFailed = fmt.Errorf("human_1_5: failed reading token file: %v", err)
		}
		return &options.OIDCCredential{
			AccessToken:  string(accessToken),
			ExpiresAt:    &t,
			RefreshToken: nil,
		}, nil
	})
	if err != nil {
		return fmt.Errorf("human_1_5: failed connecting client: %v", err)
	}
	defer func() { _ = client.Disconnect(context.Background()) }()

	coll := client.Database("test").Collection("test")

	_, err = coll.Find(context.Background(), bson.D{})
	if err == nil {
		return fmt.Errorf("human_1_5: Find succeeded when it should fail")
	}
	countMutex.Lock()
	defer countMutex.Unlock()
	if callbackCount != 0 {
		return fmt.Errorf("human_1_5: expected callback count to be 0, got %d", callbackCount)
	}
	return callbackFailed
}

func human16allowedHostsBlocked() error {
	var callbackFailed error
	{
		cb := func(context.Context, *options.OIDCArgs) (*options.OIDCCredential, error) {
			t := time.Now().Add(time.Hour)
			tokenFile := tokenFile("test_user1")
			accessToken, err := os.ReadFile(tokenFile)
			if err != nil {
				callbackFailed = fmt.Errorf("human_1_6: failed reading token file: %v", err)
			}
			return &options.OIDCCredential{
				AccessToken:  string(accessToken),
				ExpiresAt:    &t,
				RefreshToken: nil,
			}, nil
		}
		cred := options.Credential{
			AuthMechanism:           "MONGODB-OIDC",
			OIDCHumanCallback:       cb,
			AuthMechanismProperties: map[string]string{"ALLOWED_HOSTS": ""},
		}
		opts := options.Client().ApplyURI(uriMulti).SetAuth(cred)
		client, err := mongo.Connect(opts)
		if err != nil {
			return fmt.Errorf("human_1_4: failed connecting client: %v", err)
		}
		defer func() { _ = client.Disconnect(context.Background()) }()

		coll := client.Database("test").Collection("test")

		_, err = coll.Find(context.Background(), bson.D{})
		if err == nil {
			return fmt.Errorf("machine_1_6: Find succeeded when it should fail with empty 'ALLOWED_HOSTS'")
		}
	}
	{
		cb := func(context.Context, *options.OIDCArgs) (*options.OIDCCredential, error) {
			t := time.Now().Add(time.Hour)
			tokenFile := tokenFile("test_user1")
			accessToken, err := os.ReadFile(tokenFile)
			if err != nil {
				callbackFailed = fmt.Errorf("human_1_6: failed reading token file: %v", err)
			}
			return &options.OIDCCredential{
				AccessToken:  string(accessToken),
				ExpiresAt:    &t,
				RefreshToken: nil,
			}, nil
		}
		cred := options.Credential{
			AuthMechanism:           "MONGODB-OIDC",
			OIDCHumanCallback:       cb,
			AuthMechanismProperties: map[string]string{"ALLOWED_HOSTS": "example.com"},
		}
		opts := options.Client().ApplyURI("mongodb://localhost/?authMechanism=MONGODB-OIDC&ignored=example.com").SetAuth(cred)
		client, err := mongo.Connect(opts)
		if err != nil {
			return fmt.Errorf("human_1_4: failed connecting client: %v", err)
		}
		defer func() { _ = client.Disconnect(context.Background()) }()

		coll := client.Database("test").Collection("test")

		_, err = coll.Find(context.Background(), bson.D{})
		if err == nil {
			return fmt.Errorf("machine_1_6: Find succeeded when it should fail with 'ALLOWED_HOSTS' 'example.com'")
		}
	}
	return callbackFailed
}

func human17AllowedHostsInConnectionStringIgnored() error {
	uri := "mongodb+srv://example.com/?authMechanism=MONGODB-OIDC&authMechanismProperties=ALLOWED_HOSTS:%5B%22example.com%22%5D"
	opts := options.Client().ApplyURI(uri)
	err := opts.Validate()
	if err == nil {
		return fmt.Errorf("human_1_7: succeeded in applying URI which should produce an error")
	}
	return nil
}

func human18MachineIDPHumanCallback() error {
	if _, ok := os.LookupEnv("OIDC_IS_LOCAL"); !ok {
		return nil
	}
	callbackCount := 0

	var callbackFailed error
	countMutex := sync.Mutex{}

	client, err := connectWithHumanCBAndUser(uriSingle, "test_machine", func(_ context.Context, _ *options.OIDCArgs) (*options.OIDCCredential, error) {
		countMutex.Lock()
		defer countMutex.Unlock()
		callbackCount++
		t := time.Now().Add(time.Hour)
		tokenFile := tokenFile("test_machine")
		accessToken, err := os.ReadFile(tokenFile)
		if err != nil {
			callbackFailed = fmt.Errorf("human_1_8: failed reading token file: %v", err)
		}
		return &options.OIDCCredential{
			AccessToken:  string(accessToken),
			ExpiresAt:    &t,
			RefreshToken: nil,
		}, nil
	})

	defer func() { _ = client.Disconnect(context.Background()) }()

	if err != nil {
		return fmt.Errorf("human_1_8: failed connecting client: %v", err)
	}

	coll := client.Database("test").Collection("test")

	_, err = coll.Find(context.Background(), bson.D{})
	if err != nil {
		return fmt.Errorf("human_1_8: failed executing Find: %v", err)
	}
	countMutex.Lock()
	defer countMutex.Unlock()
	if callbackCount != 1 {
		return fmt.Errorf("human_1_8: expected callback count to be 1, got %d", callbackCount)
	}
	return callbackFailed

}

func human21validCallbackInputs() error {
	callbackCount := 0
	var callbackFailed error
	countMutex := sync.Mutex{}

	client, err := connectWithHumanCB(uriSingle, func(_ context.Context, args *options.OIDCArgs) (*options.OIDCCredential, error) {
		countMutex.Lock()
		defer countMutex.Unlock()
		callbackCount++
		t := time.Now().Add(time.Hour)
		if args.Version != 1 {
			callbackFailed = fmt.Errorf("human_2_1: expected version to be 1, got %d", args.Version)
		}
		if args.IDPInfo == nil {
			callbackFailed = fmt.Errorf("human_2_1: expected IDPInfo to be non-nil, previous error: (%v)", callbackFailed)
		}
		tokenFile := tokenFile("test_user1")
		accessToken, err := os.ReadFile(tokenFile)
		if err != nil {
			callbackFailed = fmt.Errorf("human_2_1: failed reading token file: %v, previous error: (%v)", err, callbackFailed)
		}
		return &options.OIDCCredential{
			AccessToken:  string(accessToken),
			ExpiresAt:    &t,
			RefreshToken: nil,
		}, nil
	})

	defer func() { _ = client.Disconnect(context.Background()) }()

	if err != nil {
		return fmt.Errorf("human_2_1: failed connecting client: %v", err)
	}

	coll := client.Database("test").Collection("test")

	_, err = coll.Find(context.Background(), bson.D{})
	if err != nil {
		return fmt.Errorf("human_2_1: failed executing Find: %v", err)
	}
	countMutex.Lock()
	defer countMutex.Unlock()
	if callbackCount != 1 {
		return fmt.Errorf("human_2_1: expected callback count to be 1, got %d", callbackCount)
	}
	return callbackFailed
}

func human22CallbackReturnsMissingData() error {
	callbackCount := 0
	countMutex := sync.Mutex{}

	client, err := connectWithHumanCB(uriSingle, func(context.Context, *options.OIDCArgs) (*options.OIDCCredential, error) {
		countMutex.Lock()
		defer countMutex.Unlock()
		callbackCount++
		return &options.OIDCCredential{}, nil
	})

	defer func() { _ = client.Disconnect(context.Background()) }()

	if err != nil {
		return fmt.Errorf("human_2_2: failed connecting client: %v", err)
	}

	coll := client.Database("test").Collection("test")

	_, err = coll.Find(context.Background(), bson.D{})
	if err == nil {
		return fmt.Errorf("human_2_2: Find succeeded when it should fail")
	}
	countMutex.Lock()
	defer countMutex.Unlock()
	if callbackCount != 1 {
		return fmt.Errorf("human_2_2: expected callback count to be 1, got %d", callbackCount)
	}
	return nil
}

func human23RefreshTokenIsPassedToCallback() error {
	callbackCount := 0
	var callbackFailed error
	countMutex := sync.Mutex{}

	adminClient, err := connectAdminClient()
	if err != nil {
		return fmt.Errorf("human_2_3: failed connecting admin client: %v", err)
	}
	defer func() { _ = adminClient.Disconnect(context.Background()) }()

	client, err := connectWithHumanCB(uriSingle, func(_ context.Context, args *options.OIDCArgs) (*options.OIDCCredential, error) {
		countMutex.Lock()
		defer countMutex.Unlock()
		callbackCount++
		if callbackCount == 1 && args.RefreshToken != nil {
			callbackFailed = fmt.Errorf("human_2_3: expected refresh token to be nil first time, got %v, previous error: (%v)", args.RefreshToken, callbackFailed)
		}
		if callbackCount == 2 && args.RefreshToken == nil {
			callbackFailed = fmt.Errorf("human_2_3: expected refresh token to be non-nil second time, got %v, previous error: (%v)", args.RefreshToken, callbackFailed)
		}
		t := time.Now().Add(time.Hour)
		tokenFile := tokenFile("test_user1")
		accessToken, err := os.ReadFile(tokenFile)
		if err != nil {
			callbackFailed = fmt.Errorf("human_2_3: failed reading token file: %v", err)
		}
		rt := "this is fake"
		return &options.OIDCCredential{
			AccessToken:  string(accessToken),
			ExpiresAt:    &t,
			RefreshToken: &rt,
		}, nil
	})

	defer func() { _ = client.Disconnect(context.Background()) }()

	if err != nil {
		return fmt.Errorf("human_2_3: failed connecting client: %v", err)
	}

	res := adminClient.Database("admin").RunCommand(context.Background(), bson.D{
		{Key: "configureFailPoint", Value: "failCommand"},
		{Key: "mode", Value: bson.D{
			{Key: "times", Value: 1},
		}},
		{Key: "data", Value: bson.D{
			{Key: "failCommands", Value: bson.A{
				"find",
			}},
			{Key: "errorCode", Value: 391},
		}},
	})

	if res.Err() != nil {
		return fmt.Errorf("human_2_3: failed to set failpoint")
	}

	coll := client.Database("test").Collection("test")

	_, err = coll.Find(context.Background(), bson.D{})
	if err != nil {
		return fmt.Errorf("human_2_3: failed executing Find: %v", err)
	}
	countMutex.Lock()
	defer countMutex.Unlock()
	if callbackCount != 2 {
		return fmt.Errorf("human_2_3: expected callback count to be 2, got %d", callbackCount)
	}
	return callbackFailed
}

func human31usesSpeculativeAuth() error {
	adminClient, err := connectAdminClient()
	if err != nil {
		return fmt.Errorf("human_3_1: failed connecting admin client: %v", err)
	}
	defer func() { _ = adminClient.Disconnect(context.Background()) }()

	client, err := connectWithHumanCB(uriSingle, func(context.Context, *options.OIDCArgs) (*options.OIDCCredential, error) {
		// the callback should not even be called due to spec auth.
		return &options.OIDCCredential{}, nil
	})

	if err != nil {
		return fmt.Errorf("human_3_1: failed connecting client: %v", err)
	}
	defer func() { _ = client.Disconnect(context.Background()) }()

	// We deviate from the Prose test since the failPoint on find with no error code does not seem to
	// work. Rather we put an access token in the cache to force speculative auth.
	tokenFile := tokenFile("test_user1")
	accessToken, err := os.ReadFile(tokenFile)
	if err != nil {
		return fmt.Errorf("human_3_1: failed reading token file: %v", err)
	}
	clientElem := reflect.ValueOf(client).Elem()
	authenticatorField := clientElem.FieldByName("authenticator")
	authenticatorField = reflect.NewAt(
		authenticatorField.Type(),
		unsafe.Pointer(authenticatorField.UnsafeAddr())).Elem()
	// This is the only usage of the x packages in the test, showing the public interface is
	// correct.
	authenticatorField.Interface().(*auth.OIDCAuthenticator).SetAccessToken(string(accessToken))

	res := adminClient.Database("admin").RunCommand(context.Background(), bson.D{
		{Key: "configureFailPoint", Value: "failCommand"},
		{Key: "mode", Value: bson.D{
			{Key: "times", Value: 1},
		}},
		{Key: "data", Value: bson.D{
			{Key: "failCommands", Value: bson.A{
				"saslStart",
			}},
			{Key: "errorCode", Value: 18},
		}},
	})

	if res.Err() != nil {
		return fmt.Errorf("human_3_1: failed to set failpoint")
	}

	coll := client.Database("test").Collection("test")
	_, err = coll.Find(context.Background(), bson.D{})
	if err != nil {
		return fmt.Errorf("human_3_1: failed executing Find: %v", err)
	}

	return nil
}

func human32doesNotUseSpecualtiveAuth() error {
	var callbackFailed error

	adminClient, err := connectAdminClient()
	if err != nil {
		return fmt.Errorf("human_3_2: failed connecting admin client: %v", err)
	}
	defer func() { _ = adminClient.Disconnect(context.Background()) }()

	client, err := connectWithHumanCB(uriSingle, func(context.Context, *options.OIDCArgs) (*options.OIDCCredential, error) {
		t := time.Now().Add(time.Hour)
		tokenFile := tokenFile("test_user1")
		accessToken, err := os.ReadFile(tokenFile)
		if err != nil {
			callbackFailed = fmt.Errorf("human_3_2: failed reading token file: %v", err)
		}
		return &options.OIDCCredential{
			AccessToken:  string(accessToken),
			ExpiresAt:    &t,
			RefreshToken: nil,
		}, nil
	})

	defer func() { _ = client.Disconnect(context.Background()) }()

	if err != nil {
		return fmt.Errorf("human_3_2: failed connecting client: %v", err)
	}

	res := adminClient.Database("admin").RunCommand(context.Background(), bson.D{
		{Key: "configureFailPoint", Value: "failCommand"},
		{Key: "mode", Value: bson.D{
			{Key: "times", Value: 1},
		}},
		{Key: "data", Value: bson.D{
			{Key: "failCommands", Value: bson.A{
				"saslStart",
			}},
			{Key: "errorCode", Value: 18},
		}},
	})

	if res.Err() != nil {
		return fmt.Errorf("human_3_2: failed to set failpoint")
	}

	coll := client.Database("test").Collection("test")

	_, err = coll.Find(context.Background(), bson.D{})
	if err == nil {
		return fmt.Errorf("human_3_2: Find succeeded when it should fail")
	}
	return callbackFailed
}

func human41ReauthenticationSucceeds() error {
	callbackCount := 0
	var callbackFailed error
	countMutex := sync.Mutex{}

	adminClient, err := connectAdminClient()
	if err != nil {
		return fmt.Errorf("human_4_1: failed connecting admin client: %v", err)
	}

	defer func() { _ = adminClient.Disconnect(context.Background()) }()

	clearChannels := func(s chan *event.CommandStartedEvent, succ chan *event.CommandSucceededEvent, f chan *event.CommandFailedEvent) {
		for len(s) > 0 {
			<-s
		}
		for len(succ) > 0 {
			<-succ
		}
		for len(f) > 0 {
			<-f
		}
	}

	started := make(chan *event.CommandStartedEvent, 100)
	succeeded := make(chan *event.CommandSucceededEvent, 100)
	failed := make(chan *event.CommandFailedEvent, 100)

	monitor := event.CommandMonitor{
		Started: func(_ context.Context, e *event.CommandStartedEvent) {
			started <- e
		},
		Succeeded: func(_ context.Context, e *event.CommandSucceededEvent) {
			succeeded <- e
		},
		Failed: func(_ context.Context, e *event.CommandFailedEvent) {
			failed <- e
		},
	}

	client, err := connectWithHumanCBAndMonitor(uriSingle, func(_ context.Context, _ *options.OIDCArgs) (*options.OIDCCredential, error) {
		countMutex.Lock()
		defer countMutex.Unlock()
		callbackCount++
		t := time.Now().Add(time.Hour)
		tokenFile := tokenFile("test_user1")
		accessToken, err := os.ReadFile(tokenFile)
		if err != nil {
			callbackFailed = fmt.Errorf("human_4_1: failed reading token file: %v", err)
		}
		return &options.OIDCCredential{
			AccessToken:  string(accessToken),
			ExpiresAt:    &t,
			RefreshToken: nil,
		}, nil
	}, &monitor)
	if err != nil {
		return fmt.Errorf("human_4_1: failed connecting client: %v", err)
	}
	defer func() { _ = client.Disconnect(context.Background()) }()
	clearChannels(started, succeeded, failed)

	coll := client.Database("test").Collection("test")
	_, err = coll.Find(context.Background(), bson.D{})
	if err != nil {
		return fmt.Errorf("human_4_1: Find failed when it should succeed")
	}
	countMutex.Lock()
	if callbackCount != 1 {
		return fmt.Errorf("human_4_1: expected callback count to be 1, got %d", callbackCount)
	}
	countMutex.Unlock()
	clearChannels(started, succeeded, failed)

	res := adminClient.Database("admin").RunCommand(context.Background(), bson.D{
		{Key: "configureFailPoint", Value: "failCommand"},
		{Key: "mode", Value: bson.D{
			{Key: "times", Value: 1},
		}},
		{Key: "data", Value: bson.D{
			{Key: "failCommands", Value: bson.A{
				"find",
			}},
			{Key: "errorCode", Value: 391},
		}},
	})

	if res.Err() != nil {
		return fmt.Errorf("machine_4_1: failed setting failpoint: %v", res.Err())
	}

	_, err = coll.Find(context.Background(), bson.D{})
	if err != nil {
		return fmt.Errorf("human_4_1: Second find failed when it should succeed")
	}
	countMutex.Lock()
	if callbackCount != 2 {
		return fmt.Errorf("human_4_1: expected callback count to be 2, got %d", callbackCount)
	}
	countMutex.Unlock()

	if len(started) != 2 {
		return fmt.Errorf("human_4_1: expected 2 finds started, found %d", len(started))
	}
	for len(started) > 0 {
		ste := <-started
		if ste.CommandName != "find" {
			return fmt.Errorf("human_4_1: found unexpected command started %s", ste.CommandName)
		}
	}
	if len(succeeded) != 1 {
		return fmt.Errorf("human_4_1: expected 1 finds succeed, found %d", len(succeeded))
	}
	for len(succeeded) > 0 {
		sue := <-succeeded
		if sue.CommandName != "find" {
			return fmt.Errorf("human_4_1: found unexpected command succeeded %s", sue.CommandName)
		}
	}
	if len(failed) != 1 {
		return fmt.Errorf("human_4_1: expected 1 finds succeed, found %d", len(failed))
	}
	for len(failed) > 0 {
		fe := <-failed
		if fe.CommandName != "find" {
			return fmt.Errorf("human_4_1: found unexpected command failed %s", fe.CommandName)
		}
	}

	return callbackFailed
}

func human42ReauthenticationSucceedsNoRefreshToken() error {
	callbackCount := 0
	var callbackFailed error
	countMutex := sync.Mutex{}

	adminClient, err := connectAdminClient()
	if err != nil {
		return fmt.Errorf("human_4_2: failed connecting admin client: %v", err)
	}
	defer func() { _ = adminClient.Disconnect(context.Background()) }()

	client, err := connectWithHumanCB(uriSingle, func(context.Context, *options.OIDCArgs) (*options.OIDCCredential, error) {
		countMutex.Lock()
		defer countMutex.Unlock()
		callbackCount++
		t := time.Now().Add(time.Hour)
		tokenFile := tokenFile("test_user1")
		accessToken, err := os.ReadFile(tokenFile)
		if err != nil {
			callbackFailed = fmt.Errorf("human_4_2: failed reading token file: %v", err)
		}
		return &options.OIDCCredential{
			AccessToken:  string(accessToken),
			ExpiresAt:    &t,
			RefreshToken: nil,
		}, nil
	})

	defer func() { _ = client.Disconnect(context.Background()) }()

	if err != nil {
		return fmt.Errorf("human_4_2: failed connecting client: %v", err)
	}

	coll := client.Database("test").Collection("test")

	_, err = coll.Find(context.Background(), bson.D{})
	if err != nil {
		return fmt.Errorf("human_4_2: failed executing Find: %v", err)
	}

	countMutex.Lock()
	if callbackCount != 1 {
		return fmt.Errorf("human_4_2: expected callback count to be 1, got %d", callbackCount)
	}
	countMutex.Unlock()

	res := adminClient.Database("admin").RunCommand(context.Background(), bson.D{
		{Key: "configureFailPoint", Value: "failCommand"},
		{Key: "mode", Value: bson.D{
			{Key: "times", Value: 1},
		}},
		{Key: "data", Value: bson.D{
			{Key: "failCommands", Value: bson.A{
				"find",
			}},
			{Key: "errorCode", Value: 391},
		}},
	})

	if res.Err() != nil {
		return fmt.Errorf("human_4_2: failed to set failpoint")
	}

	_, err = coll.Find(context.Background(), bson.D{})
	if err != nil {
		return fmt.Errorf("human_4_2: failed executing Find: %v", err)
	}

	countMutex.Lock()
	if callbackCount != 2 {
		return fmt.Errorf("human_4_2: expected callback count to be 2, got %d", callbackCount)
	}
	countMutex.Unlock()
	return callbackFailed
}

func human43ReauthenticationSucceedsAfterRefreshFails() error {
	callbackCount := 0
	var callbackFailed error
	countMutex := sync.Mutex{}

	adminClient, err := connectAdminClient()
	if err != nil {
		return fmt.Errorf("human_4_3: failed connecting admin client: %v", err)
	}
	defer func() { _ = adminClient.Disconnect(context.Background()) }()

	client, err := connectWithHumanCB(uriSingle, func(context.Context, *options.OIDCArgs) (*options.OIDCCredential, error) {
		countMutex.Lock()
		defer countMutex.Unlock()
		callbackCount++
		t := time.Now().Add(time.Hour)
		tokenFile := tokenFile("test_user1")
		accessToken, err := os.ReadFile(tokenFile)
		if err != nil {
			callbackFailed = fmt.Errorf("human_4_3: failed reading token file: %v", err)
		}
		refreshToken := "bad token"
		return &options.OIDCCredential{
			AccessToken:  string(accessToken),
			ExpiresAt:    &t,
			RefreshToken: &refreshToken,
		}, nil
	})

	defer func() { _ = client.Disconnect(context.Background()) }()

	if err != nil {
		return fmt.Errorf("human_4_3: failed connecting client: %v", err)
	}

	coll := client.Database("test").Collection("test")

	_, err = coll.Find(context.Background(), bson.D{})
	if err != nil {
		return fmt.Errorf("human_4_3: failed executing Find: %v", err)
	}

	countMutex.Lock()
	if callbackCount != 1 {
		return fmt.Errorf("human_4_3: expected callback count to be 1, got %d", callbackCount)
	}
	countMutex.Unlock()

	res := adminClient.Database("admin").RunCommand(context.Background(), bson.D{
		{Key: "configureFailPoint", Value: "failCommand"},
		{Key: "mode", Value: bson.D{
			{Key: "times", Value: 1},
		}},
		{Key: "data", Value: bson.D{
			{Key: "failCommands", Value: bson.A{
				"find",
			}},
			{Key: "errorCode", Value: 391},
		}},
	})

	if res.Err() != nil {
		return fmt.Errorf("human_4_3: failed to set failpoint")
	}

	_, err = coll.Find(context.Background(), bson.D{})
	if err != nil {
		return fmt.Errorf("human_4_3: failed executing Find: %v", err)
	}

	countMutex.Lock()
	if callbackCount != 2 {
		return fmt.Errorf("human_4_3: expected callback count to be 2, got %d", callbackCount)
	}
	countMutex.Unlock()
	return callbackFailed
}

func human44ReauthenticationFails() error {
	callbackCount := 0
	var callbackFailed error
	countMutex := sync.Mutex{}

	adminClient, err := connectAdminClient()
	if err != nil {
		return fmt.Errorf("human_4_4: failed connecting admin client: %v", err)
	}
	defer func() { _ = adminClient.Disconnect(context.Background()) }()

	client, err := connectWithHumanCB(uriSingle, func(context.Context, *options.OIDCArgs) (*options.OIDCCredential, error) {
		countMutex.Lock()
		defer countMutex.Unlock()
		callbackCount++
		badToken := "bad token"
		t := time.Now().Add(time.Hour)
		if callbackCount == 1 {
			tokenFile := tokenFile("test_user1")
			accessToken, err := os.ReadFile(tokenFile)
			if err != nil {
				callbackFailed = fmt.Errorf("human_4_4: failed reading token file: %v", err)
			}
			return &options.OIDCCredential{
				AccessToken:  string(accessToken),
				ExpiresAt:    &t,
				RefreshToken: &badToken,
			}, nil
		}
		return &options.OIDCCredential{
			AccessToken:  badToken,
			ExpiresAt:    &t,
			RefreshToken: &badToken,
		}, fmt.Errorf("failed to refresh token")
	})

	defer func() { _ = client.Disconnect(context.Background()) }()

	if err != nil {
		return fmt.Errorf("human_4_4: failed connecting client: %v", err)
	}

	coll := client.Database("test").Collection("test")

	_, err = coll.Find(context.Background(), bson.D{})
	if err != nil {
		return fmt.Errorf("human_4_4: failed executing Find: %v", err)
	}

	countMutex.Lock()
	if callbackCount != 1 {
		return fmt.Errorf("human_4_4: expected callback count to be 1, got %d", callbackCount)
	}
	countMutex.Unlock()

	res := adminClient.Database("admin").RunCommand(context.Background(), bson.D{
		{Key: "configureFailPoint", Value: "failCommand"},
		{Key: "mode", Value: bson.D{
			{Key: "times", Value: 1},
		}},
		{Key: "data", Value: bson.D{
			{Key: "failCommands", Value: bson.A{
				"find",
			}},
			{Key: "errorCode", Value: 391},
		}},
	})

	if res.Err() != nil {
		return fmt.Errorf("human_4_4: failed to set failpoint")
	}

	_, err = coll.Find(context.Background(), bson.D{})
	if err == nil {
		return fmt.Errorf("human_4_4: Find succeeded when it should fail")
	}

	countMutex.Lock()
	if callbackCount != 3 {
		return fmt.Errorf("human_4_4: expected callback count to be 3, got %d", callbackCount)
	}
	countMutex.Unlock()
	return callbackFailed
}

func machine51azureWithNoUsername() error {
	opts := options.Client().ApplyURI(uriSingle)
	if opts == nil {
		return fmt.Errorf("machine_5_1: failed parsing uri: %q", uriSingle)
	}
	client, err := mongo.Connect(opts)
	if err != nil {
		return fmt.Errorf("machine_5_1: failed connecting client: %v", err)
	}
	defer func() { _ = client.Disconnect(context.Background()) }()

	coll := client.Database("test").Collection("test")

	_, err = coll.Find(context.Background(), bson.D{})
	if err != nil {
		return fmt.Errorf("machine_5_1: failed executing Find: %v", err)
	}
	return nil
}

func machine52azureWithBadUsername() error {
	opts := options.Client().ApplyURI(uriSingle)

	if opts == nil {
		return fmt.Errorf("machine_5_2: failed parsing uri: %q", uriSingle)
	}
	if opts.Auth == nil || opts.Auth.AuthMechanism != "MONGODB-OIDC" {
		return errors.New("machine_5_2: expected URI to contain MONGODB-OIDC auth information")
	}

	opts.Auth.Username = "bad"

	client, err := mongo.Connect(opts)
	if err != nil {
		return fmt.Errorf("machine_5_2: failed connecting client: %v", err)
	}
	defer func() { _ = client.Disconnect(context.Background()) }()

	coll := client.Database("test").Collection("test")

	_, err = coll.Find(context.Background(), bson.D{})
	if err == nil {
		return fmt.Errorf("machine_5_2: Find succeeded when it should fail")
	}
	return nil
}

func machine61gcpWithNoUsername() error {
	opts := options.Client().ApplyURI(uriSingle)
	client, err := mongo.Connect(opts)
	if err != nil {
		return fmt.Errorf("machine_6_1: failed connecting client: %v", err)
	}
	defer func() { _ = client.Disconnect(context.Background()) }()

	coll := client.Database("test").Collection("test")

	_, err = coll.Find(context.Background(), bson.D{})
	if err != nil {
		return fmt.Errorf("machine_6_1: failed executing Find: %v", err)
	}
	return nil
}

// machinek8s tests the "k8s" Kubernetes OIDC environment. There is no specified
// prose test for "k8s", so this test simply checks that you can run a "find"
// with the provided conn string.
func machinek8s() error {
	if !strings.Contains(uriSingle, "ENVIRONMENT:k8s") {
		return fmt.Errorf("expected MONGODB_URI_SINGLE to specify ENVIRONMENT:k8s for Kubernetes test")
	}

	opts := options.Client().ApplyURI(uriSingle)
	client, err := mongo.Connect(opts)
	if err != nil {
		return fmt.Errorf("machine_k8s: failed connecting client: %v", err)
	}
	defer func() { _ = client.Disconnect(context.Background()) }()

	coll := client.Database("test").Collection("test")

	_, err = coll.Find(context.Background(), bson.D{})
	if err != nil {
		return fmt.Errorf("machine_k8s: failed executing Find: %v", err)
	}
	return nil
}
