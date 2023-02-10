// Copyright (C) MongoDB, Inc. 2017-present.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

package auth

import (
	"context"
	"errors"

	"go.mongodb.org/mongo-driver/x/mongo/driver/auth/creds"
	awscredproviders "go.mongodb.org/mongo-driver/x/mongo/driver/auth/creds/aws_credential_providers"
	"go.mongodb.org/mongo-driver/x/mongo/driver/auth/internal/aws/credentials"
)

// MongoDBAWS is the mechanism name for MongoDBAWS.
const MongoDBAWS = "MONGODB-AWS"

func newMongoDBAWSAuthenticator(cred *Cred) (Authenticator, error) {
	if cred.Source != "" && cred.Source != "$external" {
		return nil, newAuthError("MONGODB-AWS source must be empty or $external", nil)
	}
	return &MongoDBAWSAuthenticator{
		source: cred.Source,
		credentials: &awscredproviders.StaticProvider{
			Value: credentials.Value{
				ProviderName:    cred.Source,
				AccessKeyID:     cred.Username,
				SecretAccessKey: cred.Password,
				SessionToken:    cred.Props["AWS_SESSION_TOKEN"],
			},
		},
	}, nil
}

// MongoDBAWSAuthenticator uses AWS-IAM credentials over SASL to authenticate a connection.
type MongoDBAWSAuthenticator struct {
	source      string
	credentials *awscredproviders.StaticProvider
}

// Auth authenticates the connection.
func (a *MongoDBAWSAuthenticator) Auth(ctx context.Context, cfg *Config) error {
	httpClient := cfg.HTTPClient
	if httpClient == nil {
		return errors.New("cfg.HTTPClient must not be nil")
	}
	providers := []credentials.Provider{a.credentials}
	providers = append(providers, creds.NewAwsCredentialProvider(httpClient).Providers...)
	adapter := &awsSaslAdapter{
		conversation: &awsConversation{
			credentials: credentials.NewChainCredentials(providers),
		},
	}
	err := ConductSaslConversation(ctx, cfg, a.source, adapter)
	if err != nil {
		return newAuthError("sasl conversation error", err)
	}
	return nil
}

type awsSaslAdapter struct {
	conversation *awsConversation
}

var _ SaslClient = (*awsSaslAdapter)(nil)

func (a *awsSaslAdapter) Start() (string, []byte, error) {
	step, err := a.conversation.Step(nil)
	if err != nil {
		return MongoDBAWS, nil, err
	}
	return MongoDBAWS, step, nil
}

func (a *awsSaslAdapter) Next(challenge []byte) ([]byte, error) {
	step, err := a.conversation.Step(challenge)
	if err != nil {
		return nil, err
	}
	return step, nil
}

func (a *awsSaslAdapter) Completed() bool {
	return a.conversation.Done()
}
