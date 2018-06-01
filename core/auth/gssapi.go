// Copyright (C) MongoDB, Inc. 2017-present.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

//+build gssapi
//+build windows linux darwin

package auth

import (
	"context"
	"github.com/mongodb/mongo-go-driver/core/auth/internal/gssapi"
	"github.com/mongodb/mongo-go-driver/core/description"
	"github.com/mongodb/mongo-go-driver/core/wiremessage"
)

// GSSAPI is the mechanism name for GSSAPI.
const GSSAPI = "GSSAPI"

func newGSSAPIAuthenticator(cred *Cred) (Authenticator, error) {
	if cred.Source != "" && cred.Source != "$external" {
		return nil, &Error{"GSSAPI source must be empty or $external", nil}
	}

	return &GSSAPIAuthenticator{
		Username:    cred.Username,
		Password:    cred.Password,
		PasswordSet: cred.PasswordSet,
		Props:       cred.Props,
	}, nil
}

// GSSAPIAuthenticator uses the GSSAPI algorithm over SASL to authenticate a connection.
type GSSAPIAuthenticator struct {
	Username    string
	Password    string
	PasswordSet bool
	Props       map[string]string
}

// Auth authenticates the connection.
func (a *GSSAPIAuthenticator) Auth(ctx context.Context, desc description.Server, rw wiremessage.ReadWriter) error {
	client, err := gssapi.New(desc.Addr.String(), a.Username, a.Password, a.PasswordSet, a.Props)

	if err != nil {
		return &Error{"", err}
	}
	return ConductSaslConversation(ctx, desc, rw, "$external", client)
}
