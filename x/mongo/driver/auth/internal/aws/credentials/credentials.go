// Copyright (C) MongoDB, Inc. 2023-present.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
//
// Based on github.com/aws/aws-sdk-go by Amazon.com, Inc. with code from:
// - github.com/aws/aws-sdk-go/blob/v1.34.28/aws/credentials/credentials.go
// See THIRD-PARTY-NOTICES for original license terms

package credentials

import (
	"context"
	"errors"
	"sync/atomic"
	"time"

	"golang.org/x/sync/singleflight"
)

// A Value is the AWS credentials value for individual credential fields.
type Value struct {
	// AWS Access key ID
	AccessKeyID string

	// AWS Secret Access Key
	SecretAccessKey string

	// AWS Session Token
	SessionToken string

	// Provider used to get credentials
	ProviderName string
}

// HasKeys returns if the credentials Value has both AccessKeyID and
// SecretAccessKey value set.
func (v Value) HasKeys() bool {
	return len(v.AccessKeyID) != 0 && len(v.SecretAccessKey) != 0
}

// A Provider is the interface for any component which will provide credentials
// Value. A provider is required to manage its own Expired state, and what to
// be expired means.
//
// The Provider should not need to implement its own mutexes, because
// that will be managed by Credentials.
type Provider interface {
	// Retrieve returns nil if it successfully retrieved the value.
	// Error is returned if the value were not obtainable, or empty.
	Retrieve() (Value, error)

	// IsExpired returns if the credentials are no longer valid, and need
	// to be retrieved.
	IsExpired() bool
}

// ProviderWithContext is a Provider that can retrieve credentials with a Context
type ProviderWithContext interface {
	Provider

	RetrieveWithContext(context.Context) (Value, error)
}

// A Credentials provides concurrency safe retrieval of AWS credentials Value.
// Credentials will cache the credentials value until they expire. Once the value
// expires the next Get will attempt to retrieve valid credentials.
//
// Credentials is safe to use across multiple goroutines and will manage the
// synchronous state so the Providers do not need to implement their own
// synchronization.
//
// The first Credentials.Get() will always call Provider.Retrieve() to get the
// first instance of the credentials Value. All calls to Get() after that
// will return the cached credentials Value until IsExpired() returns true.
type Credentials struct {
	creds atomic.Value
	sf    singleflight.Group

	provider Provider
}

// NewCredentials returns a pointer to a new Credentials with the provider set.
func NewCredentials(provider Provider) *Credentials {
	c := &Credentials{
		provider: provider,
	}
	c.creds.Store(Value{})
	return c
}

// GetWithContext returns the credentials value, or error if the credentials
// Value failed to be retrieved. Will return early if the passed in context is
// canceled.
//
// Will return the cached credentials Value if it has not expired. If the
// credentials Value has expired the Provider's Retrieve() will be called
// to refresh the credentials.
//
// If Credentials.Expire() was called the credentials Value will be force
// expired, and the next call to Get() will cause them to be refreshed.
//
// Passed in Context is equivalent to aws.Context, and context.Context.
func (c *Credentials) GetWithContext(ctx context.Context) (Value, error) {
	if curCreds := c.creds.Load(); !c.isExpired(curCreds) {
		return curCreds.(Value), nil
	}

	// Cannot pass context down to the actual retrieve, because the first
	// context would cancel the whole group when there is not direct
	// association of items in the group.
	resCh := c.sf.DoChan("", func() (interface{}, error) {
		return c.singleRetrieve(&suppressedContext{ctx})
	})
	select {
	case res := <-resCh:
		return res.Val.(Value), res.Err
	case <-ctx.Done():
		return Value{}, errors.New("request context canceled")
	}
}

func (c *Credentials) singleRetrieve(ctx context.Context) (creds interface{}, err error) {
	if curCreds := c.creds.Load(); !c.isExpired(curCreds) {
		return curCreds.(Value), nil
	}

	if p, ok := c.provider.(ProviderWithContext); ok {
		creds, err = p.RetrieveWithContext(ctx)
	} else {
		creds, err = c.provider.Retrieve()
	}
	if err == nil {
		c.creds.Store(creds)
	}

	return creds, err
}

// Get returns the credentials value, or error if the credentials Value failed
// to be retrieved.
//
// Will return the cached credentials Value if it has not expired. If the
// credentials Value has expired the Provider's Retrieve() will be called
// to refresh the credentials.
//
// If Credentials.Expire() was called the credentials Value will be force
// expired, and the next call to Get() will cause them to be refreshed.
func (c *Credentials) Get() (Value, error) {
	return c.GetWithContext(context.Background())
}

// Expire expires the credentials and forces them to be retrieved on the
// next call to Get().
//
// This will override the Provider's expired state, and force Credentials
// to call the Provider's Retrieve().
func (c *Credentials) Expire() {
	c.creds.Store(Value{})
}

// IsExpired returns if the credentials are no longer valid, and need
// to be retrieved.
//
// If the Credentials were forced to be expired with Expire() this will
// reflect that override.
func (c *Credentials) IsExpired() bool {
	return c.isExpired(c.creds.Load())
}

// isExpired helper method wrapping the definition of expired credentials.
func (c *Credentials) isExpired(creds interface{}) bool {
	return creds == nil || creds.(Value) == Value{} || c.provider.IsExpired()
}

type suppressedContext struct {
	context.Context
}

func (s *suppressedContext) Deadline() (deadline time.Time, ok bool) {
	return time.Time{}, false
}

func (s *suppressedContext) Done() <-chan struct{} {
	return nil
}

func (s *suppressedContext) Err() error {
	return nil
}
