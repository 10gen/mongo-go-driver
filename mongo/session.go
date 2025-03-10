// Copyright (C) MongoDB, Inc. 2017-present.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

package mongo

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/internal/mongoutil"
	"go.mongodb.org/mongo-driver/v2/internal/serverselector"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/x/bsonx/bsoncore"
	"go.mongodb.org/mongo-driver/v2/x/mongo/driver"
	"go.mongodb.org/mongo-driver/v2/x/mongo/driver/operation"
	"go.mongodb.org/mongo-driver/v2/x/mongo/driver/session"
)

// ErrWrongClient is returned when a user attempts to pass in a session created by a different client than
// the method call is using.
var ErrWrongClient = errors.New("session was not created by this client")

var withTransactionTimeout = 120 * time.Second

// Session is a MongoDB logical session. Sessions can be used to enable causal
// consistency for a group of operations or to execute operations in an ACID
// transaction. A new Session can be created from a Client instance. A Session
// created from a Client must only be used to execute operations using that
// Client or a Database or Collection created from that Client. For more
// information about sessions, and their use cases, see
// https://www.mongodb.com/docs/manual/reference/server-sessions/,
// https://www.mongodb.com/docs/manual/core/read-isolation-consistency-recency/#causal-consistency, and
// https://www.mongodb.com/docs/manual/core/transactions/.
//
// Implementations of Session are not safe for concurrent use by multiple
// goroutines.
type Session struct {
	clientSession       *session.Client
	client              *Client
	deployment          driver.Deployment
	didCommitAfterStart bool // true if commit was called after start with no other operations
}

type sessionKey struct{}

// NewSessionContext returns a Context that holds the given Session. If the
// Context already contains a Session, that Session will be replaced with the
// one provided.
//
// The returned Context can be used with Collection methods like
// [Collection.InsertOne] or [Collection.Find] to run operations in a Session.
func NewSessionContext(parent context.Context, sess *Session) context.Context {
	return context.WithValue(parent, sessionKey{}, sess)
}

// SessionFromContext extracts the mongo.Session object stored in a Context. This can be used on a SessionContext that
// was created implicitly through one of the callback-based session APIs or explicitly by calling NewSessionContext. If
// there is no Session stored in the provided Context, nil is returned.
func SessionFromContext(ctx context.Context) *Session {
	val := ctx.Value(sessionKey{})
	if val == nil {
		return nil
	}

	sess, ok := val.(*Session)
	if !ok {
		return nil
	}

	return sess
}

// ClientSession returns the experimental client session.
//
// Deprecated: This method is for internal use only and should not be used (see
// GODRIVER-2700). It may be changed or removed in any release.
func (s *Session) ClientSession() *session.Client {
	return s.clientSession
}

// ID returns the current ID document associated with the session. The ID
// document is in the form {"id": <BSON binary value>}.
func (s *Session) ID() bson.Raw {
	return bson.Raw(s.clientSession.SessionID)
}

// EndSession aborts any existing transactions and close the session.
func (s *Session) EndSession(ctx context.Context) {
	if s.clientSession.TransactionInProgress() {
		// ignore all errors aborting during an end session
		_ = s.AbortTransaction(ctx)
	}
	s.clientSession.EndSession()
}

// WithTransaction starts a transaction on this session and runs the fn
// callback. Errors with the TransientTransactionError and
// UnknownTransactionCommitResult labels are retried for up to 120 seconds.
// Inside the callback, the SessionContext must be used as the Context parameter
// for any operations that should be part of the transaction. If the ctx
// parameter already has a Session attached to it, it will be replaced by this
// session. The fn callback may be run multiple times during WithTransaction due
// to retry attempts, so it must be idempotent.
//
// If a command inside the callback fn fails, it may cause the transaction on
// the server to be aborted. This situation is normally handled transparently by
// the driver. However, if the application does not return that error from the
// fn, the driver will not be able to determine whether the transaction was
// aborted or not. The driver will then retry the block indefinitely.
//
// To avoid this situation, the application MUST NOT silently handle errors
// within the callback fn. If the application needs to handle errors within the
// block, it MUST return them after doing so.
//
// Non-retryable operation errors or any operation errors that occur after the
// timeout expires will be returned without retrying. If the callback fails, the
// driver will call AbortTransaction. Because this method must succeed to ensure
// that server-side resources are properly cleaned up, context deadlines and
// cancellations will not be respected during this call. For a usage example,
// see the Client.StartSession method documentation.
func (s *Session) WithTransaction(
	ctx context.Context,
	fn func(ctx context.Context) (interface{}, error),
	opts ...options.Lister[options.TransactionOptions],
) (interface{}, error) {
	timeout := time.NewTimer(withTransactionTimeout)
	defer timeout.Stop()
	var err error
	for {
		err = s.StartTransaction(opts...)
		if err != nil {
			return nil, err
		}

		res, err := fn(NewSessionContext(ctx, s))
		if err != nil {
			if s.clientSession.TransactionRunning() {
				// Wrap the user-provided Context in a new one that behaves like context.Background() for deadlines and
				// cancellations, but forwards Value requests to the original one.
				_ = s.AbortTransaction(newBackgroundContext(ctx))
			}

			select {
			case <-timeout.C:
				return nil, err
			default:
			}

			if errorHasLabel(err, driver.TransientTransactionError) {
				continue
			}
			return res, err
		}

		// Check if callback intentionally aborted and, if so, return immediately
		// with no error.
		err = s.clientSession.CheckAbortTransaction()
		if err != nil {
			return res, nil
		}

		// If context has errored, run AbortTransaction and return, as the CommitLoop
		// has no chance of succeeding.
		//
		// Aborting after a failed CommitTransaction is dangerous. Failed transaction
		// commits may unpin the session server-side, and subsequent transaction aborts
		// may run on a new mongos which could end up with commit and abort being executed
		// simultaneously.
		if ctx.Err() != nil {
			// Wrap the user-provided Context in a new one that behaves like context.Background() for deadlines and
			// cancellations, but forwards Value requests to the original one.
			_ = s.AbortTransaction(newBackgroundContext(ctx))
			return nil, ctx.Err()
		}

	CommitLoop:
		for {
			err = s.CommitTransaction(newBackgroundContext(ctx))
			// End when error is nil, as transaction has been committed.
			if err == nil {
				return res, nil
			}

			select {
			case <-timeout.C:
				return res, err
			default:
			}

			var cerr CommandError
			if errors.As(err, &cerr) {
				if cerr.HasErrorLabel(driver.UnknownTransactionCommitResult) && !cerr.IsMaxTimeMSExpiredError() {
					continue
				}
				if cerr.HasErrorLabel(driver.TransientTransactionError) {
					break CommitLoop
				}
			}
			return res, err
		}
	}
}

// StartTransaction starts a new transaction. This method returns an error if
// there is already a transaction in-progress for this session.
func (s *Session) StartTransaction(opts ...options.Lister[options.TransactionOptions]) error {
	err := s.clientSession.CheckStartTransaction()
	if err != nil {
		return err
	}

	s.didCommitAfterStart = false

	args, err := mongoutil.NewOptions[options.TransactionOptions](opts...)
	if err != nil {
		return fmt.Errorf("failed to construct options from builder: %w", err)
	}

	coreOpts := &session.TransactionOptions{
		ReadConcern:    args.ReadConcern,
		ReadPreference: args.ReadPreference,
		WriteConcern:   args.WriteConcern,
	}

	return s.clientSession.StartTransaction(coreOpts)
}

// AbortTransaction aborts the active transaction for this session. This method
// returns an error if there is no active transaction for this session or if the
// transaction has been committed or aborted.
func (s *Session) AbortTransaction(ctx context.Context) error {
	err := s.clientSession.CheckAbortTransaction()
	if err != nil {
		return err
	}

	// Do not run the abort command if the transaction is in starting state
	if s.clientSession.TransactionStarting() || s.didCommitAfterStart {
		return s.clientSession.AbortTransaction()
	}

	selector := makePinnedSelector(s.clientSession, &serverselector.Write{})

	s.clientSession.Aborting = true
	_ = operation.NewAbortTransaction().Session(s.clientSession).ClusterClock(s.client.clock).Database("admin").
		Deployment(s.deployment).WriteConcern(s.clientSession.CurrentWc).ServerSelector(selector).
		Retry(driver.RetryOncePerCommand).CommandMonitor(s.client.monitor).
		RecoveryToken(bsoncore.Document(s.clientSession.RecoveryToken)).ServerAPI(s.client.serverAPI).
		Authenticator(s.client.authenticator).Execute(ctx)

	s.clientSession.Aborting = false
	_ = s.clientSession.AbortTransaction()

	return nil
}

// CommitTransaction commits the active transaction for this session. This
// method returns an error if there is no active transaction for this session or
// if the transaction has been aborted.
func (s *Session) CommitTransaction(ctx context.Context) error {
	err := s.clientSession.CheckCommitTransaction()
	if err != nil {
		return err
	}

	// Do not run the commit command if the transaction is in started state
	if s.clientSession.TransactionStarting() || s.didCommitAfterStart {
		s.didCommitAfterStart = true
		return s.clientSession.CommitTransaction()
	}

	if s.clientSession.TransactionCommitted() {
		s.clientSession.RetryingCommit = true
	}

	selector := makePinnedSelector(s.clientSession, &serverselector.Write{})

	s.clientSession.Committing = true
	op := operation.NewCommitTransaction().
		Session(s.clientSession).ClusterClock(s.client.clock).Database("admin").Deployment(s.deployment).
		WriteConcern(s.clientSession.CurrentWc).ServerSelector(selector).Retry(driver.RetryOncePerCommand).
		CommandMonitor(s.client.monitor).RecoveryToken(bsoncore.Document(s.clientSession.RecoveryToken)).
		ServerAPI(s.client.serverAPI).Authenticator(s.client.authenticator)

	err = op.Execute(ctx)
	// Return error without updating transaction state if it is a timeout, as the transaction has not
	// actually been committed.
	if IsTimeout(err) {
		return replaceErrors(err)
	}
	s.clientSession.Committing = false
	commitErr := s.clientSession.CommitTransaction()

	// We set the write concern to majority for subsequent calls to CommitTransaction.
	s.clientSession.UpdateCommitTransactionWriteConcern()

	if err != nil {
		return replaceErrors(err)
	}
	return commitErr
}

// ClusterTime returns the current cluster time document associated with the
// session.
func (s *Session) ClusterTime() bson.Raw {
	return s.clientSession.ClusterTime
}

// AdvanceClusterTime advances the cluster time for a session. This method
// returns an error if the session has ended.
func (s *Session) AdvanceClusterTime(d bson.Raw) error {
	return s.clientSession.AdvanceClusterTime(d)
}

// OperationTime returns the current operation time document associated with the
// session.
func (s *Session) OperationTime() *bson.Timestamp {
	return s.clientSession.OperationTime
}

// AdvanceOperationTime advances the operation time for a session. This method
// returns an error if the session has ended.
func (s *Session) AdvanceOperationTime(ts *bson.Timestamp) error {
	return s.clientSession.AdvanceOperationTime(ts)
}

// Client is the Client associated with the session.
func (s *Session) Client() *Client {
	return s.client
}

// sessionFromContext checks for a sessionImpl in the argued context and returns the session if it
// exists
func sessionFromContext(ctx context.Context) *session.Client {
	if ses := SessionFromContext(ctx); ses != nil {
		return ses.clientSession
	}

	return nil
}
