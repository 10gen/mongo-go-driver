// Copyright (C) MongoDB, Inc. 2017-present.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

package dispatch

import (
	"context"

	"github.com/mongodb/mongo-go-driver/core/command"
	"github.com/mongodb/mongo-go-driver/core/description"
	"github.com/mongodb/mongo-go-driver/core/result"
	"github.com/mongodb/mongo-go-driver/core/session"
	"github.com/mongodb/mongo-go-driver/core/topology"
	"github.com/mongodb/mongo-go-driver/core/uuid"
	"github.com/mongodb/mongo-go-driver/core/writeconcern"
)

// Insert handles the full cycle dispatch and execution of an insert command against the provided
// topology.
func Insert(
	ctx context.Context,
	cmd command.Insert,
	topo *topology.Topology,
	selector description.ServerSelector,
	clientID uuid.UUID,
	pool *session.Pool,
) (result.Insert, error) {

	ss, err := topo.SelectServer(ctx, selector)
	if err != nil {
		return result.Insert{}, err
	}

	// If no explicit session and deployment supports sessions, start implicit session.
	if cmd.Session == nil && topo.SupportsSessions() {
		cmd.Session, err = session.NewClientSession(pool, clientID, session.Implicit)
		if err != nil {
			return result.Insert{}, err
		}
		defer cmd.Session.EndSession()
	}

	// Execute in a single trip if retry writes not supported, or retry not enabled
	if !retrySupported(topo, ss.Description(), cmd.Session, cmd.WriteConcern) || !cmd.RetryWrite {
		cmd.RetryWrite = false // explicitly set to false to prevent encoding transaction number
		return insert(ctx, cmd, ss, nil)
	}

	cmd.Session.IncrementTxnNumber()

	res, originalErr := insert(ctx, cmd, ss, nil)

	// Retry if appropriate
	if cerr, ok := originalErr.(command.Error); ok && originalErr != nil && cerr.Retryable() {
		ss, err := topo.SelectServer(ctx, selector)

		// Return original error if server selection fails or new server does not support retryable writes
		if err != nil || !retrySupported(topo, ss.Description(), cmd.Session, cmd.WriteConcern) {
			return result.Insert{}, originalErr
		}

		return insert(ctx, cmd, ss, cerr)
	} else if res.WriteConcernError != nil && command.WriteConcernErrorRetryable(res.WriteConcernError) {
		ss, err := topo.SelectServer(ctx, selector)

		// Return original error if server selection fails or new server does not support retryable writes
		if err != nil || !retrySupported(topo, ss.Description(), cmd.Session, cmd.WriteConcern) {
			return res, originalErr
		}

		return insert(ctx, cmd, ss, cerr)
	}

	return res, originalErr
}

func insert(
	ctx context.Context,
	cmd command.Insert,
	ss *topology.SelectedServer,
	oldErr error,
) (result.Insert, error) {
	desc := ss.Description()
	conn, err := ss.Connection(ctx)
	if err != nil {
		if oldErr != nil {
			return result.Insert{}, oldErr
		}
		return result.Insert{}, err
	}

	if !writeconcern.AckWrite(cmd.WriteConcern) {
		go func() {
			defer func() { _ = recover() }()
			defer conn.Close()

			_, _ = cmd.RoundTrip(ctx, desc, conn)
		}()

		return result.Insert{}, command.ErrUnacknowledgedWrite
	}
	defer conn.Close()

	return cmd.RoundTrip(ctx, desc, conn)
}
