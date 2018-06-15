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
	"github.com/mongodb/mongo-go-driver/core/topology"
	"github.com/mongodb/mongo-go-driver/core/writeconcern"
)

// Update handles the full cycle dispatch and execution of an update command against the provided
// topology.
func Update(
	ctx context.Context,
	cmd command.Update,
	topo *topology.Topology,
	selector description.ServerSelector,
	wc *writeconcern.WriteConcern,
) (result.Update, error) {

	ss, err := topo.SelectServer(ctx, selector)
	if err != nil {
		return result.Update{}, err
	}

	if wc != nil {
		opt, err := writeConcernOption(wc)
		if err != nil {
			return result.Update{}, err
		}
		cmd.Opts = append(cmd.Opts, opt)
	}

	acknowledged := wc.Acknowledged()
	desc := ss.Description()
	conn, err := ss.Connection(ctx)
	if err != nil {
		return result.Update{}, err
	}

	if !acknowledged {
		go func() {
			defer func() { _ = recover() }()
			defer conn.Close()
			_, _ = cmd.RoundTrip(ctx, desc, conn)
		}()
		return result.Update{}, ErrUnacknowledgedWrite
	}
	defer conn.Close()

	return cmd.RoundTrip(ctx, desc, conn)
}
