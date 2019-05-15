// Copyright (C) MongoDB, Inc. 2017-present.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

package driverlegacy

import (
	"context"

	"go.mongodb.org/mongo-driver/x/mongo/driver"
	"go.mongodb.org/mongo-driver/x/mongo/driverlegacy/topology"
	"go.mongodb.org/mongo-driver/x/network/command"
	"go.mongodb.org/mongo-driver/x/network/connection"
	"go.mongodb.org/mongo-driver/x/network/result"
	"go.mongodb.org/mongo-driver/x/network/wiremessage"
)

// KillCursors handles the full cycle dispatch and execution of an aggregate command against the provided
// topology.
func KillCursors(
	ctx context.Context,
	ns command.Namespace,
	server driver.Server,
	cursorID int64,
) (result.KillCursors, error) {
	legacyServer := server.(*topology.Server)
	desc := legacyServer.SelectedDescription()
	conn, err := legacyServer.ConnectionLegacy(ctx)
	if err != nil {
		return result.KillCursors{}, err
	}
	defer conn.Close()

	if desc.WireVersion.Max < 4 {
		return result.KillCursors{}, legacyKillCursors(ctx, ns, cursorID, conn)
	}

	cmd := command.KillCursors{
		NS:  ns,
		IDs: []int64{cursorID},
	}

	return cmd.RoundTrip(ctx, desc, conn)
}

func legacyKillCursors(ctx context.Context, ns command.Namespace, cursorID int64, conn connection.Connection) error {
	kc := wiremessage.KillCursors{
		NumberOfCursorIDs: 1,
		CursorIDs:         []int64{cursorID},
		CollectionName:    ns.Collection,
		DatabaseName:      ns.DB,
	}

	return conn.WriteWireMessage(ctx, kc)
}
