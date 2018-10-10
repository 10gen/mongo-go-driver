// Copyright (C) MongoDB, Inc. 2017-present.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

package dispatch

import (
	"context"
	"github.com/mongodb/mongo-go-driver/bson/bsoncodec"
	"time"

	"github.com/mongodb/mongo-go-driver/bson"
	"github.com/mongodb/mongo-go-driver/core/command"
	"github.com/mongodb/mongo-go-driver/core/description"
	"github.com/mongodb/mongo-go-driver/core/session"
	"github.com/mongodb/mongo-go-driver/core/topology"
	"github.com/mongodb/mongo-go-driver/core/uuid"
	"github.com/mongodb/mongo-go-driver/options"
)

// CountDocuments handles the full cycle dispatch and execution of a countDocuments command against the provided
// topology.
func CountDocuments(
	ctx context.Context,
	cmd command.CountDocuments,
	topo *topology.Topology,
	selector description.ServerSelector,
	clientID uuid.UUID,
	pool *session.Pool,
	opts ...*options.CountOptions,
) (int64, error) {

	ss, err := topo.SelectServer(ctx, selector)
	if err != nil {
		return 0, err
	}

	desc := ss.Description()
	conn, err := ss.Connection(ctx)
	if err != nil {
		return 0, err
	}
	defer conn.Close()

	rp, err := getReadPrefBasedOnTransaction(cmd.ReadPref, cmd.Session)
	if err != nil {
		return 0, err
	}
	cmd.ReadPref = rp

	// If no explicit session and deployment supports sessions, start implicit session.
	if cmd.Session == nil && topo.SupportsSessions() {
		cmd.Session, err = session.NewClientSession(pool, clientID, session.Implicit)
		if err != nil {
			return 0, err
		}
		defer cmd.Session.EndSession()
	}

	countOpts := options.ToCountOptions(opts...)

	// ignore Skip and Limit because we already have these options in the pipeline
	if countOpts.MaxTimeMS != nil {
		cmd.Opts = append(cmd.Opts, bson.EC.Int64("maxTimeMS", int64(time.Duration(*countOpts.MaxTimeMS)/time.Millisecond)))
	}
	if countOpts.Collation != nil {
		cmd.Opts = append(cmd.Opts, bson.EC.SubDocument("collation", countOpts.Collation.ToDocument()))
	}
	if countOpts.Hint != nil {
		switch t := (countOpts.Hint).(type) {
		case string:
			cmd.Opts = append(cmd.Opts, bson.EC.String("hint", t))
		case *bson.Document:
			cmd.Opts = append(cmd.Opts, bson.EC.SubDocument("hint", t))
		default:
			h, err := bsoncodec.Marshal(t)
			if err != nil {
				return 0, err
			}
			cmd.Opts = append(cmd.Opts, bson.EC.SubDocumentFromReader("hint", h))
		}
	}

	return cmd.RoundTrip(ctx, desc, ss, conn)
}
