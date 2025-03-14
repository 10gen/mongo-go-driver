// Copyright (C) MongoDB, Inc. 2023-present.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

package driverutil

import (
	"context"
	"fmt"
	"math"
	"time"
)

// Operation Names should be sourced from the command reference documentation:
// https://www.mongodb.com/docs/manual/reference/command/
const (
	AbortTransactionOp  = "abortTransaction"  // AbortTransactionOp is the name for aborting a transaction
	AggregateOp         = "aggregate"         // AggregateOp is the name for aggregating
	CommitTransactionOp = "commitTransaction" // CommitTransactionOp is the name for committing a transaction
	CountOp             = "count"             // CountOp is the name for counting
	CreateOp            = "create"            // CreateOp is the name for creating
	CreateIndexesOp     = "createIndexes"     // CreateIndexesOp is the name for creating indexes
	DeleteOp            = "delete"            // DeleteOp is the name for deleting
	DistinctOp          = "distinct"          // DistinctOp is the name for distinct
	DropOp              = "drop"              // DropOp is the name for dropping
	DropDatabaseOp      = "dropDatabase"      // DropDatabaseOp is the name for dropping a database
	DropIndexesOp       = "dropIndexes"       // DropIndexesOp is the name for dropping indexes
	EndSessionsOp       = "endSessions"       // EndSessionsOp is the name for ending sessions
	FindAndModifyOp     = "findAndModify"     // FindAndModifyOp is the name for finding and modifying
	FindOp              = "find"              // FindOp is the name for finding
	InsertOp            = "insert"            // InsertOp is the name for inserting
	ListCollectionsOp   = "listCollections"   // ListCollectionsOp is the name for listing collections
	ListIndexesOp       = "listIndexes"       // ListIndexesOp is the name for listing indexes
	ListDatabasesOp     = "listDatabases"     // ListDatabasesOp is the name for listing databases
	UpdateOp            = "update"            // UpdateOp is the name for updating
	BulkWriteOp         = "bulkWrite"         // BulkWriteOp is the name for client-level bulk write
)

func CalculateMaxTimeMS(ctx context.Context, rttMin time.Duration, rttStats string, err error) (int64, error) {
	deadline, ok := ctx.Deadline()
	if !ok {
		return 0, nil
	}

	remainingTimeout := time.Until(deadline)

	// Always round up to the next millisecond value so we never truncate the calculated
	// maxTimeMS value (e.g. 400 microseconds evaluates to 1ms, not 0ms).
	maxTimeMS := int64((remainingTimeout - rttMin) / time.Millisecond)
	if maxTimeMS <= 0 {
		return 0, fmt.Errorf(
			"remaining time %v until context deadline is less than or equal to min network round-trip time %v (%v): %w",
			remainingTimeout,
			rttMin,
			rttStats,
			err)
	}

	// The server will return a "BadValue" error if maxTimeMS is greater
	// than the maximum positive int32 value (about 24.9 days). If the
	// user specified a timeout value greater than that,  omit maxTimeMS
	// and let the client-side timeout handle cancelling the op if the
	// timeout is ever reached.
	if maxTimeMS > math.MaxInt32 {
		return 0, nil
	}

	return maxTimeMS, nil
}
