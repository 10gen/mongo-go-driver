// Copyright (C) MongoDB, Inc. 2017-present.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

package integration

import (
	"context"
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/internal/assert"
	"go.mongodb.org/mongo-driver/v2/internal/integration/mtest"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readconcern"
)

// set of operations that support read concerns taken from read/write concern spec.
// the spec also lists "count" but that has been deprecated and removed from the driver.
var readConcernOperations = map[string]struct{}{
	"Aggregate": {},
	"Distinct":  {},
	"Find":      {},
}

func TestCausalConsistency_Supported(t *testing.T) {
	mt := mtest.New(t, mtest.NewOptions().MinServerVersion("3.6").Topologies(mtest.ReplicaSet, mtest.Sharded).CreateClient(false))

	mt.Run("operation time nil", func(mt *mtest.T) {
		// when a ClientSession is first created, the operation time is nil

		sess, err := mt.Client.StartSession()
		assert.Nil(mt, err, "StartSession error: %v", err)
		defer sess.EndSession(context.Background())
		assert.Nil(mt, sess.OperationTime(), "expected nil operation time, got %v", sess.OperationTime())
	})
	mt.Run("no cluster time on first command", func(mt *mtest.T) {
		// first read in a causally consistent session must not send afterClusterTime to the server

		ccOpts := options.Session().SetCausalConsistency(true)
		_ = mt.Client.UseSessionWithOptions(context.Background(), ccOpts, func(ctx context.Context) error {
			_, _ = mt.Coll.Find(ctx, bson.D{})
			return nil
		})

		evt := mt.GetStartedEvent()
		assert.Equal(mt, "find", evt.CommandName, "expected 'find' event, got '%v'", evt.CommandName)
		checkOperationTime(mt, evt.Command, false)
	})
	mt.Run("operation time updated", func(mt *mtest.T) {
		// first read or write on a ClientSession should update the operationTime of the session, even if there is an error

		sess, err := mt.Client.StartSession()
		assert.Nil(mt, err, "StartSession error: %v", err)
		defer sess.EndSession(context.Background())

		_ = mongo.WithSession(context.Background(), sess, func(ctx context.Context) error {
			_, _ = mt.Coll.Find(ctx, bson.D{})
			return nil
		})

		evt := mt.GetSucceededEvent()
		assert.Equal(mt, "find", evt.CommandName, "expected 'find' event, got '%v'", evt.CommandName)
		serverT, serverI := evt.Reply.Lookup("operationTime").Timestamp()
		serverTs := &bson.Timestamp{serverT, serverI}
		sessionTs := sess.OperationTime()
		assert.NotNil(mt, sessionTs, "expected session operation time, got nil")
		assert.True(mt, serverTs.Equal(*sessionTs), "expected operation time %v, got %v", serverTs, sessionTs)
	})
	mt.RunOpts("operation time sent", noClientOpts, func(mt *mtest.T) {
		// findOne followed by another read operation should include operationTime returned by server for the first
		// operation as the afterClusterTime field of the second operation

		for _, sf := range createFunctionsSlice() {
			// skip write operations
			if _, ok := readConcernOperations[sf.fnName]; !ok {
				continue
			}

			mt.Run(sf.name, func(mt *mtest.T) {
				sess, err := mt.Client.StartSession()
				assert.Nil(mt, err, "StartSession error: %v", err)
				defer sess.EndSession(context.Background())

				_ = mongo.WithSession(context.Background(), sess, func(ctx context.Context) error {
					_ = mt.Coll.FindOne(ctx, bson.D{})
					return nil
				})
				currOptime := sess.OperationTime()
				assert.NotNil(mt, currOptime, "expected session operation time, got nil")

				mt.ClearEvents()
				_ = sf.execute(mt, sess)
				_, sentOptime := getReadConcernFields(mt, mt.GetStartedEvent().Command)
				assert.NotNil(mt, sentOptime, "expected operation time on command, got nil")
				assert.True(mt, currOptime.Equal(*sentOptime), "expected operation time %v, got %v", currOptime, sentOptime)
			})
		}
	})
	mt.RunOpts("write then read", noClientOpts, func(mt *mtest.T) {
		// any write operation followed by a findOne should include operationTime of the first operation as afterClusterTime
		// in the second operation

		for _, sf := range createFunctionsSlice() {
			// skip read operations
			if _, ok := readConcernOperations[sf.fnName]; ok {
				continue
			}

			mt.Run(sf.name, func(mt *mtest.T) {
				sess, err := mt.Client.StartSession()
				assert.Nil(mt, err, "StartSession error: %v", err)
				defer sess.EndSession(context.Background())

				_ = sf.execute(mt, sess)
				currOptime := sess.OperationTime()
				assert.NotNil(mt, currOptime, "expected session operation time, got nil")

				mt.ClearEvents()
				_ = mongo.WithSession(context.Background(), sess, func(ctx context.Context) error {
					_ = mt.Coll.FindOne(ctx, bson.D{})
					return nil
				})
				_, sentOptime := getReadConcernFields(mt, mt.GetStartedEvent().Command)
				assert.NotNil(mt, sentOptime, "expected operation time on command, got nil")
				assert.True(mt, currOptime.Equal(*sentOptime), "expected operation time %v, got %v", currOptime, sentOptime)
			})
		}
	})
	mt.Run("non-consistent read", func(mt *mtest.T) {
		// a read operation in a non causally-consistent session should not include afterClusterTime

		sessOpts := options.Session().SetCausalConsistency(false)
		_ = mt.Client.UseSessionWithOptions(context.Background(), sessOpts, func(ctx context.Context) error {
			_, _ = mt.Coll.Find(ctx, bson.D{})
			mt.ClearEvents()
			_, _ = mt.Coll.Find(ctx, bson.D{})
			return nil
		})
		evt := mt.GetStartedEvent()
		assert.Equal(mt, "find", evt.CommandName, "expected 'find' command, got '%v'", evt.CommandName)
		checkOperationTime(mt, evt.Command, false)
	})
	mt.Run("default read concern", func(mt *mtest.T) {
		// when using the default server read concern, the readConcern parameter in the command sent to the server should
		// not include a level field

		sess, err := mt.Client.StartSession()
		assert.Nil(mt, err, "StartSession error: %v", err)
		defer sess.EndSession(context.Background())

		_ = mongo.WithSession(context.Background(), sess, func(ctx context.Context) error {
			_ = mt.Coll.FindOne(ctx, bson.D{})
			return nil
		})
		currOptime := sess.OperationTime()
		mt.ClearEvents()
		_ = mongo.WithSession(context.Background(), sess, func(ctx context.Context) error {
			_ = mt.Coll.FindOne(ctx, bson.D{})
			return nil
		})

		level, sentOptime := getReadConcernFields(mt, mt.GetStartedEvent().Command)
		assert.Equal(mt, "", level, "expected command to not have read concern level, got %s", level)
		assert.NotNil(mt, sentOptime, "expected operation time on command, got nil")
		assert.True(mt, currOptime.Equal(*sentOptime), "expected operation time %v, got %v", currOptime, sentOptime)
	})
	localRcOpts := options.Client().SetReadConcern(readconcern.Local())
	mt.RunOpts("custom read concern", mtest.NewOptions().ClientOptions(localRcOpts), func(mt *mtest.T) {
		sess, err := mt.Client.StartSession()
		assert.Nil(mt, err, "StartSession error: %v", err)
		defer sess.EndSession(context.Background())

		_ = mongo.WithSession(context.Background(), sess, func(ctx context.Context) error {
			_ = mt.Coll.FindOne(ctx, bson.D{})
			return nil
		})
		currOptime := sess.OperationTime()
		mt.ClearEvents()
		_ = mongo.WithSession(context.Background(), sess, func(ctx context.Context) error {
			_ = mt.Coll.FindOne(ctx, bson.D{})
			return nil
		})

		level, sentOptime := getReadConcernFields(mt, mt.GetStartedEvent().Command)
		assert.Equal(mt, "local", level, "expected read concern level 'local', got %s", level)
		assert.NotNil(mt, sentOptime, "expected operation time on command, got nil")
		assert.True(mt, currOptime.Equal(*sentOptime), "expected operation time %v, got %v", currOptime, sentOptime)
	})
	mt.Run("clusterTime included", func(mt *mtest.T) {
		// $clusterTime should be included in commands if the deployment supports cluster times

		_ = mt.Coll.FindOne(context.Background(), bson.D{})
		evt := mt.GetStartedEvent()
		assert.Equal(mt, "find", evt.CommandName, "expected command 'find', got '%v'", evt.CommandName)
		_, err := evt.Command.LookupErr("$clusterTime")
		assert.Nil(mt, err, "expected $clusterTime in command, got nil")
	})
}

func checkOperationTime(mt *mtest.T, cmd bson.Raw, shouldInclude bool) {
	mt.Helper()

	_, optime := getReadConcernFields(mt, cmd)
	if shouldInclude {
		assert.NotNil(mt, optime, "expected operation time, got nil")
		return
	}
	assert.Nil(mt, optime, "did not expect operation time, got %v", optime)
}

func getReadConcernFields(mt *mtest.T, cmd bson.Raw) (string, *bson.Timestamp) {
	mt.Helper()

	rc, err := cmd.LookupErr("readConcern")
	if err != nil {
		return "", nil
	}
	rcDoc := rc.Document()

	var level string
	var clusterTime *bson.Timestamp

	if levelVal, err := rcDoc.LookupErr("level"); err == nil {
		level = levelVal.StringValue()
	}
	if ctVal, err := rcDoc.LookupErr("afterClusterTime"); err == nil {
		t, i := ctVal.Timestamp()
		clusterTime = &bson.Timestamp{t, i}
	}
	return level, clusterTime
}
