// Copyright (C) MongoDB, Inc. 2017-present.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

package unified

import (
	"context"
	"fmt"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/integration/mtest"
	"go.mongodb.org/mongo-driver/x/bsonx/bsoncore"
	"go.mongodb.org/mongo-driver/x/mongo/driver/session"
)

// waitForEventTimeout is the amount of time to wait for an event to occur. The
// maximum amount of time expected for this value is currently 10 seconds, which
// is the amount of time that the driver will attempt to wait between streamable
// heartbeats. Increase this value if a new maximum time is expected in another
// operation.
var waitForEventTimeout = 11 * time.Second

type loopArgs struct {
	Operations         []*operation `bson:"operations"`
	ErrorsEntityID     string       `bson:"storeErrorsAsEntity"`
	FailuresEntityID   string       `bson:"storeFailuresAsEntity"`
	SuccessesEntityID  string       `bson:"storeSuccessesAsEntity"`
	IterationsEntityID string       `bson:"storeIterationsAsEntity"`
}

func (lp *loopArgs) errorsStored() bool {
	return lp.ErrorsEntityID != ""
}

func (lp *loopArgs) failuresStored() bool {
	return lp.FailuresEntityID != ""
}

func (lp *loopArgs) successesStored() bool {
	return lp.SuccessesEntityID != ""
}

func (lp *loopArgs) iterationsStored() bool {
	return lp.IterationsEntityID != ""
}

func executeTestRunnerOperation(ctx context.Context, op *operation, loopDone <-chan struct{}) error {
	args := op.Arguments

	switch op.Name {
	case "failPoint":
		clientID := lookupString(args, "client")
		client, err := entities(ctx).client(clientID)
		if err != nil {
			return err
		}

		fpDoc := args.Lookup("failPoint").Document()
		if err := mtest.SetRawFailPoint(fpDoc, client.Client); err != nil {
			return err
		}
		return addFailPoint(ctx, fpDoc.Index(0).Value().StringValue(), client.Client)
	case "targetedFailPoint":
		sessID := lookupString(args, "session")
		sess, err := entities(ctx).session(sessID)
		if err != nil {
			return err
		}

		clientSession := extractClientSession(sess)
		if clientSession.PinnedServer == nil {
			return fmt.Errorf("session is not pinned to a server")
		}

		targetHost := clientSession.PinnedServer.Addr.String()
		fpDoc := args.Lookup("failPoint").Document()
		commandFn := func(_ context.Context, client *mongo.Client) error {
			return mtest.SetRawFailPoint(fpDoc, client)
		}

		if err := runCommandOnHost(ctx, targetHost, commandFn); err != nil {
			return err
		}
		return addTargetedFailPoint(ctx, fpDoc.Index(0).Value().StringValue(), targetHost)
	case "assertSessionTransactionState":
		sessID := lookupString(args, "session")
		sess, err := entities(ctx).session(sessID)
		if err != nil {
			return err
		}

		var expectedState session.TransactionState
		switch stateStr := lookupString(args, "state"); stateStr {
		case "none":
			expectedState = session.None
		case "starting":
			expectedState = session.Starting
		case "in_progress":
			expectedState = session.InProgress
		case "committed":
			expectedState = session.Committed
		case "aborted":
			expectedState = session.Aborted
		default:
			return fmt.Errorf("unrecognized session state type %q", stateStr)
		}

		if actualState := extractClientSession(sess).TransactionState; actualState != expectedState {
			return fmt.Errorf("expected session state %q does not match actual state %q", expectedState, actualState)
		}
		return nil
	case "assertSessionPinned":
		return verifySessionPinnedState(ctx, lookupString(args, "session"), true)
	case "assertSessionUnpinned":
		return verifySessionPinnedState(ctx, lookupString(args, "session"), false)
	case "assertSameLsidOnLastTwoCommands":
		return verifyLastTwoLsidsEqual(ctx, lookupString(args, "client"), true)
	case "assertDifferentLsidOnLastTwoCommands":
		return verifyLastTwoLsidsEqual(ctx, lookupString(args, "client"), false)
	case "assertSessionDirty":
		return verifySessionDirtyState(ctx, lookupString(args, "session"), true)
	case "assertSessionNotDirty":
		return verifySessionDirtyState(ctx, lookupString(args, "session"), false)
	case "assertCollectionExists":
		db := lookupString(args, "databaseName")
		coll := lookupString(args, "collectionName")
		return verifyCollectionExists(ctx, db, coll, true)
	case "assertCollectionNotExists":
		db := lookupString(args, "databaseName")
		coll := lookupString(args, "collectionName")
		return verifyCollectionExists(ctx, db, coll, false)
	case "assertIndexExists":
		db := lookupString(args, "databaseName")
		coll := lookupString(args, "collectionName")
		index := lookupString(args, "indexName")
		return verifyIndexExists(ctx, db, coll, index, true)
	case "assertIndexNotExists":
		db := lookupString(args, "databaseName")
		coll := lookupString(args, "collectionName")
		index := lookupString(args, "indexName")
		return verifyIndexExists(ctx, db, coll, index, false)
	case "loop":
		var unmarshaledArgs loopArgs
		if err := bson.Unmarshal(args, &unmarshaledArgs); err != nil {
			return fmt.Errorf("error unmarshalling arguments to loopArgs: %v", err)
		}
		return executeLoop(ctx, &unmarshaledArgs, loopDone)
	case "assertNumberConnectionsCheckedOut":
		clientID := lookupString(args, "client")
		client, err := entities(ctx).client(clientID)
		if err != nil {
			return err
		}

		expected := int32(lookupInteger(args, "connections"))
		actual := client.numberConnectionsCheckedOut()
		if expected != actual {
			return fmt.Errorf("expected %d connections to be checked out, got %d", expected, actual)
		}
		return nil
	case "createEntities":
		entitiesRaw, err := args.LookupErr("entities")
		if err != nil {
			return fmt.Errorf("'entities' argument not found in createEntities operation")
		}

		var createEntities []map[string]*entityOptions
		if err := entitiesRaw.Unmarshal(&createEntities); err != nil {
			return fmt.Errorf("error unmarshalling 'entities' argument to entityOptions: %v", err)
		}

		for idx, entity := range createEntities {
			for entityType, entityOptions := range entity {
				if entityType == "client" && hasOperationalFailpoint(ctx) {
					entityOptions.setHeartbeatFrequencyMS(lowHeartbeatFrequency)
				}

				if err := entities(ctx).addEntity(ctx, entityType, entityOptions); err != nil {
					return fmt.Errorf("error creating entity at index %d: %v", idx, err)
				}
			}
		}
		return nil
	case "runOnThread":
		operationRaw, err := args.LookupErr("operation")
		if err != nil {
			return fmt.Errorf("'operation' argument not found in runOnThread operation")
		}
		threadOp := new(operation)
		if err := operationRaw.Unmarshal(threadOp); err != nil {
			return fmt.Errorf("error unmarshaling 'operation' argument: %v", err)
		}
		thread := lookupString(args, "thread")
		routine, ok := entities(ctx).routinesMap.Load(thread)
		if !ok {
			return fmt.Errorf("run on unknown thread: %s", thread)
		}
		routine.(*backgroundRoutine).addTask(threadOp.Name, func() error {
			return threadOp.execute(ctx, loopDone)
		})
		return nil
	case "waitForThread":
		thread := lookupString(args, "thread")
		routine, ok := entities(ctx).routinesMap.Load(thread)
		if !ok {
			return fmt.Errorf("wait for unknown thread: %s", thread)
		}
		return routine.(*backgroundRoutine).stop()
	case "waitForEvent":
		var wfeArgs waitForEventArguments
		if err := bson.Unmarshal(op.Arguments, &wfeArgs); err != nil {
			return fmt.Errorf("error unmarshalling event to waitForEventArguments: %v", err)
		}

		wfeCtx, cancel := context.WithTimeout(ctx, waitForEventTimeout)
		defer cancel()

		return waitForEvent(wfeCtx, wfeArgs)
	case "wait":
		timeMs := lookupInteger(args, "ms")

		dur := time.Duration(timeMs) * time.Millisecond
		time.Sleep(dur)

		return nil
	case "assertEventCount":
		// args: client, event, count
		var wfeArgs waitForEventArguments
		if err := bson.Unmarshal(op.Arguments, &wfeArgs); err != nil {
			return fmt.Errorf("error unmarshalling event to waitForEventArguments: %v", err)
		}

		client, err := entities(ctx).client(wfeArgs.ClientID)
		if err != nil {
			return err
		}

		for rawEventType, eventDoc := range wfeArgs.Event {
			eventType, _ := monitoringEventTypeFromString(rawEventType)

			switch eventType {
			case serverDescriptionChangedEvent:
				if got := getServerDescriptionChangedEventCount(client, eventDoc); got != wfeArgs.Count {
					return fmt.Errorf("failed to assert count. want: %v, got: %v", wfeArgs.Count, got)
				}
			default:
				if got := client.getEventCount(eventType); got != wfeArgs.Count {
					return fmt.Errorf("failed to assert count. want: %v, got: %v", wfeArgs.Count, got)
				}
			}
		}

		return nil
	default:
		return fmt.Errorf("unrecognized testRunner operation %q", op.Name)
	}
}

func executeLoop(ctx context.Context, args *loopArgs, loopDone <-chan struct{}) error {
	// setup entities
	entityMap := entities(ctx)
	if args.errorsStored() {
		if err := entityMap.addBSONArrayEntity(args.ErrorsEntityID); err != nil {
			return err
		}
	}
	if args.failuresStored() {
		if err := entityMap.addBSONArrayEntity(args.FailuresEntityID); err != nil {
			return err
		}
	}
	if args.successesStored() {
		if err := entityMap.addSuccessesEntity(args.SuccessesEntityID); err != nil {
			return err
		}
	}
	if args.iterationsStored() {
		if err := entityMap.addIterationsEntity(args.IterationsEntityID); err != nil {
			return err
		}
	}

	for {
		select {
		case <-loopDone:
			return nil
		default:
			if args.iterationsStored() {
				if err := entityMap.incrementIterations(args.IterationsEntityID); err != nil {
					return err
				}
			}
			var loopErr error
			for i, operation := range args.Operations {
				if operation.Name == "loop" {
					return fmt.Errorf("loop sub-operations should not include loop")
				}
				loopErr = operation.execute(ctx, loopDone)

				// if the operation errors, stop this loop
				if loopErr != nil {
					// If StoreFailures or StoreErrors is set, continue looping, otherwise break
					if !args.errorsStored() && !args.failuresStored() {
						return fmt.Errorf("error running loop operation %v : %v", i, loopErr)
					}
					errDoc := bson.Raw(bsoncore.NewDocumentBuilder().
						AppendString("error", loopErr.Error()).
						AppendDouble("time", getSecondsSinceEpoch()).
						Build())
					var appendErr error
					switch {
					case !args.errorsStored(): // store errors as failures if storeErrorsAsEntity isn't specified
						appendErr = entityMap.appendBSONArrayEntity(args.FailuresEntityID, errDoc)
					case !args.failuresStored(): // store failures as errors if storeFailuressAsEntity isn't specified
						appendErr = entityMap.appendBSONArrayEntity(args.ErrorsEntityID, errDoc)
					// errors are test runner errors
					// TODO GODRIVER-1950: use error types to determine error vs failure instead of depending on the
					// TODO fact that operation.execute prepends "execution failed" to test runner errors
					case strings.Contains(loopErr.Error(), "execution failed: "):
						appendErr = entityMap.appendBSONArrayEntity(args.ErrorsEntityID, errDoc)
					// failures are if an operation returns an incorrect result or error
					default:
						appendErr = entityMap.appendBSONArrayEntity(args.FailuresEntityID, errDoc)
					}
					if appendErr != nil {
						return appendErr
					}
					// if a sub-operation errors, restart the loop
					break
				}
				if args.successesStored() {
					if err := entityMap.incrementSuccesses(args.SuccessesEntityID); err != nil {
						return err
					}
				}
			}
		}
	}
}

type waitForEventArguments struct {
	ClientID string              `bson:"client"`
	Event    map[string]bson.Raw `bson:"event"`
	Count    int32               `bson:"count"`
}

// getServerDescriptionChangedEventCount will return "true" if a specific
// server description change event has occurred, up to the description type.
//
// If the bson.Raw value is empty, then this function will only consider if a
// serverDescriptionChangeEvent has occurred at all.
//
// If the bson.Raw contains newDescription and/or previousDescription, this
// function will attempt to compare them to events up to the fields defined in
// the UST specifications.
func getServerDescriptionChangedEventCount(client *clientEntity, raw bson.Raw) int32 {
	if len(raw) == 0 {
		return 0
	}

	// If the document has no values, then we assume that the UST only
	// intends to check that the event happened.
	if values, _ := raw.Values(); len(values) == 0 {
		return client.getEventCount(serverDescriptionChangedEvent)
	}

	var expectedEvt serverDescriptionChangedEventInfo
	if err := bson.Unmarshal(raw, &expectedEvt); err != nil {
		return 0
	}

	return client.getServerDescriptionChangedEventCount(expectedEvt)
}

// eventCompleted will check all of the events in the event map and return true if all of the events have at least the
// specified number of occurrences. If the event map is empty, it will return true.
func (args waitForEventArguments) eventCompleted(client *clientEntity) bool {
	for rawEventType, eventDoc := range args.Event {
		eventType, ok := monitoringEventTypeFromString(rawEventType)
		if !ok {
			return false
		}

		switch eventType {
		case serverDescriptionChangedEvent:
			if getServerDescriptionChangedEventCount(client, eventDoc) < args.Count {
				return false
			}
		default:
			if client.getEventCount(eventType) < args.Count {
				return false
			}
		}
	}

	return true
}

func waitForEvent(ctx context.Context, args waitForEventArguments) error {
	client, err := entities(ctx).client(args.ClientID)
	if err != nil {
		return err
	}

	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("timed out waiting for event: %v", ctx.Err())
		default:
			if args.eventCompleted(client) {
				return nil
			}

		}

		time.Sleep(100 * time.Millisecond)
	}
}

func extractClientSession(sess mongo.Session) *session.Client {
	return sess.(mongo.XSession).ClientSession()
}

func verifySessionPinnedState(ctx context.Context, sessionID string, expectedPinned bool) error {
	sess, err := entities(ctx).session(sessionID)
	if err != nil {
		return err
	}

	if isPinned := extractClientSession(sess).PinnedServer != nil; expectedPinned != isPinned {
		return fmt.Errorf("session pinned state mismatch; expected to be pinned: %v, is pinned: %v", expectedPinned, isPinned)
	}
	return nil
}

func verifyLastTwoLsidsEqual(ctx context.Context, clientID string, expectedEqual bool) error {
	client, err := entities(ctx).client(clientID)
	if err != nil {
		return err
	}

	allEvents := client.startedEvents()
	if len(allEvents) < 2 {
		return fmt.Errorf("client has recorded fewer than two command started events")
	}
	lastTwoEvents := allEvents[len(allEvents)-2:]

	firstID, err := lastTwoEvents[0].Command.LookupErr("lsid")
	if err != nil {
		return fmt.Errorf("first command has no 'lsid' field: %v", client.started[0].Command)
	}
	secondID, err := lastTwoEvents[1].Command.LookupErr("lsid")
	if err != nil {
		return fmt.Errorf("first command has no 'lsid' field: %v", client.started[1].Command)
	}

	areEqual := firstID.Equal(secondID)
	if expectedEqual && !areEqual {
		return fmt.Errorf("expected last two lsids to be equal, but got %s and %s", firstID, secondID)
	}
	if !expectedEqual && areEqual {
		return fmt.Errorf("expected last two lsids to be different but both were %s", firstID)
	}
	return nil
}

func verifySessionDirtyState(ctx context.Context, sessionID string, expectedDirty bool) error {
	sess, err := entities(ctx).session(sessionID)
	if err != nil {
		return err
	}

	if isDirty := extractClientSession(sess).Dirty; expectedDirty != isDirty {
		return fmt.Errorf("session dirty state mismatch; expected to be dirty: %v, is dirty: %v", expectedDirty, isDirty)
	}
	return nil
}

func verifyCollectionExists(ctx context.Context, dbName, collName string, expectedExists bool) error {
	db := mtest.GlobalClient().Database(dbName)
	collections, err := db.ListCollectionNames(ctx, bson.M{"name": collName})
	if err != nil {
		return fmt.Errorf("error running ListCollectionNames: %v", err)
	}

	if exists := len(collections) == 1; expectedExists != exists {
		ns := fmt.Sprintf("%s.%s", dbName, collName)
		return fmt.Errorf("collection existence mismatch; expected namespace %q to exist: %v, exists: %v", ns,
			expectedExists, exists)
	}
	return nil
}

func verifyIndexExists(ctx context.Context, dbName, collName, indexName string, expectedExists bool) error {
	iv := mtest.GlobalClient().Database(dbName).Collection(collName).Indexes()
	cursor, err := iv.List(ctx)
	if err != nil {
		return fmt.Errorf("error running IndexView.List: %v", err)
	}
	defer cursor.Close(ctx)

	var exists bool
	for cursor.Next(ctx) {
		if lookupString(cursor.Current, "name") == indexName {
			exists = true
			break
		}
	}
	if expectedExists != exists {
		ns := fmt.Sprintf("%s.%s", dbName, collName)
		return fmt.Errorf("index existence mismatch: expected index %q to exist in namespace %q: %v, exists: %v",
			indexName, ns, expectedExists, exists)
	}
	return nil
}
