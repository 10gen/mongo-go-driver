// Copyright (C) MongoDB, Inc. 2017-present.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

package unified

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/mongo"
)

// ctxKey is used to define keys for values stored in context.Context objects.
type ctxKey string

const (
	// entitiesKey is used to store an EntityMap instance in a Context.
	entitiesKey ctxKey = "test-entities"
	// failPointsKey is used to store a map from a fail point name to the Client instance used to configure it.
	failPointsKey ctxKey = "test-failpoints"
	// targetedFailPointsKey is used to store a map from a fail point name to the host on which the fail point is set.
	targetedFailPointsKey ctxKey = "test-targeted-failpoints"
)

// NewTestContext creates a new Context derived from ctx with values initialized to store the state required for test
// execution.
func NewTestContext(ctx context.Context) context.Context {
	ctx = context.WithValue(ctx, entitiesKey, NewEntityMap())
	ctx = context.WithValue(ctx, failPointsKey, make(map[string]*mongo.Client))
	ctx = context.WithValue(ctx, targetedFailPointsKey, make(map[string]string))
	return ctx
}

func AddFailPoint(ctx context.Context, failPoint string, client *mongo.Client) error {
	failPoints := ctx.Value(failPointsKey).(map[string]*mongo.Client)
	if _, ok := failPoints[failPoint]; ok {
		return fmt.Errorf("fail point %q already exists in tracked fail points map", failPoint)
	}

	failPoints[failPoint] = client
	return nil
}

func AddTargetedFailPoint(ctx context.Context, failPoint string, host string) error {
	failPoints := ctx.Value(failPointsKey).(map[string]string)
	if _, ok := failPoints[failPoint]; ok {
		return fmt.Errorf("fail point %q already exists in tracked targeted fail points map", failPoint)
	}

	failPoints[failPoint] = host
	return nil
}

func FailPoints(ctx context.Context) map[string]*mongo.Client {
	return ctx.Value(failPointsKey).(map[string]*mongo.Client)
}

func TargetedFailPoints(ctx context.Context) map[string]string {
	return ctx.Value(targetedFailPointsKey).(map[string]string)
}

func Entities(ctx context.Context) *EntityMap {
	return ctx.Value(entitiesKey).(*EntityMap)
}
