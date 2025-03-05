// Copyright (C) MongoDB, Inc. 2025-present.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

package driverutil

import "context"

type ContextKey string

const (
	ContextKeyHasMaxTimeMS ContextKey = "hasMaxTimeMS"
	ContextKeyRequestID    ContextKey = "requestID"
)

func WithValueHasMaxTimeMS(parentCtx context.Context, val bool) context.Context {
	return context.WithValue(parentCtx, ContextKeyHasMaxTimeMS, val)
}

func WithRequestID(parentCtx context.Context, requestID int32) context.Context {
	return context.WithValue(parentCtx, ContextKeyRequestID, requestID)
}

func HasMaxTimeMS(ctx context.Context) bool {
	return ctx.Value(ContextKeyHasMaxTimeMS) != nil
}

func GetRequestID(ctx context.Context) (int32, bool) {
	val, ok := ctx.Value(ContextKeyRequestID).(int32)

	return val, ok
}
