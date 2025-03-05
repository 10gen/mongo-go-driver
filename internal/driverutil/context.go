// Copyright (C) MongoDB, Inc. 2025-present.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

package driverutil

import "context"

// ContextKey is a custom type used for the keys in context values to avoid
// collisions.
type ContextKey string

const (
	// ContextKeyHasMaxTimeMS represents a boolean value that indicates if
	// maxTimeMS will be set on the wire message for an operation.
	ContextKeyHasMaxTimeMS ContextKey = "hasMaxTimeMS"

	// ContextKeyRequestID is the requestID for a given operation. This is used to
	// propagate the requestID for a pending read during connection check out.
	ContextKeyRequestID ContextKey = "requestID"
)

// WithValueHasMaxTimeMS returns a copy of the parent context with an added
// value indicating whether an operation will append maxTimeMS to the wire
// message.
func WithValueHasMaxTimeMS(parentCtx context.Context, val bool) context.Context {
	return context.WithValue(parentCtx, ContextKeyHasMaxTimeMS, val)
}

// WithRequestID returns a copy of the parent context with an added request ID
// value.
func WithRequestID(parentCtx context.Context, requestID int32) context.Context {
	return context.WithValue(parentCtx, ContextKeyRequestID, requestID)
}

// HasMaxTimeMS checks if the context is for an operation that will append
// maxTimeMS to the wire message.
func HasMaxTimeMS(ctx context.Context) bool {
	return ctx.Value(ContextKeyHasMaxTimeMS) != nil
}

// GetRequestID retrieves the request ID from the context if it exists.
func GetRequestID(ctx context.Context) (int32, bool) {
	val, ok := ctx.Value(ContextKeyRequestID).(int32)

	return val, ok
}
