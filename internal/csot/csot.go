// Copyright (C) MongoDB, Inc. 2022-present.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

package csot

import (
	"context"
	"time"
)

type withoutMaxTime struct{}

// WithoutMaxTime returns a new context with a "withoutMaxTime" value that
// is used to inform operation construction to not add a maxTimeMS to a wire
// message, regardless of a context deadline. This is specifically used for
// monitoring where non-awaitable hello commands are put on the wire, or to
// indicate that the user has set a "0" (i.e. infinite) CSOT.
func WithoutMaxTime(ctx context.Context) context.Context {
	return context.WithValue(ctx, withoutMaxTime{}, true)
}

// IsWithoutMaxTime checks if the provided context has been assigned the
// "withoutMaxTime" value.
func IsWithoutMaxTime(ctx context.Context) bool {
	return ctx.Value(withoutMaxTime{}) != nil
}

// WithTimeout will apply the given timeout to the parent context, if there is
// not already a deadline on the context. Per the CSOT specifications, if the
// parent context already has a deadline it is an operation-level timeout which
// takes precedence over the client-level timeout.
//
// This helper function is meant to be used to timeout functions that require
// multiple trips to the server but must share one timeout. See gridfs methods
// for specific use cases.
func WithTimeout(parent context.Context, timeout *time.Duration) (context.Context, context.CancelFunc) {
	cancel := func() {}

	_, ok := parent.Deadline()

	// If there already exists a deadline or, if not and the timeout is nil, then
	// do nothing.
	if ok || timeout == nil || IsWithoutMaxTime(parent) {
		return parent, cancel
	}

	// If the timeout is zero, or zero has already been explicitly set as the
	// deadline on the context, then signal to not use maxTimeMS on WMs.
	if timeout != nil && *timeout == 0 {
		parent = WithoutMaxTime(parent)

		return parent, cancel
	}

	// If the timeout is set, then apply it to the parent.
	return context.WithTimeout(parent, *timeout)
}

func IsTimeoutContext(ctx context.Context) bool {
	_, ok := ctx.Deadline()

	return ok || IsWithoutMaxTime(ctx)
}

// WithServerSelectionTimeout creates a context with a timeout that is the
// minimum of serverSelectionTimeoutMS and context deadline. The usage of
// non-positive values for serverSelectionTimeoutMS are an anti-pattern and are
// not considered in this calculation.
func WithServerSelectionTimeout(
	parent context.Context,
	serverSelectionTimeout time.Duration,
) (context.Context, context.CancelFunc) {
	var timeout time.Duration

	deadline, ok := parent.Deadline()
	if ok {
		timeout = time.Until(deadline)
	}

	// If there is no deadline on the parent context and the server selection
	// timeout DNE, then do nothing.
	if !ok && serverSelectionTimeout <= 0 {
		return parent, func() {}
	}

	// Otherwise, take the minimum of the two and return a new context with that
	// value as the deadline.
	if !ok {
		timeout = serverSelectionTimeout
	} else if timeout >= serverSelectionTimeout && serverSelectionTimeout > 0 {
		// Only use the serverSelectionTimeout value if it is less than the existing
		// timeout and is positive.
		timeout = serverSelectionTimeout
	}

	return context.WithTimeout(parent, timeout)
}

// WithChangeStreamNextContext applies the timeout rules to the parent context
// for calling "next" on a ChangeStream object. In particular, drivers MUST
// apply the original timeoutMS value to each next call on the change stream
// but MUST NOT use it to derive a maxTimeMS field for getMore commands.
func WithChangeStreamNextContext(parent context.Context, timeout *time.Duration) (context.Context, context.CancelFunc) {
	ctx := parent
	cancel := func() {}

	// If there is no parent deadline, then apply a non-zero timeout.
	if _, ok := parent.Deadline(); !ok && timeout != nil && *timeout > 0 {
		ctx, cancel = context.WithTimeout(parent, *timeout)
	}

	return WithoutMaxTime(ctx), cancel
}

// ValidChangeStreamTimeouts will return "false" if maxAwaitTimeMS is set,
// timeoutMS is set to a non-zero value, and maxAwaitTimeMS is greater than or
// equal to timeoutMS. Otherwise, the timeouts are valid.
func ValidChangeStreamTimeouts(ctx context.Context, maxAwaitTime, timeout *time.Duration) bool {
	if maxAwaitTime == nil {
		return true
	}

	if deadline, ok := ctx.Deadline(); ok {
		ctxTimeout := time.Until(deadline)
		timeout = &ctxTimeout
	}

	if timeout == nil {
		return true
	}

	return *timeout <= 0 || *maxAwaitTime < *timeout
}

// ZeroRTTMonitor implements the RTTMonitor interface and is used internally for testing. It returns 0 for all
// RTT calculations and an empty string for RTT statistics.
type ZeroRTTMonitor struct{}

// EWMA implements the RTT monitor interface.
func (zrm *ZeroRTTMonitor) EWMA() time.Duration {
	return 0
}

// Min implements the RTT monitor interface.
func (zrm *ZeroRTTMonitor) Min() time.Duration {
	return 0
}

// P90 implements the RTT monitor interface.
func (zrm *ZeroRTTMonitor) P90() time.Duration {
	return 0
}

// Stats implements the RTT monitor interface.
func (zrm *ZeroRTTMonitor) Stats() string {
	return ""
}
