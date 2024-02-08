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

type timeoutKey struct{}

// MakeTimeoutContext returns a new context with Client-Side Operation Timeout (CSOT) feature-gated behavior
// and a Timeout set to the passed in Duration. Setting a Timeout on a single operation is not supported in
// public API.
//
// TODO(GODRIVER-2348) We may be able to remove this function once CSOT feature-gated behavior becomes the
// TODO default behavior.
func MakeTimeoutContext(ctx context.Context, to time.Duration) (context.Context, context.CancelFunc) {
	// Only use the passed in Duration as a timeout on the Context if it
	// is non-zero.
	cancelFunc := func() {}
	if to != 0 {
		ctx, cancelFunc = context.WithTimeout(ctx, to)
	}
	return context.WithValue(ctx, timeoutKey{}, true), cancelFunc
}

func IsTimeoutContext(ctx context.Context) bool {
	return ctx.Value(timeoutKey{}) != nil
}

type skipMaxTime struct{}

// NewSkipMaxTimeContext returns a new context with a "skipMaxTime" value that
// is used to inform operation construction to not add a maxTimeMS to a wire
// message, regardless of a context deadline. This is specifically used for
// monitoring where non-awaitable hello commands are put on the wire.
func NewSkipMaxTimeContext(ctx context.Context) context.Context {
	return context.WithValue(ctx, skipMaxTime{}, true)
}

// IsSkipMaxTimeContext checks if the provided context has been assigned the
// "skipMaxTime" value.
func IsSkipMaxTimeContext(ctx context.Context) bool {
	return ctx.Value(skipMaxTime{}) != nil
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
