// Copyright (C) MongoDB, Inc. 2023-present.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

package mnet

import (
	"context"
	"io"

	"go.mongodb.org/mongo-driver/v2/mongo/address"
	"go.mongodb.org/mongo-driver/v2/x/mongo/driver/description"
)

type ReadOption func(*ReadOptions)

type ReadOptions struct {
	HasMaxTimeMS bool
	RequestID    int32
}

func WithReadMaxTimeMS() ReadOption {
	return func(opts *ReadOptions) {
		opts.HasMaxTimeMS = true
	}
}

func WithRequestID(requestID int32) ReadOption {
	return func(opts *ReadOptions) {
		opts.RequestID = requestID
	}
}

// ReadWriteCloser represents a Connection where server operations
// can read from, written to, and closed.
type ReadWriteCloser interface {
	Read(ctx context.Context, opts ...ReadOption) ([]byte, error)
	Write(ctx context.Context, wm []byte) error
	io.Closer
}

// Describer represents a Connection that can be described.
type Describer interface {
	Description() description.Server
	ID() string
	ServerConnectionID() *int64
	DriverConnectionID() int64
	Address() address.Address
	Stale() bool
	OIDCTokenGenID() uint64
	SetOIDCTokenGenID(uint64)
}

// Streamer represents a Connection that supports streaming wire protocol
// messages using the moreToCome and exhaustAllowed flags.
//
// The SetStreaming and CurrentlyStreaming functions correspond to the
// moreToCome flag on server responses. If a response has moreToCome set,
// SetStreaming(true) will be called and CurrentlyStreaming() should return
// true.
//
// CanStream corresponds to the exhaustAllowed flag. The operations layer will
// set exhaustAllowed on outgoing wire messages to inform the server that the
// driver supports streaming.
type Streamer interface {
	SetStreaming(bool)
	CurrentlyStreaming() bool
	SupportsStreaming() bool
}

// Compressor is an interface used to compress wire messages. If a Connection
// supports compression it should implement this interface as well. The
// CompressWireMessage method will be called during the execution of an
// operation if the wire message is allowed to be compressed.
type Compressor interface {
	CompressWireMessage(src, dst []byte) ([]byte, error)
}

// Pinner represents a Connection that can be pinned by one or more cursors or
// transactions. Implementations of this interface should maintain the following
// invariants:
//
//  1. Each Pin* call should increment the number of references for the
//     connection.
//  2. Each Unpin* call should decrement the number of references for the
//     connection.
//  3. Calls to Close() should be ignored until all resources have unpinned the
//     connection.
type Pinner interface {
	PinToCursor() error
	PinToTransaction() error
	UnpinFromCursor() error
	UnpinFromTransaction() error
}

// Connection represents a connection to a MongoDB server.
type Connection struct {
	ReadWriteCloser
	Describer
	Streamer
	Compressor
	Pinner
}

// NewConnection creates a new Connection with the provided component. This
// constructor returns a component that is already a Connection to avoid
// mis-asserting the composite interfaces.
func NewConnection(component interface {
	ReadWriteCloser
	Describer
}) *Connection {
	if _, ok := component.(*Connection); ok {
		return component.(*Connection)
	}

	conn := &Connection{
		ReadWriteCloser: component,
	}

	if describer, ok := component.(Describer); ok {
		conn.Describer = describer
	}

	if streamer, ok := component.(Streamer); ok {
		conn.Streamer = streamer
	}

	if compressor, ok := component.(Compressor); ok {
		conn.Compressor = compressor
	}

	if pinner, ok := component.(Pinner); ok {
		conn.Pinner = pinner
	}

	return conn
}
