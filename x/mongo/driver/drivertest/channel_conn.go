// Copyright (C) MongoDB, Inc. 2017-present.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

package drivertest

import (
	"context"
	"errors"

	"go.mongodb.org/mongo-driver/v2/mongo/address"
	"go.mongodb.org/mongo-driver/v2/x/bsonx/bsoncore"
	"go.mongodb.org/mongo-driver/v2/x/mongo/driver/description"
	"go.mongodb.org/mongo-driver/v2/x/mongo/driver/mnet"
	"go.mongodb.org/mongo-driver/v2/x/mongo/driver/wiremessage"
)

// ChannelConn implements the driver.Connection interface by reading and writing wire messages
// to a channel
type ChannelConn struct {
	WriteErr error
	Written  chan []byte
	ReadResp chan []byte
	ReadErr  chan error
	Desc     description.Server
}

// OIDCTokenGenID implements the driver.Connection interface by returning the OIDCToken generation
// (which is always 0)
func (c *ChannelConn) OIDCTokenGenID() uint64 {
	return 0
}

// SetOIDCTokenGenID implements the driver.Connection interface by setting the OIDCToken generation
// (which is always 0)
func (c *ChannelConn) SetOIDCTokenGenID(uint64) {}

// WriteWireMessage implements the driver.Connection interface.
func (c *ChannelConn) Write(ctx context.Context, wm []byte) error {
	// Copy wm in case it came from a buffer pool.
	b := make([]byte, len(wm))
	copy(b, wm)
	select {
	case c.Written <- b:
	case <-ctx.Done():
		return ctx.Err()
	default:
		c.WriteErr = errors.New("could not write wiremessage to written channel")
	}
	return c.WriteErr
}

// ReadWireMessage implements the driver.Connection interface.
func (c *ChannelConn) Read(ctx context.Context, _ ...mnet.ReadOption) ([]byte, error) {
	var wm []byte
	var err error
	select {
	case wm = <-c.ReadResp:
	case err = <-c.ReadErr:
	case <-ctx.Done():
		err = ctx.Err()
	}
	return wm, err
}

// Description implements the driver.Connection interface.
func (c *ChannelConn) Description() description.Server { return c.Desc }

// Close implements the driver.Connection interface.
func (c *ChannelConn) Close() error {
	return nil
}

// ID implements the driver.Connection interface.
func (c *ChannelConn) ID() string {
	return "faked"
}

// DriverConnectionID implements the driver.Connection interface.
func (c *ChannelConn) DriverConnectionID() int64 {
	return 0
}

// ServerConnectionID implements the driver.Connection interface.
func (c *ChannelConn) ServerConnectionID() *int64 {
	serverConnectionID := int64(42)
	return &serverConnectionID
}

// Address implements the driver.Connection interface.
func (c *ChannelConn) Address() address.Address { return address.Address("0.0.0.0") }

// Stale implements the driver.Connection interface.
func (c *ChannelConn) Stale() bool {
	return false
}

// MakeReply creates an OP_REPLY wiremessage from a BSON document
func MakeReply(doc bsoncore.Document) []byte {
	var dst []byte
	idx, dst := wiremessage.AppendHeaderStart(dst, 10, 9, wiremessage.OpReply)
	dst = wiremessage.AppendReplyFlags(dst, 0)
	dst = wiremessage.AppendReplyCursorID(dst, 0)
	dst = wiremessage.AppendReplyStartingFrom(dst, 0)
	dst = wiremessage.AppendReplyNumberReturned(dst, 1)
	dst = append(dst, doc...)
	return bsoncore.UpdateLength(dst, idx, int32(len(dst[idx:])))
}

// GetCommandFromQueryWireMessage returns the command sent in an OP_QUERY wire message.
func GetCommandFromQueryWireMessage(wm []byte) (bsoncore.Document, error) {
	var ok bool
	_, _, _, _, wm, ok = wiremessage.ReadHeader(wm)
	if !ok {
		return nil, errors.New("could not read header")
	}
	_, wm, ok = wiremessage.ReadQueryFlags(wm)
	if !ok {
		return nil, errors.New("could not read flags")
	}
	_, wm, ok = wiremessage.ReadQueryFullCollectionName(wm)
	if !ok {
		return nil, errors.New("could not read fullCollectionName")
	}
	_, wm, ok = wiremessage.ReadQueryNumberToSkip(wm)
	if !ok {
		return nil, errors.New("could not read numberToSkip")
	}
	_, wm, ok = wiremessage.ReadQueryNumberToReturn(wm)
	if !ok {
		return nil, errors.New("could not read numberToReturn")
	}

	var query bsoncore.Document
	query, wm, ok = wiremessage.ReadQueryQuery(wm)
	if !ok {
		return nil, errors.New("could not read query")
	}
	return query, nil
}

// GetCommandFromMsgWireMessage returns the command document sent in an OP_MSG wire message.
func GetCommandFromMsgWireMessage(wm []byte) (bsoncore.Document, error) {
	var ok bool
	_, _, _, _, wm, ok = wiremessage.ReadHeader(wm)
	if !ok {
		return nil, errors.New("could not read header")
	}

	_, wm, ok = wiremessage.ReadMsgFlags(wm)
	if !ok {
		return nil, errors.New("could not read flags")
	}
	_, wm, ok = wiremessage.ReadMsgSectionType(wm)
	if !ok {
		return nil, errors.New("could not read section type")
	}

	cmdDoc, wm, ok := wiremessage.ReadMsgSectionSingleDocument(wm)
	if !ok {
		return nil, errors.New("could not read command document")
	}
	return cmdDoc, nil
}
