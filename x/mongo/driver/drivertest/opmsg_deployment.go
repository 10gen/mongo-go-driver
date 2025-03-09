// Copyright (C) MongoDB, Inc. 2017-present.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

package drivertest

import (
	"context"
	"errors"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/internal/csot"
	"go.mongodb.org/mongo-driver/v2/internal/driverutil"
	"go.mongodb.org/mongo-driver/v2/mongo/address"
	"go.mongodb.org/mongo-driver/v2/x/bsonx/bsoncore"
	"go.mongodb.org/mongo-driver/v2/x/mongo/driver"
	"go.mongodb.org/mongo-driver/v2/x/mongo/driver/description"
	"go.mongodb.org/mongo-driver/v2/x/mongo/driver/mnet"
	"go.mongodb.org/mongo-driver/v2/x/mongo/driver/wiremessage"
)

const (
	serverAddress          = address.Address("127.0.0.1:27017")
	maxDocumentSize uint32 = 16777216
	maxMessageSize  uint32 = 48000000
	maxBatchCount   uint32 = 100000
)

var (
	sessionTimeoutMinutes int64 = 30

	// MockDescription is the server description used for the mock deployment. Each mocked connection returns this
	// value from its Description method.
	MockDescription = description.Server{
		CanonicalAddr:         serverAddress,
		MaxDocumentSize:       maxDocumentSize,
		MaxMessageSize:        maxMessageSize,
		MaxBatchCount:         maxBatchCount,
		SessionTimeoutMinutes: &sessionTimeoutMinutes,
		Kind:                  description.ServerKindRSPrimary,
		WireVersion: &description.VersionRange{
			Max: driverutil.MaxWireVersion,
		},
	}
)

// connection implements the driver.Connection interface and responds to wire messages with pre-configured responses.
type connection struct {
	responses []bson.D // responses to send when ReadWireMessage is called
}

var _ mnet.ReadWriteCloser = &connection{}
var _ mnet.Describer = &connection{}

// Write is a no-op.
func (c *connection) Write(context.Context, []byte) error {
	return nil
}

func (c *connection) OIDCTokenGenID() uint64 {
	return 0
}

func (c *connection) SetOIDCTokenGenID(uint64) {
}

// Read returns the next response in the connection's list of responses.
func (c *connection) Read(_ context.Context, _ ...mnet.ReadOption) ([]byte, error) {
	var dst []byte
	if len(c.responses) == 0 {
		return dst, errors.New("no responses remaining")
	}
	nextRes := c.responses[0]
	c.responses = c.responses[1:]

	var wmindex int32
	wmindex, dst = wiremessage.AppendHeaderStart(dst, wiremessage.NextRequestID(), 0, wiremessage.OpMsg)
	dst = wiremessage.AppendMsgFlags(dst, 0)
	dst = wiremessage.AppendMsgSectionType(dst, wiremessage.SingleDocument)
	resBytes, _ := bson.Marshal(nextRes)
	dst = append(dst, resBytes...)
	dst = bsoncore.UpdateLength(dst, wmindex, int32(len(dst[wmindex:])))
	return dst, nil
}

// Description returns a fixed server description for the connection.
func (c *connection) Description() description.Server {
	return MockDescription
}

// Close is a no-op operation.
func (*connection) Close() error {
	return nil
}

// ID returns a fixed identifier for the connection.
func (*connection) ID() string {
	return "<mock_connection>"
}

// DriverConnectionID returns a fixed identifier for the driver pool connection.
func (*connection) DriverConnectionID() int64 {
	return 0
}

// ServerConnectionID returns a fixed identifier for the server connection.
func (*connection) ServerConnectionID() *int64 {
	serverConnectionID := int64(42)
	return &serverConnectionID
}

// Address returns a fixed address for the connection.
func (*connection) Address() address.Address {
	return serverAddress
}

// Stale returns if the connection is stale.
func (*connection) Stale() bool {
	return false
}

// MockDeployment wraps a connection and implements the driver.Deployment interface.
type MockDeployment struct {
	conn    *connection
	updates chan description.Topology
}

var _ driver.Deployment = &MockDeployment{}
var _ driver.Server = &MockDeployment{}
var _ driver.Connector = &MockDeployment{}
var _ driver.Disconnector = &MockDeployment{}
var _ driver.Subscriber = &MockDeployment{}

// SelectServer implements the Deployment interface. This method does not use the
// description.SelectedServer provided and instead returns itself. The Connections returned from the
// Connection method have a no-op Close method.
func (md *MockDeployment) SelectServer(context.Context, description.ServerSelector) (driver.Server, error) {
	return md, nil
}

// GetServerSelectionTimeout returns zero as a server selection timeout is not
// applicable for mock deployments.
func (*MockDeployment) GetServerSelectionTimeout() time.Duration {
	return 0
}

// Kind implements the Deployment interface. It always returns description.TopologyKindSingle.
func (md *MockDeployment) Kind() description.TopologyKind {
	return description.TopologyKindSingle
}

// Connection implements the driver.Server interface.
func (md *MockDeployment) Connection(context.Context) (*mnet.Connection, error) {
	return mnet.NewConnection(md.conn), nil
}

// RTTMonitor implements the driver.Server interface.
func (md *MockDeployment) RTTMonitor() driver.RTTMonitor {
	return &csot.ZeroRTTMonitor{}
}

// Connect is a no-op method which implements the driver.Connector interface.
func (md *MockDeployment) Connect() error {
	return nil
}

// Disconnect is a no-op method which implements the driver.Disconnector interface {
func (md *MockDeployment) Disconnect(context.Context) error {
	close(md.updates)
	return nil
}

// Subscribe returns a subscription from which new topology descriptions can be retrieved.
// Subscribe implements the driver.Subscriber interface.
func (md *MockDeployment) Subscribe() (*driver.Subscription, error) {
	if md.updates == nil {
		md.updates = make(chan description.Topology, 1)

		md.updates <- description.Topology{
			SessionTimeoutMinutes: &sessionTimeoutMinutes,
		}
	}

	return &driver.Subscription{
		Updates: md.updates,
	}, nil
}

// Unsubscribe is a no-op method which implements the driver.Subscriber interface.
func (md *MockDeployment) Unsubscribe(*driver.Subscription) error {
	return nil
}

// AddResponses adds responses to this mock deployment.
func (md *MockDeployment) AddResponses(responses ...bson.D) {
	md.conn.responses = append(md.conn.responses, responses...)
}

// ClearResponses clears all remaining responses in this mock deployment.
func (md *MockDeployment) ClearResponses() {
	md.conn.responses = md.conn.responses[:0]
}

// NewMockDeployment returns a mock driver.Deployment that responds with OP_MSG wire messages.
// However, for most use cases, we suggest testing with an actual database.
func NewMockDeployment(responses ...bson.D) *MockDeployment {
	return &MockDeployment{
		conn: &connection{
			responses: responses,
		},
	}
}
