// Copyright (C) MongoDB, Inc. 2017-present.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

package unified

import (
	"strings"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/event"
)

type monitoringEventType string

const (
	commandStartedEvent             monitoringEventType = "CommandStartedEvent"
	commandSucceededEvent           monitoringEventType = "CommandSucceededEvent"
	commandFailedEvent              monitoringEventType = "CommandFailedEvent"
	poolCreatedEvent                monitoringEventType = "PoolCreatedEvent"
	poolReadyEvent                  monitoringEventType = "PoolReadyEvent"
	poolClearedEvent                monitoringEventType = "PoolClearedEvent"
	poolClosedEvent                 monitoringEventType = "PoolClosedEvent"
	connectionCreatedEvent          monitoringEventType = "ConnectionCreatedEvent"
	connectionReadyEvent            monitoringEventType = "ConnectionReadyEvent"
	connectionClosedEvent           monitoringEventType = "ConnectionClosedEvent"
	connectionCheckOutStartedEvent  monitoringEventType = "ConnectionCheckOutStartedEvent"
	connectionCheckOutFailedEvent   monitoringEventType = "ConnectionCheckOutFailedEvent"
	connectionCheckedOutEvent       monitoringEventType = "ConnectionCheckedOutEvent"
	connectionCheckedInEvent        monitoringEventType = "ConnectionCheckedInEvent"
	connectionPendingReadStarted    monitoringEventType = "ConnectionPendingReadStarted"
	connectionPendingReadSucceeded  monitoringEventType = "ConnectionPendingReadSucceeded"
	connectionPendingReadFailed     monitoringEventType = "ConnectionPendingReadFailed"
	serverDescriptionChangedEvent   monitoringEventType = "ServerDescriptionChangedEvent"
	serverHeartbeatFailedEvent      monitoringEventType = "ServerHeartbeatFailedEvent"
	serverHeartbeatStartedEvent     monitoringEventType = "ServerHeartbeatStartedEvent"
	serverHeartbeatSucceededEvent   monitoringEventType = "ServerHeartbeatSucceededEvent"
	topologyDescriptionChangedEvent monitoringEventType = "TopologyDescriptionChangedEvent"
)

func monitoringEventTypeFromString(eventStr string) (monitoringEventType, bool) {
	switch strings.ToLower(eventStr) {
	case "commandstartedevent":
		return commandStartedEvent, true
	case "commandsucceededevent":
		return commandSucceededEvent, true
	case "commandfailedevent":
		return commandFailedEvent, true
	case "poolcreatedevent":
		return poolCreatedEvent, true
	case "poolreadyevent":
		return poolReadyEvent, true
	case "poolclearedevent":
		return poolClearedEvent, true
	case "poolclosedevent":
		return poolClosedEvent, true
	case "connectioncreatedevent":
		return connectionCreatedEvent, true
	case "connectionreadyevent":
		return connectionReadyEvent, true
	case "connectionclosedevent":
		return connectionClosedEvent, true
	case "connectioncheckoutstartedevent":
		return connectionCheckOutStartedEvent, true
	case "connectioncheckoutfailedevent":
		return connectionCheckOutFailedEvent, true
	case "connectioncheckedoutevent":
		return connectionCheckedOutEvent, true
	case "connectioncheckedinevent":
		return connectionCheckedInEvent, true
	case "connectionpendingreadstarted":
		return connectionPendingReadStarted, true
	case "connectionpendingreadsucceeded":
		return connectionPendingReadSucceeded, true
	case "connectionpendingreadfailed":
		return connectionPendingReadFailed, true
	case "serverdescriptionchangedevent":
		return serverDescriptionChangedEvent, true
	case "serverheartbeatfailedevent":
		return serverHeartbeatFailedEvent, true
	case "serverheartbeatstartedevent":
		return serverHeartbeatStartedEvent, true
	case "serverheartbeatsucceededevent":
		return serverHeartbeatSucceededEvent, true
	case "topologydescriptionchangedevent":
		return topologyDescriptionChangedEvent, true
	default:
		return "", false
	}
}

func monitoringEventTypeFromPoolEvent(evt *event.PoolEvent) monitoringEventType {
	switch evt.Type {
	case event.ConnectionPoolCreated:
		return poolCreatedEvent
	case event.ConnectionPoolReady:
		return poolReadyEvent
	case event.ConnectionPoolCleared:
		return poolClearedEvent
	case event.ConnectionPoolClosed:
		return poolClosedEvent
	case event.ConnectionCreated:
		return connectionCreatedEvent
	case event.ConnectionReady:
		return connectionReadyEvent
	case event.ConnectionClosed:
		return connectionClosedEvent
	case event.ConnectionCheckOutStarted:
		return connectionCheckOutStartedEvent
	case event.ConnectionCheckOutFailed:
		return connectionCheckOutFailedEvent
	case event.ConnectionCheckedOut:
		return connectionCheckedOutEvent
	case event.ConnectionCheckedIn:
		return connectionCheckedInEvent
	case event.ConnectionPendingReadStarted:
		return connectionPendingReadStarted
	case event.ConnectionPendingReadSucceeded:
		return connectionPendingReadSucceeded
	case event.ConnectionPendingReadFailed:
		return connectionPendingReadFailed
	default:
		return ""
	}
}

// serverDescription represents a description of a server.
type serverDescription struct {
	// Type is the type of the server in the description. Test runners MUST
	// assert that the type in the published event matches this value.
	Type string
}

// serverDescriptionChangedEventInfo represents an event generated when the server
// description changes.
type serverDescriptionChangedEventInfo struct {
	// NewDescription  corresponds to the server description as it was after
	// the change that triggered this event.
	NewDescription *serverDescription

	// PreviousDescription corresponds to the server description as it was
	// before the change that triggered this event
	PreviousDescription *serverDescription
}

// newServerDescriptionChangedEventInfo returns a new serverDescriptionChangedEvent
// instance for the given event.
func newServerDescriptionChangedEventInfo(evt *event.ServerDescriptionChangedEvent) *serverDescriptionChangedEventInfo {
	return &serverDescriptionChangedEventInfo{
		NewDescription: &serverDescription{
			Type: evt.NewDescription.Kind,
		},
		PreviousDescription: &serverDescription{
			Type: evt.PreviousDescription.Kind,
		},
	}
}

// UnmarshalBSON unmarshals the event from BSON, used when trying to create the
// expected event from a unified spec test.
func (evt *serverDescriptionChangedEventInfo) UnmarshalBSON(data []byte) error {
	if len(data) == 0 {
		return nil
	}

	var raw bson.Raw
	if err := bson.Unmarshal(data, &raw); err != nil {
		return err
	}

	// Lookup the previous description, if any.
	if newDescription, err := raw.LookupErr("newDescription"); err == nil {
		evt.NewDescription = &serverDescription{
			Type: newDescription.Document().Lookup("type").StringValue(),
		}
	}

	// Lookup the previous description, if any.
	if previousDescription, err := raw.LookupErr("previousDescription"); err == nil {
		evt.PreviousDescription = &serverDescription{
			Type: previousDescription.Document().Lookup("type").StringValue(),
		}
	}

	return nil
}
