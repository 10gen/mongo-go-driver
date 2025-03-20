// Copyright (C) MongoDB, Inc. 2017-present.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

package unified

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/event"
)

type commandMonitoringEvent struct {
	CommandStartedEvent *struct {
		Command               bson.Raw `bson:"command"`
		CommandName           *string  `bson:"commandName"`
		DatabaseName          *string  `bson:"databaseName"`
		HasServerConnectionID *bool    `bson:"hasServerConnectionId"`
		HasServiceID          *bool    `bson:"hasServiceId"`
	} `bson:"commandStartedEvent"`

	CommandSucceededEvent *struct {
		CommandName           *string  `bson:"commandName"`
		DatabaseName          *string  `bson:"databaseName"`
		Reply                 bson.Raw `bson:"reply"`
		HasServerConnectionID *bool    `bson:"hasServerConnectionId"`
		HasServiceID          *bool    `bson:"hasServiceId"`
	} `bson:"commandSucceededEvent"`

	CommandFailedEvent *struct {
		CommandName           *string `bson:"commandName"`
		DatabaseName          *string `bson:"databaseName"`
		HasServerConnectionID *bool   `bson:"hasServerConnectionId"`
		HasServiceID          *bool   `bson:"hasServiceId"`
	} `bson:"commandFailedEvent"`
}

type cmapEvent struct {
	ConnectionCreatedEvent *struct{} `bson:"connectionCreatedEvent"`

	ConnectionReadyEvent *struct{} `bson:"connectionReadyEvent"`

	ConnectionClosedEvent *struct {
		Reason *string `bson:"reason"`
	} `bson:"connectionClosedEvent"`

	ConnectionCheckedOutEvent *struct{} `bson:"connectionCheckedOutEvent"`

	ConnectionCheckOutFailedEvent *struct {
		Reason *string `bson:"reason"`
	} `bson:"connectionCheckOutFailedEvent"`

	ConnectionCheckedInEvent       *struct{} `bson:"connectionCheckedInEvent"`
	ConnectionPendingReadStarted   *struct{} `bson:"connectionPendingReadStarted"`
	ConnectionPendingreadSucceeded *struct{} `bson:"connectionPendingReadSucceeded"`
	ConnectionPendingReadFailed    *struct{} `bson:"connectionPendingReadFailed"`

	PoolClearedEvent *struct {
		HasServiceID              *bool `bson:"hasServiceId"`
		InterruptInUseConnections *bool `bson:"interruptInUseConnections"`
	} `bson:"poolClearedEvent"`
}

type sdamEvent struct {
	ServerDescriptionChangedEvent *struct {
		NewDescription *struct {
			Type *string `bson:"type"`
		} `bson:"newDescription"`

		PreviousDescription *struct {
			Type *string `bson:"type"`
		} `bson:"previousDescription"`
	} `bson:"serverDescriptionChangedEvent"`

	ServerHeartbeatStartedEvent *struct {
		Awaited *bool `bson:"awaited"`
	} `bson:"serverHeartbeatStartedEvent"`

	ServerHeartbeatSucceededEvent *struct {
		Awaited *bool `bson:"awaited"`
	} `bson:"serverHeartbeatSucceededEvent"`

	ServerHeartbeatFailedEvent *struct {
		Awaited *bool `bson:"awaited"`
	} `bson:"serverHeartbeatFailedEvent"`

	TopologyDescriptionChangedEvent *struct{} `bson:"topologyDescriptionChangedEvent"`
}

type expectedEvents struct {
	ClientID          string `bson:"client"`
	CommandEvents     []commandMonitoringEvent
	CMAPEvents        []cmapEvent
	SDAMEvents        []sdamEvent
	IgnoreExtraEvents *bool
}

var _ bson.Unmarshaler = (*expectedEvents)(nil)

func (e *expectedEvents) UnmarshalBSON(data []byte) error {
	// The data to be unmarshalled looks like {client: <client ID>, eventType: <string>, events: [event0, event1, ...]}.
	// We use the "eventType" value to determine which struct field should be used to deserialize the "events" array.

	var temp struct {
		ClientID          string                 `bson:"client"`
		EventType         string                 `bson:"eventType"`
		Events            bson.RawValue          `bson:"events"`
		IgnoreExtraEvents *bool                  `bson:"ignoreExtraEvents"`
		Extra             map[string]interface{} `bson:",inline"`
	}
	if err := bson.Unmarshal(data, &temp); err != nil {
		return fmt.Errorf("error unmarshalling to temporary expectedEvents object: %w", err)
	}
	if len(temp.Extra) > 0 {
		return fmt.Errorf("unrecognized fields for expectedEvents: %v", temp.Extra)
	}

	e.ClientID = temp.ClientID
	if temp.Events.Type != bson.TypeArray {
		return fmt.Errorf("expected 'events' to be an array but got a %q", temp.Events.Type)
	}

	var target interface{}
	switch temp.EventType {
	case "command", "":
		target = &e.CommandEvents
	case "cmap":
		target = &e.CMAPEvents
	case "sdam":
		target = &e.SDAMEvents
	default:
		return fmt.Errorf("unrecognized 'eventType' value for expectedEvents: %q", temp.EventType)
	}

	if err := temp.Events.Unmarshal(target); err != nil {
		return fmt.Errorf("error unmarshalling events array: %w", err)
	}

	if temp.IgnoreExtraEvents != nil {
		e.IgnoreExtraEvents = temp.IgnoreExtraEvents
	}
	return nil
}

func verifyEvents(ctx context.Context, expectedEvents *expectedEvents) error {
	client, err := entities(ctx).client(expectedEvents.ClientID)
	if err != nil {
		return err
	}

	switch {
	case expectedEvents.CommandEvents != nil:
		return verifyCommandEvents(ctx, client, expectedEvents)
	case expectedEvents.CMAPEvents != nil:
		return verifyCMAPEvents(client, expectedEvents)
	case expectedEvents.SDAMEvents != nil:
		return verifySDAMEvents(client, expectedEvents)
	}
	return nil
}

func verifyCommandEvents(ctx context.Context, client *clientEntity, expectedEvents *expectedEvents) error {
	started := client.startedEvents()
	succeeded := client.succeededEvents()
	failed := client.failedEvents()

	// If the Events array is nil, verify that no events were sent.
	if len(expectedEvents.CommandEvents) == 0 && (len(started)+len(succeeded)+len(failed) != 0) {
		return fmt.Errorf("expected no events to be sent but got %s", stringifyEventsForClient(client))
	}

	for idx, evt := range expectedEvents.CommandEvents {
		switch {
		case evt.CommandStartedEvent != nil:
			if len(started) == 0 {
				return newEventVerificationError(idx, client, "no CommandStartedEvent published")
			}

			actual := started[0]
			started = started[1:]

			expected := evt.CommandStartedEvent
			if expected.CommandName != nil && *expected.CommandName != actual.CommandName {
				return newEventVerificationError(idx, client, "expected command name %q, got %q", *expected.CommandName,
					actual.CommandName)
			}
			if expected.DatabaseName != nil && *expected.DatabaseName != actual.DatabaseName {
				return newEventVerificationError(idx, client, "expected database name %q, got %q", *expected.DatabaseName,
					actual.DatabaseName)
			}
			if expected.Command != nil {
				expectedDoc := documentToRawValue(expected.Command)
				actualDoc := documentToRawValue(actual.Command)

				// If actual.Command is empty, as is the case with redacted commands,
				// verifyValuesMatch will return an error from DocumentOK() because
				// there are not enough bytes to read a document from bson.RawValue{}.
				// In the case of an empty Command, hardcode an empty bson.RawValue document.
				if len(actual.Command) == 0 {
					emptyDoc := []byte{5, 0, 0, 0, 0}
					actualDoc = bson.RawValue{Type: bson.TypeEmbeddedDocument, Value: emptyDoc}
				}

				if err := verifyValuesMatch(ctx, expectedDoc, actualDoc, true); err != nil {
					return newEventVerificationError(idx, client, "error comparing command documents: %v", err)
				}
			}
			if expected.HasServiceID != nil {
				if err := verifyServiceID(*expected.HasServiceID, actual.ServiceID); err != nil {
					return newEventVerificationError(idx, client, "error verifying serviceID: %v", err)
				}
			}
			if expected.HasServerConnectionID != nil {
				if err := verifyServerConnectionID(*expected.HasServerConnectionID, actual.ServerConnectionID); err != nil {
					return newEventVerificationError(idx, client, "error verifying serverConnectionID: %v", err)
				}
			}
		case evt.CommandSucceededEvent != nil:
			if len(succeeded) == 0 {
				return newEventVerificationError(idx, client, "no CommandSucceededEvent published")
			}

			actual := succeeded[0]
			succeeded = succeeded[1:]

			expected := evt.CommandSucceededEvent
			if expected.CommandName != nil && *expected.CommandName != actual.CommandName {
				return newEventVerificationError(idx, client, "expected command name %q, got %q", *expected.CommandName,
					actual.CommandName)
			}
			if expected.DatabaseName != nil && *expected.DatabaseName != actual.DatabaseName {
				return newEventVerificationError(idx, client, "expected database name %q, got %q", *expected.DatabaseName,
					actual.DatabaseName)
			}
			if expected.Reply != nil {
				expectedDoc := documentToRawValue(expected.Reply)
				actualDoc := documentToRawValue(actual.Reply)

				// If actual.Reply is empty, as is the case with redacted replies,
				// verifyValuesMatch will return an error from DocumentOK() because
				// there are not enough bytes to read a document from bson.RawValue{}.
				// In the case of an empty Reply, hardcode an empty bson.RawValue document.
				if len(actual.Reply) == 0 {
					emptyDoc := []byte{5, 0, 0, 0, 0}
					actualDoc = bson.RawValue{Type: bson.TypeEmbeddedDocument, Value: emptyDoc}
				}

				if err := verifyValuesMatch(ctx, expectedDoc, actualDoc, true); err != nil {
					return newEventVerificationError(idx, client, "error comparing reply documents: %v", err)
				}
			}
			if expected.HasServiceID != nil {
				if err := verifyServiceID(*expected.HasServiceID, actual.ServiceID); err != nil {
					return newEventVerificationError(idx, client, "error verifying serviceID: %v", err)
				}
			}
			if expected.HasServerConnectionID != nil {
				if err := verifyServerConnectionID(*expected.HasServerConnectionID, actual.ServerConnectionID); err != nil {
					return newEventVerificationError(idx, client, "error verifying serverConnectionID: %v", err)
				}
			}
		case evt.CommandFailedEvent != nil:
			if len(failed) == 0 {
				return newEventVerificationError(idx, client, "no CommandFailedEvent published")
			}

			actual := failed[0]
			failed = failed[1:]

			expected := evt.CommandFailedEvent
			if expected.CommandName != nil && *expected.CommandName != actual.CommandName {
				return newEventVerificationError(idx, client, "expected command name %q, got %q", *expected.CommandName,
					actual.CommandName)
			}
			if expected.DatabaseName != nil && *expected.DatabaseName != actual.DatabaseName {
				return newEventVerificationError(idx, client, "expected database name %q, got %q", *expected.DatabaseName,
					actual.DatabaseName)
			}
			if expected.HasServiceID != nil {
				if err := verifyServiceID(*expected.HasServiceID, actual.ServiceID); err != nil {
					return newEventVerificationError(idx, client, "error verifying serviceID: %v", err)
				}
			}
			if expected.HasServerConnectionID != nil {
				if err := verifyServerConnectionID(*expected.HasServerConnectionID, actual.ServerConnectionID); err != nil {
					return newEventVerificationError(idx, client, "error verifying serverConnectionID: %v", err)
				}
			}
		default:
			return newEventVerificationError(idx, client, "no expected event set on commandMonitoringEvent instance")
		}
	}

	// Verify that there are no remaining events if IgnoreExtraEvents is unset or false.
	ignoreExtraEvents := expectedEvents.IgnoreExtraEvents != nil && *expectedEvents.IgnoreExtraEvents
	if !ignoreExtraEvents && (len(started) > 0 || len(succeeded) > 0 || len(failed) > 0) {
		return fmt.Errorf("extra events published; all events for client: %s", stringifyEventsForClient(client))
	}
	return nil
}

func verifyCMAPEvents(client *clientEntity, expectedEvents *expectedEvents) error {
	pooled := client.poolEvents()
	if len(expectedEvents.CMAPEvents) == 0 && len(pooled) != 0 {
		return fmt.Errorf("expected no cmap events to be sent but got %s", stringifyEventsForClient(client))
	}

	for idx, evt := range expectedEvents.CMAPEvents {
		var err error

		switch {
		case evt.ConnectionCreatedEvent != nil:
			if _, pooled, err = getNextPoolEvent(pooled, event.ConnectionCreated); err != nil {
				return newEventVerificationError(idx, client, "failed to get next pool event: %v", err.Error())
			}
		case evt.ConnectionReadyEvent != nil:
			if _, pooled, err = getNextPoolEvent(pooled, event.ConnectionReady); err != nil {
				return newEventVerificationError(idx, client, "failed to get next pool event: %v", err.Error())
			}
		case evt.ConnectionClosedEvent != nil:
			var actual *event.PoolEvent
			if actual, pooled, err = getNextPoolEvent(pooled, event.ConnectionClosed); err != nil {
				return newEventVerificationError(idx, client, "failed to get next pool event: %v", err.Error())
			}

			if expectedReason := evt.ConnectionClosedEvent.Reason; expectedReason != nil {
				if *expectedReason != actual.Reason {
					return newEventVerificationError(idx, client, "expected reason %q, got %q", *expectedReason, actual.Reason)
				}
			}
		case evt.ConnectionCheckedOutEvent != nil:
			if _, pooled, err = getNextPoolEvent(pooled, event.ConnectionCheckedOut); err != nil {
				return newEventVerificationError(idx, client, "failed to get next pool event: %v", err.Error())
			}
		case evt.ConnectionCheckOutFailedEvent != nil:
			var actual *event.PoolEvent
			if actual, pooled, err = getNextPoolEvent(pooled, event.ConnectionCheckOutFailed); err != nil {
				return newEventVerificationError(idx, client, "failed to get next pool event: %v", err.Error())
			}

			if expectedReason := evt.ConnectionCheckOutFailedEvent.Reason; expectedReason != nil {
				if *expectedReason != actual.Reason {
					return newEventVerificationError(idx, client, "expected reason %q, got %q", *expectedReason, actual.Reason)
				}
			}
		case evt.ConnectionCheckedInEvent != nil:
			if _, pooled, err = getNextPoolEvent(pooled, event.ConnectionCheckedIn); err != nil {
				return newEventVerificationError(idx, client, "failed to get next pool event: %v", err.Error())
			}
		case evt.ConnectionPendingReadStarted != nil:
			if _, pooled, err = getNextPoolEvent(pooled, event.ConnectionPendingReadStarted); err != nil {
				return newEventVerificationError(idx, client, "failed to get next pool event: %v", err.Error())
			}
		case evt.ConnectionPendingreadSucceeded != nil:
			if _, pooled, err = getNextPoolEvent(pooled, event.ConnectionPendingReadSucceeded); err != nil {
				return newEventVerificationError(idx, client, "failed to get next pool event: %v", err.Error())
			}
		case evt.ConnectionPendingReadFailed != nil:
			if _, pooled, err = getNextPoolEvent(pooled, event.ConnectionPendingReadFailed); err != nil {
				return newEventVerificationError(idx, client, "failed to get next pool event: %v", err.Error())
			}
		case evt.PoolClearedEvent != nil:
			var actual *event.PoolEvent
			if actual, pooled, err = getNextPoolEvent(pooled, event.ConnectionPoolCleared); err != nil {
				return newEventVerificationError(idx, client, "failed to get next pool event: %v", err.Error())
			}
			if expectServiceID := evt.PoolClearedEvent.HasServiceID; expectServiceID != nil {
				if err := verifyServiceID(*expectServiceID, actual.ServiceID); err != nil {
					return newEventVerificationError(idx, client, "error verifying serviceID: %v", err)
				}
			}
			if expectInterruption := evt.PoolClearedEvent.InterruptInUseConnections; expectInterruption != nil && *expectInterruption != actual.Interruption {
				return newEventVerificationError(idx, client, "expected interruptInUseConnections %v, got %v",
					expectInterruption, actual.Interruption)
			}
		default:
			return newEventVerificationError(idx, client, "no expected event set on cmapEvent instance")
		}
	}

	// Verify that there are no remaining events if ignoreExtraEvents is unset or false.
	ignoreExtraEvents := expectedEvents.IgnoreExtraEvents != nil && *expectedEvents.IgnoreExtraEvents
	if !ignoreExtraEvents && len(pooled) > 0 {
		return fmt.Errorf("extra events published; all events for client: %s", stringifyEventsForClient(client))
	}
	return nil
}

func getNextPoolEvent(events []*event.PoolEvent, expectedType string) (*event.PoolEvent, []*event.PoolEvent, error) {
	if len(events) == 0 {
		return nil, nil, fmt.Errorf("no %q event published", expectedType)
	}

	evt := events[0]
	if evt.Type != expectedType {
		return nil, nil, fmt.Errorf("expected pool event of type %q, got %q", expectedType, evt.Type)
	}
	return evt, events[1:], nil
}

func verifyServiceID(expectServiceID bool, serviceID *bson.ObjectID) error {
	if eventHasID := serviceID != nil; expectServiceID != eventHasID {
		return fmt.Errorf("expected event to have server ID: %v, event has server ID %v", expectServiceID, serviceID)
	}
	return nil
}

func verifyServerConnectionID(expectedHasSCID bool, scid *int64) error {
	if actualHasSCID := scid != nil; expectedHasSCID != actualHasSCID {
		if expectedHasSCID {
			return fmt.Errorf("expected event to have server connection ID, event has none")
		}
		return fmt.Errorf("expected event to have no server connection ID, got %d", *scid)
	}
	if expectedHasSCID && *scid <= 0 {
		return fmt.Errorf("expected event to have a positive server connection ID, got %d", *scid)
	}
	return nil
}

func newEventVerificationError(idx int, client *clientEntity, msg string, args ...interface{}) error {
	fullMsg := fmt.Sprintf(msg, args...)
	return fmt.Errorf("event comparison failed at index %d: %s; all events found for client: %s", idx, fullMsg,
		stringifyEventsForClient(client))
}

func stringifyEventsForClient(client *clientEntity) string {
	str := bytes.NewBuffer(nil)

	str.WriteString("\n\nStarted Events\n\n")
	for _, evt := range client.startedEvents() {
		str.WriteString(fmt.Sprintf("[%s] %s\n", evt.ConnectionID, evt.Command))
	}

	str.WriteString("\nSucceeded Events\n\n")
	for _, evt := range client.succeededEvents() {
		str.WriteString(fmt.Sprintf("[%s] CommandName: %s, Reply: %s\n", evt.ConnectionID, evt.CommandName, evt.Reply))
	}

	str.WriteString("\nFailed Events\n\n")
	for _, evt := range client.failedEvents() {
		str.WriteString(fmt.Sprintf("[%s] CommandName: %s, Failure: %s\n", evt.ConnectionID, evt.CommandName, evt.Failure))
	}

	str.WriteString("\nPool Events\n\n")
	for _, evt := range client.poolEvents() {
		str.WriteString(fmt.Sprintf("[%s] Event Type: %q\n", evt.Address, evt.Type))
	}

	return str.String()
}

func getNextServerDescriptionChangedEvent(
	events []*event.ServerDescriptionChangedEvent,
) (*event.ServerDescriptionChangedEvent, []*event.ServerDescriptionChangedEvent, error) {
	if len(events) == 0 {
		return nil, nil, errors.New("no server changed event published")
	}

	return events[0], events[1:], nil
}

func getNextServerHeartbeatStartedEvent(
	events []*event.ServerHeartbeatStartedEvent,
) (*event.ServerHeartbeatStartedEvent, []*event.ServerHeartbeatStartedEvent, error) {
	if len(events) == 0 {
		return nil, nil, errors.New("no heartbeat started event published")
	}

	return events[0], events[1:], nil
}

func getNextServerHeartbeatSucceededEvent(
	events []*event.ServerHeartbeatSucceededEvent,
) (*event.ServerHeartbeatSucceededEvent, []*event.ServerHeartbeatSucceededEvent, error) {
	if len(events) == 0 {
		return nil, nil, errors.New("no heartbeat succeeded event published")
	}

	return events[0], events[:1], nil
}

func getNextServerHeartbeatFailedEvent(
	events []*event.ServerHeartbeatFailedEvent,
) (*event.ServerHeartbeatFailedEvent, []*event.ServerHeartbeatFailedEvent, error) {
	if len(events) == 0 {
		return nil, nil, errors.New("no heartbeat failed event published")
	}

	return events[0], events[:1], nil
}

func getNextTopologyDescriptionChangedEvent(
	events []*event.TopologyDescriptionChangedEvent,
) (*event.TopologyDescriptionChangedEvent, []*event.TopologyDescriptionChangedEvent, error) {
	if len(events) == 0 {
		return nil, nil, errors.New("no topology description changed event published")
	}

	return events[0], events[:1], nil
}

func verifySDAMEvents(client *clientEntity, expectedEvents *expectedEvents) error {
	var (
		changed   = client.serverDescriptionChanged
		started   = client.serverHeartbeatStartedEvent
		succeeded = client.serverHeartbeatSucceeded
		failed    = client.serverHeartbeatFailedEvent
		tchanged  = client.topologyDescriptionChanged
	)

	vol := func() int { return len(changed) + len(started) + len(succeeded) + len(failed) + len(tchanged) }

	if len(expectedEvents.SDAMEvents) == 0 && vol() != 0 {
		return fmt.Errorf("expected no sdam events to be sent but got %s", stringifyEventsForClient(client))
	}

	for idx, evt := range expectedEvents.SDAMEvents {
		var err error

		switch {
		case evt.ServerDescriptionChangedEvent != nil:
			var got *event.ServerDescriptionChangedEvent
			if got, changed, err = getNextServerDescriptionChangedEvent(changed); err != nil {
				return newEventVerificationError(idx, client, "failed to get next server description changed event: %v", err.Error())
			}

			prevDesc := evt.ServerDescriptionChangedEvent.NewDescription

			var wantPrevDesc string
			if prevDesc != nil && prevDesc.Type != nil {
				wantPrevDesc = *prevDesc.Type
			}

			gotPrevDesc := got.PreviousDescription.Kind
			if gotPrevDesc != wantPrevDesc {
				return newEventVerificationError(idx, client,
					"expected previous server description %q, got %q", wantPrevDesc, gotPrevDesc)
			}

			newDesc := evt.ServerDescriptionChangedEvent.PreviousDescription

			var wantNewDesc string
			if newDesc != nil && newDesc.Type != nil {
				wantNewDesc = *newDesc.Type
			}

			gotNewDesc := got.NewDescription.Kind
			if gotNewDesc != wantNewDesc {
				return newEventVerificationError(idx, client,
					"expected new server description %q, got %q", wantNewDesc, gotNewDesc)
			}
		case evt.ServerHeartbeatStartedEvent != nil:
			var got *event.ServerHeartbeatStartedEvent
			if got, started, err = getNextServerHeartbeatStartedEvent(started); err != nil {
				return newEventVerificationError(idx, client, "failed to get next server heartbeat started event: %v", err.Error())
			}

			if want := evt.ServerHeartbeatStartedEvent.Awaited; want != nil && *want != got.Awaited {
				return newEventVerificationError(idx, client, "want awaited %v, got %v", *want, got.Awaited)
			}
		case evt.ServerHeartbeatSucceededEvent != nil:
			var got *event.ServerHeartbeatSucceededEvent
			if got, succeeded, err = getNextServerHeartbeatSucceededEvent(succeeded); err != nil {
				return newEventVerificationError(idx, client, "failed to get next server heartbeat succeeded event: %v", err.Error())
			}

			if want := evt.ServerHeartbeatSucceededEvent.Awaited; want != nil && *want != got.Awaited {
				return newEventVerificationError(idx, client, "want awaited %v, got %v", *want, got.Awaited)
			}
		case evt.ServerHeartbeatFailedEvent != nil:
			var got *event.ServerHeartbeatFailedEvent
			if got, failed, err = getNextServerHeartbeatFailedEvent(failed); err != nil {
				return newEventVerificationError(idx, client, "failed to get next server heartbeat failed event: %v", err.Error())
			}

			if want := evt.ServerHeartbeatFailedEvent.Awaited; want != nil && *want != got.Awaited {
				return newEventVerificationError(idx, client, "want awaited %v, got %v", *want, got.Awaited)
			}
		case evt.TopologyDescriptionChangedEvent != nil:
			if _, tchanged, err = getNextTopologyDescriptionChangedEvent(tchanged); err != nil {
				return newEventVerificationError(idx, client, "failed to get next description changed event: %v", err.Error())
			}
		}
	}

	// Verify that there are no remaining events if ignoreExtraEvents is unset or false.
	ignoreExtraEvents := expectedEvents.IgnoreExtraEvents != nil && *expectedEvents.IgnoreExtraEvents
	if !ignoreExtraEvents && vol() > 0 {
		return fmt.Errorf("extra sdam events published; all events for client: %s", stringifyEventsForClient(client))
	}
	return nil
}
