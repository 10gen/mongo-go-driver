// Copyright (C) MongoDB, Inc. 2024-present.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

package driverutil

import (
	"errors"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/internal/bsonutil"
	"go.mongodb.org/mongo-driver/v2/internal/handshake"
	"go.mongodb.org/mongo-driver/v2/internal/ptrutil"
	"go.mongodb.org/mongo-driver/v2/mongo/address"
	"go.mongodb.org/mongo-driver/v2/tag"
	"go.mongodb.org/mongo-driver/v2/x/mongo/driver/description"
)

const (
	MinWireVersion = 7
	MaxWireVersion = 25
)

func equalWireVersion(wv1, wv2 *description.VersionRange) bool {
	if wv1 == nil && wv2 == nil {
		return true
	}

	if wv1 == nil || wv2 == nil {
		return false
	}

	return wv1.Min == wv2.Min && wv1.Max == wv2.Max
}

// EqualServers compares two server descriptions and returns true if they are
// equal.
func EqualServers(srv1, srv2 description.Server) bool {
	if srv1.CanonicalAddr.String() != srv2.CanonicalAddr.String() {
		return false
	}

	if !sliceStringEqual(srv1.Arbiters, srv2.Arbiters) {
		return false
	}

	if !sliceStringEqual(srv1.Hosts, srv2.Hosts) {
		return false
	}

	if !sliceStringEqual(srv1.Passives, srv2.Passives) {
		return false
	}

	if srv1.Primary != srv2.Primary {
		return false
	}

	if srv1.SetName != srv2.SetName {
		return false
	}

	if srv1.Kind != srv2.Kind {
		return false
	}

	if srv1.LastError != nil || srv2.LastError != nil {
		if srv1.LastError == nil || srv2.LastError == nil {
			return false
		}
		if srv1.LastError.Error() != srv2.LastError.Error() {
			return false
		}
	}

	if !equalWireVersion(srv1.WireVersion, srv2.WireVersion) {
		return false
	}

	if len(srv1.Tags) != len(srv2.Tags) || !srv1.Tags.ContainsAll(srv2.Tags) {
		return false
	}

	if srv1.SetVersion != srv2.SetVersion {
		return false
	}

	if srv1.ElectionID != srv2.ElectionID {
		return false
	}

	if ptrutil.CompareInt64(srv1.SessionTimeoutMinutes, srv2.SessionTimeoutMinutes) != 0 {
		return false
	}

	// If TopologyVersion is nil for both servers, CompareToIncoming will return -1 because it assumes that the
	// incoming response is newer. We want the descriptions to be considered equal in this case, though, so an
	// explicit check is required.
	if srv1.TopologyVersion == nil && srv2.TopologyVersion == nil {
		return true
	}

	return CompareTopologyVersions(srv1.TopologyVersion, srv2.TopologyVersion) == 0
}

// IsServerLoadBalanced checks if a description.Server describes a server that
// is load balanced.
func IsServerLoadBalanced(srv description.Server) bool {
	return srv.Kind == description.ServerKindLoadBalancer || srv.ServiceID != nil
}

// stringSliceFromRawElement decodes the provided BSON element into a []string.
// This internally calls StringSliceFromRawValue on the element's value. The
// error conditions outlined in that function's documentation apply for this
// function as well.
func stringSliceFromRawElement(element bson.RawElement) ([]string, error) {
	return bsonutil.StringSliceFromRawValue(element.Key(), element.Value())
}

func decodeStringMap(element bson.RawElement, name string) (map[string]string, error) {
	doc, ok := element.Value().DocumentOK()
	if !ok {
		return nil, fmt.Errorf("expected '%s' to be a document but it's a BSON %s", name, element.Value().Type)
	}
	elements, err := doc.Elements()
	if err != nil {
		return nil, err
	}
	m := make(map[string]string)
	for _, element := range elements {
		key := element.Key()
		value, ok := element.Value().StringValueOK()
		if !ok {
			return nil, fmt.Errorf("expected '%s' to be a document of strings, but found a BSON %s", name, element.Value().Type)
		}
		m[key] = value
	}
	return m, nil
}

// NewTopologyVersion creates a TopologyVersion based on doc
func NewTopologyVersion(doc bson.Raw) (*description.TopologyVersion, error) {
	elements, err := doc.Elements()
	if err != nil {
		return nil, err
	}
	var tv description.TopologyVersion
	var ok bool
	for _, element := range elements {
		switch element.Key() {
		case "processId":
			tv.ProcessID, ok = element.Value().ObjectIDOK()
			if !ok {
				return nil, fmt.Errorf("expected 'processId' to be a objectID but it's a BSON %s", element.Value().Type)
			}
		case "counter":
			tv.Counter, ok = element.Value().Int64OK()
			if !ok {
				return nil, fmt.Errorf("expected 'counter' to be an int64 but it's a BSON %s", element.Value().Type)
			}
		}
	}
	return &tv, nil
}

// NewVersionRange creates a new VersionRange given a min and a max.
func NewVersionRange(min, max int32) description.VersionRange {
	return description.VersionRange{Min: min, Max: max}
}

// VersionRangeIncludes returns a bool indicating whether the supplied integer
// is included in the range.
func VersionRangeIncludes(versionRange description.VersionRange, v int32) bool {
	return v >= versionRange.Min && v <= versionRange.Max
}

// CompareTopologyVersions compares the receiver, which represents the currently
// known TopologyVersion for a server, to an incoming TopologyVersion extracted
// from a server command response.
//
// This returns -1 if the receiver version is less than the response, 0 if the
// versions are equal, and 1 if the receiver version is greater than the
// response. This comparison is not commutative.
func CompareTopologyVersions(receiver, response *description.TopologyVersion) int {
	if receiver == nil || response == nil {
		return -1
	}
	if receiver.ProcessID != response.ProcessID {
		return -1
	}
	if receiver.Counter == response.Counter {
		return 0
	}
	if receiver.Counter < response.Counter {
		return -1
	}
	return 1
}

// NewServerDescription creates a new server description from the given hello
// command response.
func NewServerDescription(addr address.Address, response bson.Raw) description.Server {
	desc := description.Server{Addr: addr, CanonicalAddr: addr, LastUpdateTime: time.Now().UTC()}
	elements, err := response.Elements()
	if err != nil {
		desc.LastError = err
		return desc
	}
	var ok bool
	var isReplicaSet, isWritablePrimary, hidden, secondary, arbiterOnly bool
	var msg string
	var versionRange description.VersionRange
	for _, element := range elements {
		switch element.Key() {
		case "arbiters":
			var err error
			desc.Arbiters, err = stringSliceFromRawElement(element)
			if err != nil {
				desc.LastError = err
				return desc
			}
		case "arbiterOnly":
			arbiterOnly, ok = element.Value().BooleanOK()
			if !ok {
				desc.LastError = fmt.Errorf("expected 'arbiterOnly' to be a boolean but it's a BSON %s", element.Value().Type)
				return desc
			}
		case "compression":
			var err error
			desc.Compression, err = stringSliceFromRawElement(element)
			if err != nil {
				desc.LastError = err
				return desc
			}
		case "electionId":
			desc.ElectionID, ok = element.Value().ObjectIDOK()
			if !ok {
				desc.LastError = fmt.Errorf("expected 'electionId' to be a objectID but it's a BSON %s", element.Value().Type)
				return desc
			}
		case "iscryptd":
			desc.IsCryptd, ok = element.Value().BooleanOK()
			if !ok {
				desc.LastError = fmt.Errorf("expected 'iscryptd' to be a boolean but it's a BSON %s", element.Value().Type)
				return desc
			}
		case "helloOk":
			desc.HelloOK, ok = element.Value().BooleanOK()
			if !ok {
				desc.LastError = fmt.Errorf("expected 'helloOk' to be a boolean but it's a BSON %s", element.Value().Type)
				return desc
			}
		case "hidden":
			hidden, ok = element.Value().BooleanOK()
			if !ok {
				desc.LastError = fmt.Errorf("expected 'hidden' to be a boolean but it's a BSON %s", element.Value().Type)
				return desc
			}
		case "hosts":
			var err error
			desc.Hosts, err = stringSliceFromRawElement(element)
			if err != nil {
				desc.LastError = err
				return desc
			}
		case "isWritablePrimary":
			isWritablePrimary, ok = element.Value().BooleanOK()
			if !ok {
				desc.LastError = fmt.Errorf("expected 'isWritablePrimary' to be a boolean but it's a BSON %s", element.Value().Type)
				return desc
			}
		case handshake.LegacyHelloLowercase:
			isWritablePrimary, ok = element.Value().BooleanOK()
			if !ok {
				desc.LastError = fmt.Errorf("expected legacy hello to be a boolean but it's a BSON %s", element.Value().Type)
				return desc
			}
		case "isreplicaset":
			isReplicaSet, ok = element.Value().BooleanOK()
			if !ok {
				desc.LastError = fmt.Errorf("expected 'isreplicaset' to be a boolean but it's a BSON %s", element.Value().Type)
				return desc
			}
		case "lastWrite":
			lastWrite, ok := element.Value().DocumentOK()
			if !ok {
				desc.LastError = fmt.Errorf("expected 'lastWrite' to be a document but it's a BSON %s", element.Value().Type)
				return desc
			}
			dateTime, err := lastWrite.LookupErr("lastWriteDate")
			if err == nil {
				dt, ok := dateTime.DateTimeOK()
				if !ok {
					desc.LastError = fmt.Errorf("expected 'lastWriteDate' to be a datetime but it's a BSON %s", dateTime.Type)
					return desc
				}
				desc.LastWriteTime = time.Unix(dt/1000, dt%1000*1000000).UTC()
			}
		case "logicalSessionTimeoutMinutes":
			i64, ok := element.Value().AsInt64OK()
			if !ok {
				desc.LastError = fmt.Errorf("expected 'logicalSessionTimeoutMinutes' to be an integer but it's a BSON %s", element.Value().Type)
				return desc
			}

			desc.SessionTimeoutMinutes = &i64
		case "maxBsonObjectSize":
			i64, ok := element.Value().AsInt64OK()
			if !ok {
				desc.LastError = fmt.Errorf("expected 'maxBsonObjectSize' to be an integer but it's a BSON %s", element.Value().Type)
				return desc
			}
			desc.MaxDocumentSize = uint32(i64)
		case "maxMessageSizeBytes":
			i64, ok := element.Value().AsInt64OK()
			if !ok {
				desc.LastError = fmt.Errorf("expected 'maxMessageSizeBytes' to be an integer but it's a BSON %s", element.Value().Type)
				return desc
			}
			desc.MaxMessageSize = uint32(i64)
		case "maxWriteBatchSize":
			i64, ok := element.Value().AsInt64OK()
			if !ok {
				desc.LastError = fmt.Errorf("expected 'maxWriteBatchSize' to be an integer but it's a BSON %s", element.Value().Type)
				return desc
			}
			desc.MaxBatchCount = uint32(i64)
		case "me":
			me, ok := element.Value().StringValueOK()
			if !ok {
				desc.LastError = fmt.Errorf("expected 'me' to be a string but it's a BSON %s", element.Value().Type)
				return desc
			}
			desc.CanonicalAddr = address.Address(me).Canonicalize()
		case "maxWireVersion":
			verMax, ok := element.Value().AsInt64OK()
			versionRange.Max = int32(verMax)
			if !ok {
				desc.LastError = fmt.Errorf("expected 'maxWireVersion' to be an integer but it's a BSON %s", element.Value().Type)
				return desc
			}
		case "minWireVersion":
			verMin, ok := element.Value().AsInt64OK()
			versionRange.Min = int32(verMin)
			if !ok {
				desc.LastError = fmt.Errorf("expected 'minWireVersion' to be an integer but it's a BSON %s", element.Value().Type)
				return desc
			}
		case "msg":
			msg, ok = element.Value().StringValueOK()
			if !ok {
				desc.LastError = fmt.Errorf("expected 'msg' to be a string but it's a BSON %s", element.Value().Type)
				return desc
			}
		case "ok":
			okay, ok := element.Value().AsInt64OK()
			if !ok {
				desc.LastError = fmt.Errorf("expected 'ok' to be a boolean but it's a BSON %s", element.Value().Type)
				return desc
			}
			if okay != 1 {
				desc.LastError = errors.New("not ok")
				return desc
			}
		case "passives":
			var err error
			desc.Passives, err = stringSliceFromRawElement(element)
			if err != nil {
				desc.LastError = err
				return desc
			}
		case "passive":
			desc.Passive, ok = element.Value().BooleanOK()
			if !ok {
				desc.LastError = fmt.Errorf("expected 'passive' to be a boolean but it's a BSON %s", element.Value().Type)
				return desc
			}
		case "primary":
			primary, ok := element.Value().StringValueOK()
			if !ok {
				desc.LastError = fmt.Errorf("expected 'primary' to be a string but it's a BSON %s", element.Value().Type)
				return desc
			}
			desc.Primary = address.Address(primary)
		case "readOnly":
			desc.ReadOnly, ok = element.Value().BooleanOK()
			if !ok {
				desc.LastError = fmt.Errorf("expected 'readOnly' to be a boolean but it's a BSON %s", element.Value().Type)
				return desc
			}
		case "secondary":
			secondary, ok = element.Value().BooleanOK()
			if !ok {
				desc.LastError = fmt.Errorf("expected 'secondary' to be a boolean but it's a BSON %s", element.Value().Type)
				return desc
			}
		case "serviceId":
			oid, ok := element.Value().ObjectIDOK()
			if !ok {
				desc.LastError = fmt.Errorf("expected 'serviceId' to be an ObjectId but it's a BSON %s", element.Value().Type)
			}
			desc.ServiceID = &oid
		case "setName":
			desc.SetName, ok = element.Value().StringValueOK()
			if !ok {
				desc.LastError = fmt.Errorf("expected 'setName' to be a string but it's a BSON %s", element.Value().Type)
				return desc
			}
		case "setVersion":
			i64, ok := element.Value().AsInt64OK()
			if !ok {
				desc.LastError = fmt.Errorf("expected 'setVersion' to be an integer but it's a BSON %s", element.Value().Type)
				return desc
			}
			desc.SetVersion = uint32(i64)
		case "tags":
			m, err := decodeStringMap(element, "tags")
			if err != nil {
				desc.LastError = err
				return desc
			}
			desc.Tags = tag.NewTagSetFromMap(m)
		case "topologyVersion":
			doc, ok := element.Value().DocumentOK()
			if !ok {
				desc.LastError = fmt.Errorf("expected 'topologyVersion' to be a document but it's a BSON %s", element.Value().Type)
				return desc
			}

			desc.TopologyVersion, err = NewTopologyVersion(doc)
			if err != nil {
				desc.LastError = err
				return desc
			}
		}
	}

	for _, host := range desc.Hosts {
		desc.Members = append(desc.Members, address.Address(host).Canonicalize())
	}

	for _, passive := range desc.Passives {
		desc.Members = append(desc.Members, address.Address(passive).Canonicalize())
	}

	for _, arbiter := range desc.Arbiters {
		desc.Members = append(desc.Members, address.Address(arbiter).Canonicalize())
	}

	desc.Kind = description.ServerKindStandalone

	switch {
	case isReplicaSet:
		desc.Kind = description.ServerKindRSGhost
	case desc.SetName != "":
		switch {
		case isWritablePrimary:
			desc.Kind = description.ServerKindRSPrimary
		case hidden:
			desc.Kind = description.ServerKindRSMember
		case secondary:
			desc.Kind = description.ServerKindRSSecondary
		case arbiterOnly:
			desc.Kind = description.ServerKindRSArbiter
		default:
			desc.Kind = description.ServerKindRSMember
		}
	case msg == "isdbgrid":
		desc.Kind = description.ServerKindMongos
	}

	desc.WireVersion = &versionRange

	return desc
}

func sliceStringEqual(a []string, b []string) bool {
	if len(a) != len(b) {
		return false
	}

	for i, v := range a {
		if v != b[i] {
			return false
		}
	}

	return true
}
