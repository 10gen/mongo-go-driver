// Copyright (C) MongoDB, Inc. 2024-present.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

package description

import (
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo/address"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"
)

// ServerKind represents the type of a single server in a topology.
type ServerKind uint32

// These constants are the possible types of servers.
const (
	ServerKindStandalone   ServerKind = 1
	ServerKindRSMember     ServerKind = 2
	ServerKindRSPrimary    ServerKind = 4 + ServerKindRSMember
	ServerKindRSSecondary  ServerKind = 8 + ServerKindRSMember
	ServerKindRSArbiter    ServerKind = 16 + ServerKindRSMember
	ServerKindRSGhost      ServerKind = 32 + ServerKindRSMember
	ServerKindMongos       ServerKind = 256
	ServerKindLoadBalancer ServerKind = 512
)

// UnknownStr represents an unknown server kind.
const UnknownStr = "Unknown"

// String returns a stringified version of the kind or "Unknown" if the kind is
// invalid.
func (kind ServerKind) String() string {
	switch kind {
	case ServerKindStandalone:
		return "Standalone"
	case ServerKindRSMember:
		return "RSOther"
	case ServerKindRSPrimary:
		return "RSPrimary"
	case ServerKindRSSecondary:
		return "RSSecondary"
	case ServerKindRSArbiter:
		return "RSArbiter"
	case ServerKindRSGhost:
		return "RSGhost"
	case ServerKindMongos:
		return "Mongos"
	case ServerKindLoadBalancer:
		return "LoadBalancer"
	}

	return UnknownStr
}

// Unknown is an unknown server or topology kind.
const Unknown = 0

// TopologyVersion represents a software version.
type TopologyVersion struct {
	ProcessID bson.ObjectID
	Counter   int64
}

// VersionRange represents a range of versions.
type VersionRange struct {
	Min int32
	Max int32
}

// Server contains information about a node in a cluster. This is created from
// hello command responses. If the value of the Kind field is LoadBalancer, only
// the Addr and Kind fields will be set. All other fields will be set to the
// zero value of the field's type.
type Server struct {
	Addr address.Address

	Arbiters              []string
	AverageRTT            time.Duration
	AverageRTTSet         bool
	Compression           []string // compression methods returned by server
	CanonicalAddr         address.Address
	ElectionID            bson.ObjectID
	HeartbeatInterval     time.Duration
	HelloOK               bool
	Hosts                 []string
	IsCryptd              bool
	LastError             error
	LastUpdateTime        time.Time
	LastWriteTime         time.Time
	MaxBatchCount         uint32
	MaxDocumentSize       uint32
	MaxMessageSize        uint32
	Members               []address.Address
	Passives              []string
	Passive               bool
	Primary               address.Address
	ReadOnly              bool
	ServiceID             *bson.ObjectID // Only set for servers that are deployed behind a load balancer.
	SessionTimeoutMinutes *int64
	SetName               string
	SetVersion            uint32
	Tags                  readpref.TagSet
	TopologyVersion       *TopologyVersion
	Kind                  ServerKind
	WireVersion           *VersionRange
}

func (s Server) String() string {
	str := fmt.Sprintf("Addr: %s, Type: %s", s.Addr, s.Kind)
	if len(s.Tags) != 0 {
		str += fmt.Sprintf(", Tag sets: %s", s.Tags)
	}

	if s.AverageRTTSet {
		str += fmt.Sprintf(", Average RTT: %d", s.AverageRTT)
	}

	if s.LastError != nil {
		str += fmt.Sprintf(", Last error: %s", s.LastError)
	}
	return str
}

// SelectedServer augments the Server type by also including the TopologyKind of
// the topology that includes the server. This type should be used to track the
// state of a server that was selected to perform an operation.
type SelectedServer struct {
	Server
	Kind TopologyKind
}

// ServerSelector is an interface implemented by types that can perform server
// selection given a topology description and list of candidate servers. The
// selector should filter the provided candidates list and return a subset that
// matches some criteria.
type ServerSelector interface {
	SelectServer(Topology, []Server) ([]Server, error)
}
