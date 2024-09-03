// Copyright (C) MongoDB, Inc. 2024-present.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

package event

import (
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo/address"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"
)

// ServerDescription contains information about a node in a cluster. This is
// created from hello command responses. If the value of the Kind field is
// LoadBalancer, only the Addr and Kind fields will be set. All other fields
// will be set to the zero value of the field's type.
type ServerDescription struct {
	Addr                     address.Address
	Arbiters                 []string
	Compression              []string // compression methods returned by server
	CanonicalAddr            address.Address
	ElectionID               bson.ObjectID
	IsCryptd                 bool
	HelloOK                  bool
	Hosts                    []string
	Kind                     string
	LastWriteTime            time.Time
	MaxBatchCount            uint32
	MaxDocumentSize          uint32
	MaxMessageSize           uint32
	MaxWireVersion           int32
	MinWireVersion           int32
	Members                  []address.Address
	Passives                 []string
	Passive                  bool
	Primary                  address.Address
	ReadOnly                 bool
	ServiceID                *bson.ObjectID // Only set for servers that are deployed behind a load balancer.
	SessionTimeoutMinutes    *int64
	SetName                  string
	SetVersion               uint32
	Tags                     readpref.TagSet
	TopologyVersionProcessID bson.ObjectID
	TopologyVersionCounter   int64
}

// TopologyDescription contains information about a MongoDB cluster.
type TopologyDescription struct {
	Servers               []ServerDescription
	SetName               string
	Kind                  string
	SessionTimeoutMinutes *int64
	CompatibilityErr      error
}
