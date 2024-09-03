// Copyright (C) MongoDB, Inc. 2017-present.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

package unified

import (
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo/readconcern"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"
	"go.mongodb.org/mongo-driver/v2/mongo/writeconcern"
)

// This file defines helper types to convert BSON documents to ReadConcern, WriteConcern, and ReadPref objects.

type readConcern struct {
	Level string `bson:"level"`
}

func (rc *readConcern) toReadConcernOption() *readconcern.ReadConcern {
	return &readconcern.ReadConcern{Level: rc.Level}
}

type writeConcern struct {
	Journal *bool       `bson:"journal"`
	W       interface{} `bson:"w"`
}

func (wc *writeConcern) toWriteConcernOption() (*writeconcern.WriteConcern, error) {
	c := &writeconcern.WriteConcern{}
	if wc.Journal != nil {
		c.Journal = wc.Journal
	}
	if wc.W != nil {
		switch converted := wc.W.(type) {
		case string:
			if converted != "majority" {
				return nil, fmt.Errorf("invalid write concern 'w' string value %q", converted)
			}
			c.W = "majority"
		case int32:
			c.W = int(converted)
		default:
			return nil, fmt.Errorf("invalid type for write concern 'w' field %T", wc.W)
		}
	}

	return c, nil
}

// ReadPreference is a representation of BSON readPreference objects in tests.
type ReadPreference struct {
	Mode                string              `bson:"mode"`
	TagSets             []map[string]string `bson:"tagSets"`
	MaxStalenessSeconds *int64              `bson:"maxStalenessSeconds"`
	Hedge               bson.M              `bson:"hedge"`
}

// ToReadPrefOption converts a ReadPreference into a readpref.ReadPref object and will
// error if the original ReadPreference is malformed.
func (rp *ReadPreference) ToReadPrefOption() (*readpref.ReadPref, error) {
	mode, err := readpref.ModeFromString(rp.Mode)
	if err != nil {
		return nil, fmt.Errorf("invalid read preference mode %q", rp.Mode)
	}

	rpOpts := readpref.Options()

	if rp.TagSets != nil {
		// Each item in the TagSets slice is a document that represents one set.
		sets := make([]readpref.TagSet, 0, len(rp.TagSets))
		for _, rawSet := range rp.TagSets {
			parsed := make(readpref.TagSet, 0, len(rawSet))
			for k, v := range rawSet {
				parsed = append(parsed, readpref.Tag{Name: k, Value: v})
			}
			sets = append(sets, parsed)
		}

		rpOpts.SetTagSets(sets)
	}
	if rp.MaxStalenessSeconds != nil {
		maxStaleness := time.Duration(*rp.MaxStalenessSeconds) * time.Second
		rpOpts.SetMaxStaleness(maxStaleness)
	}
	if rp.Hedge != nil {
		if len(rp.Hedge) > 1 {
			return nil, fmt.Errorf("invalid read preference hedge document: length cannot be greater than 1")
		}
		if enabled, ok := rp.Hedge["enabled"]; ok {
			hedgeEnabled := enabled.(bool)
			rpOpts.SetHedgeEnabled(hedgeEnabled)
		}
	}

	return readpref.New(mode, rpOpts)
}
