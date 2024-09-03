// Copyright (C) MongoDB, Inc. 2017-present.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

package readpref

import (
	"bytes"
	"fmt"
)

// Tag is a name/value pair.
type Tag struct {
	Name  string
	Value string
}

// String returns a human-readable human-readable description of the tag.
func (tag Tag) String() string {
	return fmt.Sprintf("%s=%s", tag.Name, tag.Value)
}

// TagSet is an ordered list of Tags.
type TagSet []Tag

// NewTagSet is a convenience function to specify a single tag set used to match
// replica set members. If no members match the tag set, read operations will
// return an error.
//
// For more information about read preference tags, see
// https://www.mongodb.com/docs/manual/core/read-preference-tags/
func NewTagSet(tags ...string) (TagSet, error) {
	length := len(tags)
	if length < 2 || length%2 != 0 {
		return nil, fmt.Errorf("an even number of tags must be specified")
	}

	tagset := make(TagSet, 0, length/2)

	for i := 1; i < length; i += 2 {
		tagset = append(tagset, Tag{Name: tags[i-1], Value: tags[i]})
	}

	return tagset, nil
}

// NewTagSetFromMap creates a tag set from a map.
//
// For more information about read preference tags, see
// https://www.mongodb.com/docs/manual/core/read-preference-tags/
func NewTagSetFromMap(m map[string]string) TagSet {
	set := make(TagSet, 0, len(m))
	for k, v := range m {
		set = append(set, Tag{Name: k, Value: v})
	}

	return set
}

// NewTagSetsFromMaps creates a list of tag sets from a slice of maps.
//
// For more information about read preference tags, see
// https://www.mongodb.com/docs/manual/core/read-preference-tags/
func NewTagSetsFromMaps(maps []map[string]string) []TagSet {
	sets := make([]TagSet, 0, len(maps))
	for _, m := range maps {
		sets = append(sets, NewTagSetFromMap(m))
	}
	return sets
}

// Contains indicates whether the name/value pair exists in the tagset.
func (ts TagSet) Contains(name, value string) bool {
	for _, t := range ts {
		if t.Name == name && t.Value == value {
			return true
		}
	}

	return false
}

// ContainsAll indicates whether all the name/value pairs exist in the tagset.
func (ts TagSet) ContainsAll(other []Tag) bool {
	for _, ot := range other {
		if !ts.Contains(ot.Name, ot.Value) {
			return false
		}
	}

	return true
}

// String returns a human-readable human-readable description of the tagset.
func (ts TagSet) String() string {
	var b bytes.Buffer
	for i, tag := range ts {
		if i > 0 {
			b.WriteString(",")
		}
		b.WriteString(tag.String())
	}
	return b.String()
}
