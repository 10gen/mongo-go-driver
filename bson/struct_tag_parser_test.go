// Copyright (C) MongoDB, Inc. 2017-present.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

package bson

import (
	"reflect"
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestStructTagParsers(t *testing.T) {
	testCases := []struct {
		name   string
		sf     reflect.StructField
		want   *structTags
		parser func(reflect.StructField) (*structTags, error)
	}{
		{
			"default no bson tag",
			reflect.StructField{Name: "foo", Tag: reflect.StructTag("bar")},
			&structTags{Name: "bar"},
			parseStructTags,
		},
		{
			"default empty",
			reflect.StructField{Name: "foo", Tag: reflect.StructTag("")},
			&structTags{Name: "foo"},
			parseStructTags,
		},
		{
			"default tag only dash",
			reflect.StructField{Name: "foo", Tag: reflect.StructTag("-")},
			&structTags{Skip: true},
			parseStructTags,
		},
		{
			"default bson tag only dash",
			reflect.StructField{Name: "foo", Tag: reflect.StructTag(`bson:"-"`)},
			&structTags{Skip: true},
			parseStructTags,
		},
		{
			"default all options",
			reflect.StructField{Name: "foo", Tag: reflect.StructTag(`bar,omitempty,omitzero,minsize,truncate,inline`)},
			&structTags{Name: "bar", OmitEmpty: true, OmitZero: true, MinSize: true, Truncate: true, Inline: true},
			parseStructTags,
		},
		{
			"default all options default name",
			reflect.StructField{Name: "foo", Tag: reflect.StructTag(`,omitempty,omitzero,minsize,truncate,inline`)},
			&structTags{Name: "foo", OmitEmpty: true, OmitZero: true, MinSize: true, Truncate: true, Inline: true},
			parseStructTags,
		},
		{
			"default bson tag all options",
			reflect.StructField{Name: "foo", Tag: reflect.StructTag(`bson:"bar,omitempty,omitzero,minsize,truncate,inline"`)},
			&structTags{Name: "bar", OmitEmpty: true, OmitZero: true, MinSize: true, Truncate: true, Inline: true},
			parseStructTags,
		},
		{
			"default bson tag all options default name",
			reflect.StructField{Name: "foo", Tag: reflect.StructTag(`bson:",omitempty,omitzero,minsize,truncate,inline"`)},
			&structTags{Name: "foo", OmitEmpty: true, OmitZero: true, MinSize: true, Truncate: true, Inline: true},
			parseStructTags,
		},
		{
			"default ignore xml",
			reflect.StructField{Name: "foo", Tag: reflect.StructTag(`xml:"bar"`)},
			&structTags{Name: "foo"},
			parseStructTags,
		},
		{
			"JSONFallback no bson tag",
			reflect.StructField{Name: "foo", Tag: reflect.StructTag("bar")},
			&structTags{Name: "bar"},
			parseStructTags,
		},
		{
			"JSONFallback empty",
			reflect.StructField{Name: "foo", Tag: reflect.StructTag("")},
			&structTags{Name: "foo"},
			parseJSONStructTags,
		},
		{
			"JSONFallback tag only dash",
			reflect.StructField{Name: "foo", Tag: reflect.StructTag("-")},
			&structTags{Skip: true},
			parseJSONStructTags,
		},
		{
			"JSONFallback bson tag only dash",
			reflect.StructField{Name: "foo", Tag: reflect.StructTag(`bson:"-"`)},
			&structTags{Skip: true},
			parseJSONStructTags,
		},
		{
			"JSONFallback all options",
			reflect.StructField{Name: "foo", Tag: reflect.StructTag(`bar,omitempty,omitzero,minsize,truncate,inline`)},
			&structTags{Name: "bar", OmitEmpty: true, OmitZero: true, MinSize: true, Truncate: true, Inline: true},
			parseJSONStructTags,
		},
		{
			"JSONFallback all options default name",
			reflect.StructField{Name: "foo", Tag: reflect.StructTag(`,omitempty,omitzero,minsize,truncate,inline`)},
			&structTags{Name: "foo", OmitEmpty: true, OmitZero: true, MinSize: true, Truncate: true, Inline: true},
			parseJSONStructTags,
		},
		{
			"JSONFallback bson tag all options",
			reflect.StructField{Name: "foo", Tag: reflect.StructTag(`bson:"bar,omitempty,omitzero,minsize,truncate,inline"`)},
			&structTags{Name: "bar", OmitEmpty: true, OmitZero: true, MinSize: true, Truncate: true, Inline: true},
			parseJSONStructTags,
		},
		{
			"JSONFallback bson tag all options default name",
			reflect.StructField{Name: "foo", Tag: reflect.StructTag(`bson:",omitempty,omitzero,minsize,truncate,inline"`)},
			&structTags{Name: "foo", OmitEmpty: true, OmitZero: true, MinSize: true, Truncate: true, Inline: true},
			parseJSONStructTags,
		},
		{
			"JSONFallback json tag all options",
			reflect.StructField{Name: "foo", Tag: reflect.StructTag(`json:"bar,omitempty,omitzero,minsize,truncate,inline"`)},
			&structTags{Name: "bar", OmitEmpty: true, OmitZero: true, MinSize: true, Truncate: true, Inline: true},
			parseJSONStructTags,
		},
		{
			"JSONFallback json tag all options default name",
			reflect.StructField{Name: "foo", Tag: reflect.StructTag(`json:",omitempty,omitzero,minsize,truncate,inline"`)},
			&structTags{Name: "foo", OmitEmpty: true, OmitZero: true, MinSize: true, Truncate: true, Inline: true},
			parseJSONStructTags,
		},
		{
			"JSONFallback bson tag overrides other tags",
			reflect.StructField{Name: "foo", Tag: reflect.StructTag(`bson:"bar" json:"qux,truncate"`)},
			&structTags{Name: "bar"},
			parseJSONStructTags,
		},
		{
			"JSONFallback ignore xml",
			reflect.StructField{Name: "foo", Tag: reflect.StructTag(`xml:"bar"`)},
			&structTags{Name: "foo"},
			parseJSONStructTags,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := tc.parser(tc.sf)
			noerr(t, err)
			if !cmp.Equal(got, tc.want) {
				t.Errorf("Returned struct tags do not match. got %#v; want %#v", got, tc.want)
			}
		})
	}
}
