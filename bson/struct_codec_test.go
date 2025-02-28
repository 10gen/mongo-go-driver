// Copyright (C) MongoDB, Inc. 2017-present.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

package bson

import (
	"reflect"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/internal/assert"
)

func TestIsZero(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		description    string
		value          interface{}
		omitZeroStruct bool
		want           bool
	}{
		{
			description: "false",
			value:       false,
			want:        true,
		},
		{
			description: "0",
			value:       0,
			want:        true,
		},
		{
			description: "nil pointer to int",
			value:       (*int)(nil),
			want:        true,
		},
		{
			description: "time.Time",
			value:       time.Unix(1682123781, 0),
			want:        false,
		},
		{
			description: "empty time.Time",
			value:       time.Time{},
			want:        true,
		},
		{
			description: "nil pointer to time.Time",
			value:       (*time.Time)(nil),
			want:        true,
		},
		{
			description: "zero struct",
			value:       struct{ Val bool }{},
			want:        false,
		},
		{
			description: "non-zero struct",
			value:       struct{ Val bool }{Val: true},
			want:        false,
		},
		{
			description: "nil pointer to struct",
			value:       (*struct{ Val bool })(nil),
			want:        true,
		},
		{
			description: "pointer to struct",
			value:       &struct{ Val bool }{},
			want:        false,
		},
		{
			description: "zero struct that implements Zeroer",
			value:       zeroTest{},
			want:        false,
		},
		{
			description: "non-zero struct that implements Zeroer",
			value:       zeroTest{reportZero: true},
			want:        true,
		},
		{
			description: "pointer to zero struct that implements Zeroer",
			value:       &zeroTest{},
			want:        false,
		},
		{
			description: "pointer to non-zero struct that implements Zeroer",
			value:       &zeroTest{reportZero: true},
			want:        true,
		},
		{
			description:    "zero struct with omitZeroStruct",
			value:          struct{ Val bool }{},
			omitZeroStruct: true,
			want:           true,
		},
		{
			description:    "non-zero struct with omitZeroStruct",
			value:          struct{ Val bool }{Val: true},
			omitZeroStruct: true,
			want:           false,
		},
		{
			description:    "zero struct with only private fields omitZeroStruct",
			value:          struct{ val bool }{},
			omitZeroStruct: true,
			want:           true,
		},
		// TODO(GODRIVER-2820): Change the expected value to "false" once the logic is updated to
		// TODO also inspect private struct fields.
		{
			description:    "non-zero struct with only private fields with omitZeroStruct",
			value:          struct{ val bool }{val: true},
			omitZeroStruct: true,
			want:           true,
		},
		{
			description:    "pointer to zero struct with omitZeroStruct",
			value:          &struct{ Val bool }{},
			omitZeroStruct: true,
			want:           false,
		},
		{
			description:    "pointer to non-zero struct with omitZeroStruct",
			value:          &struct{ Val bool }{Val: true},
			omitZeroStruct: true,
			want:           false,
		},
		{
			description: "empty map",
			value:       map[string]string{},
			want:        true,
		},
		{
			description: "empty slice",
			value:       []struct{}{},
			want:        true,
		},
		{
			description: "empty string",
			value:       "",
			want:        true,
		},
	}

	for _, tc := range testCases {
		tc := tc // Capture range variable.

		t.Run(tc.description, func(t *testing.T) {
			t.Parallel()

			got := isEmpty(reflect.ValueOf(tc.value), tc.omitZeroStruct)
			assert.Equal(t, tc.want, got, "expected and actual isEmpty return are different")
		})
	}
}
