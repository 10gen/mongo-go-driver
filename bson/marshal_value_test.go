// Copyright (C) MongoDB, Inc. 2017-present.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

package bson

import (
	"strings"
	"testing"

	"go.mongodb.org/mongo-driver/bson/bsoncodec"
	"go.mongodb.org/mongo-driver/bson/bsontype"
	"go.mongodb.org/mongo-driver/internal/assert"
)

func TestMarshalValue(t *testing.T) {
	marshalValueTestCases := marshalValueTestCases(t)

	t.Run("MarshalValue", func(t *testing.T) {
		for _, tc := range marshalValueTestCases {
			t.Run(tc.name, func(t *testing.T) {
				valueType, valueBytes, err := MarshalValue(tc.val)
				assert.Nil(t, err, "MarshalValue error: %v", err)
				compareMarshalValueResults(t, tc, valueType, valueBytes)
			})
		}
	})
	t.Run("MarshalValueAppend", func(t *testing.T) {
		for _, tc := range marshalValueTestCases {
			t.Run(tc.name, func(t *testing.T) {
				valueType, valueBytes, err := MarshalValueAppend(nil, tc.val)
				assert.Nil(t, err, "MarshalValueAppend error: %v", err)
				compareMarshalValueResults(t, tc, valueType, valueBytes)
			})
		}
	})
	t.Run("MarshalValueWithRegistry", func(t *testing.T) {
		for _, tc := range marshalValueTestCases {
			t.Run(tc.name, func(t *testing.T) {
				valueType, valueBytes, err := MarshalValueWithRegistry(DefaultRegistry, tc.val)
				assert.Nil(t, err, "MarshalValueWithRegistry error: %v", err)
				compareMarshalValueResults(t, tc, valueType, valueBytes)
			})
		}
	})
	t.Run("MarshalValueWithContext", func(t *testing.T) {
		ec := bsoncodec.EncodeContext{Registry: DefaultRegistry}
		for _, tc := range marshalValueTestCases {
			t.Run(tc.name, func(t *testing.T) {
				valueType, valueBytes, err := MarshalValueWithContext(ec, tc.val)
				assert.Nil(t, err, "MarshalValueWithContext error: %v", err)
				compareMarshalValueResults(t, tc, valueType, valueBytes)
			})
		}
	})
	t.Run("MarshalValueAppendWithRegistry", func(t *testing.T) {
		for _, tc := range marshalValueTestCases {
			t.Run(tc.name, func(t *testing.T) {
				valueType, valueBytes, err := MarshalValueAppendWithRegistry(DefaultRegistry, nil, tc.val)
				assert.Nil(t, err, "MarshalValueAppendWithRegistry error: %v", err)
				compareMarshalValueResults(t, tc, valueType, valueBytes)
			})
		}
	})
	t.Run("MarshalValueAppendWithContext", func(t *testing.T) {
		ec := bsoncodec.EncodeContext{Registry: DefaultRegistry}
		for _, tc := range marshalValueTestCases {
			t.Run(tc.name, func(t *testing.T) {
				valueType, valueBytes, err := MarshalValueAppendWithContext(ec, nil, tc.val)
				assert.Nil(t, err, "MarshalValueWithContext error: %v", err)
				compareMarshalValueResults(t, tc, valueType, valueBytes)
			})
		}
	})
}

func compareMarshalValueResults(t *testing.T, tc marshalValueTestCase, gotType bsontype.Type, gotBytes []byte) {
	t.Helper()
	expectedValue := RawValue{Type: tc.bsontype, Value: tc.bytes}
	gotValue := RawValue{Type: gotType, Value: gotBytes}
	assert.Equal(t, expectedValue, gotValue, "value mismatch; expected %s, got %s", expectedValue, gotValue)
}

// benchmark covering GODRIVER-2779
func BenchmarkSliceCodec_Marshal(b *testing.B) {
	testStruct := unmarshalerNonPtrStruct{B: []byte(strings.Repeat("t", 4096))}

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		_, _, err := MarshalValueWithRegistry(DefaultRegistry, testStruct)
		if err != nil {
			b.Fatal(err)
		}
	}
}
