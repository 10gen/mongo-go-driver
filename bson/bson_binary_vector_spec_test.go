// Copyright (C) MongoDB, Inc. 2024-present.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

package bson

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path"
	"testing"

	"go.mongodb.org/mongo-driver/v2/internal/require"
)

const bsonBinaryVectorDir = "../testdata/bson-binary-vector/"

type bsonBinaryVectorTests struct {
	Description string                     `json:"description"`
	TestKey     string                     `json:"test_key"`
	Tests       []bsonBinaryVectorTestCase `json:"tests"`
}

type bsonBinaryVectorTestCase struct {
	Description   string        `json:"description"`
	Valid         bool          `json:"valid"`
	Vector        []interface{} `json:"vector"`
	DtypeHex      string        `json:"dtype_hex"`
	DtypeAlias    string        `json:"dtype_alias"`
	Padding       int           `json:"padding"`
	CanonicalBson string        `json:"canonical_bson"`
}

func TestBsonBinaryVector(t *testing.T) {
	t.Parallel()

	jsonFiles, err := findJSONFilesInDir(bsonBinaryVectorDir)
	require.NoErrorf(t, err, "error finding JSON files in %s: %v", bsonBinaryVectorDir, err)

	for _, file := range jsonFiles {
		filepath := path.Join(bsonBinaryVectorDir, file)
		content, err := os.ReadFile(filepath)
		require.NoErrorf(t, err, "reading test file %s", filepath)

		var tests bsonBinaryVectorTests
		require.NoErrorf(t, json.Unmarshal(content, &tests), "parsing test file %s", filepath)

		t.Run(tests.Description, func(t *testing.T) {
			t.Parallel()

			for _, test := range tests.Tests {
				test := test
				t.Run(test.Description, func(t *testing.T) {
					t.Parallel()

					runBsonBinaryVectorTest(t, tests.TestKey, test)
				})
			}
		})
	}

	t.Run("Insufficient vector data FLOAT32", func(t *testing.T) {
		t.Parallel()

		val := Binary{Subtype: TypeBinaryVector}

		for _, tc := range [][]byte{
			{Float32Vector, 0, 42},
			{Float32Vector, 0, 42, 42},
			{Float32Vector, 0, 42, 42, 42},

			{Float32Vector, 0, 42, 42, 42, 42, 42},
			{Float32Vector, 0, 42, 42, 42, 42, 42, 42},
			{Float32Vector, 0, 42, 42, 42, 42, 42, 42, 42},
		} {
			t.Run(fmt.Sprintf("marshaling %d bytes", len(tc)-2), func(t *testing.T) {
				val.Data = tc
				b, err := Marshal(D{{"vector", val}})
				require.NoError(t, err, "marshaling test BSON")
				var got struct {
					Vector Vector
				}
				err = Unmarshal(b, &got)
				require.ErrorContains(t, err, errInsufficientVectorData.Error())
			})
		}
	})

	t.Run("FLOAT32 with padding", func(t *testing.T) {
		t.Parallel()

		t.Run("Unmarshaling", func(t *testing.T) {
			val := D{{"vector", Binary{Subtype: TypeBinaryVector, Data: []byte{Float32Vector, 3}}}}
			b, err := Marshal(val)
			require.NoError(t, err, "marshaling test BSON")
			var got struct {
				Vector Vector
			}
			err = Unmarshal(b, &got)
			require.ErrorContains(t, err, errNonZeroVectorPadding.Error())
		})
	})

	t.Run("INT8 with padding", func(t *testing.T) {
		t.Parallel()

		t.Run("Unmarshaling", func(t *testing.T) {
			val := D{{"vector", Binary{Subtype: TypeBinaryVector, Data: []byte{Int8Vector, 3}}}}
			b, err := Marshal(val)
			require.NoError(t, err, "marshaling test BSON")
			var got struct {
				Vector Vector
			}
			err = Unmarshal(b, &got)
			require.ErrorContains(t, err, errNonZeroVectorPadding.Error())
		})
	})

	t.Run("Padding specified with no vector data PACKED_BIT", func(t *testing.T) {
		t.Parallel()

		t.Run("Marshaling", func(t *testing.T) {
			_, err := NewPackedBitVector(nil, 1)
			require.EqualError(t, err, errNonZeroVectorPadding.Error())
		})
		t.Run("Unmarshaling", func(t *testing.T) {
			val := D{{"vector", Binary{Subtype: TypeBinaryVector, Data: []byte{PackedBitVector, 1}}}}
			b, err := Marshal(val)
			require.NoError(t, err, "marshaling test BSON")
			var got struct {
				Vector Vector
			}
			err = Unmarshal(b, &got)
			require.ErrorContains(t, err, errNonZeroVectorPadding.Error())
		})
	})

	t.Run("Exceeding maximum padding PACKED_BIT", func(t *testing.T) {
		t.Parallel()

		t.Run("Marshaling", func(t *testing.T) {
			_, err := NewPackedBitVector(nil, 8)
			require.EqualError(t, err, errVectorPaddingTooLarge.Error())
		})
		t.Run("Unmarshaling", func(t *testing.T) {
			val := D{{"vector", Binary{Subtype: TypeBinaryVector, Data: []byte{PackedBitVector, 8}}}}
			b, err := Marshal(val)
			require.NoError(t, err, "marshaling test BSON")
			var got struct {
				Vector Vector
			}
			err = Unmarshal(b, &got)
			require.ErrorContains(t, err, errVectorPaddingTooLarge.Error())
		})
	})
}

func convertSlice[T int8 | float32 | byte](s []interface{}) []T {
	v := make([]T, len(s))
	for i, e := range s {
		f := math.NaN()
		switch val := e.(type) {
		case float64:
			f = val
		case string:
			if val == "inf" {
				f = math.Inf(0)
			} else if val == "-inf" {
				f = math.Inf(-1)
			}
		}
		v[i] = T(f)
	}
	return v
}

func runBsonBinaryVectorTest(t *testing.T, testKey string, test bsonBinaryVectorTestCase) {
	testVector := make(map[string]Vector)
	switch alias := test.DtypeHex; alias {
	case "0x03":
		testVector[testKey] = Vector{
			dType:    Int8Vector,
			int8Data: convertSlice[int8](test.Vector),
		}
	case "0x27":
		testVector[testKey] = Vector{
			dType:       Float32Vector,
			float32Data: convertSlice[float32](test.Vector),
		}
	case "0x10":
		testVector[testKey] = Vector{
			dType:      PackedBitVector,
			bitData:    convertSlice[byte](test.Vector),
			bitPadding: uint8(test.Padding),
		}
	default:
		t.Fatalf("unsupported vector type: %s", alias)
	}

	testBSON, err := hex.DecodeString(test.CanonicalBson)
	require.NoError(t, err, "decoding canonical BSON")

	t.Run("Unmarshaling", func(t *testing.T) {
		skipCases := map[string]string{
			"FLOAT32 with padding":                             "run in alternative case",
			"Overflow Vector INT8":                             "compile-time restriction",
			"Underflow Vector INT8":                            "compile-time restriction",
			"INT8 with padding":                                "run in alternative case",
			"INT8 with float inputs":                           "compile-time restriction",
			"Overflow Vector PACKED_BIT":                       "compile-time restriction",
			"Underflow Vector PACKED_BIT":                      "compile-time restriction",
			"Vector with float values PACKED_BIT":              "compile-time restriction",
			"Padding specified with no vector data PACKED_BIT": "run in alternative case",
			"Exceeding maximum padding PACKED_BIT":             "run in alternative case",
			"Negative padding PACKED_BIT":                      "compile-time restriction",
		}
		if reason, ok := skipCases[test.Description]; ok {
			t.Skipf("skip test case %s: %s", test.Description, reason)
		}

		t.Parallel()

		var got map[string]Vector
		err := Unmarshal(testBSON, &got)
		require.NoError(t, err)
		require.Equal(t, testVector, got)
	})

	t.Run("Marshaling", func(t *testing.T) {
		skipCases := map[string]string{
			"FLOAT32 with padding":                             "private padding field",
			"Overflow Vector INT8":                             "compile-time restriction",
			"Underflow Vector INT8":                            "compile-time restriction",
			"INT8 with padding":                                "private padding field",
			"INT8 with float inputs":                           "compile-time restriction",
			"Overflow Vector PACKED_BIT":                       "compile-time restriction",
			"Underflow Vector PACKED_BIT":                      "compile-time restriction",
			"Vector with float values PACKED_BIT":              "compile-time restriction",
			"Padding specified with no vector data PACKED_BIT": "run in alternative case",
			"Exceeding maximum padding PACKED_BIT":             "run in alternative case",
			"Negative padding PACKED_BIT":                      "compile-time restriction",
		}
		if reason, ok := skipCases[test.Description]; ok {
			t.Skipf("skip test case %s: %s", test.Description, reason)
		}

		t.Parallel()

		got, err := Marshal(testVector)
		require.NoError(t, err)
		require.Equal(t, testBSON, got)
	})
}
