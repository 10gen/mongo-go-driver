// Copyright (C) MongoDB, Inc. 2017-present.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

package bson

import (
	"bytes"
	"errors"
	"reflect"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/internal/assert"
	"go.mongodb.org/mongo-driver/v2/internal/require"
	"go.mongodb.org/mongo-driver/v2/x/bsonx/bsoncore"
)

func TestBasicEncode(t *testing.T) {
	for _, tc := range marshalingTestCases {
		t.Run(tc.name, func(t *testing.T) {
			got := make(sliceWriter, 0, 1024)
			vw := NewDocumentWriter(&got)
			reg := defaultRegistry
			encoder, err := reg.LookupEncoder(reflect.TypeOf(tc.val))
			noerr(t, err)
			err = encoder.EncodeValue(EncodeContext{Registry: reg}, vw, reflect.ValueOf(tc.val))
			noerr(t, err)

			if !bytes.Equal(got, tc.want) {
				t.Errorf("Bytes are not equal. got %v; want %v", got, tc.want)
				t.Errorf("Bytes:\n%v\n%v", got, tc.want)
			}
		})
	}
}

func TestEncoderEncode(t *testing.T) {
	for _, tc := range marshalingTestCases {
		t.Run(tc.name, func(t *testing.T) {
			got := make(sliceWriter, 0, 1024)
			vw := NewDocumentWriter(&got)
			enc := NewEncoder(vw)
			err := enc.Encode(tc.val)
			noerr(t, err)

			if !bytes.Equal(got, tc.want) {
				t.Errorf("Bytes are not equal. got %v; want %v", got, tc.want)
				t.Errorf("Bytes:\n%v\n%v", got, tc.want)
			}
		})
	}

	t.Run("Marshaler", func(t *testing.T) {
		testCases := []struct {
			name    string
			buf     []byte
			err     error
			wanterr error
			vw      ValueWriter
		}{
			{
				"error",
				nil,
				errors.New("Marshaler error"),
				errors.New("Marshaler error"),
				&valueReaderWriter{},
			},
			{
				"copy error",
				[]byte{0x05, 0x00, 0x00, 0x00, 0x00},
				nil,
				errors.New("copy error"),
				&valueReaderWriter{Err: errors.New("copy error"), ErrAfter: writeDocument},
			},
			{
				"success",
				[]byte{0x07, 0x00, 0x00, 0x00, 0x0A, 0x00, 0x00},
				nil,
				nil,
				nil,
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				marshaler := testMarshaler{buf: tc.buf, err: tc.err}

				var vw ValueWriter
				b := make(sliceWriter, 0, 100)
				compareVW := false
				if tc.vw != nil {
					vw = tc.vw
				} else {
					compareVW = true
					vw = NewDocumentWriter(&b)
				}
				enc := NewEncoder(vw)
				got := enc.Encode(marshaler)
				want := tc.wanterr
				if !assert.CompareErrors(got, want) {
					t.Errorf("Did not receive expected error. got %v; want %v", got, want)
				}
				if compareVW {
					buf := b
					if !bytes.Equal(buf, tc.buf) {
						t.Errorf("Copied bytes do not match. got %v; want %v", buf, tc.buf)
					}
				}
			})
		}
	})
}

type testMarshaler struct {
	buf []byte
	err error
}

func (tm testMarshaler) MarshalBSON() ([]byte, error) { return tm.buf, tm.err }

func docToBytes(d interface{}) []byte {
	b, err := Marshal(d)
	if err != nil {
		panic(err)
	}
	return b
}

type stringerTest struct{}

func (stringerTest) String() string {
	return "test key"
}

func TestEncoderConfiguration(t *testing.T) {
	type inlineDuplicateInner struct {
		Duplicate string
	}

	type inlineDuplicateOuter struct {
		Inline    inlineDuplicateInner `bson:",inline"`
		Duplicate string
	}

	type zeroStruct struct {
		MyString string
	}

	testCases := []struct {
		description string
		configure   func(*Encoder)
		input       interface{}
		want        []byte
		wantErr     error
	}{
		// Test that ErrorOnInlineDuplicates causes the Encoder to return an error if there are any
		// duplicate fields in the marshaled document caused by using the "inline" struct tag.
		{
			description: "ErrorOnInlineDuplicates",
			configure: func(enc *Encoder) {
				enc.ErrorOnInlineDuplicates()
			},
			input: inlineDuplicateOuter{
				Inline:    inlineDuplicateInner{Duplicate: "inner"},
				Duplicate: "outer",
			},
			wantErr: errors.New("struct bson.inlineDuplicateOuter has duplicated key duplicate"),
		},
		// Test that IntMinSize encodes Go int and int64 values as BSON int32 if the value is small
		// enough.
		{
			description: "IntMinSize",
			configure: func(enc *Encoder) {
				enc.IntMinSize()
			},
			input: D{
				{Key: "myInt", Value: int(1)},
				{Key: "myInt64", Value: int64(1)},
				{Key: "myUint", Value: uint(1)},
				{Key: "myUint32", Value: uint32(1)},
				{Key: "myUint64", Value: uint64(1)},
			},
			want: bsoncore.NewDocumentBuilder().
				AppendInt32("myInt", 1).
				AppendInt32("myInt64", 1).
				AppendInt32("myUint", 1).
				AppendInt32("myUint32", 1).
				AppendInt32("myUint64", 1).
				Build(),
		},
		// Test that StringifyMapKeysWithFmt uses fmt.Sprint to convert map keys to BSON field names.
		{
			description: "StringifyMapKeysWithFmt",
			configure: func(enc *Encoder) {
				enc.StringifyMapKeysWithFmt()
			},
			input: map[stringerTest]string{
				{}: "test value",
			},
			want: bsoncore.NewDocumentBuilder().
				AppendString("test key", "test value").
				Build(),
		},
		// Test that NilMapAsEmpty encodes nil Go maps as empty BSON documents.
		{
			description: "NilMapAsEmpty",
			configure: func(enc *Encoder) {
				enc.NilMapAsEmpty()
			},
			input: D{{Key: "myMap", Value: map[string]string(nil)}},
			want: bsoncore.NewDocumentBuilder().
				AppendDocument("myMap", bsoncore.NewDocumentBuilder().Build()).
				Build(),
		},
		// Test that NilSliceAsEmpty encodes nil Go slices as empty BSON arrays.
		{
			description: "NilSliceAsEmpty",
			configure: func(enc *Encoder) {
				enc.NilSliceAsEmpty()
			},
			input: D{{Key: "mySlice", Value: []string(nil)}},
			want: bsoncore.NewDocumentBuilder().
				AppendArray("mySlice", bsoncore.NewArrayBuilder().Build()).
				Build(),
		},
		// Test that NilByteSliceAsEmpty encodes nil Go byte slices as empty BSON binary elements.
		{
			description: "NilByteSliceAsEmpty",
			configure: func(enc *Encoder) {
				enc.NilByteSliceAsEmpty()
			},
			input: D{{Key: "myBytes", Value: []byte(nil)}},
			want: bsoncore.NewDocumentBuilder().
				AppendBinary("myBytes", TypeBinaryGeneric, []byte{}).
				Build(),
		},
		// Test that OmitZeroStruct omits empty structs from the marshaled document if the
		// "omitempty" struct tag is used.
		{
			description: "OmitZeroStruct",
			configure: func(enc *Encoder) {
				enc.OmitZeroStruct()
			},
			input: struct {
				Zero zeroStruct `bson:",omitempty"`
			}{},
			want: bsoncore.NewDocumentBuilder().Build(),
		},
		// Test that OmitZeroStruct omits empty structs from the marshaled document if
		// OmitEmpty is also set.
		{
			description: "OmitEmpty with non-zeroer struct",
			configure: func(enc *Encoder) {
				enc.OmitZeroStruct()
				enc.OmitEmpty()
			},
			input: struct {
				Zero zeroStruct
			}{},
			want: bsoncore.NewDocumentBuilder().Build(),
		},
		// Test that OmitEmpty omits empty values from the marshaled document.
		{
			description: "OmitEmpty",
			configure: func(enc *Encoder) {
				enc.OmitEmpty()
			},
			input: struct {
				Zero    zeroTest
				I64     int64
				F64     float64
				String  string
				Boolean bool
				Slice   []int
				Array   [0]int
				Map     map[string]int
				Bytes   []byte
				Time    time.Time
				Pointer *int
			}{
				Zero: zeroTest{true},
			},
			want: bsoncore.NewDocumentBuilder().Build(),
		},
		// Test that UseJSONStructTags causes the Encoder to fall back to "json" struct tags if
		// "bson" struct tags are not available.
		{
			description: "UseJSONStructTags",
			configure: func(enc *Encoder) {
				enc.UseJSONStructTags()
			},
			input: struct {
				StructFieldName string `json:"jsonFieldName"`
			}{
				StructFieldName: "test value",
			},
			want: bsoncore.NewDocumentBuilder().
				AppendString("jsonFieldName", "test value").
				Build(),
		},
	}

	for _, tc := range testCases {
		tc := tc // Capture range variable.

		t.Run(tc.description, func(t *testing.T) {
			t.Parallel()

			got := new(bytes.Buffer)
			vw := NewDocumentWriter(got)
			enc := NewEncoder(vw)

			tc.configure(enc)

			err := enc.Encode(tc.input)
			if tc.wantErr != nil {
				assert.Equal(t, tc.wantErr, err, "expected and actual errors do not match")
				return
			}
			require.NoError(t, err, "Encode error")

			assert.Equal(t, tc.want, got.Bytes(), "expected and actual encoded BSON do not match")

			// After we compare the raw bytes, also decode the expected and actual BSON as a bson.D
			// and compare them. The goal is to make assertion failures easier to debug because
			// binary diffs are very difficult to understand.
			var wantDoc D
			err = Unmarshal(tc.want, &wantDoc)
			require.NoError(t, err, "Unmarshal error")
			var gotDoc D
			err = Unmarshal(got.Bytes(), &gotDoc)
			require.NoError(t, err, "Unmarshal error")

			assert.Equal(t, wantDoc, gotDoc, "expected and actual decoded documents do not match")
		})
	}
}
