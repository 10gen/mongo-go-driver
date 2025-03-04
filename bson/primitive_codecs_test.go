// Copyright (C) MongoDB, Inc. 2017-present.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

package bson

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"reflect"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"go.mongodb.org/mongo-driver/v2/internal/assert"
	"go.mongodb.org/mongo-driver/v2/x/bsonx/bsoncore"
)

func bytesFromDoc(doc interface{}) []byte {
	b, err := Marshal(doc)
	if err != nil {
		panic(fmt.Errorf("Couldn't marshal BSON document: %w", err))
	}
	return b
}

func TestPrimitiveValueEncoders(t *testing.T) {
	t.Parallel()

	var wrong = func(string, string) string { return "wrong" }

	type subtest struct {
		name   string
		val    interface{}
		ectx   *EncodeContext
		llvrw  *valueReaderWriter
		invoke invoked
		err    error
	}

	testCases := []struct {
		name     string
		ve       ValueEncoder
		subtests []subtest
	}{
		{
			"RawValueEncodeValue",
			ValueEncoderFunc(rawValueEncodeValue),
			[]subtest{
				{
					"wrong type",
					wrong,
					nil,
					nil,
					nothing,
					ValueEncoderError{
						Name:     "RawValueEncodeValue",
						Types:    []reflect.Type{tRawValue},
						Received: reflect.ValueOf(wrong),
					},
				},
				{
					"RawValue/success",
					RawValue{Type: TypeDouble, Value: bsoncore.AppendDouble(nil, 3.14159)},
					nil,
					nil,
					writeDouble,
					nil,
				},
				{
					"RawValue Type is zero with non-zero value",
					RawValue{
						Type:  0x00,
						Value: bsoncore.AppendDouble(nil, 3.14159),
					},
					nil,
					nil,
					nothing,
					fmt.Errorf("the RawValue Type specifies an invalid BSON type: 0x0"),
				},
				{
					"RawValue Type is invalid",
					RawValue{
						Type:  0x8F,
						Value: bsoncore.AppendDouble(nil, 3.14159),
					},
					nil,
					nil,
					nothing,
					fmt.Errorf("the RawValue Type specifies an invalid BSON type: 0x8f"),
				},
			},
		},
		{
			"RawEncodeValue",
			ValueEncoderFunc(rawEncodeValue),
			[]subtest{
				{
					"wrong type",
					wrong,
					nil,
					nil,
					nothing,
					ValueEncoderError{Name: "RawEncodeValue", Types: []reflect.Type{tRaw}, Received: reflect.ValueOf(wrong)},
				},
				{
					"WriteDocument Error",
					Raw{},
					nil,
					&valueReaderWriter{Err: errors.New("wd error"), ErrAfter: writeDocument},
					writeDocument,
					errors.New("wd error"),
				},
				{
					"Raw.Elements Error",
					Raw{0xFF, 0x00, 0x00, 0x00, 0x00},
					nil,
					&valueReaderWriter{},
					writeDocument,
					errors.New("length read exceeds number of bytes available. length=5 bytes=255"),
				},
				{
					"WriteDocumentElement Error",
					Raw(bytesFromDoc(D{{"foo", nil}})),
					nil,
					&valueReaderWriter{Err: errors.New("wde error"), ErrAfter: writeDocumentElement},
					writeDocumentElement,
					errors.New("wde error"),
				},
				{
					"encodeValue error",
					Raw(bytesFromDoc(D{{"foo", nil}})),
					nil,
					&valueReaderWriter{Err: errors.New("ev error"), ErrAfter: writeNull},
					writeNull,
					errors.New("ev error"),
				},
				{
					"iterator error",
					Raw{0x0C, 0x00, 0x00, 0x00, 0x01, 'f', 'o', 'o', 0x00, 0x01, 0x02, 0x03},
					nil,
					&valueReaderWriter{},
					writeDocumentElement,
					errors.New("not enough bytes available to read type. bytes=3 type=double"),
				},
			},
		},
	}

	for _, tc := range testCases {
		tc := tc // Capture the range variable

		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			for _, subtest := range tc.subtests {
				subtest := subtest // Capture the range variable

				t.Run(subtest.name, func(t *testing.T) {
					t.Parallel()

					var ec EncodeContext
					if subtest.ectx != nil {
						ec = *subtest.ectx
					}
					llvrw := new(valueReaderWriter)
					if subtest.llvrw != nil {
						llvrw = subtest.llvrw
					}
					llvrw.T = t
					err := tc.ve.EncodeValue(ec, llvrw, reflect.ValueOf(subtest.val))
					if !assert.CompareErrors(err, subtest.err) {
						t.Errorf("Errors do not match. got %v; want %v", err, subtest.err)
					}
					invoked := llvrw.invoked
					if !cmp.Equal(invoked, subtest.invoke) {
						t.Errorf("Incorrect method invoked. got %v; want %v", invoked, subtest.invoke)
					}
				})
			}
		})
	}

	t.Run("success path", func(t *testing.T) {
		t.Parallel()

		oid := NewObjectID()
		oids := []ObjectID{NewObjectID(), NewObjectID(), NewObjectID()}
		var str = new(string)
		*str = "bar"
		now := time.Now().Truncate(time.Millisecond)
		murl, err := url.Parse("https://mongodb.com/random-url?hello=world")
		if err != nil {
			t.Errorf("Error parsing URL: %v", err)
			t.FailNow()
		}
		decimal128, err := ParseDecimal128("1.5e10")
		if err != nil {
			t.Errorf("Error parsing decimal128: %v", err)
			t.FailNow()
		}

		testCases := []struct {
			name  string
			value interface{}
			b     []byte
			err   error
		}{
			{
				"D to JavaScript",
				D{{"a", JavaScript(`function() { var hello = "world"; }`)}},
				docToBytes(D{{"a", JavaScript(`function() { var hello = "world"; }`)}}),
				nil,
			},
			{
				"D to Symbol",
				D{{"a", Symbol("foobarbaz")}},
				docToBytes(D{{"a", Symbol("foobarbaz")}}),
				nil,
			},
			{
				"omitempty map",
				struct {
					T map[string]string `bson:",omitempty"`
				}{
					T: map[string]string{},
				},
				docToBytes(D{}),
				nil,
			},
			{
				"omitempty slice",
				struct {
					T []struct{} `bson:",omitempty"`
				}{
					T: []struct{}{},
				},
				docToBytes(D{}),
				nil,
			},
			{
				"omitempty string",
				struct {
					T string `bson:",omitempty"`
				}{
					T: "",
				},
				docToBytes(D{}),
				nil,
			},
			{
				"struct{}",
				struct {
					A bool
					B int32
					C int64
					D uint16
					E uint64
					F float64
					G string
					H map[string]string
					I []byte
					K [2]string
					L struct {
						M string
					}
					P  Raw
					Q  ObjectID
					T  []struct{}
					Y  json.Number
					Z  time.Time
					AA json.Number
					AB *url.URL
					AC Decimal128
					AD *time.Time
					AE testValueMarshaler
					AF RawValue
					AG *RawValue
					AH D
					AI *D
					AJ *D
				}{
					A: true,
					B: 123,
					C: 456,
					D: 789,
					E: 101112,
					F: 3.14159,
					G: "Hello, world",
					H: map[string]string{"foo": "bar"},
					I: []byte{0x01, 0x02, 0x03},
					K: [2]string{"baz", "qux"},
					L: struct {
						M string
					}{
						M: "foobar",
					},
					P:  Raw{0x05, 0x00, 0x00, 0x00, 0x00},
					Q:  oid,
					T:  nil,
					Y:  json.Number("5"),
					Z:  now,
					AA: json.Number("10.1"),
					AB: murl,
					AC: decimal128,
					AD: &now,
					AE: testValueMarshaler{t: TypeString, buf: bsoncore.AppendString(nil, "hello, world")},
					AF: RawValue{Type: TypeString, Value: bsoncore.AppendString(nil, "hello, raw value")},
					AG: &RawValue{Type: TypeDouble, Value: bsoncore.AppendDouble(nil, 3.14159)},
					AH: D{{"foo", "bar"}},
					AI: &D{{"pi", 3.14159}},
					AJ: nil,
				},
				docToBytes(D{
					{"a", true},
					{"b", int32(123)},
					{"c", int64(456)},
					{"d", int32(789)},
					{"e", int64(101112)},
					{"f", float64(3.14159)},
					{"g", "Hello, world"},
					{"h", D{{"foo", "bar"}}},
					{"i", Binary{Subtype: 0x00, Data: []byte{0x01, 0x02, 0x03}}},
					{"k", A{"baz", "qux"}},
					{"l", D{{"m", "foobar"}}},
					{"p", D{}},
					{"q", oid},
					{"t", nil},
					{"y", int64(5)},
					{"z", DateTime(now.UnixNano() / int64(time.Millisecond))},
					{"aa", float64(10.1)},
					{"ab", murl.String()},
					{"ac", decimal128},
					{"ad", DateTime(now.UnixNano() / int64(time.Millisecond))},
					{"ae", "hello, world"},
					{"af", "hello, raw value"},
					{"ag", 3.14159},
					{"ah", D{{"foo", "bar"}}},
					{"ai", D{{"pi", float64(3.14159)}}},
					{"aj", nil},
				}),
				nil,
			},
			{
				"struct{[]interface{}}",
				struct {
					A []bool
					B []int32
					C []int64
					D []uint16
					E []uint64
					F []float64
					G []string
					H []map[string]string
					I [][]byte
					K [1][2]string
					L []struct {
						M string
					}
					N  [][]string
					Q  []Raw
					R  []ObjectID
					T  []struct{}
					W  []map[string]struct{}
					X  []map[string]struct{}
					Y  []map[string]struct{}
					Z  []time.Time
					AA []json.Number
					AB []*url.URL
					AC []Decimal128
					AD []*time.Time
					AE []testValueMarshaler
					AF []D
					AG []*D
				}{
					A: []bool{true},
					B: []int32{123},
					C: []int64{456},
					D: []uint16{789},
					E: []uint64{101112},
					F: []float64{3.14159},
					G: []string{"Hello, world"},
					H: []map[string]string{{"foo": "bar"}},
					I: [][]byte{{0x01, 0x02, 0x03}},
					K: [1][2]string{{"baz", "qux"}},
					L: []struct {
						M string
					}{
						{
							M: "foobar",
						},
					},
					N:  [][]string{{"foo", "bar"}},
					Q:  []Raw{{0x05, 0x00, 0x00, 0x00, 0x00}},
					R:  oids,
					T:  nil,
					W:  nil,
					X:  []map[string]struct{}{},   // Should be empty BSON Array
					Y:  []map[string]struct{}{{}}, // Should be BSON array with one element, an empty BSON SubDocument
					Z:  []time.Time{now, now},
					AA: []json.Number{json.Number("5"), json.Number("10.1")},
					AB: []*url.URL{murl},
					AC: []Decimal128{decimal128},
					AD: []*time.Time{&now, &now},
					AE: []testValueMarshaler{
						{t: TypeString, buf: bsoncore.AppendString(nil, "hello")},
						{t: TypeString, buf: bsoncore.AppendString(nil, "world")},
					},
					AF: []D{{{"foo", "bar"}}, {{"hello", "world"}, {"number", 12345}}},
					AG: []*D{{{"pi", 3.14159}}, nil},
				},
				docToBytes(D{
					{"a", A{true}},
					{"b", A{int32(123)}},
					{"c", A{int64(456)}},
					{"d", A{int32(789)}},
					{"e", A{int64(101112)}},
					{"f", A{float64(3.14159)}},
					{"g", A{"Hello, world"}},
					{"h", A{D{{"foo", "bar"}}}},
					{"i", A{Binary{Subtype: 0x00, Data: []byte{0x01, 0x02, 0x03}}}},
					{"k", A{A{"baz", "qux"}}},
					{"l", A{D{{"m", "foobar"}}}},
					{"n", A{A{"foo", "bar"}}},
					{"q", A{D{}}},
					{"r", A{oids[0], oids[1], oids[2]}},
					{"t", nil},
					{"w", nil},
					{"x", A{}},
					{"y", A{D{}}},
					{"z", A{
						DateTime(now.UnixNano() / int64(time.Millisecond)),
						DateTime(now.UnixNano() / int64(time.Millisecond)),
					}},
					{"aa", A{int64(5), float64(10.10)}},
					{"ab", A{murl.String()}},
					{"ac", A{decimal128}},
					{"ad", A{
						DateTime(now.UnixNano() / int64(time.Millisecond)),
						DateTime(now.UnixNano() / int64(time.Millisecond)),
					}},
					{"ae", A{"hello", "world"}},
					{"af", A{D{{"foo", "bar"}}, D{{"hello", "world"}, {"number", int32(12345)}}}},
					{"ag", A{D{{"pi", float64(3.14159)}}, nil}},
				}),
				nil,
			},
		}

		for _, tc := range testCases {
			tc := tc // Capture the range variable

			t.Run(tc.name, func(t *testing.T) {
				t.Parallel()

				b := make(sliceWriter, 0, 512)
				vw := NewDocumentWriter(&b)
				enc := NewEncoder(vw)
				err := enc.Encode(tc.value)
				if !errors.Is(err, tc.err) {
					t.Errorf("Did not receive expected error. got %v; want %v", err, tc.err)
				}
				if diff := cmp.Diff([]byte(b), tc.b); diff != "" {
					t.Errorf("Bytes written differ: (-got +want)\n%s", diff)
					t.Errorf("Bytes\ngot: %v\nwant:%v\n", b, tc.b)
					t.Errorf("Readers\ngot: %v\nwant:%v\n", Raw(b), Raw(tc.b))
				}
			})
		}
	})
}

func TestPrimitiveValueDecoders(t *testing.T) {
	var wrong = func(string, string) string { return "wrong" }

	const cansetreflectiontest = "cansetreflectiontest"

	type subtest struct {
		name   string
		val    interface{}
		dctx   *DecodeContext
		llvrw  *valueReaderWriter
		invoke invoked
		err    error
	}

	testCases := []struct {
		name     string
		vd       ValueDecoder
		subtests []subtest
	}{
		{
			"RawValueDecodeValue",
			ValueDecoderFunc(rawValueDecodeValue),
			[]subtest{
				{
					"wrong type",
					wrong,
					nil,
					&valueReaderWriter{},
					nothing,
					ValueDecoderError{
						Name:     "RawValueDecodeValue",
						Types:    []reflect.Type{tRawValue},
						Received: reflect.ValueOf(wrong),
					},
				},
				{
					"ReadValue Error",
					RawValue{},
					nil,
					&valueReaderWriter{
						BSONType: TypeBinary,
						Err:      errors.New("rb error"),
						ErrAfter: readBinary,
					},
					readBinary,
					errors.New("rb error"),
				},
				{
					"RawValue/success",
					RawValue{Type: TypeBinary, Value: bsoncore.AppendBinary(nil, 0xFF, []byte{0x01, 0x02, 0x03})},
					nil,
					&valueReaderWriter{
						BSONType: TypeBinary,
						Return: bsoncore.Value{
							Type: bsoncore.TypeBinary,
							Data: bsoncore.AppendBinary(nil, 0xFF, []byte{0x01, 0x02, 0x03}),
						},
					},
					readBinary,
					nil,
				},
			},
		},
		{
			"RawDecodeValue",
			ValueDecoderFunc(rawDecodeValue),
			[]subtest{
				{
					"wrong type",
					wrong,
					nil,
					&valueReaderWriter{},
					nothing,
					ValueDecoderError{Name: "RawDecodeValue", Types: []reflect.Type{tRaw}, Received: reflect.ValueOf(wrong)},
				},
				{
					"*Raw is nil",
					(*Raw)(nil),
					nil,
					nil,
					nothing,
					ValueDecoderError{
						Name:     "RawDecodeValue",
						Types:    []reflect.Type{tRaw},
						Received: reflect.ValueOf((*Raw)(nil)),
					},
				},
				{
					"Copy error",
					Raw{},
					nil,
					&valueReaderWriter{Err: errors.New("copy error"), ErrAfter: readDocument},
					readDocument,
					errors.New("copy error"),
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			for _, rc := range tc.subtests {
				t.Run(rc.name, func(t *testing.T) {
					var dc DecodeContext
					if rc.dctx != nil {
						dc = *rc.dctx
					}
					llvrw := new(valueReaderWriter)
					if rc.llvrw != nil {
						llvrw = rc.llvrw
					}
					llvrw.T = t
					if rc.val == cansetreflectiontest { // We're doing a CanSet reflection test
						err := tc.vd.DecodeValue(dc, llvrw, reflect.Value{})
						if !assert.CompareErrors(err, rc.err) {
							t.Errorf("Errors do not match. got %v; want %v", err, rc.err)
						}

						val := reflect.New(reflect.TypeOf(rc.val)).Elem()
						err = tc.vd.DecodeValue(dc, llvrw, val)
						if !assert.CompareErrors(err, rc.err) {
							t.Errorf("Errors do not match. got %v; want %v", err, rc.err)
						}
						return
					}
					var val reflect.Value
					if rtype := reflect.TypeOf(rc.val); rtype != nil {
						val = reflect.New(rtype).Elem()
					}
					want := rc.val
					defer func() {
						if err := recover(); err != nil {
							fmt.Println(t.Name())
							panic(err)
						}
					}()
					err := tc.vd.DecodeValue(dc, llvrw, val)
					if !assert.CompareErrors(err, rc.err) {
						t.Errorf("Errors do not match. got %v; want %v", err, rc.err)
					}
					invoked := llvrw.invoked
					if !cmp.Equal(invoked, rc.invoke) {
						t.Errorf("Incorrect method invoked. got %v; want %v", invoked, rc.invoke)
					}
					var got interface{}
					if val.IsValid() && val.CanInterface() {
						got = val.Interface()
					}
					if rc.err == nil && !cmp.Equal(got, want) {
						t.Errorf("Values do not match. got (%T)%v; want (%T)%v", got, got, want, want)
					}
				})
			}
		})
	}

	t.Run("success path", func(t *testing.T) {
		oid := NewObjectID()
		oids := []ObjectID{NewObjectID(), NewObjectID(), NewObjectID()}
		var str = new(string)
		*str = "bar"
		now := time.Now().Truncate(time.Millisecond)
		murl, err := url.Parse("https://mongodb.com/random-url?hello=world")
		if err != nil {
			t.Errorf("Error parsing URL: %v", err)
			t.FailNow()
		}
		decimal128, err := ParseDecimal128("1.5e10")
		if err != nil {
			t.Errorf("Error parsing decimal128: %v", err)
			t.FailNow()
		}

		testCases := []struct {
			name  string
			value interface{}
			b     []byte
			err   error
		}{
			{
				"map[string]int",
				map[string]int32{"foo": 1},
				[]byte{
					0x0E, 0x00, 0x00, 0x00,
					0x10, 'f', 'o', 'o', 0x00,
					0x01, 0x00, 0x00, 0x00,
					0x00,
				},
				nil,
			},
			{
				"map[string]ObjectID",
				map[string]ObjectID{"foo": oid},
				docToBytes(D{{"foo", oid}}),
				nil,
			},
			{
				"map[string]Reader",
				map[string]Raw{"Z": {0x05, 0x00, 0x00, 0x00, 0x00}},
				docToBytes(D{{"Z", Raw{0x05, 0x00, 0x00, 0x00, 0x00}}}),
				nil,
			},
			{
				"map[string][]int32",
				map[string][]int32{"Z": {1, 2, 3}},
				docToBytes(D{{"Z", A{int32(1), int32(2), int32(3)}}}),
				nil,
			},
			{
				"map[string][]ObjectID",
				map[string][]ObjectID{"Z": oids},
				docToBytes(D{{"Z", A{oids[0], oids[1], oids[2]}}}),
				nil,
			},
			{
				"map[string][]json.Number(int64)",
				map[string][]json.Number{"Z": {json.Number("5"), json.Number("10")}},
				docToBytes(D{{"Z", A{int64(5), int64(10)}}}),
				nil,
			},
			{
				"map[string][]json.Number(float64)",
				map[string][]json.Number{"Z": {json.Number("5"), json.Number("10.1")}},
				docToBytes(D{{"Z", A{int64(5), float64(10.1)}}}),
				nil,
			},
			{
				"map[string][]*url.URL",
				map[string][]*url.URL{"Z": {murl}},
				docToBytes(D{{"Z", A{murl.String()}}}),
				nil,
			},
			{
				"map[string][]Decimal128",
				map[string][]Decimal128{"Z": {decimal128}},
				docToBytes(D{{"Z", A{decimal128}}}),
				nil,
			},
			{
				"-",
				struct {
					A string `bson:"-"`
				}{
					A: "",
				},
				docToBytes(D{}),
				nil,
			},
			{
				"omitempty",
				struct {
					A string `bson:",omitempty"`
				}{
					A: "",
				},
				docToBytes(D{}),
				nil,
			},
			{
				"omitempty, empty time",
				struct {
					A time.Time `bson:",omitempty"`
				}{
					A: time.Time{},
				},
				docToBytes(D{}),
				nil,
			},
			{
				"no private fields",
				noPrivateFields{a: "should be empty"},
				docToBytes(D{}),
				nil,
			},
			{
				"minsize",
				struct {
					A int64 `bson:",minsize"`
				}{
					A: 12345,
				},
				docToBytes(D{{"a", int32(12345)}}),
				nil,
			},
			{
				"inline",
				struct {
					Foo struct {
						A int64 `bson:",minsize"`
					} `bson:",inline"`
				}{
					Foo: struct {
						A int64 `bson:",minsize"`
					}{
						A: 12345,
					},
				},
				docToBytes(D{{"a", int32(12345)}}),
				nil,
			},
			{
				"inline map",
				struct {
					Foo map[string]string `bson:",inline"`
				}{
					Foo: map[string]string{"foo": "bar"},
				},
				docToBytes(D{{"foo", "bar"}}),
				nil,
			},
			{
				"alternate name bson:name",
				struct {
					A string `bson:"foo"`
				}{
					A: "bar",
				},
				docToBytes(D{{"foo", "bar"}}),
				nil,
			},
			{
				"alternate name",
				struct {
					A string `bson:"foo"`
				}{
					A: "bar",
				},
				docToBytes(D{{"foo", "bar"}}),
				nil,
			},
			{
				"inline, omitempty",
				struct {
					A   string
					Foo zeroTest `bson:"omitempty,inline"`
				}{
					A:   "bar",
					Foo: zeroTest{true},
				},
				docToBytes(D{{"a", "bar"}}),
				nil,
			},
			{
				"JavaScript to D",
				D{{"a", JavaScript(`function() { var hello = "world"; }`)}},
				docToBytes(D{{"a", JavaScript(`function() { var hello = "world"; }`)}}),
				nil,
			},
			{
				"Symbol to D",
				D{{"a", Symbol("foobarbaz")}},
				docToBytes(D{{"a", Symbol("foobarbaz")}}),
				nil,
			},
			{
				"struct{}",
				struct {
					A bool
					B int32
					C int64
					D uint16
					E uint64
					F float64
					G string
					H map[string]string
					I []byte
					K [2]string
					L struct {
						M string
					}
					P  Raw
					Q  ObjectID
					T  []struct{}
					Y  json.Number
					Z  time.Time
					AA json.Number
					AB *url.URL
					AC Decimal128
					AD *time.Time
					AE *testValueUnmarshaler
					AF RawValue
					AG *RawValue
					AH D
					AI *D
					AJ *D
				}{
					A: true,
					B: 123,
					C: 456,
					D: 789,
					E: 101112,
					F: 3.14159,
					G: "Hello, world",
					H: map[string]string{"foo": "bar"},
					I: []byte{0x01, 0x02, 0x03},
					K: [2]string{"baz", "qux"},
					L: struct {
						M string
					}{
						M: "foobar",
					},
					P:  Raw{0x05, 0x00, 0x00, 0x00, 0x00},
					Q:  oid,
					T:  nil,
					Y:  json.Number("5"),
					Z:  now,
					AA: json.Number("10.1"),
					AB: murl,
					AC: decimal128,
					AD: &now,
					AE: &testValueUnmarshaler{t: TypeString, val: bsoncore.AppendString(nil, "hello, world!")},
					AF: RawValue{Type: TypeDouble, Value: bsoncore.AppendDouble(nil, 3.14159)},
					AG: &RawValue{Type: TypeBinary, Value: bsoncore.AppendBinary(nil, 0xFF, []byte{0x01, 0x02, 0x03})},
					AH: D{{"foo", "bar"}},
					AI: &D{{"pi", 3.14159}},
					AJ: nil,
				},
				docToBytes(D{
					{"a", true},
					{"b", int32(123)},
					{"c", int64(456)},
					{"d", int32(789)},
					{"e", int64(101112)},
					{"f", float64(3.14159)},
					{"g", "Hello, world"},
					{"h", D{{"foo", "bar"}}},
					{"i", Binary{Subtype: 0x00, Data: []byte{0x01, 0x02, 0x03}}},
					{"k", A{"baz", "qux"}},
					{"l", D{{"m", "foobar"}}},
					{"p", D{}},
					{"q", oid},
					{"t", nil},
					{"y", int64(5)},
					{"z", DateTime(now.UnixNano() / int64(time.Millisecond))},
					{"aa", float64(10.1)},
					{"ab", murl.String()},
					{"ac", decimal128},
					{"ad", DateTime(now.UnixNano() / int64(time.Millisecond))},
					{"ae", "hello, world!"},
					{"af", float64(3.14159)},
					{"ag", Binary{Subtype: 0xFF, Data: []byte{0x01, 0x02, 0x03}}},
					{"ah", D{{"foo", "bar"}}},
					{"ai", D{{"pi", float64(3.14159)}}},
					{"aj", nil},
				}),
				nil,
			},
			{
				"struct{[]interface{}}",
				struct {
					A []bool
					B []int32
					C []int64
					D []uint16
					E []uint64
					F []float64
					G []string
					H []map[string]string
					I [][]byte
					K [1][2]string
					L []struct {
						M string
					}
					N  [][]string
					Q  []Raw
					R  []ObjectID
					T  []struct{}
					W  []map[string]struct{}
					X  []map[string]struct{}
					Y  []map[string]struct{}
					Z  []time.Time
					AA []json.Number
					AB []*url.URL
					AC []Decimal128
					AD []*time.Time
					AE []*testValueUnmarshaler
					AF []D
					AG []*D
				}{
					A: []bool{true},
					B: []int32{123},
					C: []int64{456},
					D: []uint16{789},
					E: []uint64{101112},
					F: []float64{3.14159},
					G: []string{"Hello, world"},
					H: []map[string]string{{"foo": "bar"}},
					I: [][]byte{{0x01, 0x02, 0x03}},
					K: [1][2]string{{"baz", "qux"}},
					L: []struct {
						M string
					}{
						{
							M: "foobar",
						},
					},
					N:  [][]string{{"foo", "bar"}},
					Q:  []Raw{{0x05, 0x00, 0x00, 0x00, 0x00}},
					R:  oids,
					T:  nil,
					W:  nil,
					X:  []map[string]struct{}{},   // Should be empty BSON Array
					Y:  []map[string]struct{}{{}}, // Should be BSON array with one element, an empty BSON SubDocument
					Z:  []time.Time{now, now},
					AA: []json.Number{json.Number("5"), json.Number("10.1")},
					AB: []*url.URL{murl},
					AC: []Decimal128{decimal128},
					AD: []*time.Time{&now, &now},
					AE: []*testValueUnmarshaler{
						{t: TypeString, val: bsoncore.AppendString(nil, "hello")},
						{t: TypeString, val: bsoncore.AppendString(nil, "world")},
					},
					AF: []D{{{"foo", "bar"}}, {{"hello", "world"}, {"number", int64(12345)}}},
					AG: []*D{{{"pi", 3.14159}}, nil},
				},
				docToBytes(D{
					{"a", A{true}},
					{"b", A{int32(123)}},
					{"c", A{int64(456)}},
					{"d", A{int32(789)}},
					{"e", A{int64(101112)}},
					{"f", A{float64(3.14159)}},
					{"g", A{"Hello, world"}},
					{"h", A{D{{"foo", "bar"}}}},
					{"i", A{Binary{Subtype: 0x00, Data: []byte{0x01, 0x02, 0x03}}}},
					{"k", A{A{"baz", "qux"}}},
					{"l", A{D{{"m", "foobar"}}}},
					{"n", A{A{"foo", "bar"}}},
					{"q", A{D{}}},
					{"r", A{oids[0], oids[1], oids[2]}},
					{"t", nil},
					{"w", nil},
					{"x", A{}},
					{"y", A{D{}}},
					{"z", A{
						DateTime(now.UnixNano() / int64(time.Millisecond)),
						DateTime(now.UnixNano() / int64(time.Millisecond)),
					}},
					{"aa", A{int64(5), float64(10.10)}},
					{"ab", A{murl.String()}},
					{"ac", A{decimal128}},
					{"ad", A{
						DateTime(now.UnixNano() / int64(time.Millisecond)),
						DateTime(now.UnixNano() / int64(time.Millisecond)),
					}},
					{"ae", A{"hello", "world"}},
					{"af", A{
						D{{"foo", "bar"}},
						D{{"hello", "world"}, {"number", int64(12345)}},
					}},
					{"ag", A{D{{"pi", float64(3.14159)}}, nil}},
				}),
				nil,
			},
		}

		t.Run("Decode", func(t *testing.T) {
			for _, tc := range testCases {
				t.Run(tc.name, func(t *testing.T) {
					vr := NewDocumentReader(bytes.NewReader(tc.b))
					dec := NewDecoder(vr)
					gotVal := reflect.New(reflect.TypeOf(tc.value))
					err := dec.Decode(gotVal.Interface())
					noerr(t, err)
					got := gotVal.Elem().Interface()
					want := tc.value
					if diff := cmp.Diff(
						got, want,
						cmp.Comparer(compareDecimal128),
						cmp.Comparer(compareNoPrivateFields),
						cmp.Comparer(compareZeroTest),
					); diff != "" {
						t.Errorf("difference:\n%s", diff)
						t.Errorf("Values are not equal.\ngot: %#v\nwant:%#v", got, want)
					}
				})
			}
		})
	})
}

type testValueMarshaler struct {
	t   Type
	buf []byte
	err error
}

func (tvm testValueMarshaler) MarshalBSONValue() (byte, []byte, error) {
	return byte(tvm.t), tvm.buf, tvm.err
}

type testValueUnmarshaler struct {
	t   Type
	val []byte
	err error
}

func (tvu *testValueUnmarshaler) UnmarshalBSONValue(t byte, val []byte) error {
	tvu.t, tvu.val = Type(t), val
	return tvu.err
}
func (tvu testValueUnmarshaler) Equal(tvu2 testValueUnmarshaler) bool {
	return tvu.t == tvu2.t && bytes.Equal(tvu.val, tvu2.val)
}

type noPrivateFields struct {
	a string
}

func compareNoPrivateFields(npf1, npf2 noPrivateFields) bool {
	return npf1.a != npf2.a // We don't want these to be equal
}

type zeroTest struct {
	reportZero bool
}

func (z zeroTest) IsZero() bool { return z.reportZero }

var _ Zeroer = zeroTest{}

func compareZeroTest(_, _ zeroTest) bool { return true }

func compareDecimal128(d1, d2 Decimal128) bool {
	d1H, d1L := d1.GetBytes()
	d2H, d2L := d2.GetBytes()

	if d1H != d2H {
		return false
	}

	if d1L != d2L {
		return false
	}

	return true
}
