// Copyright (C) MongoDB, Inc. 2017-present.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

package bson

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/url"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"go.mongodb.org/mongo-driver/v2/internal/assert"
	"go.mongodb.org/mongo-driver/v2/x/bsonx/bsoncore"
)

type myInterface interface {
	Foo() int
}

type myStruct struct {
	Val int
}

func (ms myStruct) Foo() int {
	return ms.Val
}

func TestDefaultValueEncoders(t *testing.T) {
	var wrong = func(string, string) string { return "wrong" }

	type mybool bool
	type myint8 int8
	type myint16 int16
	type myint32 int32
	type myint64 int64
	type myint int
	type myuint8 uint8
	type myuint16 uint16
	type myuint32 uint32
	type myuint64 uint64
	type myuint uint
	type myfloat32 float32
	type myfloat64 float64

	now := time.Now().Truncate(time.Millisecond)
	pjsnum := new(json.Number)
	*pjsnum = json.Number("3.14159")
	d128 := NewDecimal128(12345, 67890)
	var nilValueMarshaler *testValueMarshaler
	var nilMarshaler *testMarshaler

	vmStruct := struct{ V testValueMarshalPtr }{testValueMarshalPtr{t: TypeString, buf: []byte{0x04, 0x00, 0x00, 0x00, 'f', 'o', 'o', 0x00}}}
	mStruct := struct{ V testMarshalPtr }{testMarshalPtr{buf: bsoncore.BuildDocument(nil, bsoncore.AppendDoubleElement(nil, "pi", 3.14159))}}

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
			"BooleanEncodeValue",
			ValueEncoderFunc(booleanEncodeValue),
			[]subtest{
				{
					"wrong type",
					wrong,
					nil,
					nil,
					nothing,
					ValueEncoderError{Name: "BooleanEncodeValue", Kinds: []reflect.Kind{reflect.Bool}, Received: reflect.ValueOf(wrong)},
				},
				{"fast path", bool(true), nil, nil, writeBoolean, nil},
				{"reflection path", mybool(true), nil, nil, writeBoolean, nil},
			},
		},
		{
			"IntEncodeValue",
			ValueEncoderFunc(intEncodeValue),
			[]subtest{
				{
					"wrong type",
					wrong,
					nil,
					nil,
					nothing,
					ValueEncoderError{
						Name:     "IntEncodeValue",
						Kinds:    []reflect.Kind{reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Int},
						Received: reflect.ValueOf(wrong),
					},
				},
				{"int8/fast path", int8(127), nil, nil, writeInt32, nil},
				{"int16/fast path", int16(32767), nil, nil, writeInt32, nil},
				{"int32/fast path", int32(2147483647), nil, nil, writeInt32, nil},
				{"int64/fast path", int64(1234567890987), nil, nil, writeInt64, nil},
				{"int64/fast path - minsize", int64(math.MaxInt32), &EncodeContext{minSize: true}, nil, writeInt32, nil},
				{"int64/fast path - minsize too large", int64(math.MaxInt32 + 1), &EncodeContext{minSize: true}, nil, writeInt64, nil},
				{"int64/fast path - minsize too small", int64(math.MinInt32 - 1), &EncodeContext{minSize: true}, nil, writeInt64, nil},
				{"int/fast path - positive int32", int(math.MaxInt32 - 1), nil, nil, writeInt32, nil},
				{"int/fast path - negative int32", int(math.MinInt32 + 1), nil, nil, writeInt32, nil},
				{"int/fast path - MaxInt32", int(math.MaxInt32), nil, nil, writeInt32, nil},
				{"int/fast path - MinInt32", int(math.MinInt32), nil, nil, writeInt32, nil},
				{"int8/reflection path", myint8(127), nil, nil, writeInt32, nil},
				{"int16/reflection path", myint16(32767), nil, nil, writeInt32, nil},
				{"int32/reflection path", myint32(2147483647), nil, nil, writeInt32, nil},
				{"int64/reflection path", myint64(1234567890987), nil, nil, writeInt64, nil},
				{"int64/reflection path - minsize", myint64(math.MaxInt32), &EncodeContext{minSize: true}, nil, writeInt32, nil},
				{"int64/reflection path - minsize too large", myint64(math.MaxInt32 + 1), &EncodeContext{minSize: true}, nil, writeInt64, nil},
				{"int64/reflection path - minsize too small", myint64(math.MinInt32 - 1), &EncodeContext{minSize: true}, nil, writeInt64, nil},
				{"int/reflection path - positive int32", myint(math.MaxInt32 - 1), nil, nil, writeInt32, nil},
				{"int/reflection path - negative int32", myint(math.MinInt32 + 1), nil, nil, writeInt32, nil},
				{"int/reflection path - MaxInt32", myint(math.MaxInt32), nil, nil, writeInt32, nil},
				{"int/reflection path - MinInt32", myint(math.MinInt32), nil, nil, writeInt32, nil},
			},
		},
		{
			"UintEncodeValue",
			&uintCodec{},
			[]subtest{
				{
					"wrong type",
					wrong,
					nil,
					nil,
					nothing,
					ValueEncoderError{
						Name:     "UintEncodeValue",
						Kinds:    []reflect.Kind{reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uint},
						Received: reflect.ValueOf(wrong),
					},
				},
				{"uint8/fast path", uint8(127), nil, nil, writeInt32, nil},
				{"uint16/fast path", uint16(32767), nil, nil, writeInt32, nil},
				{"uint32/fast path", uint32(2147483647), nil, nil, writeInt64, nil},
				{"uint64/fast path", uint64(1234567890987), nil, nil, writeInt64, nil},
				{"uint/fast path", uint(1234567), nil, nil, writeInt64, nil},
				{"uint32/fast path - minsize", uint32(2147483647), &EncodeContext{minSize: true}, nil, writeInt32, nil},
				{"uint64/fast path - minsize", uint64(2147483647), &EncodeContext{minSize: true}, nil, writeInt32, nil},
				{"uint/fast path - minsize", uint(2147483647), &EncodeContext{minSize: true}, nil, writeInt32, nil},
				{"uint32/fast path - minsize too large", uint32(2147483648), &EncodeContext{minSize: true}, nil, writeInt64, nil},
				{"uint64/fast path - minsize too large", uint64(2147483648), &EncodeContext{minSize: true}, nil, writeInt64, nil},
				{"uint/fast path - minsize too large", uint(2147483648), &EncodeContext{minSize: true}, nil, writeInt64, nil},
				{"uint64/fast path - overflow", uint64(1 << 63), nil, nil, nothing, fmt.Errorf("%d overflows int64", uint64(1<<63))},
				{"uint8/reflection path", myuint8(127), nil, nil, writeInt32, nil},
				{"uint16/reflection path", myuint16(32767), nil, nil, writeInt32, nil},
				{"uint32/reflection path", myuint32(2147483647), nil, nil, writeInt64, nil},
				{"uint64/reflection path", myuint64(1234567890987), nil, nil, writeInt64, nil},
				{"uint32/reflection path - minsize", myuint32(2147483647), &EncodeContext{minSize: true}, nil, writeInt32, nil},
				{"uint64/reflection path - minsize", myuint64(2147483647), &EncodeContext{minSize: true}, nil, writeInt32, nil},
				{"uint/reflection path - minsize", myuint(2147483647), &EncodeContext{minSize: true}, nil, writeInt32, nil},
				{"uint32/reflection path - minsize too large", myuint(1 << 31), &EncodeContext{minSize: true}, nil, writeInt64, nil},
				{"uint64/reflection path - minsize too large", myuint64(1 << 31), &EncodeContext{minSize: true}, nil, writeInt64, nil},
				{"uint/reflection path - minsize too large", myuint(2147483648), &EncodeContext{minSize: true}, nil, writeInt64, nil},
				{"uint64/reflection path - overflow", myuint64(1 << 63), nil, nil, nothing, fmt.Errorf("%d overflows int64", uint64(1<<63))},
			},
		},
		{
			"FloatEncodeValue",
			ValueEncoderFunc(floatEncodeValue),
			[]subtest{
				{
					"wrong type",
					wrong,
					nil,
					nil,
					nothing,
					ValueEncoderError{
						Name:     "FloatEncodeValue",
						Kinds:    []reflect.Kind{reflect.Float32, reflect.Float64},
						Received: reflect.ValueOf(wrong),
					},
				},
				{"float32/fast path", float32(3.14159), nil, nil, writeDouble, nil},
				{"float64/fast path", float64(3.14159), nil, nil, writeDouble, nil},
				{"float32/reflection path", myfloat32(3.14159), nil, nil, writeDouble, nil},
				{"float64/reflection path", myfloat64(3.14159), nil, nil, writeDouble, nil},
			},
		},
		{
			"TimeEncodeValue",
			&timeCodec{},
			[]subtest{
				{
					"wrong type",
					wrong,
					nil,
					nil,
					nothing,
					ValueEncoderError{Name: "TimeEncodeValue", Types: []reflect.Type{tTime}, Received: reflect.ValueOf(wrong)},
				},
				{"time.Time", now, nil, nil, writeDateTime, nil},
			},
		},
		{
			"MapEncodeValue",
			&mapCodec{},
			[]subtest{
				{
					"wrong kind",
					wrong,
					nil,
					nil,
					nothing,
					ValueEncoderError{Name: "MapEncodeValue", Kinds: []reflect.Kind{reflect.Map}, Received: reflect.ValueOf(wrong)},
				},
				{
					"WriteDocument Error",
					map[string]interface{}{},
					nil,
					&valueReaderWriter{Err: errors.New("wd error"), ErrAfter: writeDocument},
					writeDocument,
					errors.New("wd error"),
				},
				{
					"Lookup Error",
					map[string]int{"foo": 1},
					&EncodeContext{Registry: newTestRegistry()},
					&valueReaderWriter{},
					writeDocument,
					fmt.Errorf("no encoder found for int"),
				},
				{
					"WriteDocumentElement Error",
					map[string]interface{}{"foo": "bar"},
					&EncodeContext{Registry: buildDefaultRegistry()},
					&valueReaderWriter{Err: errors.New("wde error"), ErrAfter: writeDocumentElement},
					writeDocumentElement,
					errors.New("wde error"),
				},
				{
					"EncodeValue Error",
					map[string]interface{}{"foo": "bar"},
					&EncodeContext{Registry: buildDefaultRegistry()},
					&valueReaderWriter{Err: errors.New("ev error"), ErrAfter: writeString},
					writeString,
					errors.New("ev error"),
				},
				{
					"empty map/success",
					map[string]interface{}{},
					&EncodeContext{Registry: newTestRegistry()},
					&valueReaderWriter{},
					writeDocumentEnd,
					nil,
				},
				{
					"with interface/success",
					map[string]myInterface{"foo": myStruct{1}},
					&EncodeContext{Registry: buildDefaultRegistry()},
					nil,
					writeDocumentEnd,
					nil,
				},
				{
					"with interface/nil/success",
					map[string]myInterface{"foo": nil},
					&EncodeContext{Registry: buildDefaultRegistry()},
					nil,
					writeDocumentEnd,
					nil,
				},
				{
					"non-string key success",
					map[int]interface{}{
						1: "foobar",
					},
					&EncodeContext{Registry: buildDefaultRegistry()},
					&valueReaderWriter{},
					writeDocumentEnd,
					nil,
				},
			},
		},
		{
			"ArrayEncodeValue",
			ValueEncoderFunc(arrayEncodeValue),
			[]subtest{
				{
					"wrong kind",
					wrong,
					nil,
					nil,
					nothing,
					ValueEncoderError{Name: "ArrayEncodeValue", Kinds: []reflect.Kind{reflect.Array}, Received: reflect.ValueOf(wrong)},
				},
				{
					"WriteArray Error",
					[1]string{},
					nil,
					&valueReaderWriter{Err: errors.New("wa error"), ErrAfter: writeArray},
					writeArray,
					errors.New("wa error"),
				},
				{
					"Lookup Error",
					[1]int{1},
					&EncodeContext{Registry: newTestRegistry()},
					&valueReaderWriter{},
					writeArray,
					fmt.Errorf("no encoder found for int"),
				},
				{
					"WriteArrayElement Error",
					[1]string{"foo"},
					&EncodeContext{Registry: buildDefaultRegistry()},
					&valueReaderWriter{Err: errors.New("wae error"), ErrAfter: writeArrayElement},
					writeArrayElement,
					errors.New("wae error"),
				},
				{
					"EncodeValue Error",
					[1]string{"foo"},
					&EncodeContext{Registry: buildDefaultRegistry()},
					&valueReaderWriter{Err: errors.New("ev error"), ErrAfter: writeString},
					writeString,
					errors.New("ev error"),
				},
				{
					"[1]E/success",
					[1]E{{"hello", "world"}},
					&EncodeContext{Registry: buildDefaultRegistry()},
					nil,
					writeDocumentEnd,
					nil,
				},
				{
					"[1]E/success",
					[1]E{{"hello", nil}},
					&EncodeContext{Registry: buildDefaultRegistry()},
					nil,
					writeDocumentEnd,
					nil,
				},
				{
					"[1]interface/success",
					[1]myInterface{myStruct{1}},
					&EncodeContext{Registry: buildDefaultRegistry()},
					nil,
					writeArrayEnd,
					nil,
				},
				{
					"[1]interface/nil/success",
					[1]myInterface{nil},
					&EncodeContext{Registry: buildDefaultRegistry()},
					nil,
					writeArrayEnd,
					nil,
				},
			},
		},
		{
			"SliceEncodeValue",
			&sliceCodec{},
			[]subtest{
				{
					"wrong kind",
					wrong,
					nil,
					nil,
					nothing,
					ValueEncoderError{Name: "SliceEncodeValue", Kinds: []reflect.Kind{reflect.Slice}, Received: reflect.ValueOf(wrong)},
				},
				{
					"WriteArray Error",
					[]string{},
					nil,
					&valueReaderWriter{Err: errors.New("wa error"), ErrAfter: writeArray},
					writeArray,
					errors.New("wa error"),
				},
				{
					"Lookup Error",
					[]int{1},
					&EncodeContext{Registry: newTestRegistry()},
					&valueReaderWriter{},
					writeArray,
					fmt.Errorf("no encoder found for int"),
				},
				{
					"WriteArrayElement Error",
					[]string{"foo"},
					&EncodeContext{Registry: buildDefaultRegistry()},
					&valueReaderWriter{Err: errors.New("wae error"), ErrAfter: writeArrayElement},
					writeArrayElement,
					errors.New("wae error"),
				},
				{
					"EncodeValue Error",
					[]string{"foo"},
					&EncodeContext{Registry: buildDefaultRegistry()},
					&valueReaderWriter{Err: errors.New("ev error"), ErrAfter: writeString},
					writeString,
					errors.New("ev error"),
				},
				{
					"D/success",
					D{{"hello", "world"}},
					&EncodeContext{Registry: buildDefaultRegistry()},
					nil,
					writeDocumentEnd,
					nil,
				},
				{
					"D/success",
					D{{"hello", nil}},
					&EncodeContext{Registry: buildDefaultRegistry()},
					nil,
					writeDocumentEnd,
					nil,
				},
				{
					"empty slice/success",
					[]interface{}{},
					&EncodeContext{Registry: newTestRegistry()},
					&valueReaderWriter{},
					writeArrayEnd,
					nil,
				},
				{
					"interface/success",
					[]myInterface{myStruct{1}},
					&EncodeContext{Registry: buildDefaultRegistry()},
					nil,
					writeArrayEnd,
					nil,
				},
				{
					"interface/success",
					[]myInterface{nil},
					&EncodeContext{Registry: buildDefaultRegistry()},
					nil,
					writeArrayEnd,
					nil,
				},
			},
		},
		{
			"ObjectIDEncodeValue",
			ValueEncoderFunc(objectIDEncodeValue),
			[]subtest{
				{
					"wrong type",
					wrong,
					nil,
					nil,
					nothing,
					ValueEncoderError{Name: "ObjectIDEncodeValue", Types: []reflect.Type{tOID}, Received: reflect.ValueOf(wrong)},
				},
				{
					"ObjectID/success",
					ObjectID{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C},
					nil, nil, writeObjectID, nil,
				},
			},
		},
		{
			"Decimal128EncodeValue",
			ValueEncoderFunc(decimal128EncodeValue),
			[]subtest{
				{
					"wrong type",
					wrong,
					nil,
					nil,
					nothing,
					ValueEncoderError{Name: "Decimal128EncodeValue", Types: []reflect.Type{tDecimal}, Received: reflect.ValueOf(wrong)},
				},
				{"Decimal128/success", d128, nil, nil, writeDecimal128, nil},
			},
		},
		{
			"JSONNumberEncodeValue",
			ValueEncoderFunc(jsonNumberEncodeValue),
			[]subtest{
				{
					"wrong type",
					wrong,
					nil,
					nil,
					nothing,
					ValueEncoderError{Name: "JSONNumberEncodeValue", Types: []reflect.Type{tJSONNumber}, Received: reflect.ValueOf(wrong)},
				},
				{
					"json.Number/invalid",
					json.Number("hello world"),
					nil, nil, nothing, errors.New(`strconv.ParseFloat: parsing "hello world": invalid syntax`),
				},
				{
					"json.Number/int64/success",
					json.Number("1234567890"),
					nil, nil, writeInt64, nil,
				},
				{
					"json.Number/float64/success",
					json.Number("3.14159"),
					nil, nil, writeDouble, nil,
				},
			},
		},
		{
			"URLEncodeValue",
			ValueEncoderFunc(urlEncodeValue),
			[]subtest{
				{
					"wrong type",
					wrong,
					nil,
					nil,
					nothing,
					ValueEncoderError{Name: "URLEncodeValue", Types: []reflect.Type{tURL}, Received: reflect.ValueOf(wrong)},
				},
				{"url.URL", url.URL{Scheme: "http", Host: "example.com"}, nil, nil, writeString, nil},
			},
		},
		{
			"ByteSliceEncodeValue",
			&byteSliceCodec{},
			[]subtest{
				{
					"wrong type",
					wrong,
					nil,
					nil,
					nothing,
					ValueEncoderError{Name: "ByteSliceEncodeValue", Types: []reflect.Type{tByteSlice}, Received: reflect.ValueOf(wrong)},
				},
				{"[]byte", []byte{0x01, 0x02, 0x03}, nil, nil, writeBinary, nil},
				{"[]byte/nil", []byte(nil), nil, nil, writeNull, nil},
			},
		},
		{
			"EmptyInterfaceEncodeValue",
			&emptyInterfaceCodec{},
			[]subtest{
				{
					"wrong type",
					wrong,
					nil,
					nil,
					nothing,
					ValueEncoderError{Name: "EmptyInterfaceEncodeValue", Types: []reflect.Type{tEmpty}, Received: reflect.ValueOf(wrong)},
				},
			},
		},
		{
			"ValueMarshalerEncodeValue",
			ValueEncoderFunc(valueMarshalerEncodeValue),
			[]subtest{
				{
					"wrong type",
					wrong,
					nil,
					nil,
					nothing,
					ValueEncoderError{
						Name:     "ValueMarshalerEncodeValue",
						Types:    []reflect.Type{tValueMarshaler},
						Received: reflect.ValueOf(wrong),
					},
				},
				{
					"MarshalBSONValue error",
					testValueMarshaler{err: errors.New("mbsonv error")},
					nil,
					nil,
					nothing,
					errors.New("mbsonv error"),
				},
				{
					"Copy error",
					testValueMarshaler{},
					nil,
					nil,
					nothing,
					fmt.Errorf("cannot copy unknown BSON type %s", Type(0)),
				},
				{
					"success struct implementation",
					testValueMarshaler{t: TypeString, buf: []byte{0x04, 0x00, 0x00, 0x00, 'f', 'o', 'o', 0x00}},
					nil,
					nil,
					writeString,
					nil,
				},
				{
					"success ptr to struct implementation",
					&testValueMarshaler{t: TypeString, buf: []byte{0x04, 0x00, 0x00, 0x00, 'f', 'o', 'o', 0x00}},
					nil,
					nil,
					writeString,
					nil,
				},
				{
					"success nil ptr to struct implementation",
					nilValueMarshaler,
					nil,
					nil,
					writeNull,
					nil,
				},
				{
					"success ptr to ptr implementation",
					&testValueMarshalPtr{t: TypeString, buf: []byte{0x04, 0x00, 0x00, 0x00, 'f', 'o', 'o', 0x00}},
					nil,
					nil,
					writeString,
					nil,
				},
				{
					"unaddressable ptr implementation",
					testValueMarshalPtr{t: TypeString, buf: []byte{0x04, 0x00, 0x00, 0x00, 'f', 'o', 'o', 0x00}},
					nil,
					nil,
					nothing,
					ValueEncoderError{
						Name:     "ValueMarshalerEncodeValue",
						Types:    []reflect.Type{tValueMarshaler},
						Received: reflect.ValueOf(testValueMarshalPtr{}),
					},
				},
			},
		},
		{
			"MarshalerEncodeValue",
			ValueEncoderFunc(marshalerEncodeValue),
			[]subtest{
				{
					"wrong type",
					wrong,
					nil,
					nil,
					nothing,
					ValueEncoderError{Name: "MarshalerEncodeValue", Types: []reflect.Type{tMarshaler}, Received: reflect.ValueOf(wrong)},
				},
				{
					"MarshalBSON error",
					testMarshaler{err: errors.New("mbson error")},
					nil,
					nil,
					nothing,
					errors.New("mbson error"),
				},
				{
					"success struct implementation",
					testMarshaler{buf: bsoncore.BuildDocument(nil, bsoncore.AppendDoubleElement(nil, "pi", 3.14159))},
					nil,
					nil,
					writeDocumentEnd,
					nil,
				},
				{
					"success ptr to struct implementation",
					&testMarshaler{buf: bsoncore.BuildDocument(nil, bsoncore.AppendDoubleElement(nil, "pi", 3.14159))},
					nil,
					nil,
					writeDocumentEnd,
					nil,
				},
				{
					"success nil ptr to struct implementation",
					nilMarshaler,
					nil,
					nil,
					writeNull,
					nil,
				},
				{
					"success ptr to ptr implementation",
					&testMarshalPtr{buf: bsoncore.BuildDocument(nil, bsoncore.AppendDoubleElement(nil, "pi", 3.14159))},
					nil,
					nil,
					writeDocumentEnd,
					nil,
				},
				{
					"unaddressable ptr implementation",
					testMarshalPtr{buf: bsoncore.BuildDocument(nil, bsoncore.AppendDoubleElement(nil, "pi", 3.14159))},
					nil,
					nil,
					nothing,
					ValueEncoderError{Name: "MarshalerEncodeValue", Types: []reflect.Type{tMarshaler}, Received: reflect.ValueOf(testMarshalPtr{})},
				},
			},
		},
		{
			"PointerCodec.EncodeValue",
			&pointerCodec{},
			[]subtest{
				{
					"nil",
					nil,
					nil,
					nil,
					writeNull,
					nil,
				},
				{
					"not pointer",
					int32(123456),
					nil,
					nil,
					nothing,
					ValueEncoderError{Name: "PointerCodec.EncodeValue", Kinds: []reflect.Kind{reflect.Ptr}, Received: reflect.ValueOf(int32(123456))},
				},
				{
					"typed nil",
					(*int32)(nil),
					nil,
					nil,
					writeNull,
					nil,
				},
				{
					"no encoder",
					&wrong,
					&EncodeContext{Registry: buildDefaultRegistry()},
					nil,
					nothing,
					errNoEncoder{Type: reflect.TypeOf(wrong)},
				},
			},
		},
		{
			"pointer implementation addressable interface",
			&pointerCodec{},
			[]subtest{
				{
					"ValueMarshaler",
					&vmStruct,
					&EncodeContext{Registry: buildDefaultRegistry()},
					nil,
					writeDocumentEnd,
					nil,
				},
				{
					"Marshaler",
					&mStruct,
					&EncodeContext{Registry: buildDefaultRegistry()},
					nil,
					writeDocumentEnd,
					nil,
				},
			},
		},
		{
			"JavaScriptEncodeValue",
			ValueEncoderFunc(javaScriptEncodeValue),
			[]subtest{
				{
					"wrong type",
					wrong,
					nil,
					nil,
					nothing,
					ValueEncoderError{Name: "JavaScriptEncodeValue", Types: []reflect.Type{tJavaScript}, Received: reflect.ValueOf(wrong)},
				},
				{"JavaScript", JavaScript("foobar"), nil, nil, writeJavascript, nil},
			},
		},
		{
			"SymbolEncodeValue",
			ValueEncoderFunc(symbolEncodeValue),
			[]subtest{
				{
					"wrong type",
					wrong,
					nil,
					nil,
					nothing,
					ValueEncoderError{Name: "SymbolEncodeValue", Types: []reflect.Type{tSymbol}, Received: reflect.ValueOf(wrong)},
				},
				{"Symbol", Symbol("foobar"), nil, nil, writeSymbol, nil},
			},
		},
		{
			"BinaryEncodeValue",
			ValueEncoderFunc(binaryEncodeValue),
			[]subtest{
				{
					"wrong type",
					wrong,
					nil,
					nil,
					nothing,
					ValueEncoderError{Name: "BinaryEncodeValue", Types: []reflect.Type{tBinary}, Received: reflect.ValueOf(wrong)},
				},
				{"Binary/success", Binary{Data: []byte{0x01, 0x02}, Subtype: 0xFF}, nil, nil, writeBinaryWithSubtype, nil},
			},
		},
		{
			"UndefinedEncodeValue",
			ValueEncoderFunc(undefinedEncodeValue),
			[]subtest{
				{
					"wrong type",
					wrong,
					nil,
					nil,
					nothing,
					ValueEncoderError{Name: "UndefinedEncodeValue", Types: []reflect.Type{tUndefined}, Received: reflect.ValueOf(wrong)},
				},
				{"Undefined/success", Undefined{}, nil, nil, writeUndefined, nil},
			},
		},
		{
			"DateTimeEncodeValue",
			ValueEncoderFunc(dateTimeEncodeValue),
			[]subtest{
				{
					"wrong type",
					wrong,
					nil,
					nil,
					nothing,
					ValueEncoderError{Name: "DateTimeEncodeValue", Types: []reflect.Type{tDateTime}, Received: reflect.ValueOf(wrong)},
				},
				{"DateTime/success", DateTime(1234567890), nil, nil, writeDateTime, nil},
			},
		},
		{
			"NullEncodeValue",
			ValueEncoderFunc(nullEncodeValue),
			[]subtest{
				{
					"wrong type",
					wrong,
					nil,
					nil,
					nothing,
					ValueEncoderError{Name: "NullEncodeValue", Types: []reflect.Type{tNull}, Received: reflect.ValueOf(wrong)},
				},
				{"Null/success", Null{}, nil, nil, writeNull, nil},
			},
		},
		{
			"RegexEncodeValue",
			ValueEncoderFunc(regexEncodeValue),
			[]subtest{
				{
					"wrong type",
					wrong,
					nil,
					nil,
					nothing,
					ValueEncoderError{Name: "RegexEncodeValue", Types: []reflect.Type{tRegex}, Received: reflect.ValueOf(wrong)},
				},
				{"Regex/success", Regex{Pattern: "foo", Options: "bar"}, nil, nil, writeRegex, nil},
			},
		},
		{
			"DBPointerEncodeValue",
			ValueEncoderFunc(dbPointerEncodeValue),
			[]subtest{
				{
					"wrong type",
					wrong,
					nil,
					nil,
					nothing,
					ValueEncoderError{Name: "DBPointerEncodeValue", Types: []reflect.Type{tDBPointer}, Received: reflect.ValueOf(wrong)},
				},
				{
					"DBPointer/success",
					DBPointer{
						DB:      "foobar",
						Pointer: ObjectID{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C},
					},
					nil, nil, writeDBPointer, nil,
				},
			},
		},
		{
			"TimestampEncodeValue",
			ValueEncoderFunc(timestampEncodeValue),
			[]subtest{
				{
					"wrong type",
					wrong,
					nil,
					nil,
					nothing,
					ValueEncoderError{Name: "TimestampEncodeValue", Types: []reflect.Type{tTimestamp}, Received: reflect.ValueOf(wrong)},
				},
				{"Timestamp/success", Timestamp{T: 12345, I: 67890}, nil, nil, writeTimestamp, nil},
			},
		},
		{
			"MinKeyEncodeValue",
			ValueEncoderFunc(minKeyEncodeValue),
			[]subtest{
				{
					"wrong type",
					wrong,
					nil,
					nil,
					nothing,
					ValueEncoderError{Name: "MinKeyEncodeValue", Types: []reflect.Type{tMinKey}, Received: reflect.ValueOf(wrong)},
				},
				{"MinKey/success", MinKey{}, nil, nil, writeMinKey, nil},
			},
		},
		{
			"MaxKeyEncodeValue",
			ValueEncoderFunc(maxKeyEncodeValue),
			[]subtest{
				{
					"wrong type",
					wrong,
					nil,
					nil,
					nothing,
					ValueEncoderError{Name: "MaxKeyEncodeValue", Types: []reflect.Type{tMaxKey}, Received: reflect.ValueOf(wrong)},
				},
				{"MaxKey/success", MaxKey{}, nil, nil, writeMaxKey, nil},
			},
		},
		{
			"CoreDocumentEncodeValue",
			ValueEncoderFunc(coreDocumentEncodeValue),
			[]subtest{
				{
					"wrong type",
					wrong,
					nil,
					nil,
					nothing,
					ValueEncoderError{
						Name:     "CoreDocumentEncodeValue",
						Types:    []reflect.Type{tCoreDocument},
						Received: reflect.ValueOf(wrong),
					},
				},
				{
					"WriteDocument Error",
					bsoncore.Document{},
					nil,
					&valueReaderWriter{Err: errors.New("wd error"), ErrAfter: writeDocument},
					writeDocument,
					errors.New("wd error"),
				},
				{
					"bsoncore.Document.Elements Error",
					bsoncore.Document{0xFF, 0x00, 0x00, 0x00, 0x00},
					nil,
					&valueReaderWriter{},
					writeDocument,
					errors.New("length read exceeds number of bytes available. length=5 bytes=255"),
				},
				{
					"WriteDocumentElement Error",
					bsoncore.Document(buildDocument(bsoncore.AppendNullElement(nil, "foo"))),
					nil,
					&valueReaderWriter{Err: errors.New("wde error"), ErrAfter: writeDocumentElement},
					writeDocumentElement,
					errors.New("wde error"),
				},
				{
					"encodeValue error",
					bsoncore.Document(buildDocument(bsoncore.AppendNullElement(nil, "foo"))),
					nil,
					&valueReaderWriter{Err: errors.New("ev error"), ErrAfter: writeNull},
					writeNull,
					errors.New("ev error"),
				},
				{
					"iterator error",
					bsoncore.Document{0x0C, 0x00, 0x00, 0x00, 0x01, 'f', 'o', 'o', 0x00, 0x01, 0x02, 0x03},
					nil,
					&valueReaderWriter{},
					writeDocumentElement,
					errors.New("not enough bytes available to read type. bytes=3 type=double"),
				},
			},
		},
		{
			"StructEncodeValue",
			newStructCodec(&mapCodec{}),
			[]subtest{
				{
					"interface value",
					struct{ Foo myInterface }{Foo: myStruct{1}},
					&EncodeContext{Registry: buildDefaultRegistry()},
					nil,
					writeDocumentEnd,
					nil,
				},
				{
					"nil interface value",
					struct{ Foo myInterface }{Foo: nil},
					&EncodeContext{Registry: buildDefaultRegistry()},
					nil,
					writeDocumentEnd,
					nil,
				},
			},
		},
		{
			"CodeWithScopeEncodeValue",
			ValueEncoderFunc(codeWithScopeEncodeValue),
			[]subtest{
				{
					"wrong type",
					wrong,
					nil,
					nil,
					nothing,
					ValueEncoderError{
						Name:     "CodeWithScopeEncodeValue",
						Types:    []reflect.Type{tCodeWithScope},
						Received: reflect.ValueOf(wrong),
					},
				},
				{
					"WriteCodeWithScope error",
					CodeWithScope{},
					nil,
					&valueReaderWriter{Err: errors.New("wcws error"), ErrAfter: writeCodeWithScope},
					writeCodeWithScope,
					errors.New("wcws error"),
				},
				{
					"CodeWithScope/success",
					CodeWithScope{
						Code:  "var hello = 'world';",
						Scope: D{},
					},
					&EncodeContext{Registry: buildDefaultRegistry()},
					nil, writeDocumentEnd, nil,
				},
			},
		},
		{
			"CoreArrayEncodeValue",
			&arrayCodec{},
			[]subtest{
				{
					"wrong type",
					wrong,
					nil,
					nil,
					nothing,
					ValueEncoderError{
						Name:     "CoreArrayEncodeValue",
						Types:    []reflect.Type{tCoreArray},
						Received: reflect.ValueOf(wrong),
					},
				},

				{
					"WriteArray Error",
					bsoncore.Array{},
					nil,
					&valueReaderWriter{Err: errors.New("wa error"), ErrAfter: writeArray},
					writeArray,
					errors.New("wa error"),
				},
				{
					"WriteArrayElement Error",
					bsoncore.Array(buildDocumentArray(func([]byte) []byte {
						return bsoncore.AppendNullElement(nil, "foo")
					})),
					nil,
					&valueReaderWriter{Err: errors.New("wae error"), ErrAfter: writeArrayElement},
					writeArrayElement,
					errors.New("wae error"),
				},
				{
					"encodeValue error",
					bsoncore.Array(buildDocumentArray(func([]byte) []byte {
						return bsoncore.AppendNullElement(nil, "foo")
					})),
					nil,
					&valueReaderWriter{Err: errors.New("ev error"), ErrAfter: writeNull},
					writeNull,
					errors.New("ev error"),
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			for _, subtest := range tc.subtests {
				t.Run(subtest.name, func(t *testing.T) {
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
				buildDocument(bsoncore.AppendObjectIDElement(nil, "foo", oid)),
				nil,
			},
			{
				"map[string][]int32",
				map[string][]int32{"Z": {1, 2, 3}},
				buildDocumentArray(func(doc []byte) []byte {
					doc = bsoncore.AppendInt32Element(doc, "0", 1)
					doc = bsoncore.AppendInt32Element(doc, "1", 2)
					return bsoncore.AppendInt32Element(doc, "2", 3)
				}),
				nil,
			},
			{
				"map[string][]ObjectID",
				map[string][]ObjectID{"Z": oids},
				buildDocumentArray(func(doc []byte) []byte {
					doc = bsoncore.AppendObjectIDElement(doc, "0", oids[0])
					doc = bsoncore.AppendObjectIDElement(doc, "1", oids[1])
					return bsoncore.AppendObjectIDElement(doc, "2", oids[2])
				}),
				nil,
			},
			{
				"map[string][]json.Number(int64)",
				map[string][]json.Number{"Z": {json.Number("5"), json.Number("10")}},
				buildDocumentArray(func(doc []byte) []byte {
					doc = bsoncore.AppendInt64Element(doc, "0", 5)
					return bsoncore.AppendInt64Element(doc, "1", 10)
				}),
				nil,
			},
			{
				"map[string][]json.Number(float64)",
				map[string][]json.Number{"Z": {json.Number("5"), json.Number("10.1")}},
				buildDocumentArray(func(doc []byte) []byte {
					doc = bsoncore.AppendInt64Element(doc, "0", 5)
					return bsoncore.AppendDoubleElement(doc, "1", 10.1)
				}),
				nil,
			},
			{
				"map[string][]*url.URL",
				map[string][]*url.URL{"Z": {murl}},
				buildDocumentArray(func(doc []byte) []byte {
					return bsoncore.AppendStringElement(doc, "0", murl.String())
				}),
				nil,
			},
			{
				"map[string][]Decimal128",
				map[string][]Decimal128{"Z": {decimal128}},
				buildDocumentArray(func(doc []byte) []byte {
					return bsoncore.AppendDecimal128Element(doc, "0", decimal128.h, decimal128.l)
				}),
				nil,
			},
			{
				"-",
				struct {
					A string `bson:"-"`
				}{
					A: "",
				},
				[]byte{0x05, 0x00, 0x00, 0x00, 0x00},
				nil,
			},
			{
				"omitempty",
				struct {
					A string `bson:",omitempty"`
				}{
					A: "",
				},
				[]byte{0x05, 0x00, 0x00, 0x00, 0x00},
				nil,
			},
			{
				"omitempty, empty time",
				struct {
					A time.Time `bson:",omitempty"`
				}{
					A: time.Time{},
				},
				[]byte{0x05, 0x00, 0x00, 0x00, 0x00},
				nil,
			},
			{
				"omitzero",
				struct {
					A [4]int `bson:",omitzero"`
				}{},
				[]byte{0x05, 0x00, 0x00, 0x00, 0x00},
				nil,
			},
			{
				"omitzero, empty time",
				struct {
					A time.Time `bson:",omitzero"`
				}{
					A: time.Time{},
				},
				[]byte{0x05, 0x00, 0x00, 0x00, 0x00},
				nil,
			},
			{
				"no private fields",
				noPrivateFields{a: "should be empty"},
				[]byte{0x05, 0x00, 0x00, 0x00, 0x00},
				nil,
			},
			{
				"minsize",
				struct {
					A int64 `bson:",minsize"`
				}{
					A: 12345,
				},
				buildDocument(bsoncore.AppendInt32Element(nil, "a", 12345)),
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
				buildDocument(bsoncore.AppendInt32Element(nil, "a", 12345)),
				nil,
			},
			{
				"inline struct pointer",
				struct {
					Foo *struct {
						A int64 `bson:",minsize"`
					} `bson:",inline"`
					Bar *struct {
						B int64
					} `bson:",inline"`
				}{
					Foo: &struct {
						A int64 `bson:",minsize"`
					}{
						A: 12345,
					},
					Bar: nil,
				},
				buildDocument(bsoncore.AppendInt32Element(nil, "a", 12345)),
				nil,
			},
			{
				"nested inline struct pointer",
				struct {
					Foo *struct {
						Bar *struct {
							A int64 `bson:",minsize"`
						} `bson:",inline"`
					} `bson:",inline"`
				}{
					Foo: &struct {
						Bar *struct {
							A int64 `bson:",minsize"`
						} `bson:",inline"`
					}{
						Bar: &struct {
							A int64 `bson:",minsize"`
						}{
							A: 12345,
						},
					},
				},
				buildDocument(bsoncore.AppendInt32Element(nil, "a", 12345)),
				nil,
			},
			{
				"inline nil struct pointer",
				struct {
					Foo *struct {
						A int64 `bson:",minsize"`
					} `bson:",inline"`
				}{
					Foo: nil,
				},
				buildDocument([]byte{}),
				nil,
			},
			{
				"inline overwrite",
				struct {
					Foo struct {
						A int32
						B string
					} `bson:",inline"`
					A int64
				}{
					Foo: struct {
						A int32
						B string
					}{
						A: 0,
						B: "foo",
					},
					A: 54321,
				},
				buildDocument(func(doc []byte) []byte {
					doc = bsoncore.AppendStringElement(doc, "b", "foo")
					doc = bsoncore.AppendInt64Element(doc, "a", 54321)
					return doc
				}(nil)),
				nil,
			},
			{
				"inline overwrite respects ordering",
				struct {
					A   int64
					Foo struct {
						A int32
						B string
					} `bson:",inline"`
				}{
					A: 54321,
					Foo: struct {
						A int32
						B string
					}{
						A: 0,
						B: "foo",
					},
				},
				buildDocument(func(doc []byte) []byte {
					doc = bsoncore.AppendInt64Element(doc, "a", 54321)
					doc = bsoncore.AppendStringElement(doc, "b", "foo")
					return doc
				}(nil)),
				nil,
			},
			{
				"inline overwrite with nested structs",
				struct {
					Foo struct {
						A int32
					} `bson:",inline"`
					Bar struct {
						A int32
					} `bson:",inline"`
					A int64
				}{
					Foo: struct {
						A int32
					}{},
					Bar: struct {
						A int32
					}{},
					A: 54321,
				},
				buildDocument(bsoncore.AppendInt64Element(nil, "a", 54321)),
				nil,
			},
			{
				"inline map",
				struct {
					Foo map[string]string `bson:",inline"`
				}{
					Foo: map[string]string{"foo": "bar"},
				},
				buildDocument(bsoncore.AppendStringElement(nil, "foo", "bar")),
				nil,
			},
			{
				"alternate name bson:name",
				struct {
					A string `bson:"foo"`
				}{
					A: "bar",
				},
				buildDocument(bsoncore.AppendStringElement(nil, "foo", "bar")),
				nil,
			},
			{
				"alternate name",
				struct {
					A string `bson:"foo"`
				}{
					A: "bar",
				},
				buildDocument(bsoncore.AppendStringElement(nil, "foo", "bar")),
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
				buildDocument(bsoncore.AppendStringElement(nil, "a", "bar")),
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
					Q  ObjectID
					T  []struct{}
					Y  json.Number
					Z  time.Time
					AA json.Number
					AB *url.URL
					AC Decimal128
					AD *time.Time
					AE testValueMarshaler
					AF map[string]interface{}
					AG CodeWithScope
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
					Q:  oid,
					T:  nil,
					Y:  json.Number("5"),
					Z:  now,
					AA: json.Number("10.1"),
					AB: murl,
					AC: decimal128,
					AD: &now,
					AE: testValueMarshaler{t: TypeString, buf: bsoncore.AppendString(nil, "hello, world")},
					AF: nil,
					AG: CodeWithScope{Code: "var hello = 'world';", Scope: D{{"pi", 3.14159}}},
				},
				buildDocument(func(doc []byte) []byte {
					doc = bsoncore.AppendBooleanElement(doc, "a", true)
					doc = bsoncore.AppendInt32Element(doc, "b", 123)
					doc = bsoncore.AppendInt64Element(doc, "c", 456)
					doc = bsoncore.AppendInt32Element(doc, "d", 789)
					doc = bsoncore.AppendInt64Element(doc, "e", 101112)
					doc = bsoncore.AppendDoubleElement(doc, "f", 3.14159)
					doc = bsoncore.AppendStringElement(doc, "g", "Hello, world")
					doc = bsoncore.AppendDocumentElement(doc, "h", buildDocument(bsoncore.AppendStringElement(nil, "foo", "bar")))
					doc = bsoncore.AppendBinaryElement(doc, "i", 0x00, []byte{0x01, 0x02, 0x03})
					doc = bsoncore.AppendArrayElement(doc, "k",
						buildArray(bsoncore.AppendStringElement(bsoncore.AppendStringElement(nil, "0", "baz"), "1", "qux")),
					)
					doc = bsoncore.AppendDocumentElement(doc, "l", buildDocument(bsoncore.AppendStringElement(nil, "m", "foobar")))
					doc = bsoncore.AppendObjectIDElement(doc, "q", oid)
					doc = bsoncore.AppendNullElement(doc, "t")
					doc = bsoncore.AppendInt64Element(doc, "y", 5)
					doc = bsoncore.AppendDateTimeElement(doc, "z", now.UnixNano()/int64(time.Millisecond))
					doc = bsoncore.AppendDoubleElement(doc, "aa", 10.1)
					doc = bsoncore.AppendStringElement(doc, "ab", murl.String())
					doc = bsoncore.AppendDecimal128Element(doc, "ac", decimal128.h, decimal128.l)
					doc = bsoncore.AppendDateTimeElement(doc, "ad", now.UnixNano()/int64(time.Millisecond))
					doc = bsoncore.AppendStringElement(doc, "ae", "hello, world")
					doc = bsoncore.AppendNullElement(doc, "af")
					doc = bsoncore.AppendCodeWithScopeElement(doc, "ag",
						"var hello = 'world';", buildDocument(bsoncore.AppendDoubleElement(nil, "pi", 3.14159)),
					)
					return doc
				}(nil)),
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
				},
				buildDocument(func(doc []byte) []byte {
					doc = appendArrayElement(doc, "a", bsoncore.AppendBooleanElement(nil, "0", true))
					doc = appendArrayElement(doc, "b", bsoncore.AppendInt32Element(nil, "0", 123))
					doc = appendArrayElement(doc, "c", bsoncore.AppendInt64Element(nil, "0", 456))
					doc = appendArrayElement(doc, "d", bsoncore.AppendInt32Element(nil, "0", 789))
					doc = appendArrayElement(doc, "e", bsoncore.AppendInt64Element(nil, "0", 101112))
					doc = appendArrayElement(doc, "f", bsoncore.AppendDoubleElement(nil, "0", 3.14159))
					doc = appendArrayElement(doc, "g", bsoncore.AppendStringElement(nil, "0", "Hello, world"))
					doc = appendArrayElement(doc, "h", bsoncore.BuildDocumentElement(nil, "0", bsoncore.AppendStringElement(nil, "foo", "bar")))
					doc = appendArrayElement(doc, "i", bsoncore.AppendBinaryElement(nil, "0", 0x00, []byte{0x01, 0x02, 0x03}))
					doc = appendArrayElement(doc, "k",
						appendArrayElement(nil, "0",
							bsoncore.AppendStringElement(bsoncore.AppendStringElement(nil, "0", "baz"), "1", "qux")),
					)
					doc = appendArrayElement(doc, "l", bsoncore.BuildDocumentElement(nil, "0", bsoncore.AppendStringElement(nil, "m", "foobar")))
					doc = appendArrayElement(doc, "n",
						appendArrayElement(nil, "0",
							bsoncore.AppendStringElement(bsoncore.AppendStringElement(nil, "0", "foo"), "1", "bar")),
					)
					doc = appendArrayElement(doc, "r",
						bsoncore.AppendObjectIDElement(
							bsoncore.AppendObjectIDElement(
								bsoncore.AppendObjectIDElement(nil,
									"0", oids[0]),
								"1", oids[1]),
							"2", oids[2]),
					)
					doc = bsoncore.AppendNullElement(doc, "t")
					doc = bsoncore.AppendNullElement(doc, "w")
					doc = appendArrayElement(doc, "x", nil)
					doc = appendArrayElement(doc, "y", bsoncore.BuildDocumentElement(nil, "0", nil))
					doc = appendArrayElement(doc, "z",
						bsoncore.AppendDateTimeElement(
							bsoncore.AppendDateTimeElement(
								nil, "0", now.UnixNano()/int64(time.Millisecond)),
							"1", now.UnixNano()/int64(time.Millisecond)),
					)
					doc = appendArrayElement(doc, "aa", bsoncore.AppendDoubleElement(bsoncore.AppendInt64Element(nil, "0", 5), "1", 10.10))
					doc = appendArrayElement(doc, "ab", bsoncore.AppendStringElement(nil, "0", murl.String()))
					doc = appendArrayElement(doc, "ac", bsoncore.AppendDecimal128Element(nil, "0", decimal128.h, decimal128.l))
					doc = appendArrayElement(doc, "ad",
						bsoncore.AppendDateTimeElement(
							bsoncore.AppendDateTimeElement(nil, "0", now.UnixNano()/int64(time.Millisecond)),
							"1", now.UnixNano()/int64(time.Millisecond)),
					)
					doc = appendArrayElement(doc, "ae",
						bsoncore.AppendStringElement(bsoncore.AppendStringElement(nil, "0", "hello"), "1", "world"),
					)
					return doc
				}(nil)),
				nil,
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				b := make(sliceWriter, 0, 512)
				vw := NewDocumentWriter(&b)
				reg := buildDefaultRegistry()
				enc, err := reg.LookupEncoder(reflect.TypeOf(tc.value))
				noerr(t, err)
				err = enc.EncodeValue(EncodeContext{Registry: reg}, vw, reflect.ValueOf(tc.value))
				if !errors.Is(err, tc.err) {
					t.Errorf("Did not receive expected error. got %v; want %v", err, tc.err)
				}
				if diff := cmp.Diff([]byte(b), tc.b); diff != "" {
					t.Errorf("Bytes written differ: (-got +want)\n%s", diff)
					t.Errorf("Bytes\ngot: %v\nwant:%v\n", b, tc.b)
					t.Errorf("Readers\ngot: %v\nwant:%v\n", bsoncore.Document(b), bsoncore.Document(tc.b))
				}
			})
		}
	})

	t.Run("error path", func(t *testing.T) {
		testCases := []struct {
			name  string
			value interface{}
			err   error
		}{
			{
				"duplicate name struct",
				struct {
					A int64
					B int64 `bson:"a"`
				}{
					A: 0,
					B: 54321,
				},
				fmt.Errorf("duplicated key a"),
			},
			{
				"inline map",
				struct {
					Foo map[string]string `bson:",inline"`
					Baz string
				}{
					Foo: map[string]string{"baz": "bar"},
					Baz: "hi",
				},
				fmt.Errorf("Key baz of inlined map conflicts with a struct field name"),
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				b := make(sliceWriter, 0, 512)
				vw := NewDocumentWriter(&b)
				reg := buildDefaultRegistry()
				enc, err := reg.LookupEncoder(reflect.TypeOf(tc.value))
				noerr(t, err)
				err = enc.EncodeValue(EncodeContext{Registry: reg}, vw, reflect.ValueOf(tc.value))
				if err == nil || !strings.Contains(err.Error(), tc.err.Error()) {
					t.Errorf("Did not receive expected error. got %v; want %v", err, tc.err)
				}
			})
		}
	})

	t.Run("EmptyInterfaceEncodeValue/nil", func(t *testing.T) {
		val := reflect.New(tEmpty).Elem()
		llvrw := new(valueReaderWriter)
		err := (&emptyInterfaceCodec{}).EncodeValue(EncodeContext{Registry: newTestRegistry()}, llvrw, val)
		noerr(t, err)
		if llvrw.invoked != writeNull {
			t.Errorf("Incorrect method called. got %v; want %v", llvrw.invoked, writeNull)
		}
	})

	t.Run("EmptyInterfaceEncodeValue/LookupEncoder error", func(t *testing.T) {
		val := reflect.New(tEmpty).Elem()
		val.Set(reflect.ValueOf(int64(1234567890)))
		llvrw := new(valueReaderWriter)
		got := (&emptyInterfaceCodec{}).EncodeValue(EncodeContext{Registry: newTestRegistry()}, llvrw, val)
		want := errNoEncoder{Type: tInt64}
		if !assert.CompareErrors(got, want) {
			t.Errorf("Did not receive expected error. got %v; want %v", got, want)
		}
	})
}

type testValueMarshalPtr struct {
	t   Type
	buf []byte
	err error
}

func (tvm *testValueMarshalPtr) MarshalBSONValue() (byte, []byte, error) {
	return byte(tvm.t), tvm.buf, tvm.err
}

type testMarshalPtr struct {
	buf []byte
	err error
}

func (tvm *testMarshalPtr) MarshalBSON() ([]byte, error) {
	return tvm.buf, tvm.err
}
