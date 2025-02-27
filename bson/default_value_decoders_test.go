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

func TestDefaultValueDecoders(t *testing.T) {
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
	type mystring string
	type mystruct struct{}

	const cansetreflectiontest = "cansetreflectiontest"
	const cansettest = "cansettest"

	now := time.Now().Truncate(time.Millisecond)
	d128 := NewDecimal128(12345, 67890)
	var pbool = func(b bool) *bool { return &b }
	var pi32 = func(i32 int32) *int32 { return &i32 }
	var pi64 = func(i64 int64) *int64 { return &i64 }

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
			"BooleanDecodeValue",
			ValueDecoderFunc(booleanDecodeValue),
			[]subtest{
				{
					"wrong type",
					wrong,
					nil,
					&valueReaderWriter{BSONType: TypeBoolean},
					nothing,
					ValueDecoderError{Name: "BooleanDecodeValue", Kinds: []reflect.Kind{reflect.Bool}, Received: reflect.ValueOf(wrong)},
				},
				{
					"type not boolean",
					bool(false),
					nil,
					&valueReaderWriter{BSONType: TypeString},
					nothing,
					fmt.Errorf("cannot decode %v into a boolean", TypeString),
				},
				{
					"fast path",
					bool(true),
					nil,
					&valueReaderWriter{BSONType: TypeBoolean, Return: bool(true)},
					readBoolean,
					nil,
				},
				{
					"reflection path",
					mybool(true),
					nil,
					&valueReaderWriter{BSONType: TypeBoolean, Return: bool(true)},
					readBoolean,
					nil,
				},
				{
					"reflection path error",
					mybool(true),
					nil,
					&valueReaderWriter{BSONType: TypeBoolean, Return: bool(true), Err: errors.New("ReadBoolean Error"), ErrAfter: readBoolean},
					readBoolean, errors.New("ReadBoolean Error"),
				},
				{
					"can set false",
					cansettest,
					nil,
					&valueReaderWriter{BSONType: TypeBoolean},
					nothing,
					ValueDecoderError{Name: "BooleanDecodeValue", Kinds: []reflect.Kind{reflect.Bool}},
				},
				{
					"decode null",
					mybool(false),
					nil,
					&valueReaderWriter{BSONType: TypeNull},
					readNull,
					nil,
				},
				{
					"decode undefined",
					mybool(false),
					nil,
					&valueReaderWriter{BSONType: TypeUndefined},
					readUndefined,
					nil,
				},
			},
		},
		{
			"IntDecodeValue",
			ValueDecoderFunc(intDecodeValue),
			[]subtest{
				{
					"wrong type",
					wrong,
					nil,
					&valueReaderWriter{BSONType: TypeInt32, Return: int32(0)},
					readInt32,
					ValueDecoderError{
						Name:     "IntDecodeValue",
						Kinds:    []reflect.Kind{reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Int},
						Received: reflect.ValueOf(wrong),
					},
				},
				{
					"type not int32/int64",
					0,
					nil,
					&valueReaderWriter{BSONType: TypeString},
					nothing,
					fmt.Errorf("cannot decode %v into an integer type", TypeString),
				},
				{
					"ReadInt32 error",
					0,
					nil,
					&valueReaderWriter{BSONType: TypeInt32, Return: int32(0), Err: errors.New("ReadInt32 error"), ErrAfter: readInt32},
					readInt32,
					errors.New("ReadInt32 error"),
				},
				{
					"ReadInt64 error",
					0,
					nil,
					&valueReaderWriter{BSONType: TypeInt64, Return: int64(0), Err: errors.New("ReadInt64 error"), ErrAfter: readInt64},
					readInt64,
					errors.New("ReadInt64 error"),
				},
				{
					"ReadDouble error",
					0,
					nil,
					&valueReaderWriter{BSONType: TypeDouble, Return: float64(0), Err: errors.New("ReadDouble error"), ErrAfter: readDouble},
					readDouble,
					errors.New("ReadDouble error"),
				},
				{
					"ReadDouble", int64(3), &DecodeContext{},
					&valueReaderWriter{BSONType: TypeDouble, Return: float64(3.00)}, readDouble,
					nil,
				},
				{
					"ReadDouble (truncate)", int64(3), &DecodeContext{truncate: true},
					&valueReaderWriter{BSONType: TypeDouble, Return: float64(3.14)}, readDouble,
					nil,
				},
				{
					"ReadDouble (no truncate)", int64(0), nil,
					&valueReaderWriter{BSONType: TypeDouble, Return: float64(3.14)}, readDouble,
					errCannotTruncate,
				},
				{
					"ReadDouble overflows int64", int64(0), nil,
					&valueReaderWriter{BSONType: TypeDouble, Return: math.MaxFloat64}, readDouble,
					fmt.Errorf("%g overflows int64", math.MaxFloat64),
				},
				{"int8/fast path", int8(127), nil, &valueReaderWriter{BSONType: TypeInt32, Return: int32(127)}, readInt32, nil},
				{"int16/fast path", int16(32676), nil, &valueReaderWriter{BSONType: TypeInt32, Return: int32(32676)}, readInt32, nil},
				{"int32/fast path", int32(1234), nil, &valueReaderWriter{BSONType: TypeInt32, Return: int32(1234)}, readInt32, nil},
				{"int64/fast path", int64(1234), nil, &valueReaderWriter{BSONType: TypeInt64, Return: int64(1234)}, readInt64, nil},
				{"int/fast path", int(1234), nil, &valueReaderWriter{BSONType: TypeInt64, Return: int64(1234)}, readInt64, nil},
				{
					"int8/fast path - nil", (*int8)(nil), nil,
					&valueReaderWriter{BSONType: TypeInt32, Return: int32(0)}, readInt32,
					ValueDecoderError{
						Name:     "IntDecodeValue",
						Kinds:    []reflect.Kind{reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Int},
						Received: reflect.ValueOf((*int8)(nil)),
					},
				},
				{
					"int16/fast path - nil", (*int16)(nil), nil,
					&valueReaderWriter{BSONType: TypeInt32, Return: int32(0)}, readInt32,
					ValueDecoderError{
						Name:     "IntDecodeValue",
						Kinds:    []reflect.Kind{reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Int},
						Received: reflect.ValueOf((*int16)(nil)),
					},
				},
				{
					"int32/fast path - nil", (*int32)(nil), nil,
					&valueReaderWriter{BSONType: TypeInt32, Return: int32(0)}, readInt32,
					ValueDecoderError{
						Name:     "IntDecodeValue",
						Kinds:    []reflect.Kind{reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Int},
						Received: reflect.ValueOf((*int32)(nil)),
					},
				},
				{
					"int64/fast path - nil", (*int64)(nil), nil,
					&valueReaderWriter{BSONType: TypeInt32, Return: int32(0)}, readInt32,
					ValueDecoderError{
						Name:     "IntDecodeValue",
						Kinds:    []reflect.Kind{reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Int},
						Received: reflect.ValueOf((*int64)(nil)),
					},
				},
				{
					"int/fast path - nil", (*int)(nil), nil,
					&valueReaderWriter{BSONType: TypeInt32, Return: int32(0)}, readInt32,
					ValueDecoderError{
						Name:     "IntDecodeValue",
						Kinds:    []reflect.Kind{reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Int},
						Received: reflect.ValueOf((*int)(nil)),
					},
				},
				{
					"int8/fast path - overflow", int8(0), nil,
					&valueReaderWriter{BSONType: TypeInt32, Return: int32(129)}, readInt32,
					fmt.Errorf("%d overflows int8", 129),
				},
				{
					"int16/fast path - overflow", int16(0), nil,
					&valueReaderWriter{BSONType: TypeInt32, Return: int32(32768)}, readInt32,
					fmt.Errorf("%d overflows int16", 32768),
				},
				{
					"int32/fast path - overflow", int32(0), nil,
					&valueReaderWriter{BSONType: TypeInt64, Return: int64(2147483648)}, readInt64,
					fmt.Errorf("%d overflows int32", int64(2147483648)),
				},
				{
					"int8/fast path - overflow (negative)", int8(0), nil,
					&valueReaderWriter{BSONType: TypeInt32, Return: int32(-129)}, readInt32,
					fmt.Errorf("%d overflows int8", -129),
				},
				{
					"int16/fast path - overflow (negative)", int16(0), nil,
					&valueReaderWriter{BSONType: TypeInt32, Return: int32(-32769)}, readInt32,
					fmt.Errorf("%d overflows int16", -32769),
				},
				{
					"int32/fast path - overflow (negative)", int32(0), nil,
					&valueReaderWriter{BSONType: TypeInt64, Return: int64(-2147483649)}, readInt64,
					fmt.Errorf("%d overflows int32", int64(-2147483649)),
				},
				{
					"int8/reflection path", myint8(127), nil,
					&valueReaderWriter{BSONType: TypeInt32, Return: int32(127)}, readInt32,
					nil,
				},
				{
					"int16/reflection path", myint16(255), nil,
					&valueReaderWriter{BSONType: TypeInt32, Return: int32(255)}, readInt32,
					nil,
				},
				{
					"int32/reflection path", myint32(511), nil,
					&valueReaderWriter{BSONType: TypeInt32, Return: int32(511)}, readInt32,
					nil,
				},
				{
					"int64/reflection path", myint64(1023), nil,
					&valueReaderWriter{BSONType: TypeInt32, Return: int32(1023)}, readInt32,
					nil,
				},
				{
					"int/reflection path", myint(2047), nil,
					&valueReaderWriter{BSONType: TypeInt32, Return: int32(2047)}, readInt32,
					nil,
				},
				{
					"int8/reflection path - overflow", myint8(0), nil,
					&valueReaderWriter{BSONType: TypeInt32, Return: int32(129)}, readInt32,
					fmt.Errorf("%d overflows int8", 129),
				},
				{
					"int16/reflection path - overflow", myint16(0), nil,
					&valueReaderWriter{BSONType: TypeInt32, Return: int32(32768)}, readInt32,
					fmt.Errorf("%d overflows int16", 32768),
				},
				{
					"int32/reflection path - overflow", myint32(0), nil,
					&valueReaderWriter{BSONType: TypeInt64, Return: int64(2147483648)}, readInt64,
					fmt.Errorf("%d overflows int32", int64(2147483648)),
				},
				{
					"int8/reflection path - overflow (negative)", myint8(0), nil,
					&valueReaderWriter{BSONType: TypeInt32, Return: int32(-129)}, readInt32,
					fmt.Errorf("%d overflows int8", -129),
				},
				{
					"int16/reflection path - overflow (negative)", myint16(0), nil,
					&valueReaderWriter{BSONType: TypeInt32, Return: int32(-32769)}, readInt32,
					fmt.Errorf("%d overflows int16", -32769),
				},
				{
					"int32/reflection path - overflow (negative)", myint32(0), nil,
					&valueReaderWriter{BSONType: TypeInt64, Return: int64(-2147483649)}, readInt64,
					fmt.Errorf("%d overflows int32", int64(-2147483649)),
				},
				{
					"can set false",
					cansettest,
					nil,
					&valueReaderWriter{BSONType: TypeInt32, Return: int32(0)},
					nothing,
					ValueDecoderError{
						Name:  "IntDecodeValue",
						Kinds: []reflect.Kind{reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Int},
					},
				},
				{
					"decode null",
					myint(0),
					nil,
					&valueReaderWriter{BSONType: TypeNull},
					readNull,
					nil,
				},
				{
					"decode undefined",
					myint(0),
					nil,
					&valueReaderWriter{BSONType: TypeUndefined},
					readUndefined,
					nil,
				},
			},
		},
		{
			"defaultUIntCodec.DecodeValue",
			&uintCodec{},
			[]subtest{
				{
					"wrong type",
					wrong,
					nil,
					&valueReaderWriter{BSONType: TypeInt32, Return: int32(0)},
					readInt32,
					ValueDecoderError{
						Name:     "UintDecodeValue",
						Kinds:    []reflect.Kind{reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uint},
						Received: reflect.ValueOf(wrong),
					},
				},
				{
					"type not int32/int64",
					0,
					nil,
					&valueReaderWriter{BSONType: TypeString},
					nothing,
					fmt.Errorf("cannot decode %v into an integer type", TypeString),
				},
				{
					"ReadInt32 error",
					uint(0),
					nil,
					&valueReaderWriter{BSONType: TypeInt32, Return: int32(0), Err: errors.New("ReadInt32 error"), ErrAfter: readInt32},
					readInt32,
					errors.New("ReadInt32 error"),
				},
				{
					"ReadInt64 error",
					uint(0),
					nil,
					&valueReaderWriter{BSONType: TypeInt64, Return: int64(0), Err: errors.New("ReadInt64 error"), ErrAfter: readInt64},
					readInt64,
					errors.New("ReadInt64 error"),
				},
				{
					"ReadDouble error",
					0,
					nil,
					&valueReaderWriter{BSONType: TypeDouble, Return: float64(0), Err: errors.New("ReadDouble error"), ErrAfter: readDouble},
					readDouble,
					errors.New("ReadDouble error"),
				},
				{
					"ReadDouble", uint64(3), &DecodeContext{},
					&valueReaderWriter{BSONType: TypeDouble, Return: float64(3.00)}, readDouble,
					nil,
				},
				{
					"ReadDouble (truncate)", uint64(3), &DecodeContext{truncate: true},
					&valueReaderWriter{BSONType: TypeDouble, Return: float64(3.14)}, readDouble,
					nil,
				},
				{
					"ReadDouble (no truncate)", uint64(0), nil,
					&valueReaderWriter{BSONType: TypeDouble, Return: float64(3.14)}, readDouble,
					errCannotTruncate,
				},
				{
					"ReadDouble overflows int64", uint64(0), nil,
					&valueReaderWriter{BSONType: TypeDouble, Return: math.MaxFloat64}, readDouble,
					fmt.Errorf("%g overflows int64", math.MaxFloat64),
				},
				{"uint8/fast path", uint8(127), nil, &valueReaderWriter{BSONType: TypeInt32, Return: int32(127)}, readInt32, nil},
				{"uint16/fast path", uint16(255), nil, &valueReaderWriter{BSONType: TypeInt32, Return: int32(255)}, readInt32, nil},
				{"uint32/fast path", uint32(1234), nil, &valueReaderWriter{BSONType: TypeInt32, Return: int32(1234)}, readInt32, nil},
				{"uint64/fast path", uint64(1234), nil, &valueReaderWriter{BSONType: TypeInt64, Return: int64(1234)}, readInt64, nil},
				{"uint/fast path", uint(1234), nil, &valueReaderWriter{BSONType: TypeInt64, Return: int64(1234)}, readInt64, nil},
				{
					"uint8/fast path - nil", (*uint8)(nil), nil,
					&valueReaderWriter{BSONType: TypeInt32, Return: int32(0)}, readInt32,
					ValueDecoderError{
						Name:     "UintDecodeValue",
						Kinds:    []reflect.Kind{reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uint},
						Received: reflect.ValueOf((*uint8)(nil)),
					},
				},
				{
					"uint16/fast path - nil", (*uint16)(nil), nil,
					&valueReaderWriter{BSONType: TypeInt32, Return: int32(0)}, readInt32,
					ValueDecoderError{
						Name:     "UintDecodeValue",
						Kinds:    []reflect.Kind{reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uint},
						Received: reflect.ValueOf((*uint16)(nil)),
					},
				},
				{
					"uint32/fast path - nil", (*uint32)(nil), nil,
					&valueReaderWriter{BSONType: TypeInt32, Return: int32(0)}, readInt32,
					ValueDecoderError{
						Name:     "UintDecodeValue",
						Kinds:    []reflect.Kind{reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uint},
						Received: reflect.ValueOf((*uint32)(nil)),
					},
				},
				{
					"uint64/fast path - nil", (*uint64)(nil), nil,
					&valueReaderWriter{BSONType: TypeInt32, Return: int32(0)}, readInt32,
					ValueDecoderError{
						Name:     "UintDecodeValue",
						Kinds:    []reflect.Kind{reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uint},
						Received: reflect.ValueOf((*uint64)(nil)),
					},
				},
				{
					"uint/fast path - nil", (*uint)(nil), nil,
					&valueReaderWriter{BSONType: TypeInt32, Return: int32(0)}, readInt32,
					ValueDecoderError{
						Name:     "UintDecodeValue",
						Kinds:    []reflect.Kind{reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uint},
						Received: reflect.ValueOf((*uint)(nil)),
					},
				},
				{
					"uint8/fast path - overflow", uint8(0), nil,
					&valueReaderWriter{BSONType: TypeInt32, Return: int32(1 << 8)}, readInt32,
					fmt.Errorf("%d overflows uint8", 1<<8),
				},
				{
					"uint16/fast path - overflow", uint16(0), nil,
					&valueReaderWriter{BSONType: TypeInt32, Return: int32(1 << 16)}, readInt32,
					fmt.Errorf("%d overflows uint16", 1<<16),
				},
				{
					"uint32/fast path - overflow", uint32(0), nil,
					&valueReaderWriter{BSONType: TypeInt64, Return: int64(1 << 32)}, readInt64,
					fmt.Errorf("%d overflows uint32", int64(1<<32)),
				},
				{
					"uint8/fast path - overflow (negative)", uint8(0), nil,
					&valueReaderWriter{BSONType: TypeInt32, Return: int32(-1)}, readInt32,
					fmt.Errorf("%d overflows uint8", -1),
				},
				{
					"uint16/fast path - overflow (negative)", uint16(0), nil,
					&valueReaderWriter{BSONType: TypeInt32, Return: int32(-1)}, readInt32,
					fmt.Errorf("%d overflows uint16", -1),
				},
				{
					"uint32/fast path - overflow (negative)", uint32(0), nil,
					&valueReaderWriter{BSONType: TypeInt64, Return: int64(-1)}, readInt64,
					fmt.Errorf("%d overflows uint32", -1),
				},
				{
					"uint64/fast path - overflow (negative)", uint64(0), nil,
					&valueReaderWriter{BSONType: TypeInt64, Return: int64(-1)}, readInt64,
					fmt.Errorf("%d overflows uint64", -1),
				},
				{
					"uint/fast path - overflow (negative)", uint(0), nil,
					&valueReaderWriter{BSONType: TypeInt64, Return: int64(-1)}, readInt64,
					fmt.Errorf("%d overflows uint", -1),
				},
				{
					"uint8/reflection path", myuint8(127), nil,
					&valueReaderWriter{BSONType: TypeInt32, Return: int32(127)}, readInt32,
					nil,
				},
				{
					"uint16/reflection path", myuint16(255), nil,
					&valueReaderWriter{BSONType: TypeInt32, Return: int32(255)}, readInt32,
					nil,
				},
				{
					"uint32/reflection path", myuint32(511), nil,
					&valueReaderWriter{BSONType: TypeInt32, Return: int32(511)}, readInt32,
					nil,
				},
				{
					"uint64/reflection path", myuint64(1023), nil,
					&valueReaderWriter{BSONType: TypeInt32, Return: int32(1023)}, readInt32,
					nil,
				},
				{
					"uint/reflection path", myuint(2047), nil,
					&valueReaderWriter{BSONType: TypeInt32, Return: int32(2047)}, readInt32,
					nil,
				},
				{
					"uint8/reflection path - overflow", myuint8(0), nil,
					&valueReaderWriter{BSONType: TypeInt32, Return: int32(1 << 8)}, readInt32,
					fmt.Errorf("%d overflows uint8", 1<<8),
				},
				{
					"uint16/reflection path - overflow", myuint16(0), nil,
					&valueReaderWriter{BSONType: TypeInt32, Return: int32(1 << 16)}, readInt32,
					fmt.Errorf("%d overflows uint16", 1<<16),
				},
				{
					"uint32/reflection path - overflow", myuint32(0), nil,
					&valueReaderWriter{BSONType: TypeInt64, Return: int64(1 << 32)}, readInt64,
					fmt.Errorf("%d overflows uint32", int64(1<<32)),
				},
				{
					"uint8/reflection path - overflow (negative)", myuint8(0), nil,
					&valueReaderWriter{BSONType: TypeInt32, Return: int32(-1)}, readInt32,
					fmt.Errorf("%d overflows uint8", -1),
				},
				{
					"uint16/reflection path - overflow (negative)", myuint16(0), nil,
					&valueReaderWriter{BSONType: TypeInt32, Return: int32(-1)}, readInt32,
					fmt.Errorf("%d overflows uint16", -1),
				},
				{
					"uint32/reflection path - overflow (negative)", myuint32(0), nil,
					&valueReaderWriter{BSONType: TypeInt64, Return: int64(-1)}, readInt64,
					fmt.Errorf("%d overflows uint32", -1),
				},
				{
					"uint64/reflection path - overflow (negative)", myuint64(0), nil,
					&valueReaderWriter{BSONType: TypeInt64, Return: int64(-1)}, readInt64,
					fmt.Errorf("%d overflows uint64", -1),
				},
				{
					"uint/reflection path - overflow (negative)", myuint(0), nil,
					&valueReaderWriter{BSONType: TypeInt64, Return: int64(-1)}, readInt64,
					fmt.Errorf("%d overflows uint", -1),
				},
				{
					"can set false",
					cansettest,
					nil,
					&valueReaderWriter{BSONType: TypeInt32, Return: int32(0)},
					nothing,
					ValueDecoderError{
						Name:  "UintDecodeValue",
						Kinds: []reflect.Kind{reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uint},
					},
				},
			},
		},
		{
			"FloatDecodeValue",
			ValueDecoderFunc(floatDecodeValue),
			[]subtest{
				{
					"wrong type",
					wrong,
					nil,
					&valueReaderWriter{BSONType: TypeDouble, Return: float64(0)},
					readDouble,
					ValueDecoderError{
						Name:     "FloatDecodeValue",
						Kinds:    []reflect.Kind{reflect.Float32, reflect.Float64},
						Received: reflect.ValueOf(wrong),
					},
				},
				{
					"type not double",
					0,
					nil,
					&valueReaderWriter{BSONType: TypeString},
					nothing,
					fmt.Errorf("cannot decode %v into a float32 or float64 type", TypeString),
				},
				{
					"ReadDouble error",
					float64(0),
					nil,
					&valueReaderWriter{BSONType: TypeDouble, Return: float64(0), Err: errors.New("ReadDouble error"), ErrAfter: readDouble},
					readDouble,
					errors.New("ReadDouble error"),
				},
				{
					"ReadInt32 error",
					float64(0),
					nil,
					&valueReaderWriter{BSONType: TypeInt32, Return: int32(0), Err: errors.New("ReadInt32 error"), ErrAfter: readInt32},
					readInt32,
					errors.New("ReadInt32 error"),
				},
				{
					"ReadInt64 error",
					float64(0),
					nil,
					&valueReaderWriter{BSONType: TypeInt64, Return: int64(0), Err: errors.New("ReadInt64 error"), ErrAfter: readInt64},
					readInt64,
					errors.New("ReadInt64 error"),
				},
				{
					"float64/int32", float32(32.0), nil,
					&valueReaderWriter{BSONType: TypeInt32, Return: int32(32)}, readInt32,
					nil,
				},
				{
					"float64/int64", float32(64.0), nil,
					&valueReaderWriter{BSONType: TypeInt64, Return: int64(64)}, readInt64,
					nil,
				},
				{
					"float32/fast path (equal)", float32(3.0), nil,
					&valueReaderWriter{BSONType: TypeDouble, Return: float64(3.0)}, readDouble,
					nil,
				},
				{
					"float64/fast path", float64(3.14159), nil,
					&valueReaderWriter{BSONType: TypeDouble, Return: float64(3.14159)}, readDouble,
					nil,
				},
				{
					"float32/fast path (truncate)", float32(3.14), &DecodeContext{truncate: true},
					&valueReaderWriter{BSONType: TypeDouble, Return: float64(3.14)}, readDouble,
					nil,
				},
				{
					"float32/fast path (no truncate)", float32(0), nil,
					&valueReaderWriter{BSONType: TypeDouble, Return: float64(3.14)}, readDouble,
					errCannotTruncate,
				},
				{
					"float32/fast path - nil", (*float32)(nil), nil,
					&valueReaderWriter{BSONType: TypeDouble, Return: float64(0)}, readDouble,
					ValueDecoderError{
						Name:     "FloatDecodeValue",
						Kinds:    []reflect.Kind{reflect.Float32, reflect.Float64},
						Received: reflect.ValueOf((*float32)(nil)),
					},
				},
				{
					"float64/fast path - nil", (*float64)(nil), nil,
					&valueReaderWriter{BSONType: TypeDouble, Return: float64(0)}, readDouble,
					ValueDecoderError{
						Name:     "FloatDecodeValue",
						Kinds:    []reflect.Kind{reflect.Float32, reflect.Float64},
						Received: reflect.ValueOf((*float64)(nil)),
					},
				},
				{
					"float32/reflection path (equal)", myfloat32(3.0), nil,
					&valueReaderWriter{BSONType: TypeDouble, Return: float64(3.0)}, readDouble,
					nil,
				},
				{
					"float64/reflection path", myfloat64(3.14159), nil,
					&valueReaderWriter{BSONType: TypeDouble, Return: float64(3.14159)}, readDouble,
					nil,
				},
				{
					"float32/reflection path (truncate)", myfloat32(3.14), &DecodeContext{truncate: true},
					&valueReaderWriter{BSONType: TypeDouble, Return: float64(3.14)}, readDouble,
					nil,
				},
				{
					"float32/reflection path (no truncate)", myfloat32(0), nil,
					&valueReaderWriter{BSONType: TypeDouble, Return: float64(3.14)}, readDouble,
					errCannotTruncate,
				},
				{
					"can set false",
					cansettest,
					nil,
					&valueReaderWriter{BSONType: TypeDouble, Return: float64(0)},
					nothing,
					ValueDecoderError{
						Name:  "FloatDecodeValue",
						Kinds: []reflect.Kind{reflect.Float32, reflect.Float64},
					},
				},
			},
		},
		{
			"defaultTimeCodec.DecodeValue",
			&timeCodec{},
			[]subtest{
				{
					"wrong type",
					wrong,
					nil,
					&valueReaderWriter{BSONType: TypeDateTime, Return: int64(0)},
					nothing,
					ValueDecoderError{Name: "TimeDecodeValue", Types: []reflect.Type{tTime}, Received: reflect.ValueOf(wrong)},
				},
				{
					"ReadDateTime error",
					time.Time{},
					nil,
					&valueReaderWriter{BSONType: TypeDateTime, Return: int64(0), Err: errors.New("ReadDateTime error"), ErrAfter: readDateTime},
					readDateTime,
					errors.New("ReadDateTime error"),
				},
				{
					"time.Time",
					now,
					nil,
					&valueReaderWriter{BSONType: TypeDateTime, Return: now.UnixNano() / int64(time.Millisecond)},
					readDateTime,
					nil,
				},
				{
					"can set false",
					cansettest,
					nil,
					&valueReaderWriter{BSONType: TypeDateTime, Return: int64(0)},
					nothing,
					ValueDecoderError{Name: "TimeDecodeValue", Types: []reflect.Type{tTime}},
				},
				{
					"decode null",
					time.Time{},
					nil,
					&valueReaderWriter{BSONType: TypeNull},
					readNull,
					nil,
				},
				{
					"decode undefined",
					time.Time{},
					nil,
					&valueReaderWriter{BSONType: TypeUndefined},
					readUndefined,
					nil,
				},
			},
		},
		{
			"defaultMapCodec.DecodeValue",
			&mapCodec{},
			[]subtest{
				{
					"wrong kind",
					wrong,
					nil,
					&valueReaderWriter{},
					nothing,
					ValueDecoderError{Name: "MapDecodeValue", Kinds: []reflect.Kind{reflect.Map}, Received: reflect.ValueOf(wrong)},
				},
				{
					"wrong kind (non-string key)",
					map[bool]interface{}{},
					&DecodeContext{Registry: buildDefaultRegistry()},
					&valueReaderWriter{},
					readElement,
					fmt.Errorf("unsupported key type: %T", false),
				},
				{
					"ReadDocument Error",
					make(map[string]interface{}),
					nil,
					&valueReaderWriter{Err: errors.New("rd error"), ErrAfter: readDocument},
					readDocument,
					errors.New("rd error"),
				},
				{
					"Lookup Error",
					map[string]string{},
					&DecodeContext{Registry: newTestRegistry()},
					&valueReaderWriter{},
					readDocument,
					errNoDecoder{Type: reflect.TypeOf("")},
				},
				{
					"ReadElement Error",
					make(map[string]interface{}),
					&DecodeContext{Registry: buildDefaultRegistry()},
					&valueReaderWriter{Err: errors.New("re error"), ErrAfter: readElement},
					readElement,
					errors.New("re error"),
				},
				{
					"can set false",
					cansettest,
					nil,
					&valueReaderWriter{},
					nothing,
					ValueDecoderError{Name: "MapDecodeValue", Kinds: []reflect.Kind{reflect.Map}},
				},
				{
					"wrong BSON type",
					map[string]interface{}{},
					nil,
					&valueReaderWriter{BSONType: TypeString},
					nothing,
					errors.New("cannot decode string into a map[string]interface {}"),
				},
				{
					"decode null",
					(map[string]interface{})(nil),
					nil,
					&valueReaderWriter{BSONType: TypeNull},
					readNull,
					nil,
				},
				{
					"decode undefined",
					(map[string]interface{})(nil),
					nil,
					&valueReaderWriter{BSONType: TypeUndefined},
					readUndefined,
					nil,
				},
			},
		},
		{
			"ArrayDecodeValue",
			ValueDecoderFunc(arrayDecodeValue),
			[]subtest{
				{
					"wrong kind",
					wrong,
					nil,
					&valueReaderWriter{},
					nothing,
					ValueDecoderError{Name: "ArrayDecodeValue", Kinds: []reflect.Kind{reflect.Array}, Received: reflect.ValueOf(wrong)},
				},
				{
					"can set false",
					cansettest,
					nil,
					&valueReaderWriter{},
					nothing,
					ValueDecoderError{Name: "ArrayDecodeValue", Kinds: []reflect.Kind{reflect.Array}},
				},
				{
					"Not Type Array",
					[1]interface{}{},
					nil,
					&valueReaderWriter{BSONType: TypeString},
					nothing,
					errors.New("cannot decode string into an array"),
				},
				{
					"ReadArray Error",
					[1]interface{}{},
					nil,
					&valueReaderWriter{Err: errors.New("ra error"), ErrAfter: readArray, BSONType: TypeArray},
					readArray,
					errors.New("ra error"),
				},
				{
					"Lookup Error",
					[1]string{},
					&DecodeContext{Registry: newTestRegistry()},
					&valueReaderWriter{BSONType: TypeArray},
					readArray,
					errNoDecoder{Type: reflect.TypeOf("")},
				},
				{
					"ReadValue Error",
					[1]string{},
					&DecodeContext{Registry: buildDefaultRegistry()},
					&valueReaderWriter{Err: errors.New("rv error"), ErrAfter: readValue, BSONType: TypeArray},
					readValue,
					errors.New("rv error"),
				},
				{
					"DecodeValue Error",
					[1]string{},
					&DecodeContext{Registry: buildDefaultRegistry()},
					&valueReaderWriter{BSONType: TypeArray},
					readValue,
					&DecodeError{keys: []string{"0"}, wrapped: errors.New("cannot decode array into a string type")},
				},
				{
					"Document but not D",
					[1]string{},
					nil,
					&valueReaderWriter{BSONType: Type(0)},
					nothing,
					errors.New("cannot decode document into [1]string"),
				},
				{
					"EmbeddedDocument but not D",
					[1]string{},
					nil,
					&valueReaderWriter{BSONType: TypeEmbeddedDocument},
					nothing,
					errors.New("cannot decode document into [1]string"),
				},
				{
					"decode null",
					[1]string{},
					nil,
					&valueReaderWriter{BSONType: TypeNull},
					readNull,
					nil,
				},
				{
					"decode undefined",
					[1]string{},
					nil,
					&valueReaderWriter{BSONType: TypeUndefined},
					readUndefined,
					nil,
				},
			},
		},
		{
			"defaultSliceCodec.DecodeValue",
			&sliceCodec{},
			[]subtest{
				{
					"wrong kind",
					wrong,
					nil,
					&valueReaderWriter{},
					nothing,
					ValueDecoderError{Name: "SliceDecodeValue", Kinds: []reflect.Kind{reflect.Slice}, Received: reflect.ValueOf(wrong)},
				},
				{
					"can set false",
					cansettest,
					nil,
					&valueReaderWriter{},
					nothing,
					ValueDecoderError{Name: "SliceDecodeValue", Kinds: []reflect.Kind{reflect.Slice}},
				},
				{
					"Not Type Array",
					[]interface{}{},
					nil,
					&valueReaderWriter{BSONType: TypeInt32},
					nothing,
					errors.New("cannot decode 32-bit integer into a slice"),
				},
				{
					"ReadArray Error",
					[]interface{}{},
					nil,
					&valueReaderWriter{Err: errors.New("ra error"), ErrAfter: readArray, BSONType: TypeArray},
					readArray,
					errors.New("ra error"),
				},
				{
					"Lookup Error",
					[]string{},
					&DecodeContext{Registry: newTestRegistry()},
					&valueReaderWriter{BSONType: TypeArray},
					readArray,
					errNoDecoder{Type: reflect.TypeOf("")},
				},
				{
					"ReadValue Error",
					[]string{},
					&DecodeContext{Registry: buildDefaultRegistry()},
					&valueReaderWriter{Err: errors.New("rv error"), ErrAfter: readValue, BSONType: TypeArray},
					readValue,
					errors.New("rv error"),
				},
				{
					"DecodeValue Error",
					[]string{},
					&DecodeContext{Registry: buildDefaultRegistry()},
					&valueReaderWriter{BSONType: TypeArray},
					readValue,
					&DecodeError{keys: []string{"0"}, wrapped: errors.New("cannot decode array into a string type")},
				},
				{
					"Document but not D",
					[]string{},
					nil,
					&valueReaderWriter{BSONType: Type(0)},
					nothing,
					errors.New("cannot decode document into []string"),
				},
				{
					"EmbeddedDocument but not D",
					[]string{},
					nil,
					&valueReaderWriter{BSONType: TypeEmbeddedDocument},
					nothing,
					errors.New("cannot decode document into []string"),
				},
				{
					"decode null",
					([]string)(nil),
					nil,
					&valueReaderWriter{BSONType: TypeNull},
					readNull,
					nil,
				},
				{
					"decode undefined",
					([]string)(nil),
					nil,
					&valueReaderWriter{BSONType: TypeUndefined},
					readUndefined,
					nil,
				},
			},
		},
		{
			"ObjectIDDecodeValue",
			ValueDecoderFunc(objectIDDecodeValue),
			[]subtest{
				{
					"wrong type",
					wrong,
					nil,
					&valueReaderWriter{BSONType: TypeObjectID},
					nothing,
					ValueDecoderError{Name: "ObjectIDDecodeValue", Types: []reflect.Type{tOID}, Received: reflect.ValueOf(wrong)},
				},
				{
					"type not objectID",
					ObjectID{},
					nil,
					&valueReaderWriter{BSONType: TypeInt32},
					nothing,
					fmt.Errorf("cannot decode %v into an ObjectID", TypeInt32),
				},
				{
					"ReadObjectID Error",
					ObjectID{},
					nil,
					&valueReaderWriter{BSONType: TypeObjectID, Err: errors.New("roid error"), ErrAfter: readObjectID},
					readObjectID,
					errors.New("roid error"),
				},
				{
					"can set false",
					cansettest,
					nil,
					&valueReaderWriter{BSONType: TypeObjectID, Return: ObjectID{}},
					nothing,
					ValueDecoderError{Name: "ObjectIDDecodeValue", Types: []reflect.Type{tOID}},
				},
				{
					"success",
					ObjectID{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C},
					nil,
					&valueReaderWriter{
						BSONType: TypeObjectID,
						Return:   ObjectID{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C},
					},
					readObjectID,
					nil,
				},
				{
					"success/string",
					ObjectID{0x30, 0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37, 0x38, 0x39, 0x61, 0x62},
					nil,
					&valueReaderWriter{
						BSONType: TypeString,
						Return:   "0123456789ab",
					},
					readString,
					nil,
				},
				{
					"success/string-hex",
					ObjectID{0x30, 0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37, 0x38, 0x39, 0x61, 0x62},
					nil,
					&valueReaderWriter{
						BSONType: TypeString,
						Return:   "303132333435363738396162",
					},
					readString,
					nil,
				},
				{
					"decode null",
					ObjectID{},
					nil,
					&valueReaderWriter{BSONType: TypeNull},
					readNull,
					nil,
				},
				{
					"decode undefined",
					ObjectID{},
					nil,
					&valueReaderWriter{BSONType: TypeUndefined},
					readUndefined,
					nil,
				},
			},
		},
		{
			"Decimal128DecodeValue",
			ValueDecoderFunc(decimal128DecodeValue),
			[]subtest{
				{
					"wrong type",
					wrong,
					nil,
					&valueReaderWriter{BSONType: TypeDecimal128},
					nothing,
					ValueDecoderError{Name: "Decimal128DecodeValue", Types: []reflect.Type{tDecimal}, Received: reflect.ValueOf(wrong)},
				},
				{
					"type not decimal128",
					Decimal128{},
					nil,
					&valueReaderWriter{BSONType: TypeString},
					nothing,
					fmt.Errorf("cannot decode %v into a Decimal128", TypeString),
				},
				{
					"ReadDecimal128 Error",
					Decimal128{},
					nil,
					&valueReaderWriter{BSONType: TypeDecimal128, Err: errors.New("rd128 error"), ErrAfter: readDecimal128},
					readDecimal128,
					errors.New("rd128 error"),
				},
				{
					"can set false",
					cansettest,
					nil,
					&valueReaderWriter{BSONType: TypeDecimal128, Return: d128},
					nothing,
					ValueDecoderError{Name: "Decimal128DecodeValue", Types: []reflect.Type{tDecimal}},
				},
				{
					"success",
					d128,
					nil,
					&valueReaderWriter{BSONType: TypeDecimal128, Return: d128},
					readDecimal128,
					nil,
				},
				{
					"decode null",
					Decimal128{},
					nil,
					&valueReaderWriter{BSONType: TypeNull},
					readNull,
					nil,
				},
				{
					"decode undefined",
					Decimal128{},
					nil,
					&valueReaderWriter{BSONType: TypeUndefined},
					readUndefined,
					nil,
				},
			},
		},
		{
			"JSONNumberDecodeValue",
			ValueDecoderFunc(jsonNumberDecodeValue),
			[]subtest{
				{
					"wrong type",
					wrong,
					nil,
					&valueReaderWriter{BSONType: TypeObjectID},
					nothing,
					ValueDecoderError{Name: "JSONNumberDecodeValue", Types: []reflect.Type{tJSONNumber}, Received: reflect.ValueOf(wrong)},
				},
				{
					"type not double/int32/int64",
					json.Number(""),
					nil,
					&valueReaderWriter{BSONType: TypeString},
					nothing,
					fmt.Errorf("cannot decode %v into a json.Number", TypeString),
				},
				{
					"ReadDouble Error",
					json.Number(""),
					nil,
					&valueReaderWriter{BSONType: TypeDouble, Err: errors.New("rd error"), ErrAfter: readDouble},
					readDouble,
					errors.New("rd error"),
				},
				{
					"ReadInt32 Error",
					json.Number(""),
					nil,
					&valueReaderWriter{BSONType: TypeInt32, Err: errors.New("ri32 error"), ErrAfter: readInt32},
					readInt32,
					errors.New("ri32 error"),
				},
				{
					"ReadInt64 Error",
					json.Number(""),
					nil,
					&valueReaderWriter{BSONType: TypeInt64, Err: errors.New("ri64 error"), ErrAfter: readInt64},
					readInt64,
					errors.New("ri64 error"),
				},
				{
					"can set false",
					cansettest,
					nil,
					&valueReaderWriter{BSONType: TypeObjectID, Return: ObjectID{}},
					nothing,
					ValueDecoderError{Name: "JSONNumberDecodeValue", Types: []reflect.Type{tJSONNumber}},
				},
				{
					"success/double",
					json.Number("3.14159"),
					nil,
					&valueReaderWriter{BSONType: TypeDouble, Return: float64(3.14159)},
					readDouble,
					nil,
				},
				{
					"success/int32",
					json.Number("12345"),
					nil,
					&valueReaderWriter{BSONType: TypeInt32, Return: int32(12345)},
					readInt32,
					nil,
				},
				{
					"success/int64",
					json.Number("1234567890"),
					nil,
					&valueReaderWriter{BSONType: TypeInt64, Return: int64(1234567890)},
					readInt64,
					nil,
				},
				{
					"decode null",
					json.Number(""),
					nil,
					&valueReaderWriter{BSONType: TypeNull},
					readNull,
					nil,
				},
				{
					"decode undefined",
					json.Number(""),
					nil,
					&valueReaderWriter{BSONType: TypeUndefined},
					readUndefined,
					nil,
				},
			},
		},
		{
			"URLDecodeValue",
			ValueDecoderFunc(urlDecodeValue),
			[]subtest{
				{
					"wrong type",
					url.URL{},
					nil,
					&valueReaderWriter{BSONType: TypeInt32},
					nothing,
					fmt.Errorf("cannot decode %v into a *url.URL", TypeInt32),
				},
				{
					"type not *url.URL",
					int64(0),
					nil,
					&valueReaderWriter{BSONType: TypeString, Return: "http://example.com"},
					nothing,
					ValueDecoderError{Name: "URLDecodeValue", Types: []reflect.Type{tURL}, Received: reflect.ValueOf(int64(0))},
				},
				{
					"ReadString error",
					url.URL{},
					nil,
					&valueReaderWriter{BSONType: TypeString, Err: errors.New("rs error"), ErrAfter: readString},
					readString,
					errors.New("rs error"),
				},
				{
					"url.Parse error",
					url.URL{},
					nil,
					&valueReaderWriter{BSONType: TypeString, Return: "not-valid-%%%%://"},
					readString,
					&url.Error{
						Op:  "parse",
						URL: "not-valid-%%%%://",
						Err: errors.New("first path segment in URL cannot contain colon"),
					},
				},
				{
					"can set false",
					cansettest,
					nil,
					&valueReaderWriter{BSONType: TypeString, Return: "http://example.com"},
					nothing,
					ValueDecoderError{Name: "URLDecodeValue", Types: []reflect.Type{tURL}},
				},
				{
					"url.URL",
					url.URL{Scheme: "http", Host: "example.com"},
					nil,
					&valueReaderWriter{BSONType: TypeString, Return: "http://example.com"},
					readString,
					nil,
				},
				{
					"decode null",
					url.URL{},
					nil,
					&valueReaderWriter{BSONType: TypeNull},
					readNull,
					nil,
				},
				{
					"decode undefined",
					url.URL{},
					nil,
					&valueReaderWriter{BSONType: TypeUndefined},
					readUndefined,
					nil,
				},
			},
		},
		{
			"defaultByteSliceCodec.DecodeValue",
			&byteSliceCodec{},
			[]subtest{
				{
					"wrong type",
					[]byte{},
					nil,
					&valueReaderWriter{BSONType: TypeInt32},
					nothing,
					fmt.Errorf("cannot decode %v into a []byte", TypeInt32),
				},
				{
					"type not []byte",
					int64(0),
					nil,
					&valueReaderWriter{BSONType: TypeBinary, Return: bsoncore.Value{Type: bsoncore.TypeBinary}},
					nothing,
					ValueDecoderError{Name: "ByteSliceDecodeValue", Types: []reflect.Type{tByteSlice}, Received: reflect.ValueOf(int64(0))},
				},
				{
					"ReadBinary error",
					[]byte{},
					nil,
					&valueReaderWriter{BSONType: TypeBinary, Err: errors.New("rb error"), ErrAfter: readBinary},
					readBinary,
					errors.New("rb error"),
				},
				{
					"incorrect subtype",
					[]byte{},
					nil,
					&valueReaderWriter{
						BSONType: TypeBinary,
						Return: bsoncore.Value{
							Type: bsoncore.TypeBinary,
							Data: bsoncore.AppendBinary(nil, 0xFF, []byte{0x01, 0x02, 0x03}),
						},
					},
					readBinary,
					decodeBinaryError{subtype: byte(0xFF), typeName: "[]byte"},
				},
				{
					"can set false",
					cansettest,
					nil,
					&valueReaderWriter{BSONType: TypeBinary, Return: bsoncore.AppendBinary(nil, 0x00, []byte{0x01, 0x02, 0x03})},
					nothing,
					ValueDecoderError{Name: "ByteSliceDecodeValue", Types: []reflect.Type{tByteSlice}},
				},
				{
					"decode null",
					([]byte)(nil),
					nil,
					&valueReaderWriter{BSONType: TypeNull},
					readNull,
					nil,
				},
				{
					"decode undefined",
					([]byte)(nil),
					nil,
					&valueReaderWriter{BSONType: TypeUndefined},
					readUndefined,
					nil,
				},
			},
		},
		{
			"defaultStringCodec.DecodeValue",
			&stringCodec{},
			[]subtest{
				{
					"symbol",
					"var hello = 'world';",
					nil,
					&valueReaderWriter{BSONType: TypeSymbol, Return: "var hello = 'world';"},
					readSymbol,
					nil,
				},
				{
					"decode null",
					"",
					nil,
					&valueReaderWriter{BSONType: TypeNull},
					readNull,
					nil,
				},
				{
					"decode undefined",
					"",
					nil,
					&valueReaderWriter{BSONType: TypeUndefined},
					readUndefined,
					nil,
				},
			},
		},
		{
			"ValueUnmarshalerDecodeValue",
			ValueDecoderFunc(valueUnmarshalerDecodeValue),
			[]subtest{
				{
					"wrong type",
					wrong,
					nil,
					nil,
					nothing,
					ValueDecoderError{
						Name:     "ValueUnmarshalerDecodeValue",
						Types:    []reflect.Type{tValueUnmarshaler},
						Received: reflect.ValueOf(wrong),
					},
				},
				{
					"copy error",
					&testValueUnmarshaler{},
					nil,
					&valueReaderWriter{BSONType: TypeString, Err: errors.New("copy error"), ErrAfter: readString},
					readString,
					errors.New("copy error"),
				},
				{
					"ValueUnmarshaler",
					&testValueUnmarshaler{t: TypeString, val: bsoncore.AppendString(nil, "hello, world")},
					nil,
					&valueReaderWriter{BSONType: TypeString, Return: "hello, world"},
					readString,
					nil,
				},
			},
		},
		{
			"UnmarshalerDecodeValue",
			ValueDecoderFunc(unmarshalerDecodeValue),
			[]subtest{
				{
					"wrong type",
					wrong,
					nil,
					nil,
					nothing,
					ValueDecoderError{Name: "UnmarshalerDecodeValue", Types: []reflect.Type{tUnmarshaler}, Received: reflect.ValueOf(wrong)},
				},
				{
					"copy error",
					&testUnmarshaler{},
					nil,
					&valueReaderWriter{BSONType: TypeString, Err: errors.New("copy error"), ErrAfter: readString},
					readString,
					errors.New("copy error"),
				},
				{
					// Only the pointer form of testUnmarshaler implements Unmarshaler
					"value does not implement Unmarshaler",
					&testUnmarshaler{
						Invoked: true,
						Val:     bsoncore.AppendDouble(nil, 3.14159),
					},
					nil,
					&valueReaderWriter{BSONType: TypeDouble, Return: float64(3.14159)},
					readDouble,
					nil,
				},
				{
					"Unmarshaler",
					&testUnmarshaler{
						Invoked: true,
						Val:     bsoncore.AppendDouble(nil, 3.14159),
					},
					nil,
					&valueReaderWriter{BSONType: TypeDouble, Return: float64(3.14159)},
					readDouble,
					nil,
				},
			},
		},
		{
			"PointerCodec.DecodeValue",
			&pointerCodec{},
			[]subtest{
				{
					"not valid", nil, nil, nil, nothing,
					ValueDecoderError{Name: "PointerCodec.DecodeValue", Kinds: []reflect.Kind{reflect.Ptr}, Received: reflect.Value{}},
				},
				{
					"can set", cansettest, nil, nil, nothing,
					ValueDecoderError{Name: "PointerCodec.DecodeValue", Kinds: []reflect.Kind{reflect.Ptr}},
				},
				{
					"No Decoder", &wrong, &DecodeContext{Registry: buildDefaultRegistry()}, nil, nothing,
					errNoDecoder{Type: reflect.TypeOf(wrong)},
				},
				{
					"decode null",
					(*mystruct)(nil),
					nil,
					&valueReaderWriter{BSONType: TypeNull},
					readNull,
					nil,
				},
				{
					"decode undefined",
					(*mystruct)(nil),
					nil,
					&valueReaderWriter{BSONType: TypeUndefined},
					readUndefined,
					nil,
				},
			},
		},
		{
			"BinaryDecodeValue",
			ValueDecoderFunc(binaryDecodeValue),
			[]subtest{
				{
					"wrong type",
					wrong,
					nil,
					&valueReaderWriter{},
					nothing,
					ValueDecoderError{Name: "BinaryDecodeValue", Types: []reflect.Type{tBinary}, Received: reflect.ValueOf(wrong)},
				},
				{
					"type not binary",
					Binary{},
					nil,
					&valueReaderWriter{BSONType: TypeString},
					nothing,
					fmt.Errorf("cannot decode %v into a Binary", TypeString),
				},
				{
					"ReadBinary Error",
					Binary{},
					nil,
					&valueReaderWriter{BSONType: TypeBinary, Err: errors.New("rb error"), ErrAfter: readBinary},
					readBinary,
					errors.New("rb error"),
				},
				{
					"Binary/success",
					Binary{Data: []byte{0x01, 0x02, 0x03}, Subtype: 0xFF},
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
				{
					"decode null",
					Binary{},
					nil,
					&valueReaderWriter{BSONType: TypeNull},
					readNull,
					nil,
				},
				{
					"decode undefined",
					Binary{},
					nil,
					&valueReaderWriter{BSONType: TypeUndefined},
					readUndefined,
					nil,
				},
			},
		},
		{
			"UndefinedDecodeValue",
			ValueDecoderFunc(undefinedDecodeValue),
			[]subtest{
				{
					"wrong type",
					wrong,
					nil,
					&valueReaderWriter{BSONType: TypeUndefined},
					nothing,
					ValueDecoderError{Name: "UndefinedDecodeValue", Types: []reflect.Type{tUndefined}, Received: reflect.ValueOf(wrong)},
				},
				{
					"type not undefined",
					Undefined{},
					nil,
					&valueReaderWriter{BSONType: TypeString},
					nothing,
					fmt.Errorf("cannot decode %v into an Undefined", TypeString),
				},
				{
					"ReadUndefined Error",
					Undefined{},
					nil,
					&valueReaderWriter{BSONType: TypeUndefined, Err: errors.New("ru error"), ErrAfter: readUndefined},
					readUndefined,
					errors.New("ru error"),
				},
				{
					"ReadUndefined/success",
					Undefined{},
					nil,
					&valueReaderWriter{BSONType: TypeUndefined},
					readUndefined,
					nil,
				},
				{
					"decode null",
					Undefined{},
					nil,
					&valueReaderWriter{BSONType: TypeNull},
					readNull,
					nil,
				},
			},
		},
		{
			"DateTimeDecodeValue",
			ValueDecoderFunc(dateTimeDecodeValue),
			[]subtest{
				{
					"wrong type",
					wrong,
					nil,
					&valueReaderWriter{BSONType: TypeDateTime},
					nothing,
					ValueDecoderError{Name: "DateTimeDecodeValue", Types: []reflect.Type{tDateTime}, Received: reflect.ValueOf(wrong)},
				},
				{
					"type not datetime",
					DateTime(0),
					nil,
					&valueReaderWriter{BSONType: TypeString},
					nothing,
					fmt.Errorf("cannot decode %v into a DateTime", TypeString),
				},
				{
					"ReadDateTime Error",
					DateTime(0),
					nil,
					&valueReaderWriter{BSONType: TypeDateTime, Err: errors.New("rdt error"), ErrAfter: readDateTime},
					readDateTime,
					errors.New("rdt error"),
				},
				{
					"success",
					DateTime(1234567890),
					nil,
					&valueReaderWriter{BSONType: TypeDateTime, Return: int64(1234567890)},
					readDateTime,
					nil,
				},
				{
					"decode null",
					DateTime(0),
					nil,
					&valueReaderWriter{BSONType: TypeNull},
					readNull,
					nil,
				},
				{
					"decode undefined",
					DateTime(0),
					nil,
					&valueReaderWriter{BSONType: TypeUndefined},
					readUndefined,
					nil,
				},
			},
		},
		{
			"NullDecodeValue",
			ValueDecoderFunc(nullDecodeValue),
			[]subtest{
				{
					"wrong type",
					wrong,
					nil,
					&valueReaderWriter{BSONType: TypeNull},
					nothing,
					ValueDecoderError{Name: "NullDecodeValue", Types: []reflect.Type{tNull}, Received: reflect.ValueOf(wrong)},
				},
				{
					"type not null",
					Null{},
					nil,
					&valueReaderWriter{BSONType: TypeString},
					nothing,
					fmt.Errorf("cannot decode %v into a Null", TypeString),
				},
				{
					"ReadNull Error",
					Null{},
					nil,
					&valueReaderWriter{BSONType: TypeNull, Err: errors.New("rn error"), ErrAfter: readNull},
					readNull,
					errors.New("rn error"),
				},
				{
					"success",
					Null{},
					nil,
					&valueReaderWriter{BSONType: TypeNull},
					readNull,
					nil,
				},
			},
		},
		{
			"RegexDecodeValue",
			ValueDecoderFunc(regexDecodeValue),
			[]subtest{
				{
					"wrong type",
					wrong,
					nil,
					&valueReaderWriter{BSONType: TypeRegex},
					nothing,
					ValueDecoderError{Name: "RegexDecodeValue", Types: []reflect.Type{tRegex}, Received: reflect.ValueOf(wrong)},
				},
				{
					"type not regex",
					Regex{},
					nil,
					&valueReaderWriter{BSONType: TypeString},
					nothing,
					fmt.Errorf("cannot decode %v into a Regex", TypeString),
				},
				{
					"ReadRegex Error",
					Regex{},
					nil,
					&valueReaderWriter{BSONType: TypeRegex, Err: errors.New("rr error"), ErrAfter: readRegex},
					readRegex,
					errors.New("rr error"),
				},
				{
					"success",
					Regex{Pattern: "foo", Options: "bar"},
					nil,
					&valueReaderWriter{
						BSONType: TypeRegex,
						Return: bsoncore.Value{
							Type: bsoncore.TypeRegex,
							Data: bsoncore.AppendRegex(nil, "foo", "bar"),
						},
					},
					readRegex,
					nil,
				},
				{
					"decode null",
					Regex{},
					nil,
					&valueReaderWriter{BSONType: TypeNull},
					readNull,
					nil,
				},
				{
					"decode undefined",
					Regex{},
					nil,
					&valueReaderWriter{BSONType: TypeUndefined},
					readUndefined,
					nil,
				},
			},
		},
		{
			"DBPointerDecodeValue",
			ValueDecoderFunc(dbPointerDecodeValue),
			[]subtest{
				{
					"wrong type",
					wrong,
					nil,
					&valueReaderWriter{BSONType: TypeDBPointer},
					nothing,
					ValueDecoderError{Name: "DBPointerDecodeValue", Types: []reflect.Type{tDBPointer}, Received: reflect.ValueOf(wrong)},
				},
				{
					"type not dbpointer",
					DBPointer{},
					nil,
					&valueReaderWriter{BSONType: TypeString},
					nothing,
					fmt.Errorf("cannot decode %v into a DBPointer", TypeString),
				},
				{
					"ReadDBPointer Error",
					DBPointer{},
					nil,
					&valueReaderWriter{BSONType: TypeDBPointer, Err: errors.New("rdbp error"), ErrAfter: readDBPointer},
					readDBPointer,
					errors.New("rdbp error"),
				},
				{
					"success",
					DBPointer{
						DB:      "foobar",
						Pointer: ObjectID{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C},
					},
					nil,
					&valueReaderWriter{
						BSONType: TypeDBPointer,
						Return: bsoncore.Value{
							Type: bsoncore.TypeDBPointer,
							Data: bsoncore.AppendDBPointer(
								nil, "foobar", ObjectID{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C},
							),
						},
					},
					readDBPointer,
					nil,
				},
				{
					"decode null",
					DBPointer{},
					nil,
					&valueReaderWriter{BSONType: TypeNull},
					readNull,
					nil,
				},
				{
					"decode undefined",
					DBPointer{},
					nil,
					&valueReaderWriter{BSONType: TypeUndefined},
					readUndefined,
					nil,
				},
			},
		},
		{
			"TimestampDecodeValue",
			ValueDecoderFunc(timestampDecodeValue),
			[]subtest{
				{
					"wrong type",
					wrong,
					nil,
					&valueReaderWriter{BSONType: TypeTimestamp},
					nothing,
					ValueDecoderError{Name: "TimestampDecodeValue", Types: []reflect.Type{tTimestamp}, Received: reflect.ValueOf(wrong)},
				},
				{
					"type not timestamp",
					Timestamp{},
					nil,
					&valueReaderWriter{BSONType: TypeString},
					nothing,
					fmt.Errorf("cannot decode %v into a Timestamp", TypeString),
				},
				{
					"ReadTimestamp Error",
					Timestamp{},
					nil,
					&valueReaderWriter{BSONType: TypeTimestamp, Err: errors.New("rt error"), ErrAfter: readTimestamp},
					readTimestamp,
					errors.New("rt error"),
				},
				{
					"success",
					Timestamp{T: 12345, I: 67890},
					nil,
					&valueReaderWriter{
						BSONType: TypeTimestamp,
						Return: bsoncore.Value{
							Type: bsoncore.TypeTimestamp,
							Data: bsoncore.AppendTimestamp(nil, 12345, 67890),
						},
					},
					readTimestamp,
					nil,
				},
				{
					"decode null",
					Timestamp{},
					nil,
					&valueReaderWriter{BSONType: TypeNull},
					readNull,
					nil,
				},
				{
					"decode undefined",
					Timestamp{},
					nil,
					&valueReaderWriter{BSONType: TypeUndefined},
					readUndefined,
					nil,
				},
			},
		},
		{
			"MinKeyDecodeValue",
			ValueDecoderFunc(minKeyDecodeValue),
			[]subtest{
				{
					"wrong type",
					wrong,
					nil,
					&valueReaderWriter{BSONType: TypeMinKey},
					nothing,
					ValueDecoderError{Name: "MinKeyDecodeValue", Types: []reflect.Type{tMinKey}, Received: reflect.ValueOf(wrong)},
				},
				{
					"type not null",
					MinKey{},
					nil,
					&valueReaderWriter{BSONType: TypeString},
					nothing,
					fmt.Errorf("cannot decode %v into a MinKey", TypeString),
				},
				{
					"ReadMinKey Error",
					MinKey{},
					nil,
					&valueReaderWriter{BSONType: TypeMinKey, Err: errors.New("rn error"), ErrAfter: readMinKey},
					readMinKey,
					errors.New("rn error"),
				},
				{
					"success",
					MinKey{},
					nil,
					&valueReaderWriter{BSONType: TypeMinKey},
					readMinKey,
					nil,
				},
				{
					"decode null",
					MinKey{},
					nil,
					&valueReaderWriter{BSONType: TypeNull},
					readNull,
					nil,
				},
				{
					"decode undefined",
					MinKey{},
					nil,
					&valueReaderWriter{BSONType: TypeUndefined},
					readUndefined,
					nil,
				},
			},
		},
		{
			"MaxKeyDecodeValue",
			ValueDecoderFunc(maxKeyDecodeValue),
			[]subtest{
				{
					"wrong type",
					wrong,
					nil,
					&valueReaderWriter{BSONType: TypeMaxKey},
					nothing,
					ValueDecoderError{Name: "MaxKeyDecodeValue", Types: []reflect.Type{tMaxKey}, Received: reflect.ValueOf(wrong)},
				},
				{
					"type not null",
					MaxKey{},
					nil,
					&valueReaderWriter{BSONType: TypeString},
					nothing,
					fmt.Errorf("cannot decode %v into a MaxKey", TypeString),
				},
				{
					"ReadMaxKey Error",
					MaxKey{},
					nil,
					&valueReaderWriter{BSONType: TypeMaxKey, Err: errors.New("rn error"), ErrAfter: readMaxKey},
					readMaxKey,
					errors.New("rn error"),
				},
				{
					"success",
					MaxKey{},
					nil,
					&valueReaderWriter{BSONType: TypeMaxKey},
					readMaxKey,
					nil,
				},
				{
					"decode null",
					MaxKey{},
					nil,
					&valueReaderWriter{BSONType: TypeNull},
					readNull,
					nil,
				},
				{
					"decode undefined",
					MaxKey{},
					nil,
					&valueReaderWriter{BSONType: TypeUndefined},
					readUndefined,
					nil,
				},
			},
		},
		{
			"JavaScriptDecodeValue",
			ValueDecoderFunc(javaScriptDecodeValue),
			[]subtest{
				{
					"wrong type",
					wrong,
					nil,
					&valueReaderWriter{BSONType: TypeJavaScript, Return: ""},
					nothing,
					ValueDecoderError{Name: "JavaScriptDecodeValue", Types: []reflect.Type{tJavaScript}, Received: reflect.ValueOf(wrong)},
				},
				{
					"type not Javascript",
					JavaScript(""),
					nil,
					&valueReaderWriter{BSONType: TypeString},
					nothing,
					fmt.Errorf("cannot decode %v into a JavaScript", TypeString),
				},
				{
					"ReadJavascript Error",
					JavaScript(""),
					nil,
					&valueReaderWriter{BSONType: TypeJavaScript, Err: errors.New("rjs error"), ErrAfter: readJavascript},
					readJavascript,
					errors.New("rjs error"),
				},
				{
					"JavaScript/success",
					JavaScript("var hello = 'world';"),
					nil,
					&valueReaderWriter{BSONType: TypeJavaScript, Return: "var hello = 'world';"},
					readJavascript,
					nil,
				},
				{
					"decode null",
					JavaScript(""),
					nil,
					&valueReaderWriter{BSONType: TypeNull},
					readNull,
					nil,
				},
				{
					"decode undefined",
					JavaScript(""),
					nil,
					&valueReaderWriter{BSONType: TypeUndefined},
					readUndefined,
					nil,
				},
			},
		},
		{
			"SymbolDecodeValue",
			ValueDecoderFunc(symbolDecodeValue),
			[]subtest{
				{
					"wrong type",
					wrong,
					nil,
					&valueReaderWriter{BSONType: TypeSymbol, Return: ""},
					nothing,
					ValueDecoderError{Name: "SymbolDecodeValue", Types: []reflect.Type{tSymbol}, Received: reflect.ValueOf(wrong)},
				},
				{
					"type not Symbol",
					Symbol(""),
					nil,
					&valueReaderWriter{BSONType: TypeInt32},
					nothing,
					fmt.Errorf("cannot decode %v into a Symbol", TypeInt32),
				},
				{
					"ReadSymbol Error",
					Symbol(""),
					nil,
					&valueReaderWriter{BSONType: TypeSymbol, Err: errors.New("rjs error"), ErrAfter: readSymbol},
					readSymbol,
					errors.New("rjs error"),
				},
				{
					"Symbol/success",
					Symbol("var hello = 'world';"),
					nil,
					&valueReaderWriter{BSONType: TypeSymbol, Return: "var hello = 'world';"},
					readSymbol,
					nil,
				},
				{
					"decode null",
					Symbol(""),
					nil,
					&valueReaderWriter{BSONType: TypeNull},
					readNull,
					nil,
				},
				{
					"decode undefined",
					Symbol(""),
					nil,
					&valueReaderWriter{BSONType: TypeUndefined},
					readUndefined,
					nil,
				},
			},
		},
		{
			"CoreDocumentDecodeValue",
			ValueDecoderFunc(coreDocumentDecodeValue),
			[]subtest{
				{
					"wrong type",
					wrong,
					nil,
					&valueReaderWriter{},
					nothing,
					ValueDecoderError{
						Name:     "CoreDocumentDecodeValue",
						Types:    []reflect.Type{tCoreDocument},
						Received: reflect.ValueOf(wrong),
					},
				},
				{
					"*bsoncore.Document is nil",
					(*bsoncore.Document)(nil),
					nil,
					nil,
					nothing,
					ValueDecoderError{
						Name:     "CoreDocumentDecodeValue",
						Types:    []reflect.Type{tCoreDocument},
						Received: reflect.ValueOf((*bsoncore.Document)(nil)),
					},
				},
				{
					"Copy error",
					bsoncore.Document{},
					nil,
					&valueReaderWriter{Err: errors.New("copy error"), ErrAfter: readDocument},
					readDocument,
					errors.New("copy error"),
				},
			},
		},
		{
			"StructCodec.DecodeValue",
			newStructCodec(nil),
			[]subtest{
				{
					"Not struct",
					reflect.New(reflect.TypeOf(struct{ Foo string }{})).Elem().Interface(),
					nil,
					&valueReaderWriter{BSONType: TypeString},
					nothing,
					errors.New("cannot decode string into a struct { Foo string }"),
				},
				{
					"decode null",
					reflect.New(reflect.TypeOf(struct{ Foo string }{})).Elem().Interface(),
					nil,
					&valueReaderWriter{BSONType: TypeNull},
					readNull,
					nil,
				},
				{
					"decode undefined",
					reflect.New(reflect.TypeOf(struct{ Foo string }{})).Elem().Interface(),
					nil,
					&valueReaderWriter{BSONType: TypeUndefined},
					readUndefined,
					nil,
				},
			},
		},
		{
			"CodeWithScopeDecodeValue",
			ValueDecoderFunc(codeWithScopeDecodeValue),
			[]subtest{
				{
					"wrong type",
					wrong,
					nil,
					&valueReaderWriter{BSONType: TypeCodeWithScope},
					nothing,
					ValueDecoderError{
						Name:     "CodeWithScopeDecodeValue",
						Types:    []reflect.Type{tCodeWithScope},
						Received: reflect.ValueOf(wrong),
					},
				},
				{
					"type not codewithscope",
					CodeWithScope{},
					nil,
					&valueReaderWriter{BSONType: TypeString},
					nothing,
					fmt.Errorf("cannot decode %v into a CodeWithScope", TypeString),
				},
				{
					"ReadCodeWithScope Error",
					CodeWithScope{},
					nil,
					&valueReaderWriter{BSONType: TypeCodeWithScope, Err: errors.New("rcws error"), ErrAfter: readCodeWithScope},
					readCodeWithScope,
					errors.New("rcws error"),
				},
				{
					"decodeDocument Error",
					CodeWithScope{
						Code:  "var hello = 'world';",
						Scope: D{{"foo", nil}},
					},
					&DecodeContext{Registry: buildDefaultRegistry()},
					&valueReaderWriter{BSONType: TypeCodeWithScope, Err: errors.New("dd error"), ErrAfter: readElement},
					readElement,
					errors.New("dd error"),
				},
				{
					"decode null",
					CodeWithScope{},
					nil,
					&valueReaderWriter{BSONType: TypeNull},
					readNull,
					nil,
				},
				{
					"decode undefined",
					CodeWithScope{},
					nil,
					&valueReaderWriter{BSONType: TypeUndefined},
					readUndefined,
					nil,
				},
			},
		},
		{
			"CoreArrayDecodeValue",
			&arrayCodec{},
			[]subtest{
				{
					"wrong type",
					wrong,
					nil,
					&valueReaderWriter{},
					nothing,
					ValueDecoderError{
						Name:     "CoreArrayDecodeValue",
						Types:    []reflect.Type{tCoreArray},
						Received: reflect.ValueOf(wrong),
					},
				},
				{
					"*bsoncore.Array is nil",
					(*bsoncore.Array)(nil),
					nil,
					nil,
					nothing,
					ValueDecoderError{
						Name:     "CoreArrayDecodeValue",
						Types:    []reflect.Type{tCoreArray},
						Received: reflect.ValueOf((*bsoncore.Array)(nil)),
					},
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
					// var got interface{}
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
					if rc.val == cansettest { // We're doing an IsValid and CanSet test
						var wanterr ValueDecoderError
						if !errors.As(rc.err, &wanterr) {
							t.Fatalf("Error must be a DecodeValueError, but got a %T", rc.err)
						}

						err := tc.vd.DecodeValue(dc, llvrw, reflect.Value{})
						wanterr.Received = reflect.ValueOf(nil)
						if !assert.CompareErrors(err, wanterr) {
							t.Errorf("Errors do not match. got %v; want %v", err, wanterr)
						}

						err = tc.vd.DecodeValue(dc, llvrw, reflect.ValueOf(int(12345)))
						wanterr.Received = reflect.ValueOf(int(12345))
						if !assert.CompareErrors(err, wanterr) {
							t.Errorf("Errors do not match. got %v; want %v", err, wanterr)
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
					if rc.err == nil && !cmp.Equal(got, want, cmp.Comparer(compareDecimal128)) {
						t.Errorf("Values do not match. got (%T)%v; want (%T)%v", got, got, want, want)
					}
				})
			}
		})
	}

	t.Run("CodeWithScopeCodec/DecodeValue/success", func(t *testing.T) {
		dc := DecodeContext{Registry: buildDefaultRegistry()}
		b := bsoncore.BuildDocument(nil,
			bsoncore.AppendCodeWithScopeElement(
				nil, "foo", "var hello = 'world';",
				buildDocument(bsoncore.AppendNullElement(nil, "bar")),
			),
		)
		dvr := NewDocumentReader(bytes.NewReader(b))
		dr, err := dvr.ReadDocument()
		noerr(t, err)
		_, vr, err := dr.ReadElement()
		noerr(t, err)

		want := CodeWithScope{
			Code:  "var hello = 'world';",
			Scope: D{{"bar", nil}},
		}
		val := reflect.New(tCodeWithScope).Elem()
		err = codeWithScopeDecodeValue(dc, vr, val)
		noerr(t, err)

		got := val.Interface().(CodeWithScope)
		if got.Code != want.Code && !cmp.Equal(got.Scope, want.Scope) {
			t.Errorf("CodeWithScopes do not match. got %v; want %v", got, want)
		}
	})
	t.Run("ValueUnmarshalerDecodeValue/UnmarshalBSONValue error", func(t *testing.T) {
		var dc DecodeContext
		llvrw := &valueReaderWriter{BSONType: TypeString, Return: string("hello, world!")}
		llvrw.T = t

		want := errors.New("ubsonv error")
		valUnmarshaler := &testValueUnmarshaler{err: want}
		got := valueUnmarshalerDecodeValue(dc, llvrw, reflect.ValueOf(valUnmarshaler))
		if !assert.CompareErrors(got, want) {
			t.Errorf("Errors do not match. got %v; want %v", got, want)
		}
	})
	t.Run("ValueUnmarshalerDecodeValue/Unaddressable value", func(t *testing.T) {
		var dc DecodeContext
		llvrw := &valueReaderWriter{BSONType: TypeString, Return: string("hello, world!")}
		llvrw.T = t

		val := reflect.ValueOf(testValueUnmarshaler{})
		want := ValueDecoderError{Name: "ValueUnmarshalerDecodeValue", Types: []reflect.Type{tValueUnmarshaler}, Received: val}
		got := valueUnmarshalerDecodeValue(dc, llvrw, val)
		if !assert.CompareErrors(got, want) {
			t.Errorf("Errors do not match. got %v; want %v", got, want)
		}
	})

	t.Run("SliceCodec/DecodeValue/too many elements", func(t *testing.T) {
		idx, doc := bsoncore.AppendDocumentStart(nil)
		aidx, doc := bsoncore.AppendArrayElementStart(doc, "foo")
		doc = bsoncore.AppendStringElement(doc, "0", "foo")
		doc = bsoncore.AppendStringElement(doc, "1", "bar")
		doc, err := bsoncore.AppendArrayEnd(doc, aidx)
		noerr(t, err)
		doc, err = bsoncore.AppendDocumentEnd(doc, idx)
		noerr(t, err)
		dvr := NewDocumentReader(bytes.NewReader(doc))
		noerr(t, err)
		dr, err := dvr.ReadDocument()
		noerr(t, err)
		_, vr, err := dr.ReadElement()
		noerr(t, err)
		var val [1]string
		want := fmt.Errorf("more elements returned in array than can fit inside %T, got 2 elements", val)

		dc := DecodeContext{Registry: buildDefaultRegistry()}
		got := arrayDecodeValue(dc, vr, reflect.ValueOf(val))
		if !assert.CompareErrors(got, want) {
			t.Errorf("Errors do not match. got %v; want %v", got, want)
		}
	})

	t.Run("success path", func(t *testing.T) {
		oid := NewObjectID()
		oids := []ObjectID{NewObjectID(), NewObjectID(), NewObjectID()}
		var str = new(string)
		*str = "bar"
		now := time.Now().Truncate(time.Millisecond).UTC()
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
				func() []byte {
					idx, doc := bsoncore.AppendDocumentStart(nil)
					doc = bsoncore.AppendObjectIDElement(doc, "foo", oid)
					doc, _ = bsoncore.AppendDocumentEnd(doc, idx)
					return doc
				}(),
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
				"map[mystring]interface{}",
				map[mystring]interface{}{"pi": 3.14159},
				buildDocument(bsoncore.AppendDoubleElement(nil, "pi", 3.14159)),
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
				"omitzero",
				struct {
					A [4]int `bson:",omitzero"`
				}{
					A: [4]int{},
				},
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
					AE *testValueUnmarshaler
					AF *bool
					AG *bool
					AH *int32
					AI *int64
					AJ *ObjectID
					AK *ObjectID
					AL testValueUnmarshaler
					AM interface{}
					AN interface{}
					AO interface{}
					AP D
					AQ A
					AR [2]E
					AS []byte
					AT map[string]interface{}
					AU CodeWithScope
					AV M
					AW D
					AX map[string]interface{}
					AY []E
					AZ interface{}
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
					AE: &testValueUnmarshaler{t: TypeString, val: bsoncore.AppendString(nil, "hello, world!")},
					AF: func(b bool) *bool { return &b }(true),
					AG: nil,
					AH: func(i32 int32) *int32 { return &i32 }(12345),
					AI: func(i64 int64) *int64 { return &i64 }(1234567890),
					AJ: &oid,
					AK: nil,
					AL: testValueUnmarshaler{t: TypeString, val: bsoncore.AppendString(nil, "hello, world!")},
					AM: "hello, world",
					AN: int32(12345),
					AO: oid,
					AP: D{{"foo", "bar"}},
					AQ: A{"foo", "bar"},
					AR: [2]E{{"hello", "world"}, {"pi", 3.14159}},
					AS: nil,
					AT: nil,
					AU: CodeWithScope{Code: "var hello = 'world';", Scope: D{{"pi", 3.14159}}},
					AV: M{"foo": D{{"bar", "baz"}}},
					AW: D{{"foo", D{{"bar", "baz"}}}},
					AX: map[string]interface{}{"foo": D{{"bar", "baz"}}},
					AY: []E{{"foo", D{{"bar", "baz"}}}},
					AZ: D{{"foo", D{{"bar", "baz"}}}},
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
					doc = bsoncore.AppendStringElement(doc, "ae", "hello, world!")
					doc = bsoncore.AppendBooleanElement(doc, "af", true)
					doc = bsoncore.AppendNullElement(doc, "ag")
					doc = bsoncore.AppendInt32Element(doc, "ah", 12345)
					doc = bsoncore.AppendInt32Element(doc, "ai", 1234567890)
					doc = bsoncore.AppendObjectIDElement(doc, "aj", oid)
					doc = bsoncore.AppendNullElement(doc, "ak")
					doc = bsoncore.AppendStringElement(doc, "al", "hello, world!")
					doc = bsoncore.AppendStringElement(doc, "am", "hello, world")
					doc = bsoncore.AppendInt32Element(doc, "an", 12345)
					doc = bsoncore.AppendObjectIDElement(doc, "ao", oid)
					doc = bsoncore.AppendDocumentElement(doc, "ap", buildDocument(bsoncore.AppendStringElement(nil, "foo", "bar")))
					doc = bsoncore.AppendArrayElement(doc, "aq",
						buildArray(bsoncore.AppendStringElement(bsoncore.AppendStringElement(nil, "0", "foo"), "1", "bar")),
					)
					doc = bsoncore.AppendDocumentElement(doc, "ar",
						buildDocument(bsoncore.AppendDoubleElement(bsoncore.AppendStringElement(nil, "hello", "world"), "pi", 3.14159)),
					)
					doc = bsoncore.AppendNullElement(doc, "as")
					doc = bsoncore.AppendNullElement(doc, "at")
					doc = bsoncore.AppendCodeWithScopeElement(doc, "au",
						"var hello = 'world';", buildDocument(bsoncore.AppendDoubleElement(nil, "pi", 3.14159)),
					)
					for _, name := range [5]string{"av", "aw", "ax", "ay", "az"} {
						doc = bsoncore.AppendDocumentElement(doc, name, buildDocument(
							bsoncore.AppendDocumentElement(nil, "foo", buildDocument(
								bsoncore.AppendStringElement(nil, "bar", "baz"),
							)),
						))
					}
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
					AE []*testValueUnmarshaler
					AF []*bool
					AG []*int32
					AH []*int64
					AI []*ObjectID
					AJ []D
					AK []A
					AL [][2]E
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
					AE: []*testValueUnmarshaler{
						{t: TypeString, val: bsoncore.AppendString(nil, "hello")},
						{t: TypeString, val: bsoncore.AppendString(nil, "world")},
					},
					AF: []*bool{pbool(true), nil},
					AG: []*int32{pi32(12345), nil},
					AH: []*int64{pi64(1234567890), nil, pi64(9012345678)},
					AI: []*ObjectID{&oid, nil},
					AJ: []D{{{"foo", "bar"}}, nil},
					AK: []A{{"foo", "bar"}, nil},
					AL: [][2]E{{{"hello", "world"}, {"pi", 3.14159}}},
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
					doc = appendArrayElement(doc, "af",
						bsoncore.AppendNullElement(bsoncore.AppendBooleanElement(nil, "0", true), "1"),
					)
					doc = appendArrayElement(doc, "ag",
						bsoncore.AppendNullElement(bsoncore.AppendInt32Element(nil, "0", 12345), "1"),
					)
					doc = appendArrayElement(doc, "ah",
						bsoncore.AppendInt64Element(
							bsoncore.AppendNullElement(bsoncore.AppendInt64Element(nil, "0", 1234567890), "1"),
							"2", 9012345678,
						),
					)
					doc = appendArrayElement(doc, "ai",
						bsoncore.AppendNullElement(bsoncore.AppendObjectIDElement(nil, "0", oid), "1"),
					)
					doc = appendArrayElement(doc, "aj",
						bsoncore.AppendNullElement(
							bsoncore.AppendDocumentElement(nil, "0", buildDocument(bsoncore.AppendStringElement(nil, "foo", "bar"))),
							"1",
						),
					)
					doc = appendArrayElement(doc, "ak",
						bsoncore.AppendNullElement(
							appendArrayElement(nil, "0",
								bsoncore.AppendStringElement(bsoncore.AppendStringElement(nil, "0", "foo"), "1", "bar"),
							),
							"1",
						),
					)
					doc = appendArrayElement(doc, "al",
						bsoncore.BuildDocumentElement(nil, "0",
							bsoncore.AppendDoubleElement(bsoncore.AppendStringElement(nil, "hello", "world"), "pi", 3.14159),
						),
					)
					return doc
				}(nil)),
				nil,
			},
		}

		t.Run("Decode", func(t *testing.T) {
			compareTime := func(t1, t2 time.Time) bool {
				if t1.Location() != t2.Location() {
					return false
				}
				return t1.Equal(t2)
			}

			for _, tc := range testCases {
				t.Run(tc.name, func(t *testing.T) {
					vr := NewDocumentReader(bytes.NewReader(tc.b))
					reg := buildDefaultRegistry()
					vtype := reflect.TypeOf(tc.value)
					dec, err := reg.LookupDecoder(vtype)
					noerr(t, err)

					gotVal := reflect.New(reflect.TypeOf(tc.value)).Elem()
					err = dec.DecodeValue(DecodeContext{Registry: reg}, vr, gotVal)
					noerr(t, err)

					got := gotVal.Interface()
					want := tc.value
					if diff := cmp.Diff(
						got, want,
						cmp.Comparer(compareDecimal128),
						cmp.Comparer(compareNoPrivateFields),
						cmp.Comparer(compareZeroTest),
						cmp.Comparer(compareTime),
					); diff != "" {
						t.Errorf("difference:\n%s", diff)
						t.Errorf("Values are not equal.\ngot: %#v\nwant:%#v", got, want)
					}
				})
			}
		})
	})
	t.Run("error path", func(t *testing.T) {
		testCases := []struct {
			name  string
			value interface{}
			b     []byte
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
				buildDocument(bsoncore.AppendInt32Element(nil, "a", 12345)),
				fmt.Errorf("duplicated key a"),
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				vr := NewDocumentReader(bytes.NewReader(tc.b))
				reg := buildDefaultRegistry()
				vtype := reflect.TypeOf(tc.value)
				dec, err := reg.LookupDecoder(vtype)
				noerr(t, err)

				gotVal := reflect.New(reflect.TypeOf(tc.value)).Elem()
				err = dec.DecodeValue(DecodeContext{Registry: reg}, vr, gotVal)
				if err == nil || !strings.Contains(err.Error(), tc.err.Error()) {
					t.Errorf("Did not receive expected error. got %v; want %v", err, tc.err)
				}
			})
		}
	})

	t.Run("defaultEmptyInterfaceCodec.DecodeValue", func(t *testing.T) {
		t.Run("DecodeValue", func(t *testing.T) {
			testCases := []struct {
				name     string
				val      interface{}
				bsontype Type
			}{
				{
					"Double - float64",
					float64(3.14159),
					TypeDouble,
				},
				{
					"String - string",
					"foo bar baz",
					TypeString,
				},
				{
					"Array - A",
					A{3.14159},
					TypeArray,
				},
				{
					"Binary - Binary",
					Binary{Subtype: 0xFF, Data: []byte{0x01, 0x02, 0x03}},
					TypeBinary,
				},
				{
					"Undefined - Undefined",
					Undefined{},
					TypeUndefined,
				},
				{
					"ObjectID - ObjectID",
					ObjectID{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C},
					TypeObjectID,
				},
				{
					"Boolean - bool",
					bool(true),
					TypeBoolean,
				},
				{
					"DateTime - DateTime",
					DateTime(1234567890),
					TypeDateTime,
				},
				{
					"Null - Null",
					nil,
					TypeNull,
				},
				{
					"Regex - Regex",
					Regex{Pattern: "foo", Options: "bar"},
					TypeRegex,
				},
				{
					"DBPointer - DBPointer",
					DBPointer{
						DB:      "foobar",
						Pointer: ObjectID{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C},
					},
					TypeDBPointer,
				},
				{
					"JavaScript - JavaScript",
					JavaScript("var foo = 'bar';"),
					TypeJavaScript,
				},
				{
					"Symbol - Symbol",
					Symbol("foobarbazlolz"),
					TypeSymbol,
				},
				{
					"Int32 - int32",
					int32(123456),
					TypeInt32,
				},
				{
					"Int64 - int64",
					int64(1234567890),
					TypeInt64,
				},
				{
					"Timestamp - Timestamp",
					Timestamp{T: 12345, I: 67890},
					TypeTimestamp,
				},
				{
					"Decimal128 - decimal.Decimal128",
					NewDecimal128(12345, 67890),
					TypeDecimal128,
				},
				{
					"MinKey - MinKey",
					MinKey{},
					TypeMinKey,
				},
				{
					"MaxKey - MaxKey",
					MaxKey{},
					TypeMaxKey,
				},
			}
			for _, tc := range testCases {
				t.Run(tc.name, func(t *testing.T) {
					llvr := &valueReaderWriter{BSONType: tc.bsontype}

					t.Run("Type Map failure", func(t *testing.T) {
						if tc.bsontype == TypeNull {
							t.Skip()
						}
						val := reflect.New(tEmpty).Elem()
						dc := DecodeContext{Registry: newTestRegistry()}
						want := errNoTypeMapEntry{Type: tc.bsontype}
						got := (&emptyInterfaceCodec{}).DecodeValue(dc, llvr, val)
						if !assert.CompareErrors(got, want) {
							t.Errorf("Errors are not equal. got %v; want %v", got, want)
						}
					})

					t.Run("Lookup failure", func(t *testing.T) {
						if tc.bsontype == TypeNull {
							t.Skip()
						}
						val := reflect.New(tEmpty).Elem()
						reg := newTestRegistry()
						reg.RegisterTypeMapEntry(tc.bsontype, reflect.TypeOf(tc.val))
						dc := DecodeContext{
							Registry: reg,
						}
						want := errNoDecoder{Type: reflect.TypeOf(tc.val)}
						got := (&emptyInterfaceCodec{}).DecodeValue(dc, llvr, val)
						if !assert.CompareErrors(got, want) {
							t.Errorf("Errors are not equal. got %v; want %v", got, want)
						}
					})

					t.Run("DecodeValue failure", func(t *testing.T) {
						if tc.bsontype == TypeNull {
							t.Skip()
						}
						want := errors.New("DecodeValue failure error")
						llc := &llCodec{t: t, err: want}
						reg := newTestRegistry()
						reg.RegisterTypeDecoder(reflect.TypeOf(tc.val), llc)
						reg.RegisterTypeMapEntry(tc.bsontype, reflect.TypeOf(tc.val))
						dc := DecodeContext{
							Registry: reg,
						}
						got := (&emptyInterfaceCodec{}).DecodeValue(dc, llvr, reflect.New(tEmpty).Elem())
						if !assert.CompareErrors(got, want) {
							t.Errorf("Errors are not equal. got %v; want %v", got, want)
						}
					})

					t.Run("Success", func(t *testing.T) {
						want := tc.val
						llc := &llCodec{t: t, decodeval: tc.val}
						reg := newTestRegistry()
						reg.RegisterTypeDecoder(reflect.TypeOf(tc.val), llc)
						reg.RegisterTypeMapEntry(tc.bsontype, reflect.TypeOf(tc.val))
						dc := DecodeContext{
							Registry: reg,
						}
						got := reflect.New(tEmpty).Elem()
						err := (&emptyInterfaceCodec{}).DecodeValue(dc, llvr, got)
						noerr(t, err)
						if !cmp.Equal(got.Interface(), want, cmp.Comparer(compareDecimal128)) {
							t.Errorf("Did not receive expected value. got %v; want %v", got.Interface(), want)
						}
					})
				})
			}
		})

		t.Run("non-interface{}", func(t *testing.T) {
			val := uint64(1234567890)
			want := ValueDecoderError{Name: "EmptyInterfaceDecodeValue", Types: []reflect.Type{tEmpty}, Received: reflect.ValueOf(val)}
			got := (&emptyInterfaceCodec{}).DecodeValue(DecodeContext{}, nil, reflect.ValueOf(val))
			if !assert.CompareErrors(got, want) {
				t.Errorf("Errors are not equal. got %v; want %v", got, want)
			}
		})

		t.Run("nil *interface{}", func(t *testing.T) {
			var val interface{}
			want := ValueDecoderError{Name: "EmptyInterfaceDecodeValue", Types: []reflect.Type{tEmpty}, Received: reflect.ValueOf(val)}
			got := (&emptyInterfaceCodec{}).DecodeValue(DecodeContext{}, nil, reflect.ValueOf(val))
			if !assert.CompareErrors(got, want) {
				t.Errorf("Errors are not equal. got %v; want %v", got, want)
			}
		})

		t.Run("no type registered", func(t *testing.T) {
			llvr := &valueReaderWriter{BSONType: TypeDouble}
			want := errNoTypeMapEntry{Type: TypeDouble}
			val := reflect.New(tEmpty).Elem()
			got := (&emptyInterfaceCodec{}).DecodeValue(DecodeContext{Registry: newTestRegistry()}, llvr, val)
			if !assert.CompareErrors(got, want) {
				t.Errorf("Errors are not equal. got %v; want %v", got, want)
			}
		})
		t.Run("top level document", func(t *testing.T) {
			data := bsoncore.BuildDocument(nil, bsoncore.AppendDoubleElement(nil, "pi", 3.14159))
			vr := NewDocumentReader(bytes.NewReader(data))
			want := D{{"pi", 3.14159}}
			var got interface{}
			val := reflect.ValueOf(&got).Elem()
			err := (&emptyInterfaceCodec{}).DecodeValue(DecodeContext{Registry: buildDefaultRegistry()}, vr, val)
			noerr(t, err)
			if !cmp.Equal(got, want) {
				t.Errorf("Did not get correct result. got %v; want %v", got, want)
			}
		})
		t.Run("custom type map entry", func(t *testing.T) {
			// registering a custom type map entry for both Type(0) anad TypeEmbeddedDocument should cause
			// the top-level to decode to registered type when unmarshalling to interface{}

			topLevelReg := &Registry{
				typeEncoders: new(typeEncoderCache),
				typeDecoders: new(typeDecoderCache),
				kindEncoders: new(kindEncoderCache),
				kindDecoders: new(kindDecoderCache),
			}
			registerDefaultEncoders(topLevelReg)
			registerDefaultDecoders(topLevelReg)
			topLevelReg.RegisterTypeMapEntry(Type(0), reflect.TypeOf(M{}))

			embeddedReg := &Registry{
				typeEncoders: new(typeEncoderCache),
				typeDecoders: new(typeDecoderCache),
				kindEncoders: new(kindEncoderCache),
				kindDecoders: new(kindDecoderCache),
			}
			registerDefaultEncoders(embeddedReg)
			registerDefaultDecoders(embeddedReg)
			embeddedReg.RegisterTypeMapEntry(Type(0), reflect.TypeOf(M{}))

			// create doc {"nested": {"foo": 1}}
			innerDoc := bsoncore.BuildDocument(
				nil,
				bsoncore.AppendInt32Element(nil, "foo", 1),
			)
			doc := bsoncore.BuildDocument(
				nil,
				bsoncore.AppendDocumentElement(nil, "nested", innerDoc),
			)
			want := M{
				"nested": D{{"foo", int32(1)}},
			}

			testCases := []struct {
				name     string
				registry *Registry
			}{
				{"top level", topLevelReg},
				{"embedded", embeddedReg},
			}
			for _, tc := range testCases {
				var got interface{}
				vr := NewDocumentReader(bytes.NewReader(doc))
				val := reflect.ValueOf(&got).Elem()

				err := (&emptyInterfaceCodec{}).DecodeValue(DecodeContext{Registry: tc.registry}, vr, val)
				noerr(t, err)
				if !cmp.Equal(got, want) {
					t.Fatalf("got %v, want %v", got, want)
				}
			}
		})
		t.Run("custom type map entry is used if there is no type information", func(t *testing.T) {
			// If a type map entry is registered for TypeEmbeddedDocument, the decoder should use it when
			// type information is not available.

			reg := &Registry{
				typeEncoders: new(typeEncoderCache),
				typeDecoders: new(typeDecoderCache),
				kindEncoders: new(kindEncoderCache),
				kindDecoders: new(kindDecoderCache),
			}
			registerDefaultEncoders(reg)
			registerDefaultDecoders(reg)
			reg.RegisterTypeMapEntry(TypeEmbeddedDocument, reflect.TypeOf(M{}))

			// build document {"nested": {"foo": 10}}
			inner := bsoncore.BuildDocument(
				nil,
				bsoncore.AppendInt32Element(nil, "foo", 10),
			)
			doc := bsoncore.BuildDocument(
				nil,
				bsoncore.AppendDocumentElement(nil, "nested", inner),
			)
			want := D{
				{"nested", M{
					"foo": int32(10),
				}},
			}

			var got D
			vr := NewDocumentReader(bytes.NewReader(doc))
			val := reflect.ValueOf(&got).Elem()
			err := (&sliceCodec{}).DecodeValue(DecodeContext{Registry: reg}, vr, val)
			noerr(t, err)
			if !cmp.Equal(got, want) {
				t.Fatalf("got %v, want %v", got, want)
			}
		})
	})

	t.Run("decode errors contain key information", func(t *testing.T) {
		decodeValueError := errors.New("decode value error")
		emptyInterfaceErrorDecode := func(DecodeContext, ValueReader, reflect.Value) error {
			return decodeValueError
		}
		emptyInterfaceErrorRegistry := newTestRegistry()
		emptyInterfaceErrorRegistry.RegisterTypeDecoder(tEmpty, ValueDecoderFunc(emptyInterfaceErrorDecode))

		// Set up a document {foo: 10} and an error that would happen if the value were decoded into interface{}
		// using the registry defined above.
		docBytes := bsoncore.BuildDocumentFromElements(
			nil,
			bsoncore.AppendInt32Element(nil, "foo", 10),
		)
		docEmptyInterfaceErr := &DecodeError{
			keys:    []string{"foo"},
			wrapped: decodeValueError,
		}

		// Set up struct definitions where Foo maps to interface{} and string. When decoded using the registry defined
		// above, the interface{} struct will get an error when calling DecodeValue and the string struct will get an
		// error when looking up a decoder.
		type emptyInterfaceStruct struct {
			Foo interface{}
		}
		type stringStruct struct {
			Foo string
		}
		emptyInterfaceStructErr := &DecodeError{
			keys:    []string{"foo"},
			wrapped: decodeValueError,
		}
		stringStructErr := &DecodeError{
			keys:    []string{"foo"},
			wrapped: errNoDecoder{reflect.TypeOf("")},
		}

		// Test a deeply nested struct mixed with maps and slices.
		// Build document {"first": {"second": {"randomKey": {"third": [{}, {"fourth": "value"}]}}}}
		type inner3 struct{ Fourth interface{} }
		type inner2 struct{ Third []inner3 }
		type inner1 struct{ Second map[string]inner2 }
		type outer struct{ First inner1 }
		inner3EmptyDoc := buildDocument(nil)
		inner3Doc := buildDocument(bsoncore.AppendStringElement(nil, "fourth", "value"))
		inner3Array := buildArray(
			// buildArray takes []byte so we first append() all of the values into a single []byte
			append(
				bsoncore.AppendDocumentElement(nil, "0", inner3EmptyDoc),
				bsoncore.AppendDocumentElement(nil, "1", inner3Doc)...,
			),
		)
		inner2Doc := buildDocument(bsoncore.AppendArrayElement(nil, "third", inner3Array))
		inner2Map := buildDocument(bsoncore.AppendDocumentElement(nil, "randomKey", inner2Doc))
		inner1Doc := buildDocument(bsoncore.AppendDocumentElement(nil, "second", inner2Map))
		outerDoc := buildDocument(bsoncore.AppendDocumentElement(nil, "first", inner1Doc))

		// Use a registry that has all default decoders with the custom interface{} decoder that always errors.
		nestedRegistry := &Registry{
			typeEncoders: new(typeEncoderCache),
			typeDecoders: new(typeDecoderCache),
			kindEncoders: new(kindEncoderCache),
			kindDecoders: new(kindDecoderCache),
		}
		registerDefaultDecoders(nestedRegistry)
		nestedRegistry.RegisterTypeDecoder(tEmpty, ValueDecoderFunc(emptyInterfaceErrorDecode))
		nestedErr := &DecodeError{
			keys:    []string{"fourth", "1", "third", "randomKey", "second", "first"},
			wrapped: decodeValueError,
		}

		testCases := []struct {
			name     string
			val      interface{}
			vr       ValueReader
			registry *Registry // buildDefaultRegistry will be used if this is nil
			decoder  ValueDecoder
			err      error
		}{
			{
				// DecodeValue error when decoding into a D.
				"D slice",
				D{},
				NewDocumentReader(bytes.NewReader(docBytes)),
				emptyInterfaceErrorRegistry,
				&sliceCodec{},
				docEmptyInterfaceErr,
			},
			{
				// DecodeValue error when decoding into a []string.
				"string slice",
				[]string{},
				&valueReaderWriter{BSONType: TypeArray},
				nil,
				&sliceCodec{},
				&DecodeError{
					keys:    []string{"0"},
					wrapped: errors.New("cannot decode array into a string type"),
				},
			},
			{
				// DecodeValue error when decoding into a E array. This should have the same behavior as
				// the "D slice" test above because both the defaultSliceCodec and ArrayDecodeValue use
				// the decodeD helper function.
				"D array",
				[1]E{},
				NewDocumentReader(bytes.NewReader(docBytes)),
				emptyInterfaceErrorRegistry,
				ValueDecoderFunc(arrayDecodeValue),
				docEmptyInterfaceErr,
			},
			{
				// DecodeValue error when decoding into a string array. This should have the same behavior as
				// the "D slice" test above because both the defaultSliceCodec and ArrayDecodeValue use
				// the decodeDefault helper function.
				"string array",
				[1]string{},
				&valueReaderWriter{BSONType: TypeArray},
				nil,
				ValueDecoderFunc(arrayDecodeValue),
				&DecodeError{
					keys:    []string{"0"},
					wrapped: errors.New("cannot decode array into a string type"),
				},
			},
			{
				// DecodeValue error when decoding into a map.
				"map",
				map[string]interface{}{},
				NewDocumentReader(bytes.NewReader(docBytes)),
				emptyInterfaceErrorRegistry,
				&mapCodec{},
				docEmptyInterfaceErr,
			},
			{
				// DecodeValue error when decoding into a struct.
				"struct - DecodeValue error",
				emptyInterfaceStruct{},
				NewDocumentReader(bytes.NewReader(docBytes)),
				emptyInterfaceErrorRegistry,
				newStructCodec(nil),
				emptyInterfaceStructErr,
			},
			{
				// ErrNoDecoder when decoding into a struct.
				// This test uses NewRegistryBuilder().Build rather than buildDefaultRegistry to ensure that there is
				// no decoder for strings.
				"struct - no decoder found",
				stringStruct{},
				NewDocumentReader(bytes.NewReader(docBytes)),
				newTestRegistry(),
				newStructCodec(nil),
				stringStructErr,
			},
			{
				"deeply nested struct",
				outer{},
				NewDocumentReader(bytes.NewReader(outerDoc)),
				nestedRegistry,
				newStructCodec(nil),
				nestedErr,
			},
		}
		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				dc := DecodeContext{Registry: tc.registry}
				if dc.Registry == nil {
					dc.Registry = buildDefaultRegistry()
				}

				var val reflect.Value
				if rtype := reflect.TypeOf(tc.val); rtype != nil {
					val = reflect.New(rtype).Elem()
				}
				err := tc.decoder.DecodeValue(dc, tc.vr, val)
				assert.Equal(t, tc.err, err, "expected error %v, got %v", tc.err, err)
			})
		}

		t.Run("keys are correctly reversed", func(t *testing.T) {
			innerBytes := bsoncore.BuildDocumentFromElements(nil, bsoncore.AppendInt32Element(nil, "bar", 10))
			outerBytes := bsoncore.BuildDocumentFromElements(nil, bsoncore.AppendDocumentElement(nil, "foo", innerBytes))

			type inner struct{ Bar string }
			type outer struct{ Foo inner }

			dc := DecodeContext{Registry: buildDefaultRegistry()}
			vr := NewDocumentReader(bytes.NewReader(outerBytes))
			val := reflect.New(reflect.TypeOf(outer{})).Elem()
			err := newStructCodec(nil).DecodeValue(dc, vr, val)

			var decodeErr *DecodeError
			assert.True(t, errors.As(err, &decodeErr), "expected DecodeError, got %v of type %T", err, err)
			expectedKeys := []string{"foo", "bar"}
			assert.Equal(t, expectedKeys, decodeErr.Keys(), "expected keys slice %v, got %v", expectedKeys,
				decodeErr.Keys())
			keyPath := strings.Join(expectedKeys, ".")
			assert.True(t, strings.Contains(decodeErr.Error(), keyPath),
				"expected error %v to contain key pattern %s", decodeErr, keyPath)
		})
	})

	t.Run("values are converted", func(t *testing.T) {
		// When decoding into a D or M, values must be converted if they are not being decoded to the default type.

		t.Run("D", func(t *testing.T) {
			trueValue := bsoncore.Value{
				Type: bsoncore.TypeBoolean,
				Data: bsoncore.AppendBoolean(nil, true),
			}
			docBytes := bsoncore.BuildDocumentFromElements(nil,
				bsoncore.AppendBooleanElement(nil, "bool", true),
				bsoncore.BuildArrayElement(nil, "boolArray", trueValue),
			)

			reg := &Registry{
				typeEncoders: new(typeEncoderCache),
				typeDecoders: new(typeDecoderCache),
				kindEncoders: new(kindEncoderCache),
				kindDecoders: new(kindDecoderCache),
			}
			registerDefaultDecoders(reg)
			reg.RegisterTypeMapEntry(TypeBoolean, reflect.TypeOf(mybool(true)))

			dc := DecodeContext{Registry: reg}
			vr := NewDocumentReader(bytes.NewReader(docBytes))
			val := reflect.New(tD).Elem()
			err := dDecodeValue(dc, vr, val)
			assert.Nil(t, err, "DDecodeValue error: %v", err)

			want := D{
				{"bool", mybool(true)},
				{"boolArray", A{mybool(true)}},
			}
			got := val.Interface().(D)
			assert.Equal(t, want, got, "want document %v, got %v", want, got)
		})
		t.Run("M", func(t *testing.T) {
			docBytes := bsoncore.BuildDocumentFromElements(nil,
				bsoncore.AppendBooleanElement(nil, "bool", true),
			)

			type myMap map[string]mybool
			dc := DecodeContext{Registry: buildDefaultRegistry()}
			vr := NewDocumentReader(bytes.NewReader(docBytes))
			val := reflect.New(reflect.TypeOf(myMap{})).Elem()
			err := (&mapCodec{}).DecodeValue(dc, vr, val)
			assert.Nil(t, err, "DecodeValue error: %v", err)

			want := myMap{
				"bool": mybool(true),
			}
			got := val.Interface().(myMap)
			assert.Equal(t, want, got, "expected map %v, got %v", want, got)
		})
	})
}

// buildDocumentArray inserts vals inside of an array inside of a document.
func buildDocumentArray(fn func([]byte) []byte) []byte {
	aix, doc := bsoncore.AppendArrayElementStart(nil, "Z")
	doc = fn(doc)
	doc, _ = bsoncore.AppendArrayEnd(doc, aix)
	return buildDocument(doc)
}

func buildArray(vals []byte) []byte {
	aix, doc := bsoncore.AppendArrayStart(nil)
	doc = append(doc, vals...)
	doc, _ = bsoncore.AppendArrayEnd(doc, aix)
	return doc
}

func appendArrayElement(dst []byte, key string, vals []byte) []byte {
	aix, doc := bsoncore.AppendArrayElementStart(dst, key)
	doc = append(doc, vals...)
	doc, _ = bsoncore.AppendArrayEnd(doc, aix)
	return doc
}

// buildDocument inserts elems inside of a document.
func buildDocument(elems []byte) []byte {
	idx, doc := bsoncore.AppendDocumentStart(nil)
	doc = append(doc, elems...)
	doc, _ = bsoncore.AppendDocumentEnd(doc, idx)
	return doc
}

func buildDefaultRegistry() *Registry {
	reg := &Registry{
		typeEncoders: new(typeEncoderCache),
		typeDecoders: new(typeDecoderCache),
		kindEncoders: new(kindEncoderCache),
		kindDecoders: new(kindDecoderCache),
	}
	registerDefaultEncoders(reg)
	registerDefaultDecoders(reg)
	return reg
}
