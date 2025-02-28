// Copyright (C) MongoDB, Inc. 2017-present.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

package bson

import (
	"fmt"
	"reflect"
	"strings"
)

var (
	emptyValue = reflect.Value{}
)

// ValueEncoderError is an error returned from a ValueEncoder when the provided value can't be
// encoded by the ValueEncoder.
type ValueEncoderError struct {
	Name     string
	Types    []reflect.Type
	Kinds    []reflect.Kind
	Received reflect.Value
}

func (vee ValueEncoderError) Error() string {
	typeKinds := make([]string, 0, len(vee.Types)+len(vee.Kinds))
	for _, t := range vee.Types {
		typeKinds = append(typeKinds, t.String())
	}
	for _, k := range vee.Kinds {
		if k == reflect.Map {
			typeKinds = append(typeKinds, "map[string]*")
			continue
		}
		typeKinds = append(typeKinds, k.String())
	}
	received := vee.Received.Kind().String()
	if vee.Received.IsValid() {
		received = vee.Received.Type().String()
	}
	return fmt.Sprintf("%s can only encode valid %s, but got %s", vee.Name, strings.Join(typeKinds, ", "), received)
}

// ValueDecoderError is an error returned from a ValueDecoder when the provided value can't be
// decoded by the ValueDecoder.
type ValueDecoderError struct {
	Name     string
	Types    []reflect.Type
	Kinds    []reflect.Kind
	Received reflect.Value
}

func (vde ValueDecoderError) Error() string {
	typeKinds := make([]string, 0, len(vde.Types)+len(vde.Kinds))
	for _, t := range vde.Types {
		typeKinds = append(typeKinds, t.String())
	}
	for _, k := range vde.Kinds {
		if k == reflect.Map {
			typeKinds = append(typeKinds, "map[string]*")
			continue
		}
		typeKinds = append(typeKinds, k.String())
	}
	received := vde.Received.Kind().String()
	if vde.Received.IsValid() {
		received = vde.Received.Type().String()
	}
	return fmt.Sprintf("%s can only decode valid and settable %s, but got %s", vde.Name, strings.Join(typeKinds, ", "), received)
}

// EncodeContext is the contextual information required for a Codec to encode a
// value.
type EncodeContext struct {
	*Registry

	// minSize causes the Encoder to marshal Go integer values (int, int8, int16, int32, int64,
	// uint, uint8, uint16, uint32, or uint64) as the minimum BSON int size (either 32 or 64 bits)
	// that can represent the integer value.
	minSize bool

	errorOnInlineDuplicates bool
	stringifyMapKeysWithFmt bool
	nilMapAsEmpty           bool
	nilSliceAsEmpty         bool
	nilByteSliceAsEmpty     bool
	omitZeroStruct          bool
	omitEmpty               bool
	useJSONStructTags       bool
}

// DecodeContext is the contextual information required for a Codec to decode a
// value.
type DecodeContext struct {
	*Registry

	// truncate, if true, instructs decoders to to truncate the fractional part of BSON "double"
	// values when attempting to unmarshal them into a Go integer (int, int8, int16, int32, int64,
	// uint, uint8, uint16, uint32, or uint64) struct field. The truncation logic does not apply to
	// BSON "decimal128" values.
	truncate bool

	// defaultDocumentType specifies the Go type to decode top-level and nested BSON documents into. In particular, the
	// usage for this field is restricted to data typed as "interface{}" or "map[string]interface{}". If DocumentType is
	// set to a type that a BSON document cannot be unmarshaled into (e.g. "string"), unmarshalling will result in an
	// error.
	defaultDocumentType reflect.Type

	binaryAsSlice bool

	// a false value results in a decoding error.
	objectIDAsHexString bool

	useJSONStructTags bool
	useLocalTimeZone  bool
	zeroMaps          bool
	zeroStructs       bool
}

// ValueEncoder is the interface implemented by types that can encode a provided Go type to BSON.
// The value to encode is provided as a reflect.Value and a bson.ValueWriter is used within the
// EncodeValue method to actually create the BSON representation. For convenience, ValueEncoderFunc
// is provided to allow use of a function with the correct signature as a ValueEncoder. An
// EncodeContext instance is provided to allow implementations to lookup further ValueEncoders and
// to provide configuration information.
type ValueEncoder interface {
	EncodeValue(EncodeContext, ValueWriter, reflect.Value) error
}

// ValueEncoderFunc is an adapter function that allows a function with the correct signature to be
// used as a ValueEncoder.
type ValueEncoderFunc func(EncodeContext, ValueWriter, reflect.Value) error

// EncodeValue implements the ValueEncoder interface.
func (fn ValueEncoderFunc) EncodeValue(ec EncodeContext, vw ValueWriter, val reflect.Value) error {
	return fn(ec, vw, val)
}

// ValueDecoder is the interface implemented by types that can decode BSON to a provided Go type.
// Implementations should ensure that the value they receive is settable. Similar to ValueEncoderFunc,
// ValueDecoderFunc is provided to allow the use of a function with the correct signature as a
// ValueDecoder. A DecodeContext instance is provided and serves similar functionality to the
// EncodeContext.
type ValueDecoder interface {
	DecodeValue(DecodeContext, ValueReader, reflect.Value) error
}

// ValueDecoderFunc is an adapter function that allows a function with the correct signature to be
// used as a ValueDecoder.
type ValueDecoderFunc func(DecodeContext, ValueReader, reflect.Value) error

// DecodeValue implements the ValueDecoder interface.
func (fn ValueDecoderFunc) DecodeValue(dc DecodeContext, vr ValueReader, val reflect.Value) error {
	return fn(dc, vr, val)
}

// typeDecoder is the interface implemented by types that can handle the decoding of a value given its type.
type typeDecoder interface {
	decodeType(DecodeContext, ValueReader, reflect.Type) (reflect.Value, error)
}

// typeDecoderFunc is an adapter function that allows a function with the correct signature to be used as a typeDecoder.
type typeDecoderFunc func(DecodeContext, ValueReader, reflect.Type) (reflect.Value, error)

func (fn typeDecoderFunc) decodeType(dc DecodeContext, vr ValueReader, t reflect.Type) (reflect.Value, error) {
	return fn(dc, vr, t)
}

// decodeAdapter allows two functions with the correct signatures to be used as both a ValueDecoder and typeDecoder.
type decodeAdapter struct {
	ValueDecoderFunc
	typeDecoderFunc
}

var _ ValueDecoder = decodeAdapter{}
var _ typeDecoder = decodeAdapter{}

func decodeTypeOrValueWithInfo(vd ValueDecoder, dc DecodeContext, vr ValueReader, t reflect.Type) (reflect.Value, error) {
	if td, _ := vd.(typeDecoder); td != nil {
		val, err := td.decodeType(dc, vr, t)
		if err == nil && val.Type() != t {
			// This conversion step is necessary for slices and maps. If a user declares variables like:
			//
			// type myBool bool
			// var m map[string]myBool
			//
			// and tries to decode BSON bytes into the map, the decoding will fail if this conversion is not present
			// because we'll try to assign a value of type bool to one of type myBool.
			val = val.Convert(t)
		}
		return val, err
	}

	val := reflect.New(t).Elem()
	err := vd.DecodeValue(dc, vr, val)
	return val, err
}
