// Copyright (C) MongoDB, Inc. 2017-present.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

package bson

import (
	"bytes"
	"encoding/json"
	"sync"
)

const defaultDstCap = 256

var extjPool = NewExtJSONValueWriterPool()

// Marshaler is the interface implemented by types that can marshal themselves
// into a valid BSON document.
//
// Implementations of Marshaler must return a full BSON document. To create
// custom BSON marshaling behavior for individual values in a BSON document,
// implement the ValueMarshaler interface instead.
type Marshaler interface {
	MarshalBSON() ([]byte, error)
}

// ValueMarshaler is the interface implemented by types that can marshal
// themselves into a valid BSON value. The format of the returned bytes must
// match the returned type.
//
// Implementations of ValueMarshaler must return an individual BSON value. To
// create custom BSON marshaling behavior for an entire BSON document, implement
// the Marshaler interface instead.
type ValueMarshaler interface {
	MarshalBSONValue() (Type, []byte, error)
}

// Pool of buffers for marshalling BSON.
var bufPool = sync.Pool{
	New: func() interface{} {
		return new(bytes.Buffer)
	},
}

// Marshal returns the BSON encoding of val as a BSON document. If val is not a type that can be transformed into a
// document, MarshalValue should be used instead.
//
// Marshal will use the default registry created by NewRegistry to recursively
// marshal val into a []byte. Marshal will inspect struct tags and alter the
// marshaling process accordingly.
func Marshal(val interface{}) ([]byte, error) {
	sw := bufPool.Get().(*bytes.Buffer)
	defer func() {
		// Proper usage of a sync.Pool requires each entry to have approximately
		// the same memory cost. To obtain this property when the stored type
		// contains a variably-sized buffer, we add a hard limit on the maximum
		// buffer to place back in the pool. We limit the size to 16MiB because
		// that's the maximum wire message size supported by any current MongoDB
		// server.
		//
		// Comment based on
		// https://cs.opensource.google/go/go/+/refs/tags/go1.19:src/fmt/print.go;l=147
		//
		// Recycle byte slices that are smaller than 16MiB and at least half
		// occupied.
		if sw.Cap() < 16*1024*1024 && sw.Cap()/2 < sw.Len() {
			bufPool.Put(sw)
		}
	}()
	sw.Reset()
	vw := NewValueWriter(sw)
	enc := encPool.Get().(*Encoder)
	defer encPool.Put(enc)
	enc.Reset(vw)
	enc.SetRegistry(DefaultRegistry)
	err := enc.Encode(val)
	if err != nil {
		return nil, err
	}
	buf := append([]byte(nil), sw.Bytes()...)
	return buf, nil
}

// MarshalValue returns the BSON encoding of val.
//
// MarshalValue will use bson.DefaultRegistry to transform val into a BSON value. If val is a struct, this function will
// inspect struct tags and alter the marshalling process accordingly.
func MarshalValue(val interface{}) (Type, []byte, error) {
	return MarshalValueWithRegistry(DefaultRegistry, val)
}

// MarshalValueWithRegistry returns the BSON encoding of val using Registry r.
//
// Deprecated: Using a custom registry to marshal individual BSON values will not be supported in Go
// Driver 2.0.
func MarshalValueWithRegistry(r *Registry, val interface{}) (Type, []byte, error) {
	sw := SliceWriter(make([]byte, 0))
	vwFlusher := bvwPool.GetAtModeElement(&sw)

	// get an Encoder and encode the value
	enc := encPool.Get().(*Encoder)
	defer encPool.Put(enc)
	enc.Reset(vwFlusher)
	enc.ec = EncodeContext{Registry: r}
	if err := enc.Encode(val); err != nil {
		return 0, nil, err
	}

	// flush the bytes written because we cannot guarantee that a full document has been written
	// after the flush, *sw will be in the format
	// [value type, 0 (null byte to indicate end of empty element name), value bytes..]
	if err := vwFlusher.Flush(); err != nil {
		return 0, nil, err
	}
	return Type(sw[0]), sw[2:], nil
}

// MarshalExtJSON returns the extended JSON encoding of val.
func MarshalExtJSON(val interface{}, canonical, escapeHTML bool) ([]byte, error) {
	sw := SliceWriter(make([]byte, 0, defaultDstCap))
	ejvw := extjPool.Get(&sw, canonical, escapeHTML)
	defer extjPool.Put(ejvw)

	enc := encPool.Get().(*Encoder)
	defer encPool.Put(enc)

	enc.Reset(ejvw)
	enc.ec = EncodeContext{Registry: DefaultRegistry}

	err := enc.Encode(val)
	if err != nil {
		return nil, err
	}

	return sw, nil
}

// IndentExtJSON will prefix and indent the provided extended JSON src and append it to dst.
func IndentExtJSON(dst *bytes.Buffer, src []byte, prefix, indent string) error {
	return json.Indent(dst, src, prefix, indent)
}

// MarshalExtJSONIndent returns the extended JSON encoding of val with each line with prefixed
// and indented.
func MarshalExtJSONIndent(val interface{}, canonical, escapeHTML bool, prefix, indent string) ([]byte, error) {
	marshaled, err := MarshalExtJSON(val, canonical, escapeHTML)
	if err != nil {
		return nil, err
	}

	var buf bytes.Buffer
	err = IndentExtJSON(&buf, marshaled, prefix, indent)
	if err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}
