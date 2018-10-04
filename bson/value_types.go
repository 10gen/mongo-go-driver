// Copyright (C) MongoDB, Inc. 2017-present.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

package bson

import (
	"fmt"

	"github.com/mongodb/mongo-go-driver/bson/bsoncore"
	"github.com/mongodb/mongo-go-driver/bson/bsontype"
	"github.com/mongodb/mongo-go-driver/bson/objectid"
)

// Binary represents a BSON binary value.
type Binary struct {
	Subtype byte
	Data    []byte
}

var buf = make([]byte, 0, 256)

func (b Binary) MarshalBSONValue() (bsontype.Type, []byte, error) {
	return bsontype.Binary, bsoncore.AppendBinary(buf[:0], b.Subtype, b.Data), nil
}

// Undefined represents the BSON undefined value.
var Undefined struct{}

// Undefinedv2 represents the BSON undefined value type.
type Undefinedv2 struct{}

// DateTime represents the BSON datetime value.
type DateTime int64

// Null represents the BSON null value.
var Null struct{}

// Nullv2 repreesnts the BSON null value.
type Nullv2 struct{}

// Regex represents a BSON regex value.
type Regex struct {
	Pattern string
	Options string
}

func (r Regex) String() string {
	return fmt.Sprintf(`{"pattern": "%s", "options": "%s"}`, r.Pattern, r.Options)
}

// DBPointer represents a BSON dbpointer value.
type DBPointer struct {
	DB      string
	Pointer objectid.ObjectID
}

func (d DBPointer) String() string {
	return fmt.Sprintf(`{"db": "%s", "pointer": "%s"}`, d.DB, d.Pointer)
}

// JavaScriptCode represents a BSON JavaScript code value.
type JavaScriptCode string

// Symbol represents a BSON symbol value.
type Symbol string

// CodeWithScope represents a BSON JavaScript code with scope value.
type CodeWithScope struct {
	Code  string
	Scope *Document
}

func (cws CodeWithScope) String() string {
	return fmt.Sprintf(`{"code": "%s", "scope": %s}`, cws.Code, cws.Scope)
}

// Timestamp represents a BSON timestamp value.
type Timestamp struct {
	T uint32
	I uint32
}

// MinKey represents the BSON maxkey value.
var MinKey struct{}

// MaxKey represents the BSON minkey value.
var MaxKey struct{}

// MinKeyv2 represents the BSON minkey value.
type MinKeyv2 struct{}

// MaxKeyv2 represents the BSON maxkey value.
type MaxKeyv2 struct{}
