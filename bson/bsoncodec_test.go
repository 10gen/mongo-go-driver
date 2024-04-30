// Copyright (C) MongoDB, Inc. 2017-present.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

package bson

import (
	"reflect"
	"testing"
)

type llCodec struct {
	t         *testing.T
	decodeval interface{}
	encodeval interface{}
	err       error
}

func (llc *llCodec) EncodeValue(_ EncodeContext, _ ValueWriter, i interface{}) error {
	if llc.err != nil {
		return llc.err
	}

	llc.encodeval = i
	return nil
}

func (llc *llCodec) DecodeValue(_ DecodeContext, _ ValueReader, val reflect.Value) error {
	if llc.err != nil {
		return llc.err
	}

	if !reflect.TypeOf(llc.decodeval).AssignableTo(val.Type()) {
		llc.t.Errorf("decodeval must be assignable to val provided to DecodeValue, but is not. decodeval %T; val %T", llc.decodeval, val)
		return nil
	}

	val.Set(reflect.ValueOf(llc.decodeval))
	return nil
}
