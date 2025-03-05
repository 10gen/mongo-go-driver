// Copyright (C) MongoDB, Inc. 2017-present.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

package bson

import (
	"bytes"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"go.mongodb.org/mongo-driver/bson/bsoncodec"
	"go.mongodb.org/mongo-driver/bson/bsonoptions"
	"go.mongodb.org/mongo-driver/bson/bsontype"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/internal/assert"
	"go.mongodb.org/mongo-driver/x/bsonx/bsoncore"
)

func noerr(t *testing.T, err error) {
	if err != nil {
		t.Helper()
		t.Errorf("Unexpected error: (%T)%v", err, err)
		t.FailNow()
	}
}

func TestTimeRoundTrip(t *testing.T) {
	val := struct {
		Value time.Time
		ID    string
	}{
		ID: "time-rt-test",
	}

	if !val.Value.IsZero() {
		t.Errorf("Did not get zero time as expected.")
	}

	bsonOut, err := Marshal(val)
	noerr(t, err)
	rtval := struct {
		Value time.Time
		ID    string
	}{}

	err = Unmarshal(bsonOut, &rtval)
	noerr(t, err)
	if !cmp.Equal(val, rtval) {
		t.Errorf("Did not round trip properly. got %v; want %v", val, rtval)
	}
	if !rtval.Value.IsZero() {
		t.Errorf("Did not get zero time as expected.")
	}
}

func TestNonNullTimeRoundTrip(t *testing.T) {
	now := time.Now()
	now = time.Unix(now.Unix(), 0)
	val := struct {
		Value time.Time
		ID    string
	}{
		ID:    "time-rt-test",
		Value: now,
	}

	bsonOut, err := Marshal(val)
	noerr(t, err)
	rtval := struct {
		Value time.Time
		ID    string
	}{}

	err = Unmarshal(bsonOut, &rtval)
	noerr(t, err)
	if !cmp.Equal(val, rtval) {
		t.Errorf("Did not round trip properly. got %v; want %v", val, rtval)
	}
}

func TestD(t *testing.T) {
	t.Run("can marshal", func(t *testing.T) {
		d := D{{"foo", "bar"}, {"hello", "world"}, {"pi", 3.14159}}
		idx, want := bsoncore.AppendDocumentStart(nil)
		want = bsoncore.AppendStringElement(want, "foo", "bar")
		want = bsoncore.AppendStringElement(want, "hello", "world")
		want = bsoncore.AppendDoubleElement(want, "pi", 3.14159)
		want, err := bsoncore.AppendDocumentEnd(want, idx)
		noerr(t, err)
		got, err := Marshal(d)
		noerr(t, err)
		if !bytes.Equal(got, want) {
			t.Errorf("Marshaled documents do not match. got %v; want %v", Raw(got), Raw(want))
		}
	})
	t.Run("can unmarshal", func(t *testing.T) {
		want := D{{"foo", "bar"}, {"hello", "world"}, {"pi", 3.14159}}
		idx, doc := bsoncore.AppendDocumentStart(nil)
		doc = bsoncore.AppendStringElement(doc, "foo", "bar")
		doc = bsoncore.AppendStringElement(doc, "hello", "world")
		doc = bsoncore.AppendDoubleElement(doc, "pi", 3.14159)
		doc, err := bsoncore.AppendDocumentEnd(doc, idx)
		noerr(t, err)
		var got D
		err = Unmarshal(doc, &got)
		noerr(t, err)
		if !cmp.Equal(got, want) {
			t.Errorf("Unmarshaled documents do not match. got %v; want %v", got, want)
		}
	})
}

type stringerString string

func (ss stringerString) String() string {
	return "bar"
}

type keyBool bool

func (kb keyBool) MarshalKey() (string, error) {
	return fmt.Sprintf("%v", kb), nil
}

func (kb *keyBool) UnmarshalKey(key string) error {
	switch key {
	case "true":
		*kb = true
	case "false":
		*kb = false
	default:
		return fmt.Errorf("invalid bool value %v", key)
	}
	return nil
}

type keyStruct struct {
	val int64
}

func (k keyStruct) MarshalText() (text []byte, err error) {
	str := strconv.FormatInt(k.val, 10)

	return []byte(str), nil
}

func (k *keyStruct) UnmarshalText(text []byte) error {
	val, err := strconv.ParseInt(string(text), 10, 64)
	if err != nil {
		return err
	}

	*k = keyStruct{
		val: val,
	}

	return nil
}

func TestMapCodec(t *testing.T) {
	t.Run("EncodeKeysWithStringer", func(t *testing.T) {
		strstr := stringerString("foo")
		mapObj := map[stringerString]int{strstr: 1}
		testCases := []struct {
			name string
			opts *bsonoptions.MapCodecOptions
			key  string
		}{
			{"default", bsonoptions.MapCodec(), "foo"},
			{"true", bsonoptions.MapCodec().SetEncodeKeysWithStringer(true), "bar"},
			{"false", bsonoptions.MapCodec().SetEncodeKeysWithStringer(false), "foo"},
		}
		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				mapCodec := bsoncodec.NewMapCodec(tc.opts)
				mapRegistry := NewRegistryBuilder().RegisterDefaultEncoder(reflect.Map, mapCodec).Build()
				val, err := MarshalWithRegistry(mapRegistry, mapObj)
				assert.Nil(t, err, "Marshal error: %v", err)
				assert.True(t, strings.Contains(string(val), tc.key), "expected result to contain %v, got: %v", tc.key, string(val))
			})
		}
	})

	t.Run("keys implements keyMarshaler and keyUnmarshaler", func(t *testing.T) {
		mapObj := map[keyBool]int{keyBool(true): 1}

		doc, err := Marshal(mapObj)
		assert.Nil(t, err, "Marshal error: %v", err)
		idx, want := bsoncore.AppendDocumentStart(nil)
		want = bsoncore.AppendInt32Element(want, "true", 1)
		want, _ = bsoncore.AppendDocumentEnd(want, idx)
		assert.Equal(t, want, doc, "expected result %v, got %v", string(want), string(doc))

		var got map[keyBool]int
		err = Unmarshal(doc, &got)
		assert.Nil(t, err, "Unmarshal error: %v", err)
		assert.Equal(t, mapObj, got, "expected result %v, got %v", mapObj, got)

	})

	t.Run("keys implements encoding.TextMarshaler and encoding.TextUnmarshaler", func(t *testing.T) {
		mapObj := map[keyStruct]int{
			{val: 10}: 100,
		}

		doc, err := Marshal(mapObj)
		assert.Nil(t, err, "Marshal error: %v", err)
		idx, want := bsoncore.AppendDocumentStart(nil)
		want = bsoncore.AppendInt32Element(want, "10", 100)
		want, _ = bsoncore.AppendDocumentEnd(want, idx)
		assert.Equal(t, want, doc, "expected result %v, got %v", string(want), string(doc))

		var got map[keyStruct]int
		err = Unmarshal(doc, &got)
		assert.Nil(t, err, "Unmarshal error: %v", err)
		assert.Equal(t, mapObj, got, "expected result %v, got %v", mapObj, got)

	})
}

func TestExtJSONEscapeKey(t *testing.T) {
	doc := D{
		{
			Key:   "\\usb#",
			Value: int32(1),
		},
		{
			Key:   "regex",
			Value: primitive.Regex{Pattern: "ab\\\\\\\"ab", Options: "\""},
		},
	}
	b, err := MarshalExtJSON(&doc, false, false)
	noerr(t, err)

	want := `{"\\usb#":1,"regex":{"$regularExpression":{"pattern":"ab\\\\\\\"ab","options":"\""}}}`
	if diff := cmp.Diff(want, string(b)); diff != "" {
		t.Errorf("Marshaled documents do not match. got %v, want %v", string(b), want)
	}

	var got D
	err = UnmarshalExtJSON(b, false, &got)
	noerr(t, err)
	if !cmp.Equal(got, doc) {
		t.Errorf("Unmarshaled documents do not match. got %v; want %v", got, doc)
	}
}

func TestBsoncoreArray(t *testing.T) {
	type BSONDocumentArray struct {
		Array []D `bson:"array"`
	}

	type BSONArray struct {
		Array bsoncore.Array `bson:"array"`
	}

	bda := BSONDocumentArray{
		Array: []D{
			{{"x", 1}},
			{{"x", 2}},
			{{"x", 3}},
		},
	}

	expectedBSON, err := Marshal(bda)
	assert.Nil(t, err, "Marshal bsoncore.Document array error: %v", err)

	var ba BSONArray
	err = Unmarshal(expectedBSON, &ba)
	assert.Nil(t, err, "Unmarshal error: %v", err)

	actualBSON, err := Marshal(ba)
	assert.Nil(t, err, "Marshal bsoncore.Array error: %v", err)

	assert.Equal(t, expectedBSON, actualBSON,
		"expected BSON to be %v after Marshalling again; got %v", expectedBSON, actualBSON)

	doc := bsoncore.Document(actualBSON)
	v := doc.Lookup("array")
	assert.Equal(t, bsontype.Array, v.Type, "expected type array, got %v", v.Type)
}
