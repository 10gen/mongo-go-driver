// Copyright (C) MongoDB, Inc. 2022-present.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

// fuzz_test.go is used by the "oss-fuzz" integration. Use caution when
// modifying this file because it may break that integration.
//
// See https://github.com/google/oss-fuzz/tree/master/projects/mongo-go-driver

package bson

import (
	"testing"
)

func FuzzDecode(f *testing.F) {
	seedBSONCorpus(f)

	f.Fuzz(func(t *testing.T, data []byte) {
		for _, typ := range []func() interface{}{
			func() interface{} { return new(D) },
			func() interface{} { return new([]E) },
			func() interface{} { return new(M) },
			func() interface{} { return new(interface{}) },
			func() interface{} { return make(map[string]interface{}) },
			func() interface{} { return new([]interface{}) },
		} {
			i := typ()
			if err := Unmarshal(data, i); err != nil {
				return
			}

			encoded, err := Marshal(i)
			if err != nil {
				t.Fatalf("failed to marshal: %v", err)
			}

			if err := Unmarshal(encoded, i); err != nil {
				t.Fatalf("failed to unmarshal: %v", err)
			}
		}
	})
}
