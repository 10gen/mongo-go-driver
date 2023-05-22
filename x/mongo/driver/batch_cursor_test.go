// Copyright (C) MongoDB, Inc. 2022-present.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

package driver

import (
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/internal/assert"
)

func TestBatchCursor(t *testing.T) {
	t.Parallel()

	t.Run("setBatchSize", func(t *testing.T) {
		t.Parallel()

		var size int32
		bc := &BatchCursor{
			batchSize: size,
		}
		assert.Equal(t, size, bc.batchSize, "expected batchSize %v, got %v", size, bc.batchSize)

		size = int32(4)
		bc.SetBatchSize(size)
		assert.Equal(t, size, bc.batchSize, "expected batchSize %v, got %v", size, bc.batchSize)
	})

	t.Run("calcGetMoreBatchSize", func(t *testing.T) {
		t.Parallel()

		for _, tcase := range []struct {
			name                               string
			size, limit, numReturned, expected int32
			ok                                 bool
		}{
			{
				name:     "empty",
				expected: 0,
				ok:       true,
			},
			{
				name:     "batchSize NEQ 0",
				size:     4,
				expected: 4,
				ok:       true,
			},
			{
				name:     "limit NEQ 0",
				limit:    4,
				expected: 0,
				ok:       true,
			},
			{
				name:        "limit NEQ and batchSize + numReturned EQ limit",
				size:        4,
				limit:       8,
				numReturned: 4,
				expected:    4,
				ok:          true,
			},
			{
				name:        "limit makes batchSize negative",
				numReturned: 4,
				limit:       2,
				expected:    -2,
				ok:          false,
			},
		} {
			tcase := tcase
			t.Run(tcase.name, func(t *testing.T) {
				t.Parallel()

				bc := &BatchCursor{
					limit:       tcase.limit,
					batchSize:   tcase.size,
					numReturned: tcase.numReturned,
				}

				bc.SetBatchSize(tcase.size)

				size, ok := calcGetMoreBatchSize(*bc)

				assert.Equal(t, tcase.expected, size, "expected batchSize %v, got %v", tcase.expected, size)
				assert.Equal(t, tcase.ok, ok, "expected ok %v, got %v", tcase.ok, ok)
			})
		}
	})
}

func TestBatchCursorSetComment(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		comment interface{}
		want    string
	}{
		{
			name:    "empty",
			comment: nil,
			want:    "",
		},
		{
			name:    "bson.D",
			comment: bson.D{{"foo", "bar"}},
			want:    `{"foo": "bar"}`,
		},
		{
			name:    "map",
			comment: map[string]interface{}{"foo": "bar"},
			want:    `{"foo": "bar"}`,
		},
		{
			name:    "struct",
			comment: struct{ Foo string }{Foo: "bar"},
			want:    `{"foo": "bar"}`,
		},
		{
			name:    "non-document type",
			comment: "foo: bar",
			want:    "",
		},
	}

	for _, test := range tests {
		test := test

		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			bc := BatchCursor{}
			bc.SetComment(test.comment)

			got := bc.comment.String()
			if got != test.want {
				t.Fatalf("bc.comment=%v, want %v", got, test.want)
			}
		})
	}
}

func TestBatchCursorSetMaxTime(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		dur  time.Duration
		want int64
	}{
		{
			name: "empty",
			dur:  0,
			want: 0,
		},
		{
			name: "non-specified (nanosecond) input",
			// 10 million nanoseconds = 10 millseconds
			dur:  time.Duration(10_000_000),
			want: 10,
		},
		{
			name: "non-millisecond input",
			dur:  10_000 * time.Microsecond,
			want: 10,
		},
		{
			name: "millisecond input",
			dur:  10 * time.Millisecond,
			want: 10,
		},
	}

	for _, test := range tests {
		test := test

		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			bc := BatchCursor{}
			bc.SetMaxTime(test.dur)

			got := bc.maxTimeMS
			if got != test.want {
				t.Fatalf("bc.maxTimeMS=%v, want %v", got, test.want)
			}
		})
	}
}
