// Copyright (C) MongoDB, Inc. 2017-present.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

package readpref

import (
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/internal/assert"
	"go.mongodb.org/mongo-driver/v2/internal/ptrutil"
)

func TestNew(t *testing.T) {
	t.Parallel()

	tagSets := []TagSet{
		{
			{Name: "a", Value: "1"},
			{Name: "b", Value: "2"},
		},
	}

	tests := []struct {
		name    string
		mode    Mode
		opts    []*Builder
		want    *ReadPref
		wantErr error
	}{
		{
			name:    "primary",
			mode:    PrimaryMode,
			opts:    nil,
			want:    &ReadPref{Mode: PrimaryMode},
			wantErr: nil,
		},
		{
			name:    "primary with maxStaleness",
			mode:    PrimaryMode,
			opts:    []*Builder{Options().SetMaxStaleness(1)},
			want:    nil,
			wantErr: errInvalidReadPreference,
		},
		{
			name:    "primary with tags",
			mode:    PrimaryMode,
			opts:    []*Builder{Options().SetTagSets([]TagSet{{}})},
			want:    nil,
			wantErr: errInvalidReadPreference,
		},
		{
			name:    "primary with hedgeEnabled",
			mode:    PrimaryMode,
			opts:    []*Builder{Options().SetHedgeEnabled(false)},
			want:    nil,
			wantErr: errInvalidReadPreference,
		},
		{
			name:    "primaryPreferred",
			mode:    PrimaryPreferredMode,
			opts:    nil,
			want:    &ReadPref{Mode: PrimaryPreferredMode},
			wantErr: nil,
		},
		{
			name: "primaryPreferred with options",
			mode: PrimaryPreferredMode,
			opts: []*Builder{Options().SetMaxStaleness(1).SetTagSets(tagSets)},
			want: &ReadPref{
				Mode:         PrimaryPreferredMode,
				maxStaleness: ptrutil.Ptr[time.Duration](1),
				tagSets:      tagSets,
			},
			wantErr: nil,
		},
		{
			name:    "secondary",
			mode:    SecondaryMode,
			opts:    nil,
			want:    &ReadPref{Mode: SecondaryMode},
			wantErr: nil,
		},
		{
			name: "secondary with options",
			mode: SecondaryMode,
			opts: []*Builder{Options().SetMaxStaleness(1).SetTagSets(tagSets)},
			want: &ReadPref{
				Mode:         SecondaryMode,
				maxStaleness: ptrutil.Ptr[time.Duration](1),
				tagSets:      tagSets,
			},
			wantErr: nil,
		},
		{
			name:    "nearest",
			mode:    NearestMode,
			opts:    nil,
			want:    &ReadPref{Mode: NearestMode},
			wantErr: nil,
		},
		{
			name: "nearest with options",
			mode: NearestMode,
			opts: []*Builder{Options().SetMaxStaleness(1).SetTagSets(tagSets)},
			want: &ReadPref{
				Mode:         NearestMode,
				maxStaleness: ptrutil.Ptr[time.Duration](1),
				tagSets:      tagSets,
			},
			wantErr: nil,
		},
	}

	for _, test := range tests {
		test := test

		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			readPref, err := New(test.mode, test.opts...)

			if test.wantErr == nil {
				assert.NoError(t, err)
			} else {
				assert.ErrorIs(t, err, test.wantErr)
			}

			if test.want == nil {
				return
			}

			assert.Equal(t, test.mode, readPref.Mode)
			assert.EqualValues(t, test.want, readPref)
		})
	}
}

func TestReadPref_String(t *testing.T) {
	t.Run("ReadPref.String() with all options", func(t *testing.T) {
		opts := Options().SetMaxStaleness(120 * time.Second).SetHedgeEnabled(true).SetTagSets([]TagSet{
			{{"a", "1"}, {"b", "2"}},
			{{"q", "5"}, {"r", "6"}},
		})

		readPref, err := New(NearestMode, opts)
		assert.NoError(t, err)

		expected := "nearest(maxStaleness=2m0s tagSet=a=1,b=2 tagSet=q=5,r=6 hedgeEnabled=true)"
		assert.Equal(t, expected, readPref.String(), "expected %q, got %q", expected, readPref.String())
	})
	t.Run("ReadPref.String() with one option", func(t *testing.T) {
		opts := Options().SetTagSets([]TagSet{{{"a", "1"}, {"b", "2"}}})

		readPref, err := New(SecondaryMode, opts)
		assert.NoError(t, err)

		expected := "secondary(tagSet=a=1,b=2)"
		assert.Equal(t, expected, readPref.String(), "expected %q, got %q", expected, readPref.String())
	})
	t.Run("ReadPref.String() with no options", func(t *testing.T) {
		readPref := Primary()
		expected := "primary"
		assert.Equal(t, expected, readPref.String(), "expected %q, got %q", expected, readPref.String())
	})
}
