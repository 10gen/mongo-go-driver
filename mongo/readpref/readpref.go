// Copyright (C) MongoDB, Inc. 2017-present.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

// Package readpref defines read preferences for MongoDB queries.
package readpref

import (
	"bytes"
	"errors"
	"fmt"
	"time"
)

var errInvalidReadPreference = errors.New("can not specify tags, max staleness, or hedge with mode primary")

// ReadPref determines which servers are considered suitable for read operations.
type ReadPref struct {
	Mode Mode

	maxStaleness *time.Duration
	tagSets      []TagSet
	hedgeEnabled *bool
}

// Builder contains options to configure a ReadPref object. Each option
// can be set through setter functions. See documentation for each setter
// function for an explanation of the option.
type Builder struct {
	opts []func(*ReadPref)
}

// Options creates a new Builder instance.
func Options() *Builder {
	return &Builder{}
}

// List returns a list of ReadPref setter functions.
func (bldr *Builder) List() []func(*ReadPref) {
	return bldr.opts
}

// SetMaxStaleness sets the value for the MaxStaleness field which is the
// maximum amount of time to allow a server to be considered eligible for
// selection.
func (bldr *Builder) SetMaxStaleness(dur time.Duration) *Builder {
	bldr.opts = append(bldr.opts, func(opts *ReadPref) {
		opts.maxStaleness = &dur
	})

	return bldr
}

// SetTagSets sets the multiple tag sets indicating which servers should be
// considered.
func (bldr *Builder) SetTagSets(sets []TagSet) *Builder {
	bldr.opts = append(bldr.opts, func(opts *ReadPref) {
		opts.tagSets = sets
	})

	return bldr
}

// SetHedgeEnabled sets whether or not hedged reads are enabled for this read
// preference.
func (bldr *Builder) SetHedgeEnabled(hedgeEnabled bool) *Builder {
	bldr.opts = append(bldr.opts, func(opts *ReadPref) {
		opts.hedgeEnabled = &hedgeEnabled
	})

	return bldr
}

func validOpts(mode Mode, opts *ReadPref) bool {
	if opts == nil || mode != PrimaryMode {
		return true
	}

	return opts.maxStaleness == nil && len(opts.tagSets) == 0 && opts.hedgeEnabled == nil
}

func mergeBuilders(builders ...*Builder) *ReadPref {
	opts := new(ReadPref)
	for _, bldr := range builders {
		if bldr == nil {
			continue
		}

		for _, setterFn := range bldr.List() {
			setterFn(opts)
		}
	}

	return opts
}

// Primary constructs a read preference with a PrimaryMode.
func Primary() *ReadPref {
	return &ReadPref{Mode: PrimaryMode}
}

// PrimaryPreferred constructs a read preference with a PrimaryPreferredMode.
func PrimaryPreferred(opts ...*Builder) *ReadPref {
	// New only returns an error with a mode of Primary
	rp, _ := New(PrimaryPreferredMode, opts...)
	return rp
}

// SecondaryPreferred constructs a read preference with a SecondaryPreferredMode.
func SecondaryPreferred(opts ...*Builder) *ReadPref {
	// New only returns an error with a mode of Primary
	rp, _ := New(SecondaryPreferredMode, opts...)
	return rp
}

// Secondary constructs a read preference with a SecondaryMode.
func Secondary(opts ...*Builder) *ReadPref {
	// New only returns an error with a mode of Primary
	rp, _ := New(SecondaryMode, opts...)
	return rp
}

// Nearest constructs a read preference with a NearestMode.
func Nearest(opts ...*Builder) *ReadPref {
	// New only returns an error with a mode of Primary
	rp, _ := New(NearestMode, opts...)
	return rp
}

// New creates a new ReadPref.
func New(mode Mode, builders ...*Builder) (*ReadPref, error) {
	rp := mergeBuilders(builders...)
	rp.Mode = mode

	if !validOpts(mode, rp) {
		return nil, errInvalidReadPreference
	}

	return rp, nil
}

// MaxStaleness is the maximum amount of time to allow
// a server to be considered eligible for selection. The
// second return value indicates if this value has been set.
func (r *ReadPref) MaxStaleness() *time.Duration {
	return r.maxStaleness
}

// TagSets are multiple tag sets indicating
// which servers should be considered.
func (r *ReadPref) TagSets() []TagSet {
	return r.tagSets
}

// HedgeEnabled returns whether or not hedged reads are enabled for this read preference. If this option was not
// specified during read preference construction, nil is returned.
func (r *ReadPref) HedgeEnabled() *bool {
	return r.hedgeEnabled
}

// String returns a human-readable description of the read preference.
func (r *ReadPref) String() string {
	var b bytes.Buffer
	b.WriteString(string(r.Mode))
	delim := "("
	if r.MaxStaleness() != nil {
		fmt.Fprintf(&b, "%smaxStaleness=%v", delim, *r.MaxStaleness())
		delim = " "
	}
	for _, tagSet := range r.TagSets() {
		fmt.Fprintf(&b, "%stagSet=%s", delim, tagSet.String())
		delim = " "
	}
	if r.HedgeEnabled() != nil {
		fmt.Fprintf(&b, "%shedgeEnabled=%v", delim, *r.HedgeEnabled())
		delim = " "
	}
	if delim != "(" {
		b.WriteString(")")
	}
	return b.String()
}
