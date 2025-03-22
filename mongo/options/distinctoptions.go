// Copyright (C) MongoDB, Inc. 2017-present.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

package options

// DistinctOptions represents arguments that can be used to configure a Distinct
// operation.
//
// See corresponding setter methods for documentation.
type DistinctOptions struct {
	Collation *Collation
	Comment   interface{}
	Hint      interface{}
}

// DistinctOptionsBuilder contains options to configure distinct operations. Each
// option can be set through setter functions. See documentation for each setter
// function for an explanation of the option.
type DistinctOptionsBuilder struct {
	Opts []func(*DistinctOptions) error
}

// Distinct creates a new DistinctOptions instance.
func Distinct() *DistinctOptionsBuilder {
	return &DistinctOptionsBuilder{}
}

// List returns a list of DistinctArg setter functions.
func (do *DistinctOptionsBuilder) List() []func(*DistinctOptions) error {
	return do.Opts
}

// SetCollation sets the value for the Collation field. Specifies a collation to
// use for string comparisons during the operation. The default value is nil,
// which means the default collation of the collection will be used.
func (do *DistinctOptionsBuilder) SetCollation(c *Collation) *DistinctOptionsBuilder {
	do.Opts = append(do.Opts, func(opts *DistinctOptions) error {
		opts.Collation = c

		return nil
	})

	return do
}

// SetComment sets the value for the Comment field. Specifies a string or document that
// will be included in server logs, profiling logs, and currentOp queries to help trace
// the operation. The default value is nil, which means that no comment will be included
// in the logs.
func (do *DistinctOptionsBuilder) SetComment(comment interface{}) *DistinctOptionsBuilder {
	do.Opts = append(do.Opts, func(opts *DistinctOptions) error {
		opts.Comment = comment

		return nil
	})

	return do
}

// SetHint specifies the index to use for the operation. This should either be
// the index name as a string or the index specification as a document. This
// option is only valid for MongoDB versions >= 7.1. Previous server versions
// will return an error if an index hint is specified. Distinct returns an error
// if the hint parameter is a multi-key map. The default value is nil, which
// means that no index hint will be sent.
//
// SetHint sets the Hint field.
func (do *DistinctOptionsBuilder) SetHint(hint interface{}) *DistinctOptionsBuilder {
	do.Opts = append(do.Opts, func(opts *DistinctOptions) error {
		opts.Hint = hint

		return nil
	})

	return do
}
