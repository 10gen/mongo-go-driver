// Copyright (C) MongoDB, Inc. 2017-present.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

package options

// CountOptions represents arguments that can be used to configure a
// CountDocuments operation.
//
// See corresponding setter methods for documentation.
type CountOptions struct {
	Collation *Collation
	Comment   interface{}
	Hint      interface{}
	Limit     *int64
	Skip      *int64
}

// CountOptionsBuilder contains options to configure count operations. Each
// option can be set through setter functions. See documentation for each setter
// function for an explanation of the option.
type CountOptionsBuilder struct {
	Opts []func(*CountOptions) error
}

// Count creates a new CountOptions instance.
func Count() *CountOptionsBuilder {
	return &CountOptionsBuilder{}
}

// List returns a list of CountOptions setter functions.
func (co *CountOptionsBuilder) List() []func(*CountOptions) error {
	return co.Opts
}

// SetCollation sets the value for the Collation field. Specifies a collation to
// use for string comparisons during the operation. The default value is nil,
// which means the default collation of the collection will be used.
func (co *CountOptionsBuilder) SetCollation(c *Collation) *CountOptionsBuilder {
	co.Opts = append(co.Opts, func(opts *CountOptions) error {
		opts.Collation = c

		return nil
	})

	return co
}

// SetComment sets the value for the Comment field. Specifies a string or document that will be included
// in server logs, profiling logs, and currentOp queries to help trace the operation. The default is nil,
// which means that no comment will be included in the logs.
func (co *CountOptionsBuilder) SetComment(comment interface{}) *CountOptionsBuilder {
	co.Opts = append(co.Opts, func(opts *CountOptions) error {
		opts.Comment = comment

		return nil
	})

	return co
}

// SetHint sets the value for the Hint field. Specifies the index to use for the aggregation. This should
// either be the index name as a string or the index specification as a document. The driver will return
// an error if the hint parameter is a multi-key map. The default value is nil, which means that no hint
// will be sent.
func (co *CountOptionsBuilder) SetHint(h interface{}) *CountOptionsBuilder {
	co.Opts = append(co.Opts, func(opts *CountOptions) error {
		opts.Hint = h

		return nil
	})

	return co
}

// SetLimit sets the value for the Limit field. Specifies the maximum number of documents to count. The
// default value is 0, which means that there is no limit and all documents matching the filter will be
// counted.
func (co *CountOptionsBuilder) SetLimit(i int64) *CountOptionsBuilder {
	co.Opts = append(co.Opts, func(opts *CountOptions) error {
		opts.Limit = &i

		return nil
	})

	return co
}

// SetSkip sets the value for the Skip field. Specifies the number of documents to skip before counting.
// The default value is 0.
func (co *CountOptionsBuilder) SetSkip(i int64) *CountOptionsBuilder {
	co.Opts = append(co.Opts, func(opts *CountOptions) error {
		opts.Skip = &i

		return nil
	})

	return co
}
