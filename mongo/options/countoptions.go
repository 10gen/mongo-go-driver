// Copyright (C) MongoDB, Inc. 2017-present.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

package options

// CountOptions represents arguments that can be used to configure a
// CountDocuments operation.
type CountOptions struct {
	// Specifies a collation to use for string comparisons during the operation. This option is only valid for MongoDB
	// versions >= 3.4. For previous server versions, the driver will return an error if this option is used. The
	// default value is nil, which means the default collation of the collection will be used.
	Collation *Collation

	// A string or document that will be included in server logs, profiling logs, and currentOp queries to help trace
	// the operation.  The default is nil, which means that no comment will be included in the logs.
	Comment interface{}

	// The index to use for the aggregation. This should either be the index name as a string or the index specification
	// as a document. The driver will return an error if the hint parameter is a multi-key map. The default value is nil,
	// which means that no hint will be sent.
	Hint interface{}

	// The maximum number of documents to count. The default value is 0, which means that there is no limit and all
	// documents matching the filter will be counted.
	Limit *int64

	// The number of documents to skip before counting. The default value is 0.
	Skip *int64
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

// ArgsSetters returns a list of CountArgs setter functions.
func (co *CountOptionsBuilder) ArgsSetters() []func(*CountOptions) error {
	return co.Opts
}

// SetCollation sets the value for the Collation field.
func (co *CountOptionsBuilder) SetCollation(c *Collation) *CountOptionsBuilder {
	co.Opts = append(co.Opts, func(args *CountOptions) error {
		args.Collation = c

		return nil
	})

	return co
}

// SetComment sets the value for the Comment field.
func (co *CountOptionsBuilder) SetComment(comment interface{}) *CountOptionsBuilder {
	co.Opts = append(co.Opts, func(args *CountOptions) error {
		args.Comment = comment

		return nil
	})

	return co
}

// SetHint sets the value for the Hint field.
func (co *CountOptionsBuilder) SetHint(h interface{}) *CountOptionsBuilder {
	co.Opts = append(co.Opts, func(args *CountOptions) error {
		args.Hint = h

		return nil
	})

	return co
}

// SetLimit sets the value for the Limit field.
func (co *CountOptionsBuilder) SetLimit(i int64) *CountOptionsBuilder {
	co.Opts = append(co.Opts, func(args *CountOptions) error {
		args.Limit = &i

		return nil
	})

	return co
}

// SetSkip sets the value for the Skip field.
func (co *CountOptionsBuilder) SetSkip(i int64) *CountOptionsBuilder {
	co.Opts = append(co.Opts, func(args *CountOptions) error {
		args.Skip = &i

		return nil
	})

	return co
}
