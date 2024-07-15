// Copyright (C) MongoDB, Inc. 2017-present.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

package options

// InsertOneOptions represents arguments that can be used to configure an InsertOne
// operation.
type InsertOneOptions struct {
	// If true, writes executed as part of the operation will opt out of document-level validation on the server. This
	// option is valid for MongoDB versions >= 3.2 and is ignored for previous server versions. The default value is
	// false. See https://www.mongodb.com/docs/manual/core/schema-validation/ for more information about document
	// validation.
	BypassDocumentValidation *bool

	// A string or document that will be included in server logs, profiling logs, and currentOp queries to help trace
	// the operation.  The default value is nil, which means that no comment will be included in the logs.
	Comment interface{}
}

// InsertOneOptionsBuilder represents functional options that configure an
// InsertOneopts.
type InsertOneOptionsBuilder struct {
	Opts []func(*InsertOneOptions) error
}

// InsertOne creates a new InsertOneOptions instance.
func InsertOne() *InsertOneOptionsBuilder {
	return &InsertOneOptionsBuilder{}
}

// OptionsSetters returns a list of InsertOneOptions setter functions.
func (ioo *InsertOneOptionsBuilder) OptionsSetters() []func(*InsertOneOptions) error {
	return ioo.Opts
}

// SetBypassDocumentValidation sets the value for the BypassDocumentValidation field.
func (ioo *InsertOneOptionsBuilder) SetBypassDocumentValidation(b bool) *InsertOneOptionsBuilder {
	ioo.Opts = append(ioo.Opts, func(opts *InsertOneOptions) error {
		opts.BypassDocumentValidation = &b
		return nil
	})
	return ioo
}

// SetComment sets the value for the Comment field.
func (ioo *InsertOneOptionsBuilder) SetComment(comment interface{}) *InsertOneOptionsBuilder {
	ioo.Opts = append(ioo.Opts, func(opts *InsertOneOptions) error {
		opts.Comment = &comment
		return nil
	})
	return ioo
}

// InsertManyOptions represents arguments that can be used to configure an
// InsertMany operation.
type InsertManyOptions struct {
	// If true, writes executed as part of the operation will opt out of document-level validation on the server. This
	// option is valid for MongoDB versions >= 3.2 and is ignored for previous server versions. The default value is
	// false. See https://www.mongodb.com/docs/manual/core/schema-validation/ for more information about document
	// validation.
	BypassDocumentValidation *bool

	// A string or document that will be included in server logs, profiling logs, and currentOp queries to help trace
	// the operation.  The default value is nil, which means that no comment will be included in the logs.
	Comment interface{}

	// If true, no writes will be executed after one fails. The default value is true.
	Ordered *bool
}

// InsertManyOptionsBuilder contains options to configure insert operations.
// Each option can be set through setter functions. See documentation for each
// setter function for an explanation of the option.
type InsertManyOptionsBuilder struct {
	Opts []func(*InsertManyOptions) error
}

// InsertMany creates a new InsertManyOptions instance.
func InsertMany() *InsertManyOptionsBuilder {
	opts := &InsertManyOptionsBuilder{}
	opts.SetOrdered(DefaultOrdered)

	return opts
}

// OptionsSetters returns a list of InsertManyOptions setter functions.
func (imo *InsertManyOptionsBuilder) OptionsSetters() []func(*InsertManyOptions) error {
	return imo.Opts
}

// SetBypassDocumentValidation sets the value for the BypassDocumentValidation field.
func (imo *InsertManyOptionsBuilder) SetBypassDocumentValidation(b bool) *InsertManyOptionsBuilder {
	imo.Opts = append(imo.Opts, func(opts *InsertManyOptions) error {
		opts.BypassDocumentValidation = &b

		return nil
	})

	return imo
}

// SetComment sets the value for the Comment field.
func (imo *InsertManyOptionsBuilder) SetComment(comment interface{}) *InsertManyOptionsBuilder {
	imo.Opts = append(imo.Opts, func(opts *InsertManyOptions) error {
		opts.Comment = comment

		return nil
	})

	return imo
}

// SetOrdered sets the value for the Ordered field.
func (imo *InsertManyOptionsBuilder) SetOrdered(b bool) *InsertManyOptionsBuilder {
	imo.Opts = append(imo.Opts, func(opts *InsertManyOptions) error {
		opts.Ordered = &b

		return nil
	})

	return imo
}
