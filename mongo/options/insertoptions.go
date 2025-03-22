// Copyright (C) MongoDB, Inc. 2017-present.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

package options

// InsertOneOptions represents arguments that can be used to configure an InsertOne
// operation.
//
// See corresponding setter methods for documentation.
type InsertOneOptions struct {
	BypassDocumentValidation *bool
	Comment                  interface{}
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

// List returns a list of InsertOneOptions setter functions.
func (ioo *InsertOneOptionsBuilder) List() []func(*InsertOneOptions) error {
	return ioo.Opts
}

// SetBypassDocumentValidation sets the value for the BypassDocumentValidation field. If true,
// writes executed as part of the operation will opt out of document-level validation on the
// server. The default value is false. See https://www.mongodb.com/docs/manual/core/schema-validation/
// for more information about document validation.
func (ioo *InsertOneOptionsBuilder) SetBypassDocumentValidation(b bool) *InsertOneOptionsBuilder {
	ioo.Opts = append(ioo.Opts, func(opts *InsertOneOptions) error {
		opts.BypassDocumentValidation = &b
		return nil
	})
	return ioo
}

// SetComment sets the value for the Comment field. Specifies a string or document that will be included in server logs, profiling logs, and currentOp queries to help trace
// the operation.  The default value is nil, which means that no comment will be included in the logs.
func (ioo *InsertOneOptionsBuilder) SetComment(comment interface{}) *InsertOneOptionsBuilder {
	ioo.Opts = append(ioo.Opts, func(opts *InsertOneOptions) error {
		opts.Comment = &comment
		return nil
	})
	return ioo
}

// InsertManyOptions represents arguments that can be used to configure an
// InsertMany operation.
//
// See corresponding setter methods for documentation.
type InsertManyOptions struct {
	BypassDocumentValidation *bool
	Comment                  interface{}
	Ordered                  *bool
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

// List returns a list of InsertManyOptions setter functions.
func (imo *InsertManyOptionsBuilder) List() []func(*InsertManyOptions) error {
	return imo.Opts
}

// SetBypassDocumentValidation sets the value for the BypassDocumentValidation field. If true,
// writes executed as part of the operation will opt out of document-level validation on the
// server. The default value is false. See https://www.mongodb.com/docs/manual/core/schema-validation/
// for more information about document validation.
func (imo *InsertManyOptionsBuilder) SetBypassDocumentValidation(b bool) *InsertManyOptionsBuilder {
	imo.Opts = append(imo.Opts, func(opts *InsertManyOptions) error {
		opts.BypassDocumentValidation = &b

		return nil
	})

	return imo
}

// SetComment sets the value for the Comment field. Specifies a string or document that will be
// included in server logs, profiling logs, and currentOp queries to help trace the operation.
// The default value is nil, which means that no comment will be included in the logs.
func (imo *InsertManyOptionsBuilder) SetComment(comment interface{}) *InsertManyOptionsBuilder {
	imo.Opts = append(imo.Opts, func(opts *InsertManyOptions) error {
		opts.Comment = comment

		return nil
	})

	return imo
}

// SetOrdered sets the value for the Ordered field. If true, no writes will be executed after
// one fails. The default value is true.
func (imo *InsertManyOptionsBuilder) SetOrdered(b bool) *InsertManyOptionsBuilder {
	imo.Opts = append(imo.Opts, func(opts *InsertManyOptions) error {
		opts.Ordered = &b

		return nil
	})

	return imo
}
