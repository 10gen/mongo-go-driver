// Copyright (C) MongoDB, Inc. 2017-present.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

package options

// DefaultOrdered is the default value for the Ordered option in BulkWriteOptions.
var DefaultOrdered = true

// BulkWriteArgs represents args that can be used to configure a BulkWrite
// operation.
type BulkWriteArgs struct {
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

	// Specifies parameters for all update and delete commands in the BulkWrite. This option is only valid for MongoDB
	// versions >= 5.0. Older servers will report an error for using this option. This must be a document mapping
	// parameter names to values. Values must be constant or closed expressions that do not reference document fields.
	// Parameters can then be accessed as variables in an aggregate expression context (e.g. "$$var").
	Let interface{}
}

// BulkWriteOptions represents options that can be used to configure a BulkWrite
// operation.
type BulkWriteOptions struct {
	Opts []func(*BulkWriteArgs) error
}

// BulkWrite creates a new *BulkWriteOptions instance.
func BulkWrite() *BulkWriteOptions {
	opts := &BulkWriteOptions{}
	opts = opts.SetOrdered(DefaultOrdered)

	return opts
}

// ArgsSetters returns a list of BulkWriteArgs setter functions.
func (b *BulkWriteOptions) ArgsSetters() []func(*BulkWriteArgs) error {
	return b.Opts
}

// SetComment sets the value for the Comment field.
func (b *BulkWriteOptions) SetComment(comment interface{}) *BulkWriteOptions {
	b.Opts = append(b.Opts, func(args *BulkWriteArgs) error {
		args.Comment = comment

		return nil
	})

	return b
}

// SetOrdered sets the value for the Ordered field.
func (b *BulkWriteOptions) SetOrdered(ordered bool) *BulkWriteOptions {
	b.Opts = append(b.Opts, func(args *BulkWriteArgs) error {
		args.Ordered = &ordered

		return nil
	})

	return b
}

// SetBypassDocumentValidation sets the value for the BypassDocumentValidation field.
func (b *BulkWriteOptions) SetBypassDocumentValidation(bypass bool) *BulkWriteOptions {
	b.Opts = append(b.Opts, func(args *BulkWriteArgs) error {
		args.BypassDocumentValidation = &bypass

		return nil
	})

	return b
}

// SetLet sets the value for the Let field. Let specifies parameters for all update and delete commands in the BulkWrite.
// This option is only valid for MongoDB versions >= 5.0. Older servers will report an error for using this option.
// This must be a document mapping parameter names to values. Values must be constant or closed expressions that do not
// reference document fields. Parameters can then be accessed as variables in an aggregate expression context (e.g. "$$var").
func (b *BulkWriteOptions) SetLet(let interface{}) *BulkWriteOptions {
	b.Opts = append(b.Opts, func(args *BulkWriteArgs) error {
		args.Let = &let

		return nil
	})

	return b
}
