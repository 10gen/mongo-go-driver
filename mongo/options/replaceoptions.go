// Copyright (C) MongoDB, Inc. 2017-present.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

package options

// ReplaceOptions represents arguments that can be used to configure a ReplaceOne
// operation.
//
// See corresponding setter methods for documentation.
type ReplaceOptions struct {
	BypassDocumentValidation *bool
	Collation                *Collation
	Comment                  interface{}
	Hint                     interface{}
	Upsert                   *bool
	Let                      interface{}
	Sort                     interface{}
}

// ReplaceOptionsBuilder contains options to configure replace operations. Each
// option can be set through setter functions. See documentation for each setter
// function for an explanation of the option.
type ReplaceOptionsBuilder struct {
	Opts []func(*ReplaceOptions) error
}

// Replace creates a new ReplaceOptions instance.
func Replace() *ReplaceOptionsBuilder {
	return &ReplaceOptionsBuilder{}
}

// List returns a list of CountOptions setter functions.
func (ro *ReplaceOptionsBuilder) List() []func(*ReplaceOptions) error {
	return ro.Opts
}

// SetBypassDocumentValidation sets the value for the BypassDocumentValidation field. If true,
// writes executed as part of the operation will opt out of document-level validation on the server.
// The default value is false. See https://www.mongodb.com/docs/manual/core/schema-validation/ for
// more information about document validation.
func (ro *ReplaceOptionsBuilder) SetBypassDocumentValidation(b bool) *ReplaceOptionsBuilder {
	ro.Opts = append(ro.Opts, func(opts *ReplaceOptions) error {
		opts.BypassDocumentValidation = &b

		return nil
	})

	return ro
}

// SetCollation sets the value for the Collation field. Specifies a collation to
// use for string comparisons during the operation. The default value is nil,
// which means the default collation of the collection will be used.
func (ro *ReplaceOptionsBuilder) SetCollation(c *Collation) *ReplaceOptionsBuilder {
	ro.Opts = append(ro.Opts, func(opts *ReplaceOptions) error {
		opts.Collation = c

		return nil
	})

	return ro
}

// SetComment sets the value for the Comment field. Specifies a string or document that will
// be included in server logs, profiling logs, and currentOp queries to help trace the operation.
// The default value is nil, which means that no comment will be included in the logs.
func (ro *ReplaceOptionsBuilder) SetComment(comment interface{}) *ReplaceOptionsBuilder {
	ro.Opts = append(ro.Opts, func(opts *ReplaceOptions) error {
		opts.Comment = comment

		return nil
	})

	return ro
}

// SetHint sets the value for the Hint field. Specifies the index to use for the
// operation. This should either be the index name as a string or the index
// specification as a document. This option is only valid for MongoDB versions
// >= 4.2. Server versions < 4.2 will return an error if this option is
// specified. The driver will return an error if this option is specified during
// an unacknowledged write operation. The driver will return an error if the
// hint parameter is a multi-key map. The default value is nil, which means that
// no hint will be sent.
func (ro *ReplaceOptionsBuilder) SetHint(h interface{}) *ReplaceOptionsBuilder {
	ro.Opts = append(ro.Opts, func(opts *ReplaceOptions) error {
		opts.Hint = h

		return nil
	})

	return ro
}

// SetUpsert sets the value for the Upsert field. If true, a new document will be inserted
// if the filter does not match any documents in the collection. The default value is false.
func (ro *ReplaceOptionsBuilder) SetUpsert(b bool) *ReplaceOptionsBuilder {
	ro.Opts = append(ro.Opts, func(opts *ReplaceOptions) error {
		opts.Upsert = &b

		return nil
	})

	return ro
}

// SetLet sets the value for the Let field. Specifies parameters for the aggregate expression.
// This option is only valid for MongoDB versions >= 5.0. Older servers will report an error
// for using this option. This must be a document mapping parameter names to values. Values
// must be constant or closed expressions that do not reference document fields. Parameters
// can then be accessed as variables in an aggregate expression context (e.g. "$$var").
func (ro *ReplaceOptionsBuilder) SetLet(l interface{}) *ReplaceOptionsBuilder {
	ro.Opts = append(ro.Opts, func(opts *ReplaceOptions) error {
		opts.Let = l

		return nil
	})

	return ro
}

// SetSort sets the value for the Sort field. Specifies a document specifying which document should
// be replaced if the filter used by the operation matches multiple documents in the collection. If
// set, the first document in the sorted order will be replaced. This option is only valid for MongoDB
// versions >= 8.0. The sort parameter is evaluated sequentially, so the driver will return an error
// if it is a multi-key map (which is unordeded). The default value is nil.
func (ro *ReplaceOptionsBuilder) SetSort(s interface{}) *ReplaceOptionsBuilder {
	ro.Opts = append(ro.Opts, func(opts *ReplaceOptions) error {
		opts.Sort = s

		return nil
	})

	return ro
}
