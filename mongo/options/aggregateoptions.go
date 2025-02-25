// Copyright (C) MongoDB, Inc. 2017-present.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

package options

import (
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// AggregateOptions represents arguments that can be used to configure an
// Aggregate operation.
//
// See corresponding setter methods for documentation.
type AggregateOptions struct {
	AllowDiskUse             *bool
	BatchSize                *int32
	BypassDocumentValidation *bool
	Collation                *Collation
	MaxAwaitTime             *time.Duration
	Comment                  interface{}
	Hint                     interface{}
	Let                      interface{}
	Custom                   bson.M
}

// AggregateOptionsBuilder contains options to configure aggregate operations.
// Each option can be set through setter functions. See documentation for each
// setter function for an explanation of the option.
type AggregateOptionsBuilder struct {
	Opts []func(*AggregateOptions) error
}

// Aggregate creates a new AggregateOptions instance.
func Aggregate() *AggregateOptionsBuilder {
	return &AggregateOptionsBuilder{}
}

// List returns a list of AggergateOptions setter functions.
func (ao *AggregateOptionsBuilder) List() []func(*AggregateOptions) error {
	return ao.Opts
}

// SetAllowDiskUse sets the value for the AllowDiskUse field. If true, the operation can write to temporary
// files in the _tmp subdirectory of the database directory path on the server. The default value is false.
func (ao *AggregateOptionsBuilder) SetAllowDiskUse(b bool) *AggregateOptionsBuilder {
	ao.Opts = append(ao.Opts, func(opts *AggregateOptions) error {
		opts.AllowDiskUse = &b

		return nil
	})

	return ao
}

// SetBatchSize sets the value for the BatchSize field. Specifies the maximum number of documents
// to be included in each batch returned by the server.
func (ao *AggregateOptionsBuilder) SetBatchSize(i int32) *AggregateOptionsBuilder {
	ao.Opts = append(ao.Opts, func(opts *AggregateOptions) error {
		opts.BatchSize = &i

		return nil
	})

	return ao
}

// SetBypassDocumentValidation sets the value for the BypassDocumentValidation field. If true, writes
// executed as part of the operation will opt out of document-level validation on the server. The default value
// is false. See https://www.mongodb.com/docs/manual/core/schema-validation/ for more information about
// document validation.
func (ao *AggregateOptionsBuilder) SetBypassDocumentValidation(b bool) *AggregateOptionsBuilder {
	ao.Opts = append(ao.Opts, func(opts *AggregateOptions) error {
		opts.BypassDocumentValidation = &b

		return nil
	})

	return ao
}

// SetCollation sets the value for the Collation field. Specifies a collation to
// use for string comparisons during the operation. The default value is nil,
// which means the default collation of the collection will be used.
func (ao *AggregateOptionsBuilder) SetCollation(c *Collation) *AggregateOptionsBuilder {
	ao.Opts = append(ao.Opts, func(opts *AggregateOptions) error {
		opts.Collation = c

		return nil
	})

	return ao
}

// SetMaxAwaitTime sets the value for the MaxAwaitTime field. Specifies maximum amount of time
// that the server should wait for new documents to satisfy a tailable cursor query.
func (ao *AggregateOptionsBuilder) SetMaxAwaitTime(d time.Duration) *AggregateOptionsBuilder {
	ao.Opts = append(ao.Opts, func(opts *AggregateOptions) error {
		opts.MaxAwaitTime = &d

		return nil
	})

	return ao
}

// SetComment sets the value for the Comment field. Specifies a string or document that will be included in
// server logs, profiling logs, and currentOp queries to help trace the operation. The default is nil,
// which means that no comment will be included in the logs.
func (ao *AggregateOptionsBuilder) SetComment(comment interface{}) *AggregateOptionsBuilder {
	ao.Opts = append(ao.Opts, func(opts *AggregateOptions) error {
		opts.Comment = comment

		return nil
	})

	return ao
}

// SetHint sets the value for the Hint field. Specifies the index to use for the aggregation. This should
// either be the index name as a string or the index specification as a document. The hint does not apply to
// $lookup and $graphLookup aggregation stages. The driver will return an error if the hint parameter
// is a multi-key map. The default value is nil, which means that no hint will be sent.
func (ao *AggregateOptionsBuilder) SetHint(h interface{}) *AggregateOptionsBuilder {
	ao.Opts = append(ao.Opts, func(opts *AggregateOptions) error {
		opts.Hint = h

		return nil
	})

	return ao
}

// SetLet sets the value for the Let field. Specifies parameters for the aggregate expression. This
// option is only valid for MongoDB versions >= 5.0. Older servers will report an error for using this
// option. This must be a document mapping parameter names to values. Values must be constant or closed
// expressions that do not reference document fields. Parameters can then be accessed as variables in
// an aggregate expression context (e.g. "$$var").
func (ao *AggregateOptionsBuilder) SetLet(let interface{}) *AggregateOptionsBuilder {
	ao.Opts = append(ao.Opts, func(opts *AggregateOptions) error {
		opts.Let = let

		return nil
	})

	return ao
}

// SetCustom sets the value for the Custom field. Key-value pairs of the BSON map should correlate
// with desired option names and values. Values must be Marshalable. Custom options may conflict
// with non-custom options, and custom options bypass client-side validation. Prefer using non-custom
// options where possible.
func (ao *AggregateOptionsBuilder) SetCustom(c bson.M) *AggregateOptionsBuilder {
	ao.Opts = append(ao.Opts, func(opts *AggregateOptions) error {
		opts.Custom = c

		return nil
	})

	return ao
}
