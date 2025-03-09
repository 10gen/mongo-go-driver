// Copyright (C) MongoDB, Inc. 2017-present.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

package options

// UpdateOneOptions represents arguments that can be used to configure UpdateOne
// operations.
//
// See corresponding setter methods for documentation.
type UpdateOneOptions struct {
	ArrayFilters             []interface{}
	BypassDocumentValidation *bool
	Collation                *Collation
	Comment                  interface{}
	Hint                     interface{}
	Upsert                   *bool
	Let                      interface{}
	Sort                     interface{}
}

// UpdateOneOptionsBuilder contains options to configure UpdateOne operations.
// Each option can be set through setter functions. See documentation for each
// setter function for an explanation of the option.
type UpdateOneOptionsBuilder struct {
	Opts []func(*UpdateOneOptions) error
}

// UpdateOne creates a new UpdateOneOptions instance.
func UpdateOne() *UpdateOneOptionsBuilder {
	return &UpdateOneOptionsBuilder{}
}

// List returns a list of UpdateOneOptions setter functions.
func (uo *UpdateOneOptionsBuilder) List() []func(*UpdateOneOptions) error {
	return uo.Opts
}

// SetArrayFilters sets the value for the ArrayFilters field. ArrayFilters is a
// set of filters specifying to which array elements an update should apply. The
// default value is nil, which means the update will apply to all array
// elements.
func (uo *UpdateOneOptionsBuilder) SetArrayFilters(af []interface{}) *UpdateOneOptionsBuilder {
	uo.Opts = append(uo.Opts, func(opts *UpdateOneOptions) error {
		opts.ArrayFilters = af

		return nil
	})

	return uo
}

// SetBypassDocumentValidation sets the value for the BypassDocumentValidation field. If true,
// writes executed as part of the operation will opt out of document-level validation on the server.
// The default value is false. See https://www.mongodb.com/docs/manual/core/schema-validation/ for
// more information about document validation.
func (uo *UpdateOneOptionsBuilder) SetBypassDocumentValidation(b bool) *UpdateOneOptionsBuilder {
	uo.Opts = append(uo.Opts, func(opts *UpdateOneOptions) error {
		opts.BypassDocumentValidation = &b

		return nil
	})

	return uo
}

// SetCollation sets the value for the Collation field. Specifies a collation to
// use for string comparisons during the operation. The default value is nil,
// which means the default collation of the collection will be used.
func (uo *UpdateOneOptionsBuilder) SetCollation(c *Collation) *UpdateOneOptionsBuilder {
	uo.Opts = append(uo.Opts, func(opts *UpdateOneOptions) error {
		opts.Collation = c

		return nil
	})

	return uo
}

// SetComment sets the value for the Comment field. Specifies a string or document that will be
// included in server logs, profiling logs, and currentOp queries to help trace the operation.
// The default value is nil, which means that no comment will be included in the logs.
func (uo *UpdateOneOptionsBuilder) SetComment(comment interface{}) *UpdateOneOptionsBuilder {
	uo.Opts = append(uo.Opts, func(opts *UpdateOneOptions) error {
		opts.Comment = comment

		return nil
	})

	return uo
}

// SetHint sets the value for the Hint field. Specifies the index to use for the
// operation. This should either be the index name as a string or the index
// specification as a document. This option is only valid for MongoDB versions
// >= 4.2. Server versions < 4.2 will return an error if this option is
// specified. The driver will return an error if this option is specified during
// an unacknowledged write operation. The driver will return an error if the
// hint parameter is a multi-key map. The default value is nil, which means that
// no hint will be sent.
func (uo *UpdateOneOptionsBuilder) SetHint(h interface{}) *UpdateOneOptionsBuilder {
	uo.Opts = append(uo.Opts, func(opts *UpdateOneOptions) error {
		opts.Hint = h

		return nil
	})

	return uo
}

// SetUpsert sets the value for the Upsert field. If true, a new document will be inserted if the
// filter does not match any documents in the collection. The default value is false.
func (uo *UpdateOneOptionsBuilder) SetUpsert(b bool) *UpdateOneOptionsBuilder {
	uo.Opts = append(uo.Opts, func(opts *UpdateOneOptions) error {
		opts.Upsert = &b

		return nil
	})

	return uo
}

// SetLet sets the value for the Let field. Specifies parameters for the update expression. This
// option is only valid for MongoDB versions >= 5.0. Older servers will report an error for using
// this option. This must be a document mapping parameter names to values. Values must be constant
// or closed expressions that do not reference document fields. Parameters can then be accessed
// as variables in an aggregate expression context (e.g. "$$var").
func (uo *UpdateOneOptionsBuilder) SetLet(l interface{}) *UpdateOneOptionsBuilder {
	uo.Opts = append(uo.Opts, func(opts *UpdateOneOptions) error {
		opts.Let = l

		return nil
	})

	return uo
}

// SetSort sets the value for the Sort field. Specifies a document specifying which document should
// be updated if the filter used by the operation matches multiple documents in the collection. If
// set, the first document in the sorted order will be updated. This option is only valid for MongoDB
// versions >= 8.0. The sort parameter is evaluated sequentially, so the driver will return an error
// if it is a multi-key map (which is unordeded). The default value is nil.
func (uo *UpdateOneOptionsBuilder) SetSort(s interface{}) *UpdateOneOptionsBuilder {
	uo.Opts = append(uo.Opts, func(opts *UpdateOneOptions) error {
		opts.Sort = s

		return nil
	})

	return uo
}

// UpdateManyOptions represents arguments that can be used to configure UpdateMany
// operations.
//
// See corresponding setter methods for documentation.
type UpdateManyOptions struct {
	ArrayFilters             []interface{}
	BypassDocumentValidation *bool
	Collation                *Collation
	Comment                  interface{}
	Hint                     interface{}
	Upsert                   *bool
	Let                      interface{}
}

// UpdateManyOptionsBuilder contains options to configure UpdateMany operations.
// Each option can be set through setter functions. See documentation for each
// setter function for an explanation of the option.
type UpdateManyOptionsBuilder struct {
	Opts []func(*UpdateManyOptions) error
}

// UpdateMany creates a new UpdateManyOptions instance.
func UpdateMany() *UpdateManyOptionsBuilder {
	return &UpdateManyOptionsBuilder{}
}

// List returns a list of UpdateManyOptions setter functions.
func (uo *UpdateManyOptionsBuilder) List() []func(*UpdateManyOptions) error {
	return uo.Opts
}

// SetArrayFilters sets the value for the ArrayFilters field. ArrayFilters is a
// set of filters specifying to which array elements an update should apply. The
// default value is nil, which means the update will apply to all array
// elements.
func (uo *UpdateManyOptionsBuilder) SetArrayFilters(af []interface{}) *UpdateManyOptionsBuilder {
	uo.Opts = append(uo.Opts, func(opts *UpdateManyOptions) error {
		opts.ArrayFilters = af

		return nil
	})

	return uo
}

// SetBypassDocumentValidation sets the value for the BypassDocumentValidation field. If true,
// writes executed as part of the operation will opt out of document-level validation on the server.
// The default value is false. See https://www.mongodb.com/docs/manual/core/schema-validation/ for
// more information about document validation.
func (uo *UpdateManyOptionsBuilder) SetBypassDocumentValidation(b bool) *UpdateManyOptionsBuilder {
	uo.Opts = append(uo.Opts, func(opts *UpdateManyOptions) error {
		opts.BypassDocumentValidation = &b

		return nil
	})

	return uo
}

// SetCollation sets the value for the Collation field. Specifies a collation to
// use for string comparisons during the operation. The default value is nil,
// which means the default collation of the collection will be used.
func (uo *UpdateManyOptionsBuilder) SetCollation(c *Collation) *UpdateManyOptionsBuilder {
	uo.Opts = append(uo.Opts, func(opts *UpdateManyOptions) error {
		opts.Collation = c

		return nil
	})

	return uo
}

// SetComment sets the value for the Comment field. Specifies a string or document that will be
// included in server logs, profiling logs, and currentOp queries to help trace the operation.
// The default value is nil, which means that no comment will be included in the logs.
func (uo *UpdateManyOptionsBuilder) SetComment(comment interface{}) *UpdateManyOptionsBuilder {
	uo.Opts = append(uo.Opts, func(opts *UpdateManyOptions) error {
		opts.Comment = comment

		return nil
	})

	return uo
}

// SetHint sets the value for the Hint field. Specifies the index to use for the
// operation. This should either be the index name as a string or the index
// specification as a document. This option is only valid for MongoDB versions
// >= 4.2. Server versions < 4.2 will return an error if this option is
// specified. The driver will return an error if this option is specified during
// an unacknowledged write operation. The driver will return an error if the
// hint parameter is a multi-key map. The default value is nil, which means that
// no hint will be sent.
func (uo *UpdateManyOptionsBuilder) SetHint(h interface{}) *UpdateManyOptionsBuilder {
	uo.Opts = append(uo.Opts, func(opts *UpdateManyOptions) error {
		opts.Hint = h

		return nil
	})

	return uo
}

// SetUpsert sets the value for the Upsert field. If true, a new document will be inserted if the
// filter does not match any documents in the collection. The default value is false.
func (uo *UpdateManyOptionsBuilder) SetUpsert(b bool) *UpdateManyOptionsBuilder {
	uo.Opts = append(uo.Opts, func(opts *UpdateManyOptions) error {
		opts.Upsert = &b

		return nil
	})

	return uo
}

// SetLet sets the value for the Let field. Specifies parameters for the update expression. This
// option is only valid for MongoDB versions >= 5.0. Older servers will report an error for using
// this option. This must be a document mapping parameter names to values. Values must be constant
// or closed expressions that do not reference document fields. Parameters can then be accessed
// as variables in an aggregate expression context (e.g. "$$var").
func (uo *UpdateManyOptionsBuilder) SetLet(l interface{}) *UpdateManyOptionsBuilder {
	uo.Opts = append(uo.Opts, func(opts *UpdateManyOptions) error {
		opts.Let = l

		return nil
	})

	return uo
}
