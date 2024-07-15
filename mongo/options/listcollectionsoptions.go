// Copyright (C) MongoDB, Inc. 2017-present.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

package options

// ListCollectionsOptions represents arguments that can be used to configure a
// ListCollections operation.
type ListCollectionsOptions struct {
	// If true, each collection document will only contain a field for the collection name. The default value is false.
	NameOnly *bool

	// The maximum number of documents to be included in each batch returned by the server.
	BatchSize *int32

	// If true, and NameOnly is true, limits the documents returned to only contain collections the user is authorized to use. The default value
	// is false. This option is only valid for MongoDB server versions >= 4.0. Server versions < 4.0 ignore this option.
	AuthorizedCollections *bool
}

// ListCollectionsOptionsBuilder contains options to configure list collection
// operations. Each option can be set through setter functions. See
// documentation for each setter function for an explanation of the option.
type ListCollectionsOptionsBuilder struct {
	Opts []func(*ListCollectionsOptions) error
}

// ListCollections creates a new ListCollectionsOptions instance.
func ListCollections() *ListCollectionsOptionsBuilder {
	return &ListCollectionsOptionsBuilder{}
}

// ArgsSetters returns a list of CountArgs setter functions.
func (lc *ListCollectionsOptionsBuilder) ArgsSetters() []func(*ListCollectionsOptions) error {
	return lc.Opts
}

// SetNameOnly sets the value for the NameOnly field.
func (lc *ListCollectionsOptionsBuilder) SetNameOnly(b bool) *ListCollectionsOptionsBuilder {
	lc.Opts = append(lc.Opts, func(args *ListCollectionsOptions) error {
		args.NameOnly = &b

		return nil
	})

	return lc
}

// SetBatchSize sets the value for the BatchSize field.
func (lc *ListCollectionsOptionsBuilder) SetBatchSize(size int32) *ListCollectionsOptionsBuilder {
	lc.Opts = append(lc.Opts, func(args *ListCollectionsOptions) error {
		args.BatchSize = &size

		return nil
	})

	return lc
}

// SetAuthorizedCollections sets the value for the AuthorizedCollections field. This option is only valid for MongoDB server versions >= 4.0. Server
// versions < 4.0 ignore this option.
func (lc *ListCollectionsOptionsBuilder) SetAuthorizedCollections(b bool) *ListCollectionsOptionsBuilder {
	lc.Opts = append(lc.Opts, func(args *ListCollectionsOptions) error {
		args.AuthorizedCollections = &b

		return nil
	})

	return lc
}
