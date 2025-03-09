// Copyright (C) MongoDB, Inc. 2017-present.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

package options

// CreateIndexesOptions represents arguments that can be used to configure
// IndexView.CreateOne and IndexView.CreateMany operations.
//
// See corresponding setter methods for documentation.
type CreateIndexesOptions struct {
	CommitQuorum interface{}
}

// CreateIndexesOptionsBuilder contains options to create indexes. Each option
// can be set through setter functions. See documentation for each setter
// function for an explanation of the option.
//
// See corresponding setter methods for documentation.
type CreateIndexesOptionsBuilder struct {
	Opts []func(*CreateIndexesOptions) error
}

// CreateIndexes creates a new CreateIndexesOptions instance.
func CreateIndexes() *CreateIndexesOptionsBuilder {
	return &CreateIndexesOptionsBuilder{}
}

// List returns a list of CreateIndexesOptions setter functions.
func (c *CreateIndexesOptionsBuilder) List() []func(*CreateIndexesOptions) error {
	return c.Opts
}

// SetCommitQuorumInt sets the value for the CommitQuorum field as an int32.
// Specifies the number of data-bearing members of a replica set, including the primary,
// that must complete the index builds successfully before the primary marks the indexes
// as ready.
//
// Semantics for int: the number of members that must complete the build.
//
// This option is only available on MongoDB versions >= 4.4. A client-side error will
// be returned if the option is specified for MongoDB versions <= 4.2. The default
// value is nil, meaning that the server-side default will be used. See
// dochub.mongodb.org/core/index-commit-quorum for more information.
func (c *CreateIndexesOptionsBuilder) SetCommitQuorumInt(quorum int32) *CreateIndexesOptionsBuilder {
	c.Opts = append(c.Opts, func(opts *CreateIndexesOptions) error {
		opts.CommitQuorum = quorum

		return nil
	})

	return c
}

// SetCommitQuorumString sets the value for the CommitQuorum field as a string.
// Specifies the number of data-bearing members of a replica set, including the primary,
// that must complete the index builds successfully before the primary marks the indexes
// as ready.
//
// Semantics for String: specifies a tag. All members with that tag must complete the build.
//
// This option is only available on MongoDB versions >= 4.4. A client-side error will
// be returned if the option is specified for MongoDB versions <= 4.2. The default
// value is nil, meaning that the server-side default will be used. See
// dochub.mongodb.org/core/index-commit-quorum for more information.
func (c *CreateIndexesOptionsBuilder) SetCommitQuorumString(quorum string) *CreateIndexesOptionsBuilder {
	c.Opts = append(c.Opts, func(opts *CreateIndexesOptions) error {
		opts.CommitQuorum = quorum

		return nil
	})

	return c
}

// SetCommitQuorumMajority sets the value for the CommitQuorum to special "majority" value.
// Specifies the number of data-bearing members of a replica set, including the primary,
// that must complete the index builds successfully before the primary marks the indexes
// as ready.
//
// Semantics for "majority": A special value to indicate that more than half the nodes
// must complete the build.
//
// This option is only available on MongoDB versions >= 4.4. A client-side error will
// be returned if the option is specified for MongoDB versions <= 4.2. The default
// value is nil, meaning that the server-side default will be used. See
// dochub.mongodb.org/core/index-commit-quorum for more information.
func (c *CreateIndexesOptionsBuilder) SetCommitQuorumMajority() *CreateIndexesOptionsBuilder {
	c.Opts = append(c.Opts, func(opts *CreateIndexesOptions) error {
		opts.CommitQuorum = "majority"

		return nil
	})

	return c
}

// SetCommitQuorumVotingMembers sets the value for the CommitQuorum to special "votingMembers" value.
// Specifies the number of data-bearing members of a replica set, including the primary,
// that must complete the index builds successfully before the primary marks the indexes
// as ready.
//
// Semantics for "votingMembers": A special value to indicate that all voting data-bearing
// nodes must complete.
//
// This option is only available on MongoDB versions >= 4.4. A client-side error will
// be returned if the option is specified for MongoDB versions <= 4.2. The default
// value is nil, meaning that the server-side default will be used. See
// dochub.mongodb.org/core/index-commit-quorum for more information.
func (c *CreateIndexesOptionsBuilder) SetCommitQuorumVotingMembers() *CreateIndexesOptionsBuilder {
	c.Opts = append(c.Opts, func(opts *CreateIndexesOptions) error {
		opts.CommitQuorum = "votingMembers"

		return nil
	})

	return c
}

// DropIndexesOptions represents arguments that can be used to configure
// IndexView.DropOne and IndexView.DropAll operations.
type DropIndexesOptions struct{}

// DropIndexesOptionsBuilder contains options to configure dropping indexes.
// Each option can be set through setter functions. See documentation for each
// setter function for an explanation of the option.
type DropIndexesOptionsBuilder struct {
	Opts []func(*DropIndexesOptions) error
}

// DropIndexes creates a new DropIndexesOptions instance.
func DropIndexes() *DropIndexesOptionsBuilder {
	return &DropIndexesOptionsBuilder{}
}

// List returns a list of DropIndexesOptions setter functions.
func (d *DropIndexesOptionsBuilder) List() []func(*DropIndexesOptions) error {
	return d.Opts
}

// ListIndexesOptions represents arguments that can be used to configure an
// IndexView.List operation.
//
// See corresponding setter methods for documentation.
type ListIndexesOptions struct {
	BatchSize *int32
}

// ListIndexesOptionsBuilder contains options to configure count operations. Each
// option can be set through setter functions. See documentation for each setter
// function for an explanation of the option.
type ListIndexesOptionsBuilder struct {
	Opts []func(*ListIndexesOptions) error
}

// ListIndexes creates a new ListIndexesOptions instance.
func ListIndexes() *ListIndexesOptionsBuilder {
	return &ListIndexesOptionsBuilder{}
}

// List returns a list of CountOptions setter functions.
func (l *ListIndexesOptionsBuilder) List() []func(*ListIndexesOptions) error {
	return l.Opts
}

// SetBatchSize sets the value for the BatchSize field. Specifies the maximum number
// of documents to be included in each batch returned by the server.
func (l *ListIndexesOptionsBuilder) SetBatchSize(i int32) *ListIndexesOptionsBuilder {
	l.Opts = append(l.Opts, func(opts *ListIndexesOptions) error {
		opts.BatchSize = &i

		return nil
	})

	return l
}

// IndexOptions represents arguments that can be used to configure a new index
// created through the IndexView.CreateOne or IndexView.CreateMany operations.
//
// See corresponding setter methods for documentation.
type IndexOptions struct {
	ExpireAfterSeconds      *int32
	Name                    *string
	Sparse                  *bool
	StorageEngine           interface{}
	Unique                  *bool
	Version                 *int32
	DefaultLanguage         *string
	LanguageOverride        *string
	TextVersion             *int32
	Weights                 interface{}
	SphereVersion           *int32
	Bits                    *int32
	Max                     *float64
	Min                     *float64
	BucketSize              *int32
	PartialFilterExpression interface{}
	Collation               *Collation
	WildcardProjection      interface{}
	Hidden                  *bool
}

// IndexOptionsBuilder contains options to configure index operations. Each option
// can be set through setter functions. See documentation for each setter
// function for an explanation of the option.
type IndexOptionsBuilder struct {
	Opts []func(*IndexOptions) error
}

// Index creates a new IndexOptions instance.
func Index() *IndexOptionsBuilder {
	return &IndexOptionsBuilder{}
}

// List returns a list of IndexOptions setter functions.
func (i *IndexOptionsBuilder) List() []func(*IndexOptions) error {
	return i.Opts
}

// SetExpireAfterSeconds sets value for the ExpireAfterSeconds field. Specifies the length
// of time, in seconds, for documents to remain in the collection. The default value is 0,
// which means that documents will remain in the collection until they're explicitly
// deleted or the collection is dropped.
func (i *IndexOptionsBuilder) SetExpireAfterSeconds(seconds int32) *IndexOptionsBuilder {
	i.Opts = append(i.Opts, func(opts *IndexOptions) error {
		opts.ExpireAfterSeconds = &seconds

		return nil
	})

	return i
}

// SetName sets the value for the Name field. Specifies the name of the index. The default
// value is "[field1]_[direction1]_[field2]_[direction2]...". For example, an index with
// the specification {name: 1, age: -1} will be named "name_1_age_-1".
func (i *IndexOptionsBuilder) SetName(name string) *IndexOptionsBuilder {
	i.Opts = append(i.Opts, func(opts *IndexOptions) error {
		opts.Name = &name

		return nil
	})

	return i
}

// SetSparse sets the value of the Sparse field. If true, the index will only reference
// documents that contain the fields specified in the index. The default is false.
func (i *IndexOptionsBuilder) SetSparse(sparse bool) *IndexOptionsBuilder {
	i.Opts = append(i.Opts, func(opts *IndexOptions) error {
		opts.Sparse = &sparse

		return nil
	})

	return i
}

// SetStorageEngine sets the value for the StorageEngine field. Specifies the
// storage engine to use for the index. The value must be a document in the form
// {<storage engine name>: <options>}. The default value is nil, which means that
// the default storage engine will be used.
func (i *IndexOptionsBuilder) SetStorageEngine(engine interface{}) *IndexOptionsBuilder {
	i.Opts = append(i.Opts, func(opts *IndexOptions) error {
		opts.StorageEngine = engine

		return nil
	})

	return i
}

// SetUnique sets the value for the Unique field. If true, the collection will not
// accept insertion or update of documents where the index key value matches an
// existing value in the index. The default is false.
func (i *IndexOptionsBuilder) SetUnique(unique bool) *IndexOptionsBuilder {
	i.Opts = append(i.Opts, func(opts *IndexOptions) error {
		opts.Unique = &unique

		return nil
	})

	return i
}

// SetVersion sets the value for the Version field. Specifies the index version
// number, either 0 or 1.
func (i *IndexOptionsBuilder) SetVersion(version int32) *IndexOptionsBuilder {
	i.Opts = append(i.Opts, func(opts *IndexOptions) error {
		opts.Version = &version

		return nil
	})

	return i
}

// SetDefaultLanguage sets the value for the DefaultLanguage field. Specifies the
// language that determines the list of stop words and the rules for the stemmer
// and tokenizer. This option is only applicable for text indexes and is ignored for
// other index types. The default value is "english".
func (i *IndexOptionsBuilder) SetDefaultLanguage(language string) *IndexOptionsBuilder {
	i.Opts = append(i.Opts, func(opts *IndexOptions) error {
		opts.DefaultLanguage = &language

		return nil
	})

	return i
}

// SetLanguageOverride sets the value of the LanguageOverride field. Specifies the name
// of the field in the collection's documents that contains the override language for the
// document. This option is only applicable for text indexes and is ignored for other index
// types. The default value is the value of the DefaultLanguage option.
func (i *IndexOptionsBuilder) SetLanguageOverride(override string) *IndexOptionsBuilder {
	i.Opts = append(i.Opts, func(opts *IndexOptions) error {
		opts.LanguageOverride = &override

		return nil
	})

	return i
}

// SetTextVersion sets the value for the TextVersion field. Specifies the index version number
// for a text index. See https://www.mongodb.com/docs/manual/core/index-text/#text-versions
// for information about different version numbers.
func (i *IndexOptionsBuilder) SetTextVersion(version int32) *IndexOptionsBuilder {
	i.Opts = append(i.Opts, func(opts *IndexOptions) error {
		opts.TextVersion = &version

		return nil
	})

	return i
}

// SetWeights sets the value for the Weights field. Sets a document that contains field
// and weight pairs. The weight is an integer ranging from 1 to 99,999, inclusive,
// indicating the significance of the field relative to the other indexed fields in
// terms of the score. This option is only applicable for text indexes and is ignored
// for other index types. The default value is nil, which means that every field will
// have a weight of 1.
func (i *IndexOptionsBuilder) SetWeights(weights interface{}) *IndexOptionsBuilder {
	i.Opts = append(i.Opts, func(opts *IndexOptions) error {
		opts.Weights = weights

		return nil
	})

	return i
}

// SetSphereVersion sets the value for the SphereVersion field. Specifies the index version number
// for a 2D sphere index. See https://www.mongodb.com/docs/manual/core/2dsphere/#dsphere-v2 for
// information about different version numbers.
func (i *IndexOptionsBuilder) SetSphereVersion(version int32) *IndexOptionsBuilder {
	i.Opts = append(i.Opts, func(opts *IndexOptions) error {
		opts.SphereVersion = &version

		return nil
	})

	return i
}

// SetBits sets the value for the Bits field. Specifies the precision of the stored geohash
// value of the location data. This option only applies to 2D indexes and is ignored for
// other index types. The value must be between 1 and 32, inclusive. The default value is 26.
func (i *IndexOptionsBuilder) SetBits(bits int32) *IndexOptionsBuilder {
	i.Opts = append(i.Opts, func(opts *IndexOptions) error {
		opts.Bits = &bits

		return nil
	})

	return i
}

// SetMax sets the value for the Max field. Specifies the upper inclusive boundary for
// longitude and latitude values. This option is only applicable to 2D indexes and
// is ignored for other index types. The default value is 180.0.
func (i *IndexOptionsBuilder) SetMax(max float64) *IndexOptionsBuilder {
	i.Opts = append(i.Opts, func(opts *IndexOptions) error {
		opts.Max = &max

		return nil
	})

	return i
}

// SetMin sets the value for the Min field. Specifies the lower inclusive boundary for
// longitude and latitude values. This option is only applicable to 2D indexes and
// is ignored for other index types. The default value is -180.0.
func (i *IndexOptionsBuilder) SetMin(min float64) *IndexOptionsBuilder {
	i.Opts = append(i.Opts, func(opts *IndexOptions) error {
		opts.Min = &min

		return nil
	})

	return i
}

// SetBucketSize sets the value for the BucketSize field. Specifies the number of units
// within which to group location values. Location values that are within BucketSize
// units of each other will be grouped in the same bucket. This option is only applicable
// to geoHaystack indexes and is ignored for other index types. The value must be greater
// than 0.
func (i *IndexOptionsBuilder) SetBucketSize(bucketSize int32) *IndexOptionsBuilder {
	i.Opts = append(i.Opts, func(opts *IndexOptions) error {
		opts.BucketSize = &bucketSize

		return nil
	})

	return i
}

// SetPartialFilterExpression sets the value for the PartialFilterExpression field. Sets
// a document that defines which collection documents the index should reference.
func (i *IndexOptionsBuilder) SetPartialFilterExpression(expression interface{}) *IndexOptionsBuilder {
	i.Opts = append(i.Opts, func(opts *IndexOptions) error {
		opts.PartialFilterExpression = expression

		return nil
	})

	return i
}

// SetCollation sets the value for the Collation field. Specifies the collation to use for
// string comparisons for the index.
func (i *IndexOptionsBuilder) SetCollation(collation *Collation) *IndexOptionsBuilder {
	i.Opts = append(i.Opts, func(opts *IndexOptions) error {
		opts.Collation = collation

		return nil
	})

	return i
}

// SetWildcardProjection sets the value for the WildcardProjection field. Sets a document
// that defines the wildcard projection for the index.
func (i *IndexOptionsBuilder) SetWildcardProjection(wildcardProjection interface{}) *IndexOptionsBuilder {
	i.Opts = append(i.Opts, func(opts *IndexOptions) error {
		opts.WildcardProjection = wildcardProjection

		return nil
	})

	return i
}

// SetHidden sets the value for the Hidden field. If true, the index will exist on the
// target collection but will not be used by the query planner when executing operations.
// This option is only valid for MongoDB versions >= 4.4. The default value is false.
func (i *IndexOptionsBuilder) SetHidden(hidden bool) *IndexOptionsBuilder {
	i.Opts = append(i.Opts, func(opts *IndexOptions) error {
		opts.Hidden = &hidden

		return nil
	})

	return i
}
