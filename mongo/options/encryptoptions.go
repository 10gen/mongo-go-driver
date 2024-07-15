// Copyright (C) MongoDB, Inc. 2017-present.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

package options

import (
	"go.mongodb.org/mongo-driver/bson"
)

// These constants specify valid values for QueryType
// QueryType is used for Queryable Encryption.
const (
	QueryTypeEquality string = "equality"
)

// RangeOptions specifies index options for a Queryable Encryption field
// supporting "rangePreview" queries. Beta: The Range algorithm is experimental
// only. It is not intended for public use. It is subject to breaking changes.
type RangeOptions struct {
	Min       *bson.RawValue
	Max       *bson.RawValue
	Sparsity  int64
	Precision *int32
}

// RangeOptionsBuilder contains options to configure RangeArgs for queryeable
// encryption. Each option can be set through setter functions. See
// documentation for each setter function for an explanation of the option.
type RangeOptionsBuilder struct {
	Opts []func(*RangeOptions) error
}

// Range creates a new RangeOptions instance.
func Range() *RangeOptionsBuilder {
	return &RangeOptionsBuilder{}
}

// ArgsSetters returns a list of RangeArgs setter functions.
func (ro *RangeOptionsBuilder) ArgsSetters() []func(*RangeOptions) error {
	return ro.Opts
}

// EncryptOptions represents options to explicitly encrypt a value.
type EncryptOptions struct {
	KeyID            *bson.Binary
	KeyAltName       *string
	Algorithm        string
	QueryType        string
	ContentionFactor *int64
	RangeOptions     *RangeOptionsBuilder
}

// EncryptOptionsBuilder contains options to configure EncryptArgs for
// queryeable encryption. Each option can be set through setter functions. See
// documentation for each setter function for an explanation of the option.
type EncryptOptionsBuilder struct {
	Opts []func(*EncryptOptions) error
}

// ArgsSetters returns a list of EncryptArgs setter functions.
func (e *EncryptOptionsBuilder) ArgsSetters() []func(*EncryptOptions) error {
	return e.Opts
}

// Encrypt creates a new EncryptOptions instance.
func Encrypt() *EncryptOptionsBuilder {
	return &EncryptOptionsBuilder{}
}

// SetKeyID specifies an _id of a data key. This should be a UUID (a primitive.Binary with subtype 4).
func (e *EncryptOptionsBuilder) SetKeyID(keyID bson.Binary) *EncryptOptionsBuilder {
	e.Opts = append(e.Opts, func(args *EncryptOptions) error {
		args.KeyID = &keyID

		return nil
	})
	return e
}

// SetKeyAltName identifies a key vault document by 'keyAltName'.
func (e *EncryptOptionsBuilder) SetKeyAltName(keyAltName string) *EncryptOptionsBuilder {
	e.Opts = append(e.Opts, func(args *EncryptOptions) error {
		args.KeyAltName = &keyAltName

		return nil
	})

	return e
}

// SetAlgorithm specifies an algorithm to use for encryption. This should be one of the following:
// - AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic
// - AEAD_AES_256_CBC_HMAC_SHA_512-Random
// - Indexed
// - Unindexed
// This is required.
// Indexed and Unindexed are used for Queryable Encryption.
func (e *EncryptOptionsBuilder) SetAlgorithm(algorithm string) *EncryptOptionsBuilder {
	e.Opts = append(e.Opts, func(args *EncryptOptions) error {
		args.Algorithm = algorithm

		return nil
	})

	return e
}

// SetQueryType specifies the intended query type. It is only valid to set if algorithm is "Indexed".
// This should be one of the following:
// - equality
// QueryType is used for Queryable Encryption.
func (e *EncryptOptionsBuilder) SetQueryType(queryType string) *EncryptOptionsBuilder {
	e.Opts = append(e.Opts, func(args *EncryptOptions) error {
		args.QueryType = queryType

		return nil
	})

	return e
}

// SetContentionFactor specifies the contention factor. It is only valid to set if algorithm is "Indexed".
// ContentionFactor is used for Queryable Encryption.
func (e *EncryptOptionsBuilder) SetContentionFactor(contentionFactor int64) *EncryptOptionsBuilder {
	e.Opts = append(e.Opts, func(args *EncryptOptions) error {
		args.ContentionFactor = &contentionFactor

		return nil
	})

	return e
}

// SetRangeOptions specifies the options to use for explicit encryption with range. It is only valid to set if algorithm is "rangePreview".
// Beta: The Range algorithm is experimental only. It is not intended for public use. It is subject to breaking changes.
func (e *EncryptOptionsBuilder) SetRangeOptions(ro *RangeOptionsBuilder) *EncryptOptionsBuilder {
	e.Opts = append(e.Opts, func(args *EncryptOptions) error {
		args.RangeOptions = ro

		return nil
	})

	return e
}

// SetMin sets the range index minimum value.
// Beta: The Range algorithm is experimental only. It is not intended for public use. It is subject to breaking changes.
func (ro *RangeOptionsBuilder) SetMin(min bson.RawValue) *RangeOptionsBuilder {
	ro.Opts = append(ro.Opts, func(args *RangeOptions) error {
		args.Min = &min

		return nil
	})

	return ro
}

// SetMax sets the range index maximum value.
// Beta: The Range algorithm is experimental only. It is not intended for public use. It is subject to breaking changes.
func (ro *RangeOptionsBuilder) SetMax(max bson.RawValue) *RangeOptionsBuilder {
	ro.Opts = append(ro.Opts, func(args *RangeOptions) error {
		args.Max = &max

		return nil
	})

	return ro
}

// SetSparsity sets the range index sparsity.
// Beta: The Range algorithm is experimental only. It is not intended for public use. It is subject to breaking changes.
func (ro *RangeOptionsBuilder) SetSparsity(sparsity int64) *RangeOptionsBuilder {
	ro.Opts = append(ro.Opts, func(args *RangeOptions) error {
		args.Sparsity = sparsity

		return nil
	})

	return ro
}

// SetPrecision sets the range index precision.
// Beta: The Range algorithm is experimental only. It is not intended for public use. It is subject to breaking changes.
func (ro *RangeOptionsBuilder) SetPrecision(precision int32) *RangeOptionsBuilder {
	ro.Opts = append(ro.Opts, func(args *RangeOptions) error {
		args.Precision = &precision

		return nil
	})

	return ro
}
