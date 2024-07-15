// Copyright (C) MongoDB, Inc. 2022-present.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

package options

// RewrapManyDataKeyOptions represents all possible options used to decrypt and
// encrypt all matching data keys with a possibly new masterKey.
type RewrapManyDataKeyOptions struct {
	// Provider identifies the new KMS provider. If omitted, encrypting uses the current KMS provider.
	Provider *string

	// MasterKey identifies the new masterKey. If omitted, rewraps with the current masterKey.
	MasterKey interface{}
}

// RewrapManyDataKeyOptionsBuilder contains options to configure rewraping a
// data key. Each option can be set through setter functions. See documentation
// for each setter function for an explanation of the option.
type RewrapManyDataKeyOptionsBuilder struct {
	Opts []func(*RewrapManyDataKeyOptions) error
}

// RewrapManyDataKey creates a new RewrapManyDataKeyOptions instance.
func RewrapManyDataKey() *RewrapManyDataKeyOptionsBuilder {
	return new(RewrapManyDataKeyOptionsBuilder)
}

// ArgsSetters returns a list of CountArgs setter functions.
func (rmdko *RewrapManyDataKeyOptionsBuilder) ArgsSetters() []func(*RewrapManyDataKeyOptions) error {
	return rmdko.Opts
}

// SetProvider sets the value for the Provider field.
func (rmdko *RewrapManyDataKeyOptionsBuilder) SetProvider(provider string) *RewrapManyDataKeyOptionsBuilder {
	rmdko.Opts = append(rmdko.Opts, func(args *RewrapManyDataKeyOptions) error {
		args.Provider = &provider

		return nil
	})

	return rmdko
}

// SetMasterKey sets the value for the MasterKey field.
func (rmdko *RewrapManyDataKeyOptionsBuilder) SetMasterKey(masterKey interface{}) *RewrapManyDataKeyOptionsBuilder {
	rmdko.Opts = append(rmdko.Opts, func(args *RewrapManyDataKeyOptions) error {
		args.MasterKey = masterKey

		return nil
	})

	return rmdko
}
