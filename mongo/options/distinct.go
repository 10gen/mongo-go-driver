// Copyright (C) MongoDB, Inc. 2017-present.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

package options

// DistinctOption is for internal use.
type DistinctOption interface {
	DistinctOptioner

	DistinctName() string
	DistinctValue() interface{}
}

// DistinctOptioner is the interface implemented by types that can be used as
// Options for Distinct commands.
type DistinctOptioner interface {
	Optioner
	distinctOption()
}
