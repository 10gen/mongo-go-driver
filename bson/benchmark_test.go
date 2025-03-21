// Copyright (C) MongoDB, Inc. 2017-present.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

package bson

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"sync"
	"testing"
)

type encodetest struct {
	Field1String  string
	Field1Int64   int64
	Field1Float64 float64
	Field2String  string
	Field2Int64   int64
	Field2Float64 float64
	Field3String  string
	Field3Int64   int64
	Field3Float64 float64
	Field4String  string
	Field4Int64   int64
	Field4Float64 float64
}

type nestedtest1 struct {
	Nested nestedtest2
}

type nestedtest2 struct {
	Nested nestedtest3
}

type nestedtest3 struct {
	Nested nestedtest4
}

type nestedtest4 struct {
	Nested nestedtest5
}

type nestedtest5 struct {
	Nested nestedtest6
}

type nestedtest6 struct {
	Nested nestedtest7
}

type nestedtest7 struct {
	Nested nestedtest8
}

type nestedtest8 struct {
	Nested nestedtest9
}

type nestedtest9 struct {
	Nested nestedtest10
}

type nestedtest10 struct {
	Nested nestedtest11
}

type nestedtest11 struct {
	Nested encodetest
}

var encodetestInstance = encodetest{
	Field1String:  "foo",
	Field1Int64:   1,
	Field1Float64: 3.0,
	Field2String:  "bar",
	Field2Int64:   2,
	Field2Float64: 3.1,
	Field3String:  "baz",
	Field3Int64:   3,
	Field3Float64: 3.14,
	Field4String:  "qux",
	Field4Int64:   4,
	Field4Float64: 3.141,
}

var nestedInstance = nestedtest1{
	nestedtest2{
		nestedtest3{
			nestedtest4{
				nestedtest5{
					nestedtest6{
						nestedtest7{
							nestedtest8{
								nestedtest9{
									nestedtest10{
										nestedtest11{
											encodetest{
												Field1String:  "foo",
												Field1Int64:   1,
												Field1Float64: 3.0,
												Field2String:  "bar",
												Field2Int64:   2,
												Field2Float64: 3.1,
												Field3String:  "baz",
												Field3Int64:   3,
												Field3Float64: 3.14,
												Field4String:  "qux",
												Field4Int64:   4,
												Field4Float64: 3.141,
											},
										},
									},
								},
							},
						},
					},
				},
			},
		},
	},
}

const extendedBSONDir = "../testdata/extended_bson"

var (
	extJSONFiles   map[string]map[string]interface{}
	extJSONFilesMu sync.Mutex
)

// readExtJSONFile reads the GZIP-compressed extended JSON document from the given filename in the
// "extended BSON" test data directory (../testdata/extended_bson) and returns it as a
// map[string]interface{}. It panics on any errors.
func readExtJSONFile(filename string) map[string]interface{} {
	extJSONFilesMu.Lock()
	defer extJSONFilesMu.Unlock()
	if v, ok := extJSONFiles[filename]; ok {
		return v
	}
	filePath := path.Join(extendedBSONDir, filename)
	file, err := os.Open(filePath)
	if err != nil {
		panic(fmt.Sprintf("error opening file %q: %s", filePath, err))
	}
	defer func() {
		_ = file.Close()
	}()

	gz, err := gzip.NewReader(file)
	if err != nil {
		panic(fmt.Sprintf("error creating GZIP reader: %s", err))
	}
	defer func() {
		_ = gz.Close()
	}()

	data, err := ioutil.ReadAll(gz)
	if err != nil {
		panic(fmt.Sprintf("error reading GZIP contents of file: %s", err))
	}

	var v map[string]interface{}
	err = UnmarshalExtJSON(data, false, &v)
	if err != nil {
		panic(fmt.Sprintf("error unmarshalling extended JSON: %s", err))
	}

	if extJSONFiles == nil {
		extJSONFiles = make(map[string]map[string]interface{})
	}
	extJSONFiles[filename] = v
	return v
}

func BenchmarkMarshal(b *testing.B) {
	cases := []struct {
		desc  string
		value any
	}{
		{
			desc:  "simple struct",
			value: encodetestInstance,
		},
		{
			desc:  "nested struct",
			value: nestedInstance,
		},
		{
			desc: "simple D",
			value: D{
				{"field1String", "foo"},
				{"field1Int64", int64(1)},
				{"field1Float64", float64(3.0)},
				{"field2String", "bar"},
				{"field2Int64", int64(2)},
				{"field2Float64", float64(3.1)},
				{"field3String", "baz"},
				{"field3Int64", int64(3)},
				{"field3Float64", float64(3.14)},
				{"field4String", "qux"},
				{"field4Int64", int64(4)},
				{"field4Float64", float64(3.141)},
			},
		},
		{
			desc:  "deep_bson.json.gz",
			value: readExtJSONFile("deep_bson.json.gz"),
		},
		{
			desc:  "flat_bson.json.gz",
			value: readExtJSONFile("flat_bson.json.gz"),
		},
		{
			desc:  "full_bson.json.gz",
			value: readExtJSONFile("full_bson.json.gz"),
		},
	}

	b.Run("BSON", func(b *testing.B) {
		for _, tc := range cases {
			tc := tc // Capture range variable.

			b.Run(tc.desc, func(b *testing.B) {
				b.ReportAllocs()
				b.RunParallel(func(pb *testing.PB) {
					for pb.Next() {
						_, err := Marshal(tc.value)
						if err != nil {
							b.Errorf("error marshalling BSON: %s", err)
						}
					}
				})
			})
		}
	})

	b.Run("extJSON", func(b *testing.B) {
		for _, tc := range cases {
			tc := tc // Capture range variable.

			b.Run(tc.desc, func(b *testing.B) {
				b.ReportAllocs()
				b.RunParallel(func(pb *testing.PB) {
					for pb.Next() {
						_, err := MarshalExtJSON(tc.value, true, false)
						if err != nil {
							b.Errorf("error marshalling extended JSON: %s", err)
						}
					}
				})
			})
		}
	})

	b.Run("JSON", func(b *testing.B) {
		for _, tc := range cases {
			tc := tc // Capture range variable.

			b.Run(tc.desc, func(b *testing.B) {
				b.ReportAllocs()
				b.RunParallel(func(pb *testing.PB) {
					for pb.Next() {
						_, err := json.Marshal(tc.value)
						if err != nil {
							b.Errorf("error marshalling JSON: %s", err)
						}
					}
				})
			})
		}
	})
}

func BenchmarkUnmarshal(b *testing.B) {
	type testcase struct {
		desc  string
		value any
		dst   func() any
	}

	cases := []testcase{
		{
			desc:  "simple struct",
			value: encodetestInstance,
			dst:   func() any { return &encodetest{} },
		},
		{
			desc:  "nested struct",
			value: nestedInstance,
			dst:   func() any { return &encodetest{} },
		},
	}

	inputs := []struct {
		name  string
		value any
	}{
		{
			name:  "simple",
			value: encodetestInstance,
		},
		{
			name:  "nested",
			value: nestedInstance,
		},
		{
			name:  "deep_bson.json.gz",
			value: readExtJSONFile("deep_bson.json.gz"),
		},
		{
			name:  "flat_bson.json.gz",
			value: readExtJSONFile("flat_bson.json.gz"),
		},
		{
			name:  "full_bson.json.gz",
			value: readExtJSONFile("full_bson.json.gz"),
		},
	}

	destinations := []struct {
		name string
		dst  func() any
	}{
		{
			name: "to map",
			dst:  func() any { return &map[string]any{} },
		},
		{
			name: "to D",
			dst:  func() any { return &D{} },
		},
	}

	for _, input := range inputs {
		for _, dest := range destinations {
			cases = append(cases, testcase{
				desc:  input.name + " " + dest.name,
				value: input.value,
				dst:   dest.dst,
			})
		}
	}

	b.Run("BSON", func(b *testing.B) {
		for _, tc := range cases {
			tc := tc // Capture range variable.

			b.Run(tc.desc, func(b *testing.B) {
				b.ReportAllocs()
				data, err := Marshal(tc.value)
				if err != nil {
					b.Errorf("error marshalling BSON: %s", err)
					return
				}

				b.SetBytes(int64(len(data)))
				b.ResetTimer()
				b.RunParallel(func(pb *testing.PB) {
					for pb.Next() {
						val := tc.dst()
						err := Unmarshal(data, val)
						if err != nil {
							b.Errorf("error unmarshalling BSON: %s", err)
						}
					}
				})
			})
		}
	})

	b.Run("extJSON", func(b *testing.B) {
		for _, tc := range cases {
			tc := tc // Capture range variable.

			b.Run(tc.desc, func(b *testing.B) {
				b.ReportAllocs()
				data, err := MarshalExtJSON(tc.value, true, false)
				if err != nil {
					b.Errorf("error marshalling extended JSON: %s", err)
					return
				}

				b.SetBytes(int64(len(data)))
				b.ResetTimer()
				b.RunParallel(func(pb *testing.PB) {
					for pb.Next() {
						val := tc.dst()
						err := UnmarshalExtJSON(data, true, val)
						if err != nil {
							b.Errorf("error unmarshalling extended JSON: %s", err)
						}
					}
				})
			})
		}
	})

	b.Run("JSON", func(b *testing.B) {
		for _, tc := range cases {
			tc := tc // Capture range variable.

			b.Run(tc.desc, func(b *testing.B) {
				b.ReportAllocs()
				data, err := json.Marshal(tc.value)
				if err != nil {
					b.Errorf("error marshalling JSON: %s", err)
					return
				}

				b.SetBytes(int64(len(data)))
				b.ResetTimer()
				b.RunParallel(func(pb *testing.PB) {
					for pb.Next() {
						val := tc.dst()
						err := json.Unmarshal(data, val)
						if err != nil {
							b.Errorf("error unmarshalling JSON: %s", err)
						}
					}
				})
			})
		}
	})
}

// The following benchmarks are copied from the Go standard library's
// encoding/json package.

type codeResponse struct {
	Tree     *codeNode `json:"tree"`
	Username string    `json:"username"`
}

type codeNode struct {
	Name     string      `json:"name"`
	Kids     []*codeNode `json:"kids"`
	CLWeight float64     `json:"cl_weight"`
	Touches  int         `json:"touches"`
	MinT     int64       `json:"min_t"`
	MaxT     int64       `json:"max_t"`
	MeanT    int64       `json:"mean_t"`
}

var codeJSON []byte
var codeBSON []byte
var codeStruct codeResponse

func codeInit() {
	f, err := os.Open("testdata/code.json.gz")
	if err != nil {
		panic(err)
	}
	defer f.Close()
	gz, err := gzip.NewReader(f)
	if err != nil {
		panic(err)
	}
	data, err := io.ReadAll(gz)
	if err != nil {
		panic(err)
	}

	codeJSON = data

	if err := json.Unmarshal(codeJSON, &codeStruct); err != nil {
		panic("json.Unmarshal code.json: " + err.Error())
	}

	if data, err = json.Marshal(&codeStruct); err != nil {
		panic("json.Marshal code.json: " + err.Error())
	}

	if codeBSON, err = Marshal(&codeStruct); err != nil {
		panic("Marshal code.json: " + err.Error())
	}

	if !bytes.Equal(data, codeJSON) {
		println("different lengths", len(data), len(codeJSON))
		for i := 0; i < len(data) && i < len(codeJSON); i++ {
			if data[i] != codeJSON[i] {
				println("re-marshal: changed at byte", i)
				println("orig: ", string(codeJSON[i-10:i+10]))
				println("new: ", string(data[i-10:i+10]))
				break
			}
		}
		panic("re-marshal code.json: different result")
	}
}

func BenchmarkCodeUnmarshal(b *testing.B) {
	if codeJSON == nil {
		b.StopTimer()
		codeInit()
		b.StartTimer()
	}
	b.Run("BSON", func(b *testing.B) {
		b.ReportAllocs()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				var r codeResponse
				if err := Unmarshal(codeBSON, &r); err != nil {
					b.Fatal("Unmarshal:", err)
				}
			}
		})
		b.SetBytes(int64(len(codeBSON)))
	})
	b.Run("JSON", func(b *testing.B) {
		b.ReportAllocs()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				var r codeResponse
				if err := json.Unmarshal(codeJSON, &r); err != nil {
					b.Fatal("json.Unmarshal:", err)
				}
			}
		})
		b.SetBytes(int64(len(codeJSON)))
	})
}

func BenchmarkCodeMarshal(b *testing.B) {
	if codeJSON == nil {
		b.StopTimer()
		codeInit()
		b.StartTimer()
	}
	b.Run("BSON", func(b *testing.B) {
		b.ReportAllocs()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				if _, err := Marshal(&codeStruct); err != nil {
					b.Fatal("Marshal:", err)
				}
			}
		})
		b.SetBytes(int64(len(codeBSON)))
	})
	b.Run("JSON", func(b *testing.B) {
		b.ReportAllocs()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				if _, err := json.Marshal(&codeStruct); err != nil {
					b.Fatal("json.Marshal:", err)
				}
			}
		})
		b.SetBytes(int64(len(codeJSON)))
	})
}
