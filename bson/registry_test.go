// Copyright (C) MongoDB, Inc. 2017-present.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

package bson

import (
	"errors"
	"reflect"
	"testing"

	"github.com/google/go-cmp/cmp"
	"go.mongodb.org/mongo-driver/v2/internal/assert"
)

// newTestRegistry creates a new Registry.
func newTestRegistry() *Registry {
	return &Registry{
		typeEncoders: new(typeEncoderCache),
		typeDecoders: new(typeDecoderCache),
		kindEncoders: new(kindEncoderCache),
		kindDecoders: new(kindDecoderCache),
	}
}

func TestRegistry(t *testing.T) {
	t.Parallel()

	t.Run("Register", func(t *testing.T) {
		t.Parallel()

		fc1, fc2, fc3, fc4 := new(fakeCodec), new(fakeCodec), new(fakeCodec), new(fakeCodec)
		t.Run("interface", func(t *testing.T) {
			t.Parallel()

			var t1f *testInterface1
			var t2f *testInterface2
			var t4f *testInterface4
			ips := []interfaceValueEncoder{
				{i: reflect.TypeOf(t1f).Elem(), ve: fc1},
				{i: reflect.TypeOf(t2f).Elem(), ve: fc2},
				{i: reflect.TypeOf(t1f).Elem(), ve: fc3},
				{i: reflect.TypeOf(t4f).Elem(), ve: fc4},
			}
			want := []interfaceValueEncoder{
				{i: reflect.TypeOf(t1f).Elem(), ve: fc3},
				{i: reflect.TypeOf(t2f).Elem(), ve: fc2},
				{i: reflect.TypeOf(t4f).Elem(), ve: fc4},
			}
			reg := newTestRegistry()
			for _, ip := range ips {
				reg.RegisterInterfaceEncoder(ip.i, ip.ve)
			}
			got := reg.interfaceEncoders
			if !cmp.Equal(got, want, cmp.AllowUnexported(interfaceValueEncoder{}, fakeCodec{}), cmp.Comparer(typeComparer)) {
				t.Errorf("registered interfaces are not correct: got %#v, want %#v", got, want)
			}
		})
		t.Run("type", func(t *testing.T) {
			t.Parallel()

			ft1, ft2, ft4 := fakeType1{}, fakeType2{}, fakeType4{}
			reg := newTestRegistry()
			reg.RegisterTypeEncoder(reflect.TypeOf(ft1), fc1)
			reg.RegisterTypeEncoder(reflect.TypeOf(ft2), fc2)
			reg.RegisterTypeEncoder(reflect.TypeOf(ft1), fc3)
			reg.RegisterTypeEncoder(reflect.TypeOf(ft4), fc4)

			want := []struct {
				t reflect.Type
				c ValueEncoder
			}{
				{reflect.TypeOf(ft1), fc3},
				{reflect.TypeOf(ft2), fc2},
				{reflect.TypeOf(ft4), fc4},
			}
			got := reg.typeEncoders
			for _, s := range want {
				wantT, wantC := s.t, s.c
				gotC, exists := got.Load(wantT)
				if !exists {
					t.Errorf("type missing in registry: %v", wantT)
				}
				if !cmp.Equal(gotC, wantC, cmp.AllowUnexported(fakeCodec{})) {
					t.Errorf("codecs did not match: got %#v; want %#v", gotC, wantC)
				}
			}
		})
		t.Run("kind", func(t *testing.T) {
			t.Parallel()

			k1, k2, k4 := reflect.Struct, reflect.Slice, reflect.Map
			reg := newTestRegistry()
			reg.RegisterKindEncoder(k1, fc1)
			reg.RegisterKindEncoder(k2, fc2)
			reg.RegisterKindEncoder(k1, fc3)
			reg.RegisterKindEncoder(k4, fc4)

			want := []struct {
				k reflect.Kind
				c ValueEncoder
			}{
				{k1, fc3},
				{k2, fc2},
				{k4, fc4},
			}
			got := reg.kindEncoders
			for _, s := range want {
				wantK, wantC := s.k, s.c
				gotC, exists := got.Load(wantK)
				if !exists {
					t.Errorf("type missing in registry: %v", wantK)
				}
				if !cmp.Equal(gotC, wantC, cmp.AllowUnexported(fakeCodec{})) {
					t.Errorf("codecs did not match: got %#v, want %#v", gotC, wantC)
				}
			}
		})
		t.Run("RegisterDefault", func(t *testing.T) {
			t.Parallel()

			t.Run("MapCodec", func(t *testing.T) {
				t.Parallel()

				codec := &fakeCodec{num: 1}
				codec2 := &fakeCodec{num: 2}
				reg := newTestRegistry()
				reg.RegisterKindEncoder(reflect.Map, codec)
				if reg.kindEncoders.get(reflect.Map) != codec {
					t.Errorf("map codec not properly set: got %#v, want %#v", reg.kindEncoders.get(reflect.Map), codec)
				}
				reg.RegisterKindEncoder(reflect.Map, codec2)
				if reg.kindEncoders.get(reflect.Map) != codec2 {
					t.Errorf("map codec properly set: got %#v, want %#v", reg.kindEncoders.get(reflect.Map), codec2)
				}
			})
			t.Run("StructCodec", func(t *testing.T) {
				t.Parallel()

				codec := &fakeCodec{num: 1}
				codec2 := &fakeCodec{num: 2}
				reg := newTestRegistry()
				reg.RegisterKindEncoder(reflect.Struct, codec)
				if reg.kindEncoders.get(reflect.Struct) != codec {
					t.Errorf("struct codec not properly set: got %#v, want %#v", reg.kindEncoders.get(reflect.Struct), codec)
				}
				reg.RegisterKindEncoder(reflect.Struct, codec2)
				if reg.kindEncoders.get(reflect.Struct) != codec2 {
					t.Errorf("struct codec not properly set: got %#v, want %#v", reg.kindEncoders.get(reflect.Struct), codec2)
				}
			})
			t.Run("SliceCodec", func(t *testing.T) {
				t.Parallel()

				codec := &fakeCodec{num: 1}
				codec2 := &fakeCodec{num: 2}
				reg := newTestRegistry()
				reg.RegisterKindEncoder(reflect.Slice, codec)
				if reg.kindEncoders.get(reflect.Slice) != codec {
					t.Errorf("slice codec not properly set: got %#v, want %#v", reg.kindEncoders.get(reflect.Slice), codec)
				}
				reg.RegisterKindEncoder(reflect.Slice, codec2)
				if reg.kindEncoders.get(reflect.Slice) != codec2 {
					t.Errorf("slice codec not properly set: got %#v, want %#v", reg.kindEncoders.get(reflect.Slice), codec2)
				}
			})
			t.Run("ArrayCodec", func(t *testing.T) {
				t.Parallel()

				codec := &fakeCodec{num: 1}
				codec2 := &fakeCodec{num: 2}
				reg := newTestRegistry()
				reg.RegisterKindEncoder(reflect.Array, codec)
				if reg.kindEncoders.get(reflect.Array) != codec {
					t.Errorf("slice codec not properly set: got %#v, want %#v", reg.kindEncoders.get(reflect.Array), codec)
				}
				reg.RegisterKindEncoder(reflect.Array, codec2)
				if reg.kindEncoders.get(reflect.Array) != codec2 {
					t.Errorf("slice codec not properly set: got %#v, want %#v", reg.kindEncoders.get(reflect.Array), codec2)
				}
			})
		})
		t.Run("Lookup", func(t *testing.T) {
			t.Parallel()

			type Codec interface {
				ValueEncoder
				ValueDecoder
			}

			var (
				arrinstance     [12]int
				arr             = reflect.TypeOf(arrinstance)
				slc             = reflect.TypeOf(make([]int, 12))
				m               = reflect.TypeOf(make(map[string]int))
				strct           = reflect.TypeOf(struct{ Foo string }{})
				ft1             = reflect.PtrTo(reflect.TypeOf(fakeType1{}))
				ft2             = reflect.TypeOf(fakeType2{})
				ft3             = reflect.TypeOf(fakeType5(func(string, string) string { return "fakeType5" }))
				ti1             = reflect.TypeOf((*testInterface1)(nil)).Elem()
				ti2             = reflect.TypeOf((*testInterface2)(nil)).Elem()
				ti1Impl         = reflect.TypeOf(testInterface1Impl{})
				ti2Impl         = reflect.TypeOf(testInterface2Impl{})
				ti3             = reflect.TypeOf((*testInterface3)(nil)).Elem()
				ti3Impl         = reflect.TypeOf(testInterface3Impl{})
				ti3ImplPtr      = reflect.TypeOf((*testInterface3Impl)(nil))
				fc1, fc2        = &fakeCodec{num: 1}, &fakeCodec{num: 2}
				fsc, fslcc, fmc = new(fakeStructCodec), new(fakeSliceCodec), new(fakeMapCodec)
				pc              = &pointerCodec{}
			)

			reg := newTestRegistry()
			reg.RegisterTypeEncoder(ft1, fc1)
			reg.RegisterTypeEncoder(ft2, fc2)
			reg.RegisterTypeEncoder(ti1, fc1)
			reg.RegisterKindEncoder(reflect.Struct, fsc)
			reg.RegisterKindEncoder(reflect.Slice, fslcc)
			reg.RegisterKindEncoder(reflect.Array, fslcc)
			reg.RegisterKindEncoder(reflect.Map, fmc)
			reg.RegisterKindEncoder(reflect.Ptr, pc)
			reg.RegisterTypeDecoder(ft1, fc1)
			reg.RegisterTypeDecoder(ft2, fc2)
			reg.RegisterTypeDecoder(ti1, fc1) // values whose exact type is testInterface1 will use fc1 encoder
			reg.RegisterKindDecoder(reflect.Struct, fsc)
			reg.RegisterKindDecoder(reflect.Slice, fslcc)
			reg.RegisterKindDecoder(reflect.Array, fslcc)
			reg.RegisterKindDecoder(reflect.Map, fmc)
			reg.RegisterKindDecoder(reflect.Ptr, pc)
			reg.RegisterInterfaceEncoder(ti2, fc2)
			reg.RegisterInterfaceDecoder(ti2, fc2)
			reg.RegisterInterfaceEncoder(ti3, fc3)
			reg.RegisterInterfaceDecoder(ti3, fc3)

			testCases := []struct {
				name      string
				t         reflect.Type
				wantcodec Codec
				wanterr   error
				testcache bool
			}{
				{
					"type registry (pointer)",
					ft1,
					fc1,
					nil,
					false,
				},
				{
					"type registry (non-pointer)",
					ft2,
					fc2,
					nil,
					false,
				},
				{
					// lookup an interface type and expect that the registered encoder is returned
					"interface with type encoder",
					ti1,
					fc1,
					nil,
					true,
				},
				{
					// lookup a type that implements an interface and expect that the default struct codec is returned
					"interface implementation with type encoder",
					ti1Impl,
					fsc,
					nil,
					false,
				},
				{
					// lookup an interface type and expect that the registered hook is returned
					"interface with hook",
					ti2,
					fc2,
					nil,
					false,
				},
				{
					// lookup a type that implements an interface and expect that the registered hook is returned
					"interface implementation with hook",
					ti2Impl,
					fc2,
					nil,
					false,
				},
				{
					// lookup a pointer to a type where the pointer implements an interface and expect that the
					// registered hook is returned
					"interface pointer to implementation with hook (pointer)",
					ti3ImplPtr,
					fc3,
					nil,
					false,
				},
				{
					"default struct codec (pointer)",
					reflect.PtrTo(strct),
					pc,
					nil,
					false,
				},
				{
					"default struct codec (non-pointer)",
					strct,
					fsc,
					nil,
					false,
				},
				{
					"default array codec",
					arr,
					fslcc,
					nil,
					false,
				},
				{
					"default slice codec",
					slc,
					fslcc,
					nil,
					false,
				},
				{
					"default map",
					m,
					fmc,
					nil,
					false,
				},
				{
					"map non-string key",
					reflect.TypeOf(map[int]int{}),
					fmc,
					nil,
					false,
				},
				{
					"No Codec Registered",
					ft3,
					nil,
					errNoEncoder{Type: ft3},
					false,
				},
			}

			allowunexported := cmp.AllowUnexported(fakeCodec{}, fakeStructCodec{}, fakeSliceCodec{}, fakeMapCodec{})
			comparepc := func(pc1, pc2 *pointerCodec) bool { return pc1 == pc2 }
			for _, tc := range testCases {
				tc := tc

				t.Run(tc.name, func(t *testing.T) {
					t.Parallel()

					t.Run("Encoder", func(t *testing.T) {
						t.Parallel()

						gotcodec, goterr := reg.LookupEncoder(tc.t)
						if !cmp.Equal(goterr, tc.wanterr, cmp.Comparer(assert.CompareErrors)) {
							t.Errorf("errors did not match: got %#v, want %#v", goterr, tc.wanterr)
						}
						if !cmp.Equal(gotcodec, tc.wantcodec, allowunexported, cmp.Comparer(comparepc)) {
							t.Errorf("codecs did not match: got %#v, want %#v", gotcodec, tc.wantcodec)
						}
					})
					t.Run("Decoder", func(t *testing.T) {
						t.Parallel()

						wanterr := tc.wanterr
						var ene errNoEncoder
						if errors.As(tc.wanterr, &ene) {
							wanterr = errNoDecoder(ene)
						}

						gotcodec, goterr := reg.LookupDecoder(tc.t)
						if !cmp.Equal(goterr, wanterr, cmp.Comparer(assert.CompareErrors)) {
							t.Errorf("errors did not match: got %#v, want %#v", goterr, wanterr)
						}
						if !cmp.Equal(gotcodec, tc.wantcodec, allowunexported, cmp.Comparer(comparepc)) {
							t.Errorf("codecs did not match: got %v: want %v", gotcodec, tc.wantcodec)
						}
					})
				})
			}
			t.Run("nil type", func(t *testing.T) {
				t.Parallel()

				t.Run("Encoder", func(t *testing.T) {
					t.Parallel()

					wanterr := errNoEncoder{Type: reflect.TypeOf(nil)}

					gotcodec, goterr := reg.LookupEncoder(nil)
					if !cmp.Equal(goterr, wanterr, cmp.Comparer(assert.CompareErrors)) {
						t.Errorf("errors did not match: got %#v, want %#v", goterr, wanterr)
					}
					if !cmp.Equal(gotcodec, nil, allowunexported, cmp.Comparer(comparepc)) {
						t.Errorf("codecs did not match: got %#v, want nil", gotcodec)
					}
				})
				t.Run("Decoder", func(t *testing.T) {
					t.Parallel()

					wanterr := "cannot perform a decoder lookup on <nil>"

					gotcodec, goterr := reg.LookupDecoder(nil)
					assert.EqualError(t, goterr, wanterr, "errors did not match")
					if !cmp.Equal(gotcodec, nil, allowunexported, cmp.Comparer(comparepc)) {
						t.Errorf("codecs did not match: got %v: want nil", gotcodec)
					}
				})
			})
			// lookup a type whose pointer implements an interface and expect that the registered hook is
			// returned
			t.Run("interface implementation with hook (pointer)", func(t *testing.T) {
				t.Parallel()

				t.Run("Encoder", func(t *testing.T) {
					t.Parallel()
					gotEnc, err := reg.LookupEncoder(ti3Impl)
					assert.Nil(t, err, "LookupEncoder error: %v", err)

					cae, ok := gotEnc.(*condAddrEncoder)
					assert.True(t, ok, "Expected CondAddrEncoder, got %T", gotEnc)
					if !cmp.Equal(cae.canAddrEnc, fc3, allowunexported, cmp.Comparer(comparepc)) {
						t.Errorf("expected canAddrEnc %#v, got %#v", cae.canAddrEnc, fc3)
					}
					if !cmp.Equal(cae.elseEnc, fsc, allowunexported, cmp.Comparer(comparepc)) {
						t.Errorf("expected elseEnc %#v, got %#v", cae.elseEnc, fsc)
					}
				})
				t.Run("Decoder", func(t *testing.T) {
					t.Parallel()

					gotDec, err := reg.LookupDecoder(ti3Impl)
					assert.Nil(t, err, "LookupDecoder error: %v", err)

					cad, ok := gotDec.(*condAddrDecoder)
					assert.True(t, ok, "Expected CondAddrDecoder, got %T", gotDec)
					if !cmp.Equal(cad.canAddrDec, fc3, allowunexported, cmp.Comparer(comparepc)) {
						t.Errorf("expected canAddrDec %#v, got %#v", cad.canAddrDec, fc3)
					}
					if !cmp.Equal(cad.elseDec, fsc, allowunexported, cmp.Comparer(comparepc)) {
						t.Errorf("expected elseDec %#v, got %#v", cad.elseDec, fsc)
					}
				})
			})
		})
	})
	t.Run("Type Map", func(t *testing.T) {
		t.Parallel()
		reg := newTestRegistry()
		reg.RegisterTypeMapEntry(TypeString, reflect.TypeOf(""))
		reg.RegisterTypeMapEntry(TypeInt32, reflect.TypeOf(int(0)))

		var got, want reflect.Type

		want = reflect.TypeOf("")
		got, err := reg.LookupTypeMapEntry(TypeString)
		noerr(t, err)
		if got != want {
			t.Errorf("unexpected type: got %#v, want %#v", got, want)
		}

		want = reflect.TypeOf(int(0))
		got, err = reg.LookupTypeMapEntry(TypeInt32)
		noerr(t, err)
		if got != want {
			t.Errorf("unexpected type: got %#v, want %#v", got, want)
		}

		want = nil
		wanterr := errNoTypeMapEntry{Type: TypeObjectID}
		got, err = reg.LookupTypeMapEntry(TypeObjectID)
		if !errors.Is(err, wanterr) {
			t.Errorf("unexpected error: got %#v, want %#v", err, wanterr)
		}
		if got != want {
			t.Errorf("unexpected error: got %#v, want %#v", got, want)
		}
	})
}

// get is only for testing as it does return if the value was found
func (c *kindEncoderCache) get(rt reflect.Kind) ValueEncoder {
	e, _ := c.Load(rt)
	return e
}

func BenchmarkLookupEncoder(b *testing.B) {
	type childStruct struct {
		V1, V2, V3, V4 int
	}
	type nestedStruct struct {
		childStruct
		A struct{ C1, C2, C3, C4 childStruct }
		B struct{ C1, C2, C3, C4 childStruct }
		C struct{ M1, M2, M3, M4 map[int]int }
	}
	types := [...]reflect.Type{
		reflect.TypeOf(int64(1)),
		reflect.TypeOf(&fakeCodec{}),
		reflect.TypeOf(&testInterface1Impl{}),
		reflect.TypeOf(&nestedStruct{}),
	}
	r := NewRegistry()
	for _, typ := range types {
		r.RegisterTypeEncoder(typ, &fakeCodec{})
	}
	b.Run("Serial", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, err := r.LookupEncoder(types[i%len(types)])
			if err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("Parallel", func(b *testing.B) {
		b.RunParallel(func(pb *testing.PB) {
			for i := 0; pb.Next(); i++ {
				_, err := r.LookupEncoder(types[i%len(types)])
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	})
}

type fakeType1 struct{}
type fakeType2 struct{}
type fakeType4 struct{}
type fakeType5 func(string, string) string
type fakeStructCodec struct{ *fakeCodec }
type fakeSliceCodec struct{ *fakeCodec }
type fakeMapCodec struct{ *fakeCodec }

type fakeCodec struct {
	// num is used to differentiate fakeCodec instances and to force Go to allocate a new value in
	// memory for every fakeCodec. If fakeCodec were an empty struct, Go may use the same pointer
	// for every instance of fakeCodec, making comparisons between pointers to instances of
	// fakeCodec sometimes meaningless.
	num int
}

func (*fakeCodec) EncodeValue(EncodeContext, ValueWriter, reflect.Value) error {
	return nil
}
func (*fakeCodec) DecodeValue(DecodeContext, ValueReader, reflect.Value) error {
	return nil
}

type testInterface1 interface{ test1() }
type testInterface2 interface{ test2() }
type testInterface3 interface{ test3() }
type testInterface4 interface{ test4() }

type testInterface1Impl struct{}

var _ testInterface1 = testInterface1Impl{}

func (testInterface1Impl) test1() {}

type testInterface2Impl struct{}

var _ testInterface2 = testInterface2Impl{}

func (testInterface2Impl) test2() {}

type testInterface3Impl struct{}

var _ testInterface3 = (*testInterface3Impl)(nil)

func (*testInterface3Impl) test3() {}

func typeComparer(i1, i2 reflect.Type) bool { return i1 == i2 }
