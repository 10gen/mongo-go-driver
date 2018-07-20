package bson

import (
	"errors"
	"reflect"
	"sync"
)

// ErrNoCodec is returned when there is no codec available for a type or interface in the registry.
type ErrNoCodec struct {
	Type reflect.Type
}

func (enc ErrNoCodec) Error() string {
	return "no codec found for " + enc.Type.String()
}

// ErrNotInterface is returned when the provided type is not an interface.
var ErrNotInterface = errors.New("The provided typeis not an interface")

var defaultRegistry = NewRegistryBuilder().Build()

// ErrFrozenRegistry is returned when an attempt to mutate a frozen Registry is
// made. A Registry is considered frozen when a call to Lookup has been made.
var ErrFrozenRegistry = errors.New("the Registry has been frozen and can no longer be modified")

// A RegistryBuilder is used to build a Registry. This type is not goroutine
// safe.
type RegistryBuilder struct {
	types      map[reflect.Type]Codec
	interfaces []interfacePair
	kinds      map[reflect.Kind]Codec
}

// A Registry is used to store and retrieve codecs for types and interfaces. This type is the main
// typed passed around and Encoders and Decoders are constructed from it.
//
// TODO: Create a RegistryBuilder type and make the Registry type immutable.
type Registry struct {
	tr       typeRegistry
	kr       kindRegistry
	ir       interfaceRegistry
	ircache  map[reflect.Type]Codec
	ircacheL sync.RWMutex
}

// NewRegistryBuilder creates a new RegistryBuilder.
func NewRegistryBuilder() *RegistryBuilder {
	types := map[reflect.Type]Codec{}
	kinds := map[reflect.Kind]Codec{
		reflect.Bool:    defaultBoolCodec,
		reflect.Int:     defaultIntCodec,
		reflect.Int8:    defaultIntCodec,
		reflect.Int16:   defaultIntCodec,
		reflect.Int32:   defaultIntCodec,
		reflect.Int64:   defaultIntCodec,
		reflect.Uint:    defaultUintCodec,
		reflect.Uint8:   defaultUintCodec,
		reflect.Uint16:  defaultUintCodec,
		reflect.Uint32:  defaultUintCodec,
		reflect.Uint64:  defaultUintCodec,
		reflect.Float32: defaultFloatCodec,
		reflect.Float64: defaultFloatCodec,
		reflect.Array:   defaultSliceCodec,
		reflect.Map:     defaultMapCodec,
		reflect.Slice:   defaultSliceCodec,
		reflect.String:  defaultStringCodec,
		reflect.Struct:  defaultStructCodec,
	}

	return &RegistryBuilder{
		types:      types,
		kinds:      kinds,
		interfaces: make([]interfacePair, 0),
	}
}

// NewEmptyRegistryBuilder creates a new RegistryBuilder with no default kind
// Codecs.
func NewEmptyRegistryBuilder() *RegistryBuilder {
	return &RegistryBuilder{
		types:      make(map[reflect.Type]Codec),
		kinds:      make(map[reflect.Kind]Codec),
		interfaces: make([]interfacePair, 0),
	}
}

// Register will register the provided Codec to the provided type. If the type is
// an interface, it will be registered in the interface registry. If the type is
// a pointer to or a type that is not an interface, it will be registered in the type
// registry.
func (r *RegistryBuilder) Register(t reflect.Type, codec Codec) *RegistryBuilder {
	switch t.Kind() {
	case reflect.Interface:
		for idx, ip := range r.interfaces {
			if ip.i == t {
				r.interfaces[idx].c = codec
				return r
			}
		}

		r.interfaces = append(r.interfaces, interfacePair{i: t, c: codec})
	default:
		if t.Kind() != reflect.Ptr {
			t = reflect.PtrTo(t)
		}

		r.types[t] = codec
	}
	return r
}

// RegisterDefault will register the provided Codec to the provided kind.
func (rb *RegistryBuilder) RegisterDefault(kind reflect.Kind, codec Codec) *RegistryBuilder {
	rb.kinds[kind] = codec
	return rb
}

// Build creates a Registry from the current state of this RegistryBuilder.
func (rb *RegistryBuilder) Build() *Registry {
	tr := make(typeRegistry)
	for t, c := range rb.types {
		tr[t] = c
	}
	kr := make(kindRegistry)
	for k, c := range rb.kinds {
		kr[k] = c
	}

	ir := make(interfaceRegistry, len(rb.interfaces))
	copy(ir, rb.interfaces)

	return &Registry{
		tr:      tr,
		kr:      kr,
		ir:      ir,
		ircache: make(map[reflect.Type]Codec),
	}
}

// Lookup will inspect the type registry for either the type or a pointer to the type,
// if it doesn't find a codec it will inspect the interface registry for an interface
// that the type satisfies, if it doesn't find a codec there it will attempt to
// return either the default map codec or the default struct codec. If none of those
// apply, an error will be returned.
func (r *Registry) Lookup(t reflect.Type) (Codec, error) {
	// We make this year so if we strip a pointer off it won't confuse user. If
	// we did it where we return this and the user provided a pointer to the
	// type, the error message would be for a lookup for the non-pointer version
	// of the type.
	codecerr := ErrNoCodec{Type: t}
	codec, found := r.tr.lookup(t)
	if found {
		return codec, nil
	}

	r.ircacheL.RLock()
	codec, found = r.ircache[t]
	r.ircacheL.RUnlock()
	if found {
		return codec, nil
	}

	codec, found = r.ir.lookup(t)
	if found {
		r.ircacheL.Lock()
		r.ircache[t] = codec
		r.ircacheL.Unlock()
		return codec, nil
	}

	// We don't allow maps with non-string keys
	if t.Kind() == reflect.Map && t.Key().Kind() != reflect.String {
		return nil, ErrNoCodec{Type: t}
	}

	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	codec, found = r.kr.lookup(t.Kind())
	if !found {
		return nil, codecerr
	}

	return codec, nil
}

// The type registry handles codecs that are for specifics types that are not interfaces.
// This registry will handle both the types themselves and pointers to those types.
type typeRegistry map[reflect.Type]Codec

// lookup handles finding a codec for the registered type. Will return an error if no codec
// could be found.
func (tr typeRegistry) lookup(t reflect.Type) (Codec, bool) {
	if t.Kind() != reflect.Ptr {
		t = reflect.PtrTo(t)
	}

	codec, found := tr[t]
	return codec, found
}

type interfacePair struct {
	i reflect.Type
	c Codec
}

// The kind registry handles codecs that are for base kinds.
type kindRegistry map[reflect.Kind]Codec

// lookup handles finding a codec for the registered kind. Will return an error if no codec
// could be found.
func (kr kindRegistry) lookup(k reflect.Kind) (Codec, bool) {
	codec, found := kr[k]
	return codec, found
}

// The interface registry handles codecs that are for interface types.
type interfaceRegistry []interfacePair

// lookup handles finding a codec for the registered interface. Will return an error if no codec
// could be found.
func (ir interfaceRegistry) lookup(t reflect.Type) (Codec, bool) {
	for _, ip := range ir {
		if !t.Implements(ip.i) {
			continue
		}

		return ip.c, true
	}
	return nil, false
}
