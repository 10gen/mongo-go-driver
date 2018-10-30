package bson

import (
	"github.com/mongodb/mongo-go-driver/bson/bsoncodec"
)

type marshalingTestCase struct {
	name string
	reg  *bsoncodec.Registry
	val  interface{}
	want []byte
}

var marshalingTestCases = []marshalingTestCase{
	{
		"small struct",
		nil,
		struct {
			Foo bool
		}{Foo: true},
		docToBytes(NewDocumentv2(Elementv2{"foo", Boolean(true)})),
	},
}
