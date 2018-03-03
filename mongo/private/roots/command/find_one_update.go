package command

import (
	"context"

	"github.com/mongodb/mongo-go-driver/bson"
	"github.com/mongodb/mongo-go-driver/mongo/private/options"
	"github.com/mongodb/mongo-go-driver/mongo/private/roots/description"
	"github.com/mongodb/mongo-go-driver/mongo/private/roots/wiremessage"
)

// FindOneAndUpdate represents the findOneAndUpdate operation.
//
// The findOneAndUpdate command modifies and returns a single document.
type FindOneAndUpdate struct {
	NS     Namespace
	Query  *bson.Document
	Update *bson.Document
	Opts   []options.FindOneAndUpdateOptioner
}

// Encode will encode this command into a wire message for the given server description.
func (f *FindOneAndUpdate) Encode(description.Server) (wiremessage.WireMessage, error) {
	return nil, nil
}

// Decode will decode the wire message using the provided server description. Errors during decoding
// are deferred until either the Result or Err methods are called.
func (f *FindOneAndUpdate) Decode(description.Server, wiremessage.WireMessage) *FindOneAndUpdate {
	return nil
}

// Result returns the result of a decoded wire message and server description.
func (f *FindOneAndUpdate) Result() (Cursor, error) { return nil, nil }

// Err returns the error set on this command.
func (f *FindOneAndUpdate) Err() error { return nil }

// RoundTrip handles the execution of this command using the provided wiremessage.ReadWriter.
func (f *FindOneAndUpdate) RoundTrip(context.Context, description.Server, wiremessage.ReadWriter) (Cursor, error) {
	return nil, nil
}
