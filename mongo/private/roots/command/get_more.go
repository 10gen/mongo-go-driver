package command

import (
	"context"

	"github.com/mongodb/mongo-go-driver/bson"
	"github.com/mongodb/mongo-go-driver/mongo/private/options"
	"github.com/mongodb/mongo-go-driver/mongo/private/roots/description"
	"github.com/mongodb/mongo-go-driver/mongo/private/roots/wiremessage"
)

// GetMore represents the getMore command.
//
// The getMore command retrieves additional documents from a cursor.
type GetMore struct {
	ID   int64
	NS   Namespace
	Opts []options.CursorOptioner

	result bson.Reader
	err    error
}

// Encode will encode this command into a wire message for the given server description.
func (gm *GetMore) Encode(desc description.SelectedServer) (wiremessage.WireMessage, error) {
	cmd := bson.NewDocument(
		bson.EC.Int64("getMore", gm.ID),
		bson.EC.String("collection", gm.NS.Collection),
	)
	for _, opt := range gm.Opts {
		opt.Option(cmd)
	}
	return (&Command{DB: gm.NS.DB, Command: cmd}).Encode(desc)
}

// Decode will decode the wire message using the provided server description. Errors during decoding
// are deferred until either the Result or Err methods are called.
func (gm *GetMore) Decode(desc description.SelectedServer, wm wiremessage.WireMessage) *GetMore {
	gm.result, gm.err = (&Command{}).Decode(desc, wm).Result()
	return gm
}

// Result returns the result of a decoded wire message and server description.
func (gm *GetMore) Result() (bson.Reader, error) {
	if gm.err != nil {
		return nil, gm.err
	}
	return gm.result, nil
}

// Err returns the error set on this command.
func (gm *GetMore) Err() error { return gm.err }

// RoundTrip handles the execution of this command using the provided wiremessage.ReadWriter.
func (gm *GetMore) RoundTrip(ctx context.Context, desc description.SelectedServer, rw wiremessage.ReadWriter) (bson.Reader, error) {
	wm, err := gm.Encode(desc)
	if err != nil {
		return nil, err
	}

	err = rw.WriteWireMessage(ctx, wm)
	if err != nil {
		return nil, err
	}
	wm, err = rw.ReadWireMessage(ctx)
	if err != nil {
		return nil, err
	}
	return gm.Decode(desc, wm).Result()
}
