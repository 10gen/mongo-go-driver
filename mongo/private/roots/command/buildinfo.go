package command

import (
	"context"
	"errors"
	"fmt"

	"github.com/mongodb/mongo-go-driver/bson"
	"github.com/mongodb/mongo-go-driver/mongo/private/roots/result"
	"github.com/mongodb/mongo-go-driver/mongo/private/roots/wiremessage"
)

// BuildInfo represents the buildInfo command.
//
// The buildInfo command is used for getting the build information for a
// MongoDB server.
//
// Since BuildInfo can only be run on a connection, there is no Dispatch method.
type BuildInfo struct {
	err error
	res result.BuildInfo
}

// Encode will encode this command into a wire message for the given server description.
func (bi *BuildInfo) Encode() (wiremessage.WireMessage, error) {
	// This can probably just be a global variable that we reuse.
	cmd := bson.NewDocument(bson.EC.Int32("buildInfo", 1))
	rdr, err := cmd.MarshalBSON()
	if err != nil {
		return nil, err
	}
	query := wiremessage.Query{
		MsgHeader:          wiremessage.Header{RequestID: wiremessage.NextRequestID()},
		FullCollectionName: "admin.$cmd",
		Flags:              wiremessage.SlaveOK,
		NumberToReturn:     -1,
		Query:              rdr,
	}
	return query, nil
}

// Decode will decode the wire message using the provided server description. Errors during decoding
// are deferred until either the Result or Err methods are called.
func (bi *BuildInfo) Decode(wm wiremessage.WireMessage) *BuildInfo {
	reply, ok := wm.(wiremessage.Reply)
	if !ok {
		bi.err = errors.New(fmt.Sprintf("unsupported response wiremessage type %T", wm))
		return bi
	}
	rdr, err := decodeCommandOpReply(reply)
	if err != nil {
		bi.err = err
		return bi
	}
	err = bson.Unmarshal(rdr, &bi.res)
	if err != nil {
		bi.err = err
		return bi
	}
	return bi
}

// Result returns the result of a decoded wire message and server description.
func (bi *BuildInfo) Result() (result.BuildInfo, error) {
	if bi.err != nil {
		return result.BuildInfo{}, bi.err
	}

	return bi.res, nil
}

// Err returns the error set on this command.
func (bi *BuildInfo) Err() error { return bi.err }

// RoundTrip handles the execution of this command using the provided wiremessage.ReadWriter.
func (bi *BuildInfo) RoundTrip(ctx context.Context, rw wiremessage.ReadWriter) (result.BuildInfo, error) {
	wm, err := bi.Encode()
	if err != nil {
		return result.BuildInfo{}, err
	}

	err = rw.WriteWireMessage(ctx, wm)
	if err != nil {
		return result.BuildInfo{}, err
	}
	wm, err = rw.ReadWireMessage(ctx)
	if err != nil {
		return result.BuildInfo{}, err
	}
	return bi.Decode(wm).Result()
}
