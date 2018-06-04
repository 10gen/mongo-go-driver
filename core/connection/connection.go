// Copyright (C) MongoDB, Inc. 2017-present.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

// Package connection contains the types for building and pooling connections that can speak the
// MongoDB Wire Protocol. Since this low level library is meant to be used in the context of either
// a driver or a server there are some extra identifiers on a connection so one can keep track of
// what a connection is. This package purposefully hides the underlying network and abstracts the
// writing to and reading from a connection to wireops.Op's. This package also provides types for
// listening for and accepting Connections, as well as some types for handling connections and
// proxying connections to another server.
package connection

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync/atomic"
	"time"

	"github.com/mongodb/mongo-go-driver/core/address"
	"github.com/mongodb/mongo-go-driver/core/compressor"
	"github.com/mongodb/mongo-go-driver/core/description"
	"github.com/mongodb/mongo-go-driver/core/wiremessage"
)

var globalClientConnectionID uint64

func nextClientConnectionID() uint64 {
	return atomic.AddUint64(&globalClientConnectionID, 1)
}

// Connection is used to read and write wire protocol messages to a network.
type Connection interface {
	WriteWireMessage(context.Context, wiremessage.WireMessage) error
	ReadWireMessage(context.Context) (wiremessage.WireMessage, error)
	Close() error
	Expired() bool
	Alive() bool
	ID() string
}

// Dialer is used to make network connections.
type Dialer interface {
	DialContext(ctx context.Context, network, address string) (net.Conn, error)
}

// DialerFunc is a type implemented by functions that can be used as a Dialer.
type DialerFunc func(ctx context.Context, network, address string) (net.Conn, error)

// DialContext implements the Dialer interface.
func (df DialerFunc) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	return df(ctx, network, address)
}

// DefaultDialer is the Dialer implementation that is used by this package. Changing this
// will also change the Dialer used for this package. This should only be changed why all
// of the connections being made need to use a different Dialer. Most of the time, using a
// WithDialer option is more appropriate than changing this variable.
var DefaultDialer Dialer = &net.Dialer{}

// Handshaker is the interface implemented by types that can perform a MongoDB
// handshake over a provided ReadWriter. This is used during connection
// initialization.
type Handshaker interface {
	Handshake(context.Context, address.Address, wiremessage.ReadWriter) (description.Server, error)
}

// HandshakerFunc is an adapter to allow the use of ordinary functions as
// connection handshakers.
type HandshakerFunc func(context.Context, address.Address, wiremessage.ReadWriter) (description.Server, error)

// Handshake implements the Handshaker interface.
func (hf HandshakerFunc) Handshake(ctx context.Context, addr address.Address, rw wiremessage.ReadWriter) (description.Server, error) {
	return hf(ctx, addr, rw)
}

type connection struct {
	addr        address.Address
	id          string
	conn        net.Conn
	compressBuf []byte                // buffer to compress messages
	compressor  compressor.Compressor // use for compressing messages
	// server can compress response with any compressor supported by driver
	compressorMap    map[wiremessage.CompressorID]compressor.Compressor
	dead             bool
	idleTimeout      time.Duration
	idleDeadline     time.Time
	lifetimeDeadline time.Time
	readTimeout      time.Duration
	uncompressBuf    []byte // buffer to uncompress messages
	writeTimeout     time.Duration
	readBuf          []byte
	writeBuf         []byte
	wireMessageBuf   []byte // buffer to store uncompressed wire message before compressing
}

// New opens a connection to a given Addr
//
// The server description returned is nil if there was no handshaker provided.
func New(ctx context.Context, addr address.Address, opts ...Option) (Connection, *description.Server, error) {
	cfg, err := newConfig(opts...)
	if err != nil {
		return nil, nil, err
	}

	nc, err := cfg.dialer.DialContext(ctx, addr.Network(), addr.String())
	if err != nil {
		return nil, nil, err
	}

	if cfg.tlsConfig != nil {
		tlsConfig := cfg.tlsConfig.Clone()
		nc, err = configureTLS(ctx, nc, addr, tlsConfig)
		if err != nil {
			return nil, nil, err
		}
	}

	var lifetimeDeadline time.Time
	if cfg.lifeTimeout > 0 {
		lifetimeDeadline = time.Now().Add(cfg.lifeTimeout)
	}

	id := fmt.Sprintf("%s[-%d]", addr, nextClientConnectionID())
	compressorMap := make(map[wiremessage.CompressorID]compressor.Compressor)

	for _, comp := range cfg.compressors {
		compressorMap[comp.CompressorID()] = comp
	}

	c := &connection{
		id:               id,
		conn:             nc,
		compressBuf:      make([]byte, 256),
		compressorMap:    compressorMap,
		addr:             addr,
		idleTimeout:      cfg.idleTimeout,
		lifetimeDeadline: lifetimeDeadline,
		readTimeout:      cfg.readTimeout,
		writeTimeout:     cfg.writeTimeout,
		readBuf:          make([]byte, 256),
		uncompressBuf:    make([]byte, 256),
		writeBuf:         make([]byte, 0, 256),
		wireMessageBuf:   make([]byte, 256),
	}

	c.bumpIdleDeadline()

	var desc *description.Server
	if cfg.handshaker != nil {
		d, err := cfg.handshaker.Handshake(ctx, c.addr, c)
		if err != nil {
			return nil, nil, err
		}

		if len(d.Compression) > 0 {
		clientMethodLoop:
			for _, compressor := range cfg.compressors {
				method := compressor.Name()

				for _, serverMethod := range d.Compression {
					if method != serverMethod {
						continue
					}

					c.compressor = compressor // found matching compressor
					break clientMethodLoop
				}
			}

		}

		desc = &d
	}

	return c, desc, nil
}

func configureTLS(ctx context.Context, nc net.Conn, addr address.Address, config *TLSConfig) (net.Conn, error) {
	if !config.InsecureSkipVerify {
		hostname := addr.String()
		colonPos := strings.LastIndex(hostname, ":")
		if colonPos == -1 {
			colonPos = len(hostname)
		}

		hostname = hostname[:colonPos]
		config.ServerName = hostname
	}

	client := tls.Client(nc, config.Config)

	errChan := make(chan error, 1)
	go func() {
		errChan <- client.Handshake()
	}()

	select {
	case err := <-errChan:
		if err != nil {
			return nil, err
		}
	case <-ctx.Done():
		return nil, errors.New("server connection cancelled/timeout during TLS handshake")
	}
	return client, nil
}

func (c *connection) Alive() bool {
	return !c.dead
}

func (c *connection) Expired() bool {
	now := time.Now()
	if !c.idleDeadline.IsZero() && now.After(c.idleDeadline) {
		return true
	}

	if !c.lifetimeDeadline.IsZero() && now.After(c.lifetimeDeadline) {
		return true
	}

	return c.dead
}

func canCompress(cmd string) bool {
	if cmd == "isMaster" || cmd == "saslStart" || cmd == "saslContinue" || cmd == "getnonce" || cmd == "authenticate" ||
		cmd == "createUser" || cmd == "updateUser" || cmd == "copydbSaslStart" || cmd == "copydbgetnonce" || cmd == "copydb" {
		return false
	}
	return true
}

func (c *connection) compressMessage(wm wiremessage.WireMessage) (wiremessage.WireMessage, error) {
	var requestID int32
	var responseTo int32
	var origOpcode wiremessage.OpCode

	switch wm.(type) {
	case wiremessage.Query:
		queryMsg := wm.(wiremessage.Query)
		firstElem, err := queryMsg.Query.ElementAt(0)

		if err != nil {
			return wiremessage.Compressed{}, err
		}

		key := firstElem.Key()
		if !canCompress(key) {
			return wm, nil // return original message because this command can't be compressed
		}
		requestID = queryMsg.MsgHeader.RequestID
		origOpcode = wiremessage.OpQuery
		responseTo = queryMsg.MsgHeader.ResponseTo
	}

	// can compress
	c.wireMessageBuf = c.wireMessageBuf[:0] // truncate
	var err error
	c.wireMessageBuf, err = wm.AppendWireMessage(c.wireMessageBuf)
	if err != nil {
		return wiremessage.Compressed{}, err
	}

	c.wireMessageBuf = c.wireMessageBuf[16:] // strip header
	c.compressBuf = c.compressBuf[:0]
	compressedBytes, err := c.compressor.CompressBytes(c.wireMessageBuf, c.compressBuf)
	if err != nil {
		return wiremessage.Compressed{}, err
	}

	compressedMessage := wiremessage.Compressed{
		MsgHeader: wiremessage.Header{
			// MessageLength and OpCode will be set when marshalling wire message by SetDefaults()
			RequestID:  requestID,
			ResponseTo: responseTo,
		},
		OriginalOpCode:    origOpcode,
		UncompressedSize:  int32(len(c.wireMessageBuf)), // length of uncompressed message excluding MsgHeader
		CompressorID:      wiremessage.CompressorID(c.compressor.CompressorID()),
		CompressedMessage: compressedBytes,
	}

	return compressedMessage, nil
}

// returns []byte of uncompressed message with reconstructed header, original opcode, error
func (c *connection) uncompressMessage(compressed wiremessage.Compressed) ([]byte, wiremessage.OpCode, error) {
	// server doesn't guarantee the same compression method will be used each time so the CompressorID field must be
	// used to find the correct method for uncompressing data
	uncompressor := c.compressorMap[compressed.CompressorID]

	// reset uncompressBuf
	c.uncompressBuf = c.uncompressBuf[:0]
	if int(compressed.UncompressedSize) > cap(c.uncompressBuf) {
		c.uncompressBuf = make([]byte, 0, compressed.UncompressedSize)
	}

	uncompressedMessage, err := uncompressor.UncompressBytes(compressed.CompressedMessage, c.uncompressBuf)

	if err != nil {
		return nil, 0, err
	}

	switch compressed.OriginalOpCode {
	case wiremessage.OpReply:
		var fullMessage []byte

		// reconstruct original header
		origHeader := wiremessage.Header{
			MessageLength: int32(len(uncompressedMessage)) + 16, // add 16 for original header
			RequestID:     compressed.MsgHeader.RequestID,
			ResponseTo:    compressed.MsgHeader.ResponseTo,
			OpCode:        wiremessage.OpReply,
		}

		fullMessage = origHeader.AppendHeader(fullMessage)
		fullMessage = append(fullMessage, uncompressedMessage...)
		return fullMessage, origHeader.OpCode, nil

	default:
		return nil, 0, fmt.Errorf("opcode %s not implemented", compressed.OriginalOpCode)
	}
}

func (c *connection) WriteWireMessage(ctx context.Context, wm wiremessage.WireMessage) error {
	var err error
	if c.dead {
		return Error{
			ConnectionID: c.id,
			message:      "connection is dead",
		}
	}

	select {
	case <-ctx.Done():
		return Error{
			ConnectionID: c.id,
			Wrapped:      ctx.Err(),
			message:      "failed to write",
		}
	default:
	}

	deadline := time.Time{}
	if c.writeTimeout != 0 {
		deadline = time.Now().Add(c.writeTimeout)
	}

	if dl, ok := ctx.Deadline(); ok && (deadline.IsZero() || dl.Before(deadline)) {
		deadline = dl
	}

	if err := c.conn.SetWriteDeadline(deadline); err != nil {
		return Error{
			ConnectionID: c.id,
			Wrapped:      err,
			message:      "failed to set write deadline",
		}
	}

	// Truncate the write buffer
	c.writeBuf = c.writeBuf[:0]

	messageToWrite := wm
	// Compress if possible
	if c.compressor != nil {
		compressed, err := c.compressMessage(wm)
		if err != nil {
			return Error{
				ConnectionID: c.id,
				Wrapped:      err,
				message:      "unable to compress wire message",
			}
		}
		messageToWrite = compressed
	}

	c.writeBuf, err = messageToWrite.AppendWireMessage(c.writeBuf)
	if err != nil {
		return Error{
			ConnectionID: c.id,
			Wrapped:      err,
			message:      "unable to encode wire message",
		}
	}

	_, err = c.conn.Write(c.writeBuf)
	if err != nil {
		c.Close()
		return Error{
			ConnectionID: c.id,
			Wrapped:      err,
			message:      "unable to write wire message to network",
		}
	}

	c.bumpIdleDeadline()
	return nil
}

func (c *connection) ReadWireMessage(ctx context.Context) (wiremessage.WireMessage, error) {
	if c.dead {
		return nil, Error{
			ConnectionID: c.id,
			message:      "connection is dead",
		}
	}

	select {
	case <-ctx.Done():
		// We close the connection because we don't know if there
		// is an unread message on the wire.
		c.Close()
		return nil, Error{
			ConnectionID: c.id,
			Wrapped:      ctx.Err(),
			message:      "failed to read",
		}
	default:
	}

	deadline := time.Time{}
	if c.readTimeout != 0 {
		deadline = time.Now().Add(c.readTimeout)
	}

	if ctxDL, ok := ctx.Deadline(); ok && (deadline.IsZero() || ctxDL.Before(deadline)) {
		deadline = ctxDL
	}

	if err := c.conn.SetReadDeadline(deadline); err != nil {
		return nil, Error{
			ConnectionID: c.id,
			Wrapped:      ctx.Err(),
			message:      "failed to set read deadline",
		}
	}

	var sizeBuf [4]byte
	_, err := io.ReadFull(c.conn, sizeBuf[:])
	if err != nil {
		c.Close()
		return nil, Error{
			ConnectionID: c.id,
			Wrapped:      err,
			message:      "unable to decode message length",
		}
	}

	size := readInt32(sizeBuf[:], 0)

	// Isn't the best reuse, but resizing a []byte to be larger
	// is difficult.
	if len(c.readBuf) > int(size) {
		c.readBuf = c.readBuf[:size]
	} else {
		c.readBuf = make([]byte, size)
	}

	c.readBuf[0], c.readBuf[1], c.readBuf[2], c.readBuf[3] = sizeBuf[0], sizeBuf[1], sizeBuf[2], sizeBuf[3]

	_, err = io.ReadFull(c.conn, c.readBuf[4:])
	if err != nil {
		c.Close()
		return nil, Error{
			ConnectionID: c.id,
			Wrapped:      err,
			message:      "unable to read full message",
		}
	}

	hdr, err := wiremessage.ReadHeader(c.readBuf, 0)
	if err != nil {
		c.Close()
		return nil, Error{
			ConnectionID: c.id,
			Wrapped:      err,
			message:      "unable to decode header",
		}
	}

	messageToDecode := c.readBuf
	opcodeToCheck := hdr.OpCode

	if hdr.OpCode == wiremessage.OpCompressed {
		var compressed wiremessage.Compressed
		err := compressed.UnmarshalWireMessage(c.readBuf)
		if err != nil {
			defer c.Close()
			return nil, Error{
				ConnectionID: c.id,
				Wrapped:      err,
				message:      "unable to decode OP_COMPRESSED",
			}
		}

		uncompressed, origOpcode, err := c.uncompressMessage(compressed)
		if err != nil {
			defer c.Close()
			return nil, Error{
				ConnectionID: c.id,
				Wrapped:      err,
				message:      "unable to uncompress message",
			}
		}
		messageToDecode = uncompressed
		opcodeToCheck = origOpcode
	}

	var wm wiremessage.WireMessage
	switch opcodeToCheck {
	case wiremessage.OpReply:
		var r wiremessage.Reply
		err := r.UnmarshalWireMessage(messageToDecode)
		if err != nil {
			c.Close()
			return nil, Error{
				ConnectionID: c.id,
				Wrapped:      err,
				message:      "unable to decode OP_REPLY",
			}
		}
		wm = r
	default:
		c.Close()
		return nil, Error{
			ConnectionID: c.id,
			message:      fmt.Sprintf("opcode %s not implemented", hdr.OpCode),
		}
	}

	c.bumpIdleDeadline()
	return wm, nil
}

func (c *connection) bumpIdleDeadline() {
	if c.idleTimeout > 0 {
		c.idleDeadline = time.Now().Add(c.idleTimeout)
	}
}

func (c *connection) Close() error {
	c.dead = true
	err := c.conn.Close()
	if err != nil {
		return Error{
			ConnectionID: c.id,
			Wrapped:      err,
			message:      "failed to close net.Conn",
		}
	}

	return nil
}

func (c *connection) ID() string {
	return c.id
}

func (c *connection) initialize(ctx context.Context, appName string) error {
	return nil
}

func readInt32(b []byte, pos int32) int32 {
	return (int32(b[pos+0])) | (int32(b[pos+1]) << 8) | (int32(b[pos+2]) << 16) | (int32(b[pos+3]) << 24)
}
