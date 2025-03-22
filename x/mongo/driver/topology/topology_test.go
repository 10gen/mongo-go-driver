// Copyright (C) MongoDB, Inc. 2017-present.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

package topology

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"path"
	"sync/atomic"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/internal/assert"
	"go.mongodb.org/mongo-driver/v2/internal/require"
	"go.mongodb.org/mongo-driver/v2/internal/serverselector"
	"go.mongodb.org/mongo-driver/v2/internal/spectest"
	"go.mongodb.org/mongo-driver/v2/mongo/address"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"
	"go.mongodb.org/mongo-driver/v2/x/mongo/driver/description"
)

const testTimeout = 2 * time.Second

func compareErrors(err1, err2 error) bool {
	if err1 == nil && err2 == nil {
		return true
	}

	if err1 == nil || err2 == nil {
		return false
	}

	if err1.Error() != err2.Error() {
		return false
	}

	return true
}

func TestServerSelection(t *testing.T) {
	var selectFirst serverselector.Func = func(_ description.Topology, candidates []description.Server) ([]description.Server, error) {
		if len(candidates) == 0 {
			return []description.Server{}, nil
		}
		return candidates[0:1], nil
	}
	var selectNone serverselector.Func = func(description.Topology, []description.Server) ([]description.Server, error) {
		return []description.Server{}, nil
	}

	t.Run("Success", func(t *testing.T) {
		topo, err := New(nil)
		require.NoError(t, err)
		desc := description.Topology{
			Servers: []description.Server{
				{Addr: address.Address("one"), Kind: description.ServerKindStandalone},
				{Addr: address.Address("two"), Kind: description.ServerKindStandalone},
				{Addr: address.Address("three"), Kind: description.ServerKindStandalone},
			},
		}
		subCh := make(chan description.Topology, 1)
		subCh <- desc

		srvs, err := topo.selectServerFromSubscription(context.Background(), subCh, selectFirst)
		require.NoError(t, err)
		if len(srvs) != 1 {
			t.Errorf("Incorrect number of descriptions returned. got %d; want %d", len(srvs), 1)
		}
		if srvs[0].Addr != desc.Servers[0].Addr {
			t.Errorf("Incorrect sever selected. got %s; want %s", srvs[0].Addr, desc.Servers[0].Addr)
		}
	})
	t.Run("Compatibility Error Min Version Too High", func(t *testing.T) {
		topo, err := New(nil)
		require.NoError(t, err)
		desc := description.Topology{
			Kind: description.TopologyKindSingle,
			Servers: []description.Server{
				{Addr: address.Address("one:27017"), Kind: description.ServerKindStandalone, WireVersion: &description.VersionRange{Max: 11, Min: 11}},
				{Addr: address.Address("two:27017"), Kind: description.ServerKindStandalone, WireVersion: &description.VersionRange{Max: 9, Min: 6}},
				{Addr: address.Address("three:27017"), Kind: description.ServerKindStandalone, WireVersion: &description.VersionRange{Max: 9, Min: 6}},
			},
		}
		want := fmt.Errorf(
			"server at %s requires wire version %d, but this version of the Go driver only supports up to %d",
			desc.Servers[0].Addr.String(),
			desc.Servers[0].WireVersion.Min,
			SupportedWireVersions.Max,
		)
		desc.CompatibilityErr = want
		atomic.StoreInt64(&topo.state, topologyConnected)
		topo.desc.Store(desc)
		_, err = topo.SelectServer(context.Background(), selectFirst)
		assert.Equal(t, err, want, "expected %v, got %v", want, err)
	})
	t.Run("Compatibility Error Max Version Too Low", func(t *testing.T) {
		topo, err := New(nil)
		require.NoError(t, err)
		desc := description.Topology{
			Kind: description.TopologyKindSingle,
			Servers: []description.Server{
				{Addr: address.Address("one:27017"), Kind: description.ServerKindStandalone, WireVersion: &description.VersionRange{Max: 21, Min: 6}},
				{Addr: address.Address("two:27017"), Kind: description.ServerKindStandalone, WireVersion: &description.VersionRange{Max: 9, Min: 2}},
				{Addr: address.Address("three:27017"), Kind: description.ServerKindStandalone, WireVersion: &description.VersionRange{Max: 9, Min: 2}},
			},
		}
		want := fmt.Errorf(
			"server at %s reports wire version %d, but this version of the Go driver requires "+
				"at least 7 (MongoDB 4.0)",
			desc.Servers[0].Addr.String(),
			desc.Servers[0].WireVersion.Max,
		)
		desc.CompatibilityErr = want
		atomic.StoreInt64(&topo.state, topologyConnected)
		topo.desc.Store(desc)
		_, err = topo.SelectServer(context.Background(), selectFirst)
		assert.Equal(t, err, want, "expected %v, got %v", want, err)
	})
	t.Run("Updated", func(t *testing.T) {
		topo, err := New(nil)
		require.NoError(t, err)
		desc := description.Topology{Servers: []description.Server{}}
		subCh := make(chan description.Topology, 1)
		subCh <- desc

		resp := make(chan []description.Server)
		go func() {
			srvs, err := topo.selectServerFromSubscription(context.Background(), subCh, selectFirst)
			require.NoError(t, err)
			resp <- srvs
		}()

		desc = description.Topology{
			Servers: []description.Server{
				{Addr: address.Address("one"), Kind: description.ServerKindStandalone},
				{Addr: address.Address("two"), Kind: description.ServerKindStandalone},
				{Addr: address.Address("three"), Kind: description.ServerKindStandalone},
			},
		}
		select {
		case subCh <- desc:
		case <-time.After(100 * time.Millisecond):
			t.Error("Timed out while trying to send topology description")
		}

		var srvs []description.Server
		select {
		case srvs = <-resp:
		case <-time.After(100 * time.Millisecond):
			t.Errorf("Timed out while trying to retrieve selected servers")
		}

		if len(srvs) != 1 {
			t.Errorf("Incorrect number of descriptions returned. got %d; want %d", len(srvs), 1)
		}
		if srvs[0].Addr != desc.Servers[0].Addr {
			t.Errorf("Incorrect sever selected. got %s; want %s", srvs[0].Addr, desc.Servers[0].Addr)
		}
	})
	t.Run("Cancel", func(t *testing.T) {
		desc := description.Topology{
			Servers: []description.Server{
				{Addr: address.Address("one"), Kind: description.ServerKindStandalone},
				{Addr: address.Address("two"), Kind: description.ServerKindStandalone},
				{Addr: address.Address("three"), Kind: description.ServerKindStandalone},
			},
		}
		topo, err := New(nil)
		require.NoError(t, err)
		subCh := make(chan description.Topology, 1)
		subCh <- desc
		resp := make(chan error)
		ctx, cancel := context.WithCancel(context.Background())
		go func() {
			_, err := topo.selectServerFromSubscription(ctx, subCh, selectNone)
			resp <- err
		}()

		select {
		case err := <-resp:
			t.Errorf("Received error from server selection too soon: %v", err)
		case <-time.After(100 * time.Millisecond):
		}

		cancel()

		select {
		case err = <-resp:
		case <-time.After(100 * time.Millisecond):
			t.Errorf("Timed out while trying to retrieve selected servers")
		}

		want := ServerSelectionError{Wrapped: context.Canceled, Desc: desc}
		assert.Equal(t, err, want, "Incorrect error received. got %v; want %v", err, want)
	})
	t.Run("findServer returns topology kind", func(t *testing.T) {
		topo, err := New(nil)
		require.NoError(t, err)
		atomic.StoreInt64(&topo.state, topologyConnected)
		srvr, err := ConnectServer(address.Address("one"), topo.updateCallback, topo.id, defaultConnectionTimeout)
		require.NoError(t, err)
		topo.servers[address.Address("one")] = srvr
		desc := topo.desc.Load().(description.Topology)
		desc.Kind = description.TopologyKindSingle
		topo.desc.Store(desc)

		selected := description.Server{Addr: address.Address("one")}

		ss, err := topo.FindServer(selected)
		require.NoError(t, err)
		if ss.Kind != description.TopologyKindSingle {
			t.Errorf("findServer does not properly set the topology description kind. got %v; want %v", ss.Kind, description.TopologyKindSingle)
		}
	})
	t.Run("fast path does not subscribe or check timeouts", func(t *testing.T) {
		// Assert that the server selection fast path does not create a Subscription or check for timeout errors.
		topo, err := New(nil)
		require.NoError(t, err)
		atomic.StoreInt64(&topo.state, topologyConnected)

		primaryAddr := address.Address("one")
		desc := description.Topology{
			Servers: []description.Server{
				{Addr: primaryAddr, Kind: description.ServerKindRSPrimary},
			},
		}
		topo.desc.Store(desc)
		for _, srv := range desc.Servers {
			s, err := ConnectServer(srv.Addr, topo.updateCallback, topo.id, defaultConnectionTimeout)
			require.NoError(t, err)
			topo.servers[srv.Addr] = s
		}

		// Manually close subscriptions so calls to Subscribe will error and pass in a cancelled context to ensure the
		// fast path ignores timeout errors.
		topo.subscriptionsClosed = true
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		selectedServer, err := topo.SelectServer(ctx, &serverselector.Write{})
		require.NoError(t, err)
		selectedAddr := selectedServer.(*SelectedServer).address
		assert.Equal(t, primaryAddr, selectedAddr, "expected address %v, got %v", primaryAddr, selectedAddr)
	})
	t.Run("default to selecting from subscription if fast path fails", func(t *testing.T) {
		topo, err := New(nil)
		require.NoError(t, err)

		atomic.StoreInt64(&topo.state, topologyConnected)
		desc := description.Topology{
			Servers: []description.Server{},
		}
		topo.desc.Store(desc)

		topo.subscriptionsClosed = true
		_, err = topo.SelectServer(context.Background(), &serverselector.Write{})
		assert.Equal(t, ErrSubscribeAfterClosed, err, "expected error %v, got %v", ErrSubscribeAfterClosed, err)
	})
}

func TestSessionTimeout(t *testing.T) {
	int64ToPtr := func(i64 int64) *int64 { return &i64 }

	t.Run("UpdateSessionTimeout", func(t *testing.T) {
		topo, err := New(nil)
		require.NoError(t, err)
		topo.servers["foo"] = nil
		topo.fsm.Servers = []description.Server{
			{
				Addr:                  address.Address("foo").Canonicalize(),
				Kind:                  description.ServerKindRSPrimary,
				SessionTimeoutMinutes: int64ToPtr(60),
			},
		}

		ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
		defer cancel()

		desc := description.Server{
			Addr:                  "foo",
			Kind:                  description.ServerKindRSPrimary,
			SessionTimeoutMinutes: int64ToPtr(30),
		}
		topo.apply(ctx, desc)

		currDesc := topo.desc.Load().(description.Topology)
		want := int64(30)
		require.Equal(t, &want, currDesc.SessionTimeoutMinutes,
			"session timeout minutes mismatch")
	})
	t.Run("MultipleUpdates", func(t *testing.T) {
		topo, err := New(nil)
		require.NoError(t, err)
		topo.fsm.Kind = description.TopologyKindReplicaSetWithPrimary
		topo.servers["foo"] = nil
		topo.servers["bar"] = nil
		topo.fsm.Servers = []description.Server{
			{
				Addr:                  address.Address("foo").Canonicalize(),
				Kind:                  description.ServerKindRSPrimary,
				SessionTimeoutMinutes: int64ToPtr(60),
			},
			{
				Addr:                  address.Address("bar").Canonicalize(),
				Kind:                  description.ServerKindRSSecondary,
				SessionTimeoutMinutes: int64ToPtr(60),
			},
		}

		ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
		defer cancel()

		desc1 := description.Server{
			Addr:                  "foo",
			Kind:                  description.ServerKindRSPrimary,
			SessionTimeoutMinutes: int64ToPtr(30),
			Members:               []address.Address{address.Address("foo").Canonicalize(), address.Address("bar").Canonicalize()},
		}
		// should update because new timeout is lower
		desc2 := description.Server{
			Addr:                  "bar",
			Kind:                  description.ServerKindRSPrimary,
			SessionTimeoutMinutes: int64ToPtr(20),
			Members:               []address.Address{address.Address("foo").Canonicalize(), address.Address("bar").Canonicalize()},
		}
		topo.apply(ctx, desc1)
		topo.apply(ctx, desc2)

		currDesc := topo.Description()
		want := int64(20)
		require.Equal(t, &want, currDesc.SessionTimeoutMinutes,
			"session timeout minutes mismatch")
	})
	t.Run("NoUpdate", func(t *testing.T) {
		topo, err := New(nil)
		require.NoError(t, err)
		topo.servers["foo"] = nil
		topo.servers["bar"] = nil
		topo.fsm.Servers = []description.Server{
			{
				Addr:                  address.Address("foo").Canonicalize(),
				Kind:                  description.ServerKindRSPrimary,
				SessionTimeoutMinutes: int64ToPtr(60),
			},
			{
				Addr:                  address.Address("bar").Canonicalize(),
				Kind:                  description.ServerKindRSSecondary,
				SessionTimeoutMinutes: int64ToPtr(60),
			},
		}

		ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
		defer cancel()

		desc1 := description.Server{
			Addr:                  "foo",
			Kind:                  description.ServerKindRSPrimary,
			SessionTimeoutMinutes: int64ToPtr(20),
			Members:               []address.Address{address.Address("foo").Canonicalize(), address.Address("bar").Canonicalize()},
		}
		// should not update because new timeout is higher
		desc2 := description.Server{
			Addr:                  "bar",
			Kind:                  description.ServerKindRSPrimary,
			SessionTimeoutMinutes: int64ToPtr(30),
			Members:               []address.Address{address.Address("foo").Canonicalize(), address.Address("bar").Canonicalize()},
		}
		topo.apply(ctx, desc1)
		topo.apply(ctx, desc2)

		currDesc := topo.desc.Load().(description.Topology)
		want := int64(20)
		require.Equal(t, &want, currDesc.SessionTimeoutMinutes,
			"session timeout minutes mismatch")
	})
	t.Run("TimeoutDataBearing", func(t *testing.T) {
		topo, err := New(nil)
		require.NoError(t, err)
		topo.servers["foo"] = nil
		topo.servers["bar"] = nil
		topo.fsm.Servers = []description.Server{
			{
				Addr:                  address.Address("foo").Canonicalize(),
				Kind:                  description.ServerKindRSPrimary,
				SessionTimeoutMinutes: int64ToPtr(60),
			},
			{
				Addr:                  address.Address("bar").Canonicalize(),
				Kind:                  description.ServerKindRSSecondary,
				SessionTimeoutMinutes: int64ToPtr(60),
			},
		}

		ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
		defer cancel()

		desc1 := description.Server{
			Addr:                  "foo",
			Kind:                  description.ServerKindRSPrimary,
			SessionTimeoutMinutes: int64ToPtr(20),
			Members:               []address.Address{address.Address("foo").Canonicalize(), address.Address("bar").Canonicalize()},
		}
		// should not update because not a data bearing server
		desc2 := description.Server{
			Addr:                  "bar",
			Kind:                  description.Unknown,
			SessionTimeoutMinutes: int64ToPtr(10),
			Members:               []address.Address{address.Address("foo").Canonicalize(), address.Address("bar").Canonicalize()},
		}
		topo.apply(ctx, desc1)
		topo.apply(ctx, desc2)

		currDesc := topo.desc.Load().(description.Topology)
		want := int64(20)
		assert.Equal(t, &want, currDesc.SessionTimeoutMinutes,
			"session timeout minutes mismatch")
	})
	t.Run("MixedSessionSupport", func(t *testing.T) {
		topo, err := New(nil)
		require.NoError(t, err)
		topo.fsm.Kind = description.TopologyKindReplicaSetWithPrimary
		topo.servers["one"] = nil
		topo.servers["two"] = nil
		topo.servers["three"] = nil
		topo.fsm.Servers = []description.Server{
			{
				Addr:                  address.Address("one").Canonicalize(),
				Kind:                  description.ServerKindRSPrimary,
				SessionTimeoutMinutes: int64ToPtr(20),
			},
			{
				// does not support sessions
				Addr: address.Address("two").Canonicalize(),
				Kind: description.ServerKindRSSecondary,
			},
			{
				Addr:                  address.Address("three").Canonicalize(),
				Kind:                  description.ServerKindRSPrimary,
				SessionTimeoutMinutes: int64ToPtr(60),
			},
		}

		ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
		defer cancel()

		desc := description.Server{
			Addr:                  address.Address("three"),
			Kind:                  description.ServerKindRSSecondary,
			SessionTimeoutMinutes: int64ToPtr(30),
		}

		topo.apply(ctx, desc)

		currDesc := topo.desc.Load().(description.Topology)
		require.Nil(t, currDesc.SessionTimeoutMinutes,
			"session timeout minutes mismatch. expected: nil")
	})
}

func TestMinPoolSize(t *testing.T) {
	cfg, err := NewConfig(options.Client().SetHosts([]string{"localhost:27017"}).SetMinPoolSize(10), nil)
	if err != nil {
		t.Errorf("error constructing topology config: %v", err)
	}

	topo, err := New(cfg)
	if err != nil {
		t.Errorf("topology.New shouldn't error. got: %v", err)
	}
	err = topo.Connect()
	if err != nil {
		t.Errorf("topology.Connect shouldn't error. got: %v", err)
	}
}

func TestTopology_String_Race(_ *testing.T) {
	ch := make(chan bool)
	topo := &Topology{
		servers: make(map[address.Address]*Server),
	}

	go func() {
		topo.serversLock.Lock()
		srv := &Server{}
		srv.desc.Store(description.Server{})
		topo.servers[address.Address("127.0.0.1:27017")] = srv
		topo.serversLock.Unlock()
		ch <- true
	}()

	go func() {
		_ = topo.String()
		ch <- true
	}()

	<-ch
	<-ch
}

func TestTopologyConstruction(t *testing.T) {
	t.Run("construct with URI", func(t *testing.T) {
		testCases := []struct {
			name            string
			uri             string
			pollingRequired bool
		}{
			{
				name:            "normal",
				uri:             "mongodb://localhost:27017",
				pollingRequired: false,
			},
		}
		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				cfg, err := NewConfig(options.Client().ApplyURI(tc.uri), nil)
				assert.Nil(t, err, "error constructing topology config: %v", err)

				topo, err := New(cfg)
				assert.Nil(t, err, "topology.New error: %v", err)

				assert.Equal(t, tc.uri, topo.cfg.URI, "expected topology URI to be %v, got %v", tc.uri, topo.cfg.URI)
				assert.Equal(t, tc.pollingRequired, topo.pollingRequired,
					"expected topo.pollingRequired to be %v, got %v", tc.pollingRequired, topo.pollingRequired)
			})
		}
	})
}

type mockLogSink struct {
	msgs []string
}

func (s *mockLogSink) Info(_ int, msg string, _ ...interface{}) {
	s.msgs = append(s.msgs, msg)
}
func (*mockLogSink) Error(error, string, ...interface{}) {
	// Do nothing.
}

// Note: SRV connection strings are intentionally untested, since initial
// lookup responses cannot be easily mocked.
func TestTopologyConstructionLogging(t *testing.T) {
	const (
		cosmosDBMsg   = `You appear to be connected to a CosmosDB cluster. For more information regarding feature compatibility and support please visit https://www.mongodb.com/supportability/cosmosdb`
		documentDBMsg = `You appear to be connected to a DocumentDB cluster. For more information regarding feature compatibility and support please visit https://www.mongodb.com/supportability/documentdb`
	)

	newLoggerOptionsBldr := func(sink options.LogSink) *options.LoggerOptions {
		return options.
			Logger().
			SetSink(sink).
			SetComponentLevel(options.LogComponentTopology, options.LogLevelInfo)
	}

	t.Run("CosmosDB URIs", func(t *testing.T) {
		t.Parallel()

		testCases := []struct {
			name string
			uri  string
			msgs []string
		}{
			{
				name: "normal",
				uri:  "mongodb://a.mongo.cosmos.azure.com:19555/",
				msgs: []string{cosmosDBMsg},
			},
			{
				name: "multiple hosts",
				uri:  "mongodb://a.mongo.cosmos.azure.com:1955,b.mongo.cosmos.azure.com:19555/",
				msgs: []string{cosmosDBMsg},
			},
			{
				name: "case-insensitive matching",
				uri:  "mongodb://a.MONGO.COSMOS.AZURE.COM:19555/",
				msgs: []string{},
			},
			{
				name: "Mixing genuine and nongenuine hosts (unlikely in practice)",
				uri:  "mongodb://a.example.com:27017,b.mongo.cosmos.azure.com:19555/",
				msgs: []string{cosmosDBMsg},
			},
		}
		for _, tc := range testCases {
			tc := tc

			t.Run(tc.name, func(t *testing.T) {
				t.Parallel()

				sink := &mockLogSink{}
				cfg, err := NewConfig(options.Client().ApplyURI(tc.uri).SetLoggerOptions(newLoggerOptionsBldr(sink)), nil)
				require.Nil(t, err, "error constructing topology config: %v", err)

				topo, err := New(cfg)
				require.Nil(t, err, "topology.New error: %v", err)

				err = topo.Connect()
				assert.Nil(t, err, "Connect error: %v", err)

				assert.ElementsMatch(t, tc.msgs, sink.msgs, "expected messages to be %v, got %v", tc.msgs, sink.msgs)
			})
		}
	})
	t.Run("DocumentDB URIs", func(t *testing.T) {
		t.Parallel()

		testCases := []struct {
			name string
			uri  string
			msgs []string
		}{
			{
				name: "normal",
				uri:  "mongodb://a.docdb.amazonaws.com:27017/",
				msgs: []string{documentDBMsg},
			},
			{
				name: "normal",
				uri:  "mongodb://a.docdb-elastic.amazonaws.com:27017/",
				msgs: []string{documentDBMsg},
			},
			{
				name: "multiple hosts",
				uri:  "mongodb://a.docdb.amazonaws.com:27017,a.docdb-elastic.amazonaws.com:27017/",
				msgs: []string{documentDBMsg},
			},
			{
				name: "case-insensitive matching",
				uri:  "mongodb://a.DOCDB.AMAZONAWS.COM:27017/",
				msgs: []string{},
			},
			{
				name: "case-insensitive matching",
				uri:  "mongodb://a.DOCDB-ELASTIC.AMAZONAWS.COM:27017/",
				msgs: []string{},
			},
			{
				name: "Mixing genuine and nongenuine hosts (unlikely in practice)",
				uri:  "mongodb://a.example.com:27017,b.docdb.amazonaws.com:27017/",
				msgs: []string{documentDBMsg},
			},
			{
				name: "Mixing genuine and nongenuine hosts (unlikely in practice)",
				uri:  "mongodb://a.example.com:27017,b.docdb-elastic.amazonaws.com:27017/",
				msgs: []string{documentDBMsg},
			},
		}
		for _, tc := range testCases {
			tc := tc

			t.Run(tc.name, func(t *testing.T) {
				t.Parallel()

				sink := &mockLogSink{}
				cfg, err := NewConfig(options.Client().ApplyURI(tc.uri).SetLoggerOptions(newLoggerOptionsBldr(sink)), nil)
				require.Nil(t, err, "error constructing topology config: %v", err)

				topo, err := New(cfg)
				require.Nil(t, err, "topology.New error: %v", err)

				err = topo.Connect()
				assert.Nil(t, err, "Connect error: %v", err)

				assert.ElementsMatch(t, tc.msgs, sink.msgs, "expected messages to be %v, got %v", tc.msgs, sink.msgs)
			})
		}
	})
	t.Run("Mixing CosmosDB and DocumentDB URIs", func(t *testing.T) {
		t.Parallel()

		testCases := []struct {
			name string
			uri  string
			msgs []string
		}{
			{
				name: "Mixing hosts",
				uri:  "mongodb://a.mongo.cosmos.azure.com:19555,a.docdb.amazonaws.com:27017/",
				msgs: []string{cosmosDBMsg, documentDBMsg},
			},
		}
		for _, tc := range testCases {
			tc := tc

			t.Run(tc.name, func(t *testing.T) {
				t.Parallel()

				sink := &mockLogSink{}
				cfg, err := NewConfig(options.Client().ApplyURI(tc.uri).SetLoggerOptions(newLoggerOptionsBldr(sink)), nil)
				require.Nil(t, err, "error constructing topology config: %v", err)

				topo, err := New(cfg)
				require.Nil(t, err, "topology.New error: %v", err)

				err = topo.Connect()
				assert.Nil(t, err, "Connect error: %v", err)

				assert.ElementsMatch(t, tc.msgs, sink.msgs, "expected messages to be %v, got %v", tc.msgs, sink.msgs)
			})
		}
	})
	t.Run("genuine URIs", func(t *testing.T) {
		t.Parallel()

		testCases := []struct {
			name string
			uri  string
			msgs []string
		}{
			{
				name: "normal",
				uri:  "mongodb://a.example.com:27017/",
				msgs: []string{},
			},
			{
				name: "socket",
				uri:  "mongodb://%2Ftmp%2Fmongodb-27017.sock/",
				msgs: []string{},
			},
			{
				name: "srv",
				uri:  "mongodb+srv://test22.test.build.10gen.cc/?srvServiceName=customname",
				msgs: []string{},
			},
			{
				name: "multiple hosts",
				uri:  "mongodb://a.example.com:27017,b.example.com:27017/",
				msgs: []string{},
			},
			{
				name: "unexpected suffix",
				uri:  "mongodb://a.mongo.cosmos.azure.com.tld:19555/",
				msgs: []string{},
			},
			{
				name: "unexpected suffix",
				uri:  "mongodb://a.docdb.amazonaws.com.tld:27017/",
				msgs: []string{},
			},
			{
				name: "unexpected suffix",
				uri:  "mongodb://a.docdb-elastic.amazonaws.com.tld:27017/",
				msgs: []string{},
			},
		}
		for _, tc := range testCases {
			tc := tc

			t.Run(tc.name, func(t *testing.T) {
				t.Parallel()

				sink := &mockLogSink{}
				cfg, err := NewConfig(options.Client().ApplyURI(tc.uri).SetLoggerOptions(newLoggerOptionsBldr(sink)), nil)
				require.Nil(t, err, "error constructing topology config: %v", err)

				topo, err := New(cfg)
				require.Nil(t, err, "topology.New error: %v", err)

				err = topo.Connect()
				assert.Nil(t, err, "Connect error: %v", err)

				assert.ElementsMatch(t, tc.msgs, sink.msgs, "expected messages to be %v, got %v", tc.msgs, sink.msgs)
			})
		}
	})
}

type inWindowServer struct {
	Address  string `json:"address"`
	Type     string `json:"type"`
	AvgRTTMS int64  `json:"avg_rtt_ms"`
}

type inWindowTopology struct {
	Type    string           `json:"type"`
	Servers []inWindowServer `json:"servers"`
}

type inWindowOutcome struct {
	Tolerance           float64            `json:"tolerance"`
	ExpectedFrequencies map[string]float64 `json:"expected_frequencies"`
}

type inWindowTopologyState struct {
	Address        string `json:"address"`
	OperationCount int64  `json:"operation_count"`
}

type inWindowTestCase struct {
	TopologyDescription inWindowTopology        `json:"topology_description"`
	MockedTopologyState []inWindowTopologyState `json:"mocked_topology_state"`
	Iterations          int                     `json:"iterations"`
	Outcome             inWindowOutcome         `json:"outcome"`
}

// TestServerSelectionSpecInWindow runs the "in_window" server selection spec tests. This test is
// in the "topology" package instead of the "description" package (where the rest of the server
// selection spec tests are) because it primarily tests load-based server selection. Load-based
// server selection is implemented in Topology.SelectServer() because it requires knowledge of the
// current "operation count" (the number of currently running operations) for each server, so it
// can't be effectively accomplished just with server descriptions like most other server selection
// algorithms.
func TestServerSelectionSpecInWindow(t *testing.T) {
	const testsDir = "../../../../testdata/server-selection/in_window"

	files := spectest.FindJSONFilesInDir(t, testsDir)

	for _, file := range files {
		t.Run(file, func(t *testing.T) {
			runInWindowTest(t, testsDir, file)
		})
	}
}

func runInWindowTest(t *testing.T, directory string, filename string) {
	filepath := path.Join(directory, filename)
	content, err := ioutil.ReadFile(filepath)
	require.NoError(t, err)

	var test inWindowTestCase
	require.NoError(t, json.Unmarshal(content, &test))

	// For each server described in the test's "topology_description", create both a *Server and
	// description.Server, which are both required to run Topology.SelectServer().
	servers := make(map[string]*Server, len(test.TopologyDescription.Servers))
	descriptions := make([]description.Server, 0, len(test.TopologyDescription.Servers))
	for _, testDesc := range test.TopologyDescription.Servers {
		server := NewServer(
			address.Address(testDesc.Address),
			bson.NilObjectID,
			defaultConnectionTimeout,
			withMonitoringDisabled(func(bool) bool { return true }))
		servers[testDesc.Address] = server

		desc := description.Server{
			Kind:          serverKindFromString(t, testDesc.Type),
			Addr:          address.Address(testDesc.Address),
			AverageRTT:    time.Duration(testDesc.AvgRTTMS) * time.Millisecond,
			AverageRTTSet: true,
		}

		if testDesc.AvgRTTMS > 0 {
			desc.AverageRTT = time.Duration(testDesc.AvgRTTMS) * time.Millisecond
			desc.AverageRTTSet = true
		}

		descriptions = append(descriptions, desc)
	}

	// For each server state in the test's "mocked_topology_state", set the connection pool's
	// in-use connections count to the test operation count value.
	for _, state := range test.MockedTopologyState {
		servers[state.Address].operationCount = state.OperationCount
	}

	// Create a new Topology, set the state to "connected", store a topology description
	// containing all server descriptions created from the test server descriptions, and copy
	// all *Server instances to the Topology's servers list.
	topology, err := New(nil)
	require.NoError(t, err, "error creating new Topology")
	topology.state = topologyConnected
	topology.desc.Store(description.Topology{
		Kind:    topologyKindFromString(t, test.TopologyDescription.Type),
		Servers: descriptions,
	})
	for addr, server := range servers {
		topology.servers[address.Address(addr)] = server
	}

	// Run server selection the required number of times and record how many times each server
	// address was selected.
	counts := make(map[string]int, len(test.TopologyDescription.Servers))
	for i := 0; i < test.Iterations; i++ {
		selected, err := topology.SelectServer(
			context.Background(),
			&serverselector.ReadPref{ReadPref: readpref.Nearest()})
		require.NoError(t, err, "error selecting server")
		counts[string(selected.(*SelectedServer).address)]++
	}

	// Convert the server selection counts to selection frequencies by dividing the counts by
	// the total number of server selection attempts.
	frequencies := make(map[string]float64, len(counts))
	for addr, count := range counts {
		frequencies[addr] = float64(count) / float64(test.Iterations)
	}

	// Assert that the observed server selection frequency for each server address matches the
	// expected server selection frequency.
	for addr, expected := range test.Outcome.ExpectedFrequencies {
		actual := frequencies[addr]

		// If the expected frequency for a given server is 1 or 0, then the observed frequency
		// MUST be exactly equal to the expected one.
		if expected == 1 || expected == 0 {
			assert.Equal(
				t,
				expected,
				actual,
				"expected frequency of %q to be equal to %f, but is %f",
				addr, expected, actual)
			continue
		}

		// Otherwise, check if the expected frequency is within the given tolerance range.
		// TODO(GODRIVER-2179): Use assert.Deltaf() when we migrate all test code to the "testify/assert" or an
		// TODO API-compatible library for assertions.
		low := expected - test.Outcome.Tolerance
		high := expected + test.Outcome.Tolerance
		assert.True(
			t,
			actual >= low && actual <= high,
			"expected frequency of %q to be in range [%f, %f], but is %f",
			addr, low, high, actual)
	}
}

func topologyKindFromString(t *testing.T, s string) description.TopologyKind {
	t.Helper()

	switch s {
	case "Single":
		return description.TopologyKindSingle
	case "ReplicaSet":
		return description.TopologyKindReplicaSet
	case "ReplicaSetNoPrimary":
		return description.TopologyKindReplicaSetNoPrimary
	case "ReplicaSetWithPrimary":
		return description.TopologyKindReplicaSetWithPrimary
	case "Sharded":
		return description.TopologyKindSharded
	case "LoadBalanced":
		return description.TopologyKindLoadBalanced
	case "Unknown":
		return description.Unknown
	default:
		t.Fatalf("unrecognized topology kind: %q", s)
	}

	return description.Unknown
}

func serverKindFromString(t *testing.T, s string) description.ServerKind {
	t.Helper()

	switch s {
	case "Standalone":
		return description.ServerKindStandalone
	case "RSOther":
		return description.ServerKindRSMember
	case "RSPrimary":
		return description.ServerKindRSPrimary
	case "RSSecondary":
		return description.ServerKindRSSecondary
	case "RSArbiter":
		return description.ServerKindRSArbiter
	case "RSGhost":
		return description.ServerKindRSGhost
	case "Mongos":
		return description.ServerKindMongos
	case "LoadBalancer":
		return description.ServerKindLoadBalancer
	case "PossiblePrimary", "Unknown":
		// Go does not have a PossiblePrimary server type and per the SDAM spec, this type is synonymous with Unknown.
		return description.Unknown
	default:
		t.Fatalf("unrecognized server kind: %q", s)
	}

	return description.Unknown
}

func BenchmarkSelectServerFromDescription(b *testing.B) {
	for _, bcase := range []struct {
		name        string
		serversHook func(servers []description.Server)
	}{
		{
			name:        "AllFit",
			serversHook: func([]description.Server) {},
		},
		{
			name: "AllButOneFit",
			serversHook: func(servers []description.Server) {
				servers[0].Kind = description.Unknown
			},
		},
		{
			name: "HalfFit",
			serversHook: func(servers []description.Server) {
				for i := 0; i < len(servers); i += 2 {
					servers[i].Kind = description.Unknown
				}
			},
		},
		{
			name: "OneFit",
			serversHook: func(servers []description.Server) {
				for i := 1; i < len(servers); i++ {
					servers[i].Kind = description.Unknown
				}
			},
		},
	} {
		bcase := bcase

		b.Run(bcase.name, func(b *testing.B) {
			s := description.Server{
				Addr:              address.Address("localhost:27017"),
				HeartbeatInterval: time.Duration(10) * time.Second,
				LastWriteTime:     time.Date(2017, 2, 11, 14, 0, 0, 0, time.UTC),
				LastUpdateTime:    time.Date(2017, 2, 11, 14, 0, 2, 0, time.UTC),
				Kind:              description.ServerKindMongos,
				WireVersion:       &description.VersionRange{Min: 6, Max: 21},
			}
			servers := make([]description.Server, 100)
			for i := 0; i < len(servers); i++ {
				servers[i] = s
			}
			bcase.serversHook(servers)
			desc := description.Topology{
				Servers: servers,
			}

			b.ResetTimer()
			b.RunParallel(func(p *testing.PB) {
				b.ReportAllocs()
				for p.Next() {
					var c Topology
					_, _ = c.selectServerFromDescription(desc, selectNone)
				}
			})
		})
	}
}
