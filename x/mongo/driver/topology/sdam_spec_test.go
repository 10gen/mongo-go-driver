// Copyright (C) MongoDB, Inc. 2017-present.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

package topology

import (
	"context"
	"fmt"
	"io/ioutil"
	"net"
	"path"
	"sync"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/event"
	"go.mongodb.org/mongo-driver/v2/internal/assert"
	"go.mongodb.org/mongo-driver/v2/internal/driverutil"
	"go.mongodb.org/mongo-driver/v2/internal/spectest"
	"go.mongodb.org/mongo-driver/v2/mongo/address"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/x/bsonx/bsoncore"
	"go.mongodb.org/mongo-driver/v2/x/mongo/driver"
	"go.mongodb.org/mongo-driver/v2/x/mongo/driver/description"
)

type response struct {
	Host  string
	Hello Hello
}

type Hello struct {
	Arbiters                     []string          `bson:"arbiters,omitempty"`
	ArbiterOnly                  bool              `bson:"arbiterOnly,omitempty"`
	ClusterTime                  bson.Raw          `bson:"$clusterTime,omitempty"`
	Compression                  []string          `bson:"compression,omitempty"`
	ElectionID                   bson.ObjectID     `bson:"electionId,omitempty"`
	Hidden                       bool              `bson:"hidden,omitempty"`
	Hosts                        []string          `bson:"hosts,omitempty"`
	HelloOK                      bool              `bson:"helloOk,omitempty"`
	IsWritablePrimary            bool              `bson:"isWritablePrimary,omitempty"`
	IsReplicaSet                 bool              `bson:"isreplicaset,omitempty"`
	LastWrite                    *lastWriteDate    `bson:"lastWrite,omitempty"`
	LogicalSessionTimeoutMinutes uint32            `bson:"logicalSessionTimeoutMinutes,omitempty"`
	MaxBSONObjectSize            uint32            `bson:"maxBsonObjectSize,omitempty"`
	MaxMessageSizeBytes          uint32            `bson:"maxMessageSizeBytes,omitempty"`
	MaxWriteBatchSize            uint32            `bson:"maxWriteBatchSize,omitempty"`
	Me                           string            `bson:"me,omitempty"`
	MaxWireVersion               int32             `bson:"maxWireVersion,omitempty"`
	MinWireVersion               int32             `bson:"minWireVersion,omitempty"`
	Msg                          string            `bson:"msg,omitempty"`
	OK                           int32             `bson:"ok"`
	Passives                     []string          `bson:"passives,omitempty"`
	Primary                      string            `bson:"primary,omitempty"`
	ReadOnly                     bool              `bson:"readOnly,omitempty"`
	SaslSupportedMechs           []string          `bson:"saslSupportedMechs,omitempty"`
	Secondary                    bool              `bson:"secondary,omitempty"`
	SetName                      string            `bson:"setName,omitempty"`
	SetVersion                   uint32            `bson:"setVersion,omitempty"`
	Tags                         map[string]string `bson:"tags,omitempty"`
	TopologyVersion              *topologyVersion  `bson:"topologyVersion,omitempty"`
}

type lastWriteDate struct {
	LastWriteDate time.Time `bson:"lastWriteDate"`
}

type server struct {
	Type            string
	SetName         string
	SetVersion      uint32
	ElectionID      *bson.ObjectID `bson:"electionId"`
	MinWireVersion  *int32
	MaxWireVersion  *int32
	TopologyVersion *topologyVersion
	Pool            *testPool
}

type topologyVersion struct {
	ProcessID bson.ObjectID `bson:"processId"`
	Counter   int64
}

type testPool struct {
	Generation uint64
}

type applicationError struct {
	Address        string
	Generation     *uint64
	MaxWireVersion *int32
	When           string
	Type           string
	Response       bsoncore.Document
}

type topologyDescription struct {
	TopologyType string              `bson:"topologyType"`
	Servers      []serverDescription `bson:"servers"`
	SetName      string              `bson:"setName,omitempty"`
}

type serverDescription struct {
	Address  string   `bson:"address"`
	Arbiters []string `bson:"arbiters"`
	Hosts    []string `bson:"hosts"`
	Passives []string `bson:"passives"`
	Primary  string   `bson:"primary,omitempty"`
	SetName  string   `bson:"setName,omitempty"`
	Type     string   `bson:"type"`
}

type topologyOpeningEvent struct {
	TopologyID string `bson:"topologyId"`
}

type serverOpeningEvent struct {
	Address    string `bson:"address"`
	TopologyID string `bson:"topologyId"`
}

type topologyDescriptionChangedEvent struct {
	TopologyID          string              `bson:"topologyId"`
	PreviousDescription topologyDescription `bson:"previousDescription"`
	NewDescription      topologyDescription `bson:"newDescription"`
}

type serverDescriptionChangedEvent struct {
	Address             string            `bson:"address"`
	TopologyID          string            `bson:"topologyId"`
	PreviousDescription serverDescription `bson:"previousDescription"`
	NewDescription      serverDescription `bson:"newDescription"`
}

type serverClosedEvent struct {
	Address    string `bson:"address"`
	TopologyID string `bson:"topologyId"`
}

type monitoringEvent struct {
	TopologyOpeningEvent            *topologyOpeningEvent            `bson:"topology_opening_event,omitempty"`
	ServerOpeningEvent              *serverOpeningEvent              `bson:"server_opening_event,omitempty"`
	TopologyDescriptionChangedEvent *topologyDescriptionChangedEvent `bson:"topology_description_changed_event,omitempty"`
	ServerDescriptionChangedEvent   *serverDescriptionChangedEvent   `bson:"server_description_changed_event,omitempty"`
	ServerClosedEvent               *serverClosedEvent               `bson:"server_closed_event,omitempty"`
}

type outcome struct {
	Servers                      map[string]server
	TopologyType                 string
	SetName                      string
	LogicalSessionTimeoutMinutes *int64
	MaxSetVersion                uint32
	MaxElectionID                bson.ObjectID `bson:"maxElectionId"`
	Compatible                   *bool
	Events                       []monitoringEvent
}

type phase struct {
	Description       string
	Responses         []response
	ApplicationErrors []applicationError
	Outcome           outcome
}

type testCase struct {
	Description string
	URI         string
	Phases      []phase
}

func serverDescriptionChanged(e *event.ServerDescriptionChangedEvent) {
	lock.Lock()
	publishedEvents = append(publishedEvents, *e)
	lock.Unlock()
}

func serverOpening(e *event.ServerOpeningEvent) {
	lock.Lock()
	publishedEvents = append(publishedEvents, *e)
	lock.Unlock()
}

func topologyDescriptionChanged(e *event.TopologyDescriptionChangedEvent) {
	lock.Lock()
	publishedEvents = append(publishedEvents, *e)
	lock.Unlock()
}

func topologyOpening(e *event.TopologyOpeningEvent) {
	lock.Lock()
	publishedEvents = append(publishedEvents, *e)
	lock.Unlock()
}

func serverClosed(e *event.ServerClosedEvent) {
	lock.Lock()
	publishedEvents = append(publishedEvents, *e)
	lock.Unlock()
}

const testsDir string = "../../../../testdata/server-discovery-and-monitoring/"

var publishedEvents []interface{}
var lock sync.Mutex

func (r *response) UnmarshalBSON(buf []byte) error {
	doc := bson.Raw(buf)
	if err := doc.Index(0).Value().Unmarshal(&r.Host); err != nil {
		return fmt.Errorf("error unmarshalling Host: %w", err)
	}

	if err := doc.Index(1).Value().Unmarshal(&r.Hello); err != nil {
		return fmt.Errorf("error unmarshalling Hello: %w", err)
	}

	return nil
}

func setUpTopology(t *testing.T, uri string) *Topology {
	sdam := &event.ServerMonitor{
		ServerDescriptionChanged:   serverDescriptionChanged,
		ServerOpening:              serverOpening,
		TopologyDescriptionChanged: topologyDescriptionChanged,
		TopologyOpening:            topologyOpening,
		ServerClosed:               serverClosed,
	}

	cfg, err := NewConfig(options.Client().ApplyURI(uri).SetServerMonitor(sdam), nil)
	assert.Nil(t, err, "error constructing topology config: %v", err)

	// Disable server monitoring because the hosts in the SDAM spec tests don't actually exist, so the server monitor
	// can race with the test and mark the server Unknown when it fails to connect, which causes tests to fail.
	cfg.ServerOpts = append(cfg.ServerOpts, withMonitoringDisabled(func(bool) bool { return true }))

	topo, err := New(cfg)
	assert.Nil(t, err, "topology.New error: %v", err)

	err = topo.Connect()
	assert.Nil(t, err, "topology.Connect error: %v", err)

	return topo
}

func applyResponses(t *testing.T, topo *Topology, responses []response, sub *driver.Subscription) {
	select {
	case <-sub.Updates:
	default:
	}
	for _, response := range responses {
		doc, err := bson.Marshal(response.Hello)
		assert.Nil(t, err, "Marshal error: %v", err)

		addr := address.Address(response.Host)
		desc := driverutil.NewServerDescription(addr, doc)
		server, ok := topo.servers[addr]
		if ok {
			server.updateDescription(desc)
		} else {
			// for tests that check that server descriptions that aren't in the topology aren't applied
			topo.apply(context.Background(), desc)
		}
		select {
		case <-sub.Updates:
		default:
			return
		}
	}
}

type netErr struct {
	timeout bool
}

func (n netErr) Error() string {
	return "error"
}

func (n netErr) Timeout() bool {
	return n.timeout
}

func (n netErr) Temporary() bool {
	return false
}

var _ net.Error = (*netErr)(nil)

func applyErrors(t *testing.T, topo *Topology, errors []applicationError) {
	for _, appErr := range errors {
		var currError error
		switch appErr.Type {
		case "command":
			currError = driver.ExtractErrorFromServerResponse(appErr.Response)
		case "network":
			currError = driver.Error{
				Labels:  []string{driver.NetworkError},
				Wrapped: ConnectionError{Wrapped: netErr{false}},
			}
		case "timeout":
			currError = driver.Error{
				Labels:  []string{driver.NetworkError},
				Wrapped: ConnectionError{Wrapped: netErr{true}},
			}
		default:
			t.Fatalf("unrecognized error type: %v", appErr.Type)
		}
		server, ok := topo.servers[address.Address(appErr.Address)]
		assert.True(t, ok, "server not found: %v", appErr.Address)

		desc := server.Description()
		versionRange := driverutil.NewVersionRange(0, *appErr.MaxWireVersion)
		desc.WireVersion = &versionRange

		generation, _ := server.pool.generation.getGeneration(nil)
		if appErr.Generation != nil {
			generation = *appErr.Generation
		}
		// use generation number to check conn stale
		innerConn := connection{
			desc:       desc,
			generation: generation,
			pool:       server.pool,
		}
		conn := Connection{connection: &innerConn}

		switch appErr.When {
		case "beforeHandshakeCompletes":
			server.ProcessHandshakeError(currError, generation, nil)
		case "afterHandshakeCompletes":
			_ = server.ProcessError(currError, &conn)
		default:
			t.Fatalf("unrecognized applicationError.When value: %v", appErr.When)
		}
	}
}

func compareServerDescriptions(t *testing.T,
	expected serverDescription, actual event.ServerDescription, idx int) {
	t.Helper()

	assert.Equal(t, expected.Address, actual.Addr.String(),
		"%v: expected server address %s, got %s", idx, expected.Address, actual.Addr)

	assert.Equal(t, len(expected.Hosts), len(actual.Hosts),
		"%v: expected %d hosts, got %d", idx, len(expected.Hosts), len(actual.Hosts))
	for idx, expectedHost := range expected.Hosts {
		actualHost := actual.Hosts[idx]
		assert.Equal(t, expectedHost, actualHost, "%v: expected host %s, got %s", idx, expectedHost, actualHost)
	}

	assert.Equal(t, len(expected.Passives), len(actual.Passives),
		"%v: expected %d hosts, got %d", idx, len(expected.Passives), len(actual.Passives))
	for idx, expectedPassive := range expected.Passives {
		actualPassive := actual.Passives[idx]
		assert.Equal(t, expectedPassive, actualPassive, "%v: expected passive %s, got %s", idx, expectedPassive, actualPassive)
	}

	assert.Equal(t, expected.Primary, string(actual.Primary),
		"%v: expected primary %s, got %s", idx, expected.Primary, actual.Primary)
	assert.Equal(t, expected.SetName, actual.SetName,
		"%v: expected set name %s, got %s", idx, expected.SetName, actual.SetName)

	// PossiblePrimary is only relevant to single-threaded drivers.
	if expected.Type == "PossiblePrimary" {
		expected.Type = "Unknown"
	}
	assert.Equal(t, expected.Type, actual.Kind,
		"%v: expected server kind %s, got %s", idx, expected.Type, actual.Kind)
}

func compareTopologyDescriptions(t *testing.T,
	expected topologyDescription, actual event.TopologyDescription, idx int) {
	t.Helper()

	assert.Equal(t, expected.TopologyType, actual.Kind,
		"%v: expected topology kind %s, got %s", idx, expected.TopologyType, actual.Kind)
	assert.Equal(t, len(expected.Servers), len(actual.Servers),
		"%v: expected %d servers, got %d", idx, len(expected.Servers), len(actual.Servers))

	for idx, es := range expected.Servers {
		as := actual.Servers[idx]
		compareServerDescriptions(t, es, as, idx)
	}

	assert.Equal(t, expected.SetName, actual.SetName,
		"%v: expected set name %s, got %s", idx, expected.SetName, actual.SetName)
}

func compareEvents(t *testing.T, events []monitoringEvent) {
	t.Helper()

	lock.Lock()
	defer lock.Unlock()

	assert.Equal(t, len(events), len(publishedEvents),
		"expected %d published events, got %d\n",
		len(events), len(publishedEvents))

	for idx, me := range events {
		if me.TopologyOpeningEvent != nil {
			actual, ok := publishedEvents[idx].(event.TopologyOpeningEvent)
			assert.True(t, ok, "%v: expected type %T, got %T", idx, event.TopologyOpeningEvent{}, publishedEvents[idx])
			assert.False(t, actual.TopologyID.IsZero(), "%v: expected topology id", idx)
		}
		if me.ServerOpeningEvent != nil {
			actual, ok := publishedEvents[idx].(event.ServerOpeningEvent)
			assert.True(t, ok, "%v: expected type %T, got %T", idx, event.ServerOpeningEvent{}, publishedEvents[idx])

			evt := me.ServerOpeningEvent
			assert.Equal(t, evt.Address, string(actual.Address),
				"%v: expected address %s, got %s", idx, evt.Address, actual.Address)
			assert.False(t, actual.TopologyID.IsZero(), "%v: expected topology id", idx)
		}
		if me.TopologyDescriptionChangedEvent != nil {
			actual, ok := publishedEvents[idx].(event.TopologyDescriptionChangedEvent)
			assert.True(t, ok, "%v: expected type %T, got %T", idx, event.TopologyDescriptionChangedEvent{}, publishedEvents[idx])

			evt := me.TopologyDescriptionChangedEvent
			compareTopologyDescriptions(t, evt.PreviousDescription, actual.PreviousDescription, idx)
			compareTopologyDescriptions(t, evt.NewDescription, actual.NewDescription, idx)
			assert.False(t, actual.TopologyID.IsZero(), "%v: expected topology id", idx)
		}
		if me.ServerDescriptionChangedEvent != nil {
			actual, ok := publishedEvents[idx].(event.ServerDescriptionChangedEvent)
			assert.True(t, ok, "%v: expected type %T, got %T", idx, event.ServerDescriptionChangedEvent{}, publishedEvents[idx])

			evt := me.ServerDescriptionChangedEvent
			assert.Equal(t, evt.Address, string(actual.Address),
				"%v: expected server address %s, got %s", idx, evt.Address, actual.Address)
			compareServerDescriptions(t, evt.PreviousDescription, actual.PreviousDescription, idx)
			compareServerDescriptions(t, evt.NewDescription, actual.NewDescription, idx)
			assert.False(t, actual.TopologyID.IsZero(), "%v: expected topology id", idx)
		}
		if me.ServerClosedEvent != nil {
			actual, ok := publishedEvents[idx].(event.ServerClosedEvent)
			assert.True(t, ok, "%v: expected type %T, got %T", idx, event.ServerClosedEvent{}, publishedEvents[idx])

			evt := me.ServerClosedEvent
			assert.Equal(t, evt.Address, string(actual.Address),
				"%v: expected server address %s, got %s", idx, evt.Address, actual.Address)
			assert.False(t, actual.TopologyID.IsZero(), "%v: expected topology id", idx)
		}
	}
}

func findServerInTopology(topo description.Topology, addr address.Address) (description.Server, bool) {
	for _, server := range topo.Servers {
		if server.Addr.String() == addr.String() {
			return server, true
		}
	}
	return description.Server{}, false
}

func runTest(t *testing.T, directory string, filename string) {
	filepath := path.Join(testsDir, directory, filename)
	content, err := ioutil.ReadFile(filepath)
	assert.Nil(t, err, "ReadFile error: %v", err)

	t.Run(directory+"/"+filename, func(t *testing.T) {
		var test testCase
		err = bson.UnmarshalExtJSON(content, false, &test)
		assert.Nil(t, err, "Unmarshal error: %v", err)
		topo := setUpTopology(t, test.URI)
		sub, err := topo.Subscribe()
		assert.Nil(t, err, "subscribe error: %v", err)

		for _, phase := range test.Phases {
			applyResponses(t, topo, phase.Responses, sub)
			applyErrors(t, topo, phase.ApplicationErrors)

			if phase.Outcome.Events != nil {
				compareEvents(t, phase.Outcome.Events)
				publishedEvents = nil
				continue
			}
			publishedEvents = nil
			if phase.Outcome.Compatible == nil || *phase.Outcome.Compatible {
				assert.True(t, topo.fsm.compatible.Load().(bool), "Expected servers to be compatible")
				assert.Nil(t, topo.fsm.compatibilityErr, "expected fsm.compatibility to be nil, got %v",
					topo.fsm.compatibilityErr)
			} else {
				assert.False(t, topo.fsm.compatible.Load().(bool), "Expected servers to not be compatible")
				assert.NotNil(t, topo.fsm.compatibilityErr, "expected fsm.compatibility error to be non-nil")
				continue
			}
			desc := topo.Description()

			assert.Equal(t, phase.Outcome.TopologyType, desc.Kind.String(),
				"expected TopologyType to be %v, got %v", phase.Outcome.TopologyType, desc.Kind.String())
			assert.Equal(t, phase.Outcome.SetName, topo.fsm.SetName,
				"expected SetName to be %v, got %v", phase.Outcome.SetName, topo.fsm.SetName)
			assert.Equal(t, len(phase.Outcome.Servers), len(desc.Servers),
				"expected %v servers, got %v", len(phase.Outcome.Servers), len(desc.Servers))

			assert.Equal(t,
				phase.Outcome.LogicalSessionTimeoutMinutes,
				desc.SessionTimeoutMinutes,
				"expected and actual logical session timeout minutes are different")

			assert.Equal(t, phase.Outcome.MaxSetVersion, topo.fsm.maxSetVersion,
				"expected maxSetVersion to be %v, got %v", phase.Outcome.MaxSetVersion, topo.fsm.maxSetVersion)
			assert.Equal(t, phase.Outcome.MaxElectionID, topo.fsm.maxElectionID,
				"expected maxElectionID to be %v, got %v", phase.Outcome.MaxElectionID, topo.fsm.maxElectionID)

			for addr, server := range phase.Outcome.Servers {
				fsmServer, ok := findServerInTopology(desc, address.Address(addr))
				assert.True(t, ok, "Couldn't find server %v", addr)

				assert.Equal(t, address.Address(addr), fsmServer.Addr,
					"expected server address to be %v, got %v", address.Address(addr), fsmServer.Addr)
				assert.Equal(t, server.SetName, fsmServer.SetName,
					"expected server SetName to be %v, got %v", server.SetName, fsmServer.SetName)
				assert.Equal(t, server.SetVersion, fsmServer.SetVersion,
					"expected server SetVersion to be %v, got %v", server.SetVersion, fsmServer.SetVersion)
				if server.ElectionID != nil {
					assert.Equal(t, *server.ElectionID, fsmServer.ElectionID,
						"expected server ElectionID to be %v, got %v", *server.ElectionID, fsmServer.ElectionID)
				}
				if server.TopologyVersion != nil {

					assert.NotNil(t, fsmServer.TopologyVersion, "expected server TopologyVersion not to be nil")
					assert.Equal(t, server.TopologyVersion.ProcessID, fsmServer.TopologyVersion.ProcessID,
						"expected server TopologyVersion ProcessID to be %v, got %v", server.TopologyVersion.ProcessID, fsmServer.TopologyVersion.ProcessID)
					assert.Equal(t, server.TopologyVersion.Counter, fsmServer.TopologyVersion.Counter,
						"expected server TopologyVersion Counter to be %v, got %v", server.TopologyVersion.Counter, fsmServer.TopologyVersion.Counter)
				} else {
					assert.Nil(t, fsmServer.TopologyVersion, "expected server TopologyVersion to be nil")
				}

				// PossiblePrimary is only relevant to single-threaded drivers.
				if server.Type == "PossiblePrimary" {
					server.Type = "Unknown"
				}

				assert.Equal(t, server.Type, fsmServer.Kind.String(),
					"expected server Type to be %v, got %v", server.Type, fsmServer.Kind.String())
				if server.Pool != nil {
					topo.serversLock.Lock()
					actualServer := topo.servers[address.Address(addr)]
					topo.serversLock.Unlock()
					actualGeneration, _ := actualServer.pool.generation.getGeneration(nil)
					assert.Equal(t, server.Pool.Generation, actualGeneration,
						"expected server pool generation to be %v, got %v", server.Pool.Generation, actualGeneration)
				}
			}
		}
	})
}

// Test case for all SDAM spec tests.
func TestSDAMSpec(t *testing.T) {
	for _, subdir := range []string{"single", "rs", "sharded", "load-balanced", "errors", "monitoring"} {
		for _, file := range spectest.FindJSONFilesInDir(t, path.Join(testsDir, subdir)) {
			runTest(t, subdir, file)
		}
	}
}

func TestHasStalePrimary(t *testing.T) {
	t.Parallel()

	t.Run("WV17 SEI EQ MEI and SSV LT MSV", func(t *testing.T) {
		t.Parallel()

		srv := description.Server{
			WireVersion: &description.VersionRange{Min: 17, Max: 17},
			ElectionID:  bson.NewObjectIDFromTimestamp(time.Now()),
			SetVersion:  1,
		}

		fsm := fsm{
			maxElectionID: srv.ElectionID,
			maxSetVersion: 2,
		}

		boolVal := hasStalePrimary(fsm, srv)
		assert.True(t, boolVal, "expected true, got false")
	})

	t.Run("WV17 SEI EQ MEI and SSV GT MSV", func(t *testing.T) {
		t.Parallel()

		srv := description.Server{
			WireVersion: &description.VersionRange{Min: 17, Max: 17},
			ElectionID:  bson.NewObjectIDFromTimestamp(time.Now()),
			SetVersion:  2,
		}

		fsm := fsm{
			maxElectionID: srv.ElectionID,
			maxSetVersion: 1,
		}

		boolVal := hasStalePrimary(fsm, srv)
		assert.False(t, boolVal, "expected false, got true")
	})

	t.Run("WV17 SEI EQ MEI and SSV EQ MSV", func(t *testing.T) {
		t.Parallel()

		srv := description.Server{
			WireVersion: &description.VersionRange{Min: 17, Max: 17},
			ElectionID:  bson.NewObjectIDFromTimestamp(time.Now()),
			SetVersion:  1,
		}

		fsm := fsm{
			maxElectionID: srv.ElectionID,
			maxSetVersion: 1,
		}

		boolVal := hasStalePrimary(fsm, srv)
		assert.False(t, boolVal, "expected false, got true")
	})

	t.Run("WV17 SEI GT MEI and SSV LT MSV", func(t *testing.T) {
		t.Parallel()

		fsm := fsm{
			maxElectionID: bson.NewObjectIDFromTimestamp(time.Now()),
			maxSetVersion: 2,
		}

		srv := description.Server{
			WireVersion: &description.VersionRange{Min: 17, Max: 17},
			ElectionID:  bson.NewObjectIDFromTimestamp(time.Now().Add(time.Second)),
			SetVersion:  1,
		}

		boolVal := hasStalePrimary(fsm, srv)
		assert.False(t, boolVal, "expected false, got true")
	})

	t.Run("WV17 SEI GT MEI and SSV GT MSV", func(t *testing.T) {
		t.Parallel()

		fsm := fsm{
			maxElectionID: bson.NewObjectIDFromTimestamp(time.Now()),
			maxSetVersion: 1,
		}

		srv := description.Server{
			WireVersion: &description.VersionRange{Min: 17, Max: 17},
			ElectionID:  bson.NewObjectIDFromTimestamp(time.Now().Add(time.Second)),
			SetVersion:  2,
		}

		boolVal := hasStalePrimary(fsm, srv)
		assert.False(t, boolVal, "expected false, got true")
	})

	t.Run("WV17 SEI GT MEI and SSV EQ MSV", func(t *testing.T) {
		t.Parallel()

		fsm := fsm{
			maxElectionID: bson.NewObjectIDFromTimestamp(time.Now()),
			maxSetVersion: 1,
		}

		srv := description.Server{
			WireVersion: &description.VersionRange{Min: 17, Max: 17},
			ElectionID:  bson.NewObjectIDFromTimestamp(time.Now().Add(time.Second)),
			SetVersion:  1,
		}

		boolVal := hasStalePrimary(fsm, srv)
		assert.False(t, boolVal, "expected false, got true")
	})

	t.Run("WV17 SEI LT MEI and SSV LT MSV", func(t *testing.T) {
		t.Parallel()

		srv := description.Server{
			WireVersion: &description.VersionRange{Min: 17, Max: 17},
			ElectionID:  bson.NewObjectIDFromTimestamp(time.Now().Add(-time.Second)),
			SetVersion:  1,
		}

		fsm := fsm{
			maxElectionID: bson.NewObjectIDFromTimestamp(time.Now()),
			maxSetVersion: 2,
		}

		boolVal := hasStalePrimary(fsm, srv)
		assert.True(t, boolVal, "expected true, got false")
	})

	t.Run("WV17 SEI LT MEI and SSV GT MSV", func(t *testing.T) {
		t.Parallel()

		srv := description.Server{
			WireVersion: &description.VersionRange{Min: 17, Max: 17},
			ElectionID:  bson.NewObjectIDFromTimestamp(time.Now().Add(-time.Second)),
			SetVersion:  2,
		}

		fsm := fsm{
			maxElectionID: bson.NewObjectIDFromTimestamp(time.Now()),
			maxSetVersion: 1,
		}

		boolVal := hasStalePrimary(fsm, srv)
		assert.True(t, boolVal, "expected true, got false")
	})

	t.Run("WV17 SEI LT MEI and SSV EQ MSV", func(t *testing.T) {
		t.Parallel()

		srv := description.Server{
			WireVersion: &description.VersionRange{Min: 17, Max: 17},
			ElectionID:  bson.NewObjectIDFromTimestamp(time.Now().Add(-time.Second)),
			SetVersion:  1,
		}

		fsm := fsm{
			maxElectionID: bson.NewObjectIDFromTimestamp(time.Now()),
			maxSetVersion: 1,
		}

		boolVal := hasStalePrimary(fsm, srv)
		assert.True(t, boolVal, "expected true, got false")
	})
}
