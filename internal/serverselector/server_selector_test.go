// Copyright (C) MongoDB, Inc. 2024-present.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

package serverselector

import (
	"errors"
	"io/ioutil"
	"path"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/internal/assert"
	"go.mongodb.org/mongo-driver/v2/internal/driverutil"
	"go.mongodb.org/mongo-driver/v2/internal/require"
	"go.mongodb.org/mongo-driver/v2/internal/spectest"
	"go.mongodb.org/mongo-driver/v2/mongo/address"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"
	"go.mongodb.org/mongo-driver/v2/x/mongo/driver/description"
)

type lastWriteDate struct {
	LastWriteDate int64 `bson:"lastWriteDate"`
}

type serverDesc struct {
	Address        string            `bson:"address"`
	AverageRTTMS   *int              `bson:"avg_rtt_ms"`
	MaxWireVersion *int32            `bson:"maxWireVersion"`
	LastUpdateTime *int              `bson:"lastUpdateTime"`
	LastWrite      *lastWriteDate    `bson:"lastWrite"`
	Type           string            `bson:"type"`
	Tags           map[string]string `bson:"tags"`
}

type topDesc struct {
	Type    string        `bson:"type"`
	Servers []*serverDesc `bson:"servers"`
}

type readPref struct {
	MaxStaleness *int                `bson:"maxStalenessSeconds"`
	Mode         string              `bson:"mode"`
	TagSets      []map[string]string `bson:"tag_sets"`
}

type testCase struct {
	TopologyDescription  topDesc       `bson:"topology_description"`
	Operation            string        `bson:"operation"`
	ReadPreference       readPref      `bson:"read_preference"`
	SuitableServers      []*serverDesc `bson:"suitable_servers"`
	InLatencyWindow      []*serverDesc `bson:"in_latency_window"`
	HeartbeatFrequencyMS *int          `bson:"heartbeatFrequencyMS"`
	Error                *bool
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

func anyTagsInSets(sets []readpref.TagSet) bool {
	for _, set := range sets {
		if len(set) > 0 {
			return true
		}
	}

	return false
}

func findServerByAddress(servers []description.Server, address string) description.Server {
	for _, server := range servers {
		if server.Addr.String() == address {
			return server
		}
	}

	return description.Server{}
}

func compareServers(t *testing.T, expected []*serverDesc, actual []description.Server) {
	require.Equal(t, len(expected), len(actual))

	for _, expectedServer := range expected {
		actualServer := findServerByAddress(actual, expectedServer.Address)
		require.NotNil(t, actualServer)

		if expectedServer.AverageRTTMS != nil {
			require.Equal(t, *expectedServer.AverageRTTMS, int(actualServer.AverageRTT/time.Millisecond))
		}

		require.Equal(t, expectedServer.Type, actualServer.Kind.String())

		require.Equal(t, len(expectedServer.Tags), len(actualServer.Tags))
		for _, actualTag := range actualServer.Tags {
			expectedTag, ok := expectedServer.Tags[actualTag.Name]
			require.True(t, ok)
			require.Equal(t, expectedTag, actualTag.Value)
		}
	}
}

const maxStalenessTestsDir = "../../testdata/max-staleness"

// Test case for all max staleness spec tests.
func TestMaxStalenessSpec(t *testing.T) {
	for _, topology := range [...]string{
		"ReplicaSetNoPrimary",
		"ReplicaSetWithPrimary",
		"Sharded",
		"Single",
		"Unknown",
	} {
		for _, file := range spectest.FindJSONFilesInDir(t,
			path.Join(maxStalenessTestsDir, topology)) {

			runTest(t, maxStalenessTestsDir, topology, file)
		}
	}
}

const selectorTestsDir = "../../testdata/server-selection/server_selection"

func selectServers(t *testing.T, test *testCase) error {
	servers := make([]description.Server, 0, len(test.TopologyDescription.Servers))

	// Times in the JSON files are given as offsets from an unspecified time, but the driver
	// stores the lastWrite field as a timestamp, so we arbitrarily choose the current time
	// as the base to offset from.
	baseTime := time.Now()

	for _, serverDescription := range test.TopologyDescription.Servers {
		server := description.Server{
			Addr: address.Address(serverDescription.Address),
			Kind: serverKindFromString(t, serverDescription.Type),
		}

		if serverDescription.AverageRTTMS != nil {
			server.AverageRTT = time.Duration(*serverDescription.AverageRTTMS) * time.Millisecond
			server.AverageRTTSet = true
		}

		if test.HeartbeatFrequencyMS != nil {
			server.HeartbeatInterval = time.Duration(*test.HeartbeatFrequencyMS) * time.Millisecond
		}

		if serverDescription.LastUpdateTime != nil {
			ms := int64(*serverDescription.LastUpdateTime)
			server.LastUpdateTime = time.Unix(ms/1e3, ms%1e3/1e6)
		}

		if serverDescription.LastWrite != nil {
			i := serverDescription.LastWrite.LastWriteDate

			timeWithOffset := baseTime.Add(time.Duration(i) * time.Millisecond)
			server.LastWriteTime = timeWithOffset
		}

		if serverDescription.MaxWireVersion != nil {
			versionRange := driverutil.NewVersionRange(0, *serverDescription.MaxWireVersion)
			server.WireVersion = &versionRange
		}

		if serverDescription.Tags != nil {
			server.Tags = readpref.NewTagSetFromMap(serverDescription.Tags)
		}

		if test.ReadPreference.MaxStaleness != nil && server.WireVersion == nil {
			server.WireVersion = &description.VersionRange{Max: 21}
		}

		servers = append(servers, server)
	}

	c := description.Topology{
		Kind:    topologyKindFromString(t, test.TopologyDescription.Type),
		Servers: servers,
	}

	if len(test.ReadPreference.Mode) == 0 {
		test.ReadPreference.Mode = "Primary"
	}

	readprefMode, err := readpref.ModeFromString(test.ReadPreference.Mode)
	if err != nil {
		return err
	}

	rpOpts := readpref.Options()

	tagSets := readpref.NewTagSetsFromMaps(test.ReadPreference.TagSets)
	if anyTagsInSets(tagSets) {
		rpOpts.SetTagSets(tagSets)
	}

	if test.ReadPreference.MaxStaleness != nil {
		s := time.Duration(*test.ReadPreference.MaxStaleness) * time.Second
		rpOpts.SetMaxStaleness(s)
	}

	rp, err := readpref.New(readprefMode, rpOpts)
	if err != nil {
		return err
	}

	var selector description.ServerSelector

	selector = &ReadPref{ReadPref: rp}
	if test.Operation == "write" {
		selector = &Composite{
			Selectors: []description.ServerSelector{&Write{}, selector},
		}
	}

	result, err := selector.SelectServer(c, c.Servers)
	if err != nil {
		return err
	}

	compareServers(t, test.SuitableServers, result)

	latencySelector := &Latency{Latency: time.Duration(15) * time.Millisecond}
	selector = &Composite{
		Selectors: []description.ServerSelector{selector, latencySelector},
	}

	result, err = selector.SelectServer(c, c.Servers)
	if err != nil {
		return err
	}

	compareServers(t, test.InLatencyWindow, result)

	return nil
}

func runTest(t *testing.T, testsDir string, directory string, filename string) {
	filepath := path.Join(testsDir, directory, filename)
	content, err := ioutil.ReadFile(filepath)
	require.NoError(t, err)

	// Remove ".json" from filename.
	filename = filename[:len(filename)-5]
	testName := directory + "/" + filename + ":"

	t.Run(testName, func(t *testing.T) {
		var test testCase
		require.NoError(t, bson.UnmarshalExtJSON(content, true, &test))

		err := selectServers(t, &test)

		if test.Error == nil || !*test.Error {
			require.NoError(t, err)
		} else {
			require.Error(t, err)
		}
	})
}

// Test case for all SDAM spec tests.
func TestServerSelectionSpec(t *testing.T) {
	for _, topology := range [...]string{
		"ReplicaSetNoPrimary",
		"ReplicaSetWithPrimary",
		"Sharded",
		"Single",
		"Unknown",
		"LoadBalanced",
	} {
		for _, subdir := range [...]string{"read", "write"} {
			subdirPath := path.Join(topology, subdir)

			for _, file := range spectest.FindJSONFilesInDir(t,
				path.Join(selectorTestsDir, subdirPath)) {

				runTest(t, selectorTestsDir, subdirPath, file)
			}
		}
	}
}

func TestServerSelection(t *testing.T) {
	noerr := func(t *testing.T, err error) {
		if err != nil {
			t.Errorf("Unepexted error: %v", err)
			t.FailNow()
		}
	}

	t.Run("WriteSelector", func(t *testing.T) {
		testCases := []struct {
			name  string
			desc  description.Topology
			start int
			end   int
		}{
			{
				name: "ReplicaSetWithPrimary",
				desc: description.Topology{
					Kind: description.TopologyKindReplicaSetWithPrimary,
					Servers: []description.Server{
						{Addr: address.Address("localhost:27017"), Kind: description.ServerKindRSPrimary},
						{Addr: address.Address("localhost:27018"), Kind: description.ServerKindRSSecondary},
						{Addr: address.Address("localhost:27019"), Kind: description.ServerKindRSSecondary},
					},
				},
				start: 0,
				end:   1,
			},
			{
				name: "ReplicaSetNoPrimary",
				desc: description.Topology{
					Kind: description.TopologyKindReplicaSetNoPrimary,
					Servers: []description.Server{
						{Addr: address.Address("localhost:27018"), Kind: description.ServerKindRSSecondary},
						{Addr: address.Address("localhost:27019"), Kind: description.ServerKindRSSecondary},
					},
				},
				start: 0,
				end:   0,
			},
			{
				name: "Sharded",
				desc: description.Topology{
					Kind: description.TopologyKindSharded,
					Servers: []description.Server{
						{Addr: address.Address("localhost:27018"), Kind: description.ServerKindMongos},
						{Addr: address.Address("localhost:27019"), Kind: description.ServerKindMongos},
					},
				},
				start: 0,
				end:   2,
			},
			{
				name: "Single",
				desc: description.Topology{
					Kind: description.TopologyKindSingle,
					Servers: []description.Server{
						{Addr: address.Address("localhost:27018"), Kind: description.ServerKindStandalone},
					},
				},
				start: 0,
				end:   1,
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				result, err := (&Write{}).SelectServer(tc.desc, tc.desc.Servers)
				noerr(t, err)
				if len(result) != tc.end-tc.start {
					t.Errorf("Incorrect number of servers selected. got %d; want %d", len(result), tc.end-tc.start)
				}
				if diff := cmp.Diff(result, tc.desc.Servers[tc.start:tc.end]); diff != "" {
					t.Errorf("Incorrect servers selected (-got +want):\n%s", diff)
				}
			})
		}
	})
	t.Run("LatencySelector", func(t *testing.T) {
		testCases := []struct {
			name  string
			desc  description.Topology
			start int
			end   int
		}{
			{
				name: "NoRTTSet",
				desc: description.Topology{
					Servers: []description.Server{
						{Addr: address.Address("localhost:27017")},
						{Addr: address.Address("localhost:27018")},
						{Addr: address.Address("localhost:27019")},
					},
				},
				start: 0,
				end:   3,
			},
			{
				name: "MultipleServers PartialNoRTTSet",
				desc: description.Topology{
					Servers: []description.Server{
						{Addr: address.Address("localhost:27017"), AverageRTT: 5 * time.Second, AverageRTTSet: true},
						{Addr: address.Address("localhost:27018"), AverageRTT: 10 * time.Second, AverageRTTSet: true},
						{Addr: address.Address("localhost:27019")},
					},
				},
				start: 0,
				end:   2,
			},
			{
				name: "MultipleServers",
				desc: description.Topology{
					Servers: []description.Server{
						{Addr: address.Address("localhost:27017"), AverageRTT: 5 * time.Second, AverageRTTSet: true},
						{Addr: address.Address("localhost:27018"), AverageRTT: 10 * time.Second, AverageRTTSet: true},
						{Addr: address.Address("localhost:27019"), AverageRTT: 26 * time.Second, AverageRTTSet: true},
					},
				},
				start: 0,
				end:   2,
			},
			{
				name:  "No Servers",
				desc:  description.Topology{Servers: []description.Server{}},
				start: 0,
				end:   0,
			},
			{
				name: "1 Server",
				desc: description.Topology{
					Servers: []description.Server{
						{Addr: address.Address("localhost:27017"), AverageRTT: 26 * time.Second, AverageRTTSet: true},
					},
				},
				start: 0,
				end:   1,
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				result, err := (&Latency{Latency: 20 * time.Second}).SelectServer(tc.desc, tc.desc.Servers)
				noerr(t, err)
				if len(result) != tc.end-tc.start {
					t.Errorf("Incorrect number of servers selected. got %d; want %d", len(result), tc.end-tc.start)
				}
				if diff := cmp.Diff(result, tc.desc.Servers[tc.start:tc.end]); diff != "" {
					t.Errorf("Incorrect servers selected (-got +want):\n%s", diff)
				}
			})
		}
	})
}

var readPrefTestPrimary = description.Server{
	Addr:              address.Address("localhost:27017"),
	HeartbeatInterval: time.Duration(10) * time.Second,
	LastWriteTime:     time.Date(2017, 2, 11, 14, 0, 0, 0, time.UTC),
	LastUpdateTime:    time.Date(2017, 2, 11, 14, 0, 2, 0, time.UTC),
	Kind:              description.ServerKindRSPrimary,
	Tags:              readpref.TagSet{readpref.Tag{Name: "a", Value: "1"}},
	WireVersion:       &description.VersionRange{Min: 6, Max: 21},
}
var readPrefTestSecondary1 = description.Server{
	Addr:              address.Address("localhost:27018"),
	HeartbeatInterval: time.Duration(10) * time.Second,
	LastWriteTime:     time.Date(2017, 2, 11, 13, 58, 0, 0, time.UTC),
	LastUpdateTime:    time.Date(2017, 2, 11, 14, 0, 2, 0, time.UTC),
	Kind:              description.ServerKindRSSecondary,
	Tags:              readpref.TagSet{readpref.Tag{Name: "a", Value: "1"}},
	WireVersion:       &description.VersionRange{Min: 6, Max: 21},
}
var readPrefTestSecondary2 = description.Server{
	Addr:              address.Address("localhost:27018"),
	HeartbeatInterval: time.Duration(10) * time.Second,
	LastWriteTime:     time.Date(2017, 2, 11, 14, 0, 0, 0, time.UTC),
	LastUpdateTime:    time.Date(2017, 2, 11, 14, 0, 2, 0, time.UTC),
	Kind:              description.ServerKindRSSecondary,
	Tags:              readpref.TagSet{readpref.Tag{Name: "a", Value: "2"}},
	WireVersion:       &description.VersionRange{Min: 6, Max: 21},
}
var readPrefTestTopology = description.Topology{
	Kind:    description.TopologyKindReplicaSetWithPrimary,
	Servers: []description.Server{readPrefTestPrimary, readPrefTestSecondary1, readPrefTestSecondary2},
}

func TestSelector_Sharded(t *testing.T) {
	t.Parallel()

	subject := readpref.Primary()

	s := description.Server{
		Addr:              address.Address("localhost:27017"),
		HeartbeatInterval: time.Duration(10) * time.Second,
		LastWriteTime:     time.Date(2017, 2, 11, 14, 0, 0, 0, time.UTC),
		LastUpdateTime:    time.Date(2017, 2, 11, 14, 0, 2, 0, time.UTC),
		Kind:              description.ServerKindMongos,
		WireVersion:       &description.VersionRange{Min: 6, Max: 21},
	}
	c := description.Topology{
		Kind:    description.TopologyKindSharded,
		Servers: []description.Server{s},
	}

	result, err := (&ReadPref{ReadPref: subject}).SelectServer(c, c.Servers)

	require.NoError(t, err)
	require.Len(t, result, 1)
	require.Equal(t, []description.Server{s}, result)
}

func BenchmarkLatencySelector(b *testing.B) {
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
				servers[0].AverageRTT = 2 * time.Second
			},
		},
		{
			name: "HalfFit",
			serversHook: func(servers []description.Server) {
				for i := 0; i < len(servers); i += 2 {
					servers[i].AverageRTT = 2 * time.Second
				}
			},
		},
		{
			name: "OneFit",
			serversHook: func(servers []description.Server) {
				for i := 1; i < len(servers); i++ {
					servers[i].AverageRTT = 2 * time.Second
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
				AverageRTTSet:     true,
				AverageRTT:        time.Second,
			}
			servers := make([]description.Server, 100)
			for i := 0; i < len(servers); i++ {
				servers[i] = s
			}
			bcase.serversHook(servers)
			// this will make base 1 sec latency < min (0.5) + conf (1)
			// and high latency 2 higher than the threshold
			servers[99].AverageRTT = 500 * time.Millisecond
			c := description.Topology{
				Kind:    description.TopologyKindSharded,
				Servers: servers,
			}

			b.ResetTimer()
			b.RunParallel(func(p *testing.PB) {
				b.ReportAllocs()
				for p.Next() {
					_, _ = (&Latency{Latency: time.Second}).SelectServer(c, c.Servers)
				}
			})
		})
	}
}

func BenchmarkSelector_Sharded(b *testing.B) {
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
				servers[0].Kind = description.ServerKindLoadBalancer
			},
		},
		{
			name: "HalfFit",
			serversHook: func(servers []description.Server) {
				for i := 0; i < len(servers); i += 2 {
					servers[i].Kind = description.ServerKindLoadBalancer
				}
			},
		},
		{
			name: "OneFit",
			serversHook: func(servers []description.Server) {
				for i := 1; i < len(servers); i++ {
					servers[i].Kind = description.ServerKindLoadBalancer
				}
			},
		},
	} {
		bcase := bcase

		b.Run(bcase.name, func(b *testing.B) {
			subject := readpref.Primary()

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
			c := description.Topology{
				Kind:    description.TopologyKindSharded,
				Servers: servers,
			}

			b.ResetTimer()
			b.RunParallel(func(p *testing.PB) {
				b.ReportAllocs()
				for p.Next() {
					_, _ = (&ReadPref{ReadPref: subject}).SelectServer(c, c.Servers)
				}
			})
		})
	}
}

func Benchmark_SelectServer_SelectServer(b *testing.B) {
	topology := description.Topology{Kind: description.TopologyKindReplicaSet} // You can change the topology as needed
	candidates := []description.Server{
		{Kind: description.ServerKindMongos},
		{Kind: description.ServerKindRSPrimary},
		{Kind: description.ServerKindStandalone},
	}

	selector := &Write{} // Assuming this is the receiver type

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := selector.SelectServer(topology, candidates)
		if err != nil {
			b.Fatalf("Error selecting server: %v", err)
		}
	}
}

func TestSelector_Single(t *testing.T) {
	t.Parallel()

	subject := readpref.Primary()

	s := description.Server{
		Addr:              address.Address("localhost:27017"),
		HeartbeatInterval: time.Duration(10) * time.Second,
		LastWriteTime:     time.Date(2017, 2, 11, 14, 0, 0, 0, time.UTC),
		LastUpdateTime:    time.Date(2017, 2, 11, 14, 0, 2, 0, time.UTC),
		Kind:              description.ServerKindMongos,
		WireVersion:       &description.VersionRange{Min: 6, Max: 21},
	}
	c := description.Topology{
		Kind:    description.TopologyKindSingle,
		Servers: []description.Server{s},
	}

	result, err := (&ReadPref{ReadPref: subject}).SelectServer(c, c.Servers)

	require.NoError(t, err)
	require.Len(t, result, 1)
	require.Equal(t, []description.Server{s}, result)
}

func TestSelector_Primary(t *testing.T) {
	t.Parallel()

	subject := readpref.Primary()

	result, err := (&ReadPref{ReadPref: subject}).SelectServer(readPrefTestTopology, readPrefTestTopology.Servers)

	require.NoError(t, err)
	require.Len(t, result, 1)
	require.Equal(t, []description.Server{readPrefTestPrimary}, result)
}

func TestSelector_Primary_with_no_primary(t *testing.T) {
	t.Parallel()

	subject := readpref.Primary()

	result, err := (&ReadPref{ReadPref: subject}).
		SelectServer(readPrefTestTopology, []description.Server{readPrefTestSecondary1, readPrefTestSecondary2})

	require.NoError(t, err)
	require.Len(t, result, 0)
}

func TestSelector_PrimaryPreferred(t *testing.T) {
	t.Parallel()

	subject := &readpref.ReadPref{Mode: readpref.PrimaryPreferredMode}

	result, err := (&ReadPref{ReadPref: subject}).
		SelectServer(readPrefTestTopology, readPrefTestTopology.Servers)

	require.NoError(t, err)
	require.Len(t, result, 1)
	require.Equal(t, []description.Server{readPrefTestPrimary}, result)
}

func TestSelector_PrimaryPreferred_ignores_tags(t *testing.T) {
	t.Parallel()

	rpOpts := readpref.Options()

	tagSet, err := readpref.NewTagSet("a", "2")
	assert.NoError(t, err)

	rpOpts.SetTagSets([]readpref.TagSet{tagSet})

	subject, err := readpref.New(readpref.PrimaryPreferredMode, rpOpts)
	assert.NoError(t, err)

	result, err := (&ReadPref{ReadPref: subject}).
		SelectServer(readPrefTestTopology, readPrefTestTopology.Servers)

	require.NoError(t, err)
	require.Len(t, result, 1)
	require.Equal(t, []description.Server{readPrefTestPrimary}, result)
}

func TestSelector_PrimaryPreferred_with_no_primary(t *testing.T) {
	t.Parallel()

	subject, err := readpref.New(readpref.PrimaryPreferredMode, nil)
	assert.NoError(t, err)

	result, err := (&ReadPref{ReadPref: subject}).
		SelectServer(readPrefTestTopology, []description.Server{readPrefTestSecondary1, readPrefTestSecondary2})

	require.NoError(t, err)
	require.Len(t, result, 2)
	require.Equal(t, []description.Server{readPrefTestSecondary1, readPrefTestSecondary2}, result)
}

func TestSelector_PrimaryPreferred_with_no_primary_and_tags(t *testing.T) {
	t.Parallel()

	rpOpts := readpref.Options()

	tagSet, err := readpref.NewTagSet("a", "2")
	assert.NoError(t, err)

	rpOpts.SetTagSets([]readpref.TagSet{tagSet})

	subject, err := readpref.New(readpref.PrimaryPreferredMode, rpOpts)
	assert.NoError(t, err)

	result, err := (&ReadPref{ReadPref: subject}).
		SelectServer(readPrefTestTopology, []description.Server{readPrefTestSecondary1, readPrefTestSecondary2})

	require.NoError(t, err)
	require.Len(t, result, 1)
	require.Equal(t, []description.Server{readPrefTestSecondary2}, result)
}

func TestSelector_PrimaryPreferred_with_maxStaleness(t *testing.T) {
	t.Parallel()

	rpOpts := readpref.Options()

	maxStaleness := 90 * time.Second
	rpOpts.SetMaxStaleness(maxStaleness)

	subject, err := readpref.New(readpref.PrimaryPreferredMode, rpOpts)
	assert.NoError(t, err)

	result, err := (&ReadPref{ReadPref: subject}).
		SelectServer(readPrefTestTopology, readPrefTestTopology.Servers)

	require.NoError(t, err)
	require.Len(t, result, 1)
	require.Equal(t, []description.Server{readPrefTestPrimary}, result)
}

func TestSelector_PrimaryPreferred_with_maxStaleness_and_no_primary(t *testing.T) {
	t.Parallel()

	rpOpts := readpref.Options()

	maxStaleness := 90 * time.Second
	rpOpts.SetMaxStaleness(maxStaleness)

	subject, err := readpref.New(readpref.PrimaryPreferredMode, rpOpts)
	assert.NoError(t, err)

	result, err := (&ReadPref{ReadPref: subject}).
		SelectServer(readPrefTestTopology, []description.Server{readPrefTestSecondary1, readPrefTestSecondary2})

	require.NoError(t, err)
	require.Len(t, result, 1)
	require.Equal(t, []description.Server{readPrefTestSecondary2}, result)
}

func TestSelector_SecondaryPreferred(t *testing.T) {
	t.Parallel()

	subject, err := readpref.New(readpref.SecondaryPreferredMode, nil)
	assert.NoError(t, err)

	result, err := (&ReadPref{ReadPref: subject}).SelectServer(readPrefTestTopology, readPrefTestTopology.Servers)

	require.NoError(t, err)
	require.Len(t, result, 2)
	require.Equal(t, []description.Server{readPrefTestSecondary1, readPrefTestSecondary2}, result)
}

func TestSelector_SecondaryPreferred_with_tags(t *testing.T) {
	t.Parallel()

	rpOpts := readpref.Options()

	tagSet, err := readpref.NewTagSet("a", "2")
	assert.NoError(t, err)

	rpOpts.SetTagSets([]readpref.TagSet{tagSet})

	subject, err := readpref.New(readpref.SecondaryPreferredMode, rpOpts)
	assert.NoError(t, err)

	result, err := (&ReadPref{ReadPref: subject}).SelectServer(readPrefTestTopology, readPrefTestTopology.Servers)

	require.NoError(t, err)
	require.Len(t, result, 1)
	require.Equal(t, []description.Server{readPrefTestSecondary2}, result)
}

func TestSelector_SecondaryPreferred_with_tags_that_do_not_match(t *testing.T) {
	t.Parallel()

	rpOpts := readpref.Options()

	tagSet, err := readpref.NewTagSet("a", "3")
	assert.NoError(t, err)

	rpOpts.SetTagSets([]readpref.TagSet{tagSet})

	subject, err := readpref.New(readpref.SecondaryPreferredMode, rpOpts)
	assert.NoError(t, err)

	result, err := (&ReadPref{ReadPref: subject}).SelectServer(readPrefTestTopology, readPrefTestTopology.Servers)

	require.NoError(t, err)
	require.Len(t, result, 1)
	require.Equal(t, []description.Server{readPrefTestPrimary}, result)
}

func TestSelector_SecondaryPreferred_with_tags_that_do_not_match_and_no_primary(t *testing.T) {
	t.Parallel()

	rpOpts := readpref.Options()

	tagSet, err := readpref.NewTagSet("a", "3")
	assert.NoError(t, err)

	rpOpts.SetTagSets([]readpref.TagSet{tagSet})

	subject, err := readpref.New(readpref.SecondaryPreferredMode, rpOpts)
	assert.NoError(t, err)

	result, err := (&ReadPref{ReadPref: subject}).SelectServer(readPrefTestTopology, []description.Server{readPrefTestSecondary1, readPrefTestSecondary2})

	require.NoError(t, err)
	require.Len(t, result, 0)
}

func TestSelector_SecondaryPreferred_with_no_secondaries(t *testing.T) {
	t.Parallel()

	subject, err := readpref.New(readpref.SecondaryPreferredMode, nil)
	assert.NoError(t, err)

	result, err := (&ReadPref{ReadPref: subject}).SelectServer(readPrefTestTopology, []description.Server{readPrefTestPrimary})

	require.NoError(t, err)
	require.Len(t, result, 1)
	require.Equal(t, []description.Server{readPrefTestPrimary}, result)
}

func TestSelector_SecondaryPreferred_with_no_secondaries_or_primary(t *testing.T) {
	t.Parallel()

	subject := &readpref.ReadPref{Mode: readpref.SecondaryPreferredMode}

	result, err := (&ReadPref{ReadPref: subject}).SelectServer(readPrefTestTopology, []description.Server{})

	require.NoError(t, err)
	require.Len(t, result, 0)
}

func TestSelector_SecondaryPreferred_with_maxStaleness(t *testing.T) {
	t.Parallel()

	rpOpts := readpref.Options()

	maxStaleness := 90 * time.Second
	rpOpts.SetMaxStaleness(maxStaleness)

	subject, err := readpref.New(readpref.SecondaryPreferredMode, rpOpts)
	assert.NoError(t, err)

	result, err := (&ReadPref{ReadPref: subject}).SelectServer(readPrefTestTopology, readPrefTestTopology.Servers)

	require.NoError(t, err)
	require.Len(t, result, 1)
	require.Equal(t, []description.Server{readPrefTestSecondary2}, result)
}

func TestSelector_SecondaryPreferred_with_maxStaleness_and_no_primary(t *testing.T) {
	t.Parallel()

	rpOpts := readpref.Options()

	maxStaleness := 90 * time.Second
	rpOpts.SetMaxStaleness(maxStaleness)

	subject, err := readpref.New(readpref.SecondaryPreferredMode, rpOpts)
	assert.NoError(t, err)

	result, err := (&ReadPref{ReadPref: subject}).SelectServer(readPrefTestTopology, []description.Server{readPrefTestSecondary1, readPrefTestSecondary2})

	require.NoError(t, err)
	require.Len(t, result, 1)
	require.Equal(t, []description.Server{readPrefTestSecondary2}, result)
}

func TestSelector_Secondary(t *testing.T) {
	t.Parallel()

	subject := &readpref.ReadPref{Mode: readpref.SecondaryPreferredMode}

	result, err := (&ReadPref{ReadPref: subject}).SelectServer(readPrefTestTopology, readPrefTestTopology.Servers)

	require.NoError(t, err)
	require.Len(t, result, 2)
	require.Equal(t, []description.Server{readPrefTestSecondary1, readPrefTestSecondary2}, result)
}

func TestSelector_Secondary_with_tags(t *testing.T) {
	t.Parallel()

	rpOpts := readpref.Options()

	maxStaleness := 90 * time.Second
	rpOpts.SetMaxStaleness(maxStaleness)

	subject, _ := readpref.New(readpref.SecondaryMode, rpOpts)

	result, err := (&ReadPref{ReadPref: subject}).SelectServer(readPrefTestTopology, readPrefTestTopology.Servers)

	require.NoError(t, err)
	require.Len(t, result, 1)
	require.Equal(t, []description.Server{readPrefTestSecondary2}, result)
}

func TestSelector_Secondary_with_empty_tag_set(t *testing.T) {
	t.Parallel()

	primaryNoTags := description.Server{
		Addr:        address.Address("localhost:27017"),
		Kind:        description.ServerKindRSPrimary,
		WireVersion: &description.VersionRange{Min: 6, Max: 21},
	}
	firstSecondaryNoTags := description.Server{
		Addr:        address.Address("localhost:27018"),
		Kind:        description.ServerKindRSSecondary,
		WireVersion: &description.VersionRange{Min: 6, Max: 21},
	}
	secondSecondaryNoTags := description.Server{
		Addr:        address.Address("localhost:27019"),
		Kind:        description.ServerKindRSSecondary,
		WireVersion: &description.VersionRange{Min: 6, Max: 21},
	}
	topologyNoTags := description.Topology{
		Kind:    description.TopologyKindReplicaSetWithPrimary,
		Servers: []description.Server{primaryNoTags, firstSecondaryNoTags, secondSecondaryNoTags},
	}

	nonMatchingSet := readpref.TagSet{
		{Name: "foo", Value: "bar"},
	}
	emptyTagSet := readpref.TagSet{}

	rpOpts := readpref.Options()
	rpOpts.SetTagSets([]readpref.TagSet{nonMatchingSet, emptyTagSet})

	rp, _ := readpref.New(readpref.SecondaryPreferredMode, rpOpts)

	result, err := (&ReadPref{ReadPref: rp}).SelectServer(topologyNoTags, topologyNoTags.Servers)
	assert.Nil(t, err, "SelectServer error: %v", err)
	expectedResult := []description.Server{firstSecondaryNoTags, secondSecondaryNoTags}
	assert.Equal(t, expectedResult, result, "expected result %v, got %v", expectedResult, result)
}

func TestSelector_Secondary_with_tags_that_do_not_match(t *testing.T) {
	t.Parallel()

	tagSet, err := readpref.NewTagSet("a", "3")
	assert.NoError(t, err)

	rpOpts := readpref.Options()
	rpOpts.SetTagSets([]readpref.TagSet{tagSet})

	subject, _ := readpref.New(readpref.SecondaryMode, rpOpts)

	result, err := (&ReadPref{ReadPref: subject}).SelectServer(readPrefTestTopology, readPrefTestTopology.Servers)

	require.NoError(t, err)
	require.Len(t, result, 0)
}

func TestSelector_Secondary_with_no_secondaries(t *testing.T) {
	t.Parallel()

	subject := &readpref.ReadPref{Mode: readpref.SecondaryMode}

	result, err := (&ReadPref{ReadPref: subject}).SelectServer(readPrefTestTopology, []description.Server{readPrefTestPrimary})

	require.NoError(t, err)
	require.Len(t, result, 0)
}

func TestSelector_Secondary_with_maxStaleness(t *testing.T) {
	t.Parallel()

	rpOpts := readpref.Options()

	maxStaleness := 90 * time.Second
	rpOpts.SetMaxStaleness(maxStaleness)

	subject, _ := readpref.New(readpref.SecondaryMode, rpOpts)

	result, err := (&ReadPref{ReadPref: subject}).SelectServer(readPrefTestTopology, readPrefTestTopology.Servers)

	require.NoError(t, err)
	require.Len(t, result, 1)
	require.Equal(t, []description.Server{readPrefTestSecondary2}, result)
}

func TestSelector_Secondary_with_maxStaleness_and_no_primary(t *testing.T) {
	t.Parallel()

	rpOpts := readpref.Options()

	maxStaleness := 90 * time.Second
	rpOpts.SetMaxStaleness(maxStaleness)

	subject, _ := readpref.New(readpref.SecondaryMode, rpOpts)

	result, err := (&ReadPref{ReadPref: subject}).SelectServer(readPrefTestTopology, []description.Server{readPrefTestSecondary1, readPrefTestSecondary2})

	require.NoError(t, err)
	require.Len(t, result, 1)
	require.Equal(t, []description.Server{readPrefTestSecondary2}, result)
}

func TestSelector_Nearest(t *testing.T) {
	t.Parallel()

	subject := &readpref.ReadPref{Mode: readpref.NearestMode}

	result, err := (&ReadPref{ReadPref: subject}).SelectServer(readPrefTestTopology, readPrefTestTopology.Servers)

	require.NoError(t, err)
	require.Len(t, result, 3)
	require.Equal(t, []description.Server{readPrefTestPrimary, readPrefTestSecondary1, readPrefTestSecondary2}, result)
}

func TestSelector_Nearest_with_tags(t *testing.T) {
	t.Parallel()

	rpOpts := readpref.Options()

	tagSet, err := readpref.NewTagSet("a", "1")
	assert.NoError(t, err)

	rpOpts.SetTagSets([]readpref.TagSet{tagSet})

	subject, _ := readpref.New(readpref.NearestMode, rpOpts)

	result, err := (&ReadPref{ReadPref: subject}).SelectServer(readPrefTestTopology, readPrefTestTopology.Servers)

	require.NoError(t, err)
	require.Len(t, result, 2)
	require.Equal(t, []description.Server{readPrefTestPrimary, readPrefTestSecondary1}, result)
}

func TestSelector_Nearest_with_tags_that_do_not_match(t *testing.T) {
	t.Parallel()

	rpOpts := readpref.Options()

	tagSet, err := readpref.NewTagSet("a", "3")
	assert.NoError(t, err)

	rpOpts.SetTagSets([]readpref.TagSet{tagSet})

	subject, _ := readpref.New(readpref.NearestMode, rpOpts)

	result, err := (&ReadPref{ReadPref: subject}).SelectServer(readPrefTestTopology, readPrefTestTopology.Servers)

	require.NoError(t, err)
	require.Len(t, result, 0)
}

func TestSelector_Nearest_with_no_primary(t *testing.T) {
	t.Parallel()

	subject := &readpref.ReadPref{Mode: readpref.NearestMode}

	result, err := (&ReadPref{ReadPref: subject}).SelectServer(readPrefTestTopology, []description.Server{readPrefTestSecondary1, readPrefTestSecondary2})

	require.NoError(t, err)
	require.Len(t, result, 2)
	require.Equal(t, []description.Server{readPrefTestSecondary1, readPrefTestSecondary2}, result)
}

func TestSelector_Nearest_with_no_secondaries(t *testing.T) {
	t.Parallel()

	subject := &readpref.ReadPref{Mode: readpref.NearestMode}

	result, err := (&ReadPref{ReadPref: subject}).SelectServer(readPrefTestTopology, []description.Server{readPrefTestPrimary})

	require.NoError(t, err)
	require.Len(t, result, 1)
	require.Equal(t, []description.Server{readPrefTestPrimary}, result)
}

func TestSelector_Nearest_with_maxStaleness(t *testing.T) {
	t.Parallel()

	rpOpts := readpref.Options()

	maxStaleness := 90 * time.Second
	rpOpts.SetMaxStaleness(maxStaleness)

	subject, _ := readpref.New(readpref.NearestMode, rpOpts)

	result, err := (&ReadPref{ReadPref: subject}).SelectServer(readPrefTestTopology, readPrefTestTopology.Servers)

	require.NoError(t, err)
	require.Len(t, result, 2)
	require.Equal(t, []description.Server{readPrefTestPrimary, readPrefTestSecondary2}, result)
}

func TestSelector_Nearest_with_maxStaleness_and_no_primary(t *testing.T) {
	t.Parallel()

	rpOpts := readpref.Options()

	maxStaleness := 90 * time.Second
	rpOpts.SetMaxStaleness(maxStaleness)

	subject, _ := readpref.New(readpref.NearestMode, rpOpts)

	result, err := (&ReadPref{ReadPref: subject}).SelectServer(readPrefTestTopology, []description.Server{readPrefTestSecondary1, readPrefTestSecondary2})

	require.NoError(t, err)
	require.Len(t, result, 1)
	require.Equal(t, []description.Server{readPrefTestSecondary2}, result)
}

func TestSelector_Max_staleness_is_less_than_90_seconds(t *testing.T) {
	t.Parallel()

	rpOpts := readpref.Options()

	maxStaleness := 50 * time.Second
	rpOpts.SetMaxStaleness(maxStaleness)

	subject, _ := readpref.New(readpref.NearestMode, rpOpts)

	s := description.Server{
		Addr:              address.Address("localhost:27017"),
		HeartbeatInterval: time.Duration(10) * time.Second,
		LastWriteTime:     time.Date(2017, 2, 11, 14, 0, 0, 0, time.UTC),
		LastUpdateTime:    time.Date(2017, 2, 11, 14, 0, 2, 0, time.UTC),
		Kind:              description.ServerKindRSPrimary,
		WireVersion:       &description.VersionRange{Min: 6, Max: 21},
	}
	c := description.Topology{
		Kind:    description.TopologyKindReplicaSetWithPrimary,
		Servers: []description.Server{s},
	}

	_, err := (&ReadPref{ReadPref: subject}).SelectServer(c, c.Servers)

	require.Error(t, err)
}

func TestSelector_Max_staleness_is_too_low(t *testing.T) {
	t.Parallel()

	rpOpts := readpref.Options()

	maxStaleness := 100 * time.Second
	rpOpts.SetMaxStaleness(maxStaleness)

	subject, _ := readpref.New(readpref.NearestMode, rpOpts)

	s := description.Server{
		Addr:              address.Address("localhost:27017"),
		HeartbeatInterval: time.Duration(100) * time.Second,
		LastWriteTime:     time.Date(2017, 2, 11, 14, 0, 0, 0, time.UTC),
		LastUpdateTime:    time.Date(2017, 2, 11, 14, 0, 2, 0, time.UTC),
		Kind:              description.ServerKindRSPrimary,
		WireVersion:       &description.VersionRange{Min: 6, Max: 21},
	}
	c := description.Topology{
		Kind:    description.TopologyKindReplicaSetWithPrimary,
		Servers: []description.Server{s},
	}

	_, err := (&ReadPref{ReadPref: subject}).SelectServer(c, c.Servers)

	require.Error(t, err)
}

func TestEqualServers(t *testing.T) {
	int64ToPtr := func(i64 int64) *int64 { return &i64 }

	t.Run("equals", func(t *testing.T) {
		defaultServer := description.Server{}
		// Only some of the Server fields affect equality
		testCases := []struct {
			name   string
			server description.Server
			equal  bool
		}{
			{"empty", description.Server{}, true},
			{"address", description.Server{Addr: address.Address("foo")}, true},
			{"arbiters", description.Server{Arbiters: []string{"foo"}}, false},
			{"rtt", description.Server{AverageRTT: time.Second}, true},
			{"compression", description.Server{Compression: []string{"foo"}}, true},
			{"canonicalAddr", description.Server{CanonicalAddr: address.Address("foo")}, false},
			{"electionID", description.Server{ElectionID: bson.NewObjectID()}, false},
			{"heartbeatInterval", description.Server{HeartbeatInterval: time.Second}, true},
			{"hosts", description.Server{Hosts: []string{"foo"}}, false},
			{"lastError", description.Server{LastError: errors.New("foo")}, false},
			{"lastUpdateTime", description.Server{LastUpdateTime: time.Now()}, true},
			{"lastWriteTime", description.Server{LastWriteTime: time.Now()}, true},
			{"maxBatchCount", description.Server{MaxBatchCount: 1}, true},
			{"maxDocumentSize", description.Server{MaxDocumentSize: 1}, true},
			{"maxMessageSize", description.Server{MaxMessageSize: 1}, true},
			{"members", description.Server{Members: []address.Address{address.Address("foo")}}, true},
			{"passives", description.Server{Passives: []string{"foo"}}, false},
			{"passive", description.Server{Passive: true}, true},
			{"primary", description.Server{Primary: address.Address("foo")}, false},
			{"readOnly", description.Server{ReadOnly: true}, true},
			{
				"sessionTimeoutMinutes",
				description.Server{
					SessionTimeoutMinutes: int64ToPtr(1),
				},
				false,
			},
			{"setName", description.Server{SetName: "foo"}, false},
			{"setVersion", description.Server{SetVersion: 1}, false},
			{"tags", description.Server{Tags: readpref.TagSet{readpref.Tag{"foo", "bar"}}}, false},
			{"topologyVersion", description.Server{TopologyVersion: &description.TopologyVersion{bson.NewObjectID(), 0}}, false},
			{"kind", description.Server{Kind: description.ServerKindStandalone}, false},
			{"wireVersion", description.Server{WireVersion: &description.VersionRange{1, 2}}, false},
		}
		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				actual := driverutil.EqualServers(defaultServer, tc.server)
				assert.Equal(t, actual, tc.equal, "expected %v, got %v", tc.equal, actual)
			})
		}
	})
}

func TestVersionRangeIncludes(t *testing.T) {
	t.Parallel()

	subject := driverutil.NewVersionRange(1, 3)

	tests := []struct {
		n        int32
		expected bool
	}{
		{0, false},
		{1, true},
		{2, true},
		{3, true},
		{4, false},
		{10, false},
	}

	for _, test := range tests {
		actual := driverutil.VersionRangeIncludes(subject, test.n)
		if actual != test.expected {
			t.Fatalf("expected %v to be %t", test.n, test.expected)
		}
	}
}
