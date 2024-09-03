// Copyright (C) MongoDB, Inc. 2024-present.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

package serverselector

import (
	"fmt"
	"math"
	"time"

	"go.mongodb.org/mongo-driver/v2/mongo/readpref"
	"go.mongodb.org/mongo-driver/v2/x/mongo/driver/description"
)

// Composite combines multiple selectors into a single selector by applying them
// in order to the candidates list.
//
// For example, if the initial candidates list is [s0, s1, s2, s3] and two
// selectors are provided where the first matches s0 and s1 and the second
// matches s1 and s2, the following would occur during server selection:
//
// 1. firstSelector([s0, s1, s2, s3]) -> [s0, s1]
// 2. secondSelector([s0, s1]) -> [s1]
//
// The final list of candidates returned by the composite selector would be
// [s1].
type Composite struct {
	Selectors []description.ServerSelector
}

var _ description.ServerSelector = &Composite{}

// SelectServer combines multiple selectors into a single selector.
func (selector *Composite) SelectServer(
	topo description.Topology,
	candidates []description.Server,
) ([]description.Server, error) {
	var err error
	for _, sel := range selector.Selectors {
		candidates, err = sel.SelectServer(topo, candidates)
		if err != nil {
			return nil, err
		}
	}

	return candidates, nil
}

// Latency creates a ServerSelector which selects servers based on their average
// RTT values.
type Latency struct {
	Latency time.Duration
}

var _ description.ServerSelector = &Latency{}

// SelectServer selects servers based on average RTT.
func (selector *Latency) SelectServer(
	topo description.Topology,
	candidates []description.Server,
) ([]description.Server, error) {
	if selector.Latency < 0 {
		return candidates, nil
	}
	if topo.Kind == description.TopologyKindLoadBalanced {
		// In LoadBalanced mode, there should only be one server in the topology and
		// it must be selected.
		return candidates, nil
	}

	switch len(candidates) {
	case 0, 1:
		return candidates, nil
	default:
		min := time.Duration(math.MaxInt64)
		for _, candidate := range candidates {
			if candidate.AverageRTTSet {
				if candidate.AverageRTT < min {
					min = candidate.AverageRTT
				}
			}
		}

		if min == math.MaxInt64 {
			return candidates, nil
		}

		max := min + selector.Latency

		viableIndexes := make([]int, 0, len(candidates))
		for i, candidate := range candidates {
			if candidate.AverageRTTSet {
				if candidate.AverageRTT <= max {
					viableIndexes = append(viableIndexes, i)
				}
			}
		}
		if len(viableIndexes) == len(candidates) {
			return candidates, nil
		}
		result := make([]description.Server, len(viableIndexes))
		for i, idx := range viableIndexes {
			result[i] = candidates[idx]
		}
		return result, nil
	}
}

// ReadPref selects servers based on the provided read preference.
type ReadPref struct {
	ReadPref          *readpref.ReadPref
	IsOutputAggregate bool
}

var _ description.ServerSelector = &ReadPref{}

// SelectServer selects servers based on read preference.
func (selector *ReadPref) SelectServer(
	topo description.Topology,
	candidates []description.Server,
) ([]description.Server, error) {
	if topo.Kind == description.TopologyKindLoadBalanced {
		// In LoadBalanced mode, there should only be one server in the topology and
		// it must be selected. We check this before checking MaxStaleness support
		// because there's no monitoring in this mode, so the candidate server
		// wouldn't have a wire version set, which would result in an error.
		return candidates, nil
	}

	switch topo.Kind {
	case description.TopologyKindSingle:
		return candidates, nil
	case description.TopologyKindReplicaSetNoPrimary, description.TopologyKindReplicaSetWithPrimary:
		return selectForReplicaSet(selector.ReadPref, selector.IsOutputAggregate, topo, candidates)
	case description.TopologyKindSharded:
		return selectByKind(candidates, description.ServerKindMongos), nil
	}

	return nil, nil
}

// Write selects all the writable servers.
type Write struct{}

var _ description.ServerSelector = &Write{}

// SelectServer selects all writable servers.
func (selector *Write) SelectServer(
	topo description.Topology,
	candidates []description.Server,
) ([]description.Server, error) {
	switch topo.Kind {
	case description.TopologyKindSingle, description.TopologyKindLoadBalanced:
		return candidates, nil
	default:
		// Determine the capacity of the results slice.
		selected := 0
		for _, candidate := range candidates {
			switch candidate.Kind {
			case description.ServerKindMongos, description.ServerKindRSPrimary, description.ServerKindStandalone:
				selected++
			}
		}

		// Append candidates to the results slice.
		result := make([]description.Server, 0, selected)
		for _, candidate := range candidates {
			switch candidate.Kind {
			case description.ServerKindMongos, description.ServerKindRSPrimary, description.ServerKindStandalone:
				result = append(result, candidate)
			}
		}
		return result, nil
	}
}

// Func is a function that can be used as a ServerSelector.
type Func func(description.Topology, []description.Server) ([]description.Server, error)

// SelectServer implements the ServerSelector interface.
func (ssf Func) SelectServer(
	t description.Topology,
	s []description.Server,
) ([]description.Server, error) {
	return ssf(t, s)
}

func verifyMaxStaleness(rp *readpref.ReadPref, topo description.Topology) error {
	maxStaleness := rp.MaxStaleness()
	if maxStaleness == nil {
		return nil
	}

	if *maxStaleness < 90*time.Second {
		return fmt.Errorf("max staleness (%s) must be greater than or equal to 90s", maxStaleness)
	}

	if len(topo.Servers) < 1 {
		// Maybe we should return an error here instead?
		return nil
	}

	// we'll assume all candidates have the same heartbeat interval.
	s := topo.Servers[0]
	idleWritePeriod := 10 * time.Second

	if *maxStaleness < s.HeartbeatInterval+idleWritePeriod {
		return fmt.Errorf(
			"max staleness (%s) must be greater than or equal to the heartbeat interval (%s) plus idle write period (%s)",
			maxStaleness, s.HeartbeatInterval, idleWritePeriod,
		)
	}

	return nil
}

func selectByKind(candidates []description.Server, kind description.ServerKind) []description.Server {
	// Record the indices of viable candidates first and then append those to the returned slice
	// to avoid appending costly Server structs directly as an optimization.
	viableIndexes := make([]int, 0, len(candidates))
	for i, s := range candidates {
		if s.Kind == kind {
			viableIndexes = append(viableIndexes, i)
		}
	}
	if len(viableIndexes) == len(candidates) {
		return candidates
	}
	result := make([]description.Server, len(viableIndexes))
	for i, idx := range viableIndexes {
		result[i] = candidates[idx]
	}
	return result
}

func selectSecondaries(rp *readpref.ReadPref, candidates []description.Server) []description.Server {
	secondaries := selectByKind(candidates, description.ServerKindRSSecondary)
	if len(secondaries) == 0 {
		return secondaries
	}
	if maxStaleness := rp.MaxStaleness(); maxStaleness != nil {
		primaries := selectByKind(candidates, description.ServerKindRSPrimary)
		if len(primaries) == 0 {
			baseTime := secondaries[0].LastWriteTime
			for i := 1; i < len(secondaries); i++ {
				if secondaries[i].LastWriteTime.After(baseTime) {
					baseTime = secondaries[i].LastWriteTime
				}
			}

			var selected []description.Server
			for _, secondary := range secondaries {
				estimatedStaleness := baseTime.Sub(secondary.LastWriteTime) + secondary.HeartbeatInterval
				if estimatedStaleness <= *maxStaleness {
					selected = append(selected, secondary)
				}
			}

			return selected
		}

		primary := primaries[0]

		var selected []description.Server
		for _, secondary := range secondaries {
			estimatedStaleness := secondary.LastUpdateTime.Sub(secondary.LastWriteTime) -
				primary.LastUpdateTime.Sub(primary.LastWriteTime) + secondary.HeartbeatInterval
			if estimatedStaleness <= *maxStaleness {
				selected = append(selected, secondary)
			}
		}
		return selected
	}

	return secondaries
}

func selectByTagSet(candidates []description.Server, tagSets []readpref.TagSet) []description.Server {
	if len(tagSets) == 0 {
		return candidates
	}

	for _, ts := range tagSets {
		// If this tag set is empty, we can take a fast path because the empty list
		// is a subset of all tag sets, so all candidate servers will be selected.
		if len(ts) == 0 {
			return candidates
		}

		var results []description.Server
		for _, s := range candidates {
			// ts is non-empty, so only servers with a non-empty set of tags need to be checked.
			if len(s.Tags) > 0 && s.Tags.ContainsAll(ts) {
				results = append(results, s)
			}
		}

		if len(results) > 0 {
			return results
		}
	}

	return []description.Server{}
}

func selectForReplicaSet(
	rp *readpref.ReadPref,
	isOutputAggregate bool,
	topo description.Topology,
	candidates []description.Server,
) ([]description.Server, error) {
	if err := verifyMaxStaleness(rp, topo); err != nil {
		return nil, err
	}

	// If underlying operation is an aggregate with an output stage, only apply read preference
	// if all candidates are 5.0+. Otherwise, operate under primary read preference.
	if isOutputAggregate {
		for _, s := range candidates {
			if s.WireVersion.Max < 13 {
				return selectByKind(candidates, description.ServerKindRSPrimary), nil
			}
		}
	}

	switch rp.Mode {
	case readpref.PrimaryMode:
		return selectByKind(candidates, description.ServerKindRSPrimary), nil
	case readpref.PrimaryPreferredMode:
		selected := selectByKind(candidates, description.ServerKindRSPrimary)

		if len(selected) == 0 {
			selected = selectSecondaries(rp, candidates)
			return selectByTagSet(selected, rp.TagSets()), nil
		}

		return selected, nil
	case readpref.SecondaryPreferredMode:
		selected := selectSecondaries(rp, candidates)
		selected = selectByTagSet(selected, rp.TagSets())
		if len(selected) > 0 {
			return selected, nil
		}
		return selectByKind(candidates, description.ServerKindRSPrimary), nil
	case readpref.SecondaryMode:
		selected := selectSecondaries(rp, candidates)
		return selectByTagSet(selected, rp.TagSets()), nil
	case readpref.NearestMode:
		selected := selectByKind(candidates, description.ServerKindRSPrimary)
		selected = append(selected, selectSecondaries(rp, candidates)...)
		return selectByTagSet(selected, rp.TagSets()), nil
	}

	return nil, fmt.Errorf("unsupported mode: %d", rp.Mode)
}
