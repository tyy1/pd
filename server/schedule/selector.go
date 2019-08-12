// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package schedule

import (
	"github.com/pingcap/kvproto/pkg/metapb"
	"math/rand"

	"github.com/pingcap/pd/server/core"
)

// BalanceSelector selects source/target from store candidates based on their
// resource scores.
type BalanceSelector struct {
	kind    core.ResourceKind
	filters []Filter
}

// NewBalanceSelector creates a BalanceSelector instance.
func NewBalanceSelector(kind core.ResourceKind, filters []Filter) *BalanceSelector {
	return &BalanceSelector{
		kind:    kind,
		filters: filters,
	}
}


// SourceStore is used to store label that user hope to raise or down.
type SourceLabel []*metapb.StoreLabel
var UserSource SourceLabel
var UserTarget SourceLabel

// SelectSource selects the store that can pass all filters and has the maximal
// resource score.
func (s *BalanceSelector) SelectSource(opt Options, stores []*core.StoreInfo, filters ...Filter) *core.StoreInfo {
	filters = append(filters, s.filters...)
	var result *core.StoreInfo

	// Test
	//tem := &metapb.StoreLabel{Key: "rack", Value: "1"}
	//UserSource = append(UserSource, tem)
	var result2 *core.StoreInfo
	for _, store := range stores {
		if FilterSource(opt, store, filters) {
			continue
		}
		if result == nil ||
			result.ResourceScore(s.kind, opt.GetHighSpaceRatio(), opt.GetLowSpaceRatio(), 0) <
				store.ResourceScore(s.kind, opt.GetHighSpaceRatio(), opt.GetLowSpaceRatio(), 0) {
			result = store
		}

		flag := selectSourceStore(store)
		if (result2 == nil && flag)||
			(result2.ResourceScore(s.kind, opt.GetHighSpaceRatio(), opt.GetLowSpaceRatio(), 0) <
				store.ResourceScore(s.kind, opt.GetHighSpaceRatio(), opt.GetLowSpaceRatio(), 0) && flag) {
			result2 = store
		}
		if result2 != nil {
			return result2
		}
	}
	return result
}


// If label of the store in UserSource and not in UserTarget return true.
func selectSourceStore(store *core.StoreInfo) bool {
	flag := false
	for _, label := range UserSource {
		if label.Value == store.GetLabelValue(label.Key) {
			flag = true
			break
		}
	}

	for _, label := range UserTarget {
		if label.Value == store.GetLabelValue(label.Key) {
			flag = false
			break
		}
	}

	return flag
}

// SelectTarget selects the store that can pass all filters and has the minimal
// resource score.
func (s *BalanceSelector) SelectTarget(opt Options, stores []*core.StoreInfo, filters ...Filter) *core.StoreInfo {
	filters = append(filters, s.filters...)
	var result *core.StoreInfo

	for _, store := range stores {
		if FilterTarget(opt, store, filters) {
			continue
		}
		if result == nil ||
			result.ResourceScore(s.kind, opt.GetHighSpaceRatio(), opt.GetLowSpaceRatio(), 0) >
				store.ResourceScore(s.kind, opt.GetHighSpaceRatio(), opt.GetLowSpaceRatio(), 0) {
			result = store
		}
	}
	return result
}

// ReplicaSelector selects source/target store candidates based on their
// distinct scores based on a region's peer stores.
type ReplicaSelector struct {
	regionStores []*core.StoreInfo
	labels       []string
	filters      []Filter
}

// NewReplicaSelector creates a ReplicaSelector instance.
func NewReplicaSelector(regionStores []*core.StoreInfo, labels []string, filters ...Filter) *ReplicaSelector {
	return &ReplicaSelector{
		regionStores: regionStores,
		labels:       labels,
		filters:      filters,
	}
}

// SelectSource selects the store that can pass all filters and has the minimal
// distinct score.
func (s *ReplicaSelector) SelectSource(opt Options, stores []*core.StoreInfo) *core.StoreInfo {
	var (
		best      *core.StoreInfo
		bestScore float64
	)
	for _, store := range stores {
		score := DistinctScore(s.labels, s.regionStores, store)
		if best == nil || compareStoreScore(opt, store, score, best, bestScore) < 0 {
			best, bestScore = store, score
		}
	}
	if best == nil || FilterSource(opt, best, s.filters) {
		return nil
	}
	return best
}

// TemRegion is used to store the region that will be scheduled.
var TemRegion uint64

// SelectTarget selects the store that can pass all filters and has the maximal
// distinct score.
func (s *ReplicaSelector) SelectTarget(opt Options, stores []*core.StoreInfo, filters ...Filter) *core.StoreInfo {
	var (
		best      *core.StoreInfo
		bestScore float64
	)

	// Initial value
	var (
		best2      *core.StoreInfo
		bestScore2 float64
	)
	for _, store := range stores {
		// If TargetStore of the region that will be scheduled is in UserScheRecords,
		// then continue.
		if label, ok := UserScheRecords[TemRegion]; ok {
			labelValue := store.GetLabelValue(label.Key)
			if labelValue != label.Value {
				continue
			}
		}
		if FilterTarget(opt, store, filters) {
			continue
		}
		score := DistinctScore(s.labels, s.regionStores, store)
		if best == nil || compareStoreScore(opt, store, score, best, bestScore) > 0 {
			best, bestScore = store, score
		}

		// Meanwhile satisfied in UserTarget and not in UserSource
		flag := selectSourceStore(store)
		if (best2 == nil || compareStoreScore(opt, store, score, best2, bestScore2) > 0) && !flag {
			best2, bestScore2 = store, score
		}
	}

	// If best2 is not nil, return best2, otherwise return best.
	if best2 != nil || FilterTarget(opt, best2, s.filters){
		return best2
	}
	if best == nil || FilterTarget(opt, best, s.filters) {
		return nil
	}
	return best
}

// RandomSelector selects source/target store randomly.
type RandomSelector struct {
	filters []Filter
}

// NewRandomSelector creates a RandomSelector instance.
func NewRandomSelector(filters []Filter) *RandomSelector {
	return &RandomSelector{filters: filters}
}

func (s *RandomSelector) randStore(stores []*core.StoreInfo) *core.StoreInfo {
	if len(stores) == 0 {
		return nil
	}
	return stores[rand.Int()%len(stores)]
}

// SelectSource randomly selects a source store from those can pass all filters.
func (s *RandomSelector) SelectSource(opt Options, stores []*core.StoreInfo) *core.StoreInfo {
	var candidates []*core.StoreInfo
	for _, store := range stores {
		if FilterSource(opt, store, s.filters) {
			continue
		}
		candidates = append(candidates, store)
	}
	return s.randStore(candidates)
}

// SelectTarget randomly selects a target store from those can pass all filters.
func (s *RandomSelector) SelectTarget(opt Options, stores []*core.StoreInfo, filters ...Filter) *core.StoreInfo {
	filters = append(filters, s.filters...)

	var candidates []*core.StoreInfo
	for _, store := range stores {
		if FilterTarget(opt, store, filters) {
			continue
		}
		candidates = append(candidates, store)
	}
	return s.randStore(candidates)
}
