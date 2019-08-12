// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package server

import (
	"bytes"
	"strconv"
	"time"

	"github.com/pingcap/errcode"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"
	"github.com/pingcap/pd/server/core"
	"github.com/pingcap/pd/server/schedule"
	"github.com/pingcap/pd/server/statistics"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

var (
	// ErrNotBootstrapped is error info for cluster not bootstrapped
	ErrNotBootstrapped = errors.New("TiKV cluster not bootstrapped, please start TiKV first")
	// ErrOperatorNotFound is error info for operator not found
	ErrOperatorNotFound = errors.New("operator not found")
	// ErrAddOperator is error info for already have an operator when adding operator
	ErrAddOperator = errors.New("failed to add operator, maybe already have one")
	// ErrRegionNotAdjacent is error info for region not adjacent
	ErrRegionNotAdjacent = errors.New("two regions are not adjacent")
	// ErrRegionNotFound is error info for region not found
	ErrRegionNotFound = func(regionID uint64) error {
		return errors.Errorf("region %v not found", regionID)
	}
	//ErrStoreNotFound is error for store not found
	ErrStoreNotFound= func(storeID uint64) error {
		return errors.Errorf("store %v not found", storeID)
	}

	// ErrRegionAbnormalPeer is error info for region has abonormal peer
	ErrRegionAbnormalPeer = func(regionID uint64) error {
		return errors.Errorf("region %v has abnormal peer", regionID)
	}
	// ErrRegionIsStale is error info for region is stale
	ErrRegionIsStale = func(region *metapb.Region, origin *metapb.Region) error {
		return errors.Errorf("region is stale: region %v origin %v", region, origin)
	}
)

// Handler is a helper to export methods to handle API/RPC requests.
type Handler struct {
	s   *Server
	opt *scheduleOption
}

func newHandler(s *Server) *Handler {
	return &Handler{s: s, opt: s.scheduleOpt}
}

// GetRaftCluster returns RaftCluster.
func (h *Handler) GetRaftCluster() *RaftCluster {
	return h.s.GetRaftCluster()
}

// GetScheduleConfig returns ScheduleConfig.
func (h *Handler) GetScheduleConfig() *ScheduleConfig {
	return h.s.GetScheduleConfig()
}

func (h *Handler)  getCoordinator() (*coordinator, error) {
	cluster := h.s.GetRaftCluster()
	if cluster == nil {
		return nil, errors.WithStack(ErrNotBootstrapped)
	}
	cluster.RLock()
	defer cluster.RUnlock()
	return cluster.coordinator, nil
}

// GetSchedulers returns all names of schedulers.
func (h *Handler) GetSchedulers() ([]string, error) {
	c, err := h.getCoordinator()
	if err != nil {
		return nil, err
	}
	return c.getSchedulers(), nil
}

// GetStores returns all stores in the cluster.
func (h *Handler) GetStores() ([]*core.StoreInfo, error) {
	cluster := h.s.GetRaftCluster()
	if cluster == nil {
		return nil, errors.WithStack(ErrNotBootstrapped)
	}
	storeMetas := cluster.GetStores()
	stores := make([]*core.StoreInfo, 0, len(storeMetas))
	for _, s := range storeMetas {
		store, err := cluster.GetStore(s.GetId())
		if err != nil {
			return nil, err
		}
		stores = append(stores, store)
	}
	return stores, nil
}

// GetHotWriteRegions gets all hot write regions stats.
func (h *Handler) GetHotWriteRegions() *statistics.StoreHotRegionInfos {
	c, err := h.getCoordinator()
	if err != nil {
		return nil
	}
	return c.getHotWriteRegions()
}

// GetHotReadRegions gets all hot read regions stats.
func (h *Handler) GetHotReadRegions() *statistics.StoreHotRegionInfos {
	c, err := h.getCoordinator()
	if err != nil {
		return nil
	}
	return c.getHotReadRegions()
}

// GetHotBytesWriteStores gets all hot write stores stats.
func (h *Handler) GetHotBytesWriteStores() map[uint64]uint64 {
	cluster := h.s.GetRaftCluster()
	if cluster == nil {
		return nil
	}
	cluster.RLock()
	defer cluster.RUnlock()
	return cluster.cachedCluster.getStoresBytesWriteStat()
}

// GetHotBytesReadStores gets all hot write stores stats.
func (h *Handler) GetHotBytesReadStores() map[uint64]uint64 {
	cluster := h.s.GetRaftCluster()
	if cluster == nil {
		return nil
	}
	cluster.RLock()
	defer cluster.RUnlock()
	return cluster.cachedCluster.getStoresBytesReadStat()
}

// GetHotKeysWriteStores gets all hot write stores stats.
func (h *Handler) GetHotKeysWriteStores() map[uint64]uint64 {
	cluster := h.s.GetRaftCluster()
	if cluster == nil {
		return nil
	}
	cluster.RLock()
	defer cluster.RUnlock()
	return cluster.cachedCluster.getStoresKeysWriteStat()
}

// GetHotKeysReadStores gets all hot write stores stats.
func (h *Handler) GetHotKeysReadStores() map[uint64]uint64 {
	cluster := h.s.GetRaftCluster()
	if cluster == nil {
		return nil
	}
	cluster.RLock()
	defer cluster.RUnlock()
	return cluster.cachedCluster.getStoresKeysReadStat()
}

// AddScheduler adds a scheduler.
func (h *Handler) AddScheduler(name string, args ...string) error {
	c, err := h.getCoordinator()
	if err != nil {
		return err
	}
	s, err := schedule.CreateScheduler(name, c.opController, args...)
	if err != nil {
		return err
	}
	log.Info("create scheduler", zap.String("scheduler-name", s.GetName()))
	if err = c.addScheduler(s, args...); err != nil {
		log.Error("can not add scheduler", zap.String("scheduler-name", s.GetName()), zap.Error(err))
	} else if err = h.opt.persist(c.cluster.storage); err != nil {
		log.Error("can not persist scheduler config", zap.Error(err))
	}
	return err
}

// RemoveScheduler removes a scheduler by name.
func (h *Handler) RemoveScheduler(name string) error {
	c, err := h.getCoordinator()
	if err != nil {
		return err
	}
	if err = c.removeScheduler(name); err != nil {
		log.Error("can not remove scheduler", zap.String("scheduler-name", name), zap.Error(err))
	} else if err = h.opt.persist(c.cluster.storage); err != nil {
		log.Error("can not persist scheduler config", zap.Error(err))
	}
	return err
}

// AddBalanceLeaderScheduler adds a balance-leader-scheduler.
func (h *Handler) AddBalanceLeaderScheduler() error {
	return h.AddScheduler("balance-leader")
}

// AddBalanceRegionScheduler adds a balance-region-scheduler.
func (h *Handler) AddBalanceRegionScheduler() error {
	return h.AddScheduler("balance-region")
}

// AddBalanceHotRegionScheduler adds a balance-hot-region-scheduler.
func (h *Handler) AddBalanceHotRegionScheduler() error {
	return h.AddScheduler("hot-region")
}

// AddLabelScheduler adds a label-scheduler.
func (h *Handler) AddLabelScheduler() error {
	return h.AddScheduler("label")
}

// AddScatterRangeScheduler adds a balance-range-leader-scheduler
func (h *Handler) AddScatterRangeScheduler(args ...string) error {
	return h.AddScheduler("scatter-range", args...)
}

// AddAdjacentRegionScheduler adds a balance-adjacent-region-scheduler.
func (h *Handler) AddAdjacentRegionScheduler(args ...string) error {
	return h.AddScheduler("adjacent-region", args...)
}

// AddGrantLeaderScheduler adds a grant-leader-scheduler.
func (h *Handler) AddGrantLeaderScheduler(storeID uint64) error {
	return h.AddScheduler("grant-leader", strconv.FormatUint(storeID, 10))
}

// Add TransferRegionToStoreScheduler to coordinator by api.
func (h *Handler)AddTransferRegionToStoreScheduler(regionID uint64,storeID uint64)error{
	c, err := h.getCoordinator()
	if err != nil {
		return err
	}
	region:=c.cluster.GetRegion(regionID)
	if region==nil {
		return ErrRegionNotFound(regionID)
	}
	store:=c.cluster.GetStore(storeID)
	if store==nil {
		return ErrStoreNotFound(storeID)
	}
	regionIDstr:=strconv.FormatUint(regionID,10)
	storeIDstr:=strconv.FormatUint(storeID,10)
	return h.AddScheduler("transfer-region",regionIDstr,storeIDstr)
}


// AddEvictLeaderScheduler adds an evict-leader-scheduler.
func (h *Handler) AddEvictLeaderScheduler(storeID uint64) error {
	return h.AddScheduler("evict-leader", strconv.FormatUint(storeID, 10))
}

// AddShuffleLeaderScheduler adds a shuffle-leader-scheduler.
func (h *Handler) AddShuffleLeaderScheduler() error {
	return h.AddScheduler("shuffle-leader")
}

// AddShuffleRegionScheduler adds a shuffle-region-scheduler.
func (h *Handler) AddShuffleRegionScheduler() error {
	return h.AddScheduler("shuffle-region")
}

// AddShuffleHotRegionScheduler adds a shuffle-hot-region-scheduler.
func (h *Handler) AddShuffleHotRegionScheduler(limit uint64) error {
	return h.AddScheduler("shuffle-hot-region", strconv.FormatUint(limit, 10))
}

// AddTransferRegionsOfLabelToLabelScheduler add a transfer-regions-of-label-to-label-scheduler.
func (h *Handler)AddTransferRegionsOfLabelToLabelScheduler(from_lk string,from_lv string,lk string,lv string) error {
	c, err := h.getCoordinator()
	if err != nil {
		return err
	}
	var source_store []uint64
	allStores:=c.cluster.GetStores()
	for _,store:=range allStores {
		if store.GetLabelValue(from_lk)==from_lv {
			source_store=append(source_store,store.GetID())
		}
	}
	if len(source_store)==0{return errors.New("the source label not exist!")}
	var source_region_ids =make(map[uint64]struct{})
	for _,storeid:=range source_store{
		regions:=c.cluster.getStoreRegions(storeid)
		for _,region:=range regions{
			source_region_ids[region.GetID()]= struct{}{}
		}
	}
	if len(source_region_ids)==0 {
		return errors.New("the source label has no region!")
	}
	flag:=true
	for _,store:=range allStores {
		if store.GetLabelValue(lk)==lv {
			flag=false
			break
		}
	}
	if flag{return errors.New("the target label not found!")}
	for regionid,_:=range source_region_ids{
		var args []string
		args=append(args,strconv.FormatUint(regionid,10))
		args=append(args,lk)
		args=append(args,lv)
		return h.AddScheduler("transfer-region-to-label",args[0:]...)
	}
	return nil
}

// AddTransferRegionToLabelScheduler add a transfer-regions-of-keyrange-to-label-scheduler,
// transfer-region-to-label-scheduler, and transfer-region-to-label-scheduler.
func (h *Handler)AddTransferRegionToLabelScheduler(regionID uint64,lk string,lv string)  error{
	c, err := h.getCoordinator()
	if err != nil {
		return err
	}
	region:=c.cluster.GetRegion(regionID)
	if region==nil {
		return ErrRegionNotFound(regionID)
	}
	stores:=c.cluster.GetStores()
	for _,store:=range stores  {
		if store.GetLabelValue(lk)==lv {
			var args []string
			args=append(args,strconv.FormatUint(regionID,10))
			args=append(args,lk)
			args=append(args,lv)
			return h.AddScheduler("transfer-region-to-label",args[0:]...)
		}
	}
	return errors.Errorf("label not found")

}
// AddRandomMergeScheduler adds a random-merge-scheduler.
func (h *Handler) AddRandomMergeScheduler() error {
	return h.AddScheduler("random-merge")
}

// GetOperator returns the region operator.
func (h *Handler) GetOperator(regionID uint64) (*schedule.Operator, error) {
	c, err := h.getCoordinator()
	if err != nil {
		return nil, err
	}

	op := c.opController.GetOperator(regionID)
	if op == nil {
		return nil, ErrOperatorNotFound
	}

	return op, nil
}

// GetOperatorStatus returns the status of the region operator.
func (h *Handler) GetOperatorStatus(regionID uint64) (*schedule.OperatorWithStatus, error) {
	c, err := h.getCoordinator()
	if err != nil {
		return nil, err
	}

	op := c.opController.GetOperatorStatus(regionID)
	if op == nil {
		return nil, ErrOperatorNotFound
	}

	return op, nil
}

// RemoveOperator removes the region operator.
func (h *Handler) RemoveOperator(regionID uint64) error {
	c, err := h.getCoordinator()
	if err != nil {
		return err
	}

	op := c.opController.GetOperator(regionID)
	if op == nil {
		return ErrOperatorNotFound
	}

	c.opController.RemoveOperator(op)
	return nil
}

// GetOperators returns the running operators.
func (h *Handler) GetOperators() ([]*schedule.Operator, error) {
	c, err := h.getCoordinator()
	if err != nil {
		return nil, err
	}
	return c.opController.GetOperators(), nil
}

// GetWaitingOperators returns the waiting operators.
func (h *Handler) GetWaitingOperators() ([]*schedule.Operator, error) {
	c, err := h.getCoordinator()
	if err != nil {
		return nil, err
	}
	return c.opController.GetWaitingOperators(), nil
}

// GetAdminOperators returns the running admin operators.
func (h *Handler) GetAdminOperators() ([]*schedule.Operator, error) {
	return h.GetOperatorsOfKind(schedule.OpAdmin)
}

// GetLeaderOperators returns the running leader operators.
func (h *Handler) GetLeaderOperators() ([]*schedule.Operator, error) {
	return h.GetOperatorsOfKind(schedule.OpLeader)
}

// GetRegionOperators returns the running region operators.
func (h *Handler) GetRegionOperators() ([]*schedule.Operator, error) {
	return h.GetOperatorsOfKind(schedule.OpRegion)
}

// GetOperatorsOfKind returns the running operators of the kind.
func (h *Handler) GetOperatorsOfKind(mask schedule.OperatorKind) ([]*schedule.Operator, error) {
	ops, err := h.GetOperators()
	if err != nil {
		return nil, err
	}
	var results []*schedule.Operator
	for _, op := range ops {
		if op.Kind()&mask != 0 {
			results = append(results, op)
		}
	}
	return results, nil
}

// GetHistory returns finished operators' history since start.
func (h *Handler) GetHistory(start time.Time) ([]schedule.OperatorHistory, error) {
	c, err := h.getCoordinator()
	if err != nil {
		return nil, err
	}
	return c.opController.GetHistory(start), nil
}

// SetAllStoresLimit is used to set limit of all stores.
func (h *Handler) SetAllStoresLimit(rate float64) error {
	c, err := h.getCoordinator()
	if err != nil {
		return err
	}
	c.opController.SetAllStoresLimit(rate)
	return nil
}

// GetAllStoresLimit is used to get limit of all stores.
func (h *Handler) GetAllStoresLimit() (map[uint64]float64, error) {
	c, err := h.getCoordinator()
	if err != nil {
		return nil, err
	}
	return c.opController.GetAllStoresLimit(), nil
}

// SetStoreLimit is used to set the limit of a store.
func (h *Handler) SetStoreLimit(storeID uint64, rate float64) error {
	c, err := h.getCoordinator()
	if err != nil {
		return err
	}
	c.opController.SetStoreLimit(storeID, rate)
	return nil
}

// AddTransferLeaderOperator adds an operator to transfer leader to the store.
func (h *Handler) AddTransferLeaderOperator(regionID uint64, storeID uint64) error {
	c, err := h.getCoordinator()
	if err != nil {
		return err
	}

	region := c.cluster.GetRegion(regionID)
	if region == nil {
		return ErrRegionNotFound(regionID)
	}

	newLeader := region.GetStoreVoter(storeID)
	if newLeader == nil {
		return errors.Errorf("region has no voter in store %v", storeID)
	}
	op := schedule.CreateTransferLeaderOperator("admin-transfer-leader", region, region.GetLeader().GetStoreId(), newLeader.GetStoreId(), schedule.OpAdmin)
	if ok := c.opController.AddOperator(op); !ok {
		return errors.WithStack(ErrAddOperator)
	}
	return nil
}

// AddTransferRegionOperator adds an operator to transfer region to the stores.
func (h *Handler) AddTransferRegionOperator(regionID uint64, storeIDs map[uint64]struct{}) error {
	c, err := h.getCoordinator()
	if err != nil {
		return err
	}

	region := c.cluster.GetRegion(regionID)
	if region == nil {
		return ErrRegionNotFound(regionID)
	}

	if len(storeIDs) > c.cluster.GetMaxReplicas() {
		return errors.Errorf("the number of stores is %v, beyond the max replicas", len(storeIDs))
	}

	for id := range storeIDs {
		store := c.cluster.GetStore(id)
		if store == nil {
			return core.NewStoreNotFoundErr(id)
		}
		if store.IsTombstone() {
			return errcode.Op("operator.add").AddTo(core.StoreTombstonedErr{StoreID: id})
		}
	}

	op, err := schedule.CreateMoveRegionOperator("admin-move-region", c.cluster, region, schedule.OpAdmin, storeIDs)
	if err != nil {
		return err
	}
	if ok := c.opController.AddOperator(op); !ok {
		return errors.WithStack(ErrAddOperator)
	}
	return nil
}

// AddTransferPeerOperator adds an operator to transfer peer.
func (h *Handler) AddTransferPeerOperator(regionID uint64, fromStoreID, toStoreID uint64) error {
	c, err := h.getCoordinator()
	if err != nil {
		return err
	}

	region := c.cluster.GetRegion(regionID)
	if region == nil {
		return ErrRegionNotFound(regionID)
	}

	oldPeer := region.GetStorePeer(fromStoreID)
	if oldPeer == nil {
		return errors.Errorf("region has no peer in store %v", fromStoreID)
	}

	toStore := c.cluster.GetStore(toStoreID)
	if toStore == nil {
		return core.NewStoreNotFoundErr(toStoreID)
	}
	if toStore.IsTombstone() {
		return errcode.Op("operator.add").AddTo(core.StoreTombstonedErr{StoreID: toStoreID})
	}

	newPeer, err := c.cluster.AllocPeer(toStoreID)
	if err != nil {
		return err
	}

	op, err := schedule.CreateMovePeerOperator("admin-move-peer", c.cluster, region, schedule.OpAdmin, fromStoreID, toStoreID, newPeer.GetId())
	if err != nil {
		return err
	}
	if ok := c.opController.AddOperator(op); !ok {
		return errors.WithStack(ErrAddOperator)
	}
	return nil
}

// checkAdminAddPeerOperator checks adminAddPeer operator with given region ID and store ID.
func (h *Handler) checkAdminAddPeerOperator(regionID uint64, toStoreID uint64) (*coordinator, *core.RegionInfo, error) {
	c, err := h.getCoordinator()
	if err != nil {
		return nil, nil, err
	}

	region := c.cluster.GetRegion(regionID)
	if region == nil {
		return nil, nil, ErrRegionNotFound(regionID)
	}

	if region.GetStorePeer(toStoreID) != nil {
		return nil, nil, errors.Errorf("region already has peer in store %v", toStoreID)
	}

	toStore := c.cluster.GetStore(toStoreID)
	if toStore == nil {
		return nil, nil, core.NewStoreNotFoundErr(toStoreID)
	}
	if toStore.IsTombstone() {
		return nil, nil, errcode.Op("operator.add").AddTo(core.StoreTombstonedErr{StoreID: toStoreID})
	}

	return c, region, nil
}

// AddAddPeerOperator adds an operator to add peer.
func (h *Handler) AddAddPeerOperator(regionID uint64, toStoreID uint64) error {
	c, region, err := h.checkAdminAddPeerOperator(regionID, toStoreID)
	if err != nil {
		return err
	}

	newPeer, err := c.cluster.AllocPeer(toStoreID)
	if err != nil {
		return err
	}

	op := schedule.CreateAddPeerOperator("admin-add-peer", c.cluster, region, newPeer.GetId(), toStoreID, schedule.OpAdmin)
	if ok := c.opController.AddOperator(op); !ok {
		return errors.WithStack(ErrAddOperator)
	}
	return nil
}

// AddAddLearnerOperator adds an operator to add learner.
func (h *Handler) AddAddLearnerOperator(regionID uint64, toStoreID uint64) error {
	c, region, err := h.checkAdminAddPeerOperator(regionID, toStoreID)
	if err != nil {
		return err
	}

	if !c.cluster.IsRaftLearnerEnabled() {
		return ErrOperatorNotFound
	}

	newPeer, err := c.cluster.AllocPeer(toStoreID)
	if err != nil {
		return err
	}

	op := schedule.CreateAddLearnerOperator("admin-add-learner", c.cluster, region, newPeer.GetId(), toStoreID, schedule.OpAdmin)
	if ok := c.opController.AddOperator(op); !ok {
		return errors.WithStack(ErrAddOperator)
	}
	return nil
}

// AddRemovePeerOperator adds an operator to remove peer.
func (h *Handler) AddRemovePeerOperator(regionID uint64, fromStoreID uint64) error {
	c, err := h.getCoordinator()
	if err != nil {
		return err
	}

	region := c.cluster.GetRegion(regionID)
	if region == nil {
		return ErrRegionNotFound(regionID)
	}

	if region.GetStorePeer(fromStoreID) == nil {
		return errors.Errorf("region has no peer in store %v", fromStoreID)
	}

	op, err := schedule.CreateRemovePeerOperator("admin-remove-peer", c.cluster, schedule.OpAdmin, region, fromStoreID)
	if err != nil {
		return err
	}
	if ok := c.opController.AddOperator(op); !ok {
		return errors.WithStack(ErrAddOperator)
	}
	return nil
}

// AddMergeRegionOperator adds an operator to merge region.
func (h *Handler) AddMergeRegionOperator(regionID uint64, targetID uint64) error {
	c, err := h.getCoordinator()
	if err != nil {
		return err
	}

	region := c.cluster.GetRegion(regionID)
	if region == nil {
		return ErrRegionNotFound(regionID)
	}

	target := c.cluster.GetRegion(targetID)
	if target == nil {
		return ErrRegionNotFound(targetID)
	}

	if len(region.GetDownPeers()) > 0 || len(region.GetPendingPeers()) > 0 || len(region.GetLearners()) > 0 ||
		len(region.GetPeers()) != c.cluster.GetMaxReplicas() {
		return ErrRegionAbnormalPeer(regionID)
	}

	if len(target.GetDownPeers()) > 0 || len(target.GetPendingPeers()) > 0 || len(target.GetLearners()) > 0 ||
		len(target.GetMeta().GetPeers()) != c.cluster.GetMaxReplicas() {
		return ErrRegionAbnormalPeer(targetID)
	}

	// for the case first region (start key is nil) with the last region (end key is nil) but not adjacent
	if (bytes.Equal(region.GetStartKey(), target.GetEndKey()) || len(region.GetStartKey()) == 0) &&
		(bytes.Equal(region.GetEndKey(), target.GetStartKey()) || len(region.GetEndKey()) == 0) {
		return ErrRegionNotAdjacent
	}

	ops, err := schedule.CreateMergeRegionOperator("admin-merge-region", c.cluster, region, target, schedule.OpAdmin)
	if err != nil {
		return err
	}
	if ok := c.opController.AddOperator(ops...); !ok {
		return errors.WithStack(ErrAddOperator)
	}
	return nil
}

// AddSplitRegionOperator adds an operator to split a region.
func (h *Handler) AddSplitRegionOperator(regionID uint64, policy string) error {
	c, err := h.getCoordinator()
	if err != nil {
		return err
	}

	region := c.cluster.GetRegion(regionID)
	if region == nil {
		return ErrRegionNotFound(regionID)
	}

	op := schedule.CreateSplitRegionOperator("admin-split-region", region, schedule.OpAdmin, policy)
	if ok := c.opController.AddOperator(op); !ok {
		return errors.WithStack(ErrAddOperator)
	}
	return nil
}

// AddScatterRegionOperator adds an operator to scatter a region.
func (h *Handler) AddScatterRegionOperator(regionID uint64) error {
	c, err := h.getCoordinator()
	if err != nil {
		return err
	}

	region := c.cluster.GetRegion(regionID)
	if region == nil {
		return ErrRegionNotFound(regionID)
	}

	op, err := c.regionScatterer.Scatter(region)
	if err != nil {
		return err
	}

	if op == nil {
		return nil
	}
	if ok := c.opController.AddOperator(op); !ok {
		return errors.WithStack(ErrAddOperator)
	}
	return nil
}

// GetDownPeerRegions gets the region with down peer.
func (h *Handler) GetDownPeerRegions() ([]*core.RegionInfo, error) {
	c := h.s.GetRaftCluster()
	if c == nil {
		return nil, ErrNotBootstrapped
	}
	c.RLock()
	defer c.RUnlock()
	return c.cachedCluster.GetRegionStatsByType(statistics.DownPeer), nil
}

// GetExtraPeerRegions gets the region exceeds the specified number of peers.
func (h *Handler) GetExtraPeerRegions() ([]*core.RegionInfo, error) {
	c := h.s.GetRaftCluster()
	if c == nil {
		return nil, ErrNotBootstrapped
	}
	c.RLock()
	defer c.RUnlock()
	return c.cachedCluster.GetRegionStatsByType(statistics.ExtraPeer), nil
}

// GetMissPeerRegions gets the region less than the specified number of peers.
func (h *Handler) GetMissPeerRegions() ([]*core.RegionInfo, error) {
	c := h.s.GetRaftCluster()
	if c == nil {
		return nil, ErrNotBootstrapped
	}
	c.RLock()
	defer c.RUnlock()
	return c.cachedCluster.GetRegionStatsByType(statistics.MissPeer), nil
}

// GetPendingPeerRegions gets the region with pending peer.
func (h *Handler) GetPendingPeerRegions() ([]*core.RegionInfo, error) {
	c := h.s.GetRaftCluster()
	if c == nil {
		return nil, ErrNotBootstrapped
	}
	c.RLock()
	defer c.RUnlock()
	return c.cachedCluster.GetRegionStatsByType(statistics.PendingPeer), nil
}

// GetIncorrectNamespaceRegions gets the region with incorrect namespace peer.
func (h *Handler) GetIncorrectNamespaceRegions() ([]*core.RegionInfo, error) {
	c := h.s.GetRaftCluster()
	if c == nil {
		return nil, ErrNotBootstrapped
	}
	c.RLock()
	defer c.RUnlock()
	return c.cachedCluster.GetRegionStatsByType(statistics.IncorrectNamespace), nil
}
