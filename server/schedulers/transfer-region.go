package schedulers

import (
	"fmt"
	"github.com/pingcap/log"
	"github.com/pingcap/pd/server/schedule"
	"github.com/pkg/errors"
	"strconv"
)

//tyy
func init()  {
	schedule.RegisterScheduler("transfer-region", func(opController *schedule.OperatorController, args []string) (scheduler schedule.Scheduler, e error) {
		if len(args) != 2 {
			return nil, errors.New("transfer-region needs 2 argument")
		}
		regionID, err := strconv.ParseUint(args[0], 10, 64)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		storeID, err := strconv.ParseUint(args[1], 10, 64)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		return newTransferRegionScheduler(opController,regionID,storeID),nil
	})
}

//transferRegionScheduler transfer a region's peers to other stores
type transferRegionScheduler struct {
	*baseScheduler
	name string
	regionID uint64
	storeID uint64
}

func newTransferRegionScheduler(opController *schedule.OperatorController,regionID uint64,storeID uint64) schedule.Scheduler{
	base:=newBaseScheduler(opController)
	return &transferRegionScheduler{
		baseScheduler:base,
		name:		  fmt.Sprintf("transfer-region%v-to-store%v-scheduler",regionID,storeID),
		regionID: 	  regionID,
		storeID:     storeID,
	}
}

func (s *transferRegionScheduler)GetName()  string{
	return s.name
}

func (s *transferRegionScheduler) GetType() string {
	return "transfer-region"
}

func (s *transferRegionScheduler) IsScheduleAllowed(cluster schedule.Cluster) bool {
	return s.opController.OperatorCount(schedule.OpRegion) < cluster.GetRegionScheduleLimit()
}

func (s *transferRegionScheduler) Schedule(cluster schedule.Cluster) []*schedule.Operator{
	schedulerCounter.WithLabelValues(s.GetName(), "schedule").Inc()
	region:=cluster.GetRegion(s.regionID)
	if region == nil {
		schedulerCounter.WithLabelValues(s.GetName(), "no_region").Inc()
		return nil
	}
	schedulerCounter.WithLabelValues(s.GetName(), "new_operator").Inc()
	if region.GetStorePeer(s.storeID)!=nil{
		return nil
	}
	newPeer,err:=cluster.AllocPeer(s.storeID)
	if err!=nil {
		schedulerCounter.WithLabelValues(s.GetName(), "alloc_newpeer_fail").Inc()
		return nil
	}
	oldStoreId:=region.GetPeers()[0].StoreId
	op, err := schedule.CreateMovePeerOperator("transfer-region-peer", cluster, region, schedule.OpRegion, oldStoreId, newPeer.GetStoreId(), newPeer.GetId())

	if err != nil {
		schedulerCounter.WithLabelValues(s.GetName(), "create_operator_fail").Inc()
		return nil
	}
	log.Info("create a new operator")
	log.Info(strconv.FormatUint(region.GetID(),10))
	log.Info(strconv.FormatUint(oldStoreId,10))
	log.Info(strconv.FormatUint(newPeer.GetStoreId(),10))
	return []*schedule.Operator{op}
	//CreateMovePeerOperator("transfer-region-peer", cluster, region, schedule.OpRegion, peer_storeID, newPeer.GetStoreId(), newPeer.GetId())
}