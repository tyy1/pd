package schedulers
//tyy

import (
	"fmt"
	"github.com/pingcap/pd/server/core"
	"github.com/pingcap/pd/server/schedule"
	"github.com/pkg/errors"
	"strconv"
)

func init()  {
	schedule.RegisterScheduler("transfer-region-to-label", func(opController *schedule.OperatorController, args []string) (scheduler schedule.Scheduler, e error) {
		if len(args)!=3 {
			return nil, errors.New("transfer-region-to-label needs three argument")
		}
		regionID, err := strconv.ParseUint(args[0], 10, 64)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		return newTransferRegionToLabelScheduler(opController,regionID,args[1],args[2]),nil
	})
}

type transferRegionToLabelScheduler struct {
	*baseScheduler
	name string
	regionID uint64
	label_key string
	label_value string
	selector     *schedule.BalanceSelector
}

func newTransferRegionToLabelScheduler(opController *schedule.OperatorController,regionID uint64,label_key string,label_value string) schedule.Scheduler {
	base:=newBaseScheduler(opController)
	base.SetUserTrue()
	filters := []schedule.Filter{
		schedule.StoreStateFilter{MoveRegion: true},
	}
	return &transferRegionToLabelScheduler{
		baseScheduler:base,
		name:fmt.Sprintf("tranfer-region%d-to-label-{key:%v-value:%v}",regionID,label_key,label_value),
		regionID:regionID,
		label_key:label_key,
		label_value:label_value,
		selector:      schedule.NewBalanceSelector(core.RegionKind, filters),
	}
}

func (s *transferRegionToLabelScheduler)GetName() string {
	return s.name
}
func (s *transferRegionToLabelScheduler)GetType() string {
	return "transfer-region-to-label"
}
func (s *transferRegionToLabelScheduler)IsScheduleAllowed(cluster schedule.Cluster) bool {
	return s.opController.OperatorCount(schedule.OpRegion) < cluster.GetRegionScheduleLimit()
	//isAllowed=false减少一次调度
}
func (s *transferRegionToLabelScheduler)Schedule(cluster schedule.Cluster) []*schedule.Operator  {
	schedulerCounter.WithLabelValues(s.GetName(), "schedule").Inc()
	region:=cluster.GetRegion(s.regionID)
	if region==nil {
		schedulerCounter.WithLabelValues(s.GetName(),"no_region").Inc()
		return nil
	}
	var source_peerIds []uint64// source stores
	peers:=region.GetPeers()
	for  _,peer:=range peers{
		 if cluster.GetStore(peer.GetStoreId()).GetLabelValue(s.label_key)==s.label_value{//peer in label
			continue
		 }
		 source_peerIds=append(source_peerIds, peer.GetStoreId())
		 break
	}
	if len(source_peerIds)==0 {
		//log.Info("no peer  need  to transfer")
		return nil //the region need't to tranfer
	}
	//select best target store for the peer
	var storeIds_label []uint64//stores that label match given label and region's no peer in this store
	stores := cluster.GetStores()
	for _,store:=range stores{
		if (region.GetStorePeer(store.GetID())==nil)&&(store.GetLabelValue(s.label_key)==s.label_value){
			storeIds_label=append(storeIds_label,store.GetID()) //these stores can be selectes for target
		}
	}
	if len(storeIds_label)==0 {//no store can be choose
		return nil
	}
	target_store_score:=make(map[uint64]float64)
	for _,storeID:=range storeIds_label {
		target_store_score[storeID]=cluster.GetStore(storeID).RegionScore(cluster.GetHighSpaceRatio(), cluster.GetLowSpaceRatio(), 0)
	}
	var ops []*schedule.Operator
	for _,peer_storeID:=range source_peerIds  {
		if len(target_store_score)>0 {
			target_storeID:=select_min_score_region(target_store_score)
			newPeer,err:=cluster.AllocPeer(target_storeID)
			if err!=nil {
				schedulerCounter.WithLabelValues(s.GetName(), "alloc_newpeer_fail").Inc()
				return nil
			}
			op, err := schedule.CreateMovePeerOperator("transfer-region-peer", cluster, region, schedule.OpRegion, peer_storeID, newPeer.GetStoreId(), newPeer.GetId())
			if err != nil {
				schedulerCounter.WithLabelValues(s.GetName(), "create_operator_fail").Inc()
				return nil
			}
			ops=append(ops,op)
		}
	}
	return ops
}

func select_min_score_region(m map[uint64]float64)uint64{
	if len(m)==0 {
		return 0
	}
	var i=0
	var min_score float64
	var min_score_region uint64
	for k,v:=range m {
		if i==0 {
			min_score=v
			min_score_region=k
			i++
		}else{break;}
	}
	for k,v:=range m {
		if v<min_score {
			min_score=v
			min_score_region=k
		}
	}
	delete(m,min_score_region)
	return min_score_region
}