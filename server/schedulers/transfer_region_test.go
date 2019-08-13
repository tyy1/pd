package schedulers

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/pd/pkg/mock/mockcluster"
	"github.com/pingcap/pd/pkg/mock/mockoption"
	"github.com/pingcap/pd/server/schedule"
	"testing"
)
func Test(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testTransferRegionSuite{})

type testTransferRegionSuite struct{
	tc *mockcluster.Cluster
	lb schedule.Scheduler
	oc *schedule.OperatorController
}

func (s *testTransferRegionSuite) TestTransferRegion(c *C){
	opt := mockoption.NewScheduleOptions()
	tc := mockcluster.NewCluster(opt)

	s.oc=schedule.NewOperatorController(nil, nil)
	_, err := schedule.CreateScheduler("transfer-region", s.oc)
	//messageError=append(messageError,err.Error())
	c.Assert(err, NotNil)

	// Add stores 1,2,3,4
	tc.AddLeaderStore(1, 6)
	tc.AddLeaderStore(2, 7)
	tc.AddLeaderStore(3, 8)
	tc.AddLeaderStore(4, 9)

	// Add regions 1,2,3,4 with leaders in stores 1,2,3,4
	tc.AddLeaderRegion(1, 1)
	tc.AddLeaderRegion(2, 2)
	tc.AddLeaderRegion(3, 3)
	tc.AddLeaderRegion(4, 4)

    sl:=newTransferRegionScheduler(s.oc,1,4)
    c.Assert(sl,NotNil)
    ops:=sl.Schedule(tc)
    c.Assert(ops,NotNil)

    c.Assert(sl.GetName(),Equals,"transfer-region1-to-store4-scheduler")
    c.Assert(sl.GetType(),Equals,"transfer-region")
    isAllowed:=s.oc.OperatorCount(schedule.OpRegion)<tc.RegionScheduleLimit
    c.Assert(isAllowed,Equals,sl.IsScheduleAllowed(tc))

	sl=newTransferRegionScheduler(s.oc,1,1)
	c.Assert(sl,NotNil)
	ops=sl.Schedule(tc)
	c.Assert(ops,IsNil)

	sl=newTransferRegionScheduler(s.oc,10,1)
	c.Assert(sl,NotNil)
	ops=sl.Schedule(tc)
	c.Assert(ops,IsNil)
}

var _ = Suite(&testTransferRegionToLabelSuite{})

type testTransferRegionToLabelSuite struct{}

func (s *testTransferRegionToLabelSuite)TestTransferRegionToLabel(c *C)  {
	opt := mockoption.NewScheduleOptions()
	tc := mockcluster.NewCluster(opt)

	m:=make(map[uint64]float64)
	m[1]=0.1
	m[2]=0.2
	min:=select_min_score_region(m)
	c.Assert(min,Equals,1)
	delete(m,1)
	delete(m,2)
	c.Assert(0,Equals,select_min_score_region(m))

	// Add stores 1,2,3,4
	tc.AddLabelsStore(1, 4, map[string]string{"zone": "z1", "rack": "r1", "host": "h1"})
	tc.AddLabelsStore(2, 5, map[string]string{"zone": "z2", "rack": "r1", "host": "h1"})
	tc.AddLabelsStore(3, 6, map[string]string{"zone": "z3", "rack": "r1", "host": "h1"})
	tc.AddLabelsStore(4, 7, map[string]string{"zone": "z4", "rack": "r1", "host": "h1"})

	// Add regions 1,2,3,4 with leaders in stores 1,2,3,4
	tc.AddLeaderRegion(1, 1)
	tc.AddLeaderRegion(2, 2)
	tc.AddLeaderRegion(3, 3)
	tc.AddLeaderRegion(4, 4)

	sl, err := schedule.CreateScheduler("transfer-region-to-label", schedule.NewOperatorController(nil, nil),"1","zone","z2")
	c.Assert(err, IsNil)
	c.Assert(sl.Schedule(tc), NotNil)

	sl, err = schedule.CreateScheduler("transfer-region-to-label", schedule.NewOperatorController(nil, nil),"1","zone","z1")
	c.Assert(err, IsNil)
	c.Assert(sl.Schedule(tc), IsNil)

	oc:=schedule.NewOperatorController(nil, nil)
	sl=newTransferRegionToLabelScheduler(oc,1,"zone","z1")
	c.Assert(sl,NotNil)
	ops:=sl.Schedule(tc)
	c.Assert(ops,NotNil)

	c.Assert(sl.GetName(),Equals,"tranfer-region%d-to-label-{key:zone-value:z1}")
	c.Assert(sl.GetType(),Equals,"transfer-region-to-label")
	isAllowed:=oc.OperatorCount(schedule.OpRegion)<tc.RegionScheduleLimit
	c.Assert(isAllowed,Equals,sl.IsScheduleAllowed(tc))


}
