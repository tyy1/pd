package schedulers

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/pd/pkg/mock/mockcluster"
	"github.com/pingcap/pd/pkg/mock/mockoption"
	"github.com/pingcap/pd/server/schedule"
	"testing"
)

var _ = Suite(&testTransferRegionSuite{})

type testTransferRegionSuite struct{}

func Test(t *testing.T) {
	TestingT(t)
}

func (s *testTransferRegionSuite) TestTransferRegion(c *C){
	opt := mockoption.NewScheduleOptions()
	tc := mockcluster.NewCluster(opt)

	_, err := schedule.CreateScheduler("transfer-region", schedule.NewOperatorController(nil, nil))
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

	region1:=tc.GetRegion(1)
	c.Assert(region1.GetStorePeer(4),IsNil)
	sl, err := schedule.CreateScheduler("transfer-region", schedule.NewOperatorController(nil, nil),"1","4")
	c.Assert(err, IsNil)
	c.Assert(sl.Schedule(tc), NotNil)
}

var _ = Suite(&testTransferRegionToLabelSuite{})

type testTransferRegionToLabelSuite struct{}

func (s *testTransferRegionToLabelSuite)TestTransferRegionToLabel(c *C)  {
	opt := mockoption.NewScheduleOptions()
	tc := mockcluster.NewCluster(opt)

	_, err := schedule.CreateScheduler("transfer-region", schedule.NewOperatorController(nil, nil))
	//messageError=append(messageError,err.Error())
	c.Assert(err, NotNil)

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

}
