// Copyright 2019 PingCAP, Inc.
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

package scheduler_test

import (
	"encoding/json"
	"testing"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/pd/server"
	"github.com/pingcap/pd/tests"
	"github.com/pingcap/pd/tests/pdctl"
)

func Test(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&schedulerTestSuite{})

type schedulerTestSuite struct{}

func (s *schedulerTestSuite) SetUpSuite(c *C) {
	server.EnableZap = true
}

func (s *schedulerTestSuite) TestScheduler(c *C) {
	c.Parallel()

	cluster, err := tests.NewTestCluster(1)
	c.Assert(err, IsNil)
	err = cluster.RunInitialServers()
	c.Assert(err, IsNil)
	cluster.WaitLeader()
	pdAddr := cluster.GetConfig().GetClientURLs()
	cmd := pdctl.InitCommand()

	label1:=metapb.StoreLabel{Key:"zone",Value:"s1"}
	label2:=metapb.StoreLabel{Key:"zone",Value:"s2"}
	label3:=metapb.StoreLabel{Key:"zone",Value:"s3"}
	label4:=metapb.StoreLabel{Key:"zone",Value:"s4"}
	stores := []*metapb.Store{
		{
			Id:    1,
			State: metapb.StoreState_Up,
			Labels:[]*metapb.StoreLabel{&label1},

		},
		{
			Id:    2,
			State: metapb.StoreState_Up,
			Labels:[]*metapb.StoreLabel{&label2},
		},
		{
			Id:    3,
			State: metapb.StoreState_Up,
			Labels:[]*metapb.StoreLabel{&label3},
		},
		{
			Id:    4,
			State: metapb.StoreState_Up,
			Labels:[]*metapb.StoreLabel{&label4},
		},
	}

	leaderServer := cluster.GetServer(cluster.GetLeader())
	c.Assert(leaderServer.BootstrapCluster(), IsNil)
	for _, store := range stores {
		pdctl.MustPutStore(c, leaderServer.GetServer(), store.Id, store.State, store.Labels)
	}
	pdctl.MustPutRegion(c, cluster, 1, 1, []byte("a"), []byte("b"))
	pdctl.MustPutRegion(c, cluster, 2, 2, []byte("c"),[]byte("d"))
	pdctl.MustPutRegion(c, cluster, 3, 3, []byte("e"), []byte("f"))
	pdctl.MustPutRegion(c, cluster, 4, 4, []byte("g"), []byte("h"))

	defer cluster.Destroy()

	time.Sleep(3 * time.Second)
	// scheduler show command
	args := []string{"-u", pdAddr, "scheduler", "show"}
	_, output, err := pdctl.ExecuteCommandC(cmd, args...)
	c.Assert(err, IsNil)
	var schedulers []string
	c.Assert(json.Unmarshal(output, &schedulers), IsNil)
	expected := map[string]bool{
		"balance-region-scheduler":     true,
		"balance-leader-scheduler":     true,
		"balance-hot-region-scheduler": true,
		"label-scheduler":              true,
	}
	for _, scheduler := range schedulers {
		c.Assert(expected[scheduler], Equals, true)
	}

	// scheduler add command
	args = []string{"-u", pdAddr, "scheduler", "add", "grant-leader-scheduler", "1"}
	_, _, err = pdctl.ExecuteCommandC(cmd, args...)
	c.Assert(err, IsNil)
	args = []string{"-u", pdAddr, "scheduler", "show"}
	_, output, err = pdctl.ExecuteCommandC(cmd, args...)
	c.Assert(err, IsNil)
	schedulers = schedulers[:0]
	c.Assert(json.Unmarshal(output, &schedulers), IsNil)
	expected = map[string]bool{
		"balance-region-scheduler":     true,
		"balance-leader-scheduler":     true,
		"balance-hot-region-scheduler": true,
		"label-scheduler":              true,
		"grant-leader-scheduler-1":     true,
	}
	for _, scheduler := range schedulers {
		c.Assert(expected[scheduler], Equals, true)
	}

	// scheduelr t_add command:transfer-region-to-store-scheduler
	args=[]string{"-u", pdAddr, "scheduler", "t_add","transfer-region-to-store-scheduler","1","2"}
	_, _, err = pdctl.ExecuteCommandC(cmd, args...)
	c.Assert(err, IsNil)
	args=[]string{"-u",pdAddr,"scheduler","remove","transfer-region1-to-store2-scheduler"}
	_, _, err = pdctl.ExecuteCommandC(cmd, args...)
	c.Assert(err, IsNil)

	// scheduler delete command
	args = []string{"-u", pdAddr, "scheduler", "remove", "balance-region-scheduler"}
	_, _, err = pdctl.ExecuteCommandC(cmd, args...)
	c.Assert(err, IsNil)
	args = []string{"-u", pdAddr, "scheduler", "show"}
	_, output, err = pdctl.ExecuteCommandC(cmd, args...)
	c.Assert(err, IsNil)
	schedulers = schedulers[:0]
	c.Assert(json.Unmarshal(output, &schedulers), IsNil)
	expected = map[string]bool{
		"balance-leader-scheduler":     true,
		"balance-hot-region-scheduler": true,
		"label-scheduler":              true,
		"grant-leader-scheduler-1":     true,
	}
	for _, scheduler := range schedulers {
		c.Assert(expected[scheduler], Equals, true)
	}

	// scheduler t_add command
	args=[]string{"-u", pdAddr, "operator", "add","transfer-region","2","1"}
	_, _, err = pdctl.ExecuteCommandC(cmd, args...)
	c.Assert(err, IsNil)

	args=[]string{"-u", pdAddr, "scheduler", "t_add","transfer-region-to-label-scheduler","3","zone","s1"}
	_, _, err = pdctl.ExecuteCommandC(cmd, args...)
	c.Assert(err, IsNil)
	args=[]string{"-u", pdAddr, "scheduler", "t_add","transfer-region-to-label-scheduler","qw3","zone","s1"}
	_, _, err = pdctl.ExecuteCommandC(cmd, args...)
	c.Assert(err, IsNil)
	args=[]string{"-u", pdAddr, "scheduler", "t_add","transfer-region-to-label-scheduler","3","","s1"}
	_, _, err = pdctl.ExecuteCommandC(cmd, args...)
	c.Assert(err, IsNil)
	args=[]string{"-u", pdAddr, "scheduler", "t_add","transfer-region-to-label-scheduler","3","s1"}
	_, _, err = pdctl.ExecuteCommandC(cmd, args...)
	c.Assert(err, IsNil)

	args=[]string{"-u", pdAddr, "scheduler", "t_add","transfer-regions-of-keyrange-to-label-scheduler","a","2","zone","s4"}
	_, output, err = pdctl.ExecuteCommandC(cmd, args...)
	c.Assert(err, IsNil)
	args=[]string{"-u", pdAddr, "scheduler", "t_add","transfer-regions-of-keyrange-to-label-scheduler","a","ax","zone","s4"}
	_, output, err = pdctl.ExecuteCommandC(cmd, args...)
	c.Assert(err, IsNil)
	args=[]string{"-u", pdAddr, "scheduler", "t_add","transfer-regions-of-keyrange-to-label-scheduler","a","zone","s4"}
	_, output, err = pdctl.ExecuteCommandC(cmd, args...)
	c.Assert(err, IsNil)
	args=[]string{"-u", pdAddr, "scheduler", "t_add","transfer-regions-of-keyrange-to-label-scheduler","sd","2","zone","s4"}
	_, output, err = pdctl.ExecuteCommandC(cmd, args...)
	c.Assert(err, IsNil)
	args=[]string{"-u", pdAddr, "scheduler", "t_add","transfer-regions-of-keyrange-to-label-scheduler","a","2","zone","ss"}
	_, output, err = pdctl.ExecuteCommandC(cmd, args...)
	c.Assert(err, IsNil)
	args=[]string{"-u", pdAddr, "scheduler", "t_add","transfer-regions-of-keyrange-to-label-scheduler","a","2","zone","s4","-format=hex"}
	_, output, err = pdctl.ExecuteCommandC(cmd, args...)
	c.Assert(err, IsNil)
	args=[]string{"-u", pdAddr, "scheduler", "t_add","transfer-regions-of-keyrange-to-label-scheduler","a","2","zone","s4","-format=hex1"}
	_, output, err = pdctl.ExecuteCommandC(cmd, args...)
	c.Assert(err, IsNil)
	args=[]string{"-u", pdAddr, "scheduler", "t_add","transfer-regions-of-keyrange-to-label-scheduler","a","2","zone","s4","-format=row"}
	_, output, err = pdctl.ExecuteCommandC(cmd, args...)
	c.Assert(err, IsNil)
	args=[]string{"-u", pdAddr, "scheduler", "t_add","transfer-regions-of-keyrange-to-label-scheduler","a","2","zone","s4","-format=encode"}
	_, output, err = pdctl.ExecuteCommandC(cmd, args...)
	c.Assert(err, IsNil)

	args=[]string{"-u", pdAddr, "scheduler", "t_add","transfer-table-to-label-scheduler","mysql","user","zone","s1"}
	_, _, err = pdctl.ExecuteCommandC(cmd, args...)
	c.Assert(err, IsNil)
	args=[]string{"-u", pdAddr, "scheduler", "t_add","transfer-table-to-label-scheduler","mysql","user","zone"}
	_, _, err = pdctl.ExecuteCommandC(cmd, args...)
	c.Assert(err, IsNil)
	args=[]string{"-u", pdAddr, "scheduler", "t_add","transfer-regions-of-label-to-label-scheduler","zone","s1","zone","s4"}
	_, _, err = pdctl.ExecuteCommandC(cmd, args...)
	c.Assert(err, IsNil)
	args=[]string{"-u", pdAddr, "scheduler", "t_add","transfer-regions-of-label-to-label-scheduler","2","zone","s1"}
	_, _, err = pdctl.ExecuteCommandC(cmd, args...)
	c.Assert(err, IsNil)
	args=[]string{"-u", pdAddr, "scheduler", "t_add","transfer-regions-of-label-to-label-scheduler","zone","ss","zone","s4"}
	_, _, err = pdctl.ExecuteCommandC(cmd, args...)
	c.Assert(err, IsNil)


	args=[]string{"-u", pdAddr, "scheduler", "t_add","set-scheduler","2","zone","s1"}
	_, _, err = pdctl.ExecuteCommandC(cmd, args...)
	c.Assert(err, IsNil)
	args=[]string{"-u", pdAddr, "scheduler", "t_add","set-scheduler","2","zone"}
	_, _, err = pdctl.ExecuteCommandC(cmd, args...)
	c.Assert(err, IsNil)
	args=[]string{"-u", pdAddr, "scheduler", "t_add","set-scheduler","0","zone","s1"}
	_, _, err = pdctl.ExecuteCommandC(cmd, args...)
	c.Assert(err, IsNil)
	args=[]string{"-u", pdAddr, "scheduler", "t_add","set-scheduler","1","zone","s1"}
	_, _, err = pdctl.ExecuteCommandC(cmd, args...)
	c.Assert(err, IsNil)



}
