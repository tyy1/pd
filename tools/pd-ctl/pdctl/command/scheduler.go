// Copyright 2017 PingCAP, Inc.
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

package command

import (
	"fmt"
	"github.com/spf13/cobra"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)
var (
	schedulersPrefix = "pd/api/v1/schedulers"
)

// NewSchedulerCommand returns a scheduler command.
func NewSchedulerCommand() *cobra.Command {
	c := &cobra.Command{
		Use:   "scheduler",
		Short: "scheduler commands",
	}
	c.AddCommand(NewShowSchedulerCommand())
	c.AddCommand(NewAddSchedulerCommand())
	c.AddCommand(NewRemoveSchedulerCommand())
	c.AddCommand(T_NewAddSchedulerCommand())


	return c
}

// NewShowSchedulerCommand returns a command to show schedulers.
func NewShowSchedulerCommand() *cobra.Command {
	c := &cobra.Command{
		Use:   "show",
		Short: "show schedulers",
		Run:   showSchedulerCommandFunc,
	}
	return c
}

func showSchedulerCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 0 {
		cmd.Println(cmd.UsageString())
		return
	}

	r, err := doRequest(cmd, schedulersPrefix, http.MethodGet)
	if err != nil {
		cmd.Println(err)
		return
	}
	cmd.Println(r)
}

func T_NewAddSchedulerCommand() *cobra.Command  {
	c := &cobra.Command{
		Use:   "t_add <scheduler>",
		Short: "add a scheduler",
	}
	c.AddCommand(NewTransferRegionsSchedulerCommand())
	c.AddCommand(NewTransferRegionToLabelCommand())
	c.AddCommand(NewTransferTableTolabelCommand())
	c.AddCommand(NewTransferRegionsOfKeyRangeToLabel())
	c.AddCommand(NewTransferRegionsOfLabelToLabel())
	return c
}
func NewTransferRegionsOfLabelToLabel()  *cobra.Command{
	c:=&cobra.Command{
		Use:"transfer-regions-of-label-to-label-scheduler <label_key> <label_value> <to_label_key> <to_label_value>",
		Short:"transfer a label's regions to the specified label's stores",
		Run:transRegionsOfLabelToLabelCommandfunc,
	}
	return c
}
func transRegionsOfLabelToLabelCommandfunc(cmd *cobra.Command,args []string)  {
	if len(args)!=4{
		cmd.Println(cmd.UsageString())
		return
	}
	input := make(map[string]interface{})
	input["name"] = cmd.Name()
	input["label_key"]=args[2]
	input["label_value"]=args[3]
	input["from_lk"]=args[0]
	input["from_lv"]=args[1]
	postJSON(cmd,schedulersPrefix,input)
}
func NewTransferRegionsOfKeyRangeToLabel() *cobra.Command {
	c:=&cobra.Command{
		Use:"transfer-regions-of-keyrange-to-label-scheduler <start_key> <limit> <label_key> <label_value>",
		Short:"transfer a keyrange's regions to the specified label's stores",
		Run:transRegionsOfKeyRangeToLabelCommandfunc,
	}
	c.Flags().String("format", "hex", "the key format")
	return c
}
// NewAddSchedulerCommand returns a command to add scheduler.
func NewAddSchedulerCommand() *cobra.Command {
	c := &cobra.Command{
		Use:   "add <scheduler>",
		Short: "add a scheduler",
	}
	c.AddCommand(NewGrantLeaderSchedulerCommand())
	c.AddCommand(NewEvictLeaderSchedulerCommand())
	c.AddCommand(NewShuffleLeaderSchedulerCommand())
	c.AddCommand(NewShuffleRegionSchedulerCommand())
	c.AddCommand(NewShuffleHotRegionSchedulerCommand())
	c.AddCommand(NewScatterRangeSchedulerCommand())
	c.AddCommand(NewBalanceLeaderSchedulerCommand())
	c.AddCommand(NewBalanceRegionSchedulerCommand())
	c.AddCommand(NewBalanceHotRegionSchedulerCommand())
	c.AddCommand(NewRandomMergeSchedulerCommand())
	c.AddCommand(NewBalanceAdjacentRegionSchedulerCommand())
	c.AddCommand(NewLabelSchedulerCommand())
	return c
}
func transRegionsOfKeyRangeToLabelCommandfunc(cmd *cobra.Command,args []string)  {
	if len(args)!=4{
		cmd.Println(cmd.UsageString())
		return
	}
	input := make(map[string]interface{})
	input["name"] = cmd.Name()
	input["label_key"]=args[2]
	input["label_value"]=args[3]
	startkey,err:=parseKey(cmd.Flags(),args[0])
	if err!=nil{
		cmd.Println("Error:",err)
		return
	}
	startkey=url.QueryEscape(startkey)
	_,err=strconv.ParseUint(args[1], 10, 64)
	if err != nil {
		cmd.Println(err)
		cmd.Println(cmd.UsageString())
		return
	}
	prefix:=nwpuGetRegionByKey+"/"+startkey+"/"+args[1]
	r, err := doRequest(cmd, prefix, http.MethodGet)
	if err != nil {
		cmd.Printf("Failed to get region: %s\n", err)
		return
	}
	//input["region_ids"]=r
	ids:=parse_region_ids(r)
	region_count:=len(ids)
	input["region_count"]=region_count
	for i,id:=range ids {
		id_uint64,err:=strconv.ParseUint(id,10,64)
		if err!=nil {
			cmd.Println(err)
			return
		}
		input[fmt.Sprintf("region%d",i)]=id_uint64
	}
	postJSON(cmd,schedulersPrefix,input)
}
func parse_region_ids(str string) []string{
	var strs []string
	str_split:=strings.Split(str[1:len(str)-2],",")
	for _,sp:=range str_split{
			strs=append(strs,sp)
	}
	return strs
}
//tyy
func NewTransferTableTolabelCommand() *cobra.Command{
	c:=&cobra.Command{
		Use:"transfer-table-to-label-scheduler <db_name>  <table_name> <label_key> <label_value>",
		Short:"transfer a table's regions to the specified label's stores",
		Run:transferTableTolabelCommandfunc,
	}
	//c.Flags().String("afterTime","0min","the time to execute the scheduler")
	return c
}
func transferTableTolabelCommandfunc(cmd *cobra.Command,args []string)  {
	if len(args)!=4{
		cmd.Println(cmd.UsageString())
		return
	}
	regionIDS,err:=get_regions_of_table(cmd,args[0],args[1])
	if err != nil {
		cmd.Println(err.Error())
		return
	}
	/*dur,err:=parseAftertime(cmd.Flags())
	if err != nil {
		cmd.Println(err.Error())
	}else{
		cmd.Println(dur.String())
	}*/
	input := make(map[string]interface{})
	input["name"] = cmd.Name()
	//input["region_ids"] = regionIDS
	input["region_count"]=len(regionIDS)
	for i,regionid:=range regionIDS {
		input[fmt.Sprintf("region%d",i)]=regionid
	}
	input["label_key"]=args[2]
	input["label_value"]=args[3]
	postJSON(cmd,schedulersPrefix,input)

}
//tyy
func NewTransferRegionToLabelCommand() *cobra.Command{
	c:=&cobra.Command{
		Use:"transfer-region-to-label-scheduler <region_id> <label_key> <label_value>",
		Short:"transfer a region's peers to the specified label's stores",
		Run:transferRegionToLabelSchedulerCommandfunc,
	}
	return c
}
//tyy
func transferRegionToLabelSchedulerCommandfunc(cmd *cobra.Command,args []string){
	if len(args)!=3{
		cmd.Println(cmd.UsageString())
		return
	}
	regionId, err := strconv.ParseUint(args[0], 10, 64)
	if err != nil {
		cmd.Println(err)
		return
	}
	input := make(map[string]interface{})
	input["name"] = cmd.Name()
	input["region_id"] = regionId
	input["label_key"]=args[1]
	input["label_value"]=args[2]
	postJSON(cmd,schedulersPrefix,input)
}

//tyy
func NewTransferRegionsSchedulerCommand() *cobra.Command{
	c:=&cobra.Command{
		Use:"transfer-region-to-store-scheduler <region_id> <to_store_id>",
		Short:"transfer a region's peers to the specified stores",
		Run:transferRegionsSchedulerCommandfunc,
	}
	//c.Flags().String("afterTime","","the time delay to schedule ")
	return c
}
//tyy
func transferRegionsSchedulerCommandfunc(cmd *cobra.Command,args []string){
	if len(args)!=2{
		cmd.Println(cmd.UsageString())
		return
	}
	ids, err := parseUint64s(args)
	if err != nil {
		cmd.Println(err)
		return
	}
	input := make(map[string]interface{})
	input["name"] = cmd.Name()
	input["region_id"] = ids[0]
	input["to_store_id"] = ids[1]

	postJSON(cmd, schedulersPrefix, input)

}

// NewGrantLeaderSchedulerCommand returns a command to add a grant-leader-scheduler.
func NewGrantLeaderSchedulerCommand() *cobra.Command {
	c := &cobra.Command{
		Use:   "grant-leader-scheduler <store_id>",
		Short: "add a scheduler to grant leader to a store",
		Run:   addSchedulerForStoreCommandFunc,
	}
	return c
}

// NewEvictLeaderSchedulerCommand returns a command to add a evict-leader-scheduler.
func NewEvictLeaderSchedulerCommand() *cobra.Command {
	c := &cobra.Command{
		Use:   "evict-leader-scheduler <store_id>",
		Short: "add a scheduler to evict leader from a store",
		Run:   addSchedulerForStoreCommandFunc,
	}
	return c
}

func addSchedulerForStoreCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 1 {
		cmd.Println(cmd.UsageString())
		return
	}

	storeID, err := strconv.ParseUint(args[0], 10, 64)
	if err != nil {
		cmd.Println(err)
		return
	}

	input := make(map[string]interface{})
	input["name"] = cmd.Name()
	input["store_id"] = storeID
	postJSON(cmd, schedulersPrefix, input)
}

// NewShuffleLeaderSchedulerCommand returns a command to add a shuffle-leader-scheduler.
func NewShuffleLeaderSchedulerCommand() *cobra.Command {
	c := &cobra.Command{
		Use:   "shuffle-leader-scheduler",
		Short: "add a scheduler to shuffle leaders between stores",
		Run:   addSchedulerCommandFunc,
	}
	return c
}

// NewShuffleRegionSchedulerCommand returns a command to add a shuffle-region-scheduler.
func NewShuffleRegionSchedulerCommand() *cobra.Command {
	c := &cobra.Command{
		Use:   "shuffle-region-scheduler",
		Short: "add a scheduler to shuffle regions between stores",
		Run:   addSchedulerCommandFunc,
	}
	return c
}

// NewShuffleHotRegionSchedulerCommand returns a command to add a shuffle-hot-region-scheduler.
func NewShuffleHotRegionSchedulerCommand() *cobra.Command {
	c := &cobra.Command{
		Use:   "shuffle-hot-region-scheduler [limit]",
		Short: "add a scheduler to shuffle hot regions",
		Run:   addSchedulerForShuffleHotRegionCommandFunc,
	}
	return c
}

func addSchedulerForShuffleHotRegionCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) > 1 {
		cmd.Println(cmd.UsageString())
		return
	}
	limit := uint64(1)
	if len(args) == 1 {
		l, err := strconv.ParseUint(args[0], 10, 64)
		if err != nil {
			cmd.Println("Error: ", err)
			return
		}
		limit = l
	}
	input := make(map[string]interface{})
	input["name"] = cmd.Name()
	input["limit"] = limit
	postJSON(cmd, schedulersPrefix, input)
}

// NewBalanceLeaderSchedulerCommand returns a command to add a balance-leader-scheduler.
func NewBalanceLeaderSchedulerCommand() *cobra.Command {
	c := &cobra.Command{
		Use:   "balance-leader-scheduler",
		Short: "add a scheduler to balance leaders between stores",
		Run:   addSchedulerCommandFunc,
	}
	return c
}

// NewBalanceRegionSchedulerCommand returns a command to add a balance-region-scheduler.
func NewBalanceRegionSchedulerCommand() *cobra.Command {
	c := &cobra.Command{
		Use:   "balance-region-scheduler",
		Short: "add a scheduler to balance regions between stores",
		Run:   addSchedulerCommandFunc,
	}
	return c
}

// NewBalanceHotRegionSchedulerCommand returns a command to add a balance-hot-region-scheduler.
func NewBalanceHotRegionSchedulerCommand() *cobra.Command {
	c := &cobra.Command{
		Use:   "balance-hot-region-scheduler",
		Short: "add a scheduler to balance hot regions between stores",
		Run:   addSchedulerCommandFunc,
	}
	return c
}

// NewRandomMergeSchedulerCommand returns a command to add a random-merge-scheduler.
func NewRandomMergeSchedulerCommand() *cobra.Command {
	c := &cobra.Command{
		Use:   "random-merge-scheduler",
		Short: "add a scheduler to merge regions randomly",
		Run:   addSchedulerCommandFunc,
	}
	return c
}

// NewLabelSchedulerCommand returns a command to add a label-scheduler.
func NewLabelSchedulerCommand() *cobra.Command {
	c := &cobra.Command{
		Use:   "label-scheduler",
		Short: "add a scheduler to schedule regions according to the label",
		Run:   addSchedulerCommandFunc,
	}
	return c
}

func addSchedulerCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 0 {
		cmd.Println(cmd.UsageString())
		return
	}
	input := make(map[string]interface{})
	input["name"] = cmd.Name()
	postJSON(cmd, schedulersPrefix, input)
}

// NewScatterRangeSchedulerCommand returns a command to add a scatter-range-scheduler.
func NewScatterRangeSchedulerCommand() *cobra.Command {
	c := &cobra.Command{
		Use:   "scatter-range [--format=raw|encode|hex] <start_key> <end_key> <range_name>",
		Short: "add a scheduler to scatter range",
		Run:   addSchedulerForScatterRangeCommandFunc,
	}
	c.Flags().String("format", "hex", "the key format")
	return c
}

func addSchedulerForScatterRangeCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 3 {
		cmd.Println(cmd.UsageString())
		return
	}
	startKey, err := parseKey(cmd.Flags(), args[0])
	if err != nil {
		cmd.Println("Error: ", err)
		return
	}
	endKey, err := parseKey(cmd.Flags(), args[1])
	if err != nil {
		cmd.Println("Error: ", err)
		return
	}

	input := make(map[string]interface{})
	input["name"] = cmd.Name()
	input["start_key"] = url.QueryEscape(startKey)
	input["end_key"] = url.QueryEscape(endKey)
	input["range_name"] = args[2]
	postJSON(cmd, schedulersPrefix, input)
}

// NewBalanceAdjacentRegionSchedulerCommand returns a command to add a balance-adjacent-region-scheduler.
func NewBalanceAdjacentRegionSchedulerCommand() *cobra.Command {
	c := &cobra.Command{
		Use:   "balance-adjacent-region-scheduler [leader_limit] [peer_limit]",
		Short: "add a scheduler to disperse adjacent regions on each store",
		Run:   addSchedulerForBalanceAdjacentRegionCommandFunc,
	}
	return c
}

func addSchedulerForBalanceAdjacentRegionCommandFunc(cmd *cobra.Command, args []string) {
	l := len(args)
	input := make(map[string]interface{})
	if l > 2 {
		cmd.Println(cmd.UsageString())
		return
	} else if l == 1 {
		input["leader_limit"] = url.QueryEscape(args[0])
	} else if l == 2 {
		input["leader_limit"] = url.QueryEscape(args[0])
		input["peer_limit"] = url.QueryEscape(args[1])
	}
	input["name"] = cmd.Name()

	postJSON(cmd, schedulersPrefix, input)
}

// NewRemoveSchedulerCommand returns a command to remove scheduler.
func NewRemoveSchedulerCommand() *cobra.Command {
	c := &cobra.Command{
		Use:   "remove <scheduler>",
		Short: "remove a scheduler",
		Run:   removeSchedulerCommandFunc,
	}
	return c
}

func removeSchedulerCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 1 {
		cmd.Println(cmd.Usage())
		return
	}

	path := schedulersPrefix + "/" + args[0]
	_, err := doRequest(cmd, path, http.MethodDelete)
	if err != nil {
		cmd.Println(err)
		return
	}
}
