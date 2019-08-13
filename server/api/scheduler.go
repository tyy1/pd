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

package api

import (
	"fmt"
	"github.com/gorilla/mux"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/pd/server"
	"github.com/unrolled/render"
	"net/http"
	"strconv"

	"github.com/pingcap/pd/server/schedule"
)
type schedulerHandler struct {
	*server.Handler
	r *render.Render
}

func newSchedulerHandler(handler *server.Handler, r *render.Render) *schedulerHandler {
	return &schedulerHandler{
		Handler: handler,
		r:       r,
	}
}

func (h *schedulerHandler) List(w http.ResponseWriter, r *http.Request) {
	schedulers, err := h.GetSchedulers()
	if err != nil {
		h.r.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	h.r.JSON(w, http.StatusOK, schedulers)
}

func (h *schedulerHandler) Post(w http.ResponseWriter, r *http.Request) {
	var input map[string]interface{}
	if err := readJSONRespondError(h.r, w, r.Body, &input); err != nil {
		return
	}
	name, ok := input["name"].(string)
	if !ok {
		h.r.JSON(w, http.StatusBadRequest, "missing scheduler name")
		return
	}
	switch name {
	case "set-scheduler":
		z_or_o,ok:=input["num"].(string)
		if !ok {
			h.r.JSON(w, http.StatusBadRequest, "missing scheduler set number")
			return
		}
		label_key,ok:=input["label_key"].(string)
		if !ok {
			h.r.JSON(w, http.StatusBadRequest, "missing label_key")
			return
		}
		label_value,ok:=input["label_value"].(string)
		if !ok {
			h.r.JSON(w, http.StatusBadRequest, "missing label_value")
			return
		}
		u_z_or_o, err := strconv.ParseUint(z_or_o, 10, 64)
		if err != nil {
			h.r.JSON(w, http.StatusBadRequest, "missing scheduler set number into uint64")
			return
		}
		switch uint64(u_z_or_o) {
		case 0:
			schedule.UserSource=[]*metapb.StoreLabel{{Key:label_key,Value:label_value}}
		case 1:
			schedule.UserTarget=[]*metapb.StoreLabel{{Key:label_key,Value:label_value}}
		default:
			h.r.JSON(w, http.StatusBadRequest, "err number")
			return
		}
	case "balance-leader-scheduler":
		if err := h.AddBalanceLeaderScheduler(); err != nil {
			h.r.JSON(w, http.StatusInternalServerError, err.Error())
			return
		}
	case "balance-hot-region-scheduler":
		if err := h.AddBalanceHotRegionScheduler(); err != nil {
			h.r.JSON(w, http.StatusInternalServerError, err.Error())
			return
		}
	case "balance-region-scheduler":
		if err := h.AddBalanceRegionScheduler(); err != nil {
			h.r.JSON(w, http.StatusInternalServerError, err.Error())
			return
		}
	case "label-scheduler":
		if err := h.AddLabelScheduler(); err != nil {
			h.r.JSON(w, http.StatusInternalServerError, err.Error())
			return
		}
	case "scatter-range":
		var args []string
		startKey, ok := input["start_key"].(string)
		if ok {
			args = append(args, startKey)
		}
		endKey, ok := input["end_key"].(string)
		if ok {
			args = append(args, endKey)
		}
		name, ok := input["range_name"].(string)
		if ok {
			args = append(args, name)
		}
		if err := h.AddScatterRangeScheduler(args...); err != nil {
			h.r.JSON(w, http.StatusInternalServerError, err.Error())
			return
		}
	case "balance-adjacent-region-scheduler":
		var args []string
		leaderLimit, ok := input["leader_limit"].(string)
		if ok {
			args = append(args, leaderLimit)
		}
		peerLimit, ok := input["peer_limit"].(string)
		if ok {
			args = append(args, peerLimit)
		}

		if err := h.AddAdjacentRegionScheduler(args...); err != nil {
			h.r.JSON(w, http.StatusInternalServerError, err.Error())
			return
		}
	case "grant-leader-scheduler":
		storeID, ok := input["store_id"].(float64)
		if !ok {
			h.r.JSON(w, http.StatusBadRequest, "missing store id")
			return
		}
		if err := h.AddGrantLeaderScheduler(uint64(storeID)); err != nil {
			h.r.JSON(w, http.StatusInternalServerError, err.Error())
			return
		}
	case "transfer-region-to-store-scheduler":
		regionID, ok := input["region_id"].(float64)
		if !ok {
			h.r.JSON(w, http.StatusBadRequest, "missing region id")
			return
		}
		storeID,ok:=input["to_store_id"].(float64)
		if !ok {
			h.r.JSON(w, http.StatusBadRequest, "missing store id to transfer region to")
			return
		}
		if err:=h.AddTransferRegionToStoreScheduler(uint64(regionID),uint64(storeID));err!=nil{
			h.r.JSON(w,http.StatusInternalServerError,err.Error())
			return
		}
	case "evict-leader-scheduler":
		storeID, ok := input["store_id"].(float64)
		if !ok {
			h.r.JSON(w, http.StatusBadRequest, "missing store id")
			return
		}
		if err := h.AddEvictLeaderScheduler(uint64(storeID)); err != nil {
			h.r.JSON(w, http.StatusInternalServerError, err.Error())
			return
		}
	case "shuffle-leader-scheduler":
		if err := h.AddShuffleLeaderScheduler(); err != nil {
			h.r.JSON(w, http.StatusInternalServerError, err.Error())
			return
		}
	case "shuffle-region-scheduler":
		if err := h.AddShuffleRegionScheduler(); err != nil {
			h.r.JSON(w, http.StatusInternalServerError, err.Error())
			return
		}
	case "random-merge-scheduler":
		if err := h.AddRandomMergeScheduler(); err != nil {
			h.r.JSON(w, http.StatusInternalServerError, err.Error())
			return
		}
	case "shuffle-hot-region-scheduler":
		limit := uint64(1)
		l, ok := input["limit"].(float64)
		if ok {
			limit = uint64(l)
		}
		if err := h.AddShuffleHotRegionScheduler(limit); err != nil {
			h.r.JSON(w, http.StatusInternalServerError, err.Error())
			return
		}
	case "transfer-region-to-label-scheduler":
		regionID, ok := input["region_id"].(float64)
		if !ok {
			/*x:=reflect.TypeOf(regionID)
			y:=x.String()*/
			h.r.JSON(w, http.StatusBadRequest, "missing region id")
			return
		}
		label_key,ok:=input["label_key"].(string)
		if !ok {
			h.r.JSON(w, http.StatusBadRequest, "missing label_key")
			return
		}
		label_value,ok:=input["label_value"].(string)
		if !ok {
			h.r.JSON(w, http.StatusBadRequest, "missing label_value")
			return
		}
		if err:=h.AddTransferRegionToLabelScheduler(uint64(regionID),label_key,label_value);err!=nil{
			h.r.JSON(w, http.StatusInternalServerError, err.Error())
			return
		}
	case "transfer-regions-of-keyrange-to-label-scheduler":
		label_key,ok:=input["label_key"].(string)
		if !ok {
			h.r.JSON(w, http.StatusBadRequest, "missing label_key")
			return
		}
		label_value,ok:=input["label_value"].(string)
		if !ok {
			h.r.JSON(w, http.StatusBadRequest, "missing label_value")
			return
		}
		var regionID []float64
		region_count,ok:=input["region_count"].(float64)
		if !ok {
			h.r.JSON(w, http.StatusBadRequest, "missing region_count")
			return
		}
		for i:=0;i<int(region_count);i++{
			r,ok:=input[fmt.Sprintf("region%d",i)].(float64)
			if !ok {
				h.r.JSON(w, http.StatusBadRequest, "missing region_id")
				return
			}
			regionID=append(regionID,r)
		}
		for _,regionid:=range regionID{
			if err:=h.AddTransferRegionToLabelScheduler(uint64(regionid),label_key,label_value);err!=nil{
				h.r.JSON(w, http.StatusInternalServerError, err.Error())
				return
			}
		}
	case "transfer-regions-of-label-to-label-scheduler":
		label_key,ok:=input["label_key"].(string)
		if !ok {
			h.r.JSON(w, http.StatusBadRequest, "missing label_key")
			return
		}
		label_value,ok:=input["label_value"].(string)
		if !ok {
			h.r.JSON(w, http.StatusBadRequest, "missing label_value")
			return
		}
		from_label_key,ok:=input["from_lk"].(string)
		if !ok {
			h.r.JSON(w, http.StatusBadRequest, "missing from_label_key")
			return
		}
		from_label_value,ok:=input["from_lv"].(string)
		if !ok {
			h.r.JSON(w, http.StatusBadRequest, "missing from_label_value")
			return
		}
		if err:=h.AddTransferRegionsOfLabelToLabelScheduler(from_label_key,from_label_value,label_key,label_value);err!=nil{
			h.r.JSON(w,http.StatusBadRequest,err.Error())
			return
	}

	case "transfer-table-to-label-scheduler":
		var regionID []float64
		region_count,ok:=input["region_count"].(float64)
		if !ok {
			h.r.JSON(w, http.StatusBadRequest, "missing region_count")
			return
		}
		for i:=0;i<int(region_count);i++{
			r,ok:=input[fmt.Sprintf("region%d",i)].(float64)
			if !ok {
				h.r.JSON(w, http.StatusBadRequest, "missing region_id")
				return
			}
			regionID=append(regionID,r)
		}
		label_key,ok:=input["label_key"].(string)
		if !ok {
			h.r.JSON(w, http.StatusBadRequest, "missing label_key")
			return
		}
		label_value,ok:=input["label_value"].(string)
		if !ok {
			h.r.JSON(w, http.StatusBadRequest, "missing label_value")
			return
		}
		for _,regionid:=range regionID{
			if err:=h.AddTransferRegionToLabelScheduler(uint64(regionid),label_key,label_value);err!=nil{
				h.r.JSON(w, http.StatusInternalServerError, err.Error())
				return
			}
		}
	default:
		h.r.JSON(w, http.StatusBadRequest, "unknown scheduler")
		return
	}

	h.r.JSON(w, http.StatusOK, nil)
}

func (h *schedulerHandler) Delete(w http.ResponseWriter, r *http.Request) {
	name := mux.Vars(r)["name"]

	if err := h.RemoveScheduler(name); err != nil {
		h.r.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}

	h.r.JSON(w, http.StatusOK, nil)
}
