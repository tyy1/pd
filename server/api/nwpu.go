package api

import (
	"fmt"
	"github.com/gorilla/mux"
	"github.com/pingcap/pd/server"
	"github.com/pingcap/pd/server/schedule"
	"github.com/unrolled/render"
	"net/http"
	"strconv"
	"time"
)

type nwpuHandler struct {
	*server.Handler
	r *render.Render
}


func newNwpuHandler(hanlder *server.Handler, rd *render.Render) *nwpuHandler {
	return &nwpuHandler{
		Handler:hanlder,
		r:rd,
	}
}

func (h *nwpuHandler) List(w http.ResponseWriter, r *http.Request) {
	var (
		results []*schedule.Operator
		ops     []*schedule.Operator
		err     error
	)

	kinds, ok := r.URL.Query()["kind"]
	if !ok {
		results, err = h.GetOperators()
		if err != nil {
			h.r.JSON(w, http.StatusInternalServerError, err.Error())
			return
		}
	} else {
		for _, kind := range kinds {
			switch kind {
			case "admin":
				ops, err = h.GetAdminOperators()
			case "leader":
				ops, err = h.GetLeaderOperators()
			case "region":
				ops, err = h.GetRegionOperators()
			case "waiting":
				ops, err = h.GetWaitingOperators()
			}
			if err != nil {
				h.r.JSON(w, http.StatusInternalServerError, err.Error())
				return
			}
			results = append(results, ops...)
		}
	}

	h.r.JSON(w, http.StatusOK, results)
}

func (h *nwpuHandler) Post (w http.ResponseWriter, r *http.Request) {
	var input map[string]interface{}
	if err := readJSONRespondError(h.r, w, r.Body, &input); err != nil {
		return
	}

	name, ok := input["name"].(string)
	if !ok {
		h.r.JSON(w, http.StatusBadRequest, "missing operator name")
		return
	}
	switch name {
	case "transfer-leader":
		regionID, ok := input["region_id"].(float64)
		if !ok {
			h.r.JSON(w, http.StatusBadRequest, "missing region id")
			return
		}
		storeID, ok := input["to_store_id"].(float64)
		if !ok {
			h.r.JSON(w, http.StatusBadRequest, "missing store id to transfer leader to")
			return
		}
		time_type,ok:=input["time_type"].(string)
		if !ok {
			if err := h.AddTransferLeaderOperator(uint64(regionID), uint64(storeID)); err != nil {
				h.r.JSON(w, http.StatusInternalServerError, err.Error())
				return
			}
		}else{
			timeInt:=input["time_int"].(float64)
			if !ok {
				h.r.JSON(w, http.StatusBadRequest, "missing timeInt to transfer leader to")
				return
			}
			//h.r.JSON(w, 1008, "timeINT is ok ")
			var dur time.Duration
			switch time_type {
			case "min":
				dur=time.Duration(timeInt)*time.Minute
			case "hour":
				dur=time.Duration(timeInt)*time.Hour
			case "day":
				dur=time.Duration(timeInt*24)*time.Hour
			default:
				h.r.JSON(w, http.StatusBadRequest, "missing time_type to transfer leader to")
				return
			}
			rr,_:=h.Handler.GetRaftCluster().GetRegionByID(uint64(regionID))
			if rr==nil{
				h.r.JSON(w, http.StatusInternalServerError, "region not found")//   need to correct here
				return
			}
			//_,err1:=h.Handler.GetRascheduleftCluster().GetStore(uint64(storeID))
			_,err1:=h.Handler.GetRaftCluster().GetStore(uint64(storeID))
			if err1!=nil{
				errStoreNotFound:=fmt.Sprint("region has no voter in store ",storeID)
				h.r.JSON(w, http.StatusInternalServerError,errStoreNotFound)
				return
			}
			time.AfterFunc(dur, func() {
				if err := h.AddTransferLeaderOperator(uint64(regionID), uint64(storeID)); err != nil {
					h.r.JSON(w, http.StatusInternalServerError, err.Error())
					return
				}
			})
		}
	default:
		h.r.JSON(w, http.StatusBadRequest, "unknown operator")
		return
	}

	h.r.JSON(w, http.StatusOK, nil)
}
func (h *nwpuHandler)GetRegionByKey(w http.ResponseWriter,r *http.Request){
	cluster:=h.Handler.GetRaftCluster()
	if cluster == nil {
		h.r.JSON(w, http.StatusInternalServerError, server.ErrNotBootstrapped.Error())
		return
	}
	vars := mux.Vars(r)
	key := vars["key"]
	regionInfo := cluster.GetRegionInfoByKey([]byte(key))
	regionID:=regionInfo.GetMeta().Id
	//h.r.JSON(w, http.StatusOK, NewRegionInfo(regionInfo))
	h.r.JSON(w,http.StatusOK,regionID)
}
func (h *nwpuHandler)GetRegionsByKeyRange(w http.ResponseWriter,r *http.Request) {
	cluster:=h.Handler.GetRaftCluster()
	if cluster == nil {
		h.r.JSON(w, http.StatusInternalServerError, server.ErrNotBootstrapped.Error())
		return
	}
	vars := mux.Vars(r)
	start_key := vars["start_key"]
	limit:=vars["limit"]
	limitInt,err:=strconv.ParseUint(limit, 10, 64)
	if err != nil {
		h.r.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}
    //regionsInfo:=core.NewRegionsInfo()
	//regions:=regionsInfo.ScanRangeWithEndKey([]byte(start_key),[]byte(end_key))
	startKey:=[]byte(start_key)
	regions:=cluster.ScanRegionsByKey(startKey,int(limitInt))
	var regionIDS =""
	//end_region,_:=cluster.GetRegionByKey(endKey)
	//regionIDS=append(regionIDS,end_region.Id)
	for i,region:=range regions{
			regionID:=region.GetMeta().Id
			regionID_str:=strconv.FormatUint(regionID,10)
			regionIDS+=regionID_str
		if i!=len(regions)-1 {
			regionIDS+=","
		}

	}
	//out:="  start:"+start_key+" end:"+end_key +"\n "+string(len(regionIDS))
	h.r.JSON(w,http.StatusOK, regionIDS)
}

