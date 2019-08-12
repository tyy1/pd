package command

import (
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"net/http"
	"strconv"
	"strings"
)

//parse response of get-regions-from-table into stores info and region info
func parse_regionIDs_from_table(str string)([]uint64,error) {
	if !strings.HasPrefix(str,"{") {
		return nil,errors.New(str)
	}else {
		result:=make(map[uint64]struct{})
		strsplit:=strings.Split(str," ")
		for i,item:=range strsplit {
			if item=="\"region_id\":" {
				ss:=strings.Split(strsplit[i+1],",")
				regionID, err := strconv.ParseUint(ss[0], 10, 64)
				if err!=nil {
					return nil,err
				}
				result[regionID]= struct{}{}
			}else{continue}
		}
		var regionIDS []uint64
		for k,_:=range result{
			regionIDS=append(regionIDS,k)
		}
		return regionIDS,nil
	}
}

func get_regions_of_table(cmd *cobra.Command,db string,table string) ([]uint64,error) {
	var result []uint64
	var result_err error
	tryURLs(cmd, func(endpoint string) (error) {
		endpointSplit:=strings.Split(endpoint,":")
		endpointSplit[len(endpointSplit)-1]="10080"
		endpoint=""
		for i,e:=range endpointSplit{
			endpoint+=e
			if i!=len(endpointSplit)-1 {
				endpoint+=":"
			}
		}
		url := endpoint + "/tables/" + db+"/"+table+"/regions"
		b := &bodyOption{}
		req, err := http.NewRequest(http.MethodGet, url, b.body)
		if err != nil {
			return err
		}
		if b.contentType != "" {
			req.Header.Set("Content-Type", b.contentType)
		}
		resp, err := dail(req)
		if err != nil {
			return err
		}
		//cmd.Println(resp)
		regionIdS,err:=parse_regionIDs_from_table(resp)
		if err!=nil {
			result_err=err
			result=nil
		}else{
			result=regionIdS
			result_err=err
		}
		return nil
	})
	return result,result_err
}