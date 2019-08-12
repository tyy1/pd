package command

import (
	"github.com/spf13/cobra"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

var(
	nwpuPrefix="pd/api/v1/nwpu"
	nwpuGetRegionByKey="pd/api/v1/nwpu/key"
)
// tyy
func NewTOperatorCommand() *cobra.Command{
	c :=&cobra.Command{
		Use: 	"t_operator",
		Short:	"t_operator commands",
	}
	c.AddCommand(NewTAddOPeratorCommand())
	c.AddCommand(NewGetSchemaFromCurlCommand())
	c.AddCommand(NewGetRegionsFromTableCommand())
	c.AddCommand(NewTGetRegionByKeyCommand())
	c.AddCommand(NewGetRegionsOfKeyRangeCommand())
	c.AddCommand(NewTestTimeCommand())
	return c
}

func  NewTestTimeCommand()  *cobra.Command{
c:=&cobra.Command{
	Use:"test <start_time> <end_time>",
	Short:"test the string into time",
	Run:testTimeCommandfunc,
}
return c
}

func testTimeCommandfunc(cmd *cobra.Command,args []string)  {
	if len(args)!=2 {
		cmd.Println(cmd.UsageString())
		return
	}
	//toBeCharge := "2015-01-01 00:00:00"   //待转化为时间戳的字符串 注意 这里的小时和分钟还要秒必须写 因为是跟着模板走的 修改模板的话也可以不写
	timeLayout := "2006-01-02 15:04:05"  //转化所需模板
	//loc, _ := time.LoadLocation("Local")
	start_time,err:=time.Parse(timeLayout,args[0])
	if err!= nil{
		cmd.Println(err.Error())
		return
	}
	end_time,err:=time.Parse(timeLayout,args[1])
	if err!= nil{
		cmd.Println(err.Error())
		return
	}
	cmd.Println(start_time)
	cmd.Println(end_time)
	cmd.Println(end_time.Sub(start_time))
}

func NewGetRegionsOfKeyRangeCommand()*cobra.Command  {
	c:=&cobra.Command{
		Use:"get-regions-by-keyrange [--format=raw|encode|hex] <start_key> <limit>",
		Short:"get region by key",
		Run:getRegionsOfKeyRangeCommandfunc,
	}
	c.Flags().String("format","hex","the key format")
	return c
}

func NewGetRegionsFromTableCommand() *cobra.Command  {
	c:=&cobra.Command{
		Use:"get-regions-of-table <db_name> <table_name>",
		Short:"get regions of special table from curl",
		Run:getRegionsFromTableCommandfunc,
	}
	return c
}
func getRegionsOfKeyRangeCommandfunc(cmd *cobra.Command,args []string)  {
	if len(args)!=2 {
		cmd.Println(cmd.UsageString())
		return
	}
	startkey,err:=parseKey(cmd.Flags(),args[0])
	if err!=nil{
		cmd.Println("Error:",err)
		return
	}
	startkey=url.QueryEscape(startkey)
	_,err=strconv.ParseUint(args[1], 10, 64)
	if err != nil {
		cmd.Println(err)
		return
	}
	prefix:=nwpuGetRegionByKey+"/"+startkey+"/"+args[1]
	r, err := doRequest(cmd, prefix, http.MethodGet)
	if err != nil {
		cmd.Printf("Failed to get region: %s\n", err)
		return
	}
	cmd.Println(r)

}

func getRegionsFromTableCommandfunc(cmd *cobra.Command,args []string)  {
	if len(args)!=2 {
		cmd.Println(cmd.UsageString())
		return
	}
	db:=args[0]
	table:=args[1]
	tryURLs(cmd, func(endpoint string) error {
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
		if err!=nil{
			cmd.Println(err.Error())
		}else{
			cmd.Println(regionIdS)
		}
		return nil
	})
}

func NewGetSchemaFromCurlCommand()*cobra.Command  {
	c:=&cobra.Command{
		Use: "get-schema-info",
		Short:"get schema info from curl ",
		Run:getSchemaFromCurlCommandFunc,
	}
	return c
}
func getSchemaFromCurlCommandFunc(cmd *cobra.Command,args []string){
	if len(args)!=0 {
		cmd.Println("you need no argument!")
		cmd.Println(cmd.UsageString())
		return
	}
	tryURLs(cmd, func(endpoint string) error {
		endpointSplit:=strings.Split(endpoint,":")
		endpointSplit[len(endpointSplit)-1]="10080"
		endpoint=""
		for i,e:=range endpointSplit{
			endpoint+=e
			if i!=len(endpointSplit)-1 {
				endpoint+=":"
			}
		}
		url := endpoint + "/schema"
		b := &bodyOption{}
		req, err := http.NewRequest(http.MethodGet, url, b.body)
		if err != nil {
			return err
		}
		if b.contentType != "" {
			req.Header.Set("Content-Type", b.contentType)
		}
		// the resp would be returned by the outer function
		resp, err := dail(req)
		if err != nil {
			return err
		}
		cmd.Println("success!")
		cmd.Println(resp)
		return nil
	})
}
func NewTAddOPeratorCommand() *cobra.Command{
	c:=&cobra.Command{
		Use:"add <operator>",
		Short:"add an operator",
	}
	c.AddCommand(NewTTransferLeaderCommand())
	//c.AddCommand(NewTGetRegionByKeyCommand())
	return c
}
func NewTGetRegionByKeyCommand()  *cobra.Command{
	c:=&cobra.Command{
		Use:"get-region-by-key [--format=raw|encode|hex] <key>",
		Short:"get region by key",
		Run:tgetRegionByKeyCommandFunc,
	}
	c.Flags().String("format","hex","the key format")
	return c
}
func tgetRegionByKeyCommandFunc(cmd *cobra.Command,args []string){
	if len(args)!=1 {
		cmd.Println(cmd.UsageString())
		return
	}
	key,err:=parseKey(cmd.Flags(),args[0])
	if err!=nil{
		cmd.Println("Error:",err)
		return
	}
	key=url.QueryEscape(key)
	prefix:=nwpuGetRegionByKey+"/"+key
	r, err := doRequest(cmd, prefix, http.MethodGet)
	if err != nil {
		cmd.Printf("Failed to get region: %s\n", err)
		return
	}
	cmd.Println(r)
}

func NewTTransferLeaderCommand() *cobra.Command{
	c:=&cobra.Command{
		Use: "transfer-leader <region_id> <to_store_id> [--timeType-min|hour|day] <timeInt>",
		Short:"test TransferLeader",
		Run:ttransferLeaderCommandFunc,
	}
	c.Flags().String("timeType","min","the time type")
	return c
}
func ttransferLeaderCommandFunc(cmd *cobra.Command,args []string)  {
	if len(args)!=2 && len(args)!=3{
		cmd.Println(cmd.UsageString())
		return
	}
	ids,err:=parseUint64s(args[:2])
	if err!=nil{
		cmd.Println(err)
		return
	}

	input:=make(map[string]interface{})
	input["name"]=cmd.Name()//transfer-leader
	input["region_id"]=ids[0]
	input["to_store_id"]=ids[1]

	if len(args)==3{
		switch cmd.Flags().Lookup("timeType").Value.String(){
		case "min":
			input["time_type"]="min"
		case "hour":
			input["time_type"]="hour"
		case "day":
			input["time_type"]="day"
		default:
			cmd.Println(cmd.UsageString())
			return
		}
		time,err:=parseUint64s(args[2:])
		if err!=nil{
			cmd.Println(err)
			return
		}
		input["time_int"]=time[0]
		/*t:=args[2]
		if t!="min"&&t!="hour"&&t!="day"{
			cmd.Println(cmd.UsageString())
			return
		}
		time,err:=parseUint64s(args[3:])
		if err!=nil{
			cmd.Println(err)
			return
		}
		input["time_type"]=args[2]
		input["time_int"]=time[0]*/
	}
	postJSON(cmd,nwpuPrefix,input)
}