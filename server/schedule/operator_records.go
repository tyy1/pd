package schedule

import "time"

const (
	OperatorIntervalTime = 5 * time.Minute
)
// record user last op time
var OpRecords map[uint64]time.Time

func init()  {
	OpRecords=make(map[uint64]time.Time)
}

func OpRecordCheck(regionId uint64,now_time time.Time) bool {
		time,ok:=OpRecords[regionId]
		if !ok {
			return true
		}else{
			lastTime:=time
			if now_time.Sub(lastTime)>OperatorIntervalTime {
				return true
			}else{return false}
		}
}
func OpRecordAdd(regionId uint64,now_time time.Time){
	OpRecords[regionId]=now_time
}
