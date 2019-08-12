#!/bin/sh
cd /root/pd && \
./bin/pd-server --name=pd1   --data-dir=pd1 --client-urls="http://127.0.0.1:2379"  --peer-urls="http://127.0.0.1:2380"   --initial-cluster="pd1=http://127.0.0.1:2380"  --log-file=pd1.log  && \
cd /root/TiKV_Using_Binary_Files/tidb-latest-linux-amd64  &&	 \			
./bin/tikv-server --pd-endpoints="127.0.0.1:2379"  --addr="127.0.0.1:20160"  --data-dir=tikv1 --log-file=tikv1.log && \
cd /root/TiKV_Using_Binary_Files/tidb-latest-linux-amd64 &&	 \
./bin/tikv-server --pd-endpoints="127.0.0.1:2379" --addr="127.0.0.1:20161" --data-dir=tikv2 --log-file=tikv2.log&& \
cd /root/TiKV_Using_Binary_Files/tidb-latest-linux-amd64	&&		\			\
./bin/tikv-server --pd-endpoints="127.0.0.1:2379"  --addr="127.0.0.1:20162" --data-dir=tikv3  --log-file=tikv3.log&& \
cd /root/TiKV_Using_Binary_Files/tidb-latest-linux-amd64  &&		\
./bin/tikv-server --pd-endpoints="127.0.0.1:2379"  --addr="127.0.0.1:20163"  --data-dir=tikv4 --log-file=tikv4.log && \
cd /root/TiKV_Using_Binary_Files/tidb-latest-linux-amd64	&&	\
./bin/tikv-server --pd-endpoints="127.0.0.1:2379" --addr="127.0.0.1:20164"  --data-dir=tikv5--log-file=tikv5.log				
				
				

#cd go-ycsb 
#./bin/go-ycsb load tikv -p tikv.pd=127.0.0.1:2379 -P workloads/workloada	
 