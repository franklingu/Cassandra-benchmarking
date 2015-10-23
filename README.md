Cassandra benchmarking
==============================================

### Intro
This is a benchmarking of different modeling of the same data set and same query set. By comparing performance of different ways of modeling, we will draw conclusions about how to use Cassandra effectively and gain a better understanding.

### Setup
1. Get Cassandra(~> 2.2) installed first by following the instructions [on datastax](http://docs.datastax.com/en/cassandra/2.1/cassandra/install/install_cassandraTOC.html). And you need to install Gradle(>= 2.7) and Maven(>=3.0) beforehand. 
2. Config the cassandra server and start it. (Start the seed node first if you are doing two-node benchmarking)
3. Run ```./setup.sh true D8``` for database creation and data importing.
   There are 2 parameters that you can supply to the script: download_or_not and which_dataset_to_load.
   So for example ```./setup.sh true D8``` means that you want to download and load the D8 dataset. And ```./setup.sh false D40``` means you have downloaded D40 in the data folder already and just to load the dataset directly without downloading.

Note: *During our testing, sometimes the ```drop keyspace cs4224;``` cql command may fail. So it is advisable for you to manually make sure it actually succeeds*

### Cassandra Configuration (cassandra.yaml)
In our case, seed node is compg27 and client node is compg28. 

Find and edit the cassandra.yaml as suggested below:

#### Timeout setting
```
# How long the coordinator should wait for read operations to complete
read_request_timeout_in_ms: 30000
# How long the coordinator should wait for seq or index scans to complete
range_request_timeout_in_ms: 30000
# How long the coordinator should wait for writes to complete
write_request_timeout_in_ms: 30000
# How long the coordinator should wait for counter writes to complete
counter_write_request_timeout_in_ms: 30000
# How long a coordinator should continue to retry a CAS operation
# that contends with other proposals for the same row
cas_contention_timeout_in_ms: 30000
# How long the coordinator should wait for truncates to complete
# (This can be much longer, because unless auto_snapshot is disabled
# we need to flush first so we can snapshot before removing the data.)
truncate_request_timeout_in_ms: 60000
# The default timeout for other, miscellaneous operations
request_timeout_in_ms: 30000
```

#### Two node communication
compg27 (seed node)
```
- seeds: "192.168.8.159"
listen_address: 192.168.8.159
rpc_address: 192.168.8.159
#broadcast_rpc_address: 192.168.9.255 (comment it out)
```

compg28(client node):
```
- seeds: "192.168.8.159"
listen_address: 192.168.8.158
rpc_address: 192.168.8.158
#broadcast_rpc_address: 192.168.9.255 (comment it out)
```

### Benchmarking
1. Navigate to app folder and run ```./launcher.sh``` to create some number of clients and compile benchmarking results after all sub processes exit. (Note that you are supposed to change the number in launcher script for the desired number of clients as you want. e.g.: ```for i in `seq 1 10`;``` means you want to run 10 clients at the same time) 

Note: *During our testing, sometimes some processes may fail with an IOException: cannot create native thread. Check with ```ulimit -a``` that the allowed maximum number of processes is not too long and you should have enough memory. You can use ```ulimit -Su 8192``` to increase maximum process allowed to 8192.*

### Analysis
To be continued
