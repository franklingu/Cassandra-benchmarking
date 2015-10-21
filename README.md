Cassandra benchmarking
==============================================

### Intro
This is a benchmarking of different modeling of the same data set and same query set. By comparing performance of different ways of modeling, we will draw conclusions about how to use Cassandra effectively and gain a better understanding.

### Setup
1. Get Cassandra(~> 2.2) installed first by following the instructions [on datastax](http://docs.datastax.com/en/cassandra/2.1/cassandra/install/install_cassandraTOC.html). And you need to install Gradle(>= 2.7) and Maven(>=3.0) beforehand.
2. Start your Cassandra server.
3. Run ```./setup.sh true D8``` for database creation and data importing.
   There are 2 parameters that you can supply to the script: download_or_not and which_dataset_to_load.
   So for example ```./setup.sh true D8``` means that you want to download and load the D8 dataset. And ```./setup.sh false D40``` means you have downloaded D40 in the data folder already and just to load the dataset directly without downloading.

Note: *During our testing, sometimes the ```drop keyspace cs4224;``` cql command may fail. So it is advisable for you to manually make sure it actually succeeds*

### Benchmarking
1. Navigate to app folder and run ```./launcher.sh``` to create some number of clients and compile benchmarking results after all sub processes exit. (Note that you are supposed to change the number in launcher script for the desired number of clients as you want. e.g.: ```for i in `seq 1 10`;``` means you want to run 10 clients at the same time) 

Note: *During our testing, sometimes some processes may fail with an IOException: cannot create native thread. Check with ```ulimit -a``` that the allowed maximum number of processes is not too long and you should have enough memory*

### Analysis
To be continued
