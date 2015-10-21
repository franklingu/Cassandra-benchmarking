#!/bin/bash
mvn install
mvn compile
rm -rf log
mkdir log
echo "Starting simulating clients..."
for i in `seq 1 3`; do
    mvn exec:java -Dexec.args="D8 $i" 1> log/out_$i.log 2> log/err_$i.log &
done
wait
./compile_results.sh
echo "Done with simulation"
