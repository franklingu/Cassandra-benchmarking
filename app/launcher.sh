#!/bin/bash
for i in `seq 1 3`; do
    mvn exec:java -Dexec.args="D8 $i" 1> log/out_$i.log 2> log/err_$i.log &
done
