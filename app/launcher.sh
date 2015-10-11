#!/bin/bash
for i in `seq 1 10`; do
    mvn exec:java 1> log/out_$i.log 2> log/err_$i.log &
done
