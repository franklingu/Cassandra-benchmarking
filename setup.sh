#!/usr/bin/env bash

# This script takes into 2 arguments: 1st for performing download or not;
#   2nd tries to setup database based on the selected datasource

if [ ! -d "data" ]
then
  mkdir data
else
  echo "Data folder exists already"
fi
data_op=$2
if [ $data_op == "D40" ]
then
  echo "Selected datasource ${data_op}"
else
  data_op="D8"
  echo "Selected datasource ${data_op}"
fi
cd data
if [ $1 == "true" ]
then
  wget http://www.comp.nus.edu.sg/~cs4224/D8.zip -O D8.zip
  if [ -d $data_op ]
  then
    rm -rf $data_op
  fi
  unzip ${data_op}.zip
fi
cd $data_op
cqlsh -f ../../schema.cql
