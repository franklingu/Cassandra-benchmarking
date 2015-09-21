#!/usr/bin/env bash
if [ ! -d "data" ]
then
  mkdir data
else
  echo "data folder exists already"
fi
cd data
wget http://www.comp.nus.edu.sg/~cs4224/D8.zip -O D8.zip
if [ -d "D8" ]
then
  rm -rf D8
fi
unzip D8.zip
cd D8
cqlsh -f ../../schema.cql
