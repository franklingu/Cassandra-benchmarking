#!/usr/bin/env bash

# This script takes into 2 arguments: 1st for performing download or not;
#   2nd tries to setup database based on the selected datasource
#   e.g.: ./setup.sh true D8    -- for downloading D8.zip and use D8 as datasource
#   e.g.: ./setup.sh false D40    -- for skipping downloading and use D40 as datasource

# Dataset Initializing Phase
# 
if [ -z $1 ] 
then 
  echo "No download perference specified. Default no downloading action."
  download_op="false"
else
  download_op=$1
  echo "Download option: ${download_op}"
fi

if [ ! -d "data" ]
then
  mkdir data
  echo "Created data folder"
else
  echo "Data folder exists already"
fi

if [ -z $2 ] 
then 
    data_op="D8"
    echo "Use default datasource D8"
else
    data_op=$2
    echo "Use specified datasource ${data_op}"
fi

cd data
if [ $download_op == "true" ]
then
  wget http://www.comp.nus.edu.sg/~cs4224/${data_op}.zip -O ${data_op}.zip
  if [ -d $data_op ]
  then
    rm -rf $data_op
  fi
  unzip ${data_op}.zip
  wget  http://www.comp.nus.edu.sg/~cs4224/xact-spec-files.zip
  if [ -d "xact-spec-files" ]
  then
    rm -rf xact-spec-files
  fi
  unzip xact-spec-files.zip
fi

cd ..
rm -rf import
mkdir import

# Bulk Loader Build Phase
cd loader
echo "Bulkloader build start..."
./gradlew build
echo "Bulkloader build success"

echo "Bulkloader table creation start..."
./gradlew run -Pdataset=$data_op
echo "Bulkloader table creation success"

# Schema Import Phase
cd ../data/$data_op
echo "Loading schema..."
cqlsh -f ../../schema.cql
echo "Loading schema success"

# Bulk Load Phase
echo "Bulkloading..."
cd ../../
tableNames="customers customers_unused orders items items_unused orderlines stocks stocks_unused"
for name in $tableNames  # Note: No quotes
do
  sstableloader -d localhost import/cs4224/$name
done
echo "Initialization Successful"
