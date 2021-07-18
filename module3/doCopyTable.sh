#!/usr/bin/env bash

# doCopyTable.sh cities undkit_staging

tabName=$1
dbName=$2
hiveTab="$dbName.$tabName"
hiveOrc="$dbName.$tabName"_orc

path="$( cd "$( dirname $0 )" && cd -P "$( dirname "$SOURCE" )" && pwd )"
cd $path

./sqoopImport.sh "$tabName" "$dbName"
./execHiveCmd.sh "drop table if exists $hiveOrc purge"
./execHiveCmd.sh "create table $hiveOrc stored as ORC as select * from $hiveTab"
./execHiveCmd.sh "drop table $hiveTab purge"