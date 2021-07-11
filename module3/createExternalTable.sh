#!/usr/bin/env bash

path="$( cd "$( dirname $0 )" && cd -P "$( dirname "$SOURCE" )" && pwd )"
cd $path

fileName=$1
tabName=$2
dbName=$3
createFields=$4
selectFields=$5
hiveTab=$dbName"."$tabName
hiveOrc=$hiveTab"_orc"

# скопировать файл в *HDFS*
./copyToHdfs.sh $fileName /tmp/ai/$tabName data.csv

# удалить внешнюю таблицу
./execHiveCmd.sh "DROP TABLE IF EXISTS $hiveTab;"

# создать внешнюю таблицу
./execHiveCmd.sh "CREATE EXTERNAL TABLE $hiveTab
(
    $createFields
)
    COMMENT '$fileName'
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    LOCATION '/tmp/ai/$tabName'
    TBLPROPERTIES ('skip.header.line.count' = '1');"
    
# удалить предыдущий вариант таблицы
./execHiveCmd.sh "DROP TABLE IF EXISTS $hiveOrc;"
# создать таблицу нужного формата
./execHiveCmd.sh "CREATE TABLE $hiveOrc STORED AS ORC AS 
SELECT $selectFields
  FROM $hiveTab;"

# удалить внешнюю таблицу
./execHiveCmd.sh "DROP TABLE $hiveTab PURGE;"
# удалить все файлы из директории с таблицей из *HDFS*.
./copyToHdfs.sh $fileName /tmp/ai/$tabName