#!/usr/bin/env bash

path="$( cd "$( dirname $0 )" && cd -P "$( dirname "$SOURCE" )" && pwd )"
cd $path

fileName="/home/deng/Data/nobel-laureates.csv"
tabName=nobel_laureates
dbName=undkit_staging
hiveTab=$dbName"."$tabName
hiveOrc=$hiveTab"_orc"

# скопировать файл в *HDFS*
./copyToHdfs.sh $fileName /tmp/ai/$tabName data.csv

# удалить внешнюю таблицу
./execHiveCmd.sh "DROP TABLE IF EXISTS $hiveTab;"

# создать внешнюю таблицу
./execHiveCmd.sh "CREATE EXTERNAL TABLE $hiveTab
(
    year                 SMALLINT,
    category             STRING,
    prize                STRING,
    motivation           STRING,
    prize_share          STRING,
    laureate_id          SMALLINT,
    laureate_type        STRING,
    full_name            STRING,
    birth_date           DATE,
    birth_city           STRING,
    birth_country        STRING,
    sex                  STRING,
    organization_name    STRING,
    organization_city    STRING,
    organization_country STRING,
    death_date           DATE,
    death_city           STRING,
    death_country        STRING
)
    COMMENT 'nobel-laureates.csv'
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    LOCATION '/tmp/ai/$tabName'
    TBLPROPERTIES ('skip.header.line.count' = '1');"

# удалить предыдущий вариант таблицы
./execHiveCmd.sh "DROP TABLE IF EXISTS $hiveOrc;"
# создать таблицу нужного формата
./execHiveCmd.sh "CREATE TABLE $hiveOrc STORED AS ORC AS
SELECT CAST(year AS smallint),
       category,
       prize,
       motivation,
       prize_share,
       CAST(laureate_id AS smallint),
       laureate_type,
       full_name,
       CAST(birth_date AS date),
       birth_city,
       birth_country,
       sex,
       organization_name,
       organization_city,
       organization_country,
       CAST(death_date AS date),
       death_city,
       death_country
  FROM $hiveTab;"

# удалить внешнюю таблицу
./execHiveCmd.sh "DROP TABLE $hiveTab PURGE;"
# удалить все файлы из директории с таблицей из *HDFS*.
./copyToHdfs.sh $fileName /tmp/ai/$tabName