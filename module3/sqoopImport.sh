#!/usr/bin/env bash

tabName=$1
dbName=default

if [ "$#" -gt 1 ]; then
    dbName=$2
fi


# запускает sqoop, перенаправляет протоколы
echo "Importing $tabName table into database $dbName with sqoop import..."

hdfs dfs -rm -r -f -skipTrash $tabName >sqoop_stdout.txt 2>sqoop_stderr.txt

export JAVA_HOME="/usr"
/usr/lib/sqoop/bin/sqoop import --connect "jdbc:mysql://10.93.1.9/skillfactory" \
        --username mysql --password arenadata --hive-import -m 1 \
        --table $tabName --hive-table $dbName.$tabName

echo "Done"