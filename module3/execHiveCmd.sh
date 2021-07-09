#!/usr/bin/env bash

# выполняет команду Hive, перенаправляет вывод
sql=$1

echo "Executing Hive QL command: \"$sql\" ..."

beeline -n hive -p cloudera -u jdbc:hive2://10.93.1.9:10000 -e "$sql" 2> hive_stderr.txt

if [ $? -ne 0 ]; then
  cat hive_stderr.txt
else
  echo "Done"
fi