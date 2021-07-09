#!/usr/bin/env bash
# копирует файл в HDFS "таблицу" или удаляет директорию, перенаправляет протоколы

checkResult() {
  if [ $? -ne 0 ]; then
    cat hdfs_stderr.txt
    exit 1
  fi
}

fileName=$1
hdfsDir=$2

if [ "$#" -gt 2 ]; then
  # если имя файла есть - копируем
  hdfsName=$3
  echo "Copying $fileName into HDFS..."
  hdfs dfs -mkdir -p $hdfsDir >hdfs_stdout.txt 2>hdfs_stderr.txt
  checkResult
  hdfs dfs -put -f $fileName $hdfsDir"/"$hdfsName >hdfs_stdout.txt 2>hdfs_stderr.txt
else
  # иначе - удаляем директорию
  echo "Deleting $hdfsDir from HDFS..."
  hdfs dfs -rm -r -f -skipTrash $hdfsDir >hdfs_stdout.txt 2>hdfs_stderr.txt
fi

checkResult
echo "Done"
