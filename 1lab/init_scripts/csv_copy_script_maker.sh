#!/bin/bash

#Я скрипт который создаёт файл на языке PostgreSQL для копирования из $1.csv файла всей инфы в базу данных (у всех колонок тип TEXT будет) 
mock_file="$1"
echo "CREATE table mock_data("
head -1 "$mock_file" | sed 's/\r/ TEXT/' | sed 's/,/ TEXT,\n/g'
echo ");"

echo "COPY mock_data("
head -1 "$mock_file"
echo ")"
echo "FROM '"$mock_file"'"
echo "DELIMITER ','"
echo "CSV HEADER;"

