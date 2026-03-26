#!/bin/bash
#Чистим Докер
docker stop db_sql
docker rm db_sql
docker rmi postgre

#Собираем образ
docker build . -t postgre