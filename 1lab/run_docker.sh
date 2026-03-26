#!/bin/bash
#Запускаем докер и входим в него
docker stop db_sql
docker rm db_sql
docker run --name "db_sql" -v $(pwd):/data -d postgre
docker exec -it db_sql bash 