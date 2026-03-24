#!/bin/bash 
#запуск скрипта который делает кучу строк кода для копирования в бд 
#всех csv файлов из $2 используя скрипт $1 и кидает из в copy.sql
/data/init_scripts/make_script.sh "/data/init_scripts/csv_copy_script_maker.sh" "/data/BDSnowflake/исходные данные" > /data/db/copy.sql

# А теперь выполняем этот файл в БД и получаем ПОЛНОСТЬЮ готовую СЫРУЮ БД
psql -f /data/db/copy.sql