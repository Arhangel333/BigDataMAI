#!/bin/bash 
#запуск скрипта который делает кучу строк кода для копирования в бд 
#всех csv файлов из $2 используя скрипт $1 и кидает из в copy.sql
/data/init_scripts/make_script.sh "/data/init_scripts/csv_copy_script_maker.sh" "/data/BDSnowflake/исходные данные" > /data/db/copy.sql


PGUSER=${POSTGRES_USER:-postgres}
PGDATABASE=${POSTGRES_DB:-postgres}


# А теперь выполняем этот файл в БД и получаем ПОЛНОСТЬЮ готовую СЫРУЮ БД
psql -U "$PGUSER" -d $PGDATABASE -f /data/db/copy.sql
# Это DDL скрипты создают таблицы итоговые
psql -U "$PGUSER" -d $PGDATABASE -f /data/db/ddl_new_tables.sql
# Это DML скрипты заполняют таблицы из сырой таблицы
psql -U "$PGUSER" -d $PGDATABASE -f /data/db/dml_new_tables.sql