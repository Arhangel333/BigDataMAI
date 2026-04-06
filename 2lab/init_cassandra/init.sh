#!/bin/bash
# Запускаем Cassandra в фоне
docker-entrypoint.sh cassandra -f &

# Ждём готовности
echo "⏳ Ожидание Cassandra..."
until cqlsh -u cassandra -p cassandra123 -e "describe keyspaces" 2>/dev/null; do
  sleep 2
done

echo "✅ Cassandra готова, создаём keyspace..."
cqlsh -u cassandra -p cassandra123 -e "CREATE KEYSPACE IF NOT EXISTS etl_reports WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1};" 2>/dev/null
echo "✅ Keyspace etl_reports создан"

# Держим контейнер живым
wait
