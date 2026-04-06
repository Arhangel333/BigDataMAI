#!/bin/bash
# Init-скрипт для Spark контейнера - устанавливает зависимости при старте

echo "=== Установка зависимостей в Spark контейнере... ==="

# Redis для Valkey
echo "📦 Установка redis..."
pip install redis --quiet 2>/dev/null && echo "✅ redis установлен" || echo "⚠ redis уже установлен или ошибка"

# Cassandra CQL client
echo "📦 Установка cassandra-driver..."
pip install cassandra-driver --quiet 2>/dev/null && echo "✅ cassandra-driver установлен" || echo "⚠ cassandra-driver уже установлен или ошибка"

# Ждём доступности MongoDB (иногда нужен таймаут)
echo "⏳ Ожидание MongoDB..."
for i in $(seq 1 30); do
  python3 -c "import socket; s=socket.socket(); s.connect(('mongodb',27017)); s.close(); break" 2>/dev/null && { echo "✅ MongoDB доступна"; break; } || sleep 1
done

# Создаём keyspace в Cassandra
echo "📦 Создание keyspace в Cassandra..."
python3 -c "
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
auth = PlainTextAuthProvider(username='cassandra', password='cassandra123')
cluster = Cluster(['cassandra'], auth_provider=auth, port=9042)
session = cluster.connect()
session.execute(\"CREATE KEYSPACE IF NOT EXISTS etl_reports WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}\")
print('✅ Keyspace etl_reports создан')
session.shutdown()
" 2>&1 || echo "⚠ Keyspace уже существует или Cassandra ещё не готова"

pip install pymongo neo4j redis cassandra-driver --quiet 2>/dev/null
echo "✅ pymongo, neo4j, redis, cassandra-driver установлены"

echo "=== Все зависимости готовы ==="

/home/jovyan/work/script_spark.sh > /home/jovyan/work/output.txt
# Запускаем Jupyter
echo "🚀 Запуск Jupyter..."
exec start-notebook.sh "$@"
