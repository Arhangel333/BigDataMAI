from pyspark.sql import SparkSession

# Создаем Spark сессию с указанием драйвера
spark = SparkSession.builder \
    .appName("TestPostgres") \
    .config("spark.jars", "/home/jovyan/jars/postgresql-42.6.0.jar") \
    .getOrCreate()

print(f"Spark версия: {spark.version}")

# Подключение к PostgreSQL
jdbc_url = "jdbc:postgresql://postgres:5432/etl_lab"
props = {
    "user": "spark",
    "password": "spark123",
    "driver": "org.postgresql.Driver"
}

try:
    # Проверяем подключение, читаем таблицу (если есть)
    df = spark.read.jdbc(jdbc_url, "mock_data", properties=props)
    print(f"✅ Подключение успешно!")
    print(f"Количество строк в mock_data: {df.count()}")
    print("\nПервые 5 строк:")
    df.show(5)
    
except Exception as e:
    print(f"❌ Ошибка: {e}")
    print("\nВозможно, таблица mock_data еще не создана.")
    print("Попробуем создать тестовую таблицу...")
    
    # Создаем тестовую таблицу
    test_data = [(1, "test1"), (2, "test2")]
    test_df = spark.createDataFrame(test_data, ["id", "name"])
    
    test_df.write.jdbc(jdbc_url, "test_table", mode="overwrite", properties=props)
    print("✅ Тестовая таблица 'test_table' создана")
    
    # Проверяем
    df_test = spark.read.jdbc(jdbc_url, "test_table", properties=props)
    print("Содержимое test_table:")
    df_test.show()

spark.stop()
EOF