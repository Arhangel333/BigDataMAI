from pyspark.sql import SparkSession
import os

print("=" * 50)
print("ЗАГРУЗКА CSV В POSTGRESQL")
print("=" * 50)


# Создаем Spark сессию
spark = SparkSession.builder \
    .appName("LoadCSVtoPostgres") \
    .getOrCreate()

print(f"✅ Spark версия: {spark.version}")

# Параметры подключения
jdbc_url = "jdbc:postgresql://postgres:5432/etl_lab"
props = {
    "user": "spark",
    "password": "spark123",
    "driver": "org.postgresql.Driver"
}

# Путь к CSV файлам
csv_path = "/home/jovyan/data/"

# Находим все CSV файлы
csv_files = [f for f in os.listdir(csv_path) if f.endswith('.csv')]
print(f"\n📁 Найдено CSV файлов: {len(csv_files)}")

if not csv_files:
    print("❌ Нет CSV файлов в папке /home/jovyan/data/")
    print("Создайте файлы или проверьте путь")
    spark.stop()
    exit(1)

for f in csv_files:
    print(f"   - {f}")

# Читаем все CSV файлы
print("\n📖 Чтение CSV файлов...")
df = None
for file in csv_files:
    file_path = os.path.join(csv_path, file)
    print(f"   Читаем: {file}")
    
    #temp_df = spark.read.csv(file_path, header=True, inferSchema=True)
    
    temp_df = spark.read.csv(
    file_path, 
    header=True, 
    inferSchema=True,
    quote='"',           # символ кавычек
    escape='"',          # экранирование
    multiLine=True       # строки могут быть многострочными
)

    if df is None:
        df = temp_df
    else:
        df = df.union(temp_df)

row_count = df.count()
print(f"\n✅ Загружено {row_count} строк")

# Показываем пример данных
print("\n📊 Пример данных (первые 3 строки):")
df.show(3, truncate=30)

# Сохраняем в PostgreSQL
print("\n💾 Сохранение в PostgreSQL...")
df.write.jdbc(jdbc_url, "mock_data", mode="overwrite", properties=props)
print("✅ Таблица 'mock_data' создана и заполнена!")

# Проверяем
print("\n🔍 Проверка: читаем из PostgreSQL...")
result = spark.read.jdbc(jdbc_url, "mock_data", properties=props)
result_count = result.count()
print(f"✅ В таблице mock_data {result_count} строк")

print("\n📊 Первые 5 строк из PostgreSQL:")
result.show(5, truncate=40)

print("\n" + "=" * 50)
print("ЗАГРУЗКА ЗАВЕРШЕНА УСПЕШНО!")
print("=" * 50)

spark.stop()
