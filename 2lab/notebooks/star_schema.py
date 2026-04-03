from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month, dayofmonth, concat, lit, row_number
from pyspark.sql.window import Window

print("=" * 60)
print("ПРЕОБРАЗОВАНИЕ В МОДЕЛЬ ЗВЕЗДА")
print("=" * 60)

# Создаем Spark сессию
spark = SparkSession.builder \
    .appName("StarSchema") \
    .config("spark.jars", "/home/jovyan/jars/postgresql-42.6.0.jar") \
    .getOrCreate()

# Параметры подключения
jdbc_url = "jdbc:postgresql://postgres:5432/etl_lab"
props = {
    "user": "spark",
    "password": "spark123",
    "driver": "org.postgresql.Driver"
}

# 1. Читаем исходную таблицу
print("\n1. Читаем mock_data из PostgreSQL...")
df = spark.read.jdbc(jdbc_url, "mock_data", properties=props)
total_rows = df.count()
print(f"   Загружено {total_rows} строк")

# 2. Создаем измерение "Клиенты"
print("\n2. Создаем dim_customer...")
dim_customer = df.select(
    "sale_customer_id",
    "customer_email",
    "customer_country"
).distinct()
dim_customer = dim_customer.withColumnRenamed("sale_customer_id", "customer_id")
print(f"   Уникальных клиентов: {dim_customer.count()}")

# 3. Создаем измерение "Товары"
print("\n3. Создаем dim_product...")
dim_product = df.select(
    "sale_product_id",
    "product_name",
    "product_category"
).distinct()
print(f"   Уникальных товаров: {dim_product.count()}")

# 4. Создаем измерение "Магазины"
print("\n4. Создаем dim_store...")
dim_store = df.select(
    "store_name",
    "store_city",
    "store_country"
).distinct()

# Добавляем суррогатный ключ store_id
window_store = Window.orderBy("store_name", "store_city")
dim_store = dim_store.withColumn("store_id", row_number().over(window_store))
# Переставляем колонки, чтобы store_id был первым
dim_store = dim_store.select("store_id", "store_name", "store_city", "store_country")
print(f"   Уникальных магазинов: {dim_store.count()}")
dim_store.show(5)

# 5. Создаем измерение "Даты"
print("\n5. Создаем dim_date...")
dim_date = df.select("sale_date").distinct() \
    .withColumn("full_date", col("sale_date")) \
    .withColumn("year", year("sale_date")) \
    .withColumn("month", month("sale_date")) \
    .withColumn("day", dayofmonth("sale_date")) \
    .withColumn("quarter", (col("month") - 1) / 3 + 1) \
    .withColumn("week_day", dayofmonth("sale_date") % 7) \
    .drop("sale_date")

# Добавляем суррогатный ключ
window = Window.orderBy("full_date")
dim_date = dim_date.withColumn("date_id", row_number().over(window))
print(f"   Уникальных дат: {dim_date.count()}")

# 6. Создаем факт-таблицу
print("\n6. Создаем fact_sales...")

# Присоединяем date_id к фактам
df_with_date = df.join(dim_date, df.sale_date == dim_date.full_date, "left")

fact_sales = df_with_date.select(
    col("id"),
    col("sale_customer_id"),
    col("product_id"),
    col("store_id"),
    col("date_id"),
    (col("price") * col("quantity")).alias("revenue"),
    col("quantity"),
    col("review_rating")
)
print(f"   Записей в факт-таблице: {fact_sales.count()}")

# 7. Сохраняем в PostgreSQL
print("\n7. Сохраняем таблицы в PostgreSQL...")

# Сохраняем измерения
dim_customer.write.jdbc(jdbc_url, "dim_customer", mode="overwrite", properties=props)
print("   ✅ dim_customer сохранена")

dim_product.write.jdbc(jdbc_url, "dim_product", mode="overwrite", properties=props)
print("   ✅ dim_product сохранена")

dim_store.write.jdbc(jdbc_url, "dim_store", mode="overwrite", properties=props)
print("   ✅ dim_store сохранена")

dim_date.write.jdbc(jdbc_url, "dim_date", mode="overwrite", properties=props)
print("   ✅ dim_date сохранена")

fact_sales.write.jdbc(jdbc_url, "fact_sales", mode="overwrite", properties=props)
print("   ✅ fact_sales сохранена")

print("\n" + "=" * 60)
print("МОДЕЛЬ ЗВЕЗДА УСПЕШНО СОЗДАНА!")
print("=" * 60)

# 8. Показываем примеры
print("\n📊 Пример dim_customer:")
dim_customer.show(5)

print("\n📊 Пример dim_product:")
dim_product.show(5)

print("\n📊 Пример fact_sales:")
fact_sales.show(5)

spark.stop()
