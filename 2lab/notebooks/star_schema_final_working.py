from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month, dayofmonth, row_number, lit, to_date, avg as _avg
from pyspark.sql.window import Window

print("=" * 60)
print("ПРЕОБРАЗОВАНИЕ В МОДЕЛЬ ЗВЕЗДА")
print("=" * 60)

spark = SparkSession.builder \
    .appName("StarSchema") \
    .config("spark.jars", "/home/jovyan/jars/postgresql-42.6.0.jar") \
    .getOrCreate()

jdbc_url = "jdbc:postgresql://postgres:5432/etl_lab"
props = {"user": "spark", "password": "spark123", "driver": "org.postgresql.Driver"}

# 1. Читаем данные
print("\n1. Читаем mock_data...")
df = spark.read.jdbc(jdbc_url, "mock_data", properties=props)
print(f"   Загружено {df.count()} строк")

# Показываем все колонки для справки
print("\n   Доступные колонки:")
for c in df.columns:
    print(f"   - {c}")

# 2. Создаем dim_customer
print("\n2. Создаем dim_customer...")
dim_customer = df.select(
    col("sale_customer_id").alias("customer_id"),
    col("customer_first_name"),
    col("customer_last_name"),
    col("customer_country")
).distinct()
print(f"   Уникальных клиентов: {dim_customer.count()}")

# 3. Создаем dim_product (используем sale_product_id)
print("\n3. Создаем dim_product...")
dim_product = df.select(
    col("sale_product_id").alias("product_id"),
    col("product_name"),
    col("product_category"),
    col("product_rating").cast("double"),
    col("product_reviews").cast("int")
).distinct()
print(f"   Уникальных товаров: {dim_product.count()}")

# 4. Создаем dim_store
print("\n4. Создаем dim_store...")
dim_store = df.select(
    col("store_name"),
    col("store_city"),
    col("store_country")
).distinct()
window_store = Window.orderBy("store_name", "store_city")
dim_store = dim_store.withColumn("store_id", row_number().over(window_store))
dim_store = dim_store.select("store_id", "store_name", "store_city", "store_country")
print(f"   Уникальных магазинов: {dim_store.count()}")

# 5. Создаем dim_date
print("\n5. Создаем dim_date...")
dim_date = df.select("sale_date").distinct() \
    .withColumn("full_date", to_date(col("sale_date"), "M/d/yyyy")) \
    .drop("sale_date") \
    .withColumn("year", year("full_date")) \
    .withColumn("month", month("full_date")) \
    .withColumn("day", dayofmonth("full_date"))
window = Window.orderBy("full_date")
dim_date = dim_date.withColumn("date_id", row_number().over(window))
print(f"   Уникальных дат: {dim_date.count()}")

# 5.5 Создаем dim_supplier
print("\n5.5. Создаем dim_supplier...")
dim_supplier = df.select(
    col("supplier_name"),
    col("supplier_email"),
    col("supplier_country"),
    col("supplier_city"),
    col("supplier_address"),
    col("supplier_phone")
).distinct()
window_sup = Window.orderBy("supplier_name")
dim_supplier = dim_supplier.withColumn("supplier_id", row_number().over(window_sup))
dim_supplier = dim_supplier.select("supplier_id", "supplier_name", "supplier_email", "supplier_country", "supplier_city", "supplier_address", "supplier_phone")
print(f"   Уникальных поставщиков: {dim_supplier.count()}")

# 6. Присоединяем store_id
print("\n6. Присоединяем store_id...")
df_with_store = df.join(dim_store, on=["store_name", "store_city", "store_country"], how="left")

# 6.5 Присоединяем supplier_id
print("\n6.5. Присоединяем supplier_id...")
df_with_supplier = df_with_store.join(dim_supplier, on="supplier_name", how="left")

# 7. Присоединяем date_id
print("\n7. Присоединяем date_id...")
df_with_date = df_with_supplier.join(
    dim_date,
    to_date(col("sale_date"), "M/d/yyyy") == dim_date.full_date,
    "left"
)

# 8. Создаем fact_sales (используем ПРАВИЛЬНЫЕ имена колонок из df)
print("\n8. Создаем fact_sales...")
fact_sales = df_with_date.select(
    col("id").alias("transaction_id"),
    col("sale_customer_id").alias("customer_id"),
    col("sale_product_id").alias("product_id"),
    col("store_id"),
    col("supplier_id"),
    col("date_id"),
    col("sale_total_price").alias("revenue"),
    col("sale_quantity").alias("quantity"),
    col("product_rating").cast("double").alias("review_rating")
)
print(f"   Записей в факт-таблице: {fact_sales.count()}")

# 9. Сохраняем
print("\n9. Сохраняем в PostgreSQL...")
dim_customer.write.jdbc(jdbc_url, "dim_customer", mode="overwrite", properties=props)
print("   ✅ dim_customer")
dim_product.write.jdbc(jdbc_url, "dim_product", mode="overwrite", properties=props)
print("   ✅ dim_product")
dim_store.write.jdbc(jdbc_url, "dim_store", mode="overwrite", properties=props)
print("   ✅ dim_store")
dim_date.write.jdbc(jdbc_url, "dim_date", mode="overwrite", properties=props)
print("   ✅ dim_date")
dim_supplier.write.jdbc(jdbc_url, "dim_supplier", mode="overwrite", properties=props)
print("   ✅ dim_supplier")
fact_sales.write.jdbc(jdbc_url, "fact_sales", mode="overwrite", properties=props)
print("   ✅ fact_sales")

print("\n" + "=" * 60)
print("✅ МОДЕЛЬ ЗВЕЗДА УСПЕШНО СОЗДАНА!")
print("=" * 60)

print("\n📊 Пример fact_sales:")
fact_sales.show(5)

spark.stop()
