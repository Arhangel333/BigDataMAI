from pyspark.sql import SparkSession

# Создаем Spark сессию
spark = SparkSession.builder \
    .appName("Reports") \
    .getOrCreate()

print("Spark version:", spark.version)

# PostgreSQL
pg_url = "jdbc:postgresql://postgres:5432/etl_lab"
pg_props = {
    "user": "spark",
    "password": "spark123",
    "driver": "org.postgresql.Driver"
}

print("Loading data from PostgreSQL...")
fact_sales = spark.read.jdbc(pg_url, "fact_sales", properties=pg_props)
dim_product = spark.read.jdbc(pg_url, "dim_product", properties=pg_props)
print(f"Loaded {fact_sales.count()} sales, {dim_product.count()} products")

# Регистрируем временные таблицы
fact_sales.createOrReplaceTempView("fact_sales")
dim_product.createOrReplaceTempView("dim_product")

# Создаем отчет (простой, для проверки)
result = spark.sql("""
    SELECT 
        p.product_name,
        SUM(f.quantity) as total_sold
    FROM fact_sales f
    JOIN dim_product p ON f.product_id = p.product_id
    GROUP BY p.product_name
    ORDER BY total_sold DESC
    LIMIT 10
""")

print("Report created:")
result.show()

# Сохраняем в CSV (вместо ClickHouse для проверки)
result.write.csv("/home/jovyan/work/report_output", mode="overwrite")
print("Report saved to CSV")

spark.stop()
