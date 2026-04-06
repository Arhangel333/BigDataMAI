from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import time

print("=" * 60)
print("СОЗДАНИЕ ОТЧЕТОВ В CASSANDRA")
print("=" * 60)

# Создаём таблицы в Cassandra
print("\n0. Создаём таблицы...")
auth = PlainTextAuthProvider(username='cassandra', password='cassandra123')
cluster = Cluster(['cassandra'], auth_provider=auth, port=9042)
session = cluster.connect('etl_reports')

session.execute("DROP TABLE IF EXISTS report_top_products")
session.execute("DROP TABLE IF EXISTS report_revenue_by_category")
session.execute("DROP TABLE IF EXISTS report_top_customers")
session.execute("DROP TABLE IF EXISTS report_customers_by_country")
session.execute("DROP TABLE IF EXISTS report_monthly_trends")
session.execute("DROP TABLE IF EXISTS report_top_stores")
session.execute("DROP TABLE IF EXISTS report_popular_products")

session.execute("""
    CREATE TABLE report_top_products (
        product_id INT PRIMARY KEY,
        product_name TEXT,
        product_category TEXT,
        total_quantity BIGINT,
        total_revenue DOUBLE
    )
""")
session.execute("""
    CREATE TABLE report_revenue_by_category (
        product_category TEXT PRIMARY KEY,
        total_revenue DOUBLE,
        total_quantity BIGINT,
        sales_count BIGINT
    )
""")
session.execute("""
    CREATE TABLE report_top_customers (
        customer_id INT PRIMARY KEY,
        customer_name TEXT,
        customer_country TEXT,
        total_spent DOUBLE,
        orders_count BIGINT,
        avg_order_value DOUBLE
    )
""")
session.execute("""
    CREATE TABLE report_customers_by_country (
        customer_country TEXT PRIMARY KEY,
        customers_count BIGINT,
        total_revenue DOUBLE,
        avg_revenue_per_customer DOUBLE
    )
""")
session.execute("""
    CREATE TABLE report_monthly_trends (
        year INT,
        month INT,
        sales_count BIGINT,
        total_revenue DOUBLE,
        avg_revenue DOUBLE,
        total_quantity BIGINT,
        PRIMARY KEY (year, month)
    )
""")
session.execute("""
    CREATE TABLE report_top_stores (
        store_id INT PRIMARY KEY,
        store_name TEXT,
        store_city TEXT,
        store_country TEXT,
        total_revenue DOUBLE,
        sales_count BIGINT,
        avg_revenue DOUBLE
    )
""")
session.execute("""
    CREATE TABLE report_popular_products (
        product_id INT PRIMARY KEY,
        product_name TEXT,
        product_category TEXT,
        total_sold BIGINT,
        total_revenue DOUBLE
    )
""")
print("   ✅ Все 7 таблиц созданы")
cluster.shutdown()

# Создаем Spark сессию с драйвером Cassandra
spark = SparkSession.builder \
    .appName("ReportsToCassandra") \
    .config("spark.cassandra.connection.host", "cassandra") \
    .config("spark.cassandra.connection.port", "9042") \
    .config("spark.cassandra.auth.username", "cassandra") \
    .config("spark.cassandra.auth.password", "cassandra123") \
    .getOrCreate()

# Подключение к PostgreSQL
pg_url = "jdbc:postgresql://postgres:5432/etl_lab"
pg_props = {"user": "spark", "password": "spark123", "driver": "org.postgresql.Driver"}

print("\n1. Загружаем данные из PostgreSQL...")
fact_sales = spark.read.jdbc(pg_url, "fact_sales", properties=pg_props)
dim_product = spark.read.jdbc(pg_url, "dim_product", properties=pg_props)
dim_customer = spark.read.jdbc(pg_url, "dim_customer", properties=pg_props)
dim_store = spark.read.jdbc(pg_url, "dim_store", properties=pg_props)
dim_date = spark.read.jdbc(pg_url, "dim_date", properties=pg_props)

print(f"   fact_sales: {fact_sales.count()} строк")
print(f"   dim_product: {dim_product.count()} строк")
print(f"   dim_customer: {dim_customer.count()} строк")
print(f"   dim_store: {dim_store.count()} строк")
print(f"   dim_date: {dim_date.count()} строк")

fact_sales.createOrReplaceTempView("fact_sales")
dim_product.createOrReplaceTempView("dim_product")
dim_customer.createOrReplaceTempView("dim_customer")
dim_store.createOrReplaceTempView("dim_store")
dim_date.createOrReplaceTempView("dim_date")

# ============================================
# ОТЧЕТ 1: Витрина продаж по продуктам
# ============================================
print("\n2. Создаем отчет: Витрина продаж по продуктам...")

top_products = spark.sql("""
    SELECT
        CAST(p.product_id AS INT) as product_id,
        p.product_name, p.product_category,
        CAST(SUM(f.quantity) AS BIGINT) as total_quantity,
        CAST(SUM(f.revenue) AS DOUBLE) as total_revenue
    FROM fact_sales f
    JOIN dim_product p ON f.product_id = p.product_id
    GROUP BY p.product_id, p.product_name, p.product_category
    ORDER BY total_quantity DESC
    LIMIT 10
""")
top_products.write \
    .format("org.apache.spark.sql.cassandra") \
    .option("keyspace", "etl_reports") \
    .option("table", "report_top_products") \
    .option("confirm.truncate", "true") \
    .mode("overwrite") \
    .save()
print("   ✅ report_top_products")

revenue_by_category = spark.sql("""
    SELECT
        p.product_category,
        CAST(SUM(f.revenue) AS DOUBLE) as total_revenue,
        CAST(SUM(f.quantity) AS BIGINT) as total_quantity,
        CAST(COUNT(DISTINCT f.transaction_id) AS BIGINT) as sales_count
    FROM fact_sales f
    JOIN dim_product p ON f.product_id = p.product_id
    GROUP BY p.product_category
    ORDER BY total_revenue DESC
""")
revenue_by_category.write \
    .format("org.apache.spark.sql.cassandra") \
    .option("keyspace", "etl_reports") \
    .option("table", "report_revenue_by_category") \
    .option("confirm.truncate", "true") \
    .mode("overwrite") \
    .save()
print("   ✅ report_revenue_by_category")

# ============================================
# ОТЧЕТ 2: Витрина продаж по клиентам
# ============================================
print("\n3. Создаем отчет: Витрина продаж по клиентам...")

top_customers = spark.sql("""
    SELECT
        CAST(c.customer_id AS INT) as customer_id,
        CONCAT(c.customer_first_name, ' ', c.customer_last_name) as customer_name,
        c.customer_country,
        CAST(SUM(f.revenue) AS DOUBLE) as total_spent,
        CAST(COUNT(f.transaction_id) AS BIGINT) as orders_count,
        CAST(AVG(f.revenue) AS DOUBLE) as avg_order_value
    FROM fact_sales f
    JOIN dim_customer c ON f.customer_id = c.customer_id
    GROUP BY c.customer_id, c.customer_first_name, c.customer_last_name, c.customer_country
    ORDER BY total_spent DESC
    LIMIT 10
""")
top_customers.write \
    .format("org.apache.spark.sql.cassandra") \
    .option("keyspace", "etl_reports") \
    .option("table", "report_top_customers") \
    .option("confirm.truncate", "true") \
    .mode("overwrite") \
    .save()
print("   ✅ report_top_customers")

customers_by_country = spark.sql("""
    SELECT
        c.customer_country,
        CAST(COUNT(DISTINCT c.customer_id) AS BIGINT) as customers_count,
        CAST(SUM(f.revenue) AS DOUBLE) as total_revenue,
        CAST(AVG(f.revenue) AS DOUBLE) as avg_revenue_per_customer
    FROM dim_customer c
    JOIN fact_sales f ON c.customer_id = f.customer_id
    GROUP BY c.customer_country
    ORDER BY customers_count DESC
""")
customers_by_country.write \
    .format("org.apache.spark.sql.cassandra") \
    .option("keyspace", "etl_reports") \
    .option("table", "report_customers_by_country") \
    .option("confirm.truncate", "true") \
    .mode("overwrite") \
    .save()
print("   ✅ report_customers_by_country")

# ============================================
# ОТЧЕТ 3: Витрина продаж по времени
# ============================================
print("\n4. Создаем отчет: Витрина продаж по времени...")

monthly_trends = spark.sql("""
    SELECT
        CAST(d.year AS INT) as year,
        CAST(d.month AS INT) as month,
        CAST(COUNT(f.transaction_id) AS BIGINT) as sales_count,
        CAST(SUM(f.revenue) AS DOUBLE) as total_revenue,
        CAST(AVG(f.revenue) AS DOUBLE) as avg_revenue,
        CAST(SUM(f.quantity) AS BIGINT) as total_quantity
    FROM fact_sales f
    JOIN dim_date d ON f.date_id = d.date_id
    WHERE d.year IS NOT NULL
    GROUP BY d.year, d.month
    ORDER BY d.year, d.month
""")
monthly_trends.write \
    .format("org.apache.spark.sql.cassandra") \
    .option("keyspace", "etl_reports") \
    .option("table", "report_monthly_trends") \
    .option("confirm.truncate", "true") \
    .mode("overwrite") \
    .save()
print("   ✅ report_monthly_trends")

# ============================================
# ОТЧЕТ 4: Витрина продаж по магазинам
# ============================================
print("\n5. Создаем отчет: Витрина продаж по магазинам...")

top_stores = spark.sql("""
    SELECT
        CAST(s.store_id AS INT) as store_id,
        s.store_name, s.store_city, s.store_country,
        CAST(SUM(f.revenue) AS DOUBLE) as total_revenue,
        CAST(COUNT(f.transaction_id) AS BIGINT) as sales_count,
        CAST(AVG(f.revenue) AS DOUBLE) as avg_revenue
    FROM fact_sales f
    JOIN dim_store s ON f.store_id = s.store_id
    GROUP BY s.store_id, s.store_name, s.store_city, s.store_country
    ORDER BY total_revenue DESC
    LIMIT 5
""")
top_stores.write \
    .format("org.apache.spark.sql.cassandra") \
    .option("keyspace", "etl_reports") \
    .option("table", "report_top_stores") \
    .option("confirm.truncate", "true") \
    .mode("overwrite") \
    .save()
print("   ✅ report_top_stores")

# ============================================
# ОТЧЕТ 5: Витрина качества продукции
# ============================================
print("\n6. Создаем отчет: Витрина качества продукции...")

popular_products = spark.sql("""
    SELECT
        CAST(p.product_id AS INT) as product_id,
        p.product_name, p.product_category,
        CAST(SUM(f.quantity) AS BIGINT) as total_sold,
        CAST(SUM(f.revenue) AS DOUBLE) as total_revenue
    FROM fact_sales f
    JOIN dim_product p ON f.product_id = p.product_id
    GROUP BY p.product_id, p.product_name, p.product_category
    ORDER BY total_sold DESC
    LIMIT 20
""")
popular_products.write \
    .format("org.apache.spark.sql.cassandra") \
    .option("keyspace", "etl_reports") \
    .option("table", "report_popular_products") \
    .option("confirm.truncate", "true") \
    .mode("overwrite") \
    .save()
print("   ✅ report_popular_products")

print("\n" + "=" * 60)
print("✅ ВСЕ ОТЧЕТЫ УСПЕШНО СОЗДАНЫ В CASSANDRA!")
print("=" * 60)
print("\n📊 Таблицы в Cassandra (keyspace: etl_reports):")
tables = [
    "report_top_products",
    "report_revenue_by_category",
    "report_top_customers",
    "report_customers_by_country",
    "report_monthly_trends",
    "report_top_stores",
    "report_popular_products"
]
for t in tables:
    print(f"   - etl_reports.{t}")

spark.stop()
