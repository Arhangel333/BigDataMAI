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
session.execute("DROP TABLE IF EXISTS report_product_ratings")
session.execute("DROP TABLE IF EXISTS report_yearly_revenue")
session.execute("DROP TABLE IF EXISTS report_stores_by_location")
session.execute("DROP TABLE IF EXISTS report_top_reviews")
session.execute("DROP TABLE IF EXISTS report_top_suppliers")
session.execute("DROP TABLE IF EXISTS report_suppliers_by_country")
session.execute("DROP TABLE IF EXISTS report_rating_sales_corr")

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
session.execute("""
    CREATE TABLE report_product_ratings (
        product_id INT PRIMARY KEY,
        product_name TEXT,
        product_category TEXT,
        avg_rating DOUBLE,
        max_reviews INT
    )
""")
session.execute("""
    CREATE TABLE report_yearly_revenue (
        year INT PRIMARY KEY,
        total_sales BIGINT,
        total_revenue DOUBLE,
        avg_order_value DOUBLE,
        total_quantity BIGINT
    )
""")
session.execute("""
    CREATE TABLE report_stores_by_location (
        store_country TEXT,
        store_city TEXT,
        store_count BIGINT,
        total_revenue DOUBLE,
        total_sales BIGINT,
        PRIMARY KEY (store_country, store_city)
    )
""")
session.execute("""
    CREATE TABLE report_top_reviews (
        product_id INT PRIMARY KEY,
        product_name TEXT,
        product_category TEXT,
        reviews_count INT,
        avg_rating DOUBLE,
        total_revenue DOUBLE
    )
""")
session.execute("""
    CREATE TABLE report_top_suppliers (
        supplier_id INT PRIMARY KEY,
        supplier_name TEXT,
        supplier_country TEXT,
        supplier_city TEXT,
        total_revenue DOUBLE,
        total_sales BIGINT,
        avg_product_price DOUBLE
    )
""")
session.execute("""
    CREATE TABLE report_suppliers_by_country (
        supplier_country TEXT PRIMARY KEY,
        supplier_count BIGINT,
        total_revenue DOUBLE,
        avg_price DOUBLE
    )
""")
session.execute("""
    CREATE TABLE report_rating_sales_corr (
        rating_group FLOAT PRIMARY KEY,
        sales_count BIGINT,
        total_revenue DOUBLE,
        avg_revenue DOUBLE,
        total_quantity BIGINT
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
dim_supplier = spark.read.jdbc(pg_url, "dim_supplier", properties=pg_props)

print(f"   fact_sales: {fact_sales.count()} строк")
print(f"   dim_product: {dim_product.count()} строк")
print(f"   dim_customer: {dim_customer.count()} строк")
print(f"   dim_store: {dim_store.count()} строк")
print(f"   dim_date: {dim_date.count()} строк")
print(f"   dim_supplier: {dim_supplier.count()} строк")

fact_sales.createOrReplaceTempView("fact_sales")
dim_product.createOrReplaceTempView("dim_product")
dim_customer.createOrReplaceTempView("dim_customer")
dim_store.createOrReplaceTempView("dim_store")
dim_date.createOrReplaceTempView("dim_date")
dim_supplier.createOrReplaceTempView("dim_supplier")

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

# ОТЧЕТ 6: Рейтинг и отзывы
print("\n7. Создаем отчет: Рейтинг и отзывы...")
product_ratings = spark.sql("""
    SELECT CAST(p.product_id AS INT) as product_id,
        p.product_name, p.product_category,
        CAST(ROUND(AVG(f.review_rating), 2) AS DOUBLE) as avg_rating,
        CAST(MAX(p.product_reviews) AS INT) as max_reviews
    FROM fact_sales f JOIN dim_product p ON f.product_id = p.product_id
    WHERE f.review_rating IS NOT NULL
    GROUP BY p.product_id, p.product_name, p.product_category
    ORDER BY avg_rating DESC LIMIT 20
""")
product_ratings.write \
    .format("org.apache.spark.sql.cassandra") \
    .option("keyspace", "etl_reports") \
    .option("table", "report_product_ratings") \
    .option("confirm.truncate", "true") \
    .mode("overwrite") \
    .save()
print("   ✅ report_product_ratings")

# ОТЧЕТ 7: Сравнение выручки по годам
print("\n8. Создаем отчет: Сравнение выручки по годам...")
yearly_revenue = spark.sql("""
    SELECT CAST(d.year AS INT) as year,
        CAST(COUNT(f.transaction_id) AS BIGINT) as total_sales,
        CAST(SUM(f.revenue) AS DOUBLE) as total_revenue,
        CAST(AVG(f.revenue) AS DOUBLE) as avg_order_value,
        CAST(SUM(f.quantity) AS BIGINT) as total_quantity
    FROM fact_sales f JOIN dim_date d ON f.date_id = d.date_id
    WHERE d.year IS NOT NULL
    GROUP BY d.year ORDER BY d.year
""")
yearly_revenue.write \
    .format("org.apache.spark.sql.cassandra") \
    .option("keyspace", "etl_reports") \
    .option("table", "report_yearly_revenue") \
    .option("confirm.truncate", "true") \
    .mode("overwrite") \
    .save()
print("   ✅ report_yearly_revenue")

# ОТЧЕТ 8: Распределение магазинов по локациям
print("\n9. Создаем отчет: Распределение магазинов по локациям...")
stores_by_location = spark.sql("""
    SELECT s.store_country, s.store_city,
        CAST(COUNT(DISTINCT s.store_id) AS BIGINT) as store_count,
        CAST(SUM(f.revenue) AS DOUBLE) as total_revenue,
        CAST(COUNT(f.transaction_id) AS BIGINT) as total_sales
    FROM fact_sales f JOIN dim_store s ON f.store_id = s.store_id
    GROUP BY s.store_country, s.store_city
    ORDER BY total_revenue DESC
""")
stores_by_location.write \
    .format("org.apache.spark.sql.cassandra") \
    .option("keyspace", "etl_reports") \
    .option("table", "report_stores_by_location") \
    .option("confirm.truncate", "true") \
    .mode("overwrite") \
    .save()
print("   ✅ report_stores_by_location")

# ОТЧЕТ 9: Топ продуктов по отзывам
print("\n10. Создаем отчет: Топ продуктов по отзывам...")
top_reviews = spark.sql("""
    SELECT CAST(p.product_id AS INT) as product_id,
        p.product_name, p.product_category,
        CAST(MAX(p.product_reviews) AS INT) as reviews_count,
        CAST(ROUND(AVG(f.review_rating), 2) AS DOUBLE) as avg_rating,
        CAST(SUM(f.revenue) AS DOUBLE) as total_revenue
    FROM fact_sales f JOIN dim_product p ON f.product_id = p.product_id
    WHERE p.product_reviews IS NOT NULL
    GROUP BY p.product_id, p.product_name, p.product_category
    ORDER BY reviews_count DESC LIMIT 20
""")
top_reviews.write \
    .format("org.apache.spark.sql.cassandra") \
    .option("keyspace", "etl_reports") \
    .option("table", "report_top_reviews") \
    .option("confirm.truncate", "true") \
    .mode("overwrite") \
    .save()
print("   ✅ report_top_reviews")

# ОТЧЕТ 10: Витрина по поставщикам
print("\n11. Создаем отчет: Витрина по поставщикам...")

top_suppliers = spark.sql("""
    SELECT CAST(s.supplier_id AS INT) as supplier_id,
        s.supplier_name, s.supplier_country, s.supplier_city,
        CAST(SUM(f.revenue) AS DOUBLE) as total_revenue,
        CAST(COUNT(DISTINCT f.transaction_id) AS BIGINT) as total_sales,
        CAST(AVG(f.revenue / f.quantity) AS DOUBLE) as avg_product_price
    FROM fact_sales f JOIN dim_supplier s ON f.supplier_id = s.supplier_id
    GROUP BY s.supplier_id, s.supplier_name, s.supplier_country, s.supplier_city
    ORDER BY total_revenue DESC LIMIT 5
""")
top_suppliers.write \
    .format("org.apache.spark.sql.cassandra") \
    .option("keyspace", "etl_reports") \
    .option("table", "report_top_suppliers") \
    .option("confirm.truncate", "true") \
    .mode("overwrite") \
    .save()
print("   ✅ report_top_suppliers")

suppliers_by_country = spark.sql("""
    SELECT s.supplier_country,
        CAST(COUNT(DISTINCT s.supplier_id) AS BIGINT) as supplier_count,
        CAST(SUM(f.revenue) AS DOUBLE) as total_revenue,
        CAST(AVG(f.revenue / f.quantity) AS DOUBLE) as avg_price
    FROM fact_sales f JOIN dim_supplier s ON f.supplier_id = s.supplier_id
    GROUP BY s.supplier_country
    ORDER BY total_revenue DESC
""")
suppliers_by_country.write \
    .format("org.apache.spark.sql.cassandra") \
    .option("keyspace", "etl_reports") \
    .option("table", "report_suppliers_by_country") \
    .option("confirm.truncate", "true") \
    .mode("overwrite") \
    .save()
print("   ✅ report_suppliers_by_country")

# ОТЧЕТ 11: Корреляция рейтинга и продаж
print("\n12. Создаем отчет: Корреляция рейтинга и продаж...")

rating_sales_corr = spark.sql("""
    SELECT CAST(ROUND(f.review_rating, 1) AS FLOAT) as rating_group,
        CAST(COUNT(f.transaction_id) AS BIGINT) as sales_count,
        CAST(SUM(f.revenue) AS DOUBLE) as total_revenue,
        CAST(AVG(f.revenue) AS DOUBLE) as avg_revenue,
        CAST(SUM(f.quantity) AS BIGINT) as total_quantity
    FROM fact_sales f
    WHERE f.review_rating IS NOT NULL
    GROUP BY ROUND(f.review_rating, 1)
    ORDER BY rating_group
""")
rating_sales_corr.write \
    .format("org.apache.spark.sql.cassandra") \
    .option("keyspace", "etl_reports") \
    .option("table", "report_rating_sales_corr") \
    .option("confirm.truncate", "true") \
    .mode("overwrite") \
    .save()
print("   ✅ report_rating_sales_corr")

print("\n" + "=" * 60)
print("✅ ВСЕ ОТЧЕТЫ УСПЕШНО СОЗДАНЫ В CASSANDRA!")
print("=" * 60)
print("\n📊 Таблицы в Cassandra (keyspace: etl_reports):")
tables = [
    "report_top_products", "report_revenue_by_category",
    "report_top_customers", "report_customers_by_country",
    "report_monthly_trends", "report_top_stores",
    "report_popular_products", "report_product_ratings",
    "report_yearly_revenue", "report_stores_by_location",
    "report_top_reviews", "report_top_suppliers",
    "report_suppliers_by_country", "report_rating_sales_corr"
]
for t in tables:
    print(f"   - etl_reports.{t}")

spark.stop()
