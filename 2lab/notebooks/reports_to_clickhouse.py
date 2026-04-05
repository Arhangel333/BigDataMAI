from pyspark.sql import SparkSession

print("=" * 60)
print("СОЗДАНИЕ ОТЧЕТОВ В CLICKHOUSE")
print("=" * 60)

# Создаем Spark сессию с драйверами
spark = SparkSession.builder \
    .appName("ReportsToClickHouse") \
    .config("spark.jars", "/home/jovyan/jars/postgresql-42.6.0.jar,/home/jovyan/jars/clickhouse-jdbc-0.4.6-shaded.jar") \
    .config("spark.driver.extraClassPath", "/home/jovyan/jars/postgresql-42.6.0.jar:/home/jovyan/jars/clickhouse-jdbc-0.4.6-shaded.jar") \
    .config("spark.executor.extraClassPath", "/home/jovyan/jars/postgresql-42.6.0.jar:/home/jovyan/jars/clickhouse-jdbc-0.4.6-shaded.jar") \
    .getOrCreate()

# Подключение к PostgreSQL (читаем модель звезда)
pg_url = "jdbc:postgresql://postgres:5432/etl_lab"
pg_props = {"user": "spark", "password": "spark123", "driver": "org.postgresql.Driver"}

# Подключение к ClickHouse (пишем отчеты)
ch_url = "jdbc:clickhouse://clickhouse:8123/default"
ch_props = {
    "user": "default",
    "password": "clickhouse123",
    "driver": "com.clickhouse.jdbc.ClickHouseDriver",
    "compress": "false"
}

print("\n1. Загружаем данные из PostgreSQL...")
# Загружаем таблицы модели звезда
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

# Создаем временные представления для SQL запросов
fact_sales.createOrReplaceTempView("fact_sales")
dim_product.createOrReplaceTempView("dim_product")
dim_customer.createOrReplaceTempView("dim_customer")
dim_store.createOrReplaceTempView("dim_store")
dim_date.createOrReplaceTempView("dim_date")

# ============================================
# ОТЧЕТ 1: Витрина продаж по продуктам
# ============================================
print("\n2. Создаем отчет: Витрина продаж по продуктам...")

# 1.1 Топ-10 самых продаваемых продуктов
top_products = spark.sql("""
    SELECT 
        p.product_id,
        p.product_name,
        p.product_category,
        SUM(f.quantity) as total_quantity,
        SUM(f.revenue) as total_revenue
    FROM fact_sales f
    JOIN dim_product p ON f.product_id = p.product_id
    GROUP BY p.product_id, p.product_name, p.product_category
    ORDER BY total_quantity DESC
    LIMIT 10
""")
top_products.write.jdbc(ch_url, "report_top_products", mode="overwrite", properties=ch_props)
print("   ✅ report_top_products")

# 1.2 Общая выручка по категориям продуктов
revenue_by_category = spark.sql("""
    SELECT 
        p.product_category,
        SUM(f.revenue) as total_revenue,
        SUM(f.quantity) as total_quantity,
        COUNT(DISTINCT f.transaction_id) as sales_count
    FROM fact_sales f
    JOIN dim_product p ON f.product_id = p.product_id
    GROUP BY p.product_category
    ORDER BY total_revenue DESC
""")
revenue_by_category.write.jdbc(ch_url, "report_revenue_by_category", mode="overwrite", properties=ch_props)
print("   ✅ report_revenue_by_category")

# ============================================
# ОТЧЕТ 2: Витрина продаж по клиентам
# ============================================
print("\n3. Создаем отчет: Витрина продаж по клиентам...")

# 2.1 Топ-10 клиентов по сумме покупок
top_customers = spark.sql("""
    SELECT 
        c.customer_id,
        CONCAT(c.customer_first_name, ' ', c.customer_last_name) as customer_name,
        c.customer_country,
        SUM(f.revenue) as total_spent,
        COUNT(f.transaction_id) as orders_count,
        AVG(f.revenue) as avg_order_value
    FROM fact_sales f
    JOIN dim_customer c ON f.customer_id = c.customer_id
    GROUP BY c.customer_id, c.customer_first_name, c.customer_last_name, c.customer_country
    ORDER BY total_spent DESC
    LIMIT 10
""")
top_customers.write.jdbc(ch_url, "report_top_customers", mode="overwrite", properties=ch_props)
print("   ✅ report_top_customers")

# 2.2 Распределение клиентов по странам
customers_by_country = spark.sql("""
    SELECT
        c.customer_country,
        COUNT(DISTINCT c.customer_id) as customers_count,
        SUM(f.revenue) as total_revenue,
        AVG(f.revenue) as avg_revenue_per_customer
    FROM dim_customer c
    JOIN fact_sales f ON c.customer_id = f.customer_id
    GROUP BY c.customer_country
    ORDER BY customers_count DESC
""")
customers_by_country.write.jdbc(ch_url, "report_customers_by_country", mode="overwrite", properties=ch_props)
print("   ✅ report_customers_by_country")

# ============================================
# ОТЧЕТ 3: Витрина продаж по времени
# ============================================
print("\n4. Создаем отчет: Витрина продаж по времени...")

# 3.1 Месячные тренды продаж
monthly_trends = spark.sql("""
    SELECT
        d.year,
        d.month,
        COUNT(f.transaction_id) as sales_count,
        SUM(f.revenue) as total_revenue,
        AVG(f.revenue) as avg_revenue,
        SUM(f.quantity) as total_quantity
    FROM fact_sales f
    JOIN dim_date d ON f.date_id = d.date_id
    GROUP BY d.year, d.month
    ORDER BY d.year, d.month
""")
monthly_trends.write.jdbc(ch_url, "report_monthly_trends", mode="overwrite", properties=ch_props)
print("   ✅ report_monthly_trends")

# ============================================
# ОТЧЕТ 4: Витрина продаж по магазинам
# ============================================
print("\n5. Создаем отчет: Витрина продаж по магазинам...")

# 4.1 Топ-5 магазинов по выручке
top_stores = spark.sql("""
    SELECT 
        s.store_id,
        s.store_name,
        s.store_city,
        s.store_country,
        SUM(f.revenue) as total_revenue,
        COUNT(f.transaction_id) as sales_count,
        AVG(f.revenue) as avg_revenue
    FROM fact_sales f
    JOIN dim_store s ON f.store_id = s.store_id
    GROUP BY s.store_id, s.store_name, s.store_city, s.store_country
    ORDER BY total_revenue DESC
    LIMIT 5
""")
top_stores.write.jdbc(ch_url, "report_top_stores", mode="overwrite", properties=ch_props)
print("   ✅ report_top_stores")

# ============================================
# ОТЧЕТ 5: Витрина качества продукции
# ============================================
print("\n6. Создаем отчет: Витрина качества продукции...")

# 5.1 Продукты с наибольшим количеством продаж (популярные)
popular_products = spark.sql("""
    SELECT 
        p.product_id,
        p.product_name,
        p.product_category,
        SUM(f.quantity) as total_sold,
        SUM(f.revenue) as total_revenue
    FROM fact_sales f
    JOIN dim_product p ON f.product_id = p.product_id
    GROUP BY p.product_id, p.product_name, p.product_category
    ORDER BY total_sold DESC
    LIMIT 20
""")
popular_products.write.jdbc(ch_url, "report_popular_products", mode="overwrite", properties=ch_props)
print("   ✅ report_popular_products")

print("\n" + "=" * 60)
print("✅ ВСЕ ОТЧЕТЫ УСПЕШНО СОЗДАНЫ В CLICKHOUSE!")
print("=" * 60)

print("\n📊 Список созданных таблиц в ClickHouse:")
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
    print(f"   - {t}")

spark.stop()