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
dim_supplier = spark.read.jdbc(pg_url, "dim_supplier", properties=pg_props)

print(f"   fact_sales: {fact_sales.count()} строк")
print(f"   dim_product: {dim_product.count()} строк")
print(f"   dim_customer: {dim_customer.count()} строк")
print(f"   dim_store: {dim_store.count()} строк")
print(f"   dim_date: {dim_date.count()} строк")
print(f"   dim_supplier: {dim_supplier.count()} строк")

# Создаем временные представления для SQL запросов
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

# ============================================
# ОТЧЕТ 6: Рейтинг и отзывы
# ============================================
print("\n7. Создаем отчет: Рейтинг и отзывы...")

product_ratings = spark.sql("""
    SELECT
        p.product_id, p.product_name, p.product_category,
        ROUND(AVG(f.review_rating), 2) as avg_rating,
        MAX(p.product_reviews) as max_reviews
    FROM fact_sales f
    JOIN dim_product p ON f.product_id = p.product_id
    WHERE f.review_rating IS NOT NULL
    GROUP BY p.product_id, p.product_name, p.product_category
    ORDER BY avg_rating DESC
    LIMIT 20
""")
product_ratings.write.jdbc(ch_url, "report_product_ratings", mode="overwrite", properties=ch_props)
print("   ✅ report_product_ratings")

# ============================================
# ОТЧЕТ 7: Сравнение выручки по годам
# ============================================
print("\n8. Создаем отчет: Сравнение выручки по годам...")

yearly_revenue = spark.sql("""
    SELECT
        d.year,
        COUNT(f.transaction_id) as total_sales,
        SUM(f.revenue) as total_revenue,
        AVG(f.revenue) as avg_order_value,
        SUM(f.quantity) as total_quantity
    FROM fact_sales f
    JOIN dim_date d ON f.date_id = d.date_id
    WHERE d.year IS NOT NULL
    GROUP BY d.year
    ORDER BY d.year
""")
yearly_revenue.write.jdbc(ch_url, "report_yearly_revenue", mode="overwrite", properties=ch_props)
print("   ✅ report_yearly_revenue")

# ============================================
# ОТЧЕТ 8: Распределение магазинов по локациям
# ============================================
print("\n9. Создаем отчет: Распределение магазинов по локациям...")

stores_by_location = spark.sql("""
    SELECT
        s.store_country, s.store_city,
        COUNT(DISTINCT s.store_id) as store_count,
        SUM(f.revenue) as total_revenue,
        COUNT(f.transaction_id) as total_sales
    FROM fact_sales f
    JOIN dim_store s ON f.store_id = s.store_id
    GROUP BY s.store_country, s.store_city
    ORDER BY total_revenue DESC
""")
stores_by_location.write.jdbc(ch_url, "report_stores_by_location", mode="overwrite", properties=ch_props)
print("   ✅ report_stores_by_location")

# ============================================
# ОТЧЕТ 9: Топ продуктов по отзывам
# ============================================
print("\n10. Создаем отчет: Топ продуктов по отзывам...")

top_reviews = spark.sql("""
    SELECT
        p.product_id, p.product_name, p.product_category,
        MAX(p.product_reviews) as reviews_count,
        ROUND(AVG(f.review_rating), 2) as avg_rating,
        SUM(f.revenue) as total_revenue
    FROM fact_sales f
    JOIN dim_product p ON f.product_id = p.product_id
    WHERE p.product_reviews IS NOT NULL
    GROUP BY p.product_id, p.product_name, p.product_category
    ORDER BY reviews_count DESC
    LIMIT 20
""")
top_reviews.write.jdbc(ch_url, "report_top_reviews", mode="overwrite", properties=ch_props)
print("   ✅ report_top_reviews")

# ============================================
# ОТЧЕТ 10: Витрина по поставщикам
# ============================================
print("\n11. Создаем отчет: Витрина по поставщикам...")

# Топ-5 поставщиков по выручке
top_suppliers = spark.sql("""
    SELECT
        s.supplier_id, s.supplier_name, s.supplier_country, s.supplier_city,
        SUM(f.revenue) as total_revenue,
        COUNT(DISTINCT f.transaction_id) as total_sales,
        AVG(f.revenue / f.quantity) as avg_product_price
    FROM fact_sales f
    JOIN dim_supplier s ON f.supplier_id = s.supplier_id
    GROUP BY s.supplier_id, s.supplier_name, s.supplier_country, s.supplier_city
    ORDER BY total_revenue DESC
    LIMIT 5
""")
top_suppliers.write.jdbc(ch_url, "report_top_suppliers", mode="overwrite", properties=ch_props)
print("   ✅ report_top_suppliers")

# Распределение по странам поставщиков
suppliers_by_country = spark.sql("""
    SELECT
        s.supplier_country,
        COUNT(DISTINCT s.supplier_id) as supplier_count,
        SUM(f.revenue) as total_revenue,
        AVG(f.revenue / f.quantity) as avg_price
    FROM fact_sales f
    JOIN dim_supplier s ON f.supplier_id = s.supplier_id
    GROUP BY s.supplier_country
    ORDER BY total_revenue DESC
""")
suppliers_by_country.write.jdbc(ch_url, "report_suppliers_by_country", mode="overwrite", properties=ch_props)
print("   ✅ report_suppliers_by_country")

# ============================================
# ОТЧЕТ 11: Корреляция рейтинга и продаж
# ============================================
print("\n12. Создаем отчет: Корреляция рейтинга и продаж...")

rating_sales_corr = spark.sql("""
    SELECT
        ROUND(f.review_rating, 1) as rating_group,
        COUNT(f.transaction_id) as sales_count,
        SUM(f.revenue) as total_revenue,
        AVG(f.revenue) as avg_revenue,
        SUM(f.quantity) as total_quantity
    FROM fact_sales f
    WHERE f.review_rating IS NOT NULL
    GROUP BY ROUND(f.review_rating, 1)
    ORDER BY rating_group
""")
rating_sales_corr.write.jdbc(ch_url, "report_rating_sales_corr", mode="overwrite", properties=ch_props)
print("   ✅ report_rating_sales_corr")

print("\n" + "=" * 60)
print("✅ ВСЕ ОТЧЕТЫ УСПЕШНО СОЗДАНЫ В CLICKHOUSE!")
print("=" * 60)

print("\n📊 Список созданных таблиц в ClickHouse:")
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
    print(f"   - {t}")

spark.stop()