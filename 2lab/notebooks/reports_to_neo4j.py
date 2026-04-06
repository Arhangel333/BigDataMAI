from pyspark.sql import SparkSession
from neo4j import GraphDatabase

print("=" * 60)
print("СОЗДАНИЕ ОТЧЕТОВ В NEO4J")
print("=" * 60)

# Spark — только для чтения из PostgreSQL
spark = SparkSession.builder \
    .appName("ReportsToNeo4j") \
    .config("spark.jars", "/home/jovyan/jars/postgresql-42.6.0.jar") \
    .config("spark.driver.extraClassPath", "/home/jovyan/jars/postgresql-42.6.0.jar") \
    .getOrCreate()

# Neo4j — прямое подключение
driver = GraphDatabase.driver("bolt://neo4j:7687", auth=("neo4j", "neo4j123"))

pg_url = "jdbc:postgresql://postgres:5432/etl_lab"
pg_props = {"user": "spark", "password": "spark123", "driver": "org.postgresql.Driver"}

def run_query(query, params=None):
    with driver.session() as session:
        session.run(query, params or {})

def save_to_neo4j(df, label, key_field):
    data = [row.asDict() for row in df.collect()]
    if not data:
        return
    for doc in data:
        for k, v in doc.items():
            if hasattr(v, 'item'):
                doc[k] = v.item()
    # Очищаем и заполняем
    run_query(f"MATCH (n:{label}) DETACH DELETE n")
    for doc in data:
        sets = ", ".join(f"n.{k} = ${k}" for k in doc if k != key_field)
        run_query(f"MERGE (n:{label} {{{key_field}: ${key_field}}}) SET {sets}", doc)
    print(f"   ✅ {label} ({len(data)} узлов)")

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
    SELECT p.product_id, p.product_name, p.product_category,
        CAST(SUM(f.quantity) AS INT) as total_quantity,
        CAST(SUM(f.revenue) AS FLOAT) as total_revenue
    FROM fact_sales f JOIN dim_product p ON f.product_id = p.product_id
    GROUP BY p.product_id, p.product_name, p.product_category
    ORDER BY total_quantity DESC LIMIT 10
""")
save_to_neo4j(top_products, "TopProduct", "product_id")

revenue_by_category = spark.sql("""
    SELECT p.product_category as category,
        CAST(SUM(f.revenue) AS FLOAT) as total_revenue,
        CAST(SUM(f.quantity) AS INT) as total_quantity,
        CAST(COUNT(DISTINCT f.transaction_id) AS INT) as sales_count
    FROM fact_sales f JOIN dim_product p ON f.product_id = p.product_id
    GROUP BY p.product_category ORDER BY total_revenue DESC
""")
save_to_neo4j(revenue_by_category, "Category", "category")

# ============================================
# ОТЧЕТ 2: Витрина продаж по клиентам
# ============================================
print("\n3. Создаем отчет: Витрина продаж по клиентам...")

top_customers = spark.sql("""
    SELECT c.customer_id,
        CONCAT(c.customer_first_name, ' ', c.customer_last_name) as customer_name,
        c.customer_country,
        CAST(SUM(f.revenue) AS FLOAT) as total_spent,
        CAST(COUNT(f.transaction_id) AS INT) as orders_count,
        CAST(AVG(f.revenue) AS FLOAT) as avg_order_value
    FROM fact_sales f JOIN dim_customer c ON f.customer_id = c.customer_id
    GROUP BY c.customer_id, c.customer_first_name, c.customer_last_name, c.customer_country
    ORDER BY total_spent DESC LIMIT 10
""")
save_to_neo4j(top_customers, "TopCustomer", "customer_id")

customers_by_country = spark.sql("""
    SELECT c.customer_country as country,
        CAST(COUNT(DISTINCT c.customer_id) AS INT) as customers_count,
        CAST(SUM(f.revenue) AS FLOAT) as total_revenue,
        CAST(AVG(f.revenue) AS FLOAT) as avg_revenue_per_customer
    FROM dim_customer c JOIN fact_sales f ON c.customer_id = f.customer_id
    GROUP BY c.customer_country ORDER BY customers_count DESC
""")
save_to_neo4j(customers_by_country, "Country", "country")

# ============================================
# ОТЧЕТ 3: Витрина продаж по времени
# ============================================
print("\n4. Создаем отчет: Витрина продаж по времени...")

monthly_trends = spark.sql("""
    SELECT CAST(d.year AS INT) as year, CAST(d.month AS INT) as month,
        CAST(COUNT(f.transaction_id) AS INT) as sales_count,
        CAST(SUM(f.revenue) AS FLOAT) as total_revenue,
        CAST(AVG(f.revenue) AS FLOAT) as avg_revenue,
        CAST(SUM(f.quantity) AS INT) as total_quantity
    FROM fact_sales f JOIN dim_date d ON f.date_id = d.date_id
    WHERE d.year IS NOT NULL
    GROUP BY d.year, d.month ORDER BY d.year, d.month
""")
save_to_neo4j(monthly_trends, "MonthTrend", "year")

# ============================================
# ОТЧЕТ 4: Витрина продаж по магазинам
# ============================================
print("\n5. Создаем отчет: Витрина продаж по магазинам...")

top_stores = spark.sql("""
    SELECT s.store_id, s.store_name, s.store_city, s.store_country,
        CAST(SUM(f.revenue) AS FLOAT) as total_revenue,
        CAST(COUNT(f.transaction_id) AS INT) as sales_count,
        CAST(AVG(f.revenue) AS FLOAT) as avg_revenue
    FROM fact_sales f JOIN dim_store s ON f.store_id = s.store_id
    GROUP BY s.store_id, s.store_name, s.store_city, s.store_country
    ORDER BY total_revenue DESC LIMIT 5
""")
save_to_neo4j(top_stores, "TopStore", "store_id")

# ============================================
# ОТЧЕТ 5: Витрина качества продукции
# ============================================
print("\n6. Создаем отчет: Витрина качества продукции...")

popular_products = spark.sql("""
    SELECT p.product_id, p.product_name, p.product_category,
        CAST(SUM(f.quantity) AS INT) as total_sold,
        CAST(SUM(f.revenue) AS FLOAT) as total_revenue
    FROM fact_sales f JOIN dim_product p ON f.product_id = p.product_id
    GROUP BY p.product_id, p.product_name, p.product_category
    ORDER BY total_sold DESC LIMIT 20
""")
save_to_neo4j(popular_products, "PopularProduct", "product_id")

# ОТЧЕТ 6: Рейтинг и отзывы
print("\n7. Создаем отчет: Рейтинг и отзывы...")
product_ratings = spark.sql("""
    SELECT p.product_id, p.product_name, p.product_category,
        CAST(ROUND(AVG(f.review_rating), 2) AS FLOAT) as avg_rating,
        CAST(MAX(p.product_reviews) AS INT) as max_reviews
    FROM fact_sales f JOIN dim_product p ON f.product_id = p.product_id
    WHERE f.review_rating IS NOT NULL
    GROUP BY p.product_id, p.product_name, p.product_category
    ORDER BY avg_rating DESC LIMIT 20
""")
save_to_neo4j(product_ratings, "ProductRating", "product_id")

# ОТЧЕТ 7: Сравнение выручки по годам
print("\n8. Создаем отчет: Сравнение выручки по годам...")
yearly_revenue = spark.sql("""
    SELECT CAST(d.year AS INT) as year,
        CAST(COUNT(f.transaction_id) AS INT) as total_sales,
        CAST(SUM(f.revenue) AS FLOAT) as total_revenue,
        CAST(AVG(f.revenue) AS FLOAT) as avg_order_value,
        CAST(SUM(f.quantity) AS INT) as total_quantity
    FROM fact_sales f JOIN dim_date d ON f.date_id = d.date_id
    WHERE d.year IS NOT NULL
    GROUP BY d.year ORDER BY d.year
""")
save_to_neo4j(yearly_revenue, "YearlyRevenue", "year")

# ОТЧЕТ 8: Распределение магазинов по локациям
print("\n9. Создаем отчет: Распределение магазинов по локациям...")
stores_by_location = spark.sql("""
    SELECT s.store_country as country, s.store_city as city,
        CAST(COUNT(DISTINCT s.store_id) AS INT) as store_count,
        CAST(SUM(f.revenue) AS FLOAT) as total_revenue,
        CAST(COUNT(f.transaction_id) AS INT) as total_sales
    FROM fact_sales f JOIN dim_store s ON f.store_id = s.store_id
    GROUP BY s.store_country, s.store_city
    ORDER BY total_revenue DESC
""")
save_to_neo4j(stores_by_location, "StoreLocation", "country")

# ОТЧЕТ 9: Топ продуктов по отзывам
print("\n10. Создаем отчет: Топ продуктов по отзывам...")
top_reviews = spark.sql("""
    SELECT p.product_id, p.product_name, p.product_category,
        CAST(MAX(p.product_reviews) AS INT) as reviews_count,
        CAST(ROUND(AVG(f.review_rating), 2) AS FLOAT) as avg_rating,
        CAST(SUM(f.revenue) AS FLOAT) as total_revenue
    FROM fact_sales f JOIN dim_product p ON f.product_id = p.product_id
    WHERE p.product_reviews IS NOT NULL
    GROUP BY p.product_id, p.product_name, p.product_category
    ORDER BY reviews_count DESC LIMIT 20
""")
save_to_neo4j(top_reviews, "TopReview", "product_id")

# ОТЧЕТ 10: Витрина по поставщикам
print("\n11. Создаем отчет: Витрина по поставщикам...")

top_suppliers = spark.sql("""
    SELECT s.supplier_id, s.supplier_name, s.supplier_country, s.supplier_city,
        CAST(SUM(f.revenue) AS FLOAT) as total_revenue,
        CAST(COUNT(DISTINCT f.transaction_id) AS INT) as total_sales,
        CAST(AVG(f.revenue / f.quantity) AS FLOAT) as avg_product_price
    FROM fact_sales f JOIN dim_supplier s ON f.supplier_id = s.supplier_id
    GROUP BY s.supplier_id, s.supplier_name, s.supplier_country, s.supplier_city
    ORDER BY total_revenue DESC LIMIT 5
""")
save_to_neo4j(top_suppliers, "TopSupplier", "supplier_id")

suppliers_by_country = spark.sql("""
    SELECT s.supplier_country as country,
        CAST(COUNT(DISTINCT s.supplier_id) AS INT) as supplier_count,
        CAST(SUM(f.revenue) AS FLOAT) as total_revenue,
        CAST(AVG(f.revenue / f.quantity) AS FLOAT) as avg_price
    FROM fact_sales f JOIN dim_supplier s ON f.supplier_id = s.supplier_id
    GROUP BY s.supplier_country
    ORDER BY total_revenue DESC
""")
save_to_neo4j(suppliers_by_country, "SupplierCountry", "country")

# ОТЧЕТ 11: Корреляция рейтинга и продаж
print("\n12. Создаем отчет: Корреляция рейтинга и продаж...")

rating_sales_corr = spark.sql("""
    SELECT CAST(ROUND(f.review_rating, 1) AS FLOAT) as rating_group,
        CAST(COUNT(f.transaction_id) AS INT) as sales_count,
        CAST(SUM(f.revenue) AS FLOAT) as total_revenue,
        CAST(AVG(f.revenue) AS FLOAT) as avg_revenue,
        CAST(SUM(f.quantity) AS INT) as total_quantity
    FROM fact_sales f
    WHERE f.review_rating IS NOT NULL
    GROUP BY ROUND(f.review_rating, 1)
    ORDER BY rating_group
""")
save_to_neo4j(rating_sales_corr, "RatingSales", "rating_group")

print("\n" + "=" * 60)
print("✅ ВСЕ ОТЧЕТЫ УСПЕШНО СОЗДАНЫ В NEO4J!")
print("=" * 60)
print("📊 Узлы: TopProduct, Category, TopCustomer, Country, MonthTrend, TopStore, PopularProduct, ProductRating, YearlyRevenue, StoreLocation, TopReview, TopSupplier, SupplierCountry, RatingSales")
print("📊 Открыть: http://localhost:7474 (neo4j / neo4j123)")

driver.close()
spark.stop()
