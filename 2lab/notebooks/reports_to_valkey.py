from pyspark.sql import SparkSession
import redis
import json

print("=" * 60)
print("СОЗДАНИЕ ОТЧЕТОВ В VALKEY")
print("=" * 60)

# Создаем Spark сессию
spark = SparkSession.builder \
    .appName("ReportsToValkey") \
    .config("spark.jars", "/home/jovyan/jars/postgresql-42.6.0.jar") \
    .config("spark.driver.extraClassPath", "/home/jovyan/jars/postgresql-42.6.0.jar") \
    .getOrCreate()

# Подключение к Valkey
r = redis.Redis(host='valkey', port=6379, password='valkey123', decode_responses=True)

pg_url = "jdbc:postgresql://postgres:5432/etl_lab"
pg_props = {"user": "spark", "password": "spark123", "driver": "org.postgresql.Driver"}

def save_to_valkey(df, key):
    data = [row.asDict() for row in df.collect()]
    # Конвертируем типы
    for doc in data:
        for k, v in doc.items():
            if hasattr(v, 'item'):
                doc[k] = v.item()
    r.set(f"report:{key}", json.dumps(data, default=str))
    print(f"   ✅ {key} ({len(data)} записей)")

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
    SELECT p.product_id, p.product_name, p.product_category,
        CAST(SUM(f.quantity) AS INT) as total_quantity,
        CAST(SUM(f.revenue) AS FLOAT) as total_revenue
    FROM fact_sales f JOIN dim_product p ON f.product_id = p.product_id
    GROUP BY p.product_id, p.product_name, p.product_category
    ORDER BY total_quantity DESC LIMIT 10
""")
save_to_valkey(top_products, "top_products")

revenue_by_category = spark.sql("""
    SELECT p.product_category,
        CAST(SUM(f.revenue) AS FLOAT) as total_revenue,
        CAST(SUM(f.quantity) AS INT) as total_quantity,
        CAST(COUNT(DISTINCT f.transaction_id) AS INT) as sales_count
    FROM fact_sales f JOIN dim_product p ON f.product_id = p.product_id
    GROUP BY p.product_category ORDER BY total_revenue DESC
""")
save_to_valkey(revenue_by_category, "revenue_by_category")

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
save_to_valkey(top_customers, "top_customers")

customers_by_country = spark.sql("""
    SELECT c.customer_country,
        CAST(COUNT(DISTINCT c.customer_id) AS INT) as customers_count,
        CAST(SUM(f.revenue) AS FLOAT) as total_revenue,
        CAST(AVG(f.revenue) AS FLOAT) as avg_revenue_per_customer
    FROM dim_customer c JOIN fact_sales f ON c.customer_id = f.customer_id
    GROUP BY c.customer_country ORDER BY customers_count DESC
""")
save_to_valkey(customers_by_country, "customers_by_country")

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
save_to_valkey(monthly_trends, "monthly_trends")

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
save_to_valkey(top_stores, "top_stores")

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
save_to_valkey(popular_products, "popular_products")

print("\n" + "=" * 60)
print("✅ ВСЕ ОТЧЕТЫ УСПЕШНО СОЗДАНЫ В VALKEY!")
print("=" * 60)

print("\n📊 Ключи в Valkey:")
keys = r.keys("report:*")
for k in sorted(keys):
    print(f"   - {k}")

spark.stop()
