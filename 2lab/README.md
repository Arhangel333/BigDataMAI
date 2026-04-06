# ETL Лабораторная работа — Apache Spark + 5 СУБД

## Описание
ETL-пайплайн на Apache Spark (PySpark), который:
1. Загружает CSV-данные в **PostgreSQL**
2. Преобразует их в модель **«Звезда»** (dim + fact)
3. Формирует **14 аналитических отчётов** и записывает в **5 СУБД**

---

## Архитектура

```
CSV (mock_data) 
    → PostgreSQL (raw) 
    → Spark ETL 
    → PostgreSQL (Star Schema: dim_customer, dim_product, dim_store, dim_date, dim_supplier + fact_sales)
    → 14 отчётов → ClickHouse, MongoDB, Neo4j, Valkey, Cassandra
```

---

## Контейнеры

| Контейнер | Порт | Назначение |
|-----------|------|------------|
| `postgres_lab` | 5432 | Исходные данные + модель звезда |
| `clickhouse_lab` | 8123, 9000 | Отчёты (HTTP + native) |
| `mongodb_lab` | 27017 | Отчёты (документы) |
| `neo4j_lab` | 7474, 7687 | Отчёты (граф) |
| `valkey_lab` | 6379 | Отчёты (key-value) |
| `cassandra_lab` | 9042 | Отчёты (колоночная) |
| `spark_lab` | 8888, 4040 | Jupyter Lab + Spark |

---

## Запуск

```bash
# 1. Поднять все контейнеры
docker-compose up -d
```

При старте контейнера `spark_lab` автоматически устанавливаются все зависимости (`pymongo`, `neo4j`, `redis`, `cassandra-driver`) и создаётся keyspace в Cassandra.

---

## Модель «Звезда» в PostgreSQL

| Таблица | Описание |
|---------|----------|
| `dim_customer` | Измерение клиентов (id, имя, фамилия, страна) |
| `dim_product` | Измерение товаров (id, имя, категория, рейтинг, отзывы) |
| `dim_store` | Измерение магазинов (id, имя, город, страна) |
| `dim_date` | Измерение дат (id, полная дата, год, месяц, день) |
| `dim_supplier` | Измерение поставщиков (id, имя, email, страна, город) |
| `fact_sales` | Факт продаж (transaction_id, customer_id, product_id, store_id, supplier_id, date_id, revenue, quantity, review_rating) |

---

## 14 отчётов (витрин)

### Витрина продаж по продуктам
| # | Отчёт | Таблица |
|---|-------|---------|
| 1 | Топ-10 самых продаваемых продуктов | `report_top_products` |
| 2 | Выручка по категориям продуктов | `report_revenue_by_category` |
| 3 | Средний рейтинг и отзывы | `report_product_ratings` |

### Витрина продаж по клиентам
| # | Отчёт | Таблица |
|---|-------|---------|
| 4 | Топ-10 клиентов по сумме покупок | `report_top_customers` |
| 5 | Распределение клиентов по странам | `report_customers_by_country` |
| 6 | Средний чек клиента (avg_order_value) | в `report_top_customers` |

### Витрина продаж по времени
| # | Отчёт | Таблица |
|---|-------|---------|
| 7 | Месячные тренды продаж | `report_monthly_trends` |
| 8 | Сравнение выручки по годам | `report_yearly_revenue` |
| 9 | Средний заказ по месяцам (avg_revenue) | в `report_monthly_trends` |

### Витрина продаж по магазинам
| # | Отчёт | Таблица |
|---|-------|---------|
| 10 | Топ-5 магазинов по выручке | `report_top_stores` |
| 11 | Распределение по городам и странам | `report_stores_by_location` |
| 12 | Средний чек магазина (avg_revenue) | в `report_top_stores` |

### Витрина продаж по поставщикам
| # | Отчёт | Таблица |
|---|-------|---------|
| 13 | Топ-5 поставщиков по выручке | `report_top_suppliers` |
| 14 | Распределение по странам поставщиков | `report_suppliers_by_country` |
| 15 | Средняя цена товаров от поставщика | в `report_top_suppliers` |

### Витрина качества продукции
| # | Отчёт | Таблица |
|---|-------|---------|
| 16 | Наивысший/наименьший рейтинг | `report_product_ratings` |
| 17 | Корреляция рейтинга и продаж | `report_rating_sales_corr` |
| 18 | Топ продуктов по отзывам | `report_top_reviews` |

---

## Как проверить отчёты через браузер

### ClickHouse
| Параметр | Значение |
|----------|----------|
| URL | `http://localhost:8123/play` |
| Login | `default` |
| Password | `clickhouse123` |
| Пример запроса | `SELECT * FROM etl_reports.report_top_products LIMIT 10` |

### Neo4j
| Параметр | Значение |
|----------|----------|
| URL | `http://localhost:7474` |
| Login | `neo4j` |
| Password | `neo4j123` |
| Пример запроса | `MATCH (n:TopProduct) RETURN n LIMIT 10` |

### MongoDB (mongo-express)
| Параметр | Значение |
|----------|----------|
| URL | `http://localhost:8081` |
| Login | `admin` |
| Password | `admin123` |
| Как смотреть | Выбрать БД `etl_reports` → выбрать коллекцию → Browse |

### Valkey (Redis Commander)
| Параметр | Значение |
|----------|----------|
| URL | `http://localhost:8082` |
| Login | без пароля |
| Как смотреть | Ключи `report:*` → нажать для просмотра JSON |

### Jupyter Lab
| Параметр | Значение |
|----------|----------|
| URL | `http://localhost:8888` |
| Password / Token | `admin` |
| Spark UI | `http://localhost:4040` |
| Пример | Открыть ноутбук и выполнить запрос к любой СУБД |

---

## Проверка через командную строку (Cassandra — без веб-интерфейса)

### Cassandra

**Посмотреть список всех таблиц:**
```bash
docker exec -it cassandra_lab cqlsh -u cassandra -p cassandra123 -e "USE etl_reports; DESCRIBE TABLES;"
```

**Проверить данные в таблице:**
```bash
docker exec -it cassandra_lab cqlsh -u cassandra -p cassandra123 -e "SELECT * FROM etl_reports.report_top_products LIMIT 3;"
```

### PostgreSQL
```bash
docker exec -it postgres_lab psql -U spark -d etl_lab -c "SELECT * FROM fact_sales LIMIT 5;"
```

---

## Структура файлов

```
2lab/
├── docker-compose.yml          # Конфигурация 7 контейнеров
├── init_cassandra/init.sh      # Init-скрипт Cassandra (keyspace)
├── init_postgres.sql/init.sql  # Init-скрипт PostgreSQL
├── spark_init.sh               # Init-скрипт Spark (зависимости)
├── jars/                       # JDBC драйверы
│   ├── clickhouse-jdbc-0.4.6-shaded.jar
│   └── postgresql-42.6.0.jar
├── mock_data/                  # 10 CSV файлов
└── notebooks/
    ├── load_csv.py             # Загрузка CSV → PostgreSQL
    ├── star_schema_final_working.py  # Модель звезда
    ├── reports_to_clickhouse.py      # 14 отчётов → ClickHouse
    ├── reports_to_mongodb.py         # 14 отчётов → MongoDB
    ├── reports_to_neo4j.py           # 14 отчётов → Neo4j
    ├── reports_to_valkey.py          # 14 отчётов → Valkey
    ├── reports_to_cassandra.py       # 14 отчётов → Cassandra
    ├── script_spark.sh               # Полный пайплайн (PG + CH)
    └── sub_script_spark.sh           # MongoDB + Neo4j + Valkey + Cassandra
```

---

## Итого

- **5 СУБД** (PostgreSQL, ClickHouse, MongoDB, Neo4j, Valkey, Cassandra)
- **6 таблиц измерений/фактов** (Star Schema)
- **14 аналитических отчётов** в каждой из 5 СУБД
- **70 таблиц/коллекций/ключей/узлов** с данными
- Всё запускается через `docker-compose up -d`
