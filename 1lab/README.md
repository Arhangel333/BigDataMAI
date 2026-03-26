# Лабораторная работа: Проектирование хранилища данных (схема "звезда")

## Цель работы
Спроектировать и реализовать хранилище данных для интернет-магазина товаров для домашних питомцев, преобразовав исходные данные из CSV-файлов в нормализованную схему "звезда" (таблицы фактов и измерений) с использованием PostgreSQL и Docker.

## Выполненные задачи

### 1. Анализ исходных данных
- Изучена структура 10 CSV-файлов (`MOCK_DATA (1).csv` ... `MOCK_DATA (10).csv`), каждый содержит 1000 строк (всего 10 000 записей)
- Выявлены сущности: покупатели, продавцы, товары, магазины, поставщики, продажи
- Определены типы данных для каждого поля

### 2. Создание инфраструктуры (Docker)
Создан файл `docker-compose.yml` для автоматического развертывания PostgreSQL:

```yaml
services:
  postgres:
    image: postgres:15
    container_name: db_sql
    env_file:
      - envfile.env
    volumes:
      - ./container:/data
      - ./container/script_me.sh:/docker-entrypoint-initdb.d/script_me.sh
```

- Переменные окружения: `POSTGRES_DB=pet_shop`, `POSTGRES_USER=admin`, `POSTGRES_PASSWORD=admin123`

### 3. Реализация DDL (создание таблиц)
Созданы таблицы:

**Измерения:**
- `customer` (покупатели) — ID, имя, возраст, email, страна, почтовый индекс, информация о питомце
- `seller` (продавцы) — ID, имя, email, страна, почтовый индекс
- `product` (товары) — ID, название, категория, цена, вес, размер, бренд, рейтинг, отзывы
- `store` (магазины) — ID, название, адрес, город, страна, телефон, email
- `supplier` (поставщики) — ID, название, контакт, email, телефон, адрес

**Таблица фактов:**
- `sale` — дата продажи, внешние ключи к измерениям, количество, итоговая сумма

**Связующие таблицы:**
- `seller_store` — связи продавцов с магазинами
- `supplier_store` — связи поставщиков с магазинами

### 4. Реализация DML (заполнение данными)
Скрипты для переноса данных из сырой таблицы `mock_data`:
- Заполнение измерений с использованием `DISTINCT` для уникальных значений
- Заполнение таблицы фактов всеми 10 000 продаж
- Преобразование типов данных (INTEGER, NUMERIC, DATE)
- Обработка NULL-значений

### 5. Автоматизация процесса
Скрипт инициализации `script_me.sh`:
- Генерирует SQL для импорта CSV-файлов
- Выполняет импорт в таблицу `mock_data`
- Запускает DDL и DML скрипты

При запуске контейнера скрипт выполняется автоматически благодаря монтированию в `/docker-entrypoint-initdb.d/`.

## Структура проекта

```
1lab/
├── docker-compose.yml              # Конфигурация Docker
├── envfile.env                     # Переменные окружения
├── container/
│   ├── script_me.sh               # Главный скрипт инициализации
│   ├── BDSnowflake/
│   │   └── исходные данные/       # 10 CSV файлов
│   │       ├── MOCK_DATA (1).csv
│   │       ├── MOCK_DATA (2).csv
│   │       └── ...
│   ├── db/
│   │   ├── copy.sql               # SQL для импорта CSV
│   │   ├── ddl_new_tables.sql     # Создание таблиц
│   │   └── dml_new_tables.sql     # Заполнение данными
│   └── init_scripts/
│       ├── csv_copy_script_maker.sh  # Генератор импорта
│       └── make_script.sh            # Сборщик скриптов
```

## Результаты

### Количество записей в таблицах

| Таблица | Количество строк |
|---------|-----------------|
| mock_data | 10 000 |
| customer | 1 000 |
| seller | 1 000 |
| product | 1 000 |
| store | 10 000 |
| supplier | 10 000 |
| sale | 10 000 |
| seller_store | 10 000 |
| supplier_store | 10 000 |

### Проверка целостности
- Все внешние ключи корректны
- Данные в таблице фактов соответствуют исходным продажам
- Уникальные сущности выделены в измерения

## Запуск проекта

```bash
# Клонировать репозиторий
git clone <repository-url>
cd 1lab

# Запустить контейнер
docker-compose up -d

# Проверить логи
docker-compose logs -f

# Подключиться к базе данных
docker exec -it db_sql psql -U admin -d pet_shop
```

## Проверка работы

```sql
-- Проверить количество записей
SELECT COUNT(*) FROM sale;  -- должно быть 10 000

-- Посмотреть продажи
SELECT s.sale_id, c.customer_first_name, p.product_name, s.sale_total_price
FROM sale s
JOIN customer c ON s.sale_customer_id = c.customer_id
JOIN product p ON s.sale_product_id = p.product_id
LIMIT 10;

-- Анализ продаж по категориям
SELECT p.product_category, SUM(s.sale_total_price) as total_revenue
FROM sale s
JOIN product p ON s.sale_product_id = p.product_id
GROUP BY p.product_category
ORDER BY total_revenue DESC;
```

## Выводы
- Спроектирована и реализована схема "звезда" для аналитического хранилища
- Автоматизирован процесс ETL (извлечение, трансформация, загрузка)
- Данные успешно перенесены из 10 CSV-файлов в нормализованную структуру
- Готово к выполнению аналитических запросов

## Используемые технологии
- PostgreSQL 15
- Docker / Docker Compose
- Bash scripting
- SQL (DDL, DML)

## Автор
[Ваше имя]
- GitHub: [@username](https://github.com/Arhangel333)
```
