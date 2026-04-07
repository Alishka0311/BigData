# BigDataSpark

Лабораторная работа №2 по дисциплине Big Data.  
Проект реализует обязательную часть задания на Apache Spark с использованием PostgreSQL и ClickHouse.

## Соответствие заданию

В обязательной части задания требовалось:

1. Использовать 10 CSV-файлов как источник данных.
2. Подготовить `docker-compose.yml` с сервисами PostgreSQL, Spark и ClickHouse.
3. Загрузить исходные данные в PostgreSQL.
4. Построить аналитическую модель данных звезда/снежинка средствами Spark.
5. Построить 6 отчетов средствами Spark и загрузить их в ClickHouse.
6. Описать проект и способ запуска в `README.md`.

В этом проекте все пункты ОБЯЗАТЕЛЬНОЙ части выполнены.

## Что находится в проекте

```text
lr2/
├── docker-compose.yml
├── README.md
├── data/
│   ├── MOCK_DATA.csv
│   ├── MOCK_DATA (1).csv
│   ├── MOCK_DATA (2).csv
│   ├── MOCK_DATA (3).csv
│   ├── MOCK_DATA (4).csv
│   ├── MOCK_DATA (5).csv
│   ├── MOCK_DATA (6).csv
│   ├── MOCK_DATA (7).csv
│   ├── MOCK_DATA (8).csv
│   └── MOCK_DATA (9).csv
├── jobs/
│   ├── etl_to_postgres.py
│   └── build_clickhouse_reports.py
├── scripts/
│   ├── run_etl.sh
│   └── run_reports.sh
└── sql/
    ├── 01_clickhouse_init.sql
    ├── 02_postgres_checks.sql
    └── 03_clickhouse_checks.sql
```

## Используемые сервисы

### PostgreSQL

- Host: `localhost`
- Port: `5435`
- Database: `bdspark`
- User: `postgres`
- Password: `postgres`

### ClickHouse

- HTTP port: `8123`
- Native port: `9000`
- Database: `bdspark_reports`
- User: `default`
- Password: `clickhouse`

### Spark

Spark запускается в отдельном контейнере и используется для двух ETL-джоб:

- `etl_to_postgres.py`  
  Загружает CSV в PostgreSQL и строит аналитическую модель.
- `build_clickhouse_reports.py` 
  Читает модель из PostgreSQL, рассчитывает витрины и пишет их в ClickHouse.

## Аналитическая модель в PostgreSQL

После выполнения ETL в PostgreSQL создаются:

- staging-таблица `mock_data`
- таблица фактов `fact_sales`
- измерения `dim_customer`, `dim_pet`, `dim_seller`, `dim_product`, `dim_store`, `dim_supplier`
- нормализованные справочники `dim_product_category`, `dim_brand`, `dim_material`

В проекте реализована аналитическая модель звезда/снежинка в PostgreSQL.  
Построение модели выполняется средствами Apache Spark на основе исходных CSV-файлов.

## Реализованные отчеты в ClickHouse

В ClickHouse создаются 6 обязательных витрин:

1. `sales_product_report`  
   Продажи по продуктам: топ-10 продуктов по выручке, выручка и количество продаж, средний рейтинг и число отзывов.
2. `sales_customer_report`  
   Продажи по клиентам: топ-10 клиентов по сумме покупок, распределение по странам, средний чек.
3. `sales_time_report`  
   Продажи по времени: месячные и годовые тренды, сравнение периодов, средний размер заказа по месяцам.
4. `sales_store_report`  
   Продажи по магазинам: топ-5 магазинов по выручке, распределение продаж по городам и странам, средний чек.
5. `sales_supplier_report`  
   Продажи по поставщикам: топ-5 поставщиков по выручке, средняя цена товаров, распределение по странам поставщиков.
6. `product_quality_report`  
   Качество продукции: товары с максимальным и минимальным рейтингом, связь рейтинга с объемом продаж, число отзывов.

## Порядок запуска

Все команды выполняются из папки [`lr2`](/Users/alinakaracarova/Desktop/BDSnowflake/lr2).

### 1. Поднять контейнеры

```bash
docker compose up -d
```

### 2. Запустить загрузку данных и построение модели в PostgreSQL

```bash
chmod +x scripts/run_etl.sh scripts/run_reports.sh
./scripts/run_etl.sh
```

### 3. Запустить построение витрин в ClickHouse

```bash
./scripts/run_reports.sh
```

## Проверка результата

### Проверка PostgreSQL

```bash
docker exec -i bd_spark_pg psql -U postgres -d bdspark < sql/02_postgres_checks.sql
```

Ожидаемый результат:

- `mock_data_count = 10000`
- `fact_sales_count = 10000`
- `raw_sum = fact_sum`
- `customer_nulls = 0`
- `seller_nulls = 0`
- `product_nulls = 0`
- `store_nulls = 0`
- `supplier_nulls = 0`

### Проверка ClickHouse

```bash
docker exec -i bd_spark_clickhouse clickhouse-client \
  --user default \
  --password clickhouse \
  --multiquery < sql/03_clickhouse_checks.sql
```

Ожидаемый результат:

- `sales_product_report` содержит `1000` строк
- `sales_customer_report` содержит `1000` строк
- `sales_time_report` содержит `12` строк
- `sales_store_report` содержит `10000` строк
- `sales_supplier_report` содержит `10000` строк
- `product_quality_report` содержит `1000` строк
- итоговые `SELECT ... LIMIT` возвращают данные

## Фактический результат проверки

При проверке проекта были получены следующие результаты:

- в PostgreSQL таблица `mock_data` содержит `10000` строк
- в PostgreSQL таблица `fact_sales` содержит `10000` строк
- сумма `sale_total_price` в `mock_data` и `fact_sales` совпадает: `2529852.12`
- в `fact_sales` отсутствуют `NULL` по внешним ключам
- в ClickHouse созданы и заполнены все 6 витрин
- количество строк в витринах:
  - `sales_product_report` = `1000`
  - `sales_customer_report` = `1000`
  - `sales_time_report` = `12`
  - `sales_store_report` = `10000`
  - `sales_supplier_report` = `10000`
  - `product_quality_report` = `1000`

## Быстрая ручная проверка

Топ-10 продуктов по выручке:

```bash
docker exec -it bd_spark_clickhouse clickhouse-client \
  --user default \
  --password clickhouse \
  --query "SELECT product_name, total_revenue, total_quantity FROM bdspark_reports.sales_product_report ORDER BY sales_rank LIMIT 10"
```

Топ-5 магазинов по выручке:

```bash
docker exec -it bd_spark_clickhouse clickhouse-client \
  --user default \
  --password clickhouse \
  --query "SELECT store_name, store_city, total_revenue FROM bdspark_reports.sales_store_report ORDER BY store_rank LIMIT 5"
```

