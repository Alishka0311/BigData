# BigDataSpark

Лабораторная работа №2  
ETL, реализованный с помощью Apache Spark

---

## Цель работы

Построить ETL-пайплайн на **Apache Spark**, который:

- загружает исходные данные из CSV-файлов;
- формирует аналитическую модель данных типа **звезда/снежинка** в PostgreSQL;
- создает 6 аналитических витрин в ClickHouse.

---

## Исходные данные

Используются 10 CSV-файлов с данными о продажах товаров для домашних животных.

Все исходные файлы находятся в папке `data/`.

---

## Что реализовано

В проекте выполнена обязательная часть задания:

- подготовлен `docker-compose.yml` с PostgreSQL, Spark и ClickHouse;
- реализована Spark-джоба загрузки CSV и построения аналитической модели в PostgreSQL;
- реализована Spark-джоба построения 6 витрин в ClickHouse;
- добавлена инструкция по запуску и проверке результата.

---

## Построенная модель в PostgreSQL

После выполнения ETL в PostgreSQL создаются:

### Staging:

- `mock_data`

### Факт:

- `fact_sales`

### Измерения:

- `dim_customer`
- `dim_pet`
- `dim_seller`
- `dim_product`
- `dim_store`
- `dim_supplier`

### Нормализованные справочники:

- `dim_product_category`
- `dim_brand`
- `dim_material`

Таким образом, в PostgreSQL строится аналитическая модель типа **звезда/снежинка**, сформированная средствами Apache Spark.

---

## Реализованные витрины в ClickHouse

Создаются 6 отдельных таблиц-отчетов:

### 1. `sales_product_report`

- топ-10 продуктов по выручке;
- выручка и количество продаж;
- средний рейтинг и количество отзывов.

### 2. `sales_customer_report`

- топ-10 клиентов по общей сумме покупок;
- распределение клиентов по странам;
- средний чек по клиенту.

### 3. `sales_time_report`

- месячные и годовые тренды продаж;
- сравнение выручки по периодам;
- средний размер заказа по месяцам.

### 4. `sales_store_report`

- топ-5 магазинов по выручке;
- распределение продаж по городам и странам;
- средний чек по магазину.

### 5. `sales_supplier_report`

- топ-5 поставщиков по выручке;
- средняя цена товаров по поставщикам;
- распределение продаж по странам поставщиков.

### 6. `product_quality_report`

- продукты с максимальным и минимальным рейтингом;
- связь рейтинга с объемом продаж;
- продукты с наибольшим количеством отзывов.

---

## Структура проекта

```text
BigDataSpark/
│
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
│
├── jobs/
│   ├── etl_to_postgres.py
│   └── build_clickhouse_reports.py
│
├── scripts/
│   ├── run_etl.sh
│   └── run_reports.sh
│
└── sql/
    ├── 01_clickhouse_init.sql
    ├── 02_postgres_checks.sql
    └── 03_clickhouse_checks.sql
```

---

## Запуск лабораторной работы

Все команды выполняются из папки `lr2`.

### 1. Запуск сервисов

```bash
docker compose up -d
```

Параметры подключения к PostgreSQL:

| Параметр | Значение |
| -------- | -------- |
| Host     | localhost |
| Port     | 5435 |
| Database | bdspark |
| User     | postgres |
| Password | postgres |

Параметры подключения к ClickHouse:

| Параметр | Значение |
| -------- | -------- |
| Host     | localhost |
| HTTP Port | 8123 |
| Native Port | 9000 |
| Database | bdspark_reports |
| User     | default |
| Password | clickhouse |

---

### 2. Запуск ETL в PostgreSQL

```bash
chmod +x scripts/run_etl.sh scripts/run_reports.sh
./scripts/run_etl.sh
```

Что делает эта джоба:

- читает все 10 CSV-файлов из папки `data/`;
- корректно обрабатывает многострочные текстовые поля;
- загружает исходные данные в `mock_data`;
- строит измерения и таблицу фактов в PostgreSQL.

---

### 3. Построение витрин в ClickHouse

```bash
./scripts/run_reports.sh
```

Что делает эта джоба:

- читает аналитическую модель из PostgreSQL;
- рассчитывает 6 отчетов;
- сохраняет каждый отчет в отдельную таблицу ClickHouse.

---

## Проверка результата

### 1. Проверка PostgreSQL

```bash
docker exec -i bd_spark_pg psql -U postgres -d bdspark < sql/02_postgres_checks.sql
```

Ожидается:

```text
mock_data_count = 10000
fact_sales_count = 10000
raw_sum = fact_sum
customer_nulls = 0
seller_nulls = 0
product_nulls = 0
store_nulls = 0
supplier_nulls = 0
```

---

### 2. Проверка ClickHouse

```bash
docker exec -i bd_spark_clickhouse clickhouse-client --user default --password clickhouse --multiquery < sql/03_clickhouse_checks.sql
```

Ожидается:

```text
sales_product_report  = 1000
sales_customer_report = 1000
sales_time_report     = 12
sales_store_report    = 10000
sales_supplier_report = 10000
product_quality_report = 1000
```

Также запросы `LIMIT` из проверочного файла должны возвращать реальные строки с данными.

---

## Фактический результат проверки

При проверке проекта были получены следующие результаты:

- `mock_data` содержит `10000` строк;
- `fact_sales` содержит `10000` строк;
- сумма `sale_total_price` в `mock_data` и `fact_sales` совпадает и равна `2529852.12`;
- в `fact_sales` отсутствуют `NULL` по внешним ключам;
- в ClickHouse созданы и заполнены все 6 витрин;
- количество строк в витринах:
  - `sales_product_report` = `1000`
  - `sales_customer_report` = `1000`
  - `sales_time_report` = `12`
  - `sales_store_report` = `10000`
  - `sales_supplier_report` = `10000`
  - `product_quality_report` = `1000`

---

## Примеры аналитических запросов

Топ-10 продуктов по выручке:

```bash
docker exec -it bd_spark_clickhouse clickhouse-client --user default --password clickhouse --query "SELECT product_name, total_revenue, total_quantity FROM bdspark_reports.sales_product_report ORDER BY sales_rank LIMIT 10"
```

Выручка по категориям продуктов:

```bash
docker exec -it bd_spark_clickhouse clickhouse-client --user default --password clickhouse --query "SELECT product_category, sum(total_revenue) AS revenue FROM bdspark_reports.sales_product_report GROUP BY product_category ORDER BY revenue DESC"
```

Топ-10 клиентов по общей сумме покупок:

```bash
docker exec -it bd_spark_clickhouse clickhouse-client --user default --password clickhouse --query "SELECT customer_first_name, customer_last_name, customer_country, total_revenue, avg_check FROM bdspark_reports.sales_customer_report ORDER BY customer_rank LIMIT 10"
```

Продажи по месяцам:

```bash
docker exec -it bd_spark_clickhouse clickhouse-client --user default --password clickhouse --query "SELECT sale_year, sale_month, total_revenue, avg_order_value FROM bdspark_reports.sales_time_report ORDER BY sale_year, sale_month"
```

Топ-5 магазинов по выручке:

```bash
docker exec -it bd_spark_clickhouse clickhouse-client --user default --password clickhouse --query "SELECT store_name, store_city, total_revenue FROM bdspark_reports.sales_store_report ORDER BY store_rank LIMIT 5"
```

Топ-5 поставщиков по выручке:

```bash
docker exec -it bd_spark_clickhouse clickhouse-client --user default --password clickhouse --query "SELECT supplier_name, supplier_country, total_revenue, avg_product_price FROM bdspark_reports.sales_supplier_report ORDER BY supplier_rank LIMIT 5"
```

Товары с наивысшим рейтингом:

```bash
docker exec -it bd_spark_clickhouse clickhouse-client --user default --password clickhouse --query "SELECT product_name, product_category, product_rating, product_reviews, total_quantity FROM bdspark_reports.product_quality_report ORDER BY rating_rank_desc LIMIT 10"
```

---

## Используемые скрипты

| Файл | Назначение |
| ---- | ---------- |
| `jobs/etl_to_postgres.py` | Spark-трансформация исходных данных из CSV в аналитическую модель PostgreSQL |
| `jobs/build_clickhouse_reports.py` | Spark-трансформация аналитической модели PostgreSQL в витрины ClickHouse |
| `scripts/run_etl.sh` | запуск Spark-джобы загрузки и построения модели |
| `scripts/run_reports.sh` | запуск Spark-джобы построения витрин |
| `sql/02_postgres_checks.sql` | проверка результата в PostgreSQL |
| `sql/03_clickhouse_checks.sql` | проверка результата в ClickHouse |


