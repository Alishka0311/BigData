# BigDataTrino

Лабораторная работа №4  
ETL, реализованный с помощью Trino

---

## Цель работы

Построить ETL-пайплайн на **Trino**, который:

- загружает первые 5 CSV-файлов в ClickHouse;
- загружает следующие 5 CSV-файлов в PostgreSQL;
- объединяет оба источника через Trino;
- формирует аналитическую модель типа **snowflake** в ClickHouse;
- создает 6 аналитических витрин в ClickHouse.

---

## Исходные данные

Используются 10 CSV-файлов с данными о продажах товаров для домашних животных.

Файлы находятся в папке `исходные данные/`.

Распределение источников соответствует условию задания:

- ClickHouse: `MOCK_DATA.csv`, `MOCK_DATA (1).csv` ... `MOCK_DATA (4).csv`
- PostgreSQL: `MOCK_DATA (5).csv` ... `MOCK_DATA (9).csv`

---

## Что реализовано

- подготовлен `docker-compose.yml` с PostgreSQL, ClickHouse и Trino;
- добавлены init-скрипты для создания source- и target-таблиц;
- реализована загрузка CSV в ClickHouse и PostgreSQL отдельными shell-скриптами;
- реализован Trino-скрипт построения snowflake-модели в ClickHouse;
- реализован Trino-скрипт построения 6 витрин в ClickHouse;
- добавлены скрипты запуска и SQL-проверки результата.

---

## Построенная модель в ClickHouse

### Staging

- `bdtrino_source.mock_data`

### Факт

- `bdtrino_analytics.fact_sales`

### Измерения

- `bdtrino_analytics.dim_customer`
- `bdtrino_analytics.dim_pet`
- `bdtrino_analytics.dim_seller`
- `bdtrino_analytics.dim_product`
- `bdtrino_analytics.dim_store`
- `bdtrino_analytics.dim_supplier`

### Нормализованные справочники

- `bdtrino_analytics.dim_product_category`
- `bdtrino_analytics.dim_brand`
- `bdtrino_analytics.dim_material`

Таким образом, аналитическая модель реализована в виде **snowflake-схемы** внутри ClickHouse, а заполнение выполняется средствами Trino.

---

## Реализованные витрины в ClickHouse

Создаются 6 отдельных таблиц-отчетов:

### 1. `sales_product_report`

- выручка и количество продаж по продуктам;
- рейтинг продуктов и число отзывов;
- ранжирование продуктов по выручке.

### 2. `sales_customer_report`

- общая сумма покупок по клиентам;
- страна клиента;
- средний чек и количество заказов.

### 3. `sales_time_report`

- помесячная и погодовая динамика продаж;
- количество заказов;
- средний размер заказа.

### 4. `sales_store_report`

- выручка по магазинам;
- распределение по городам и странам;
- средний чек по магазину.

### 5. `sales_supplier_report`

- выручка по поставщикам;
- средняя цена товаров поставщика;
- количество уникальных продуктов.

### 6. `product_quality_report`

- рейтинг и количество отзывов по товарам;
- связь качества и объема продаж;
- группировка товаров по уровню рейтинга.

---

## Структура проекта

```text
lr4/
├── docker-compose.yml
├── README.md
├── clickhouse/
│   └── init/
│       └── 01_init_clickhouse.sql
├── postgres/
│   └── init/
│       └── 01_init_postgres.sql
├── scripts/
│   ├── init_sources.sh
│   ├── load_clickhouse_sources.sh
│   ├── load_postgres_sources.sh
│   ├── run_all.sh
│   ├── run_trino_etl.sh
│   └── run_trino_reports.sh
├── sql/
│   ├── 01_postgres_checks.sql
│   ├── 02_clickhouse_checks.sql
│   ├── 03_clickhouse_reset_analytics.sql
│   ├── 04_clickhouse_reset_reports.sql
│   └── trino/
│       ├── 01_build_snowflake.sql
│       └── 02_build_reports.sql
├── trino/
│   └── etc/
│       ├── catalog/
│       │   ├── clickhouse.properties
│       │   └── postgresql.properties
│       ├── config.properties
│       ├── jvm.config
│       └── node.properties
└── исходные данные/
    ├── MOCK_DATA.csv
    ├── MOCK_DATA (1).csv
    ├── ...
    └── MOCK_DATA (9).csv
```

---

## Запуск лабораторной работы

Все команды выполняются из папки `lr4`.

### 1. Поднять инфраструктуру

```bash
docker compose up -d
```

Параметры подключения к PostgreSQL:

| Параметр | Значение |
| -------- | -------- |
| Host     | localhost |
| Port     | 5437 |
| Database | bdtrino_source |
| User     | postgres |
| Password | postgres |

Параметры подключения к ClickHouse:

| Параметр | Значение |
| -------- | -------- |
| Host     | localhost |
| HTTP Port | 8124 |
| Native Port | 9001 |
| Database | bdtrino_analytics |
| User     | default |
| Password | clickhouse |

Параметры подключения к Trino:

| Параметр | Значение |
| -------- | -------- |
| Host     | localhost |
| Port     | 8082 |
| Catalog `clickhouse` | ClickHouse connector |
| Catalog `postgresql` | PostgreSQL connector |

---

### 2. Загрузить исходные данные в обе БД

```bash
chmod +x scripts/*.sh
./scripts/init_sources.sh
```

Что делает этот шаг:

- очищает `bdtrino_source.mock_data` в ClickHouse;
- загружает первые 5 CSV-файлов в ClickHouse;
- очищает `mock_data` в PostgreSQL;
- загружает следующие 5 CSV-файлов в PostgreSQL.

Ожидается:

- в ClickHouse source-таблице `5000` строк;
- в PostgreSQL source-таблице `5000` строк.

---

### 3. Построить snowflake-модель через Trino

```bash
./scripts/run_trino_etl.sh
```

Что делает этот шаг:

- очищает аналитические таблицы в ClickHouse;
- через Trino читает данные из `clickhouse.bdtrino_source.mock_data` и `postgresql.public.mock_data`;
- формирует измерения, нормализованные справочники и таблицу фактов в `bdtrino_analytics`.

---

### 4. Построить витрины через Trino

```bash
./scripts/run_trino_reports.sh
```

Что делает этот шаг:

- очищает таблицы витрин в ClickHouse;
- через Trino рассчитывает 6 отчетов;
- сохраняет каждый отчет в отдельную таблицу `bdtrino_analytics.*_report`.

---

### 5. Запуск одним скриптом

```bash
./scripts/run_all.sh
```

Скрипт последовательно:

- поднимает сервисы;
- загружает источники;
- строит аналитическую модель;
- строит витрины.

---

## Проверка результата

### 1. Проверка PostgreSQL

```bash
docker exec -i lr4-postgres psql -U postgres -d bdtrino_source < sql/01_postgres_checks.sql
```

Ожидается:

```text
postgres_rows = 5000
```

---

### 2. Проверка ClickHouse

```bash
docker exec -i lr4-clickhouse clickhouse-client --user default --password clickhouse --multiquery < sql/02_clickhouse_checks.sql
```

Ожидается:

```text
clickhouse_source_rows = 5000
fact_sales_rows = 10000
sales_product_report_rows > 0
sales_customer_report_rows > 0
sales_time_report_rows > 0
sales_store_report_rows > 0
sales_supplier_report_rows > 0
product_quality_report_rows > 0
```

Также в конце проверки должны выводиться реальные строки из `sales_product_report`.

---

## Используемые скрипты

| Файл | Назначение |
| ---- | ---------- |
| `postgres/init/01_init_postgres.sql` | создание source-таблицы `mock_data` в PostgreSQL |
| `clickhouse/init/01_init_clickhouse.sql` | создание source-, analytics- и report-таблиц в ClickHouse |
| `scripts/load_clickhouse_sources.sh` | загрузка первых 5 CSV в ClickHouse |
| `scripts/load_postgres_sources.sh` | загрузка следующих 5 CSV в PostgreSQL |
| `scripts/init_sources.sh` | общий запуск загрузки исходников |
| `scripts/run_trino_etl.sh` | построение snowflake-модели через Trino |
| `scripts/run_trino_reports.sh` | построение 6 витрин через Trino |
| `sql/trino/01_build_snowflake.sql` | Trino ETL из двух источников в аналитическую модель ClickHouse |
| `sql/trino/02_build_reports.sql` | Trino расчеты витрин в ClickHouse |
| `sql/01_postgres_checks.sql` | проверка количества строк в PostgreSQL |
| `sql/02_clickhouse_checks.sql` | проверка source, facts и витрин в ClickHouse |

---

## Итог

В результате лабораторной работы:

- источники распределены между ClickHouse и PostgreSQL по условию задания;
- Trino используется как единый SQL-движок для ETL между двумя источниками;
- аналитическая модель snowflake строится в ClickHouse;
- 6 витрин сохраняются в виде отдельных таблиц ClickHouse;
- проект оформлен в том же стиле, что и предыдущие лабораторные работы.
