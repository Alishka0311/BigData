# BigDataSnowflake

Лабораторная работа №1  
Нормализация данных в аналитическую модель типа Snowflake

---

## Цель работы

Преобразовать исходные данные (CSV-файлы) в аналитическую модель данных типа **снежинка (Snowflake Schema)**.

---

##  Исходные данные

Используются 10 CSV-файлов с данными о продажах товаров для домашних животных.

Все CSV-файлы помещены в папку `data/` и автоматически монтируются в контейнер PostgreSQL.

---

##  Построенная модель

Реализована схема снежинка:

### Факт:
- `fact_sales`

### Измерения:
- `dim_customer`
- `dim_pet`
- `dim_seller`
- `dim_product`
- `dim_store`
- `dim_supplier`

### Нормализация измерения товара:
- `dim_product_category`
- `dim_brand`
- `dim_material`

Измерение `dim_product` разбито на подтаблицы, поэтому модель является схемой **Snowflake**.

---

## Структура проекта

```text
BigDataSnowflake/
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
└── sql/
    ├── 01_create_stage.sql
    ├── 02_load_csv.sql
    ├── 03_create_snowflake.sql
    ├── 04_load_dimensions.sql
    ├── 05_load_fact.sql
    └── 06_checks.sql
```


---

## Запуск лабораторной работы

### 1. Запуск базы данных

В корне проекта выполнить:

```bash
docker compose up -d
```

Параметры подключения:

| Параметр | Значение    |
| -------- | ----------- |
| Host     | localhost   |
| Port     | 5434        |
| Database | bdsnowflake |
| User     | postgres    |
| Password | postgres    |

---

### 2. Создание staging-таблицы

```bash
docker exec -i bd_snowflake_pg psql -U postgres -d bdsnowflake < sql/01_create_stage.sql
```

---

### 3. Автоматическая загрузка CSV-файлов

```bash
docker exec -i bd_snowflake_pg psql -U postgres -d bdsnowflake < sql/02_load_csv.sql
```

Проверка:

```bash
docker exec -i bd_snowflake_pg psql -U postgres -d bdsnowflake -c "SELECT count(*) FROM mock_data;"
```

Ожидается:

```text
10000
```

---

### 4. Создание схемы снежинка

```bash
docker exec -i bd_snowflake_pg psql -U postgres -d bdsnowflake < sql/03_create_snowflake.sql
```

---

### 5. Заполнение таблиц измерений

```bash
docker exec -i bd_snowflake_pg psql -U postgres -d bdsnowflake < sql/04_load_dimensions.sql
```

---

### 6. Заполнение таблицы фактов

```bash
docker exec -i bd_snowflake_pg psql -U postgres -d bdsnowflake < sql/05_load_fact.sql
```

Ожидаемый вывод:

```text
INSERT 0 10000
```

---

### 7. Проверка результата

Можно выполнить единый скрипт:

```bash
docker exec -i bd_snowflake_pg psql -U postgres -d bdsnowflake < sql/06_checks.sql
```

Или проверить вручную.

#### Проверка количества записей

```bash
docker exec -i bd_snowflake_pg psql -U postgres -d bdsnowflake -c "SELECT count(*) FROM mock_data;"
docker exec -i bd_snowflake_pg psql -U postgres -d bdsnowflake -c "SELECT count(*) FROM fact_sales;"
```

Ожидается:

```text
10000
10000
```

#### Проверка сумм

```bash
docker exec -i bd_snowflake_pg psql -U postgres -d bdsnowflake -c "SELECT (SELECT sum(sale_total_price) FROM mock_data) AS mock_sum, (SELECT sum(sale_total_price) FROM fact_sales) AS fact_sum;"
```

Суммы должны совпадать.

#### Проверка связей

```bash
docker exec -i bd_snowflake_pg psql -U postgres -d bdsnowflake -c "SELECT 
    (SELECT count(*) FROM fact_sales WHERE customer_key IS NULL) AS customer_null,
    (SELECT count(*) FROM fact_sales WHERE seller_key IS NULL) AS seller_null,
    (SELECT count(*) FROM fact_sales WHERE product_key IS NULL) AS product_null,
    (SELECT count(*) FROM fact_sales WHERE store_key IS NULL) AS store_null,
    (SELECT count(*) FROM fact_sales WHERE supplier_key IS NULL) AS supplier_null;"
```

Ожидается, что все значения равны `0`.

---

##  Пример аналитического запроса

```bash
docker exec -i bd_snowflake_pg psql -U postgres -d bdsnowflake -c "
SELECT
    c.country,
    pr.product_name,
    sum(f.sale_quantity) AS total_qty,
    round(sum(f.sale_total_price), 2) AS total_sum
FROM fact_sales f
JOIN dim_customer c ON c.customer_key = f.customer_key
JOIN dim_product pr ON pr.product_key = f.product_key
GROUP BY c.country, pr.product_name
ORDER BY total_sum DESC
LIMIT 10;"
```

Пример результата:

```text
   country   | product_name | total_qty | total_sum
-------------+--------------+-----------+-----------
 China       | Dog Food     |      3307 | 159164.96
 China       | Bird Cage    |      3107 | 154014.81
 China       | Cat Toy      |      3059 | 140382.08
 Indonesia   | Bird Cage    |      2750 | 122692.64
 Indonesia   | Dog Food     |      2230 | 102301.43
```

---

## Используемые скрипты

| Файл                          | Назначение                                          |
| ----------------------------- | --------------------------------------------------- |
| `sql/01_create_stage.sql`     | создание staging-таблицы `mock_data`                |
| `sql/02_load_csv.sql`         | автоматическая загрузка 10 CSV-файлов               |
| `sql/03_create_snowflake.sql` | создание таблиц фактов и измерений                  |
| `sql/04_load_dimensions.sql`  | загрузка измерений                                  |
| `sql/05_load_fact.sql`        | загрузка таблицы фактов                             |
| `sql/06_checks.sql`           | проверка результата                                 |

---

## Повторный запуск

Если требуется повторно выполнить загрузку измерений и фактов, сначала очистить аналитические таблицы:

```bash
docker exec -i bd_snowflake_pg psql -U postgres -d bdsnowflake < sql/00_truncate_all.sql
```

Затем заново выполнить:

```bash
docker exec -i bd_snowflake_pg psql -U postgres -d bdsnowflake < sql/04_load_dimensions.sql
docker exec -i bd_snowflake_pg psql -U postgres -d bdsnowflake < sql/05_load_fact.sql
docker exec -i bd_snowflake_pg psql -U postgres -d bdsnowflake < sql/06_checks.sql
```

