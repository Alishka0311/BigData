# BigDataFlink

Лабораторная работа №3  
Streaming processing с помощью Apache Flink + Kafka + PostgreSQL

---

## Цель работы

Реализовать потоковый ETL-пайплайн, который:

- читает строки из CSV-файлов;
- преобразует каждую строку в JSON и отправляет в Kafka;
- читает поток из Kafka в Apache Flink;
- раскладывает данные в аналитическую модель типа **звезда** в PostgreSQL.

---

## Связь с `lr1` и `lr2`

В `lr3` сохранена логика предметной области из предыдущих лабораторных:

- используются те же 10 файлов `MOCK_DATA*.csv`;
- сохраняются знакомые таблицы `fact_sales`, `dim_customer`, `dim_pet`, `dim_seller`, `dim_product`, `dim_store`, `dim_supplier`;
- данные теперь загружаются не batch-процессом, а через streaming-цепочку `CSV -> JSON -> Kafka -> Flink -> PostgreSQL`.

---

## Что реализовано

- `docker-compose.yml` поднимает PostgreSQL, Kafka, Flink JobManager, Flink TaskManager и контейнер-продюсер;
- Python-приложение из папки `producer/` читает все CSV, корректно обрабатывает многострочные поля и отправляет JSON-сообщения в Kafka;
- Java/Flink-приложение из папки `flink-job/` читает Kafka topic в streaming-режиме;
- Flink-приложение формирует таблицы измерений и таблицу фактов в PostgreSQL через `upsert`;
- добавлены скрипты запуска и SQL-проверки результата.

---

## Структура проекта

```text
lr3/
├── docker-compose.yml
├── README.md
├── producer/
│   ├── Dockerfile
│   ├── producer.py
│   └── requirements.txt
├── flink-job/
│   ├── pom.xml
│   └── src/main/java/ru/bdsnowflake/lr3/
├── postgres/init/
│   └── 01_schema.sql
├── scripts/
│   ├── build_flink_job.sh
│   ├── run_producer.sh
│   └── submit_flink_job.sh
├── sql/
│   └── 01_checks.sql
└── исходные данные/
    ├── MOCK_DATA.csv
    ├── MOCK_DATA (1).csv
    ├── ...
    └── MOCK_DATA (9).csv
```

---

## Построенная модель в PostgreSQL

### Измерения

- `dim_customer`
- `dim_pet`
- `dim_seller`
- `dim_product`
- `dim_store`
- `dim_supplier`

### Факт

- `fact_sales`

Особенность `lr3`: так как поток идёт из Kafka и данные приходят непрерывно, в качестве первичных ключей используются стабильные текстовые ключи, вычисленные из содержимого записей. Это позволяет безопасно выполнять `upsert` при повторном запуске.

---

## Параметры сервисов

### PostgreSQL

- Host: `localhost`
- Port: `5436`
- Database: `pet_shop`
- User: `pet_user`
- Password: `pet_password`

### Kafka

- Bootstrap server из Docker-сети: `kafka:19092`
- Bootstrap server с хоста: `localhost:9094`
- Topic: `petshop.sales.raw`

### Flink UI

- [http://localhost:8081](http://localhost:8081)

---

## Как запускать лабораторную работу

Все команды выполняются из папки `lr3`.

### 1. Собрать Flink job

```bash
chmod +x scripts/*.sh
./scripts/build_flink_job.sh
```

Если Maven не установлен локально, это не проблема: сборка выполняется в контейнере `flink-build`.

---

### 2. Поднять инфраструктуру

```bash
docker compose up -d postgres kafka jobmanager taskmanager
```

После этого PostgreSQL автоматически создаст таблицы из `postgres/init/01_schema.sql`.

---

### 3. Отправить данные из CSV в Kafka

```bash
./scripts/run_producer.sh
```

Продюсер:

- читает все 10 CSV-файлов;
- корректно обрабатывает многострочный `product_description`;
- добавляет в сообщение поле `source_file`;
- отправляет каждую строку как отдельный JSON в Kafka topic `petshop.sales.raw`.
- при запуске автоматически пересобирает Docker-образ producer, чтобы не использовать старые зависимости.

---

### 4. Запустить Flink job

```bash
./scripts/submit_flink_job.sh
```

После запуска job можно проверить её статус в Flink UI: [http://localhost:8081](http://localhost:8081)

Дополнительно можно посмотреть список job:

```bash
docker compose exec jobmanager flink list
```

---

### 5. Проверить результат в PostgreSQL

```bash
docker exec -i lr3-postgres psql -U pet_user -d pet_shop < sql/01_checks.sql
```

Проверку лучше выполнять после успешной отправки данных в Kafka и после запуска Flink job. Если job только что отправлена, можно подождать несколько секунд и затем выполнить SQL-проверку.

Также можно выполнить выборки вручную, например:

```bash
docker exec -it lr3-postgres psql -U pet_user -d pet_shop
```

И затем:

```sql
select count(*) from fact_sales;

select
    c.customer_country,
    p.product_name,
    sum(f.sale_quantity) as total_qty,
    round(sum(f.sale_total_price), 2) as total_sum
from fact_sales f
join dim_customer c on c.customer_key = f.customer_key
join dim_product p on p.product_key = f.product_key
group by c.customer_country, p.product_name
order by total_sum desc
limit 10;
```

---

## Ожидаемый результат

После прохождения всего пайплайна:

- в Kafka будет отправлено 10000 JSON-сообщений;
- в PostgreSQL заполнится `fact_sales` на 10000 строк;
- измерения `dim_*` будут заполнены уникальными сущностями;
- повторный запуск продюсера и Flink job не должен ломать таблицы, так как запись выполняется через `upsert`.
