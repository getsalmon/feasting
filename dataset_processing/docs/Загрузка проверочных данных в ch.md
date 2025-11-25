Для проверки данных загрузим исходные csv в clickhouse.

Для этого создаем бакет в minio, заливаем файл в него и перекладываем в ch

```sql
create database raw;
-- raw.events_csv definition
CREATE TABLE raw.events_csv
(
    `event_ts` DateTime('UTC'),
    `event_type` LowCardinality(String),
    `product_id` UInt32,
    `category_id` LowCardinality(String),
    `category_code` LowCardinality(String),
    `brand` String,
    `price` Decimal(9, 2),
    `user_id` UInt32,
    `user_session` UUID
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_ts)
ORDER BY (event_ts, user_id)
SETTINGS index_granularity = 8192;

insert into raw.events_csv (
    event_ts,
    event_type,
    product_id,
    category_id,
    category_code,
    brand,
    price,
    user_id,
    user_session
)
select 
    parseDateTimeBestEffort(event_time),
    event_type,
    product_id,
    category_id,
    category_code,
    brand,
    price,
    user_id,
    user_session
from s3(
    'http://minio:9000/raw-events/2019-Nov.csv', 
    'minioadmin', 
    'minioadmin'
)
;
```

Заодно пробуем второй вариант, с использованием clickhouse-client

Для этого создаем таблицу:
```sql
CREATE TABLE raw.events_csv_to_load
(

    `event_time_raw` String,
    `event_type` LowCardinality(String),
    `product_id` UInt32,
    `category_id` LowCardinality(String),
    `category_code` LowCardinality(String),
    `brand` String,
    `price` Decimal(9, 2),
    `user_id` UInt32,
    `user_session` UUID
)
ENGINE = MergeTree
ORDER BY (event_time_raw,
 user_id)
SETTINGS index_granularity = 8192;
```

Грузим данные следующим образом:
```zsh
clickhouse client --host localhost --user clickhouse --password clickhouse -q "insert into raw.events_csv_to_load format CSVWithNames" < 2019-Oct.csv
```

```

insert into raw.events_csv (
    event_ts,
    event_type,
    product_id,
    category_id,
    category_code,
    brand,
    price,
    user_id,
    user_session
)
select 
    parseDateTimeBestEffort(event_time),
    event_type,
    product_id,
    category_id,
    category_code,
    brand,
    price,
    user_id,
    user_session
from raw.events_csv_to_load
```