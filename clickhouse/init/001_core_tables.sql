CREATE DATABASE IF NOT EXISTS ecommerce;

-- Clean fact table: one row per valid event.
CREATE TABLE IF NOT EXISTS ecommerce.ecommerce_events
(
    event_time       DateTime('UTC'),
    event_date       Date,
    event_hour       DateTime('UTC'),
    hour_of_day      UInt8,
    day_of_week      UInt8,                    -- ISO: 1 = Monday ... 7 = Sunday
    event_type       LowCardinality(String),   -- view | cart | purchase
    product_id       UInt64,
    category_id      UInt64,                   -- 0 = unknown
    category_code    String,                   -- '' = unknown
    primary_category LowCardinality(String),   -- 'unknown' when missing
    sub_category     LowCardinality(String),
    category_depth   UInt8,
    brand            LowCardinality(String),   -- 'unknown' when missing
    price            Decimal(10, 2),           -- 0 when missing (see dq_flags)
    user_id          UInt64,
    user_session     String,                   -- '' = unknown
    dq_flags         Array(String),
    kafka_partition  UInt8,
    kafka_offset     UInt64,
    ingested_at      DateTime('UTC')
)
ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toYYYYMMDD(event_date)
PRIMARY KEY (event_type, event_date, category_id)
ORDER BY (event_type, event_date, category_id, product_id, user_id, event_time, user_session);

-- Rejected rows, with the reason and the exact Kafka position.
CREATE TABLE IF NOT EXISTS ecommerce.events_quarantine
(
    quarantined_at  DateTime('UTC'),
    reject_reason   LowCardinality(String),
    kafka_partition UInt8,
    kafka_offset    UInt64,
    kafka_timestamp DateTime('UTC'),
    raw_value       String
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(quarantined_at)
ORDER BY (reject_reason, quarantined_at)
TTL quarantined_at + INTERVAL 30 DAY;

-- One row per processed micro-batch: idempotency guard AND pipeline metrics.
CREATE TABLE IF NOT EXISTS ecommerce.batch_ledger
(
    query_name         LowCardinality(String),
    batch_id           UInt64,
    signature          String,                 -- e.g. "0:0-6666;1:0-6667;2:0-6666" (Kafka offset ranges)
    total_rows         UInt64,
    valid_rows         UInt64,                 -- rows written to ecommerce_events (after dedup)
    quarantined_rows   UInt64,
    duplicates_removed UInt64,
    processed_at       DateTime('UTC')
)
ENGINE = MergeTree
ORDER BY (query_name, signature);
