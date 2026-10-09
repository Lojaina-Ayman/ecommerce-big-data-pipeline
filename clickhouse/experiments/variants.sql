-- B: ONE monthly partition instead of daily
CREATE TABLE IF NOT EXISTS ecommerce.ev_b AS ecommerce.ecommerce_events
ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toYYYYMM(event_date)
PRIMARY KEY (event_type, event_date, category_id)
ORDER BY (event_type, event_date, category_id, product_id, user_id, event_time, user_session);

-- C: key aligned with the dashboard filters (event type, category, brand), then date
CREATE TABLE IF NOT EXISTS ecommerce.ev_c AS ecommerce.ecommerce_events
ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toYYYYMMDD(event_date)
PRIMARY KEY (event_type, primary_category, brand, event_date)
ORDER BY (event_type, primary_category, brand, event_date, product_id, user_id, event_time, user_session, category_id);


-- D: baseline layout + codecs on the heavy columns
CREATE TABLE IF NOT EXISTS ecommerce.ev_d
(
    event_time       DateTime('UTC') CODEC(DoubleDelta, ZSTD(1)),
    event_date       Date,
    event_hour       DateTime('UTC') CODEC(DoubleDelta, ZSTD(1)),
    hour_of_day      UInt8,
    day_of_week      UInt8,
    event_type       LowCardinality(String),
    product_id       UInt64,
    category_id      UInt64,
    category_code    String,
    primary_category LowCardinality(String),
    sub_category     LowCardinality(String),
    category_depth   UInt8,
    brand            LowCardinality(String),
    price            Decimal(10, 2),
    user_id          UInt64 CODEC(ZSTD(1)),
    user_session     String CODEC(ZSTD(3)),
    dq_flags         Array(String),
    kafka_partition  UInt8,
    kafka_offset     UInt64 CODEC(Delta, ZSTD(1)),
    ingested_at      DateTime('UTC') CODEC(DoubleDelta, ZSTD(1))
)
ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toYYYYMMDD(event_date)
PRIMARY KEY (event_type, event_date, category_id)
ORDER BY (event_type, event_date, category_id, product_id, user_id, event_time, user_session);
