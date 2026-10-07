CREATE TABLE IF NOT EXISTS ecommerce.dq_results
(
    checked_at DateTime('UTC') DEFAULT now(),
    dag_id     LowCardinality(String),
    run_id     String,
    check_name LowCardinality(String),
    category   LowCardinality(String),
    status     LowCardinality(String),
    observed   Float64,
    expected   String,
    detail     String
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(checked_at)
ORDER BY (checked_at, dag_id, check_name)
TTL toDateTime(checked_at) + INTERVAL 180 DAY;

CREATE OR REPLACE VIEW ecommerce.v_dq_latest AS
SELECT dag_id, check_name, category,
       argMax(status,   checked_at) AS latest_status,
       argMax(observed, checked_at) AS latest_observed,
       argMax(expected, checked_at) AS latest_expected,
       argMax(detail,   checked_at) AS latest_detail,
       max(checked_at)              AS last_checked
FROM ecommerce.dq_results
GROUP BY dag_id, check_name, category;