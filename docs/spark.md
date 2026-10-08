# Spark job (Scala 2.13, Spark 4.0.2)

Build: `docker build -f spark/Dockerfile --target export --output type=local,dest=spark/apps spark` (sbt runs in Docker).
Run: the `spark-job` service (`--profile job`), client mode, packages resolved at start (Kafka source, ClickHouse connector + JDBC).

## Modules

| File | Content |
|---|---|
| `Config.scala` | Environment-driven settings |
| `Transformations.scala` | `parse` (from_json, all strings, `_corrupt_record`), `typed` (try_cast / try_to_timestamp), `withQuality`, `withFeatures` |
| `Sinks.scala` | `logSink` (debug counts), `noopSink` (benchmark) |
| `ClickHouseSink.scala` | Ledger check, dedup, writes, ledger row |
| `Main.scala` | Session, Kafka source, listener, queries (`clean`, `window` demo) |

## Quality rules (first failing rule wins)

malformed_json, invalid_timestamp, invalid_event_type, missing/invalid product_id, missing/invalid user_id, invalid_category_id,
invalid_price, negative_price, missing_price_on_purchase. Soft flags on kept rows: missing_user_session, missing_brand,
missing_category_code, missing_price, zero_price. Details: docs/data_quality.md.

## Features

event_date, event_hour, hour_of_day, day_of_week (ISO, Monday = 1), category_depth, primary_category, sub_category
(`unknown` when missing, via `try_element_at`), brand (`unknown` when missing).

## Checkpointing

`/opt/spark-checkpoints/<mode>_<sink>_v1` (named volume): `offsets/N` before a batch, `commits/N` after. Without it a restart would
re-read or skip data. One folder per query, never shared. A different sink is a different query.

## Tuning parameters

`SPARK_MAX_OFFSETS_PER_TRIGGER=600000`, `SPARK_TRIGGER_SECONDS=0`, `CH_WRITE_BATCH_SIZE=600000`, `CH_REPARTITION_BY_PARTITION=true`,
`CH_WRITE_FORMAT=json`, `KAFKA_MIN_PARTITIONS=4`, `SPARK_EXECUTOR_MEMORY=2g`, `SPARK_CORES_MAX=2`. Adopted values and evidence: docs/performance.md.

## Why aggregation is not here

Funnel and revenue math runs in ClickHouse materialized views (exact, cheap). The `window` mode (event-time window + 10-minute
watermark) exists to demonstrate stateful processing and late-data dropping.