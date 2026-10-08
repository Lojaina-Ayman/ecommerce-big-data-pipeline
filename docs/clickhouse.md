# ClickHouse

DDL: `clickhouse/init/001..004`. Apply to an existing volume with `scripts\ch.ps1 -File <file>`. Fresh volumes run them automatically.

## Tables

| Table | Engine | Purpose |
|---|---|---|
| `ecommerce_events` | ReplacingMergeTree(ingested_at), daily partition | Clean facts. Key `(event_type, event_date, category_id)` is the index, full `ORDER BY` adds product, user, time, session so identical events collapse |
| `events_quarantine` | MergeTree, 30-day TTL | Rejected rows with reason, Kafka position, raw payload |
| `batch_ledger` | MergeTree | One row per batch: idempotency guard and throughput record |
| `daily_funnel`, `hourly_activity`, `category_sales`, `brand_sales` | SummingMergeTree | Additive aggregates (counts, purchase revenue) |
| `daily_uniques`, `daily_buyers` | AggregatingMergeTree | `uniqExact` states, merged at query time (distinct counts are not additive) |
| `session_state` | AggregatingMergeTree | One row per session (basis of abandonment) |
| `dq_results` | MergeTree, 180-day TTL | Airflow check evidence |

Views: `v_funnel_daily`, `v_revenue_daily`, `v_hourly_activity`, `v_cart_abandonment_daily`, `v_session_funnel`, `v_kpi_summary`, `v_dq_latest`.

## Design points

- A materialized view is an insert trigger. It sees only new inserts, so a new MV needs an explicit backfill.
- Aggregates are eventually merged: always `sum()` ... `GROUP BY`.
- Aliases must not reuse source column names inside aggregates (ClickHouse resolves them to the alias).
- Types: `LowCardinality(String)` for low-cardinality text, `Decimal(10,2)` for money, defaults instead of Nullable.
- Dedup identity excludes Kafka offsets on purpose (content duplicates have different offsets).
- Daily partitions: prune dates, allow dropping or reloading one day. Cost: more partitions to merge.

## Operations

Compaction after bulk loads (`OPTIMIZE ... FINAL`, never scheduled), aggregate rebuild from `... FINAL` (`rebuild_aggregates.sql`),
parts monitoring (`clickhouse_maintenance` DAG), read-only Superset user.

## Adopted optimizations (Phase 11)

- **O1 Compaction (`OPTIMIZE TABLE FINAL`):** Collapsed 167 parts to 31 parts; reduced W6 FINAL scan latency by -99.0% (1,988 ms -> 19 ms).
- **O4 Column Codecs:** Applied DoubleDelta on timestamps, Delta on offsets, ZSTD(3) on sessions, ZSTD(1) on user_id. Reduced storage on disk by 38.8% (1.51 GiB down to 943.7 MiB) with 0% query row change.
- **O6 Approximate Distinct Counts:** Adopted `uniqCombined64` for interactive BI cards; cut memory from 232.4 MiB to 5.51 MiB (-97.6%) with an error of only 0.192%.
- **O7 FileSystemCache:** Configured in `superset_config.py`; dropped repeat dashboard query load on ClickHouse from 119 queries to 0.
- **O8 Task Parallelism (`minPartitions=4`):** Improved Spark core balancing, delivering a +17.8% throughput increase.
- **O10 Micro-batch Scaling (600k):** Increased batch size to 600,000 rows, boosting write throughput to ~35,200 rows/s.
- **Rejected O2 (Monthly Partitions):** Marginal scan gains, but sacrifices single-day lifecycle and backfill pruning.
- **Rejected O3 (Filter-Aligned Key):** Gained on category/brand drilldowns (-76%), but severely regressed date queries (+150% to +540%).
- **Rejected O9 (Arrow Format):** 4.5% throughput regression compared to optimized HTTP JSON streaming.