# Airflow (3.3, LocalExecutor)

Services: api-server, scheduler, dag-processor, triggerer (not used by our DAGs, can be stopped). Shared code: `airflow/plugins/ecom_lib`.
All checks write to `ecommerce.dq_results`. `v_dq_latest` shows the latest status per check.

| DAG | Schedule | Purpose | Key parameters |
|---|---|---|---|
| `pipeline_health` | every 10 min | Kafka, topic (3 partitions), ClickHouse and tables, Connect, Spark job, lag | `strict` (warnings become failures) |
| `load_dataset` | manual | Drop CSV, wait for Connect, Kafka delta, ClickHouse ledger delta, reconcile | `csv_path`, `wait_timeout_minutes` |
| `data_quality` | daily | Rules, aggregate reconciliation, revenue and duplicate checks, Kafka-ledger reconciliation | date range, ratio thresholds |
| `rebuild_aggregates` | manual | Truncate and rebuild aggregates from deduplicated raw | `confirm`, `force_while_streaming`, `optimize_raw` |
| `clickhouse_maintenance` | daily | Parts health (threshold from the server's own setting), sizes, quarantine summary | `optimize_aggregates` |

Design: Airflow never triggers micro-batches (Spark does that). Gates use `retries=0` (a failed assertion won't improve on retry),
health tasks retry (network flakiness). Sensors use `reschedule` mode. Optional services (Connect, Spark) are warnings in normal mode,
errors in strict mode.

Lag = Kafka end offsets - ledger total (Spark does not commit consumer-group offsets).
Great Expectations was not used (ClickHouse support unverified, heavy dependency). The checks are SQL assertions with the same
semantics, stored as data.

Run: `docker compose exec airflow-scheduler airflow dags test <dag_id>` for quick feedback, or trigger from the UI.
Memory: the full orchestrated load needs Kafka, Connect, Spark, ClickHouse, and Airflow together, which needs more than 10 GB. Validated at sample scale (Phase 8).