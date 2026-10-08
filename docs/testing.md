# Testing

| Level | Test | How | Pass criterion |
|---|---|---|---|
| Golden end-to-end | `scripts\smoke-test.ps1` | 30 fixture messages through Connect, Kafka, Spark, ClickHouse | Prints SMOKE TEST PASSED (20+ assertions) |
| Rules | `tests/fixtures/bad_rows.csv` | One row per rule | Reasons match the table in data_quality.md |
| Business logic | `tests/fixtures/abandonment.csv` | Known sessions | Abandonment 3/4 sessions (session level), 2/4 users, ordered funnel 3-2-1 |
| Load orchestration | `airflow_load_test.csv` + `load_dataset` DAG | 6 rows | Kafka delta = ledger delta = 6 |
| Reconciliation | `clickhouse/queries/reconcile.sql` | after any load | all `ok = 1` |
| Drift detect and repair | duplicate insert, `data_quality` red, `rebuild_aggregates`, green | Airflow | See Phase 8 Step 9 |
| Performance | `bench.ps1`, `bench-workload.ps1`, `run-experiment.ps1` | scripts | docs/performance.md |
| Reproducibility | clean-clone test | docs/pipeline.md | bootstrap + smoke-test pass on a fresh clone |

Checklist per component (what was verified in each phase): Docker (`compose config`, healthy, networking by service name, volumes persist),
Kafka (3 partitions, offsets, consumer group, lag), Spark (schema, rules, checkpoint resume, micro-batches), ClickHouse (types, parts,
`EXPLAIN indexes = 1`, MV parity), Airflow (DAG parse, params, sensors, dq_results), Superset (connection, datasets, KPI parity, filters,
query_log evidence).

## Full-scale validation (Phase 10 & 11)

Kafka total = 42,448,764 (matches CSV lines - 1). Ledger identity holds: `42,448,764 = 42,418,544 valid + 0 quarantined + 30,220 duplicates`.
`FINAL` count is exactly 42,418,544 matching the Phase 2 distinct_rows profile. Reconcile shows all `ok = 1`.

Golden values come from the fixture files (hand-derived):
30 messages = 14 (bad_rows) + 10 (abandonment) + 6 (airflow_load_test).
Quarantined: 7 (one per rule).
Duplicates: 1 (rows 1 and 11 of bad_rows).
Valid written: 22. Event breakdown: 10 views, 7 carts, 5 purchases. Total revenue: 955.00.
Unique users: 15. Unique sessions: 15.
Cart abandonment rate: 5/7 (0.7143).