# Performance Results (Full-Scale 42.4M Run)

Environment: Windows 11 + WSL2 (12 GB Docker limit), Docker Desktop, 1 Spark worker (2 cores, 3 GB).

## Stage 1: Kafka Connect -> Kafka
- **Ingestion Volume:** 42,448,764 events (~5.3 GB CSV)
- **Mid-file restart behavior:** Validated resume behavior (Stage 1c). Re-ingestion without data loss or corruption.
- **Kafka Volume Size:** 5.5 GB (compressed lz4 topic log).

## Stage 2 & 3: Spark Optimization Experiments
- **Winning Configuration:** `ch_300k_b300k_norepart`
- **Max Offsets per Trigger:** 300,000
- **ClickHouse Write Batch Size:** 300,000
- **Repartition By Partition:** `false` (avoids single-thread day-level bottlenecking, allows maximum parallel write streams into ClickHouse)
- **Peak Ingestion Throughput:** ~31,000 rows/sec

## Stage 4 & 5: Full Run Verification
| Metric | Value |
|---|---|
| Raw CSV Events | 42,448,764 |
| Deduplicated Events in ClickHouse | 42,418,544 |
| Quarantined Records | 0 |
| Active Parts in `ecommerce_events` | 167 parts |
| Final Consumer Lag | 0 |
| Raw vs Compressed Size | 5.95 GiB -> 1.48 GiB (4.0x compression) |
| Total ClickHouse Storage Footprint | 2.9 GiB |

## Query Benchmark at 42M Rows (`bench.ps1`)
| Test | Runs | Avg Latency (ms) | Rows Read | Bytes Read | Peak Memory |
|---|---|---|---|---|---|
| `abandon_agg` | 3 | 6,530 ms | 18,612,772 | 535.44 MiB | 1.17 GiB |
| `abandon_raw` | 3 | 7,483 ms | 42,418,544 | 1.58 GiB | 1.11 GiB |
| `brands_agg` | 3 | 17 ms | 23,449 | 440.19 KiB | 965.09 KiB |
| `brands_raw` | 3 | 87 ms | 2,046,745 | 21.32 MiB | 3.37 MiB |
| `funnel_agg` | 3 | 11 ms | 1,238 | 10.93 KiB | 261.21 KiB |
| `funnel_raw` | 3 | 98 ms | 42,418,544 | 40.46 MiB | 2.33 MiB |
| `revenue_day_agg` | 3 | 25 ms | 1,238 | 23.02 KiB | 842.01 KiB |
| `revenue_day_raw` | 3 | 105 ms | 42,418,544 | 444.99 MiB | 10.06 MiB |
| `users_agg` | 3 | 1,186 ms | 40 | 15.31 KiB | 974.19 MiB |
| `users_raw` | 3 | 7,870 ms | 42,418,544 | 1.86 GiB | 829.12 MiB |

## Airflow Data Quality Audit
- **DAG Execution:** `data_quality` passed 100% (5/5 tasks green).
- **Checks Verified:** Null checks, date boundaries, zero-price filter, ledger integrity, sketch aggregations.
