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

## Optimization results (Phase 11)

| Metric | Phase 10 baseline | After optimization | Change |
|---|---|---|---|
| Full drain time (ledger started -> finished) | ~23 min (Phase 10) | ~19.5 min | **-15.2% duration** |
| End-to-end write throughput | ~30,500 rows/s | ~35,200 rows/s | **+15.4% throughput** |
| ClickHouse size on disk (compressed) | 1.51 GiB | 943.73 MiB | **-38.8% storage saved** |
| Active parts (post-compaction) | 167 parts | 31 parts (1 per day) | **-81.4% parts** |
| Workload: W6 FINAL scan latency | 1,988 ms | 19 ms | **-99.0% latency** |
| Workload: W1 day-type scan rows read | 73,574 rows | 16,240 rows | **-77.9% I/O reduction** |
| Workload: W2 category-brand scan rows read | 36,319,739 rows | 21,377,390 rows | **-41.1% I/O reduction** |
| Superset distinct-count card memory | 232.4 MiB (`uniqExact`) | 5.51 MiB (`uniqCombined64`) | **-97.6% memory reduction** |
| Superset repeated dashboard load queries | 119 queries hit ClickHouse | 0 queries (File Cache hit) | **100% offloaded** |
| Container memory caps (suggested sum) | ~12.5 GiB | 10.75 GiB | **Right-sized per measured peaks** |

**Rejected / No-Change Findings:**
* **W4 User Journey Sorting:** Ordering primary key by `user_id` rejected because no business requirement exists for user journey drilldowns and projections double table disk size.
* **Monthly Partitioning (O2):** Rejected because collapsing to 1 partition eliminates partition-level drop/attach lifecycle management and single-day repair pruning without query wins.
* **Filter-Aligned Primary Key (O3):** Rejected because gaining on category/brand drilldowns regressed date-filtering queries by +150% to +540%.
* **Binary Arrow Format (O9):** Rejected because Arrow IPC serialization yielded 4.5% lower throughput than native JSON HTTP streaming under Java 17.
* **Checkpoint Compaction:** Audit proved Spark checkpoint footprint is 204 KiB with negligible overhead.
