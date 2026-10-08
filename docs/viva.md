# Viva guide

## 1. The 60-second story

We replay 42 million eCommerce events as a stream. Kafka Connect feeds Kafka; Spark cleans, quarantines, and enriches the events and loads
them into ClickHouse; materialized views keep aggregates ready; Superset reads only aggregates; Airflow proves the data is right. Everything runs
in Docker on one laptop and is reproducible from a clean clone.

## 2. What / Why / Problem / Where / If removed

| Technology | What | Why we used it | Problem solved | Where | If removed |
|---|---|---|---|---|---|
| Docker + Compose | Containers and one file describing them | Reproducible, isolated | "Works on my machine" | Everything | Manual installs of 8 products |
| Kafka | Distributed commit log | Buffering and replay, keyed ordering | Ingest and processing speeds differ; need replay | Between Connect and Spark | Spark would read files directly: no buffer, no replay, no decoupling |
| Kafka Connect | Runtime for source/sink connectors | Declarative file ingestion with tracked progress | Moving a 5.3 GB CSV without custom code | Before Kafka | A custom producer to write and maintain |
| Spark Structured Streaming | Distributed micro-batch engine | Scalable cleaning with SQL and Scala | Cleaning, typing, quarantine, features | Kafka to ClickHouse | A hand-written consumer: no checkpoint, no parallelism |
| Scala | JVM language | Native for Spark, typed | Safe, fast transformations | Spark job | Python/PySpark (works, slower and untyped here) |
| ClickHouse | Columnar OLAP database | Fast scans and aggregation, MVs | Analytics over 42M rows in milliseconds | After Spark | Postgres: row store, slow aggregation |
| Materialized views | Insert-time pre-aggregation | Dashboard speed | Re-scanning the raw table | Inside ClickHouse | Every chart scans tens of millions of rows |
| Airflow | Workflow scheduler | Orchestrate and record checks | Operational confidence, repair | Beside the pipeline | Manual checks, no evidence |
| PostgreSQL | Relational database | Metadata for Airflow and Superset | State storage | Orchestration/BI | Those tools cannot start |
| Superset | BI web application | Dashboards on SQL | Communicating results | After ClickHouse | No interactive view of KPIs |

## 3. Concept glossary (with our examples)

| Concept | One-liner |
|---|---|
| Big Data | Data too large or fast for one simple process: 42M rows, replayed as a stream |
| Data pipeline | Automated flow from source to insight (this project) |
| ETL vs ELT | Transform before loading (ETL: Spark cleans before ClickHouse) vs after (ELT: raw first). We do ETL for cleaning, ELT-style aggregation in ClickHouse |
| Streaming vs batch | Continuous small units vs one large job. Spark micro-batches sit between |
| Micro-batch | Spark processes new offsets every trigger (we used 0 s = back-to-back) |
| Producer / consumer | Writes to / reads from Kafka (Connect is our producer, Spark our consumer) |
| Topic / partition | Named log / its parallel pieces (3) |
| Offset | Position of a message within one partition |
| Consumer group | Consumers sharing partitions. Spark tracks its own offsets instead |
| KRaft | Kafka manages its own metadata without ZooKeeper |
| DataFrame | Typed distributed table |
| Transformation vs action | Lazy plan (`select`) vs execution (`count`) |
| Watermark | "Ignore events older than newest event time minus X": used in the window demo |
| Checkpointing | Persisted offsets so a restart resumes: `offsets/N`, `commits/N` |
| OLAP | Analytical, read-heavy workloads over many rows |
| MergeTree | ClickHouse storage: sorted immutable parts merged in the background |
| Partitioning vs ordering | Coarse split (day) vs physical sort and sparse index (`ORDER BY`) |
| DAG / task / scheduler | Dependency graph / unit of work / the process that runs them |
| Container / network / volume | Isolated process / private DNS between containers / storage that outlives containers |
| Superset dataset / chart / dashboard | A table or query + metrics / one visual / a page of charts and filters |

## 4. Likely questions (answers anchored in what we measured)

1. **Why no HDFS?** Kafka is the buffer and ClickHouse the store. A single datanode gives no fault tolerance, and HDFS would add 2 containers and a second copy of the data.
2. **Why Connect and not Python?** Declarative, tracked file progress, no code. Cost: one more JVM, one task per file, no throttle.
3. **Why 3 partitions?** Parallel Spark tasks and keyed ordering, without overhead on a laptop. More would not help 2 cores.
4. **Why key by user_session?** All events of a session land in one partition, in order.
5. **Is it exactly-once?** No: at-least-once, with a ledger that skips replayed batches. Residual window described in docs/architecture.md. Detection and repair exist.
6. **What does the checkpoint store?** Offsets before processing (`offsets/N`) and a commit marker after (`commits/N`).
7. **Why `try_cast`?** Spark 4 runs ANSI mode: a bad `cast` kills the stream.
8. **Why aggregate in ClickHouse, not Spark?** Exact, cheap, always current. Spark state for 9M sessions is memory-heavy.
9. **Why daily partitions?** Date pruning and single-day reloads. Cost: more partitions.
10. **Why does the `ORDER BY` include product/user/time/session?** ReplacingMergeTree deduplicates by that key, so identical events collapse. The primary key is a short prefix for speed.
11. **How do you know the data is right?** Ledger identities (42,448,764 = 42,418,544 valid + 30,220 duplicates), `reconcile.sql` passing 10/10 assertions, the Phase 2 `distinct_rows` cross-check matching ClickHouse FINAL row counts, and automated golden fixtures.
12. **What is the cart-abandonment limitation?** Session-level: a later purchase in another session still counts as abandoned. Our fixture shows 3/4 sessions vs 2/4 users.
13. **What would you change for production?** Multi-node Kafka and ClickHouse with replication, TLS and authentication, a secrets manager, Kafka Connect distributed workers, monitoring and alerting, CI.
14. **What was the biggest bottleneck?** ClickHouse HTTP batch write overhead with small micro-batches. Resolved by scaling micro-batch size to 600,000 offsets and repartitioning by daily partition, lifting throughput from ~24k to ~35k rows/s.
15. **What did you reject and why?** Rejected user-journey sorting because no dashboard requirement existed and it would double disk storage; rejected monthly partitioning because it eliminated partition drop/attach maintenance; rejected Arrow write format because it was 4.5% slower than HTTP streaming.

## 5. Honest limitations

Replay not live data; single node and replication 1; plain-text listeners and development credentials; at-least-once with a small duplicate window;
session-level abandonment; filter scope gaps in Superset (aggregates lack some dimensions); distinct-count cards read the raw table; Windows-only scripts;
one month of data.

## 6. Presentation outline (about 12 slides)

1. Problem and dataset. 2. Architecture diagram. 3. Why these technologies (and what we rejected). 4. Ingestion (Connect, keys, partitions).
5. Spark: quality rules and quarantine. 6. Delivery semantics and the ledger. 7. ClickHouse design (partition, order key, MVs).
8. Airflow: what it orchestrates and why not micro-batches. 9. Dashboard walkthrough. 10. Performance: experiments, winners, rejections.
11. Data-quality evidence (reconcile, drift demo). 12. Limitations and future work.

## 7. Live demo (8 minutes)

`docker compose ps` (services healthy) -> Kafka UI (topic, 3 partitions, messages) -> `smoke-test.ps1` output (PASS table) -> ClickHouse
`SELECT * FROM v_kpi_summary` -> Superset dashboard with a filter -> Airflow `data_quality` run and `v_dq_latest` -> the drift demo (insert duplicate, red, rebuild, green).
Rehearse the drift demo at least twice: it is the most convincing 90 seconds you have.