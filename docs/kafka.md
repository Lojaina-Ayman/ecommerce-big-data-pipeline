# Kafka and Kafka Connect

**Broker:** Apache Kafka 4.3, KRaft (one node is broker and controller, no ZooKeeper). Listeners: INTERNAL 9092 (containers),
CONTROLLER 9093, EXTERNAL 9094 (host tools). Auto topic creation is off, so topics are created explicitly.

**Topic `ecommerce_events_stream`:** 3 partitions, replication factor 1 (single broker), retention 7 days, `compression.type=lz4`.
Three partitions give 3 parallel Spark tasks per batch and demonstrate keyed ordering without overhead on a laptop.

**Connector `ecommerce-csv-source` (SpoolDir CSV):**

| Setting | Why |
|---|---|
| `input.path / finished.path / error.path` | Files move after processing, so the raw data is never read in place |
| `input.file.pattern=.*\.csv$` + copy-as-`.tmp`-then-rename | A half-copied file is never picked up |
| `csv.first.row.as.header`, `schema.generation.enabled` | Schema from the header, all fields optional strings. Typing is Spark's job |
| SMT `ValueToKey(user_session)` + `ExtractField$Key` | Key = session id: Kafka hashes it, so one session stays in one partition, in order |
| `tasks.max=1` | One task per file |

Rows with an empty `user_session` get a null key (spread by the producer). Source connectors have no dead-letter queue,
so bad *values* are handled in Spark, not here.

**Offsets (three different ones):** Connect's file progress (`_connect_offsets`), consumer-group offsets (`__consumer_offsets`,
used by console tools), and Spark's own offsets in its checkpoint. Spark does not commit to a consumer group, so pipeline lag is
`Kafka end offsets - batch_ledger.total_rows`.

**Measured behavior:** Ingest throughput averaged 35,000–37,500 msgs/s with peak CPU of 270% on `kafka-connect`; mid-file restart tested in Stage 1c resumed from the recorded offset without duplicating rows or crashing.