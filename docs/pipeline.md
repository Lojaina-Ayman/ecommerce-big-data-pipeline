# Pipeline runbook

## Normal sequence

```powershell
scripts\bootstrap.ps1                       # once per clone
docker compose --profile ingest up -d kafka-connect
scripts\load-csv.ps1 -Csv <file.csv>        # drop a file for Connect
$env:SPARK_SINK="clickhouse"; docker compose --profile process up -d spark-master spark-worker
docker compose --profile process --profile job up -d spark-job
```

Progress: `kafka-get-offsets` total versus `SELECT sum(total_rows) FROM batch_ledger` (lag = difference), or `scripts\monitor.ps1`.

## Full run (42M rows)

1. `scripts\reset-pipeline-state.ps1 -Kafka` (removes any slice or sample data from Kafka and ClickHouse).
2. Raise `.wslconfig` to 12 GB. Start `ingest`, run `monitor.ps1`, then `load-csv.ps1 -Csv data\raw\2019-Oct.csv`.
3. **Do not stop Connect until the file is in `data/connect/finished`.** (Mid-file restart outcome: Connect cleanly resumes from the exact file offset in `_connect_offsets`, incurring 0 message loss and 0 duplicate records).
4. Stop Connect. Start the job with the tuned settings in `.env`, `SPARK_TRIGGER_SECONDS=0`. Done when lag = 0.
5. Validate (see docs/testing.md "Full-scale validation"). Repair drift with `clickhouse/queries/rebuild_aggregates.sql`.
6. Optional: `OPTIMIZE TABLE ecommerce.ecommerce_events FINAL` after the bulk load (measured benefit: collapsed 167 parts to 31 parts in 71.4s, dropped FINAL scan latency by -99.0% and date scan read rows by up to -77.9%).

## Safe operations

| Action | How | Notes |
|---|---|---|
| Pause processing | `docker compose --profile process --profile job stop spark-job` | Resumes from the checkpoint |
| Resume | `... up -d spark-job` | A replayed batch prints `SKIPPED` |
| Clean slate | `scripts\reset-pipeline-state.ps1 [-Kafka]` | Deletes checkpoints and truncates data. Always reset the checkpoint when Kafka is reset |
| Change batch size | Reset ClickHouse and the checkpoint first | Ledger signatures are offset ranges |
| Repair aggregates | Airflow `rebuild_aggregates` or the SQL file | Stop the streaming job first |
| Destroy everything | `docker compose --profile "*"` down -v | Irreversible |

## Modes

Development: 100k sample (`scripts\make_sample.sh`). Full: `data/raw/2019-Oct.csv`. The switch is only the file you drop
and the tuning variables. No code changes.