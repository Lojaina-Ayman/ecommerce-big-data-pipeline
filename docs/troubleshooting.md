# Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| Docker commands fail | Docker Desktop not running | Start it, wait for "Engine running" |
| Everything slow, containers killed | WSL memory too low or too many profiles | `.wslconfig`, stop unused profiles, `docker stats` |
| `port is already allocated` | Another program uses a host port | Change the port in `.env` |
| `.env` values ignored | BOM or duplicate keys | `init-env.ps1` writes UTF-8 without BOM. Check duplicates |
| `bad interpreter` / `\r: not found` | CRLF in a `.sh` | Convert to LF (VS Code status bar) |
| Kafka exits: cluster id mismatch | Cluster id changed after first start | `docker compose down -v` (deletes Kafka data) |
| Connector FAILED, unknown topic | Topic missing (auto-create off) | Create the topic, restart the connector |
| File never leaves `input/` | Still `.tmp`, or too young | Rename, wait 5 s |
| Connect stuck on `.PROCESSING` | Stopped mid-file | Reset the topic and reload (see docs/kafka.md) |
| `Failed to find data source: kafka` | `--packages` not resolved | Check internet and the Ivy lines at job start |
| Spark: `Permission denied` on checkpoints | Volume owned by root | `chown -R 185:185 /opt/spark-checkpoints` as root once |
| Spark executors never start | Worker down or memory mismatch | http://localhost:8081, executor memory must fit the worker |
| Job crashes on bad data | Plain `cast` in ANSI mode | Use `try_cast` (already done) |
| `failOnDataLoss` | Topic reset with an old checkpoint | `reset-pipeline-state.ps1` |
| Batches print SKIPPED unexpectedly | Ledger has an earlier run | Reset ClickHouse data and the checkpoint |
| ClickHouse OOM | Heavy FINAL / GROUP BY within the memory cap | Raise cap temporarily, run heavy SQL alone |
| Too many parts | Many tiny inserts | Raise `CH_WRITE_BATCH_SIZE` and the batch size |
| Aggregates differ from raw | Duplicates counted by MVs | `rebuild_aggregates` |
| Superset cannot connect | Wrong host/port/driver | URI `clickhousedb://user:pass@clickhouse:8123/ecommerce`, host is the service name |
| Superset chart empty after a date filter | Range outside the data | Use a range within October 2019 (upper bound exclusive) |
| Airflow login or health fails | Auth manager / healthcheck mismatch | Compare with the official 3.3.2 compose file |
| New DAG not visible | Not yet parsed | `airflow dags reserialize` |
| `ModuleNotFoundError: ecom_lib` | `plugins` not mounted | Check compose volumes, recreate |
| Disk full | Connect `finished/` copy, Kafka log, parts | `docker system df`, delete `finished/*` |
| Smoke test timed out | Services not up, or Ivy download slow | Increase `-TimeoutMin`, read `docker compose logs` |