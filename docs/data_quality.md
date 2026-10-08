# Data quality

## Layers

| Layer | Mechanism | Evidence |
|---|---|---|
| Ingest | Pattern and age guards, `.tmp` rename | Connect status, file in `finished` |
| Parse and type | All-string schema, `_corrupt_record`, `try_*` casts | quarantine reasons |
| Rules | Ordered rule list, first failure becomes `reject_reason` | `events_quarantine` |
| No silent loss | Ledger: `total = valid + quarantined + duplicates_removed` | `batch_ledger` |
| Storage | Typed NOT NULL columns, ReplacingMergeTree | schema |
| Reconciliation | Aggregates versus deduplicated raw (`reconcile.sql`) | `dq_results` |
| Pipeline | Kafka end offsets = ledger total | `dq_results` |

## Rule table

| Rule | Action |
|---|---|
| malformed JSON, `{}`, null value | quarantine |
| invalid timestamp, invalid event_type | quarantine |
| missing/non-numeric product_id, user_id | quarantine |
| non-numeric category_id | quarantine |
| non-numeric/NaN/infinite price, negative price | quarantine |
| purchase without price | quarantine |
| missing session, brand, category_code, price; zero price | keep, flag |

Every quarantined row keeps its raw payload and Kafka position, so any decision can be audited or replayed.

## Airflow checks (in `data_quality`)

Aggregate reconciliation (10 comparisons), non-empty table, null-session ratio, invalid event types in the clean table (0),
negative prices (0), dates outside the expected range (0), missing ids (0), quarantine ratio, revenue only from purchases,
zero-price purchases, duplicate rows pending merge, Kafka vs ledger, ledger balance, events vs ledger, quarantine vs ledger.
Thresholds are DAG parameters.

## Reading results

`SELECT * FROM v_dq_latest ORDER BY latest_status DESC`. History: `dq_results`.

## When a check fails

| Failing check | Likely cause | Action |
|---|---|---|
| `kafka_vs_ledger` | Still draining, or job down | Wait or restart the job |
| `agg_*` | Duplicate inserts counted by MVs | `rebuild_aggregates` |
| `events_vs_ledger` positive | Duplicate inserts | Investigate replay, then rebuild |
| `events_vs_ledger` negative (warn) | Merges collapsed duplicates | Expected |
| `quarantine_ratio` | Bad source batch | Inspect `events_quarantine` by reason |

Evidence of the full run:
- Total rows processed: 42,448,764 across 71 batches
- Valid rows written: 42,418,544
- Duplicates removed: 30,220 (cross-batch duplicates collapsed)
- Quarantined: 0 (clean dataset)
- Final reconcile.sql check: 10/10 checks PASS (0 discrepancies)