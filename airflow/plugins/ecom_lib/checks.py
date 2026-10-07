"""All pipeline checks. Each returns Result objects; DAG tasks pass them around as plain dicts (XCom-safe)."""
from __future__ import annotations

import logging
from dataclasses import asdict, dataclass
from pathlib import Path

from airflow.exceptions import AirflowFailException
from ecom_lib import clients

log = logging.getLogger(__name__)
OK, WARN, FAIL = "ok", "warn", "fail"

REQUIRED_TABLES = [
    "ecommerce_events", "events_quarantine", "batch_ledger", "daily_funnel", "hourly_activity",
    "category_sales", "brand_sales", "daily_uniques", "daily_buyers", "session_state",
    "dq_results", "v_kpi_summary",
]
SQL_DIR = Path("/opt/airflow/sql")


@dataclass
class Result:
    check_name: str
    category: str
    status: str
    observed: float
    expected: str
    detail: str = ""


def as_dicts(results):
    return [asdict(r) for r in results]


def from_dicts(items):
    return [Result(**d) for d in items]


def strictify(results):
    return [Result(r.check_name, r.category, FAIL if r.status == WARN else r.status,
                   r.observed, r.expected, r.detail) for r in results]


def record(dag_id, run_id, results):
    for r in results:
        log.info("[%s] %s/%s observed=%s expected=%s %s",
                 r.status.upper(), r.category, r.check_name, r.observed, r.expected, r.detail)
    try:
        rows = [[dag_id, run_id, r.check_name, r.category, r.status, float(r.observed),
                 r.expected, r.detail[:500]] for r in results]
        clients.ch().insert("dq_results", rows, column_names=[
            "dag_id", "run_id", "check_name", "category", "status", "observed", "expected", "detail"])
    except Exception as exc:
        log.warning("Could not write dq_results: %s", exc)


def gate(results):
    bad = [r for r in results if r.status == FAIL]
    if bad:
        raise AirflowFailException("Failed checks: " + "; ".join(f"{r.check_name} ({r.detail})" for r in bad))


def check_kafka():
    try:
        parts = clients.kafka_topic_partitions()
    except Exception as exc:
        return [Result("kafka_broker", "health", FAIL, 0, "reachable", str(exc)[:300])]
    out = [Result("kafka_broker", "health", OK, 1, "reachable")]
    if parts is None:
        out.append(Result("kafka_topic", "health", FAIL, 0, f"{clients.TOPIC} exists", "topic missing"))
    else:
        out.append(Result("kafka_topic_partitions", "health", OK if len(parts) == 3 else FAIL,
                          len(parts), "3"))
    return out


def check_clickhouse():
    try:
        c = clients.ch()
        c.ping()
        have = {r[0] for r in c.query(
            "SELECT name FROM system.tables WHERE database = 'ecommerce'").result_rows}
    except Exception as exc:
        return [Result("clickhouse_reachable", "health", FAIL, 0, "reachable", str(exc)[:300])]
    missing = [t for t in REQUIRED_TABLES if t not in have]
    return [Result("clickhouse_reachable", "health", OK, 1, "reachable"),
            Result("clickhouse_tables", "health", OK if not missing else FAIL,
                   len(REQUIRED_TABLES) - len(missing), str(len(REQUIRED_TABLES)),
                   ("missing: " + ", ".join(missing)) if missing else "")]


def check_connect():
    try:
        st = clients.connect_status()
    except Exception as exc:
        return [Result("connect_reachable", "health", WARN, 0, "reachable", f"Connect not running? {str(exc)[:200]}")]
    if st is None:
        return [Result("connect_connector", "health", WARN, 0, "registered", f"{clients.CONNECTOR} not registered")]
    states = [st["connector"]["state"]] + [t["state"] for t in st.get("tasks", [])]
    failed = [s for s in states if s == "FAILED"]
    ok = bool(states) and all(s == "RUNNING" for s in states)
    return [Result("connect_connector", "health", OK if ok else (FAIL if failed else WARN),
                   1 if ok else 0, "RUNNING", ",".join(states))]


def check_spark():
    try:
        data = clients.spark_master_json()
    except Exception as exc:
        return [Result("spark_master", "health", WARN, 0, "reachable", f"Spark not running? {str(exc)[:200]}")]
    workers = int(data.get("aliveworkers", 0))
    running = clients.streaming_job_running()
    return [Result("spark_master", "health", OK, 1, "reachable"),
            Result("spark_workers", "health", OK if workers >= 1 else WARN, workers, ">= 1"),
            Result("spark_streaming_job", "health", OK if running else WARN, 1 if running else 0,
                   f"app {clients.STREAMING_APP} active")]


def ledger_snapshot():
    row = clients.ch().query(
        "SELECT toUInt64(ifNull(sum(total_rows), 0)), toUInt64(ifNull(sum(valid_rows), 0)), "
        "toUInt64(ifNull(sum(quarantined_rows), 0)), toUInt64(ifNull(sum(duplicates_removed), 0)) "
        "FROM ecommerce.batch_ledger").result_rows[0]
    return {"total": int(row[0]), "valid": int(row[1]), "quarantined": int(row[2]), "dups": int(row[3])}


def check_lag():
    try:
        _, kafka_total = clients.kafka_end_offsets()
        lag = kafka_total - ledger_snapshot()["total"]
    except Exception as exc:
        return [Result("pipeline_lag_rows", "health", WARN, 0, "computable", str(exc)[:200])]
    running = clients.streaming_job_running()
    status = OK if (lag <= 0 or running) else WARN
    return [Result("pipeline_lag_rows", "health", status, lag, "0 or draining",
                   f"kafka_total={kafka_total}, streaming_job_running={running}")]


def dq_reconcile_aggregates():
    sql = (SQL_DIR / "reconcile.sql").read_text().strip().rstrip(";")
    out = []
    for row in clients.ch().query(sql).result_rows:
        name, agg, raw, ok = row[0], row[1], row[2], row[3]
        out.append(Result(f"agg_{name}", "reconciliation", OK if ok else FAIL, float(agg),
                          f"= raw ({float(raw):g})", f"aggregate={float(agg):g} raw={float(raw):g}"))
    return out


def dq_quality_rules(min_date, max_date, max_null_session_ratio, max_quarantine_ratio):
    c = clients.ch()
    row = c.query(
        """SELECT count(),
                  countIf(user_session = ''),
                  countIf(event_type NOT IN ('view', 'cart', 'purchase')),
                  countIf(price < 0),
                  countIf(event_date < toDate({lo:String}) OR event_date > toDate({hi:String})),
                  countIf(user_id = 0 OR product_id = 0)
           FROM ecommerce.ecommerce_events""",
        parameters={"lo": min_date, "hi": max_date}).result_rows[0]
    n, no_session, bad_type, negative, out_of_range, no_ids = (int(x) for x in row)
    quarantined = int(clients.scalar("SELECT count() FROM ecommerce.events_quarantine"))

    def ratio(a, b):
        return (a / b) if b else 0.0

    null_ratio, q_ratio = ratio(no_session, n), ratio(quarantined, n + quarantined)
    return [
        Result("events_not_empty", "quality", OK if n > 0 else FAIL, n, "> 0"),
        Result("null_user_session_ratio", "quality", OK if null_ratio <= max_null_session_ratio else WARN,
               null_ratio, f"<= {max_null_session_ratio}", f"{no_session} of {n}"),
        Result("invalid_event_type_in_clean_table", "quality", OK if bad_type == 0 else FAIL, bad_type, "0"),
        Result("negative_price_in_clean_table", "quality", OK if negative == 0 else FAIL, negative, "0"),
        Result("dates_outside_range", "quality", OK if out_of_range == 0 else FAIL, out_of_range, "0",
               f"range {min_date}..{max_date}"),
        Result("missing_ids_in_clean_table", "quality", OK if no_ids == 0 else FAIL, no_ids, "0"),
        Result("quarantine_ratio", "quality", OK if q_ratio <= max_quarantine_ratio else WARN, q_ratio,
               f"<= {max_quarantine_ratio}", f"{quarantined} quarantined"),
    ]


def dq_revenue_and_duplicates():
    c = clients.ch()
    non_purchase_revenue = float(clients.scalar(
        "SELECT toFloat64(sumIf(revenue, event_type != 'purchase')) FROM ecommerce.daily_funnel"))
    purchases, zero_price = (int(x) for x in c.query(
        "SELECT countIf(event_type = 'purchase'), countIf(event_type = 'purchase' AND price = 0) "
        "FROM ecommerce.ecommerce_events").result_rows[0])
    raw = int(clients.scalar("SELECT count() FROM ecommerce.ecommerce_events"))
    final = int(clients.scalar("SELECT count() FROM ecommerce.ecommerce_events FINAL"))
    return [
        Result("revenue_only_from_purchases", "quality", OK if non_purchase_revenue == 0 else FAIL,
               non_purchase_revenue, "0", "revenue on view/cart rows in daily_funnel"),
        Result("zero_price_purchases", "quality", OK if zero_price == 0 else WARN, zero_price, "0",
               f"of {purchases} purchases"),
        Result("duplicate_rows_pending_merge", "quality", OK if raw == final else WARN, raw - final, "0",
               "ReplacingMergeTree collapses these at merge time"),
    ]


def dq_pipeline_reconciliation():
    try:
        _, kafka_total = clients.kafka_end_offsets()
    except Exception as exc:
        return [Result("kafka_vs_ledger", "reconciliation", FAIL, 0, "computable", str(exc)[:200])]
    led = ledger_snapshot()
    events = int(clients.scalar("SELECT count() FROM ecommerce.ecommerce_events"))
    quarantine = int(clients.scalar("SELECT count() FROM ecommerce.events_quarantine"))
    parts = led["valid"] + led["quarantined"] + led["dups"]
    return [
        Result("kafka_vs_ledger", "reconciliation", OK if kafka_total == led["total"] else FAIL,
               kafka_total - led["total"], "0",
               f"kafka={kafka_total} ledger={led['total']} (still draining?)"),
        Result("ledger_internal_balance", "reconciliation", OK if parts == led["total"] else FAIL,
               parts - led["total"], "0", f"valid+quarantined+dups={parts} total={led['total']}"),
        Result("events_vs_ledger", "reconciliation", OK if abs(events - led["valid"]) <= 1 else FAIL,
               events - led["valid"], "0",
               f"events={events} ledger_valid={led['valid']} (positive = duplicate inserts)"),
        Result("quarantine_vs_ledger", "reconciliation", OK if quarantine == led["quarantined"] else FAIL,
               quarantine - led["quarantined"], "0", f"rows={quarantine} ledger={led['quarantined']}"),
    ]


def maint_parts():
    c = clients.ch()
    limit = float(clients.scalar(
        "SELECT toFloat64(value) FROM system.merge_tree_settings WHERE name = 'parts_to_delay_insert'"))
    rows = c.query(
        "SELECT table, partition, count() AS n FROM system.parts "
        "WHERE database = 'ecommerce' AND active GROUP BY table, partition ORDER BY n DESC LIMIT 1").result_rows
    table, partition, n = rows[0] if rows else ("-", "-", 0)
    status = FAIL if n >= limit else (WARN if n >= limit / 2 else OK)
    return [Result("max_active_parts_in_a_partition", "maintenance", status, n,
                   f"< {limit / 2:g} (inserts delayed at {limit:g})", f"{table} / {partition}")]


def maint_sizes():
    rows = clients.ch().query(
        "SELECT table, sum(rows), sum(bytes_on_disk) FROM system.parts "
        "WHERE database = 'ecommerce' AND active GROUP BY table ORDER BY sum(bytes_on_disk) DESC").result_rows
    total = sum(int(r[2]) for r in rows)
    detail = "; ".join(f"{t}: {int(n)} rows, {int(b) / 1e6:.1f} MB" for t, n, b in rows)
    return [Result("ecommerce_disk_mb", "maintenance", OK, total / 1e6, "informational", detail)]


def maint_quarantine():
    rows = clients.ch().query(
        "SELECT reject_reason, count() FROM ecommerce.events_quarantine "
        "WHERE quarantined_at > now() - INTERVAL 1 DAY GROUP BY reject_reason ORDER BY 2 DESC").result_rows
    return [Result("quarantined_last_24h", "maintenance", OK, sum(int(r[1]) for r in rows), "informational",
                   "; ".join(f"{r[0]}: {r[1]}" for r in rows))]