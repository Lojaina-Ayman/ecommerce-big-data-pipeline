"""Drop a CSV into Kafka Connect's folder, then PROVE it arrived: Kafka delta, ClickHouse ledger delta, reconciliation."""
import os
import shutil
import time
from pathlib import Path

import pendulum
from airflow.exceptions import AirflowFailException
from airflow.sdk import Param, dag, get_current_context, task
from ecom_lib import checks, clients

DATA_ROOT = Path("/opt/airflow/data")


def _count_data_rows(path: Path) -> int:
    n = 0
    with open(path, "rb") as f:
        for _ in f:
            n += 1
    return n - 1


def _deadline(pre: dict) -> None:
    if time.time() > pre["deadline_epoch"]:
        raise AirflowFailException("Timed out waiting. Is the spark-job running and Connect healthy?")


@dag(
    dag_id="load_dataset",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    params={
        "csv_path": Param("data/sample/events_airflow_load_test.csv", type="string",
                          description="Path relative to /opt/airflow (must be inside data/)"),
        "wait_timeout_minutes": Param(30, type="integer", minimum=1, maximum=720),
    },
    tags=["ecommerce", "load"],
)
def load_dataset():
    @task(retries=0)
    def preflight() -> dict:
        params = get_current_context()["params"]
        src = (Path("/opt/airflow") / params["csv_path"]).resolve()
        if not src.is_relative_to(DATA_ROOT.resolve()) or not src.is_file() or src.suffix != ".csv":
            raise AirflowFailException(f"{params['csv_path']} must be an existing .csv inside data/")

        everything = checks.strictify(checks.check_kafka() + checks.check_clickhouse()
                                      + checks.check_connect() + checks.check_spark())
        problems = [f"{r.check_name}: {r.detail or r.status}" for r in everything if r.status != checks.OK]
        if problems:
            raise AirflowFailException("Pipeline is not ready: " + " | ".join(problems))

        return {
            "src": str(src),
            "expected_rows": _count_data_rows(src),
            "kafka_before": clients.kafka_end_offsets()[1],
            "ledger": checks.ledger_snapshot(),
            "deadline_epoch": time.time() + params["wait_timeout_minutes"] * 60,
        }

    @task(retries=0)
    def submit_file(pre: dict) -> dict:
        src = Path(pre["src"])
        inbox = DATA_ROOT / "connect" / "unprocessed"
        inbox.mkdir(parents=True, exist_ok=True)
        name = f"events_{src.stem}_{pendulum.now('UTC').format('YYYYMMDD_HHmmss')}"
        shutil.copyfile(src, inbox / f"{name}.tmp")
        os.replace(inbox / f"{name}.tmp", inbox / f"{name}.csv")
        return {"name": f"{name}.csv"}

    @task.sensor(poke_interval=15, timeout=12 * 3600, mode="reschedule")
    def wait_connect_finished(sub: dict, pre: dict) -> bool:
        _deadline(pre)
        stem = Path(sub["name"]).stem
        if any((DATA_ROOT / "connect" / "error").glob(f"{stem}*")):
            raise AirflowFailException(f"Connect moved {sub['name']} to the error folder")
        return any((DATA_ROOT / "connect" / "finished").glob(f"{stem}*"))

    @task.sensor(poke_interval=15, timeout=12 * 3600, mode="reschedule")
    def wait_kafka_has_rows(pre: dict) -> bool:
        _deadline(pre)
        return clients.kafka_end_offsets()[1] - pre["kafka_before"] >= pre["expected_rows"]

    @task.sensor(poke_interval=15, timeout=12 * 3600, mode="reschedule")
    def wait_clickhouse_caught_up(pre: dict) -> bool:
        _deadline(pre)
        return checks.ledger_snapshot()["total"] - pre["ledger"]["total"] >= pre["expected_rows"]

    @task(retries=0)
    def reconcile_load(pre: dict, sub: dict) -> None:
        exp = pre["expected_rows"]
        kafka_delta = clients.kafka_end_offsets()[1] - pre["kafka_before"]
        led = checks.ledger_snapshot()
        d = {k: led[k] - pre["ledger"][k] for k in led}
        R = checks.Result
        results = [
            R("load_kafka_delta", "load", checks.OK if kafka_delta == exp else checks.FAIL,
              kafka_delta, str(exp), sub["name"]),
            R("load_ledger_delta", "load", checks.OK if d["total"] == exp else checks.FAIL,
              d["total"], str(exp)),
            R("load_rows_accounted_for", "load",
              checks.OK if d["valid"] + d["quarantined"] + d["dups"] == exp else checks.FAIL,
              d["valid"] + d["quarantined"] + d["dups"], str(exp),
              f"valid={d['valid']} quarantined={d['quarantined']} duplicates_removed={d['dups']}"),
        ] + checks.dq_reconcile_aggregates()
        checks.record("load_dataset", get_current_context()["run_id"], results)
        checks.gate(results)

    pre = preflight()
    sub = submit_file(pre)
    done = wait_connect_finished(sub, pre)
    in_kafka = wait_kafka_has_rows(pre)
    in_clickhouse = wait_clickhouse_caught_up(pre)
    final = reconcile_load(pre, sub)
    sub >> done >> in_kafka >> in_clickhouse >> final


load_dataset()