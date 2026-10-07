"""Repair path: rebuild all aggregate tables from the deduplicated raw table. Manual, guarded."""
import logging
import time
import pendulum
from airflow.exceptions import AirflowFailException
from airflow.sdk import Param, dag, get_current_context, task
from ecom_lib import checks, clients

log = logging.getLogger(__name__)


@dag(
    dag_id="rebuild_aggregates",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    params={
        "confirm": Param(False, type="boolean", description="Must be true. This TRUNCATES and rebuilds all aggregates"),
        "force_while_streaming": Param(False, type="boolean",
                                       description="Rows inserted during a rebuild are LOST from the aggregates. Leave false"),
        "optimize_raw": Param(False, type="boolean",
                              description="OPTIMIZE ... FINAL on ecommerce_events first (fine on the sample, HEAVY on 42M rows)"),
    },
    tags=["ecommerce", "repair"],
)
def rebuild_aggregates():
    @task(retries=0)
    def guard() -> None:
        p = get_current_context()["params"]
        if not p["confirm"]:
            raise AirflowFailException("Set confirm=true to run this DAG")
        if clients.streaming_job_running() and not p["force_while_streaming"]:
            raise AirflowFailException("The streaming job is running. Stop it first (or set force_while_streaming)")

    @task(retries=0)
    def rebuild() -> None:
        if get_current_context()["params"]["optimize_raw"]:
            log.info("OPTIMIZE ecommerce_events FINAL")
            clients.ch().command("OPTIMIZE TABLE ecommerce.ecommerce_events FINAL")
        sql = (checks.SQL_DIR / "rebuild_aggregates.sql").read_text()
        for stmt in (s.strip() for s in sql.split(";")):
            if stmt:
                t0 = time.time()
                clients.ch().command(stmt)
                log.info("%.1fs  %s", time.time() - t0, stmt.splitlines()[0][:90])

    @task(retries=0)
    def verify() -> None:
        results = checks.dq_reconcile_aggregates() + checks.dq_pipeline_reconciliation()
        checks.record("rebuild_aggregates", get_current_context()["run_id"], results)
        checks.gate(results)

    guard() >> rebuild() >> verify()


rebuild_aggregates()