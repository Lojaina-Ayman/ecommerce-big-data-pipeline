"""Daily data-quality gate: rules, aggregate reconciliation, Kafka <-> ClickHouse reconciliation."""
import pendulum
from airflow.sdk import Param, dag, get_current_context, task
from ecom_lib import checks


@dag(
    dag_id="data_quality",
    schedule="0 2 * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    params={
        "min_date": Param("2019-10-01", type="string", format="date"),
        "max_date": Param("2019-10-31", type="string", format="date"),
        "max_null_session_ratio": Param(0.01, type="number", minimum=0, maximum=1),
        "max_quarantine_ratio": Param(0.01, type="number", minimum=0, maximum=1),
    },
    tags=["ecommerce", "quality"],
)
def data_quality():
    @task
    def reconcile_aggregates() -> list[dict]:
        return checks.as_dicts(checks.dq_reconcile_aggregates())

    @task
    def quality_rules() -> list[dict]:
        p = get_current_context()["params"]
        return checks.as_dicts(checks.dq_quality_rules(
            p["min_date"], p["max_date"], p["max_null_session_ratio"], p["max_quarantine_ratio"]))

    @task
    def revenue_and_duplicates() -> list[dict]:
        return checks.as_dicts(checks.dq_revenue_and_duplicates())

    @task
    def pipeline_reconciliation() -> list[dict]:
        return checks.as_dicts(checks.dq_pipeline_reconciliation())

    @task(retries=0)
    def finalize(*groups: list[dict]) -> None:
        results = checks.from_dicts([d for g in groups for d in g])
        checks.record("data_quality", get_current_context()["run_id"], results)
        checks.gate(results)

    finalize(reconcile_aggregates(), quality_rules(), revenue_and_duplicates(), pipeline_reconciliation())


data_quality()