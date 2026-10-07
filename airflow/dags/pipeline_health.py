"""Is every service alive, and is anyone draining the lag? Runs every 10 minutes."""
from datetime import timedelta
import pendulum
from airflow.sdk import Param, dag, get_current_context, task
from ecom_lib import checks


@dag(
    dag_id="pipeline_health",
    schedule="*/10 * * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    params={"strict": Param(False, type="boolean",
                            description="true = optional services (Connect, Spark) must be up; warnings become failures")},
    default_args={"retries": 1, "retry_delay": timedelta(seconds=20)},
    tags=["ecommerce", "health"],
)
def pipeline_health():
    @task
    def kafka() -> list[dict]:
        return checks.as_dicts(checks.check_kafka())

    @task
    def connect() -> list[dict]:
        return checks.as_dicts(checks.check_connect())

    @task
    def clickhouse() -> list[dict]:
        return checks.as_dicts(checks.check_clickhouse())

    @task
    def spark() -> list[dict]:
        return checks.as_dicts(checks.check_spark())

    @task
    def lag() -> list[dict]:
        return checks.as_dicts(checks.check_lag())

    @task(retries=0)
    def finalize(*groups: list[dict]) -> None:
        ctx = get_current_context()
        results = checks.from_dicts([d for g in groups for d in g])
        if ctx["params"]["strict"]:
            results = checks.strictify(results)
        checks.record("pipeline_health", ctx["run_id"], results)
        checks.gate(results)

    finalize(kafka(), connect(), clickhouse(), spark(), lag())


pipeline_health()