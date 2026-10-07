"""Daily housekeeping: parts health, sizes, quarantine summary, optional compaction of the SMALL aggregate tables."""
import pendulum
from airflow.sdk import Param, dag, get_current_context, task
from ecom_lib import checks, clients

AGGREGATES = ["daily_funnel", "hourly_activity", "category_sales", "brand_sales",
              "daily_uniques", "daily_buyers", "session_state"]


@dag(
    dag_id="clickhouse_maintenance",
    schedule="0 3 * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    params={"optimize_aggregates": Param(False, type="boolean",
                                         description="OPTIMIZE ... FINAL on the 7 small aggregate tables (never on ecommerce_events)")},
    tags=["ecommerce", "maintenance"],
)
def clickhouse_maintenance():
    @task
    def parts() -> list[dict]:
        return checks.as_dicts(checks.maint_parts())

    @task
    def sizes() -> list[dict]:
        return checks.as_dicts(checks.maint_sizes())

    @task
    def quarantine() -> list[dict]:
        return checks.as_dicts(checks.maint_quarantine())

    @task
    def compact() -> list[dict]:
        if not get_current_context()["params"]["optimize_aggregates"]:
            return []
        for table in AGGREGATES:
            clients.ch().command(f"OPTIMIZE TABLE ecommerce.{table} FINAL")
        return checks.as_dicts([checks.Result("aggregates_optimized", "maintenance", checks.OK,
                                              len(AGGREGATES), "informational")])

    @task(retries=0)
    def finalize(*groups: list[dict]) -> None:
        results = checks.from_dicts([d for g in groups for d in g])
        checks.record("clickhouse_maintenance", get_current_context()["run_id"], results)
        checks.gate(results)

    finalize(parts(), sizes(), quarantine(), compact())


clickhouse_maintenance()