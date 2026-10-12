"""Daily pipeline: extract -> land in S3 -> COPY into Snowflake -> dbt build.

Runs at 03:00 Adelaide time, after the previous service day has finished
(the last trams and trains run past midnight).

    extract_gtfs_static ─┐
                         ├─> load_snowflake ─> dbt_deps ─> dbt_seed ─> dbt_snapshot ─> dbt_build ─> record_run
    extract_weather ─────┘

dbt_build runs models and their tests together, in DAG order, so a failing
test on a staging model stops the marts built on top of it.
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta

import pendulum

try:  # Airflow 3
    from airflow.providers.standard.operators.bash import BashOperator
    from airflow.sdk import dag, task
except ImportError:  # Airflow 2.x
    from airflow.decorators import dag, task
    from airflow.operators.bash import BashOperator

PROJECT_DIR = os.getenv("PIPELINE_PROJECT_DIR", "/opt/airflow/project")
DBT_DIR = f"{PROJECT_DIR}/dbt"
DBT_BIN = os.getenv("DBT_BIN", "dbt")
DBT = f"{DBT_BIN} --no-use-colors --project-dir {DBT_DIR} --profiles-dir {DBT_DIR} --target prod"

DEFAULT_ARGS = {
    "owner": "data-eng",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
}


@dag(
    dag_id="adelaide_transit_daily",
    description="Adelaide Metro + BOM -> S3 -> Snowflake -> dbt star schema",
    schedule="0 3 * * *",
    start_date=pendulum.datetime(2026, 10, 1, tz="Australia/Adelaide"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["transit", "snowflake", "dbt"],
    doc_md=__doc__,
)
def adelaide_transit_daily():

    @task
    def run_started() -> str:
        return datetime.now(pendulum.UTC).isoformat()

    @task(execution_timeout=timedelta(minutes=10))
    def extract_gtfs_static() -> dict:
        from ingestion.extract_gtfs_static import run
        return run()

    @task(execution_timeout=timedelta(minutes=5))
    def extract_weather() -> dict:
        from ingestion.extract_weather import run
        return run()

    @task(execution_timeout=timedelta(minutes=20))
    def load_snowflake(run_id: str) -> dict:
        os.environ["PIPELINE_RUN_ID"] = run_id
        from ingestion.load_snowflake import run
        return run()

    common_env = {**os.environ, "DBT_TARGET": "prod"}

    dbt_deps = BashOperator(
        task_id="dbt_deps",
        # clear yesterday's results so record_run can't report a stale run
        bash_command=f"rm -f {DBT_DIR}/target/run_results.json && {DBT} deps",
        env=common_env,
    )
    dbt_seed = BashOperator(task_id="dbt_seed", bash_command=f"{DBT} seed", env=common_env)
    dbt_snapshot = BashOperator(task_id="dbt_snapshot", bash_command=f"{DBT} snapshot", env=common_env)
    dbt_source_freshness = BashOperator(
        task_id="dbt_source_freshness",
        bash_command=f"{DBT} source freshness || true",  # report, don't block
        env=common_env,
    )
    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=f"{DBT} build --exclude resource_type:snapshot resource_type:seed",
        env=common_env,
        execution_timeout=timedelta(minutes=45),
    )
    record_run = BashOperator(
        task_id="record_run",
        bash_command=(
            f"python {PROJECT_DIR}/scripts/record_run.py "
            "--run-id '{{ run_id }}' "
            "--started-at '{{ ti.xcom_pull(task_ids=\"run_started\") }}' "
            "--orchestrator airflow"
        ),
        env=common_env,
        trigger_rule="all_done",  # record failures too - honest numbers
    )

    started = run_started()
    loaded = load_snowflake(run_id="{{ run_id }}")
    started >> [extract_gtfs_static(), extract_weather()] >> loaded
    loaded >> dbt_deps >> dbt_seed >> dbt_snapshot >> dbt_source_freshness >> dbt_build >> record_run


adelaide_transit_daily()
