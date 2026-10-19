"""transit_lakehouse_pipeline: refresh gold and gate it on data quality, every 15 minutes.

    dbt_run -> dbt_test -> ge_checkpoint -> mark_refresh
                  \____________\_____________> alert_on_failure  (runs only if something failed)

* dbt_run         incremental MERGE of gold.route_lateness_by_hour + rebuild of the small tables
* dbt_test        24 dbt tests on silver (source) and gold
* ge_checkpoint   Great Expectations suite on silver rows written in the last 6 h
* mark_refresh    records the successful refresh in gold.pipeline_refreshes (idempotent MERGE on run_id)
* alert_on_failure  only runs if something failed: logs an ALERT line and fails itself, so the run
                  ends red; the PipelineDagFailed Prometheus alert picks that up via StatsD.
                  On a healthy run it is skipped.

Idempotent: dbt merges by key, tests and GE only read, mark_refresh merges by run_id; so every
task can retry safely. max_active_runs=1 keeps runs from overlapping.
"""
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

VENV = "/opt/dbt-venv/bin"  # dbt + Great Expectations live in their own venv (no clash with Airflow pins)
DBT = f"cd /work/dbt && {VENV}/dbt"
log = logging.getLogger(__name__)


def log_task_failure(context) -> None:
    ti = context["task_instance"]
    log.error("ALERT pipeline task failed: dag=%s task=%s run_id=%s try=%s log=%s",
              ti.dag_id, ti.task_id, context["run_id"], ti.try_number, ti.log_url)


def raise_alert(**context) -> None:
    failed = [ti.task_id for ti in context["dag_run"].get_task_instances()
              if ti.state in ("failed", "upstream_failed")]
    log.error("ALERT transit_lakehouse_pipeline run %s FAILED; failed tasks: %s", context["run_id"], failed)
    # This task is the DAG's only leaf, so Airflow derives the run's state from it: fail it on
    # purpose, otherwise a run whose quality gate failed would show green.
    raise AirflowFailException(f"pipeline failed at: {', '.join(failed)}")


default_args = {
    "owner": "data-eng",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "execution_timeout": timedelta(minutes=10),
    "on_failure_callback": log_task_failure,
}

with DAG(
    dag_id="transit_lakehouse_pipeline",
    description="dbt gold refresh + quality gates",
    schedule="*/15 * * * *",
    start_date=datetime(2026, 10, 19),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    doc_md=__doc__,
    tags=["gold", "quality"],
) as dag:
    dbt_run = BashOperator(task_id="dbt_run", bash_command=f"{DBT} run", retries=2)
    dbt_test = BashOperator(task_id="dbt_test", bash_command=f"{DBT} test")
    ge_checkpoint = BashOperator(task_id="ge_checkpoint", bash_command=f"{VENV}/python /work/quality/run_checks.py")
    mark_refresh = BashOperator(
        task_id="mark_refresh",
        bash_command=f"{VENV}/python /opt/airflow/scripts/mark_refresh.py '{{{{ run_id }}}}' '{{{{ ts }}}}'",
    )
    alert_on_failure = PythonOperator(
        task_id="alert_on_failure",
        python_callable=raise_alert,
        trigger_rule=TriggerRule.ONE_FAILED,
        retries=0,
    )

    dbt_run >> dbt_test >> ge_checkpoint >> mark_refresh
    [dbt_run, dbt_test, ge_checkpoint, mark_refresh] >> alert_on_failure
