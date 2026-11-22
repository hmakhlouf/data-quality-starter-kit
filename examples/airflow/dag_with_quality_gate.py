"""Airflow 2.x DAG: load -> quality gate -> publish.

The gate runs dq/run_checks.py with --fail-on-critical. If a critical rule fails the task
fails, so `publish` never runs and bad data never reaches the reports. Warnings are logged
and the issue log is still written.
"""
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

KIT = "/opt/airflow/dags/data-quality-starter-kit"

with DAG(
    dag_id="programs_daily_with_dq_gate",
    start_date=datetime(2026, 1, 1),
    schedule="0 6 * * *",
    catchup=False,
    default_args={"retries": 0},
    tags=["data-quality"],
) as dag:
    load = BashOperator(task_id="load_raw_to_target", bash_command="echo 'run your ETL here'")

    quality_gate = BashOperator(
        task_id="quality_gate",
        bash_command=f"cd {KIT} && python dq/run_checks.py --no-demo --db $DQ_DB --fail-on-critical",
    )

    publish = BashOperator(task_id="publish_to_reporting", bash_command="echo 'swap tables / refresh BI'")

    load >> quality_gate >> publish
