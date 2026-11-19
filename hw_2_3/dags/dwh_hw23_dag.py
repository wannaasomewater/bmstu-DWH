from datetime import datetime, timedelta
import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.timetables.interval import CronDataIntervalTimetable

LOCAL_TZ = pendulum.timezone("UTC")

default_args = {
    "owner": "student",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="dwh_hw23_scd2_campaigns",
    default_args=default_args,
    start_date=datetime(2026, 1, 23, tzinfo=LOCAL_TZ),
    end_date=datetime(2026, 1, 25, tzinfo=LOCAL_TZ), 
    schedule=CronDataIntervalTimetable("0 0 * * *", timezone="UTC"),
    catchup=True,
    max_active_runs=1,
    tags=["dwh", "hw2.3"],
) as dag:

    build_scd2 = BashOperator(
        task_id="build_user_scd2",
        bash_command=(
            "python /opt/airflow/jobs/build_user_scd2.py "
            "--dag-id {{ dag.dag_id }} "
            "--run-id {{ run_id }} "
            "--task-id build_user_scd2 "
            "--interval-start {{ data_interval_start.isoformat() }} "
            "--interval-end {{ data_interval_end.isoformat() }} "
            "--snapshot-date {{ data_interval_start | ds }}"
        ),
    )

    build_recipients = BashOperator(
        task_id="build_campaign_recipients",
        bash_command=(
            "python /opt/airflow/jobs/build_campaign_recipients.py "
            "--dag-id {{ dag.dag_id }} "
            "--run-id {{ run_id }} "
            "--task-id build_campaign_recipients "
            "--interval-start {{ data_interval_start.isoformat() }} "
            "--interval-end {{ data_interval_end.isoformat() }} "
            "--calculation-date {{ data_interval_start | ds }}"
        ),
    )

    build_scd2 >> build_recipients
