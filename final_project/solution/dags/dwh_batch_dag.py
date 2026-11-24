"""DAG - prepare -> scd2 -> marts -> validate -> publish -> advance_boundary.

Параметр: calc_date (YYYY-MM-DD, UTC) — расчётный интервал
[calc_date 00:00, calc_date+1 00:00). T продвигается на calc_date+1
только после успешной публикации и только если текущий T == calc_date.
"""
from __future__ import annotations

import urllib.parse
import urllib.request
from datetime import datetime, timedelta

import docker
from airflow import DAG
from airflow.operators.python import PythonOperator, get_current_context


SPARK_SUBMIT = (
    "/opt/bitnami/spark/bin/spark-submit "
    "--master spark://spark-master:7077 "
    "--jars /opt/spark/scripts/jars/hadoop-aws-3.4.1.jar,"
    "/opt/spark/scripts/jars/bundle-2.20.0.jar "
    "/opt/spark/scripts/spark_batch.py"
)
CH_URL = "http://clickhouse:8123/"


def _ch(sql: str) -> str:
    """POST body - работает и для SELECT, и для INSERT.
    GET ?query= ломается на INSERT ... VALUES (HTTP 500)."""
    req = urllib.request.Request(
        CH_URL,
        data=sql.encode("utf-8"),
        method="POST",
        headers={"Content-Type": "text/plain; charset=utf-8"},
    )
    with urllib.request.urlopen(req, timeout=15) as r:
        return r.read().decode().strip()


def run_stage(stage: str):
    ctx = get_current_context()
    ds = ctx["params"]["calc_date"]
    run_id = f"airflow-{ds}"
    start = f"{ds}T00:00:00Z"
    end = (datetime.fromisoformat(ds) + timedelta(days=1)).strftime("%Y-%m-%dT00:00:00Z")
    cmd = f"{SPARK_SUBMIT} --stage={stage} --run-id={run_id} --interval-start={start} --interval-end={end}"
    print(f"[stage {stage}] {cmd}")
    client = docker.from_env()
    container = client.containers.get("spark-master")
    code, output = container.exec_run(cmd, stream=False)
    text = output.decode("utf-8", errors="replace")
    for line in text.splitlines():
        if line.startswith("[") or "ERROR" in line or "Traceback" in line:
            print(line)
    if code != 0:
        raise RuntimeError(f"stage {stage} exit={code}")


def advance_boundary():
    ctx = get_current_context()
    ds = ctx["params"]["calc_date"]
    current_t = _ch("SELECT toString(boundary_t) FROM report_boundary FINAL WHERE id=1")
    expected = ds + " 00:00:00"
    if current_t != expected:
        print(f"[boundary] skip: current={current_t}, expected_prev={expected}")
        return
    next_t = (datetime.fromisoformat(ds) + timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S")
    _ch(f"INSERT INTO report_boundary (id, boundary_t) VALUES (1, toDateTime('{next_t}'))")
    print(f"[boundary] advanced {current_t} -> {next_t}")


default_args = {"owner": "student", "retries": 0, "retry_delay": timedelta(minutes=2)}

with DAG(
    dag_id="dwh_batch_daily",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule="0 3 * * *",
    catchup=False,
    max_active_runs=1,
    params={"calc_date": "2026-01-01"},
    doc_md=__doc__,
) as dag:

    prepare  = PythonOperator(task_id="prepare",  python_callable=run_stage, op_args=["prepare"])
    scd2     = PythonOperator(task_id="scd2",     python_callable=run_stage, op_args=["scd2"])
    marts    = PythonOperator(task_id="marts",    python_callable=run_stage, op_args=["marts"])
    validate = PythonOperator(task_id="validate", python_callable=run_stage, op_args=["validate"])
    publish  = PythonOperator(task_id="publish",  python_callable=run_stage, op_args=["publish"])
    advance  = PythonOperator(task_id="advance_boundary", python_callable=advance_boundary)

    prepare >> scd2 >> marts >> validate >> publish >> advance
