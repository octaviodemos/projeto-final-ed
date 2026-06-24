from __future__ import annotations

import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow.operators.python import PythonOperator
from airflow.providers.papermill.operators.papermill import PapermillOperator
from airflow.sdk import DAG

NOTEBOOKS_DIR = Path("/opt/projeto/notebooks")
OUTPUT_DIR = Path("/tmp/notebooks_out")
TASK_TIMEOUT = timedelta(hours=3)


def _register_trino_tables():
    import trino

    conn = trino.dbapi.connect(
        host="host.docker.internal",
        port=int(os.environ.get("TRINO_PORT", "8080")),
        user="admin",
        catalog="delta",
    )
    cur = conn.cursor()
    cur.execute("CREATE SCHEMA IF NOT EXISTS delta.gold")
    cur.fetchall()

    tables = [
        ("dim_agent",            "s3://gold/dim_agent/"),
        ("dim_customer",         "s3://gold/dim_customer/"),
        ("dim_date",             "s3://gold/dim_date/"),
        ("dim_product",          "s3://gold/dim_product/"),
        ("dim_seller",           "s3://gold/dim_seller/"),
        ("fact_orders",          "s3://gold/fact_orders/"),
        ("fact_reviews",         "s3://gold/fact_reviews/"),
        ("fact_support_tickets", "s3://gold/fact_support_tickets/"),
    ]
    for table, location in tables:
        try:
            cur.execute(
                f"CALL delta.system.register_table("
                f"schema_name => 'gold', table_name => '{table}', "
                f"table_location => '{location}')"
            )
            cur.fetchall()
        except Exception:
            pass  # já registrada


def _seed_metabase():
    import subprocess
    import sys

    env = os.environ.copy()
    env["MB_URL"] = "http://host.docker.internal:" + os.environ.get("METABASE_PORT", "3000")
    result = subprocess.run(
        [sys.executable, "/opt/projeto/metabase/seed.py"],
        env=env,
    )
    if result.returncode != 0:
        raise RuntimeError("seed_metabase falhou")


with DAG(
    dag_id="pipeline_medalhao",
    description="Pipeline Medallion: Landing → Bronze → Silver → Gold via Papermill",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["medallion", "pyspark", "papermill"],
    dagrun_timeout=timedelta(hours=6),
    default_args={
        "owner": "projeto-final-ed",
        "retries": 1,
        "retry_delay": timedelta(minutes=10),
        "execution_timeout": TASK_TIMEOUT,
    },
    doc_md=__doc__,
) as dag:
    download_olist = PapermillOperator(
        task_id="00_download_olist",
        input_nb=str(NOTEBOOKS_DIR / "00_download_olist.ipynb"),
        output_nb=str(OUTPUT_DIR / "00_download_olist-{{ ds }}.ipynb"),
        execution_timeout=TASK_TIMEOUT,
    )

    landing_to_bronze = PapermillOperator(
        task_id="01a_landing_to_bronze",
        input_nb=str(NOTEBOOKS_DIR / "01a_landing_to_bronze.ipynb"),
        output_nb=str(OUTPUT_DIR / "01a_landing_to_bronze-{{ ds }}.ipynb"),
        parameters={
            "landing_bucket": "landing",
            "bronze_bucket": "bronze",
        },
        execution_timeout=TASK_TIMEOUT,
    )

    bronze_to_silver = PapermillOperator(
        task_id="02_bronze_to_silver",
        input_nb=str(NOTEBOOKS_DIR / "02_bronze_to_silver.ipynb"),
        output_nb=str(OUTPUT_DIR / "02_bronze_to_silver-{{ ds }}.ipynb"),
        parameters={
            "bronze_bucket": "bronze",
            "silver_bucket": "silver",
            "write_mode": "overwrite",
        },
        execution_timeout=TASK_TIMEOUT,
    )

    silver_to_gold = PapermillOperator(
        task_id="03_silver_to_gold",
        input_nb=str(NOTEBOOKS_DIR / "03_silver_to_gold.ipynb"),
        output_nb=str(OUTPUT_DIR / "03_silver_to_gold-{{ ds }}.ipynb"),
        parameters={
            "silver_bucket": "silver",
            "gold_bucket": "gold",
            "write_mode": "overwrite",
        },
        execution_timeout=TASK_TIMEOUT,
    )

    register_trino = PythonOperator(
        task_id="register_trino_tables",
        python_callable=_register_trino_tables,
    )

    seed_metabase = PythonOperator(
        task_id="seed_metabase",
        python_callable=_seed_metabase,
    )

    download_olist >> landing_to_bronze >> bronze_to_silver >> silver_to_gold >> register_trino >> seed_metabase
