from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

from airflow.providers.papermill.operators.papermill import PapermillOperator
from airflow.sdk import DAG

NOTEBOOKS_DIR = Path("/opt/projeto/notebooks")
OUTPUT_DIR = Path("/usr/local/airflow/include/out")
TASK_TIMEOUT = timedelta(hours=3)

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

    landing_to_bronze >> bronze_to_silver >> silver_to_gold
