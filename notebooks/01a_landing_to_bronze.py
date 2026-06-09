"""
01a_landing_to_bronze.py
========================
Lê os CSVs da camada Landing no MinIO e persiste como Delta Lake na camada Bronze.

Execução:
    python notebooks/01a_landing_to_bronze.py

Requisitos:
    - MinIO rodando em localhost:9000 (docker compose up -d)
    - CSVs já ingeridos no bucket `landing` (scripts/ingest/download_olist.py)
    - PySpark + delta-spark instalados no venv
"""

from __future__ import annotations

import os
import sys
import logging
from pathlib import Path

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable

# ──────────────────────────────────────────────
# Logging
# ──────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ──────────────────────────────────────────────
# Configuração
# ──────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parents[1]

# Tabelas esperadas (nome do CSV → nome da tabela Delta)
EXPECTED_TABLES = [
    "olist_customers_dataset",
    "olist_geolocation_dataset",
    "olist_order_items_dataset",
    "olist_order_payments_dataset",
    "olist_order_reviews_dataset",
    "olist_orders_dataset",
    "olist_products_dataset",
    "olist_sellers_dataset",
    "product_category_name_translation",
]


def _load_env() -> None:
    """Carrega variáveis do arquivo .env (formato KEY=VALUE) sem depender de python-dotenv."""
    env_path = PROJECT_ROOT / ".env"
    if not env_path.exists():
        log.warning(".env não encontrado em %s – usando variáveis do sistema", env_path)
        return
    with open(env_path) as fh:
        for line in fh:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            key, _, value = line.partition("=")
            os.environ.setdefault(key.strip(), value.strip())


def _create_spark_session() -> SparkSession:
    """
    Cria uma SparkSession local configurada para:
      - Acessar MinIO via protocolo S3A (hadoop-aws)
      - Usar Delta Lake como formato de tabela

    Nota: os JARs hadoop-aws e aws-java-sdk-bundle devem estar em
    $SPARK_HOME/jars/ (ou no diretório jars/ do pyspark instalado via pip).
    """
    endpoint = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
    access_key = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    secret_key = os.getenv("MINIO_SECRET_KEY", "minioadmin")

    builder = (
        SparkSession.builder
        .appName("LandingToBronze")
        .master("local[*]")
        # ── Delta Lake ──
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        # ── Hadoop / S3A para MinIO ──
        .config("spark.hadoop.fs.s3a.endpoint", endpoint)
        .config("spark.hadoop.fs.s3a.access.key", access_key)
        .config("spark.hadoop.fs.s3a.secret.key", secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        # Desabilita a verificação de região do AWS SDK (necessário para MinIO)
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        # ── Performance ──
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.driver.memory", "2g")
    )

    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    # Reduz verbosidade do Spark nos logs
    spark.sparkContext.setLogLevel("WARN")

    return spark


def _ingest_table(
    spark: SparkSession,
    table_name: str,
    landing_bucket: str,
    bronze_bucket: str,
) -> int:
    """
    Lê um CSV do bucket landing e persiste como Delta no bucket bronze.

    Retorna o número de linhas ingeridas.
    """
    source_path = f"s3a://{landing_bucket}/{table_name}.csv"
    target_path = f"s3a://{bronze_bucket}/{table_name}"

    log.info("Lendo: %s", source_path)

    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .option("multiLine", "true")
        .option("escape", '"')
        .csv(source_path)
    )

    # ── Metadados de ingestão ──
    df = (
        df
        .withColumn("_ingestion_timestamp", F.current_timestamp())
        .withColumn("_source_file", F.lit(f"{table_name}.csv"))
    )

    row_count = df.count()

    log.info("Persistindo %d linhas em: %s", row_count, target_path)

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(target_path)
    )

    return row_count


def _verify_delta_tables(spark: SparkSession, bronze_bucket: str) -> None:
    """Verifica que todas as tabelas Delta foram criadas e imprime o histórico."""
    log.info("=" * 60)
    log.info("VERIFICAÇÃO DAS TABELAS DELTA")
    log.info("=" * 60)

    for table_name in EXPECTED_TABLES:
        target_path = f"s3a://{bronze_bucket}/{table_name}"
        try:
            dt = DeltaTable.forPath(spark, target_path)
            history = dt.history(1).select("version", "timestamp", "operation").collect()
            row = history[0]
            log.info(
                "  ✔ %-45s | v%s | %s | %s",
                table_name,
                row["version"],
                row["timestamp"],
                row["operation"],
            )
        except Exception as exc:
            log.error("  ✘ %-45s | ERRO: %s", table_name, exc)


def main() -> None:
    _load_env()

    landing_bucket = os.getenv("MINIO_BUCKET_LANDING", "landing")
    bronze_bucket = os.getenv("MINIO_BUCKET_BRONZE", "bronze")

    log.info("Iniciando ingestão Landing → Bronze")
    log.info("  Landing bucket: s3a://%s/", landing_bucket)
    log.info("  Bronze  bucket: s3a://%s/", bronze_bucket)

    spark = _create_spark_session()

    total_rows = 0
    success_count = 0

    for table_name in EXPECTED_TABLES:
        try:
            rows = _ingest_table(spark, table_name, landing_bucket, bronze_bucket)
            total_rows += rows
            success_count += 1
        except Exception as exc:
            log.error("Falha ao ingerir '%s': %s", table_name, exc)

    log.info("-" * 60)
    log.info(
        "Ingestão concluída: %d/%d tabelas | %d linhas totais",
        success_count,
        len(EXPECTED_TABLES),
        total_rows,
    )

    _verify_delta_tables(spark, bronze_bucket)

    spark.stop()

    if success_count < len(EXPECTED_TABLES):
        log.error("Nem todas as tabelas foram ingeridas com sucesso!")
        sys.exit(1)

    log.info("✔ Pipeline Landing → Bronze finalizado com sucesso!")


if __name__ == "__main__":
    main()
