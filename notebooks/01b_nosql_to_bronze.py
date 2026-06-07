from __future__ import annotations

"""Independent Bronze ingestion notebook for the NoSQL reviews JSON source.

The `tags` field is intentionally preserved as ArrayType in the Delta output.
"""

import argparse
import ctypes
import json
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING
from urllib.parse import urlparse

import boto3
from botocore.exceptions import BotoCoreError, ClientError

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession


DEFAULT_APP_NAME = "01b_nosql_to_bronze"
DEFAULT_INPUT_BUCKET = "landing"
DEFAULT_INPUT_PREFIX = "nosql"
DEFAULT_INPUT_OBJECT_NAME = "reviews_nosql.json"
DEFAULT_OUTPUT_BUCKET = "bronze"
DEFAULT_OUTPUT_PREFIX = "reviews_nosql"
DEFAULT_WRITE_MODE = "overwrite"
DEFAULT_SPARK_MASTER = "local[*]"
HADOOP_AWS_PACKAGE = "org.apache.hadoop:hadoop-aws:3.3.4"
TIMESTAMP_PATTERN = "yyyy-MM-dd'T'HH:mm:ssX"
TAGS_STRATEGY = "preserved_as_array"


@dataclass(frozen=True)
class StorageConfig:
    endpoint_url: str
    access_key: str
    secret_key: str
    input_bucket: str
    input_prefix: str
    output_bucket: str
    output_prefix: str


def parse_args() -> argparse.Namespace:
    """Define os parametros de execucao do notebook/script."""

    parser = argparse.ArgumentParser(
        description="Read NoSQL JSON data from Landing and persist it as Delta in Bronze."
    )
    parser.add_argument(
        "--input-bucket",
        default=os.getenv("MINIO_BUCKET_LANDING", DEFAULT_INPUT_BUCKET),
        help="Landing bucket name. Defaults to MINIO_BUCKET_LANDING or 'landing'.",
    )
    parser.add_argument(
        "--input-prefix",
        default=DEFAULT_INPUT_PREFIX,
        help="Prefix inside Landing that stores the NoSQL JSON files. Default: nosql.",
    )
    parser.add_argument(
        "--input-key",
        help="Optional full object key for a specific JSON file in Landing.",
    )
    parser.add_argument(
        "--output-bucket",
        default=os.getenv("MINIO_BUCKET_BRONZE", DEFAULT_OUTPUT_BUCKET),
        help="Bronze bucket name. Defaults to MINIO_BUCKET_BRONZE or 'bronze'.",
    )
    parser.add_argument(
        "--output-prefix",
        default=DEFAULT_OUTPUT_PREFIX,
        help="Destination prefix inside Bronze. Default: reviews_nosql.",
    )
    parser.add_argument(
        "--write-mode",
        default=DEFAULT_WRITE_MODE,
        choices=("overwrite", "append"),
        help="Delta write mode. Default: overwrite.",
    )
    parser.add_argument(
        "--minio-endpoint",
        default=os.getenv("MINIO_ENDPOINT", "http://localhost:9000"),
        help="MinIO endpoint URL. Defaults to MINIO_ENDPOINT or http://localhost:9000.",
    )
    parser.add_argument(
        "--minio-access-key",
        default=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
        help="MinIO access key. Defaults to MINIO_ACCESS_KEY or minioadmin.",
    )
    parser.add_argument(
        "--minio-secret-key",
        default=os.getenv("MINIO_SECRET_KEY", "minioadmin"),
        help="MinIO secret key. Defaults to MINIO_SECRET_KEY or minioadmin.",
    )
    parser.add_argument(
        "--app-name",
        default=DEFAULT_APP_NAME,
        help="Spark application name. Default: 01b_nosql_to_bronze.",
    )
    parser.add_argument(
        "--spark-master",
        default=DEFAULT_SPARK_MASTER,
        help="Spark master URL used for local execution. Default: local[*].",
    )
    return parser.parse_args()


def load_env_file(path: Path) -> None:
    """Carrega variaveis de um .env simples sem depender de biblioteca externa."""

    if not path.exists():
        return

    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


def build_storage_config(args: argparse.Namespace) -> StorageConfig:
    """Agrupa os argumentos relacionados ao armazenamento em uma configuracao unica."""

    return StorageConfig(
        endpoint_url=args.minio_endpoint,
        access_key=args.minio_access_key,
        secret_key=args.minio_secret_key,
        input_bucket=args.input_bucket,
        input_prefix=normalize_prefix(args.input_prefix),
        output_bucket=args.output_bucket,
        output_prefix=normalize_prefix(args.output_prefix),
    )


def normalize_prefix(prefix: str) -> str:
    """Remove barras excedentes para manter os caminhos S3 consistentes."""

    return prefix.strip("/")


def build_s3_client(config: StorageConfig):
    """Cria um cliente S3 compativel com o MinIO via boto3."""

    return boto3.client(
        "s3",
        endpoint_url=config.endpoint_url,
        aws_access_key_id=config.access_key,
        aws_secret_access_key=config.secret_key,
        region_name="us-east-1",
    )


def resolve_input_key(s3_client, bucket_name: str, input_prefix: str, input_key: str | None) -> str:
    """Resolve qual JSON de reviews sera lido, usando a chave informada ou o arquivo mais recente."""

    if input_key:
        return input_key.lstrip("/")

    prefix = f"{input_prefix}/" if input_prefix else ""
    paginator = s3_client.get_paginator("list_objects_v2")
    matches: list[dict] = []

    for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(DEFAULT_INPUT_OBJECT_NAME):
                matches.append(obj)

    if not matches:
        raise FileNotFoundError(
            f"Could not find '{DEFAULT_INPUT_OBJECT_NAME}' in bucket '{bucket_name}' "
            f"under prefix '{prefix or '/'}'."
        )

    matches.sort(key=lambda item: item["LastModified"], reverse=True)
    return matches[0]["Key"]


def build_s3a_uri(bucket_name: str, key_or_prefix: str) -> str:
    """Monta o caminho s3a:// usado pelo Spark para leitura e escrita."""

    normalized = key_or_prefix.strip("/")
    if not normalized:
        return f"s3a://{bucket_name}"
    return f"s3a://{bucket_name}/{normalized}"


def parse_minio_endpoint(endpoint_url: str) -> tuple[str, bool]:
    """Extrai host/porta e identifica se a conexao com o MinIO usa SSL."""

    parsed = urlparse(
        endpoint_url if "://" in endpoint_url else f"http://{endpoint_url}"
    )
    endpoint = parsed.netloc or parsed.path
    ssl_enabled = parsed.scheme == "https"
    return endpoint, ssl_enabled


def resolve_windows_short_path(path: Path) -> Path:
    """Converte caminhos para o formato curto no Windows, evitando falhas do launcher do Spark."""

    buffer_size = 1024
    output_buffer = ctypes.create_unicode_buffer(buffer_size)
    result = ctypes.windll.kernel32.GetShortPathNameW(str(path), output_buffer, buffer_size)
    if result == 0:
        return path
    return Path(output_buffer.value)


def prepare_local_spark_environment() -> None:
    """Define SPARK_HOME/PYSPARK_PYTHON para o PySpark instalado localmente."""

    try:
        import pyspark
    except ModuleNotFoundError as exc:
        raise RuntimeError("pyspark is required to prepare the local Spark environment.") from exc

    spark_home = Path(pyspark.__file__).resolve().parent
    python_executable = Path(sys.executable).resolve()

    if os.name == "nt":
        spark_home = resolve_windows_short_path(spark_home)
        python_executable = resolve_windows_short_path(python_executable)

    os.environ["SPARK_HOME"] = str(spark_home)
    os.environ.setdefault("PYSPARK_PYTHON", str(python_executable))
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", str(python_executable))


def build_spark_session(app_name: str, spark_master: str, config: StorageConfig) -> "SparkSession":
    """Inicializa a SparkSession com suporte a Delta e acesso ao MinIO via S3A."""

    try:
        from pyspark.sql import SparkSession
        from delta import configure_spark_with_delta_pip
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "pyspark and delta-spark are required to run this notebook script. "
            "Use a Spark/Databricks runtime with Delta support."
        ) from exc

    prepare_local_spark_environment()
    endpoint, ssl_enabled = parse_minio_endpoint(config.endpoint_url)
    builder = configure_spark_with_delta_pip(
        SparkSession.builder.appName(app_name)
        .master(spark_master)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.endpoint", endpoint)
        .config("spark.hadoop.fs.s3a.access.key", config.access_key)
        .config("spark.hadoop.fs.s3a.secret.key", config.secret_key)
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", str(ssl_enabled).lower())
        .config("spark.hadoop.fs.s3a.fast.upload", "true")
        .config("spark.hadoop.fs.s3a.fast.upload.buffer", "array"),
        extra_packages=[HADOOP_AWS_PACKAGE],
    )
    spark = builder.getOrCreate()

    return spark


def build_source_schema():
    """Define explicitamente o schema do JSON NoSQL sem inferencia automatica."""

    try:
        from pyspark.sql.types import ArrayType, StringType, StructField, StructType
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "pyspark is required to build the explicit StructType schema."
        ) from exc

    return StructType(
        [
            StructField("review_id", StringType(), nullable=False),
            StructField("order_id", StringType(), nullable=False),
            StructField("customer_id", StringType(), nullable=False),
            StructField("sentiment", StringType(), nullable=True),
            StructField("comment_text", StringType(), nullable=True),
            StructField("tags", ArrayType(StringType(), containsNull=False), nullable=True),
            StructField("created_at", StringType(), nullable=False),
        ]
    )


def read_reviews_dataframe(spark: "SparkSession", input_path: str) -> "DataFrame":
    """Le o JSON de reviews, aplica o schema fixo e adiciona metadados de ingestao."""

    from pyspark.sql import functions as F

    source_df = (
        spark.read.option("multiLine", True)
        .schema(build_source_schema())
        .json(input_path)
    )

    return (
        source_df.withColumn("created_at", F.to_timestamp("created_at", TIMESTAMP_PATTERN))
        # Os metadados ajudam a rastrear quando e de qual arquivo a carga veio.
        .withColumn("_ingestion_timestamp", F.current_timestamp())
        .withColumn("_source_file", F.input_file_name())
    )


def validate_bronze_dataframe(df: "DataFrame") -> None:
    """Confere se o DataFrame Bronze ficou com o schema e os campos obrigatorios esperados."""

    from pyspark.sql import functions as F
    from pyspark.sql.types import ArrayType, StringType, TimestampType

    if not df.take(1):
        raise ValueError("The NoSQL source produced zero rows.")

    field_map = {field.name: field.dataType for field in df.schema.fields}
    expected_columns = {
        "review_id",
        "order_id",
        "customer_id",
        "sentiment",
        "comment_text",
        "tags",
        "created_at",
        "_ingestion_timestamp",
        "_source_file",
    }
    missing_columns = expected_columns.difference(field_map)
    if missing_columns:
        raise ValueError(f"Missing expected Bronze columns: {sorted(missing_columns)}")

    # A issue permite explodir tags ou preserva-las; aqui a escolha foi manter ArrayType.
    if not isinstance(field_map["tags"], ArrayType):
        raise TypeError("Column 'tags' must be preserved as ArrayType in Bronze.")
    if not isinstance(field_map["tags"].elementType, StringType):
        raise TypeError("Column 'tags' must contain string elements.")
    if not isinstance(field_map["created_at"], TimestampType):
        raise TypeError("Column 'created_at' must be stored as TimestampType in Bronze.")
    if not isinstance(field_map["_ingestion_timestamp"], TimestampType):
        raise TypeError("Column '_ingestion_timestamp' must be stored as TimestampType.")

    invalid_rows = (
        df.filter(
            F.col("review_id").isNull()
            | F.col("order_id").isNull()
            | F.col("customer_id").isNull()
            | F.col("created_at").isNull()
            | F.col("_source_file").isNull()
        )
        .limit(1)
        .count()
    )
    if invalid_rows:
        raise ValueError(
            "The Bronze dataset contains null values in required columns or failed timestamp parsing."
        )


def write_bronze_delta(df: "DataFrame", output_path: str, write_mode: str) -> None:
    """Persiste o DataFrame Bronze no destino Delta configurado."""

    (
        df.write.format("delta")
        .mode(write_mode)
        .option("overwriteSchema", "true")
        .save(output_path)
    )


def read_back_delta(spark: "SparkSession", output_path: str) -> "DataFrame":
    """Reler o Delta apos a escrita para validar o resultado persistido."""

    return spark.read.format("delta").load(output_path)


def print_summary(input_path: str, output_path: str, row_count: int) -> None:
    """Exibe um resumo final da execucao para facilitar validacao manual."""

    summary = {
        "input_path": input_path,
        "output_path": output_path,
        "rows_written": row_count,
        "tags_strategy": TAGS_STRATEGY,
    }
    print(json.dumps(summary, ensure_ascii=False, indent=2))


def main() -> int:
    """Orquestra a carga completa da fonte NoSQL da Landing para a Bronze."""

    repo_root = Path(__file__).resolve().parents[1]
    load_env_file(repo_root / ".env")
    args = parse_args()
    config = build_storage_config(args)
    s3_client = build_s3_client(config)

    try:
        input_key = resolve_input_key(
            s3_client=s3_client,
            bucket_name=config.input_bucket,
            input_prefix=config.input_prefix,
            input_key=args.input_key,
        )
    except (BotoCoreError, ClientError) as exc:
        raise RuntimeError(f"Failed to resolve the NoSQL JSON in Landing: {exc}") from exc

    input_path = build_s3a_uri(config.input_bucket, input_key)
    output_path = build_s3a_uri(config.output_bucket, config.output_prefix)

    spark = build_spark_session(args.app_name, args.spark_master, config)
    # A leitura, validacao e releitura final garantem que o Delta salvo ficou consistente.
    bronze_df = read_reviews_dataframe(spark, input_path)
    validate_bronze_dataframe(bronze_df)
    row_count = bronze_df.count()

    write_bronze_delta(bronze_df, output_path, args.write_mode)

    persisted_df = read_back_delta(spark, output_path)
    validate_bronze_dataframe(persisted_df)
    print_summary(input_path, output_path, row_count)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
