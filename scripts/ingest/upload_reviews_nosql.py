from __future__ import annotations

import argparse
import json
import logging
import os
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlparse

import boto3
from botocore.exceptions import BotoCoreError, ClientError


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_SUPABASE_SCHEMA = "public"
DEFAULT_SUPABASE_TABLE = "reviews_nosql"
DEFAULT_BUCKET_NAME = "landing"
DEFAULT_OBJECT_KEY = "nosql/reviews_nosql.json"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


@dataclass(frozen=True)
class SupabaseConfig:
    url: str
    schema_name: str
    table_name: str


@dataclass(frozen=True)
class MinioConfig:
    endpoint_url: str
    access_key: str
    secret_key: str
    bucket_name: str
    object_key: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Read the reviews_nosql Supabase table, which stores the complementary "
            "support-ticket NoSQL source, and upload it as JSON to MinIO Landing."
        )
    )
    parser.add_argument(
        "--supabase-url",
        default=os.getenv("SUPABASE_URL"),
        help="PostgreSQL connection URL for Supabase. Defaults to SUPABASE_URL.",
    )
    parser.add_argument(
        "--schema",
        default=os.getenv("SUPABASE_SCHEMA", DEFAULT_SUPABASE_SCHEMA),
        help="Supabase/Postgres schema name. Default: public.",
    )
    parser.add_argument(
        "--table",
        default=get_env_value(
            "SUPABASE_NOSQL_TABLE",
            "SUPABASE_REVIEWS_TABLE",
            DEFAULT_SUPABASE_TABLE,
        ),
        help=(
            "Supabase NoSQL table to read. Default: reviews_nosql. "
            "In this project, this table stores the complementary support-ticket "
            "NoSQL source."
        ),
    )
    parser.add_argument(
        "--order-by",
        default=get_env_value("SUPABASE_NOSQL_ORDER_BY", "SUPABASE_REVIEWS_ORDER_BY"),
        help="Optional single column used to make the exported JSON row order stable.",
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
        "--bucket-name",
        default=os.getenv("MINIO_BUCKET_LANDING", DEFAULT_BUCKET_NAME),
        help="Landing bucket name. Defaults to MINIO_BUCKET_LANDING or landing.",
    )
    parser.add_argument(
        "--object-key",
        default=get_env_value(
            "MINIO_NOSQL_OBJECT_KEY",
            "MINIO_REVIEWS_NOSQL_KEY",
            DEFAULT_OBJECT_KEY,
        ),
        help="Destination object key in Landing. Default: nosql/reviews_nosql.json.",
    )
    args = parser.parse_args()

    if not args.supabase_url:
        parser.error("--supabase-url or SUPABASE_URL is required.")
    if not args.schema.strip():
        parser.error("--schema cannot be empty.")
    if not args.table.strip():
        parser.error("--table cannot be empty.")
    if not args.object_key.strip():
        parser.error("--object-key cannot be empty.")
    return args


def load_env_file(path: Path) -> None:
    if not path.exists():
        return

    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


def get_env_value(primary_key: str, fallback_key: str, default: str | None = None) -> str | None:
    return os.getenv(primary_key) or os.getenv(fallback_key) or default


def build_supabase_config(args: argparse.Namespace) -> SupabaseConfig:
    return SupabaseConfig(
        url=args.supabase_url,
        schema_name=args.schema.strip(),
        table_name=args.table.strip(),
    )


def build_minio_config(args: argparse.Namespace) -> MinioConfig:
    return MinioConfig(
        endpoint_url=args.minio_endpoint,
        access_key=args.minio_access_key,
        secret_key=args.minio_secret_key,
        bucket_name=args.bucket_name,
        object_key=args.object_key.strip().lstrip("/"),
    )


def fetch_table_as_json(
    config: SupabaseConfig,
    order_by: str | None = None,
) -> list[dict[str, object]]:
    try:
        import psycopg
        from psycopg import sql
        from psycopg.rows import dict_row
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "psycopg is required to read Supabase. Run `uv sync` after updating dependencies."
        ) from exc

    table_identifier = sql.Identifier(config.schema_name, config.table_name)
    query = sql.SQL("SELECT row_to_json(source_row) AS record FROM {} AS source_row").format(
        table_identifier
    )

    if order_by:
        query += sql.SQL(" ORDER BY {}").format(sql.Identifier(order_by.strip()))

    with psycopg.connect(config.url, row_factory=dict_row) as conn:
        with conn.cursor() as cursor:
            cursor.execute(query)
            rows = cursor.fetchall()

    records: list[dict[str, object]] = []
    for row in rows:
        record = row["record"]
        if isinstance(record, str):
            record = json.loads(record)
        records.append(record)
    return records


def create_s3_client(config: MinioConfig):
    parsed = urlparse(config.endpoint_url)
    return boto3.client(
        "s3",
        endpoint_url=config.endpoint_url,
        aws_access_key_id=config.access_key,
        aws_secret_access_key=config.secret_key,
        use_ssl=parsed.scheme == "https",
        region_name="us-east-1",
    )


def ensure_bucket_exists(s3_client, bucket_name: str) -> None:
    try:
        s3_client.head_bucket(Bucket=bucket_name)
    except ClientError as exc:
        error_code = str(exc.response.get("Error", {}).get("Code", ""))
        if error_code in {"404", "NoSuchBucket", "NotFound"}:
            s3_client.create_bucket(Bucket=bucket_name)
            return
        raise


def upload_json(
    s3_client,
    records: list[dict[str, object]],
    bucket_name: str,
    object_key: str,
) -> None:
    payload = json.dumps(records, ensure_ascii=False, indent=2, default=str).encode("utf-8")

    s3_client.put_object(
        Bucket=bucket_name,
        Key=object_key,
        Body=payload,
        ContentType="application/json; charset=utf-8",
    )
    s3_client.head_object(Bucket=bucket_name, Key=object_key)


def main() -> int:
    load_env_file(PROJECT_ROOT / ".env")
    args = parse_args()
    supabase_config = build_supabase_config(args)
    minio_config = build_minio_config(args)

    log.info(
        "Lendo Supabase: %s.%s",
        supabase_config.schema_name,
        supabase_config.table_name,
    )
    records = fetch_table_as_json(supabase_config, order_by=args.order_by)
    if not records:
        raise RuntimeError(
            f"Supabase table '{supabase_config.schema_name}.{supabase_config.table_name}' "
            "returned zero rows."
        )

    log.info(
        "Enviando %d registro(s) para s3://%s/%s",
        len(records),
        minio_config.bucket_name,
        minio_config.object_key,
    )
    s3_client = create_s3_client(minio_config)
    try:
        ensure_bucket_exists(s3_client, minio_config.bucket_name)
        upload_json(
            s3_client=s3_client,
            records=records,
            bucket_name=minio_config.bucket_name,
            object_key=minio_config.object_key,
        )
    except (BotoCoreError, ClientError) as exc:
        raise RuntimeError(f"Failed to upload support-ticket JSON to MinIO: {exc}") from exc

    log.info(
        "Arquivo JSON disponivel em s3://%s/%s",
        minio_config.bucket_name,
        minio_config.object_key,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
