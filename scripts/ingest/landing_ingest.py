from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import tempfile
from pathlib import Path
from urllib.parse import urlparse, unquote

import boto3
from botocore.exceptions import (
    BotoCoreError,
    ClientError,
    ConnectionClosedError,
    EndpointConnectionError,
)
import psycopg2
from psycopg2 import DatabaseError as PgDatabaseError
from psycopg2 import OperationalError as PgOperationalError

TABLES: dict[str, str] = {
    "olist_customers_dataset.csv": "olist_customers_dataset.csv",
    "olist_geolocation_dataset.csv": "olist_geolocation_dataset.csv",
    "olist_order_items_dataset.csv": "olist_order_items_dataset.csv",
    "olist_order_payments_dataset.csv": "olist_order_payments_dataset.csv",
    "olist_order_reviews_dataset.csv": "olist_order_reviews_dataset.csv",
    "olist_orders_dataset.csv": "olist_orders_dataset.csv",
    "olist_products_dataset.csv": "olist_products_dataset.csv",
    "olist_sellers_dataset.csv": "olist_sellers_dataset.csv",
    "product_category_name_translation": "product_category_name_translation.csv",
    "agents": "agents.csv",
    "support_tickets": "support_tickets.csv",
    "support_ticket_messages": "support_ticket_messages.csv",
}

EXPECTED_FILES = list(TABLES.values())

log = logging.getLogger("landing_ingest")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Extrai tabelas do Supabase como CSV e envia para o bucket landing no MinIO."
    )
    parser.add_argument(
        "--supabase-url",
        default=os.getenv("SUPABASE_URL", ""),
        help="URL de conexão com o Supabase. Padrão: variável SUPABASE_URL.",
    )
    parser.add_argument(
        "--landing-bucket",
        default=os.getenv("MINIO_BUCKET_LANDING", "landing"),
        help="Bucket de destino no MinIO. Padrão: MINIO_BUCKET_LANDING ou 'landing'.",
    )
    return parser.parse_args()


def load_env_file(path: Path) -> None:
    if not path.exists():
        return
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


def _connect_supabase(dsn: str):
    try:
        parsed = urlparse(dsn)
        return psycopg2.connect(
            host=parsed.hostname,
            port=parsed.port or 5432,
            dbname=parsed.path.lstrip("/"),
            user=parsed.username,
            password=unquote(parsed.password or ""),
            connect_timeout=10,
            sslmode="require",
        )
    except PgOperationalError as exc:
        raise RuntimeError(f"Falha de conexão com o Supabase: {exc}") from exc
    except PgDatabaseError as exc:
        raise RuntimeError(f"Erro de banco de dados ao conectar no Supabase: {exc}") from exc


def _extract_table_to_csv(conn, table_name: str, dest_path: Path) -> int:
    log.info("Extraindo tabela '%s' …", table_name)
    try:
        with conn.cursor() as cur:
            cur.execute(f'SELECT * FROM "{table_name}"')
            rows = cur.fetchall()
            col_names = [desc[0] for desc in cur.description]
    except PgDatabaseError as exc:
        conn.rollback()
        raise RuntimeError(f"Erro ao consultar tabela '{table_name}': {exc}") from exc

    with open(dest_path, "w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerow(col_names)
        writer.writerows(rows)

    log.info("  → %d linhas exportadas para %s", len(rows), dest_path.name)
    return len(rows)


def _extract_all_tables(dsn: str, dest_dir: Path) -> list[Path]:
    conn = _connect_supabase(dsn)
    csv_files: list[Path] = []

    try:
        for table_name, csv_filename in TABLES.items():
            csv_path = dest_dir / csv_filename
            _extract_table_to_csv(conn, table_name, csv_path)
            csv_files.append(csv_path)
    finally:
        conn.close()

    if not csv_files:
        raise RuntimeError("Nenhuma tabela foi extraída com sucesso.")

    return csv_files


def _get_s3_client():
    endpoint = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
    access_key = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    secret_key = os.getenv("MINIO_SECRET_KEY", "minioadmin")
    parsed = urlparse(endpoint)

    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        use_ssl=parsed.scheme == "https",
        region_name="us-east-1",
    )


def _ensure_bucket(s3_client, bucket: str) -> None:
    try:
        s3_client.head_bucket(Bucket=bucket)
    except ClientError as exc:
        error_code = int(exc.response["Error"]["Code"])
        if error_code == 404:
            s3_client.create_bucket(Bucket=bucket)
        else:
            raise


def _upload_to_minio(csv_files: list[Path], bucket: str) -> None:
    try:
        s3 = _get_s3_client()
        s3.list_buckets()
    except (EndpointConnectionError, ConnectionClosedError, BotoCoreError) as exc:
        raise RuntimeError(f"Falha de conexão com o MinIO: {exc}") from exc

    _ensure_bucket(s3, bucket)

    for csv_path in csv_files:
        log.info("Upload: %s → s3://%s/%s", csv_path.name, bucket, csv_path.name)
        try:
            s3.upload_file(
                Filename=str(csv_path),
                Bucket=bucket,
                Key=csv_path.name,
            )
        except (ClientError, BotoCoreError) as exc:
            raise RuntimeError(f"Erro ao enviar '{csv_path.name}': {exc}") from exc


def _verify(bucket: str) -> None:
    s3 = _get_s3_client()
    response = s3.list_objects_v2(Bucket=bucket)
    objects = [obj["Key"] for obj in response.get("Contents", [])]
    missing = set(EXPECTED_FILES) - set(objects)
    if missing:
        raise RuntimeError(f"CSVs ausentes no bucket '{bucket}': {sorted(missing)}")


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    repo_root = Path(__file__).resolve().parents[2]
    load_env_file(repo_root / ".env")
    args = parse_args()

    dsn = args.supabase_url or os.getenv("SUPABASE_URL", "")
    if not dsn:
        raise RuntimeError("SUPABASE_URL não configurada.")

    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        csv_files = _extract_all_tables(dsn, tmp_path)
        _upload_to_minio(csv_files, args.landing_bucket)

    _verify(args.landing_bucket)

    print(json.dumps({
        "tables_exported": len(csv_files),
        "landing_bucket": args.landing_bucket,
        "files": EXPECTED_FILES,
    }, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
