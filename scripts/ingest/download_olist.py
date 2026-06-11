"""
download_olist.py
=================
Extrai as 9 tabelas do dataset Olist armazenadas no Supabase (PostgreSQL)
e faz upload como CSV na camada Landing do MinIO.

Execução:
    python scripts/ingest/download_olist.py

Requisitos:
    - Supabase (PostgreSQL) acessível com as credenciais definidas no .env
    - MinIO rodando em localhost:9000 (docker compose up -d)
    - psycopg2-binary e boto3 instalados no venv
"""

from __future__ import annotations

import csv
import os
import sys
import tempfile
import logging
from pathlib import Path
from urllib.parse import urlparse

import boto3
from botocore.exceptions import (
    BotoCoreError,
    ClientError,
    EndpointConnectionError,
    ConnectionClosedError,
)

try:
    import psycopg2
    from psycopg2 import OperationalError as PgOperationalError
    from psycopg2 import DatabaseError as PgDatabaseError
except ImportError:
    print(
        "ERRO: psycopg2 não está instalado.\n"
        "Execute: uv add psycopg2-binary   (ou pip install psycopg2-binary)"
    )
    sys.exit(1)

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
PROJECT_ROOT = Path(__file__).resolve().parents[2]

# Mapeamento: nome da tabela no Supabase → nome do arquivo CSV no landing
# Os nomes dos arquivos são mantidos iguais aos originais do Kaggle para
# compatibilidade com o pipeline Landing → Bronze existente.
# Nota: as tabelas foram importadas no Supabase com ".csv" no nome.
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
}

EXPECTED_FILES = list(TABLES.values())


# ──────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────
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


# ──────────────────────────────────────────────
# Supabase / PostgreSQL
# ──────────────────────────────────────────────
def _get_pg_connection():
    """
    Cria e retorna uma conexão com o Supabase (PostgreSQL).

    Lê a connection string da variável de ambiente SUPABASE_URL.
    Formato esperado: postgresql://user:password@host:port/dbname
    """
    dsn = os.getenv("SUPABASE_URL", "")

    if not dsn:
        log.error(
            "SUPABASE_URL não configurada. "
            "Defina a URL de conexão no .env ou nas variáveis de ambiente.\n"
            "Formato: postgresql://user:password@host:port/dbname"
        )
        sys.exit(1)

    try:
        conn = psycopg2.connect(
            dsn=dsn,
            connect_timeout=10,
            # Supabase exige SSL
            sslmode="require",
        )
        log.info("Conectado ao Supabase com sucesso.")
        return conn
    except PgOperationalError as exc:
        log.error(
            "Falha de conexão com o Supabase. Verifique se o host está correto "
            "e se as credenciais estão válidas.\n  Detalhes: %s",
            exc,
        )
        sys.exit(1)
    except PgDatabaseError as exc:
        log.error("Erro de banco de dados ao conectar no Supabase: %s", exc)
        sys.exit(1)


def _extract_table_to_csv(conn, table_name: str, dest_path: Path) -> int:
    """
    Extrai todos os registros de uma tabela e grava como CSV.

    Retorna o número de linhas exportadas.
    """
    log.info("Extraindo tabela '%s' …", table_name)

    try:
        with conn.cursor() as cur:
            # Usa identificador seguro para evitar SQL injection
            cur.execute(
                f'SELECT * FROM "{table_name}"'  # noqa: S608
            )
            rows = cur.fetchall()
            col_names = [desc[0] for desc in cur.description]
    except PgDatabaseError as exc:
        log.error("Erro ao consultar tabela '%s': %s", table_name, exc)
        # Faz rollback para poder continuar com as próximas tabelas
        conn.rollback()
        raise

    with open(dest_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(col_names)
        writer.writerows(rows)

    log.info("  → %d linhas exportadas para %s", len(rows), dest_path.name)
    return len(rows)


def _extract_all_tables(dest_dir: Path) -> list[Path]:
    """
    Conecta no Supabase e extrai todas as tabelas como CSV.

    Retorna a lista de caminhos dos CSVs gerados.
    """
    conn = _get_pg_connection()
    csv_files: list[Path] = []

    try:
        for table_name, csv_filename in TABLES.items():
            csv_path = dest_dir / csv_filename
            try:
                _extract_table_to_csv(conn, table_name, csv_path)
                csv_files.append(csv_path)
            except Exception as exc:
                log.error("Falha ao extrair tabela '%s': %s", table_name, exc)
    finally:
        conn.close()
        log.info("Conexão com Supabase encerrada.")

    if not csv_files:
        log.error("Nenhuma tabela foi extraída com sucesso.")
        sys.exit(1)

    log.info("CSVs extraídos: %d arquivo(s)", len(csv_files))
    return csv_files


# ──────────────────────────────────────────────
# MinIO
# ──────────────────────────────────────────────
def _get_s3_client():
    """Cria e retorna um client boto3 configurado para o MinIO."""
    endpoint = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
    access_key = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    secret_key = os.getenv("MINIO_SECRET_KEY", "minioadmin")

    parsed = urlparse(endpoint)
    use_ssl = parsed.scheme == "https"

    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        use_ssl=use_ssl,
        # MinIO não utiliza regiões, mas o boto3 exige um valor
        region_name="us-east-1",
    )


def _ensure_bucket(s3_client, bucket: str) -> None:
    """Garante que o bucket existe, criando-o se necessário."""
    try:
        s3_client.head_bucket(Bucket=bucket)
        log.info("Bucket '%s' já existe.", bucket)
    except ClientError as exc:
        error_code = int(exc.response["Error"]["Code"])
        if error_code == 404:
            log.info("Criando bucket '%s' …", bucket)
            s3_client.create_bucket(Bucket=bucket)
        else:
            raise


def _upload_to_minio(csv_files: list[Path]) -> None:
    """Faz upload dos CSVs para o bucket landing/ no MinIO (idempotente – sobrescreve)."""
    bucket = os.getenv("MINIO_BUCKET_LANDING", "landing")

    try:
        s3 = _get_s3_client()
        # Teste rápido de conexão
        s3.list_buckets()
    except (EndpointConnectionError, ConnectionClosedError, BotoCoreError) as exc:
        log.error(
            "Falha de conexão com o MinIO. Verifique se o serviço está rodando "
            "e se MINIO_ENDPOINT está correto.\n  Detalhes: %s",
            exc,
        )
        sys.exit(1)

    _ensure_bucket(s3, bucket)

    for csv_path in csv_files:
        object_key = csv_path.name
        log.info("Upload: %s → s3://%s/%s", csv_path.name, bucket, object_key)
        try:
            s3.upload_file(
                Filename=str(csv_path),
                Bucket=bucket,
                Key=object_key,
            )
        except (ClientError, BotoCoreError) as exc:
            log.error("Erro ao enviar '%s': %s", csv_path.name, exc)
            sys.exit(1)

    log.info("✔ %d arquivo(s) enviados para o bucket '%s' com sucesso.", len(csv_files), bucket)


# ──────────────────────────────────────────────
# Verificação final
# ──────────────────────────────────────────────
def _verify(bucket: str | None = None) -> None:
    """Lista os objetos no bucket landing e valida que os 9 CSVs estão presentes."""
    bucket = bucket or os.getenv("MINIO_BUCKET_LANDING", "landing")
    s3 = _get_s3_client()
    response = s3.list_objects_v2(Bucket=bucket)
    objects = [obj["Key"] for obj in response.get("Contents", [])]

    log.info("Objetos no bucket '%s': %s", bucket, objects)
    missing = set(EXPECTED_FILES) - set(objects)
    if missing:
        log.warning("CSVs ausentes no bucket: %s", missing)
    else:
        log.info("✔ Todos os 9 CSVs estão presentes no bucket '%s'.", bucket)


# ──────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────
def main() -> None:
    _load_env()

    log.info("=" * 60)
    log.info("INGESTÃO: Supabase → MinIO (Landing)")
    log.info("=" * 60)

    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        csv_files = _extract_all_tables(tmp_path)
        _upload_to_minio(csv_files)

    _verify()

    log.info("✔ Pipeline de ingestão Supabase → Landing finalizado com sucesso!")


if __name__ == "__main__":
    main()
