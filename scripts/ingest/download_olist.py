

from __future__ import annotations

import os
import sys
import zipfile
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
DATASET_SLUG = "olistbr/brazilian-ecommerce"

EXPECTED_FILES = [
    "olist_customers_dataset.csv",
    "olist_geolocation_dataset.csv",
    "olist_order_items_dataset.csv",
    "olist_order_payments_dataset.csv",
    "olist_order_reviews_dataset.csv",
    "olist_orders_dataset.csv",
    "olist_products_dataset.csv",
    "olist_sellers_dataset.csv",
    "product_category_name_translation.csv",
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


def _download_dataset(dest_dir: Path) -> list[Path]:
    """Baixa o dataset Olist do Kaggle e devolve a lista de CSVs extraídos."""
    # Configura credenciais da Kaggle via variáveis de ambiente
    os.environ.setdefault("KAGGLE_USERNAME", os.getenv("KAGGLE_USERNAME", ""))
    os.environ.setdefault("KAGGLE_KEY", os.getenv("KAGGLE_KEY", ""))

    if not os.environ.get("KAGGLE_USERNAME") or not os.environ.get("KAGGLE_KEY"):
        log.error(
            "Credenciais do Kaggle não configuradas. "
            "Defina KAGGLE_USERNAME e KAGGLE_KEY no .env ou nas variáveis de ambiente."
        )
        sys.exit(1)

    # Importação tardia — a lib kaggle lê as env vars no momento do import
    from kaggle.api.kaggle_api_extended import KaggleApi

    api = KaggleApi()
    api.authenticate()

    log.info("Baixando dataset '%s' do Kaggle …", DATASET_SLUG)
    api.dataset_download_files(DATASET_SLUG, path=str(dest_dir), unzip=False)

    # Localiza o ZIP baixado e descompacta
    zip_files = list(dest_dir.glob("*.zip"))
    if not zip_files:
        log.error("Nenhum arquivo ZIP encontrado em %s", dest_dir)
        sys.exit(1)

    for zf in zip_files:
        log.info("Extraindo %s …", zf.name)
        with zipfile.ZipFile(zf) as z:
            z.extractall(dest_dir)
        zf.unlink()  # remove o zip após extração

    csv_files = sorted(dest_dir.glob("*.csv"))
    log.info("CSVs extraídos: %d arquivo(s)", len(csv_files))

    # Valida que temos os 9 esperados
    found = {f.name for f in csv_files}
    missing = set(EXPECTED_FILES) - found
    if missing:
        log.warning("CSVs esperados mas NÃO encontrados: %s", missing)

    return csv_files


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
    """Faz upload dos CSVs para o bucket landing/ no MinIO."""
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

    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        csv_files = _download_dataset(tmp_path)
        _upload_to_minio(csv_files)

    _verify()


if __name__ == "__main__":
    main()
