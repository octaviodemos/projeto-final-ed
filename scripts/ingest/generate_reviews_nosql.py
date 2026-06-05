from __future__ import annotations

import argparse
import csv
import io
import json
import os
import random
import tempfile
from collections import Counter
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path, PurePosixPath
from typing import Iterable, Sequence

import boto3
from botocore.exceptions import BotoCoreError, ClientError
from faker import Faker

DEFAULT_RECORD_COUNT = 12_000
DEFAULT_START_DATE = date(2022, 1, 1)
DEFAULT_END_DATE = date(2025, 12, 31)
DEFAULT_BUCKET_NAME = "landing"
DEFAULT_PREFIX = "nosql"
DEFAULT_OBJECT_NAME = "reviews_nosql.json"
DEFAULT_ORDERS_FILENAME = "olist_orders_dataset.csv"
REQUIRED_FIELDS = (
    "review_id",
    "order_id",
    "customer_id",
    "sentiment",
    "comment_text",
    "tags",
    "created_at",
)
SENTIMENT_WEIGHTS = (
    ("positive", 0.62),
    ("neutral", 0.23),
    ("negative", 0.15),
)
COMMENT_PREFIXES = {
    "positive": [
        "Entrega rapida e produto conforme o esperado",
        "Experiencia excelente do inicio ao fim",
        "Atendimento bom e compra sem problemas",
        "Produto chegou em otimo estado e antes do prazo",
    ],
    "neutral": [
        "Compra dentro do esperado, sem grandes destaques",
        "Pedido entregue corretamente, mas a experiencia foi comum",
        "Produto atendeu o basico, sem surpreender",
        "Tudo certo com o pedido, embora haja pontos a melhorar",
    ],
    "negative": [
        "Entrega atrasada e experiencia abaixo do esperado",
        "Produto chegou com problemas e gerou frustracao",
        "Atendimento demorado e resolucao insatisfatoria",
        "Compra trouxe mais desgaste do que beneficio",
    ],
}
TAG_POOLS = {
    "positive": [
        "delivery",
        "packaging",
        "product_quality",
        "seller_service",
        "value",
        "speed",
    ],
    "neutral": [
        "delivery",
        "communication",
        "expectation",
        "price",
        "product_match",
        "service",
    ],
    "negative": [
        "delay",
        "damaged_item",
        "support",
        "wrong_item",
        "refund",
        "packaging",
    ],
}


@dataclass(frozen=True)
class OrderReference:
    """Representa um par valido order_id/customer_id vindo do Olist."""

    order_id: str
    customer_id: str


@dataclass(frozen=True)
class MinioConfig:
    """Agrupa as configuracoes necessarias para falar com o MinIO."""

    endpoint_url: str
    access_key: str
    secret_key: str
    bucket_name: str
    prefix: str


def parse_args() -> argparse.Namespace:
    """Lê os argumentos de linha de comando e valida o basico."""

    parser = argparse.ArgumentParser(
        description="Generate a complementary NoSQL review dataset and upload it to Landing."
    )
    parser.add_argument(
        "--record-count",
        type=int,
        default=DEFAULT_RECORD_COUNT,
        help="Number of review documents to generate. Default: 12000.",
    )
    parser.add_argument(
        "--start-date",
        type=parse_date,
        default=DEFAULT_START_DATE,
        help="Inclusive lower bound for created_at in YYYY-MM-DD format.",
    )
    parser.add_argument(
        "--end-date",
        type=parse_date,
        default=DEFAULT_END_DATE,
        help="Inclusive upper bound for created_at in YYYY-MM-DD format.",
    )
    parser.add_argument(
        "--orders-csv",
        type=Path,
        help="Optional local path to olist_orders_dataset.csv for development or testing.",
    )
    parser.add_argument(
        "--orders-key",
        help="Optional object key for olist_orders_dataset.csv in Landing.",
    )
    parser.add_argument(
        "--output-file",
        type=Path,
        help="Optional local output path for the generated JSON file.",
    )
    parser.add_argument(
        "--bucket-name",
        default=os.getenv("MINIO_BUCKET_LANDING", DEFAULT_BUCKET_NAME),
        help="Landing bucket name. Defaults to MINIO_BUCKET_LANDING or 'landing'.",
    )
    parser.add_argument(
        "--prefix",
        default=DEFAULT_PREFIX,
        help="Destination prefix inside the Landing bucket. Default: nosql.",
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
        "--seed",
        type=int,
        default=42,
        help="Deterministic seed for reproducible generation. Default: 42.",
    )
    parser.add_argument(
        "--skip-upload",
        action="store_true",
        help="Generate the JSON file locally without uploading it to MinIO.",
    )
    args = parser.parse_args()
    if args.record_count <= 0:
        parser.error("--record-count must be a positive integer.")
    if args.start_date > args.end_date:
        parser.error("--start-date cannot be after --end-date.")
    return args


def parse_date(value: str) -> date:
    """Converte uma string YYYY-MM-DD em date para uso no argparse."""

    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            f"Invalid date '{value}'. Use YYYY-MM-DD."
        ) from exc


def load_env_file(path: Path) -> None:
    """Carrega um arquivo .env simples sem depender de bibliotecas extras."""

    if not path.exists():
        return

    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        os.environ.setdefault(key, value)


def build_minio_config(args: argparse.Namespace) -> MinioConfig:
    """Transforma os argumentos recebidos em uma configuracao de acesso ao MinIO."""

    return MinioConfig(
        endpoint_url=args.minio_endpoint,
        access_key=args.minio_access_key,
        secret_key=args.minio_secret_key,
        bucket_name=args.bucket_name,
        prefix=args.prefix.strip("/"),
    )


def create_s3_client(config: MinioConfig):
    """Cria um cliente S3 compativel com MinIO usando boto3."""

    return boto3.client(
        "s3",
        endpoint_url=config.endpoint_url,
        aws_access_key_id=config.access_key,
        aws_secret_access_key=config.secret_key,
        region_name="us-east-1",
    )


def load_order_references_from_csv(path: Path) -> list[OrderReference]:
    """Lê um CSV local do Olist e extrai os pares validos de pedido e cliente."""

    with path.open("r", encoding="utf-8-sig", newline="") as csv_file:
        reader = csv.DictReader(csv_file)
        return extract_order_references(reader)


def load_order_references_from_minio(
    s3_client,
    bucket_name: str,
    orders_key: str | None,
) -> tuple[list[OrderReference], str]:
    """Baixa o CSV de pedidos do Landing e devolve os pares validos extraidos dele."""

    resolved_key = resolve_orders_key(s3_client, bucket_name, orders_key)
    response = s3_client.get_object(Bucket=bucket_name, Key=resolved_key)
    payload = response["Body"].read().decode("utf-8-sig")
    reader = csv.DictReader(io.StringIO(payload))
    return extract_order_references(reader), resolved_key


def resolve_orders_key(s3_client, bucket_name: str, orders_key: str | None) -> str:
    """Descobre qual objeto do bucket contem o CSV de pedidos do Olist."""

    if orders_key:
        return orders_key

    paginator = s3_client.get_paginator("list_objects_v2")
    matches: list[dict] = []

    for page in paginator.paginate(Bucket=bucket_name):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(DEFAULT_ORDERS_FILENAME):
                matches.append(obj)

    if not matches:
        raise FileNotFoundError(
            f"Could not find '{DEFAULT_ORDERS_FILENAME}' in bucket '{bucket_name}'. "
            "Provide --orders-csv or --orders-key."
        )

    matches.sort(key=lambda item: item["LastModified"], reverse=True)
    return matches[0]["Key"]


def extract_order_references(rows: Iterable[dict[str, str]]) -> list[OrderReference]:
    """Deduplica e valida os pares order_id/customer_id da origem."""

    references: dict[str, str] = {}

    for row in rows:
        order_id = (row.get("order_id") or "").strip()
        customer_id = (row.get("customer_id") or "").strip()
        if not order_id or not customer_id:
            continue

        existing_customer_id = references.get(order_id)
        if existing_customer_id and existing_customer_id != customer_id:
            raise ValueError(
                f"Order '{order_id}' is linked to multiple customer_ids in the source data."
            )
        references[order_id] = customer_id

    if not references:
        raise ValueError("No valid order_id/customer_id pairs were found in the orders dataset.")

    return [
        OrderReference(order_id=order_id, customer_id=customer_id)
        for order_id, customer_id in references.items()
    ]


def allocate_years(
    record_count: int,
    start_date: date,
    end_date: date,
    rng: random.Random,
) -> list[int]:
    """Distribui os registros entre os anos da janela para garantir cobertura anual."""

    years = list(range(start_date.year, end_date.year + 1))
    base_count, remainder = divmod(record_count, len(years))
    allocations = {year: base_count for year in years}

    for year in rng.sample(years, k=remainder):
        allocations[year] += 1

    year_assignments: list[int] = []
    for year in years:
        year_assignments.extend([year] * allocations[year])

    rng.shuffle(year_assignments)
    return year_assignments


def random_datetime_for_year(
    year: int,
    start_date: date,
    end_date: date,
    rng: random.Random,
) -> datetime:
    """Gera um timestamp aleatorio dentro de um ano respeitando a janela total."""

    lower_bound = max(start_date, date(year, 1, 1))
    upper_bound = min(end_date, date(year, 12, 31))

    start_dt = datetime.combine(lower_bound, time(0, 0, 0), tzinfo=timezone.utc)
    end_dt = datetime.combine(upper_bound, time(23, 59, 59), tzinfo=timezone.utc)
    delta_seconds = int((end_dt - start_dt).total_seconds())
    return start_dt + timedelta(seconds=rng.randint(0, delta_seconds))


def generate_reviews(
    order_references: Sequence[OrderReference],
    record_count: int,
    start_date: date,
    end_date: date,
    seed: int,
) -> list[dict[str, object]]:
    """Monta os documentos JSON usando pares validos do Olist e campos sinteticos."""

    if not order_references:
        raise ValueError("At least one valid order reference is required.")

    rng = random.Random(seed)
    Faker.seed(seed)
    fake = Faker("pt_BR")
    fake.seed_instance(seed)

    selected_orders = select_order_references(order_references, record_count, rng)
    assigned_years = allocate_years(record_count, start_date, end_date, rng)
    sentiments = [label for label, _ in SENTIMENT_WEIGHTS]
    weights = [weight for _, weight in SENTIMENT_WEIGHTS]

    reviews: list[dict[str, object]] = []
    for order_reference, assigned_year in zip(selected_orders, assigned_years):
        sentiment = rng.choices(sentiments, weights=weights, k=1)[0]
        created_at = random_datetime_for_year(
            assigned_year,
            start_date=start_date,
            end_date=end_date,
            rng=rng,
        )
        reviews.append(
            {
                "review_id": f"rvw_{rng.getrandbits(128):032x}",
                "order_id": order_reference.order_id,
                "customer_id": order_reference.customer_id,
                "sentiment": sentiment,
                "comment_text": build_comment_text(sentiment, fake, rng),
                "tags": build_tags(sentiment, rng),
                "created_at": created_at.isoformat().replace("+00:00", "Z"),
            }
        )

    validate_reviews(reviews, order_references, record_count, start_date, end_date)
    return reviews


def select_order_references(
    order_references: Sequence[OrderReference],
    record_count: int,
    rng: random.Random,
) -> list[OrderReference]:
    """Escolhe os pedidos base usados na geracao, repetindo quando necessario."""

    if record_count <= len(order_references):
        return rng.sample(list(order_references), k=record_count)
    return [rng.choice(order_references) for _ in range(record_count)]


def build_comment_text(sentiment: str, fake: Faker, rng: random.Random) -> str:
    """Gera um comentario coerente com o sentimento usando textos base e Faker."""

    prefix = rng.choice(COMMENT_PREFIXES[sentiment])
    extra_sentences = fake.sentences(nb=rng.randint(1, 2))
    return " ".join([prefix + "."] + extra_sentences)


def build_tags(sentiment: str, rng: random.Random) -> list[str]:
    """Sorteia tags coerentes com o sentimento do review."""

    tag_pool = TAG_POOLS[sentiment]
    tag_count = rng.randint(1, min(4, len(tag_pool)))
    return rng.sample(tag_pool, k=tag_count)


def validate_reviews(
    reviews: Sequence[dict[str, object]],
    order_references: Sequence[OrderReference],
    minimum_count: int,
    start_date: date,
    end_date: date,
) -> Counter[int]:
    """Aplica os asserts do criterio de aceite e devolve a distribuicao por ano."""

    assert len(reviews) >= minimum_count, "Generated record count is below the requested minimum."

    review_ids = [str(review["review_id"]) for review in reviews]
    assert len(review_ids) == len(set(review_ids)), "review_id values must be unique."

    valid_pairs = {
        (order_reference.order_id, order_reference.customer_id)
        for order_reference in order_references
    }
    created_at_years: Counter[int] = Counter()

    for review in reviews:
        assert set(REQUIRED_FIELDS).issubset(review), "A generated review is missing required fields."
        assert isinstance(review["tags"], list), "tags must be serialized as an array."
        assert review["tags"], "tags cannot be empty."
        assert isinstance(review["comment_text"], str) and review["comment_text"].strip(), (
            "comment_text cannot be empty."
        )
        assert (review["order_id"], review["customer_id"]) in valid_pairs, (
            "Generated order/customer pair does not exist in the source Olist data."
        )

        created_at = parse_created_at(str(review["created_at"]))
        assert start_date <= created_at.date() <= end_date, (
            "created_at is outside the accepted date window."
        )
        created_at_years[created_at.year] += 1

    expected_years = set(range(start_date.year, end_date.year + 1))
    assert expected_years.issubset(created_at_years), (
        "The generated dataset must include records in every year of the configured range."
    )
    return created_at_years


def parse_created_at(value: str) -> datetime:
    """Converte o timestamp serializado de volta para datetime."""

    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def write_json(reviews: Iterable[dict[str, object]], output_path: Path) -> None:
    """Escreve todos os reviews em um unico arquivo JSON como lista de objetos."""

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8", newline="\n") as output_file:
        json.dump(list(reviews), output_file, ensure_ascii=False, indent=2)


def build_object_key(prefix: str, execution_date: date) -> str:
    """Monta o caminho do objeto dentro do bucket Landing."""

    return str(PurePosixPath(prefix) / execution_date.isoformat() / DEFAULT_OBJECT_NAME)


def upload_json(s3_client, bucket_name: str, object_key: str, file_path: Path) -> None:
    """Envia o arquivo JSON gerado para o bucket configurado no MinIO."""

    with file_path.open("rb") as file_obj:
        s3_client.upload_fileobj(
            file_obj,
            bucket_name,
            object_key,
            ExtraArgs={"ContentType": "application/json"},
        )


def print_summary(
    reviews: Sequence[dict[str, object]],
    bucket_name: str,
    object_key: str | None,
    orders_source: str,
    year_counts: Counter[int],
) -> None:
    """Mostra um resumo final para facilitar validacao manual da execucao."""

    summary = {
        "records_generated": len(reviews),
        "orders_source": orders_source,
        "destination_bucket": bucket_name,
        "destination_key": object_key,
        "year_distribution": dict(sorted(year_counts.items())),
    }
    print(json.dumps(summary, ensure_ascii=False, indent=2))


def main() -> int:
    """Orquestra leitura da origem, geracao, validacao, escrita e upload."""

    repo_root = Path(__file__).resolve().parents[2]
    load_env_file(repo_root / ".env")
    args = parse_args()
    config = build_minio_config(args)

    if args.orders_csv:
        order_references = load_order_references_from_csv(args.orders_csv)
        orders_source = str(args.orders_csv)
    else:
        s3_client = create_s3_client(config)
        order_references, resolved_orders_key = load_order_references_from_minio(
            s3_client=s3_client,
            bucket_name=config.bucket_name,
            orders_key=args.orders_key,
        )
        orders_source = f"s3://{config.bucket_name}/{resolved_orders_key}"

    reviews = generate_reviews(
        order_references=order_references,
        record_count=args.record_count,
        start_date=args.start_date,
        end_date=args.end_date,
        seed=args.seed,
    )
    year_counts = validate_reviews(
        reviews,
        order_references=order_references,
        minimum_count=args.record_count,
        start_date=args.start_date,
        end_date=args.end_date,
    )

    object_key = build_object_key(config.prefix, datetime.now(timezone.utc).date())
    temp_output: Path | None = None

    try:
        if args.output_file:
            output_path = args.output_file
        else:
            # Quando o usuario nao escolhe um destino local, o script usa um arquivo temporario
            # apenas como etapa intermediaria antes do upload.
            temp_file = tempfile.NamedTemporaryFile(
                mode="w",
                suffix=".json",
                prefix="reviews_nosql_",
                delete=False,
                encoding="utf-8",
            )
            temp_file.close()
            temp_output = Path(temp_file.name)
            output_path = temp_output

        write_json(reviews, output_path)

        if args.skip_upload:
            print_summary(
                reviews,
                bucket_name=config.bucket_name,
                object_key=None,
                orders_source=orders_source,
                year_counts=year_counts,
            )
            print(f"Local JSON generated at: {output_path}")
            return 0

        if args.orders_csv:
            s3_client = create_s3_client(config)

        upload_json(
            s3_client=s3_client,
            bucket_name=config.bucket_name,
            object_key=object_key,
            file_path=output_path,
        )
        print_summary(
            reviews,
            bucket_name=config.bucket_name,
            object_key=object_key,
            orders_source=orders_source,
            year_counts=year_counts,
        )
        return 0
    except (BotoCoreError, ClientError) as exc:
        raise RuntimeError(f"Failed to interact with MinIO: {exc}") from exc
    finally:
        if temp_output and temp_output.exists():
            temp_output.unlink()


if __name__ == "__main__":
    raise SystemExit(main())
