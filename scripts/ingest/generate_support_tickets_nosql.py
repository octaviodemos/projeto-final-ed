from __future__ import annotations

import argparse
import csv
import json
import os
import random
from collections import Counter
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Iterable, Sequence

import psycopg2
from psycopg2 import OperationalError as PgOperationalError
from psycopg2.extras import execute_batch
from faker import Faker

DEFAULT_RECORD_COUNT = 12_000
DEFAULT_START_DATE = date(2016, 1, 1)
DEFAULT_END_DATE = date(2018, 12, 31)
DEFAULT_ORDERS_FILENAME = "olist_orders_dataset.csv"

REQUIRED_FIELDS = (
    "ticket_id",
    "order_id",
    "customer_id",
    "agent_id",
    "channel",
    "issue_type",
    "priority",
    "status",
    "opened_at",
    "first_response_minutes",
    "sla_target_hours",
    "resolution",
    "messages",
)

CHANNEL_WEIGHTS = (
    ("chat", 0.35),
    ("email", 0.28),
    ("whatsapp", 0.22),
    ("phone", 0.15),
)

STATUS_WEIGHTS = (
    ("resolved", 0.56),
    ("closed", 0.22),
    ("waiting_customer", 0.09),
    ("waiting_seller", 0.08),
    ("escalated", 0.05),
)

ISSUE_WEIGHTS = {
    "delivered": (
        ("damaged_package", 0.24),
        ("return_exchange", 0.22),
        ("invoice_request", 0.16),
        ("delivery_delay", 0.15),
        ("wrong_item", 0.13),
        ("warranty_question", 0.10),
    ),
    "shipped": (
        ("tracking_question", 0.42),
        ("delivery_delay", 0.30),
        ("address_change", 0.15),
        ("invoice_request", 0.08),
        ("payment_question", 0.05),
    ),
    "canceled": (
        ("refund_status", 0.48),
        ("cancellation_request", 0.27),
        ("payment_question", 0.17),
        ("invoice_request", 0.08),
    ),
    "unavailable": (
        ("refund_status", 0.38),
        ("cancellation_request", 0.24),
        ("seller_stock_question", 0.23),
        ("payment_question", 0.15),
    ),
    "default": (
        ("tracking_question", 0.22),
        ("payment_question", 0.18),
        ("invoice_request", 0.16),
        ("address_change", 0.14),
        ("delivery_delay", 0.13),
        ("return_exchange", 0.10),
        ("seller_stock_question", 0.07),
    ),
}

PRIORITY_WEIGHTS_BY_ISSUE = {
    "damaged_package": (("high", 0.45), ("medium", 0.42), ("low", 0.13)),
    "wrong_item": (("high", 0.44), ("medium", 0.43), ("low", 0.13)),
    "delivery_delay": (("high", 0.35), ("medium", 0.45), ("low", 0.20)),
    "refund_status": (("high", 0.30), ("medium", 0.47), ("low", 0.23)),
    "return_exchange": (("high", 0.25), ("medium", 0.50), ("low", 0.25)),
    "payment_question": (("high", 0.22), ("medium", 0.48), ("low", 0.30)),
    "cancellation_request": (("high", 0.20), ("medium", 0.48), ("low", 0.32)),
    "default": (("high", 0.12), ("medium", 0.46), ("low", 0.42)),
}

SLA_TARGET_HOURS = {
    "high": 8,
    "medium": 24,
    "low": 48,
}

AGENT_TEAMS = {
    "delivery_delay": "logistics",
    "tracking_question": "logistics",
    "address_change": "logistics",
    "damaged_package": "post_sale",
    "wrong_item": "post_sale",
    "return_exchange": "post_sale",
    "warranty_question": "post_sale",
    "payment_question": "payments",
    "refund_status": "payments",
    "cancellation_request": "orders",
    "invoice_request": "orders",
    "seller_stock_question": "seller_ops",
}

COMPENSATION_OPTIONS = {
    "damaged_package": ("product_replacement", "partial_refund", "discount_coupon"),
    "wrong_item": ("product_replacement", "full_refund", "discount_coupon"),
    "delivery_delay": ("freight_refund", "discount_coupon", "no_compensation"),
    "return_exchange": ("exchange_authorized", "full_refund", "no_compensation"),
    "refund_status": ("full_refund", "partial_refund", "no_compensation"),
    "default": ("no_compensation", "discount_coupon"),
}

CUSTOMER_MESSAGE_TEMPLATES = {
    "delivery_delay": [
        "Preciso de ajuda para entender o atraso do meu pedido.",
        "O prazo informado passou e ainda nao recebi atualizacao.",
    ],
    "tracking_question": [
        "Gostaria de confirmar o andamento da entrega.",
        "O rastreio nao atualiza e quero saber o proximo passo.",
    ],
    "damaged_package": [
        "Recebi a embalagem com avaria e preciso registrar o problema.",
        "O pacote chegou danificado e quero orientacao para resolver.",
    ],
    "wrong_item": [
        "O item recebido nao corresponde ao pedido.",
        "Preciso de ajuda porque veio um produto diferente do esperado.",
    ],
    "return_exchange": [
        "Quero solicitar troca ou devolucao deste pedido.",
        "Preciso entender o procedimento de devolucao.",
    ],
    "payment_question": [
        "Tenho uma duvida sobre a confirmacao do pagamento.",
        "Preciso de suporte para verificar a cobranca do pedido.",
    ],
    "refund_status": [
        "Gostaria de acompanhar o status do reembolso.",
        "Ainda nao identifiquei o estorno e preciso de ajuda.",
    ],
    "cancellation_request": [
        "Quero confirmar se o cancelamento do pedido foi processado.",
        "Preciso de suporte para cancelar a compra.",
    ],
    "invoice_request": [
        "Preciso da nota fiscal ou segunda via do documento.",
        "Nao localizei a nota fiscal e gostaria de receber uma copia.",
    ],
    "address_change": [
        "Preciso alterar dados de entrega antes do envio.",
        "Identifiquei um problema no endereco informado.",
    ],
    "seller_stock_question": [
        "Quero confirmar a disponibilidade do item comprado.",
        "Preciso de retorno sobre a reposicao do produto.",
    ],
    "warranty_question": [
        "Tenho uma duvida sobre garantia e assistencia.",
        "Preciso entender como acionar a garantia do produto.",
    ],
}

AGENT_MESSAGE_TEMPLATES = {
    "resolved": [
        "A solicitacao foi tratada e registramos a solucao no atendimento.",
        "Concluimos a analise e enviamos as orientacoes necessarias.",
    ],
    "closed": [
        "O atendimento foi encerrado apos a orientacao final.",
        "Finalizamos o chamado com as informacoes disponiveis.",
    ],
    "waiting_customer": [
        "Aguardamos uma confirmacao do cliente para prosseguir.",
        "Solicitamos informacoes adicionais para continuar o atendimento.",
    ],
    "waiting_seller": [
        "Acionamos o vendedor responsavel e aguardamos retorno.",
        "O caso foi direcionado ao vendedor para validacao.",
    ],
    "escalated": [
        "O chamado foi escalado para uma equipe especializada.",
        "Encaminhamos o caso para analise prioritaria.",
    ],
}

CLOSED_STATUSES = {"resolved", "closed"}


@dataclass(frozen=True)
class OrderReference:
    order_id: str
    customer_id: str
    order_status: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate support-ticket dataset linked to Olist orders and insert into Supabase."
    )
    parser.add_argument("--record-count", type=int, default=DEFAULT_RECORD_COUNT)
    parser.add_argument("--start-date", type=parse_date, default=DEFAULT_START_DATE)
    parser.add_argument("--end-date", type=parse_date, default=DEFAULT_END_DATE)
    parser.add_argument(
        "--orders-csv",
        type=Path,
        help="Local path to olist_orders_dataset.csv (skips Supabase order fetch).",
    )
    parser.add_argument(
        "--supabase-url",
        default=os.getenv("SUPABASE_URL", ""),
        help="Supabase connection string. Defaults to SUPABASE_URL env var.",
    )
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()
    if args.record_count <= 0:
        parser.error("--record-count must be a positive integer.")
    if args.start_date > args.end_date:
        parser.error("--start-date cannot be after --end-date.")
    return args


def parse_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"Invalid date '{value}'. Use YYYY-MM-DD.") from exc


def load_env_file(path: Path) -> None:
    if not path.exists():
        return
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


def _connect(dsn: str):
    from urllib.parse import urlparse, unquote
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


def load_agents_from_supabase(dsn: str) -> dict[str, list[str]]:
    """Returns a dict of team -> list of agent_ids."""
    conn = _connect(dsn)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT agent_id, team FROM agents")
            rows = cur.fetchall()
        agents_by_team: dict[str, list[str]] = {}
        for agent_id, team in rows:
            agents_by_team.setdefault(team, []).append(agent_id)
        if not agents_by_team:
            raise RuntimeError("No agents found in Supabase. Run generate_agents.py first.")
        return agents_by_team
    finally:
        conn.close()


def load_order_references_from_supabase(dsn: str) -> list[OrderReference]:
    conn = _connect(dsn)
    try:
        with conn.cursor() as cur:
            cur.execute('SELECT order_id, customer_id, order_status FROM "olist_orders_dataset.csv"')
            rows = cur.fetchall()
        return extract_order_references(
            {"order_id": r[0], "customer_id": r[1], "order_status": r[2]} for r in rows
        )
    finally:
        conn.close()


def load_order_references_from_csv(path: Path) -> list[OrderReference]:
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return extract_order_references(csv.DictReader(f))


def extract_order_references(rows: Iterable[dict[str, str]]) -> list[OrderReference]:
    references: dict[str, OrderReference] = {}
    for row in rows:
        order_id = (row.get("order_id") or "").strip()
        customer_id = (row.get("customer_id") or "").strip()
        order_status = (row.get("order_status") or "unknown").strip().lower()
        if not order_id or not customer_id:
            continue
        existing = references.get(order_id)
        if existing and existing.customer_id != customer_id:
            raise ValueError(f"Order '{order_id}' linked to multiple customer_ids.")
        references[order_id] = OrderReference(
            order_id=order_id,
            customer_id=customer_id,
            order_status=order_status or "unknown",
        )
    if not references:
        raise ValueError("No valid order_id/customer_id pairs found.")
    return list(references.values())


def create_table_if_not_exists(conn) -> None:
    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS support_ticket_messages")
        cur.execute("DROP TABLE IF EXISTS support_tickets")
        cur.execute("""
            CREATE TABLE support_tickets (
                ticket_id              TEXT PRIMARY KEY,
                order_id               TEXT NOT NULL,
                customer_id            TEXT NOT NULL,
                agent_id               TEXT REFERENCES agents(agent_id),
                channel                TEXT,
                issue_type             TEXT,
                priority               TEXT,
                status                 TEXT,
                opened_at              TIMESTAMPTZ,
                first_response_minutes INTEGER,
                sla_target_hours       INTEGER,
                closed_at              TIMESTAMPTZ,
                resolution_outcome        TEXT,
                resolution_hours          INTEGER,
                requires_seller_action    BOOLEAN,
                resolution_compensation   TEXT
            )
        """)
        cur.execute("""
            CREATE TABLE support_ticket_messages (
                message_id TEXT PRIMARY KEY,
                ticket_id  TEXT NOT NULL REFERENCES support_tickets(ticket_id) ON DELETE CASCADE,
                sender     TEXT NOT NULL,
                sent_at    TIMESTAMPTZ,
                body       TEXT
            )
        """)
    conn.commit()


def insert_into_supabase(dsn: str, tickets: list[dict]) -> None:
    conn = _connect(dsn)
    try:
        create_table_if_not_exists(conn)
        with conn.cursor() as cur:
            execute_batch(
                cur,
                """
                INSERT INTO support_tickets (
                    ticket_id, order_id, customer_id, agent_id, channel, issue_type,
                    priority, status, opened_at, first_response_minutes,
                    sla_target_hours, closed_at,
                    resolution_outcome, resolution_hours,
                    requires_seller_action, resolution_compensation
                ) VALUES (
                    %(ticket_id)s, %(order_id)s, %(customer_id)s, %(agent_id)s, %(channel)s, %(issue_type)s,
                    %(priority)s, %(status)s, %(opened_at)s, %(first_response_minutes)s,
                    %(sla_target_hours)s, %(closed_at)s,
                    %(resolution_outcome)s, %(resolution_hours)s,
                    %(requires_seller_action)s, %(resolution_compensation)s
                )
                """,
                [
                    {
                        **t,
                        "resolution_outcome": t["resolution"]["outcome"],
                        "resolution_hours": t["resolution"]["resolution_hours"],
                        "requires_seller_action": t["resolution"]["requires_seller_action"],
                        "resolution_compensation": t["resolution"]["compensation"],
                    }
                    for t in tickets
                ],
                page_size=500,
            )
            messages = [
                {
                    "message_id": msg["message_id"],
                    "ticket_id": t["ticket_id"],
                    "sender": msg["sender"],
                    "sent_at": msg["sent_at"],
                    "body": msg["body"],
                }
                for t in tickets
                for msg in t["messages"]
            ]
            execute_batch(
                cur,
                """
                INSERT INTO support_ticket_messages (message_id, ticket_id, sender, sent_at, body)
                VALUES (%(message_id)s, %(ticket_id)s, %(sender)s, %(sent_at)s, %(body)s)
                """,
                messages,
                page_size=500,
            )
        conn.commit()
    finally:
        conn.close()


def allocate_years(record_count: int, start_date: date, end_date: date, rng: random.Random) -> list[int]:
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


def random_datetime_for_year(year: int, start_date: date, end_date: date, rng: random.Random) -> datetime:
    lower_bound = max(start_date, date(year, 1, 1))
    upper_bound = min(end_date, date(year, 12, 31))
    start_dt = datetime.combine(lower_bound, time(0, 0, 0), tzinfo=timezone.utc)
    end_dt = datetime.combine(upper_bound, time(23, 59, 59), tzinfo=timezone.utc)
    delta_seconds = int((end_dt - start_dt).total_seconds())
    return start_dt + timedelta(seconds=rng.randint(0, delta_seconds))


def generate_support_tickets(
    order_references: Sequence[OrderReference],
    agents_by_team: dict[str, list[str]],
    record_count: int,
    start_date: date,
    end_date: date,
    seed: int,
) -> list[dict[str, object]]:
    if not order_references:
        raise ValueError("At least one valid order reference is required.")

    rng = random.Random(seed)
    Faker.seed(seed)
    fake = Faker("pt_BR")
    fake.seed_instance(seed)

    selected_orders = select_order_references(order_references, record_count, rng)
    assigned_years = allocate_years(record_count, start_date, end_date, rng)

    tickets: list[dict[str, object]] = []
    for order_reference, assigned_year in zip(selected_orders, assigned_years):
        ticket_id = f"sup_{rng.getrandbits(128):032x}"
        opened_at = random_datetime_for_year(assigned_year, start_date=start_date, end_date=end_date, rng=rng)
        issue_type = choose_issue_type(order_reference.order_status, rng)
        priority = choose_priority(issue_type, rng)
        status = weighted_choice(STATUS_WEIGHTS, rng)
        first_response_minutes = build_first_response_minutes(priority, rng)
        closed_at, resolution_hours = build_resolution_time(opened_at, status, priority, rng)

        tickets.append(
            {
                "ticket_id": ticket_id,
                "order_id": order_reference.order_id,
                "customer_id": order_reference.customer_id,
                "agent_id": pick_agent(issue_type, agents_by_team, rng),
                "channel": weighted_choice(CHANNEL_WEIGHTS, rng),
                "issue_type": issue_type,
                "priority": priority,
                "status": status,
                "opened_at": format_datetime(opened_at),
                "first_response_minutes": first_response_minutes,
                "sla_target_hours": SLA_TARGET_HOURS[priority],
                "closed_at": format_datetime(closed_at) if closed_at else None,
                "resolution": build_resolution(issue_type, status, resolution_hours, rng),
                "messages": build_messages(
                    ticket_id=ticket_id,
                    issue_type=issue_type,
                    status=status,
                    opened_at=opened_at,
                    first_response_minutes=first_response_minutes,
                    fake=fake,
                    rng=rng,
                ),
            }
        )

    validate_support_tickets(tickets, order_references, record_count, start_date, end_date)
    return tickets


def select_order_references(
    order_references: Sequence[OrderReference],
    record_count: int,
    rng: random.Random,
) -> list[OrderReference]:
    if record_count <= len(order_references):
        return rng.sample(list(order_references), k=record_count)
    return [rng.choice(order_references) for _ in range(record_count)]


def choose_issue_type(order_status: str, rng: random.Random) -> str:
    return weighted_choice(ISSUE_WEIGHTS.get(order_status, ISSUE_WEIGHTS["default"]), rng)


def choose_priority(issue_type: str, rng: random.Random) -> str:
    weights = PRIORITY_WEIGHTS_BY_ISSUE.get(issue_type, PRIORITY_WEIGHTS_BY_ISSUE["default"])
    return weighted_choice(weights, rng)


def weighted_choice(options: Sequence[tuple[str, float]], rng: random.Random) -> str:
    labels = [label for label, _ in options]
    weights = [weight for _, weight in options]
    return rng.choices(labels, weights=weights, k=1)[0]


def build_first_response_minutes(priority: str, rng: random.Random) -> int:
    if priority == "high":
        return rng.randint(3, 90)
    if priority == "medium":
        return rng.randint(15, 360)
    return rng.randint(60, 960)


def build_resolution_time(
    opened_at: datetime,
    status: str,
    priority: str,
    rng: random.Random,
) -> tuple[datetime | None, int | None]:
    if status not in CLOSED_STATUSES:
        return None, None
    if priority == "high":
        resolution_hours = rng.randint(2, 72)
    elif priority == "medium":
        resolution_hours = rng.randint(6, 168)
    else:
        resolution_hours = rng.randint(12, 336)
    return opened_at + timedelta(hours=resolution_hours), resolution_hours


def pick_agent(issue_type: str, agents_by_team: dict[str, list[str]], rng: random.Random) -> str:
    team = AGENT_TEAMS[issue_type]
    pool = agents_by_team.get(team) or agents_by_team[next(iter(agents_by_team))]
    return rng.choice(pool)


def build_agent(issue_type: str, fake: Faker, rng: random.Random) -> dict[str, str]:
    return {
        "agent_id": f"agt_{rng.randint(1000, 9999)}",
        "team": AGENT_TEAMS[issue_type],
        "alias": fake.first_name(),
    }


def build_resolution(
    issue_type: str,
    status: str,
    resolution_hours: int | None,
    rng: random.Random,
) -> dict[str, object]:
    requires_seller_action = issue_type in {
        "damaged_package", "wrong_item", "return_exchange",
        "seller_stock_question", "warranty_question",
    }
    compensation_pool = COMPENSATION_OPTIONS.get(issue_type, COMPENSATION_OPTIONS["default"])
    if status in CLOSED_STATUSES:
        outcome = "solved" if status == "resolved" else "closed_without_followup"
        compensation = rng.choice(compensation_pool)
    elif status == "escalated":
        outcome = "escalated_to_specialist"
        compensation = None
    elif status == "waiting_seller":
        outcome = "waiting_seller_response"
        compensation = None
    else:
        outcome = "waiting_customer_response"
        compensation = None
    return {
        "outcome": outcome,
        "resolution_hours": resolution_hours,
        "requires_seller_action": requires_seller_action,
        "compensation": compensation,
    }


def build_messages(
    ticket_id: str,
    issue_type: str,
    status: str,
    opened_at: datetime,
    first_response_minutes: int,
    fake: Faker,
    rng: random.Random,
) -> list[dict[str, str]]:
    customer_text = build_customer_message(issue_type, fake, rng)
    agent_text = build_agent_message(status, fake, rng)
    first_response_at = opened_at + timedelta(minutes=first_response_minutes)
    return [
        {
            "message_id": f"{ticket_id}_msg_001",
            "sender": "customer",
            "sent_at": format_datetime(opened_at),
            "body": customer_text,
        },
        {
            "message_id": f"{ticket_id}_msg_002",
            "sender": "support_agent",
            "sent_at": format_datetime(first_response_at),
            "body": agent_text,
        },
    ]


def build_customer_message(issue_type: str, fake: Faker, rng: random.Random) -> str:
    prefix = rng.choice(CUSTOMER_MESSAGE_TEMPLATES[issue_type])
    return f"{prefix} {fake.sentence(nb_words=rng.randint(6, 12))}"


def build_agent_message(status: str, fake: Faker, rng: random.Random) -> str:
    prefix = rng.choice(AGENT_MESSAGE_TEMPLATES[status])
    return f"{prefix} {fake.sentence(nb_words=rng.randint(6, 12))}"


def validate_support_tickets(
    tickets: Sequence[dict[str, object]],
    order_references: Sequence[OrderReference],
    minimum_count: int,
    start_date: date,
    end_date: date,
) -> Counter[int]:
    assert len(tickets) >= minimum_count, "Generated record count is below the requested minimum."

    ticket_ids = [str(ticket["ticket_id"]) for ticket in tickets]
    assert len(ticket_ids) == len(set(ticket_ids)), "ticket_id values must be unique."

    valid_pairs = {(r.order_id, r.customer_id) for r in order_references}
    opened_at_years: Counter[int] = Counter()
    expected_issue_types = {
        issue_type for issue_options in ISSUE_WEIGHTS.values() for issue_type, _ in issue_options
    }
    expected_channels = {channel for channel, _ in CHANNEL_WEIGHTS}
    expected_statuses = {status for status, _ in STATUS_WEIGHTS}

    for ticket in tickets:
        assert set(REQUIRED_FIELDS).issubset(ticket), "A generated support ticket is missing required fields."
        assert "review_score" not in ticket
        assert "review_comment_message" not in ticket
        assert (ticket["order_id"], ticket["customer_id"]) in valid_pairs
        assert ticket["issue_type"] in expected_issue_types
        assert ticket["channel"] in expected_channels
        assert ticket["status"] in expected_statuses
        assert ticket["priority"] in SLA_TARGET_HOURS
        assert ticket["agent_id"], "agent_id must be set."
        assert isinstance(ticket["messages"], list) and ticket["messages"]

        opened_at = parse_created_at(str(ticket["opened_at"]))
        assert start_date <= opened_at.date() <= end_date
        opened_at_years[opened_at.year] += 1

        closed_at_raw = ticket.get("closed_at")
        if ticket["status"] in CLOSED_STATUSES:
            assert closed_at_raw
            assert parse_created_at(str(closed_at_raw)) > opened_at
        else:
            assert closed_at_raw is None

    expected_years = set(range(start_date.year, end_date.year + 1))
    assert expected_years.issubset(opened_at_years)
    return opened_at_years


def parse_created_at(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def format_datetime(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def print_summary(records: Sequence[dict], orders_source: str, year_counts: Counter[int]) -> None:
    summary = {
        "dataset": "support_tickets",
        "records_inserted": len(records),
        "orders_source": orders_source,
        "destination": "supabase:support_tickets",
        "year_distribution": dict(sorted(year_counts.items())),
    }
    print(json.dumps(summary, ensure_ascii=False, indent=2))


def main() -> int:
    repo_root = Path(__file__).resolve().parents[2]
    load_env_file(repo_root / ".env")
    args = parse_args()

    dsn = args.supabase_url or os.getenv("SUPABASE_URL", "")
    if not dsn:
        raise RuntimeError(
            "SUPABASE_URL não configurada. Defina no .env ou passe --supabase-url."
        )

    if args.orders_csv:
        order_references = load_order_references_from_csv(args.orders_csv)
        orders_source = str(args.orders_csv)
    else:
        order_references = load_order_references_from_supabase(dsn)
        orders_source = "supabase:olist_orders_dataset.csv"

    agents_by_team = load_agents_from_supabase(dsn)

    records = generate_support_tickets(
        order_references=order_references,
        agents_by_team=agents_by_team,
        record_count=args.record_count,
        start_date=args.start_date,
        end_date=args.end_date,
        seed=args.seed,
    )
    year_counts = validate_support_tickets(
        records,
        order_references=order_references,
        minimum_count=args.record_count,
        start_date=args.start_date,
        end_date=args.end_date,
    )

    insert_into_supabase(dsn, records)
    print_summary(records, orders_source, year_counts)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
