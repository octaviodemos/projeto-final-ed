from __future__ import annotations

import argparse
import json
import os
import random
from pathlib import Path

import psycopg2
from psycopg2 import OperationalError as PgOperationalError
from psycopg2.extras import execute_batch
from faker import Faker

TEAMS = ["logistics", "post_sale", "payments", "orders", "seller_ops"]
DEFAULT_AGENTS_PER_TEAM = 10
DEFAULT_SEED = 42


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate agents table in Supabase.")
    parser.add_argument("--agents-per-team", type=int, default=DEFAULT_AGENTS_PER_TEAM)
    parser.add_argument(
        "--supabase-url",
        default=os.getenv("SUPABASE_URL", ""),
        help="Supabase connection string. Defaults to SUPABASE_URL env var.",
    )
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED)
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


def generate_agents(agents_per_team: int, seed: int) -> list[dict]:
    Faker.seed(seed)
    fake = Faker("pt_BR")
    rng = random.Random(seed)

    agents = []
    counter = 1
    for team in TEAMS:
        for _ in range(agents_per_team):
            agents.append({
                "agent_id": f"agt_{counter:04d}",
                "alias": fake.name(),
                "team": team,
            })
            counter += 1

    rng.shuffle(agents)
    return agents


def create_table_if_not_exists(conn) -> None:
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS agents (
                agent_id TEXT PRIMARY KEY,
                alias    TEXT NOT NULL,
                team     TEXT NOT NULL
            )
        """)
    conn.commit()


def insert_agents(dsn: str, agents: list[dict]) -> None:
    conn = _connect(dsn)
    try:
        create_table_if_not_exists(conn)
        with conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE agents")
            execute_batch(
                cur,
                "INSERT INTO agents (agent_id, alias, team) VALUES (%(agent_id)s, %(alias)s, %(team)s)",
                agents,
            )
        conn.commit()
    finally:
        conn.close()


def main() -> int:
    repo_root = Path(__file__).resolve().parents[2]
    load_env_file(repo_root / ".env")
    args = parse_args()

    dsn = args.supabase_url or os.getenv("SUPABASE_URL", "")
    if not dsn:
        raise RuntimeError("SUPABASE_URL não configurada.")

    agents = generate_agents(args.agents_per_team, args.seed)
    insert_agents(dsn, agents)

    print(json.dumps({
        "agents_inserted": len(agents),
        "teams": TEAMS,
        "agents_per_team": args.agents_per_team,
    }, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
