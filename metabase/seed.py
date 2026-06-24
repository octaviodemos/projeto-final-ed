#!/usr/bin/env python3
import json
import os
import sys
import time
import urllib.error
import urllib.request

MB_URL      = os.environ.get("MB_URL", "http://metabase:3000")
MB_EMAIL    = os.environ["MB_ADMIN_EMAIL"]
MB_PASSWORD = os.environ["MB_ADMIN_PASSWORD"]
TRINO_PORT  = int(os.environ.get("TRINO_PORT", "8080"))


def api(method, path, data=None, token=None):
    url = f"{MB_URL}{path}"
    body = json.dumps(data).encode() if data is not None else None
    headers = {"Content-Type": "application/json"}
    if token:
        headers["X-Metabase-Session"] = token
    req = urllib.request.Request(url, data=body, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req) as r:
            body = r.read()
            return json.loads(body) if body else None
    except urllib.error.HTTPError as e:
        try:
            body = e.read().decode()
        except Exception:
            body = f"HTTP {e.code}"
        raise RuntimeError(f"{method} {path} → {e.code}: {body}") from None


def wait_ready():
    print("Aguardando Metabase...", flush=True)
    for _ in range(60):
        try:
            api("GET", "/api/health")
            return
        except Exception:
            time.sleep(5)
    sys.exit("Metabase não ficou pronto a tempo.")


def authenticate():
    props = api("GET", "/api/session/properties")
    setup_token = props.get("setup-token")

    if setup_token:
        print("Configurando Metabase pela primeira vez...", flush=True)
        try:
            api("POST", "/api/setup", {
                "token": setup_token,
                "user": {
                    "first_name": "Admin",
                    "last_name": "User",
                    "email": MB_EMAIL,
                    "password": MB_PASSWORD,
                    "site_name": "Lakehouse",
                },
                "prefs": {"site_name": "Lakehouse", "allow_tracking": False},
                "database": None,
            })
        except RuntimeError as e:
            if "403" in str(e):
                print("Usuário já existe, fazendo login...", flush=True)
            else:
                raise

    resp = api("POST", "/api/session", {"username": MB_EMAIL, "password": MB_PASSWORD})
    return resp["id"]


def get_or_create_db(token):
    dbs = api("GET", "/api/database", token=token)
    for db in dbs.get("data", []):
        if db["name"] == "Trino Gold":
            print(f"Conexão Trino já existe (id={db['id']})", flush=True)
            return db["id"]

    print("Criando conexão com Trino...", flush=True)
    db = api("POST", "/api/database", {
        "name": "Trino Gold",
        "engine": "starburst",
        "details": {
            "host": "trino",
            "port": TRINO_PORT,
            "catalog": "delta",
            "schema": "gold",
            "user": "admin",
            "ssl": False,
        },
    }, token=token)
    time.sleep(5)
    return db["id"]


def get_or_create_card(token, db_id, name, sql, display="scalar", viz=None):
    existing = api("GET", "/api/card", token=token)
    for card in existing:
        if card["name"] == name and not card.get("archived", False):
            print(f"Card '{name}' já existe (id={card['id']})", flush=True)
            return card["id"]
    card = api("POST", "/api/card", {
        "name": name,
        "display": display,
        "dataset_query": {
            "type": "native",
            "native": {"query": sql},
            "database": db_id,
        },
        "visualization_settings": viz or {},
    }, token=token)
    return card["id"]


def main():
    wait_ready()
    token = authenticate()
    print("Autenticado.", flush=True)

    def _find_dashboard(response):
        items = response if isinstance(response, list) else response.get("data", [])
        return next((d for d in items if d.get("name") == "Dashboard Gold"), None)

    did = None
    found = _find_dashboard(api("GET", "/api/dashboard?archived=false", token=token))
    if found is None:
        found = _find_dashboard(api("GET", "/api/dashboard?archived=true", token=token))
        if found:
            api("PUT", f"/api/dashboard/{found['id']}", {"archived": False}, token=token)
    if found:
        did = found["id"]
        print(f"Dashboard já existe (id={did}), atualizando...", flush=True)

    db_id = get_or_create_db(token)
    api("POST", f"/api/database/{db_id}/sync_schema", token=token)
    print("Sync do schema iniciado.", flush=True)

    print("Criando cards...", flush=True)

    kpi_receita = get_or_create_card(token, db_id, "Receita total",
        "SELECT SUM(price) FROM delta.gold.fact_orders WHERE is_delivered = true")

    kpi_pedidos = get_or_create_card(token, db_id, "Pedidos abertos",
        "SELECT COUNT(DISTINCT order_id) FROM delta.gold.fact_orders WHERE is_pending = true")

    kpi_avaliacao = get_or_create_card(token, db_id, "Avaliação média",
        "SELECT ROUND(AVG(CAST(review_score AS double)), 2) FROM delta.gold.fact_reviews")

    kpi_prazo = get_or_create_card(token, db_id, "Prazo médio de entrega",
        "SELECT ROUND(AVG(CAST(delivery_days AS double)), 2) FROM delta.gold.fact_orders WHERE is_delivered = true",
        viz={"scalar.suffix": "d"})

    chart_rating = get_or_create_card(token, db_id, "Classificação x Prazo de Entrega", """\
SELECT
  CAST(d.year AS varchar) || '-' || LPAD(CAST(d.month AS varchar), 2, '0') AS "Mês",
  ROUND(AVG(CAST(r.review_score AS double)), 2) AS "Avaliação Média"
FROM delta.gold.fact_reviews r
JOIN delta.gold.dim_date d ON r.date_id = d.date_id
GROUP BY d.year, d.month
ORDER BY d.year, d.month""", display="line")

    chart_tickets = get_or_create_card(token, db_id, "Tickets resolvidos / tempo", """\
SELECT
  CAST(d.year AS varchar) || '-' || LPAD(CAST(d.month AS varchar), 2, '0') AS "Mês",
  COUNT(*) AS "Tickets Resolvidos"
FROM delta.gold.fact_support_tickets t
JOIN delta.gold.dim_date d ON t.date_id = d.date_id
WHERE t.is_solved = true
GROUP BY d.year, d.month
ORDER BY d.year, d.month""", display="line")

    if did is None:
        print("Criando dashboard...", flush=True)
        did = api("POST", "/api/dashboard", {"name": "Dashboard Gold"}, token=token)["id"]

    layout = [
        (kpi_receita,    0, 0,  6, 4),
        (kpi_pedidos,    0, 6,  6, 4),
        (kpi_avaliacao,  0, 12, 6, 4),
        (kpi_prazo,      0, 18, 6, 4),
        (chart_rating,   4, 0,  12, 8),
        (chart_tickets,  4, 12, 12, 8),
    ]
    api("PUT", f"/api/dashboard/{did}/cards", {
        "cards": [
            {
                "id": -(i + 1),
                "card_id": card_id,
                "row": row, "col": col,
                "size_x": size_x, "size_y": size_y,
                "visualization_settings": {},
                "parameter_mappings": [],
            }
            for i, (card_id, row, col, size_x, size_y) in enumerate(layout)
        ]
    }, token=token)

    print(f"Pronto! Dashboard disponível em {MB_URL}/dashboard/{did}", flush=True)


if __name__ == "__main__":
    main()
