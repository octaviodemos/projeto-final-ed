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
        raise RuntimeError(f"{method} {path} → {e.code}: {e.read().decode()}")


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


def make_card(token, db_id, name, sql, display="scalar", viz=None):
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

    dashboards = api("GET", "/api/dashboard?archived=false", token=token)
    for d in dashboards:
        if d["name"] == "Dashboard Gold":
            print(f"Dashboard já existe (id={d['id']}), recriando...", flush=True)
            api("PUT", f"/api/dashboard/{d['id']}", {"archived": True}, token=token)
            break

    db_id = get_or_create_db(token)

    print("Criando cards...", flush=True)

    kpi_receita = make_card(token, db_id, "Receita total",
        "SELECT SUM(price) FROM delta.gold.fact_orders WHERE is_delivered = true")

    kpi_pedidos = make_card(token, db_id, "Pedidos abertos",
        "SELECT COUNT(DISTINCT order_id) FROM delta.gold.fact_orders WHERE is_pending = true")

    kpi_avaliacao = make_card(token, db_id, "Avaliação média",
        "SELECT ROUND(AVG(CAST(review_score AS double)), 2) FROM delta.gold.fact_reviews")

    kpi_prazo = make_card(token, db_id, "Prazo médio de entrega",
        "SELECT ROUND(AVG(CAST(delivery_days AS double)), 2) FROM delta.gold.fact_orders WHERE is_delivered = true",
        viz={"scalar.suffix": "d"})

    chart_rating = make_card(token, db_id, "Classificação x Prazo de Entrega", """\
SELECT
  CAST(d.year AS varchar) || '-' || LPAD(CAST(d.month AS varchar), 2, '0') AS "Mês",
  ROUND(AVG(CAST(r.review_score AS double)), 2) AS "Avaliação Média"
FROM delta.gold.fact_reviews r
JOIN delta.gold.dim_date d ON r.date_id = d.date_id
GROUP BY d.year, d.month
ORDER BY d.year, d.month""", display="line")

    chart_tickets = make_card(token, db_id, "Tickets resolvidos / tempo", """\
SELECT
  CAST(d.year AS varchar) || '-' || LPAD(CAST(d.month AS varchar), 2, '0') AS "Mês",
  COUNT(*) AS "Tickets Resolvidos"
FROM delta.gold.fact_support_tickets t
JOIN delta.gold.dim_date d ON t.date_id = d.date_id
WHERE t.is_solved = true
GROUP BY d.year, d.month
ORDER BY d.year, d.month""", display="line")

    print("Criando dashboard...", flush=True)
    dashboard = api("POST", "/api/dashboard", {"name": "Dashboard Gold"}, token=token)
    did = dashboard["id"]

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
