# Jobs

Documentação dos jobs de processamento da pipeline.

## Visão geral

A pipeline é composta por jobs sequenciais, onde cada camada depende da anterior. O Supabase é a fonte única de dados — os scripts de ingestão extraem as tabelas e fazem upload para a camada Landing no MinIO, de onde o processamento Medallion segue até o Gold.

---

## Jobs de ingestão

Scripts em `scripts/ingest/`, executados antes da pipeline Medallion.

| Job | Script | Descrição | Frequência |
|---|---|---|---|
| Geração de agentes | `generate_agents.py` | Gera 50 agentes sintéticos (5 equipes × 10) e insere na tabela `agents` do Supabase | Uma vez / sob demanda |
| Geração de tickets | `generate_support_tickets_nosql.py` | Gera 12.000 tickets de suporte vinculados aos pedidos Olist e insere nas tabelas `support_tickets` e `support_ticket_messages` do Supabase | Uma vez / sob demanda |
| Download Olist | `landing_ingest.py` | Extrai as 12 tabelas do Supabase como CSV e faz upload para o bucket `landing` no MinIO | Uma vez / sob demanda |

> **Ordem obrigatória:** `generate_agents.py` → `generate_support_tickets_nosql.py` → `landing_ingest.py`. Os tickets referenciam os agentes via `agent_id`.

### Parâmetros — `generate_agents.py`

| Parâmetro | Padrão | Descrição |
|---|---|---|
| `--agents-per-team` | `10` | Agentes gerados por equipe |
| `--seed` | `42` | Semente para reprodutibilidade |
| `--supabase-url` | `SUPABASE_URL` | URL de conexão com o Supabase |

### Parâmetros — `generate_support_tickets_nosql.py`

| Parâmetro | Padrão | Descrição |
|---|---|---|
| `--record-count` | `12000` | Quantidade de tickets a gerar |
| `--start-date` | `2016-01-01` | Data mínima de `opened_at` |
| `--end-date` | `2018-12-31` | Data máxima de `opened_at` |
| `--seed` | `42` | Semente para reprodutibilidade |
| `--supabase-url` | `SUPABASE_URL` | URL de conexão com o Supabase |
| `--orders-csv` | — | CSV local de pedidos (modo dev, sem Supabase) |

---

## Jobs de processamento

Notebooks em `notebooks/`, executados via Papermill na ordem abaixo.

| Job | Notebook | Entrada | Saída | Frequência |
|---|---|---|---|---|
| Landing → Bronze | `01a_landing_to_bronze.ipynb` | `s3a://landing/*.csv` (12 tabelas) | `s3a://bronze/<tabela>` (Delta Lake) | Sob demanda / Airflow |
| Bronze → Silver | `02_bronze_to_silver.ipynb` | `s3a://bronze/*` (12 tabelas) | `s3a://silver/*` (Delta Lake) | Sob demanda / Airflow |
| Silver → Gold | `03_silver_to_gold.ipynb` | `s3a://silver/*` (12 tabelas) | `s3a://gold/*` (Delta Lake — 5 dims + 3 fatos) | Sob demanda / Airflow |

### Parâmetros — `01a_landing_to_bronze.ipynb`

| Parâmetro | Padrão | Descrição |
|---|---|---|
| `landing_bucket` | `landing` | Bucket de origem no MinIO |
| `bronze_bucket` | `bronze` | Bucket de destino no MinIO |

### Parâmetros — `02_bronze_to_silver.ipynb`

| Parâmetro | Padrão | Descrição |
|---|---|---|
| `bronze_bucket` | `bronze` | Bucket de origem no MinIO |
| `silver_bucket` | `silver` | Bucket de destino no MinIO |
| `write_mode` | `overwrite` | Modo de escrita Delta (`overwrite` ou `append`) |

### Parâmetros — `03_silver_to_gold.ipynb`

| Parâmetro | Padrão | Descrição |
|---|---|---|
| `silver_bucket` | `silver` | Bucket de origem no MinIO |
| `gold_bucket` | `gold` | Bucket de destino no MinIO |
| `write_mode` | `overwrite` | Modo de escrita Delta (`overwrite` ou `append`) |

---

## Jobs de infraestrutura

Scripts em `scripts/infra/`:

| Job | Script | Descrição |
|---|---|---|
| Setup de JARs | `setup_jars.py` | Baixa os JARs `hadoop-aws` e `aws-java-sdk-bundle` necessários para o PySpark acessar o MinIO via S3A |

Após a pipeline Gold, as tabelas Delta precisam ser registradas no Hive Metastore para ficarem visíveis no Trino:

```sql
CREATE SCHEMA IF NOT EXISTS delta.gold WITH (location = 's3a://gold/');

CALL delta.system.register_table(schema_name => 'gold', table_name => 'fact_orders',          table_location => 's3a://gold/fact_orders');
CALL delta.system.register_table(schema_name => 'gold', table_name => 'fact_reviews',         table_location => 's3a://gold/fact_reviews');
CALL delta.system.register_table(schema_name => 'gold', table_name => 'fact_support_tickets', table_location => 's3a://gold/fact_support_tickets');
CALL delta.system.register_table(schema_name => 'gold', table_name => 'dim_date',             table_location => 's3a://gold/dim_date');
CALL delta.system.register_table(schema_name => 'gold', table_name => 'dim_customer',         table_location => 's3a://gold/dim_customer');
CALL delta.system.register_table(schema_name => 'gold', table_name => 'dim_seller',           table_location => 's3a://gold/dim_seller');
CALL delta.system.register_table(schema_name => 'gold', table_name => 'dim_product',          table_location => 's3a://gold/dim_product');
CALL delta.system.register_table(schema_name => 'gold', table_name => 'dim_agent',            table_location => 's3a://gold/dim_agent');
```

---

## Execução via Airflow

Os jobs podem ser orquestrados automaticamente pelas DAGs definidas em `airflow/dags/`. Acesse o painel em `http://localhost:8080` e ative as DAGs disponíveis.

### DAG `pipeline_medalhao`

DAG principal em `airflow/dags/pipeline_medalhao.py`. Executa os notebooks de processamento em sequência via `PapermillOperator`:

```
01a_landing_to_bronze → 02_bronze_to_silver → 03_silver_to_gold
```

| Task | Notebook | Descrição |
|---|---|---|
| `01a_landing_to_bronze` | `01a_landing_to_bronze.ipynb` | Landing (CSV) → Bronze (Delta Lake) |
| `02_bronze_to_silver` | `02_bronze_to_silver.ipynb` | Bronze → Silver (limpeza e DQ) |
| `03_silver_to_gold` | `03_silver_to_gold.ipynb` | Silver → Gold (modelagem dimensional) |

**Pré-requisitos:**

1. Infra MinIO/Trino/Metabase rodando: `cd docker && docker compose --env-file ../.env up -d`
2. Dados na camada Landing: `uv run python scripts/ingest/landing_ingest.py`
3. Airflow local (Astro CLI):

```bash
cd airflow
astro dev restart --no-cache
```

Use `--no-cache` após alterar `Dockerfile`, `requirements.txt` ou `packages.txt`. Para um restart simples (só DAGs), basta `astro dev restart`.

4. No Airflow UI (`http://localhost:8080`), ative a DAG `pipeline_medalhao` e dispare manualmente (▶).

> **Nota:** o Trino usa a porta `8081` quando o Airflow está na `8080`. Configure `TRINO_PORT=8081` no `.env`.

**Print da DAG (Graph view com run Succeeded):**

Salve o screenshot em `docs/assets/airflow-pipeline-medalhao.png` após a execução end-to-end.

![Pipeline Medallion — Airflow DAG](assets/airflow-pipeline-medalhao.png)

---

## Tratamento de erros

| Job | Comportamento em caso de falha |
|---|---|
| `generate_agents.py` | Lança `RuntimeError` se `SUPABASE_URL` não estiver configurada ou a conexão falhar |
| `generate_support_tickets_nosql.py` | Lança `RuntimeError` se não encontrar agentes no Supabase (`run generate_agents.py first`) ou se os pares `order_id`/`customer_id` forem inválidos |
| `landing_ingest.py` | Lança `RuntimeError` em caso de falha de conexão com MinIO ou Supabase |
| `01a_landing_to_bronze.ipynb` | Lança `RuntimeError` se o número de tabelas ingeridas com sucesso for menor que 12 |
| `02_bronze_to_silver.ipynb` | Lança `RuntimeError` com a lista de tabelas que falharam no processamento de DQ |
| `03_silver_to_gold.ipynb` | Lança `RuntimeError` com a lista de tabelas Gold que falharam na escrita Delta |