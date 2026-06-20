# Camada Silver

Documentação da camada silver da pipeline.

## Objetivo

A camada Silver é responsável por aplicar regras de **Data Quality** nas tabelas da camada Bronze, garantindo consistência, padronização de tipos e integridade dos dados antes de chegarem à camada analítica.

Nesta etapa nenhuma lógica de negócio ou agregação é aplicada — o foco é exclusivamente na qualidade e confiabilidade dos dados.

---

## Formato e Tabelas Presentes

- **Formato de entrada:** Delta Lake (bucket `bronze`)
- **Formato de saída:** Delta Lake (bucket `silver`)
- **Total de tabelas processadas:** 12

| Tabela | Origem |
|---|---|
| `olist_customers_dataset` | Kaggle / Olist |
| `olist_geolocation_dataset` | Kaggle / Olist |
| `olist_order_items_dataset` | Kaggle / Olist |
| `olist_order_payments_dataset` | Kaggle / Olist |
| `olist_order_reviews_dataset` | Kaggle / Olist |
| `olist_orders_dataset` | Kaggle / Olist |
| `olist_products_dataset` | Kaggle / Olist |
| `olist_sellers_dataset` | Kaggle / Olist |
| `product_category_name_translation` | Kaggle / Olist |
| `agents` | Supabase |
| `support_tickets` | Supabase |
| `support_ticket_messages` | Supabase |

---

## Regras de Data Quality

### 1. `snake_case` — Padronização de nomes de colunas

Renomeia todas as colunas para snake_case, garantindo consistência de nomenclatura entre tabelas.

```python
def _to_snake_case(name: str) -> str:
    name = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", name)
    name = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", name)
    name = re.sub(r"[\s\-]+", "_", name)
    return name.lower()

def apply_snake_case(df: DataFrame) -> tuple[DataFrame, int]:
    renamed = 0
    for col in df.columns:
        new_name = _to_snake_case(col)
        if new_name != col:
            df = df.withColumnRenamed(col, new_name)
            renamed += 1
    return df, renamed
```

---

### 2. `drop_duplicates` — Remoção de duplicatas

Remove linhas completamente duplicadas em cada tabela.

```python
def apply_dedup(df: DataFrame) -> tuple[DataFrame, int]:
    before = df.count()
    df = df.dropDuplicates()
    return df, before - df.count()
```

---

### 3. `drop_critical_nulls` — Remoção de nulos em colunas-chave

Remove linhas com valor nulo em colunas obrigatórias, definidas individualmente por tabela. Apenas colunas presentes no DataFrame são verificadas.

```python
def apply_critical_null_drop(
    df: DataFrame, critical_cols: list[str]
) -> tuple[DataFrame, int]:
    cols_present = [c for c in critical_cols if c in df.columns]
    if not cols_present:
        return df, 0
    before = df.count()
    df = df.dropna(subset=cols_present)
    return df, before - df.count()
```

Exemplos de colunas críticas por tabela:

| Tabela | Colunas críticas |
|---|---|
| `olist_orders_dataset` | `order_id`, `customer_id`, `order_status`, `order_purchase_timestamp` |
| `olist_order_items_dataset` | `order_id`, `product_id`, `seller_id` |
| `support_tickets` | `ticket_id`, `order_id`, `customer_id`, `agent_id`, `status`, `opened_at` |
| `support_ticket_messages` | `message_id`, `ticket_id`, `sender`, `body` |
| `agents` | `agent_id`, `alias`, `team` |

---

### 4. `fill_optional_nulls` — Preenchimento de nulos opcionais

Preenche nulos em colunas opcionais com valores-padrão de negócio definidos por tabela.

```python
def apply_optional_fill(
    df: DataFrame, fill_map: dict[str, Any]
) -> tuple[DataFrame, int]:
    valid_map = {k: v for k, v in fill_map.items() if k in df.columns}
    if not valid_map:
        return df, 0
    df = df.fillna(valid_map)
    return df, len(valid_map)
```

Valores-padrão utilizados no projeto:

| Tabela | Coluna | Valor padrão |
|---|---|---|
| `olist_order_payments_dataset` | `payment_installments` | `1` |
| `olist_order_payments_dataset` | `payment_value` | `0.0` |
| `olist_order_reviews_dataset` | `review_comment_title` | `""` |
| `olist_order_reviews_dataset` | `review_comment_message` | `""` |
| `olist_products_dataset` | `product_category_name` | `"unknown"` |
| `support_tickets` | `channel` | `"unknown"` |
| `support_tickets` | `priority` | `"normal"` |

---

### 5. `cast_types` — Garantia de tipos corretos

Aplica casts explícitos de tipo por coluna, garantindo que timestamps, doubles, integers e booleans sejam armazenados com o tipo correto no Silver.

```python
def apply_type_casts(
    df: DataFrame, cast_map: dict[str, T.DataType]
) -> tuple[DataFrame, int]:
    cast_count = 0
    for col_name, dtype in cast_map.items():
        if col_name in df.columns:
            df = df.withColumn(col_name, F.col(col_name).cast(dtype))
            cast_count += 1
    return df, cast_count
```

Exemplos de casts aplicados:

| Tabela | Coluna | Tipo |
|---|---|---|
| `olist_orders_dataset` | `order_purchase_timestamp` | `TimestampType` |
| `olist_order_items_dataset` | `price`, `freight_value` | `DoubleType` |
| `olist_order_payments_dataset` | `payment_value` | `DoubleType` |
| `olist_geolocation_dataset` | `geolocation_lat`, `geolocation_lng` | `DoubleType` |
| `support_tickets` | `opened_at`, `closed_at` | `TimestampType` |
| `support_tickets` | `requires_seller_action` | `BooleanType` |
| `support_ticket_messages` | `sent_at` | `TimestampType` |

---

## Metadados Silver

Após as regras de DQ, dois campos de rastreabilidade são adicionados a todas as tabelas:

```python
df = (
    df
    .withColumn("_silver_timestamp", F.current_timestamp())
    .withColumn("_silver_source", F.lit(f"{bronze_bucket}/{table_name}"))
)
```

| Campo | Tipo | Descrição |
|---|---|---|
| `_silver_timestamp` | timestamp | Data e hora do processamento Silver |
| `_silver_source` | string | Caminho da tabela Bronze de origem |

---

## Log de Data Quality

Ao final do processamento, o job gera um log estruturado em JSON com as métricas de DQ de cada tabela, persistido em `/tmp/dq_log_bronze_to_silver_<timestamp>.json` para auditoria.

```json
[
  {
    "table": "olist_orders_dataset",
    "rows_bronze": 99441,
    "rows_silver": 99433,
    "dropped_duplicates": 0,
    "dropped_critical_nulls": 8,
    "optional_cols_filled": 0,
    "cols_cast": 5,
    "cols_renamed_to_snake_case": 0,
    "nulls_remaining_by_col": {},
    "status": "ok"
  },
  {
    "table": "support_tickets",
    "rows_bronze": 12000,
    "rows_silver": 12000,
    "dropped_duplicates": 0,
    "dropped_critical_nulls": 0,
    "optional_cols_filled": 2,
    "cols_cast": 6,
    "cols_renamed_to_snake_case": 0,
    "nulls_remaining_by_col": {},
    "status": "ok"
  }
]
```

---

## Estrutura no MinIO

```text
silver/
├── olist_customers_dataset/
│   ├── _delta_log/
│   └── part-*.parquet
├── olist_geolocation_dataset/
├── olist_order_items_dataset/
├── olist_order_payments_dataset/
├── olist_order_reviews_dataset/
├── olist_orders_dataset/
├── olist_products_dataset/
├── olist_sellers_dataset/
├── product_category_name_translation/
├── agents/
├── support_tickets/
└── support_ticket_messages/
```

---

## Resultado

Ao final da execução, as 12 tabelas ficam disponíveis no bucket `silver` em formato Delta Lake, com dados limpos, tipados corretamente e rastreados por metadados de pipeline. O log de DQ gerado documenta cada transformação aplicada, permitindo auditoria completa do processamento. Os dados ficam prontos para a modelagem dimensional da camada Gold.