# Dados — Brazilian E-Commerce (Olist)

Esta pasta armazena arquivos de dados locais utilizados durante o desenvolvimento da pipeline.
Arquivos `.csv`, `.json` e `.parquet` nesta pasta são **ignorados pelo Git** (ver `.gitignore`).

## Fonte

| Item | Detalhe |
|------|---------|
| **Dataset** | [Brazilian E-Commerce Public Dataset by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) |
| **Licença** | CC BY-NC-SA 4.0 |
| **Período** | 2016 – 2018 |
| **Volume** | ~100 mil pedidos |

---

## Tabelas (9 arquivos CSV)

### 1. `olist_customers_dataset.csv`

Dados de clientes com localização anonimizada.

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| `customer_id` | string | Chave do cliente no pedido (PK) |
| `customer_unique_id` | string | Identificador único do cliente |
| `customer_zip_code_prefix` | string | 5 primeiros dígitos do CEP |
| `customer_city` | string | Cidade do cliente |
| `customer_state` | string | UF do cliente |

---

### 2. `olist_geolocation_dataset.csv`

Coordenadas geográficas associadas a CEPs brasileiros.

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| `geolocation_zip_code_prefix` | string | 5 primeiros dígitos do CEP |
| `geolocation_lat` | float | Latitude |
| `geolocation_lng` | float | Longitude |
| `geolocation_city` | string | Cidade |
| `geolocation_state` | string | UF |

---

### 3. `olist_order_items_dataset.csv`

Itens incluídos em cada pedido.

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| `order_id` | string | ID do pedido (FK → orders) |
| `order_item_id` | int | Sequencial do item dentro do pedido |
| `product_id` | string | ID do produto (FK → products) |
| `seller_id` | string | ID do vendedor (FK → sellers) |
| `shipping_limit_date` | datetime | Data-limite para o vendedor postar |
| `price` | float | Preço do item |
| `freight_value` | float | Valor do frete do item |

---

### 4. `olist_order_payments_dataset.csv`

Informações de pagamento dos pedidos.

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| `order_id` | string | ID do pedido (FK → orders) |
| `payment_sequential` | int | Sequencial do pagamento |
| `payment_type` | string | Tipo (credit_card, boleto, voucher, debit_card) |
| `payment_installments` | int | Número de parcelas |
| `payment_value` | float | Valor do pagamento |

---

### 5. `olist_order_reviews_dataset.csv`

Avaliações feitas pelos clientes após a entrega.

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| `review_id` | string | ID da avaliação (PK) |
| `order_id` | string | ID do pedido (FK → orders) |
| `review_score` | int | Nota de 1 a 5 |
| `review_comment_title` | string | Título do comentário (opcional) |
| `review_comment_message` | string | Corpo do comentário (opcional) |
| `review_creation_date` | datetime | Data de criação da avaliação |
| `review_answer_timestamp` | datetime | Data da resposta ao questionário |

---

### 6. `olist_orders_dataset.csv`

Tabela central de pedidos.

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| `order_id` | string | ID do pedido (PK) |
| `customer_id` | string | ID do cliente (FK → customers) |
| `order_status` | string | Status (delivered, shipped, canceled, …) |
| `order_purchase_timestamp` | datetime | Data/hora da compra |
| `order_approved_at` | datetime | Data/hora da aprovação do pagamento |
| `order_delivered_carrier_date` | datetime | Data de postagem |
| `order_delivered_customer_date` | datetime | Data de entrega ao cliente |
| `order_estimated_delivery_date` | datetime | Data estimada de entrega |

---

### 7. `olist_products_dataset.csv`

Catálogo de produtos vendidos na plataforma.

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| `product_id` | string | ID do produto (PK) |
| `product_category_name` | string | Categoria (em português) |
| `product_name_length` | int | Comprimento do nome |
| `product_description_length` | int | Comprimento da descrição |
| `product_photos_qty` | int | Quantidade de fotos |
| `product_weight_g` | int | Peso em gramas |
| `product_length_cm` | int | Comprimento em cm |
| `product_height_cm` | int | Altura em cm |
| `product_width_cm` | int | Largura em cm |

---

### 8. `olist_sellers_dataset.csv`

Dados de localização dos vendedores.

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| `seller_id` | string | ID do vendedor (PK) |
| `seller_zip_code_prefix` | string | 5 primeiros dígitos do CEP |
| `seller_city` | string | Cidade do vendedor |
| `seller_state` | string | UF do vendedor |

---

### 9. `product_category_name_translation.csv`

Tradução dos nomes de categoria de produto (PT → EN).

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| `product_category_name` | string | Nome da categoria em português |
| `product_category_name_english` | string | Nome da categoria em inglês |

---

## Diagrama de Relacionamentos

```
orders ──┬── order_items ──── products
         │         └──────── sellers
         ├── order_payments
         ├── order_reviews
         └── customers
                └──── geolocation (via zip_code_prefix)

products ──── product_category_name_translation (via product_category_name)
```

## Como baixar os dados

```bash
# Certifique-se de que o MinIO está rodando
docker compose -f docker/docker-compose.yml up -d

# Execute o script de ingestão
python scripts/ingest/download_olist.py
```

> **Pré-requisitos:** configurar `KAGGLE_USERNAME` e `KAGGLE_KEY` no arquivo `.env`.
