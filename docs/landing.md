# Camada Landing

Documentação da camada Landing da pipeline.

## Objetivo

A camada **Landing** é o ponto de entrada dos dados no pipeline. Sua principal função é receber e armazenar os dados brutos provenientes das fontes externas, preservando integralmente seu conteúdo e estrutura original.

Nesta etapa não são realizadas transformações, limpezas ou enriquecimentos dos dados. O objetivo é garantir a rastreabilidade das informações recebidas, permitindo auditoria, reprocessamento e recuperação dos dados sempre que necessário.

No contexto deste projeto, a camada Landing armazena dois conjuntos de dados distintos:

- Os arquivos CSV do dataset **Brazilian E-Commerce Public Dataset by Olist**, obtidos a partir da plataforma Kaggle.
- O arquivo JSON de **support tickets NoSQL** gerado sinteticamente a partir dos pedidos Olist, ou exportado diretamente do Supabase.

Ambos servem como base para as etapas posteriores de processamento nas camadas Bronze, Silver e Gold.

## Fontes de Dados

O projeto utiliza duas fontes de dados na camada Landing.

### Fonte 1 — Dataset Olist (CSV)

O conjunto **Brazilian E-Commerce Public Dataset by Olist**, disponibilizado na plataforma Kaggle, contém informações reais de um marketplace brasileiro, abrangendo aproximadamente 100 mil pedidos realizados entre 2016 e 2018. Os dados estão organizados em múltiplos arquivos CSV relacionados entre si, contendo informações sobre:

- Clientes
- Pedidos
- Produtos
- Vendedores
- Pagamentos
- Avaliações
- Geolocalização

#### Dataset

- **Nome:** Brazilian E-Commerce Public Dataset by Olist
- **Fonte:** Kaggle
- **Formato:** CSV
- **Período:** 2016–2018

**Link do dataset:**

<https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce>

#### Arquivos utilizados

| Arquivo | Descrição |
|---|---|
| `olist_orders_dataset.csv` | Informações dos pedidos |
| `olist_customers_dataset.csv` | Dados dos clientes |
| `olist_products_dataset.csv` | Informações dos produtos |
| `olist_sellers_dataset.csv` | Dados dos vendedores |
| `olist_order_items_dataset.csv` | Itens dos pedidos |
| `olist_order_payments_dataset.csv` | Dados dos pagamentos |
| `olist_order_reviews_dataset.csv` | Avaliações dos clientes |
| `olist_geolocation_dataset.csv` | Dados geográficos dos CEPs |
| `product_category_name_translation.csv` | Tradução das categorias de produto |

---

### Fonte 2 — Support Tickets NoSQL (JSON)

Conjunto complementar de dados sintéticos que simula tickets de suporte ao cliente, gerados com base nos pedidos reais do dataset Olist. Cada ticket é relacionado a um `order_id` e `customer_id` existentes, adicionando informações operacionais não presentes nas tabelas relacionais.

- **Nome:** Support Tickets NoSQL
- **Formato:** JSON
- **Período:** 2016–2018
- **Arquivo:** `reviews_nosql.json`

> O nome do arquivo e da tabela permanece `reviews_nosql` para manter compatibilidade com o restante da pipeline.

#### Campos principais

| Campo | Tipo | Descrição |
|---|---|---|
| `ticket_id` | string | Identificador único do ticket (PK) |
| `order_id` | string | ID do pedido relacionado (FK → orders) |
| `customer_id` | string | ID do cliente relacionado (FK → customers) |
| `channel` | string | Canal de atendimento (chat, email, whatsapp, phone) |
| `issue_type` | string | Tipo do problema relatado |
| `priority` | string | Prioridade do atendimento (high, medium, low) |
| `status` | string | Status atual do ticket |
| `opened_at` | timestamp | Data e hora de abertura |
| `first_response_minutes` | int | Tempo até a primeira resposta (em minutos) |
| `sla_target_hours` | int | Meta de SLA conforme a prioridade |
| `agent` | object | Dados do agente responsável |
| `closed_at` | timestamp | Data e hora de encerramento (quando aplicável) |
| `resolution` | object | Resultado e compensação do atendimento |
| `messages` | array | Histórico de mensagens do ticket |

---

## Estrutura no MinIO

```
landing/
├── olist/
│   └── 2026-06-08/
│       ├── olist_customers_dataset.csv
│       ├── olist_geolocation_dataset.csv
│       ├── olist_order_items_dataset.csv
│       ├── olist_order_payments_dataset.csv
│       ├── olist_order_reviews_dataset.csv
│       ├── olist_orders_dataset.csv
│       ├── olist_products_dataset.csv
│       ├── olist_sellers_dataset.csv
│       └── product_category_name_translation.csv
└── nosql/
    └── 2026-06-08/
        └── reviews_nosql.json
```

---

## Jobs de Ingestão

A camada Landing é alimentada por três scripts distintos, cada um responsável por uma etapa da carga inicial.

### 1. Download do Dataset Olist

Script responsável por baixar os arquivos CSV do Kaggle e armazená-los no bucket Landing sem nenhuma transformação.

**Fluxo de execução:**

1. Carrega as variáveis de ambiente a partir do arquivo `.env`.
2. Realiza a autenticação na API do Kaggle.
3. Baixa o dataset `olistbr/brazilian-ecommerce`.
4. Extrai os arquivos CSV do pacote compactado.
5. Valida a presença dos arquivos esperados.
6. Conecta ao MinIO utilizando a API compatível com S3.
7. Cria o bucket da camada Landing caso ele não exista.
8. Realiza o upload dos arquivos CSV.
9. Verifica se todos os arquivos foram armazenados corretamente.

**Exemplo de execução:**

```bash
python scripts/ingest/landing_ingest.py
```

Exemplo de saída:

```text
INFO Baixando dataset 'olistbr/brazilian-ecommerce'
INFO Extraindo arquivos
INFO Upload: olist_orders_dataset.csv
INFO Upload: olist_customers_dataset.csv
INFO Upload: olist_products_dataset.csv
INFO Todos os arquivos enviados com sucesso
```

---

### 2. Geração dos Support Tickets NoSQL

Script `scripts/ingest/generate_support_tickets_nosql.py` responsável por gerar sinteticamente os tickets de suporte com base nos pedidos do dataset Olist e enviá-los ao bucket Landing.

**Fluxo de execução:**

1. Carrega as variáveis de ambiente do arquivo `.env`.
2. Lê os pedidos do Olist (CSV local ou via MinIO).
3. Gera os documentos de ticket com dados sintéticos reproduzíveis (seed fixo).
4. Valida unicidade de `ticket_id`, integridade dos pares `order_id`/`customer_id` e distribuição temporal.
5. Serializa os registros como JSON.
6. Faz upload do arquivo para o bucket Landing no prefixo `nosql/`.

**Parâmetros principais:**

| Parâmetro | Padrão | Descrição |
|---|---|---|
| `--record-count` | 12000 | Quantidade de tickets a gerar |
| `--start-date` | 2016-01-01 | Data mínima de `opened_at` |
| `--end-date` | 2018-12-31 | Data máxima de `opened_at` |
| `--seed` | 42 | Semente para reprodutibilidade |
| `--skip-upload` | False | Gera o arquivo localmente sem enviar ao MinIO |
| `--orders-csv` | — | Caminho local para o CSV de pedidos (modo dev) |

**Exemplo de execução:**

```bash
python scripts/ingest/generate_support_tickets_nosql.py \
    --record-count 12000 \
    --start-date 2016-01-01 \
    --end-date 2018-12-31 \
    --seed 42
```

---

### 3. Upload de Reviews NoSQL do Supabase

Script `scripts/ingest/upload_reviews_nosql.py` responsável por ler a tabela `reviews_nosql` do Supabase e enviá-la como JSON para o bucket Landing, substituindo a geração sintética quando os dados já estão disponíveis em banco.

**Fluxo de execução:**

1. Carrega as variáveis de ambiente do arquivo `.env`.
2. Conecta ao Supabase via `psycopg`.
3. Lê todos os registros da tabela configurada.
4. Serializa os dados como JSON.
5. Garante que o bucket Landing existe no MinIO.
6. Faz upload do arquivo para o caminho configurado.

**Parâmetros principais:**

| Parâmetro | Padrão | Descrição |
|---|---|---|
| `--supabase-url` | `SUPABASE_URL` | URL de conexão com o Supabase |
| `--schema` | public | Schema do Postgres |
| `--table` | reviews_nosql | Tabela a exportar |
| `--object-key` | nosql/reviews_nosql.json | Destino no bucket Landing |

**Exemplo de execução:**

```bash
python scripts/ingest/upload_reviews_nosql.py \
    --supabase-url postgresql://... \
    --table reviews_nosql \
    --object-key nosql/reviews_nosql.json
```

---

## Tabelas — Dataset Olist (9 arquivos CSV)

### 1. `olist_customers_dataset.csv`

Dados de clientes com localização anonimizada.

| Coluna | Tipo | Descrição |
|---|---|---|
| `customer_id` | string | Chave do cliente no pedido (PK) |
| `customer_unique_id` | string | Identificador único do cliente |
| `customer_zip_code_prefix` | string | 5 primeiros dígitos do CEP |
| `customer_city` | string | Cidade do cliente |
| `customer_state` | string | UF do cliente |

---

### 2. `olist_geolocation_dataset.csv`

Coordenadas geográficas associadas a CEPs brasileiros.

| Coluna | Tipo | Descrição |
|---|---|---|
| `geolocation_zip_code_prefix` | string | 5 primeiros dígitos do CEP |
| `geolocation_lat` | float | Latitude |
| `geolocation_lng` | float | Longitude |
| `geolocation_city` | string | Cidade |
| `geolocation_state` | string | UF |

---

### 3. `olist_order_items_dataset.csv`

Itens incluídos em cada pedido.

| Coluna | Tipo | Descrição |
|---|---|---|
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
|---|---|---|
| `order_id` | string | ID do pedido (FK → orders) |
| `payment_sequential` | int | Sequencial do pagamento |
| `payment_type` | string | Tipo (credit_card, boleto, voucher, debit_card) |
| `payment_installments` | int | Número de parcelas |
| `payment_value` | float | Valor do pagamento |

---

### 5. `olist_order_reviews_dataset.csv`

Avaliações feitas pelos clientes após a entrega.

| Coluna | Tipo | Descrição |
|---|---|---|
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
|---|---|---|
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
|---|---|---|
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
|---|---|---|
| `seller_id` | string | ID do vendedor (PK) |
| `seller_zip_code_prefix` | string | 5 primeiros dígitos do CEP |
| `seller_city` | string | Cidade do vendedor |
| `seller_state` | string | UF do vendedor |

---

### 9. `product_category_name_translation.csv`

Tradução dos nomes de categoria de produto (PT → EN).

| Coluna | Tipo | Descrição |
|---|---|---|
| `product_category_name` | string | Nome da categoria em português |
| `product_category_name_english` | string | Nome da categoria em inglês |

---

## Estrutura da Camada Landing

Representação visual do fluxo de ingestão dos dados brutos no Data Lake.

![Landing](landing.PNG)

---

## Implementação no Projeto

A ingestão da camada Landing foi implementada para automatizar o processo de obtenção e armazenamento dos dados brutos. A seguir são apresentados os principais trechos de código utilizados.

### Download do Dataset

O download é realizado através da API oficial do Kaggle.

```python
api.dataset_download_files(
    DATASET_SLUG,
    path=str(dest_dir),
    unzip=False
)
```

---

### Extração dos Arquivos

Após o download, o arquivo compactado é extraído automaticamente.

```python
with zipfile.ZipFile(zf) as z:
    z.extractall(dest_dir)
```

---

### Conexão com o MinIO

A comunicação com o Data Lake é realizada utilizando a biblioteca `boto3`, compatível com a API S3.

```python
return boto3.client(
    "s3",
    endpoint_url=endpoint,
    aws_access_key_id=access_key,
    aws_secret_access_key=secret_key,
    region_name="us-east-1",
)
```

---

### Criação do Bucket

Antes do envio dos arquivos, o sistema verifica se o bucket da camada Landing já existe.

```python
try:
    s3_client.head_bucket(Bucket=bucket)
except ClientError:
    s3_client.create_bucket(Bucket=bucket)
```

---

### Upload dos Arquivos CSV

Os arquivos CSV são enviados individualmente para o MinIO sem qualquer transformação.

```python
s3.upload_file(
    Filename=str(csv_path),
    Bucket=bucket,
    Key=object_key,
)
```

---

### Upload do JSON NoSQL

O arquivo JSON de support tickets é enviado diretamente via `put_object`.

```python
s3_client.put_object(
    Bucket=bucket_name,
    Key=object_key,
    Body=payload,
    ContentType="application/json; charset=utf-8",
)
```

---

### Validação da Carga

Ao final da execução, é realizada uma verificação para garantir que todos os arquivos esperados foram armazenados corretamente.

```python
response = s3.list_objects_v2(Bucket=bucket)
objects = [obj["Key"] for obj in response.get("Contents", [])]
```

---

## Tratamento de Erros

Os scripts realizam validações para:

* Ausência de credenciais do Kaggle ou do Supabase.
* Falha de conexão com o MinIO.
* Erros durante o upload dos arquivos.
* Ausência de arquivos esperados após a extração.
* Tabela Supabase vazia (zero registros retornados).
* Pares `order_id`/`customer_id` inválidos nos tickets gerados.

Em caso de erro crítico, a execução é interrompida para evitar cargas incompletas.

---

## Resultado

Ao final da execução, todos os arquivos ficam disponíveis na camada Landing:

* Os CSVs do dataset Olist no prefixo `olist/`.
* O JSON de support tickets no prefixo `nosql/`.

Esses dados servem como fonte bruta para os processos subsequentes de transformação e modelagem nas camadas Bronze, Silver e Gold.