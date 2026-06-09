# Camada Bronze

Documentação da camada Bronze da pipeline.

## Objetivo

A camada Bronze é responsável por armazenar os dados ingeridos da camada Landing com o mínimo de transformação possível, preservando a estrutura original da fonte e adicionando metadados técnicos de ingestão.

Nesta etapa, os dados de reviews provenientes de uma fonte NoSQL em formato JSON são convertidos para Delta Lake e armazenados no bucket Bronze do MinIO, garantindo rastreabilidade, padronização de tipos e persistência otimizada para as próximas camadas da arquitetura.

### Fonte de dados

* Origem: Landing Zone (MinIO)
* Formato de entrada: JSON
* Arquivo esperado: `reviews_nosql.json`
* Destino: Delta Lake na camada Bronze

---

## Jobs de Ingestão

O script responsável pela carga na camada Bronze é `notebooks/01b_nosql_to_bronze.py`.

Este script realiza a leitura do arquivo JSON disponível na camada Landing, aplica as transformações estruturais necessárias e persiste os dados no formato Delta Lake no bucket Bronze do MinIO.

---

## Fluxo de Execução

1. Carrega as variáveis de ambiente a partir do arquivo `.env`.
2. Inicializa a sessão Spark com suporte a Delta Lake.
3. Conecta ao MinIO e localiza o arquivo `reviews_nosql.json` na camada Landing.
4. Aplica o schema explícito para padronização dos tipos.
5. Converte o campo `created_at` de string para timestamp.
6. Adiciona os metadados de ingestão (`_ingestion_timestamp` e `_source_file`).
7. Executa as validações de qualidade pré-escrita.
8. Persiste os dados no formato Delta Lake na camada Bronze.
9. Executa as validações de qualidade pós-escrita.
10. Gera o resumo de metadados da execução.

---

## Transformações

Durante a transição Landing → Bronze são aplicadas apenas transformações estruturais necessárias para padronização e rastreabilidade dos dados:

### Aplicadas

1. **Aplicação de schema explícito**

   * Evita inferência automática de tipos pelo Spark.
   * Garante consistência entre execuções.

2. **Conversão de data/hora**

   * Campo `created_at` convertido de string para `TimestampType`.
   * Formato esperado:
 yyyy-MM-dd'T'HHmmssX

3. **Adição de metadados de ingestão**

   * `_ingestion_timestamp` — Data e hora da carga.
   * `_source_file` — Caminho do arquivo de origem utilizado na ingestão.

4. **Persistência em Delta Lake**

   * Escrita no formato Delta para suporte a versionamento, schema enforcement e otimizações futuras.

### Preservação de dados

O campo:

```python
tags: ArrayType(StringType)
```

é mantido como array de strings, sem explosão ou normalização, preservando a estrutura original do documento NoSQL.

---

## Schema Bronze

| Campo                | Tipo          | Obrigatório |
| -------------------- | ------------- | ----------- |
| review_id            | string        | Sim         |
| order_id             | string        | Sim         |
| customer_id          | string        | Sim         |
| sentiment            | string        | Não         |
| comment_text         | string        | Não         |
| tags                 | array<string> | Não         |
| created_at           | timestamp     | Sim         |
| _ingestion_timestamp | timestamp     | Sim         |
| _source_file         | string        | Sim         |

---

## Estrutura no MinIO

Estrutura lógica da camada Bronze:

```text
bronze/
└── reviews_nosql/
    └── _delta_log/
    └── part-*.parquet
```

Neste pipeline específico:

```text
bronze/
└── reviews_nosql/
```

Os dados são armazenados como tabela Delta Lake.

---

## Qualidade de Dados

Antes e após a escrita do Delta são executadas validações para garantir a consistência dos dados.

### Validações realizadas

#### Existência de dados

Verifica se a fonte retornou ao menos um registro.

Regra:

```text
Quantidade de registros > 0
```

---

#### Presença das colunas obrigatórias

Verifica a existência dos campos:

* review_id
* order_id
* customer_id
* sentiment
* comment_text
* tags
* created_at
* _ingestion_timestamp
* _source_file

---

#### Validação do campo tags

Regras:

```text
tags deve ser ArrayType
tags deve conter apenas strings
```

---

#### Validação de timestamps

Regras:

```text
created_at -> TimestampType
_ingestion_timestamp -> TimestampType
```

---

#### Verificação de nulos em campos obrigatórios

Não são permitidos valores nulos em:

* review_id
* order_id
* customer_id
* created_at
* _source_file

Caso existam registros inválidos, a carga é interrompida.

---

#### Validação pós-escrita

Após persistir os dados em Delta:

1. A tabela é relida.
2. Todas as validações são executadas novamente.
3. Garante-se que os dados persistidos mantiveram o schema esperado.

---

## Metadados de Execução

Ao final da carga é gerado um resumo contendo:

```json
{
  "input_path": "s3a://landing/nosql/reviews_nosql.json",
  "output_path": "s3a://bronze/reviews_nosql",
  "rows_written": 1000,
  "tags_strategy": "preserved_as_array"
}
```

Essas informações auxiliam na auditoria e rastreabilidade da ingestão.

---

## Resultado

Ao final da execução, os dados de reviews NoSQL ficam disponíveis na camada Bronze no formato Delta Lake, prontos para as etapas de limpeza e integração na camada Silver.