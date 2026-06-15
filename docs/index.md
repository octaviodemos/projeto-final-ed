---
hide:
  - navigation
---

# Projeto Final — Engenharia de Dados

Trabalho desenvolvido para a disciplina de **Engenharia de Dados** do curso de **Engenharia de Software** da **UNISATC**.

## Sobre o Projeto

Este projeto tem como objetivo a construção de uma **pipeline de Engenharia de Dados completa**, seguindo a arquitetura **Medallion**, composta pelas camadas:

- 📥 **Landing** — ingestão dos dados brutos, sem transformações
- 🥉 **Bronze** — padronização inicial e adição de metadados de rastreabilidade
- 🥈 **Silver** — limpeza, integração e transformações de negócio
- 🥇 **Gold** — camada analítica otimizada para consumo e visualização

O projeto combina duas fontes de dados complementares: os arquivos CSV do **Brazilian E-Commerce Public Dataset by Olist** (Kaggle) e um conjunto de **support tickets NoSQL** gerados sinteticamente com base nos pedidos reais, simulando dados operacionais de atendimento ao cliente.

Todo o fluxo contempla ingestão, transformação, armazenamento e visualização dos dados, utilizando tecnologias como **PySpark**, **Delta Lake**, **MinIO**, **Airflow** e **Metabase**.

## Tecnologias Utilizadas

| Tecnologia | Função |
|---|---|
| Python 3.11+ | Linguagem principal |
| PySpark + Delta Lake | Processamento e armazenamento em camadas |
| MinIO | Data Lake compatível com S3 |
| Apache Airflow | Orquestração dos pipelines |
| Supabase | Fonte opcional dos dados NoSQL |
| Docker Compose | Orquestração dos serviços locais |
| Metabase | Dashboards e visualização analítica |
| MkDocs Material | Documentação do projeto |

## Navegação

Utilize o menu lateral para explorar a documentação de cada camada da pipeline:

- **Arquitetura** — visão geral do fluxo e dos componentes
- **Landing** — fontes de dados, estrutura no MinIO e scripts de ingestão
- **Bronze** — jobs de processamento, schema e validações de qualidade
- **Silver** — transformações de negócio e integração dos datasets
- **Gold** — camada analítica e modelo dimensional

## Repositório

O código-fonte do projeto está disponível em:

[https://github.com/octaviodemos/projeto-final-ed](https://github.com/octaviodemos/projeto-final-ed)