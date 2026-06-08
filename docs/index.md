---
hide:
  - navigation
---

# Projeto Final — Engenharia de Dados

Trabalho desenvolvido para a disciplina de **Engenharia de Dados** do curso de **Engenharia de Software** da **UNISATC**.

## Sobre o Projeto

Este projeto tem como objetivo a construção de uma **pipeline de Engenharia de Dados completa**, seguindo a arquitetura **Medallion**, composta pelas camadas:

- 📥 **Landing** — ingestão dos dados brutos
- 🥉 **Bronze** — padronização inicial
- 🥈 **Silver** — limpeza e integração
- 🥇 **Gold** — camada analítica para consumo

Todo o fluxo contempla ingestão, transformação, armazenamento e visualização dos dados, utilizando tecnologias como **PySpark**, **MinIO**, **Airflow** e **Metabase**.

## Principais comandos do MkDocs

* `mkdocs new [dir-name]` - Cria um novo projeto.
* `mkdocs serve` - Inicia o preview das paginas *.md da pasta /docs.
* `mkdocs build` - Cria a estrutura de paginas web no padrao html, css, js.
* `mkdocs gh-deploy` - Publica as paginas criadas pelo 'mkdocs build' na estrutura do github pages.

## Documentacao para referencia e estudo

[https://squidfunk.github.io/mkdocs-material/](https://squidfunk.github.io/mkdocs-material/)
