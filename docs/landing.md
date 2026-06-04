# Camada Landing

Documentação da camada Landing da pipeline.

## Objetivo

A camada Landing recebe os dados brutos das fontes externas, sem transformações, preservando o formato original.

## Fontes de dados

Descreva aqui as fontes utilizadas (APIs, Kaggle, arquivos locais, etc.).

## Estrutura no MinIO

```
landing/
  └── <fonte>/
      └── <data>/
          └── arquivos brutos
```

## Jobs de ingestão

Descreva os scripts em `scripts/ingest/` responsáveis pela carga na camada Landing.
