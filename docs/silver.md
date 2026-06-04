# Camada Silver

Documentação da camada Silver da pipeline.

## Objetivo

A camada Silver contém dados limpos, padronizados e deduplicados, prontos para modelagem analítica.

## Transformações

Descreva as regras de limpeza, padronização e enriquecimento aplicadas na transição Bronze → Silver.

## Estrutura no MinIO

```
silver/
  └── <dominio>/
      └── <tabela>/
          └── particoes por data
```

## Modelo de dados

Descreva o schema e as relações entre as entidades nesta camada.
