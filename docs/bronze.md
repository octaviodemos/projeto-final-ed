# Camada Bronze

Documentação da camada Bronze da pipeline.

## Objetivo

A camada Bronze armazena os dados ingeridos com schema mínimo aplicado, mantendo fidelidade aos dados originais e adicionando metadados de ingestão.

## Transformações

Descreva as transformações aplicadas na transição Landing → Bronze.

## Estrutura no MinIO

```
bronze/
  └── <dominio>/
      └── <tabela>/
          └── particoes por data
```

## Qualidade de dados

Descreva as validações básicas aplicadas nesta camada.
