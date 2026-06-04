# Camada Gold

Documentação da camada Gold da pipeline.

## Objetivo

A camada Gold contém dados agregados e modelados para consumo analítico e dashboards.

## Transformações

Descreva as agregações, métricas e dimensões criadas na transição Silver → Gold.

## Estrutura no MinIO

```
gold/
  └── <dominio>/
      └── <tabela_ou_view>/
          └── dados prontos para consumo
```

## Métricas de negócio

Descreva as KPIs e indicadores disponíveis nesta camada.
