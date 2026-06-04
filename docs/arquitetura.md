# Arquitetura

Descreva aqui a arquitetura geral do projeto de engenharia de dados.

## Visão geral

Inclua um diagrama da arquitetura utilizando Mermaid ou uma imagem em `assets/`.

```mermaid
flowchart LR
    A[Fontes de Dados] --> B[Landing]
    B --> C[Bronze]
    C --> D[Silver]
    D --> E[Gold]
    E --> F[Dashboard]
```

## Componentes

| Componente | Tecnologia | Descrição |
|------------|------------|-----------|
| Armazenamento | MinIO | Object storage compatível com S3 |
| Processamento | PySpark | Transformações nas camadas medallion |
| Visualização | Metabase | Dashboards e análises |
