# Projeto Final - Engenharia de Dados SATC

<!--[![Docs](https://img.shields.io/badge/docs-mkdocs-blue)](https://octaviodemos.github.io/projeto-final-ed/) -->
[![License](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Python](https://img.shields.io/badge/Python-3.11+-3776AB?logo=python\&logoColor=white)](https://www.python.org/)
[![PySpark](https://img.shields.io/badge/PySpark-3.x-E25A1C?logo=apachespark\&logoColor=white)](https://spark.apache.org/docs/latest/api/python/)
[![Delta Lake](https://img.shields.io/badge/Delta%20Lake-Lakehouse-00ADD8)](https://delta.io/)
[![MinIO](https://img.shields.io/badge/MinIO-S3%20Storage-C72E49?logo=minio\&logoColor=white)](https://min.io/)
[![Docker Compose](https://img.shields.io/badge/Docker%20Compose-Orchestration-2496ED?logo=docker\&logoColor=white)](https://docs.docker.com/compose/)
[![Metabase](https://img.shields.io/badge/Metabase-BI%20Dashboard-509EE3?logo=metabase\&logoColor=white)](https://www.metabase.com/)
[![MkDocs](https://img.shields.io/badge/MkDocs-Material-526CFE?logo=materialformkdocs\&logoColor=white)](https://www.mkdocs.org/)

Repositório do projeto final da disciplina de Engenharia de Dados do curso de Engenharia de Software da UNISATC.


## Desenho de Arquitetura

Coloque uma imagem do seu projeto, como no exemplo abaixo:

![image](https://github.com/jlsilva01/projeto-ed-satc/assets/484662/541de6ab-03fa-49b3-a29f-dec8857360c1)

## Pré-requisitos e ferramentas utilizadas

- **Linguagem:** Python 3.11+
- **Armazenamento:** MinIO (compatível com S3)
- **Processamento:** PySpark + Delta Lake
- **Orquestração local:** Docker Compose
- **Visualização:** Metabase
- **Documentação:** MkDocs + mkdocs-material

```
Dar exemplos
```

## Instalação

### 1. Clonar o repositório

```bash
git clone https://github.com/octaviodemos/projeto-final-ed.git
cd projeto-final-ed
```

### 2. Configurar ambiente

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e .
cp .env.example .env
```

### 3. Subir infraestrutura local

```bash
docker compose -f docker/docker-compose.yml up -d
```

Acesse o MinIO em `http://localhost:9000` e o Metabase em `http://localhost:3000`.

## Documentação (MkDocs)

Toda a documentação está em `docs/`:

```bash
mkdocs build
mkdocs serve
```

Acesse o site em [Documentação Mkdocs](https://octaviodemos.github.io/projeto-final-ed/).

Para publicar o site estático:

```bash
mkdocs gh-deploy
```

## Colaboração

1. Abra uma **issue** para discutir sua feature ou bug.
2. Crie um **branch**:

   ```bash
   git checkout -b feature/nome-da-sua-feature
   ```
3. Faça suas alterações e **commit** seguindo o [Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0/).
4. Envie um **pull request** para `main`.
5. Aguarde revisão e merge.

## Autores

* **Ana Laura Vicenzi Dordete** - *Engenharia de Dados, Data Quality e Transformações da Camada Silver* - [https://github.com/anaavicenzi](https://github.com/anaavicenzi)

* **Gabriel Ribeiro Fernandes** - *Orquestração de Pipelines e Automação do Fluxo de Dados* - [https://github.com/gabrielribbz](https://github.com/gabrielribbz)

* **Ismael Damasceno Tristão** - *Modelagem Dimensional (Kimball) e Camada Gold* - [https://github.com/IsmaelDamasceno](https://github.com/IsmaelDamasceno)

* **João Vitor de Oliveira Lima** - *Conversão Landing → Bronze e Estruturação do Data Lake* - [https://github.com/JoaoVitorOL](https://github.com/JoaoVitorOL)

* **Luiz Fillipy Vefago Binatti** - *Business Intelligence, Metabase e Dashboards Analíticos* - [https://github.com/luizzzxq](https://github.com/luizzzxq)

* **Octávio da Silva Demos** - *Coordenação do Projeto, Gestão das Issues e Integração da Solução* - [https://github.com/octaviodemos](https://github.com/octaviodemos)

* **Vinícius Pedroso Milanez** - *Documentação, MkDocs e Publicação no GitHub Pages* - [https://github.com/viniciusmilanez](https://github.com/viniciusmilanez)



## Licença

Este projeto está sob a licença MIT - veja o arquivo [LICENSE](LICENSE) para detalhes.
[![License](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

## Referências

- [projeto-ed-satc](https://github.com/jlsilva01/projeto-ed-satc) — repositório modelo do professor
- [Canal DataWay BR no YouTube](https://www.youtube.com/@DataWayBR)
- Material das aulas de Engenharia de Dados - Prof. Jorge Luiz da Silva - UNISATC
