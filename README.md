# Projeto Final - Engenharia de Dados SATC

[![Docs](https://img.shields.io/badge/docs-mkdocs-blue)](https://octaviodemos.github.io/projeto-final-ed/)
[![License](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Python](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)

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

Acesse o site em `http://127.0.0.1:8000`.

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

Mencione todos aqueles que ajudaram a levantar o projeto desde o seu início

* **Aluno 1** - *Trabalho Inicial* - [(https://github.com/linkParaPerfil)](https://github.com/linkParaPerfil)
* **Aluno 2** - *Documentação* - [https://github.com/linkParaPerfil](https://github.com/linkParaPerfil)

## Licença

Este projeto está sob a licença MIT - veja o arquivo [LICENSE](LICENSE) para detalhes.
[![License](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

## Referências

- [projeto-ed-satc](https://github.com/jlsilva01/projeto-ed-satc) — repositório modelo do professor
- [Canal DataWay BR no YouTube](https://www.youtube.com/@DataWayBR)
- Material das aulas de Engenharia de Dados - Prof. Jorge Luiz da Silva - UNISATC
