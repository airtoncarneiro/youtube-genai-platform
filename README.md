# YouTube GenAI Platform

Plataforma de dados no Databricks para acompanhar vídeos públicos do YouTube,
registrar a evolução de métricas e observar a saúde operacional da coleta. A
visão de evolução do projeto inclui uma camada de LLM e RAG (Retrieval-
Augmented Generation), permitindo fazer perguntas em linguagem natural sobre
os vídeos, canais e métricas armazenados.

## O que é

O projeto combina uma arquitetura Lakehouse, Jobs serverless, tabelas Delta,
Unity Catalog, descoberta automática por canal, snapshots históricos,
observabilidade e dashboards versionados em um Declarative Automation Bundle.

O fluxo atual:

```text
YouTube Data API
      ↓
descoberta de vídeos → control.video_targets
      ↓
ingestão de vídeo, canal, comentários e replies
      ↓
raw + silver + snapshots + telemetria
      ↓
dashboards operacionais
```

A fundação de Engenharia de Dados está funcional. A camada de LLM/RAG ainda é
uma evolução planejada: os dados já formam a base para perguntas em linguagem
natural, mas a busca semântica, a recuperação de contexto e a geração das
respostas ainda não foram implementadas.

## Principais recursos

- ingestão incremental e idempotente de vídeos, canais, comentários e replies;
- descoberta automática de novos vídeos por canal;
- histórico imutável de métricas de vídeos e canais;
- controle de fila, tentativas, estados e resultados por etapa;
- retries para falhas transitórias da API e custo estimado por chamada;
- dashboards de operação, descoberta e saúde da ingestão;
- alertas SQL para resultados parciais;
- testes locais, Poetry e Databricks Asset Bundle.

## Arquitetura resumida

- **control**: targets, fila, estado, auditoria e telemetria;
- **raw**: respostas originais da YouTube Data API;
- **silver**: entidades normalizadas e snapshots;
- **gold**: reservado para futuros modelos analíticos.

## Dashboards e demonstração

- [Apresentação do projeto](docs/apresentacao_projeto.md)

## Documentação

- [README detalhado](docs/README_detalhado.md)

O README detalhado contém pré-requisitos, configuração do Lakehouse,
parâmetros dos Jobs, consultas de verificação, troubleshooting,
desenvolvimento local e roadmap completo.

## Stack

Databricks, Apache Spark, Delta Lake, Unity Catalog, Python, SQL, YouTube Data
API, Poetry, Ruff, pytest, Declarative Automation Bundles e dashboards Lakeview.

## Status

- [x] Ingestão incremental e idempotente de vídeos, canais, comentários e replies
- [x] Descoberta automática de novos vídeos por canal
- [x] Camadas raw/silver, snapshots históricos e telemetria operacional
- [x] Controle de fila, tentativas, retries e custo estimado da API
- [x] Dashboards versionados de operação, descoberta e saúde da ingestão
- [x] Alertas SQL para resultados parciais
- [x] Deploy reproduzível com Databricks Asset Bundle no target **dev**
- [ ] Modelos analíticos na camada **gold**
- [ ] NLP e geração de embeddings
- [ ] Vector Search para busca semântica
- [ ] Pipeline RAG para recuperar contexto relevante
- [ ] Integração com LLM para perguntas e respostas em linguagem natural
- [ ] Agentes especializados e avaliação de qualidade das respostas

Para detalhes técnicos e comandos operacionais, consulte o
[README detalhado](docs/README_detalhado.md).
