# YouTube GenAI Platform

Projeto de estudo e desenvolvimento prático para evolução profissional de Engenharia de Dados para AI Engineering e GenAI Engineering, utilizando o ecossistema Databricks.

O objetivo é construir uma plataforma moderna de dados e IA capaz de coletar, processar, analisar e gerar insights a partir de dados de canais do YouTube.

---

# Objetivos

Este projeto foi criado para servir como laboratório prático de aprendizado e experimentação em:

- Data Engineering
- Lakehouse Architecture
- Delta Lake
- Databricks
- Python
- APIs
- NLP
- Embeddings
- Vector Search
- RAG (Retrieval-Augmented Generation)
- AI Agents
- Observabilidade
- MLOps / LLMOps

Ao final, a plataforma deverá ser capaz de responder perguntas inteligentes sobre conteúdos e comentários de canais do YouTube, além de gerar insights utilizando Inteligência Artificial Generativa.

---

# Caso de Uso

A plataforma realiza a ingestão de informações de canais do YouTube, incluindo:

- Canais
- Vídeos
- Comentários
- Descrições
- Transcrições

Esses dados serão processados em uma arquitetura Lakehouse e posteriormente enriquecidos para aplicações de IA Generativa.

Exemplos de perguntas que a plataforma deverá responder:

- Quais vídeos geraram mais engajamento?
- Quais foram os vídeos mais criticados?
- Quais temas aparecem com maior frequência nos comentários?
- O que a audiência pensa sobre Databricks?
- Quais reclamações são mais recorrentes em determinado canal?
- Quais tendências estão surgindo na comunidade?

---

# Arquitetura Evolutiva

```text
YouTube API
     │
     ▼
 Bronze
     │
     ▼
 Silver
     │
     ▼
  Gold
     │
     ▼
Transcrições
     │
     ▼
Embeddings
     │
     ▼
Vector Search
     │
     ▼
    RAG
     │
     ▼
 AI Agents
```

---

# Roadmap

## Fase 1 — Engenharia de Dados

Construção da fundação Lakehouse.

### Entregáveis

- Ingestão de canais
- Ingestão de vídeos
- Ingestão de comentários
- Processamento incremental
- Delta Lake
- Medallion Architecture
- Databricks Workflows
- Monitoramento básico

---

## Fase 2 — Dados Não Estruturados

Preparação dos dados para aplicações GenAI.

### Entregáveis

- Captura de descrições
- Captura de comentários
- Captura de transcrições
- Limpeza e normalização de textos
- Enriquecimento dos dados

---

## Fase 3 — GenAI

Implementação da camada de busca semântica.

### Entregáveis

- Geração de embeddings
- Indexação vetorial
- Busca semântica
- Agrupamento de temas
- Análise de sentimentos

---

## Fase 4 — RAG

Construção de consultas inteligentes sobre os dados coletados.

### Entregáveis

- Pipeline RAG
- Recuperação contextual
- Chat com os dados do YouTube
- Avaliação de respostas

---

## Fase 5 — AI Agents

Construção de agentes especializados para geração de insights.

### Entregáveis

- Agente de crescimento
- Agente de engajamento
- Agente comparador de canais
- Agente gerador de insights
- Agente identificador de tendências

---

# Tecnologias

## Data Engineering

- Databricks
- Apache Spark
- Delta Lake
- Unity Catalog
- Databricks Workflows
- Python
- SQL
- Git

## GenAI

- OpenAI
- Embeddings
- Vector Search
- LangChain
- LangGraph

## Observabilidade

- MLflow
- Monitoramento de pipelines
- Observabilidade de aplicações GenAI

---

# Estrutura do Projeto

```text
youtube-genai-platform
├── docs
├── notebooks
├── src
├── tests
├── infrastructure
├── bronze
├── silver
├── gold
├── embeddings
├── vector-search
├── rag
├── agents
├── mlflow
└── observability
```

---

# Objetivo Profissional

Este projeto tem como finalidade desenvolver experiência prática em tecnologias modernas de dados e IA, com foco em posições como:

- AI Engineer
- GenAI Engineer
- LLM Engineer
- AI Platform Engineer
- LLMOps Engineer
- Data Engineer Sênior com foco em IA

---

# Status

🚧 Em desenvolvimento

Atualmente o projeto encontra-se na fase de construção da fundação de Engenharia de Dados, incluindo modelagem, ingestão incremental e arquitetura Lakehouse no Databricks.