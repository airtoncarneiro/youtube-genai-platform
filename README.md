# YouTube GenAI Platform

Plataforma de dados para acompanhar vídeos públicos do YouTube no Databricks.
O pipeline consulta uma lista controlada de vídeos, atualiza os dados mais
recentes e registra a evolução das métricas de vídeos e canais ao longo do
tempo.

## O que o projeto faz hoje

Para cada vídeo elegível em uma tabela de controle, o Job:

1. busca os dados atuais do vídeo;
2. busca os dados atuais do canal proprietário;
3. busca os comentários de primeiro nível;
4. busca as respostas de cada comentário;
5. atualiza as tabelas com o estado atual;
6. grava snapshots temporais de métricas de vídeo e canal;
7. registra o resultado e agenda a próxima atualização do vídeo.

Uma primeira coleta e uma atualização posterior percorrem o mesmo fluxo. A
diferença está apenas na persistência: registros atuais são atualizados por
`MERGE`, enquanto snapshots nunca sobrescrevem medições anteriores.

> A YouTube Data API oferece replies apenas para comentários de primeiro nível.
> Portanto, não há uma terceira camada de “replies de replies” para coletar.

## Arquitetura atual

```text
control.video_targets
        |
        v
controle de elegibilidade e processamento
        |
        v
YouTube Data API: vídeo -> canal -> comentários -> replies
        |
        +--> raw.api_responses          (payloads originais)
        |
        +--> silver.*                   (estado atual)
        |
        +--> silver.*_snapshots         (histórico de métricas)
```

O catálogo usado pelo projeto é `youtube_lakehouse`.

## Camadas implementadas

| Camada | Tabelas | Finalidade |
|---|---|---|
| `control` | `video_targets` | Lista inicial de vídeos, prioridade, ativação e intervalo de atualização. |
| `control` | `video_processing_state` | Reserva de processamento, tentativas, último sucesso, próximo refresh e erro mais recente por vídeo. |
| `control` | `ingestion_runs` | Auditoria da execução: início, fim, estado, canal ou canais observados e erro global. |
| `raw` | `api_responses` | Respostas JSON originais da API, preservadas para auditoria e reprocessamento. |
| `silver` | `channels`, `videos` | Estado atual do canal e do vídeo, incluindo as contagens públicas mais recentes. |
| `silver` | `comments`, `replies` | Estado atual dos comentários e respostas conhecidos. Novos registros são inseridos e registros existentes são atualizados. |
| `silver` | `channel_snapshots`, `video_snapshots` | Histórico imutável de contagens públicas, com uma observação por entidade e execução. |
| `gold` | — | Reservada para métricas, fatos e modelos analíticos de consumo. |

Exemplo: `silver.videos.view_count` responde quantas visualizações um vídeo
tem agora; `silver.video_snapshots` permite calcular quanto ele cresceu entre
duas coletas.

## Persistência e comportamento incremental

`video_id` é a chave de negócio de vídeos, `channel_id` a de canais e
`comment_id` a de comentários e respostas.

- As tabelas atuais em `silver` usam `MERGE` por chave de negócio e sempre
  exibem o último valor conhecido.
- `video_snapshots` guarda `view_count`, `like_count` e `comment_count`.
- `channel_snapshots` guarda `view_count`, `subscriber_count` e `video_count`.
- O par `entidade + ingestion_id` torna o snapshot idempotente em uma mesma
  execução.
- O valor `refresh_interval_hours` de cada target define quando ele volta a
  ficar elegível após a conclusão; um processamento abandonado em estado
  `PROCESSING` pode ser retomado após duas horas.

Por padrão, os limites de comentários e replies são `0`, que significa
paginação completa. Um valor positivo limita deliberadamente a coleta e é
adequado apenas para desenvolvimento ou recuperação controlada.

### Atualização de schema para ambientes existentes

Em um catálogo que já existe, execute uma única vez no Databricks SQL antes do
próximo Job:

```sql
ALTER TABLE youtube_lakehouse.control.ingestion_runs
ADD COLUMN channel_name STRING;

COMMENT ON COLUMN youtube_lakehouse.control.ingestion_runs.channel_name
IS 'Nome público do canal observado na API; para vários canais, nomes separados por vírgula';
```

Em uma instalação nova, o notebook `00_setup.ipynb` já cria a coluna. O Job
preenche `channel_name` com o nome público retornado pela API; em uma execução
que processar mais de um canal, os nomes são concatenados por vírgula.

## Resiliência da API

Cada chamada à YouTube Data API tem até três tentativas. Falhas de conexão,
timeout, limitação temporária (`429`/`rateLimitExceeded`) e erros de servidor
transitórios (`5xx`) aguardam 1 e depois 2 segundos antes de nova tentativa.
Erros definitivos, como credencial inválida, vídeo inexistente e cota diária
esgotada, não são repetidos.

As falhas são classificadas no código (por exemplo, `TRANSIENT_NETWORK`,
`RATE_LIMITED`, `QUOTA_EXCEEDED` e `COMMENTS_DISABLED`) e a categoria é
preservada na mensagem de erro registrada nas tabelas de controle.

## Estrutura do projeto

```text
.
├── src/
│   ├── notebooks/
│   │   ├── 00_setup.ipynb                  # cria catálogo, schemas e tabelas
│   │   └── 01_youtube_ingestion_test.ipynb # exploração do cliente da API
│   └── youtube_etl_genai/
│       ├── main.py                         # entry point da wheel e leitura do segredo
│       ├── pipeline.py                     # seleção, coleta e orquestração
│       ├── persistence.py                  # schemas, Delta MERGE e estado de controle
│       └── youtube_client.py               # cliente reutilizável da YouTube Data API
├── resources/
│   └── youtube_ingestion.job.yml           # Job serverless do Bundle
├── tests/
├── databricks.yml
├── pyproject.toml
└── README.md
```

O código de produção fica exclusivamente em `src/youtube_etl_genai` e é
empacotado na wheel. Os notebooks são sincronizados pelo Bundle, mas não fazem
parte do pacote Python. O notebook `01_youtube_ingestion_test.ipynb` serve para
exploração; a ingestão operacional deve ser feita pelo Job `youtube_ingestion`.

## Pré-requisitos

- Python 3.12 para desenvolvimento local.
- Poetry.
- Databricks CLI autenticada no workspace alvo.
- Secret Scope com a chave da YouTube Data API:

  ```text
  scope: youtube_api_key
  key:   api-key
  ```

- Permissões para criar e gravar no catálogo `youtube_lakehouse` e seus schemas.

O valor da API key nunca deve ser incluído em código, notebook, configuração
versionada ou comando de terminal.

## Preparar o Lakehouse e cadastrar vídeos

1. Abra e execute `src/notebooks/00_setup.ipynb` no Databricks.

   > A primeira célula do notebook executa `DROP CATALOG ... CASCADE`. Use-a
   > apenas para reinicializar um ambiente de desenvolvimento ou testes. Em um
   > catálogo com dados que devem ser preservados, não execute essa célula.

2. Cadastre os vídeos a acompanhar. Em uma URL comum, o ID é o valor após
   `v=`. Por exemplo, `https://www.youtube.com/watch?v=dNJbFHRuHRk` corresponde
   a `dNJbFHRuHRk`.

   ```sql
   INSERT INTO youtube_lakehouse.control.video_targets (
     video_id,
     is_active,
     priority,
     refresh_interval_hours,
     created_at,
     updated_at
   ) VALUES (
     'dNJbFHRuHRk',
     true,
     100,
     24,
     current_timestamp(),
     current_timestamp()
   );
   ```

   Os timestamps são armazenados em UTC. Para exibir no horário de São Paulo:

   ```sql
   SELECT
     video_id,
     from_utc_timestamp(created_at, 'America/Sao_Paulo') AS created_at_brasil
   FROM youtube_lakehouse.control.video_targets;
   ```

3. Para pausar um vídeo sem apagar o histórico:

   ```sql
   UPDATE youtube_lakehouse.control.video_targets
   SET is_active = false,
       updated_at = current_timestamp()
   WHERE video_id = 'dNJbFHRuHRk';
   ```

## Databricks Asset Bundle e execução

O Bundle cria o Job `youtube_ingestion` com uma única task, `fetch_youtube`.
Ela usa compute serverless, environment version 5 (Python 3.12), instala a
wheel produzida pelo Poetry e executa o entry point `run`.

Em serverless, a wheel lê a chave com `DBUtils.secrets.get` usando os parâmetros
`secret_scope` e `secret_key`. Em um cluster clássico, o entry point também
aceita a variável de ambiente `YOUTUBE_API_KEY` como alternativa de execução.

Parâmetros atuais da task:

| Parâmetro | Padrão | Significado |
|---|---:|---|
| `batch_size` | `20` | Máximo de vídeos elegíveis reservados na execução. |
| `max_comments_per_video` | `20` | Máximo de comentários por vídeo; `0` pagina todos. |
| `max_replies_per_comment` | `5` | Máximo de replies por comentário; `0` pagina todas. |
| `secret_scope` | `youtube_api_key` | Scope que contém a chave da YouTube API. |
| `secret_key` | `api-key` | Nome da chave dentro do scope. |

Comandos principais:

```bash
databricks bundle validate -t dev
databricks bundle deploy -t dev
databricks bundle run -t dev youtube_ingestion
```

`bundle deploy` constrói a wheel, sincroniza os arquivos e cria ou atualiza o
Job. `bundle run` apenas executa o Job já implantado. Pelo plugin Databricks do
VS Code, escolha **Run now** no Job/Workflow `dev-youtube-ingestion`; não use a
opção de executar o notebook atual, pois ela cria uma execução avulsa que não
representa este pipeline.

O Job foi implantado e executado com sucesso no target `dev` durante a validação
inicial desta implementação.

## Consultas de verificação

Após uma execução, confira o estado da fila:

```sql
SELECT
  target.video_id,
  state.status,
  state.last_succeeded_at,
  state.next_refresh_at,
  state.error_message
FROM youtube_lakehouse.control.video_targets AS target
LEFT JOIN youtube_lakehouse.control.video_processing_state AS state
  ON target.video_id = state.video_id;
```

Confira o estado atual e o histórico de um vídeo:

```sql
SELECT video_id, title, view_count, like_count, comment_count, ingested_at
FROM youtube_lakehouse.silver.videos
WHERE video_id = 'dNJbFHRuHRk';

SELECT collected_at, view_count, like_count, comment_count
FROM youtube_lakehouse.silver.video_snapshots
WHERE video_id = 'dNJbFHRuHRk'
ORDER BY collected_at;
```

## Desenvolvimento local

O projeto usa Poetry e um ambiente virtual dentro do repositório:

```bash
poetry config virtualenvs.in-project true
poetry install
```

Use o interpretador `.venv/bin/python` no VS Code e no Jupyter. O pacote segue
o layout `src`, então a validação local usa `PYTHONPATH=src` quando a wheel não
está instalada no ambiente.

```bash
poetry check --strict
poetry run ruff check .
poetry run ruff format --check .
PYTHONPATH=src poetry run python -m pytest
poetry build
```

Valide os notebooks separadamente, pois são arquivos JSON:

```bash
python -m json.tool src/notebooks/00_setup.ipynb >/dev/null
python -m json.tool src/notebooks/01_youtube_ingestion_test.ipynb >/dev/null
```

## Próximas evoluções

- Marcar de forma segura comentários e replies que deixam de aparecer em uma
  varredura completa, sem apagar histórico.
- Adicionar observabilidade operacional, métricas de qualidade e alertas.
- Criar modelos `gold` para crescimento, engajamento e comparação entre vídeos
  e canais.
- Incluir transcrições, NLP, embeddings, Vector Search, RAG e agentes de IA.

## Status

✅ A primeira versão do pipeline está funcional: o Job serverless processa a
tabela de targets, coleta vídeo/canal/comentários/replies, atualiza o estado
atual e grava snapshots temporais. As camadas analíticas `gold` e as capacidades
de GenAI permanecem como evolução planejada.
