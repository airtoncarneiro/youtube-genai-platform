# YouTube GenAI Platform

Plataforma de dados para acompanhar vídeos públicos do YouTube no Databricks.
O pipeline consulta uma lista controlada de vídeos, atualiza os dados mais
recentes e registra a evolução das métricas de vídeos e canais ao longo do
tempo.

## Visão geral

O projeto é um **Declarative Automation Bundle (DAB)** do Databricks que
implementa uma arquitetura lakehouse para ingestão, acompanhamento operacional
e análise de métricas públicas do YouTube. Ele combina um Job serverless
orquestrado, tabelas Delta no Unity Catalog, rastreabilidade de execuções,
alertas SQL e dashboards versionados no próprio Bundle.

No target `dev`, o catálogo é `youtube_lakehouse`; outros targets podem
substituir esse valor pela variável `catalog` do Bundle.

## Índice

- [O que o projeto faz hoje](#o-que-o-projeto-faz-hoje)
- [Arquitetura do Workflow](#arquitetura-do-workflow)
- [Camadas implementadas](#camadas-implementadas)
- [Persistência e comportamento incremental](#persistência-e-comportamento-incremental)
- [Resiliência da API](#resiliência-da-api)
- [Observabilidade operacional](#observabilidade-operacional)
- [Estrutura do projeto](#estrutura-do-projeto)
- [Pré-requisitos](#pré-requisitos)
- [Preparar o Lakehouse e cadastrar vídeos](#preparar-o-lakehouse-e-cadastrar-vídeos)
- [Databricks Asset Bundle e execução](#databricks-asset-bundle-e-execução)
- [Dashboards e monitoramento](#dashboards-e-monitoramento)
- [Consultas de verificação](#consultas-de-verificação)
- [Troubleshooting](#troubleshooting)
- [Desenvolvimento local](#desenvolvimento-local)
- [Próximas evoluções](#próximas-evoluções)

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

## Arquitetura do Workflow

```text
control.video_targets
        |
        v
claim_targets
        |
        v
fetch_videos ──┬──> fetch_channels
               └──> fetch_comments ──> fetch_replies
                         \                 /
                          \               /
                           --> finalize_ingestion

Cada fetch grava seus payloads em raw.api_responses e seus dados em silver.
control.ingestion_step_outcomes é o handoff durável entre as tasks.
```

## Camadas implementadas

| Camada | Tabelas | Finalidade |
|---|---|---|
| `control` | `video_targets` | Lista inicial de vídeos, prioridade, ativação e intervalo de atualização. |
| `control` | `video_processing_state` | Reserva de processamento, tentativas, último sucesso, próximo refresh e erro mais recente por vídeo. |
| `control` | `ingestion_runs` | Auditoria da execução: início, fim, estado, canal ou canais observados e erro global. |
| `control` | `ingestion_step_outcomes` | Resultado de cada fetch por vídeo e execução; permite diagnosticar falhas e finalizar o target somente quando todas as etapas terminarem. |
| `control` | `ingestion_comments` | Handoff temporário dos comentários retornados na execução, usado exclusivamente pela task de replies. |
| `control` | `task_execution_logs` | Resumo idempotente de cada tentativa de task: duração, estado, contagens por vídeo, registros retornados, custo estimado da API e erro técnico. |
| `raw` | `api_responses` | Respostas JSON originais da API, preservadas para auditoria e reprocessamento. |
| `silver` | `channels`, `videos` | Estado atual do canal e do vídeo, incluindo as contagens públicas mais recentes. `videos` referencia `channels` por `channel_id`. |
| `silver` | `video_tags` | Bridge normalizada de tags: uma associação vídeo-tag por linha. |
| `silver` | `comments`, `replies` | Estado atual dos comentários e respostas conhecidos. Novos registros são inseridos e registros existentes são atualizados. |
| `silver` | `channel_snapshots`, `video_snapshots` | Histórico imutável de contagens públicas, com uma observação por entidade e execução. |
| `gold` | — | Reservada para métricas, fatos e modelos analíticos de consumo. |

Exemplo: `silver.videos.view_count` responde quantas visualizações um vídeo
tem agora; `silver.video_snapshots` permite calcular quanto ele cresceu entre
duas coletas.

Todas essas tabelas Delta têm Change Data Feed habilitado. As chaves primárias
e estrangeiras declaradas no Unity Catalog documentam relações de negócio e
são `NOT ENFORCED`; a integridade operacional é preservada pelo pipeline.

## Persistência e comportamento incremental

`video_id` é a chave de negócio de vídeos, `channel_id` a de canais e
`comment_id` a de comentários e respostas.

- As tabelas atuais em `silver` usam `MERGE` por chave de negócio e sempre
  exibem o último valor conhecido.
- `video_snapshots` guarda `view_count`, `like_count` e `comment_count`.
- `channel_snapshots` guarda `view_count`, `subscriber_count` e `video_count`.
- `video_snapshots` e `channel_snapshots` são particionadas por `collected_date`.
- `videos.published_at`, `videos.duration` e `videos.category_id` usam,
  respectivamente, `TIMESTAMP`, `INTERVAL DAY TO SECOND` e `INT`.
- `channel_title` é obtido de `channels` nas views de apresentação, evitando a
  duplicação na tabela de vídeos.
- O par `entidade + ingestion_id` torna o snapshot idempotente em uma mesma
  execução.
- O valor `refresh_interval_hours` de cada target define quando ele volta a
  ficar elegível após a conclusão; um processamento abandonado em estado
  `PROCESSING` pode ser retomado após duas horas.
- `ingestion_comments` é removida pela task `finalize_ingestion`, depois que o
  fetch de replies termina ou é definitivamente interrompido. Ela não é uma
  tabela histórica.

No target `dev`, os limites padrão são 20 comentários por vídeo e 5 replies por
comentário. O valor `0` habilita paginação completa; um limite positivo reduz
deliberadamente a coleta e é adequado para desenvolvimento ou recuperação
controlada.

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

Para migrar um catálogo existente para o Workflow separado, execute também:

```sql
CREATE TABLE IF NOT EXISTS youtube_lakehouse.control.ingestion_step_outcomes (
  ingestion_id STRING NOT NULL,
  video_id STRING NOT NULL,
  step STRING NOT NULL,
  status STRING NOT NULL,
  completed_at TIMESTAMP NOT NULL,
  error_message STRING
) USING DELTA;

CREATE TABLE IF NOT EXISTS youtube_lakehouse.control.ingestion_comments (
  ingestion_id STRING NOT NULL,
  video_id STRING NOT NULL,
  comment_id STRING NOT NULL
) USING DELTA;
```

Para adicionar a telemetria das tasks em um catálogo existente, execute também
[002_task_execution_logs.sql](migrations/002_task_execution_logs.sql) uma única
vez antes do próximo Job. Em uma instalação nova, `00_setup.ipynb` já cria essa
tabela.

### Evolução da modelagem silver

Para um catálogo que já contenha as tabelas silver antigas, pare o Job e execute
uma única vez [001_silver_modeling.sql](migrations/001_silver_modeling.sql) em
um SQL Warehouse. A migração cria as tabelas v2, move os dados, normaliza as
tags e troca os nomes das tabelas; os backups `*_legacy_001` são preservados
para validação e rollback. Em uma instalação nova, basta executar
`src/notebooks/00_setup.ipynb`.

## Resiliência da API

Cada chamada à YouTube Data API tem até três tentativas. Falhas de conexão,
timeout, limitação temporária (`429`/`rateLimitExceeded`) e erros de servidor
transitórios (`5xx`) aguardam 1 e depois 2 segundos antes de nova tentativa.
Erros definitivos, como credencial inválida, vídeo inexistente e cota diária
esgotada, não são repetidos.

As falhas são classificadas no código (por exemplo, `TRANSIENT_NETWORK`,
`RATE_LIMITED`, `QUOTA_EXCEEDED` e `COMMENTS_DISABLED`) e a categoria é
preservada na mensagem de erro registrada nas tabelas de controle.

## Observabilidade operacional

Os notebooks do Job emitem uma linha JSON por início, término ou falha de task.
Todos os eventos usam `ingestion_id`, `task_key` e, quando o contexto do Job
está disponível, `task_run_id`. O fim da task registra explicitamente
`SUCCESS`, `PARTIAL_SUCCESS` ou `FAILED`, além de duração, totais por vídeo,
registros retornados e custo estimado da API.

Cada chamada HTTP da YouTube Data API acumula uma unidade de custo estimado e
registra o recurso, a tentativa e eventuais retries. Isso é uma estimativa de
consumo, não um saldo de quota: a API não disponibiliza saldo restante confiável
por resposta. Chave de API e parâmetros secretos nunca entram nesses eventos.

O resumo de cada tentativa é persistido em
`control.task_execution_logs`. Escritas repetidas da mesma tentativa fazem
`MERGE`; retries reais possuem outro `task_run_id` e permanecem distinguíveis.

## Estrutura do projeto

```text
.
├── src/
│   ├── dashboards/
│   │   ├── youtube_operational.lvdash.json # dashboard de operação e evolução
│   │   └── youtube_task_execution.lvdash.json # dashboard de saúde da ingestão
│   ├── notebooks/
│   │   ├── 00_setup.ipynb                  # cria catálogo, schemas e tabelas
│   │   ├── 01_youtube_ingestion_test.ipynb # consulta as tabelas do lakehouse
│   │   ├── 10_claim_targets.ipynb          # task de reserva dos targets
│   │   ├── 20_fetch_videos.ipynb           # task de vídeos
│   │   ├── 30_fetch_channels.ipynb         # task de canais
│   │   ├── 40_fetch_comments.ipynb         # task de comentários
│   │   ├── 50_fetch_replies.ipynb          # task de replies
│   │   └── 60_finalize_ingestion.ipynb     # task de consolidação
│   └── youtube_etl_genai/
│       ├── main.py                         # entry point da wheel e leitura do segredo
│       ├── observability.py                # logs JSON, contexto da task e resumo operacional
│       ├── pipeline.py                     # funções Python de cada etapa do Workflow
│       ├── persistence.py                  # schemas, Delta MERGE e estado de controle
│       └── youtube_client.py               # cliente reutilizável da YouTube Data API
├── resources/
│   ├── youtube_operational.dashboard.yml   # recurso do dashboard no Bundle
│   ├── youtube_task_execution.dashboard.yml # recurso do dashboard de saúde
│   ├── youtube_ingestion.job.yml            # Job serverless do Bundle
│   └── youtube_partial_success.alert.yml    # alerta SQL de resultado parcial
├── migrations/
│   ├── 001_silver_modeling.sql             # evolução reversível da silver existente
│   ├── 002_task_execution_logs.sql         # telemetria para catálogos existentes
│   └── 003_rename_dashboard_views.sql       # renomeia as views de apresentação
├── tests/
├── databricks.yml
├── pyproject.toml
└── README.md
```

O código de produção fica exclusivamente em `src/youtube_etl_genai` e é
empacotado na wheel. Os notebooks são adaptadores finos: leem os parâmetros da
task, obtêm a sessão/segredo e chamam as funções Python. Eles são sincronizados
pelo Bundle, mas não fazem parte do pacote Python.

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

O Bundle cria o Job `youtube_ingestion` com seis tasks serverless:
`claim_targets`, `fetch_videos`, `fetch_channels`, `fetch_comments`,
`fetch_replies` e `finalize_ingestion`. Após vídeos, canais e comentários
podem rodar em paralelo; replies depende de comentários. A finalização usa
`ALL_DONE` para registrar corretamente falhas de qualquer ramo.

O Job aceita uma execução por vez e mantém execuções adicionais em fila, mas
`fetch_channels` e `fetch_comments` podem rodar em paralelo na mesma execução.
As tasks de fetch têm até duas tentativas adicionais, com intervalo mínimo de
30 segundos; as tasks de controle têm uma tentativa adicional. Há timeout de
duas horas para o Job, uma hora para fetches e quinze minutos para
controle/finalização. As operações Delta são idempotentes para o mesmo
`ingestion_id`; respostas raw podem se repetir quando uma task é novamente
tentada, preservando o histórico técnico da API.

Todas as tasks usam o environment version 5 (Python 3.12) e a wheel produzida
pelo Poetry. Cada notebook chama o módulo Python correspondente, sem duplicar
lógica de API ou persistência.

O Bundle também cria o alerta SQL **YouTube ingestion partial success**. Ele é
avaliado a cada 15 minutos e notifica a pessoa que executou o deploy quando uma
execução encerrada nos últimos 15 minutos tem `PARTIAL_SUCCESS`. Ajuste a
assinatura do alerta para o grupo responsável antes de promover o Bundle a
produção.

Em serverless, a wheel lê a chave com `DBUtils.secrets.get` usando os parâmetros
`secret_scope` e `secret_key`. Em um cluster clássico, o entry point também
aceita a variável de ambiente `YOUTUBE_API_KEY` como alternativa de execução.

Parâmetros do Workflow:

| Parâmetro | Padrão | Significado |
|---|---:|---|
| `batch_size` | `20` | Máximo de vídeos elegíveis reservados por `claim_targets`. |
| `max_comments_per_video` | `20` | Máximo de comentários por vídeo em `fetch_comments`; `0` pagina todos. |
| `max_replies_per_comment` | `5` | Máximo de replies por comentário em `fetch_replies`; `0` pagina todas. |
| `secret_scope` | `youtube_api_key` | Scope que contém a chave da YouTube API. |
| `secret_key` | `api-key` | Nome da chave dentro do scope. |

Os valores são definidos em `databricks.yml` como variáveis e o target `dev`
os fornece ao Job. Para criar outro target, defina os valores adequados de
`catalog`, limites e segredo em `targets.<nome>.variables`; os notebooks não
precisam ser alterados.

Comandos principais:

```bash
databricks bundle validate --strict -t dev
databricks bundle deploy -t dev
databricks bundle run -t dev youtube_ingestion
```

`bundle deploy` constrói a wheel, sincroniza os arquivos e cria ou atualiza o
Job. `bundle run` apenas executa o Job já implantado. Pelo plugin Databricks do
VS Code, escolha **Run now** no Job/Workflow `dev-youtube-ingestion`; não use a
opção de executar o notebook atual, pois ela cria uma execução avulsa que não
representa este pipeline.

## Dashboards e monitoramento

O Bundle também versiona o dashboard **YouTube - Operação e evolução**. Ele
mostra o estado dos vídeos monitorados, falhas pendentes, a evolução diária de
views e inscritos, as métricas atuais por vídeo e o histórico das execuções.
As séries temporais usam somente o último snapshot de cada entidade em cada
dia, evitando somar múltiplas coletas do mesmo vídeo ou canal.

O dashboard usa quatro views de apresentação no schema `silver`:
`vw_dashboard_ingestion_runs`, `vw_dashboard_video_targets`,
`vw_dashboard_video_metrics` e `vw_dashboard_channel_metrics`. Elas são criadas pelo
notebook `00_setup.ipynb`; execute novamente apenas a célula das tabelas
`silver` ao atualizar um ambiente já existente.

As séries exibem os 90 dias mais recentes, consideram somente targets ativos e
carregam a última observação conhecida de cada entidade até cada dia (*last
observation carried forward*). A tabela de execuções é limitada aos últimos 30
dias e a 500 registros. O catálogo do recurso permanece `youtube_lakehouse`, o
mesmo catálogo no qual o notebook de setup cria as views de apresentação.

No target `dev`, o Bundle já aponta para o SQL Warehouse Serverless Starter.
Para outro target, configure `dashboard_warehouse_id` com o ID de um SQL
Warehouse adequado na seção `targets.<nome>.variables`.

```bash
databricks bundle validate --strict -t dev
databricks bundle deploy -t dev
```

O dashboard é criado como rascunho pelo deploy. Publique-o no workspace após a
validação visual e conceda acesso apenas às pessoas que deverão consultá-lo.

O Bundle também versiona o dashboard **YouTube - Saúde da ingestão**, voltado
à operação do Workflow. Ele acompanha tentativas de tasks, taxa de sucesso,
duração média, custo estimado da API, distribuição de status por task e os
detalhes de erros por execução. Seus dados vêm de
`control.task_execution_logs`.

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

Confira as últimas tentativas de task e identifique resultados parciais:

```sql
SELECT
  ingestion_id,
  task_key,
  task_run_id,
  status,
  duration_seconds,
  videos_attempted,
  videos_failed,
  records_fetched,
  api_cost_units,
  error_message
FROM (
  SELECT
    *,
    unix_timestamp(ended_at) - unix_timestamp(started_at) AS duration_seconds
  FROM youtube_lakehouse.control.task_execution_logs
)
ORDER BY ended_at DESC;
```

## Troubleshooting

| Situação | Como resolver |
|---|---|
| A CLI retorna erro de autenticação ou não encontra credenciais. | Autentique a Databricks CLI no workspace alvo e execute os comandos com o profile correto. Valide antes com `databricks auth profiles`. |
| O Job não lê a chave da YouTube API. | Confirme que o secret scope e a chave existem e que o Job tem permissão para lê-los. No target `dev`, os nomes esperados são `youtube_api_key` e `api-key`; ajuste `secret_scope` e `secret_key` para outro ambiente. Nunca inclua a chave no repositório. |
| A API retorna `QUOTA_EXCEEDED`, `RATE_LIMITED` ou uma falha transitória. | Consulte `control.task_execution_logs` e a mensagem de erro. O cliente repete falhas transitórias; para quota esgotada, aguarde a reposição ou ajuste a frequência, o batch e os limites de comentários/replies. |
| Um vídeo permanece em `PROCESSING`. | Consulte `control.video_processing_state`. Um processamento abandonado volta a ficar elegível após duas horas; verifique também a execução correspondente em `control.ingestion_runs` antes de intervir. |
| O dashboard não apresenta dados ou uma view não existe. | Execute a célula de criação das tabelas/views `silver` do notebook `src/notebooks/00_setup.ipynb` no catálogo configurado para o target. Depois, valide as permissões do SQL Warehouse e do catálogo. |
| O dashboard foi implantado, mas ainda não está acessível aos consumidores. | O deploy cria ou atualiza o rascunho. Faça a validação visual, publique o dashboard no workspace e conceda acesso às pessoas ou grupos necessários. |

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
python -m json.tool src/notebooks/10_claim_targets.ipynb >/dev/null
```

## Próximas evoluções

- Marcar de forma segura comentários e replies que deixam de aparecer em uma
  varredura completa, sem apagar histórico.
- Adicionar métricas de qualidade e alertas de taxa de erro ou duração anômala.
- Criar modelos `gold` para crescimento, engajamento e comparação entre vídeos
  e canais.
- Não implementar a coleta de transcrições de canais de terceiros, para respeitar
  direitos autorais e os termos de uso do YouTube. Essa capacidade poderá ser
  implementada para o proprietário do próprio canal, mediante autenticação OAuth
  e a permissão necessária para acessar as legendas pela API oficial.
- Incluir NLP, embeddings, Vector Search, RAG e agentes de IA.

## Status

✅ A primeira versão do pipeline está funcional: o Job serverless processa a
tabela de targets, coleta vídeo/canal/comentários/replies, atualiza o estado
atual e grava snapshots temporais. As camadas analíticas `gold` e as capacidades
de GenAI permanecem como evolução planejada.
