CREATE TABLE IF NOT EXISTS youtube_lakehouse.control.channel_targets (
  channel_id STRING NOT NULL,
  discovery_mode STRING NOT NULL DEFAULT 'NONE',
  created_at TIMESTAMP NOT NULL,
  updated_at TIMESTAMP NOT NULL
) USING DELTA
COMMENT 'Configuração manual para descoberta automática de novos vídeos por canal'
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true',
  'delta.feature.allowColumnDefaults' = 'supported'
);

COMMENT ON COLUMN youtube_lakehouse.control.channel_targets.channel_id IS 'PK, FK → silver.channels.channel_id. Canal elegível para configuração de descoberta';
COMMENT ON COLUMN youtube_lakehouse.control.channel_targets.discovery_mode IS 'Modo de descoberta: NONE não consulta uploads, ALL cadastra todos após o corte e LAST cadastra somente o upload mais recente';
COMMENT ON COLUMN youtube_lakehouse.control.channel_targets.created_at IS 'Data e hora UTC de criação da configuração do canal';
COMMENT ON COLUMN youtube_lakehouse.control.channel_targets.updated_at IS 'Data e hora UTC da última alteração da configuração do canal';

MERGE INTO youtube_lakehouse.control.channel_targets AS target
USING (
  SELECT channel_id
  FROM youtube_lakehouse.silver.channels
  WHERE uploads_playlist_id IS NOT NULL
) AS source
ON target.channel_id = source.channel_id
WHEN NOT MATCHED THEN INSERT (
  channel_id,
  discovery_mode,
  created_at,
  updated_at
) VALUES (
  source.channel_id,
  'NONE',
  current_timestamp(),
  current_timestamp()
);

CREATE TABLE IF NOT EXISTS youtube_lakehouse.control.channel_discovery_runs (
  discovery_id STRING NOT NULL,
  started_at TIMESTAMP NOT NULL,
  ended_at TIMESTAMP,
  status STRING NOT NULL,
  channels_attempted BIGINT NOT NULL,
  channels_succeeded BIGINT NOT NULL,
  channels_failed BIGINT NOT NULL,
  videos_discovered BIGINT NOT NULL,
  videos_registered BIGINT NOT NULL,
  api_cost_units BIGINT NOT NULL,
  error_message STRING,
  CONSTRAINT channel_discovery_runs_pk PRIMARY KEY (discovery_id) NOT ENFORCED RELY
) USING DELTA
COMMENT 'Auditoria das execuções do Job de descoberta de novos vídeos por canal'
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');

COMMENT ON COLUMN youtube_lakehouse.control.channel_discovery_runs.discovery_id IS 'PK: Identificador da execução de descoberta';
COMMENT ON COLUMN youtube_lakehouse.control.channel_discovery_runs.started_at IS 'Data e hora UTC de início da descoberta';
COMMENT ON COLUMN youtube_lakehouse.control.channel_discovery_runs.ended_at IS 'Data e hora UTC de encerramento da descoberta';
COMMENT ON COLUMN youtube_lakehouse.control.channel_discovery_runs.status IS 'Estado da descoberta: RUNNING, SUCCESS, PARTIAL_SUCCESS ou FAILED';
COMMENT ON COLUMN youtube_lakehouse.control.channel_discovery_runs.channels_attempted IS 'Quantidade de canais consultados na API';
COMMENT ON COLUMN youtube_lakehouse.control.channel_discovery_runs.channels_succeeded IS 'Quantidade de canais consultados sem erro';
COMMENT ON COLUMN youtube_lakehouse.control.channel_discovery_runs.channels_failed IS 'Quantidade de canais cuja descoberta falhou';
COMMENT ON COLUMN youtube_lakehouse.control.channel_discovery_runs.videos_discovered IS 'Quantidade de IDs de vídeos posteriores ao corte por canal';
COMMENT ON COLUMN youtube_lakehouse.control.channel_discovery_runs.videos_registered IS 'Quantidade de IDs inseridos em control.video_targets';
COMMENT ON COLUMN youtube_lakehouse.control.channel_discovery_runs.api_cost_units IS 'Custo estimado da YouTube Data API, acumulado por chamada HTTP da descoberta';
COMMENT ON COLUMN youtube_lakehouse.control.channel_discovery_runs.error_message IS 'Resumo dos erros por canal, quando houver';

COMMENT ON COLUMN youtube_lakehouse.raw.api_responses.ingestion_id IS 'Identificador da execução de ingestão ou descoberta que originou a chamada à API';
