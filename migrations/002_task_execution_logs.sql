CREATE TABLE IF NOT EXISTS youtube_lakehouse.control.task_execution_logs (
  ingestion_id STRING,
  task_key STRING NOT NULL,
  task_run_id STRING,
  started_at TIMESTAMP NOT NULL,
  ended_at TIMESTAMP NOT NULL,
  status STRING NOT NULL,
  videos_attempted BIGINT NOT NULL,
  videos_succeeded BIGINT NOT NULL,
  videos_failed BIGINT NOT NULL,
  records_fetched BIGINT NOT NULL,
  api_cost_units BIGINT NOT NULL,
  error_message STRING
) USING DELTA
COMMENT 'Resumo idempotente de cada tentativa de task do Workflow, para observabilidade operacional'
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');

COMMENT ON COLUMN youtube_lakehouse.control.task_execution_logs.ingestion_id IS 'Execução de ingestão associada; pode ser nula quando claim_targets falha antes de criar a execução';
COMMENT ON COLUMN youtube_lakehouse.control.task_execution_logs.task_key IS 'Chave estável da task do Databricks Job';
COMMENT ON COLUMN youtube_lakehouse.control.task_execution_logs.task_run_id IS 'Identificador da tentativa da task no Databricks Job; distingue retries';
COMMENT ON COLUMN youtube_lakehouse.control.task_execution_logs.started_at IS 'Data e hora UTC de início da tentativa da task';
COMMENT ON COLUMN youtube_lakehouse.control.task_execution_logs.ended_at IS 'Data e hora UTC de fim da tentativa da task';
COMMENT ON COLUMN youtube_lakehouse.control.task_execution_logs.status IS 'Resultado semântico da task: SUCCESS, PARTIAL_SUCCESS ou FAILED';
COMMENT ON COLUMN youtube_lakehouse.control.task_execution_logs.videos_attempted IS 'Quantidade de vídeos considerados pela task';
COMMENT ON COLUMN youtube_lakehouse.control.task_execution_logs.videos_succeeded IS 'Quantidade de vídeos concluídos sem erro na task';
COMMENT ON COLUMN youtube_lakehouse.control.task_execution_logs.videos_failed IS 'Quantidade de vídeos com erro ou estado não concluído na task';
COMMENT ON COLUMN youtube_lakehouse.control.task_execution_logs.records_fetched IS 'Quantidade de registros de domínio retornados pela task';
COMMENT ON COLUMN youtube_lakehouse.control.task_execution_logs.api_cost_units IS 'Custo estimado da YouTube Data API, acumulado por chamada HTTP da task';
COMMENT ON COLUMN youtube_lakehouse.control.task_execution_logs.error_message IS 'Erro técnico da task quando a tentativa falha';
