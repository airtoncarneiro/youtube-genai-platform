-- Evolui a camada silver para a modelagem analítica v2.
-- Execute em um SQL Warehouse com a ingestão parada. O script preserva as
-- tabelas anteriores com o sufixo _legacy_001 para validação e rollback.

CREATE TABLE youtube_lakehouse.silver.channels_v2 (
  channel_id STRING NOT NULL,
  title STRING,
  description STRING,
  custom_url STRING,
  published_at STRING,
  country STRING,
  view_count BIGINT,
  subscriber_count BIGINT,
  video_count BIGINT,
  uploads_playlist_id STRING,
  ingested_at TIMESTAMP NOT NULL,
  CONSTRAINT channels_pk PRIMARY KEY (channel_id) NOT ENFORCED
) USING DELTA
COMMENT 'Dimensão normalizada de canais do YouTube, atualizada por channel_id'
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');

INSERT INTO youtube_lakehouse.silver.channels_v2
SELECT * FROM youtube_lakehouse.silver.channels;

CREATE TABLE youtube_lakehouse.silver.videos_v2 (
  video_id STRING NOT NULL,
  channel_id STRING NOT NULL,
  title STRING,
  description STRING,
  published_at TIMESTAMP,
  category_id INT,
  duration INTERVAL DAY TO SECOND,
  definition STRING,
  caption STRING,
  view_count BIGINT,
  like_count BIGINT,
  comment_count BIGINT,
  privacy_status STRING,
  ingested_at TIMESTAMP NOT NULL,
  CONSTRAINT videos_pk PRIMARY KEY (video_id) NOT ENFORCED
) USING DELTA
COMMENT 'Entidade normalizada de vídeos do YouTube, atualizada por video_id'
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');

INSERT INTO youtube_lakehouse.silver.videos_v2
SELECT
  video_id,
  channel_id,
  title,
  description,
  try_cast(published_at AS TIMESTAMP) AS published_at,
  try_cast(category_id AS INT) AS category_id,
  make_dt_interval(
    try_cast(coalesce(nullif(regexp_extract(duration, '(\\d+)D', 1), ''), '0') AS INT),
    try_cast(coalesce(nullif(regexp_extract(duration, '(\\d+)H', 1), ''), '0') AS INT),
    try_cast(coalesce(nullif(regexp_extract(duration, '(\\d+)M', 1), ''), '0') AS INT),
    try_cast(coalesce(nullif(regexp_extract(duration, '(\\d+(?:\\.\\d+)?)S', 1), ''), '0') AS DECIMAL(18, 6))
  ) AS duration,
  definition,
  caption,
  view_count,
  like_count,
  comment_count,
  privacy_status,
  ingested_at
FROM youtube_lakehouse.silver.videos
;

CREATE TABLE youtube_lakehouse.silver.video_tags (
  video_id STRING NOT NULL,
  tag STRING NOT NULL,
  ingested_at TIMESTAMP NOT NULL,
  CONSTRAINT video_tags_pk PRIMARY KEY (video_id, tag) NOT ENFORCED
) USING DELTA
COMMENT 'Bridge normalizada das tags públicas vigentes por vídeo'
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');

INSERT INTO youtube_lakehouse.silver.video_tags
SELECT DISTINCT video_id, tag, ingested_at
FROM youtube_lakehouse.silver.videos
LATERAL VIEW explode(coalesce(tags, array())) AS tag
WHERE tag IS NOT NULL AND tag <> '';

CREATE TABLE youtube_lakehouse.silver.channel_snapshots_v2 (
  channel_id STRING NOT NULL,
  ingestion_id STRING NOT NULL,
  collected_at TIMESTAMP NOT NULL,
  collected_date DATE NOT NULL,
  view_count BIGINT,
  subscriber_count BIGINT,
  video_count BIGINT
) USING DELTA
PARTITIONED BY (collected_date)
COMMENT 'Snapshots imutáveis das métricas de canal; silver.channels contém somente o estado atual'
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');

INSERT INTO youtube_lakehouse.silver.channel_snapshots_v2
SELECT
  channel_id,
  ingestion_id,
  collected_at,
  CAST(collected_at AS DATE) AS collected_date,
  view_count,
  subscriber_count,
  video_count
FROM youtube_lakehouse.silver.channel_snapshots;

CREATE TABLE youtube_lakehouse.silver.video_snapshots_v2 (
  video_id STRING NOT NULL,
  ingestion_id STRING NOT NULL,
  collected_at TIMESTAMP NOT NULL,
  collected_date DATE NOT NULL,
  view_count BIGINT,
  like_count BIGINT,
  comment_count BIGINT
) USING DELTA
PARTITIONED BY (collected_date)
COMMENT 'Snapshots imutáveis das métricas de vídeo; silver.videos contém somente o estado atual'
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');

INSERT INTO youtube_lakehouse.silver.video_snapshots_v2
SELECT
  video_id,
  ingestion_id,
  collected_at,
  CAST(collected_at AS DATE) AS collected_date,
  view_count,
  like_count,
  comment_count
FROM youtube_lakehouse.silver.video_snapshots;

ALTER TABLE youtube_lakehouse.silver.channels RENAME TO channels_legacy_001;
ALTER TABLE youtube_lakehouse.silver.videos RENAME TO videos_legacy_001;
ALTER TABLE youtube_lakehouse.silver.channel_snapshots RENAME TO channel_snapshots_legacy_001;
ALTER TABLE youtube_lakehouse.silver.video_snapshots RENAME TO video_snapshots_legacy_001;

ALTER TABLE youtube_lakehouse.silver.channels_v2 RENAME TO channels;
ALTER TABLE youtube_lakehouse.silver.videos_v2 RENAME TO videos;
ALTER TABLE youtube_lakehouse.silver.channel_snapshots_v2 RENAME TO channel_snapshots;
ALTER TABLE youtube_lakehouse.silver.video_snapshots_v2 RENAME TO video_snapshots;

ALTER TABLE youtube_lakehouse.silver.videos
  ADD CONSTRAINT videos_channel_fk
  FOREIGN KEY (channel_id)
  REFERENCES youtube_lakehouse.silver.channels(channel_id)
  NOT ENFORCED;

ALTER TABLE youtube_lakehouse.silver.video_tags
  ADD CONSTRAINT video_tags_video_fk
  FOREIGN KEY (video_id)
  REFERENCES youtube_lakehouse.silver.videos(video_id)
  NOT ENFORCED;

CREATE OR REPLACE VIEW youtube_lakehouse.silver.dashboard_video_metrics
COMMENT 'Camada de apresentação do dashboard: snapshots temporais de métricas de vídeo'
AS
SELECT
  snapshot.video_id,
  video.title AS video_title,
  video.channel_id,
  channel.title AS channel_title,
  snapshot.collected_at,
  snapshot.view_count,
  snapshot.like_count,
  snapshot.comment_count
FROM youtube_lakehouse.silver.video_snapshots AS snapshot
LEFT JOIN youtube_lakehouse.silver.videos AS video
  ON snapshot.video_id = video.video_id
LEFT JOIN youtube_lakehouse.silver.channels AS channel
  ON video.channel_id = channel.channel_id;

-- Valide antes de descartar os backups:
-- SELECT COUNT(*) FROM youtube_lakehouse.silver.videos;
-- SELECT COUNT(*) FROM youtube_lakehouse.silver.videos_legacy_001;
-- SELECT * FROM youtube_lakehouse.silver.dashboard_video_metrics LIMIT 10;
