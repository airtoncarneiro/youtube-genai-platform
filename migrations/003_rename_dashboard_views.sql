-- Padroniza as views de apresentação com o prefixo vw_.
ALTER VIEW youtube_lakehouse.silver.dashboard_ingestion_runs
  RENAME TO youtube_lakehouse.silver.vw_dashboard_ingestion_runs;

ALTER VIEW youtube_lakehouse.silver.dashboard_video_targets
  RENAME TO youtube_lakehouse.silver.vw_dashboard_video_targets;

ALTER VIEW youtube_lakehouse.silver.dashboard_video_metrics
  RENAME TO youtube_lakehouse.silver.vw_dashboard_video_metrics;

ALTER VIEW youtube_lakehouse.silver.dashboard_channel_metrics
  RENAME TO youtube_lakehouse.silver.vw_dashboard_channel_metrics;
