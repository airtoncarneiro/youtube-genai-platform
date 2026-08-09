from __future__ import annotations

import os

from youtube_etl_genai.observability import configure_job_logging
from youtube_etl_genai.pipeline import run_ingestion

LOGGER = configure_job_logging()


def _get_api_key(spark: object, secret_scope: str, secret_key: str) -> str:
    """Read the API key from an environment variable or a Databricks secret.

    Classic job clusters can inject ``YOUTUBE_API_KEY`` as an environment
    variable. Serverless Jobs do not use cluster Spark environment variables,
    so they resolve the same value from the configured secret scope.
    """
    api_key = os.environ.get("YOUTUBE_API_KEY")
    if api_key:
        return api_key

    try:
        from pyspark.dbutils import DBUtils

        return DBUtils(spark).secrets.get(scope=secret_scope, key=secret_key)
    except Exception as exc:
        raise RuntimeError(
            "Não foi possível obter YOUTUBE_API_KEY nem o segredo "
            f"{secret_scope}/{secret_key}"
        ) from exc


def _get_spark_session() -> object:
    """Return the Job session or open a Databricks Connect session for debugging.

    A Databricks Job already has an active Spark session. When this module runs
    locally under the VS Code Databricks Connect integration, a local
    ``SparkSession.builder`` is unsupported and the remote session must be
    created through ``DatabricksSession`` instead.
    """
    from pyspark.sql import SparkSession

    active_session = SparkSession.getActiveSession()
    if active_session:
        return active_session

    try:
        from databricks.connect import DatabricksSession
    except ModuleNotFoundError:
        # This fallback supports a regular local/classic Spark environment.
        return SparkSession.builder.getOrCreate()

    return DatabricksSession.builder.serverless().getOrCreate()


def run(
    batch_size: str = "20",
    max_comments_per_video: str = "0",
    max_replies_per_comment: str = "0",
    secret_scope: str = "youtube_api_key",
    secret_key: str = "api-key",
    catalog: str = "youtube_lakehouse",
) -> None:
    """Run a due batch from the Databricks wheel entry point.

    Job parameters are received as strings so the function can be called by
    Databricks task parameters; validation and conversion happen in the
    pipeline layer.
    """
    spark = _get_spark_session()
    api_key = _get_api_key(spark, secret_scope, secret_key)
    result = run_ingestion(
        spark=spark,
        api_key=api_key,
        batch_size=batch_size,
        max_comments_per_video=max_comments_per_video,
        max_replies_per_comment=max_replies_per_comment,
        catalog=catalog,
    )
    LOGGER.info("Coleta e persistência concluídas: %s", result)


def main() -> None:
    """Configure logging and run the ingestion using environment parameters."""
    configure_job_logging()
    run()


if __name__ == "__main__":
    main()
