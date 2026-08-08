from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any
from uuid import uuid4

if TYPE_CHECKING:
    from pyspark.sql.types import StructType


def schemas() -> dict[str, StructType]:
    """Return the explicit Spark schemas used by the Delta tables.

    PySpark is imported lazily so API clients and unit tests can import this
    module without requiring a Spark runtime.
    """
    from pyspark.sql.types import (
        DateType,
        DayTimeIntervalType,
        IntegerType,
        LongType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    def fields(*definitions: tuple[str, Any]) -> StructType:
        """Build a nullable schema for API-derived silver records."""
        return StructType(
            [
                StructField(name, datatype, nullable=True)
                for name, datatype in definitions
            ]
        )

    return {
        "api_responses": StructType(
            [
                StructField("ingestion_id", StringType(), nullable=False),
                StructField("resource", StringType(), nullable=False),
                StructField("request_params_json", StringType(), nullable=False),
                StructField("response_json", StringType(), nullable=False),
                StructField("received_at", TimestampType(), nullable=False),
            ]
        ),
        "channels": fields(
            ("channel_id", StringType()),
            ("title", StringType()),
            ("description", StringType()),
            ("custom_url", StringType()),
            ("published_at", StringType()),
            ("country", StringType()),
            ("view_count", LongType()),
            ("subscriber_count", LongType()),
            ("video_count", LongType()),
            ("uploads_playlist_id", StringType()),
        ),
        "videos": fields(
            ("video_id", StringType()),
            ("channel_id", StringType()),
            ("title", StringType()),
            ("description", StringType()),
            ("published_at", TimestampType()),
            ("category_id", IntegerType()),
            ("duration", DayTimeIntervalType()),
            ("definition", StringType()),
            ("caption", StringType()),
            ("view_count", LongType()),
            ("like_count", LongType()),
            ("comment_count", LongType()),
            ("privacy_status", StringType()),
        ),
        "channel_snapshots": StructType(
            [
                StructField("channel_id", StringType(), nullable=False),
                StructField("ingestion_id", StringType(), nullable=False),
                StructField("collected_at", TimestampType(), nullable=False),
                StructField("collected_date", DateType(), nullable=False),
                StructField("view_count", LongType(), nullable=True),
                StructField("subscriber_count", LongType(), nullable=True),
                StructField("video_count", LongType(), nullable=True),
            ]
        ),
        "video_snapshots": StructType(
            [
                StructField("video_id", StringType(), nullable=False),
                StructField("ingestion_id", StringType(), nullable=False),
                StructField("collected_at", TimestampType(), nullable=False),
                StructField("collected_date", DateType(), nullable=False),
                StructField("view_count", LongType(), nullable=True),
                StructField("like_count", LongType(), nullable=True),
                StructField("comment_count", LongType(), nullable=True),
            ]
        ),
        "comments": fields(
            ("thread_id", StringType()),
            ("comment_id", StringType()),
            ("parent_id", StringType()),
            ("video_id", StringType()),
            ("author_name", StringType()),
            ("author_channel_id", StringType()),
            ("text", StringType()),
            ("like_count", LongType()),
            ("published_at", StringType()),
            ("updated_at", StringType()),
            ("reply_count", LongType()),
        ),
        "replies": fields(
            ("comment_id", StringType()),
            ("parent_id", StringType()),
            ("video_id", StringType()),
            ("author_name", StringType()),
            ("author_channel_id", StringType()),
            ("text", StringType()),
            ("like_count", LongType()),
            ("published_at", StringType()),
            ("updated_at", StringType()),
        ),
    }


def append_run_start(spark: Any, catalog: str, ingestion_id: str, source: str) -> None:
    """Append a RUNNING control record for a new ingestion."""
    spark.createDataFrame(
        [
            (
                ingestion_id,
                source,
                None,
                datetime.now(timezone.utc),
                None,
                "RUNNING",
                None,
            )
        ],
        "ingestion_id string, channel_handle string, channel_name string, "
        "started_at timestamp, ended_at timestamp, status string, error_message string",
    ).write.mode("append").format("delta").saveAsTable(
        f"{catalog}.control.ingestion_runs"
    )


def finish_run(
    spark: Any,
    catalog: str,
    ingestion_id: str,
    status: str,
    error_message: str | None = None,
    channel_name: str | None = None,
) -> None:
    """Update a control record with its outcome and observed channel name(s)."""
    view_name = f"staged_run_{uuid4().hex}"
    spark.createDataFrame(
        [(ingestion_id, status, error_message, channel_name)],
        "ingestion_id string, status string, error_message string, channel_name string",
    ).createOrReplaceTempView(view_name)
    try:
        spark.sql(
            f"""
        MERGE INTO {catalog}.control.ingestion_runs AS target
        USING {view_name} AS source
        ON target.ingestion_id = source.ingestion_id
        WHEN MATCHED THEN UPDATE SET
          target.ended_at = current_timestamp(),
          target.status = source.status,
          target.error_message = source.error_message,
          target.channel_name = COALESCE(source.channel_name, target.channel_name)
        """
        )
    finally:
        # Always remove the temporary view, including when the MERGE fails.
        spark.catalog.dropTempView(view_name)


def append_raw(
    spark: Any, catalog: str, responses: list[dict[str, Any]], raw_schema: StructType
) -> None:
    """Append immutable API responses to the raw Delta table."""
    if not responses:
        return
    (
        spark.createDataFrame(responses, raw_schema)
        .write.mode("append")
        .format("delta")
        .saveAsTable(f"{catalog}.raw.api_responses")
    )


def merge_silver(
    spark: Any,
    catalog: str,
    table: str,
    rows: list[dict[str, Any]],
    schema: StructType,
    key_column: str,
) -> None:
    """Deduplicate rows and merge one normalized entity into silver."""
    if not rows:
        return

    from pyspark.sql import functions as F

    view_name = f"staged_{table}_{uuid4().hex}"
    (
        spark.createDataFrame(rows, schema)
        .dropDuplicates([key_column])
        .withColumn("ingested_at", F.current_timestamp())
        .createOrReplaceTempView(view_name)
    )
    try:
        spark.sql(
            f"""
        MERGE INTO {catalog}.silver.{table} AS target
        USING {view_name} AS source
        ON target.{key_column} = source.{key_column}
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
        )
    finally:
        # Temporary views are session-scoped; clean them up after each merge.
        spark.catalog.dropTempView(view_name)


def merge_snapshots(
    spark: Any,
    catalog: str,
    table: str,
    rows: list[dict[str, Any]],
    schema: StructType,
    entity_key: str,
) -> None:
    """Insert immutable metric snapshots once per entity and ingestion run."""
    if not rows:
        return

    view_name = f"staged_{table}_{uuid4().hex}"
    (
        spark.createDataFrame(rows, schema)
        .dropDuplicates([entity_key, "ingestion_id"])
        .createOrReplaceTempView(view_name)
    )
    try:
        spark.sql(
            f"""
        MERGE INTO {catalog}.silver.{table} AS target
        USING {view_name} AS source
        ON target.{entity_key} = source.{entity_key}
          AND target.ingestion_id = source.ingestion_id
        WHEN NOT MATCHED THEN INSERT *
        """
        )
    finally:
        spark.catalog.dropTempView(view_name)


def replace_video_tags(
    spark: Any,
    catalog: str,
    videos: list[dict[str, Any]],
) -> None:
    """Synchronize the normalized tag bridge for the videos fetched in a run."""
    video_ids = sorted({row["video_id"] for row in videos if row.get("video_id")})
    if not video_ids:
        return

    observed_at = datetime.now(timezone.utc)
    target_view = f"video_tag_targets_{uuid4().hex}"
    tag_view = f"video_tags_{uuid4().hex}"
    target_rows = [(video_id,) for video_id in video_ids]
    tag_rows = [
        (row["video_id"], tag, observed_at)
        for row in videos
        if row.get("video_id")
        for tag in row.get("tags") or []
        if tag
    ]
    spark.createDataFrame(target_rows, "video_id string").createOrReplaceTempView(
        target_view
    )
    spark.createDataFrame(
        tag_rows,
        "video_id string, tag string, ingested_at timestamp",
    ).dropDuplicates(["video_id", "tag"]).createOrReplaceTempView(tag_view)
    try:
        spark.sql(
            f"""
            MERGE INTO {catalog}.silver.video_tags AS target
            USING {tag_view} AS source
            ON target.video_id = source.video_id AND target.tag = source.tag
            WHEN MATCHED THEN UPDATE SET target.ingested_at = source.ingested_at
            WHEN NOT MATCHED THEN INSERT *
            """
        )
        spark.sql(
            f"""
            DELETE FROM {catalog}.silver.video_tags AS target
            WHERE EXISTS (
              SELECT 1 FROM {target_view} AS source
              WHERE source.video_id = target.video_id
            )
            AND NOT EXISTS (
              SELECT 1 FROM {tag_view} AS source
              WHERE source.video_id = target.video_id AND source.tag = target.tag
            )
            """
        )
    finally:
        spark.catalog.dropTempView(tag_view)
        spark.catalog.dropTempView(target_view)


def claim_video_targets(
    spark: Any,
    catalog: str,
    ingestion_id: str,
    batch_size: int,
) -> list[tuple[str, int]]:
    """Reserve due targets so one job run owns their processing state.

    A target left in ``PROCESSING`` by a failed worker becomes eligible again
    after two hours. The state table retains attempts and the last error.
    """
    candidates = spark.sql(
        f"""
        SELECT target.video_id, target.refresh_interval_hours
        FROM {catalog}.control.video_targets AS target
        LEFT JOIN {catalog}.control.video_processing_state AS state
          ON target.video_id = state.video_id
        WHERE target.is_active = true
          AND (
            state.video_id IS NULL
            OR (
              state.status <> 'PROCESSING'
              AND (
                state.next_refresh_at IS NULL
                OR state.next_refresh_at <= current_timestamp()
              )
            )
            OR (
              state.status = 'PROCESSING'
              AND state.claimed_at < current_timestamp() - INTERVAL 2 HOURS
            )
          )
        ORDER BY
          CASE WHEN state.last_succeeded_at IS NULL THEN 0 ELSE 1 END,
          target.priority DESC,
          state.next_refresh_at ASC NULLS FIRST,
          target.video_id
        LIMIT {batch_size}
        """
    ).collect()
    if not candidates:
        return []

    claimed_at = datetime.now(timezone.utc)
    view_name = f"claimed_targets_{uuid4().hex}"
    spark.createDataFrame(
        [
            (row.video_id, row.refresh_interval_hours, ingestion_id, claimed_at)
            for row in candidates
        ],
        "video_id string, refresh_interval_hours int, ingestion_id string, claimed_at timestamp",
    ).createOrReplaceTempView(view_name)
    try:
        spark.sql(
            f"""
        MERGE INTO {catalog}.control.video_processing_state AS target
        USING {view_name} AS source
        ON target.video_id = source.video_id
        WHEN MATCHED THEN UPDATE SET
          target.status = 'PROCESSING',
          target.claimed_at = source.claimed_at,
          target.last_attempt_at = source.claimed_at,
          target.last_ingestion_id = source.ingestion_id,
          target.attempt_count = target.attempt_count + 1,
          target.error_message = NULL
        WHEN NOT MATCHED THEN INSERT (
          video_id, status, first_processed_at, claimed_at, last_attempt_at,
          last_succeeded_at, next_refresh_at, attempt_count,
          last_ingestion_id, error_message
        ) VALUES (
          source.video_id, 'PROCESSING', NULL, source.claimed_at,
          source.claimed_at, NULL, NULL, 1, source.ingestion_id, NULL
        )
        """
        )
    finally:
        spark.catalog.dropTempView(view_name)

    return [(row.video_id, row.refresh_interval_hours) for row in candidates]


def finish_video_target(
    spark: Any,
    catalog: str,
    video_id: str,
    ingestion_id: str,
    status: str,
    refresh_interval_hours: int,
    error_message: str | None = None,
) -> None:
    """Persist the outcome and calculate the next scheduled refresh."""
    completed_at = datetime.now(timezone.utc)
    next_refresh_at = completed_at + timedelta(hours=refresh_interval_hours)
    view_name = f"completed_target_{uuid4().hex}"
    spark.createDataFrame(
        [
            (
                video_id,
                ingestion_id,
                status,
                completed_at,
                next_refresh_at,
                error_message,
            )
        ],
        "video_id string, ingestion_id string, status string, completed_at timestamp, next_refresh_at timestamp, error_message string",
    ).createOrReplaceTempView(view_name)
    try:
        spark.sql(
            f"""
        MERGE INTO {catalog}.control.video_processing_state AS target
        USING {view_name} AS source
        ON target.video_id = source.video_id
        WHEN MATCHED THEN UPDATE SET
          target.status = source.status,
          target.claimed_at = NULL,
          target.last_succeeded_at = CASE
            WHEN source.status = 'SUCCESS' THEN source.completed_at
            ELSE target.last_succeeded_at
          END,
          target.first_processed_at = COALESCE(
            target.first_processed_at,
            CASE WHEN source.status = 'SUCCESS' THEN source.completed_at END
          ),
          target.next_refresh_at = COALESCE(
            source.next_refresh_at, target.next_refresh_at
          ),
          target.last_ingestion_id = source.ingestion_id,
          target.error_message = source.error_message
        """
        )
    finally:
        spark.catalog.dropTempView(view_name)


def ingestion_targets(
    spark: Any, catalog: str, ingestion_id: str
) -> list[tuple[str, int]]:
    """Return the targets owned by one ingestion run.

    ``video_processing_state`` is the durable hand-off between Workflow tasks.
    The query intentionally only returns targets still owned by ``ingestion_id``.
    """
    rows = spark.sql(
        f"""
        SELECT target.video_id, target.refresh_interval_hours
        FROM {catalog}.control.video_targets AS target
        INNER JOIN {catalog}.control.video_processing_state AS state
          ON target.video_id = state.video_id
        WHERE state.last_ingestion_id = '{ingestion_id}'
          AND state.status = 'PROCESSING'
        ORDER BY target.video_id
        """
    ).collect()
    return [(row.video_id, row.refresh_interval_hours) for row in rows]


def record_step_outcomes(
    spark: Any,
    catalog: str,
    ingestion_id: str,
    step: str,
    outcomes: dict[str, tuple[str, str | None]],
) -> None:
    """Upsert the outcome of one Workflow step for each video.

    A rerun of a task replaces its prior result for the same ingestion and
    video. This makes task retries safe while preserving the latest error for
    the finalizer to act on.
    """
    if not outcomes:
        return

    completed_at = datetime.now(timezone.utc)
    view_name = f"step_outcomes_{uuid4().hex}"
    spark.createDataFrame(
        [
            (ingestion_id, video_id, step, status, completed_at, error_message)
            for video_id, (status, error_message) in outcomes.items()
        ],
        "ingestion_id string, video_id string, step string, status string, "
        "completed_at timestamp, error_message string",
    ).createOrReplaceTempView(view_name)
    try:
        spark.sql(
            f"""
            MERGE INTO {catalog}.control.ingestion_step_outcomes AS target
            USING {view_name} AS source
            ON target.ingestion_id = source.ingestion_id
              AND target.video_id = source.video_id
              AND target.step = source.step
            WHEN MATCHED THEN UPDATE SET
              target.status = source.status,
              target.completed_at = source.completed_at,
              target.error_message = source.error_message
            WHEN NOT MATCHED THEN INSERT *
            """
        )
    finally:
        spark.catalog.dropTempView(view_name)


def replace_ingestion_comments(
    spark: Any,
    catalog: str,
    ingestion_id: str,
    comments: list[dict[str, Any]],
) -> None:
    """Persist the exact top-level comments returned in one ingestion.

    This is a short-lived operational hand-off, not a second silver entity. It
    prevents the replies task from accidentally processing comments retained
    from a prior ingestion of the same video.
    """
    spark.sql(
        f"""
        DELETE FROM {catalog}.control.ingestion_comments
        WHERE ingestion_id = '{ingestion_id}'
        """
    )
    rows = [
        (ingestion_id, row["video_id"], row["comment_id"])
        for row in comments
        if row.get("video_id") and row.get("comment_id")
    ]
    if not rows:
        return
    spark.createDataFrame(
        rows,
        "ingestion_id string, video_id string, comment_id string",
    ).dropDuplicates(["ingestion_id", "comment_id"]).write.mode("append").format(
        "delta"
    ).saveAsTable(f"{catalog}.control.ingestion_comments")


def clear_ingestion_comments(spark: Any, catalog: str, ingestion_id: str) -> None:
    """Remove operational comment hand-off rows after ingestion finalization."""
    spark.sql(
        f"""
        DELETE FROM {catalog}.control.ingestion_comments
        WHERE ingestion_id = '{ingestion_id}'
        """
    )


def ingestion_comment_ids(
    spark: Any, catalog: str, ingestion_id: str, video_ids: list[str]
) -> dict[str, list[str]]:
    """Return comments staged by ``fetch_comments`` for the requested videos."""
    result = {video_id: [] for video_id in video_ids}
    if not video_ids:
        return result
    values = ", ".join(f"'{video_id}'" for video_id in video_ids)
    rows = spark.sql(
        f"""
        SELECT video_id, comment_id
        FROM {catalog}.control.ingestion_comments
        WHERE ingestion_id = '{ingestion_id}'
          AND video_id IN ({values})
        """
    ).collect()
    for row in rows:
        result[row.video_id].append(row.comment_id)
    return result


def step_outcomes(
    spark: Any, catalog: str, ingestion_id: str
) -> dict[str, dict[str, tuple[str, str | None]]]:
    """Return persisted outcomes indexed by video and Workflow step."""
    rows = spark.sql(
        f"""
        SELECT video_id, step, status, error_message
        FROM {catalog}.control.ingestion_step_outcomes
        WHERE ingestion_id = '{ingestion_id}'
        """
    ).collect()
    result: dict[str, dict[str, tuple[str, str | None]]] = {}
    for row in rows:
        result.setdefault(row.video_id, {})[row.step] = (
            row.status,
            row.error_message,
        )
    return result
