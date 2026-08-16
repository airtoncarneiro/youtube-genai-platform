from __future__ import annotations

import json
import logging
from collections.abc import Callable
from datetime import datetime, timezone
from itertools import islice
from typing import Any, Iterable, Iterator
from uuid import uuid4

from youtube_etl_genai.persistence import (
    append_raw,
    append_channel_discovery_run,
    append_run_start,
    channel_discovery_targets,
    claim_video_targets,
    clear_ingestion_comments,
    finish_channel_discovery_run,
    finish_run,
    finish_video_target,
    ingestion_comment_ids,
    ingestion_targets,
    merge_silver,
    merge_snapshots,
    record_step_outcomes,
    register_discovered_video_targets,
    replace_ingestion_comments,
    replace_video_tags,
    schemas,
    step_outcomes,
)
from youtube_etl_genai.youtube_client import (
    YouTubeAPIError,
    YouTubeAPIErrorCategory,
    YouTubeClient,
)

LOGGER = logging.getLogger(__name__)

FETCH_STEPS = ("fetch_videos", "fetch_channels", "fetch_comments", "fetch_replies")


def _step_result(
    *,
    record_name: str,
    records_fetched: int,
    videos_attempted: int,
    videos_succeeded: int,
    videos_failed: int,
    api_cost_units: int = 0,
) -> dict[str, int | str]:
    """Return a uniform, serializable operational result for a Job task."""
    return {
        "status": "SUCCESS" if videos_failed == 0 else "PARTIAL_SUCCESS",
        record_name: records_fetched,
        "videos_attempted": videos_attempted,
        "videos_succeeded": videos_succeeded,
        "videos_failed": videos_failed,
        "records_fetched": records_fetched,
        "api_cost_units": api_cost_units,
    }


def _validate_limit(value: str, name: str, allow_zero: bool = True) -> int:
    """Parse and validate a non-negative job limit."""
    try:
        limit = int(value)
    except ValueError as exc:
        raise ValueError(f"{name} deve ser um número inteiro") from exc
    if limit < 0 or (not allow_zero and limit == 0):
        comparator = "maior ou igual a zero" if allow_zero else "maior que zero"
        raise ValueError(f"{name} deve ser {comparator}")
    return limit


def _bounded(items: Iterable[dict[str, Any]], limit: int) -> Iterator[dict[str, Any]]:
    """Return all items for zero, otherwise the configured bounded prefix."""
    return iter(items) if limit == 0 else islice(items, limit)


def _snapshot_rows(
    entity: str,
    rows: list[dict[str, Any]],
    ingestion_id: str,
    collected_at: datetime,
) -> list[dict[str, Any]]:
    """Project current entity records into immutable metric snapshots."""
    if entity == "channels":
        metrics = ("view_count", "subscriber_count", "video_count")
        entity_key = "channel_id"
    else:
        metrics = ("view_count", "like_count", "comment_count")
        entity_key = "video_id"

    return [
        {
            entity_key: row[entity_key],
            "ingestion_id": ingestion_id,
            "collected_at": collected_at,
            "collected_date": collected_at.date(),
            **{metric: row.get(metric) for metric in metrics},
        }
        for row in rows
        if row.get(entity_key)
    ]


def _response_collector(ingestion_id: str) -> tuple[list[dict[str, Any]], Any]:
    """Create an observer that records successful API calls without secrets."""
    responses: list[dict[str, Any]] = []

    def capture(
        resource: str, params: dict[str, Any], response: dict[str, Any]
    ) -> None:
        responses.append(
            {
                "ingestion_id": ingestion_id,
                "resource": resource,
                "request_params_json": json.dumps(params, sort_keys=True),
                "response_json": json.dumps(response, sort_keys=True),
                "received_at": datetime.now(timezone.utc),
            }
        )

    return responses, capture


def claim_targets_step(
    *, spark: Any, batch_size: str = "20", catalog: str = "youtube_lakehouse"
) -> dict[str, int | str]:
    """Create an ingestion run and reserve its due video targets."""
    ingestion_id = str(uuid4())
    requested_batch_size = _validate_limit(batch_size, "batch_size", allow_zero=False)
    append_run_start(spark, catalog, ingestion_id, "control.video_targets")
    targets = claim_video_targets(spark, catalog, ingestion_id, requested_batch_size)
    if not targets:
        finish_run(spark, catalog, ingestion_id, "SUCCESS")
    result = {
        "ingestion_id": ingestion_id,
        "status": "SUCCESS",
        "targets": len(targets),
        "videos_attempted": len(targets),
        "videos_succeeded": len(targets),
        "videos_failed": 0,
        "records_fetched": len(targets),
        "api_cost_units": 0,
    }
    LOGGER.info("Targets reservados: %s", result)
    return result


def _as_utc_datetime(value: object) -> datetime | None:
    """Normalize an API or Spark timestamp before comparing published dates."""
    if isinstance(value, str):
        try:
            value = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
    if not isinstance(value, datetime):
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _latest_upload_id_after_watermark(
    client: YouTubeClient,
    uploads_playlist_id: str,
    last_downloaded_published_at: datetime,
) -> str | None:
    """Return only the most recent upload when it is newer than the watermark.

    The uploads playlist is ordered newest first. Intentionally inspect only
    its first item: historical uploads between the persisted watermark and the
    current execution are not backfilled.
    """
    watermark = _as_utc_datetime(last_downloaded_published_at)
    if watermark is None:
        return None

    try:
        item = next(client.iter_uploads(uploads_playlist_id))
    except StopIteration:
        return None

    normalized = client.normalize_playlist_item(item)
    published_at = _as_utc_datetime(normalized.get("published_at"))
    if published_at is None or published_at <= watermark:
        return None
    video_id = normalized.get("video_id")
    return video_id if isinstance(video_id, str) and video_id else None


def _all_upload_ids_after_watermark(
    client: YouTubeClient,
    uploads_playlist_id: str,
    last_downloaded_published_at: datetime,
) -> list[str]:
    """Return every upload newer than the persisted watermark, newest first."""
    watermark = _as_utc_datetime(last_downloaded_published_at)
    if watermark is None:
        return []

    video_ids: list[str] = []
    for item in client.iter_uploads(uploads_playlist_id):
        normalized = client.normalize_playlist_item(item)
        published_at = _as_utc_datetime(normalized.get("published_at"))
        if published_at is None:
            continue
        if published_at <= watermark:
            break
        video_id = normalized.get("video_id")
        if isinstance(video_id, str) and video_id:
            video_ids.append(video_id)
    return video_ids


def _upload_ids_for_discovery_mode(
    client: YouTubeClient,
    uploads_playlist_id: str,
    last_downloaded_published_at: datetime,
    discovery_mode: str,
) -> list[str]:
    """Select uploads according to one of the supported channel discovery modes."""
    if discovery_mode == "ALL":
        return _all_upload_ids_after_watermark(
            client, uploads_playlist_id, last_downloaded_published_at
        )
    if discovery_mode == "LAST":
        latest_upload_id = _latest_upload_id_after_watermark(
            client, uploads_playlist_id, last_downloaded_published_at
        )
        return [latest_upload_id] if latest_upload_id is not None else []
    raise ValueError(
        f"discovery_mode inválido para o canal: {discovery_mode!r}. "
        "Use NONE, ALL ou LAST."
    )


def discover_channel_videos_step(
    *,
    spark: Any,
    api_key: str,
    new_video_priority: str = "100",
    new_video_refresh_interval_hours: str = "24",
    catalog: str = "youtube_lakehouse",
    api_cost_observer: Callable[[int], None] | None = None,
) -> dict[str, int | str]:
    """Discover enabled-channel uploads and enqueue IDs for existing ingestion.

    This intentionally does not write ``silver.videos``. The established
    ``youtube_ingestion`` Job remains the sole owner of complete video,
    channel, comment, reply, and snapshot persistence.
    """
    priority = _validate_limit(
        new_video_priority, "new_video_priority", allow_zero=False
    )
    refresh_interval_hours = _validate_limit(
        new_video_refresh_interval_hours,
        "new_video_refresh_interval_hours",
        allow_zero=False,
    )
    discovery_id = str(uuid4())
    append_channel_discovery_run(spark, catalog, discovery_id)
    raw, observe = _response_collector(discovery_id)
    targets = channel_discovery_targets(spark, catalog)
    channels_attempted = 0
    channels_succeeded = 0
    channels_failed = 0
    channels_skipped_without_watermark = 0
    errors: list[str] = []
    discovered_ids: list[str] = []
    raw_persisted = False
    client = YouTubeClient(
        api_key=api_key,
        response_observer=observe,
        ingestion_id=discovery_id,
        api_cost_observer=api_cost_observer,
    )

    try:
        for (
            channel_id,
            uploads_playlist_id,
            discovery_mode,
            last_published_at,
        ) in targets:
            watermark = _as_utc_datetime(last_published_at)
            if watermark is None:
                channels_skipped_without_watermark += 1
                LOGGER.warning(
                    "Canal %s ignorado: não há vídeo baixado para formar o corte",
                    channel_id,
                )
                continue
            channels_attempted += 1
            try:
                discovered_ids.extend(
                    _upload_ids_for_discovery_mode(
                        client,
                        uploads_playlist_id,
                        watermark,
                        discovery_mode,
                    )
                )
                channels_succeeded += 1
            except Exception as exc:
                channels_failed += 1
                errors.append(f"{channel_id}: {exc}")
                LOGGER.exception("Falha ao descobrir uploads do canal %s", channel_id)

        append_raw(spark, catalog, raw, schemas()["api_responses"])
        raw_persisted = True
        unique_ids = sorted(set(discovered_ids))
        videos_registered = register_discovered_video_targets(
            spark,
            catalog,
            unique_ids,
            priority=priority,
            refresh_interval_hours=refresh_interval_hours,
        )
        status = "SUCCESS" if channels_failed == 0 else "PARTIAL_SUCCESS"
        error_message = "; ".join(errors) or None
        finish_channel_discovery_run(
            spark,
            catalog,
            discovery_id,
            status=status,
            channels_attempted=channels_attempted,
            channels_succeeded=channels_succeeded,
            channels_failed=channels_failed,
            videos_discovered=len(unique_ids),
            videos_registered=videos_registered,
            api_cost_units=client.api_cost_units,
            error_message=error_message,
        )
    except Exception as exc:
        if not raw_persisted:
            append_raw(spark, catalog, raw, schemas()["api_responses"])
        finish_channel_discovery_run(
            spark,
            catalog,
            discovery_id,
            status="FAILED",
            channels_attempted=channels_attempted,
            channels_succeeded=channels_succeeded,
            channels_failed=channels_failed,
            videos_discovered=len(set(discovered_ids)),
            videos_registered=0,
            api_cost_units=client.api_cost_units,
            error_message=str(exc),
        )
        raise

    result = {
        "ingestion_id": discovery_id,
        "status": status,
        "channels_attempted": channels_attempted,
        "channels_succeeded": channels_succeeded,
        "channels_failed": channels_failed,
        "channels_skipped_without_watermark": channels_skipped_without_watermark,
        "videos_discovered": len(unique_ids),
        "videos_registered": videos_registered,
        "videos_attempted": channels_attempted,
        "videos_succeeded": channels_succeeded,
        "videos_failed": channels_failed,
        "records_fetched": videos_registered,
        "api_cost_units": client.api_cost_units,
    }
    LOGGER.info("Descoberta de vídeos por canal finalizada: %s", result)
    return result


def fetch_videos_step(
    *,
    spark: Any,
    api_key: str,
    ingestion_id: str,
    catalog: str = "youtube_lakehouse",
    api_cost_observer: Callable[[int], None] | None = None,
) -> dict[str, int | str]:
    """Fetch videos owned by an ingestion and persist current data and snapshots."""
    targets = ingestion_targets(spark, catalog, ingestion_id)
    if not targets:
        return _step_result(
            record_name="videos",
            records_fetched=0,
            videos_attempted=0,
            videos_succeeded=0,
            videos_failed=0,
        )

    raw, observe = _response_collector(ingestion_id)
    table_schemas = schemas()
    client = YouTubeClient(
        api_key=api_key,
        response_observer=observe,
        ingestion_id=ingestion_id,
        api_cost_observer=api_cost_observer,
    )
    target_ids = [video_id for video_id, _ in targets]
    try:
        videos = [client.normalize_video(row) for row in client.get_videos(target_ids)]
    except Exception:
        append_raw(spark, catalog, raw, table_schemas["api_responses"])
        raise
    append_raw(spark, catalog, raw, table_schemas["api_responses"])
    merge_silver(spark, catalog, "videos", videos, table_schemas["videos"], "video_id")
    replace_video_tags(spark, catalog, videos)
    merge_snapshots(
        spark,
        catalog,
        "video_snapshots",
        _snapshot_rows("videos", videos, ingestion_id, datetime.now(timezone.utc)),
        table_schemas["video_snapshots"],
        "video_id",
    )
    found = {row["video_id"] for row in videos if row.get("video_id")}
    record_step_outcomes(
        spark,
        catalog,
        ingestion_id,
        "fetch_videos",
        {
            video_id: (
                ("SUCCESS", None)
                if video_id in found
                else ("NOT_FOUND", "Vídeo não encontrado ou não está acessível")
            )
            for video_id in target_ids
        },
    )
    return _step_result(
        record_name="videos",
        records_fetched=len(videos),
        videos_attempted=len(target_ids),
        videos_succeeded=len(found),
        videos_failed=len(target_ids) - len(found),
        api_cost_units=client.api_cost_units,
    )


def _successful_video_ids(spark: Any, catalog: str, ingestion_id: str) -> list[str]:
    outcomes = step_outcomes(spark, catalog, ingestion_id)
    return [
        video_id
        for video_id, _ in ingestion_targets(spark, catalog, ingestion_id)
        if outcomes.get(video_id, {}).get("fetch_videos", (None, None))[0] == "SUCCESS"
    ]


def _video_channels(
    spark: Any, catalog: str, ingestion_id: str, video_ids: list[str]
) -> dict[str, str | None]:
    if not video_ids:
        return {}
    values = ", ".join(f"'{video_id}'" for video_id in video_ids)
    rows = spark.sql(
        f"""
        SELECT video_id, channel_id
        FROM {catalog}.silver.videos
        WHERE video_id IN ({values})
        """
    ).collect()
    return {row.video_id: row.channel_id for row in rows}


def fetch_channels_step(
    *,
    spark: Any,
    api_key: str,
    ingestion_id: str,
    catalog: str = "youtube_lakehouse",
    api_cost_observer: Callable[[int], None] | None = None,
) -> dict[str, int | str]:
    """Fetch channels referenced by the successfully fetched videos."""
    video_ids = _successful_video_ids(spark, catalog, ingestion_id)
    if not video_ids:
        return _step_result(
            record_name="channels",
            records_fetched=0,
            videos_attempted=0,
            videos_succeeded=0,
            videos_failed=0,
        )
    video_channels = _video_channels(spark, catalog, ingestion_id, video_ids)
    channel_ids = sorted(
        {channel_id for channel_id in video_channels.values() if channel_id}
    )
    raw, observe = _response_collector(ingestion_id)
    table_schemas = schemas()
    client = YouTubeClient(
        api_key=api_key,
        response_observer=observe,
        ingestion_id=ingestion_id,
        api_cost_observer=api_cost_observer,
    )
    try:
        channels = [
            client.normalize_channel(row) for row in client.get_channels(channel_ids)
        ]
    except Exception:
        append_raw(spark, catalog, raw, table_schemas["api_responses"])
        raise
    append_raw(spark, catalog, raw, table_schemas["api_responses"])
    merge_silver(
        spark, catalog, "channels", channels, table_schemas["channels"], "channel_id"
    )
    merge_snapshots(
        spark,
        catalog,
        "channel_snapshots",
        _snapshot_rows("channels", channels, ingestion_id, datetime.now(timezone.utc)),
        table_schemas["channel_snapshots"],
        "channel_id",
    )
    found_channels = {row["channel_id"] for row in channels if row.get("channel_id")}
    record_step_outcomes(
        spark,
        catalog,
        ingestion_id,
        "fetch_channels",
        {
            video_id: (
                ("SUCCESS", None)
                if channel_id in found_channels
                else ("FAILED", "Canal do vídeo não foi retornado pela API")
            )
            for video_id, channel_id in video_channels.items()
        },
    )
    videos_succeeded = sum(
        channel_id in found_channels for channel_id in video_channels.values()
    )
    return _step_result(
        record_name="channels",
        records_fetched=len(channels),
        videos_attempted=len(video_ids),
        videos_succeeded=videos_succeeded,
        videos_failed=len(video_ids) - videos_succeeded,
        api_cost_units=client.api_cost_units,
    )


def fetch_comments_step(
    *,
    spark: Any,
    api_key: str,
    ingestion_id: str,
    max_comments_per_video: str = "0",
    catalog: str = "youtube_lakehouse",
    api_cost_observer: Callable[[int], None] | None = None,
) -> dict[str, int | str]:
    """Fetch top-level comments independently for every fetched video."""
    comment_limit = _validate_limit(max_comments_per_video, "max_comments_per_video")
    video_ids = _successful_video_ids(spark, catalog, ingestion_id)
    if not video_ids:
        return _step_result(
            record_name="comments",
            records_fetched=0,
            videos_attempted=0,
            videos_succeeded=0,
            videos_failed=0,
        )
    raw, observe = _response_collector(ingestion_id)
    table_schemas = schemas()
    client = YouTubeClient(
        api_key=api_key,
        response_observer=observe,
        ingestion_id=ingestion_id,
        api_cost_observer=api_cost_observer,
    )
    comments: list[dict[str, Any]] = []
    outcomes: dict[str, tuple[str, str | None]] = {}
    for video_id in video_ids:
        try:
            comments.extend(
                client.normalize_top_level_comment(thread)
                for thread in _bounded(
                    client.iter_comment_threads(video_id), comment_limit
                )
            )
            outcomes[video_id] = ("SUCCESS", None)
        except YouTubeAPIError as exc:
            if exc.category is YouTubeAPIErrorCategory.COMMENTS_DISABLED:
                LOGGER.info("Comentários desabilitados para o vídeo %s", video_id)
                outcomes[video_id] = ("SUCCESS", None)
            else:
                outcomes[video_id] = ("FAILED", str(exc))
    append_raw(spark, catalog, raw, table_schemas["api_responses"])
    merge_silver(
        spark, catalog, "comments", comments, table_schemas["comments"], "comment_id"
    )
    replace_ingestion_comments(spark, catalog, ingestion_id, comments)
    record_step_outcomes(spark, catalog, ingestion_id, "fetch_comments", outcomes)
    videos_succeeded = sum(status == "SUCCESS" for status, _ in outcomes.values())
    return _step_result(
        record_name="comments",
        records_fetched=len(comments),
        videos_attempted=len(video_ids),
        videos_succeeded=videos_succeeded,
        videos_failed=len(video_ids) - videos_succeeded,
        api_cost_units=client.api_cost_units,
    )


def _comments_for_ingestion(
    spark: Any, catalog: str, ingestion_id: str
) -> dict[str, list[str]]:
    outcomes = step_outcomes(spark, catalog, ingestion_id)
    eligible_video_ids = [
        video_id
        for video_id, steps in outcomes.items()
        if steps.get("fetch_comments", (None, None))[0] == "SUCCESS"
    ]
    return ingestion_comment_ids(spark, catalog, ingestion_id, eligible_video_ids)


def _channel_name(spark: Any, catalog: str, video_ids: list[str]) -> str | None:
    """Return the observed public channel names for the finalized targets."""
    if not video_ids:
        return None
    values = ", ".join(f"'{video_id}'" for video_id in video_ids)
    rows = spark.sql(
        f"""
        SELECT DISTINCT channel.title
        FROM {catalog}.silver.videos AS video
        INNER JOIN {catalog}.silver.channels AS channel
          ON video.channel_id = channel.channel_id
        WHERE video.video_id IN ({values})
          AND channel.title IS NOT NULL
        ORDER BY channel.title
        """
    ).collect()
    names = [row.title for row in rows]
    return ", ".join(names) or None


def fetch_replies_step(
    *,
    spark: Any,
    api_key: str,
    ingestion_id: str,
    max_replies_per_comment: str = "0",
    catalog: str = "youtube_lakehouse",
    api_cost_observer: Callable[[int], None] | None = None,
) -> dict[str, int | str]:
    """Fetch replies for comments of the current ingestion's successful videos."""
    reply_limit = _validate_limit(max_replies_per_comment, "max_replies_per_comment")
    comments_by_video = _comments_for_ingestion(spark, catalog, ingestion_id)
    if not comments_by_video:
        return _step_result(
            record_name="replies",
            records_fetched=0,
            videos_attempted=0,
            videos_succeeded=0,
            videos_failed=0,
        )
    raw, observe = _response_collector(ingestion_id)
    table_schemas = schemas()
    client = YouTubeClient(
        api_key=api_key,
        response_observer=observe,
        ingestion_id=ingestion_id,
        api_cost_observer=api_cost_observer,
    )
    replies: list[dict[str, Any]] = []
    outcomes: dict[str, tuple[str, str | None]] = {}
    for video_id, comment_ids in comments_by_video.items():
        try:
            for comment_id in comment_ids:
                replies.extend(
                    client.normalize_reply(reply, video_id=video_id)
                    for reply in _bounded(client.iter_replies(comment_id), reply_limit)
                )
            outcomes[video_id] = ("SUCCESS", None)
        except YouTubeAPIError as exc:
            outcomes[video_id] = ("FAILED", str(exc))
    append_raw(spark, catalog, raw, table_schemas["api_responses"])
    merge_silver(
        spark, catalog, "replies", replies, table_schemas["replies"], "comment_id"
    )
    record_step_outcomes(spark, catalog, ingestion_id, "fetch_replies", outcomes)
    videos_succeeded = sum(status == "SUCCESS" for status, _ in outcomes.values())
    return _step_result(
        record_name="replies",
        records_fetched=len(replies),
        videos_attempted=len(comments_by_video),
        videos_succeeded=videos_succeeded,
        videos_failed=len(comments_by_video) - videos_succeeded,
        api_cost_units=client.api_cost_units,
    )


def finalize_ingestion_step(
    *, spark: Any, ingestion_id: str, catalog: str = "youtube_lakehouse"
) -> dict[str, int | str]:
    """Close targets only after every required fetch step has succeeded."""
    targets = ingestion_targets(spark, catalog, ingestion_id)
    if not targets:
        return {
            "ingestion_id": ingestion_id,
            "targets": 0,
            "status": "SUCCESS",
            "videos_attempted": 0,
            "videos_succeeded": 0,
            "videos_failed": 0,
            "records_fetched": 0,
            "api_cost_units": 0,
        }
    outcomes_by_video = step_outcomes(spark, catalog, ingestion_id)
    # Replies has already finished (or been skipped) when the finalizer runs.
    # Do this before releasing targets so a cleanup failure remains retryable.
    clear_ingestion_comments(spark, catalog, ingestion_id)
    succeeded = 0
    for video_id, refresh_interval_hours in targets:
        steps = outcomes_by_video.get(video_id, {})
        video_status, video_error = steps.get("fetch_videos", ("FAILED", None))
        if video_status == "NOT_FOUND":
            status, error_message = "NOT_FOUND", video_error
        else:
            failed = [
                (step, error)
                for step in FETCH_STEPS
                if steps.get(step, ("FAILED", None))[0] != "SUCCESS"
                for error in [steps.get(step, (None, "etapa não concluída"))[1]]
            ]
            if failed:
                status = "FAILED"
                error_message = "; ".join(
                    f"{step}: {error or 'etapa não concluída'}"
                    for step, error in failed
                )
            else:
                status, error_message = "SUCCESS", None
                succeeded += 1
        finish_video_target(
            spark,
            catalog,
            video_id,
            ingestion_id,
            status,
            refresh_interval_hours,
            error_message,
        )
    failed = len(targets) - succeeded
    status = "SUCCESS" if failed == 0 else "PARTIAL_SUCCESS"
    finish_run(
        spark,
        catalog,
        ingestion_id,
        status,
        channel_name=_channel_name(
            spark, catalog, [video_id for video_id, _ in targets]
        ),
    )
    result = {
        "ingestion_id": ingestion_id,
        "targets": len(targets),
        "status": status,
        "videos_attempted": len(targets),
        "videos_succeeded": succeeded,
        "videos_failed": failed,
        "records_fetched": len(targets),
        "api_cost_units": 0,
    }
    LOGGER.info("Ingestão finalizada: %s", result)
    return result


def run_ingestion(
    *,
    spark: Any,
    api_key: str,
    batch_size: str = "20",
    max_comments_per_video: str = "0",
    max_replies_per_comment: str = "0",
    catalog: str = "youtube_lakehouse",
) -> dict[str, int | str]:
    """Run all Workflow steps sequentially for local execution and compatibility."""
    claimed = claim_targets_step(spark=spark, batch_size=batch_size, catalog=catalog)
    ingestion_id = str(claimed["ingestion_id"])
    if not claimed["targets"]:
        return {**claimed, "videos": 0, "channels": 0, "comments": 0, "replies": 0}
    try:
        videos = fetch_videos_step(
            spark=spark, api_key=api_key, ingestion_id=ingestion_id, catalog=catalog
        )
        channels = fetch_channels_step(
            spark=spark, api_key=api_key, ingestion_id=ingestion_id, catalog=catalog
        )
        comments = fetch_comments_step(
            spark=spark,
            api_key=api_key,
            ingestion_id=ingestion_id,
            max_comments_per_video=max_comments_per_video,
            catalog=catalog,
        )
        replies = fetch_replies_step(
            spark=spark,
            api_key=api_key,
            ingestion_id=ingestion_id,
            max_replies_per_comment=max_replies_per_comment,
            catalog=catalog,
        )
    except Exception:
        finalize_ingestion_step(spark=spark, ingestion_id=ingestion_id, catalog=catalog)
        raise
    return {
        **finalize_ingestion_step(
            spark=spark, ingestion_id=ingestion_id, catalog=catalog
        ),
        **videos,
        **channels,
        **comments,
        **replies,
    }
