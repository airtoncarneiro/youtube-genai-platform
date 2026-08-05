from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from itertools import islice
from typing import Any, Iterable, Iterator
from uuid import uuid4

from youtube_etl_genai.persistence import (
    append_raw,
    append_run_start,
    claim_video_targets,
    finish_run,
    finish_video_target,
    merge_silver,
    merge_snapshots,
    schemas,
)
from youtube_etl_genai.youtube_client import YouTubeAPIError, YouTubeClient

LOGGER = logging.getLogger(__name__)


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
    """Project current entity records into their immutable metric snapshots."""
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
            **{metric: row.get(metric) for metric in metrics},
        }
        for row in rows
        if row.get(entity_key)
    ]


def run_ingestion(
    *,
    spark: Any,
    api_key: str,
    batch_size: str = "20",
    max_comments_per_video: str = "0",
    max_replies_per_comment: str = "0",
    catalog: str = "youtube_lakehouse",
) -> dict[str, int | str]:
    """Refresh a due batch from ``control.video_targets``.

    The target list, rather than a channel uploads playlist, is the source of
    truth. Every due target is fetched again: current tables receive the newest
    state while video and channel metric snapshots are immutable per run.
    A limit of zero means complete pagination for comments or replies.
    """
    requested_batch_size = _validate_limit(batch_size, "batch_size", allow_zero=False)
    comment_limit = _validate_limit(max_comments_per_video, "max_comments_per_video")
    reply_limit = _validate_limit(max_replies_per_comment, "max_replies_per_comment")
    table_schemas = schemas()
    ingestion_id = str(uuid4())
    collected_at = datetime.now(timezone.utc)
    raw_responses: list[dict[str, Any]] = []
    claimed_targets: list[tuple[str, int]] = []
    raw_written = False

    def capture_response(
        resource: str, params: dict[str, Any], response: dict[str, Any]
    ) -> None:
        """Record a successful response without retaining the API key."""
        raw_responses.append(
            {
                "ingestion_id": ingestion_id,
                "resource": resource,
                "request_params_json": json.dumps(params, sort_keys=True),
                "response_json": json.dumps(response, sort_keys=True),
                "received_at": datetime.now(timezone.utc),
            }
        )

    append_run_start(spark, catalog, ingestion_id, "control.video_targets")
    try:
        claimed_targets = claim_video_targets(
            spark, catalog, ingestion_id, requested_batch_size
        )
        if not claimed_targets:
            finish_run(spark, catalog, ingestion_id, "SUCCESS")
            return {
                "ingestion_id": ingestion_id,
                "targets": 0,
                "videos": 0,
                "channels": 0,
                "comments": 0,
                "replies": 0,
            }

        client = YouTubeClient(api_key=api_key, response_observer=capture_response)
        target_ids = [video_id for video_id, _ in claimed_targets]
        videos = [
            client.normalize_video(video) for video in client.get_videos(target_ids)
        ]
        found_video_ids = {
            video["video_id"] for video in videos if video.get("video_id")
        }
        outcomes: dict[str, tuple[str, str | None]] = {
            video_id: ("NOT_FOUND", "Vídeo não encontrado ou não está acessível")
            for video_id in target_ids
            if video_id not in found_video_ids
        }

        channel_ids = sorted(
            {video["channel_id"] for video in videos if video.get("channel_id")}
        )
        channels = [
            client.normalize_channel(channel)
            for channel in client.get_channels(channel_ids)
        ]
        comments: list[dict[str, Any]] = []
        replies: list[dict[str, Any]] = []

        for video in videos:
            video_id = video["video_id"]
            try:
                for thread in _bounded(
                    client.iter_comment_threads(video_id), comment_limit
                ):
                    comment = client.normalize_top_level_comment(thread)
                    comments.append(comment)
                    for reply_raw in _bounded(
                        client.iter_replies(comment["comment_id"]), reply_limit
                    ):
                        replies.append(client.normalize_reply(reply_raw))
            except YouTubeAPIError as exc:
                if "commentsDisabled" in str(exc):
                    LOGGER.info("Comentários desabilitados para o vídeo %s", video_id)
                else:
                    LOGGER.exception(
                        "Falha ao atualizar comentários do vídeo %s", video_id
                    )
                    outcomes[video_id] = ("FAILED", str(exc))
                    continue
            outcomes[video_id] = ("SUCCESS", None)

        # Raw is persisted before normalized tables so it remains available
        # for audit and reprocessing if a later Delta write fails.
        append_raw(spark, catalog, raw_responses, table_schemas["api_responses"])
        raw_written = True
        merge_silver(
            spark,
            catalog,
            "channels",
            channels,
            table_schemas["channels"],
            "channel_id",
        )
        merge_silver(
            spark, catalog, "videos", videos, table_schemas["videos"], "video_id"
        )
        merge_silver(
            spark,
            catalog,
            "comments",
            comments,
            table_schemas["comments"],
            "comment_id",
        )
        merge_silver(
            spark, catalog, "replies", replies, table_schemas["replies"], "comment_id"
        )
        merge_snapshots(
            spark,
            catalog,
            "channel_snapshots",
            _snapshot_rows("channels", channels, ingestion_id, collected_at),
            table_schemas["channel_snapshots"],
            "channel_id",
        )
        merge_snapshots(
            spark,
            catalog,
            "video_snapshots",
            _snapshot_rows("videos", videos, ingestion_id, collected_at),
            table_schemas["video_snapshots"],
            "video_id",
        )

        for video_id, refresh_interval_hours in claimed_targets:
            status, error_message = outcomes.get(
                video_id, ("FAILED", "Vídeo não retornou um resultado de processamento")
            )
            finish_video_target(
                spark,
                catalog,
                video_id,
                ingestion_id,
                status,
                refresh_interval_hours,
                error_message,
            )

        succeeded = sum(status == "SUCCESS" for status, _ in outcomes.values())
        failed = len(claimed_targets) - succeeded
        run_status = "SUCCESS" if failed == 0 else "PARTIAL_SUCCESS"
        finish_run(spark, catalog, ingestion_id, run_status)
    except Exception as exc:
        if raw_responses and not raw_written:
            append_raw(spark, catalog, raw_responses, table_schemas["api_responses"])
        for video_id, refresh_interval_hours in claimed_targets:
            finish_video_target(
                spark,
                catalog,
                video_id,
                ingestion_id,
                "FAILED",
                refresh_interval_hours,
                str(exc),
            )
        finish_run(spark, catalog, ingestion_id, "FAILED", str(exc))
        raise

    result = {
        "ingestion_id": ingestion_id,
        "targets": len(claimed_targets),
        "videos": len(videos),
        "channels": len(channels),
        "comments": len(comments),
        "replies": len(replies),
    }
    LOGGER.info("Ingestão concluída: %s", result)
    return result
