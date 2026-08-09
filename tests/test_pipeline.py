from __future__ import annotations

from datetime import datetime, timezone

import pytest

from youtube_etl_genai.persistence import append_raw, merge_silver, schemas
from youtube_etl_genai.pipeline import _bounded, _snapshot_rows, _validate_limit


@pytest.mark.parametrize("value, expected", [("0", 0), ("15", 15)])
def test_validate_limit_accepts_non_negative_values(value: str, expected: int) -> None:
    assert _validate_limit(value, "limit") == expected


@pytest.mark.parametrize("value", ["-1", "invalid"])
def test_validate_limit_rejects_invalid_values(value: str) -> None:
    with pytest.raises(ValueError):
        _validate_limit(value, "limit")


def test_validate_limit_can_require_positive_value() -> None:
    with pytest.raises(ValueError, match="maior que zero"):
        _validate_limit("0", "max_videos", allow_zero=False)


def test_append_raw_skips_empty_batches() -> None:
    append_raw(object(), "catalog", [], object())


def test_merge_silver_skips_empty_batches() -> None:
    merge_silver(object(), "catalog", "videos", [], object(), "video_id")


def test_schemas_expose_the_optimized_video_and_snapshot_types() -> None:
    table_schemas = schemas()

    assert table_schemas["videos"].simpleString() == (
        "struct<video_id:string,channel_id:string,title:string,description:string,"
        "published_at:timestamp,category_id:int,duration:interval day to second,"
        "definition:string,caption:string,view_count:bigint,like_count:bigint,"
        "comment_count:bigint,privacy_status:string>"
    )
    assert "collected_date:date" in table_schemas["video_snapshots"].simpleString()


def test_zero_limit_keeps_the_complete_paginated_collection() -> None:
    assert list(_bounded(iter([{"id": "one"}, {"id": "two"}]), 0)) == [
        {"id": "one"},
        {"id": "two"},
    ]


def test_positive_limit_bounds_the_paginated_collection() -> None:
    assert list(_bounded(iter([{"id": "one"}, {"id": "two"}]), 1)) == [{"id": "one"}]


def test_projects_only_video_metrics_into_an_immutable_snapshot() -> None:
    collected_at = datetime(2026, 8, 8, 10, 30, tzinfo=timezone.utc)

    snapshots = _snapshot_rows(
        "videos",
        [
            {
                "video_id": "video-1",
                "title": "A title that must remain in the current table",
                "view_count": 10,
                "like_count": 2,
                "comment_count": 1,
            }
        ],
        "run-1",
        collected_at,
    )

    assert snapshots == [
        {
            "video_id": "video-1",
            "ingestion_id": "run-1",
            "collected_at": collected_at,
            "collected_date": collected_at.date(),
            "view_count": 10,
            "like_count": 2,
            "comment_count": 1,
        }
    ]


def test_fetch_videos_persists_raw_current_snapshot_and_outcome(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from youtube_etl_genai import pipeline

    events: list[tuple[str, object]] = []

    class FakeClient:
        def __init__(
            self,
            api_key: str,
            response_observer: object,
            ingestion_id: str,
            api_cost_observer: object,
        ) -> None:
            assert api_key == "api-key"
            assert ingestion_id == "run-1"
            assert api_cost_observer is None
            self.response_observer = response_observer
            self.api_cost_units = 1

        def get_videos(self, video_ids: list[str]) -> list[dict[str, object]]:
            assert video_ids == ["video-1", "missing"]
            self.response_observer("videos", {"id": "video-1"}, {"items": []})
            return [{"video_id": "video-1", "view_count": 10}]

        @staticmethod
        def normalize_video(video: dict[str, object]) -> dict[str, object]:
            return video

    monkeypatch.setattr(
        pipeline, "ingestion_targets", lambda *_: [("video-1", 24), ("missing", 24)]
    )
    monkeypatch.setattr(
        pipeline,
        "schemas",
        lambda: {
            name: object() for name in ("api_responses", "videos", "video_snapshots")
        },
    )
    monkeypatch.setattr(pipeline, "YouTubeClient", FakeClient)
    monkeypatch.setattr(
        pipeline, "append_raw", lambda *args: events.append(("raw", args[2]))
    )
    monkeypatch.setattr(
        pipeline, "merge_silver", lambda *args: events.append(("silver", args[3]))
    )
    monkeypatch.setattr(
        pipeline,
        "replace_video_tags",
        lambda *args: events.append(("tags", args[2])),
    )
    monkeypatch.setattr(
        pipeline, "merge_snapshots", lambda *args: events.append(("snapshot", args[3]))
    )
    monkeypatch.setattr(
        pipeline,
        "record_step_outcomes",
        lambda *args: events.append(("outcomes", args[4])),
    )

    assert pipeline.fetch_videos_step(
        spark=object(), api_key="api-key", ingestion_id="run-1"
    ) == {
        "status": "PARTIAL_SUCCESS",
        "videos": 1,
        "videos_attempted": 2,
        "videos_succeeded": 1,
        "videos_failed": 1,
        "records_fetched": 1,
        "api_cost_units": 1,
    }
    assert [event[0] for event in events] == [
        "raw",
        "silver",
        "tags",
        "snapshot",
        "outcomes",
    ]
    assert events[-1][1] == {
        "video-1": ("SUCCESS", None),
        "missing": ("NOT_FOUND", "Vídeo não encontrado ou não está acessível"),
    }


def test_finalize_marks_target_failed_when_a_step_is_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from youtube_etl_genai import pipeline

    completed: list[tuple[str, str, str | None]] = []
    monkeypatch.setattr(pipeline, "ingestion_targets", lambda *_: [("video-1", 24)])
    monkeypatch.setattr(
        pipeline,
        "step_outcomes",
        lambda *_: {"video-1": {"fetch_videos": ("SUCCESS", None)}},
    )
    monkeypatch.setattr(
        pipeline,
        "finish_video_target",
        lambda _, __, video_id, ___, status, ____, error: completed.append(
            (video_id, status, error)
        ),
    )
    cleanup_calls: list[str] = []
    monkeypatch.setattr(
        pipeline,
        "clear_ingestion_comments",
        lambda _, __, ingestion_id: cleanup_calls.append(ingestion_id),
    )
    run_status: list[str] = []
    monkeypatch.setattr(pipeline, "_channel_name", lambda *_: "Canal de teste")
    monkeypatch.setattr(
        pipeline,
        "finish_run",
        lambda spark, catalog, ingestion_id, status, **kwargs: run_status.append(
            status
        ),
    )

    result = pipeline.finalize_ingestion_step(spark=object(), ingestion_id="run-1")

    assert result["status"] == "PARTIAL_SUCCESS"
    assert completed[0][0:2] == ("video-1", "FAILED")
    assert "fetch_channels" in (completed[0][2] or "")
    assert run_status == ["PARTIAL_SUCCESS"]
    assert cleanup_calls == ["run-1"]


def test_run_ingestion_calls_every_step_in_workflow_order(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from youtube_etl_genai import pipeline

    calls: list[str] = []
    monkeypatch.setattr(
        pipeline,
        "claim_targets_step",
        lambda **_: calls.append("claim") or {"ingestion_id": "run-1", "targets": 1},
    )
    for name, key in (
        ("fetch_videos_step", "videos"),
        ("fetch_channels_step", "channels"),
        ("fetch_comments_step", "comments"),
        ("fetch_replies_step", "replies"),
    ):
        monkeypatch.setattr(
            pipeline,
            name,
            lambda _name=name, _key=key, **_: calls.append(_name) or {_key: 1},
        )
    monkeypatch.setattr(
        pipeline,
        "finalize_ingestion_step",
        lambda **_: calls.append("finalize")
        or {"ingestion_id": "run-1", "targets": 1, "status": "SUCCESS"},
    )

    result = pipeline.run_ingestion(spark=object(), api_key="api-key")

    assert calls == [
        "claim",
        "fetch_videos_step",
        "fetch_channels_step",
        "fetch_comments_step",
        "fetch_replies_step",
        "finalize",
    ]
    assert (
        result["videos"]
        == result["channels"]
        == result["comments"]
        == result["replies"]
        == 1
    )
