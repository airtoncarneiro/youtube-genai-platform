from __future__ import annotations

import pytest

from youtube_etl_genai.persistence import append_raw, merge_silver
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


def test_zero_limit_keeps_the_complete_paginated_collection() -> None:
    assert list(_bounded(iter([{"id": "one"}, {"id": "two"}]), 0)) == [
        {"id": "one"},
        {"id": "two"},
    ]


def test_positive_limit_bounds_the_paginated_collection() -> None:
    assert list(_bounded(iter([{"id": "one"}, {"id": "two"}]), 1)) == [{"id": "one"}]


def test_projects_only_video_metrics_into_an_immutable_snapshot() -> None:
    collected_at = object()

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
            "view_count": 10,
            "like_count": 2,
            "comment_count": 1,
        }
    ]


def test_run_ingestion_orchestrates_a_complete_video_refresh(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A due target must reach raw, silver, snapshots and control completion."""
    from youtube_etl_genai import pipeline

    events: list[tuple[str, object]] = []

    class FakeClient:
        def __init__(self, api_key: str, response_observer: object) -> None:
            assert api_key == "api-key"
            self.response_observer = response_observer

        def get_videos(self, video_ids: list[str]) -> list[dict[str, object]]:
            assert video_ids == ["video-1"]
            self.response_observer("videos", {"id": "video-1"}, {"items": ["raw"]})
            return [{"video_id": "video-1", "channel_id": "channel-1"}]

        @staticmethod
        def normalize_video(video: dict[str, object]) -> dict[str, object]:
            return video

        def get_channels(self, channel_ids: list[str]) -> list[dict[str, object]]:
            assert channel_ids == ["channel-1"]
            return [
                {
                    "channel_id": "channel-1",
                    "title": "Canal de teste",
                    "view_count": 4,
                }
            ]

        @staticmethod
        def normalize_channel(channel: dict[str, object]) -> dict[str, object]:
            return channel

        @staticmethod
        def iter_comment_threads(video_id: str) -> object:
            assert video_id == "video-1"
            return iter([{"comment_id": "comment-1"}])

        @staticmethod
        def normalize_top_level_comment(
            thread: dict[str, object],
        ) -> dict[str, object]:
            return {
                "comment_id": thread["comment_id"],
                "video_id": "video-1",
                "like_count": 2,
                "reply_count": 1,
            }

        @staticmethod
        def iter_replies(comment_id: str) -> object:
            assert comment_id == "comment-1"
            return iter([{"comment_id": "reply-1"}])

        @staticmethod
        def normalize_reply(reply: dict[str, object]) -> dict[str, object]:
            return {
                "comment_id": reply["comment_id"],
                "parent_id": "comment-1",
                "video_id": "video-1",
                "like_count": 1,
            }

    monkeypatch.setattr(
        pipeline,
        "schemas",
        lambda: {
            name: object()
            for name in (
                "api_responses",
                "channels",
                "videos",
                "comments",
                "replies",
                "channel_snapshots",
                "video_snapshots",
            )
        },
    )
    monkeypatch.setattr(pipeline, "YouTubeClient", FakeClient)
    monkeypatch.setattr(
        pipeline,
        "append_run_start",
        lambda *_: events.append(("run_start", None)),
    )
    monkeypatch.setattr(
        pipeline,
        "claim_video_targets",
        lambda *_: [("video-1", 24)],
    )
    monkeypatch.setattr(
        pipeline,
        "append_raw",
        lambda _, __, rows, ___: events.append(("raw", rows)),
    )
    monkeypatch.setattr(
        pipeline,
        "merge_silver",
        lambda _, __, table, rows, ___, ____: events.append((f"silver:{table}", rows)),
    )
    monkeypatch.setattr(
        pipeline,
        "merge_snapshots",
        lambda _, __, table, rows, ___, ____: events.append(
            (f"snapshot:{table}", rows)
        ),
    )
    monkeypatch.setattr(
        pipeline,
        "finish_video_target",
        lambda _, __, video_id, ___, status, ____, error: events.append(
            (f"target:{video_id}", (status, error))
        ),
    )
    monkeypatch.setattr(
        pipeline,
        "finish_run",
        lambda _, __, ___, status, error_message=None, channel_name=None: events.append(
            ("run_finish", (status, error_message, channel_name))
        ),
    )

    result = pipeline.run_ingestion(spark=object(), api_key="api-key")

    assert result["targets"] == 1
    assert result["videos"] == result["channels"] == result["comments"] == 1
    assert result["replies"] == 1
    assert [event[0] for event in events] == [
        "run_start",
        "raw",
        "silver:channels",
        "silver:videos",
        "silver:comments",
        "silver:replies",
        "snapshot:channel_snapshots",
        "snapshot:video_snapshots",
        "target:video-1",
        "run_finish",
    ]
    assert events[-2] == ("target:video-1", ("SUCCESS", None))
    assert events[-1] == ("run_finish", ("SUCCESS", None, "Canal de teste"))


def test_run_ingestion_finishes_when_no_target_is_due(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An empty schedule is a successful execution and must not call the API."""
    from youtube_etl_genai import pipeline

    completed: list[str] = []
    monkeypatch.setattr(pipeline, "schemas", lambda: {})
    monkeypatch.setattr(pipeline, "append_run_start", lambda *_: None)
    monkeypatch.setattr(pipeline, "claim_video_targets", lambda *_: [])
    monkeypatch.setattr(
        pipeline,
        "finish_run",
        lambda _, __, ___, status, *args: completed.append(status),
    )
    monkeypatch.setattr(
        pipeline,
        "YouTubeClient",
        lambda *_args, **_kwargs: pytest.fail("A API não deveria ser chamada"),
    )

    result = pipeline.run_ingestion(spark=object(), api_key="api-key")

    assert result["targets"] == 0
    assert completed == ["SUCCESS"]
