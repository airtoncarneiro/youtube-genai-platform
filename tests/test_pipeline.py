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
