from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import Mock

import pytest
import requests

from youtube_etl_genai.youtube_client import (
    YouTubeAPIError,
    YouTubeAPIErrorCategory,
    YouTubeClient,
)


def test_requires_an_api_key() -> None:
    with pytest.raises(ValueError, match="API Key"):
        YouTubeClient(api_key="")


def test_normalizes_channel_handle_before_request() -> None:
    client = YouTubeClient(api_key="test-key")
    client._get = Mock(return_value={"items": []})

    assert client.get_channel_by_handle("@example") is None
    client._get.assert_called_once_with(
        resource="channels",
        params={
            "part": "snippet,statistics,contentDetails",
            "forHandle": "example",
        },
    )


def test_get_channels_sends_ids_in_a_single_batched_request() -> None:
    client = YouTubeClient(api_key="test-key")
    client._get = Mock(return_value={"items": [{"id": "channel-1"}]})

    assert client.get_channels(["channel-1", "channel-2"]) == [{"id": "channel-1"}]
    client._get.assert_called_once_with(
        resource="channels",
        params={
            "part": "snippet,statistics,contentDetails",
            "id": "channel-1,channel-2",
            "maxResults": 50,
        },
    )


def test_paginates_using_the_next_page_token() -> None:
    client = YouTubeClient(api_key="test-key")
    client._get = Mock(
        side_effect=[
            {"items": [{"id": "first"}], "nextPageToken": "second-page"},
            {"items": [{"id": "second"}]},
        ]
    )

    assert list(client._paginate("channels", {"part": "snippet"})) == [
        {"id": "first"},
        {"id": "second"},
    ]
    assert client._get.call_args_list[1].args == (
        "channels",
        {"part": "snippet", "pageToken": "second-page"},
    )


def test_observes_successful_response_without_api_key() -> None:
    observed: list[tuple[str, dict[str, object], dict[str, object]]] = []
    client = YouTubeClient(
        api_key="test-key",
        response_observer=lambda resource, params, response: observed.append(
            (resource, params, response)
        ),
    )
    response = Mock()
    response.json.return_value = {"items": [{"id": "channel-id"}]}
    client.session.get = Mock(return_value=response)

    assert client.get_channel_by_handle("@example") == {"id": "channel-id"}
    assert observed == [
        (
            "channels",
            {
                "part": "snippet,statistics,contentDetails",
                "forHandle": "example",
            },
            {"items": [{"id": "channel-id"}]},
        )
    ]


def test_wraps_timeout_as_youtube_api_error() -> None:
    client = YouTubeClient(api_key="test-key", sleep=lambda _: None)
    client.session.get = Mock(side_effect=requests.Timeout("timed out"))

    with pytest.raises(YouTubeAPIError, match="TRANSIENT_NETWORK") as raised:
        client.get_channel_by_handle("@example")

    assert raised.value.retryable is True
    assert raised.value.category is YouTubeAPIErrorCategory.TRANSIENT_NETWORK


def test_wraps_http_error_with_response_details() -> None:
    client = YouTubeClient(api_key="test-key")
    response = Mock(status_code=403, text="forbidden")
    response.raise_for_status.side_effect = requests.HTTPError("forbidden")
    response.json.return_value = {"error": {"reason": "commentsDisabled"}}
    client.session.get = Mock(return_value=response)

    with pytest.raises(YouTubeAPIError, match="COMMENTS_DISABLED") as raised:
        client.get_channel_by_handle("@example")

    assert raised.value.retryable is False
    assert raised.value.status_code == 403
    assert raised.value.category is YouTubeAPIErrorCategory.COMMENTS_DISABLED


def test_retries_transient_timeout_with_exponential_backoff() -> None:
    delays: list[float] = []
    cost_events: list[int] = []
    client = YouTubeClient(
        api_key="test-key",
        max_attempts=3,
        backoff_seconds=0.25,
        sleep=delays.append,
        api_cost_observer=cost_events.append,
    )
    response = Mock()
    response.json.return_value = {"items": [{"id": "channel-id"}]}
    client.session.get = Mock(
        side_effect=[requests.Timeout("first"), requests.Timeout("second"), response]
    )

    assert client.get_channel_by_handle("@example") == {"id": "channel-id"}
    assert delays == [0.25, 0.5]
    assert client.session.get.call_count == 3
    assert client.api_cost_units == 3
    assert cost_events == [1, 1, 1]


def test_retries_rate_limit_but_not_quota_exhaustion() -> None:
    delays: list[float] = []
    client = YouTubeClient(api_key="test-key", sleep=delays.append)
    rate_limited = Mock(status_code=429, text="too many requests")
    rate_limited.raise_for_status.side_effect = requests.HTTPError(
        response=rate_limited
    )
    rate_limited.json.return_value = {
        "error": {"errors": [{"reason": "rateLimitExceeded"}]}
    }
    success = Mock()
    success.json.return_value = {"items": []}
    client.session.get = Mock(side_effect=[rate_limited, success])

    assert client.get_channel_by_handle("@example") is None
    assert delays == [1.0]

    quota_client = YouTubeClient(api_key="test-key", sleep=delays.append)
    quota_exceeded = Mock(status_code=403, text="quota exhausted")
    quota_exceeded.raise_for_status.side_effect = requests.HTTPError(
        response=quota_exceeded
    )
    quota_exceeded.json.return_value = {
        "error": {"errors": [{"reason": "quotaExceeded"}]}
    }
    quota_client.session.get = Mock(return_value=quota_exceeded)

    with pytest.raises(YouTubeAPIError, match="QUOTA_EXCEEDED") as raised:
        quota_client.get_channel_by_handle("@example")

    assert raised.value.retryable is False
    assert quota_client.session.get.call_count == 1


def test_get_videos_sends_at_most_fifty_ids_per_request() -> None:
    client = YouTubeClient(api_key="test-key")
    client._get = Mock(return_value={"items": []})

    client.get_videos([str(number) for number in range(51)])

    assert client._get.call_count == 2
    assert len(client._get.call_args_list[0].kwargs["params"]["id"].split(",")) == 50
    assert client._get.call_args_list[1].kwargs["params"]["id"] == "50"


def test_normalizes_comment_with_nullable_parent_id() -> None:
    normalized = YouTubeClient.normalize_top_level_comment(
        {"id": "thread", "snippet": {"topLevelComment": {"id": "comment"}}}
    )

    assert normalized["comment_id"] == "comment"
    assert normalized["parent_id"] is None


def test_normalizes_reply_with_video_id_from_collection_context() -> None:
    normalized = YouTubeClient.normalize_reply(
        {
            "id": "reply-1",
            "snippet": {
                "parentId": "comment-1",
                "videoId": "não-confiar-no-campo-ausente-ou-incompleto",
            },
        },
        video_id="video-1",
    )

    assert normalized["comment_id"] == "reply-1"
    assert normalized["parent_id"] == "comment-1"
    assert normalized["video_id"] == "video-1"


def test_normalizes_video_to_analytical_types_without_channel_title() -> None:
    normalized = YouTubeClient.normalize_video(
        {
            "id": "video-1",
            "snippet": {
                "channelId": "channel-1",
                "channelTitle": "Título que pertence à dimensão de canais",
                "publishedAt": "2026-08-08T12:30:45Z",
                "categoryId": "22",
                "tags": ["dados", "youtube"],
            },
            "contentDetails": {"duration": "PT1H2M3.5S"},
        }
    )

    assert normalized["published_at"] == datetime(
        2026, 8, 8, 12, 30, 45, tzinfo=timezone.utc
    )
    assert normalized["duration"] == timedelta(hours=1, minutes=2, seconds=3.5)
    assert normalized["category_id"] == 22
    assert normalized["tags"] == ["dados", "youtube"]
    assert "channel_title" not in normalized
