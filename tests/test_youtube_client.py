from __future__ import annotations

from unittest.mock import Mock

import pytest
import requests

from youtube_etl_genai.youtube_client import YouTubeAPIError, YouTubeClient


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
    client = YouTubeClient(api_key="test-key")
    client.session.get = Mock(side_effect=requests.Timeout("timed out"))

    with pytest.raises(YouTubeAPIError, match="Falha ao acessar"):
        client.get_channel_by_handle("@example")


def test_wraps_http_error_with_response_details() -> None:
    client = YouTubeClient(api_key="test-key")
    response = Mock(status_code=403, text="forbidden")
    response.raise_for_status.side_effect = requests.HTTPError("forbidden")
    response.json.return_value = {"error": {"reason": "commentsDisabled"}}
    client.session.get = Mock(return_value=response)

    with pytest.raises(YouTubeAPIError, match="Erro HTTP 403"):
        client.get_channel_by_handle("@example")


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
