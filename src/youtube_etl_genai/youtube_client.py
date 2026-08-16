from __future__ import annotations

from collections.abc import Callable, Iterator
from datetime import datetime, timedelta
from enum import StrEnum
import logging
import re
import time
from typing import Any

import requests

from youtube_etl_genai.observability import log_event


LOGGER = logging.getLogger(__name__)


class YouTubeAPIErrorCategory(StrEnum):
    """Classify failures so the pipeline can decide whether to retry them."""

    AUTHENTICATION = "AUTHENTICATION"
    COMMENTS_DISABLED = "COMMENTS_DISABLED"
    INVALID_REQUEST = "INVALID_REQUEST"
    NOT_FOUND = "NOT_FOUND"
    QUOTA_EXCEEDED = "QUOTA_EXCEEDED"
    RATE_LIMITED = "RATE_LIMITED"
    TRANSIENT_NETWORK = "TRANSIENT_NETWORK"
    TRANSIENT_SERVER = "TRANSIENT_SERVER"
    UNKNOWN = "UNKNOWN"


class YouTubeAPIError(RuntimeError):
    """Raised when a YouTube Data API request cannot be completed."""

    def __init__(
        self,
        category: YouTubeAPIErrorCategory,
        message: str,
        *,
        status_code: int | None = None,
        retryable: bool = False,
    ) -> None:
        self.category = category
        self.status_code = status_code
        self.retryable = retryable
        super().__init__(f"[{category}] {message}")


_TRANSIENT_STATUS_CODES = frozenset({429, 500, 502, 503, 504})
_RATE_LIMIT_REASONS = frozenset({"rateLimitExceeded", "userRateLimitExceeded"})
_QUOTA_REASONS = frozenset({"dailyLimitExceeded", "quotaExceeded"})
_API_COST_UNITS = {
    "videos": 1,
    "channels": 1,
    "commentThreads": 1,
    "comments": 1,
}


ResponseObserver = Callable[[str, dict[str, Any], dict[str, Any]], None]
ApiCostObserver = Callable[[int], None]

_YOUTUBE_DURATION = re.compile(
    r"^P(?:(?P<days>\d+(?:\.\d+)?)D)?(?:T(?:(?P<hours>\d+(?:\.\d+)?)H)?"
    r"(?:(?P<minutes>\d+(?:\.\d+)?)M)?(?:(?P<seconds>\d+(?:\.\d+)?)S)?)?$"
)


def _parse_rfc3339(value: object) -> datetime | None:
    """Convert an API RFC 3339 timestamp to a Spark-compatible datetime."""
    if not isinstance(value, str) or not value:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _parse_youtube_duration(value: object) -> timedelta | None:
    """Convert the YouTube ISO 8601 day-time duration to ``timedelta``."""
    if not isinstance(value, str) or not value:
        return None
    match = _YOUTUBE_DURATION.fullmatch(value)
    if not match:
        raise ValueError(f"Duração ISO 8601 inválida retornada pela API: {value!r}")
    return timedelta(
        days=float(match.group("days") or 0),
        hours=float(match.group("hours") or 0),
        minutes=float(match.group("minutes") or 0),
        seconds=float(match.group("seconds") or 0),
    )


def _optional_int(value: object) -> int | None:
    """Convert optional numeric fields returned as strings by the API."""
    if value is None or value == "":
        return None
    return int(value)


class YouTubeClient:
    """Small client for the YouTube Data API resources used by ingestion."""

    BASE_URL = "https://www.googleapis.com/youtube/v3"

    def __init__(
        self,
        api_key: str,
        timeout: int = 30,
        response_observer: ResponseObserver | None = None,
        ingestion_id: str | None = None,
        api_cost_observer: ApiCostObserver | None = None,
        max_attempts: int = 3,
        backoff_seconds: float = 1.0,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        """Create a client with bounded retry/backoff for transient failures."""
        if not api_key:
            raise ValueError("A API Key não foi informada.")
        if max_attempts < 1:
            raise ValueError("max_attempts deve ser maior ou igual a um.")
        if backoff_seconds < 0:
            raise ValueError("backoff_seconds não pode ser negativo.")

        self.api_key = api_key
        self.timeout = timeout
        self.response_observer = response_observer
        self.ingestion_id = ingestion_id
        self.api_cost_observer = api_cost_observer
        self.max_attempts = max_attempts
        self.backoff_seconds = backoff_seconds
        self.sleep = sleep
        self.session = requests.Session()
        self.api_cost_units = 0

    @staticmethod
    def _api_error_from_response(response: requests.Response) -> YouTubeAPIError:
        """Translate an API error response into a category and retry policy."""
        try:
            error_data = response.json()
        except ValueError:
            error_data = response.text

        error = error_data.get("error", {}) if isinstance(error_data, dict) else {}
        errors = error.get("errors", []) if isinstance(error, dict) else []
        reasons = {
            item.get("reason")
            for item in errors
            if isinstance(item, dict) and item.get("reason")
        }
        if isinstance(error, dict) and error.get("reason"):
            reasons.add(error["reason"])
        status_code = response.status_code

        if "commentsDisabled" in reasons:
            category = YouTubeAPIErrorCategory.COMMENTS_DISABLED
        elif reasons & _RATE_LIMIT_REASONS or status_code == 429:
            category = YouTubeAPIErrorCategory.RATE_LIMITED
        elif reasons & _QUOTA_REASONS:
            category = YouTubeAPIErrorCategory.QUOTA_EXCEEDED
        elif status_code in {401, 403}:
            category = YouTubeAPIErrorCategory.AUTHENTICATION
        elif status_code == 404:
            category = YouTubeAPIErrorCategory.NOT_FOUND
        elif status_code == 400:
            category = YouTubeAPIErrorCategory.INVALID_REQUEST
        elif status_code in _TRANSIENT_STATUS_CODES:
            category = YouTubeAPIErrorCategory.TRANSIENT_SERVER
        else:
            category = YouTubeAPIErrorCategory.UNKNOWN

        return YouTubeAPIError(
            category,
            f"Erro HTTP {status_code}: {error_data}",
            status_code=status_code,
            retryable=category
            in {
                YouTubeAPIErrorCategory.RATE_LIMITED,
                YouTubeAPIErrorCategory.TRANSIENT_SERVER,
            },
        )

    def _retry_delay(self, attempt: int) -> float:
        """Return exponential backoff before the next attempt (one-indexed)."""
        return self.backoff_seconds * (2 ** (attempt - 1))

    def _get(
        self,
        resource: str,
        params: dict[str, Any],
    ) -> dict[str, Any]:
        """Perform one authenticated GET request and observe its response."""
        url = f"{self.BASE_URL}/{resource}"

        request_params = {
            **params,
            "key": self.api_key,
        }
        cost_units = _API_COST_UNITS.get(resource, 1)

        for attempt in range(1, self.max_attempts + 1):
            cause: Exception | None = None
            try:
                self.api_cost_units += cost_units
                if self.api_cost_observer:
                    self.api_cost_observer(cost_units)
                log_event(
                    LOGGER,
                    "api_call",
                    ingestion_id=self.ingestion_id,
                    resource=resource,
                    attempt=attempt,
                    cost_units=cost_units,
                )
                response = self.session.get(
                    url,
                    params=request_params,
                    timeout=self.timeout,
                )
                response.raise_for_status()
            except requests.HTTPError as exc:
                cause = exc
                error = self._api_error_from_response(exc.response or response)
            except (requests.Timeout, requests.ConnectionError) as exc:
                cause = exc
                error = YouTubeAPIError(
                    YouTubeAPIErrorCategory.TRANSIENT_NETWORK,
                    f"Falha transitória ao acessar a API do YouTube: {exc}",
                    retryable=True,
                )
            except requests.RequestException as exc:
                raise YouTubeAPIError(
                    YouTubeAPIErrorCategory.UNKNOWN,
                    f"Falha ao acessar a API do YouTube: {exc}",
                ) from exc
            else:
                response_data = response.json()
                if self.response_observer:
                    self.response_observer(resource, params.copy(), response_data)
                return response_data

            if not error.retryable or attempt == self.max_attempts:
                if error.category in {
                    YouTubeAPIErrorCategory.QUOTA_EXCEEDED,
                    YouTubeAPIErrorCategory.RATE_LIMITED,
                }:
                    log_event(
                        LOGGER,
                        "api_limit_reached",
                        level=logging.WARNING,
                        ingestion_id=self.ingestion_id,
                        resource=resource,
                        category=error.category,
                        status_code=error.status_code,
                    )
                raise error from cause
            delay_seconds = self._retry_delay(attempt)
            log_event(
                LOGGER,
                "api_retry",
                level=logging.WARNING,
                ingestion_id=self.ingestion_id,
                resource=resource,
                attempt=attempt,
                delay_seconds=delay_seconds,
                category=error.category,
            )
            self.sleep(delay_seconds)

        raise AssertionError("Tentativas esgotadas sem retornar ou lançar um erro")

    def _paginate(
        self,
        resource: str,
        params: dict[str, Any],
    ) -> Iterator[dict[str, Any]]:
        """Yield items across all API pages for a resource."""
        page_token: str | None = None

        while True:
            request_params = params.copy()

            if page_token:
                request_params["pageToken"] = page_token

            response = self._get(resource, request_params)

            yield from response.get("items", [])

            page_token = response.get("nextPageToken")

            if not page_token:
                break

    def get_channel_by_id(
        self,
        channel_id: str,
    ) -> dict[str, Any] | None:
        """Return a channel by ID, or ``None`` when it does not exist."""
        response = self._get(
            resource="channels",
            params={
                "part": "snippet,statistics,contentDetails",
                "id": channel_id,
            },
        )

        items = response.get("items", [])
        return items[0] if items else None

    def get_channels(self, channel_ids: list[str]) -> list[dict[str, Any]]:
        """Fetch channel resources in API-compliant batches of at most 50 IDs."""
        if not channel_ids:
            return []

        channels: list[dict[str, Any]] = []
        for start in range(0, len(channel_ids), 50):
            batch = channel_ids[start : start + 50]
            response = self._get(
                resource="channels",
                params={
                    "part": "snippet,statistics,contentDetails",
                    "id": ",".join(batch),
                    "maxResults": 50,
                },
            )
            channels.extend(response.get("items", []))

        return channels

    def get_channel_by_handle(
        self,
        handle: str,
    ) -> dict[str, Any] | None:
        """Return a channel by handle, accepting an optional leading ``@``."""
        normalized_handle = handle.removeprefix("@")

        response = self._get(
            resource="channels",
            params={
                "part": "snippet,statistics,contentDetails",
                "forHandle": normalized_handle,
            },
        )

        items = response.get("items", [])
        return items[0] if items else None

    @staticmethod
    def normalize_channel(
        channel: dict[str, Any],
    ) -> dict[str, Any]:
        """Map a raw channel resource to the silver channel schema."""
        snippet = channel.get("snippet", {})
        statistics = channel.get("statistics", {})
        content_details = channel.get("contentDetails", {})

        uploads_playlist_id = content_details.get("relatedPlaylists", {}).get("uploads")

        return {
            "channel_id": channel.get("id"),
            "title": snippet.get("title"),
            "description": snippet.get("description"),
            "custom_url": snippet.get("customUrl"),
            "published_at": snippet.get("publishedAt"),
            "country": snippet.get("country"),
            "view_count": int(statistics.get("viewCount", 0)),
            "subscriber_count": (
                int(statistics["subscriberCount"])
                if "subscriberCount" in statistics
                else None
            ),
            "video_count": int(statistics.get("videoCount", 0)),
            "uploads_playlist_id": uploads_playlist_id,
        }

    def iter_uploads(
        self,
        uploads_playlist_id: str,
    ) -> Iterator[dict[str, Any]]:
        """Yield playlist items from a channel's uploads playlist."""
        yield from self._paginate(
            resource="playlistItems",
            params={
                "part": "snippet,contentDetails",
                "playlistId": uploads_playlist_id,
                "maxResults": 50,
            },
        )

    @staticmethod
    def normalize_playlist_item(
        item: dict[str, Any],
    ) -> dict[str, Any]:
        """Map an uploads playlist item to the fields needed for lookup."""
        snippet = item.get("snippet", {})
        content_details = item.get("contentDetails", {})

        return {
            "video_id": content_details.get("videoId"),
            "title": snippet.get("title"),
            "description": snippet.get("description"),
            "published_at": content_details.get("videoPublishedAt"),
            "playlist_position": snippet.get("position"),
        }

    def get_videos(
        self,
        video_ids: list[str],
    ) -> list[dict[str, Any]]:
        """Fetch video resources in API-compliant batches of at most 50 IDs."""
        if not video_ids:
            return []

        videos: list[dict[str, Any]] = []

        for start in range(0, len(video_ids), 50):
            batch = video_ids[start : start + 50]

            response = self._get(
                resource="videos",
                params={
                    "part": ("snippet,contentDetails,statistics,status"),
                    "id": ",".join(batch),
                    "maxResults": 50,
                },
            )

            videos.extend(response.get("items", []))

        return videos

    @staticmethod
    def normalize_video(
        video: dict[str, Any],
    ) -> dict[str, Any]:
        """Map a raw video resource to the silver video schema."""
        snippet = video.get("snippet", {})
        content_details = video.get("contentDetails", {})
        statistics = video.get("statistics", {})
        status = video.get("status", {})

        return {
            "video_id": video.get("id"),
            "channel_id": snippet.get("channelId"),
            "title": snippet.get("title"),
            "description": snippet.get("description"),
            "published_at": _parse_rfc3339(snippet.get("publishedAt")),
            "category_id": _optional_int(snippet.get("categoryId")),
            "tags": snippet.get("tags", []),
            "duration": _parse_youtube_duration(content_details.get("duration")),
            "definition": content_details.get("definition"),
            "caption": content_details.get("caption"),
            "view_count": int(statistics.get("viewCount", 0)),
            "like_count": (
                int(statistics["likeCount"]) if "likeCount" in statistics else None
            ),
            "comment_count": (
                int(statistics["commentCount"])
                if "commentCount" in statistics
                else None
            ),
            "privacy_status": status.get("privacyStatus"),
        }

    def iter_comment_threads(
        self,
        video_id: str,
        order: str = "time",
    ) -> Iterator[dict[str, Any]]:
        """Yield top-level comment threads for one video."""
        yield from self._paginate(
            resource="commentThreads",
            params={
                "part": "snippet,replies",
                "videoId": video_id,
                "maxResults": 100,
                "order": order,
                "textFormat": "plainText",
            },
        )

    @staticmethod
    def normalize_top_level_comment(
        thread: dict[str, Any],
    ) -> dict[str, Any]:
        """Map a comment thread to a normalized top-level comment."""
        thread_snippet = thread.get("snippet", {})
        top_level_comment = thread_snippet.get(
            "topLevelComment",
            {},
        )

        comment_id = top_level_comment.get("id")
        snippet = top_level_comment.get("snippet", {})

        return {
            "thread_id": thread.get("id"),
            "comment_id": comment_id,
            "parent_id": None,
            "video_id": snippet.get("videoId"),
            "author_name": snippet.get("authorDisplayName"),
            "author_channel_id": (snippet.get("authorChannelId", {}).get("value")),
            "text": snippet.get("textDisplay"),
            "like_count": snippet.get("likeCount", 0),
            "published_at": snippet.get("publishedAt"),
            "updated_at": snippet.get("updatedAt"),
            "reply_count": thread_snippet.get(
                "totalReplyCount",
                0,
            ),
        }

    def iter_replies(
        self,
        parent_comment_id: str,
    ) -> Iterator[dict[str, Any]]:
        """Yield replies for one top-level comment."""
        yield from self._paginate(
            resource="comments",
            params={
                "part": "snippet",
                "parentId": parent_comment_id,
                "maxResults": 100,
                "textFormat": "plainText",
            },
        )

    @staticmethod
    def normalize_reply(
        reply: dict[str, Any],
        *,
        video_id: str,
    ) -> dict[str, Any]:
        """Map a raw reply resource to the silver replies schema.

        The comments.list response for a reply contains its parent comment but
        not its video. The caller already knows the video being processed and
        must provide that context explicitly.
        """
        snippet = reply.get("snippet", {})

        return {
            "comment_id": reply.get("id"),
            "parent_id": snippet.get("parentId"),
            "video_id": video_id,
            "author_name": snippet.get("authorDisplayName"),
            "author_channel_id": (snippet.get("authorChannelId", {}).get("value")),
            "text": snippet.get("textDisplay"),
            "like_count": snippet.get("likeCount", 0),
            "published_at": snippet.get("publishedAt"),
            "updated_at": snippet.get("updatedAt"),
        }
