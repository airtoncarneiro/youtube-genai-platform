from __future__ import annotations

from collections.abc import Callable, Iterator
from typing import Any

import requests


class YouTubeAPIError(RuntimeError):
    """Raised when a YouTube Data API request cannot be completed."""

    pass


ResponseObserver = Callable[[str, dict[str, Any], dict[str, Any]], None]


class YouTubeClient:
    """Small client for the YouTube Data API resources used by ingestion."""

    BASE_URL = "https://www.googleapis.com/youtube/v3"

    def __init__(
        self,
        api_key: str,
        timeout: int = 30,
        response_observer: ResponseObserver | None = None,
    ) -> None:
        """Create a client with an API key and optional response observer."""
        if not api_key:
            raise ValueError("A API Key não foi informada.")

        self.api_key = api_key
        self.timeout = timeout
        self.response_observer = response_observer
        self.session = requests.Session()

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

        try:
            response = self.session.get(
                url,
                params=request_params,
                timeout=self.timeout,
            )
            response.raise_for_status()
        except requests.HTTPError as exc:
            try:
                error_data = response.json()
            except ValueError:
                error_data = response.text

            raise YouTubeAPIError(
                f"Erro HTTP {response.status_code}: {error_data}"
            ) from exc
        except requests.RequestException as exc:
            raise YouTubeAPIError(f"Falha ao acessar a API do YouTube: {exc}") from exc

        response_data = response.json()

        if self.response_observer:
            self.response_observer(resource, params.copy(), response_data)

        return response_data

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
            "channel_title": snippet.get("channelTitle"),
            "title": snippet.get("title"),
            "description": snippet.get("description"),
            "published_at": snippet.get("publishedAt"),
            "category_id": snippet.get("categoryId"),
            "tags": snippet.get("tags", []),
            "duration": content_details.get("duration"),
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
    ) -> dict[str, Any]:
        """Map a raw reply resource to the silver replies schema."""
        snippet = reply.get("snippet", {})

        return {
            "comment_id": reply.get("id"),
            "parent_id": snippet.get("parentId"),
            "video_id": snippet.get("videoId"),
            "author_name": snippet.get("authorDisplayName"),
            "author_channel_id": (snippet.get("authorChannelId", {}).get("value")),
            "text": snippet.get("textDisplay"),
            "like_count": snippet.get("likeCount", 0),
            "published_at": snippet.get("publishedAt"),
            "updated_at": snippet.get("updatedAt"),
        }
