from __future__ import annotations

from youtube_etl_genai.main import _get_api_key


def test_prefers_environment_api_key(monkeypatch: object) -> None:
    monkeypatch.setenv("YOUTUBE_API_KEY", "environment-key")

    assert _get_api_key(object(), "scope", "key") == "environment-key"
