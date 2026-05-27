from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

from pipeline.quest_music_lookup import lookup_recording_artists


def test_lookup_uses_cache(tmp_path: Path) -> None:
    cache = tmp_path / "cache.json"
    cache.write_text(
        json.dumps({"entries": {"cochise": {"title": "Cochise", "artists": ["Audioslave"], "confidence": 0.72, "source": "musicbrainz"}}}),
        encoding="utf-8",
    )
    result = lookup_recording_artists("Cochise", cache_path=cache)
    assert result["artists"] == ["Audioslave"]
    assert result["source"] == "musicbrainz"


def test_lookup_skips_untitled() -> None:
    result = lookup_recording_artists("Oyuun quest (untitled)")
    assert result["artists"] == []
    assert result["source"] == "skipped"


def test_lookup_network_mock(tmp_path: Path) -> None:
    payload = json.dumps({"recordings": [{"artist-credit": [{"artist": {"name": "Guns N' Roses"}}], "id": "x"}]}).encode("utf-8")

    class FakeResp:
        def read(self):
            return payload

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

    cache = tmp_path / "cache.json"
    with patch("pipeline.quest_music_lookup.urllib.request.urlopen", return_value=FakeResp()):
        result = lookup_recording_artists("Sweet Child O' Mine", cache_path=cache)
    assert "Guns N' Roses" in result["artists"]
