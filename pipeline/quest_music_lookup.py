"""MusicBrainz recording lookup for unknown quest song titles (Stage 08Q)."""

from __future__ import annotations

import json
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any

from pipeline.common import read_json, write_json

DEFAULT_CACHE_PATH = Path("artifacts/learning/quest_music_lookup_cache.json")
MUSICBRAINZ_API = "https://musicbrainz.org/ws/2/recording"
_LAST_REQUEST_AT = 0.0
_MIN_INTERVAL_S = 1.0


def _cache_load(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"entries": {}}
    try:
        payload = read_json(path)
        if isinstance(payload, dict):
            return payload
    except Exception:
        pass
    return {"entries": {}}


def _cache_save(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    write_json(path, payload)


def _rate_limit() -> None:
    global _LAST_REQUEST_AT
    elapsed = time.time() - _LAST_REQUEST_AT
    if elapsed < _MIN_INTERVAL_S:
        time.sleep(_MIN_INTERVAL_S - elapsed)
    _LAST_REQUEST_AT = time.time()


def lookup_recording_artists(
    title: str,
    *,
    user_agent: str = "TheriacLorePipeline/1.0 (local research)",
    cache_path: Path | None = None,
    timeout_seconds: float = 15.0,
) -> dict[str, Any]:
    """Return {title, artists[], confidence, source} for a song title."""
    clean = str(title or "").strip()
    if not clean or "untitled" in clean.lower():
        return {"title": clean, "artists": [], "confidence": 0.0, "source": "skipped"}

    cache_file = cache_path or DEFAULT_CACHE_PATH
    cache = _cache_load(cache_file)
    entries = cache.setdefault("entries", {})
    cache_key = clean.lower()
    if cache_key in entries:
        cached = entries[cache_key]
        if isinstance(cached, dict):
            return cached

    query = urllib.parse.urlencode({"query": f'recording:"{clean}"', "fmt": "json", "limit": "3"})
    url = f"{MUSICBRAINZ_API}?{query}"
    req = urllib.request.Request(url, headers={"User-Agent": user_agent})
    result: dict[str, Any] = {"title": clean, "artists": [], "confidence": 0.0, "source": "musicbrainz"}

    try:
        _rate_limit()
        with urllib.request.urlopen(req, timeout=timeout_seconds) as resp:
            payload = json.loads(resp.read().decode("utf-8"))
        recordings = payload.get("recordings", []) if isinstance(payload, dict) else []
        if recordings and isinstance(recordings[0], dict):
            rec = recordings[0]
            artists: list[str] = []
            for ac in rec.get("artist-credit", []) or []:
                if isinstance(ac, dict) and isinstance(ac.get("artist"), dict):
                    name = str(ac["artist"].get("name", "")).strip()
                    if name:
                        artists.append(name)
            if artists:
                result = {
                    "title": clean,
                    "artists": artists,
                    "confidence": 0.72,
                    "source": "musicbrainz",
                    "mbid": rec.get("id"),
                }
    except (urllib.error.URLError, TimeoutError, json.JSONDecodeError, OSError):
        result["source"] = "musicbrainz_error"

    entries[cache_key] = result
    _cache_save(cache_file, cache)
    return result


def enrich_tag_with_external_lookup(
    tag: dict[str, Any],
    *,
    motifs: list[dict[str, Any]],
    artist_bindings: dict[str, dict[str, Any]],
    cfg: dict[str, Any],
) -> dict[str, Any]:
    """If tag needs lookup, resolve artists and attempt motif → character assignment."""
    from pipeline.quest_motifs import match_artist_to_motif, normalize_artist_key
    from pipeline.quest_tagging import resolve_character_from_motif

    if not cfg.get("external_lookup_enabled", True):
        return tag
    if not tag.get("needs_external_lookup") and not cfg.get("always_use_external_lookup"):
        if float(tag.get("confidence", 0) or 0) >= float(cfg.get("external_lookup_confidence_threshold", 0.45)):
            return tag

    title = str(tag.get("quest_label", "")).strip()
    lookup = lookup_recording_artists(
        title,
        user_agent=str(cfg.get("musicbrainz_user_agent", "TheriacLorePipeline/1.0")),
    )
    artists = list(tag.get("artist_attributions", []) or [])
    for artist in lookup.get("artists", []) or []:
        if artist not in artists:
            artists.append(artist)

    tag = dict(tag)
    tag["artist_attributions"] = artists
    tag["external_lookup_used"] = lookup.get("source") == "musicbrainz"
    tag["needs_external_lookup"] = False

    if tag.get("main_character") and float(tag.get("character_confidence", 0) or 0) >= 0.65:
        return tag

    for artist in artists:
        binding = artist_bindings.get(normalize_artist_key(artist))
        motif = (
            {"motif_id": binding.get("motif_id"), "primary_character": binding.get("primary_character")}
            if binding
            else match_artist_to_motif(artist, motifs)
        )
        character, char_conf, motif_id = resolve_character_from_motif(motif, cast_characters=[])
        if character and char_conf >= 0.55:
            tag["main_character"] = character
            tag["character_confidence"] = char_conf
            tag["motif_id"] = motif_id
            tag["confidence"] = max(float(tag.get("confidence", 0) or 0), min(0.8, char_conf))
            tag["match_kind"] = tag.get("match_kind") or "motif_inferred"
            tag["rationale"] = f"{tag.get('rationale', '')} External lookup artists: {', '.join(artists)}."
            tag["source"] = "external_lookup"
            break
    return tag
