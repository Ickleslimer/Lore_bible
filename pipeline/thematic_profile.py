from __future__ import annotations

from pathlib import Path
from typing import Any

from pipeline.common import now_utc_iso, read_json, write_json


def load_runtime_profile(path: Path | None) -> dict[str, Any]:
    if not path or not path.exists():
        return {
            "updated_at_utc": None,
            "sources": {},
            "historical_counts": {},
            "music_counts": {},
            "quest_song_counts": {},
            "active_historical_markers": [],
            "active_music_markers": [],
            "active_quest_song_markers": [],
        }
    try:
        data = read_json(path)
        if isinstance(data, dict):
            return data
    except Exception:
        pass
    return {
        "updated_at_utc": None,
        "sources": {},
        "historical_counts": {},
        "music_counts": {},
        "quest_song_counts": {},
        "active_historical_markers": [],
        "active_music_markers": [],
        "active_quest_song_markers": [],
    }


def _load_quest_song_seeds(seed_path: str | None) -> list[str]:
    """Load quest-song markers from the seed json file."""
    if not seed_path:
        return []
    path = Path(seed_path)
    if not path.exists():
        return []
    try:
        payload = read_json(path)
    except Exception:
        return []
    seeds = payload.get("quest_song_seeds", []) if isinstance(payload, dict) else []
    markers: list[str] = []
    for seed in seeds:
        if not isinstance(seed, dict):
            continue
        title = str(seed.get("quest_title", "")).strip()
        if title:
            markers.append(title.lower())
    return sorted(set(markers))


def merge_thematic_config(base_cfg: dict[str, Any], runtime_profile: dict[str, Any]) -> dict[str, Any]:
    base_hist = list((base_cfg.get("thematic_linking", {}) or {}).get("historical_markers", []))
    base_music = list((base_cfg.get("thematic_linking", {}) or {}).get("music_markers", []))
    learned_hist = list(runtime_profile.get("active_historical_markers", []))
    learned_music = list(runtime_profile.get("active_music_markers", []))
    # Quest-song markers: seed + runtime learned
    qs_cfg = (base_cfg.get("thematic_linking", {}) or {}).get("quest_song_markers", {}) or {}
    seed_path = str(qs_cfg.get("seed_path", "")) if isinstance(qs_cfg, dict) else ""
    seed_markers = _load_quest_song_seeds(seed_path)
    learned_qs = list(runtime_profile.get("active_quest_song_markers", []))
    return {
        "enabled": bool((base_cfg.get("thematic_linking", {}) or {}).get("enabled", True)),
        "historical_markers": sorted(set(base_hist + learned_hist)),
        "music_markers": sorted(set(base_music + learned_music)),
        "quest_song_markers": sorted(set(seed_markers + learned_qs)),
    }


def update_runtime_profile(
    runtime_path: Path | None,
    source_stage: str,
    historical_markers: list[str],
    music_markers: list[str],
    min_support: int = 2,
    quest_song_markers: list[str] | None = None,
) -> dict[str, Any]:
    if runtime_path is None:
        return {}
    profile = load_runtime_profile(runtime_path)
    hist_counts = dict(profile.get("historical_counts", {}))
    music_counts = dict(profile.get("music_counts", {}))
    qs_counts = dict(profile.get("quest_song_counts", {}))

    for marker in historical_markers:
        m = str(marker).strip().lower()
        if not m:
            continue
        hist_counts[m] = int(hist_counts.get(m, 0)) + 1
    for marker in music_markers:
        m = str(marker).strip().lower()
        if not m:
            continue
        music_counts[m] = int(music_counts.get(m, 0)) + 1
    for marker in quest_song_markers or []:
        m = str(marker).strip().lower()
        if not m:
            continue
        qs_counts[m] = int(qs_counts.get(m, 0)) + 1

    profile["historical_counts"] = hist_counts
    profile["music_counts"] = music_counts
    profile["quest_song_counts"] = qs_counts
    profile["active_historical_markers"] = sorted([k for k, v in hist_counts.items() if int(v) >= int(min_support)])
    profile["active_music_markers"] = sorted([k for k, v in music_counts.items() if int(v) >= int(min_support)])
    profile["active_quest_song_markers"] = sorted([k for k, v in qs_counts.items() if int(v) >= int(min_support)])
    profile["updated_at_utc"] = now_utc_iso()
    sources = dict(profile.get("sources", {}))
    sources[source_stage] = int(sources.get(source_stage, 0)) + 1
    profile["sources"] = sources

    runtime_path.parent.mkdir(parents=True, exist_ok=True)
    write_json(runtime_path, profile)
    return profile
