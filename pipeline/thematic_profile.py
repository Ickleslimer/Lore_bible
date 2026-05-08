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
            "active_historical_markers": [],
            "active_music_markers": [],
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
        "active_historical_markers": [],
        "active_music_markers": [],
    }


def merge_thematic_config(base_cfg: dict[str, Any], runtime_profile: dict[str, Any]) -> dict[str, Any]:
    base_hist = list((base_cfg.get("thematic_linking", {}) or {}).get("historical_markers", []))
    base_music = list((base_cfg.get("thematic_linking", {}) or {}).get("music_markers", []))
    learned_hist = list(runtime_profile.get("active_historical_markers", []))
    learned_music = list(runtime_profile.get("active_music_markers", []))
    return {
        "enabled": bool((base_cfg.get("thematic_linking", {}) or {}).get("enabled", True)),
        "historical_markers": sorted(set(base_hist + learned_hist)),
        "music_markers": sorted(set(base_music + learned_music)),
    }


def update_runtime_profile(
    runtime_path: Path | None,
    source_stage: str,
    historical_markers: list[str],
    music_markers: list[str],
    min_support: int = 2,
) -> dict[str, Any]:
    if runtime_path is None:
        return {}
    profile = load_runtime_profile(runtime_path)
    hist_counts = dict(profile.get("historical_counts", {}))
    music_counts = dict(profile.get("music_counts", {}))

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

    profile["historical_counts"] = hist_counts
    profile["music_counts"] = music_counts
    profile["active_historical_markers"] = sorted([k for k, v in hist_counts.items() if int(v) >= int(min_support)])
    profile["active_music_markers"] = sorted([k for k, v in music_counts.items() if int(v) >= int(min_support)])
    profile["updated_at_utc"] = now_utc_iso()
    sources = dict(profile.get("sources", {}))
    sources[source_stage] = int(sources.get(source_stage, 0)) + 1
    profile["sources"] = sources

    runtime_path.parent.mkdir(parents=True, exist_ok=True)
    write_json(runtime_path, profile)
    return profile
