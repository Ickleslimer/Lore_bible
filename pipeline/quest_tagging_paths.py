"""Artifact paths and review-memory loaders for Stage 08Q."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from pipeline.common import read_json, read_jsonl

CODA_WORK_IDS = frozenset({"theriac_coda", "theriac_coda_path_a"})


def snippet_quest_tags_path(run_root: Path) -> Path:
    return run_root / "08_quest_tagging" / "snippet_quest_tags.jsonl"


def discovered_quests_path(run_root: Path) -> Path:
    return run_root / "08_quest_tagging" / "discovered_quests.json"


def quest_tagging_summary_path(run_root: Path) -> Path:
    return run_root / "08_quest_tagging" / "tagging_summary.json"


def artist_character_review_queue_path(run_root: Path) -> Path:
    return run_root / "08_quest_tagging" / "artist_character_review_queue.jsonl"


def load_snippet_narrative_work_tags(path: Path | None) -> dict[str, str]:
    if not path or not path.exists():
        return {}
    out: dict[str, str] = {}
    for row in read_jsonl(path):
        if not isinstance(row, dict):
            continue
        snippet_id = str(row.get("snippet_id", "")).strip()
        work_id = str(row.get("narrative_work_id", "")).strip()
        if snippet_id and work_id:
            out[snippet_id] = work_id
    return out


def load_snippet_quest_tags(path: Path | None) -> list[dict[str, Any]]:
    if not path or not path.exists():
        return []
    return [row for row in read_jsonl(path) if isinstance(row, dict)]


def quest_tags_by_snippet(path: Path | None) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in load_snippet_quest_tags(path):
        snippet_id = str(row.get("snippet_id", "")).strip()
        if snippet_id:
            grouped.setdefault(snippet_id, []).append(row)
    return grouped


def load_quest_tag_overrides(review_memory_path: Path | None) -> dict[str, list[dict[str, Any]]]:
    """snippet_id -> list of override tag dicts."""
    if not review_memory_path or not review_memory_path.exists():
        return {}
    payload = read_json(review_memory_path)
    rows = payload.get("quest_tag_overrides", []) if isinstance(payload, dict) else []
    out: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        snippet_id = str(row.get("snippet_id", "")).strip()
        if snippet_id:
            out.setdefault(snippet_id, []).append(row)
    return out


def load_motif_artist_bindings(review_memory_path: Path | None) -> dict[str, dict[str, Any]]:
    """normalized artist key -> binding row."""
    if not review_memory_path or not review_memory_path.exists():
        return {}
    payload = read_json(review_memory_path)
    rows = payload.get("motif_artist_bindings", []) if isinstance(payload, dict) else []
    out: dict[str, dict[str, Any]] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        from pipeline.quest_motifs import normalize_artist_key

        artist = str(row.get("artist_normalized", "") or row.get("artist_label", "")).strip()
        key = normalize_artist_key(artist)
        if key:
            out[key] = row
    return out


def quest_hints_for_snippet(
    snippet_id: str,
    tags_path: Path | None,
    *,
    min_confidence: float = 0.75,
) -> list[dict[str, Any]]:
    """Optional Stage 09 helper; returns [] when tags missing."""
    hints: list[dict[str, Any]] = []
    for row in quest_tags_by_snippet(tags_path).get(snippet_id, []):
        if float(row.get("confidence", 0) or 0) >= min_confidence:
            hints.append(row)
    return hints
