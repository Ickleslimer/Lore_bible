"""Narrative work registry (franchise / spin-off / route scopes)."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from pipeline.common import read_json
from pipeline.entity_resolution import normalized_name_key

DEFAULT_REGISTRY_PATH = Path("canon/narrative_works.json")


def load_narrative_works(path: Path | None = None) -> list[dict[str, Any]]:
    registry_path = path or DEFAULT_REGISTRY_PATH
    if not registry_path.exists():
        return []
    payload = read_json(registry_path)
    works = payload.get("works", []) if isinstance(payload, dict) else []
    return [row for row in works if isinstance(row, dict) and str(row.get("work_id", "")).strip()]


def work_by_id(works: list[dict[str, Any]], work_id: str) -> dict[str, Any] | None:
    key = normalized_name_key(work_id)
    for work in works:
        if normalized_name_key(str(work.get("work_id", ""))) == key:
            return work
    return None


def active_works(works: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [work for work in works if str(work.get("status", "")).strip().lower() == "active"]


def character_history_section_keys(config: dict[str, Any] | None = None) -> list[str]:
    nw_cfg = narrative_works_config(config)
    registry_path = nw_cfg.get("registry_path")
    path = Path(str(registry_path)) if registry_path else DEFAULT_REGISTRY_PATH
    keys: list[str] = []
    for work in load_narrative_works(path):
        section = str(work.get("character_history_section", "")).strip()
        if section and section not in keys:
            keys.append(section)
    return keys


def work_isolation_markers(works: list[dict[str, Any]]) -> dict[str, tuple[str, ...]]:
    markers: dict[str, tuple[str, ...]] = {}
    for work in works:
        section = str(work.get("character_history_section", "")).strip()
        if not section:
            continue
        hints = tuple(
            str(item).strip().lower()
            for item in work.get("keyword_hints", []) or []
            if str(item).strip()
        )
        if hints:
            markers[section] = hints
    return markers


def narrative_works_config(config: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(config, dict):
        return {}
    block = config.get("narrative_works")
    return block if isinstance(block, dict) else {}


def snippet_tag_path(run_root: Path) -> Path:
    return run_root / "08_narrative_work_tagging" / "snippet_narrative_work_tags.jsonl"


def work_cards_path(run_root: Path) -> Path:
    return run_root / "11_work_synthesis" / "work_cards.json"


HISTORY_SECTION_TO_WORK_ID: dict[str, str] = {
    "history_theriac_coda": "theriac_coda",
    "history_path_a_side_route": "theriac_coda_path_a",
}


def history_section_for_work(work_id: str, works: list[dict[str, Any]] | None = None) -> str | None:
    registry = works if works is not None else load_narrative_works()
    work = work_by_id(registry, work_id)
    if not work:
        return None
    return str(work.get("character_history_section", "")).strip() or None


def narrative_work_for_history_section(section_key: str, works: list[dict[str, Any]] | None = None) -> str | None:
    if section_key in HISTORY_SECTION_TO_WORK_ID:
        return HISTORY_SECTION_TO_WORK_ID[section_key]
    if not section_key.startswith("history_"):
        return None
    registry = works if works is not None else load_narrative_works()
    for work in registry:
        if str(work.get("character_history_section", "")).strip() == section_key:
            return str(work.get("work_id", "")).strip() or None
    return None


def load_snippet_narrative_work_tags(path: Path | None) -> dict[str, str]:
    from pipeline.common import read_jsonl

    if not path or not path.exists():
        return {}
    tags: dict[str, str] = {}
    for row in read_jsonl(path):
        if not isinstance(row, dict):
            continue
        snippet_id = str(row.get("snippet_id", "")).strip()
        work_id = str(row.get("narrative_work_id", "")).strip()
        if snippet_id and work_id:
            tags[snippet_id] = work_id
    return tags


def load_narrative_work_tag_overrides(review_memory_path: Path | None) -> dict[str, str]:
    if not review_memory_path or not review_memory_path.exists():
        return {}
    payload = read_json(review_memory_path)
    overrides = payload.get("narrative_work_tag_overrides", []) if isinstance(payload, dict) else []
    out: dict[str, str] = {}
    for row in overrides:
        if not isinstance(row, dict):
            continue
        snippet_id = str(row.get("snippet_id", "")).strip()
        work_id = str(row.get("narrative_work_id", "")).strip()
        if snippet_id and work_id:
            out[snippet_id] = work_id
    return out


def heuristic_narrative_work_tag(snippet: dict[str, Any], works: list[dict[str, Any]]) -> str:
    text = " ".join(
        [
            str(snippet.get("display_text_normalized", "")),
            str(snippet.get("conversation_patch_summary", "")),
            " ".join(str(item) for item in snippet.get("conversation_patch_lore_developments", []) or []),
        ]
    ).lower()
    best_work = "theriac_coda"
    best_score = 0
    for work in works:
        work_id = str(work.get("work_id", "")).strip()
        if not work_id:
            continue
        score = 0
        for hint in work.get("keyword_hints", []) or []:
            hint_text = str(hint).strip().lower()
            if hint_text and hint_text in text:
                score += 2 if work.get("kind") == "side_route" else 1
        if score > best_score:
            best_score = score
            best_work = work_id
    return best_work


def filter_snippet_ids_by_narrative_work(
    snippet_ids: list[str],
    narrative_work_id: str | None,
    tags_by_snippet: dict[str, str],
    *,
    include_untagged: bool = True,
) -> list[str]:
    if not narrative_work_id:
        return snippet_ids
    filtered: list[str] = []
    for snippet_id in snippet_ids:
        tagged = tags_by_snippet.get(snippet_id, "")
        if not tagged:
            if include_untagged:
                filtered.append(snippet_id)
            continue
        if tagged == narrative_work_id:
            filtered.append(snippet_id)
    return filtered if filtered else snippet_ids
