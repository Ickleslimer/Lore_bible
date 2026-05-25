from __future__ import annotations

from pathlib import Path
from typing import Any

from pipeline.artifact_paths import ArtifactPaths
from pipeline.common import now_utc_iso, write_json
from pipeline.ui_review_app import _read_json_or_default

ENTITY_INVENTORY_BROWSER_CACHE_VERSION = 9


def _clean_text_list(values: Any) -> list[str]:
    if not isinstance(values, list):
        return []
    out: list[str] = []
    seen: set[str] = set()
    for value in values:
        text = str(value or "").strip()
        if text and text not in seen:
            out.append(text)
            seen.add(text)
    return out


def slim_entity_browser_row(row: dict[str, Any]) -> dict[str, Any]:
    item = row.get("item") if isinstance(row.get("item"), dict) else {}
    slim = {key: value for key, value in row.items() if key != "item"}
    aliases = _clean_text_list([*(row.get("aliases", []) or []), *(item.get("aliases", []) or [])])
    if aliases:
        slim["aliases"] = aliases
    if str(row.get("row_kind") or "") == "merged_entity":
        entity_id = str(item.get("entity_id") or "").strip()
        if entity_id:
            slim["entity_id"] = entity_id
        card_id = str(item.get("card_id") or "").strip()
        if card_id:
            slim["card_id"] = card_id
    for field in ("referent_kind", "referent_kind_label"):
        value = str(row.get(field) or "").strip()
        if value:
            slim[field] = value
    return slim


def slim_entity_browser_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [slim_entity_browser_row(row) for row in rows if isinstance(row, dict)]


def entity_inventory_source_paths(active: Path, repo_root: Path, review_memory_path: Path | None) -> list[Path]:
    paths = ArtifactPaths(active)
    candidates = [
        paths.entity_candidate_harvest,
        paths.conversation_entity_proposals,
        paths.conversation_entity_decisions,
        paths.entity_adjudication_recommendations,
        paths.theme_candidate_reclassification,
        paths.identity_merged_entities_preview,
        paths.resolved_entities,
        paths.identity_merge_proposals,
        paths.identity_merge_decisions,
        paths.claim_drafts,
        paths.claim_review_decisions,
        paths.author_claims,
        repo_root / "canon" / "theme_profile.json",
        paths.theme_profile_update_report,
    ]
    if review_memory_path is not None:
        candidates.append(review_memory_path)
    return [path for path in candidates if path.exists()]


def entity_inventory_fingerprints(source_paths: list[Path]) -> dict[str, float]:
    fingerprints: dict[str, float] = {}
    for path in source_paths:
        try:
            fingerprints[path.name] = path.stat().st_mtime
        except OSError:
            continue
    return fingerprints


def load_entity_inventory_browser_cache(cache_path: Path, fingerprints: dict[str, float]) -> dict[str, Any] | None:
    if not cache_path.exists():
        return None
    payload = _read_json_or_default(cache_path, {})
    if not isinstance(payload, dict):
        return None
    if int(payload.get("cache_version") or 0) != ENTITY_INVENTORY_BROWSER_CACHE_VERSION:
        return None
    cached = payload.get("fingerprints", {})
    if not isinstance(cached, dict) or cached != fingerprints:
        return None
    return payload


def write_entity_inventory_browser_cache(
    cache_path: Path,
    *,
    fingerprints: dict[str, float],
    active_root: str,
    rows: list[dict[str, Any]],
    merged_rows: list[dict[str, Any]],
    merged_metadata: dict[str, Any],
) -> None:
    write_json(
        cache_path,
        {
            "cache_version": ENTITY_INVENTORY_BROWSER_CACHE_VERSION,
            "generated_at_utc": now_utc_iso(),
            "active_root": active_root,
            "fingerprints": fingerprints,
            "rows": rows,
            "merged_rows": merged_rows,
            "merged_metadata": merged_metadata,
            "total": len(rows),
            "merged_total": len(merged_rows),
        },
    )
