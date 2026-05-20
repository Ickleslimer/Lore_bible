from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from pathlib import Path
from typing import Any

from pipeline.artifact_paths import ArtifactPaths, migrate_run_artifacts_to_numbered
from pipeline.common import now_utc_iso, read_json, write_json
from pipeline.entity_resolution import load_entity_records, normalize_entity_type, normalized_name_key
from pipeline.ui_review_app import (
    NEW_RUN_SELECTOR_VALUE,
    _display_path,
    _read_json_or_default,
    discover_review_runs,
    load_last_open_artifacts_root,
    pending_review_counts_for_root,
    pending_review_summary,
    pending_review_total,
    pipeline_progress_artifact_snapshot,
    pipeline_progress_from_logs,
    new_run_artifacts_root,
    save_last_open_artifacts_root,
)


def _looks_like_project_root(path: Path) -> bool:
    return (
        (path / "config" / "pipeline_config.json").exists()
        and (path / "pipeline" / "run_pipeline.py").exists()
    )


def _plain_windows_path(path: Path | str) -> Path:
    raw = str(path)
    if raw.startswith("\\\\?\\UNC\\"):
        raw = "\\" + raw[7:]
    elif raw.startswith("\\\\?\\"):
        raw = raw[4:]
    return Path(raw)


def _resolve_plain(path: Path | str) -> Path:
    return _plain_windows_path(path).expanduser().resolve()


def find_repo_root(explicit: str | None = None) -> Path:
    if explicit:
        path = _plain_windows_path(explicit).expanduser()
        if path.exists():
            return _resolve_plain(path)
    env_root = os.environ.get("THERIAC_LORE_ROOT")
    if env_root:
        path = _plain_windows_path(env_root).expanduser()
        if path.exists():
            return _resolve_plain(path)
    starts = [Path.cwd(), Path(__file__).resolve().parents[1]]
    for start in starts:
        for candidate in (start, *start.parents):
            if _looks_like_project_root(candidate):
                return _resolve_plain(candidate)
    return _resolve_plain(Path.cwd())


def _json_safe(value: Any) -> Any:
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_safe(item) for item in value]
    if isinstance(value, tuple):
        return [_json_safe(item) for item in value]
    return value


def _active_root(repo_root: Path, payload: dict[str, Any]) -> Path:
    raw = str(payload.get("artifacts_root") or "").strip()
    if raw and raw != NEW_RUN_SELECTOR_VALUE:
        path = _plain_windows_path(raw)
        if not path.is_absolute():
            path = repo_root / path
        return _resolve_plain(path)
    last = load_last_open_artifacts_root(repo_root)
    if last is not None:
        return _resolve_plain(last)
    runs_base = repo_root / "artifacts" / "runs"
    if runs_base.exists():
        runs = [path for path in runs_base.iterdir() if path.is_dir()]
        if runs:
            return _resolve_plain(max(runs, key=lambda path: path.stat().st_mtime))
    return repo_root / "artifacts"


def _run_state(repo_root: Path, active_root: Path) -> dict[str, Any]:
    repo_root = _resolve_plain(repo_root)
    active_root = _resolve_plain(active_root)
    if active_root.exists():
        migrate_run_artifacts_to_numbered(active_root)
    counts = pending_review_counts_for_root(active_root) if active_root.exists() else {}
    runs = discover_review_runs(repo_root, active_root) if active_root.exists() else []
    snapshot = pipeline_progress_artifact_snapshot(active_root) if active_root.exists() else {}
    progress = pipeline_progress_from_logs(
        [str(line) for line in snapshot.get("logs", [])] if isinstance(snapshot, dict) else [],
        str(snapshot.get("status", "idle")) if isinstance(snapshot, dict) else "idle",
        str(snapshot.get("message", "")) if isinstance(snapshot, dict) else "",
    )
    return {
        "repo_root": str(repo_root),
        "active_root": str(active_root),
        "active_label": _display_path(active_root, repo_root),
        "counts": counts,
        "pending_total": pending_review_total(counts) if counts else 0,
        "pending_summary": pending_review_summary(counts) if counts else "no review artifacts yet",
        "progress": progress,
        "runs": [
            {
                **run,
                "artifacts_root": str(_resolve_plain(run["artifacts_root"])),
                "label": _display_path(_resolve_plain(run["artifacts_root"]), repo_root),
                "is_active": _resolve_plain(run["artifacts_root"]) == active_root,
                "latest_mtime": float(run.get("latest_mtime", 0) or 0),
            }
            for run in runs
        ],
    }


def _decision_path(root: Path) -> Path:
    path = ArtifactPaths(root).identity_merge_decisions
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists():
        write_json(path, {"decisions": []})
    return path


def _append_identity_decision(path: Path, payload: dict[str, Any]) -> None:
    data = _read_json_or_default(path, {"decisions": []})
    if not isinstance(data, dict):
        data = {"decisions": []}
    decisions = data.setdefault("decisions", [])
    if not isinstance(decisions, list):
        decisions = []
        data["decisions"] = decisions
    decisions.append(payload)
    write_json(path, data)


def _identity_clusters(root: Path) -> list[dict[str, Any]]:
    from pipeline.review_inventory import identity_merge_inventory_browser_rows

    proposals_path = ArtifactPaths(root).identity_merge_proposals
    decisions_path = _decision_path(root)
    rows = identity_merge_inventory_browser_rows(proposals_path, decisions_path)
    clusters = [row for row in rows if row.get("row_kind") == "identity_merge"]
    return _json_safe(clusters)


def _claim_rows(root: Path) -> list[dict[str, Any]]:
    from pipeline.review_inventory import claim_inventory_browser_rows, sort_candidate_inventory_rows

    rows = claim_inventory_browser_rows(
        ArtifactPaths(root).claim_drafts,
        ArtifactPaths(root).claim_review_decisions,
        root,
    )
    return _json_safe(sort_candidate_inventory_rows(rows, "bucket", False))


def _entity_rows(root: Path) -> list[dict[str, Any]]:
    from pipeline.review_inventory import candidate_inventory_browser_rows, sort_candidate_inventory_rows

    rows = candidate_inventory_browser_rows(
        ArtifactPaths(root).conversation_entity_proposals,
        ArtifactPaths(root).conversation_entity_decisions,
    )
    return _json_safe(sort_candidate_inventory_rows(rows, "bucket", False))


def _load_identity_merged_entities_preview(root: Path) -> dict[str, Any]:
    from pipeline.stage_11_card_synthesis import (
        _load_identity_merge_decisions,
        _load_identity_merge_proposals,
        build_identity_merged_entities_preview,
    )

    paths = ArtifactPaths(root)
    preview_path = paths.identity_merged_entities_preview
    if preview_path.exists():
        payload = _load_json_payload(preview_path, {"entities": []})
        if isinstance(payload.get("entities"), list):
            return payload
    if not paths.identity_merge_proposals.exists() or not paths.resolved_entities.exists():
        return {"status": "missing", "entities": []}
    proposals = _load_identity_merge_proposals(paths.identity_merge_proposals) or []
    decisions = _load_identity_merge_decisions(paths.identity_merge_decisions)
    entities = load_entity_records(paths.resolved_entities)
    payload = build_identity_merged_entities_preview(entities, proposals, decisions, include_pending=True)
    write_json(preview_path, payload)
    return payload


def _all_claims_for_evidence(root: Path) -> tuple[list[dict[str, Any]], set[str]]:
    from pipeline.stage_11_card_synthesis import load_author_claims

    paths = ArtifactPaths(root)
    claims_payload = _load_json_payload(paths.claim_drafts, {"claims": []})
    claims = [claim for claim in claims_payload.get("claims", []) if isinstance(claim, dict)]
    decisions_payload = _load_json_payload(paths.claim_review_decisions, {"decisions": []})
    accepted_ids = {
        str(decision.get("claim_id", "")).strip()
        for decision in decisions_payload.get("decisions", [])
        if isinstance(decision, dict) and str(decision.get("decision", "")).strip().lower() in {"accept", "approve"}
    }
    try:
        author_claims, _failures = load_author_claims(paths.author_claims, load_entity_records(paths.resolved_entities))
    except Exception:
        author_claims = []
    for claim in author_claims:
        claim_id = str(claim.get("claim_id", "")).strip()
        if claim_id:
            accepted_ids.add(claim_id)
    return claims + author_claims, accepted_ids


def _accepted_claim_counts_by_target(root: Path, target_map: dict[str, str] | None = None) -> dict[str, int]:
    claims, accepted_ids = _all_claims_for_evidence(root)
    target_map = target_map or {}
    counts: dict[str, int] = {}
    for claim in claims:
        claim_id = str(claim.get("claim_id", "")).strip()
        if claim_id not in accepted_ids:
            continue
        source_id = str(claim.get("target_entity_id", "")).strip()
        target_id = target_map.get(source_id, source_id)
        if target_id:
            counts[target_id] = counts.get(target_id, 0) + 1
    return counts


def _merged_entity_rows(root: Path) -> dict[str, Any]:
    payload = _load_identity_merged_entities_preview(root)
    entities = payload.get("entities", []) if isinstance(payload, dict) else []
    if not isinstance(entities, list):
        entities = []
    target_map = payload.get("target_map", {}) if isinstance(payload.get("target_map"), dict) else {}
    claim_counts = _accepted_claim_counts_by_target(root, {str(k): str(v) for k, v in target_map.items()})
    rows: list[dict[str, Any]] = []
    for entity in entities:
        if not isinstance(entity, dict):
            continue
        entity_id = str(entity.get("entity_id", "")).strip()
        canonical_name = str(entity.get("canonical_name") or entity_id).strip()
        merged_from = entity.get("merged_from_entities", []) if isinstance(entity.get("merged_from_entities"), list) else []
        merge_status = str(entity.get("identity_merge_preview_status") or "unchanged")
        merge_claim_ids = [str(item) for item in entity.get("identity_merge_evidence_claim_ids", []) or [] if str(item).strip()]
        accepted_claim_count = claim_counts.get(entity_id, 0)
        if merged_from:
            reason = "Merged from " + ", ".join(
                str(source.get("canonical_name") or source.get("entity_id") or "").strip()
                for source in merged_from[:6]
                if isinstance(source, dict)
            )
        else:
            reason = "Resolved entity with no proposed identity merge."
        rows.append(
            {
                "row_id": f"merged_entity:{entity_id}",
                "row_kind": "merged_entity",
                "bucket": "merged" if merged_from else "unchanged",
                "source_bucket": "identity_merged_entities_preview",
                "category": normalize_entity_type(entity.get("entity_type") or "term"),
                "candidate_name": canonical_name,
                "raw_candidate_name": canonical_name,
                "canonical_name": canonical_name,
                "proposed_entity_type": normalize_entity_type(entity.get("entity_type") or "term"),
                "evidence_count": accepted_claim_count + len(merge_claim_ids),
                "topics": [f"merged from {len(merged_from)}"] if merged_from else [],
                "tracks": [],
                "triage_reason": reason,
                "review_priority": merge_status,
                "decision": merge_status,
                "item": entity,
            }
        )
    rows.sort(key=lambda row: (row["bucket"] != "merged", str(row["candidate_name"]).lower()))
    metadata = {
        "available": bool(entities),
        "source_path": str(ArtifactPaths(root).identity_merged_entities_preview),
        "generated_at_utc": payload.get("generated_at_utc", "") if isinstance(payload, dict) else "",
        "mode": payload.get("mode", "") if isinstance(payload, dict) else "",
        "source_entity_count": int(payload.get("source_entity_count", 0) or 0) if isinstance(payload, dict) else 0,
        "merged_entity_count": int(payload.get("merged_entity_count", len(rows)) or len(rows)) if isinstance(payload, dict) else len(rows),
        "merge_record_count": int(payload.get("merge_record_count", 0) or 0) if isinstance(payload, dict) else 0,
        "pending_identity_merge_count": int(payload.get("pending_identity_merge_count", 0) or 0) if isinstance(payload, dict) else 0,
        "approved_identity_merge_count": int(payload.get("approved_identity_merge_count", 0) or 0) if isinstance(payload, dict) else 0,
    }
    return {"rows": _json_safe(rows), "metadata": metadata}


def _source_snippet_rows(root: Path, source_ids: list[str], limit: int = 8) -> list[dict[str, Any]]:
    wanted = [str(item).strip() for item in source_ids if str(item).strip()]
    if not wanted:
        return []
    wanted_set = set(wanted[:limit])
    path = ArtifactPaths(root).snippets
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    try:
        with path.open("r", encoding="utf-8") as handle:
            for line in handle:
                if len(rows) >= len(wanted_set):
                    break
                if not line.strip():
                    continue
                item = json.loads(line)
                snippet_id = str(item.get("snippet_id", "")).strip()
                if snippet_id not in wanted_set:
                    continue
                rows.append(
                    {
                        "snippet_id": snippet_id,
                        "topic_label": str(item.get("conversation_topic_label") or item.get("conversation_patch_topic_label") or ""),
                        "conversation_id": str(item.get("conversation_id") or ""),
                        "created_at": str(item.get("created_at") or item.get("timestamp") or ""),
                        "text": str(
                            item.get("patch_item_text")
                            or item.get("display_text_normalized")
                            or item.get("conversation_patch_summary")
                            or item.get("text")
                            or ""
                        ),
                    }
                )
    except Exception:
        return rows
    return rows


def _claims_for_entity_evidence(root: Path, entity_ids: set[str], limit: int = 18) -> list[dict[str, Any]]:
    claims, accepted_ids = _all_claims_for_evidence(root)
    out: list[dict[str, Any]] = []
    for claim in claims:
        claim_id = str(claim.get("claim_id", "")).strip()
        if claim_id not in accepted_ids:
            continue
        if str(claim.get("target_entity_id", "")).strip() not in entity_ids:
            continue
        out.append(
            {
                "claim_id": claim_id,
                "claim_text": str(claim.get("claim_text") or ""),
                "claim_type": str(claim.get("claim_type") or ""),
                "target_entity_name": str(claim.get("target_entity_name") or ""),
                "source_snippet_ids": [str(item) for item in claim.get("source_snippet_ids", []) or [] if str(item).strip()],
                "confidence": claim.get("confidence"),
                "author_claim": bool(claim.get("author_claim") or claim.get("manual_claim")),
            }
        )
        if len(out) >= limit:
            break
    return out


def _word_count(value: Any) -> int:
    import re

    return len(re.findall(r"\b\w+\b", str(value or "")))


def _card_sections(card: dict[str, Any]) -> list[dict[str, Any]]:
    details = card.get("details") if isinstance(card.get("details"), dict) else {}
    sections = details.get("sections") if isinstance(details.get("sections"), dict) else {}
    preferred = [
        ("background", "Background"),
        ("role_in_story", "Role In Story"),
        ("relationships", "Relationships"),
        ("timeline", "Timeline"),
        ("inspirations", "Inspirations"),
        ("open_questions", "Open Questions"),
    ]
    out: list[dict[str, Any]] = []
    seen: set[str] = set()
    for key, title in preferred:
        text = str(sections.get(key, "") or "").strip()
        seen.add(key)
        if text:
            out.append({"key": key, "title": title, "text": text, "word_count": _word_count(text)})
    for key, value in sections.items():
        key_text = str(key)
        if key_text in seen:
            continue
        text = str(value or "").strip()
        if text:
            out.append(
                {
                    "key": key_text,
                    "title": key_text.replace("_", " ").title(),
                    "text": text,
                    "word_count": _word_count(text),
                }
            )
    return out


def _card_word_count(card: dict[str, Any], sections: list[dict[str, Any]]) -> int:
    return _word_count(card.get("summary", "")) + sum(int(section.get("word_count", 0) or 0) for section in sections)


def _load_json_payload(path: Path, default: dict[str, Any]) -> dict[str, Any]:
    if not path.exists():
        return default
    try:
        payload = read_json(path)
    except Exception:
        return default
    return payload if isinstance(payload, dict) else default


def _draft_source(paths: ArtifactPaths) -> tuple[str, Path | None, dict[str, Any]]:
    candidates = [
        ("final", paths.card_drafts),
        ("partial", paths.stage11 / "card_drafts.partial.json"),
        ("checkpoint", paths.stage11 / "card_synthesis_checkpoint.json"),
    ]
    existing = [(kind, path) for kind, path in candidates if path.exists()]
    if not existing:
        return "missing", None, {"cards": []}
    kind, path = max(existing, key=lambda item: item[1].stat().st_mtime)
    return kind, path, _load_json_payload(path, {"cards": []})


def _draft_card_rows(root: Path) -> dict[str, Any]:
    migrate_run_artifacts_to_numbered(root)
    paths = ArtifactPaths(root)
    source_kind, source_path, payload = _draft_source(paths)
    cards = payload.get("cards", []) if isinstance(payload, dict) else []
    if not isinstance(cards, list):
        cards = []
    rows: list[dict[str, Any]] = []
    for index, card in enumerate(cards):
        if not isinstance(card, dict):
            continue
        sections = _card_sections(card)
        details = card.get("details") if isinstance(card.get("details"), dict) else {}
        card_id = str(card.get("card_id") or f"draft_card_{index}")
        rows.append(
            {
                "card_id": card_id,
                "canonical_name": str(card.get("canonical_name") or card_id),
                "entity_type": normalize_entity_type(card.get("entity_type") or "term"),
                "status": str(card.get("status") or "draft"),
                "summary": str(card.get("summary") or ""),
                "sections": sections,
                "word_count": _card_word_count(card, sections),
                "section_count": len(sections),
                "claim_count": len(details.get("accepted_claim_ids", []) or []),
                "evidence_count": len(card.get("source_evidence", []) or []),
                "relationships": card.get("relationships", []) if isinstance(card.get("relationships"), list) else [],
                "timeline": card.get("timeline", []) if isinstance(card.get("timeline"), list) else [],
                "wiki_links": details.get("wiki_links", []) if isinstance(details.get("wiki_links"), list) else [],
                "unresolved_conflicts": details.get("unresolved_conflicts", [])
                if isinstance(details.get("unresolved_conflicts"), list)
                else [],
                "item": card,
            }
        )
    rows.sort(key=lambda row: str(row.get("canonical_name", "")).lower())
    failures_payload = _load_json_payload(paths.stage11 / "card_synthesis_failures.json", {"failures": []})
    failures = failures_payload.get("failures", []) if isinstance(failures_payload, dict) else []
    checkpoint_payload = _load_json_payload(paths.stage11 / "card_synthesis_checkpoint.json", {})
    metadata = {
        "source_kind": source_kind,
        "source_path": str(source_path) if source_path is not None else "",
        "updated_at_utc": payload.get("updated_at_utc") or checkpoint_payload.get("updated_at_utc") or "",
        "status": payload.get("status") or checkpoint_payload.get("status") or ("missing" if source_path is None else "available"),
        "processed_count": int(payload.get("processed_count") or checkpoint_payload.get("processed_count") or 0),
        "total_count": int(payload.get("total_count") or checkpoint_payload.get("total_count") or 0),
        "current_entity_id": payload.get("current_entity_id") or checkpoint_payload.get("current_entity_id") or "",
        "current_entity_name": payload.get("current_entity_name") or checkpoint_payload.get("current_entity_name") or "",
        "failure_count": len(failures) if isinstance(failures, list) else 0,
    }
    return {
        "active_root": str(root),
        "metadata": metadata,
        "cards": _json_safe(rows),
        "total": len(rows),
        "failures": _json_safe(failures if isinstance(failures, list) else []),
    }


def _stable_graph_id(prefix: str, value: str) -> str:
    key = normalized_name_key(value)
    if key:
        return f"{prefix}:{key}"
    digest = hashlib.sha1(value.encode("utf-8", errors="ignore")).hexdigest()[:16]
    return f"{prefix}:{digest}"


def _relationship_graph_entities(root: Path) -> tuple[dict[str, dict[str, Any]], dict[str, str], dict[str, str], dict[str, str]]:
    paths = ArtifactPaths(root)
    nodes: dict[str, dict[str, Any]] = {}
    name_map: dict[str, str] = {}
    card_map: dict[str, str] = {}
    target_map: dict[str, str] = {}
    preview = _load_identity_merged_entities_preview(root)
    entities = preview.get("entities", []) if isinstance(preview.get("entities"), list) else []
    if isinstance(preview.get("target_map"), dict):
        target_map = {str(key): str(value) for key, value in preview.get("target_map", {}).items()}
    if not entities and paths.resolved_entities.exists():
        entities = load_entity_records(paths.resolved_entities)

    def register_name(name: str, entity_id: str) -> None:
        key = normalized_name_key(name)
        if key and key not in name_map:
            name_map[key] = entity_id

    for entity in entities:
        if not isinstance(entity, dict):
            continue
        raw_id = str(entity.get("entity_id") or "").strip()
        entity_id = target_map.get(raw_id, raw_id)
        if not entity_id:
            continue
        canonical_name = str(entity.get("canonical_name") or entity_id).strip()
        aliases = [str(alias) for alias in entity.get("aliases", []) or [] if str(alias).strip()]
        merged_from = entity.get("merged_from_entities", []) if isinstance(entity.get("merged_from_entities"), list) else []
        existing = nodes.get(entity_id, {})
        merged_aliases = set(existing.get("aliases", []) or [])
        merged_aliases.update(aliases)
        for source in merged_from:
            if not isinstance(source, dict):
                continue
            source_name = str(source.get("canonical_name") or "").strip()
            if source_name:
                merged_aliases.add(source_name)
            for alias in source.get("aliases", []) or []:
                if str(alias).strip():
                    merged_aliases.add(str(alias))
        node = {
            "node_id": entity_id,
            "entity_id": entity_id,
            "card_id": str(entity.get("card_id") or "").strip(),
            "name": canonical_name,
            "entity_type": normalize_entity_type(entity.get("entity_type") or existing.get("entity_type") or "term"),
            "aliases": sorted(merged_aliases),
            "resolved": True,
            "degree": 0,
            "evidence_count": 0,
            "track_counts": {},
            "source": "resolved_entity",
        }
        nodes[entity_id] = node
        register_name(canonical_name, entity_id)
        for alias in node["aliases"]:
            register_name(str(alias), entity_id)
        card_id = str(entity.get("card_id") or "").strip()
        if card_id:
            card_map[card_id] = entity_id
        if raw_id and raw_id != entity_id:
            target_map[raw_id] = entity_id

    return nodes, name_map, card_map, target_map


def _relationship_graph(root: Path) -> dict[str, Any]:
    migrate_run_artifacts_to_numbered(root)
    paths = ArtifactPaths(root)
    nodes, name_map, card_map, target_map = _relationship_graph_entities(root)
    edges: dict[tuple[str, str, str, str], dict[str, Any]] = {}
    source_counts: dict[str, int] = {}

    def resolve_ref(*, name: Any = "", entity_id: Any = "", card_id: Any = "") -> str:
        raw_entity_id = str(entity_id or "").strip()
        if raw_entity_id:
            mapped = target_map.get(raw_entity_id, raw_entity_id)
            if mapped in nodes:
                return mapped
        raw_card_id = str(card_id or "").strip()
        if raw_card_id and raw_card_id in card_map:
            return card_map[raw_card_id]
        raw_name = str(name or "").strip()
        name_key = normalized_name_key(raw_name)
        if name_key and name_key in name_map:
            return name_map[name_key]
        if raw_card_id:
            node_id = _stable_graph_id("unresolved_card", raw_card_id)
            display_name = raw_name or raw_card_id
        else:
            node_id = _stable_graph_id("unresolved", raw_name or raw_entity_id or "unknown")
            display_name = raw_name or raw_entity_id or "Unknown"
        if node_id not in nodes:
            nodes[node_id] = {
                "node_id": node_id,
                "entity_id": raw_entity_id,
                "card_id": raw_card_id,
                "name": display_name,
                "entity_type": "unresolved",
                "aliases": [],
                "resolved": False,
                "degree": 0,
                "evidence_count": 0,
                "track_counts": {},
                "source": "unresolved_reference",
            }
            if name_key:
                name_map[name_key] = node_id
        return node_id

    def add_edge(
        *,
        source_id: str,
        target_id: str,
        relation_type: Any,
        note: Any = "",
        track: Any = "",
        confidence: Any = None,
        support_ids: list[Any] | None = None,
        source_kind: str,
        source_ref: Any = "",
    ) -> None:
        if not source_id or not target_id or source_id == target_id:
            return
        relation = str(relation_type or "related_to").strip() or "related_to"
        track_text = str(track or "unknown").strip().lower() or "unknown"
        key = (source_id, target_id, relation.lower(), track_text)
        edge = edges.setdefault(
            key,
            {
                "edge_id": _stable_graph_id("relationship_edge", "|".join(key)),
                "source_id": source_id,
                "target_id": target_id,
                "source_name": nodes.get(source_id, {}).get("name", source_id),
                "target_name": nodes.get(target_id, {}).get("name", target_id),
                "relation_type": relation,
                "track": track_text,
                "evidence_count": 0,
                "confidence": None,
                "descriptions": [],
                "support_ids": [],
                "source_refs": [],
                "source_kinds": [],
            },
        )
        edge["evidence_count"] = int(edge.get("evidence_count", 0) or 0) + 1
        if note and len(edge["descriptions"]) < 10:
            edge["descriptions"].append(str(note))
        for support_id in support_ids or []:
            support_text = str(support_id).strip()
            if support_text and support_text not in edge["support_ids"]:
                edge["support_ids"].append(support_text)
        source_ref_text = str(source_ref or "").strip()
        if source_ref_text and source_ref_text not in edge["source_refs"]:
            edge["source_refs"].append(source_ref_text)
        if source_kind not in edge["source_kinds"]:
            edge["source_kinds"].append(source_kind)
        try:
            confidence_value = float(confidence)
        except (TypeError, ValueError):
            confidence_value = None
        if confidence_value is not None:
            existing = edge.get("confidence")
            edge["confidence"] = confidence_value if existing is None else max(float(existing), confidence_value)
        source_counts[source_kind] = source_counts.get(source_kind, 0) + 1

    source_kind, source_path, draft_payload = _draft_source(paths)
    cards = draft_payload.get("cards", []) if isinstance(draft_payload, dict) and isinstance(draft_payload.get("cards"), list) else []
    for card in cards:
        if not isinstance(card, dict):
            continue
        source_id = resolve_ref(
            name=card.get("canonical_name"),
            entity_id=card.get("entity_id") or card.get("target_entity_id"),
            card_id=card.get("card_id"),
        )
        for rel in card.get("relationships", []) or []:
            if not isinstance(rel, dict):
                continue
            target_id = resolve_ref(
                name=rel.get("target_entity_name"),
                entity_id=rel.get("target_entity_id"),
                card_id=rel.get("target_card_id"),
            )
            add_edge(
                source_id=source_id,
                target_id=target_id,
                relation_type=rel.get("relation_type"),
                note=rel.get("note") or rel.get("description"),
                track=rel.get("track") or card.get("knowledge_track") or "lore",
                confidence=rel.get("confidence"),
                support_ids=rel.get("support_claim_ids", []) or [],
                source_kind="card_relationship",
                source_ref=card.get("card_id"),
            )
        details = card.get("details") if isinstance(card.get("details"), dict) else {}
        for link in details.get("wiki_links", []) or []:
            if not isinstance(link, dict):
                continue
            target_id = resolve_ref(
                name=link.get("target_entity_name"),
                entity_id=link.get("target_entity_id"),
                card_id=link.get("target_card_id"),
            )
            add_edge(
                source_id=source_id,
                target_id=target_id,
                relation_type=link.get("relation_type"),
                note=f"Wiki link in {link.get('section') or 'card'}",
                track=link.get("track") or card.get("knowledge_track") or "lore",
                confidence=link.get("confidence"),
                support_ids=link.get("support_claim_ids", []) or [],
                source_kind="wiki_link",
                source_ref=card.get("card_id"),
            )

    notes_payload = _load_json_payload(paths.conversation_patch_notes, {"notes": []})
    notes = notes_payload.get("notes", []) if isinstance(notes_payload.get("notes"), list) else []
    if not notes and paths.conversation_patch_notes_jsonl.exists():
        try:
            with paths.conversation_patch_notes_jsonl.open("r", encoding="utf-8") as handle:
                notes = [json.loads(line) for line in handle if line.strip()]
        except Exception:
            notes = []
    for note in notes:
        if not isinstance(note, dict):
            continue
        track = note.get("track") or "unknown"
        patch_note_id = note.get("patch_note_id") or note.get("conversation_id") or ""
        for rel in note.get("relationship_updates", []) or []:
            if not isinstance(rel, dict):
                continue
            source_name = str(rel.get("source_entity") or "").strip()
            target_name = str(rel.get("target_entity") or "").strip()
            if not source_name or not target_name:
                continue
            source_id = resolve_ref(name=source_name)
            target_id = resolve_ref(name=target_name)
            add_edge(
                source_id=source_id,
                target_id=target_id,
                relation_type=rel.get("relationship_type"),
                note=rel.get("description"),
                track=track,
                confidence=rel.get("confidence"),
                support_ids=rel.get("supporting_message_ids", []) or [],
                source_kind="patch_note_relationship",
                source_ref=patch_note_id,
            )

    claims, accepted_ids = _all_claims_for_evidence(root)
    decision_payload = _load_json_payload(paths.claim_review_decisions, {"decisions": []})
    decision_text_by_id = {
        str(decision.get("claim_id") or ""): str(decision.get("edited_claim_text") or "").strip()
        for decision in decision_payload.get("decisions", []) or []
        if isinstance(decision, dict) and str(decision.get("edited_claim_text") or "").strip()
    }
    structured_hint_count = 0
    accepted_relationship_claim_count = 0
    for claim in claims:
        if not isinstance(claim, dict):
            continue
        claim_id = str(claim.get("claim_id") or "").strip()
        if claim_id not in accepted_ids:
            continue
        claim_type = str(claim.get("claim_type") or "").strip().lower()
        if claim_type in {"relationship", "alias", "inspiration"}:
            accepted_relationship_claim_count += 1
        source_id = resolve_ref(
            name=claim.get("target_entity_name"),
            entity_id=claim.get("target_entity_id"),
            card_id=claim.get("target_card_id"),
        )
        claim_text = decision_text_by_id.get(claim_id) or str(claim.get("claim_text") or "")
        for hint in claim.get("proposed_relationship_hints", []) or []:
            if not isinstance(hint, dict):
                continue
            target_name = hint.get("target_entity_name") or hint.get("related_entity_name")
            target_entity_id = hint.get("target_entity_id") or hint.get("related_entity_id")
            target_card_id = hint.get("target_card_id") or hint.get("related_card_id")
            if not any(str(value or "").strip() for value in (target_name, target_entity_id, target_card_id)):
                continue
            target_id = resolve_ref(name=target_name, entity_id=target_entity_id, card_id=target_card_id)
            add_edge(
                source_id=source_id,
                target_id=target_id,
                relation_type=hint.get("relation_type") or claim_type,
                note=hint.get("note") or claim_text,
                track=claim.get("knowledge_track") or "unknown",
                confidence=hint.get("confidence") or claim.get("confidence"),
                support_ids=[claim_id],
                source_kind="claim_relationship_hint",
                source_ref=claim_id,
            )
            structured_hint_count += 1

    for edge in edges.values():
        for node_id in (str(edge.get("source_id")), str(edge.get("target_id"))):
            node = nodes.get(node_id)
            if not node:
                continue
            node["degree"] = int(node.get("degree", 0) or 0) + 1
            node["evidence_count"] = int(node.get("evidence_count", 0) or 0) + int(edge.get("evidence_count", 0) or 0)
            track_counts = node.setdefault("track_counts", {})
            track = str(edge.get("track") or "unknown")
            track_counts[track] = int(track_counts.get(track, 0) or 0) + int(edge.get("evidence_count", 0) or 0)

    involved = {
        node_id
        for edge in edges.values()
        for node_id in (str(edge.get("source_id")), str(edge.get("target_id")))
        if node_id
    }
    graph_nodes = [node for node_id, node in nodes.items() if node_id in involved]
    graph_nodes.sort(key=lambda item: (-int(item.get("degree", 0) or 0), str(item.get("name", "")).lower()))
    graph_edges = sorted(edges.values(), key=lambda item: (-int(item.get("evidence_count", 0) or 0), str(item.get("relation_type", "")).lower()))
    return {
        "active_root": str(root),
        "nodes": _json_safe(graph_nodes),
        "edges": _json_safe(graph_edges),
        "metadata": {
            "node_count": len(graph_nodes),
            "edge_count": len(graph_edges),
            "isolated_entity_count": max(0, len(nodes) - len(involved)),
            "source_counts": source_counts,
            "draft_card_source_kind": source_kind,
            "draft_card_source_path": str(source_path) if source_path is not None else "",
            "accepted_relationship_claim_count": accepted_relationship_claim_count,
            "structured_claim_hint_count": structured_hint_count,
        },
    }


def handle_state(repo_root: Path, payload: dict[str, Any]) -> dict[str, Any]:
    active = _active_root(repo_root, payload)
    return _run_state(repo_root, active)


def handle_select_run(repo_root: Path, payload: dict[str, Any]) -> dict[str, Any]:
    active = _active_root(repo_root, payload)
    if active.exists():
        save_last_open_artifacts_root(repo_root, active)
    return _run_state(repo_root, active)


def handle_create_run(repo_root: Path, payload: dict[str, Any]) -> dict[str, Any]:
    active = new_run_artifacts_root(repo_root)
    active = _resolve_plain(active)
    save_last_open_artifacts_root(repo_root, active)
    return _run_state(repo_root, active)


def handle_identity_clusters(repo_root: Path, payload: dict[str, Any]) -> dict[str, Any]:
    active = _active_root(repo_root, payload)
    return {
        "active_root": str(active),
        "clusters": _identity_clusters(active) if active.exists() else [],
    }


def handle_claim_inventory(repo_root: Path, payload: dict[str, Any]) -> dict[str, Any]:
    active = _active_root(repo_root, payload)
    rows = _claim_rows(active) if active.exists() else []
    return {"active_root": str(active), "rows": rows, "total": len(rows)}


def handle_entity_inventory(repo_root: Path, payload: dict[str, Any]) -> dict[str, Any]:
    active = _active_root(repo_root, payload)
    rows = _entity_rows(active) if active.exists() else []
    merged = _merged_entity_rows(active) if active.exists() else {"rows": [], "metadata": {"available": False}}
    merged_rows = merged.get("rows", []) if isinstance(merged, dict) else []
    metadata = merged.get("metadata", {}) if isinstance(merged, dict) else {}
    return {
        "active_root": str(active),
        "rows": rows,
        "total": len(rows),
        "merged_rows": merged_rows,
        "merged_total": len(merged_rows) if isinstance(merged_rows, list) else 0,
        "merged_metadata": metadata,
    }


def handle_entity_evidence(repo_root: Path, payload: dict[str, Any]) -> dict[str, Any]:
    active = _active_root(repo_root, payload)
    row_id = str(payload.get("row_id") or "").strip()
    view = str(payload.get("view") or "").strip().lower()
    if not row_id:
        raise ValueError("Missing row_id.")
    if view == "merged" or row_id.startswith("merged_entity:"):
        merged = _merged_entity_rows(active)
        rows = merged.get("rows", []) if isinstance(merged, dict) else []
    else:
        rows = _entity_rows(active) if active.exists() else []
    row = next((item for item in rows if str(item.get("row_id") or "") == row_id), None)
    if row is None:
        raise ValueError(f"Unknown entity row: {row_id}")
    item = row.get("item", {}) if isinstance(row.get("item"), dict) else {}
    if row_id.startswith("merged_entity:"):
        entity_id = str(item.get("entity_id", "")).strip()
        source_entity_ids = {
            str(source.get("entity_id", "")).strip()
            for source in item.get("merged_from_entities", []) or []
            if isinstance(source, dict) and str(source.get("entity_id", "")).strip()
        }
        entity_ids = {entity_id, *source_entity_ids} if entity_id else source_entity_ids
        claims = _claims_for_entity_evidence(active, entity_ids)
        source_ids = []
        for claim in claims:
            source_ids.extend(str(source_id) for source_id in claim.get("source_snippet_ids", []) or [] if str(source_id).strip())
        source_ids.extend(str(source_id) for source_id in item.get("identity_merge_source_snippet_ids", []) or [] if str(source_id).strip())
        return {
            "active_root": str(active),
            "row_id": row_id,
            "view": "merged",
            "claims": _json_safe(claims),
            "snippets": _json_safe(_source_snippet_rows(active, list(dict.fromkeys(source_ids)), limit=10)),
            "sample_texts": [],
            "type_evidence": [],
            "merge_records": _json_safe(item.get("identity_merge_preview_records", []) if isinstance(item.get("identity_merge_preview_records"), list) else []),
            "merged_from_entities": _json_safe(item.get("merged_from_entities", []) if isinstance(item.get("merged_from_entities"), list) else []),
            "aliases": _json_safe(item.get("aliases", []) if isinstance(item.get("aliases"), list) else []),
        }

    sample_texts = [str(text) for text in item.get("sample_texts", []) or [] if str(text).strip()]
    type_evidence = [entry for entry in item.get("type_evidence", []) or [] if isinstance(entry, dict)]
    source_ids = [
        str(entry.get("snippet_id", "")).strip()
        for entry in type_evidence
        if str(entry.get("snippet_id", "")).strip()
    ]
    for key in ("source_snippet_ids", "evidence_snippet_ids", "sample_snippet_ids"):
        source_ids.extend(str(source_id).strip() for source_id in item.get(key, []) or [] if str(source_id).strip())
    return {
        "active_root": str(active),
        "row_id": row_id,
        "view": "candidates",
        "claims": [],
        "snippets": _json_safe(_source_snippet_rows(active, list(dict.fromkeys(source_ids)), limit=10)),
        "sample_texts": sample_texts[:12],
        "type_evidence": _json_safe(type_evidence[:24]),
        "merge_records": [],
        "merged_from_entities": [],
        "aliases": _json_safe(item.get("alias_candidates", []) if isinstance(item.get("alias_candidates"), list) else []),
    }


def handle_draft_cards(repo_root: Path, payload: dict[str, Any]) -> dict[str, Any]:
    active = _active_root(repo_root, payload)
    return _draft_card_rows(active) if active.exists() else {"active_root": str(active), "metadata": {}, "cards": [], "total": 0, "failures": []}


def handle_entity_relationships(repo_root: Path, payload: dict[str, Any]) -> dict[str, Any]:
    active = _active_root(repo_root, payload)
    return _relationship_graph(active) if active.exists() else {"active_root": str(active), "nodes": [], "edges": [], "metadata": {}}


def handle_card_agent_activity(repo_root: Path, payload: dict[str, Any]) -> dict[str, Any]:
    from pipeline.cardbase_agent import card_agent_activity_payload

    active = _active_root(repo_root, payload)
    return card_agent_activity_payload(active) if active.exists() else {"active_root": str(active), "transactions": [], "total": 0}


def handle_undo_card_agent_transaction(repo_root: Path, payload: dict[str, Any]) -> dict[str, Any]:
    from pipeline.cardbase_agent import undo_card_agent_transaction

    active = _active_root(repo_root, payload)
    transaction_id = str(payload.get("transaction_id") or "").strip()
    if not transaction_id:
        raise ValueError("Missing transaction_id.")
    undo_card_agent_transaction(
        ArtifactPaths(active).stage11,
        transaction_id,
        reviewer=str(payload.get("reviewer") or "tauri_user"),
        rationale=str(payload.get("rationale") or ""),
    )
    return handle_card_agent_activity(repo_root, {"artifacts_root": str(active)})


def handle_identity_cluster_decision(repo_root: Path, payload: dict[str, Any]) -> dict[str, Any]:
    active = _active_root(repo_root, payload)
    proposal_id = str(payload.get("proposal_id") or "").strip()
    if not proposal_id:
        raise ValueError("Missing proposal_id.")
    decision = str(payload.get("decision") or "defer").strip().lower()
    if decision == "accept":
        decision = "approve"
    if decision not in {"approve", "reject", "defer", "needs_more_context"}:
        raise ValueError(f"Unsupported decision: {decision}")
    canonical_name = str(payload.get("canonical_name") or "").strip()
    canonical_entity_id = str(payload.get("canonical_entity_id") or "").strip()
    if canonical_name and not canonical_entity_id:
        for row in _identity_clusters(active):
            item = row.get("item", {}) if isinstance(row, dict) else {}
            if str(item.get("proposal_id") or "") != proposal_id:
                continue
            name_key = normalized_name_key(canonical_name)
            for member in item.get("member_entities", []) or []:
                if not isinstance(member, dict):
                    continue
                names = [str(member.get("canonical_name") or ""), *[str(alias) for alias in member.get("aliases", []) or []]]
                if any(normalized_name_key(name) == name_key for name in names if name.strip()):
                    canonical_entity_id = str(member.get("entity_id") or "")
                    break
            break
    decision_payload: dict[str, Any] = {
        "proposal_id": proposal_id,
        "decision": decision,
        "reviewer": str(payload.get("reviewer") or "tauri_user"),
        "rationale": str(payload.get("rationale") or ""),
        "timestamp_utc": now_utc_iso(),
        "human_override": True,
        "override_source": "tauri_identity_cluster_panel",
    }
    if canonical_name:
        decision_payload["canonical_name"] = canonical_name
    if canonical_entity_id:
        decision_payload["canonical_entity_id"] = canonical_entity_id
    _append_identity_decision(_decision_path(active), decision_payload)
    return handle_identity_clusters(repo_root, {"artifacts_root": str(active)})


def handle_identity_edge_decision(repo_root: Path, payload: dict[str, Any]) -> dict[str, Any]:
    active = _active_root(repo_root, payload)
    edge_id = str(payload.get("edge_proposal_id") or payload.get("edge_id") or "").strip()
    cluster_id = str(payload.get("cluster_id") or payload.get("proposal_id") or "").strip()
    if not edge_id or not cluster_id:
        raise ValueError("Missing edge_proposal_id or cluster_id.")
    decision = str(payload.get("decision") or "defer").strip().lower()
    decision = {
        "approve": "accept",
        "keep": "accept",
        "restore": "accept",
        "refute": "reject",
    }.get(decision, decision)
    if decision not in {"accept", "reject", "defer", "needs_more_context"}:
        raise ValueError(f"Unsupported edge decision: {decision}")
    _append_identity_decision(
        _decision_path(active),
        {
            "decision_scope": "identity_edge",
            "cluster_id": cluster_id,
            "edge_proposal_id": edge_id,
            "source_entity_id": str(payload.get("source_entity_id") or ""),
            "source_entity_name": str(payload.get("source_entity_name") or ""),
            "target_entity_id": str(payload.get("target_entity_id") or ""),
            "target_entity_name": str(payload.get("target_entity_name") or ""),
            "decision": decision,
            "reviewer": str(payload.get("reviewer") or "tauri_user"),
            "rationale": str(payload.get("rationale") or ""),
            "timestamp_utc": now_utc_iso(),
            "human_override": True,
            "override_source": "tauri_identity_edge_panel",
        },
    )
    return handle_identity_clusters(repo_root, {"artifacts_root": str(active)})


def handle_claim_decision(repo_root: Path, payload: dict[str, Any]) -> dict[str, Any]:
    from pipeline.review_inventory import write_claim_inventory_override_decision

    active = _active_root(repo_root, payload)
    row_id = str(payload.get("row_id") or "").strip()
    if not row_id:
        raise ValueError("Missing row_id.")
    rows = _claim_rows(active)
    row = next((item for item in rows if str(item.get("row_id") or "") == row_id), None)
    if row is None:
        raise ValueError(f"Unknown claim row: {row_id}")
    decision = str(payload.get("decision") or "defer").strip().lower()
    if decision not in {"approve", "accept", "reject", "defer", "needs_more_context"}:
        raise ValueError(f"Unsupported decision: {decision}")
    write_claim_inventory_override_decision(
        ArtifactPaths(active).claim_review_decisions,
        row,
        decision,
        str(payload.get("reviewer") or "tauri_user"),
        str(payload.get("rationale") or ""),
    )
    return handle_claim_inventory(repo_root, {"artifacts_root": str(active)})


def handle_entity_decision(repo_root: Path, payload: dict[str, Any]) -> dict[str, Any]:
    from pipeline.review_inventory import write_candidate_inventory_override_decision

    active = _active_root(repo_root, payload)
    row_id = str(payload.get("row_id") or "").strip()
    if not row_id:
        raise ValueError("Missing row_id.")
    rows = _entity_rows(active)
    row = next((item for item in rows if str(item.get("row_id") or "") == row_id), None)
    if row is None:
        raise ValueError(f"Unknown entity row: {row_id}")
    decision = str(payload.get("decision") or "defer").strip().lower()
    if decision not in {"approve", "accept", "reject", "defer", "needs_more_context"}:
        raise ValueError(f"Unsupported decision: {decision}")
    canonical_name = str(payload.get("canonical_name") or row.get("canonical_name") or row.get("raw_candidate_name") or "").strip()
    entity_type = normalize_entity_type(payload.get("entity_type") or row.get("proposed_entity_type") or "term")
    write_candidate_inventory_override_decision(
        ArtifactPaths(active).conversation_entity_decisions,
        row,
        decision,
        canonical_name,
        entity_type,
        str(payload.get("reviewer") or "tauri_user"),
        str(payload.get("rationale") or ""),
    )
    return handle_entity_inventory(repo_root, {"artifacts_root": str(active)})


COMMANDS = {
    "state": handle_state,
    "select_run": handle_select_run,
    "create_run": handle_create_run,
    "identity_clusters": handle_identity_clusters,
    "identity_cluster_decision": handle_identity_cluster_decision,
    "identity_edge_decision": handle_identity_edge_decision,
    "claim_inventory": handle_claim_inventory,
    "claim_decision": handle_claim_decision,
    "entity_inventory": handle_entity_inventory,
    "entity_evidence": handle_entity_evidence,
    "entity_decision": handle_entity_decision,
    "draft_cards": handle_draft_cards,
    "entity_relationships": handle_entity_relationships,
    "card_agent_activity": handle_card_agent_activity,
    "undo_card_agent_transaction": handle_undo_card_agent_transaction,
}


def handle_request(request: dict[str, Any]) -> dict[str, Any]:
    command = str(request.get("command") or "").strip()
    if command not in COMMANDS:
        raise ValueError(f"Unknown command: {command}")
    payload = request.get("payload") if isinstance(request.get("payload"), dict) else {}
    repo_root = find_repo_root(str(request.get("repo_root") or payload.get("repo_root") or "").strip() or None)
    return COMMANDS[command](repo_root, payload)


def main() -> None:
    if hasattr(sys.stdout, "reconfigure"):
        try:
            sys.stdout.reconfigure(encoding="utf-8")
        except Exception:
            pass
    parser = argparse.ArgumentParser()
    parser.add_argument("--request", help="JSON request payload. If omitted, stdin is used.")
    args = parser.parse_args()
    try:
        raw = args.request if args.request is not None else sys.stdin.read()
        request = json.loads(raw or "{}")
        result = {"ok": True, "result": handle_request(request)}
    except Exception as exc:
        result = {"ok": False, "error": str(exc)}
    sys.stdout.write(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main()
