from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
from pathlib import Path
from typing import Any

from pipeline.artifact_paths import ArtifactPaths, migrate_run_artifacts_to_numbered
from pipeline.common import now_utc_iso, read_json, write_json
from pipeline.entity_inventory_browser import (
    entity_inventory_fingerprints,
    entity_inventory_source_paths,
    load_entity_inventory_browser_cache,
    slim_entity_browser_rows,
    write_entity_inventory_browser_cache,
)
from pipeline.entity_resolution import load_entity_records, normalize_entity_type, normalized_name_key
from pipeline.theme_rescue_status import theme_rescue_status_payload, write_theme_rescue_approval
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


def _resolve_configured_path(repo_root: Path, value: Any, default: str = "") -> Path:
    raw = str(value or default).strip()
    path = _plain_windows_path(raw or default)
    if not path.is_absolute():
        path = repo_root / path
    return _resolve_plain(path)


def _path_for_config(repo_root: Path, path: Path) -> str:
    resolved = _resolve_plain(path)
    try:
        return resolved.relative_to(_resolve_plain(repo_root)).as_posix()
    except ValueError:
        return str(resolved)


def _read_env_lines(path: Path) -> list[str]:
    if not path.exists():
        return []
    return path.read_text(encoding="utf-8").splitlines()


def _unquote_env_value(value: str) -> str:
    text = value.strip()
    if (text.startswith('"') and text.endswith('"')) or (text.startswith("'") and text.endswith("'")):
        text = text[1:-1]
    return text.strip()


def _openrouter_key_from_env(repo_root: Path) -> tuple[str, str]:
    names = ("OPENROUTER_API_KEY", "OPENROUTER_KEY", "OPEN_ROUTER_API_KEY")
    for name in names:
        value = os.environ.get(name, "").strip()
        if value:
            return value, f"process:{name}"
    for line in _read_env_lines(repo_root / ".env"):
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key, value = stripped.split("=", 1)
        if key.strip() in names and value.strip():
            return _unquote_env_value(value), f".env:{key.strip()}"
    return "", ""


def _key_preview(value: str) -> str:
    text = value.strip()
    if not text:
        return ""
    if len(text) <= 10:
        return "*" * len(text)
    return f"{text[:6]}...{text[-4:]}"


def _write_openrouter_key(repo_root: Path, api_key: str) -> None:
    clean = api_key.strip().strip('"').strip("'")
    if not clean:
        return
    env_path = repo_root / ".env"
    names = {"OPENROUTER_API_KEY", "OPENROUTER_KEY", "OPEN_ROUTER_API_KEY"}
    lines = _read_env_lines(env_path)
    updated = False
    out: list[str] = []
    for line in lines:
        stripped = line.strip()
        if stripped and not stripped.startswith("#") and "=" in stripped:
            key, _value = stripped.split("=", 1)
            if key.strip() in names:
                if not updated:
                    out.append(f"OPENROUTER_API_KEY={clean}")
                    updated = True
                continue
        out.append(line)
    if not updated:
        if out and out[-1].strip():
            out.append("")
        out.append(f"OPENROUTER_API_KEY={clean}")
    env_path.write_text("\n".join(out).rstrip() + "\n", encoding="utf-8")


MODEL_SLOT_PROFILES: dict[str, list[str]] = {
    "volume_model": ["high_volume", "balanced_reasoning"],
    "reasoning_model": ["deep_reasoning", "premium_reasoning"],
    "card_writing_model": ["card_writing"],
}


def _model_choice_catalog() -> list[dict[str, str]]:
    return [
        {
            "id": "qwen/qwen3.5-flash-02-23",
            "label": "Qwen 3.5 Flash",
            "description": "Fast, low-cost batch work",
        },
        {
            "id": "qwen/qwen3-235b-a22b-2507",
            "label": "Qwen 3 235B",
            "description": "Higher-capacity annotation and harvest work",
        },
        {
            "id": "deepseek/deepseek-v4-flash",
            "label": "DeepSeek V4 Flash",
            "description": "Deep reasoning, card writing, and theme mining",
        },
        {
            "id": "openai/gpt-oss-120b",
            "label": "GPT-OSS 120B",
            "description": "Web-backed externality adjudication",
        },
    ]


def _routing_profiles(config: dict[str, Any]) -> dict[str, Any]:
    routing = config.get("model_routing", {}) if isinstance(config.get("model_routing", {}), dict) else {}
    profiles = routing.get("profiles", {}) if isinstance(routing.get("profiles", {}), dict) else {}
    return profiles if isinstance(profiles, dict) else {}


def _profile_api_model(profiles: dict[str, Any], profile_name: str, default: str = "") -> str:
    profile = profiles.get(profile_name, {})
    if not isinstance(profile, dict):
        return default
    return str(profile.get("api_model") or default).strip()


def _read_model_slot(profiles: dict[str, Any], slot: str, default: str) -> str:
    for profile_name in MODEL_SLOT_PROFILES.get(slot, []):
        value = _profile_api_model(profiles, profile_name)
        if value:
            return value
    return default


def _model_choices_for_payload(profiles: dict[str, Any]) -> list[dict[str, str]]:
    catalog = _model_choice_catalog()
    known = {entry["id"] for entry in catalog}
    extras: list[dict[str, str]] = []
    for slot in MODEL_SLOT_PROFILES:
        current = _read_model_slot(profiles, slot, "")
        if current and current not in known:
            extras.append(
                {
                    "id": current,
                    "label": current,
                    "description": "Currently configured custom model",
                }
            )
            known.add(current)
    return catalog + extras


def _model_selection_payload(config: dict[str, Any]) -> dict[str, Any]:
    profiles = _routing_profiles(config)
    model_provider = config.get("model_provider", {}) if isinstance(config.get("model_provider", {}), dict) else {}
    story_questions = config.get("story_questions", {}) if isinstance(config.get("story_questions", {}), dict) else {}
    volume_default = str(model_provider.get("api_model") or "qwen/qwen3.5-flash-02-23")
    reasoning_default = str(story_questions.get("model") or "deepseek/deepseek-v4-flash")
    card_default = "deepseek/deepseek-v4-flash"
    return {
        "volume_model": _read_model_slot(profiles, "volume_model", volume_default),
        "reasoning_model": _read_model_slot(profiles, "reasoning_model", reasoning_default),
        "card_writing_model": _read_model_slot(profiles, "card_writing_model", card_default),
        "model_choices": _model_choices_for_payload(profiles),
    }


def _apply_model_selections(config: dict[str, Any], payload: dict[str, Any]) -> None:
    selections = {
        "volume_model": str(payload.get("volume_model") or "").strip(),
        "reasoning_model": str(payload.get("reasoning_model") or "").strip(),
        "card_writing_model": str(payload.get("card_writing_model") or "").strip(),
    }
    if not any(selections.values()):
        return

    routing = config.get("model_routing", {}) if isinstance(config.get("model_routing", {}), dict) else {}
    profiles = routing.get("profiles", {}) if isinstance(routing.get("profiles", {}), dict) else {}
    if not isinstance(profiles, dict):
        profiles = {}

    for slot, model_id in selections.items():
        if not model_id:
            continue
        for profile_name in MODEL_SLOT_PROFILES.get(slot, []):
            profile = profiles.get(profile_name, {})
            if not isinstance(profile, dict):
                profile = {}
            profile["api_model"] = model_id
            profiles[profile_name] = profile

    routing["profiles"] = profiles
    config["model_routing"] = routing

    if selections["volume_model"]:
        model_provider = config.get("model_provider", {}) if isinstance(config.get("model_provider", {}), dict) else {}
        model_provider["api_model"] = selections["volume_model"]
        config["model_provider"] = model_provider

    if selections["reasoning_model"]:
        story_questions = config.get("story_questions", {}) if isinstance(config.get("story_questions", {}), dict) else {}
        story_questions["model"] = selections["reasoning_model"]
        config["story_questions"] = story_questions


def app_config_payload(repo_root: Path) -> dict[str, Any]:
    config_path = repo_root / "config" / "pipeline_config.json"
    config = read_json(config_path) if config_path.exists() else {}
    if not isinstance(config, dict):
        config = {}
    paths = config.get("paths", {}) if isinstance(config.get("paths", {}), dict) else {}
    bootstrap_config_value = str(paths.get("docx_lore_bible") or "theriac-coda---lore-bible.docx")
    bootstrap_doc_path = _resolve_configured_path(repo_root, bootstrap_config_value, "theriac-coda---lore-bible.docx")
    openrouter_key, openrouter_source = _openrouter_key_from_env(repo_root)
    return {
        "repo_root": str(repo_root),
        "config_path": str(config_path),
        "env_path": str(repo_root / ".env"),
        "bootstrap_doc_path": str(bootstrap_doc_path),
        "bootstrap_doc_config_value": bootstrap_config_value,
        "bootstrap_doc_exists": bootstrap_doc_path.exists(),
        "openrouter_key_present": bool(openrouter_key),
        "openrouter_key_source": openrouter_source,
        "openrouter_key_preview": _key_preview(openrouter_key),
        **_model_selection_payload(config),
    }


def handle_app_config(repo_root: Path, payload: dict[str, Any]) -> dict[str, Any]:
    return app_config_payload(repo_root)


def handle_save_app_config(repo_root: Path, payload: dict[str, Any]) -> dict[str, Any]:
    config_path = repo_root / "config" / "pipeline_config.json"
    config = read_json(config_path) if config_path.exists() else {}
    if not isinstance(config, dict):
        config = {}
    paths = config.get("paths", {}) if isinstance(config.get("paths", {}), dict) else {}

    bootstrap_raw = str(payload.get("bootstrap_doc_path") or "").strip()
    if bootstrap_raw:
        bootstrap_path = _resolve_configured_path(repo_root, bootstrap_raw)
        if not bootstrap_path.exists():
            raise ValueError(f"Bootstrap DOCX not found: {bootstrap_path}")
        if bootstrap_path.suffix.lower() != ".docx":
            raise ValueError("Bootstrap document must be a .docx file.")
        paths["docx_lore_bible"] = _path_for_config(repo_root, bootstrap_path)
    config["paths"] = paths
    _apply_model_selections(config, payload)
    config["desktop_config_updated_at_utc"] = now_utc_iso()
    write_json(config_path, config)

    openrouter_key = str(payload.get("openrouter_api_key") or "").strip()
    if openrouter_key:
        _write_openrouter_key(repo_root, openrouter_key)

    return app_config_payload(repo_root)


def handle_select_bootstrap_doc(repo_root: Path, payload: dict[str, Any]) -> dict[str, Any]:
    initial = str(payload.get("initial_path") or "").strip()
    initial_path = _resolve_configured_path(repo_root, initial or "theriac-coda---lore-bible.docx")
    initial_dir = initial_path.parent if initial_path.parent.exists() else repo_root
    try:
        import tkinter as tk
        from tkinter import filedialog
    except Exception as exc:
        raise RuntimeError(f"Could not open file picker: {exc}") from exc
    root = tk.Tk()
    root.withdraw()
    root.attributes("-topmost", True)
    try:
        selected = filedialog.askopenfilename(
            title="Select bootstrap DOCX",
            initialdir=str(initial_dir),
            filetypes=[("Word documents", "*.docx"), ("All files", "*.*")],
        )
    finally:
        root.destroy()
    return {"path": str(_resolve_plain(selected)) if selected else ""}


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
    theme_rescue = theme_rescue_status_payload(active_root, repo_root) if active_root.exists() else None
    return {
        "repo_root": str(repo_root),
        "active_root": str(active_root),
        "active_label": _display_path(active_root, repo_root),
        "counts": counts,
        "pending_total": pending_review_total(counts) if counts else 0,
        "pending_summary": pending_review_summary(counts) if counts else "no review artifacts yet",
        "progress": progress,
        "theme_rescue": theme_rescue,
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

    paths = ArtifactPaths(root)
    source_path = paths.conversation_entity_proposals
    if paths.entity_candidate_harvest.exists():
        harvest_payload = _read_json_or_default(paths.entity_candidate_harvest, {"candidates": []})
        harvest_candidates = harvest_payload.get("candidates", []) if isinstance(harvest_payload, dict) else []
        if harvest_candidates or not source_path.exists():
            source_path = paths.entity_candidate_harvest
    rows = candidate_inventory_browser_rows(
        source_path,
        paths.conversation_entity_decisions,
    )
    return _json_safe(sort_candidate_inventory_rows(rows, "bucket", False))


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


def _review_memory_path_for_root(root: Path, repo_root: Path | None = None) -> Path | None:
    candidates: list[Path] = []
    if repo_root is not None:
        candidates.append(repo_root / "canon" / "review_memory.json")
    for candidate in (root, *root.parents):
        if _looks_like_project_root(candidate):
            candidates.append(candidate / "canon" / "review_memory.json")
            break
    for candidate in candidates:
        if candidate.exists():
            return candidate
    return candidates[0] if candidates else None


def _build_identity_preview_from_merge_records(
    entities: list[dict[str, Any]],
    merge_records: list[dict[str, Any]],
    *,
    identity_merge_proposal_count: int,
) -> dict[str, Any]:
    from pipeline.stage_11_card_synthesis import apply_entity_merges_to_entities

    merged_entities, target_map, sources_by_target = apply_entity_merges_to_entities(entities, merge_records)
    original_entity_by_id = {str(entity.get("entity_id", "")): entity for entity in entities}
    records_by_target: dict[str, list[dict[str, Any]]] = {}
    for record in merge_records:
        target_id = str(record.get("target_entity_id", "")).strip()
        final_target_id = target_map.get(target_id, target_id)
        if final_target_id:
            records_by_target.setdefault(final_target_id, []).append(record)

    enriched_entities: list[dict[str, Any]] = []
    for entity in merged_entities:
        entity_id = str(entity.get("entity_id", "")).strip()
        records = records_by_target.get(entity_id, [])
        source_ids = sources_by_target.get(entity_id, [])
        merged_from_entities = []
        for source_id in source_ids:
            source = original_entity_by_id.get(source_id, {})
            if source:
                merged_from_entities.append(
                    {
                        "entity_id": source_id,
                        "card_id": str(source.get("card_id", "")),
                        "canonical_name": str(source.get("canonical_name", "")),
                        "entity_type": normalize_entity_type(source.get("entity_type", "term")),
                        "aliases": _clean_text_list(source.get("aliases", [])),
                    }
                )
        statuses = _clean_text_list([record.get("review_status", "") for record in records])
        claim_ids = _clean_text_list([claim_id for record in records for claim_id in record.get("source_claim_ids", []) or []])
        snippet_ids = _clean_text_list([snippet_id for record in records for snippet_id in record.get("source_snippet_ids", []) or []])
        enriched_entities.append(
            {
                **entity,
                "identity_merge_preview_status": "mixed" if len(set(statuses)) > 1 else (statuses[0] if statuses else "unchanged"),
                "identity_merge_preview_record_count": len(records),
                "identity_merge_preview_records": records,
                "identity_merge_proposal_ids": _clean_text_list([record.get("proposal_id", "") for record in records]),
                "identity_merge_evidence_claim_ids": claim_ids,
                "identity_merge_source_snippet_ids": snippet_ids,
                "merged_from_entities": merged_from_entities,
            }
        )

    pending_count = sum(1 for record in merge_records if str(record.get("review_status", "")).lower() == "pending")
    approved_count = sum(1 for record in merge_records if str(record.get("review_status", "")).lower() in {"approve", "accepted", "approved"})
    return {
        "generated_at_utc": now_utc_iso(),
        "mode": "approved_memory_and_identity_merge_preview",
        "source_entity_count": len(entities),
        "merged_entity_count": len(enriched_entities),
        "merge_record_count": len(merge_records),
        "identity_merge_proposal_count": identity_merge_proposal_count,
        "pending_identity_merge_count": pending_count,
        "approved_identity_merge_count": approved_count,
        "target_map": target_map,
        "sources_by_target": sources_by_target,
        "merge_records": merge_records,
        "entities": sorted(enriched_entities, key=lambda entity: str(entity.get("canonical_name", "")).lower()),
    }


def _load_identity_merged_entities_preview(root: Path, repo_root: Path | None = None) -> dict[str, Any]:
    from pipeline.stage_11_card_synthesis import (
        _load_identity_merge_decisions,
        _load_identity_merge_proposals,
        approved_entity_merges_from_memory,
        build_identity_merged_entities_preview,
        identity_merge_records_from_proposals,
    )

    paths = ArtifactPaths(root)
    preview_path = paths.identity_merged_entities_preview
    review_memory_path = _review_memory_path_for_root(root, repo_root)
    memory_payload = _load_json_payload(review_memory_path, {}) if review_memory_path is not None and review_memory_path.exists() else {}
    memory_records = approved_entity_merges_from_memory(memory_payload if isinstance(memory_payload, dict) else {})
    if preview_path.exists():
        payload = _load_json_payload(preview_path, {"entities": []})
        if isinstance(payload.get("entities"), list):
            if not memory_records:
                return payload
            try:
                preview_mtime = preview_path.stat().st_mtime
                source_paths = [
                    path
                    for path in [
                        review_memory_path,
                        paths.identity_merge_proposals,
                        paths.identity_merge_decisions,
                        paths.resolved_entities,
                    ]
                    if path is not None and path.exists()
                ]
                if payload.get("mode") == "approved_memory_and_identity_merge_preview" and all(
                    path.stat().st_mtime <= preview_mtime for path in source_paths
                ):
                    return payload
            except OSError:
                pass
    if not paths.identity_merge_proposals.exists() or not paths.resolved_entities.exists():
        return {"status": "missing", "entities": []}
    proposals = _load_identity_merge_proposals(paths.identity_merge_proposals) or []
    decisions = _load_identity_merge_decisions(paths.identity_merge_decisions)
    entities = load_entity_records(paths.resolved_entities)
    if memory_records:
        proposal_records = identity_merge_records_from_proposals(proposals, decisions, include_pending=True)
        records_by_pair: dict[tuple[str, str], dict[str, Any]] = {}
        for record in [*proposal_records, *memory_records]:
            source_id = str(record.get("source_entity_id", "")).strip()
            target_id = str(record.get("target_entity_id", "")).strip()
            if not source_id or not target_id or source_id == target_id:
                continue
            normalized = {
                **record,
                "review_status": str(record.get("review_status") or "approve").strip().lower(),
                "proposal_id": str(record.get("proposal_id") or record.get("merge_id") or ""),
            }
            records_by_pair[(source_id, target_id)] = normalized
        payload = _build_identity_preview_from_merge_records(
            entities,
            list(records_by_pair.values()),
            identity_merge_proposal_count=len(proposals),
        )
    else:
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


def _merged_entity_rows(root: Path, repo_root: Path | None = None) -> dict[str, Any]:
    payload = _load_identity_merged_entities_preview(root, repo_root)
    entities = payload.get("entities", []) if isinstance(payload, dict) else []
    if not isinstance(entities, list):
        entities = []
    review_memory_path = _review_memory_path_for_root(root, repo_root)
    memory_payload = _load_json_payload(review_memory_path, {}) if review_memory_path is not None else {}
    removed_ids = {
        str(item.get("entity_id", "")).strip()
        for item in memory_payload.get("removed_entities", [])
        if isinstance(item, dict) and str(item.get("entity_id", "")).strip()
    }
    removed_card_ids = {
        str(item.get("card_id", "")).strip()
        for item in memory_payload.get("removed_entities", [])
        if isinstance(item, dict) and str(item.get("card_id", "")).strip()
    }
    removed_name_keys = {
        normalized_name_key(str(item.get("canonical_name", "")))
        for item in memory_payload.get("removed_entities", [])
        if isinstance(item, dict) and str(item.get("canonical_name", "")).strip()
    }
    target_map = payload.get("target_map", {}) if isinstance(payload.get("target_map"), dict) else {}
    claim_counts = _accepted_claim_counts_by_target(root, {str(k): str(v) for k, v in target_map.items()})
    rows: list[dict[str, Any]] = []
    for entity in entities:
        if not isinstance(entity, dict):
            continue
        entity_id = str(entity.get("entity_id", "")).strip()
        canonical_name = str(entity.get("canonical_name") or entity_id).strip()
        if (
            entity_id in removed_ids
            or str(entity.get("card_id", "")).strip() in removed_card_ids
            or normalized_name_key(canonical_name) in removed_name_keys
        ):
            continue
        merged_from = entity.get("merged_from_entities", []) if isinstance(entity.get("merged_from_entities"), list) else []
        merge_status = str(entity.get("identity_merge_preview_status") or "unchanged")
        merge_claim_ids = [str(item) for item in entity.get("identity_merge_evidence_claim_ids", []) or [] if str(item).strip()]
        accepted_claim_count = claim_counts.get(entity_id, 0)
        alias_values = _clean_text_list(
            [
                *[str(alias) for alias in entity.get("aliases", []) or []],
                *[
                    str(source.get("canonical_name") or source.get("entity_id") or "")
                    for source in merged_from
                    if isinstance(source, dict)
                ],
                *[
                    str(alias)
                    for source in merged_from
                    if isinstance(source, dict)
                    for alias in source.get("aliases", []) or []
                ],
            ]
        )
        alias_values = [alias for alias in alias_values if normalized_name_key(alias) != normalized_name_key(canonical_name)]
        if merged_from:
            reason = "Merged aliases are listed on the card."
        else:
            reason = "Resolved entity with no proposed identity merge."
        item = {**entity, "aliases": alias_values}
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
                "topics": [],
                "tracks": [],
                "triage_reason": reason,
                "review_priority": merge_status,
                "decision": merge_status,
                "item": item,
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
    paths = ArtifactPaths(root)
    snippet_paths = [paths.snippets_with_theme_rescue, paths.snippets]
    rows: list[dict[str, Any]] = []
    seen_ids: set[str] = set()
    try:
        for path in snippet_paths:
            if not path.exists():
                continue
            with path.open("r", encoding="utf-8") as handle:
                for line in handle:
                    if len(rows) >= len(wanted_set):
                        break
                    if not line.strip():
                        continue
                    item = json.loads(line)
                    snippet_id = str(item.get("snippet_id", "")).strip()
                    if snippet_id not in wanted_set or snippet_id in seen_ids:
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
                    seen_ids.add(snippet_id)
            if len(rows) >= len(wanted_set):
                break
    except Exception:
        return rows
    order = {snippet_id: idx for idx, snippet_id in enumerate(wanted)}
    rows.sort(key=lambda item: order.get(str(item.get("snippet_id", "")), 9999))
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
    from pipeline.card_sections import card_review_section_order

    preferred = card_review_section_order()
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


def _relationship_graph_entities(root: Path, repo_root: Path | None = None) -> tuple[dict[str, dict[str, Any]], dict[str, str], dict[str, str], dict[str, str]]:
    paths = ArtifactPaths(root)
    nodes: dict[str, dict[str, Any]] = {}
    name_map: dict[str, str] = {}
    card_map: dict[str, str] = {}
    target_map: dict[str, str] = {}
    preview = _load_identity_merged_entities_preview(root, repo_root)
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


def _relationship_graph(root: Path, repo_root: Path | None = None) -> dict[str, Any]:
    migrate_run_artifacts_to_numbered(root)
    paths = ArtifactPaths(root)
    nodes, name_map, card_map, target_map = _relationship_graph_entities(root, repo_root)
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

    notes_payload = _load_json_payload(paths.entity_development_history, {"by_entity": {}})
    by_entity = notes_payload.get("by_entity", {}) if isinstance(notes_payload.get("by_entity"), dict) else {}
    for entity_id, entries in by_entity.items():
        if not isinstance(entries, list):
            continue
        for entry in entries:
            if not isinstance(entry, dict):
                continue
            if str(entry.get("change_type", "")).strip().lower() != "relationship":
                continue
            source_name = str(entry.get("subject_label") or "").strip()
            headline = str(entry.get("headline") or "").strip()
            if not source_name or not headline:
                continue
            target_match = re.search(r"(?:with|to|and)\s+([A-Z][A-Za-z0-9' -]+)", headline)
            target_name = target_match.group(1).strip() if target_match else ""
            if not target_name:
                continue
            source_id = resolve_ref(name=source_name)
            target_id = resolve_ref(name=target_name)
            add_edge(
                source_id=source_id,
                target_id=target_id,
                relation_type=entry.get("change_type"),
                note=headline,
                track="lore",
                confidence=entry.get("confidence"),
                support_ids=entry.get("supporting_message_ids", []) or [],
                source_kind="ledger_relationship",
                source_ref=str(entry.get("entry_id") or entity_id),
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


def _build_entity_inventory_payload(active: Path, repo_root: Path) -> dict[str, Any]:
    rows = _entity_rows(active) if active.exists() else []
    merged = _merged_entity_rows(active, repo_root) if active.exists() else {"rows": [], "metadata": {"available": False}}
    merged_rows = merged.get("rows", []) if isinstance(merged, dict) else []
    metadata = merged.get("metadata", {}) if isinstance(merged, dict) else {}
    theme_associations_by_entity = _theme_associations_by_entity(active, repo_root) if active.exists() else {}
    rows = slim_entity_browser_rows(
        _annotate_entity_rows_with_theme_associations(rows, theme_associations_by_entity),
    )
    merged_rows = slim_entity_browser_rows(
        _annotate_entity_rows_with_theme_associations(
            merged_rows if isinstance(merged_rows, list) else [],
            theme_associations_by_entity,
        ),
    )
    return {
        "active_root": str(active),
        "rows": rows,
        "total": len(rows),
        "merged_rows": merged_rows,
        "merged_total": len(merged_rows) if isinstance(merged_rows, list) else 0,
        "merged_metadata": metadata,
    }


def handle_entity_inventory(repo_root: Path, payload: dict[str, Any]) -> dict[str, Any]:
    active = _active_root(repo_root, payload)
    if not active.exists():
        return {
            "active_root": str(active),
            "rows": [],
            "total": 0,
            "merged_rows": [],
            "merged_total": 0,
            "merged_metadata": {"available": False},
        }
    review_memory_path = _review_memory_path_for_root(active, repo_root)
    source_paths = entity_inventory_source_paths(active, repo_root, review_memory_path)
    fingerprints = entity_inventory_fingerprints(source_paths)
    cache_path = ArtifactPaths(active).entity_inventory_browser_cache
    cached = load_entity_inventory_browser_cache(cache_path, fingerprints)
    if cached is not None:
        return {
            "active_root": str(active),
            "rows": cached.get("rows", []) if isinstance(cached.get("rows"), list) else [],
            "total": int(cached.get("total", 0) or 0),
            "merged_rows": cached.get("merged_rows", []) if isinstance(cached.get("merged_rows"), list) else [],
            "merged_total": int(cached.get("merged_total", 0) or 0),
            "merged_metadata": cached.get("merged_metadata", {}) if isinstance(cached.get("merged_metadata"), dict) else {},
            "cache_hit": True,
        }
    payload_out = _build_entity_inventory_payload(active, repo_root)
    write_entity_inventory_browser_cache(
        cache_path,
        fingerprints=fingerprints,
        active_root=str(active),
        rows=payload_out["rows"],
        merged_rows=payload_out["merged_rows"],
        merged_metadata=payload_out["merged_metadata"],
    )
    payload_out["cache_hit"] = False
    return payload_out


def _theme_profile_path(repo_root: Path) -> Path:
    return repo_root / "canon" / "theme_profile.json"


def _theme_learning_association_id(theme_id: str, normalized_key: str, candidate_name: str) -> str:
    source = f"{theme_id}|{normalized_key}|{candidate_name}".encode("utf-8", errors="ignore")
    return f"theme_assoc:{hashlib.sha1(source).hexdigest()[:16]}"


def _theme_status_counts(themes: list[dict[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for theme in themes:
        status = str(theme.get("status") or "unknown").strip() or "unknown"
        counts[status] = counts.get(status, 0) + 1
    return counts


def _theme_association_base_score(row: dict[str, Any]) -> float:
    strength = _safe_float(row.get("match_strength"))
    boost = min(1.0, _safe_float(row.get("prior_boost")) / 0.35) if row.get("prior_boost") is not None else 0.0
    indicators = row.get("matched_indicators", [])
    indicator_count = min(len(indicators), 10) / 10 if isinstance(indicators, list) else 0.0
    local_gain = max(0.0, _safe_float(row.get("theme_adjusted_lore_prior")) - _safe_float(row.get("base_local_lore_prior")))
    existing = row.get("theme_match_score")
    if existing is not None:
        return _safe_float(existing)
    return round(max(0.0, min(1.0, (0.72 * strength) + (0.18 * boost) + (0.06 * indicator_count) + (0.04 * local_gain))), 3)


def _rank_adjusted_theme_association_score(score: float, entity_theme_rank: int) -> float:
    if entity_theme_rank <= 1:
        weight = 1.0
    elif entity_theme_rank == 2:
        weight = 0.62
    elif entity_theme_rank == 3:
        weight = 0.42
    else:
        weight = 0.3
    return round(max(0.0, min(1.0, score * weight)), 3)


def _safe_float(value: Any) -> float:
    try:
        return float(value or 0.0)
    except (TypeError, ValueError):
        return 0.0


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return default


def _entity_theme_role(rank: int) -> str:
    if rank <= 1:
        return "primary"
    if rank == 2:
        return "secondary"
    return "supporting"


def _rank_theme_learning_associations(associations: list[dict[str, Any]]) -> None:
    by_entity: dict[str, list[dict[str, Any]]] = {}
    by_theme: dict[str, list[dict[str, Any]]] = {}
    for association in associations:
        association["theme_match_score"] = round(_theme_association_base_score(association), 3)
        entity_key = str(association.get("normalized_key") or association.get("candidate_name") or "").strip()
        theme_id = str(association.get("theme_id") or "").strip()
        if entity_key:
            by_entity.setdefault(entity_key, []).append(association)
        if theme_id:
            by_theme.setdefault(theme_id, []).append(association)
    for group in by_entity.values():
        group.sort(key=lambda item: (-_theme_association_base_score(item), str(item.get("theme_label") or "").lower()))
        for index, association in enumerate(group, start=1):
            association["entity_theme_rank"] = index
            association["entity_theme_count"] = len(group)
            association["entity_theme_role"] = _entity_theme_role(index)
            association["ranking_score"] = _rank_adjusted_theme_association_score(_theme_association_base_score(association), index)
    for group in by_theme.values():
        group.sort(
            key=lambda item: (
                -_safe_float(item.get("ranking_score")),
                -_safe_float(item.get("theme_adjusted_lore_prior")),
                str(item.get("candidate_name") or "").lower(),
            )
        )
        for index, association in enumerate(group, start=1):
            association["theme_candidate_rank"] = index
            association["theme_candidate_count"] = len(group)


def _theme_learning_payload(root: Path, repo_root: Path) -> dict[str, Any]:
    migrate_run_artifacts_to_numbered(root)
    paths = ArtifactPaths(root)
    profile_path = _theme_profile_path(repo_root)
    profile = _load_json_payload(
        profile_path,
        {"schema_version": 1, "updated_at_utc": None, "policy": {}, "themes": [], "theme_update_log": []},
    )
    report = _load_json_payload(paths.theme_profile_update_report, {"summary": {}, "applied_theme_updates": [], "raw_theme_updates": []})
    reclassification = _load_json_payload(paths.theme_candidate_reclassification, {"summary": {}, "candidate_reclassifications": []})

    themes = [
        theme
        for theme in (profile.get("themes", []) if isinstance(profile, dict) else [])
        if isinstance(theme, dict)
    ]
    theme_labels = {
        str(theme.get("theme_id") or "").strip(): str(theme.get("label") or theme.get("theme_id") or "").strip()
        for theme in themes
    }
    associations: list[dict[str, Any]] = []
    association_counts_by_theme: dict[str, int] = {}
    for row in reclassification.get("candidate_reclassifications", []) if isinstance(reclassification, dict) else []:
        if not isinstance(row, dict):
            continue
        candidate_name = str(row.get("candidate_name") or "").strip()
        normalized_key = str(row.get("normalized_key") or normalized_name_key(candidate_name)).strip()
        for match in row.get("theme_matches", []) or []:
            if not isinstance(match, dict):
                continue
            theme_id = str(match.get("theme_id") or "").strip()
            if not theme_id:
                continue
            theme_label = str(match.get("label") or theme_labels.get(theme_id) or theme_id).strip()
            association_counts_by_theme[theme_id] = association_counts_by_theme.get(theme_id, 0) + 1
            associations.append(
                {
                    "association_id": _theme_learning_association_id(theme_id, normalized_key, candidate_name),
                    "theme_id": theme_id,
                    "theme_label": theme_label,
                    "theme_status": str(match.get("status") or ""),
                    "candidate_id": str(row.get("candidate_id") or ""),
                    "candidate_name": candidate_name,
                    "normalized_key": normalized_key,
                    "match_strength": match.get("match_strength"),
                    "prior_boost": match.get("prior_boost"),
                    "theme_match_score": match.get("theme_match_score"),
                    "ranking_score": match.get("ranking_score"),
                    "entity_theme_rank": match.get("entity_theme_rank"),
                    "entity_theme_count": match.get("entity_theme_count"),
                    "entity_theme_role": str(match.get("entity_theme_role") or ""),
                    "theme_candidate_rank": match.get("theme_candidate_rank"),
                    "theme_candidate_count": match.get("theme_candidate_count"),
                    "matched_indicators": _clean_text_list(match.get("matched_indicators", [])),
                    "reason": str(match.get("reason") or ""),
                    "theme_prior_boost": row.get("theme_prior_boost"),
                    "theme_adjusted_lore_prior": row.get("theme_adjusted_lore_prior"),
                    "base_local_lore_prior": row.get("base_local_lore_prior"),
                    "base_external_reference_prior": row.get("base_external_reference_prior"),
                    "externality_class": str(row.get("externality_class") or ""),
                    "base_recommended_action": str(row.get("base_recommended_action") or ""),
                    "base_recommended_track": str(row.get("base_recommended_track") or ""),
                    "theme_adjusted_recommended_action": str(row.get("theme_adjusted_recommended_action") or ""),
                    "theme_adjusted_recommended_track": str(row.get("theme_adjusted_recommended_track") or ""),
                    "theme_reclassification_source": str(row.get("theme_reclassification_source") or ""),
                    "model_reclassification_status": str(row.get("model_reclassification_status") or ""),
                    "model_reasoning_summary": str(row.get("model_reasoning_summary") or ""),
                    "why_not_auto_promote": str(row.get("why_not_auto_promote") or ""),
                    "human_review_question": str(row.get("human_review_question") or ""),
                }
            )
    _rank_theme_learning_associations(associations)
    associations.sort(
        key=lambda item: (
            str(item.get("theme_label") or "").lower(),
            int(item.get("theme_candidate_rank") or 999999),
            -_safe_float(item.get("ranking_score")),
            str(item.get("candidate_name") or "").lower(),
        )
    )
    update_summary = report.get("summary", {}) if isinstance(report.get("summary"), dict) else {}
    reclassification_summary = reclassification.get("summary", {}) if isinstance(reclassification.get("summary"), dict) else {}
    return {
        "active_root": str(root),
        "theme_profile_path": str(profile_path),
        "update_report_path": str(paths.theme_profile_update_report),
        "reclassification_path": str(paths.theme_candidate_reclassification),
        "theme_count": len(themes),
        "association_count": len(associations),
        "themes": _json_safe(themes),
        "associations": _json_safe(associations),
        "policy": _json_safe(profile.get("policy", {}) if isinstance(profile, dict) else {}),
        "theme_update_log": _json_safe(profile.get("theme_update_log", []) if isinstance(profile, dict) else []),
        "applied_theme_updates": _json_safe(report.get("applied_theme_updates", []) if isinstance(report, dict) else []),
        "raw_theme_updates": _json_safe(report.get("raw_theme_updates", []) if isinstance(report, dict) else []),
        "summary": {
            "theme_profile_exists": profile_path.exists(),
            "theme_profile_updated_at_utc": profile.get("updated_at_utc") if isinstance(profile, dict) else "",
            "theme_status_counts": _theme_status_counts(themes),
            "association_counts_by_theme": association_counts_by_theme,
            "theme_profile_schema_version": profile.get("schema_version") if isinstance(profile, dict) else None,
            "theme_miner_summary": update_summary,
            "theme_reclassification_summary": reclassification_summary,
            "theme_miner_generated_at_utc": report.get("generated_at_utc") if isinstance(report, dict) else "",
            "theme_reclassification_generated_at_utc": reclassification.get("generated_at_utc") if isinstance(reclassification, dict) else "",
            "evidence_packet_count": int((report.get("inputs", {}) if isinstance(report.get("inputs"), dict) else {}).get("evidence_packet_count", 0) or 0),
        },
    }


def _theme_association_sort_key(item: dict[str, Any]) -> tuple[int, float, int, str]:
    return (
        _safe_int(item.get("entity_theme_rank"), 999999),
        -_safe_float(item.get("ranking_score")),
        _safe_int(item.get("theme_candidate_rank"), 999999),
        str(item.get("theme_label") or "").lower(),
    )


def _theme_associations_by_entity(root: Path, repo_root: Path) -> dict[str, list[dict[str, Any]]]:
    payload = _theme_learning_payload(root, repo_root)
    associations = payload.get("associations", []) if isinstance(payload, dict) else []
    by_key: dict[str, list[dict[str, Any]]] = {}
    for association in associations:
        if not isinstance(association, dict):
            continue
        keys = {
            normalized_name_key(str(association.get("normalized_key") or "")),
            normalized_name_key(str(association.get("candidate_name") or "")),
        }
        for key in keys:
            if key:
                by_key.setdefault(key, []).append(association)
    for group in by_key.values():
        group.sort(key=_theme_association_sort_key)
    return by_key


def _pick_better_theme_association(
    current: dict[str, Any] | None,
    candidate: dict[str, Any],
) -> dict[str, Any]:
    if current is None:
        return candidate
    return candidate if _theme_association_sort_key(candidate) < _theme_association_sort_key(current) else current


def _entity_row_theme_keys(row: dict[str, Any]) -> list[str]:
    values: list[str] = []
    for field in ("candidate_name", "raw_candidate_name", "canonical_name"):
        values.append(str(row.get(field) or ""))
    for alias in row.get("aliases", []) or []:
        values.append(str(alias))
    item = row.get("item", {}) if isinstance(row.get("item"), dict) else {}
    for field in (
        "candidate_name",
        "raw_candidate_name",
        "canonical_name",
        "normalized_key",
        "normalized_name_key",
        "target_entity_name",
        "source_entity_name",
        "entity_id",
    ):
        values.append(str(item.get(field) or ""))
    for alias in item.get("aliases", []) or []:
        values.append(str(alias.get("canonical_name") or alias.get("candidate_name") or alias) if isinstance(alias, dict) else str(alias))
    for alias in item.get("alias_candidates", []) or []:
        values.append(str(alias.get("candidate_name") or alias.get("canonical_name") or alias) if isinstance(alias, dict) else str(alias))
    for source in item.get("merged_from_entities", []) or []:
        if not isinstance(source, dict):
            continue
        values.append(str(source.get("canonical_name") or source.get("entity_id") or ""))
        for alias in source.get("aliases", []) or []:
            values.append(str(alias))

    keys: list[str] = []
    seen: set[str] = set()
    for value in values:
        key = normalized_name_key(value)
        if key and key not in seen:
            keys.append(key)
            seen.add(key)
    return keys


def _theme_associations_for_entity_row(
    row: dict[str, Any],
    associations_by_entity: dict[str, list[dict[str, Any]]],
) -> list[dict[str, Any]]:
    by_theme: dict[str, dict[str, Any]] = {}
    for key in _entity_row_theme_keys(row):
        for association in associations_by_entity.get(key, []):
            theme_id = str(association.get("theme_id") or "").strip()
            if not theme_id:
                continue
            by_theme[theme_id] = _pick_better_theme_association(by_theme.get(theme_id), association)
    out = list(by_theme.values())
    out.sort(key=_theme_association_sort_key)
    return _json_safe(out)


def _annotate_entity_rows_with_theme_associations(
    rows: list[dict[str, Any]],
    associations_by_entity: dict[str, list[dict[str, Any]]],
    *,
    limit: int = 8,
) -> list[dict[str, Any]]:
    annotated: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        associations = _theme_associations_for_entity_row(row, associations_by_entity) if associations_by_entity else []
        annotated.append(
            {
                **row,
                "theme_associations": associations[:limit],
                "theme_association_count": len(associations),
            }
        )
    return _json_safe(annotated)


def handle_theme_learning(repo_root: Path, payload: dict[str, Any]) -> dict[str, Any]:
    active = _active_root(repo_root, payload)
    return _theme_learning_payload(active, repo_root) if active.exists() else {
        "active_root": str(active),
        "theme_profile_path": str(_theme_profile_path(repo_root)),
        "update_report_path": str(ArtifactPaths(active).theme_profile_update_report),
        "reclassification_path": str(ArtifactPaths(active).theme_candidate_reclassification),
        "theme_count": 0,
        "association_count": 0,
        "themes": [],
        "associations": [],
        "policy": {},
        "theme_update_log": [],
        "applied_theme_updates": [],
        "raw_theme_updates": [],
        "summary": {"theme_profile_exists": _theme_profile_path(repo_root).exists()},
    }


def handle_entity_evidence(repo_root: Path, payload: dict[str, Any]) -> dict[str, Any]:
    active = _active_root(repo_root, payload)
    row_id = str(payload.get("row_id") or "").strip()
    view = str(payload.get("view") or "").strip().lower()
    if not row_id:
        raise ValueError("Missing row_id.")
    if view == "merged" or row_id.startswith("merged_entity:"):
        merged = _merged_entity_rows(active, repo_root)
        rows = merged.get("rows", []) if isinstance(merged, dict) else []
    else:
        rows = _entity_rows(active) if active.exists() else []
    row = next((item for item in rows if str(item.get("row_id") or "") == row_id), None)
    if row is None:
        raise ValueError(f"Unknown entity row: {row_id}")
    item = row.get("item", {}) if isinstance(row.get("item"), dict) else {}
    theme_associations_by_entity = _theme_associations_by_entity(active, repo_root) if active.exists() else {}
    theme_associations = _theme_associations_for_entity_row(row, theme_associations_by_entity) if theme_associations_by_entity else []
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
            "theme_associations": theme_associations,
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
        "theme_associations": theme_associations,
    }


def handle_draft_cards(repo_root: Path, payload: dict[str, Any]) -> dict[str, Any]:
    active = _active_root(repo_root, payload)
    return _draft_card_rows(active) if active.exists() else {"active_root": str(active), "metadata": {}, "cards": [], "total": 0, "failures": []}


def handle_entity_relationships(repo_root: Path, payload: dict[str, Any]) -> dict[str, Any]:
    active = _active_root(repo_root, payload)
    return _relationship_graph(active, repo_root) if active.exists() else {"active_root": str(active), "nodes": [], "edges": [], "metadata": {}}


def handle_card_agent_activity(repo_root: Path, payload: dict[str, Any]) -> dict[str, Any]:
    from pipeline.cardbase_agent import card_agent_activity_payload

    active = _active_root(repo_root, payload)
    return card_agent_activity_payload(active) if active.exists() else {"active_root": str(active), "transactions": [], "total": 0}


def handle_card_agent_progress(repo_root: Path, payload: dict[str, Any]) -> dict[str, Any]:
    from pipeline.cardbase_agent import card_agent_progress_payload

    active = _active_root(repo_root, payload)
    max_lines = max(1, int(payload.get("max_lines") or 80))
    return card_agent_progress_payload(active, max_lines=max_lines)


def handle_run_card_agent_request(repo_root: Path, payload: dict[str, Any]) -> dict[str, Any]:
    from pipeline.cardbase_agent import card_agent_activity_payload, run_card_agent_request

    active = _active_root(repo_root, payload)
    instruction_text = str(payload.get("instruction_text") or payload.get("request_text") or "").strip()
    if not instruction_text:
        raise ValueError("Missing instruction_text.")
    result = run_card_agent_request(
        artifacts_root=active,
        instruction_text=instruction_text,
        requester=str(payload.get("requester") or "desktop_user"),
        target_text=str(payload.get("target_text") or ""),
        rationale=str(payload.get("rationale") or ""),
        review_memory_path=repo_root / "canon" / "review_memory.json",
        config_path=repo_root / "config" / "pipeline_config.json",
        max_steps=max(1, int(payload.get("max_steps") or 16)),
    )
    activity = card_agent_activity_payload(active)
    activity["last_run"] = result
    return activity


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


def handle_theme_rescue(repo_root: Path, payload: dict[str, Any]) -> dict[str, Any]:
    active = _active_root(repo_root, payload)
    return theme_rescue_status_payload(active, repo_root)


def handle_approve_theme_rescue(repo_root: Path, payload: dict[str, Any]) -> dict[str, Any]:
    active = _active_root(repo_root, payload)
    write_theme_rescue_approval(
        active,
        approved_by=str(payload.get("approved_by") or "desktop_user"),
        note=str(payload.get("note") or ""),
    )
    return theme_rescue_status_payload(active, repo_root)


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
    "app_config": handle_app_config,
    "save_app_config": handle_save_app_config,
    "select_bootstrap_doc": handle_select_bootstrap_doc,
    "select_run": handle_select_run,
    "create_run": handle_create_run,
    "identity_clusters": handle_identity_clusters,
    "identity_cluster_decision": handle_identity_cluster_decision,
    "identity_edge_decision": handle_identity_edge_decision,
    "claim_inventory": handle_claim_inventory,
    "claim_decision": handle_claim_decision,
    "entity_inventory": handle_entity_inventory,
    "theme_learning": handle_theme_learning,
    "theme_rescue": handle_theme_rescue,
    "approve_theme_rescue": handle_approve_theme_rescue,
    "entity_evidence": handle_entity_evidence,
    "entity_decision": handle_entity_decision,
    "draft_cards": handle_draft_cards,
    "entity_relationships": handle_entity_relationships,
    "card_agent_activity": handle_card_agent_activity,
    "card_agent_progress": handle_card_agent_progress,
    "run_card_agent_request": handle_run_card_agent_request,
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
