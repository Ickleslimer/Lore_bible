"""
Run Stage 11–12 for a small set of lore entities (e.g. Enoch, Krypteia).

Requires a source pipeline run that already has resolved entities and claim drafts
(typically after claim review on a full or partial run). Copies a filtered artifact
subset into a new run root, synthesizes cards for the targets only, optionally
auto-approves them, then runs Stage 12 export and Notion canonical sync.
"""

from __future__ import annotations

import argparse
import json
import shutil
from pathlib import Path
from typing import Any

from pipeline.artifact_paths import ArtifactPaths, migrate_run_artifacts_to_numbered
from pipeline.common import get_logger, now_utc_iso, read_json, read_jsonl, setup_logging, write_json, write_jsonl
from pipeline.entity_resolution import normalized_name_key
from pipeline.card_architecture_agent import ensure_card_architecture_files
from pipeline.notion_draft_sync import sync_canonical_cards_to_notion, sync_draft_cards_to_notion
from pipeline.card_first_review import load_snippet_clusters, snippet_ids_for_entity
from pipeline.stage_11_card_synthesis import (
    default_snippet_clusters_lore_path,
    entity_matches_target_names,
    finalize_approved_card_drafts,
    run as run_stage_11,
)
from pipeline.stage_12_notion_export import run as run_stage_12


def _parse_entity_list(raw: str) -> list[str]:
    return [part.strip() for part in str(raw or "").split(",") if part.strip()]


def _copy_if_exists(src: Path, dst: Path) -> bool:
    if not src.exists():
        return False
    dst.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src, dst)
    return True


def _filter_resolved_entities(payload: dict[str, Any], target_keys: set[str]) -> dict[str, Any]:
    resolved = payload.get("resolved_entities", []) if isinstance(payload, dict) else []
    seed_only = payload.get("seed_only_entities", []) if isinstance(payload, dict) else []
    matched_resolved = [row for row in resolved if isinstance(row, dict) and entity_matches_target_names(row, target_keys)]
    matched_seed = [row for row in seed_only if isinstance(row, dict) and entity_matches_target_names(row, target_keys)]
    out = dict(payload)
    out["resolved_entities"] = matched_resolved
    out["seed_only_entities"] = matched_seed
    return out


def _claim_targets_entity(claim: dict[str, Any], target_keys: set[str], entity_ids: set[str]) -> bool:
    entity_id = str(claim.get("target_entity_id", "")).strip()
    if entity_id and entity_id in entity_ids:
        return True
    name = str(claim.get("target_entity_name") or claim.get("canonical_name") or "").strip()
    return bool(name) and normalized_name_key(name) in target_keys


def _filter_claim_drafts(payload: dict[str, Any], target_keys: set[str], entity_ids: set[str]) -> dict[str, Any]:
    claims = payload.get("claims", []) if isinstance(payload, dict) else []
    filtered = [claim for claim in claims if isinstance(claim, dict) and _claim_targets_entity(claim, target_keys, entity_ids)]
    return {"claims": filtered}


def _filter_claim_decisions(payload: dict[str, Any], claim_ids: set[str]) -> dict[str, Any]:
    decisions = payload.get("decisions", []) if isinstance(payload, dict) else []
    filtered = [row for row in decisions if isinstance(row, dict) and str(row.get("claim_id", "")) in claim_ids]
    return {"decisions": filtered}


def _auto_accept_claim_decisions(claims: list[dict[str, Any]], reviewer: str) -> dict[str, Any]:
    decisions = []
    for claim in claims:
        claim_id = str(claim.get("claim_id", "")).strip()
        if not claim_id:
            continue
        decisions.append(
            {
                "claim_id": claim_id,
                "decision": "accept",
                "reviewer": reviewer,
                "rationale": "Auto-accepted for targeted wiki seed run.",
                "timestamp_utc": now_utc_iso(),
            }
        )
    return {"decisions": decisions}


def _filter_snippets_jsonl(path: Path, snippet_ids: set[str], out_path: Path) -> int:
    if not path.exists():
        write_jsonl(out_path, [])
        return 0
    rows = [row for row in read_jsonl(path) if str(row.get("snippet_id", "")) in snippet_ids]
    write_jsonl(out_path, rows)
    return len(rows)


def _collect_snippet_ids(claims: list[dict[str, Any]]) -> set[str]:
    ids: set[str] = set()
    for claim in claims:
        for key in ("source_snippet_ids", "snippet_ids"):
            values = claim.get(key, [])
            if isinstance(values, list):
                ids.update(str(value).strip() for value in values if str(value).strip())
    return ids


def _filter_development_history(payload: dict[str, Any], entity_ids: set[str]) -> dict[str, Any]:
    if not isinstance(payload, dict):
        return {"entities": {}}
    entities = payload.get("entities", {})
    if not isinstance(entities, dict):
        return payload
    filtered = {entity_id: entries for entity_id, entries in entities.items() if entity_id in entity_ids}
    out = dict(payload)
    out["entities"] = filtered
    return out


def _write_card_approve_decisions(cards: list[dict[str, Any]], reviewer: str) -> dict[str, Any]:
    decisions = []
    for card in cards:
        card_id = str(card.get("card_id", "")).strip()
        if not card_id:
            continue
        decisions.append(
            {
                "card_id": card_id,
                "decision": "approve",
                "reviewer": reviewer,
                "rationale": "Auto-approved for targeted wiki seed run.",
                "timestamp_utc": now_utc_iso(),
            }
        )
    return {"decisions": decisions}


def prepare_targeted_run(
    source_root: Path,
    dest_root: Path,
    entity_names: list[str],
    *,
    auto_accept_claims: bool = False,
) -> dict[str, Any]:
    migrate_run_artifacts_to_numbered(source_root)
    migrate_run_artifacts_to_numbered(dest_root)
    source = ArtifactPaths(source_root)
    dest = ArtifactPaths(dest_root)
    target_keys = {normalized_name_key(name) for name in entity_names}

    resolved_payload = read_json(source.resolved_entities) if source.resolved_entities.exists() else {"resolved_entities": []}
    filtered_resolved = _filter_resolved_entities(resolved_payload, target_keys)
    matched_entities = filtered_resolved.get("resolved_entities", [])
    entity_ids = {str(row.get("entity_id", "")).strip() for row in matched_entities if str(row.get("entity_id", "")).strip()}

    dest.stage06.mkdir(parents=True, exist_ok=True)
    dest.stage09.mkdir(parents=True, exist_ok=True)
    dest.stage10.mkdir(parents=True, exist_ok=True)
    dest.stage11.mkdir(parents=True, exist_ok=True)
    write_json(dest.resolved_entities, filtered_resolved)
    _copy_if_exists(source.alias_map, dest.alias_map)

    claims_payload = read_json(source.claim_drafts) if source.claim_drafts.exists() else {"claims": []}
    filtered_claims = _filter_claim_drafts(claims_payload, target_keys, entity_ids)
    claims = filtered_claims.get("claims", [])
    write_json(dest.claim_drafts, filtered_claims)

    claim_ids = {str(claim.get("claim_id", "")).strip() for claim in claims if str(claim.get("claim_id", "")).strip()}
    if auto_accept_claims:
        write_json(dest.claim_review_decisions, _auto_accept_claim_decisions(claims, "targeted_card_run"))
    elif source.claim_review_decisions.exists():
        write_json(
            dest.claim_review_decisions,
            _filter_claim_decisions(read_json(source.claim_review_decisions), claim_ids),
        )
    else:
        write_json(dest.claim_review_decisions, {"decisions": []})

    dest.stage08.mkdir(parents=True, exist_ok=True)
    _copy_if_exists(source.snippet_clusters_lore, dest.snippet_clusters_lore)
    _copy_if_exists(source.snippet_clusters_meta, dest.snippet_clusters_meta)

    snippet_ids = _collect_snippet_ids(claims)
    lore_clusters = load_snippet_clusters(source.snippet_clusters_lore)
    cluster_snippet_count = 0
    for entity in matched_entities:
        entity_snippet_ids = snippet_ids_for_entity(
            entity,
            lore_clusters,
            max_snippets=100_000,
        )
        cluster_snippet_count += len(entity_snippet_ids)
        snippet_ids.update(entity_snippet_ids)
    snippet_count = _filter_snippets_jsonl(source.effective_snippets(), snippet_ids, dest.effective_snippets())

    if source.entity_development_history.exists():
        write_json(
            dest.entity_development_history,
            _filter_development_history(read_json(source.entity_development_history), entity_ids),
        )

    _copy_if_exists(source.identity_merge_proposals, dest.identity_merge_proposals)
    _copy_if_exists(source.identity_merge_decisions, dest.identity_merge_decisions)
    _copy_if_exists(source.meta_cards_draft, dest.meta_cards_draft)
    _copy_if_exists(source.author_directives, dest.author_directives)

    if not dest.identity_merge_decisions.exists():
        write_json(dest.identity_merge_decisions, {"decisions": []})
    if not dest.identity_merge_proposals.exists():
        write_json(
            dest.identity_merge_proposals,
            {
                "generated_at_utc": now_utc_iso(),
                "proposals": [],
                "decisions_path": str(dest.identity_merge_decisions),
            },
        )
    if not dest.meta_cards_draft.exists():
        write_json(dest.meta_cards_draft, {"meta_cards": []})
    if not dest.author_directives.exists():
        write_json(dest.author_directives, {"directives": []})
    _copy_if_exists(source.card_architecture_proposals, dest.card_architecture_proposals)
    _copy_if_exists(source.card_architecture_decisions, dest.card_architecture_decisions)
    ensure_card_architecture_files(dest.stage11)
    write_json(dest.card_review_decisions, {"decisions": []})
    write_json(dest.canonical_cards, {"cards": []})
    write_jsonl(dest.merge_log, [])

    _copy_if_exists(source.source_profiles, dest.source_profiles)

    missing_names = [name for name in entity_names if not any(entity_matches_target_names(row, {normalized_name_key(name)}) for row in matched_entities)]
    return {
        "entity_names": entity_names,
        "matched_entities": [row.get("canonical_name") for row in matched_entities],
        "entity_ids": sorted(entity_ids),
        "claim_count": len(claims),
        "cluster_snippet_count": cluster_snippet_count,
        "snippet_count": snippet_count,
        "missing_names": missing_names,
    }


def run_targeted_pipeline(
    dest_root: Path,
    entity_names: list[str],
    *,
    config_path: Path = Path("config/pipeline_config.json"),
    env_path: Path = Path(".env"),
    auto_approve_cards: bool = False,
    sync_notion: bool = True,
) -> dict[str, Any]:
    logger = get_logger(__name__)
    paths = ArtifactPaths(dest_root)
    memory_path = Path("canon/review_memory.json")

    logger.info("Stage 11 targeted synthesis for: %s", ", ".join(entity_names))
    run_stage_11(
        paths.resolved_entities,
        paths.claim_drafts,
        paths.claim_review_decisions,
        paths.card_review_decisions,
        paths.author_directives,
        memory_path,
        paths.card_drafts,
        paths.canonical_cards,
        paths.merge_log,
        config_path,
        paths.effective_snippets(),
        paths.entity_development_history,
        target_entity_names=entity_names,
    )

    draft_payload = read_json(paths.card_drafts)
    draft_cards = draft_payload.get("cards", []) if isinstance(draft_payload, dict) else []

    if auto_approve_cards and draft_cards:
        write_json(paths.card_review_decisions, _write_card_approve_decisions(draft_cards, "targeted_card_run"))
        canonical_cards = finalize_approved_card_drafts(
            paths.card_drafts,
            paths.card_review_decisions,
            paths.canonical_cards,
        )
        logger.info("Auto-approved %d draft card(s) into canonical_cards.json", len(canonical_cards))
    else:
        canonical_cards = read_json(paths.canonical_cards).get("cards", []) if paths.canonical_cards.exists() else []

    logger.info("Stage 12 Notion NDJSON export")
    run_stage_12(
        paths.canonical_cards,
        paths.meta_cards_draft,
        paths.alias_map,
        paths.effective_snippets(),
        paths.source_profiles,
        paths.merge_log,
        paths.notion_import,
    )

    notion_reports: dict[str, Any] = {}
    if sync_notion:
        notion_reports["draft"] = sync_draft_cards_to_notion(dest_root, config_path, env_path)
        notion_reports["canonical"] = sync_canonical_cards_to_notion(dest_root, config_path, env_path)

    return {
        "artifacts_root": str(dest_root),
        "draft_cards": len(draft_cards),
        "canonical_cards": len(canonical_cards),
        "notion": notion_reports,
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run Stage 11–12 for specific entities using artifacts from an existing pipeline run.",
    )
    parser.add_argument(
        "--source-run",
        type=Path,
        required=True,
        help="Existing run with resolved_entities.json and claim_drafts.json (e.g. artifacts/runs/<full_run>).",
    )
    parser.add_argument(
        "--artifacts-root",
        type=Path,
        required=True,
        help="Output run root for the targeted wiki seed (created/populated).",
    )
    parser.add_argument(
        "--entities",
        type=str,
        default="Enoch,Krypteia",
        help="Comma-separated canonical entity names (default: Enoch,Krypteia).",
    )
    parser.add_argument(
        "--auto-accept-claims",
        action="store_true",
        help="Accept all filtered claim drafts for the targets (use when claims are not yet reviewed).",
    )
    parser.add_argument(
        "--auto-approve-cards",
        action="store_true",
        help="After synthesis, approve all draft cards and write canonical_cards.json.",
    )
    parser.add_argument("--skip-notion", action="store_true", help="Skip Notion draft/canonical sync.")
    parser.add_argument("--config", type=Path, default=Path("config/pipeline_config.json"))
    parser.add_argument("--env", type=Path, default=Path(".env"))
    args = parser.parse_args()

    setup_logging()
    logger = get_logger(__name__)
    entity_names = _parse_entity_list(args.entities)
    if not entity_names:
        raise SystemExit("No entities specified.")

    source_root = args.source_run.resolve()
    dest_root = args.artifacts_root.resolve()
    if not source_root.exists():
        raise SystemExit(f"Source run not found: {source_root}")

    dest_root.mkdir(parents=True, exist_ok=True)
    prep = prepare_targeted_run(
        source_root,
        dest_root,
        entity_names,
        auto_accept_claims=args.auto_accept_claims,
    )
    logger.info("Prepared targeted run: %s", json.dumps(prep, ensure_ascii=False))
    if prep.get("missing_names"):
        logger.warning("No resolved entity matched: %s", ", ".join(prep["missing_names"]))
    if not prep.get("matched_entities"):
        raise SystemExit("No target entities matched in source run resolved_entities.json.")
    if prep.get("claim_count", 0) == 0 and prep.get("cluster_snippet_count", 0) == 0:
        raise SystemExit("No claim drafts or lore snippet clusters found for the target entities in the source run.")

    summary = run_targeted_pipeline(
        dest_root,
        entity_names,
        config_path=args.config,
        env_path=args.env,
        auto_approve_cards=args.auto_approve_cards,
        sync_notion=not args.skip_notion,
    )
    print(json.dumps(summary, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
