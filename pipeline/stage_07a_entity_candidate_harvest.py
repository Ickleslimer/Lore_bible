from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any

from pipeline.common import get_logger, now_utc_iso, read_json, read_jsonl, stable_id, write_json
from pipeline.entity_resolution import (
    clean_candidate_name,
    display_name,
    load_entity_records,
    normalize_entity_type,
    normalized_name_key,
    resolve_entities,
)
from pipeline.review_memory import load_review_memory
from pipeline.stage_07_entity_resolution import (
    CANON_ADOPTION_MARKERS,
    CONVERSATION_ENTITY_NAME_STOPWORDS,
    EXTERNAL_MEDIA_REFERENCE_MARKERS,
    META_PROJECT_CONTEXT_MARKERS,
    META_TEAM_ROLE_MARKERS,
    REFERENCE_INSPIRATION_MARKERS,
    _entity_for_candidate_key,
    conversation_entity_seed_records_from_memory,
    infer_type_evidence_for_candidate,
    is_external_media_reference_candidate,
    is_generic_conversation_entity_name,
    is_low_value_phrase_name,
    is_meta_team_contributor_candidate,
    is_reference_inspiration_candidate,
    is_structured_alias_evidence,
    latest_proposal_timestamp,
    promote_band_grouped_quest_candidates,
    proposal_evidence_text,
    proposal_recency_metrics,
    re_contains_name,
    record_conversation_entity_proposal,
    refresh_type_review_fields,
    snippet_evidence_text,
    text_marker_hits,
    triage_conversation_entity_proposal,
)
from pipeline.model_provider import call_model_chat, model_call_kwargs


HARVEST_SCHEMA_VERSION = 1
TASK_NAME = "stage_07a_entity_candidate_harvest"
HARVEST_SOURCE_FIELDS = ("candidate_entities", "patch_candidate_entities", "conversation_anchor_entities")
MAX_HARVEST_NAME_WORDS = 12
MAX_HARVEST_NAME_CHARS = 120
DEFAULT_MAX_MODEL_CANDIDATES_PER_CALL = 24
DEFAULT_MODEL_CHUNK_RETRY_ATTEMPTS = 2
SOURCE_PROFILE_FIELDS = (
    "thread_id",
    "partner_id",
    "partner_label",
    "conversation_id",
    "dm_pair_id",
    "conversation_topic_label",
    "conversation_relevance_type",
    "speaker",
)


def run(
    in_snippets_jsonl: Path,
    in_seed_json: Path,
    out_alias_json: Path,
    out_timeline_json: Path,
    out_resolved_entities_json: Path | None = None,
    in_review_memory_json: Path | None = None,
    out_entity_candidate_harvest_json: Path | None = None,
    in_pipeline_config_json: Path | None = None,
) -> None:
    logger = get_logger(__name__)
    snippets = read_jsonl(in_snippets_jsonl)
    snippets.sort(key=lambda row: (row.get("timestamp_start_utc", ""), row.get("snippet_id", "")))
    provider_config = read_json(in_pipeline_config_json) if in_pipeline_config_json and in_pipeline_config_json.exists() else {}
    review_memory = load_review_memory(in_review_memory_json)
    seed_entities = load_entity_records(in_seed_json) + conversation_entity_seed_records_from_memory(review_memory)
    resolved_payload = resolve_entities(seed_entities, review_memory)
    all_resolved_entities = resolved_payload.get("resolved_entities", [])

    name_targets: dict[str, dict[str, Any]] = {}
    for entity in all_resolved_entities:
        names = [entity.get("canonical_name", ""), *list(entity.get("aliases", []) or [])]
        for name in names:
            key = normalized_name_key(str(name))
            if key:
                name_targets[key] = entity

    timelines: dict[str, list[dict[str, Any]]] = {}
    seen_aliases: dict[tuple[str, str], dict[str, Any]] = {}
    candidate_records_by_key: dict[str, dict[str, Any]] = {}
    approved_memory_keys, rejected_memory_keys = review_memory_candidate_keys(review_memory)

    def record_observation(
        *,
        entity: dict[str, Any],
        matched_name: str,
        snip: dict[str, Any],
        match_type: str,
        add_alias: bool,
    ) -> None:
        linked_entity_id = str(entity.get("entity_id"))
        linked_card_id = str(entity.get("card_id"))
        if add_alias:
            alias_key = (linked_entity_id, matched_name)
            if alias_key not in seen_aliases:
                seen_aliases[alias_key] = {
                    "alias_id": stable_id("alias", linked_entity_id, matched_name),
                    "entity_id": linked_entity_id,
                    "entity_card_id": linked_card_id,
                    "alias_text": matched_name,
                    "alias_type": "working_name",
                    "first_seen_timestamp_utc": snip.get("timestamp_start_utc", ""),
                    "last_seen_timestamp_utc": snip.get("timestamp_end_utc", snip.get("timestamp_start_utc", "")),
                    "source_snippet_ids": [snip.get("snippet_id", "")],
                    "resolution_confidence": snip.get("relevance_score", 0.5),
                    "resolution_status": "resolved",
                    "notes": "Auto-linked by canonical name mention.",
                }
            else:
                entry = seen_aliases[alias_key]
                entry["last_seen_timestamp_utc"] = snip.get("timestamp_end_utc", snip.get("timestamp_start_utc", ""))
                if snip.get("snippet_id") not in entry["source_snippet_ids"]:
                    entry["source_snippet_ids"].append(snip.get("snippet_id", ""))

        timeline_entry = {
            "timestamp_utc": snip.get("timestamp_start_utc", ""),
            "snippet_id": snip.get("snippet_id", ""),
            "text": snip.get("display_text_normalized", ""),
            "status": "revision_candidate",
            "match_type": match_type,
        }
        existing = timelines.setdefault(linked_entity_id, [])
        if not any(
            item.get("snippet_id") == timeline_entry["snippet_id"]
            and item.get("match_type") == timeline_entry["match_type"]
            for item in existing
        ):
            existing.append(timeline_entry)

    logger.info("Stage 07A: harvesting entity candidates from %d snippet(s).", len(snippets))
    heartbeat_every = max(1, min(1000, max(100, len(snippets) // 20 or 1)))
    for snippet_index, snip in enumerate(snippets, start=1):
        evidence_text = snippet_evidence_text(snip)
        lower = evidence_text.lower()
        known_hits = known_entity_hits(lower, name_targets)
        for name_key, entity in known_hits.items():
            record_observation(
                entity=entity,
                matched_name=name_key,
                snip=snip,
                match_type="literal_text",
                add_alias=True,
            )

        snippet_candidates = harvestable_snippet_candidates(snip, lower)
        snippet_candidate_keys = [key for _candidate, _field, key in snippet_candidates]
        for candidate, source_field, candidate_key in snippet_candidates:
            target_entity = _entity_for_candidate_key(candidate_key, name_targets)
            if target_entity is not None:
                record_observation(
                    entity=target_entity,
                    matched_name=candidate_key,
                    snip=snip,
                    match_type="candidate_entity_metadata",
                    add_alias=False,
                )
            record_candidate_harvest(
                candidate_records_by_key,
                candidate,
                source_field,
                candidate_key,
                snip,
                known_hits,
                snippet_candidate_keys,
                target_entity,
            )
        if snippet_index == len(snippets) or snippet_index % heartbeat_every == 0:
            logger.info(
                "Stage 07A progress: %d/%d candidates=%d observations=%d",
                snippet_index,
                len(snippets),
                len(candidate_records_by_key),
                sum(len(items) for items in timelines.values()),
            )

    candidates = list(candidate_records_by_key.values())
    promote_band_grouped_quest_candidates(candidates, snippets)
    latest_seen = latest_proposal_timestamp(candidates)
    output_candidates = [
        finalize_candidate(candidate, approved_memory_keys, rejected_memory_keys, latest_seen)
        for candidate in candidates
    ]
    output_candidates.sort(key=candidate_annotation_priority_key)
    model_result = annotate_candidates_with_model(output_candidates, all_resolved_entities, provider_config, logger)
    output_candidates = model_result["candidates"]
    output_candidates.sort(key=lambda item: (-int(item.get("evidence_count", 0) or 0), str(item.get("candidate_name", "")).lower()))

    observed_entity_ids = set(timelines)
    observed_entities_by_id = {
        str(entity.get("entity_id", "")): entity
        for entity in all_resolved_entities
        if str(entity.get("entity_id", "")) in observed_entity_ids
    }
    seed_only_entities = [
        {**entity, "observation_status": "seed_only_unobserved"}
        for entity in all_resolved_entities
        if str(entity.get("entity_id", "")) not in observed_entities_by_id
    ]
    alias_entries = sorted(seen_aliases.values(), key=lambda item: (item["entity_card_id"], item["alias_text"]))
    resolved_output = {
        **resolved_payload,
        "resolved_entities": sorted(observed_entities_by_id.values(), key=lambda item: (item.get("entity_type", ""), item.get("canonical_name", ""))),
        "seed_only_entities": sorted(seed_only_entities, key=lambda item: (item.get("entity_type", ""), item.get("canonical_name", ""))),
        "candidate_harvest_path": str(out_entity_candidate_harvest_json) if out_entity_candidate_harvest_json else "",
        "observation_policy": (
            "Stage 07A only emits timeline/alias observations for seed or review-memory-approved entities. "
            "Unapproved conversation candidates are retained in entity_candidate_harvest.json with Qwen annotations and are not promoted."
        ),
        "all_resolved_seed_entities_count": len(all_resolved_entities),
    }
    harvest_payload = {
        "schema_version": HARVEST_SCHEMA_VERSION,
        "generated_at_utc": now_utc_iso(),
        "stage": "07A_entity_candidate_harvest",
        "inputs": {
            "snippets_jsonl": str(in_snippets_jsonl),
            "entity_seed_json": str(in_seed_json),
            "review_memory_json": str(in_review_memory_json) if in_review_memory_json else "",
            "snippet_count": len(snippets),
            "seed_entity_count": len(seed_entities),
        },
        "policy": {
            "model_calls": "required",
            "model_task": TASK_NAME,
            "model_provider": model_result.get("provider", ""),
            "model_name": model_result.get("api_model", ""),
            "model_candidate_batches": model_result.get("batch_count", 0),
            "review_gate": "disabled",
            "canon_promotion": "approved_or_seed_entities_only",
            "candidate_filter": (
                "Broad harvest keeps generic, meta, external-looking, and previously rejected names when reobserved; "
                "only empty, malformed, numeric-only, or unusably long anchors are discarded."
            ),
            "legacy_triage_hint": "diagnostic_only",
            "model_annotation": (
                "Qwen reviews local evidence packets only. Its annotations affect harvest-review metadata, "
                "not deterministic canon promotion."
            ),
        },
        "summary": {
            "candidate_count": len(output_candidates),
            "model_annotated_candidate_count": sum(1 for item in output_candidates if item.get("model_annotation_status") == "annotated"),
            "qwen_requested_candidate_count": model_result.get("model_candidate_count", len(output_candidates)),
            "qwen_candidate_limit": model_result.get("model_candidate_limit", 0),
            "qwen_fallback_candidate_count": sum(1 for item in output_candidates if item.get("model_annotation_status") == "fallback_after_model_failure"),
            "resolved_entity_count": len(resolved_output["resolved_entities"]),
            "seed_only_entity_count": len(seed_only_entities),
            "alias_count": len(alias_entries),
            "entity_timeline_count": len(timelines),
        },
        "candidates": output_candidates,
    }

    write_json(out_alias_json, {"aliases": alias_entries})
    write_json(out_timeline_json, {"entity_timelines": timelines})
    if out_resolved_entities_json is not None:
        write_json(out_resolved_entities_json, resolved_output)
    if out_entity_candidate_harvest_json is not None:
        write_json(out_entity_candidate_harvest_json, harvest_payload)
    logger.info(
        "Stage 07A complete: candidates=%d resolved_entities=%d seed_only_entities=%d aliases=%d timelines=%d",
        len(output_candidates),
        len(resolved_output["resolved_entities"]),
        len(seed_only_entities),
        len(alias_entries),
        len(timelines),
    )


def known_entity_hits(lower_evidence_text: str, name_targets: dict[str, dict[str, Any]]) -> dict[str, dict[str, Any]]:
    hits: dict[str, dict[str, Any]] = {}
    seen_entity_ids: set[str] = set()
    for name_key, entity in name_targets.items():
        entity_id = str(entity.get("entity_id", "")).strip()
        if entity_id in seen_entity_ids:
            continue
        if name_key and re_contains_name(lower_evidence_text, name_key):
            hits[name_key] = entity
            if entity_id:
                seen_entity_ids.add(entity_id)
    return hits


def annotate_candidates_with_model(
    candidates: list[dict[str, Any]],
    resolved_entities: list[dict[str, Any]],
    provider_config: dict[str, Any],
    logger: Any,
) -> dict[str, Any]:
    task_cfg = stage_task_config(provider_config)
    kwargs = stage_model_kwargs(provider_config, task_cfg)
    if not candidates:
        return {
            "candidates": candidates,
            "batch_count": 0,
            "provider": kwargs.get("provider", ""),
            "api_model": kwargs.get("api_model", ""),
        }
    if not bool(task_cfg.get("enabled", True)):
        for candidate in candidates:
            candidate["model_annotation_status"] = "disabled"
        return {
            "candidates": candidates,
            "batch_count": 0,
            "provider": kwargs.get("provider", ""),
            "api_model": kwargs.get("api_model", ""),
        }

    max_per_call = max(1, int(task_cfg.get("max_candidates_per_call", DEFAULT_MAX_MODEL_CANDIDATES_PER_CALL) or DEFAULT_MAX_MODEL_CANDIDATES_PER_CALL))
    max_model_candidates = max(0, int(task_cfg.get("max_model_candidates_per_run", 0) or 0))
    model_candidates = candidates[:max_model_candidates] if max_model_candidates else candidates
    if max_model_candidates and len(candidates) > len(model_candidates):
        logger.info(
            "Stage 07A: limiting Qwen annotation to top %d/%d candidate(s); remaining candidates receive fallback status.",
            len(model_candidates),
            len(candidates),
        )
    annotations_by_key: dict[str, dict[str, Any]] = {}
    failures: list[dict[str, Any]] = []
    batch_count = (len(model_candidates) + max_per_call - 1) // max_per_call
    known_entities = model_known_entities(resolved_entities)
    model_call_count = 0
    logger.info(
        "Stage 07A: requesting Qwen candidate harvest annotations for %d candidate(s) in %d initial batch(es).",
        len(model_candidates),
        batch_count,
    )
    for batch_index, offset in enumerate(range(0, len(model_candidates), max_per_call), start=1):
        chunk = model_candidates[offset : offset + max_per_call]
        logger.info(
            "Stage 07A model batch %d/%d: candidates=%d offset=%d model=%s",
            batch_index,
            batch_count,
            len(chunk),
            offset,
            kwargs.get("api_model", ""),
        )
        annotations, chunk_failures, chunk_call_count = request_model_annotations_for_chunk(
            chunk,
            known_entities,
            kwargs,
            logger,
            batch_label=f"{batch_index}/{batch_count}",
            offset=offset,
        )
        model_call_count += chunk_call_count
        if chunk_failures:
            failures.extend(chunk_failures)
            continue
        for annotation in annotations:
            key = model_annotation_key(annotation)
            if key:
                annotations_by_key[key] = annotation

    missing_keys = [
        str(candidate.get("normalized_name_key", ""))
        for candidate in candidates
        if str(candidate.get("normalized_name_key", "")) not in annotations_by_key
    ]
    if missing_keys:
        failures.append(
            {
                "reason": "missing_model_annotations",
                "candidate_keys": missing_keys[:50],
                "missing_count": len(missing_keys),
            }
        )
    if failures:
        logger.warning(
            "Stage 07A Qwen annotation pass had %d recoverable failure group(s); "
            "missing candidates will be retained with fallback annotation status. failures=%s",
            len(failures),
            json_preview(failures),
        )

    annotated = [
        apply_model_annotation(
            candidate,
            annotations_by_key.get(str(candidate.get("normalized_name_key", "")))
            or fallback_model_annotation(candidate, "qwen_annotation_missing_or_invalid"),
        )
        for candidate in candidates
    ]
    return {
        "candidates": annotated,
        "batch_count": model_call_count,
        "model_candidate_limit": max_model_candidates,
        "model_candidate_count": len(model_candidates),
        "provider": kwargs.get("provider", ""),
        "api_model": kwargs.get("api_model", ""),
    }


def candidate_annotation_priority_key(candidate: dict[str, Any]) -> tuple[int, int, int, int, str]:
    flags = candidate.get("signal_flags", {}) if isinstance(candidate.get("signal_flags"), dict) else {}
    evidence = int(candidate.get("evidence_count", 0) or 0)
    conflict = int(bool(candidate.get("type_conflicts")))
    external_or_meta = int(bool(flags.get("external_media_marker") or flags.get("inspiration_marker") or flags.get("meta_team_marker")))
    lore_like = int(bool(flags.get("canon_adoption_marker") or flags.get("music_quest_pattern") or "lore" in candidate.get("knowledge_tracks", [])))
    return (-evidence, -conflict, -external_or_meta, -lore_like, str(candidate.get("candidate_name", "")).lower())


def request_model_annotations_for_chunk(
    chunk: list[dict[str, Any]],
    known_entities: list[dict[str, Any]],
    kwargs: dict[str, Any],
    logger: Any,
    batch_label: str,
    offset: int,
    depth: int = 0,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], int]:
    attempts = max(1, int(kwargs.get("annotation_retry_attempts", DEFAULT_MODEL_CHUNK_RETRY_ATTEMPTS) or DEFAULT_MODEL_CHUNK_RETRY_ATTEMPTS))
    prompt = build_candidate_harvest_prompt(chunk, known_entities)
    candidate_keys = [str(item.get("normalized_name_key", "")) for item in chunk]
    attempt_failures: list[dict[str, Any]] = []
    call_count = 0
    for attempt in range(1, attempts + 1):
        call_count += 1
        logger.info(
            "Stage 07A model request %s attempt %d/%d: candidates=%d offset=%d depth=%d",
            batch_label,
            attempt,
            attempts,
            len(chunk),
            offset,
            depth,
        )
        try:
            response = call_model_chat(prompt=prompt, **kwargs)
        except Exception as exc:
            attempt_failures.append(
                {
                    "batch_label": batch_label,
                    "offset": offset,
                    "depth": depth,
                    "attempt": attempt,
                    "reason": "model_call_failed",
                    "error": str(exc),
                    "candidate_keys": candidate_keys,
                }
            )
            continue
        annotations = normalize_model_annotation_response(response)
        if annotations is None:
            attempt_failures.append(
                {
                    "batch_label": batch_label,
                    "offset": offset,
                    "depth": depth,
                    "attempt": attempt,
                    "reason": "invalid_model_json",
                    "response_keys": sorted(response.keys()) if isinstance(response, dict) else [],
                    "candidate_keys": candidate_keys,
                }
            )
            continue
        annotation_keys = {model_annotation_key(annotation) for annotation in annotations if isinstance(annotation, dict)}
        missing = [key for key in candidate_keys if key and key not in annotation_keys]
        if missing:
            logger.warning(
                "Stage 07A model request %s returned %d/%d annotations; missing keys will use fallback if not recovered later.",
                batch_label,
                len(annotation_keys),
                len(candidate_keys),
            )
        return annotations, [], call_count

    if len(chunk) > 1:
        midpoint = max(1, len(chunk) // 2)
        logger.warning(
            "Stage 07A model batch %s failed validation after %d attempt(s); splitting %d candidate(s) into %d and %d.",
            batch_label,
            attempts,
            len(chunk),
            len(chunk[:midpoint]),
            len(chunk[midpoint:]),
        )
        left_annotations, left_failures, left_calls = request_model_annotations_for_chunk(
            chunk[:midpoint],
            known_entities,
            kwargs,
            logger,
            batch_label=f"{batch_label}a",
            offset=offset,
            depth=depth + 1,
        )
        right_annotations, right_failures, right_calls = request_model_annotations_for_chunk(
            chunk[midpoint:],
            known_entities,
            kwargs,
            logger,
            batch_label=f"{batch_label}b",
            offset=offset + midpoint,
            depth=depth + 1,
        )
        split_failures = left_failures + right_failures
        if not split_failures:
            return left_annotations + right_annotations, [], call_count + left_calls + right_calls
        return left_annotations + right_annotations, split_failures, call_count + left_calls + right_calls

    return [], attempt_failures, call_count


def stage_task_config(provider_config: dict[str, Any]) -> dict[str, Any]:
    routing = provider_config.get("model_routing", {}) if isinstance(provider_config, dict) else {}
    tasks = routing.get("tasks", {}) if isinstance(routing, dict) else {}
    task_cfg = tasks.get(TASK_NAME, {}) if isinstance(tasks, dict) and isinstance(tasks.get(TASK_NAME, {}), dict) else {}
    return dict(task_cfg)


def stage_model_kwargs(provider_config: dict[str, Any], task_cfg: dict[str, Any]) -> dict[str, Any]:
    kwargs = model_call_kwargs(provider_config, TASK_NAME)
    if not task_cfg or "provider" not in task_cfg:
        kwargs["provider"] = "openrouter"
    if not task_cfg or "api_base_url" not in task_cfg:
        kwargs["api_base_url"] = "https://openrouter.ai/api/v1"
    if not task_cfg or "api_model" not in task_cfg:
        kwargs["api_model"] = "qwen/qwen3-235b-a22b-2507"
    kwargs["timeout_seconds"] = max(int(kwargs.get("timeout_seconds", 60)), 180)
    kwargs["max_tokens"] = max(int(kwargs.get("max_tokens", 4096)), 8192)
    if not task_cfg or "rate_state_path" not in task_cfg:
        kwargs["rate_state_path"] = Path("artifacts/learning/openrouter_qwen_235b_stage_07a_rate_runtime.json")
    return kwargs


def model_known_entities(resolved_entities: list[dict[str, Any]], limit: int = 120) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for entity in resolved_entities:
        if not isinstance(entity, dict):
            continue
        rows.append(
            {
                "entity_id": entity.get("entity_id", ""),
                "canonical_name": entity.get("canonical_name", ""),
                "entity_type": normalize_entity_type(entity.get("entity_type", "term")),
                "aliases": list(entity.get("aliases", []) or [])[:12],
            }
        )
    return sorted(rows, key=lambda item: str(item.get("canonical_name", "")).lower())[:limit]


def build_candidate_harvest_prompt(candidates: list[dict[str, Any]], known_entities: list[dict[str, Any]]) -> str:
    candidate_rows = [model_candidate_row(candidate) for candidate in candidates]
    return f"""You are Stage 07A of the THERIAC Lore Bible pipeline.
Classify local Discord-derived candidate entity anchors into reviewable harvest metadata.
Return strict JSON only.

Core rules:
- Use only the local THERIAC evidence in the candidate rows and the known entity list.
- Do not use web search.
- Do not decide from evidence count alone. Ask what the candidate phrase denotes in context.
- Do not promote canon. Human review remains the canon gate.
- Externality, meta context, aliases, and generic phrases are metadata for later review, not final canon decisions.
- DISAMBIGUATION RULE: THERIAC features characters codenamed after emotional concepts (Love, Loss, Fear, Greed, Altruism). If the local context treats the entity as a person, faction member, or actor (e.g., having a spouse, feeling emotions, wearing a suit), propose `character`. If it discusses an abstract concept, motif, or lineage, propose `theme`.

Known approved/seed entities:
{json_dumps(known_entities)}

Candidate evidence packets:
{json_dumps(candidate_rows)}

For every candidate row, return exactly one annotation with the same normalized_name_key.
Use these enum values:
- denotation_class: likely_lore_entity | likely_meta_reference | likely_external_reference | likely_alias | likely_generic_phrase | mixed_or_uncertain
- proposed_entity_type: character | faction | organization | location | quest | event | timeline_node | theme | term
- recommended_track: lore | meta | mixed | unknown

Return JSON object:
{{
  "candidates": [
    {{
      "normalized_name_key": "same key from input",
      "candidate_name": "best display name from local evidence",
      "proposed_entity_type": "term",
      "denotation_class": "mixed_or_uncertain",
      "recommended_track": "unknown",
      "local_lore_prior": 0.0,
      "external_reference_prior": 0.0,
      "confidence": 0.0,
      "canonical_name": null,
      "alias_of": null,
      "signal_flags": {{
        "generic_phrase": false,
        "meta_team_marker": false,
        "inspiration_marker": false,
        "external_media_marker": false,
        "canon_adoption_marker": false,
        "music_quest_pattern": false,
        "known_entity_match": false
      }},
      "reasoning_summary": "brief local-evidence explanation",
      "human_review_question": "question for a human editor"
    }}
  ]
}}
"""


def model_candidate_row(candidate: dict[str, Any]) -> dict[str, Any]:
    return {
        "candidate_name": candidate.get("candidate_name", ""),
        "normalized_name_key": candidate.get("normalized_name_key", ""),
        "surface_forms": candidate.get("surface_forms", [])[:12],
        "proposed_entity_type": candidate.get("proposed_entity_type", "term"),
        "evidence_count": candidate.get("evidence_count", 0),
        "knowledge_track_counts": candidate.get("knowledge_track_counts", {}),
        "candidate_topics": candidate.get("candidate_topics", [])[:12],
        "source_kinds": candidate.get("source_kinds", [])[:12],
        "patch_item_type_counts": candidate.get("patch_item_type_counts", {}),
        "patch_update_type_counts": candidate.get("patch_update_type_counts", {}),
        "patch_relationship_type_counts": candidate.get("patch_relationship_type_counts", {}),
        "type_vote_totals": candidate.get("type_vote_totals", {}),
        "type_conflicts": candidate.get("type_conflicts", [])[:8],
        "known_entities_co_mentioned": [
            {
                "canonical_name": item.get("canonical_name", ""),
                "entity_type": item.get("entity_type", ""),
                "match_kinds": item.get("match_kinds", []),
                "count": item.get("count", 0),
            }
            for item in candidate.get("known_entities_co_mentioned", [])[:10]
            if isinstance(item, dict)
        ],
        "candidate_cooccurrences": [
            {
                "candidate_name": item.get("candidate_name", ""),
                "count": item.get("count", 0),
            }
            for item in candidate.get("candidate_cooccurrences", [])[:12]
            if isinstance(item, dict)
        ],
        "structured_alias_rename_snippet_ids": candidate.get("structured_alias_rename_snippet_ids", [])[:12],
        "deterministic_signal_flags": candidate.get("signal_flags", {}),
        "signal_details": candidate.get("signal_details", {}),
        "legacy_triage_hint": candidate.get("legacy_triage_hint", {}),
        "sample_texts": [str(text)[:700] for text in candidate.get("sample_texts", [])[:5]],
    }


def normalize_model_annotation_response(response: Any) -> list[dict[str, Any]] | None:
    if isinstance(response, dict) and isinstance(response.get("candidates"), list):
        raw = response["candidates"]
    elif isinstance(response, dict) and isinstance(response.get("annotations"), list):
        raw = response["annotations"]
    elif isinstance(response, dict) and isinstance(response.get("candidate_annotations"), list):
        raw = response["candidate_annotations"]
    elif isinstance(response, dict) and isinstance(response.get("entity_candidates"), list):
        raw = response["entity_candidates"]
    elif isinstance(response, dict) and isinstance(response.get("results"), list):
        raw = response["results"]
    elif isinstance(response, dict) and isinstance(response.get("items"), list):
        raw = response["items"]
    elif isinstance(response, dict) and isinstance(response.get("_json_root"), list):
        raw = response["_json_root"]
    elif isinstance(response, dict) and (
        response.get("candidate_name") or response.get("normalized_name_key") or response.get("normalized_key")
    ):
        raw = [response]
    elif isinstance(response, list):
        raw = response
    else:
        return None
    out = [item for item in raw if isinstance(item, dict)]
    return out if out else None


def model_annotation_key(annotation: dict[str, Any]) -> str:
    return normalized_name_key(
        str(
            annotation.get("normalized_name_key")
            or annotation.get("normalized_key")
            or annotation.get("candidate_key")
            or annotation.get("name_key")
            or annotation.get("candidate_name")
            or annotation.get("name")
            or ""
        )
    )


def fallback_model_annotation(candidate: dict[str, Any], reason: str) -> dict[str, Any]:
    flags = candidate.get("signal_flags", {}) if isinstance(candidate.get("signal_flags"), dict) else {}
    tracks = {str(track).strip().lower() for track in candidate.get("knowledge_tracks", []) or [] if str(track).strip()}
    denotation = "mixed_or_uncertain"
    recommended_track = "unknown"
    local_lore_prior = 0.35
    external_reference_prior = 0.35
    if flags.get("generic_phrase") or flags.get("low_value_phrase"):
        denotation = "likely_generic_phrase"
        recommended_track = "unknown"
        local_lore_prior = 0.15
        external_reference_prior = 0.25
    elif flags.get("meta_team_marker"):
        denotation = "likely_meta_reference"
        recommended_track = "meta"
        local_lore_prior = 0.2
        external_reference_prior = 0.55
    elif flags.get("external_media_marker") or flags.get("inspiration_marker"):
        denotation = "likely_external_reference"
        recommended_track = "meta"
        local_lore_prior = 0.25
        external_reference_prior = 0.7
    elif flags.get("canon_adoption_marker") or flags.get("music_quest_pattern") or "lore" in tracks:
        denotation = "likely_lore_entity"
        recommended_track = "lore"
        local_lore_prior = 0.65
        external_reference_prior = 0.2
    elif "meta" in tracks:
        denotation = "likely_meta_reference"
        recommended_track = "meta"
        local_lore_prior = 0.25
        external_reference_prior = 0.45
    return {
        "normalized_name_key": candidate.get("normalized_name_key", ""),
        "candidate_name": candidate.get("candidate_name", ""),
        "proposed_entity_type": candidate.get("proposed_entity_type") or candidate.get("initial_proposed_entity_type") or "term",
        "denotation_class": denotation,
        "recommended_track": recommended_track,
        "local_lore_prior": local_lore_prior,
        "external_reference_prior": external_reference_prior,
        "confidence": 0.25,
        "canonical_name": None,
        "alias_of": None,
        "signal_flags": {},
        "reasoning_summary": f"Qwen annotation unavailable or incomplete ({reason}); retained using deterministic 07A signal flags for review routing only.",
        "human_review_question": f"Should {candidate.get('candidate_name', 'this candidate')} be treated as lore, meta, alias evidence, or ignored?",
        "_annotation_status": "fallback_after_model_failure",
    }


def apply_model_annotation(candidate: dict[str, Any], annotation: dict[str, Any]) -> dict[str, Any]:
    merged = dict(candidate)
    model_type = normalize_entity_type(annotation.get("proposed_entity_type"), merged.get("proposed_entity_type", "term"))
    deterministic_flags = {
        str(key): bool(value)
        for key, value in (merged.get("signal_flags", {}) if isinstance(merged.get("signal_flags"), dict) else {}).items()
    }
    model_flags = {
        str(key): bool(value)
        for key, value in (annotation.get("signal_flags", {}) if isinstance(annotation.get("signal_flags"), dict) else {}).items()
    }
    merged["deterministic_signal_flags"] = deterministic_flags
    merged["signal_flags"] = {**deterministic_flags, **model_flags}
    merged["proposed_entity_type"] = model_type
    merged["model_annotation_status"] = str(annotation.get("_annotation_status") or "annotated")
    merged["model_annotation"] = {
        "candidate_name": str(annotation.get("candidate_name") or merged.get("candidate_name", "")).strip(),
        "proposed_entity_type": model_type,
        "denotation_class": normalize_denotation_class(annotation.get("denotation_class")),
        "recommended_track": normalize_recommended_track(annotation.get("recommended_track")),
        "local_lore_prior": clamp_float(annotation.get("local_lore_prior")),
        "external_reference_prior": clamp_float(annotation.get("external_reference_prior")),
        "confidence": clamp_float(annotation.get("confidence")),
        "canonical_name": optional_text(annotation.get("canonical_name")),
        "alias_of": optional_text(annotation.get("alias_of")),
        "reasoning_summary": str(annotation.get("reasoning_summary", "")).strip()[:800],
        "human_review_question": str(annotation.get("human_review_question", "")).strip()[:500],
    }
    merged["model_denotation_class"] = merged["model_annotation"]["denotation_class"]
    merged["recommended_track"] = merged["model_annotation"]["recommended_track"]
    merged["local_lore_prior"] = merged["model_annotation"]["local_lore_prior"]
    merged["external_reference_prior"] = merged["model_annotation"]["external_reference_prior"]
    merged["model_confidence"] = merged["model_annotation"]["confidence"]
    merged["model_reasoning_summary"] = merged["model_annotation"]["reasoning_summary"]
    merged["human_review_question"] = merged["model_annotation"]["human_review_question"]
    if merged["model_annotation"]["canonical_name"]:
        merged["model_suggested_canonical_name"] = merged["model_annotation"]["canonical_name"]
    if merged["model_annotation"]["alias_of"]:
        merged["model_suggested_alias_of"] = merged["model_annotation"]["alias_of"]
    return merged


def normalize_denotation_class(value: Any) -> str:
    clean = str(value or "").strip().lower()
    allowed = {
        "likely_lore_entity",
        "likely_meta_reference",
        "likely_external_reference",
        "likely_alias",
        "likely_generic_phrase",
        "mixed_or_uncertain",
    }
    return clean if clean in allowed else "mixed_or_uncertain"


def normalize_recommended_track(value: Any) -> str:
    clean = str(value or "").strip().lower()
    return clean if clean in {"lore", "meta", "mixed", "unknown"} else "unknown"


def optional_text(value: Any) -> str | None:
    text = str(value or "").strip()
    if not text or text.lower() in {"null", "none", "n/a"}:
        return None
    return text[:240]


def clamp_float(value: Any) -> float:
    try:
        return round(max(0.0, min(1.0, float(value))), 3)
    except (TypeError, ValueError):
        return 0.0


def json_dumps(value: Any) -> str:
    return json_preview(value, limit=24000)


def json_preview(value: Any, limit: int = 2000) -> str:
    import json

    text = json.dumps(value, ensure_ascii=False, indent=2)
    if len(text) <= limit:
        return text
    return text[:limit] + "\n...<truncated>"


def harvestable_snippet_candidates(snip: dict[str, Any], lower_evidence_text: str) -> list[tuple[str, str, str]]:
    rows: list[tuple[str, str, str]] = []
    seen: set[tuple[str, str]] = set()
    for source_field in HARVEST_SOURCE_FIELDS:
        raw = snip.get(source_field, [])
        if not isinstance(raw, list):
            continue
        for value in raw:
            candidate = clean_candidate_name(str(value))
            key = normalized_name_key(candidate)
            if not is_harvestable_candidate_name(candidate):
                continue
            if not re_contains_name(lower_evidence_text, key):
                continue
            row_key = (source_field, key)
            if row_key in seen:
                continue
            seen.add(row_key)
            rows.append((candidate, source_field, key))
    return rows


def is_harvestable_candidate_name(candidate: str) -> bool:
    key = normalized_name_key(candidate)
    if not key:
        return False
    if key.isdigit():
        return False
    if not any(ch.isalpha() for ch in key):
        return False
    if len(key) > MAX_HARVEST_NAME_CHARS:
        return False
    if len(key.split()) > MAX_HARVEST_NAME_WORDS:
        return False
    return True


def record_candidate_harvest(
    candidates_by_key: dict[str, dict[str, Any]],
    candidate: str,
    source_field: str,
    candidate_key: str,
    snip: dict[str, Any],
    known_hits: dict[str, dict[str, Any]],
    snippet_candidate_keys: list[str],
    target_entity: dict[str, Any] | None,
) -> None:
    record_conversation_entity_proposal(candidates_by_key, candidate, snip)
    row = candidates_by_key[candidate_key]
    row["candidate_id"] = stable_id("entity_candidate", candidate_key)
    append_unique(row.setdefault("_surface_forms", []), clean_candidate_name(candidate))
    increment_nested_count(row.setdefault("_source_field_counts", {}), source_field)
    for field in SOURCE_PROFILE_FIELDS:
        value = str(snip.get(field, "")).strip()
        if value:
            increment_nested_count(row.setdefault("_source_profile_counts", {}).setdefault(field, {}), value)
    if is_structured_alias_evidence(snip):
        append_unique(row.setdefault("_structured_alias_rename_snippet_ids", []), str(snip.get("snippet_id", "")))
    if target_entity is not None:
        add_known_entity_match(row, target_entity, str(snip.get("snippet_id", "")), "deterministic_candidate_match")
    for entity in known_hits.values():
        add_known_entity_match(row, entity, str(snip.get("snippet_id", "")), "co_mentioned")
    for other_key in snippet_candidate_keys:
        if other_key and other_key != candidate_key:
            add_candidate_cooccurrence(row, other_key, str(snip.get("snippet_id", "")))


def add_known_entity_match(row: dict[str, Any], entity: dict[str, Any], snippet_id: str, match_kind: str) -> None:
    entity_id = str(entity.get("entity_id", "")).strip()
    if not entity_id:
        return
    matches = row.setdefault("_known_entity_matches", {})
    record = matches.setdefault(
        entity_id,
        {
            "entity_id": entity_id,
            "card_id": entity.get("card_id", ""),
            "canonical_name": entity.get("canonical_name", ""),
            "entity_type": normalize_entity_type(entity.get("entity_type", "term")),
            "match_kinds": [],
            "source_snippet_ids": [],
            "count": 0,
        },
    )
    append_unique(record["match_kinds"], match_kind)
    append_unique(record["source_snippet_ids"], snippet_id)
    record["count"] = int(record.get("count", 0) or 0) + 1


def add_candidate_cooccurrence(row: dict[str, Any], other_key: str, snippet_id: str) -> None:
    cooccurrences = row.setdefault("_candidate_cooccurrences", {})
    record = cooccurrences.setdefault(
        other_key,
        {
            "normalized_name_key": other_key,
            "candidate_name": display_name(other_key),
            "source_snippet_ids": [],
            "count": 0,
        },
    )
    append_unique(record["source_snippet_ids"], snippet_id)
    record["count"] = int(record.get("count", 0) or 0) + 1


def finalize_candidate(
    proposal: dict[str, Any],
    approved_memory_keys: set[str],
    rejected_memory_keys: set[str],
    latest_seen: Any,
) -> dict[str, Any]:
    refresh_type_review_fields(proposal)
    key = str(proposal.get("normalized_name_key") or normalized_name_key(str(proposal.get("candidate_name", "")))).strip()
    metrics = proposal_recency_metrics(proposal, latest_seen)
    triage_status, triage_reason, review_priority = triage_conversation_entity_proposal(proposal, metrics)
    flags, details = candidate_signal_flags(proposal, key, approved_memory_keys, rejected_memory_keys)
    output = {
        k: v
        for k, v in proposal.items()
        if not k.startswith("_") and k not in {"proposal_id", "review_status", "triage_status", "triage_reason", "review_priority"}
    }
    output["candidate_id"] = proposal.get("candidate_id") or stable_id("entity_candidate", key)
    output["surface_forms"] = sorted(set(str(item) for item in proposal.get("_surface_forms", []) if str(item).strip()), key=str.lower)
    output["candidate_source_field_counts"] = proposal.get("_source_field_counts", {})
    output["source_profile_counts"] = proposal.get("_source_profile_counts", {})
    output["known_entities_co_mentioned"] = sorted(
        proposal.get("_known_entity_matches", {}).values(),
        key=lambda item: (-int(item.get("count", 0) or 0), str(item.get("canonical_name", "")).lower()),
    )
    output["candidate_cooccurrences"] = sorted(
        proposal.get("_candidate_cooccurrences", {}).values(),
        key=lambda item: (-int(item.get("count", 0) or 0), str(item.get("candidate_name", "")).lower()),
    )
    output["structured_alias_rename_snippet_ids"] = proposal.get("_structured_alias_rename_snippet_ids", [])
    output["signal_flags"] = flags
    output["signal_details"] = details
    output["legacy_triage_hint"] = {
        "triage_status": triage_status,
        "triage_reason": triage_reason,
        "review_priority": review_priority,
        **metrics,
    }
    output["harvest_reason"] = "Observed local candidate anchor retained for Stage 07B adjudication."
    return output


def candidate_signal_flags(
    proposal: dict[str, Any],
    key: str,
    approved_memory_keys: set[str],
    rejected_memory_keys: set[str],
) -> tuple[dict[str, bool], dict[str, list[str]]]:
    evidence_text = proposal_evidence_text(proposal)
    canon_hits = sorted(text_marker_hits(evidence_text, CANON_ADOPTION_MARKERS))
    inspiration_hits = sorted(text_marker_hits(evidence_text, REFERENCE_INSPIRATION_MARKERS))
    external_media_hits = sorted(text_marker_hits(evidence_text, EXTERNAL_MEDIA_REFERENCE_MARKERS))
    meta_role_hits = sorted(text_marker_hits(evidence_text, META_TEAM_ROLE_MARKERS))
    meta_context_hits = sorted(text_marker_hits(evidence_text, META_PROJECT_CONTEXT_MARKERS))
    music_votes = [
        str(vote.get("basis", ""))
        for vote in proposal.get("type_evidence", []) or []
        if isinstance(vote, dict) and "music" in str(vote.get("basis", "")).lower()
    ]
    band_votes = [
        str(vote.get("basis", ""))
        for vote in proposal.get("type_evidence", []) or []
        if isinstance(vote, dict) and "band_grouped_quest_naming_pattern" in str(vote.get("basis", "")).lower()
    ]
    flags = {
        "stopword_name": key in CONVERSATION_ENTITY_NAME_STOPWORDS,
        "generic_phrase": is_generic_conversation_entity_name(key),
        "low_value_phrase": is_low_value_phrase_name(key),
        "meta_team_marker": is_meta_team_contributor_candidate(proposal),
        "inspiration_marker": is_reference_inspiration_candidate(proposal),
        "external_media_marker": is_external_media_reference_candidate(proposal),
        "canon_adoption_marker": bool(canon_hits),
        "music_quest_pattern": bool(music_votes or band_votes),
        "structured_alias_rename_evidence": bool(proposal.get("_structured_alias_rename_snippet_ids")),
        "prior_approved_memory_match": key in approved_memory_keys,
        "prior_rejected_memory_match": key in rejected_memory_keys,
        "known_entity_match": bool(proposal.get("_known_entity_matches")),
    }
    details = {
        "canon_adoption_markers": canon_hits,
        "inspiration_markers": inspiration_hits,
        "external_media_markers": external_media_hits,
        "meta_team_role_markers": meta_role_hits,
        "meta_project_context_markers": meta_context_hits,
        "music_quest_type_evidence": sorted(set(music_votes + band_votes)),
    }
    return flags, details


def review_memory_candidate_keys(memory: dict[str, Any]) -> tuple[set[str], set[str]]:
    approved: set[str] = set()
    rejected: set[str] = set()
    for item in memory.get("approved_conversation_entities", []) or []:
        if isinstance(item, dict):
            add_memory_item_keys(approved, item)
    for item in memory.get("rejected_conversation_entities", []) or []:
        if isinstance(item, dict):
            add_memory_item_keys(rejected, item)
    return approved, rejected


def add_memory_item_keys(target: set[str], item: dict[str, Any]) -> None:
    for field in ("candidate_name", "canonical_name", "normalized_name_key"):
        key = normalized_name_key(str(item.get(field, "")))
        if key:
            target.add(key)
    for alias in item.get("aliases", []) or []:
        key = normalized_name_key(str(alias))
        if key:
            target.add(key)


def increment_nested_count(counts: dict[str, Any], value: str) -> None:
    clean = str(value or "").strip()
    if not clean:
        return
    counts[clean] = int(counts.get(clean, 0) or 0) + 1


def append_unique(values: list[Any], value: Any) -> None:
    text = str(value).strip()
    if text and text not in values:
        values.append(text)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--in-snippets-jsonl", type=Path, required=True)
    parser.add_argument("--in-entity-seed-json", "--in-seed-json", dest="in_seed_json", type=Path, required=True)
    parser.add_argument("--out-alias-json", type=Path, required=True)
    parser.add_argument("--out-timeline-json", type=Path, required=True)
    parser.add_argument("--out-resolved-entities-json", type=Path, required=False, default=None)
    parser.add_argument("--in-review-memory-json", type=Path, required=False, default=None)
    parser.add_argument("--out-entity-candidate-harvest-json", type=Path, required=True)
    parser.add_argument("--in-pipeline-config-json", type=Path, required=False, default=None)
    args = parser.parse_args()
    run(
        args.in_snippets_jsonl,
        args.in_seed_json,
        args.out_alias_json,
        args.out_timeline_json,
        args.out_resolved_entities_json,
        args.in_review_memory_json,
        args.out_entity_candidate_harvest_json,
        args.in_pipeline_config_json,
    )


if __name__ == "__main__":
    main()
