from __future__ import annotations

import argparse
import json
import re
import time
from pathlib import Path
from typing import Any

from pipeline.common import get_logger, now_utc_iso, read_json, read_jsonl, safe_uuid, stable_id, write_json
from pipeline.entity_resolution import load_entity_records, normalized_name_key
from pipeline.model_provider import (
    call_model_chat,
    get_model_runtime_status,
    model_call_kwargs,
)
from pipeline.review_memory import load_review_memory, rejected_claim_keys, relevant_memory_for_entity, normalize_claim_text


UNUSABLE_CLUSTER_KEYS = {
    "",
    "a",
    "an",
    "and",
    "are",
    "as",
    "at",
    "be",
    "because",
    "but",
    "by",
    "for",
    "from",
    "he",
    "i",
    "if",
    "in",
    "it",
    "its",
    "no",
    "not",
    "of",
    "on",
    "or",
    "she",
    "so",
    "that",
    "thats",
    "the",
    "then",
    "there",
    "they",
    "this",
    "to",
    "unmapped",
    "we",
    "with",
    "yeah",
    "you",
}
PACING_SKIP_REASONS = {"provider_locked", "adaptive_pacing", "rate_limit_cooldown"}


def evidence_sort_key(item: dict[str, Any]) -> tuple[int, str, str]:
    try:
        conversation_index = int(item.get("conversation_global_index", 0) or 0)
    except (TypeError, ValueError):
        conversation_index = 0
    if conversation_index <= 0:
        conversation_index = 10**9
    return (
        conversation_index,
        str(item.get("timestamp_start_utc", "")),
        str(item.get("snippet_id", "")),
    )


def cluster_evidence(cluster: dict[str, Any], snippets: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    snippet_ids = cluster.get("snippet_ids", [])
    return sorted((snippets[sid] for sid in snippet_ids if sid in snippets), key=evidence_sort_key)


def target_entity_for_cluster(
    cluster: dict[str, Any],
    evidence: list[dict[str, Any]],
    entity_by_name: dict[str, dict[str, Any]],
) -> dict[str, Any] | None:
    cluster_key = str(cluster.get("cluster_key", "")).lower()
    target_entity = entity_by_name.get(normalized_name_key(cluster_key))
    if target_entity is not None:
        return target_entity
    for snip in evidence:
        for candidate in snip.get("candidate_entities", []) or []:
            target_entity = entity_by_name.get(normalized_name_key(str(candidate)))
            if target_entity is not None:
                return target_entity
    return None


def provider_wait_seconds(reason: str, status: dict[str, Any], fallback_seconds: float) -> float:
    now_s = time.time()
    next_attempt = float(status.get("next_model_attempt_epoch_s") or 0.0)
    rate_limited_until = float(status.get("rate_limited_until_epoch_s") or 0.0)
    target = 0.0
    if reason in {"provider_locked", "adaptive_pacing"}:
        target = next_attempt
    elif reason in {"rate_limit_cooldown", "rate_limited_429"}:
        target = max(rate_limited_until, next_attempt)
    if target > now_s:
        return max(0.1, target - now_s)
    return max(0.0, fallback_seconds)


def infer_thematic_relationship_hints(
    cluster: dict[str, Any],
    card_by_id: dict[str, dict[str, Any]],
    thematic_memory: dict[str, Any],
) -> list[dict[str, Any]]:
    tags = set(cluster.get("thematic_tags", []))
    hints: list[dict[str, Any]] = []
    if "possible_artist_reference" in tags or any(t.startswith("music:") for t in tags):
        quest_targets = [c for c in card_by_id.values() if c.get("entity_type") == "quest"]
        char_targets = [c for c in card_by_id.values() if c.get("entity_type") == "character"]
        continuity_candidates: list[dict[str, Any]] = []
        for artist_row in thematic_memory.get("artists", []):
            if not isinstance(artist_row, dict):
                continue
            if int(artist_row.get("mention_count", 0)) < 2:
                continue
            top_chars = sorted(
                (artist_row.get("character_mentions") or {}).items(),
                key=lambda x: x[1],
                reverse=True,
            )[:3]
            top_quests = sorted(
                (artist_row.get("quest_mentions") or {}).items(),
                key=lambda x: x[1],
                reverse=True,
            )[:3]
            if top_chars or top_quests:
                continuity_candidates.append(
                    {
                        "artist_name": artist_row.get("artist_name"),
                        "mention_count": artist_row.get("mention_count", 0),
                        "top_character_links": top_chars,
                        "top_quest_links": top_quests,
                    }
                )
        if quest_targets and char_targets:
            hints.append(
                {
                    "relation_type": "possible_quest_character_music_link",
                    "confidence": 0.6 if continuity_candidates else 0.45,
                    "note": "Music/artist thematic markers suggest quest-to-character association candidate."
                    + (" Continuity memory raised confidence." if continuity_candidates else ""),
                    "candidate_quest_cards": [q.get("card_id") for q in quest_targets[:5]],
                    "candidate_character_cards": [c.get("card_id") for c in char_targets[:5]],
                    "continuity_memory_candidates": continuity_candidates[:5],
                }
            )
    if any(t.startswith("historical:") for t in tags):
        hints.append(
            {
                "relation_type": "possible_historical_theming_link",
                "confidence": 0.4,
                "note": "Historical naming markers present; consider linking to similarly themed entities.",
            }
        )
    return hints


def run(
    in_entities_json: Path,
    in_lore_clusters_json: Path,
    in_meta_clusters_json: Path,
    in_alias_json: Path,
    in_snippets_jsonl: Path,
    out_draft_dir: Path,
    in_pipeline_config_json: Path | None = None,
    in_review_memory_json: Path | None = None,
) -> None:
    logger = get_logger(__name__)
    entities = load_entity_records(in_entities_json)
    lore_payload = read_json(in_lore_clusters_json)
    lore_clusters = lore_payload.get("clusters", [])
    thematic_memory = lore_payload.get("thematic_memory", {})
    meta_clusters = read_json(in_meta_clusters_json).get("clusters", [])
    alias_payload = read_json(in_alias_json).get("aliases", [])
    snippets = {s["snippet_id"]: s for s in read_jsonl(in_snippets_jsonl)}

    out_draft_dir.mkdir(parents=True, exist_ok=True)

    entity_by_id = {e["entity_id"]: e for e in entities if isinstance(e, dict) and "entity_id" in e}
    entity_by_name = {normalized_name_key(e.get("canonical_name", "")): e for e in entity_by_id.values()}
    config: dict[str, Any] = {}
    if in_pipeline_config_json and in_pipeline_config_json.exists():
        config = read_json(in_pipeline_config_json)
    review_memory = load_review_memory(in_review_memory_json)
    logger.info(
        "Stage 09: drafting from %d lore cluster(s), %d meta cluster(s), %d snippet(s)",
        len(lore_clusters),
        len(meta_clusters),
        len(snippets),
    )

    claim_drafts = []
    extraction_failures: list[dict[str, Any]] = []
    model_call_total = 0
    for cluster in lore_clusters:
        evidence = cluster_evidence(cluster, snippets)
        if evidence and target_entity_for_cluster(cluster, evidence, entity_by_name) is not None:
            model_call_total += 1
    model_call_index = 0
    heartbeat_every = max(1, min(100, max(10, len(lore_clusters) // 20 or 1)))
    for cluster_index, cluster in enumerate(lore_clusters, start=1):
        snippet_ids = cluster.get("snippet_ids", [])
        evidence = cluster_evidence(cluster, snippets)
        if not evidence:
            if cluster_index == len(lore_clusters) or cluster_index % heartbeat_every == 0:
                logger.info(
                    "Stage 09 progress: %d/%d preparing lore clusters claim_drafts=%d failures=%d",
                    cluster_index,
                    len(lore_clusters),
                    len(claim_drafts),
                    len(extraction_failures),
                )
            continue
        cluster_key = str(cluster.get("cluster_key", "")).lower()
        target_entity = target_entity_for_cluster(cluster, evidence, entity_by_name)
        if target_entity is None:
            reason = "unmapped_or_unanchored_cluster" if is_unusable_cluster_key(cluster_key) else "unknown_target_entity"
            extraction_failures.append(build_extraction_failure(cluster, evidence, reason, "No resolved entity target for claim extraction."))
            logger.info(
                "Stage 09 skipped cluster_id=%s key=%s reason=%s",
                cluster.get("cluster_id"),
                cluster.get("cluster_key"),
                reason,
            )
            if cluster_index == len(lore_clusters) or cluster_index % heartbeat_every == 0:
                logger.info(
                    "Stage 09 progress: %d/%d preparing lore clusters claim_drafts=%d failures=%d",
                    cluster_index,
                    len(lore_clusters),
                    len(claim_drafts),
                    len(extraction_failures),
                )
            continue

        memory_for_entity = relevant_memory_for_entity(
            review_memory,
            str(target_entity.get("entity_id", "")),
            str(target_entity.get("canonical_name", "")),
        )
        rejected_keys = rejected_claim_keys(review_memory, str(target_entity.get("entity_id", "")))
        model_call_index += 1
        logger.info(
            "Stage 09 model call: %d/%d entity=%s cluster=%s snippets=%d",
            model_call_index,
            model_call_total,
            target_entity.get("canonical_name"),
            cluster.get("cluster_key"),
            len(evidence),
        )
        try:
            model_claims = extract_claims_with_model(target_entity, cluster, evidence, memory_for_entity, config)
        except RuntimeError as exc:
            extraction_failures.append(build_extraction_failure(cluster, evidence, "model_claim_extraction_failed", str(exc), target_entity))
            logger.warning(
                "Stage 09 claim extraction failed cluster_id=%s key=%s entity=%s error=%s",
                cluster.get("cluster_id"),
                cluster.get("cluster_key"),
                target_entity.get("canonical_name"),
                exc,
            )
            if cluster_index == len(lore_clusters) or cluster_index % heartbeat_every == 0:
                logger.info(
                    "Stage 09 progress: %d/%d preparing lore clusters claim_drafts=%d failures=%d",
                    cluster_index,
                    len(lore_clusters),
                    len(claim_drafts),
                    len(extraction_failures),
                )
            continue
        append_claim_drafts_from_model_claims(
            claim_drafts=claim_drafts,
            extraction_failures=extraction_failures,
            model_claims=model_claims,
            target_entity=target_entity,
            cluster=cluster,
            evidence=evidence,
            snippets=snippets,
            entity_by_id=entity_by_id,
            thematic_memory=thematic_memory,
            rejected_keys=rejected_keys,
        )
        if cluster_index == len(lore_clusters) or cluster_index % heartbeat_every == 0:
            logger.info(
                "Stage 09 progress: %d/%d preparing lore clusters claim_drafts=%d failures=%d",
                cluster_index,
                len(lore_clusters),
                len(claim_drafts),
                len(extraction_failures),
            )

    meta_cards = []
    meta_heartbeat_every = max(1, min(100, max(10, len(meta_clusters) // 20 or 1)))
    for meta_index, cluster in enumerate(meta_clusters, start=1):
        snippet_ids = cluster.get("snippet_ids", [])
        evidence = sorted((snippets[sid] for sid in snippet_ids if sid in snippets), key=evidence_sort_key)
        if not evidence:
            continue
        meta_cards.append(
            {
                "meta_id": stable_id("meta", cluster["cluster_id"]),
                "meta_type": "production",
                "title": str(cluster.get("cluster_key", "Meta Cluster")).title(),
                "summary": " ".join(e["display_text_normalized"] for e in evidence[:2]),
                "details": {
                    "cluster_id": cluster["cluster_id"],
                    "topics": cluster.get("topics", []),
                    "thematic_tags": cluster.get("thematic_tags", []),
                },
                "linked_lore_cards": [],
                "source_evidence": snippet_ids,
                "status": "draft",
            }
        )
        if meta_index == len(meta_clusters) or meta_index % meta_heartbeat_every == 0:
            logger.info(
                "Stage 09 progress: %d/%d drafting meta cards meta_cards=%d",
                meta_index,
                len(meta_clusters),
                len(meta_cards),
            )

    write_json(out_draft_dir / "claim_drafts.json", {"claims": claim_drafts})
    write_json(out_draft_dir / "claim_extraction_failures.json", {"failures": extraction_failures})
    write_json(out_draft_dir / "meta_cards_draft.json", {"meta_cards": meta_cards})
    write_json(out_draft_dir / "alias_snapshot.json", {"aliases": alias_payload})
    logger.info(
        "Stage 09 complete: claim_drafts=%d, extraction_failures=%d, meta_cards=%d",
        len(claim_drafts),
        len(extraction_failures),
        len(meta_cards),
    )


def is_unusable_cluster_key(value: str) -> bool:
    key = normalized_name_key(value)
    return not key or key in UNUSABLE_CLUSTER_KEYS or len(key) <= 2 or key.isdigit()


def validated_claim_source_ids(model_claim: dict[str, Any], cluster_snippet_ids: list[str]) -> list[str]:
    raw_ids = model_claim.get("source_snippet_ids", [])
    if not raw_ids and len(cluster_snippet_ids) == 1:
        return list(cluster_snippet_ids)
    if not isinstance(raw_ids, list):
        return []
    allowed = {str(sid) for sid in cluster_snippet_ids}
    out: list[str] = []
    for raw_id in raw_ids:
        sid = str(raw_id).strip()
        if sid in allowed and sid not in out:
            out.append(sid)
    return out


def find_claim_support_warnings(
    claim_text: str,
    evidence: list[dict[str, Any]],
    target_entity: dict[str, Any],
) -> list[str]:
    warnings: list[str] = []
    evidence_text = " ".join(str(item.get("display_text_normalized", "")) for item in evidence).lower()
    if not evidence_text:
        return ["no_source_text_available"]

    quoted_phrases = re.findall(r"(?<![A-Za-z])'([^']{3,})'(?![A-Za-z])|\"([^\"]{3,})\"", claim_text)
    for single_quoted, double_quoted in quoted_phrases:
        phrase = (single_quoted or double_quoted).strip()
        if phrase and phrase.lower() not in evidence_text:
            warnings.append(f"quoted_phrase_not_in_evidence:{phrase[:80]}")

    claim_lower = claim_text.lower()
    music_terms = {"band", "beatles", "guitar", "musician", "psychedelic"}
    if any(term in claim_lower for term in music_terms) and not any(term in evidence_text for term in music_terms):
        warnings.append("music_detail_not_in_evidence")

    target_names = {normalized_name_key(str(target_entity.get("canonical_name", "")))}
    target_names.update(normalized_name_key(str(alias)) for alias in target_entity.get("aliases", []) or [])
    capitalized_terms = re.findall(r"\b[A-Z][A-Za-z]+(?:\s+[A-Z][A-Za-z]+)+\b", claim_text)
    for term in capitalized_terms:
        if normalized_name_key(term) in target_names:
            continue
        if term.lower() not in evidence_text:
            warnings.append(f"proper_name_not_in_evidence:{term[:80]}")
    return sorted(set(warnings))


def build_extraction_failure(
    cluster: dict[str, Any],
    evidence: list[dict[str, Any]],
    reason: str,
    error: str,
    target_entity: dict[str, Any] | None = None,
) -> dict[str, Any]:
    sample_text = ""
    if evidence:
        sample_text = str(evidence[0].get("display_text_normalized", ""))[:500]
    return {
        "failure_id": safe_uuid(),
        "cluster_id": cluster.get("cluster_id"),
        "cluster_key": cluster.get("cluster_key"),
        "target_entity_id": target_entity.get("entity_id") if target_entity else "",
        "target_card_id": target_entity.get("card_id") if target_entity else "",
        "target_entity_name": target_entity.get("canonical_name") if target_entity else "",
        "reason": reason,
        "error": error,
        "snippet_ids": cluster.get("snippet_ids", []),
        "sample_text": sample_text,
        "created_at_utc": now_utc_iso(),
    }


def append_claim_drafts_from_model_claims(
    *,
    claim_drafts: list[dict[str, Any]],
    extraction_failures: list[dict[str, Any]],
    model_claims: list[dict[str, Any]],
    target_entity: dict[str, Any],
    cluster: dict[str, Any],
    evidence: list[dict[str, Any]],
    snippets: dict[str, dict[str, Any]],
    entity_by_id: dict[str, dict[str, Any]],
    thematic_memory: dict[str, Any],
    rejected_keys: set[str],
) -> None:
    snippet_ids = cluster.get("snippet_ids", [])
    for model_claim in model_claims:
        claim_text = str(model_claim.get("claim_text", "")).strip()
        if not claim_text:
            continue
        claim_source_ids = validated_claim_source_ids(model_claim, snippet_ids)
        if not claim_source_ids:
            extraction_failures.append(
                build_extraction_failure(
                    cluster,
                    evidence,
                    "claim_missing_source_evidence",
                    f"Claim omitted because it did not cite source_snippet_ids: {claim_text[:240]}",
                    target_entity,
                )
            )
            continue
        claim_evidence = [snippets[sid] for sid in claim_source_ids if sid in snippets]
        normalized_claim = normalize_claim_text(claim_text)
        if normalized_claim in rejected_keys:
            continue
        claim_drafts.append(
            {
                "claim_id": safe_uuid(),
                "target_entity_id": target_entity.get("entity_id"),
                "target_card_id": target_entity.get("card_id"),
                "target_entity_name": target_entity.get("canonical_name"),
                "knowledge_track": "lore",
                "claim_text": claim_text,
                "claim_type": str(model_claim.get("claim_type", "lore_fact")),
                "alias_text": str(model_claim.get("alias_text", "")).strip(),
                "source_snippet_ids": claim_source_ids,
                "thematic_tags": cluster.get("thematic_tags", []),
                "proposed_relationship_hints": infer_thematic_relationship_hints(cluster, entity_by_id, thematic_memory),
                "confidence": float(
                    model_claim.get(
                        "confidence",
                        round(sum(e.get("relevance_score", 0.5) for e in claim_evidence) / len(claim_evidence), 3) if claim_evidence else 0.5,
                    )
                ),
                "status": "draft",
                "contradiction_notes": str(model_claim.get("contradiction_notes", "")),
                "created_at_utc": now_utc_iso(),
                "normalized_claim_text": normalized_claim,
                "support_warnings": find_claim_support_warnings(claim_text, claim_evidence, target_entity),
            }
        )


def extract_claims_with_model(
    entity: dict[str, Any],
    cluster: dict[str, Any],
    evidence: list[dict[str, Any]],
    memory_for_entity: dict[str, Any],
    config: dict[str, Any],
) -> list[dict[str, Any]]:
    logger = get_logger(__name__)
    model_provider_cfg = config.get("model_provider", {}) if isinstance(config, dict) else {}
    validation_retries = max(0, int(model_provider_cfg.get("claim_extraction_validation_retries", 1)))
    provider_retries = max(validation_retries, int(model_provider_cfg.get("claim_extraction_provider_retries", 2)))
    validation_retry_sleep_seconds = max(
        0.0,
        float(model_provider_cfg.get("claim_extraction_retry_sleep_seconds", model_provider_cfg.get("adaptive_min_interval_seconds", 2.0))),
    )
    provider_retry_sleep_seconds = max(
        validation_retry_sleep_seconds,
        float(model_provider_cfg.get("claim_extraction_provider_retry_sleep_seconds", model_provider_cfg.get("rate_limit_cooldown_seconds", 30))),
    )
    validation_feedback = ""
    last_error = "provider returned no valid `claims` JSON"
    provider_failures = 0
    validation_failures = 0
    while True:
        prompt = build_claim_extraction_prompt(entity, cluster, evidence, memory_for_entity, validation_feedback)
        call_kwargs = model_call_kwargs(config, "stage_09_claim_drafting")
        response = call_model_chat(
            prompt=prompt,
            **call_kwargs,
        )
        if isinstance(response, dict) and isinstance(response.get("claims"), list):
            return [claim for claim in response["claims"] if isinstance(claim, dict)]
        if response is None:
            status = get_model_runtime_status()
            reason = str(status.get("last_model_skip_reason") or "provider_unavailable")
            sleep_s = provider_wait_seconds(reason, status, provider_retry_sleep_seconds)
            if reason in PACING_SKIP_REASONS:
                if sleep_s:
                    logger.info(
                        "Stage 09 provider pacing for entity=%s; retrying in %.1fs (%s).",
                        entity.get("canonical_name"),
                        sleep_s,
                        reason,
                    )
                    time.sleep(sleep_s)
                continue
            provider_failures += 1
            last_error = f"provider returned no response ({reason})"
            if provider_failures > provider_retries:
                break
            if sleep_s:
                logger.info(
                    "Stage 09 waiting %.1fs before retrying claim extraction for entity=%s after provider returned no response (%s).",
                    sleep_s,
                    entity.get("canonical_name"),
                    reason,
                )
                time.sleep(sleep_s)
            validation_feedback = (
                "Previous provider attempt returned no parseable JSON. Return strict JSON with a top-level "
                "`claims` array and no other prose."
            )
            continue

        validation_failures += 1
        response_keys = sorted(response.keys()) if isinstance(response, dict) else []
        last_error = f"provider returned no valid `claims` JSON; response_keys={response_keys}"
        if validation_failures > validation_retries:
            break
        validation_feedback = (
            "Previous response was invalid for claim extraction. Return strict JSON with a top-level "
            "`claims` array and no other prose."
        )
        if validation_retry_sleep_seconds:
            logger.info(
                "Stage 09 waiting %.1fs before retrying claim extraction for entity=%s after invalid claim JSON.",
                validation_retry_sleep_seconds,
                entity.get("canonical_name"),
            )
            time.sleep(validation_retry_sleep_seconds)
    raise RuntimeError(f"Stage 09 requires model claim extraction; {last_error}.")


def build_claim_extraction_prompt(
    entity: dict[str, Any],
    cluster: dict[str, Any],
    evidence: list[dict[str, Any]],
    memory_for_entity: dict[str, Any],
    validation_feedback: str = "",
) -> str:
    evidence_rows = [
        {
            "snippet_id": item.get("snippet_id"),
            "conversation_global_index": item.get("conversation_global_index"),
            "conversation_id": item.get("conversation_id"),
            "conversation_patch_note_id": item.get("conversation_patch_note_id"),
            "timestamp_start_utc": item.get("timestamp_start_utc"),
            "conversation_patch_summary": item.get("conversation_patch_summary", ""),
            "conversation_patch_lore_developments": item.get("conversation_patch_lore_developments", []),
            "conversation_patch_meta_developments": item.get("conversation_patch_meta_developments", []),
            "conversation_patch_open_questions": item.get("conversation_patch_open_questions", []),
            "conversation_patch_possible_contradictions": item.get("conversation_patch_possible_contradictions", []),
            "text": item.get("display_text_normalized", ""),
        }
        for item in evidence[:8]
    ]
    return f"""Extract atomic lore claims for one Theriac entity.
Return strict JSON only. Do not paste raw snippets as prose. Do not use bootstrap lore-bible text as evidence.
Suppress claims that repeat rejected memory. Keep claims concise, factual, and individually reviewable.
Domain rule: Theriac quest titles may be named after songs. Do not treat song-title quest names as weak, merely thematic, or non-diegetic when evidence links them to a path, ending, mission, or quest progression.
External reference rule: reference-only names from other media, real people, or creators should not become card subjects or target entities. If the evidence says an external source inspires, resembles, contrasts with, or influences the target entity, extract that as a claim_type "inspiration" about the target entity.

Target entity:
{json.dumps(entity, ensure_ascii=False, indent=2)}

Cluster:
{json.dumps(cluster, ensure_ascii=False, indent=2)}

Relevant review memory:
{json.dumps(memory_for_entity, ensure_ascii=False, indent=2)}

Evidence snippets:
{json.dumps(evidence_rows, ensure_ascii=False, indent=2)}

Chronology rule:
Evidence rows are ordered by conversation_global_index when available, then timestamp. Treat later conversations with other team members as reinforcement or refinement when they repeat earlier developments. Do not treat repeated communication to different partners within a short span as contradiction unless the content itself conflicts.

Previous extraction rejection to fix:
{validation_feedback or "none"}

Return JSON object:
{{
  "claims": [
    {{
      "claim_text": "one atomic lore claim",
      "claim_type": "background|role|relationship|timeline|theme|alias|inspiration|open_question|other",
      "alias_text": "only for alias claims; otherwise empty",
      "source_snippet_ids": ["exact snippet_id values that directly support this claim"],
      "confidence": 0.0,
      "contradiction_notes": ""
    }}
  ]
}}
"""


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--in-entities-json", "--in-seed-json", dest="in_entities_json", type=Path, required=True)
    parser.add_argument("--in-lore-clusters-json", type=Path, required=True)
    parser.add_argument("--in-meta-clusters-json", type=Path, required=True)
    parser.add_argument("--in-alias-json", type=Path, required=True)
    parser.add_argument("--in-snippets-jsonl", type=Path, required=True)
    parser.add_argument("--out-draft-dir", type=Path, required=True)
    parser.add_argument("--in-pipeline-config-json", type=Path, required=False, default=None)
    parser.add_argument("--in-review-memory-json", type=Path, required=False, default=None)
    args = parser.parse_args()
    run(
        args.in_entities_json,
        args.in_lore_clusters_json,
        args.in_meta_clusters_json,
        args.in_alias_json,
        args.in_snippets_jsonl,
        args.out_draft_dir,
        args.in_pipeline_config_json,
        args.in_review_memory_json,
    )


if __name__ == "__main__":
    main()
