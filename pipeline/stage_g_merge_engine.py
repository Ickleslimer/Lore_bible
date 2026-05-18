from __future__ import annotations

import argparse
import json
import re
import time
from pathlib import Path
from typing import Any

from pipeline.author_directives import apply_directive_to_card, parse_author_instruction
from pipeline.common import get_logger, now_utc_iso, read_json, read_jsonl, safe_uuid, stable_id, write_json, write_jsonl
from pipeline.entity_resolution import card_id_for_entity, load_entity_records, normalized_name_key
from pipeline.mixtral_anchor_provider import call_mixtral_chat, get_mixtral_runtime_status, model_call_kwargs
from pipeline.review_memory import (
    load_review_memory,
    remember_approved_cards,
    remember_author_directives,
    remember_claim_decisions,
    relevant_memory_for_entity,
    save_review_memory,
)


VALID_CLAIM_DECISIONS = {"accept", "reject", "defer", "needs_more_context"}
VALID_CARD_DECISIONS = {"approve", "accept", "reject", "defer", "needs_more_context"}
VALID_IDENTITY_MERGE_DECISIONS = {"approve", "accept", "reject", "defer", "needs_more_context"}
CARD_SECTION_KEYS = ["background", "role_in_story", "relationships", "timeline", "inspirations", "open_questions"]
MAX_SYNTHESIS_SOURCE_SNIPPETS = 24
MAX_SYNTHESIS_SOURCE_TEXT_CHARS = 900
WIKI_LINK_CONTEXT_LIMIT = 80
GUARDED_SPECULATIVE_PHRASES = {
    "classified",
    "undisclosed",
    "unknown",
    "unclear",
    "unspecified",
    "suggests",
    "implies",
    "indicates",
    "may",
    "might",
    "not specified",
    "not stated",
    "possibly",
    "potentially",
    "reflects",
    "reveals",
    "underscores",
    "strategic approach",
    "strategic separation",
    "governance",
    "portfolio",
    "technical mechanism",
    "technical mechanisms",
    "design principles",
    "vulnerabilities",
    "limitations",
    "operational coherence",
    "operational awareness",
    "operational specifics",
    "active participant",
    "structural consistency",
    "compartmentalization",
    "ecosystem",
}
PACING_SKIP_REASONS = {"provider_locked", "adaptive_pacing", "rate_limit_cooldown"}


def provider_wait_seconds(reason: str, status: dict[str, Any], fallback_seconds: float) -> float:
    now_s = time.time()
    next_attempt = float(status.get("next_mistral_attempt_epoch_s") or 0.0)
    rate_limited_until = float(status.get("rate_limited_until_epoch_s") or 0.0)
    target = 0.0
    if reason in {"provider_locked", "adaptive_pacing"}:
        target = next_attempt
    elif reason in {"rate_limit_cooldown", "rate_limited_429"}:
        target = max(rate_limited_until, next_attempt)
    if target > now_s:
        return max(0.1, target - now_s)
    return max(0.0, fallback_seconds)


def _load_decisions(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    payload = read_json(path)
    rows = payload.get("decisions", []) if isinstance(payload, dict) else []
    return [row for row in rows if isinstance(row, dict)]


def _load_directives(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    payload = read_json(path)
    rows = payload.get("directives", []) if isinstance(payload, dict) else []
    return [row for row in rows if isinstance(row, dict)]


def _load_identity_merge_decisions(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    payload = read_json(path)
    rows = payload.get("decisions", []) if isinstance(payload, dict) else []
    return [row for row in rows if isinstance(row, dict)]


def _latest_decision_by_claim(decisions: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for decision in decisions:
        claim_id = str(decision.get("claim_id", ""))
        if claim_id:
            out[claim_id] = decision
    return out


def _latest_decision_by_card(decisions: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for decision in decisions:
        card_id = str(decision.get("card_id") or decision.get("target_card_id") or decision.get("target_entity_id") or "")
        if card_id:
            out[card_id] = decision
    return out


def _latest_decision_by_identity_merge(decisions: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for decision in decisions:
        proposal_id = str(decision.get("proposal_id") or decision.get("merge_id") or "")
        if proposal_id:
            out[proposal_id] = decision
    return out


def apply_claim_decisions(
    claims: list[dict[str, Any]],
    decisions: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    accepted: list[dict[str, Any]] = []
    merge_log: list[dict[str, Any]] = []
    decision_by_claim = _latest_decision_by_claim(decisions)
    for claim in claims:
        claim_id = str(claim.get("claim_id", ""))
        decision = decision_by_claim.get(claim_id)
        if not decision:
            continue
        action = str(decision.get("decision", "defer"))
        if action not in VALID_CLAIM_DECISIONS:
            action = "defer"
        reviewed_claim = {
            **claim,
            "status": "accepted" if action == "accept" else action,
            "reviewer": decision.get("reviewer", "reviewer"),
            "review_rationale": decision.get("rationale", ""),
        }
        if action == "accept":
            accepted.append(reviewed_claim)
        merge_log.append(
            {
                "decision_id": decision.get("decision_id", safe_uuid()),
                "claim_id": claim_id,
                "card_id": claim.get("target_card_id"),
                "target_entity_id": claim.get("target_entity_id"),
                "knowledge_track": claim.get("knowledge_track", "lore"),
                "decision": action,
                "reviewer": decision.get("reviewer", "reviewer"),
                "rationale": decision.get("rationale", ""),
                "timestamp_utc": decision.get("timestamp_utc", now_utc_iso()),
                "source_snippet_ids": claim.get("source_snippet_ids", []),
                "source_priority": "discord_claim_draft",
                "claim_payload": claim,
            }
        )
    return accepted, merge_log


def _name_pattern(name: str) -> str:
    parts = re.split(r"\s+", str(name).strip())
    return r"\s+".join(re.escape(part) for part in parts if part)


def _entity_mentions(text: str, entities: list[dict[str, Any]]) -> list[dict[str, Any]]:
    names: list[tuple[str, dict[str, Any]]] = []
    for entity in entities:
        canonical = str(entity.get("canonical_name", "")).strip()
        if canonical:
            names.append((canonical, entity))
        for alias in entity.get("aliases", []) or []:
            alias_text = str(alias).strip()
            if alias_text:
                names.append((alias_text, entity))
    names.sort(key=lambda item: len(item[0]), reverse=True)

    mentions: list[dict[str, Any]] = []
    occupied: set[tuple[int, int]] = set()
    for name, entity in names:
        pattern = re.compile(r"(?<![A-Za-z0-9])" + _name_pattern(name) + r"(?![A-Za-z0-9])", re.IGNORECASE)
        for match in pattern.finditer(text):
            span = (match.start(), match.end())
            if span in occupied:
                continue
            occupied.add(span)
            mentions.append(
                {
                    "start": match.start(),
                    "end": match.end(),
                    "matched_text": match.group(0),
                    "entity": entity,
                }
            )
    return sorted(mentions, key=lambda mention: (int(mention["start"]), -int(mention["end"])))


def _nearest_mention_before(mentions: list[dict[str, Any]], idx: int) -> dict[str, Any] | None:
    candidates = [mention for mention in mentions if int(mention["end"]) <= idx]
    if not candidates:
        return None
    return sorted(candidates, key=lambda mention: (int(mention["end"]), -int(mention["start"])))[-1]


def _nearest_mention_after(mentions: list[dict[str, Any]], idx: int) -> dict[str, Any] | None:
    candidates = [mention for mention in mentions if int(mention["start"]) >= idx]
    if not candidates:
        return None
    return sorted(candidates, key=lambda mention: (int(mention["start"]), int(mention["end"])))[0]


def _claim_identity_pairs(claim: dict[str, Any], entities: list[dict[str, Any]]) -> list[tuple[dict[str, Any], dict[str, Any], str]]:
    text = str(claim.get("claim_text", "")).strip()
    if not text:
        return []
    lower = text.lower()
    mentions = _entity_mentions(text, entities)
    pairs: list[tuple[dict[str, Any], dict[str, Any], str]] = []

    rename_triggers = [
        "renames itself to",
        "renamed itself to",
        "renames to",
        "is renamed to",
        "was renamed to",
        "changes its name to",
        "changes name to",
        "takes the name",
        "becomes",
        "became",
    ]
    for trigger in rename_triggers:
        start = lower.find(trigger)
        if start == -1:
            continue
        source = _nearest_mention_before(mentions, start)
        target = _nearest_mention_after(mentions, start + len(trigger))
        if source and target:
            pairs.append((source["entity"], target["entity"], trigger))

    for trigger in ["formerly", "previously"]:
        start = lower.find(trigger)
        if start == -1:
            continue
        target = _nearest_mention_before(mentions, start)
        source = _nearest_mention_after(mentions, start + len(trigger))
        if source and target:
            pairs.append((source["entity"], target["entity"], trigger))

    return [
        (source, target, trigger)
        for source, target, trigger in pairs
        if str(source.get("entity_id", "")) and str(source.get("entity_id")) != str(target.get("entity_id"))
    ]


def detect_identity_merge_proposals(
    accepted_claims: list[dict[str, Any]],
    entities: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    proposals_by_pair: dict[tuple[str, str], dict[str, Any]] = {}
    for claim in accepted_claims:
        for source, target, trigger in _claim_identity_pairs(claim, entities):
            source_id = str(source.get("entity_id", ""))
            target_id = str(target.get("entity_id", ""))
            key = (source_id, target_id)
            if key not in proposals_by_pair:
                proposals_by_pair[key] = {
                    "proposal_id": stable_id("identity_merge_proposal", source_id, target_id),
                    "source_entity_id": source_id,
                    "source_card_id": source.get("card_id"),
                    "source_entity_name": source.get("canonical_name"),
                    "target_entity_id": target_id,
                    "target_card_id": target.get("card_id"),
                    "target_entity_name": target.get("canonical_name"),
                    "alias_text": source.get("canonical_name"),
                    "merge_type": "identity_rename",
                    "status": "proposed",
                    "evidence_claim_ids": [],
                    "source_snippet_ids": [],
                    "evidence": [],
                    "created_at_utc": now_utc_iso(),
                }
            proposal = proposals_by_pair[key]
            claim_id = str(claim.get("claim_id", ""))
            if claim_id and claim_id not in proposal["evidence_claim_ids"]:
                proposal["evidence_claim_ids"].append(claim_id)
            for snippet_id in claim.get("source_snippet_ids", []) or []:
                snippet_text = str(snippet_id)
                if snippet_text and snippet_text not in proposal["source_snippet_ids"]:
                    proposal["source_snippet_ids"].append(snippet_text)
            proposal["evidence"].append(
                {
                    "claim_id": claim.get("claim_id"),
                    "claim_text": claim.get("claim_text"),
                    "trigger": trigger,
                    "source_snippet_ids": claim.get("source_snippet_ids", []),
                    "confidence": claim.get("confidence"),
                }
            )
    return sorted(
        proposals_by_pair.values(),
        key=lambda proposal: (str(proposal.get("target_entity_name", "")), str(proposal.get("source_entity_name", ""))),
    )


def annotate_identity_merge_proposals(
    proposals: list[dict[str, Any]],
    decisions: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    decision_by_proposal = _latest_decision_by_identity_merge(decisions)
    annotated: list[dict[str, Any]] = []
    for proposal in proposals:
        decision = decision_by_proposal.get(str(proposal.get("proposal_id", "")))
        if not decision:
            annotated.append({**proposal, "review_status": "pending"})
            continue
        action = str(decision.get("decision", "defer")).lower()
        if action not in VALID_IDENTITY_MERGE_DECISIONS:
            action = "defer"
        annotated.append(
            {
                **proposal,
                "review_status": action,
                "reviewer": decision.get("reviewer", "reviewer"),
                "review_rationale": decision.get("rationale", ""),
                "reviewed_at_utc": decision.get("timestamp_utc", now_utc_iso()),
            }
        )
    return annotated


def remember_identity_merge_decisions(
    memory: dict[str, Any],
    proposals: list[dict[str, Any]],
    decisions: list[dict[str, Any]],
) -> None:
    proposal_by_id = {str(proposal.get("proposal_id", "")): proposal for proposal in proposals}
    existing_merges = {
        (str(item.get("source_entity_id", "")), str(item.get("target_entity_id", "")))
        for item in memory.get("entity_merges", [])
        if isinstance(item, dict)
    }
    existing_aliases = {
        (str(item.get("target_entity_id", "")), str(item.get("alias_text", "")).lower())
        for item in memory.get("approved_aliases", [])
        if isinstance(item, dict)
    }
    for decision in decisions:
        action = str(decision.get("decision", "")).lower()
        if action not in {"approve", "accept"}:
            continue
        proposal = proposal_by_id.get(str(decision.get("proposal_id") or decision.get("merge_id") or ""))
        if not proposal:
            continue
        merge_key = (str(proposal.get("source_entity_id", "")), str(proposal.get("target_entity_id", "")))
        if merge_key not in existing_merges:
            memory.setdefault("entity_merges", []).append(
                {
                    "merge_id": str(proposal.get("proposal_id", stable_id("entity_merge", *merge_key))),
                    "source_entity_id": proposal.get("source_entity_id", ""),
                    "source_card_id": proposal.get("source_card_id", ""),
                    "source_entity_name": proposal.get("source_entity_name", ""),
                    "target_entity_id": proposal.get("target_entity_id", ""),
                    "target_card_id": proposal.get("target_card_id", ""),
                    "target_entity_name": proposal.get("target_entity_name", ""),
                    "alias_text": proposal.get("alias_text", proposal.get("source_entity_name", "")),
                    "merge_type": proposal.get("merge_type", "identity_rename"),
                    "source_claim_ids": proposal.get("evidence_claim_ids", []),
                    "source_snippet_ids": proposal.get("source_snippet_ids", []),
                    "approved_by": decision.get("reviewer", "reviewer"),
                    "rationale": decision.get("rationale", ""),
                    "approved_at_utc": decision.get("timestamp_utc", now_utc_iso()),
                }
            )
            existing_merges.add(merge_key)

        alias_text = str(proposal.get("alias_text") or proposal.get("source_entity_name") or "").strip()
        alias_key = (str(proposal.get("target_entity_id", "")), alias_text.lower())
        if alias_text and alias_key not in existing_aliases:
            memory.setdefault("approved_aliases", []).append(
                {
                    "target_entity_id": proposal.get("target_entity_id", ""),
                    "canonical_name": proposal.get("target_entity_name", ""),
                    "alias_text": alias_text,
                    "source_claim_id": ",".join(str(item) for item in proposal.get("evidence_claim_ids", [])),
                    "source_snippet_ids": proposal.get("source_snippet_ids", []),
                    "approved_at_utc": decision.get("timestamp_utc", now_utc_iso()),
                }
            )
            existing_aliases.add(alias_key)


def approved_entity_merges_from_memory(memory: dict[str, Any]) -> list[dict[str, Any]]:
    return [item for item in memory.get("entity_merges", []) if isinstance(item, dict)]


def _merge_target_map(merge_records: list[dict[str, Any]]) -> dict[str, str]:
    direct = {
        str(item.get("source_entity_id", "")): str(item.get("target_entity_id", ""))
        for item in merge_records
        if str(item.get("source_entity_id", "")).strip() and str(item.get("target_entity_id", "")).strip()
    }

    def resolve(entity_id: str) -> str:
        seen: set[str] = set()
        current = entity_id
        while current in direct and direct[current] not in seen:
            seen.add(current)
            current = direct[current]
        return current

    return {source_id: resolve(source_id) for source_id in direct}


def apply_entity_merges_to_entities(
    entities: list[dict[str, Any]],
    merge_records: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], dict[str, str], dict[str, list[str]]]:
    entity_by_id = {str(entity.get("entity_id", "")): entity for entity in entities}
    target_map = _merge_target_map(merge_records)
    sources_by_target: dict[str, list[str]] = {}
    for source_id, target_id in target_map.items():
        if source_id in entity_by_id and target_id in entity_by_id and source_id != target_id:
            sources_by_target.setdefault(target_id, []).append(source_id)

    merged: dict[str, dict[str, Any]] = {}
    for entity in entities:
        entity_id = str(entity.get("entity_id", ""))
        target_id = target_map.get(entity_id, entity_id)
        if target_id != entity_id:
            continue
        merged[target_id] = {**entity, "aliases": list(entity.get("aliases", []) or [])}

    for target_id, source_ids in sources_by_target.items():
        target = merged.get(target_id)
        if not target:
            continue
        aliases = {str(alias) for alias in target.get("aliases", []) or [] if str(alias).strip()}
        seed_ids = set(str(item) for item in target.get("seed_entity_ids", []) or [])
        relationship_hints = list(target.get("relationship_hints", []) or [])
        merged_from = set(str(item) for item in target.get("merged_from_entity_ids", []) or [])
        for source_id in source_ids:
            source = entity_by_id.get(source_id)
            if not source:
                continue
            source_name = str(source.get("canonical_name", "")).strip()
            if source_name and source_name != str(target.get("canonical_name", "")):
                aliases.add(source_name)
            aliases.update(str(alias) for alias in source.get("aliases", []) or [] if str(alias).strip())
            seed_ids.update(str(item) for item in source.get("seed_entity_ids", []) or [])
            relationship_hints.extend(source.get("relationship_hints", []) or [])
            merged_from.add(source_id)
        target["aliases"] = sorted(aliases)
        target["seed_entity_ids"] = sorted(seed_ids)
        target["relationship_hints"] = relationship_hints
        target["merged_from_entity_ids"] = sorted(merged_from)
        target["resolution_status"] = "resolved_with_reviewed_entity_merges"

    return sorted(merged.values(), key=lambda entity: str(entity.get("canonical_name", ""))), target_map, sources_by_target


def remap_claims_for_entity_merges(
    claims: list[dict[str, Any]],
    entities_by_id: dict[str, dict[str, Any]],
    target_map: dict[str, str],
) -> list[dict[str, Any]]:
    remapped: list[dict[str, Any]] = []
    for claim in claims:
        source_id = str(claim.get("target_entity_id", ""))
        target_id = target_map.get(source_id, source_id)
        if target_id == source_id:
            remapped.append(claim)
            continue
        target = entities_by_id.get(target_id)
        if not target:
            remapped.append(claim)
            continue
        remapped.append(
            {
                **claim,
                "original_target_entity_id": source_id,
                "original_target_card_id": claim.get("target_card_id", ""),
                "original_target_entity_name": claim.get("target_entity_name", ""),
                "target_entity_id": target_id,
                "target_card_id": target.get("card_id", ""),
                "target_entity_name": target.get("canonical_name", ""),
                "identity_merge_applied": True,
            }
        )
    return remapped


def _merge_memory_payloads(payloads: list[dict[str, Any]]) -> dict[str, Any]:
    merged = {
        "accepted_claims": [],
        "rejected_claims": [],
        "approved_aliases": [],
        "entity_merges": [],
        "approved_cards": [],
        "author_directives": [],
        "style_corrections": [],
    }
    seen_claims: set[tuple[str, str]] = set()
    for payload in payloads:
        for key in ["accepted_claims", "rejected_claims"]:
            for claim in payload.get(key, []) or []:
                if not isinstance(claim, dict):
                    continue
                claim_key = (str(claim.get("claim_id", "")), str(claim.get("normalized_claim_text", "")))
                if claim_key in seen_claims:
                    continue
                merged[key].append(claim)
                seen_claims.add(claim_key)
        for key in ["approved_aliases", "entity_merges", "approved_cards", "author_directives", "style_corrections"]:
            merged[key].extend([item for item in payload.get(key, []) or [] if isinstance(item, dict)])
    return merged


def relevant_memory_for_merged_entity(
    memory: dict[str, Any],
    entity_id: str,
    entity: dict[str, Any],
    sources_by_target: dict[str, list[str]],
    entities_by_id: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    payloads = [relevant_memory_for_entity(memory, entity_id, str(entity.get("canonical_name", "")))]
    for source_id in sources_by_target.get(entity_id, []):
        source = entities_by_id.get(source_id, {})
        payloads.append(relevant_memory_for_entity(memory, source_id, str(source.get("canonical_name", ""))))
    return _merge_memory_payloads(payloads)


def synthesize_card_with_model(
    entity: dict[str, Any],
    claims: list[dict[str, Any]],
    memory_for_entity: dict[str, Any],
    config: dict[str, Any],
    source_snippets_by_id: dict[str, dict[str, Any]] | None = None,
    entities_by_name: dict[str, dict[str, Any]] | None = None,
) -> dict[str, Any]:
    logger = get_logger(__name__)
    mixtral_cfg = config.get("mixtral", {}) if isinstance(config, dict) else {}
    validation_retries = max(0, int(mixtral_cfg.get("synthesis_validation_retries", 1)))
    provider_retries = max(validation_retries, int(mixtral_cfg.get("synthesis_provider_retries", 2)))
    validation_retry_sleep_seconds = max(
        0.0,
        float(mixtral_cfg.get("synthesis_validation_retry_sleep_seconds", mixtral_cfg.get("adaptive_min_interval_seconds", 2.0))),
    )
    provider_retry_sleep_seconds = max(
        validation_retry_sleep_seconds,
        float(mixtral_cfg.get("synthesis_provider_retry_sleep_seconds", mixtral_cfg.get("rate_limit_cooldown_seconds", 30))),
    )
    validation_feedback = ""
    last_error: RuntimeError | None = None
    provider_failures = 0
    validation_failures = 0
    while True:
        prompt = build_card_synthesis_prompt(
            entity,
            claims,
            memory_for_entity,
            validation_feedback,
            source_snippets_by_id,
            entities_by_name,
        )
        call_kwargs = model_call_kwargs(config, "stage_g_card_synthesis")
        response = call_mixtral_chat(
            prompt=prompt,
            **call_kwargs,
        )
        if response is None:
            status = get_mixtral_runtime_status()
            reason = str(status.get("last_mistral_skip_reason") or "provider_unavailable")
            sleep_s = provider_wait_seconds(reason, status, provider_retry_sleep_seconds)
            if reason in PACING_SKIP_REASONS:
                if sleep_s:
                    logger.info(
                        "Stage 10 provider pacing for entity=%s; retrying in %.1fs (%s).",
                        entity.get("canonical_name"),
                        sleep_s,
                        reason,
                    )
                    time.sleep(sleep_s)
                continue
            provider_failures += 1
            last_error = RuntimeError(f"Stage 10 requires model card synthesis; provider returned no response ({reason}).")
            if provider_failures > provider_retries:
                break
            if sleep_s:
                logger.info(
                    "Stage 10 waiting %.1fs before retrying card synthesis for entity=%s after provider returned no response (%s).",
                    sleep_s,
                    entity.get("canonical_name"),
                    reason,
                )
                time.sleep(sleep_s)
            validation_feedback = "Previous provider attempt returned no parseable JSON. Return one strict card JSON object."
            continue
        try:
            if not isinstance(response, dict) or not isinstance(response.get("summary"), str):
                raise RuntimeError("Stage 10 requires model card synthesis; provider returned no valid card JSON.")
            sanitize_optional_synthesis_fields(response, claims, memory_for_entity)
            validate_synthesis_support(entity, claims, memory_for_entity, response)
            if provider_failures or validation_failures:
                response["_validation_retry_count"] = provider_failures + validation_failures
            return response
        except RuntimeError as exc:
            last_error = exc
            validation_failures += 1
            if validation_failures > validation_retries:
                break
            validation_feedback = str(exc)
            if validation_retry_sleep_seconds:
                logger.info(
                    "Stage 10 waiting %.1fs before retrying card synthesis for entity=%s after validation failure.",
                    validation_retry_sleep_seconds,
                    entity.get("canonical_name"),
                )
                time.sleep(validation_retry_sleep_seconds)
    raise last_error or RuntimeError("Stage 10 synthesis failed validation.")


def _truncate_source_text(value: Any, max_chars: int = MAX_SYNTHESIS_SOURCE_TEXT_CHARS) -> str:
    text = re.sub(r"\s+", " ", str(value or "")).strip()
    if len(text) <= max_chars:
        return text
    return text[: max_chars - 18].rstrip() + " ... (truncated)"


def build_synthesis_source_evidence_rows(
    claims: list[dict[str, Any]],
    source_snippets_by_id: dict[str, dict[str, Any]],
    max_rows: int = MAX_SYNTHESIS_SOURCE_SNIPPETS,
) -> list[dict[str, Any]]:
    claim_ids_by_snippet: dict[str, list[str]] = {}
    ordered_snippet_ids: list[str] = []
    for claim in claims:
        claim_id = str(claim.get("claim_id", "")).strip()
        for raw_snippet_id in claim.get("source_snippet_ids", []) or []:
            snippet_id = str(raw_snippet_id).strip()
            if not snippet_id:
                continue
            if snippet_id not in claim_ids_by_snippet:
                claim_ids_by_snippet[snippet_id] = []
                ordered_snippet_ids.append(snippet_id)
            if claim_id and claim_id not in claim_ids_by_snippet[snippet_id]:
                claim_ids_by_snippet[snippet_id].append(claim_id)

    rows: list[dict[str, Any]] = []
    for snippet_id in ordered_snippet_ids:
        snippet = source_snippets_by_id.get(snippet_id)
        if not isinstance(snippet, dict):
            continue
        rows.append(
            {
                "snippet_id": snippet_id,
                "supporting_claim_ids": claim_ids_by_snippet.get(snippet_id, []),
                "conversation_id": snippet.get("conversation_id", ""),
                "conversation_global_index": snippet.get("conversation_global_index"),
                "conversation_topic_label": snippet.get("conversation_topic_label", ""),
                "conversation_topic_summary": snippet.get("conversation_topic_summary", ""),
                "timestamp_start_utc": snippet.get("timestamp_start_utc", ""),
                "source_kind": snippet.get("source_kind", ""),
                "patch_item_type": snippet.get("patch_item_type", ""),
                "patch_update_type": snippet.get("patch_update_type", ""),
                "patch_relationship_type": snippet.get("patch_relationship_type", ""),
                "conversation_patch_summary": snippet.get("conversation_patch_summary", ""),
                "conversation_patch_lore_developments": snippet.get("conversation_patch_lore_developments", []),
                "conversation_patch_meta_developments": snippet.get("conversation_patch_meta_developments", []),
                "conversation_patch_possible_contradictions": snippet.get("conversation_patch_possible_contradictions", []),
                "text": _truncate_source_text(snippet.get("display_text_normalized", "")),
            }
        )

    rows.sort(
        key=lambda row: (
            row.get("conversation_global_index") is None,
            row.get("conversation_global_index") if row.get("conversation_global_index") is not None else 0,
            str(row.get("timestamp_start_utc", "")),
            str(row.get("snippet_id", "")),
        )
    )
    return rows[:max_rows]


def section_word_targets_for_claims(claims: list[dict[str, Any]]) -> dict[str, Any]:
    claim_count = len(claims)
    claim_types = {str(claim.get("claim_type", "")).strip().lower() for claim in claims}
    has_timeline = "timeline" in claim_types
    has_relationship = "relationship" in claim_types or "alias" in claim_types
    has_inspiration = "inspiration" in claim_types
    has_open_question = "open_question" in claim_types
    if claim_count <= 2:
        total_min, total_max = 80, 180
        section_targets = {
            "summary": "35-70 words",
            "background": "40-90 words if the accepted claims support it; otherwise empty",
            "role_in_story": "empty unless supported by a role/story claim",
            "relationships": "empty unless supported by relationship or alias claims",
            "timeline": "empty unless supported by timeline claims",
            "inspirations": "empty unless supported by inspiration claims",
            "open_questions": "empty unless supported by explicit uncertainty/open-question claims",
        }
    elif claim_count <= 4:
        total_min, total_max = 150, 300
        section_targets = {
            "summary": "45-85 words",
            "background": "50-110 words if supported",
            "role_in_story": "40-90 words if supported",
            "relationships": "35-80 words if supported",
            "timeline": "35-80 words if supported",
            "inspirations": "25-70 words if supported",
            "open_questions": "25-60 words only for explicit uncertainties",
        }
    elif claim_count <= 7:
        total_min, total_max = 250, 500
        section_targets = {
            "summary": "55-95 words",
            "background": "70-140 words if supported",
            "role_in_story": "60-130 words if supported",
            "relationships": "50-120 words if supported",
            "timeline": "40-100 words if supported",
            "inspirations": "30-90 words if supported",
            "open_questions": "25-70 words only for explicit uncertainties",
        }
    elif claim_count <= 14:
        total_min, total_max = 400, 800
        section_targets = {
            "summary": "70-120 words",
            "background": "90-180 words if supported",
            "role_in_story": "90-180 words if supported",
            "relationships": "70-160 words if supported",
            "timeline": "60-140 words if supported",
            "inspirations": "40-110 words if supported",
            "open_questions": "30-90 words only for explicit uncertainties",
        }
    else:
        total_min, total_max = 650, 1200
        section_targets = {
            "summary": "80-140 words",
            "background": "120-240 words if supported",
            "role_in_story": "120-260 words if supported",
            "relationships": "100-220 words if supported",
            "timeline": "80-180 words if supported",
            "inspirations": "50-140 words if supported",
            "open_questions": "30-100 words only for explicit uncertainties",
        }
    recommended_sections = ["summary", "background"]
    if claim_count >= 3 or "role" in claim_types:
        recommended_sections.append("role_in_story")
    if has_relationship:
        recommended_sections.append("relationships")
    if has_timeline:
        recommended_sections.append("timeline")
    if has_inspiration:
        recommended_sections.append("inspirations")
    if has_open_question:
        recommended_sections.append("open_questions")
    return {
        "accepted_claim_count": claim_count,
        "total_word_target": {"min": total_min, "max": total_max},
        "recommended_sections": list(dict.fromkeys(recommended_sections)),
        "section_word_targets": section_targets,
        "scaling_rule": (
            "Sparse entities may have only a lead plus one supported section. Heavily developed entities should use "
            "multiple supported sections and reach the upper total range."
        ),
    }


def build_available_wiki_link_rows(entity: dict[str, Any], entities_by_name: dict[str, dict[str, Any]] | None) -> list[dict[str, Any]]:
    if not entities_by_name:
        return []
    seen: set[str] = set()
    rows: list[dict[str, Any]] = []
    current_id = str(entity.get("entity_id", ""))
    current_card_id = str(entity.get("card_id", ""))
    for other in entities_by_name.values():
        if not isinstance(other, dict):
            continue
        card_id = str(other.get("card_id", "")).strip()
        entity_id_value = str(other.get("entity_id", "")).strip()
        canonical_name = str(other.get("canonical_name", "")).strip()
        if not card_id or not canonical_name or card_id in seen:
            continue
        if (current_id and entity_id_value == current_id) or (current_card_id and card_id == current_card_id):
            continue
        seen.add(card_id)
        rows.append(
            {
                "target_card_id": card_id,
                "target_entity_id": entity_id_value,
                "canonical_name": canonical_name,
                "entity_type": other.get("entity_type", "term"),
                "aliases": other.get("aliases", [])[:8] if isinstance(other.get("aliases", []), list) else [],
            }
        )
    return sorted(rows, key=lambda row: str(row.get("canonical_name", "")).lower())[:WIKI_LINK_CONTEXT_LIMIT]


def build_card_synthesis_prompt(
    entity: dict[str, Any],
    claims: list[dict[str, Any]],
    memory_for_entity: dict[str, Any],
    validation_feedback: str = "",
    source_snippets_by_id: dict[str, dict[str, Any]] | None = None,
    entities_by_name: dict[str, dict[str, Any]] | None = None,
) -> str:
    prompt_claims = [
        {
            "claim_id": claim.get("claim_id"),
            "claim_text": claim.get("claim_text"),
            "claim_type": claim.get("claim_type"),
            "alias_text": claim.get("alias_text", ""),
            "source_snippet_ids": claim.get("source_snippet_ids", []),
        }
        for claim in claims
    ]
    source_evidence_rows = build_synthesis_source_evidence_rows(claims, source_snippets_by_id or {})
    word_targets = section_word_targets_for_claims(claims)
    wiki_link_rows = build_available_wiki_link_rows(entity, entities_by_name)
    return f"""Write a full THERIAC wiki-style entry from all reviewed claims for this entity, comparable in shape and density to a strong fandom wiki page.
Return strict JSON only. Scale the entry using the word target plan below rather than forcing every entity to the same length.
Write polished article prose, not a bullet list, glossary stub, changelog, or terse database note. The summary should be a compact lead paragraph that identifies what the entity is and why it matters. The sections should then expand the entity's background, story function, relationships, chronology, inspirations, and open questions with concrete connective context from the accepted claims.
Use the accepted claims as the authority for what may be stated. Use the source snippet evidence below as texture and disambiguating context for those accepted claims, especially when expanding the card into a proper wiki entry. Do not introduce a fact from a snippet unless it is tied to an accepted claim and cited through that claim's support_map entry.
Do not merely summarize summaries. Prefer concrete names, relationships, story functions, chronology, and wording grounded in the accepted claims and their source snippets. Do not use bootstrap lore-bible text as evidence. Do not paste raw chat.
Do not invent acronym expansions, technical mechanisms, creators, dates, motives, or background facts unless an accepted claim explicitly states them.
Every non-empty summary/section must list the accepted claim IDs that support it in support_map. If a detail has no accepted claim support, omit it.
Domain rule: THERIAC quest titles may be named after songs. Do not treat song-title quest names as weak, merely thematic, or non-diegetic when accepted claims link them to a path, ending, mission, or quest progression.
External reference rule: inspiration/reference sources from other media, real people, or creators should not become card subjects unless accepted claims explicitly make them in-world THERIAC entities. If accepted claims say they inspire, resemble, contrast with, or influence this entity, put that information in the inspirations section.
Per-section word target rule: follow the word target plan. Sparse entities may have only the lead and one supported section. Heavily developed characters, factions, quests, and systems should use several supported sections and read like a full page. Do not count relationship/timeline/link arrays toward prose length.
Wiki link rule: Use available wiki link targets for cross-card references. In prose, refer to other cards by canonical name when supported by an accepted claim. Also return those links in wiki_links. Do not create links to external-media inspirations unless they are THERIAC cards in the available link targets.
Leave open_questions empty unless an accepted claim is itself an open_question or explicitly states uncertainty.
Avoid inference words such as may, might, possibly, potentially, suggests, implies, indicates, reflects, underscores, reveals, classified, undisclosed, unknown, not specified, governance, strategic approach, portfolio, vulnerabilities, limitations, and technical mechanisms unless those exact ideas appear in accepted claims.
Reconcile information across conversations before surfacing conflicts. Resolve apparent conflicts using chronology, aliases, specificity, and point-of-view when the accepted claims allow it. Only use unresolved_conflicts for contradictions that cannot be reconciled from accepted claims.

Entity:
{json.dumps(entity, ensure_ascii=False, indent=2)}

All accepted claims for this entity:
{json.dumps(prompt_claims, ensure_ascii=False, indent=2)}

Source snippet evidence for accepted claims:
{json.dumps(source_evidence_rows, ensure_ascii=False, indent=2)}

Word target plan:
{json.dumps(word_targets, ensure_ascii=False, indent=2)}

Available wiki link targets:
{json.dumps(wiki_link_rows, ensure_ascii=False, indent=2)}

Relevant review memory:
{json.dumps(memory_for_entity, ensure_ascii=False, indent=2)}

Previous synthesis rejection to fix:
{validation_feedback or "none"}

Return JSON object:
{{
  "summary": "concise lead paragraph",
  "sections": {{
    "background": "",
    "role_in_story": "",
    "relationships": "",
    "timeline": "",
    "inspirations": "",
    "open_questions": ""
  }},
  "relationships": [
    {{"target_entity_name": "", "relation_type": "", "note": "", "support_claim_ids": ["claim_id"]}}
  ],
  "timeline": [
    {{"timestamp_utc": "", "description": "", "source_snippet_ids": [""], "support_claim_ids": ["claim_id"]}}
  ],
  "wiki_links": [
    {{"target_card_id": "", "target_entity_name": "", "relation_type": "", "section": "summary|background|role_in_story|relationships|timeline|inspirations|open_questions", "support_claim_ids": ["claim_id"]}}
  ],
  "resolved_conflicts": [
    {{"description": "", "claim_ids": ["claim_id"], "resolution": ""}}
  ],
  "unresolved_conflicts": [
    {{"description": "", "claim_ids": ["claim_id"], "why_unresolved": ""}}
  ],
  "support_map": {{
    "summary": ["claim_id"],
    "background": ["claim_id"],
    "role_in_story": ["claim_id"],
    "relationships": ["claim_id"],
    "timeline": ["claim_id"],
    "inspirations": ["claim_id"],
    "open_questions": ["claim_id"],
    "resolved_conflicts": ["claim_id"],
    "unresolved_conflicts": ["claim_id"]
  }}
}}
"""


def validate_synthesis_support(
    entity: dict[str, Any],
    claims: list[dict[str, Any]],
    memory_for_entity: dict[str, Any],
    synthesis: dict[str, Any],
) -> None:
    valid_claim_ids = {str(claim.get("claim_id")) for claim in claims if str(claim.get("claim_id", "")).strip()}
    support_map = synthesis.get("support_map")
    if not isinstance(support_map, dict):
        raise RuntimeError("Stage 10 synthesis rejected: missing support_map for generated card prose.")

    sections = synthesis.get("sections", {})
    if not isinstance(sections, dict):
        sections = {}
    text_fields = {"summary": synthesis.get("summary", "")}
    for section_name in CARD_SECTION_KEYS:
        text_fields[section_name] = sections.get(section_name, "")

    for field_name, text in text_fields.items():
        if not str(text).strip():
            continue
        support_ids = support_map.get(field_name)
        if not isinstance(support_ids, list):
            raise RuntimeError(f"Stage 10 synthesis rejected: `{field_name}` lacks support_map claim IDs.")
        invalid_ids = [str(item) for item in support_ids if str(item) not in valid_claim_ids]
        valid_ids = [str(item) for item in support_ids if str(item) in valid_claim_ids]
        if invalid_ids:
            raise RuntimeError(f"Stage 10 synthesis rejected: `{field_name}` cites unknown claim IDs: {invalid_ids}.")
        if not valid_ids:
            raise RuntimeError(f"Stage 10 synthesis rejected: `{field_name}` has no accepted claim support.")

    for idx, rel in enumerate(synthesis.get("relationships", []) or []):
        if not isinstance(rel, dict) or not str(rel.get("target_entity_name", "")).strip():
            continue
        support_ids = rel.get("support_claim_ids")
        if not isinstance(support_ids, list) or not any(str(item) in valid_claim_ids for item in support_ids):
            raise RuntimeError(f"Stage 10 synthesis rejected: relationship #{idx + 1} has no accepted claim support.")

    for idx, item in enumerate(synthesis.get("timeline", []) or []):
        if not isinstance(item, dict) or not str(item.get("description", "")).strip():
            continue
        support_ids = item.get("support_claim_ids")
        if not isinstance(support_ids, list) or not any(str(claim_id) in valid_claim_ids for claim_id in support_ids):
            raise RuntimeError(f"Stage 10 synthesis rejected: timeline item #{idx + 1} has no accepted claim support.")

    for idx, item in enumerate(synthesis.get("wiki_links", []) or []):
        if not isinstance(item, dict) or not str(item.get("target_card_id") or item.get("target_entity_name") or "").strip():
            continue
        support_ids = item.get("support_claim_ids")
        if not isinstance(support_ids, list) or not any(str(claim_id) in valid_claim_ids for claim_id in support_ids):
            raise RuntimeError(f"Stage 10 synthesis rejected: wiki_links item #{idx + 1} has no accepted claim support.")

    for field_name in ["resolved_conflicts", "unresolved_conflicts"]:
        for idx, item in enumerate(synthesis.get(field_name, []) or []):
            if not isinstance(item, dict) or not str(item.get("description", "")).strip():
                continue
            claim_ids = item.get("claim_ids")
            if not isinstance(claim_ids, list) or not any(str(claim_id) in valid_claim_ids for claim_id in claim_ids):
                raise RuntimeError(f"Stage 10 synthesis rejected: {field_name} item #{idx + 1} has no accepted claim support.")

    unsupported_expansions = find_unsupported_acronym_expansions(entity, claims, memory_for_entity, synthesis)
    if unsupported_expansions:
        raise RuntimeError(
            "Stage 10 synthesis rejected: unsupported acronym expansion(s): "
            + ", ".join(f"{name} ({expansion})" for name, expansion in unsupported_expansions)
        )
    unsupported_speculation = find_unsupported_speculation(claims, memory_for_entity, synthesis)
    if unsupported_speculation:
        raise RuntimeError(
            "Stage 10 synthesis rejected: unsupported speculative phrase(s): "
            + ", ".join(sorted(unsupported_speculation))
        )
    generated_word_count = synthesis_word_count(synthesis)
    word_targets = section_word_targets_for_claims(claims)
    target_min = int(word_targets.get("total_word_target", {}).get("min", 0) or 0)
    target_max = int(word_targets.get("total_word_target", {}).get("max", 650) or 650)
    if len(claims) >= 5 and generated_word_count < target_min:
        raise RuntimeError(
            f"Stage 10 synthesis rejected: draft is too short for wiki-card target "
            f"({generated_word_count} words from {len(claims)} accepted claims; target {target_min}-{target_max})."
        )
    if generated_word_count > target_max + 80:
        raise RuntimeError(
            f"Stage 10 synthesis rejected: draft is too long for wiki-card target "
            f"({generated_word_count} words; target {target_min}-{target_max})."
        )

    open_questions = str(sections.get("open_questions", "")).strip()
    has_open_question_claim = any(str(claim.get("claim_type", "")) == "open_question" for claim in claims)
    claim_text = support_source_text(claims, memory_for_entity)
    uncertainty_claimed = any(word in claim_text for word in ["unknown", "unclear", "unresolved", "question", "uncertain"])
    if open_questions and not (has_open_question_claim or uncertainty_claimed):
        raise RuntimeError("Stage 10 synthesis rejected: open_questions were generated without accepted uncertainty claims.")


def sanitize_optional_synthesis_fields(
    synthesis: dict[str, Any],
    claims: list[dict[str, Any]],
    memory_for_entity: dict[str, Any],
) -> None:
    valid_claim_ids = {str(claim.get("claim_id")) for claim in claims if str(claim.get("claim_id", "")).strip()}
    support_map = synthesis.get("support_map")
    if not isinstance(support_map, dict):
        return
    sections = synthesis.get("sections")
    if not isinstance(sections, dict):
        sections = {}
        synthesis["sections"] = sections
    synthesis["sections"] = {key: str(sections.get(key, "")).strip() for key in CARD_SECTION_KEYS}
    sections = synthesis["sections"]

    claim_text = support_source_text(claims, memory_for_entity)
    has_open_question_claim = any(str(claim.get("claim_type", "")) == "open_question" for claim in claims)
    uncertainty_claimed = any(word in claim_text for word in ["unknown", "unclear", "unresolved", "question", "uncertain"])
    open_question_support = support_map.get("open_questions")
    open_question_has_current_claim_support = isinstance(open_question_support, list) and any(
        str(item) in valid_claim_ids for item in open_question_support
    )
    if sections.get("open_questions") and (
        not open_question_has_current_claim_support or not (has_open_question_claim or uncertainty_claimed)
    ):
        sections["open_questions"] = ""
        support_map["open_questions"] = []

    for field_name in ["summary"] + CARD_SECTION_KEYS:
        if field_name == "summary":
            original = str(synthesis.get("summary", ""))
            cleaned = strip_unsupported_speculative_sentences(original, claim_text)
            if cleaned:
                synthesis["summary"] = cleaned
            elif original.strip():
                synthesis["summary"] = conservative_summary_from_claims(claims)
                support_map["summary"] = [str(claim.get("claim_id")) for claim in claims if str(claim.get("claim_id", "")).strip()]
            continue
        original = str(sections.get(field_name, ""))
        if not original.strip():
            continue
        cleaned = strip_unsupported_speculative_sentences(original, claim_text)
        sections[field_name] = cleaned
        if not cleaned:
            support_map[field_name] = []

    for optional_section in ["timeline"]:
        support_ids = support_map.get(optional_section)
        if sections.get(optional_section) and (
            not isinstance(support_ids, list) or not any(str(item) in valid_claim_ids for item in support_ids)
        ):
            sections[optional_section] = ""
            support_map[optional_section] = []

    filtered_relationships = []
    for rel in synthesis.get("relationships", []) or []:
        if not isinstance(rel, dict):
            continue
        support_ids = rel.get("support_claim_ids")
        if isinstance(support_ids, list) and any(str(item) in valid_claim_ids for item in support_ids):
            filtered_relationships.append(rel)
    synthesis["relationships"] = filtered_relationships

    filtered_timeline = []
    for item in synthesis.get("timeline", []) or []:
        if not isinstance(item, dict):
            continue
        support_ids = item.get("support_claim_ids")
        if isinstance(support_ids, list) and any(str(claim_id) in valid_claim_ids for claim_id in support_ids):
            filtered_timeline.append(item)
    synthesis["timeline"] = filtered_timeline

    filtered_wiki_links = []
    for item in synthesis.get("wiki_links", []) or []:
        if not isinstance(item, dict):
            continue
        support_ids = item.get("support_claim_ids")
        if isinstance(support_ids, list) and any(str(claim_id) in valid_claim_ids for claim_id in support_ids):
            filtered_wiki_links.append(item)
    synthesis["wiki_links"] = filtered_wiki_links

    for field_name in ["resolved_conflicts", "unresolved_conflicts"]:
        filtered = []
        for item in synthesis.get(field_name, []) or []:
            if not isinstance(item, dict):
                continue
            claim_ids = item.get("claim_ids")
            if isinstance(claim_ids, list) and any(str(claim_id) in valid_claim_ids for claim_id in claim_ids):
                filtered.append(item)
        synthesis[field_name] = filtered


def find_unsupported_acronym_expansions(
    entity: dict[str, Any],
    claims: list[dict[str, Any]],
    memory_for_entity: dict[str, Any],
    synthesis: dict[str, Any],
) -> list[tuple[str, str]]:
    names = [str(entity.get("canonical_name", ""))]
    names.extend(str(alias) for alias in entity.get("aliases", []) or [])
    acronyms = sorted({name for name in names if name.isupper() and 2 <= len(name) <= 12})
    if not acronyms:
        return []

    generated_text = synthesis_text_blob(synthesis)
    allowed_text = support_source_text(claims, memory_for_entity)

    unsupported: list[tuple[str, str]] = []
    for acronym in acronyms:
        pattern = re.compile(r"\b" + re.escape(acronym) + r"\s*\(([^)]{3,})\)")
        for match in pattern.finditer(generated_text):
            expansion = re.sub(r"\s+", " ", match.group(1)).strip()
            if not looks_like_acronym_expansion(acronym, expansion):
                continue
            if expansion.lower() not in allowed_text:
                unsupported.append((acronym, expansion))
    return unsupported


def looks_like_acronym_expansion(acronym: str, expansion: str) -> bool:
    words = re.findall(r"[A-Za-z][A-Za-z-]*", expansion)
    if len(words) < 2:
        return False
    leading_word = words[0].lower()
    if leading_word in {"aka", "also", "and", "formerly", "later", "now", "or", "previously", "then"}:
        return False
    ignored = {"a", "an", "and", "for", "in", "of", "or", "the", "to"}
    initials = "".join(word[0].upper() for word in words if word.lower() not in ignored)
    return initials.startswith(acronym.upper())


def find_unsupported_speculation(
    claims: list[dict[str, Any]],
    memory_for_entity: dict[str, Any],
    synthesis: dict[str, Any],
) -> set[str]:
    generated_text = synthesis_text_blob(synthesis).lower()
    supported_text = support_source_text(claims, memory_for_entity)
    unsupported: set[str] = set()
    for phrase in GUARDED_SPECULATIVE_PHRASES:
        if phrase in generated_text and phrase not in supported_text:
            unsupported.add(phrase)
    return unsupported


def strip_unsupported_speculative_sentences(text: str, supported_text: str) -> str:
    raw = re.sub(r"\s+", " ", str(text)).strip()
    if not raw:
        return ""
    sentences = re.split(r"(?<=[.!?])\s+", raw)
    kept: list[str] = []
    for sentence in sentences:
        lowered = sentence.lower()
        unsupported = [
            phrase for phrase in GUARDED_SPECULATIVE_PHRASES if phrase in lowered and phrase not in supported_text
        ]
        if unsupported:
            continue
        kept.append(sentence.strip())
    return " ".join(part for part in kept if part).strip()


def conservative_summary_from_claims(claims: list[dict[str, Any]]) -> str:
    claim_texts = [str(claim.get("claim_text", "")).strip() for claim in claims if str(claim.get("claim_text", "")).strip()]
    return " ".join(claim_texts[:3]).strip()


def support_source_text(claims: list[dict[str, Any]], memory_for_entity: dict[str, Any]) -> str:
    return "\n".join(
        [str(claim.get("claim_text", "")) for claim in claims]
        + [str(claim.get("claim_text", "")) for claim in memory_for_entity.get("accepted_claims", []) if isinstance(claim, dict)]
        + [str(item.get("instruction_text", "")) for item in memory_for_entity.get("author_directives", []) if isinstance(item, dict)]
    ).lower()


def accepted_claim_history_for_entity(
    memory_for_entity: dict[str, Any],
    current_claims: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    by_key: dict[str, dict[str, Any]] = {}
    for claim in list(memory_for_entity.get("accepted_claims", [])) + current_claims:
        if not isinstance(claim, dict):
            continue
        claim_id = str(claim.get("claim_id", "")).strip()
        normalized = str(claim.get("normalized_claim_text", "")).strip()
        if not normalized:
            normalized = re.sub(r"[^a-z0-9]+", " ", str(claim.get("claim_text", "")).lower()).strip()
        key = claim_id or f"{claim.get('target_entity_id', '')}:{normalized}"
        if not key:
            continue
        by_key[key] = claim
    return sorted(
        by_key.values(),
        key=lambda claim: (
            str(claim.get("reviewed_at_utc") or claim.get("created_at_utc") or ""),
            str(claim.get("claim_id", "")),
        ),
    )


def synthesis_text_blob(synthesis: dict[str, Any]) -> str:
    parts = [synthesis_prose_blob(synthesis)]
    for rel in synthesis.get("relationships", []) or []:
        if isinstance(rel, dict):
            parts.append(str(rel.get("note", "")))
    for item in synthesis.get("timeline", []) or []:
        if isinstance(item, dict):
            parts.append(str(item.get("description", "")))
    return "\n".join(parts)


def synthesis_prose_blob(synthesis: dict[str, Any]) -> str:
    parts = [str(synthesis.get("summary", ""))]
    sections = synthesis.get("sections", {})
    if isinstance(sections, dict):
        parts.extend(str(sections.get(key, "")) for key in CARD_SECTION_KEYS)
    return "\n".join(parts)


def synthesis_word_count(synthesis: dict[str, Any]) -> int:
    return len(re.findall(r"\b[\w'-]+\b", synthesis_prose_blob(synthesis)))


def text_word_count(text: Any) -> int:
    return len(re.findall(r"\b[\w'-]+\b", str(text or "")))


def synthesis_section_word_counts(synthesis: dict[str, Any]) -> dict[str, int]:
    sections = synthesis.get("sections", {})
    if not isinstance(sections, dict):
        sections = {}
    counts = {"summary": text_word_count(synthesis.get("summary", ""))}
    for section_name in CARD_SECTION_KEYS:
        counts[section_name] = text_word_count(sections.get(section_name, ""))
    counts["total_prose"] = sum(counts.values())
    return counts


def build_wiki_links_from_synthesis(
    synthesis: dict[str, Any],
    entities_by_name: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    links_by_key: dict[tuple[str, str, str], dict[str, Any]] = {}

    def add_link(target_name: str, relation_type: str, section: str, support_claim_ids: Any) -> None:
        target = entities_by_name.get(normalized_name_key(target_name))
        if target is None:
            return
        target_card_id = str(target.get("card_id") or card_id_for_entity(str(target.get("canonical_name", ""))))
        clean_relation = str(relation_type or "related").strip() or "related"
        clean_section = str(section or "").strip()
        key = (target_card_id, clean_relation, clean_section)
        link = links_by_key.setdefault(
            key,
            {
                "target_card_id": target_card_id,
                "target_entity_name": target.get("canonical_name", target_name),
                "relation_type": clean_relation,
                "section": clean_section,
                "support_claim_ids": [],
            },
        )
        if isinstance(support_claim_ids, list):
            for claim_id in support_claim_ids:
                claim_text = str(claim_id).strip()
                if claim_text and claim_text not in link["support_claim_ids"]:
                    link["support_claim_ids"].append(claim_text)

    for rel in synthesis.get("relationships", []) or []:
        if isinstance(rel, dict):
            add_link(rel.get("target_entity_name", ""), rel.get("relation_type", ""), "relationships", rel.get("support_claim_ids", []))
    for item in synthesis.get("wiki_links", []) or []:
        if not isinstance(item, dict):
            continue
        target_name = str(item.get("target_entity_name", "") or "").strip()
        if not target_name:
            target_card_id = str(item.get("target_card_id", "")).strip()
            target = next((entity for entity in entities_by_name.values() if str(entity.get("card_id", "")) == target_card_id), None)
            target_name = str(target.get("canonical_name", "")) if target else ""
        add_link(target_name, item.get("relation_type", ""), item.get("section", ""), item.get("support_claim_ids", []))

    return sorted(links_by_key.values(), key=lambda link: (str(link.get("target_entity_name", "")).lower(), str(link.get("relation_type", ""))))


def _build_card_from_synthesis(
    entity: dict[str, Any],
    claims: list[dict[str, Any]],
    synthesis: dict[str, Any],
    entities_by_name: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    canonical_name = str(entity.get("canonical_name", "Unnamed Entity"))
    card_id = str(entity.get("card_id") or card_id_for_entity(canonical_name))
    sections = synthesis.get("sections", {})
    if not isinstance(sections, dict):
        sections = {}
    sections = {key: str(sections.get(key, "")).strip() for key in CARD_SECTION_KEYS}
    source_evidence = sorted({sid for claim in claims for sid in claim.get("source_snippet_ids", [])})
    relationships = []
    for rel in synthesis.get("relationships", []) or []:
        if not isinstance(rel, dict):
            continue
        target_name = str(rel.get("target_entity_name", "")).strip()
        target = entities_by_name.get(normalized_name_key(target_name))
        relation_type = str(rel.get("relation_type", "")).strip()
        if target and relation_type:
            relationships.append(
                {
                    "target_card_id": target.get("card_id") or card_id_for_entity(str(target.get("canonical_name", ""))),
                    "relation_type": relation_type,
                    "note": str(rel.get("note", "")),
                }
            )
    timeline = []
    for item in synthesis.get("timeline", []) or []:
        if not isinstance(item, dict):
            continue
        description = str(item.get("description", "")).strip()
        timestamp = str(item.get("timestamp_utc", "")).strip()
        if description and "T" in timestamp:
            timeline.append(
                {
                    "timestamp_utc": timestamp,
                    "description": description,
                    "source_snippet_ids": item.get("source_snippet_ids", []),
                }
            )
    avg_confidence = round(sum(float(c.get("confidence", 0.5)) for c in claims) / len(claims), 3) if claims else 0.0
    section_word_counts = synthesis_section_word_counts(synthesis)
    return {
        "card_id": card_id,
        "entity_type": entity.get("entity_type", "term"),
        "canonical_name": canonical_name,
        "aliases": entity.get("aliases", []),
        "status": "draft",
        "summary": str(synthesis.get("summary", "")).strip(),
        "details": {
            "entity_id": entity.get("entity_id"),
            "sections": sections,
            "support_map": synthesis.get("support_map", {}),
            "resolved_conflicts": synthesis.get("resolved_conflicts", []),
            "unresolved_conflicts": synthesis.get("unresolved_conflicts", []),
            "wiki_links": build_wiki_links_from_synthesis(synthesis, entities_by_name),
            "section_word_counts": section_word_counts,
            "word_target_plan": section_word_targets_for_claims(claims),
            "validation_retry_count": synthesis.get("_validation_retry_count", 0),
            "accepted_claim_ids": [claim.get("claim_id") for claim in claims],
            "synthesis_origin": "accepted_claims_model_synthesis",
        },
        "timeline": timeline,
        "relationships": relationships,
        "source_evidence": source_evidence,
        "confidence": {"score": avg_confidence, "reviewer_note": "Synthesized from human-accepted claims."},
        "revision_history": [
            {
                "timestamp_utc": now_utc_iso(),
                "action": "card_synthesized_from_claims",
                "actor": "stage_g_merge_engine",
            }
        ],
    }


def _apply_directives_to_drafts(cards: list[dict[str, Any]], directives: list[dict[str, Any]]) -> None:
    cards_by_id = {str(card.get("card_id")): card for card in cards}
    cards_by_entity = {str(card.get("details", {}).get("entity_id")): card for card in cards}
    for directive in directives:
        target = str(directive.get("target_card_id") or directive.get("target_entity_id") or "")
        card = cards_by_id.get(target) or cards_by_entity.get(target)
        if not card:
            continue
        if "parsed_payload" not in directive:
            directive["parsed_payload"] = parse_author_instruction(str(directive.get("instruction_text", "")))
        updated, note = apply_directive_to_card(card, directive)
        updated.setdefault("revision_history", []).append(
            {
                "timestamp_utc": now_utc_iso(),
                "action": "author_directive_applied",
                "actor": directive.get("author", "author"),
                "decision": "accept",
                "rationale": f"{note}: {directive.get('instruction_text', '')}",
            }
        )


def _promote_approved_cards(cards: list[dict[str, Any]], card_decisions: list[dict[str, Any]]) -> list[dict[str, Any]]:
    decision_by_card = _latest_decision_by_card(card_decisions)
    canonical: list[dict[str, Any]] = []
    for card in cards:
        card_id = str(card.get("card_id", ""))
        entity_id = str(card.get("details", {}).get("entity_id", ""))
        decision = decision_by_card.get(card_id) or decision_by_card.get(entity_id)
        if not decision:
            continue
        action = str(decision.get("decision", "")).lower()
        if action not in VALID_CARD_DECISIONS or action not in {"approve", "accept"}:
            continue
        approved = {**card, "status": "canonical"}
        if decision.get("edited_summary"):
            approved["summary"] = str(decision["edited_summary"])
        if isinstance(decision.get("edited_sections"), dict):
            approved.setdefault("details", {}).setdefault("sections", {}).update(decision["edited_sections"])
        approved.setdefault("revision_history", []).append(
            {
                "timestamp_utc": decision.get("timestamp_utc", now_utc_iso()),
                "action": "card_review_approved",
                "actor": decision.get("reviewer", "reviewer"),
                "decision": "accept",
                "rationale": decision.get("rationale", ""),
            }
        )
        canonical.append(approved)
    return canonical


def _load_existing_canonical_cards(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    try:
        payload = read_json(path)
    except Exception:
        return []
    cards = payload.get("cards", []) if isinstance(payload, dict) else []
    return [card for card in cards if isinstance(card, dict) and card.get("status") == "canonical"]


def merge_canonical_cards(existing: list[dict[str, Any]], approved_revisions: list[dict[str, Any]]) -> list[dict[str, Any]]:
    merged = {str(card.get("card_id")): card for card in existing if str(card.get("card_id", "")).strip()}
    for card in approved_revisions:
        card_id = str(card.get("card_id", "")).strip()
        if card_id:
            merged[card_id] = card
    return sorted(merged.values(), key=lambda card: str(card.get("canonical_name", "")))


def default_source_snippets_path(in_claim_drafts_json: Path) -> Path | None:
    try:
        run_root = in_claim_drafts_json.parents[2]
    except IndexError:
        return None
    candidate = run_root / "03_relevance" / "snippets_candidates.jsonl"
    return candidate if candidate.exists() else None


def load_source_snippets_by_id(path: Path | None) -> dict[str, dict[str, Any]]:
    if path is None or not path.exists():
        return {}
    snippets: dict[str, dict[str, Any]] = {}
    for row in read_jsonl(path):
        snippet_id = str(row.get("snippet_id", "")).strip()
        if snippet_id:
            snippets[snippet_id] = row
    return snippets


def run(
    in_entities_json: Path,
    in_claim_drafts_json: Path,
    in_claim_decisions_json: Path,
    in_card_review_decisions_json: Path,
    in_author_directives_json: Path,
    in_review_memory_json: Path,
    out_card_drafts_json: Path,
    out_cards_json: Path,
    out_merge_log_jsonl: Path,
    in_pipeline_config_json: Path | None = None,
    in_source_snippets_jsonl: Path | None = None,
) -> None:
    logger = get_logger(__name__)
    identity_merge_proposals_path = out_card_drafts_json.with_name("identity_merge_proposals.json")
    identity_merge_decisions_path = out_card_drafts_json.with_name("identity_merge_decisions.json")
    if not identity_merge_decisions_path.exists():
        write_json(identity_merge_decisions_path, {"decisions": []})
    existing_canonical_cards = _load_existing_canonical_cards(out_cards_json)
    entities = load_entity_records(in_entities_json)
    entity_by_id = {str(e.get("entity_id")): e for e in entities}
    entities_by_name = {normalized_name_key(e.get("canonical_name", "")): e for e in entities}
    claims_payload = read_json(in_claim_drafts_json) if in_claim_drafts_json.exists() else {"claims": []}
    claims = [c for c in claims_payload.get("claims", []) if isinstance(c, dict)]
    claim_decisions = _load_decisions(in_claim_decisions_json)
    card_decisions = _load_decisions(in_card_review_decisions_json)
    directives = _load_directives(in_author_directives_json)
    memory = load_review_memory(in_review_memory_json)
    config: dict[str, Any] = {}
    if in_pipeline_config_json and in_pipeline_config_json.exists():
        config = read_json(in_pipeline_config_json)
    source_snippets_path = in_source_snippets_jsonl or default_source_snippets_path(in_claim_drafts_json)
    source_snippets_by_id = load_source_snippets_by_id(source_snippets_path)

    logger.info(
        "Stage 10: claims=%d claim_decisions=%d card_decisions=%d directives=%d source_snippets=%d",
        len(claims),
        len(claim_decisions),
        len(card_decisions),
        len(directives),
        len(source_snippets_by_id),
    )
    accepted_claims, merge_log = apply_claim_decisions(claims, claim_decisions)
    remember_claim_decisions(memory, claims, claim_decisions)
    remember_author_directives(memory, directives)
    identity_merge_decisions = _load_identity_merge_decisions(identity_merge_decisions_path)
    identity_merge_proposals = detect_identity_merge_proposals(accepted_claims, entities)
    remember_identity_merge_decisions(memory, identity_merge_proposals, identity_merge_decisions)
    identity_merge_proposals = annotate_identity_merge_proposals(identity_merge_proposals, identity_merge_decisions)
    write_json(
        identity_merge_proposals_path,
        {
            "generated_at_utc": now_utc_iso(),
            "proposals": identity_merge_proposals,
            "decisions_path": str(identity_merge_decisions_path),
        },
    )
    pending_identity_merges = [
        proposal for proposal in identity_merge_proposals if str(proposal.get("review_status", "pending")) == "pending"
    ]
    if pending_identity_merges:
        save_review_memory(in_review_memory_json, memory)
        raise RuntimeError(
            f"Stage 10 found {len(pending_identity_merges)} identity merge proposal(s) requiring review; "
            f"review {identity_merge_proposals_path} and save decisions to {identity_merge_decisions_path}, then rerun Stage 10."
        )

    merged_entities, merge_target_map, sources_by_target = apply_entity_merges_to_entities(
        entities,
        approved_entity_merges_from_memory(memory),
    )
    entity_by_id = {str(e.get("entity_id")): e for e in merged_entities}
    original_entity_by_id = {str(e.get("entity_id")): e for e in entities}
    entities_by_name = {}
    for entity in merged_entities:
        entities_by_name[normalized_name_key(entity.get("canonical_name", ""))] = entity
        for alias in entity.get("aliases", []) or []:
            entities_by_name[normalized_name_key(str(alias))] = entity
    accepted_claims = remap_claims_for_entity_merges(accepted_claims, entity_by_id, merge_target_map)

    accepted_by_entity: dict[str, list[dict[str, Any]]] = {}
    for claim in accepted_claims:
        accepted_by_entity.setdefault(str(claim.get("target_entity_id")), []).append(claim)

    draft_cards: list[dict[str, Any]] = []
    synthesis_failures: list[dict[str, Any]] = []
    synthesis_total = len(accepted_by_entity)
    logger.info(
        "Stage 10 progress: 0/%d preparing card synthesis entities",
        synthesis_total,
    )
    for synthesis_index, (entity_id, entity_claims) in enumerate(accepted_by_entity.items(), start=1):
        entity = entity_by_id.get(entity_id)
        if not entity:
            logger.info(
                "Stage 10 progress: %d/%d skipping missing entity=%s draft_cards=%d failures=%d",
                synthesis_index,
                synthesis_total,
                entity_id,
                len(draft_cards),
                len(synthesis_failures),
            )
            continue
        memory_for_entity = relevant_memory_for_merged_entity(
            memory,
            entity_id,
            entity,
            sources_by_target,
            original_entity_by_id,
        )
        full_entity_claims = accepted_claim_history_for_entity(memory_for_entity, entity_claims)
        logger.info(
            "Stage 10 model call: %d/%d entity=%s claims=%d",
            synthesis_index,
            synthesis_total,
            entity.get("canonical_name"),
            len(full_entity_claims),
        )
        try:
            synthesis = synthesize_card_with_model(
                entity,
                full_entity_claims,
                memory_for_entity,
                config,
                source_snippets_by_id,
                entities_by_name,
            )
        except RuntimeError as exc:
            synthesis_failures.append(
                {
                    "failure_id": safe_uuid(),
                    "target_entity_id": entity_id,
                    "target_card_id": entity.get("card_id"),
                    "target_entity_name": entity.get("canonical_name"),
                    "accepted_claim_ids": [claim.get("claim_id") for claim in full_entity_claims],
                    "accepted_claim_count": len(full_entity_claims),
                    "error": str(exc),
                    "created_at_utc": now_utc_iso(),
                }
            )
            logger.warning(
                "Stage 10 card synthesis failed entity=%s claims=%d error=%s",
                entity.get("canonical_name"),
                len(full_entity_claims),
                exc,
            )
            logger.info(
                "Stage 10 progress: %d/%d synthesizing cards draft_cards=%d failures=%d",
                synthesis_index,
                synthesis_total,
                len(draft_cards),
                len(synthesis_failures),
            )
            continue
        draft_cards.append(_build_card_from_synthesis(entity, full_entity_claims, synthesis, entities_by_name))
        logger.info(
            "Stage 10 progress: %d/%d synthesizing cards draft_cards=%d failures=%d",
            synthesis_index,
            synthesis_total,
            len(draft_cards),
            len(synthesis_failures),
        )

    if accepted_by_entity and not draft_cards:
        write_json(out_card_drafts_json.with_name("card_synthesis_failures.json"), {"failures": synthesis_failures})
        first_error = str(synthesis_failures[0].get("error", "")) if synthesis_failures else ""
        raise RuntimeError(f"Stage 10 produced no draft cards; see card_synthesis_failures.json. First failure: {first_error}")

    _apply_directives_to_drafts(draft_cards, directives)
    approved_revisions = _promote_approved_cards(draft_cards, card_decisions)
    canonical_cards = merge_canonical_cards(existing_canonical_cards, approved_revisions)
    remember_approved_cards(memory, approved_revisions, card_decisions)
    save_review_memory(in_review_memory_json, memory)

    write_json(out_card_drafts_json, {"cards": sorted(draft_cards, key=lambda x: x.get("canonical_name", ""))})
    write_json(out_cards_json, {"cards": sorted(canonical_cards, key=lambda x: x.get("canonical_name", ""))})
    write_json(out_card_drafts_json.with_name("card_synthesis_failures.json"), {"failures": synthesis_failures})
    write_jsonl(out_merge_log_jsonl, merge_log)
    logger.info(
        "Stage 10 complete: accepted_claims=%d draft_cards=%d synthesis_failures=%d canonical_cards=%d merge_log=%d",
        len(accepted_claims),
        len(draft_cards),
        len(synthesis_failures),
        len(canonical_cards),
        len(merge_log),
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--in-entities-json", type=Path, required=True)
    parser.add_argument("--in-claim-drafts-json", type=Path, required=True)
    parser.add_argument("--in-claim-decisions-json", type=Path, required=True)
    parser.add_argument("--in-card-review-decisions-json", type=Path, required=True)
    parser.add_argument("--in-author-directives-json", type=Path, required=True)
    parser.add_argument("--in-review-memory-json", type=Path, required=True)
    parser.add_argument("--out-card-drafts-json", type=Path, required=True)
    parser.add_argument("--out-cards-json", type=Path, required=True)
    parser.add_argument("--out-merge-log-jsonl", type=Path, required=True)
    parser.add_argument("--in-pipeline-config-json", type=Path, required=False, default=None)
    parser.add_argument("--in-source-snippets-jsonl", type=Path, required=False, default=None)
    args = parser.parse_args()
    run(
        args.in_entities_json,
        args.in_claim_drafts_json,
        args.in_claim_decisions_json,
        args.in_card_review_decisions_json,
        args.in_author_directives_json,
        args.in_review_memory_json,
        args.out_card_drafts_json,
        args.out_cards_json,
        args.out_merge_log_jsonl,
        args.in_pipeline_config_json,
        args.in_source_snippets_jsonl,
    )


if __name__ == "__main__":
    main()
