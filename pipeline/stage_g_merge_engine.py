from __future__ import annotations

import argparse
import json
import re
import time
from pathlib import Path
from typing import Any

from pipeline.author_directives import apply_directive_to_card, parse_author_instruction
from pipeline.card_architecture_agent import (
    apply_card_architecture_actions,
    card_architecture_paths,
    ensure_card_architecture_files,
    prepare_card_architecture_review,
)
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
AUTHOR_CLAIMS_FILENAME = "author_claims.json"
AUTHOR_CLAIM_TRACKS = {"lore", "meta", "both"}
AUTHOR_CLAIM_META_MARKERS = (
    "working name",
    "canonical name",
    "later updated",
    "originally developed",
    "developed based",
    "inspired by",
    "inspiration",
    "player's",
    "player-facing",
    "gameplay",
    "game mechanic",
    "generic reference",
    "likely refer",
)
VERBATIM_CLAIM_REUSE_MIN_CHARS = 70
VERBATIM_CLAIM_REUSE_MIN_WORDS = 8

IDENTITY_MERGE_SCHEMA = {
    "type": "object",
    "properties": {
        "merges": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "source_entity_name": {"type": "string", "description": "The old or previous name of the entity."},
                    "target_entity_name": {"type": "string", "description": "The new or current name of the entity."},
                    "trigger_phrase": {"type": "string", "description": "The exact phrase from the claim that indicates the merge (e.g., 'was renamed to')."},
                    "claim_index": {"type": "integer", "description": "The index of the claim in the provided list."}
                },
                "required": ["source_entity_name", "target_entity_name", "trigger_phrase", "claim_index"],
                "additionalProperties": False
            }
        }
    },
    "required": ["merges"],
    "additionalProperties": False
}

IDENTITY_CLUSTER_CANONICAL_SCHEMA = {
    "type": "object",
    "properties": {
        "clusters": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "cluster_index": {"type": "integer"},
                    "canonical_entity_id": {"type": "string"},
                    "canonical_name": {"type": "string"},
                    "aliases": {"type": "array", "items": {"type": "string"}},
                    "former_names": {"type": "array", "items": {"type": "string"}},
                    "working_names": {"type": "array", "items": {"type": "string"}},
                    "formal_names": {"type": "array", "items": {"type": "string"}},
                    "do_not_merge_entity_ids": {"type": "array", "items": {"type": "string"}},
                    "status": {"type": "string"},
                    "confidence": {"type": "number"},
                    "rationale": {"type": "string"},
                },
                "required": [
                    "cluster_index",
                    "canonical_entity_id",
                    "canonical_name",
                    "aliases",
                    "former_names",
                    "working_names",
                    "formal_names",
                    "do_not_merge_entity_ids",
                    "status",
                    "confidence",
                    "rationale",
                ],
                "additionalProperties": False,
            },
        }
    },
    "required": ["clusters"],
    "additionalProperties": False,
}


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


def default_author_claims_path(in_claim_decisions_json: Path) -> Path:
    return in_claim_decisions_json.with_name(AUTHOR_CLAIMS_FILENAME)


def _entity_lookup_indexes(entities: list[dict[str, Any]]) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]], dict[str, dict[str, Any]]]:
    by_id: dict[str, dict[str, Any]] = {}
    by_card_id: dict[str, dict[str, Any]] = {}
    by_name: dict[str, dict[str, Any]] = {}
    for entity in entities:
        entity_id = str(entity.get("entity_id", "")).strip()
        card_id = str(entity.get("card_id", "")).strip()
        canonical_name = str(entity.get("canonical_name", "")).strip()
        if entity_id:
            by_id[entity_id] = entity
        if card_id:
            by_card_id[card_id] = entity
        if canonical_name:
            by_name[normalized_name_key(canonical_name)] = entity
        for alias in entity.get("aliases", []) or []:
            alias_text = str(alias).strip()
            if alias_text:
                by_name[normalized_name_key(alias_text)] = entity
    return by_id, by_card_id, by_name


def _normalize_claim_text_key(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", str(text or "").lower()).strip()


def _normalize_author_claim_track(raw_track: Any, claim_type: str, claim_text: str) -> str:
    track = str(raw_track or "").strip().lower()
    if track not in AUTHOR_CLAIM_TRACKS:
        track = "lore"
    lower = str(claim_text or "").lower()
    if claim_type in {"meta_note", "open_question", "inspiration"}:
        return "meta"
    if any(marker in lower for marker in AUTHOR_CLAIM_META_MARKERS):
        return "meta"
    return track


def _resolve_author_claim_target(raw_claim: dict[str, Any], entities: list[dict[str, Any]]) -> dict[str, Any] | None:
    by_id, by_card_id, by_name = _entity_lookup_indexes(entities)
    target_entity_id = str(raw_claim.get("target_entity_id", "")).strip()
    target_card_id = str(raw_claim.get("target_card_id", "")).strip()
    target_name = str(raw_claim.get("target_entity_name") or raw_claim.get("canonical_name") or "").strip()
    if target_entity_id and target_entity_id in by_id:
        return by_id[target_entity_id]
    if target_card_id and target_card_id in by_card_id:
        return by_card_id[target_card_id]
    if target_name:
        return by_name.get(normalized_name_key(target_name))

    mentions = _entity_mentions(str(raw_claim.get("claim_text", "")), entities)
    unique_entities: dict[str, dict[str, Any]] = {}
    for mention in mentions:
        entity = mention.get("entity", {})
        entity_id = str(entity.get("entity_id", "")).strip()
        if entity_id:
            unique_entities[entity_id] = entity
    if len(unique_entities) == 1:
        return next(iter(unique_entities.values()))
    return None


def load_author_claims(
    path: Path,
    entities: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    if not path.exists():
        return [], []
    payload = read_json(path)
    rows = payload.get("claims", []) if isinstance(payload, dict) else []
    author_claims: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []
    seen_claim_ids: set[str] = set()
    for index, raw in enumerate(rows):
        if not isinstance(raw, dict):
            continue
        claim_text = str(raw.get("claim_text", "")).strip()
        if not claim_text:
            failures.append({"index": index, "reason": "missing_claim_text", "claim": raw})
            continue
        entity = _resolve_author_claim_target(raw, entities)
        if not entity:
            failures.append(
                {
                    "index": index,
                    "reason": "unresolved_target_entity",
                    "target_entity_id": raw.get("target_entity_id", ""),
                    "target_card_id": raw.get("target_card_id", ""),
                    "target_entity_name": raw.get("target_entity_name") or raw.get("canonical_name") or "",
                    "claim_text": claim_text,
                }
            )
            continue
        target_entity_id = str(entity.get("entity_id", "")).strip()
        target_name = str(entity.get("canonical_name", "")).strip()
        claim_type = str(raw.get("claim_type", "lore_fact") or "lore_fact").strip() or "lore_fact"
        knowledge_track = _normalize_author_claim_track(raw.get("knowledge_track", ""), claim_type, claim_text)
        try:
            confidence = float(raw.get("confidence", 1.0) or 1.0)
        except (TypeError, ValueError):
            confidence = 1.0
        claim_id = str(raw.get("claim_id", "")).strip()
        if not claim_id:
            claim_id = stable_id("author_claim", target_entity_id, claim_type, claim_text)
        if claim_id in seen_claim_ids:
            continue
        seen_claim_ids.add(claim_id)
        author_claims.append(
            {
                **raw,
                "claim_id": claim_id,
                "target_entity_id": target_entity_id,
                "target_card_id": str(entity.get("card_id") or card_id_for_entity(target_name)),
                "target_entity_name": target_name,
                "knowledge_track": knowledge_track,
                "claim_text": claim_text,
                "claim_type": claim_type,
                "source_snippet_ids": [str(item).strip() for item in raw.get("source_snippet_ids", []) or [] if str(item).strip()],
                "confidence": confidence,
                "status": str(raw.get("status", "accepted") or "accepted"),
                "contradiction_notes": str(raw.get("contradiction_notes", "") or ""),
                "created_at_utc": str(raw.get("created_at_utc") or now_utc_iso()),
                "manual_claim": True,
                "author_claim": True,
                "source_priority": "author_claim",
                "normalized_claim_text": str(raw.get("normalized_claim_text") or _normalize_claim_text_key(claim_text)),
            }
        )
    return author_claims, failures


def default_author_claim_decisions(
    author_claims: list[dict[str, Any]],
    existing_decisions: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    decided_claim_ids = {str(decision.get("claim_id", "")).strip() for decision in existing_decisions if isinstance(decision, dict)}
    decisions: list[dict[str, Any]] = []
    for claim in author_claims:
        claim_id = str(claim.get("claim_id", "")).strip()
        if not claim_id or claim_id in decided_claim_ids:
            continue
        decisions.append(
            {
                "claim_id": claim_id,
                "decision": "accept",
                "reviewer": claim.get("reviewer") or "author",
                "rationale": claim.get("review_rationale") or claim.get("rationale") or "Author-supplied claim.",
                "timestamp_utc": claim.get("created_at_utc") or now_utc_iso(),
                "author_claim_default_accept": True,
            }
        )
    return decisions


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
        if str(decision.get("decision_scope", "")).strip() == "identity_edge":
            continue
        proposal_id = str(decision.get("proposal_id") or decision.get("merge_id") or "")
        if proposal_id:
            out[proposal_id] = decision
    return out


def _latest_identity_edge_decisions(decisions: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for decision in decisions:
        if str(decision.get("decision_scope", "")).strip() != "identity_edge":
            continue
        edge_id = str(decision.get("edge_proposal_id") or decision.get("edge_id") or "").strip()
        if edge_id:
            out[edge_id] = decision
    return out


def _rejected_identity_edge_ids_for_cluster(proposal: dict[str, Any], decisions: list[dict[str, Any]]) -> set[str]:
    cluster_id = str(proposal.get("cluster_id") or proposal.get("proposal_id") or "").strip()
    rejected = {
        str(edge_id)
        for edge_id in proposal.get("rejected_edge_proposal_ids", []) or []
        if str(edge_id).strip()
    }
    for decision in decisions:
        if str(decision.get("decision_scope", "")).strip() != "identity_edge":
            continue
        if cluster_id and str(decision.get("cluster_id") or decision.get("proposal_id") or "").strip() not in {"", cluster_id}:
            continue
        edge_id = str(decision.get("edge_proposal_id") or decision.get("edge_id") or "").strip()
        if not edge_id:
            continue
        action = str(decision.get("decision", "")).strip().lower()
        if action in {"reject", "rejected", "refute", "refuted"}:
            rejected.add(edge_id)
        elif action in {"accept", "approve", "keep", "restore"}:
            rejected.discard(edge_id)
    return rejected


def _identity_cluster_connected_member_ids(
    proposal: dict[str, Any],
    target_entity_id: str,
    rejected_edge_ids: set[str],
) -> set[str]:
    member_ids = {
        str(entity_id)
        for entity_id in proposal.get("member_entity_ids", []) or []
        if str(entity_id).strip()
    }
    if not member_ids:
        return set()
    adjacency: dict[str, set[str]] = {entity_id: set() for entity_id in member_ids}
    for edge in proposal.get("member_edges", []) or []:
        if not isinstance(edge, dict):
            continue
        edge_id = str(edge.get("proposal_id", "")).strip()
        if edge_id and edge_id in rejected_edge_ids:
            continue
        source_id = str(edge.get("source_entity_id", "")).strip()
        target_id = str(edge.get("target_entity_id", "")).strip()
        if source_id in member_ids and target_id in member_ids and source_id != target_id:
            adjacency.setdefault(source_id, set()).add(target_id)
            adjacency.setdefault(target_id, set()).add(source_id)
    root_id = target_entity_id if target_entity_id in member_ids else next(iter(member_ids))
    seen: set[str] = set()
    stack = [root_id]
    while stack:
        current = stack.pop()
        if current in seen:
            continue
        seen.add(current)
        stack.extend(sorted(adjacency.get(current, set()) - seen))
    return seen


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
        edited_claim_text = re.sub(r"\s+", " ", str(decision.get("edited_claim_text", "")).strip())
        reviewed_claim = {
            **claim,
            **({"claim_text": edited_claim_text, "review_edited_claim_text": True} if edited_claim_text and action == "accept" else {}),
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
                "source_priority": claim.get("source_priority", "discord_claim_draft"),
                "claim_payload": reviewed_claim,
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


def _cluster_claims_by_entity_overlap(
    claims_with_entities: list[tuple[dict[str, Any], set[str]]],
    max_cluster_size: int = 50
) -> list[dict[str, Any]]:
    """
    Greedily clusters claims so that claims sharing entities are placed adjacent to each other.
    Returns a sorted/reordered list of claims.
    """
    unclustered = list(claims_with_entities)
    reordered_claims = []
    
    while unclustered:
        # Start a new cluster with the first available claim
        current_cluster = [unclustered.pop(0)]
        cluster_entities = set(current_cluster[0][1])
        
        # Grow the cluster greedily by finding claims that overlap with the current cluster's entities
        i = 0
        while i < len(unclustered) and len(current_cluster) < max_cluster_size:
            claim_data, entity_ids = unclustered[i]
            # Check if there's any intersection between the claim's entities and the cluster's entities
            if entity_ids & cluster_entities:
                current_cluster.append(unclustered.pop(i))
                cluster_entities.update(entity_ids)
                # Reset index to scan again since cluster_entities expanded
                i = 0
            else:
                i += 1
                
        # Append the clustered claims to our reordered list
        for claim_data, _ in current_cluster:
            reordered_claims.append(claim_data)
            
    return reordered_claims


GENERIC_IDENTITY_NAMES = {
    "architect",
    "beast",
    "fear",
    "general",
    "joy",
    "loss",
    "love",
    "mech",
    "player",
    "suit",
}


def _clean_text_list(values: Any) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for value in values if isinstance(values, list) else ([] if values in (None, "") else [values]):
        text = str(value or "").strip()
        if not text:
            continue
        key = normalized_name_key(text)
        if key in seen:
            continue
        seen.add(key)
        out.append(text)
    return out


def _first_text_field(payload: dict[str, Any], *keys: str) -> str:
    for key in keys:
        value = payload.get(key)
        if isinstance(value, dict):
            for nested_key in ("name", "title", "value", "text"):
                nested = str(value.get(nested_key, "")).strip()
                if nested:
                    return nested
        text = str(value or "").strip()
        if text:
            return text
    return ""


def _first_list_field(payload: dict[str, Any], *keys: str) -> list[str]:
    for key in keys:
        value = payload.get(key)
        if isinstance(value, list):
            return _clean_text_list(value)
        if isinstance(value, str) and value.strip():
            return _clean_text_list([part.strip() for part in re.split(r"[,;]", value) if part.strip()])
    return []


def _first_float_field(payload: dict[str, Any], *keys: str, fallback: float = 0.65) -> float:
    for key in keys:
        value = payload.get(key)
        try:
            if value is not None and str(value).strip() != "":
                return float(value)
        except (TypeError, ValueError):
            continue
    return fallback


def _identity_name_score(name: str, incoming_edges: int = 0, outgoing_edges: int = 0) -> float:
    clean = re.sub(r"\s+", " ", str(name or "").strip())
    if not clean:
        return -1000.0
    key = normalized_name_key(clean)
    token_count = len(clean.split())
    score = float(incoming_edges * 5 - outgoing_edges)
    score += min(len(clean), 40) / 10.0
    score += token_count * 1.5
    if key in GENERIC_IDENTITY_NAMES:
        score -= 14.0
    if token_count == 1 and len(clean) <= 5:
        score -= 5.0
    if clean.isupper() and len(clean) <= 8:
        score -= 1.0
    return score


def _fallback_identity_cluster_canonical_id(
    member_ids: list[str],
    member_by_id: dict[str, dict[str, Any]],
    edge_proposals: list[dict[str, Any]],
) -> str:
    incoming: dict[str, int] = {}
    outgoing: dict[str, int] = {}
    for edge in edge_proposals:
        source_id = str(edge.get("source_entity_id", "")).strip()
        target_id = str(edge.get("target_entity_id", "")).strip()
        if source_id:
            outgoing[source_id] = outgoing.get(source_id, 0) + 1
        if target_id:
            incoming[target_id] = incoming.get(target_id, 0) + 1
    ranked = sorted(
        member_ids,
        key=lambda entity_id: (
            _identity_name_score(
                str(member_by_id.get(entity_id, {}).get("canonical_name", "")),
                incoming.get(entity_id, 0),
                outgoing.get(entity_id, 0),
            ),
            str(member_by_id.get(entity_id, {}).get("canonical_name", "")).lower(),
        ),
        reverse=True,
    )
    return ranked[0] if ranked else ""


def _identity_cluster_source_display(
    member_ids: list[str],
    canonical_entity_id: str,
    member_by_id: dict[str, dict[str, Any]],
) -> tuple[str, str, str]:
    source_ids = [entity_id for entity_id in member_ids if entity_id != canonical_entity_id]
    if not source_ids and member_ids:
        source_ids = [member_ids[0]]
    source_names = [
        str(member_by_id.get(entity_id, {}).get("canonical_name", entity_id)).strip() or entity_id
        for entity_id in source_ids
    ]
    display_name = " + ".join(source_names[:4])
    if len(source_names) > 4:
        display_name += f" +{len(source_names) - 4}"
    return (source_ids[0] if source_ids else "", display_name, display_name)


def _identity_cluster_alias_texts(
    member_entities: list[dict[str, Any]],
    canonical_entity_id: str,
    canonical_name: str,
    extra_aliases: list[str] | None = None,
) -> list[str]:
    aliases: list[str] = []
    canonical_key = normalized_name_key(canonical_name)
    for entity in member_entities:
        entity_name = str(entity.get("canonical_name", "")).strip()
        if str(entity.get("entity_id", "")) != canonical_entity_id and entity_name:
            aliases.append(entity_name)
        for alias in entity.get("aliases", []) or []:
            aliases.append(str(alias))
    aliases.extend(extra_aliases or [])
    return [text for text in _clean_text_list(aliases) if normalized_name_key(text) != canonical_key]


def _resolve_cluster_canonical_entity_id(
    judgement: dict[str, Any],
    cluster: dict[str, Any],
) -> str:
    member_ids = {str(entity_id) for entity_id in cluster.get("member_entity_ids", []) or []}
    by_name: dict[str, str] = {}
    for entity in cluster.get("member_entities", []) or []:
        entity_id = str(entity.get("entity_id", "")).strip()
        if not entity_id:
            continue
        name = str(entity.get("canonical_name", "")).strip()
        if name:
            by_name[normalized_name_key(name)] = entity_id
        for alias in entity.get("aliases", []) or []:
            alias_text = str(alias).strip()
            if alias_text:
                by_name[normalized_name_key(alias_text)] = entity_id
    proposed_id = str(judgement.get("canonical_entity_id", "")).strip()
    if proposed_id in member_ids:
        return proposed_id
    proposed_name = str(judgement.get("canonical_name", "")).strip()
    if proposed_name and normalized_name_key(proposed_name) in by_name:
        return by_name[normalized_name_key(proposed_name)]
    fallback_id = str(cluster.get("canonical_entity_id") or cluster.get("target_entity_id") or "").strip()
    if fallback_id in member_ids:
        return fallback_id
    return sorted(member_ids)[0] if member_ids else ""


def _build_identity_cluster_proposals(
    edge_proposals: list[dict[str, Any]],
    entities: list[dict[str, Any]],
    config: dict[str, Any] | None,
) -> list[dict[str, Any]]:
    if not edge_proposals:
        return []
    entity_by_id = {str(entity.get("entity_id", "")): entity for entity in entities if str(entity.get("entity_id", "")).strip()}
    parent: dict[str, str] = {}

    def find(entity_id: str) -> str:
        parent.setdefault(entity_id, entity_id)
        while parent[entity_id] != entity_id:
            parent[entity_id] = parent[parent[entity_id]]
            entity_id = parent[entity_id]
        return entity_id

    def union(left: str, right: str) -> None:
        left_root = find(left)
        right_root = find(right)
        if left_root != right_root:
            parent[right_root] = left_root

    touched: set[str] = set()
    for edge in edge_proposals:
        source_id = str(edge.get("source_entity_id", "")).strip()
        target_id = str(edge.get("target_entity_id", "")).strip()
        if source_id and target_id and source_id in entity_by_id and target_id in entity_by_id and source_id != target_id:
            union(source_id, target_id)
            touched.update([source_id, target_id])

    components: dict[str, list[str]] = {}
    for entity_id in touched:
        components.setdefault(find(entity_id), []).append(entity_id)

    clusters: list[dict[str, Any]] = []
    for component_ids in components.values():
        member_ids = sorted(set(component_ids), key=lambda entity_id: str(entity_by_id[entity_id].get("canonical_name", "")).lower())
        if len(member_ids) < 2:
            continue
        member_set = set(member_ids)
        member_edges = [
            edge
            for edge in edge_proposals
            if str(edge.get("source_entity_id", "")) in member_set and str(edge.get("target_entity_id", "")) in member_set
        ]
        canonical_entity_id = _fallback_identity_cluster_canonical_id(member_ids, entity_by_id, member_edges)
        canonical_entity = entity_by_id.get(canonical_entity_id, {})
        canonical_name = str(canonical_entity.get("canonical_name", "")).strip()
        source_entity_id, source_entity_name, alias_text = _identity_cluster_source_display(member_ids, canonical_entity_id, entity_by_id)
        member_entities = [
            {
                "entity_id": str(entity_by_id[entity_id].get("entity_id", "")),
                "card_id": str(entity_by_id[entity_id].get("card_id", "")),
                "canonical_name": str(entity_by_id[entity_id].get("canonical_name", "")),
                "entity_type": str(entity_by_id[entity_id].get("entity_type", "")),
                "aliases": _clean_text_list(entity_by_id[entity_id].get("aliases", [])),
            }
            for entity_id in member_ids
        ]
        evidence_claim_ids = _clean_text_list(
            [claim_id for edge in member_edges for claim_id in edge.get("evidence_claim_ids", []) or []]
        )
        source_snippet_ids = _clean_text_list(
            [snippet_id for edge in member_edges for snippet_id in edge.get("source_snippet_ids", []) or []]
        )
        evidence = []
        for edge in member_edges:
            for item in edge.get("evidence", []) or []:
                if isinstance(item, dict):
                    evidence.append(item)
        aliases = _identity_cluster_alias_texts(member_entities, canonical_entity_id, canonical_name)
        cluster_id = stable_id("identity_merge_cluster", *member_ids)
        clusters.append(
            {
                "proposal_id": cluster_id,
                "cluster_id": cluster_id,
                "proposal_kind": "identity_cluster",
                "member_entity_ids": member_ids,
                "member_entities": member_entities,
                "canonical_entity_id": canonical_entity_id,
                "canonical_card_id": canonical_entity.get("card_id", ""),
                "canonical_name": canonical_name,
                "target_entity_id": canonical_entity_id,
                "target_card_id": canonical_entity.get("card_id", ""),
                "target_entity_name": canonical_name,
                "source_entity_id": source_entity_id,
                "source_card_id": entity_by_id.get(source_entity_id, {}).get("card_id", ""),
                "source_entity_name": source_entity_name,
                "alias_text": alias_text,
                "alias_texts": aliases,
                "former_names": [],
                "working_names": [],
                "formal_names": [],
                "merge_type": "identity_cluster",
                "status": "proposed",
                "review_status": "pending",
                "confidence": 0.65,
                "rationale": "Deterministically collated connected identity/rename evidence into one reviewable entity cluster.",
                "cluster_review_flags": [],
                "suggested_split_entity_ids": [],
                "edge_proposal_ids": [str(edge.get("proposal_id", "")) for edge in member_edges if str(edge.get("proposal_id", "")).strip()],
                "member_edges": member_edges,
                "evidence_claim_ids": evidence_claim_ids,
                "source_snippet_ids": source_snippet_ids,
                "evidence": evidence,
                "created_at_utc": now_utc_iso(),
            }
        )

    refined = _refine_identity_clusters_with_model(clusters, config or {})
    return sorted(refined, key=lambda proposal: str(proposal.get("canonical_name") or proposal.get("target_entity_name", "")).lower())


def _refine_identity_clusters_with_model(clusters: list[dict[str, Any]], config: dict[str, Any]) -> list[dict[str, Any]]:
    if not clusters:
        return []
    tasks = config.get("model_routing", {}).get("tasks", {}) if isinstance(config.get("model_routing", {}), dict) else {}
    if "stage_g_identity_merge_cluster_judgement" not in tasks:
        return clusters

    logger = get_logger(__name__)
    call_kwargs = model_call_kwargs(config, "stage_g_identity_merge_cluster_judgement")
    mixtral_cfg = config.get("mixtral", {}) if isinstance(config, dict) else {}
    provider_retries = max(0, int(mixtral_cfg.get("identity_merge_provider_retries", mixtral_cfg.get("synthesis_provider_retries", 2))))
    provider_retry_sleep_seconds = max(
        0.0,
        float(
            mixtral_cfg.get(
                "identity_merge_provider_retry_sleep_seconds",
                mixtral_cfg.get("synthesis_provider_retry_sleep_seconds", mixtral_cfg.get("rate_limit_cooldown_seconds", 30)),
            )
        ),
    )
    refined = [dict(cluster) for cluster in clusters]
    batch_size = 8
    batch_failures = 0
    for start in range(0, len(refined), batch_size):
        batch = refined[start:start + batch_size]
        prompt_lines = [
            "You are reviewing identity-merge clusters for a lore wiki pipeline.",
            "Each cluster was made by collapsing pairwise rename/alias evidence into one connected component.",
            "For each cluster, choose the best final wiki page title and canonical entity id.",
            "Use a natural page title, not necessarily the longest name. Treat generic or working names such as 'Loss', 'Suit', 'Fear', 'General', or 'Mech' as aliases when a clearer current name is supported.",
            "If a member looks related but not identical, keep the cluster reviewable and list that member in do_not_merge_entity_ids; do not invent entity ids.",
            "Return one judgement per cluster_index.",
            "",
        ]
        for local_index, cluster in enumerate(batch):
            prompt_lines.append(f"Cluster {local_index}:")
            prompt_lines.append("Members:")
            for entity in cluster.get("member_entities", []) or []:
                prompt_lines.append(
                    f"- id={entity.get('entity_id')} name={entity.get('canonical_name')} "
                    f"type={entity.get('entity_type')} aliases={_clean_text_list(entity.get('aliases', []))}"
                )
            prompt_lines.append("Evidence edges:")
            for edge in (cluster.get("member_edges", []) or [])[:10]:
                evidence_texts = []
                for evidence in edge.get("evidence", []) or []:
                    if isinstance(evidence, dict) and evidence.get("claim_text"):
                        evidence_texts.append(str(evidence.get("claim_text")))
                prompt_lines.append(
                    f"- {edge.get('source_entity_name')} -> {edge.get('target_entity_name')} "
                    f"trigger={edge.get('merge_type') or 'identity'} claims={edge.get('evidence_claim_ids', [])} "
                    f"text={'; '.join(evidence_texts[:2])[:900]}"
                )
            prompt_lines.append("")
        prompt = "\n".join(prompt_lines)
        batch_number = start // batch_size + 1
        batch_total = (len(refined) + batch_size - 1) // batch_size
        provider_failures = 0
        while True:
            logger.info(
                "Sending identity cluster canonical-name prompt to LLM (batch %d/%d, clusters=%d)...",
                batch_number,
                batch_total,
                len(batch),
            )
            response = call_mixtral_chat(prompt=prompt, json_schema=IDENTITY_CLUSTER_CANONICAL_SCHEMA, **call_kwargs)
            if response is None:
                status = get_mixtral_runtime_status()
                reason = str(status.get("last_mistral_skip_reason") or "provider_unavailable")
                sleep_s = provider_wait_seconds(reason, status, provider_retry_sleep_seconds)
                if reason in PACING_SKIP_REASONS:
                    if sleep_s:
                        logger.info(
                            "Stage 10 identity cluster provider pacing for batch %d/%d; retrying in %.1fs (%s).",
                            batch_number,
                            batch_total,
                            sleep_s,
                            reason,
                        )
                        time.sleep(sleep_s)
                    continue
                provider_failures += 1
                if provider_failures > provider_retries:
                    batch_failures += 1
                    logger.error(
                        "Failed to judge identity clusters with LLM (batch %d/%d): provider returned no response (%s).",
                        batch_number,
                        batch_total,
                        reason,
                    )
                    break
                if sleep_s:
                    logger.info(
                        "Stage 10 waiting %.1fs before retrying identity cluster batch %d/%d after provider returned no response (%s).",
                        sleep_s,
                        batch_number,
                        batch_total,
                        reason,
                    )
                    time.sleep(sleep_s)
                continue
            cluster_rows = None
            if isinstance(response, dict) and isinstance(response.get("clusters"), list):
                cluster_rows = response.get("clusters")
            elif isinstance(response, dict) and response.get("_json_root_type") == "list" and isinstance(response.get("_json_root"), list):
                cluster_rows = response.get("_json_root")
            if cluster_rows is None:
                batch_failures += 1
                logger.error(
                    "Failed to judge identity clusters with LLM (batch %d/%d): invalid response shape keys=%s",
                    batch_number,
                    batch_total,
                    sorted(response.keys()) if isinstance(response, dict) else type(response).__name__,
                )
                break
            for judgement in cluster_rows or []:
                if not isinstance(judgement, dict):
                    continue
                try:
                    local_index = int(judgement.get("cluster_index"))
                except (TypeError, ValueError):
                    continue
                if local_index < 0 or local_index >= len(batch):
                    continue
                cluster = batch[local_index]
                normalized_judgement = {
                    **judgement,
                    "canonical_entity_id": _first_text_field(
                        judgement,
                        "canonical_entity_id",
                        "target_entity_id",
                        "chosen_entity_id",
                        "canonical_id",
                    ),
                    "canonical_name": _first_text_field(
                        judgement,
                        "canonical_name",
                        "canonical_page_title",
                        "suggested_canonical_name",
                        "final_canonical_name",
                        "page_title",
                        "display_name",
                    ),
                    "aliases": _first_list_field(judgement, "aliases", "alias_texts", "alias_names"),
                    "former_names": _first_list_field(judgement, "former_names", "old_names", "previous_names"),
                    "working_names": _first_list_field(judgement, "working_names", "working_name_aliases"),
                    "formal_names": _first_list_field(judgement, "formal_names", "full_names", "formal_name"),
                    "do_not_merge_entity_ids": _first_list_field(
                        judgement,
                        "do_not_merge_entity_ids",
                        "split_entity_ids",
                        "possible_false_identity_entity_ids",
                    ),
                    "status": _first_text_field(judgement, "status", "review_status", "recommendation"),
                    "confidence": _first_float_field(judgement, "confidence", "confidence_score", "canonical_confidence", fallback=float(cluster.get("confidence", 0.65) or 0.65)),
                    "rationale": _first_text_field(judgement, "rationale", "reason", "reasoning", "explanation", "notes"),
                }
                canonical_entity_id = _resolve_cluster_canonical_entity_id(normalized_judgement, cluster)
                member_entities = cluster.get("member_entities", []) or []
                canonical_entity = next((entity for entity in member_entities if str(entity.get("entity_id", "")) == canonical_entity_id), {})
                canonical_name = str(normalized_judgement.get("canonical_name") or canonical_entity.get("canonical_name") or cluster.get("canonical_name") or "").strip()
                if not canonical_name:
                    canonical_name = str(canonical_entity.get("canonical_name", "")).strip()
                aliases = _identity_cluster_alias_texts(
                    member_entities,
                    canonical_entity_id,
                    canonical_name,
                    _clean_text_list(normalized_judgement.get("aliases", [])),
                )
                source_entity_id, source_entity_name, alias_text = _identity_cluster_source_display(
                    [str(item) for item in cluster.get("member_entity_ids", []) or []],
                    canonical_entity_id,
                    {str(entity.get("entity_id", "")): entity for entity in member_entities},
                )
                flags = _clean_text_list(cluster.get("cluster_review_flags", []))
                status = str(normalized_judgement.get("status", "")).strip()
                split_ids = [entity_id for entity_id in _clean_text_list(normalized_judgement.get("do_not_merge_entity_ids", [])) if entity_id in cluster.get("member_entity_ids", [])]
                if split_ids and "possible_false_identity_edge" not in flags:
                    flags.append("possible_false_identity_edge")
                if status and status not in {"ready_for_review", "ok", "ready"}:
                    flags.append(status)
                cluster.update(
                    {
                        "canonical_entity_id": canonical_entity_id,
                        "canonical_card_id": canonical_entity.get("card_id", ""),
                        "canonical_name": canonical_name,
                        "target_entity_id": canonical_entity_id,
                        "target_card_id": canonical_entity.get("card_id", ""),
                        "target_entity_name": canonical_name,
                        "source_entity_id": source_entity_id,
                        "source_card_id": next(
                            (
                                entity.get("card_id", "")
                                for entity in member_entities
                                if str(entity.get("entity_id", "")) == source_entity_id
                            ),
                            "",
                        ),
                        "source_entity_name": source_entity_name,
                        "alias_text": alias_text,
                        "alias_texts": aliases,
                        "former_names": _clean_text_list(normalized_judgement.get("former_names", [])),
                        "working_names": _clean_text_list(normalized_judgement.get("working_names", [])),
                        "formal_names": _clean_text_list(normalized_judgement.get("formal_names", [])),
                        "suggested_split_entity_ids": split_ids,
                        "cluster_review_flags": flags,
                        "confidence": normalized_judgement.get("confidence", cluster.get("confidence", 0.65)),
                        "rationale": str(normalized_judgement.get("rationale") or cluster.get("rationale") or ""),
                        "canonical_judgement_model_task": "stage_g_identity_merge_cluster_judgement",
                    }
                )
            break
    if batch_failures:
        logger.warning("LLM identity cluster canonical judgement: %d/%d batches failed", batch_failures, (len(refined) + batch_size - 1) // batch_size)
    logger.info("LLM identity cluster canonical judgement complete: %d cluster(s), %d batch failure(s)", len(refined), batch_failures)
    return refined


def detect_identity_merge_proposals(
    accepted_claims: list[dict[str, Any]],
    entities: list[dict[str, Any]],
    config: dict[str, Any],
) -> list[dict[str, Any]]:
    logger = get_logger(__name__)

    # Determine whether to use the heuristic or LLM-based approach.
    # We use LLM only if a pipeline config is present AND routes
    # "stage_g_identity_merge_proposals" to a model.  In unit tests the
    # config is empty / absent, so the fast heuristic path runs instead.
    use_llm = False
    if config:
        task_routing = config.get("model_routing", {}).get("tasks", {})
        if "stage_g_identity_merge_proposals" in task_routing:
            use_llm = True

    if not use_llm:
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
        return _build_identity_cluster_proposals(list(proposals_by_pair.values()), entities, config)

    candidate_claims_with_entities = []
    for claim in accepted_claims:
        text = str(claim.get("claim_text", "")).strip()
        if not text:
            continue
        mentions = _entity_mentions(text, entities)
        unique_entity_ids = {str(m["entity"].get("entity_id")) for m in mentions if str(m["entity"].get("entity_id"))}
        if len(unique_entity_ids) >= 2:
            candidate_claims_with_entities.append((claim, unique_entity_ids))
            
    proposals_by_pair: dict[tuple[str, str], dict[str, Any]] = {}
    
    if not candidate_claims_with_entities:
        return []
        
    # Cluster the claims greedily by entity overlap so similar entities end up in the same batch
    candidate_claims = _cluster_claims_by_entity_overlap(candidate_claims_with_entities, max_cluster_size=50)
    
    call_kwargs = model_call_kwargs(config, "stage_g_identity_merge_proposals")
    batch_size = 50
    batch_failures = 0
    mixtral_cfg = config.get("mixtral", {}) if isinstance(config, dict) else {}
    provider_retries = max(0, int(mixtral_cfg.get("identity_merge_provider_retries", mixtral_cfg.get("synthesis_provider_retries", 2))))
    provider_retry_sleep_seconds = max(
        0.0,
        float(
            mixtral_cfg.get(
                "identity_merge_provider_retry_sleep_seconds",
                mixtral_cfg.get("synthesis_provider_retry_sleep_seconds", mixtral_cfg.get("rate_limit_cooldown_seconds", 30)),
            )
        ),
    )

    for i in range(0, len(candidate_claims), batch_size):
        batch = candidate_claims[i:i+batch_size]
        
        prompt_lines = [
            "Analyze the following claims and identify any that indicate two entities are actually the exact same entity under different names, or that one entity was renamed to, merged into, or is formerly known as another entity.",
            "IMPORTANT: Do NOT merge distinct entities that have a relational connection (e.g., colleagues, partners, family members, creator/creation, boss/employee, weapon/user). They must represent the exact same single individual, concept, or group.",
            "Example of a valid merge: 'The protagonist, also known as the Player Character, is silent.' -> Protagonist and Player Character are the exact same.",
            "Example of an INVALID merge: 'The Partner Character is a senior colleague of the Player Character.' -> A senior colleague is a separate person. Do NOT merge them.",
            "Return a JSON list of merges containing the source_entity_name (old/alias name), target_entity_name (new/canonical name), trigger_phrase (the exact text that indicates the merge), and claim_index.",
            "If there are no merges, return an empty array for 'merges'.",
            ""
        ]
        
        for idx, claim in enumerate(batch):
            text = str(claim.get("claim_text", ""))
            mentions = _entity_mentions(text, entities)
            mention_names = list({m["entity"].get("canonical_name") for m in mentions if m["entity"].get("canonical_name")})
            prompt_lines.append(f"[{idx}] Claim: \"{text}\"")
            prompt_lines.append(f"     Mentions: {', '.join(mention_names)}")
            prompt_lines.append("")

        prompt = "\n".join(prompt_lines)

        try:
            batch_number = i // batch_size + 1
            batch_total = (len(candidate_claims) + batch_size - 1) // batch_size
            provider_failures = 0
            while True:
                logger.info("Sending identity merge prompt to LLM (batch %d/%d, claims=%d)...", batch_number, batch_total, len(batch))
                response = call_mixtral_chat(
                    prompt=prompt,
                    json_schema=IDENTITY_MERGE_SCHEMA,
                    **call_kwargs,
                )
                if response is None:
                    status = get_mixtral_runtime_status()
                    reason = str(status.get("last_mistral_skip_reason") or "provider_unavailable")
                    sleep_s = provider_wait_seconds(reason, status, provider_retry_sleep_seconds)
                    if reason in PACING_SKIP_REASONS:
                        if sleep_s:
                            logger.info(
                                "Stage 10 identity merge provider pacing for batch %d/%d; retrying in %.1fs (%s).",
                                batch_number,
                                batch_total,
                                sleep_s,
                                reason,
                            )
                            time.sleep(sleep_s)
                        continue
                    provider_failures += 1
                    if provider_failures > provider_retries:
                        batch_failures += 1
                        logger.error(
                            "Failed to detect identity merge proposals with LLM (batch %d/%d): provider returned no response (%s).",
                            batch_number,
                            batch_total,
                            reason,
                        )
                        break
                    if sleep_s:
                        logger.info(
                            "Stage 10 waiting %.1fs before retrying identity merge batch %d/%d after provider returned no response (%s).",
                            sleep_s,
                            batch_number,
                            batch_total,
                            reason,
                        )
                        time.sleep(sleep_s)
                    continue
                if not isinstance(response, dict) or "merges" not in response or not isinstance(response.get("merges"), list):
                    batch_failures += 1
                    logger.error(
                        "Failed to detect identity merge proposals with LLM (batch %d/%d): invalid response shape keys=%s",
                        batch_number,
                        batch_total,
                        sorted(response.keys()) if isinstance(response, dict) else type(response).__name__,
                    )
                    break
                for merge in response.get("merges", []):
                    claim_idx = merge.get("claim_index")
                    if claim_idx is None or claim_idx < 0 or claim_idx >= len(batch):
                        continue
                    claim = batch[claim_idx]

                    source = _resolve_author_claim_target({"target_entity_name": merge.get("source_entity_name")}, entities)
                    target = _resolve_author_claim_target({"target_entity_name": merge.get("target_entity_name")}, entities)

                    if not source or not target:
                        continue

                    source_id = str(source.get("entity_id", ""))
                    target_id = str(target.get("entity_id", ""))
                    if not source_id or not target_id or source_id == target_id:
                        continue

                    trigger = merge.get("trigger_phrase", "model_identified")

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
                break
        except Exception as exc:
            batch_failures += 1
            logger.error("Failed to detect identity merge proposals with LLM (batch %d): %s", i // batch_size + 1, exc)

    total_batches = (len(candidate_claims) + batch_size - 1) // batch_size
    if batch_failures:
        logger.warning("LLM identity merge detection: %d/%d batches failed", batch_failures, total_batches)
    logger.info("LLM identity merge detection complete: %d proposals from %d batches (%d failures)", len(proposals_by_pair), total_batches, batch_failures)

    return _build_identity_cluster_proposals(list(proposals_by_pair.values()), entities, config)


def annotate_identity_merge_proposals(
    proposals: list[dict[str, Any]],
    decisions: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    decision_by_proposal = _latest_decision_by_identity_merge(decisions)
    edge_decisions = _latest_identity_edge_decisions(decisions)
    annotated: list[dict[str, Any]] = []
    for proposal in proposals:
        rejected_edge_ids = _rejected_identity_edge_ids_for_cluster(proposal, decisions)
        annotated_edges: list[dict[str, Any]] = []
        for edge in proposal.get("member_edges", []) or []:
            if not isinstance(edge, dict):
                continue
            edge_id = str(edge.get("proposal_id", "")).strip()
            edge_decision = edge_decisions.get(edge_id, {})
            edge_status = str(edge_decision.get("decision", "")).lower() if edge_decision else ""
            if not edge_status:
                edge_status = "rejected" if edge_id in rejected_edge_ids else "pending"
            annotated_edges.append(
                {
                    **edge,
                    "edge_review_status": edge_status,
                    "latest_edge_decision": edge_decision,
                }
            )
        decision = decision_by_proposal.get(str(proposal.get("proposal_id", "")))
        base = {
            **proposal,
            "member_edges": annotated_edges if annotated_edges else proposal.get("member_edges", []),
            "rejected_edge_proposal_ids": sorted(rejected_edge_ids),
        }
        if not decision:
            annotated.append({**base, "review_status": "pending"})
            continue
        action = str(decision.get("decision", "defer")).lower()
        if action not in VALID_IDENTITY_MERGE_DECISIONS:
            action = "defer"
        annotated.append(
            {
                **base,
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
        member_entities = proposal.get("member_entities", []) if isinstance(proposal.get("member_entities", []), list) else []
        member_by_id = {
            str(entity.get("entity_id", "")): entity
            for entity in member_entities
            if isinstance(entity, dict) and str(entity.get("entity_id", "")).strip()
        }
        member_ids = [str(item) for item in proposal.get("member_entity_ids", []) or [] if str(item).strip()]
        if not member_ids:
            member_ids = [
                str(proposal.get("source_entity_id", "")).strip(),
                str(proposal.get("target_entity_id", "")).strip(),
            ]
        decision_canonical_name = str(decision.get("canonical_name", "")).strip()
        target_id = str(decision.get("canonical_entity_id") or "").strip()
        if not target_id and decision_canonical_name:
            decision_name_key = normalized_name_key(decision_canonical_name)
            for entity_id, entity in member_by_id.items():
                names = [str(entity.get("canonical_name", "")), *[str(alias) for alias in entity.get("aliases", []) or []]]
                if any(normalized_name_key(name) == decision_name_key for name in names if name.strip()):
                    target_id = entity_id
                    break
        if not target_id:
            target_id = str(proposal.get("canonical_entity_id") or proposal.get("target_entity_id") or "").strip()
        if target_id not in set(member_ids):
            target_id = str(proposal.get("target_entity_id", "")).strip()
        target_entity = member_by_id.get(target_id, {})
        target_name = str(decision_canonical_name or proposal.get("canonical_name") or proposal.get("target_entity_name") or target_entity.get("canonical_name") or "").strip()
        target_card_id = str(target_entity.get("card_id") or proposal.get("target_card_id") or card_id_for_entity(target_name))
        rejected_edge_ids = _rejected_identity_edge_ids_for_cluster(proposal, decisions)
        excluded_member_ids = {
            str(entity_id)
            for entity_id in proposal.get("suggested_split_entity_ids", []) or []
            if str(entity_id).strip()
        }
        included_member_ids = _identity_cluster_connected_member_ids(proposal, target_id, rejected_edge_ids)
        if not included_member_ids:
            included_member_ids = set(member_ids)
        included_member_ids = {entity_id for entity_id in included_member_ids if entity_id not in excluded_member_ids}
        source_ids = [
            entity_id
            for entity_id in member_ids
            if entity_id and entity_id != target_id and entity_id in included_member_ids
        ]
        if not source_ids and str(proposal.get("source_entity_id", "")).strip():
            source_ids = [str(proposal.get("source_entity_id", "")).strip()]
        cluster_id = str(proposal.get("cluster_id") or proposal.get("proposal_id") or "").strip()
        source_claim_ids = _clean_text_list(proposal.get("evidence_claim_ids", []))
        source_snippet_ids = _clean_text_list(proposal.get("source_snippet_ids", []))
        for source_id in source_ids:
            source_entity = member_by_id.get(source_id, {})
            source_name = str(source_entity.get("canonical_name") or proposal.get("source_entity_name") or source_id).strip()
            source_card_id = str(source_entity.get("card_id") or proposal.get("source_card_id") or card_id_for_entity(source_name))
            merge_key = (source_id, target_id)
            if source_id and target_id and source_id != target_id and merge_key not in existing_merges:
                memory.setdefault("entity_merges", []).append(
                    {
                        "merge_id": stable_id("entity_merge", cluster_id, source_id, target_id),
                        "cluster_id": cluster_id,
                        "source_entity_id": source_id,
                        "source_card_id": source_card_id,
                        "source_entity_name": source_name,
                        "target_entity_id": target_id,
                        "target_card_id": target_card_id,
                        "target_entity_name": target_name,
                        "canonical_name": target_name,
                        "alias_text": source_name,
                        "merge_type": proposal.get("merge_type", "identity_cluster"),
                        "source_claim_ids": source_claim_ids,
                        "source_snippet_ids": source_snippet_ids,
                        "approved_by": decision.get("reviewer", "reviewer"),
                        "rationale": decision.get("rationale", ""),
                        "approved_at_utc": decision.get("timestamp_utc", now_utc_iso()),
                    }
                )
                existing_merges.add(merge_key)

        alias_candidates = _clean_text_list(
            list(proposal.get("alias_texts", []) or [])
            + list(proposal.get("former_names", []) or [])
            + list(proposal.get("working_names", []) or [])
            + list(proposal.get("formal_names", []) or [])
            + [
                str(member_by_id.get(entity_id, {}).get("canonical_name", ""))
                for entity_id in source_ids
            ]
        )
        excluded_alias_keys: set[str] = set()
        for entity_id, entity in member_by_id.items():
            if entity_id in included_member_ids or entity_id == target_id:
                continue
            excluded_alias_keys.add(normalized_name_key(str(entity.get("canonical_name", ""))))
            excluded_alias_keys.update(normalized_name_key(str(alias)) for alias in entity.get("aliases", []) or [])
        for alias_text in alias_candidates:
            alias_key = (target_id, alias_text.lower())
            alias_name_key = normalized_name_key(alias_text)
            if alias_text and alias_name_key not in excluded_alias_keys and alias_name_key != normalized_name_key(target_name) and alias_key not in existing_aliases:
                memory.setdefault("approved_aliases", []).append(
                    {
                        "target_entity_id": target_id,
                        "canonical_name": target_name,
                        "alias_text": alias_text,
                        "source_claim_id": ",".join(str(item) for item in source_claim_ids),
                        "source_snippet_ids": source_snippet_ids,
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
    target_name_overrides: dict[str, str] = {}
    for record in merge_records:
        target_id = str(record.get("target_entity_id", "")).strip()
        target_name = str(record.get("canonical_name") or record.get("target_entity_name") or "").strip()
        if target_id and target_name:
            target_name_overrides[target_id] = target_name
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
        override_name = target_name_overrides.get(target_id, "").strip()
        if override_name and override_name != str(target.get("canonical_name", "")):
            old_name = str(target.get("canonical_name", "")).strip()
            if old_name:
                aliases.add(old_name)
            target["canonical_name"] = override_name
            target["card_id"] = card_id_for_entity(override_name)
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
        "card_architecture_actions": [],
        "card_redirects": [],
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
        for key in [
            "approved_aliases",
            "entity_merges",
            "approved_cards",
            "author_directives",
            "card_architecture_actions",
            "card_redirects",
            "style_corrections",
        ]:
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
            "target_entity_name": claim.get("target_entity_name", ""),
            "knowledge_track": claim.get("knowledge_track", ""),
            "manual_claim": bool(claim.get("manual_claim") or claim.get("author_claim")),
            "source_priority": claim.get("source_priority", ""),
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
Author-supplied manual claims are authoritative accepted claims even when they have no source_snippet_ids. Treat them as reviewer corrections/additions and cite their claim IDs in support_map like any other accepted claim.
Respect each claim's knowledge_track. Lore claims describe in-world THERIAC facts; meta claims describe authorial/design/gameplay/naming/inspiration context. Meta claims may belong in inspirations or careful out-of-world notes, but do not restate them as diegetic facts.
Do not merely summarize summaries. Prefer concrete names, relationships, story functions, chronology, and wording grounded in the accepted claims and their source snippets. Do not use bootstrap lore-bible text as evidence. Do not paste raw chat.
Do not paste accepted claim_text verbatim into the summary or sections. Treat claims as evidence notes to synthesize from. Paraphrase, combine, and organize them into fresh article prose while preserving their meaning. Proper nouns, quest titles, aliases, and short fixed terms may be reused exactly.
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
    verbatim_claim_ids = find_verbatim_claim_reuse(claims, synthesis)
    if verbatim_claim_ids:
        raise RuntimeError(
            "Stage 10 synthesis rejected: card prose copied accepted claim_text verbatim for claim(s): "
            + ", ".join(verbatim_claim_ids)
            + ". Synthesize and paraphrase accepted claims instead."
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


def find_verbatim_claim_reuse(claims: list[dict[str, Any]], synthesis: dict[str, Any]) -> list[str]:
    generated = _normalize_claim_text_key(synthesis_prose_blob(synthesis))
    if not generated:
        return []
    reused: list[str] = []
    for claim in claims:
        claim_text = re.sub(r"\s+", " ", str(claim.get("claim_text", "") or "")).strip()
        if len(claim_text) < VERBATIM_CLAIM_REUSE_MIN_CHARS:
            continue
        if text_word_count(claim_text) < VERBATIM_CLAIM_REUSE_MIN_WORDS:
            continue
        normalized = _normalize_claim_text_key(claim_text)
        if normalized and normalized in generated:
            claim_id = str(claim.get("claim_id", "")).strip()
            reused.append(claim_id or claim_text[:60])
    return reused


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
    author_claims_path = default_author_claims_path(in_claim_decisions_json)
    author_claims, author_claim_failures = load_author_claims(author_claims_path, entities)
    write_json(
        out_card_drafts_json.with_name("author_claim_failures.json"),
        {"generated_at_utc": now_utc_iso(), "failures": author_claim_failures},
    )
    if author_claim_failures:
        raise RuntimeError(
            f"Stage 10 found {len(author_claim_failures)} author claim(s) requiring review because their target "
            f"entities could not be resolved; fix {author_claims_path} and rerun Stage 10."
        )
    author_claim_decisions = default_author_claim_decisions(author_claims, claim_decisions)
    all_claims = claims + author_claims
    all_claim_decisions = claim_decisions + author_claim_decisions
    card_decisions = _load_decisions(in_card_review_decisions_json)
    directives = _load_directives(in_author_directives_json)
    memory = load_review_memory(in_review_memory_json)
    config: dict[str, Any] = {}
    if in_pipeline_config_json and in_pipeline_config_json.exists():
        config = read_json(in_pipeline_config_json)
    source_snippets_path = in_source_snippets_jsonl or default_source_snippets_path(in_claim_drafts_json)
    source_snippets_by_id = load_source_snippets_by_id(source_snippets_path)

    logger.info(
        "Stage 10: claims=%d author_claims=%d claim_decisions=%d synthetic_author_decisions=%d card_decisions=%d directives=%d source_snippets=%d",
        len(claims),
        len(author_claims),
        len(all_claim_decisions),
        len(author_claim_decisions),
        len(card_decisions),
        len(directives),
        len(source_snippets_by_id),
    )
    accepted_claims, merge_log = apply_claim_decisions(all_claims, all_claim_decisions)
    remember_claim_decisions(memory, all_claims, all_claim_decisions)
    remember_author_directives(memory, directives)
    identity_merge_decisions = _load_identity_merge_decisions(identity_merge_decisions_path)
    identity_merge_proposals = detect_identity_merge_proposals(accepted_claims, entities, config)
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
            f"Stage 10 found {len(pending_identity_merges)} identity cluster proposal(s) requiring review; "
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

    review_dir = out_card_drafts_json.parent
    ensure_card_architecture_files(review_dir)
    architecture_paths = card_architecture_paths(review_dir)
    architecture_proposals, pending_architecture_actions, architecture_failures = prepare_card_architecture_review(
        review_dir=review_dir,
        accepted_claims=accepted_claims,
        entities=merged_entities,
        existing_canonical_cards=existing_canonical_cards,
        review_memory=memory,
        source_snippets_by_id=source_snippets_by_id,
        config=config,
    )
    logger.info(
        "Stage 10A Card Architecture Agent: proposals=%d pending_actions=%d validation_failures=%d",
        len(architecture_proposals),
        len(pending_architecture_actions),
        len(architecture_failures),
    )
    if pending_architecture_actions:
        save_review_memory(in_review_memory_json, memory)
        raise RuntimeError(
            f"Stage 10 found {len(pending_architecture_actions)} card architecture proposal action(s) requiring review; "
            f"review {architecture_paths['proposals']} and save decisions to {architecture_paths['decisions']}, then rerun Stage 10."
        )

    architecture_result = apply_card_architecture_actions(
        review_dir=review_dir,
        accepted_claims=accepted_claims,
        entities=merged_entities,
        directives=directives,
        review_memory=memory,
        author_claims_path=author_claims_path,
        directives_path=in_author_directives_json,
    )
    accepted_claims = architecture_result["accepted_claims"]
    merged_entities = architecture_result["entities"]
    directives = architecture_result["directives"]
    merge_log.extend(architecture_result.get("merge_log_rows", []))
    entity_by_id = {str(e.get("entity_id")): e for e in merged_entities}
    entities_by_name = {}
    for entity in merged_entities:
        entities_by_name[normalized_name_key(entity.get("canonical_name", ""))] = entity
        for alias in entity.get("aliases", []) or []:
            entities_by_name[normalized_name_key(str(alias))] = entity

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
