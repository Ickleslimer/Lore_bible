from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Any

from pipeline.common import get_logger, parse_discord_timestamp, read_json, read_jsonl, stable_id, write_json
from pipeline.entity_resolution import (
    card_id_for_entity,
    clean_candidate_name,
    display_name,
    entity_id,
    is_blocked_seed_name,
    is_disallowed_entity_type,
    is_protected_lore_entity_key,
    load_entity_records,
    normalize_entity_type as normalize_shared_entity_type,
    normalized_name_key,
    resolve_entities,
)
from pipeline.model_provider import call_model_chat, model_call_kwargs
from pipeline.review_memory import load_review_memory
from pipeline.review_memory import remember_conversation_entity_decisions, save_review_memory

CONVERSATION_ENTITY_NAME_STOPWORDS = {
    "art",
    "artist",
    "artists",
    "boss",
    "boss fight",
    "canon",
    "character",
    "characters",
    "concept",
    "conversation",
    "design",
    "development",
    "game",
    "games",
    "idea",
    "lore",
    "marketing",
    "mechanic",
    "mechanics",
    "meta",
    "plot",
    "production",
    "project",
    "quest",
    "roadmap",
    "story",
    "theriac",
    "theriac discussion",
    "writing",
}
GENERIC_CONVERSATION_ENTITY_KEYS = {
    "game name",
    "npc",
    "theriac narrative",
    "theriac plot",
}
# Single-token English function words that capitalized sentence starts falsely promote as entities.
ENGLISH_FUNCTION_WORD_KEYS = frozenset(
    {
        "a",
        "an",
        "the",
        "and",
        "or",
        "but",
        "if",
        "as",
        "at",
        "by",
        "for",
        "from",
        "in",
        "into",
        "of",
        "on",
        "onto",
        "to",
        "with",
        "without",
        "about",
        "above",
        "across",
        "after",
        "against",
        "along",
        "among",
        "around",
        "before",
        "behind",
        "below",
        "beneath",
        "beside",
        "between",
        "beyond",
        "down",
        "during",
        "inside",
        "near",
        "off",
        "out",
        "over",
        "through",
        "under",
        "until",
        "up",
        "upon",
        "within",
        "while",
        "is",
        "are",
        "was",
        "were",
        "be",
        "been",
        "being",
        "have",
        "has",
        "had",
        "do",
        "does",
        "did",
        "will",
        "would",
        "could",
        "should",
        "may",
        "might",
        "must",
        "can",
        "i",
        "you",
        "he",
        "she",
        "it",
        "we",
        "they",
        "me",
        "him",
        "her",
        "us",
        "them",
        "my",
        "your",
        "his",
        "its",
        "our",
        "their",
        "this",
        "that",
        "these",
        "those",
        "what",
        "which",
        "who",
        "whom",
        "whose",
        "there",
        "here",
        "not",
        "no",
        "yes",
        "so",
        "than",
        "too",
        "very",
        "just",
        "also",
        "still",
        "even",
        "only",
        "more",
        "most",
        "some",
        "any",
        "each",
        "all",
        "both",
        "few",
        "many",
        "much",
        "such",
    }
)
LOW_VALUE_NAME_SUFFIXES = {
    "arc",
    "arcs",
    "choice",
    "choices",
    "concept",
    "concepts",
    "consequence",
    "consequences",
    "equipment",
    "goals",
    "implants",
    "motivations",
    "narrative",
    "organs",
    "plot",
    "production",
    "state",
    "theme",
    "themes",
}
HIGH_VALUE_UPDATE_TYPES = {
    "introduced",
    "role_change",
    "relationship_change",
    "classification_change",
    "reinforced",
    "contradicted",
}
IDENTITY_UPDATE_TYPES = {"alias", "rename"}
ALIAS_PROPOSAL_UPDATE_TYPES = {"alias", "rename"}
HIGH_VALUE_PATCH_ITEM_TYPES = {"entity_update", "relationship_update", "timeline_update"}
META_TEAM_ROLE_MARKERS = {
    "2d",
    "3d",
    "animator",
    "animation",
    "artist",
    "artists",
    "character art",
    "character artist",
    "character artists",
    "coder",
    "composer",
    "concept art",
    "consultant",
    "developer",
    "graphic designer",
    "illustrator",
    "musician",
    "producer",
    "programmer",
    "scientific consultant",
    "sound designer",
    "voice actor",
    "writer",
}
META_PROJECT_CONTEXT_MARKERS = {
    "art discussion",
    "art discussions",
    "art team",
    "available",
    "co-designed",
    "contribute art",
    "development",
    "do art",
    "for the game",
    "for theriac",
    "game art",
    "logo",
    "on board",
    "project",
    "sketch",
    "team",
    "visuals",
}
META_TEAM_RELATIONSHIP_TYPES = {
    "collaboration",
    "co_designer",
    "co-designed",
    "designed_by",
    "has_artist",
    "has_composer",
    "has_consultant",
    "has_designer",
    "has_writer",
}
EXTERNAL_MEDIA_REFERENCE_MARKERS = {
    "warframe",
    "new war",
    "the new war",
    "the sacrifice",
    "ropalolyst",
    "narmer",
    "corpus",
    "orokin",
    "grineer",
    "sentient",
    "sentients",
    "alad v",
    "zenless zone zero",
    "zzz",
    "sons of calydon",
    "bangboo",
    "bangboos",
    "faction bonus",
    "trust event",
}
KNOWN_EXTERNAL_MEDIA_ENTITY_KEYS = {
    "alad v",
    "erra",
    "hunhow",
    "nef anyo",
    "parvos granum",
    "sons of calydon",
    "sons of calydon quest",
}
CANON_ADOPTION_MARKERS = {
    "introduced as a theriac character",
    "introduced as a theriac entity",
    "is a theriac character",
    "is a theriac entity",
    "becomes a theriac character",
    "becomes a theriac entity",
    "in-world theriac entity",
    "theriac canon entity",
    "theriac character",
    "theriac faction",
    "theriac quest",
    "adapted into theriac",
}
KNOWN_REFERENCE_ONLY_ENTITY_KEYS = {
    "adam smasher",
    "aubrey de grey",
    "gendo",
    "gendo ikari",
    "mamoru oshii",
}
REFERENCE_INSPIRATION_RELATIONSHIP_TYPES = {
    "inspiration",
    "inspired by",
    "influenced_by",
    "influences_art_style",
    "visual inspiration for art style",
}
REFERENCE_INSPIRATION_MARKERS = {
    "inspiration",
    "inspired",
    "inspired by",
    "influence",
    "influenced",
    "influenced by",
    "reference",
    "similar to",
    "akin to",
    "compared to",
    "contrasted with",
    "archetype",
    "art style",
    "visual style",
    "thematic resonance",
}
REVIEW_MIN_EVIDENCE_DEFAULT = 5
REVIEW_MIN_EVIDENCE_TERM = 10
REVIEW_MIN_EVIDENCE_CHARACTER = 5
RECENT_EVIDENCE_WINDOW_DAYS = 45
RECENT_EVIDENCE_MULTIPLIER = 1.35
CURRENT_EVIDENCE_WINDOW_DAYS = 14
CURRENT_EVIDENCE_MULTIPLIER = 1.5

ENTITY_TYPES = {"character", "faction", "organization", "location", "quest", "event", "timeline_node", "term"}
TYPE_RECONSIDERATION_MARGIN = 0.75


def normalize_entity_type(entity_type: Any, default: str = "term") -> str:
    return normalize_shared_entity_type(entity_type, default)

# Music / quest-naming detection
MUSIC_QUEST_CONTEXT_MARKERS = {
    "quest", "path", "route", "ending", "mission",
    "arc", "chapter", "boss", "boss fight", "fight",
}
MUSIC_EVIDENCE_MARKERS = {
    "song", "track", "album", "band", "music",
    "playlist", "ost", "soundtrack", "lyrics",
}
ARTIST_BY_PATTERN = re.compile(r"\bby\s+([A-Z][A-Za-z0-9&' .-]{2,})\b")
MIN_BAND_GROUP_SIZE_FOR_QUEST_PROMOTION = 2


def run(
    in_snippets_jsonl: Path,
    in_seed_json: Path,
    out_alias_json: Path,
    out_timeline_json: Path,
    out_resolved_entities_json: Path | None = None,
    in_review_memory_json: Path | None = None,
    out_conversation_entity_proposals_json: Path | None = None,
    in_conversation_entity_decisions_json: Path | None = None,
    in_pipeline_config_json: Path | None = None,
) -> None:
    logger = get_logger(__name__)
    snippets = read_jsonl(in_snippets_jsonl)
    snippets.sort(key=lambda x: (x.get("timestamp_start_utc", ""), x.get("snippet_id", "")))
    snippets_by_id = {str(snip.get("snippet_id", "")): snip for snip in snippets}
    provider_config = read_json(in_pipeline_config_json) if in_pipeline_config_json and in_pipeline_config_json.exists() else {}
    review_memory = load_review_memory(in_review_memory_json)
    seed_entities = load_entity_records(in_seed_json) + conversation_entity_seed_records_from_memory(review_memory)
    rejected_conversation_entity_keys = conversation_entity_rejected_keys(review_memory)
    conversation_entity_decisions = load_conversation_entity_decisions(in_conversation_entity_decisions_json)
    prior_conversation_entity_proposals = load_existing_conversation_entity_proposals(out_conversation_entity_proposals_json)
    resolved_payload = resolve_entities(seed_entities, review_memory)
    all_resolved_entities = resolved_payload.get("resolved_entities", [])
    logger.info(
        "Stage 07 progress: 0/%d preparing entity resolution inputs",
        len(snippets),
    )
    resolved_entity_by_name = {
        normalized_name_key(str(entity.get("canonical_name", ""))): entity
        for entity in all_resolved_entities
        if normalized_name_key(str(entity.get("canonical_name", "")))
    }
    name_targets: dict[str, dict[str, Any]] = {}
    for entity in all_resolved_entities:
        names = [entity.get("canonical_name", "")] + list(entity.get("aliases", []) or [])
        for name in names:
            key = normalized_name_key(str(name))
            if key:
                name_targets[key] = entity

    alias_entries: list[dict[str, Any]] = []
    timelines: dict[str, list[dict[str, Any]]] = {}
    seen_aliases: dict[tuple[str, str], dict[str, Any]] = {}
    conversation_proposals_by_key: dict[str, dict[str, Any]] = {}

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
            key = (linked_entity_id, matched_name)
            if key not in seen_aliases:
                seen_aliases[key] = {
                    "alias_id": stable_id("alias", linked_entity_id, matched_name),
                    "entity_id": linked_entity_id,
                    "entity_card_id": linked_card_id,
                    "alias_text": matched_name,
                    "alias_type": "working_name",
                    "first_seen_timestamp_utc": snip["timestamp_start_utc"],
                    "last_seen_timestamp_utc": snip["timestamp_end_utc"],
                    "source_snippet_ids": [snip["snippet_id"]],
                    "resolution_confidence": snip.get("relevance_score", 0.5),
                    "resolution_status": "resolved",
                    "notes": "Auto-linked by canonical name mention.",
                }
            else:
                entry = seen_aliases[key]
                entry["last_seen_timestamp_utc"] = snip["timestamp_end_utc"]
                if snip["snippet_id"] not in entry["source_snippet_ids"]:
                    entry["source_snippet_ids"].append(snip["snippet_id"])

        timeline_entry = {
            "timestamp_utc": snip["timestamp_start_utc"],
            "snippet_id": snip["snippet_id"],
            "text": snip.get("display_text_normalized", ""),
            "status": "revision_candidate",
            "match_type": match_type,
        }
        existing = timelines.setdefault(linked_entity_id, [])
        if not any(item.get("snippet_id") == timeline_entry["snippet_id"] and item.get("match_type") == match_type for item in existing):
            existing.append(timeline_entry)

    snippet_heartbeat_every = max(1, min(1000, max(100, len(snippets) // 20 or 1)))
    for snippet_index, snip in enumerate(snippets, start=1):
        evidence_text = snippet_evidence_text(snip)
        lower = evidence_text.lower()
        for name_key, entity in name_targets.items():
            if name_key and re_contains_name(lower, name_key):
                record_observation(
                    entity=entity,
                    matched_name=name_key,
                    snip=snip,
                    match_type="literal_text",
                    add_alias=True,
                )
        for candidate in _snippet_entity_candidates(snip):
            candidate_key = normalized_name_key(candidate)
            if not re_contains_name(lower, candidate_key):
                continue
            entity = _entity_for_candidate_key(candidate_key, name_targets)
            if entity is not None:
                record_observation(
                    entity=entity,
                    matched_name=candidate_key,
                    snip=snip,
                    match_type="candidate_entity_metadata",
                    add_alias=False,
                )
            elif candidate_key not in rejected_conversation_entity_keys and is_viable_conversation_entity_candidate(candidate, snip):
                record_conversation_entity_proposal(conversation_proposals_by_key, candidate, snip)
        if snippet_index == len(snippets) or snippet_index % snippet_heartbeat_every == 0:
            logger.info(
                "Stage 07 progress: %d/%d scanning snippets candidates=%d observations=%d",
                snippet_index,
                len(snippets),
                len(conversation_proposals_by_key),
                sum(len(items) for items in timelines.values()),
            )

    merge_prior_alias_resolution_annotations(conversation_proposals_by_key, prior_conversation_entity_proposals)
    if prior_review_is_fully_decided(prior_conversation_entity_proposals, conversation_entity_decisions):
        alias_resolution_failures = []
        logger.info(
            "Stage 07 progress: %d/%d prior alias proposals are fully decided; skipping model alias grouping and applying saved decisions.",
            len(snippets),
            len(snippets),
        )
    else:
        alias_resolution_failures = add_model_alias_resolution_proposals(
            conversation_proposals_by_key,
            snippets,
            all_resolved_entities,
            provider_config,
            logger,
        )
    logger.info(
        "Stage 07 progress: %d/%d model alias pass complete candidates=%d failures=%d",
        len(snippets),
        len(snippets),
        len(conversation_proposals_by_key),
        len(alias_resolution_failures),
    )

    raw_conversation_entity_proposals = annotate_conversation_entity_proposals(
        sorted(conversation_proposals_by_key.values(), key=lambda x: (x["first_seen_timestamp_utc"], x["candidate_name"])),
        conversation_entity_decisions,
    )
    # Post-annotation: detect band-grouped song-title quest candidates
    promote_band_grouped_quest_candidates(raw_conversation_entity_proposals, snippets)
    triage = triage_conversation_entity_proposals(raw_conversation_entity_proposals)
    conversation_entity_proposals = triage["review_proposals"]
    conversation_entity_candidate_inventory = triage["candidate_inventory"]
    suppressed_conversation_entity_candidates = triage["suppressed_candidates"]
    alias_review_groups = build_alias_review_groups(conversation_entity_proposals)
    logger.info(
        "Stage 07 progress: %d/%d triage complete review_proposals=%d inventory=%d suppressed=%d",
        len(snippets),
        len(snippets),
        len(conversation_entity_proposals),
        len(conversation_entity_candidate_inventory),
        len(suppressed_conversation_entity_candidates),
    )
    remember_conversation_entity_decisions(review_memory, conversation_entity_proposals, conversation_entity_decisions)
    if in_review_memory_json is not None and conversation_entity_decisions:
        save_review_memory(in_review_memory_json, review_memory)
    approved_conversation_entities: list[dict[str, Any]] = []
    for proposal in conversation_entity_proposals:
        if str(proposal.get("review_status", "pending")) != "approved":
            continue
        decision = proposal.get("latest_decision", {}) if isinstance(proposal.get("latest_decision", {}), dict) else {}
        if is_disallowed_entity_type(decision.get("entity_type") or proposal.get("proposed_entity_type") or ""):
            continue
        approved_entity = approved_conversation_entity_from_proposal(proposal, decision, resolved_entity_by_name)
        approved_conversation_entities.append(approved_entity)
        for snippet_id in proposal.get("source_snippet_ids", []):
            snip = snippets_by_id.get(str(snippet_id))
            if snip is None:
                continue
            record_observation(
                entity=approved_entity,
                matched_name=normalized_name_key(str(proposal.get("candidate_name", ""))),
                snip=snip,
                match_type="approved_conversation_entity",
                add_alias=False,
            )
        alias_text = clean_candidate_name(str(proposal.get("candidate_name", "")))
        canonical_name = str(approved_entity.get("canonical_name", ""))
        if alias_text and normalized_name_key(alias_text) != normalized_name_key(canonical_name):
            key = (str(approved_entity.get("entity_id")), normalized_name_key(alias_text))
            first_snip_id = str((proposal.get("source_snippet_ids") or [""])[0])
            first_snip = snippets_by_id.get(first_snip_id, {})
            if key not in seen_aliases:
                seen_aliases[key] = {
                    "alias_id": stable_id("alias", approved_entity.get("entity_id"), normalized_name_key(alias_text)),
                    "entity_id": approved_entity.get("entity_id"),
                    "entity_card_id": approved_entity.get("card_id"),
                    "alias_text": alias_text,
                    "alias_type": "conversation_candidate_name",
                    "first_seen_timestamp_utc": proposal.get("first_seen_timestamp_utc") or first_snip.get("timestamp_start_utc", ""),
                    "last_seen_timestamp_utc": proposal.get("last_seen_timestamp_utc") or first_snip.get("timestamp_end_utc", ""),
                    "source_snippet_ids": proposal.get("source_snippet_ids", []),
                    "resolution_confidence": proposal.get("confidence", 0.6),
                    "resolution_status": "approved",
                    "notes": "Approved conversation-born entity candidate alias.",
                }

    observed_entity_ids = set(timelines)
    observed_entities_by_id = {
        str(entity.get("entity_id", "")): entity
        for entity in all_resolved_entities
        if str(entity.get("entity_id", "")) in observed_entity_ids
    }
    for entity in approved_conversation_entities:
        observed_entities_by_id[str(entity.get("entity_id", ""))] = entity
    alias_entries = sorted(seen_aliases.values(), key=lambda x: (x["entity_card_id"], x["alias_text"]))
    seed_only_entities = [
        {**entity, "observation_status": "seed_only_unobserved"}
        for entity in all_resolved_entities
        if str(entity.get("entity_id", "")) not in observed_entities_by_id
    ]
    output_payload = {
        **resolved_payload,
        "resolved_entities": sorted(observed_entities_by_id.values(), key=lambda x: (x["entity_type"], x["canonical_name"])),
        "seed_only_entities": sorted(seed_only_entities, key=lambda x: (x["entity_type"], x["canonical_name"])),
        "conversation_entity_proposals": conversation_entity_proposals,
        "alias_review_groups": alias_review_groups,
        "conversation_entity_candidate_inventory": conversation_entity_candidate_inventory,
        "suppressed_conversation_entity_candidates": suppressed_conversation_entity_candidates,
        "alias_resolution_failures": alias_resolution_failures,
        "conversation_entity_triage_policy": {
            "review_min_evidence_default": REVIEW_MIN_EVIDENCE_DEFAULT,
            "review_min_evidence_term": REVIEW_MIN_EVIDENCE_TERM,
            "review_min_evidence_character": REVIEW_MIN_EVIDENCE_CHARACTER,
            "notes": (
                "Stage 07 sends explicit entity/relationship/timeline updates, quests, events, recurring characters, "
                "and recurring character/system names to review. Low-evidence phrases are retained in candidate_inventory "
                "instead of blocking the run. Recent evidence is weighted relative to the latest message in the corpus "
                "so newly named entities can reach review with fewer mentions. Generic scaffold names are suppressed."
            ),
        },
        "conversation_entity_decisions_path": str(in_conversation_entity_decisions_json) if in_conversation_entity_decisions_json else "",
        "observation_policy": (
            "Only entities observed in current snippets by literal mention, patch-note evidence text, or candidate "
            "entity metadata whose anchor text is also present in snippet/patch evidence are promoted to "
            "resolved_entities. Bootstrap-only entities remain seed_only_entities for audit/debugging. "
            "Conversation-born entities require an approved conversation entity proposal before promotion."
        ),
        "all_resolved_seed_entities_count": len(all_resolved_entities),
    }
    write_json(out_alias_json, {"aliases": alias_entries})
    write_json(out_timeline_json, {"entity_timelines": timelines})
    if out_resolved_entities_json is not None:
        write_json(out_resolved_entities_json, output_payload)
    if out_conversation_entity_proposals_json is not None:
        write_json(
            out_conversation_entity_proposals_json,
            {
                "proposals": conversation_entity_proposals,
                "alias_review_groups": alias_review_groups,
                "candidate_inventory": conversation_entity_candidate_inventory,
                "suppressed_candidates": suppressed_conversation_entity_candidates,
                "alias_resolution_failures": alias_resolution_failures,
                "triage_summary": {
                    "raw_candidates": len(raw_conversation_entity_proposals),
                    "review_proposals": len(conversation_entity_proposals),
                    "alias_review_groups": len(alias_review_groups),
                    "candidate_inventory": len(conversation_entity_candidate_inventory),
                    "suppressed_candidates": len(suppressed_conversation_entity_candidates),
                    "alias_resolution_failures": len(alias_resolution_failures),
                },
                "decisions_path": str(in_conversation_entity_decisions_json) if in_conversation_entity_decisions_json else "",
            },
        )
    logger.info(
        "Stage 07 complete: observed_resolved_entities=%d seed_only_entities=%d conversation_entity_proposals=%d candidate_inventory=%d suppressed_candidates=%d approved_conversation_entities=%d blocked_entities=%d aliases=%d entity_timelines=%d",
        len(output_payload.get("resolved_entities", [])),
        len(output_payload.get("seed_only_entities", [])),
        len(conversation_entity_proposals),
        len(conversation_entity_candidate_inventory),
        len(suppressed_conversation_entity_candidates),
        len(approved_conversation_entities),
        len(resolved_payload.get("blocked_entities", [])),
        len(alias_entries),
        len(timelines),
    )
    pending_conversation_entities = [
        proposal
        for proposal in conversation_entity_proposals
        if str(proposal.get("review_status", "pending")) == "pending"
    ]
    if pending_conversation_entities:
        raise RuntimeError(
            f"Stage 07 found {len(pending_conversation_entities)} conversation entity proposal(s) requiring review; "
            f"review {out_conversation_entity_proposals_json or 'conversation_entity_proposals.json'} and save decisions to "
            f"{in_conversation_entity_decisions_json or 'conversation_entity_decisions.json'}, then rerun Stage 07."
        )


def _snippet_entity_candidates(snip: dict[str, Any]) -> list[str]:
    values: list[str] = []
    for field in ("candidate_entities", "patch_candidate_entities", "conversation_anchor_entities"):
        raw = snip.get(field, [])
        if isinstance(raw, list):
            values.extend(str(item).strip() for item in raw if str(item).strip())
    return list(dict.fromkeys(values))


def triage_conversation_entity_proposals(proposals: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    review: list[dict[str, Any]] = []
    inventory: list[dict[str, Any]] = []
    suppressed: list[dict[str, Any]] = []
    latest_seen_utc = latest_proposal_timestamp(proposals)
    for proposal in proposals:
        recency_metrics = proposal_recency_metrics(proposal, latest_seen_utc)
        triage_status, triage_reason, review_priority = triage_conversation_entity_proposal(proposal, recency_metrics)
        decorated = {
            **proposal,
            **recency_metrics,
            "triage_status": triage_status,
            "triage_reason": triage_reason,
            "review_priority": review_priority,
        }
        if triage_status == "review_required":
            review.append(decorated)
        elif triage_status == "suppressed":
            suppressed.append(decorated)
        else:
            inventory.append(decorated)
    return {
        "review_proposals": review,
        "candidate_inventory": inventory,
        "suppressed_candidates": suppressed,
    }


def build_alias_review_groups(proposals: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, dict[str, Any]] = {}
    for proposal in proposals:
        if str(proposal.get("review_status", "pending")).strip().lower() != "pending":
            continue
        if not is_alias_candidate_proposal(proposal):
            continue
        target_name = display_name(str(proposal.get("suggested_canonical_name", "")).strip())
        target_key = normalized_name_key(target_name)
        candidate_name = display_name(str(proposal.get("candidate_name", "")).strip())
        candidate_key = normalized_name_key(candidate_name)
        if not target_key or not candidate_key or target_key == candidate_key:
            continue
        group = grouped.setdefault(
            target_key,
            {
                "proposal_id": stable_id("alias_review_group", target_key),
                "group_kind": "alias_review_group",
                "candidate_name": f"{target_name} aliases",
                "normalized_name_key": f"{target_key} aliases",
                "suggested_canonical_name": target_name,
                "proposed_entity_type": normalize_entity_type(proposal.get("proposed_entity_type", "term")),
                "review_status": "pending",
                "triage_status": "review_required",
                "triage_reason": f"alias review group for {target_name}",
                "review_priority": "high",
                "child_proposal_ids": [],
                "alias_candidates": [],
                "source_snippet_ids": [],
                "evidence_count": 0,
                "confidence": 0.0,
                "sample_texts": [],
            },
        )
        if normalize_entity_type(proposal.get("proposed_entity_type", "term")) == "character":
            group["proposed_entity_type"] = "character"
        proposal_id = str(proposal.get("proposal_id", "")).strip()
        if proposal_id and proposal_id not in group["child_proposal_ids"]:
            group["child_proposal_ids"].append(proposal_id)
        group["alias_candidates"].append(
            {
                "proposal_id": proposal_id,
                "candidate_name": candidate_name,
                "proposed_entity_type": normalize_entity_type(proposal.get("proposed_entity_type", "term")),
                "evidence_count": int(proposal.get("evidence_count", 0) or 0),
                "confidence": proposal.get("confidence", 0.0),
                "triage_reason": proposal.get("triage_reason", ""),
                "alias_review_notes": proposal.get("alias_review_notes", []) or [],
                "source_snippet_ids": proposal.get("source_snippet_ids", []) or [],
            }
        )
        for snippet_id in proposal.get("source_snippet_ids", []) or []:
            if snippet_id not in group["source_snippet_ids"]:
                group["source_snippet_ids"].append(snippet_id)
        for sample in proposal.get("sample_texts", []) or []:
            if sample not in group["sample_texts"] and len(group["sample_texts"]) < 6:
                group["sample_texts"].append(sample)
        group["evidence_count"] = int(group["evidence_count"]) + int(proposal.get("evidence_count", 0) or 0)
        try:
            group["confidence"] = max(float(group.get("confidence", 0.0)), float(proposal.get("confidence", 0.0) or 0.0))
        except (TypeError, ValueError):
            pass

    groups = [group for group in grouped.values() if len(group.get("alias_candidates", [])) >= 2]
    for group in groups:
        aliases = sorted(group["alias_candidates"], key=lambda item: (-int(item.get("evidence_count", 0) or 0), str(item.get("candidate_name", "")).lower()))
        group["alias_candidates"] = aliases
        group["candidate_name"] = str(group["suggested_canonical_name"] or "").strip()
        group["triage_reason"] = (
            f"{len(aliases)} name variants proposed for {group['suggested_canonical_name']}"
        )
    return sorted(groups, key=lambda item: str(item.get("suggested_canonical_name", "")).lower())


def promote_band_grouped_quest_candidates(
    proposals: list[dict[str, Any]],
    snippets: list[dict[str, Any]],
) -> None:
    """Detect groups of candidates that co-occur with the same band/artist
    in their evidence and inject quest-type votes for each member.

    Theriac quest-naming convention: quests for the same character share
    a band's (or group of bands') songs as namesakes.  When we find
    multiple un-typed candidates that share an artist
    reference in their evidence, we add strong quest votes so they reach
    review as quest candidates.
    """
    snippets_by_id = {str(s.get("snippet_id", "")): s for s in snippets}

    # Map each proposal â†’ set of artist names found in its evidence.
    proposal_artists: dict[str, set[str]] = {}
    for proposal in proposals:
        if str(proposal.get("review_status", "pending")) != "pending":
            continue
        key = normalized_name_key(str(proposal.get("candidate_name", "")))
        if not key or is_generic_conversation_entity_name(key):
            continue
        artists: set[str] = set()
        for sid in proposal.get("source_snippet_ids", []) or []:
            snip = snippets_by_id.get(str(sid))
            if snip is None:
                continue
            text = snippet_evidence_text(snip)
            for match in ARTIST_BY_PATTERN.finditer(text):
                artist = match.group(1).strip(" .,!?:;")
                if artist and len(artist) >= 3:
                    artists.add(artist.lower())
            # Also pick up music markers from thematic tags
            for tag in snip.get("thematic_tags", []) or []:
                if isinstance(tag, str) and tag.startswith("music:"):
                    marker = tag[len("music:"):].strip()
                    if marker and len(marker) >= 3:
                        artists.add(marker.lower())
        if artists:
            proposal_artists[key] = artists

    if not proposal_artists:
        return

    # Build artist â†’ list of proposal keys that reference it.
    artist_groups: dict[str, list[str]] = {}
    for key, artists in proposal_artists.items():
        for artist in artists:
            artist_groups.setdefault(artist, []).append(key)

    # Find artist groups with enough members to suggest a quest-naming pattern.
    promoted_keys: dict[str, str] = {}
    for artist, members in artist_groups.items():
        if len(members) >= MIN_BAND_GROUP_SIZE_FOR_QUEST_PROMOTION:
            for key in members:
                if key not in promoted_keys:
                    promoted_keys[key] = artist

    if not promoted_keys:
        return

    # Inject quest votes into the promoted proposals.
    for proposal in proposals:
        key = normalized_name_key(str(proposal.get("candidate_name", "")))
        if is_generic_conversation_entity_name(key):
            continue
        artist = promoted_keys.get(key)
        if artist is None:
            continue
        band_vote = {
            "entity_type": "quest",
            "weight": 3.0,
            "basis": f"context:band_grouped_quest_naming_pattern({artist})",
            "snippet_id": "",
        }
        proposal.setdefault("type_evidence", []).append(band_vote)
        proposal.setdefault("band_group_artist", [])
        if artist not in proposal["band_group_artist"]:
            proposal["band_group_artist"].append(artist)
        refresh_type_review_fields(proposal)


def triage_conversation_entity_proposal(
    proposal: dict[str, Any],
    recency_metrics: dict[str, Any] | None = None,
) -> tuple[str, str, str]:
    status = str(proposal.get("review_status", "pending")).strip().lower()
    if status != "pending":
        return "review_required", f"kept because review_status is {status}", "decided"

    name = str(proposal.get("candidate_name", ""))
    key = normalized_name_key(name)
    evidence_count = int(proposal.get("evidence_count", 0) or 0)
    metrics = recency_metrics or proposal_recency_metrics(proposal, None)
    adjusted_evidence_count = float(metrics.get("recency_adjusted_evidence_count", evidence_count) or evidence_count)
    proposed_type = normalize_entity_type(proposal.get("proposed_entity_type", "term"))
    update_types = {str(item).strip().lower() for item in proposal.get("patch_update_types", []) or [] if str(item).strip()}
    patch_item_types = {str(item).strip().lower() for item in proposal.get("patch_item_types", []) or [] if str(item).strip()}
    type_reconsidered = bool(proposal.get("type_reconsidered", False))

    if is_generic_conversation_entity_name(key):
        return "suppressed", "generic scaffold term, not a durable lore-card target", "suppressed"
    if is_alias_candidate_proposal(proposal):
        target = str(proposal.get("suggested_canonical_name", "")).strip()
        if target:
            return "review_required", f"alias/rename evidence suggests this is an alias of {target}", "high"
        return "review_required", "alias/rename evidence requires identity review", "high"
    if is_reference_inspiration_candidate(proposal):
        return (
            "candidate_inventory",
            "reference/inspiration source retained as meta context, not lore entity review",
            "low",
        )
    if is_meta_team_contributor_candidate(proposal):
        return (
            "candidate_inventory",
            "project/team contributor evidence retained as meta inventory, not lore entity review",
            "low",
        )
    if is_external_media_reference_candidate(proposal):
        return (
            "candidate_inventory",
            "external-media character/reference retained as inspiration context, not lore entity review",
            "low",
        )

    if proposed_type in {"quest", "event"}:
        return "review_required", f"{proposed_type} candidate from conversation evidence", "high"
    if update_types & IDENTITY_UPDATE_TYPES:
        return "review_required", f"explicit identity update type: {', '.join(sorted(update_types & IDENTITY_UPDATE_TYPES))}", "high"
    if update_types & HIGH_VALUE_UPDATE_TYPES and adjusted_evidence_count >= REVIEW_MIN_EVIDENCE_DEFAULT and not is_low_value_phrase_name(key):
        return "review_required", recency_triage_reason(
            f"repeated explicit patch update type: {', '.join(sorted(update_types & HIGH_VALUE_UPDATE_TYPES))}",
            evidence_count,
            adjusted_evidence_count,
            metrics,
        ), "high"
    if patch_item_types & HIGH_VALUE_PATCH_ITEM_TYPES and adjusted_evidence_count >= REVIEW_MIN_EVIDENCE_DEFAULT and not is_low_value_phrase_name(key):
        return "review_required", recency_triage_reason(
            f"repeated explicit patch item type: {', '.join(sorted(patch_item_types & HIGH_VALUE_PATCH_ITEM_TYPES))}",
            evidence_count,
            adjusted_evidence_count,
            metrics,
        ), "high"
    if proposed_type == "character" and adjusted_evidence_count >= REVIEW_MIN_EVIDENCE_CHARACTER:
        return "review_required", recency_triage_reason(
            "recurring or type-reconsidered character candidate",
            evidence_count,
            adjusted_evidence_count,
            metrics,
        ), "high" if adjusted_evidence_count >= 10 else "medium"
    if adjusted_evidence_count >= REVIEW_MIN_EVIDENCE_TERM and not is_low_value_phrase_name(key):
        return "review_required", recency_triage_reason(
            "recurring term candidate above review threshold",
            evidence_count,
            adjusted_evidence_count,
            metrics,
        ), "medium"
    if adjusted_evidence_count >= REVIEW_MIN_EVIDENCE_DEFAULT and proposed_type != "term" and not is_low_value_phrase_name(key):
        return "review_required", recency_triage_reason(
            "recurring named candidate above review threshold",
            evidence_count,
            adjusted_evidence_count,
            metrics,
        ), "medium"

    return "candidate_inventory", "low-confidence or phrase-like candidate retained for audit, not human review", "low"


def latest_proposal_timestamp(proposals: list[dict[str, Any]]) -> Any:
    latest = None
    for proposal in proposals:
        if not isinstance(proposal, dict):
            continue
        parsed = parse_optional_utc(str(proposal.get("last_seen_timestamp_utc", "")))
        if parsed is not None and (latest is None or parsed > latest):
            latest = parsed
    return latest


def proposal_recency_metrics(proposal: dict[str, Any], latest_seen_utc: Any) -> dict[str, Any]:
    evidence_count = int(proposal.get("evidence_count", 0) or 0)
    last_seen = parse_optional_utc(str(proposal.get("last_seen_timestamp_utc", "")))
    age_days = None
    multiplier = 1.0
    recency_window = "historical"
    if latest_seen_utc is not None and last_seen is not None:
        age_days = max(0.0, (latest_seen_utc - last_seen).total_seconds() / 86400.0)
        if age_days <= CURRENT_EVIDENCE_WINDOW_DAYS:
            multiplier = CURRENT_EVIDENCE_MULTIPLIER
            recency_window = "current"
        elif age_days <= RECENT_EVIDENCE_WINDOW_DAYS:
            multiplier = RECENT_EVIDENCE_MULTIPLIER
            recency_window = "recent"
    return {
        "recency_age_days": round(age_days, 3) if age_days is not None else None,
        "recency_window": recency_window,
        "recency_evidence_multiplier": multiplier,
        "recency_adjusted_evidence_count": round(evidence_count * multiplier, 3),
    }


def parse_optional_utc(value: str) -> Any:
    if not value:
        return None
    try:
        return parse_discord_timestamp(value)
    except Exception:
        return None


def recency_triage_reason(base_reason: str, raw_count: int, adjusted_count: float, metrics: dict[str, Any]) -> str:
    multiplier = float(metrics.get("recency_evidence_multiplier", 1.0) or 1.0)
    if multiplier <= 1.0:
        return base_reason
    window = str(metrics.get("recency_window") or "recent")
    age = metrics.get("recency_age_days")
    if isinstance(age, (int, float)):
        return f"{base_reason} (recentness boost: {raw_count} raw, {adjusted_count:.2f} adjusted; {window}, {age:.1f} days from corpus latest)"
    return f"{base_reason} (recentness boost: {raw_count} raw, {adjusted_count:.2f} adjusted; {window})"


def is_generic_conversation_entity_name(key: str) -> bool:
    if not key:
        return True
    if is_protected_lore_entity_key(key):
        return False
    if key in GENERIC_CONVERSATION_ENTITY_KEYS:
        return True
    if key in CONVERSATION_ENTITY_NAME_STOPWORDS:
        return True
    if key in ENGLISH_FUNCTION_WORD_KEYS:
        return True
    if " " not in key and len(key) <= 3:
        return True
    return False


def is_low_value_phrase_name(key: str) -> bool:
    parts = key.split()
    if not parts:
        return True
    if parts[-1] in LOW_VALUE_NAME_SUFFIXES:
        return True
    if len(parts) >= 2 and parts[0] in {"theriac", "character", "game", "project"}:
        return True
    return False


def is_meta_team_contributor_candidate(proposal: dict[str, Any]) -> bool:
    proposed_type = normalize_entity_type(proposal.get("proposed_entity_type") or "term")
    if proposed_type not in {"character", "term", "organization", "faction"}:
        return False

    evidence_text = proposal_evidence_text(proposal)
    role_hits = text_marker_hits(evidence_text, META_TEAM_ROLE_MARKERS)
    if not role_hits:
        return False
    context_hits = text_marker_hits(evidence_text, META_PROJECT_CONTEXT_MARKERS)
    relationship_types = {
        str(item).strip().lower()
        for item in proposal.get("patch_relationship_types", []) or []
        if str(item).strip()
    }
    if context_hits:
        return True
    return bool(relationship_types & META_TEAM_RELATIONSHIP_TYPES)


def is_reference_inspiration_candidate(proposal: dict[str, Any]) -> bool:
    key = normalized_name_key(str(proposal.get("candidate_name", "")))
    if key not in KNOWN_REFERENCE_ONLY_ENTITY_KEYS:
        return False
    evidence_text = proposal_evidence_text(proposal)
    if text_marker_hits(evidence_text, CANON_ADOPTION_MARKERS):
        return False
    relationship_types = {
        str(item).strip().lower()
        for item in proposal.get("patch_relationship_types", []) or []
        if str(item).strip()
    }
    return bool(relationship_types & REFERENCE_INSPIRATION_RELATIONSHIP_TYPES) or bool(
        text_marker_hits(evidence_text, REFERENCE_INSPIRATION_MARKERS)
    ) or key in KNOWN_REFERENCE_ONLY_ENTITY_KEYS


def is_external_media_reference_candidate(proposal: dict[str, Any]) -> bool:
    key = normalized_name_key(str(proposal.get("candidate_name", "")))
    if key not in KNOWN_EXTERNAL_MEDIA_ENTITY_KEYS:
        return False
    evidence_text = proposal_evidence_text(proposal)
    if not text_marker_hits(evidence_text, EXTERNAL_MEDIA_REFERENCE_MARKERS):
        return False
    return not text_marker_hits(evidence_text, CANON_ADOPTION_MARKERS)


def is_alias_candidate_proposal(proposal: dict[str, Any]) -> bool:
    kinds = {
        str(item).strip().lower()
        for item in proposal.get("proposal_kinds", []) or []
        if str(item).strip()
    }
    if "alias_candidate" in kinds:
        return True
    return bool(str(proposal.get("suggested_canonical_name", "")).strip())


def proposal_evidence_text(proposal: dict[str, Any]) -> str:
    parts: list[str] = []
    for field in ("candidate_name", "candidate_topics", "sample_texts", "source_kinds", "patch_item_types", "patch_update_types", "patch_relationship_types"):
        value = proposal.get(field)
        if isinstance(value, list):
            parts.extend(str(item) for item in value if str(item).strip())
        elif value:
            parts.append(str(value))
    return "\n".join(parts).lower()


def text_marker_hits(lower_text: str, markers: set[str]) -> set[str]:
    hits: set[str] = set()
    for marker in markers:
        pattern = r"(?<![a-z0-9])" + r"\s+".join(re.escape(part) for part in marker.lower().split()) + r"(?![a-z0-9])"
        if re.search(pattern, lower_text):
            hits.add(marker)
    return hits


def add_model_alias_resolution_proposals(
    proposals_by_key: dict[str, dict[str, Any]],
    snippets: list[dict[str, Any]],
    resolved_entities: list[dict[str, Any]],
    provider_config: dict[str, Any],
    logger: Any,
) -> list[dict[str, Any]]:
    task_cfg = provider_config.get("model_routing", {}).get("tasks", {}).get("stage_07_entity_resolution", {})
    if provider_config and not bool(task_cfg.get("enabled", True)):
        return []
    if not provider_config:
        return []

    alias_snippets = [snip for snip in snippets if is_structured_alias_evidence(snip)]
    candidate_proposals = [
        proposal
        for proposal in proposals_by_key.values()
        if not str(proposal.get("suggested_canonical_name", "")).strip()
    ]
    if not alias_snippets and not candidate_proposals:
        return []

    failures: list[dict[str, Any]] = []
    known_entities = alias_resolution_known_entities(resolved_entities)
    if not known_entities:
        return []
    max_evidence = int(task_cfg.get("max_evidence_per_call", 24) or 24)
    max_evidence = max(1, max_evidence)
    max_candidates = int(task_cfg.get("max_candidates_per_call", 80) or 80)
    max_candidates = max(1, max_candidates)
    alias_call_total = (len(alias_snippets) + max_evidence - 1) // max_evidence if alias_snippets else 0

    if alias_snippets:
        logger.info("Stage 07: requesting model alias resolution for %d alias/rename evidence snippet(s).", len(alias_snippets))
    for index, offset in enumerate(range(0, len(alias_snippets), max_evidence), start=1):
        chunk = alias_snippets[offset : offset + max_evidence]
        prompt = build_alias_resolution_prompt(known_entities, chunk)
        logger.info(
            "Stage 07 model call: %d/%d alias evidence snippets=%d offset=%d",
            index,
            alias_call_total,
            len(chunk),
            offset,
        )
        try:
            response = call_model_chat(prompt=prompt, **model_call_kwargs(provider_config, "stage_07_entity_resolution"))
        except Exception as exc:
            failures.append(
                {
                    "failure_id": stable_id("alias_resolution_failure", str(index), str(offset)),
                    "reason": "model_alias_resolution_failed",
                    "error": str(exc),
                    "source_snippet_ids": [str(snip.get("snippet_id", "")) for snip in chunk],
                }
            )
            continue
        alias_mappings = normalize_alias_resolution_response(response)
        if alias_mappings is None:
            failures.append(
                {
                    "failure_id": stable_id("alias_resolution_failure", str(index), str(offset)),
                    "reason": "model_alias_resolution_invalid_json",
                    "error": "provider returned no alias_mappings list",
                    "response_keys": sorted(response.keys()) if isinstance(response, dict) else [],
                    "source_snippet_ids": [str(snip.get("snippet_id", "")) for snip in chunk],
                }
            )
            continue
        add_alias_mappings_to_proposals(proposals_by_key, alias_mappings, chunk, known_entities)

    candidate_proposals = [
        proposal
        for proposal in proposals_by_key.values()
        if not str(proposal.get("suggested_canonical_name", "")).strip()
    ]
    if candidate_proposals:
        logger.info("Stage 07: requesting model alias resolution for %d unresolved candidate anchor(s).", len(candidate_proposals))
    candidate_call_total = (len(candidate_proposals) + max_candidates - 1) // max_candidates if candidate_proposals else 0
    for index, offset in enumerate(range(0, len(candidate_proposals), max_candidates), start=1):
        chunk = candidate_proposals[offset : offset + max_candidates]
        prompt = build_candidate_alias_resolution_prompt(known_entities, chunk)
        logger.info(
            "Stage 07 model call: %d/%d candidate anchors candidates=%d offset=%d",
            index,
            candidate_call_total,
            len(chunk),
            offset,
        )
        try:
            response = call_model_chat(prompt=prompt, **model_call_kwargs(provider_config, "stage_07_entity_resolution"))
        except Exception as exc:
            failures.append(
                {
                    "failure_id": stable_id("candidate_alias_resolution_failure", str(index), str(offset)),
                    "reason": "model_candidate_alias_resolution_failed",
                    "error": str(exc),
                    "candidate_names": [str(proposal.get("candidate_name", "")) for proposal in chunk],
                }
            )
            continue
        alias_mappings = normalize_alias_resolution_response(response)
        if alias_mappings is None:
            failures.append(
                {
                    "failure_id": stable_id("candidate_alias_resolution_failure", str(index), str(offset)),
                    "reason": "model_candidate_alias_resolution_invalid_json",
                    "error": "provider returned no alias_mappings list",
                    "response_keys": sorted(response.keys()) if isinstance(response, dict) else [],
                    "candidate_names": [str(proposal.get("candidate_name", "")) for proposal in chunk],
                }
            )
            continue
        add_candidate_alias_mappings_to_proposals(proposals_by_key, alias_mappings, chunk, known_entities)
    return failures


def normalize_alias_resolution_response(response: Any) -> list[Any] | None:
    if isinstance(response, dict) and isinstance(response.get("alias_mappings"), list):
        return response["alias_mappings"]
    if isinstance(response, dict) and isinstance(response.get("_json_root"), list):
        return response["_json_root"]
    if isinstance(response, list):
        return response
    return None


def is_structured_alias_evidence(snip: dict[str, Any]) -> bool:
    update_type = str(snip.get("patch_update_type", "")).strip().lower()
    relationship_type = str(snip.get("patch_relationship_type", "")).strip().lower()
    return (
        update_type in ALIAS_PROPOSAL_UPDATE_TYPES
        or "alias" in relationship_type
        or "rename" in relationship_type
    )


def alias_resolution_known_entities(resolved_entities: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    seen_ids = set()
    for entity in resolved_entities:
        if not isinstance(entity, dict):
            continue
        entity_id_value = str(entity.get("entity_id", "")).strip()
        if not entity_id_value or entity_id_value in seen_ids:
            continue
        seen_ids.add(entity_id_value)
        out.append(
            {
                "entity_id": entity_id_value,
                "card_id": entity.get("card_id", ""),
                "canonical_name": entity.get("canonical_name", ""),
                "entity_type": entity.get("entity_type", "term"),
                "aliases": entity.get("aliases", []) or [],
            }
        )
    return sorted(out, key=lambda item: str(item.get("canonical_name", "")).lower())


def build_alias_resolution_prompt(known_entities: list[dict[str, Any]], evidence_snippets: list[dict[str, Any]]) -> str:
    evidence_rows = [
        {
            "snippet_id": snip.get("snippet_id"),
            "timestamp_start_utc": snip.get("timestamp_start_utc"),
            "patch_update_type": snip.get("patch_update_type", ""),
            "patch_relationship_type": snip.get("patch_relationship_type", ""),
            "patch_candidate_entities": snip.get("patch_candidate_entities", []),
            "candidate_entities": snip.get("candidate_entities", []),
            "patch_item_text": snip.get("patch_item_text", ""),
            "conversation_patch_summary": snip.get("conversation_patch_summary", ""),
            "supporting_text": str(snip.get("display_text_normalized", ""))[:1400],
        }
        for snip in evidence_snippets
    ]
    return f"""Resolve alias and rename evidence for Theriac entity review.
Return strict JSON only. Use the evidence rows; do not guess from general knowledge.

Goal:
- Propose mappings where an evidence row says one name is the same in-world entity, working name, prior name, later name, codename, official name, or abbreviation of another.
- Prefer mapping old/working/variant names to the latest or most canonical name when the evidence states a rename.
- Use known_entities when the canonical target is already listed. If the target is clear from evidence but absent from known_entities, still return canonical_name with an empty target_entity_id.
- Do not map thematic parallels, inspirations, translations/etymology, relationship labels, quest titles, organizations, real people, or external media unless the evidence explicitly says they are names for the same Theriac entity.
- Do not create mappings for weak phrasing like "considered", "proposed", "potential", "darker X", "favorite", or "inspired by" unless the evidence also confirms the name is used/adopted.

Known entities:
{json.dumps(known_entities, ensure_ascii=False, indent=2)}

Alias/rename evidence:
{json.dumps(evidence_rows, ensure_ascii=False, indent=2)}

Return JSON object:
{{
  "alias_mappings": [
    {{
      "alias_text": "old, alternate, codename, abbreviation, or working name",
      "canonical_name": "canonical target name",
      "target_entity_id": "entity_id from known_entities when available, otherwise empty",
      "entity_type": "character|faction|organization|location|quest|event|timeline_node|term",
      "source_snippet_ids": ["exact snippet_id values"],
      "confidence": 0.0,
      "rationale": "brief evidence-backed explanation"
    }}
  ]
}}
"""


def build_candidate_alias_resolution_prompt(known_entities: list[dict[str, Any]], candidate_proposals: list[dict[str, Any]]) -> str:
    candidate_rows = [
        {
            "candidate_name": proposal.get("candidate_name", ""),
            "proposed_entity_type": normalize_entity_type(proposal.get("proposed_entity_type", "term")),
            "evidence_count": proposal.get("evidence_count", 0),
            "knowledge_tracks": proposal.get("knowledge_tracks", []),
            "candidate_topics": proposal.get("candidate_topics", []),
            "patch_item_types": proposal.get("patch_item_types", []),
            "patch_update_types": proposal.get("patch_update_types", []),
            "patch_relationship_types": proposal.get("patch_relationship_types", []),
            "source_snippet_ids": (proposal.get("source_snippet_ids", []) or [])[:12],
            "sample_texts": [str(text)[:500] for text in (proposal.get("sample_texts", []) or [])[:3]],
        }
        for proposal in candidate_proposals
    ]
    return f"""Resolve possible aliases among unresolved Theriac candidate anchors.
Return strict JSON only. Use the candidate evidence; do not guess from general knowledge.

Goal:
- Propose mappings where a candidate anchor is clearly the same Theriac in-world entity as a known entity.
- Accept spelling variants, punctuation variants, title/name variants, short forms, long forms, working names, prior/later names, codenames, and acronyms when supported by the candidate evidence.
- Prefer mapping a variant/working/older name to the latest or most canonical known entity.
- Do not map a candidate merely because it contains a known entity's name.
- Do not map subtopics, songs/themes, quests named after a character, project names, artifacts, body parts, relationships, inspirations, real people, external media, or phrase-like descriptions unless the evidence says that candidate is a name for the same Theriac entity.
- Omit uncertain cases. This is a review assistant, not an entity creator.

Known entities:
{json.dumps(known_entities, ensure_ascii=False, indent=2)}

Candidate anchors:
{json.dumps(candidate_rows, ensure_ascii=False, indent=2)}

Return JSON object:
{{
  "alias_mappings": [
    {{
      "alias_text": "candidate anchor that should become an alias",
      "canonical_name": "known canonical target",
      "target_entity_id": "entity_id from known_entities when available, otherwise empty",
      "entity_type": "character|faction|organization|location|quest|event|timeline_node|term",
      "source_snippet_ids": ["source ids from the candidate row, when useful"],
      "confidence": 0.0,
      "rationale": "brief evidence-backed explanation"
    }}
  ]
}}
"""


def add_alias_mappings_to_proposals(
    proposals_by_key: dict[str, dict[str, Any]],
    alias_mappings: list[Any],
    evidence_snippets: list[dict[str, Any]],
    known_entities: list[dict[str, Any]],
) -> None:
    snippets_by_id = {str(snip.get("snippet_id", "")): snip for snip in evidence_snippets}
    entities_by_id = {str(entity.get("entity_id", "")): entity for entity in known_entities}
    entities_by_name = {normalized_name_key(str(entity.get("canonical_name", ""))): entity for entity in known_entities}
    for mapping in alias_mappings:
        if not isinstance(mapping, dict):
            continue
        alias_text = clean_candidate_name(str(mapping.get("alias_text", "")))
        canonical_name = clean_candidate_name(str(mapping.get("canonical_name", "")))
        alias_key = normalized_name_key(alias_text)
        canonical_key = normalized_name_key(canonical_name)
        if not alias_key or not canonical_key or alias_key == canonical_key:
            continue
        source_ids = [str(sid).strip() for sid in mapping.get("source_snippet_ids", []) or [] if str(sid).strip()]
        source_snippets = [snippets_by_id[sid] for sid in source_ids if sid in snippets_by_id]
        if not source_snippets:
            continue
        target_entity = entities_by_id.get(str(mapping.get("target_entity_id", "")).strip()) or entities_by_name.get(canonical_key)
        if target_entity is not None:
            canonical_name = str(target_entity.get("canonical_name", canonical_name))
            entity_type = str(target_entity.get("entity_type", mapping.get("entity_type", "term")))
        else:
            canonical_name = display_name(canonical_name)
            entity_type = str(mapping.get("entity_type", "term"))
        entity_type = normalize_entity_type(entity_type)

        for snip in source_snippets:
            record_conversation_entity_proposal(proposals_by_key, alias_text, snip)
        proposal = proposals_by_key.get(alias_key)
        if not proposal:
            continue
        kinds = proposal.setdefault("proposal_kinds", [])
        for kind in ("alias_candidate", "llm_alias_resolution"):
            if kind not in kinds:
                kinds.append(kind)
        proposal["suggested_canonical_name"] = canonical_name
        if target_entity is not None:
            proposal["suggested_canonical_entity_id"] = target_entity.get("entity_id")
            proposal["suggested_canonical_card_id"] = target_entity.get("card_id")
        proposal["proposed_entity_type"] = entity_type
        proposal["proposal_reason"] = f"Model alias resolution suggests {display_name(alias_text)} -> {canonical_name}."
        proposal["alias_resolution_confidence"] = float(mapping.get("confidence", 0.0) or 0.0)
        alias_sources = proposal.setdefault("alias_evidence_source_snippet_ids", [])
        for sid in source_ids:
            if sid not in alias_sources:
                alias_sources.append(sid)
        notes = proposal.setdefault("alias_review_notes", [])
        rationale = str(mapping.get("rationale", "")).strip()
        if rationale and rationale not in notes and len(notes) < 5:
            notes.append(rationale[:500])


def add_candidate_alias_mappings_to_proposals(
    proposals_by_key: dict[str, dict[str, Any]],
    alias_mappings: list[Any],
    candidate_proposals: list[dict[str, Any]],
    known_entities: list[dict[str, Any]],
) -> None:
    candidate_keys = {
        normalized_name_key(str(proposal.get("candidate_name", ""))): proposal
        for proposal in candidate_proposals
        if normalized_name_key(str(proposal.get("candidate_name", "")))
    }
    entities_by_id = {str(entity.get("entity_id", "")): entity for entity in known_entities}
    entities_by_name = {normalized_name_key(str(entity.get("canonical_name", ""))): entity for entity in known_entities}
    for mapping in alias_mappings:
        if not isinstance(mapping, dict):
            continue
        alias_text = clean_candidate_name(str(mapping.get("alias_text", "")))
        canonical_name = clean_candidate_name(str(mapping.get("canonical_name", "")))
        alias_key = normalized_name_key(alias_text)
        canonical_key = normalized_name_key(canonical_name)
        if not alias_key or not canonical_key or alias_key == canonical_key:
            continue
        proposal = candidate_keys.get(alias_key) or proposals_by_key.get(alias_key)
        if not proposal:
            continue
        candidate_source_ids = {
            str(sid).strip()
            for sid in proposal.get("source_snippet_ids", []) or []
            if str(sid).strip()
        }
        mapping_source_ids = [
            str(sid).strip()
            for sid in mapping.get("source_snippet_ids", []) or []
            if str(sid).strip() and str(sid).strip() in candidate_source_ids
        ]
        if not mapping_source_ids and candidate_source_ids:
            mapping_source_ids = sorted(candidate_source_ids)[:5]

        target_entity = entities_by_id.get(str(mapping.get("target_entity_id", "")).strip()) or entities_by_name.get(canonical_key)
        if target_entity is not None:
            canonical_name = str(target_entity.get("canonical_name", canonical_name))
            entity_type = str(target_entity.get("entity_type", mapping.get("entity_type", "term")))
        else:
            canonical_name = display_name(canonical_name)
            entity_type = str(mapping.get("entity_type", proposal.get("proposed_entity_type", "term")))
        entity_type = normalize_entity_type(entity_type)

        kinds = proposal.setdefault("proposal_kinds", [])
        for kind in ("alias_candidate", "llm_alias_resolution", "llm_candidate_alias_resolution"):
            if kind not in kinds:
                kinds.append(kind)
        proposal["suggested_canonical_name"] = canonical_name
        if target_entity is not None:
            proposal["suggested_canonical_entity_id"] = target_entity.get("entity_id")
            proposal["suggested_canonical_card_id"] = target_entity.get("card_id")
        proposal["proposed_entity_type"] = entity_type
        proposal["proposal_reason"] = f"Model candidate alias resolution suggests {display_name(alias_text)} -> {canonical_name}."
        proposal["alias_resolution_confidence"] = float(mapping.get("confidence", 0.0) or 0.0)
        alias_sources = proposal.setdefault("alias_evidence_source_snippet_ids", [])
        for sid in mapping_source_ids:
            if sid not in alias_sources:
                alias_sources.append(sid)
        notes = proposal.setdefault("alias_review_notes", [])
        rationale = str(mapping.get("rationale", "")).strip()
        if rationale and rationale not in notes and len(notes) < 5:
            notes.append(rationale[:500])


def snippet_evidence_text(snip: dict[str, Any]) -> str:
    parts: list[str] = []
    for field in (
        "display_text_normalized",
        "patch_item_text",
        "conversation_patch_summary",
    ):
        text = str(snip.get(field, "") or "").strip()
        if text:
            parts.append(text)
    for field in (
        "conversation_patch_lore_developments",
        "conversation_patch_meta_developments",
        "conversation_patch_open_questions",
        "conversation_patch_possible_contradictions",
    ):
        raw = snip.get(field, [])
        if isinstance(raw, list):
            parts.extend(str(item).strip() for item in raw if str(item).strip())
    return "\n".join(dict.fromkeys(parts))


def conversation_entity_seed_records_from_memory(memory: dict[str, Any]) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for item in memory.get("approved_conversation_entities", []) or []:
        if not isinstance(item, dict):
            continue
        canonical_name = clean_candidate_name(str(item.get("canonical_name", "")))
        if not canonical_name:
            continue
        if is_disallowed_entity_type(item.get("entity_type", "")):
            continue
        aliases = []
        for alias in item.get("aliases", []) or []:
            alias_text = clean_candidate_name(str(alias))
            if alias_text and normalized_name_key(alias_text) != normalized_name_key(canonical_name):
                aliases.append(display_name(alias_text))
        candidate_name = clean_candidate_name(str(item.get("candidate_name", "")))
        if candidate_name and normalized_name_key(candidate_name) != normalized_name_key(canonical_name):
            aliases.append(display_name(candidate_name))
        entity_type = normalize_entity_type(item.get("entity_type", "term"))
        records.append(
            {
                "entity_seed_id": stable_id("conversation_entity_seed", normalized_name_key(canonical_name)),
                "canonical_name": display_name(canonical_name),
                "entity_type": entity_type,
                "aliases": sorted(set(aliases)),
                "seed_status": "active",
                "source_section_hints": [],
                "relationship_hints": [],
                "bootstrap_origin": "review_memory_conversation_entity",
                "confidence": {"score": 1.0, "reviewer_note": "Approved conversation entity from review memory."},
            }
        )
    return records


def conversation_entity_rejected_keys(memory: dict[str, Any]) -> set[str]:
    keys: set[str] = set()
    for item in memory.get("rejected_conversation_entities", []) or []:
        if not isinstance(item, dict):
            continue
        for field in ("candidate_name", "canonical_name"):
            key = normalized_name_key(str(item.get(field, "")))
            if key:
                keys.add(key)
    return keys


def _entity_for_candidate_key(candidate_key: str, name_targets: dict[str, dict[str, Any]]) -> dict[str, Any] | None:
    if not candidate_key:
        return None
    if candidate_key in name_targets:
        return name_targets[candidate_key]
    # Stage 04/Stage 06 sometimes carry a concise anchor such as "Path A" for a longer
    # seeded title like "Path A: Destructive Path". Allow only prefix matching,
    # which avoids promoting unrelated seeds from generic words inside names.
    if len(candidate_key.split()) >= 2:
        for target_key, entity in name_targets.items():
            if target_key.startswith(candidate_key + " "):
                return entity
    return None


def load_conversation_entity_decisions(path: Path | None) -> list[dict[str, Any]]:
    if path is None:
        return []
    if not path.exists():
        write_json(path, {"decisions": []})
        return []
    payload = read_json(path)
    decisions = payload.get("decisions", []) if isinstance(payload, dict) else []
    return [item for item in decisions if isinstance(item, dict)]


def load_existing_conversation_entity_proposals(path: Path | None) -> list[dict[str, Any]]:
    if path is None or not path.exists():
        return []
    try:
        payload = read_json(path)
    except Exception:
        return []
    proposals = payload.get("proposals", []) if isinstance(payload, dict) else []
    return [item for item in proposals if isinstance(item, dict)]


def prior_review_is_fully_decided(
    prior_proposals: list[dict[str, Any]],
    decisions: list[dict[str, Any]],
) -> bool:
    if not prior_proposals or not decisions:
        return False
    annotated = annotate_conversation_entity_proposals(prior_proposals, decisions)
    return all(str(proposal.get("review_status", "pending")).strip().lower() != "pending" for proposal in annotated)


PRIOR_ALIAS_RESOLUTION_FIELDS = {
    "suggested_canonical_name",
    "suggested_canonical_entity_id",
    "suggested_canonical_card_id",
    "alias_resolution_confidence",
    "alias_evidence_source_snippet_ids",
    "alias_review_notes",
}


def merge_prior_alias_resolution_annotations(
    proposals_by_key: dict[str, dict[str, Any]],
    prior_proposals: list[dict[str, Any]],
) -> None:
    if not prior_proposals:
        return
    prior_by_key: dict[str, dict[str, Any]] = {}
    for prior in prior_proposals:
        key = str(prior.get("normalized_name_key") or normalized_name_key(str(prior.get("candidate_name", "")))).strip()
        if key:
            prior_by_key[key] = prior

    for key, proposal in proposals_by_key.items():
        prior = prior_by_key.get(key)
        if not prior:
            continue
        for field in PRIOR_ALIAS_RESOLUTION_FIELDS:
            if field in prior and prior.get(field) not in (None, "", []):
                proposal[field] = prior[field]
        prior_kinds = [str(item) for item in prior.get("proposal_kinds", []) or [] if str(item).strip()]
        if prior_kinds:
            proposal["proposal_kinds"] = sorted(set((proposal.get("proposal_kinds", []) or []) + prior_kinds))
        prior_reason = str(prior.get("proposal_reason", "")).strip()
        if prior_reason and str(proposal.get("proposal_reason", "")).startswith("Text-observed"):
            proposal["proposal_reason"] = prior_reason


def is_viable_conversation_entity_candidate(candidate: str, snip: dict[str, Any]) -> bool:
    cleaned = clean_candidate_name(candidate)
    key = normalized_name_key(cleaned)
    if not key or key in CONVERSATION_ENTITY_NAME_STOPWORDS or is_blocked_seed_name(cleaned):
        return False
    if len(key) < 3 or key.isdigit():
        return False
    if len(key.split()) > 8:
        return False
    if not any(ch.isalpha() for ch in key):
        return False
    if str(snip.get("knowledge_track", "")).strip().lower() not in {"lore", "meta"}:
        return False
    return True


def infer_conversation_entity_type(candidate_name: str, snip: dict[str, Any]) -> str:
    votes = infer_type_evidence_for_candidate(candidate_name, snip)
    if votes:
        totals = type_vote_totals(votes)
        if totals:
            return sorted(totals.items(), key=lambda x: (-x[1], x[0]))[0][0]
    return "term"


def infer_type_evidence_for_candidate(candidate_name: str, snip: dict[str, Any]) -> list[dict[str, Any]]:
    votes: list[dict[str, Any]] = []
    topics = {str(topic).strip().lower() for topic in snip.get("candidate_topics", []) or []}
    key = normalized_name_key(candidate_name)
    if "quest" in topics or key.startswith("path "):
        votes.append(type_vote("quest", 2.0, "candidate_topic:quest_or_path_name", snip))
    if "event" in topics:
        votes.append(type_vote("event", 1.8, "candidate_topic:event", snip))
    if "theme" in topics:
        votes.append(type_vote("term", 0.8, "candidate_topic:theme_concept_not_entity", snip))
    if "mechanic" in topics:
        votes.append(type_vote("term", 1.2, "candidate_topic:mechanic", snip))
    if key in {"lab", "the lab"} or any(part in key.split() for part in {"facility", "clinic", "road", "city", "base"}):
        votes.append(type_vote("location", 2.0, "candidate_name:location_word", snip))

    # Music / song-title quest detection
    votes.extend(music_context_quest_votes(candidate_name, snip))

    votes.extend(contextual_type_votes(candidate_name, snip))
    return votes


def music_context_quest_votes(candidate_name: str, snip: dict[str, Any]) -> list[dict[str, Any]]:
    """Detect song-title candidates in quest-related contexts and vote quest.

    Theriac quest titles are named after songs. When a candidate appears in
    evidence alongside music markers (song, track, album, band) AND quest-
    context markers (quest, path, route, ending, mission), it gets a quest
    vote.  Artist/band names co-occurring with the candidate are recorded
    so the post-triage band-grouping step can promote sibling song titles.
    """
    evidence_text = snippet_evidence_text(snip).lower()
    if not evidence_text:
        return []
    votes: list[dict[str, Any]] = []
    has_music = any(re.search(rf"\b{re.escape(m)}\b", evidence_text) for m in MUSIC_EVIDENCE_MARKERS)
    has_quest_ctx = any(re.search(rf"\b{re.escape(m)}\b", evidence_text) for m in MUSIC_QUEST_CONTEXT_MARKERS)

    # Also check thematic tags on the snippet itself
    thematic_tags = set(snip.get("thematic_tags", []) or [])
    if any(t.startswith("music:") for t in thematic_tags):
        has_music = True
    if "possible_song_title_reference" in thematic_tags or "possible_artist_reference" in thematic_tags:
        has_music = True

    # Check patch-note fields for quest context
    for field in ("patch_item_type", "patch_update_type", "patch_relationship_type"):
        value = str(snip.get(field, "")).strip().lower()
        if value in {"quest", "quest_update", "path", "route", "ending", "mission"}:
            has_quest_ctx = True

    if has_music and has_quest_ctx:
        votes.append(type_vote("quest", 2.5, "context:music_reference_in_quest_context", snip))
    elif has_music:
        # Music reference without explicit quest context - give a moderate
        # quest signal since Theriac names quests after songs.
        votes.append(type_vote("quest", 1.2, "context:music_reference_possible_quest_name", snip))

    return votes


def type_vote(entity_type: str, weight: float, basis: str, snip: dict[str, Any]) -> dict[str, Any]:
    return {
        "entity_type": entity_type,
        "weight": round(float(weight), 3),
        "basis": basis,
        "snippet_id": str(snip.get("snippet_id", "")),
    }


def contextual_type_votes(candidate_name: str, snip: dict[str, Any]) -> list[dict[str, Any]]:
    text = snippet_evidence_text(snip)
    candidate_key = normalized_name_key(candidate_name)
    if not text or not candidate_key:
        return []
    normalized_text = normalized_name_key(text)
    pattern = r"(?<![a-z0-9])" + r"\s+".join(re_escape(part) for part in candidate_key.split()) + r"(?![a-z0-9])"
    votes: list[dict[str, Any]] = []
    import re

    location_markers = {
        "abandoned",
        "array",
        "base",
        "building",
        "built",
        "construction",
        "facility",
        "fence",
        "lab",
        "laboratory",
        "location",
        "perimeter",
        "place",
        "road",
        "site",
    }
    faction_markers = {
        "agency",
        "army",
        "faction",
        "forces",
        "group",
        "organization",
        "republic",
        "soldiers",
    }

    for match in re.finditer(pattern, normalized_text):
        following = normalized_text[match.end() : match.end() + 180]
        surrounding = normalized_text[max(0, match.start() - 120) : match.end() + 180]
        if any(re.search(rf"\b{re.escape(marker)}\b", surrounding) for marker in location_markers):
            votes.append(type_vote("location", 2.0, "context:location_marker_near_name", snip))
        if any(re.search(rf"\b{re.escape(marker)}\b", surrounding) for marker in faction_markers):
            votes.append(type_vote("faction", 1.8, "context:faction_marker_near_name", snip))
        if re.search(r"\b(who|whom|whose)\b", following[:90]):
            votes.append(type_vote("character", 3.0, "context:relative_pronoun_after_name", snip))
        if re.search(r"\b(he|him|his|she|her|hers)\b", following):
            votes.append(type_vote("character", 2.4, "context:gendered_pronoun_after_name", snip))
        if re.search(r"\b(they|them|their|theirs)\b", following):
            votes.append(type_vote("character", 1.5, "context:animate_pronoun_after_name", snip))
    return dedupe_type_votes(votes)


def dedupe_type_votes(votes: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str]] = set()
    for vote in votes:
        key = (str(vote.get("snippet_id", "")), str(vote.get("entity_type", "")), str(vote.get("basis", "")))
        if key in seen:
            continue
        seen.add(key)
        out.append(vote)
    return out


def type_vote_totals(votes: list[dict[str, Any]]) -> dict[str, float]:
    totals: dict[str, float] = {}
    for vote in votes:
        entity_type = normalize_entity_type(vote.get("entity_type", "term"), default="")
        if not entity_type:
            continue
        try:
            weight = float(vote.get("weight", 0.0))
        except (TypeError, ValueError):
            weight = 0.0
        totals[entity_type] = round(totals.get(entity_type, 0.0) + max(0.0, weight), 3)
    return totals


def select_reconsidered_entity_type(initial_type: str, votes: list[dict[str, Any]]) -> str:
    totals = type_vote_totals(votes)
    if not totals:
        return normalize_entity_type(initial_type)
    sorted_totals = sorted(totals.items(), key=lambda x: (-x[1], x[0]))
    top_type, top_score = sorted_totals[0]
    normalized_initial_type = normalize_entity_type(initial_type)
    initial_score = totals.get(normalized_initial_type, 0.0)
    if top_type != normalized_initial_type and top_score >= initial_score + TYPE_RECONSIDERATION_MARGIN:
        return top_type
    return normalized_initial_type if normalized_initial_type in ENTITY_TYPES else top_type


def refresh_type_review_fields(proposal: dict[str, Any]) -> None:
    votes = [vote for vote in proposal.get("type_evidence", []) if isinstance(vote, dict)]
    totals = type_vote_totals(votes)
    initial_type = normalize_entity_type(proposal.get("initial_proposed_entity_type") or proposal.get("proposed_entity_type") or "term")
    proposed_type = select_reconsidered_entity_type(initial_type, votes)
    proposal["type_vote_totals"] = totals
    proposal["proposed_entity_type"] = proposed_type
    proposal["type_reconsidered"] = proposed_type != initial_type
    proposal["type_conflicts"] = [
        {"entity_type": entity_type, "score": score}
        for entity_type, score in sorted(totals.items(), key=lambda x: (-x[1], x[0]))
        if entity_type != proposed_type
    ]
    if proposal["type_reconsidered"]:
        proposal["type_review_notes"] = (
            f"Entity type reconsidered from {initial_type} to {proposed_type} after aggregated usage evidence."
        )
    elif proposal["type_conflicts"]:
        proposal["type_review_notes"] = "Multiple entity-type signals present; reviewer should confirm the designation."
    else:
        proposal["type_review_notes"] = ""


def record_conversation_entity_proposal(
    proposals_by_key: dict[str, dict[str, Any]],
    candidate: str,
    snip: dict[str, Any],
) -> None:
    cleaned = clean_candidate_name(candidate)
    key = normalized_name_key(cleaned)
    if not key:
        return
    snippet_id = str(snip.get("snippet_id", ""))
    timestamp_start = str(snip.get("timestamp_start_utc", ""))
    timestamp_end = str(snip.get("timestamp_end_utc", timestamp_start))
    if key not in proposals_by_key:
        initial_type = infer_conversation_entity_type(cleaned, snip)
        proposals_by_key[key] = {
            "proposal_id": stable_id("conversation_entity_proposal", key),
            "candidate_name": display_name(cleaned),
            "normalized_name_key": key,
            "initial_proposed_entity_type": initial_type,
            "proposed_entity_type": initial_type,
            "type_evidence": [],
            "type_vote_totals": {},
            "type_conflicts": [],
            "type_reconsidered": False,
            "type_review_notes": "",
            "knowledge_tracks": [],
            "knowledge_track_counts": {},
            "candidate_topics": [],
            "source_kinds": [],
            "source_kind_counts": {},
            "patch_item_types": [],
            "patch_item_type_counts": {},
            "patch_update_types": [],
            "patch_update_type_counts": {},
            "patch_relationship_types": [],
            "patch_relationship_type_counts": {},
            "source_snippet_ids": [],
            "first_seen_timestamp_utc": timestamp_start,
            "last_seen_timestamp_utc": timestamp_end,
            "evidence_count": 0,
            "sample_texts": [],
            "confidence": 0.0,
            "review_status": "pending",
            "proposal_reason": "Text-observed conversation anchor did not resolve to any bootstrap seed or approved alias.",
        }
    proposal = proposals_by_key[key]
    source_is_new = False
    proposal["last_seen_timestamp_utc"] = max(str(proposal.get("last_seen_timestamp_utc", "")), timestamp_end)
    if timestamp_start and (not proposal.get("first_seen_timestamp_utc") or timestamp_start < str(proposal["first_seen_timestamp_utc"])):
        proposal["first_seen_timestamp_utc"] = timestamp_start
    if snippet_id and snippet_id not in proposal["source_snippet_ids"]:
        proposal["source_snippet_ids"].append(snippet_id)
        proposal["evidence_count"] = len(proposal["source_snippet_ids"])
        source_is_new = True
    track = str(snip.get("knowledge_track", "")).strip()
    if track and track not in proposal["knowledge_tracks"]:
        proposal["knowledge_tracks"].append(track)
    if source_is_new:
        increment_proposal_count(proposal, "knowledge_track_counts", track)
    for topic in snip.get("candidate_topics", []) or []:
        topic_text = str(topic).strip()
        if topic_text and topic_text not in proposal["candidate_topics"]:
            proposal["candidate_topics"].append(topic_text)
    for target_field, snippet_field in (
        ("source_kinds", "source_kind"),
        ("patch_item_types", "patch_item_type"),
        ("patch_update_types", "patch_update_type"),
        ("patch_relationship_types", "patch_relationship_type"),
    ):
        value = str(snip.get(snippet_field, "")).strip()
        if value and value not in proposal[target_field]:
            proposal[target_field].append(value)
        if source_is_new:
            increment_proposal_count(proposal, f"{target_field[:-1]}_counts", value)
    existing_type_vote_keys = {
        (str(vote.get("snippet_id", "")), str(vote.get("entity_type", "")), str(vote.get("basis", "")))
        for vote in proposal.get("type_evidence", [])
        if isinstance(vote, dict)
    }
    for vote in infer_type_evidence_for_candidate(cleaned, snip):
        vote_key = (str(vote.get("snippet_id", "")), str(vote.get("entity_type", "")), str(vote.get("basis", "")))
        if vote_key not in existing_type_vote_keys:
            proposal.setdefault("type_evidence", []).append(vote)
            existing_type_vote_keys.add(vote_key)
    refresh_type_review_fields(proposal)
    sample_text = snippet_evidence_text(snip).strip()
    if sample_text and sample_text not in proposal["sample_texts"] and len(proposal["sample_texts"]) < 5:
        proposal["sample_texts"].append(sample_text[:500])
    try:
        score = float(snip.get("relevance_score", 0.0))
    except (TypeError, ValueError):
        score = 0.0
    current_count = max(1, int(proposal["evidence_count"]))
    current_confidence = float(proposal.get("confidence", 0.0))
    proposal["confidence"] = round(max(current_confidence, min(1.0, score + min(0.2, 0.03 * current_count))), 3)


def increment_proposal_count(proposal: dict[str, Any], field: str, value: str) -> None:
    clean_value = str(value or "").strip()
    if not clean_value:
        return
    counts = proposal.setdefault(field, {})
    if not isinstance(counts, dict):
        counts = {}
        proposal[field] = counts
    counts[clean_value] = int(counts.get(clean_value, 0) or 0) + 1


def annotate_conversation_entity_proposals(
    proposals: list[dict[str, Any]],
    decisions: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    latest_by_id: dict[str, dict[str, Any]] = {}
    latest_by_key: dict[str, dict[str, Any]] = {}

    def priority(decision: dict[str, Any]) -> int:
        reviewer = str(decision.get("reviewer", "")).strip().lower()
        if bool(decision.get("human_override")):
            return 2
        if reviewer and "auto_review" not in reviewer and "gemini_auto" not in reviewer:
            return 2
        return 1

    def keep_decision(existing: dict[str, Any] | None, candidate: dict[str, Any]) -> dict[str, Any]:
        if existing is None:
            return candidate
        return candidate if priority(candidate) >= priority(existing) else existing

    for decision in decisions:
        proposal_id = str(decision.get("proposal_id", "")).strip()
        key = normalized_name_key(str(decision.get("candidate_name") or decision.get("canonical_name") or ""))
        if proposal_id:
            latest_by_id[proposal_id] = keep_decision(latest_by_id.get(proposal_id), decision)
        if key:
            latest_by_key[key] = keep_decision(latest_by_key.get(key), decision)

    annotated: list[dict[str, Any]] = []
    for proposal in proposals:
        decision = latest_by_id.get(str(proposal.get("proposal_id", ""))) or latest_by_key.get(str(proposal.get("normalized_name_key", "")))
        if not decision:
            annotated.append({**proposal, "review_status": "pending"})
            continue
        raw_decision = str(decision.get("decision", "")).strip().lower()
        status = {
            "accept": "approved",
            "approve": "approved",
            "approved": "approved",
            "reject": "rejected",
            "rejected": "rejected",
            "defer": "deferred",
            "needs_more_context": "needs_more_context",
        }.get(raw_decision, "pending")
        annotated.append({**proposal, "review_status": status, "latest_decision": decision})
    return annotated


def approved_conversation_entity_from_proposal(
    proposal: dict[str, Any],
    decision: dict[str, Any],
    resolved_entity_by_name: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    canonical_name = clean_candidate_name(
        str(decision.get("canonical_name") or proposal.get("suggested_canonical_name") or proposal.get("candidate_name", ""))
    )
    canonical_name = display_name(canonical_name)
    canonical_key = normalized_name_key(canonical_name)
    existing = resolved_entity_by_name.get(canonical_key)
    if existing is not None:
        aliases = {str(alias) for alias in existing.get("aliases", []) or [] if str(alias).strip()}
        for alias in decision.get("aliases", []) or []:
            alias_text = clean_candidate_name(str(alias))
            if alias_text and normalized_name_key(alias_text) != canonical_key:
                aliases.add(display_name(alias_text))
        candidate_name = clean_candidate_name(str(proposal.get("candidate_name", "")))
        if candidate_name and normalized_name_key(candidate_name) != canonical_key:
            aliases.add(display_name(candidate_name))
        return {**existing, "aliases": sorted(aliases), "observation_status": "conversation_candidate_approved"}

    aliases = []
    for alias in decision.get("aliases", []) or []:
        alias_text = clean_candidate_name(str(alias))
        if alias_text and normalized_name_key(alias_text) != canonical_key:
            aliases.append(display_name(alias_text))
    candidate_name = clean_candidate_name(str(proposal.get("candidate_name", "")))
    if candidate_name and normalized_name_key(candidate_name) != canonical_key:
        aliases.append(display_name(candidate_name))
    entity_type = normalize_entity_type(decision.get("entity_type") or proposal.get("proposed_entity_type") or "term")
    return {
        "entity_id": entity_id(canonical_name),
        "card_id": card_id_for_entity(canonical_name),
        "canonical_name": canonical_name,
        "entity_type": entity_type,
        "aliases": sorted(set(aliases)),
        "seed_entity_ids": [],
        "relationship_hints": [],
        "resolution_status": "conversation_candidate_approved",
        "observation_status": "conversation_observed",
        "source": "conversation_entity_proposal",
        "source_proposal_id": proposal.get("proposal_id", ""),
        "source_snippet_ids": proposal.get("source_snippet_ids", []),
    }


def re_contains_name(lower_text: str, name_key: str) -> bool:
    compact_name = name_key.lower()
    if not compact_name:
        return False
    pattern = r"(?<![a-z0-9])" + r"\s+".join(re_escape(part) for part in compact_name.split()) + r"(?![a-z0-9])"
    import re

    return re.search(pattern, lower_text) is not None


def re_escape(value: str) -> str:
    import re

    return re.escape(value)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--in-snippets-jsonl", type=Path, required=True)
    parser.add_argument("--in-entity-seed-json", "--in-seed-json", dest="in_seed_json", type=Path, required=True)
    parser.add_argument("--out-alias-json", type=Path, required=True)
    parser.add_argument("--out-timeline-json", type=Path, required=True)
    parser.add_argument("--out-resolved-entities-json", type=Path, required=False, default=None)
    parser.add_argument("--in-review-memory-json", type=Path, required=False, default=None)
    parser.add_argument("--out-conversation-entity-proposals-json", type=Path, required=False, default=None)
    parser.add_argument("--in-conversation-entity-decisions-json", type=Path, required=False, default=None)
    parser.add_argument("--in-pipeline-config-json", type=Path, required=False, default=None)
    args = parser.parse_args()
    run(
        args.in_snippets_jsonl,
        args.in_seed_json,
        args.out_alias_json,
        args.out_timeline_json,
        args.out_resolved_entities_json,
        args.in_review_memory_json,
        args.out_conversation_entity_proposals_json,
        args.in_conversation_entity_decisions_json,
        args.in_pipeline_config_json,
    )


if __name__ == "__main__":
    main()
