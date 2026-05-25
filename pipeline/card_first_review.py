"""Card-first synthesis: snippet bundles + selective claim auto-accept."""

from __future__ import annotations

from typing import Any

from pipeline.common import now_utc_iso, read_json, safe_uuid
from pipeline.entity_resolution import normalize_entity_type, normalized_name_key
from pipeline.review_memory import normalize_claim_text, rejected_claim_keys

_META_SNIPPET_MARKERS = (
    "radiohead",
    "playlist",
    "song title",
    "spotify",
    "soundtrack",
    "working title",
    "canonical name",
)

# Lore that is true but usually not lede-worthy on a *character* card (project paperwork, org bureaucracy).
_CHARACTER_INCIDENTAL_SNIPPET_MARKERS = (
    "embezzled",
    "early specifications",
    "early specs",
    "approved the",
    "gives approval",
    "approval for",
    "specifications for ruinr",
    "built with embezzled",
    "secretive organization within",
    "special projects division",
    "repurposed the mainframe",
    "weapons systems",
)

# Snippet themes that should rank higher when pooling evidence for major character cards.
_CHARACTER_CORE_SNIPPET_MARKERS = (
    "thanatophobia",
    "depression",
    "chess",
    "cyberpsych",
    "moratorium",
    "khava",
    "barely human",
    "destructive path",
    "peaceful path",
    "fulfillment",
    "father-daughter",
    "mentor",
    "principal investigator",
    "survival instinct",
    "cybernetic",
    "olympus",
)

DEFAULT_CARD_FIRST_SYNTHESIS: dict[str, Any] = {
    "enabled": True,
    "auto_accept_claims": True,
    "min_confidence": 0.0,
    "max_snippets_per_entity": 24,
    "section_chained_synthesis": True,
    "protagonist_tier": {
        "enabled": True,
        "min_approved_snippets": 80,
        "entity_types": ["character"],
        "canonical_names": [],
        "tier1_snippet_pool": 120,
        "digest_snippet_cap": 45,
        "snippets_per_section": 22,
    },
    "require_source_snippets_for_auto_accept": True,
    "auto_accept_claim_types": [
        "background",
        "role",
        "relationship",
        "timeline",
        "lore_fact",
        "other",
    ],
    "never_auto_accept_claim_types": [
        "alias",
        "open_question",
        "inspiration",
        "theme",
        "meta_note",
    ],
    "never_auto_accept_knowledge_tracks": ["meta"],
}


def card_first_synthesis_config(config: dict[str, Any] | None) -> dict[str, Any]:
    raw = config.get("card_first_synthesis", {}) if isinstance(config, dict) else {}
    merged = {**DEFAULT_CARD_FIRST_SYNTHESIS, **(raw if isinstance(raw, dict) else {})}
    merged["enabled"] = bool(merged.get("enabled", True))
    merged["auto_accept_claims"] = bool(merged.get("auto_accept_claims", True))
    merged["min_confidence"] = float(merged.get("min_confidence", 0.0) or 0.0)
    merged["max_snippets_per_entity"] = max(1, int(merged.get("max_snippets_per_entity", 24) or 24))
    merged["section_chained_synthesis"] = bool(merged.get("section_chained_synthesis", True))
    protagonist_raw = merged.get("protagonist_tier", {})
    protagonist_default = DEFAULT_CARD_FIRST_SYNTHESIS["protagonist_tier"]
    protagonist = {**protagonist_default, **(protagonist_raw if isinstance(protagonist_raw, dict) else {})}
    protagonist["enabled"] = bool(protagonist.get("enabled", True))
    protagonist["min_approved_snippets"] = max(1, int(protagonist.get("min_approved_snippets", 80) or 80))
    protagonist["tier1_snippet_pool"] = max(24, int(protagonist.get("tier1_snippet_pool", 120) or 120))
    protagonist["digest_snippet_cap"] = max(12, int(protagonist.get("digest_snippet_cap", 45) or 45))
    protagonist["snippets_per_section"] = max(8, int(protagonist.get("snippets_per_section", 22) or 22))
    entity_types = protagonist.get("entity_types", protagonist_default["entity_types"])
    protagonist["entity_types"] = [
        normalize_entity_type(item, "character") for item in (entity_types if isinstance(entity_types, list) else [])
    ] or ["character"]
    canonical_names = protagonist.get("canonical_names", [])
    protagonist["canonical_names"] = [
        str(name).strip() for name in (canonical_names if isinstance(canonical_names, list) else []) if str(name).strip()
    ]
    merged["protagonist_tier"] = protagonist
    merged["require_source_snippets_for_auto_accept"] = bool(merged.get("require_source_snippets_for_auto_accept", True))
    return merged


def protagonist_tier_config(cfg: dict[str, Any]) -> dict[str, Any]:
    raw = cfg.get("protagonist_tier", {})
    return raw if isinstance(raw, dict) else DEFAULT_CARD_FIRST_SYNTHESIS["protagonist_tier"]


def is_protagonist_tier_entity(
    entity: dict[str, Any],
    approved_snippet_count: int,
    cfg: dict[str, Any],
) -> bool:
    tier_cfg = protagonist_tier_config(cfg)
    if not tier_cfg.get("enabled", True):
        return False
    canonical = str(entity.get("canonical_name", "")).strip()
    forced_names = {normalized_name_key(name) for name in tier_cfg.get("canonical_names", []) or []}
    if canonical and normalized_name_key(canonical) in forced_names:
        return True
    entity_type = normalize_entity_type(entity.get("entity_type", "term"))
    allowed_types = {normalize_entity_type(item, "character") for item in tier_cfg.get("entity_types", []) or []}
    if entity_type not in allowed_types:
        return False
    return approved_snippet_count >= int(tier_cfg.get("min_approved_snippets", 80) or 80)


def synthesis_tier_label(
    entity: dict[str, Any],
    approved_snippet_count: int,
    cfg: dict[str, Any],
) -> str:
    if is_protagonist_tier_entity(entity, approved_snippet_count, cfg):
        return "protagonist"
    if approved_snippet_count >= 12:
        return "developed"
    if approved_snippet_count >= 4:
        return "moderate"
    return "sparse"


def snippet_priority_key(snippet: dict[str, Any], entity: dict[str, Any] | None = None) -> tuple[Any, ...]:
    patch_item = str(snippet.get("patch_item_type", "")).strip().lower()
    patch_update = str(snippet.get("patch_update_type", "")).strip().lower()
    source_kind = str(snippet.get("source_kind", "")).strip().lower()
    text = str(snippet.get("display_text_normalized", "")).strip().lower()
    summary_text = str(snippet.get("conversation_patch_summary", "")).strip().lower()
    combined = f"{text} {summary_text}"
    lore_devs = snippet.get("conversation_patch_lore_developments", []) or []

    tier = 2
    if patch_item in {"role_change", "introduced", "relationship", "location", "quest", "event"}:
        tier = 0
    elif patch_update or "patch" in source_kind:
        tier = 0
    elif lore_devs:
        tier = 1

    entity_type = normalize_entity_type(entity.get("entity_type", "term")) if isinstance(entity, dict) else ""
    if entity_type == "character" and any(marker in combined for marker in _CHARACTER_CORE_SNIPPET_MARKERS):
        tier = max(0, tier - 1)

    meta_penalty = 1 if any(marker in text for marker in _META_SNIPPET_MARKERS) else 0
    short_naming_penalty = 1 if len(text) < 90 and any(word in text for word in ("alias", "renamed", "working name", "formerly")) else 0
    incidental_penalty = 0
    if entity_type == "character" and any(marker in combined for marker in _CHARACTER_INCIDENTAL_SNIPPET_MARKERS):
        incidental_penalty = 1
    conversation_index = snippet.get("conversation_global_index")
    if conversation_index is None:
        conversation_index = 10**9
    timestamp = str(snippet.get("timestamp_start_utc", ""))
    snippet_id = str(snippet.get("snippet_id", ""))
    return (tier, meta_penalty, incidental_penalty, short_naming_penalty, conversation_index, timestamp, snippet_id)


def rank_snippet_ids(
    snippet_ids: list[str],
    source_snippets_by_id: dict[str, dict[str, Any]],
    entity: dict[str, Any] | None = None,
) -> list[str]:
    ranked = sorted(
        [snippet_id for snippet_id in snippet_ids if snippet_id in source_snippets_by_id],
        key=lambda snippet_id: snippet_priority_key(source_snippets_by_id[snippet_id], entity),
    )
    for snippet_id in snippet_ids:
        if snippet_id not in ranked:
            ranked.append(snippet_id)
    return ranked


def tier1_snippet_pool_for_entity(
    entity: dict[str, Any],
    lore_clusters: list[dict[str, Any]],
    source_snippets_by_id: dict[str, dict[str, Any]],
    cfg: dict[str, Any],
    *,
    extra_snippet_ids: list[str] | None = None,
) -> list[str]:
    ordered = snippet_ids_for_entity(
        entity,
        lore_clusters,
        max_snippets=10_000,
        extra_snippet_ids=extra_snippet_ids,
    )
    if is_protagonist_tier_entity(entity, len(ordered), cfg):
        pool_cap = int(protagonist_tier_config(cfg).get("tier1_snippet_pool", 120) or 120)
    else:
        pool_cap = max(int(cfg.get("max_snippets_per_entity", 24) or 24), 24)
    ranked = rank_snippet_ids(ordered, source_snippets_by_id, entity)
    return ranked[:pool_cap]


def should_use_section_chained_synthesis(
    entity: dict[str, Any],
    approved_snippet_count: int,
    cfg: dict[str, Any],
) -> bool:
    if not cfg.get("enabled", True) or not cfg.get("section_chained_synthesis", True):
        return False
    return is_protagonist_tier_entity(entity, approved_snippet_count, cfg)


def protagonist_word_target_plan(approved_snippet_count: int, claim_count: int) -> dict[str, Any]:
    return {
        "synthesis_tier": "protagonist",
        "accepted_claim_count": claim_count,
        "approved_snippet_count": approved_snippet_count,
        "total_word_target": {"min": 1200, "max": 1900},
        "recommended_sections": [
            "summary",
            "background",
            "role_in_story",
            "relationships",
            "history_theriac_coda",
            "history_path_a_side_route",
            "inspirations",
        ],
        "section_word_targets": {
            "summary": "70-100 words: peaceful Path B main-route lede only (no Path A plot)",
            "background": "400-600 words if supported by evidence",
            "role_in_story": "80-140 words: pre-branch lab role and ~1-hour branch choice only",
            "relationships": "200-350 words if supported by evidence",
            "history_theriac_coda": "280-480 words: main/peaceful route (side with lab, ~40+ hours)",
            "history_path_a_side_route": "100-200 words: Path A side route (~6 hours) if evidenced; optional",
            "inspirations": "80-160 words for meta/design inspiration only",
            "open_questions": "empty unless evidence explicitly states uncertainty",
        },
        "scaling_rule": (
            "Protagonist-tier main wiki page (Fandom-style): peaceful-path summary lede, chronological background, "
            "pre-branch role, history_theriac_coda (Path B main route) and optional history_path_a_side_route (Path A), "
            "relationships with wiki links, and isolated meta inspirations. Cite snippet_* and claim IDs per section. "
            "Foreground character-defining material (personality, suffering, relationships, path splits, augmentation, death); "
            "mention incidental administrative beats at most once in background, never in the summary, never repeated across sections. "
            "Do not mix Path A and Path B events in the same section."
        ),
    }


def is_snippet_id(token: str) -> bool:
    value = str(token or "").strip()
    return value.startswith("snippet_")


def load_snippet_clusters(path: Any) -> list[dict[str, Any]]:
    if not path or not getattr(path, "exists", lambda: False)():
        return []
    payload = read_json(path)
    clusters = payload.get("clusters", []) if isinstance(payload, dict) else []
    return [row for row in clusters if isinstance(row, dict)]


def _cluster_keys_for_entity(entity: dict[str, Any]) -> set[str]:
    keys: set[str] = set()
    canonical = str(entity.get("canonical_name", "")).strip()
    if canonical:
        keys.add(normalized_name_key(canonical))
    for alias in entity.get("aliases", []) or []:
        alias_text = str(alias).strip()
        if alias_text:
            keys.add(normalized_name_key(alias_text))
    return keys


def snippet_ids_for_entity(
    entity: dict[str, Any],
    lore_clusters: list[dict[str, Any]],
    *,
    max_snippets: int = 24,
    extra_snippet_ids: list[str] | None = None,
) -> list[str]:
    keys = _cluster_keys_for_entity(entity)
    ordered: list[str] = []
    seen: set[str] = set()
    for cluster in lore_clusters:
        cluster_key = normalized_name_key(str(cluster.get("cluster_key", "")))
        if cluster_key not in keys:
            continue
        for raw_id in cluster.get("snippet_ids", []) or []:
            snippet_id = str(raw_id).strip()
            if snippet_id and snippet_id not in seen:
                seen.add(snippet_id)
                ordered.append(snippet_id)
    for raw_id in extra_snippet_ids or []:
        snippet_id = str(raw_id).strip()
        if snippet_id and snippet_id not in seen:
            seen.add(snippet_id)
            ordered.append(snippet_id)
    return ordered[:max_snippets]


def _latest_decision_by_claim(decisions: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    latest: dict[str, dict[str, Any]] = {}
    for decision in decisions:
        if not isinstance(decision, dict):
            continue
        claim_id = str(decision.get("claim_id", "")).strip()
        if not claim_id:
            continue
        timestamp = str(decision.get("timestamp_utc", ""))
        previous = latest.get(claim_id)
        if not previous or timestamp >= str(previous.get("timestamp_utc", "")):
            latest[claim_id] = decision
    return latest


def should_auto_accept_claim(
    claim: dict[str, Any],
    rejected_keys: set[tuple[str, str]],
    cfg: dict[str, Any],
) -> tuple[bool, str]:
    cfg = {**DEFAULT_CARD_FIRST_SYNTHESIS, **(cfg if isinstance(cfg, dict) else {})}
    claim_id = str(claim.get("claim_id", "")).strip()
    if not claim_id:
        return False, "missing_claim_id"
    entity_id = str(claim.get("target_entity_id", "")).strip()
    claim_text = str(claim.get("claim_text", "")).strip()
    if not claim_text:
        return False, "empty_claim_text"
    normalized = normalize_claim_text(claim_text)
    if (entity_id, normalized) in rejected_keys:
        return False, "rejected_memory"
    if bool(claim.get("manual_claim") or claim.get("author_claim")):
        return False, "author_or_manual_claim"
    track = str(claim.get("knowledge_track", "lore")).strip().lower() or "lore"
    if track in {str(item).strip().lower() for item in cfg.get("never_auto_accept_knowledge_tracks", []) or []}:
        return False, f"knowledge_track:{track}"
    claim_type = str(claim.get("claim_type", "other")).strip().lower() or "other"
    never_types = {str(item).strip().lower() for item in cfg.get("never_auto_accept_claim_types", []) or []}
    if claim_type in never_types:
        return False, f"claim_type:{claim_type}"
    allowed_types = {str(item).strip().lower() for item in cfg.get("auto_accept_claim_types", []) or []}
    if allowed_types and claim_type not in allowed_types:
        return False, f"claim_type_not_allowed:{claim_type}"
    if cfg.get("require_source_snippets_for_auto_accept") and not _claim_source_ids(claim):
        return False, "missing_source_snippets"
    try:
        confidence = float(claim.get("confidence", 0.5) or 0.5)
    except (TypeError, ValueError):
        confidence = 0.5
    if confidence < float(cfg.get("min_confidence", 0.0) or 0.0):
        return False, f"low_confidence:{confidence:.2f}"
    return True, "low_risk_lore_claim"


def _claim_source_ids(claim: dict[str, Any]) -> list[str]:
    ids: list[str] = []
    for key in ("source_snippet_ids", "snippet_ids"):
        values = claim.get(key, [])
        if isinstance(values, list):
            ids.extend(str(value).strip() for value in values if str(value).strip())
    return ids


def supplement_claim_decisions(
    claims: list[dict[str, Any]],
    decisions: list[dict[str, Any]],
    memory: dict[str, Any],
    config: dict[str, Any] | None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    cfg = card_first_synthesis_config(config)
    report: dict[str, Any] = {
        "generated_at_utc": now_utc_iso(),
        "card_first_enabled": cfg["enabled"],
        "auto_accept_claims": cfg["auto_accept_claims"],
        "auto_accepted": [],
        "skipped": [],
    }
    if not cfg["enabled"] or not cfg["auto_accept_claims"]:
        return list(decisions), report

    latest = _latest_decision_by_claim(decisions)
    supplemental: list[dict[str, Any]] = []
    for claim in claims:
        claim_id = str(claim.get("claim_id", "")).strip()
        if not claim_id or claim_id in latest:
            continue
        entity_id = str(claim.get("target_entity_id", "")).strip()
        rejected_keys = rejected_claim_keys(memory, entity_id)
        ok, reason = should_auto_accept_claim(claim, rejected_keys, cfg)
        if not ok:
            report["skipped"].append({"claim_id": claim_id, "reason": reason})
            continue
        entry = {
            "decision_id": safe_uuid(),
            "claim_id": claim_id,
            "decision": "accept",
            "reviewer": "card_first_auto_accept",
            "rationale": f"Auto-accepted low-risk lore claim ({reason}).",
            "timestamp_utc": now_utc_iso(),
            "auto_accepted": True,
        }
        supplemental.append(entry)
        report["auto_accepted"].append(
            {
                "claim_id": claim_id,
                "target_entity_id": entity_id,
                "target_entity_name": claim.get("target_entity_name", ""),
                "claim_type": claim.get("claim_type", ""),
                "reason": reason,
            }
        )
    return list(decisions) + supplemental, report


def merge_synthesis_evidence_rows(
    claim_rows: list[dict[str, Any]],
    source_snippets_by_id: dict[str, dict[str, Any]],
    approved_snippet_ids: list[str],
    *,
    entity: dict[str, Any] | None = None,
    canonical_name: str = "",
    alias_terms: list[str] | None = None,
    global_alias_pairs: list[tuple[str, str]] | None = None,
    max_rows: int = 24,
    max_text_chars: int = 900,
) -> list[dict[str, Any]]:
    from pipeline.stage_11_card_synthesis import (
        MAX_SYNTHESIS_SOURCE_SNIPPETS,
        _normalize_synthesis_snippet_text,
        _truncate_source_text,
    )

    limit = max_rows or MAX_SYNTHESIS_SOURCE_SNIPPETS
    seen = {str(row.get("snippet_id", "")).strip() for row in claim_rows if str(row.get("snippet_id", "")).strip()}
    merged = list(claim_rows)
    for snippet_id in approved_snippet_ids:
        if len(merged) >= limit:
            break
        if snippet_id in seen:
            continue
        snippet = source_snippets_by_id.get(snippet_id)
        if not isinstance(snippet, dict):
            continue
        seen.add(snippet_id)
        merged.append(
            {
                "snippet_id": snippet_id,
                "supporting_claim_ids": [],
                "conversation_id": snippet.get("conversation_id", ""),
                "conversation_global_index": snippet.get("conversation_global_index"),
                "conversation_topic_label": snippet.get("conversation_topic_label", ""),
                "conversation_topic_summary": snippet.get("conversation_topic_summary", ""),
                "timestamp_start_utc": snippet.get("timestamp_start_utc", ""),
                "source_kind": snippet.get("source_kind", ""),
                "patch_item_type": snippet.get("patch_item_type", ""),
                "patch_update_type": snippet.get("patch_update_type", ""),
                "patch_relationship_type": snippet.get("patch_relationship_type", ""),
                "conversation_patch_summary": _normalize_synthesis_snippet_text(
                    str(snippet.get("conversation_patch_summary", "")),
                    entity,
                    canonical_name,
                    alias_terms,
                    global_alias_pairs,
                ),
                "conversation_patch_lore_developments": snippet.get("conversation_patch_lore_developments", []),
                "conversation_patch_meta_developments": snippet.get("conversation_patch_meta_developments", []),
                "conversation_patch_possible_contradictions": snippet.get("conversation_patch_possible_contradictions", []),
                "text": _truncate_source_text(
                    _normalize_synthesis_snippet_text(
                        str(snippet.get("display_text_normalized", "")),
                        entity,
                        canonical_name,
                        alias_terms,
                        global_alias_pairs,
                    ),
                    max_text_chars,
                ),
                "evidence_scope": "approved_entity_snippet_bundle",
            }
        )
    merged.sort(
        key=lambda row: (
            row.get("conversation_global_index") is None,
            row.get("conversation_global_index") if row.get("conversation_global_index") is not None else 0,
            str(row.get("timestamp_start_utc", "")),
            str(row.get("snippet_id", "")),
        )
    )
    return merged[:limit]


def collect_claim_snippet_ids(claims: list[dict[str, Any]]) -> list[str]:
    seen: set[str] = set()
    ordered: list[str] = []
    for claim in claims:
        for snippet_id in _claim_source_ids(claim):
            if snippet_id not in seen:
                seen.add(snippet_id)
                ordered.append(snippet_id)
    return ordered


def build_entity_evidence_bundle(
    entity: dict[str, Any],
    claims: list[dict[str, Any]],
    lore_clusters: list[dict[str, Any]],
    config: dict[str, Any] | None,
    *,
    source_snippets_by_id: dict[str, dict[str, Any]] | None = None,
) -> dict[str, Any]:
    cfg = card_first_synthesis_config(config)
    claim_snippet_ids = collect_claim_snippet_ids(claims)
    snippets_by_id = source_snippets_by_id or {}
    if snippets_by_id:
        approved_snippet_ids = tier1_snippet_pool_for_entity(
            entity,
            lore_clusters,
            snippets_by_id,
            cfg,
            extra_snippet_ids=claim_snippet_ids,
        )
    else:
        pool_cap = int(cfg["max_snippets_per_entity"])
        if is_protagonist_tier_entity(
            entity,
            len(
                snippet_ids_for_entity(
                    entity,
                    lore_clusters,
                    max_snippets=10_000,
                    extra_snippet_ids=claim_snippet_ids,
                )
            ),
            cfg,
        ):
            pool_cap = int(protagonist_tier_config(cfg).get("tier1_snippet_pool", 120) or 120)
        approved_snippet_ids = snippet_ids_for_entity(
            entity,
            lore_clusters,
            max_snippets=pool_cap,
            extra_snippet_ids=claim_snippet_ids,
        )
    tier_label = synthesis_tier_label(entity, len(approved_snippet_ids), cfg)
    review_model = (
        "Card-first: draft from approved lore snippets and entity development history. "
        "Accepted claims are guardrails (conflicts, author corrections, high-risk facts), not a sentence-by-sentence author checklist."
    )
    if should_use_section_chained_synthesis(entity, len(approved_snippet_ids), cfg):
        review_model += " Section-chained synthesis: lore digest, per-section writers, merge pass."
    return {
        "card_first_synthesis": cfg["enabled"],
        "synthesis_tier": tier_label,
        "section_chained_synthesis": should_use_section_chained_synthesis(entity, len(approved_snippet_ids), cfg),
        "entity_id": str(entity.get("entity_id", "")),
        "canonical_name": str(entity.get("canonical_name", "")),
        "approved_claim_count": len(claims),
        "approved_snippet_count": len(approved_snippet_ids),
        "approved_claim_ids": [str(claim.get("claim_id")) for claim in claims if str(claim.get("claim_id", "")).strip()],
        "approved_snippet_ids": approved_snippet_ids,
        "review_model": review_model,
    }


def entities_for_card_synthesis(
    merged_entities: list[dict[str, Any]],
    accepted_by_entity: dict[str, list[dict[str, Any]]],
    lore_clusters: list[dict[str, Any]],
    config: dict[str, Any] | None,
) -> dict[str, list[dict[str, Any]]]:
    cfg = card_first_synthesis_config(config)
    if not cfg["enabled"]:
        return accepted_by_entity
    out = dict(accepted_by_entity)
    for entity in merged_entities:
        entity_id = str(entity.get("entity_id", "")).strip()
        if not entity_id or entity_id in out:
            continue
        snippet_ids = snippet_ids_for_entity(
            entity,
            lore_clusters,
            max_snippets=int(cfg["max_snippets_per_entity"]),
        )
        if snippet_ids:
            out[entity_id] = []
    return out


def section_word_targets_for_entity(
    claims: list[dict[str, Any]],
    approved_snippet_count: int,
    config: dict[str, Any] | None,
    *,
    entity: dict[str, Any] | None = None,
) -> dict[str, Any]:
    cfg = card_first_synthesis_config(config)
    if not cfg["enabled"]:
        return section_word_targets_for_claims_only(claims)

    if entity and is_protagonist_tier_entity(entity, approved_snippet_count, cfg):
        plan = protagonist_word_target_plan(approved_snippet_count, len(claims))
        if cfg.get("section_chained_synthesis", True):
            plan["section_chained_synthesis"] = True
        return plan

    if not claims and approved_snippet_count > 0:
        if approved_snippet_count <= 3:
            total_min, total_max = 120, 280
        elif approved_snippet_count <= 8:
            total_min, total_max = 200, 450
        elif approved_snippet_count <= 40:
            total_min, total_max = 300, 700
        else:
            total_min, total_max = 500, 950
        return {
            "synthesis_tier": synthesis_tier_label(entity or {}, approved_snippet_count, cfg),
            "accepted_claim_count": 0,
            "approved_snippet_count": approved_snippet_count,
            "total_word_target": {"min": total_min, "max": total_max},
            "recommended_sections": ["summary", "background", "role_in_story"],
            "section_word_targets": {
                "summary": "50-95 words",
                "background": "70-160 words if supported by snippets",
                "role_in_story": "60-150 words if supported by snippets",
                "relationships": "50-130 words if supported by snippets",
                "timeline": "40-110 words if supported by snippets",
                "inspirations": "empty unless meta snippets clearly support it",
                "open_questions": "empty unless snippets explicitly state uncertainty",
            },
            "scaling_rule": "Snippet-backed card: use approved lore snippets as primary evidence; cite snippet IDs in support_map.",
        }

    plan = section_word_targets_for_claims_only(claims)
    if approved_snippet_count >= 6:
        total = plan.setdefault("total_word_target", {"min": 0, "max": 0})
        total["max"] = int(total.get("max", 0)) + min(250, approved_snippet_count * 12)
        total["min"] = int(total.get("min", 0)) + min(80, approved_snippet_count * 4)
    if entity:
        plan["synthesis_tier"] = synthesis_tier_label(entity, approved_snippet_count, cfg)
    plan["approved_snippet_count"] = approved_snippet_count
    return plan


def section_word_targets_for_claims_only(claims: list[dict[str, Any]]) -> dict[str, Any]:
    from pipeline.stage_11_card_synthesis import section_word_targets_for_claims

    return section_word_targets_for_claims(claims)


def valid_support_id_sets(
    claims: list[dict[str, Any]],
    evidence_bundle: dict[str, Any],
) -> tuple[set[str], set[str]]:
    claim_ids = {str(claim.get("claim_id")) for claim in claims if str(claim.get("claim_id", "")).strip()}
    snippet_ids = {
        str(snippet_id).strip()
        for snippet_id in evidence_bundle.get("approved_snippet_ids", []) or []
        if is_snippet_id(str(snippet_id))
    }
    return claim_ids, snippet_ids


def resolve_support_token(raw_id: Any, valid_claim_ids: set[str], valid_snippet_ids: set[str]) -> str | None:
    from pipeline.stage_11_card_synthesis import resolve_support_claim_id

    token = str(raw_id or "").strip()
    if not token:
        return None
    if token in valid_snippet_ids:
        return token
    if is_snippet_id(token) and token in valid_snippet_ids:
        return token
    return resolve_support_claim_id(token, valid_claim_ids)


def resolve_support_id_list(
    support_ids: list[Any],
    valid_claim_ids: set[str],
    valid_snippet_ids: set[str],
) -> tuple[list[str], list[str]]:
    resolved: list[str] = []
    invalid: list[str] = []
    for item in support_ids:
        match = resolve_support_token(item, valid_claim_ids, valid_snippet_ids)
        if match:
            if match not in resolved:
                resolved.append(match)
        else:
            invalid.append(str(item))
    return resolved, invalid


def normalize_synthesis_support_ids(
    synthesis: dict[str, Any],
    valid_claim_ids: set[str],
    valid_snippet_ids: set[str],
) -> None:
    from pipeline.stage_11_card_synthesis import _resolve_support_claim_id_list

    support_map = synthesis.get("support_map")
    if isinstance(support_map, dict):
        for field_name, support_ids in support_map.items():
            if not isinstance(support_ids, list):
                continue
            resolved, _invalid = resolve_support_id_list(support_ids, valid_claim_ids, valid_snippet_ids)
            support_map[field_name] = resolved

    for rel in synthesis.get("relationships", []) or []:
        if isinstance(rel, dict) and isinstance(rel.get("support_claim_ids"), list):
            resolved, _ = resolve_support_id_list(rel["support_claim_ids"], valid_claim_ids, valid_snippet_ids)
            rel["support_claim_ids"] = resolved
    for item in synthesis.get("timeline", []) or []:
        if isinstance(item, dict) and isinstance(item.get("support_claim_ids"), list):
            resolved, _ = resolve_support_id_list(item["support_claim_ids"], valid_claim_ids, valid_snippet_ids)
            item["support_claim_ids"] = resolved
    for item in synthesis.get("wiki_links", []) or []:
        if isinstance(item, dict) and isinstance(item.get("support_claim_ids"), list):
            resolved, _ = resolve_support_id_list(item["support_claim_ids"], valid_claim_ids, valid_snippet_ids)
            item["support_claim_ids"] = resolved
    for field_name in ["resolved_conflicts", "unresolved_conflicts"]:
        for item in synthesis.get(field_name, []) or []:
            if isinstance(item, dict) and isinstance(item.get("claim_ids"), list):
                resolved, _ = resolve_support_id_list(item["claim_ids"], valid_claim_ids, valid_snippet_ids)
                item["claim_ids"] = resolved
