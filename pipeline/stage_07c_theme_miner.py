from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Any

from pipeline.common import get_logger, now_utc_iso, read_json, stable_id, write_json
from pipeline.entity_resolution import normalized_name_key
from pipeline.model_provider import call_model_chat, model_call_kwargs
from pipeline.theme_evidence import adjudication_supports_theme_evidence


THEME_PROFILE_SCHEMA_VERSION = 1
THEME_UPDATE_REPORT_SCHEMA_VERSION = 1
TASK_NAME = "stage_07c_theme_miner"
THEME_STATUSES = {"candidate", "active", "deprecated", "meta_only", "rejected"}
THEME_ACTIONS = {"create_theme", "update_theme", "deprecate_theme", "mark_meta_only", "reject_theme", "no_change"}
THEME_DOMAINS = {
    "mythological_theological",
    "technological_scientific",
    "emotional_psychological",
    "philosophical_ideological",
    "aesthetic",
    "historical_political",
    "other",
}
DEFAULT_MAX_EVIDENCE_PACKETS = 80
DEFAULT_MAX_SEED_THEME_LABEL_PACKETS = 32
DEFAULT_MAX_REVIEW_MEMORY_THEME_PACKETS = 48
DEFAULT_MAX_ADJUDICATION_THEME_PACKETS = 48

THEME_UPDATE_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "theme_updates": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "action": {"type": "string"},
                    "theme_id": {"type": "string"},
                    "label": {"type": "string"},
                    "theme_domain": {"type": "string"},
                    "theme_type": {"type": "string"},
                    "status": {"type": "string"},
                    "confidence": {"type": "number"},
                    "canon_relevance": {"type": "string"},
                    "description": {"type": "string"},
                    "evidence_entities": {"type": "array", "items": {"type": "string"}},
                    "evidence_claim_ids": {"type": "array", "items": {"type": "string"}},
                    "evidence_snippet_ids": {"type": "array", "items": {"type": "string"}},
                    "positive_indicators": {"type": "array", "items": {"type": "string"}},
                    "negative_indicators": {"type": "array", "items": {"type": "string"}},
                    "related_themes": {"type": "array", "items": {"type": "string"}},
                    "disambiguation_notes": {"type": "array", "items": {"type": "string"}},
                    "pattern_notes": {"type": "array", "items": {"type": "string"}},
                    "provenance_summary": {"type": "string"},
                },
                "required": [
                    "action",
                    "theme_id",
                    "label",
                    "theme_domain",
                    "theme_type",
                    "status",
                    "confidence",
                    "canon_relevance",
                    "description",
                    "evidence_entities",
                    "evidence_claim_ids",
                    "evidence_snippet_ids",
                    "positive_indicators",
                    "negative_indicators",
                    "related_themes",
                    "disambiguation_notes",
                    "pattern_notes",
                    "provenance_summary",
                ],
                "additionalProperties": True,
            },
        }
    },
    "required": ["theme_updates"],
    "additionalProperties": False,
}


def run(
    in_entity_candidate_harvest_json: Path,
    in_entity_adjudication_recommendations_json: Path,
    in_resolved_entities_json: Path,
    in_review_memory_json: Path,
    inout_theme_profile_json: Path,
    out_theme_profile_update_report_json: Path,
    in_pipeline_config_json: Path | None = None,
) -> None:
    logger = get_logger(__name__)
    harvest = read_json(in_entity_candidate_harvest_json) if in_entity_candidate_harvest_json.exists() else {}
    adjudication = read_json(in_entity_adjudication_recommendations_json) if in_entity_adjudication_recommendations_json.exists() else {}
    resolved_entities = read_json(in_resolved_entities_json) if in_resolved_entities_json.exists() else {}
    review_memory = read_json(in_review_memory_json) if in_review_memory_json.exists() else {}
    provider_config = read_json(in_pipeline_config_json) if in_pipeline_config_json and in_pipeline_config_json.exists() else {}
    profile = load_theme_profile(inout_theme_profile_json)
    task_cfg = stage_task_config(provider_config)
    evidence_packets = collect_theme_evidence(harvest, adjudication, resolved_entities, review_memory, task_cfg)
    applied_updates: list[dict[str, Any]] = []
    raw_updates: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []

    if evidence_packets and bool(task_cfg.get("enabled", True)):
        kwargs = theme_model_kwargs(provider_config, task_cfg)
        prompt = build_theme_miner_prompt(profile, evidence_packets)
        logger.info(
            "Stage 07C: mining theme profile updates from %d evidence packet(s) with model=%s.",
            len(evidence_packets),
            kwargs.get("api_model", ""),
        )
        response = call_model_chat(prompt=prompt, **kwargs)
        raw_updates = normalize_theme_update_response(response)
        if not raw_updates:
            failures.append(
                {
                    "reason": "invalid_or_empty_theme_miner_response",
                    "response_keys": sorted(response.keys()) if isinstance(response, dict) else [],
                }
            )
        else:
            profile, applied_updates = apply_theme_updates(profile, raw_updates)
    else:
        logger.info(
            "Stage 07C: no theme-miner model call needed (evidence_packets=%d enabled=%s).",
            len(evidence_packets),
            bool(task_cfg.get("enabled", True)),
        )

    profile["updated_at_utc"] = now_utc_iso()
    profile.setdefault("policy", default_theme_policy())
    profile.setdefault("themes", [])
    write_json(inout_theme_profile_json, profile)
    report = {
        "schema_version": THEME_UPDATE_REPORT_SCHEMA_VERSION,
        "generated_at_utc": now_utc_iso(),
        "stage": "07C_theme_miner",
        "inputs": {
            "entity_candidate_harvest_json": str(in_entity_candidate_harvest_json),
            "entity_adjudication_recommendations_json": str(in_entity_adjudication_recommendations_json),
            "resolved_entities_json": str(in_resolved_entities_json),
            "review_memory_json": str(in_review_memory_json),
            "theme_profile_json": str(inout_theme_profile_json),
            "evidence_packet_count": len(evidence_packets),
        },
        "policy": default_theme_policy(),
        "summary": {
            "theme_count": len(profile.get("themes", [])),
            "raw_update_count": len(raw_updates),
            "applied_update_count": len(applied_updates),
            "failure_count": len(failures),
        },
        "evidence_packets": evidence_packets,
        "raw_theme_updates": raw_updates,
        "applied_theme_updates": applied_updates,
        "failures": failures,
    }
    write_json(out_theme_profile_update_report_json, report)
    logger.info(
        "Stage 07C complete: themes=%d evidence_packets=%d applied_updates=%d failures=%d",
        len(profile.get("themes", [])),
        len(evidence_packets),
        len(applied_updates),
        len(failures),
    )


def load_theme_profile(path: Path) -> dict[str, Any]:
    if path.exists():
        try:
            payload = read_json(path)
            if isinstance(payload, dict):
                payload.setdefault("schema_version", THEME_PROFILE_SCHEMA_VERSION)
                payload.setdefault("themes", [])
                payload.setdefault("policy", default_theme_policy())
                return payload
        except Exception:
            pass
    return {
        "schema_version": THEME_PROFILE_SCHEMA_VERSION,
        "updated_at_utc": None,
        "policy": default_theme_policy(),
        "themes": [],
        "theme_update_log": [],
    }


def default_theme_policy() -> dict[str, Any]:
    return {
        "canon_gate": "human_review",
        "theme_learning_scope": "relevance_prior_only",
        "transitive_thematic_learning_not_transitive_canon": True,
        "theme_match_is_not_promotion_rule": True,
        "theme_provenance_required": True,
        "theme_domains_are_separate_lanes": True,
    }


def stage_task_config(provider_config: dict[str, Any]) -> dict[str, Any]:
    routing = provider_config.get("model_routing", {}) if isinstance(provider_config, dict) else {}
    tasks = routing.get("tasks", {}) if isinstance(routing, dict) else {}
    task_cfg = tasks.get(TASK_NAME, {}) if isinstance(tasks, dict) and isinstance(tasks.get(TASK_NAME, {}), dict) else {}
    return dict(task_cfg)


def theme_model_kwargs(provider_config: dict[str, Any], task_cfg: dict[str, Any]) -> dict[str, Any]:
    kwargs = model_call_kwargs(provider_config, TASK_NAME)
    if not task_cfg or "provider" not in task_cfg:
        kwargs["provider"] = "openrouter"
    if not task_cfg or "api_base_url" not in task_cfg:
        kwargs["api_base_url"] = "https://openrouter.ai/api/v1"
    if not task_cfg or "api_model" not in task_cfg:
        kwargs["api_model"] = "deepseek/deepseek-v4-flash"
    kwargs["timeout_seconds"] = max(int(kwargs.get("timeout_seconds", 60)), 180)
    kwargs["max_tokens"] = max(int(kwargs.get("max_tokens", 4096)), 3500)
    kwargs["json_schema"] = THEME_UPDATE_SCHEMA
    if "rate_state_path" not in task_cfg:
        kwargs["rate_state_path"] = Path("artifacts/learning/openrouter_deepseek_stage_07c_theme_miner_rate_runtime.json")
    return kwargs


def collect_theme_evidence(
    harvest: dict[str, Any],
    adjudication: dict[str, Any],
    resolved_entities: dict[str, Any],
    review_memory: dict[str, Any],
    task_cfg: dict[str, Any],
) -> list[dict[str, Any]]:
    limit = max(1, int(task_cfg.get("max_evidence_packets", DEFAULT_MAX_EVIDENCE_PACKETS) or DEFAULT_MAX_EVIDENCE_PACKETS))
    seed_limit = max(
        0,
        int(
            task_cfg.get(
                "max_seed_theme_label_packets",
                task_cfg.get("max_seed_theme_entity_packets", DEFAULT_MAX_SEED_THEME_LABEL_PACKETS),
            )
            or DEFAULT_MAX_SEED_THEME_LABEL_PACKETS
        ),
    )
    review_limit = max(
        0,
        int(task_cfg.get("max_review_memory_theme_packets", DEFAULT_MAX_REVIEW_MEMORY_THEME_PACKETS) or DEFAULT_MAX_REVIEW_MEMORY_THEME_PACKETS),
    )
    adjudication_limit = max(
        0,
        int(task_cfg.get("max_adjudication_theme_packets", DEFAULT_MAX_ADJUDICATION_THEME_PACKETS) or DEFAULT_MAX_ADJUDICATION_THEME_PACKETS),
    )
    candidate_by_key = {
        normalized_name_key(str(item.get("normalized_name_key") or item.get("candidate_name") or "")): item
        for item in harvest.get("candidates", []) or []
        if isinstance(item, dict)
    }
    packets: list[dict[str, Any]] = []
    seed_packets = review_memory_theme_label_packets(review_memory, limit=seed_limit)
    seed_theme_keys = theme_seed_keys(seed_packets)
    packets.extend(seed_packets)
    packets.extend(review_memory_theme_packets(review_memory, limit=review_limit, seed_theme_keys=seed_theme_keys))
    packets.extend(adjudication_theme_packets(adjudication, candidate_by_key, limit=adjudication_limit))

    return dedupe_theme_evidence_packets(packets, limit=limit)


def dedupe_theme_evidence_packets(packets: list[dict[str, Any]], limit: int) -> list[dict[str, Any]]:
    deduped: list[dict[str, Any]] = []
    seen: set[str] = set()
    for packet in packets:
        key = stable_id(
            "theme_evidence",
            str(packet.get("source", "")),
            str(packet.get("entity_name", "")),
            str(packet.get("text", ""))[:500],
        )
        if key in seen:
            continue
        seen.add(key)
        deduped.append(packet)
        if len(deduped) >= limit:
            break
    return deduped


def adjudication_theme_packets(adjudication: dict[str, Any], candidate_by_key: dict[str, dict[str, Any]], limit: int) -> list[dict[str, Any]]:
    packets: list[dict[str, Any]] = []
    if limit <= 0:
        return packets
    for rec in adjudication.get("recommendations", []) or []:
        if not isinstance(rec, dict) or not adjudication_supports_theme_evidence(rec):
            continue
        key = normalized_name_key(str(rec.get("normalized_key") or rec.get("candidate_name") or ""))
        candidate = candidate_by_key.get(key, {})
        packets.append(
            {
                "source": "stage_07b_entity_adjudication",
                "evidence_status": "plausible_local_candidate",
                "entity_name": rec.get("candidate_name", ""),
                "entity_type": rec.get("recommended_entity_type") or candidate.get("proposed_entity_type", ""),
                "externality_class": rec.get("externality_class", ""),
                "recommended_action": rec.get("recommended_action", ""),
                "recommended_track": rec.get("recommended_track", ""),
                "source_snippet_ids": rec.get("source_snippet_ids", []) or candidate.get("source_snippet_ids", []),
                "claim_ids": [],
                "text": compact_join(
                    [
                        rec.get("reasoning_summary", ""),
                        " ".join(rec.get("in_world_signals", []) or []),
                        " ".join(f.get("finding", "") for f in rec.get("web_findings", []) or [] if isinstance(f, dict)),
                        " ".join(str(t) for t in candidate.get("sample_texts", [])[:3]),
                    ],
                    1800,
                ),
                "theme_matches": rec.get("theme_matches", []),
            }
        )
        if len(packets) >= limit:
            break
    return packets


def review_memory_theme_label_packets(review_memory: dict[str, Any], limit: int) -> list[dict[str, Any]]:
    packets: list[dict[str, Any]] = []
    if limit <= 0:
        return packets
    if not isinstance(review_memory, dict):
        return packets
    rows: list[tuple[dict[str, Any], str]] = []
    approved_theme_labels = review_memory.get("approved_theme_labels", [])
    if isinstance(approved_theme_labels, list):
        rows.extend((row, "review_memory.approved_theme_labels") for row in approved_theme_labels if isinstance(row, dict))
    legacy_entities = review_memory.get("approved_conversation_entities", [])
    if isinstance(legacy_entities, list):
        rows.extend(
            (row, "review_memory.approved_conversation_entities_legacy_theme")
            for row in legacy_entities
            if isinstance(row, dict) and str(row.get("entity_type", "")).strip() == "theme"
        )
    for row, source in rows:
        if not isinstance(row, dict):
            continue
        canonical_name = clean_text(row.get("canonical_name") or row.get("candidate_name") or "", 160)
        if not canonical_name:
            continue
        aliases = normalize_string_list(row.get("aliases", []), 20, 120)
        rationale = clean_text(row.get("rationale") or row.get("review_reason") or "", 800)
        packets.append(
            {
                "source": source,
                "evidence_status": "approved_theme_label_not_entity",
                "entity_name": canonical_name,
                "entity_type": "theme_label",
                "aliases": aliases,
                "externality_class": "",
                "recommended_action": "",
                "recommended_track": "theme_profile",
                "source_snippet_ids": row.get("source_snippet_ids", []) or row.get("supporting_snippet_ids", []),
                "claim_ids": [],
                "text": compact_join([canonical_name, " ".join(aliases), rationale], 1800),
                "theme_matches": [],
            }
        )
        if len(packets) >= limit:
            break
    return packets


def review_memory_theme_packets(review_memory: dict[str, Any], limit: int, seed_theme_keys: list[str] | None = None) -> list[dict[str, Any]]:
    priority_groups: dict[str, list[dict[str, Any]]] = {}
    fallback_packets: list[dict[str, Any]] = []
    if limit <= 0:
        return []
    seed_theme_keys = seed_theme_keys or []
    source_lists = (
        ("accepted_claims", "accepted_claim"),
        ("approved_theme_labels", "approved_theme_label"),
        ("approved_conversation_entities", "approved_entity"),
        ("author_claims", "author_claim"),
        ("story_question_answers", "author_answer"),
        ("story_answer_applications", "author_answer_application"),
    )
    for field, status in source_lists:
        rows = review_memory.get(field, []) if isinstance(review_memory, dict) else []
        if not isinstance(rows, list):
            continue
        for row in rows:
            if not isinstance(row, dict):
                continue
            if (
                field != "approved_theme_labels"
                and str(row.get("claim_type", "")).strip() != "theme"
                and str(row.get("entity_type", "")).strip() != "theme"
            ):
                continue
            text = compact_join(
                [
                    row.get("claim_text", ""),
                    row.get("answer_text", ""),
                    row.get("application_summary", ""),
                    row.get("rationale", ""),
                    row.get("canonical_name", ""),
                    row.get("candidate_name", ""),
                    " ".join(row.get("thematic_tags", []) or []),
                ],
                1800,
            )
            if not text:
                continue
            entity_name = row.get("target_entity_name") or row.get("canonical_name") or row.get("candidate_name") or ""
            packet = {
                "source": f"review_memory.{field}",
                "evidence_status": status,
                "entity_name": entity_name,
                "entity_type": row.get("entity_type", ""),
                "externality_class": "",
                "recommended_action": "",
                "recommended_track": "lore_candidate",
                "source_snippet_ids": row.get("source_snippet_ids", []) or row.get("supporting_snippet_ids", []),
                "claim_ids": [row.get("claim_id", "")] if row.get("claim_id") else [],
                "text": text,
                "theme_matches": [],
            }
            hit_key = review_memory_packet_seed_hit_key(packet, seed_theme_keys)
            if hit_key:
                priority_groups.setdefault(hit_key, []).append(packet)
            else:
                fallback_packets.append(packet)
    return (round_robin_seed_packets(priority_groups, seed_theme_keys, limit) + fallback_packets)[:limit]


def theme_seed_keys(seed_packets: list[dict[str, Any]]) -> list[str]:
    keys: list[str] = []
    for packet in seed_packets:
        for value in [packet.get("entity_name", ""), *(packet.get("aliases", []) or [])]:
            key = normalized_name_key(str(value or ""))
            if key and key not in keys:
                keys.append(key)
    return keys


def review_memory_packet_seed_hit_key(packet: dict[str, Any], seed_theme_keys: list[str]) -> str:
    if not seed_theme_keys:
        return ""
    entity_key = normalized_name_key(str(packet.get("entity_name", "")))
    if entity_key in seed_theme_keys:
        return entity_key
    text_key = normalized_name_key(str(packet.get("text", "")))
    padded_text = f" {text_key} "
    for key in seed_theme_keys:
        if key and f" {key} " in padded_text:
            return key
    return ""


def round_robin_seed_packets(priority_groups: dict[str, list[dict[str, Any]]], seed_theme_keys: list[str], limit: int) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    positions = {key: 0 for key in seed_theme_keys}
    while len(out) < limit:
        added = False
        for key in seed_theme_keys:
            group = priority_groups.get(key, [])
            index = positions.get(key, 0)
            if index >= len(group):
                continue
            out.append(group[index])
            positions[key] = index + 1
            added = True
            if len(out) >= limit:
                break
        if not added:
            break
    return out


def build_theme_miner_prompt(profile: dict[str, Any], evidence_packets: list[dict[str, Any]]) -> str:
    return f"""You are Stage 07C, the Theriac Theme Miner.
Update the persistent theme profile from accepted or plausible local evidence.

Core rules:
- Learn transitive thematic patterns, not transitive canon.
- A theme match can raise future relevance priors, but never promotes a candidate into canon.
- Every theme must have provenance in accepted entities, accepted claims, author answers, review memory, or plausible local adjudication evidence.
- Historical/mythological/theological origin alone is not enough; require local Theriac usage or explicit author/review evidence.
- Mark purely external inspiration lanes as meta_only when local evidence does not support in-world use.
- Keep updates compact and reviewable.
- Preserve successful theme lanes. Do not merge mythological/theological, technological/scientific, emotional/psychological, philosophical/ideological, aesthetic, or historical/political patterns unless the evidence explicitly connects them.
- Infer the best theme_domain from the evidence. The domain is a broad lane for later review, not a fixed ontology of allowed themes.
- WIKI THEME RULE: Themes should describe recurring in-fiction patterns useful for a lore wiki and theme rescue (faction motifs, research programs, device classes, institutions). Do NOT create catch-all aesthetic/meta themes that merely restate art direction or aggregate faction motifs; mark those meta_only or deprecate them.
- TECHNOLOGY THEME RULE: When evidence describes recurring in-world science/technology (research programs, device classes, augmentation, AI systems, preservation medicine, etc.), infer technological_scientific themes from the evidence. Name themes from patterns you observe — do not copy a fixed list from these instructions.
- FACTION MOTIF RULE: Mythological/historical borrowings used as faction signatures (Spartan secret police, Enoch Watchers, Majapahit names) stay in mythological_theological or historical_political lanes, separate from technology lanes, unless evidence explicitly ties them to a tech program.
- DISAMBIGUATION RULE: Distinguish between abstract themes and entities named after concepts. If a narrative features characters named after abstract nouns, do not extract their literal names as themes. Extract the underlying thematic patterns they represent.
- QUEST TITLE RULE: Theriac quest titles are named after real-world song titles (e.g. "Sweet Child O' Mine", "Paradise City", "The Day The World Went Away"). When evidence references a song title, do not extract the song as a theme. Instead, classify the referenced quest as an in-world entity (type: quest) or leave it as an entity candidate. The song name is a quest title, not a thematic pattern.
- CHARACTER NAMED AFTER CONCEPT RULE: Some characters in Theriac are named after abstract nouns (e.g., Altruism, Joy) or mythological figures (e.g., Izanami, Enoch). When such names appear in evidence, look for the underlying thematic pattern they represent, not the literal name. Do not create themes called "Altruism" or "Joy" just because a character has that name; create themes for altruism-as-concept if the evidence explores the concept of selflessness.

Allowed theme_domain values:
{json_dumps(sorted(THEME_DOMAINS))}

Existing theme profile:
{json_dumps(profile_summary(profile))}

Evidence packets:
{json_dumps(evidence_packets)}

Return strict JSON:
{{
  "theme_updates": [
    {{
      "action": "create_theme | update_theme | deprecate_theme | mark_meta_only | reject_theme | no_change",
      "theme_id": "theme_short_slug",
      "label": "Human label",
      "theme_domain": "mythological_theological | technological_scientific | emotional_psychological | philosophical_ideological | aesthetic | historical_political | other",
      "theme_type": "mythological | historical | religious | aesthetic | scientific_technological | emotional_psychological | philosophical | other",
      "status": "candidate | active | deprecated | meta_only | rejected",
      "confidence": 0.0,
      "canon_relevance": "lore_pattern | meta_only | rejected | unknown",
      "description": "What pattern this theme captures.",
      "evidence_entities": [],
      "evidence_claim_ids": [],
      "evidence_snippet_ids": [],
      "positive_indicators": [],
      "negative_indicators": [],
      "related_themes": [],
      "disambiguation_notes": ["Theme match changes the prior, not canon status."],
      "pattern_notes": [],
      "provenance_summary": "Why this update is justified."
    }}
  ]
}}
"""


def profile_summary(profile: dict[str, Any]) -> dict[str, Any]:
    return {
        "themes": [
            {
                "theme_id": theme.get("theme_id", ""),
                "label": theme.get("label", ""),
                "theme_domain": theme.get("theme_domain", ""),
                "theme_type": theme.get("theme_type", ""),
                "status": theme.get("status", ""),
                "confidence": theme.get("confidence", 0.0),
                "positive_indicators": theme.get("positive_indicators", [])[:8],
                "evidence_entities": theme.get("evidence_entities", [])[:8],
            }
            for theme in profile.get("themes", [])[:40]
            if isinstance(theme, dict)
        ]
    }


def normalize_theme_update_response(response: Any) -> list[dict[str, Any]]:
    if isinstance(response, dict) and isinstance(response.get("theme_updates"), list):
        return [item for item in response["theme_updates"] if isinstance(item, dict)]
    if isinstance(response, dict) and isinstance(response.get("_json_root"), list):
        return [item for item in response["_json_root"] if isinstance(item, dict)]
    return []


def apply_theme_updates(profile: dict[str, Any], updates: list[dict[str, Any]]) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    themes = [theme for theme in profile.get("themes", []) if isinstance(theme, dict)]
    by_id = {str(theme.get("theme_id", "")): theme for theme in themes if str(theme.get("theme_id", "")).strip()}
    applied: list[dict[str, Any]] = []
    for raw in updates:
        update = normalize_theme_update(raw)
        if update["action"] == "no_change":
            continue
        theme_id = update["theme_id"]
        existing = by_id.get(theme_id, {})
        merged = merge_theme(existing, update)
        by_id[theme_id] = merged
        applied.append({"action": update["action"], "theme_id": theme_id, "label": merged.get("label", "")})
    profile["themes"] = sorted(by_id.values(), key=lambda item: (str(item.get("status", "")), str(item.get("label", "")).lower()))
    log = list(profile.get("theme_update_log", []) or [])
    for item in applied:
        log.append({**item, "updated_at_utc": now_utc_iso(), "source_stage": "07C_theme_miner"})
    profile["theme_update_log"] = log[-200:]
    return profile, applied


def normalize_theme_update(raw: dict[str, Any]) -> dict[str, Any]:
    label = clean_text(raw.get("label") or raw.get("theme_id") or "Unnamed theme", 120)
    theme_id = str(raw.get("theme_id") or "").strip()
    if not theme_id:
        theme_id = "theme_" + re.sub(r"[^a-z0-9]+", "_", label.lower()).strip("_")
    if not theme_id.startswith("theme_"):
        theme_id = "theme_" + theme_id
    theme_id = re.sub(r"[^a-z0-9_]+", "_", theme_id.lower()).strip("_")
    return {
        "action": normalize_enum(raw.get("action"), THEME_ACTIONS, "update_theme"),
        "theme_id": theme_id,
        "label": label,
        "theme_domain": normalize_theme_domain(raw.get("theme_domain") or raw.get("domain"), raw.get("theme_type")),
        "theme_type": clean_text(raw.get("theme_type") or "other", 80),
        "status": normalize_enum(raw.get("status"), THEME_STATUSES, "candidate"),
        "confidence": clamp_float(raw.get("confidence", 0.0)),
        "canon_relevance": clean_text(raw.get("canon_relevance") or "unknown", 80),
        "description": clean_text(raw.get("description"), 800),
        "evidence_entities": normalize_string_list(raw.get("evidence_entities"), 40, 120),
        "evidence_claim_ids": normalize_string_list(raw.get("evidence_claim_ids"), 60, 120),
        "evidence_snippet_ids": normalize_string_list(raw.get("evidence_snippet_ids"), 80, 120),
        "positive_indicators": normalize_string_list(raw.get("positive_indicators"), 40, 160),
        "negative_indicators": normalize_string_list(raw.get("negative_indicators"), 30, 160),
        "related_themes": normalize_string_list(raw.get("related_themes"), 30, 120),
        "disambiguation_notes": normalize_string_list(raw.get("disambiguation_notes"), 30, 220),
        "pattern_notes": normalize_string_list(raw.get("pattern_notes"), 30, 220),
        "provenance_summary": clean_text(raw.get("provenance_summary"), 600),
    }


def merge_theme(existing: dict[str, Any], update: dict[str, Any]) -> dict[str, Any]:
    now = now_utc_iso()
    merged = dict(existing) if existing else {}
    for field in ("theme_id", "label", "theme_domain", "theme_type", "canon_relevance", "description", "provenance_summary"):
        if update.get(field):
            merged[field] = update[field]
    merged["status"] = update.get("status") or merged.get("status", "candidate")
    merged["confidence"] = max(clamp_float(merged.get("confidence", 0.0)), clamp_float(update.get("confidence", 0.0)))
    for field in (
        "evidence_entities",
        "evidence_claim_ids",
        "evidence_snippet_ids",
        "positive_indicators",
        "negative_indicators",
        "related_themes",
        "disambiguation_notes",
        "pattern_notes",
    ):
        merged[field] = merge_lists(merged.get(field, []), update.get(field, []))
    merged.setdefault("created_at_utc", now)
    merged["last_updated"] = now
    merged["last_update_action"] = update.get("action", "update_theme")
    return merged


def merge_lists(left: Any, right: Any) -> list[str]:
    out: list[str] = []
    for values in (left, right):
        for value in values if isinstance(values, list) else []:
            text = clean_text(value, 240)
            if text and text not in out:
                out.append(text)
    return out


def normalize_string_list(value: Any, limit: int, max_chars: int) -> list[str]:
    if not isinstance(value, list):
        return []
    out: list[str] = []
    for item in value:
        text = clean_text(item, max_chars)
        if text and text not in out:
            out.append(text)
        if len(out) >= limit:
            break
    return out


def normalize_enum(value: Any, allowed: set[str], default: str) -> str:
    clean = str(value or "").strip()
    return clean if clean in allowed else default


def normalize_theme_domain(value: Any, theme_type: Any = None) -> str:
    clean = normalized_name_key(str(value or "").replace("_", " "))
    aliases = {
        "mythological": "mythological_theological",
        "mythological theological": "mythological_theological",
        "religious": "mythological_theological",
        "theological": "mythological_theological",
        "technological": "technological_scientific",
        "technological scientific": "technological_scientific",
        "scientific": "technological_scientific",
        "emotional": "emotional_psychological",
        "emotional psychological": "emotional_psychological",
        "psychological": "emotional_psychological",
        "philosophical": "philosophical_ideological",
        "philosophical ideological": "philosophical_ideological",
        "ideological": "philosophical_ideological",
        "aesthetic": "aesthetic",
        "historical": "historical_political",
        "historical political": "historical_political",
        "political": "historical_political",
        "other": "other",
    }
    direct = aliases.get(clean)
    if direct:
        return direct
    underscored = clean.replace(" ", "_")
    if underscored in THEME_DOMAINS:
        return underscored
    if theme_type is not None and str(theme_type).strip() != str(value or "").strip():
        return normalize_theme_domain(theme_type, None)
    return "other"


def clamp_float(value: Any) -> float:
    try:
        return round(max(0.0, min(1.0, float(value))), 3)
    except (TypeError, ValueError):
        return 0.0


def clean_text(value: Any, max_chars: int) -> str:
    return " ".join(str(value or "").strip().split())[:max_chars]


def compact_join(parts: list[Any], max_chars: int) -> str:
    text = " ".join(str(part or "").strip() for part in parts if str(part or "").strip())
    return " ".join(text.split())[:max_chars]


def json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, indent=2)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--in-entity-candidate-harvest-json", type=Path, required=True)
    parser.add_argument("--in-entity-adjudication-recommendations-json", type=Path, required=True)
    parser.add_argument("--in-resolved-entities-json", type=Path, required=True)
    parser.add_argument("--in-review-memory-json", type=Path, required=True)
    parser.add_argument("--inout-theme-profile-json", type=Path, required=True)
    parser.add_argument("--out-theme-profile-update-report-json", type=Path, required=True)
    parser.add_argument("--in-pipeline-config-json", type=Path, required=False, default=None)
    args = parser.parse_args()
    run(
        args.in_entity_candidate_harvest_json,
        args.in_entity_adjudication_recommendations_json,
        args.in_resolved_entities_json,
        args.in_review_memory_json,
        args.inout_theme_profile_json,
        args.out_theme_profile_update_report_json,
        args.in_pipeline_config_json,
    )


if __name__ == "__main__":
    main()
