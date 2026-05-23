from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from pipeline.common import get_logger, now_utc_iso, read_json, stable_id, write_json
from pipeline.entity_resolution import normalized_name_key
from pipeline.model_provider import call_model_chat, model_call_kwargs


ADJUDICATION_SCHEMA_VERSION = 1
EXTERNALITY_CACHE_SCHEMA_VERSION = 1
WEB_TASK_NAME = "stage_07b_entity_adjudication_web"
DEFAULT_WEB_EVIDENCE_THRESHOLD = 3
DEFAULT_MAX_WEB_CANDIDATES_PER_RUN = 200
DEFAULT_WEB_TOOLS = [
    {
        "type": "openrouter:web_search",
        "parameters": {
            "engine": "parallel",
            "max_results": 5,
            "max_total_results": 10,
            "search_context_size": "low",
        },
    }
]

EXTERNALITY_CLASSES = {
    "none_detected",
    "external_fictional_ip",
    "real_world_person",
    "real_world_org",
    "historical_or_mythological",
    "generic_phrase",
    "ambiguous",
}
RECOMMENDED_ACTIONS = {
    "needs_author_review",
    "keep_lore_candidate",
    "demote_meta",
    "mark_generic",
    "review_alias",
    "no_action",
}
RECOMMENDED_TRACKS = {"lore_candidate", "meta", "mixed", "ignore", "unknown"}

MODEL_RESPONSE_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "recommendations": {
            "type": "array",
            "minItems": 1,
            "items": {
                "type": "object",
                "properties": {
                    "candidate_name": {"type": "string"},
                    "normalized_key": {"type": "string"},
                    "recommended_action": {"type": "string"},
                    "recommended_track": {"type": "string"},
                    "recommended_entity_type": {"type": "string"},
                    "canonical_name": {"type": ["string", "null"]},
                    "alias_of": {"type": ["string", "null"]},
                    "confidence": {"type": "number"},
                    "externality_class": {"type": "string"},
                    "local_lore_prior": {"type": "number"},
                    "external_reference_prior": {"type": "number"},
                    "theme_matches": {
                        "type": "array",
                        "items": {"type": "object", "additionalProperties": True},
                    },
                    "in_world_signals": {"type": "array", "items": {"type": "string"}},
                    "meta_signals": {"type": "array", "items": {"type": "string"}},
                    "web_findings": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "query": {"type": "string"},
                                "finding": {"type": "string"},
                                "externality_weight": {"type": "number"},
                                "source_url": {"type": "string"},
                            },
                            "required": ["query", "finding", "externality_weight"],
                            "additionalProperties": True,
                        },
                    },
                    "reasoning_summary": {"type": "string"},
                    "human_review_question": {"type": "string"},
                },
                "required": [
                    "candidate_name",
                    "normalized_key",
                    "recommended_action",
                    "recommended_track",
                    "recommended_entity_type",
                    "canonical_name",
                    "alias_of",
                    "confidence",
                    "externality_class",
                    "local_lore_prior",
                    "external_reference_prior",
                    "theme_matches",
                    "in_world_signals",
                    "meta_signals",
                    "web_findings",
                    "reasoning_summary",
                    "human_review_question",
                ],
                "additionalProperties": True,
            },
        }
    },
    "required": ["recommendations"],
    "additionalProperties": False,
}


def run(
    in_entity_candidate_harvest_json: Path,
    out_entity_adjudication_recommendations_json: Path,
    out_externality_cache_json: Path,
    in_pipeline_config_json: Path | None = None,
    in_theme_profile_json: Path | None = None,
) -> None:
    logger = get_logger(__name__)
    harvest = read_json(in_entity_candidate_harvest_json)
    provider_config = read_json(in_pipeline_config_json) if in_pipeline_config_json and in_pipeline_config_json.exists() else {}
    theme_profile = read_json(in_theme_profile_json) if in_theme_profile_json and in_theme_profile_json.exists() else {}
    candidates = [item for item in harvest.get("candidates", []) if isinstance(item, dict)] if isinstance(harvest, dict) else []
    task_cfg = stage_task_config(provider_config, WEB_TASK_NAME)
    kwargs = web_model_kwargs(provider_config, task_cfg)
    web_enabled = bool(task_cfg.get("enabled", True))
    force_refresh = bool(task_cfg.get("force_refresh", False))
    max_web_candidates = max(0, int(task_cfg.get("max_web_candidates_per_run", DEFAULT_MAX_WEB_CANDIDATES_PER_RUN) or 0))
    cache = load_externality_cache(out_externality_cache_json)
    cache_entries = cache.setdefault("entries", {})
    recommendations: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []
    selected_count = 0
    web_call_count = 0
    cache_hit_count = 0
    skipped_by_limit_count = 0

    logger.info("Stage 07B: adjudicating externality for %d harvested candidate(s).", len(candidates))
    for candidate in candidates:
        selected, selection_reasons = should_web_adjudicate(candidate, task_cfg)
        if not web_enabled:
            recommendations.append(build_local_recommendation(candidate, selection_reasons, "web_disabled"))
            continue
        if not selected:
            recommendations.append(build_local_recommendation(candidate, selection_reasons, "local_only_not_selected"))
            continue
        if selected_count >= max_web_candidates:
            skipped_by_limit_count += 1
            recommendations.append(build_local_recommendation(candidate, selection_reasons + ["web_candidate_limit_reached"], "web_skipped_limit"))
            continue

        selected_count += 1
        key = candidate_key(candidate)
        cache_key = candidate_context_cache_key(candidate)
        cached = cache_entries.get(key) if isinstance(cache_entries, dict) else None
        if (
            not force_refresh
            and isinstance(cached, dict)
            and cached.get("cache_key") == cache_key
            and isinstance(cached.get("recommendation"), dict)
        ):
            recommendation = normalize_recommendation(cached["recommendation"], candidate, selection_reasons)
            recommendation["adjudication_status"] = "web_adjudicated"
            recommendation["cache_status"] = "hit"
            recommendations.append(recommendation)
            cache_hit_count += 1
            continue

        prompt = build_web_adjudication_prompt(candidate, selection_reasons, theme_profile)
        logger.info(
            "Stage 07B web adjudication: candidate=%s evidence=%s reasons=%s model=%s",
            candidate.get("candidate_name", key),
            candidate.get("evidence_count", 0),
            ", ".join(selection_reasons),
            kwargs.get("api_model", ""),
        )
        response = call_model_chat(prompt=prompt, **kwargs)
        web_call_count += 1
        recommendation = normalize_web_response(response, candidate, selection_reasons)
        if recommendation is None:
            failures.append(
                {
                    "candidate_name": candidate.get("candidate_name", ""),
                    "normalized_key": key,
                    "reason": "invalid_or_empty_web_model_response",
                    "response_keys": sorted(response.keys()) if isinstance(response, dict) else [],
                }
            )
            recommendations.append(build_local_recommendation(candidate, selection_reasons, "web_failed"))
            continue

        recommendation["cache_status"] = "miss"
        recommendations.append(recommendation)
        cache_entries[key] = {
            "cache_key": cache_key,
            "candidate_name": candidate.get("candidate_name", ""),
            "normalized_key": key,
            "updated_at_utc": now_utc_iso(),
            "source_model": kwargs.get("api_model", ""),
            "recommendation": recommendation,
        }

    recommendations.sort(
        key=lambda item: (
            -int(item.get("web_adjudication_selected", False)),
            -float(item.get("confidence", 0.0) or 0.0),
            str(item.get("candidate_name", "")).lower(),
        )
    )
    payload = {
        "schema_version": ADJUDICATION_SCHEMA_VERSION,
        "generated_at_utc": now_utc_iso(),
        "stage": "07B_entity_adjudication",
        "inputs": {
            "entity_candidate_harvest_json": str(in_entity_candidate_harvest_json),
            "candidate_count": len(candidates),
            "pipeline_config_json": str(in_pipeline_config_json) if in_pipeline_config_json else "",
            "theme_profile_json": str(in_theme_profile_json) if in_theme_profile_json else "",
        },
        "policy": {
            "web_search_detects_externality_not_canon": True,
            "canon_gate": "human_review",
            "candidate_selection": (
                "Web adjudication runs only on uncertain, mixed, high-evidence, type-conflicted, "
                "low-confidence, or locally external-looking candidates."
            ),
            "web_task": WEB_TASK_NAME,
            "web_model_provider": kwargs.get("provider", ""),
            "web_model_name": kwargs.get("api_model", ""),
            "web_tools": kwargs.get("tools", []),
            "structured_outputs": bool(kwargs.get("json_schema")),
        },
        "summary": {
            "recommendation_count": len(recommendations),
            "web_selected_candidate_count": selected_count,
            "web_call_count": web_call_count,
            "cache_hit_count": cache_hit_count,
            "web_skipped_by_limit_count": skipped_by_limit_count,
            "failure_count": len(failures),
        },
        "failures": failures,
        "recommendations": recommendations,
    }
    cache.update(
        {
            "schema_version": EXTERNALITY_CACHE_SCHEMA_VERSION,
            "updated_at_utc": now_utc_iso(),
            "stage": "07B_entity_adjudication",
            "source_task": WEB_TASK_NAME,
            "entries": cache_entries,
        }
    )
    write_json(out_entity_adjudication_recommendations_json, payload)
    write_json(out_externality_cache_json, cache)
    logger.info(
        "Stage 07B complete: recommendations=%d web_selected=%d web_calls=%d cache_hits=%d failures=%d",
        len(recommendations),
        selected_count,
        web_call_count,
        cache_hit_count,
        len(failures),
    )


def load_externality_cache(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"schema_version": EXTERNALITY_CACHE_SCHEMA_VERSION, "entries": {}}
    try:
        payload = read_json(path)
    except Exception:
        return {"schema_version": EXTERNALITY_CACHE_SCHEMA_VERSION, "entries": {}}
    if not isinstance(payload, dict):
        return {"schema_version": EXTERNALITY_CACHE_SCHEMA_VERSION, "entries": {}}
    if not isinstance(payload.get("entries"), dict):
        payload["entries"] = {}
    return payload


def stage_task_config(provider_config: dict[str, Any], task_name: str) -> dict[str, Any]:
    routing = provider_config.get("model_routing", {}) if isinstance(provider_config, dict) else {}
    tasks = routing.get("tasks", {}) if isinstance(routing, dict) else {}
    task_cfg = tasks.get(task_name, {}) if isinstance(tasks, dict) and isinstance(tasks.get(task_name, {}), dict) else {}
    return dict(task_cfg)


def web_model_kwargs(provider_config: dict[str, Any], task_cfg: dict[str, Any]) -> dict[str, Any]:
    kwargs = model_call_kwargs(provider_config, WEB_TASK_NAME)
    if "provider" not in task_cfg:
        kwargs["provider"] = "openrouter"
    if "api_base_url" not in task_cfg:
        kwargs["api_base_url"] = "https://openrouter.ai/api/v1"
    if "api_model" not in task_cfg and "profile" not in task_cfg and "model_profile" not in task_cfg:
        kwargs["api_model"] = "openai/gpt-oss-120b"
    kwargs["timeout_seconds"] = max(int(kwargs.get("timeout_seconds", 60)), 180)
    kwargs["max_tokens"] = max(int(kwargs.get("max_tokens", 4096)), 3500)
    if not kwargs.get("tools"):
        kwargs["tools"] = DEFAULT_WEB_TOOLS
    if task_cfg.get("structured_outputs_enabled", True):
        kwargs["json_schema"] = MODEL_RESPONSE_SCHEMA
    if "rate_state_path" not in task_cfg:
        kwargs["rate_state_path"] = Path("artifacts/learning/openrouter_gpt_oss_120b_stage_07b_rate_runtime.json")
    return kwargs


def should_web_adjudicate(candidate: dict[str, Any], task_cfg: dict[str, Any]) -> tuple[bool, list[str]]:
    reasons: list[str] = []
    threshold = max(1, int(task_cfg.get("web_evidence_threshold", DEFAULT_WEB_EVIDENCE_THRESHOLD) or DEFAULT_WEB_EVIDENCE_THRESHOLD))
    low_confidence_threshold = float(task_cfg.get("low_confidence_threshold", 0.7) or 0.7)
    denotation = str(candidate.get("model_denotation_class") or candidate.get("model_annotation", {}).get("denotation_class") or "").strip()
    recommended_track = str(candidate.get("recommended_track") or candidate.get("model_annotation", {}).get("recommended_track") or "").strip()
    signal_flags = candidate.get("signal_flags", {}) if isinstance(candidate.get("signal_flags"), dict) else {}
    evidence_count = int(candidate.get("evidence_count", 0) or 0)
    model_confidence = candidate.get("model_confidence", candidate.get("model_annotation", {}).get("confidence"))
    if denotation in {"likely_external_reference", "mixed_or_uncertain"}:
        reasons.append(f"denotation:{denotation}")
    if recommended_track in {"mixed", "unknown"}:
        reasons.append(f"track:{recommended_track}")
    if evidence_count >= threshold:
        reasons.append(f"high_evidence:{evidence_count}")
    if candidate.get("type_conflicts"):
        reasons.append("type_conflict")
    if bool(signal_flags.get("external_media_marker")):
        reasons.append("external_media_marker")
    if bool(signal_flags.get("inspiration_marker")):
        reasons.append("inspiration_marker")
    if bool(signal_flags.get("prior_rejected_memory_match")):
        reasons.append("prior_rejected_memory_match")
    try:
        if model_confidence is not None and float(model_confidence) < low_confidence_threshold:
            reasons.append(f"low_model_confidence:{round(float(model_confidence), 3)}")
    except (TypeError, ValueError):
        reasons.append("missing_model_confidence")
    return bool(reasons), reasons or ["local_context_sufficient"]


def build_web_adjudication_prompt(candidate: dict[str, Any], selection_reasons: list[str], theme_profile: dict[str, Any]) -> str:
    packet = candidate_packet(candidate)
    theme_summary = theme_profile_summary(theme_profile)
    return f"""You are Stage 07B of the THERIAC Lore Bible pipeline.
Use OpenRouter web search only to detect whether a candidate phrase has a strong external referent.

Critical policy:
- Web search detects externality; it does not decide canon.
- Local THERIAC evidence and human review decide canon.
- Externality is not disqualifying: historical, mythological, and real-world names may still be canon if THERIAC adopted them.
- Strong external fictional IP plus weak local in-world usage should default to meta/inspiration review.
- Generic phrases should be judged mainly by local evidence.
- Do not promote canon directly.

Search guidance:
- Search the exact candidate name first.
- Use at most a small number of targeted searches.
- Prefer concise findings over broad encyclopedic summaries.
- If search results are ambiguous or generic, say so.

Selection reasons:
{json_dumps(selection_reasons)}

Current theme profile summary, if any:
{json_dumps(theme_summary)}

Local candidate packet:
{json_dumps(packet)}

Return strict JSON with exactly this shape:
{{
  "recommendations": [
    {{
      "candidate_name": "display name",
      "normalized_key": "normalized key",
      "recommended_action": "needs_author_review | keep_lore_candidate | demote_meta | mark_generic | review_alias | no_action",
      "recommended_track": "lore_candidate | meta | mixed | ignore | unknown",
      "recommended_entity_type": "term or more specific review type",
      "canonical_name": null,
      "alias_of": null,
      "confidence": 0.0,
      "externality_class": "none_detected | external_fictional_ip | real_world_person | real_world_org | historical_or_mythological | generic_phrase | ambiguous",
      "local_lore_prior": 0.0,
      "external_reference_prior": 0.0,
      "theme_matches": [],
      "in_world_signals": [],
      "meta_signals": [],
      "web_findings": [
        {{"query": "exact search query", "finding": "short finding", "externality_weight": 0.0}}
      ],
      "reasoning_summary": "brief review-oriented explanation",
      "human_review_question": "question for the human editor"
    }}
  ]
}}
"""


def candidate_packet(candidate: dict[str, Any]) -> dict[str, Any]:
    return {
        "candidate_name": candidate.get("candidate_name", ""),
        "normalized_key": candidate_key(candidate),
        "surface_forms": candidate.get("surface_forms", [])[:12],
        "source_snippet_ids": candidate.get("source_snippet_ids", [])[:20],
        "evidence_count": candidate.get("evidence_count", 0),
        "first_seen_timestamp_utc": candidate.get("first_seen_timestamp_utc", ""),
        "last_seen_timestamp_utc": candidate.get("last_seen_timestamp_utc", ""),
        "sample_texts": [str(text)[:900] for text in candidate.get("sample_texts", [])[:6]],
        "initial_proposed_entity_type": candidate.get("initial_proposed_entity_type", ""),
        "proposed_entity_type": candidate.get("proposed_entity_type", ""),
        "type_vote_totals": candidate.get("type_vote_totals", {}),
        "type_conflicts": candidate.get("type_conflicts", [])[:10],
        "known_entities_co_mentioned": candidate.get("known_entities_co_mentioned", [])[:10],
        "candidate_cooccurrences": candidate.get("candidate_cooccurrences", [])[:12],
        "signal_flags": candidate.get("signal_flags", {}),
        "signal_details": candidate.get("signal_details", {}),
        "legacy_triage_hint": candidate.get("legacy_triage_hint", {}),
        "model_annotation": candidate.get("model_annotation", {}),
        "model_denotation_class": candidate.get("model_denotation_class", ""),
        "recommended_track": candidate.get("recommended_track", ""),
        "local_lore_prior": candidate.get("local_lore_prior", 0.0),
        "external_reference_prior": candidate.get("external_reference_prior", 0.0),
        "model_reasoning_summary": candidate.get("model_reasoning_summary", ""),
    }


def theme_profile_summary(theme_profile: dict[str, Any]) -> dict[str, Any]:
    themes = theme_profile.get("themes", []) if isinstance(theme_profile, dict) else []
    if not isinstance(themes, list):
        themes = []
    return {
        "active_themes": [
            {
                "theme_id": theme.get("theme_id", ""),
                "label": theme.get("label", ""),
                "theme_domain": theme.get("theme_domain", ""),
                "theme_type": theme.get("theme_type", ""),
                "confidence": theme.get("confidence", 0.0),
                "positive_indicators": theme.get("positive_indicators", [])[:8],
            }
            for theme in themes[:20]
            if isinstance(theme, dict) and str(theme.get("status", "active")).strip() in {"active", "candidate"}
        ]
    }


def normalize_web_response(response: Any, candidate: dict[str, Any], selection_reasons: list[str]) -> dict[str, Any] | None:
    raw: list[dict[str, Any]]
    if isinstance(response, dict) and isinstance(response.get("recommendations"), list):
        raw = [item for item in response["recommendations"] if isinstance(item, dict)]
    elif isinstance(response, dict) and isinstance(response.get("recommendation"), dict):
        raw = [response["recommendation"]]
    elif isinstance(response, dict) and isinstance(response.get("_json_root"), list):
        raw = [item for item in response["_json_root"] if isinstance(item, dict)]
    else:
        return None
    if not raw:
        return None
    key = candidate_key(candidate)
    match = next(
        (
            item
            for item in raw
            if normalized_name_key(str(item.get("normalized_key") or item.get("normalized_name_key") or item.get("candidate_name") or "")) == key
        ),
        raw[0],
    )
    recommendation = normalize_recommendation(match, candidate, selection_reasons)
    recommendation["adjudication_mode"] = "web_enabled_externality"
    recommendation["adjudication_status"] = "web_adjudicated"
    recommendation["web_adjudication_selected"] = True
    return recommendation


def normalize_recommendation(raw: dict[str, Any], candidate: dict[str, Any], selection_reasons: list[str]) -> dict[str, Any]:
    key = candidate_key(candidate)
    externality_class = normalize_enum(raw.get("externality_class"), EXTERNALITY_CLASSES, infer_externality_class(candidate))
    local_lore_prior = clamp_float(raw.get("local_lore_prior", candidate.get("local_lore_prior", 0.0)))
    external_reference_prior = clamp_float(raw.get("external_reference_prior", candidate.get("external_reference_prior", 0.0)))
    action = normalize_enum(raw.get("recommended_action"), RECOMMENDED_ACTIONS, infer_recommended_action(externality_class, candidate, local_lore_prior))
    return {
        "recommendation_id": stable_id("entity_adjudication", key),
        "candidate_id": candidate.get("candidate_id", ""),
        "candidate_name": str(raw.get("candidate_name") or candidate.get("candidate_name") or "").strip(),
        "normalized_key": key,
        "source_snippet_ids": list(candidate.get("source_snippet_ids", []) or []),
        "evidence_count": int(candidate.get("evidence_count", 0) or 0),
        "recommended_action": action,
        "recommended_track": normalize_enum(raw.get("recommended_track"), RECOMMENDED_TRACKS, infer_recommended_track(action, externality_class, candidate)),
        "recommended_entity_type": clean_text(raw.get("recommended_entity_type") or candidate.get("proposed_entity_type") or "term", 120),
        "canonical_name": optional_text(raw.get("canonical_name")),
        "alias_of": optional_text(raw.get("alias_of")),
        "confidence": clamp_float(raw.get("confidence", 0.0)),
        "externality_class": externality_class,
        "local_lore_prior": local_lore_prior,
        "external_reference_prior": external_reference_prior,
        "theme_matches": normalize_object_list(raw.get("theme_matches")),
        "in_world_signals": normalize_string_list(raw.get("in_world_signals"), 12, 180),
        "meta_signals": normalize_string_list(raw.get("meta_signals"), 12, 180),
        "web_findings": normalize_web_findings(raw.get("web_findings"), candidate),
        "reasoning_summary": clean_text(raw.get("reasoning_summary") or local_reasoning_summary(candidate, externality_class), 900),
        "human_review_question": clean_text(raw.get("human_review_question") or default_review_question(candidate, externality_class), 500),
        "selection_reasons": selection_reasons,
        "source_model_denotation_class": candidate.get("model_denotation_class", ""),
        "source_model_reasoning_summary": candidate.get("model_reasoning_summary", ""),
        "adjudication_policy": "Web search detects externality; human review decides canon.",
    }


def build_local_recommendation(candidate: dict[str, Any], selection_reasons: list[str], status: str) -> dict[str, Any]:
    externality_class = infer_externality_class(candidate)
    raw = {
        "candidate_name": candidate.get("candidate_name", ""),
        "normalized_key": candidate_key(candidate),
        "recommended_action": infer_recommended_action(externality_class, candidate, clamp_float(candidate.get("local_lore_prior", 0.0))),
        "recommended_track": "",
        "recommended_entity_type": candidate.get("proposed_entity_type", "term"),
        "canonical_name": None,
        "alias_of": None,
        "confidence": candidate.get("model_confidence", 0.0),
        "externality_class": externality_class,
        "local_lore_prior": candidate.get("local_lore_prior", 0.0),
        "external_reference_prior": candidate.get("external_reference_prior", 0.0),
        "theme_matches": [],
        "in_world_signals": local_in_world_signals(candidate),
        "meta_signals": local_meta_signals(candidate),
        "web_findings": [],
        "reasoning_summary": local_reasoning_summary(candidate, externality_class),
        "human_review_question": default_review_question(candidate, externality_class),
    }
    recommendation = normalize_recommendation(raw, candidate, selection_reasons)
    recommendation["adjudication_mode"] = "local_07a_annotation"
    recommendation["adjudication_status"] = status
    recommendation["web_adjudication_selected"] = status not in {"local_only_not_selected", "web_disabled"}
    recommendation["cache_status"] = "not_applicable"
    return recommendation


def infer_externality_class(candidate: dict[str, Any]) -> str:
    flags = candidate.get("signal_flags", {}) if isinstance(candidate.get("signal_flags"), dict) else {}
    denotation = str(candidate.get("model_denotation_class") or "").strip()
    if bool(flags.get("generic_phrase")) or denotation == "likely_generic_phrase":
        return "generic_phrase"
    if bool(flags.get("external_media_marker")):
        return "ambiguous"
    if denotation == "likely_external_reference":
        return "ambiguous"
    return "none_detected"


def infer_recommended_action(externality_class: str, candidate: dict[str, Any], local_lore_prior: float) -> str:
    denotation = str(candidate.get("model_denotation_class") or "").strip()
    if denotation == "likely_alias":
        return "review_alias"
    if externality_class == "generic_phrase":
        return "mark_generic"
    if externality_class == "external_fictional_ip" and local_lore_prior < 0.5:
        return "demote_meta"
    if externality_class in {"real_world_person", "real_world_org"} and local_lore_prior < 0.45:
        return "demote_meta"
    if denotation == "likely_meta_reference":
        return "demote_meta"
    if denotation == "likely_lore_entity":
        return "keep_lore_candidate"
    return "needs_author_review"


def infer_recommended_track(action: str, externality_class: str, candidate: dict[str, Any]) -> str:
    source_track = str(candidate.get("recommended_track") or "").strip()
    if action in {"demote_meta"}:
        return "meta"
    if action in {"mark_generic", "no_action"}:
        return "ignore"
    if source_track == "lore":
        return "lore_candidate"
    if source_track in {"meta", "mixed", "unknown"}:
        return source_track
    if externality_class in {"external_fictional_ip", "real_world_person", "real_world_org"}:
        return "meta"
    return "unknown"


def local_in_world_signals(candidate: dict[str, Any]) -> list[str]:
    signals: list[str] = []
    if clamp_float(candidate.get("local_lore_prior", 0.0)) >= 0.65:
        signals.append("07A local lore prior is high")
    if candidate.get("known_entities_co_mentioned"):
        signals.append("Co-mentioned with known THERIAC entities")
    flags = candidate.get("signal_flags", {}) if isinstance(candidate.get("signal_flags"), dict) else {}
    if bool(flags.get("canon_adoption_marker")):
        signals.append("Local text contains canon-adoption wording")
    return signals


def local_meta_signals(candidate: dict[str, Any]) -> list[str]:
    signals: list[str] = []
    flags = candidate.get("signal_flags", {}) if isinstance(candidate.get("signal_flags"), dict) else {}
    if bool(flags.get("inspiration_marker")):
        signals.append("Local text marks candidate as inspiration/reference")
    if bool(flags.get("external_media_marker")):
        signals.append("Local text contains external-media markers")
    if bool(flags.get("meta_team_marker")):
        signals.append("Local text resembles contributor/team context")
    if bool(flags.get("generic_phrase")):
        signals.append("Candidate phrase is locally generic")
    return signals


def local_reasoning_summary(candidate: dict[str, Any], externality_class: str) -> str:
    model_summary = clean_text(candidate.get("model_reasoning_summary", ""), 600)
    if model_summary:
        return model_summary
    return f"Local 07A evidence suggests externality_class={externality_class}; web search was not used for this row."


def default_review_question(candidate: dict[str, Any], externality_class: str) -> str:
    name = str(candidate.get("candidate_name") or candidate_key(candidate) or "this candidate").strip()
    if externality_class == "external_fictional_ip":
        return f"Is {name} only an inspiration/reference, or has it been fictionalized into THERIAC canon?"
    if externality_class in {"real_world_person", "real_world_org", "historical_or_mythological"}:
        return f"Is {name} an adopted THERIAC lore element, or only an external reference?"
    if externality_class == "generic_phrase":
        return f"Does {name} denote a specific THERIAC entity in local context, or is it just a generic phrase?"
    return f"Should {name} be treated as THERIAC lore, meta/reference context, alias evidence, or ignored?"


def candidate_key(candidate: dict[str, Any]) -> str:
    return normalized_name_key(str(candidate.get("normalized_name_key") or candidate.get("normalized_key") or candidate.get("candidate_name") or ""))


def candidate_context_cache_key(candidate: dict[str, Any]) -> str:
    payload = {
        "normalized_key": candidate_key(candidate),
        "candidate_name": candidate.get("candidate_name", ""),
        "evidence_count": candidate.get("evidence_count", 0),
        "source_snippet_ids": candidate.get("source_snippet_ids", [])[:40],
        "sample_texts": candidate.get("sample_texts", [])[:8],
        "model_annotation": candidate.get("model_annotation", {}),
        "type_conflicts": candidate.get("type_conflicts", [])[:10],
        "signal_flags": candidate.get("signal_flags", {}),
    }
    return stable_id("externality_context", json.dumps(payload, ensure_ascii=False, sort_keys=True))


def normalize_web_findings(value: Any, candidate: dict[str, Any]) -> list[dict[str, Any]]:
    rows = value if isinstance(value, list) else []
    out: list[dict[str, Any]] = []
    for row in rows[:10]:
        if not isinstance(row, dict):
            continue
        query = clean_text(row.get("query") or candidate.get("candidate_name") or "", 160)
        finding = clean_text(row.get("finding") or "", 500)
        if not query and not finding:
            continue
        normalized = {
            "query": query,
            "finding": finding,
            "externality_weight": clamp_float(row.get("externality_weight", 0.0)),
        }
        source_url = optional_text(row.get("source_url") or row.get("url"))
        if source_url:
            normalized["source_url"] = source_url
        out.append(normalized)
    return out


def normalize_object_list(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [item for item in value[:12] if isinstance(item, dict)]


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


def optional_text(value: Any) -> str | None:
    text = clean_text(value, 240)
    if not text or text.lower() in {"null", "none", "n/a"}:
        return None
    return text


def clean_text(value: Any, max_chars: int) -> str:
    return " ".join(str(value or "").strip().split())[:max_chars]


def clamp_float(value: Any) -> float:
    try:
        return round(max(0.0, min(1.0, float(value))), 3)
    except (TypeError, ValueError):
        return 0.0


def json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, indent=2)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--in-entity-candidate-harvest-json", type=Path, required=True)
    parser.add_argument("--out-entity-adjudication-recommendations-json", type=Path, required=True)
    parser.add_argument("--out-externality-cache-json", type=Path, required=True)
    parser.add_argument("--in-pipeline-config-json", type=Path, required=False, default=None)
    parser.add_argument("--in-theme-profile-json", type=Path, required=False, default=None)
    args = parser.parse_args()
    run(
        args.in_entity_candidate_harvest_json,
        args.out_entity_adjudication_recommendations_json,
        args.out_externality_cache_json,
        args.in_pipeline_config_json,
        args.in_theme_profile_json,
    )


if __name__ == "__main__":
    main()
