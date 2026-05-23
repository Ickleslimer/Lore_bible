from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from pipeline.common import get_logger, now_utc_iso, read_json, stable_id, write_json
from pipeline.entity_resolution import normalized_name_key
from pipeline.model_provider import call_model_chat, model_call_kwargs


RECLASSIFICATION_SCHEMA_VERSION = 1
TASK_NAME = "stage_07d_theme_reclassification"
DEFAULT_MAX_MODEL_CANDIDATES_PER_CALL = 24
DEFAULT_MODEL_CHUNK_RETRY_ATTEMPTS = 2
THEME_RECLASSIFICATION_ACTIONS = {
    "needs_author_review",
    "keep_lore_candidate",
    "demote_meta",
    "mark_generic",
    "review_alias",
    "no_action",
}
THEME_RECLASSIFICATION_TRACKS = {"lore_candidate", "meta", "mixed", "ignore", "unknown"}
MODEL_RESPONSE_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "candidate_reclassifications": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "candidate_name": {"type": "string"},
                    "normalized_key": {"type": "string"},
                    "theme_matches": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "theme_id": {"type": "string"},
                                "label": {"type": "string"},
                                "status": {"type": "string"},
                                "match_strength": {"type": "number"},
                                "matched_indicators": {"type": "array", "items": {"type": "string"}},
                                "reason": {"type": "string"},
                                "prior_boost": {"type": "number"},
                            },
                            "required": ["theme_id", "label", "match_strength", "reason", "prior_boost"],
                            "additionalProperties": True,
                        },
                    },
                    "theme_prior_boost": {"type": "number"},
                    "theme_adjusted_lore_prior": {"type": "number"},
                    "theme_adjusted_recommended_action": {"type": "string"},
                    "theme_adjusted_recommended_track": {"type": "string"},
                    "why_not_auto_promote": {"type": "string"},
                    "human_review_question": {"type": "string"},
                    "model_reasoning_summary": {"type": "string"},
                },
                "required": [
                    "candidate_name",
                    "normalized_key",
                    "theme_matches",
                    "theme_prior_boost",
                    "theme_adjusted_lore_prior",
                    "theme_adjusted_recommended_action",
                    "theme_adjusted_recommended_track",
                    "why_not_auto_promote",
                    "human_review_question",
                    "model_reasoning_summary",
                ],
                "additionalProperties": True,
            },
        }
    },
    "required": ["candidate_reclassifications"],
    "additionalProperties": False,
}
THEME_KEYWORD_HINTS = {
    "sumerian": {"sumerian", "sumer", "mesopotamian", "annunaki", "anunnaki", "enki", "inanna", "ninhursag", "uruk", "gilgamesh", "enkidu"},
    "greek": {"greek", "hellenic", "spartan", "sparta", "krypteia", "leonidas", "olympus"},
    "spartan": {"spartan", "sparta", "krypteia", "leonidas"},
    "biblical": {"biblical", "abrahamic", "enoch", "watchers", "nephilim", "eden", "samael", "eve", "metatron"},
    "abrahamic": {"abrahamic", "biblical", "enoch", "watchers", "eden", "samael", "eve"},
    "japanese": {"japanese", "izanami", "yomi", "kami"},
}
THEME_INDICATOR_STOPWORDS = {
    "about",
    "active",
    "also",
    "and",
    "applied",
    "are",
    "associated",
    "association",
    "captures",
    "candidate",
    "canon",
    "character",
    "characters",
    "comparison",
    "does",
    "entities",
    "entity",
    "evidence",
    "explicit",
    "external",
    "for",
    "from",
    "future",
    "indicator",
    "indicators",
    "into",
    "lane",
    "local",
    "lore",
    "match",
    "mentions",
    "motif",
    "name",
    "names",
    "only",
    "origin",
    "pattern",
    "prior",
    "project",
    "reference",
    "references",
    "related",
    "status",
    "terms",
    "theme",
    "theriac",
    "this",
    "used",
    "usage",
    "use",
    "when",
    "with",
    "world",
}


def run(
    in_entity_candidate_harvest_json: Path,
    in_entity_adjudication_recommendations_json: Path,
    in_theme_profile_json: Path,
    out_theme_candidate_reclassification_json: Path,
    in_pipeline_config_json: Path | None = None,
) -> None:
    logger = get_logger(__name__)
    harvest = read_json(in_entity_candidate_harvest_json) if in_entity_candidate_harvest_json.exists() else {}
    adjudication = read_json(in_entity_adjudication_recommendations_json) if in_entity_adjudication_recommendations_json.exists() else {}
    theme_profile = read_json(in_theme_profile_json) if in_theme_profile_json.exists() else {}
    provider_config = read_json(in_pipeline_config_json) if in_pipeline_config_json and in_pipeline_config_json.exists() else {}
    candidates = {
        candidate_key(item): item
        for item in harvest.get("candidates", []) or []
        if isinstance(item, dict) and candidate_key(item)
    }
    recommendations = [
        item
        for item in adjudication.get("recommendations", []) or []
        if isinstance(item, dict)
    ]
    themes = active_themes(theme_profile)
    rows = [
        reclassify_candidate(rec, candidates.get(candidate_key(rec), {}), themes)
        for rec in recommendations
    ]
    model_stats = annotate_reclassifications_with_model(
        rows,
        recommendations,
        candidates,
        themes,
        provider_config,
        model_configured=bool(in_pipeline_config_json and in_pipeline_config_json.exists()),
        logger=logger,
    )
    rows = model_stats["rows"]
    rows.sort(key=lambda item: (-float(item.get("theme_adjusted_lore_prior", 0.0) or 0.0), str(item.get("candidate_name", "")).lower()))
    payload = {
        "schema_version": RECLASSIFICATION_SCHEMA_VERSION,
        "generated_at_utc": now_utc_iso(),
        "stage": "07D_theme_aware_candidate_reclassification",
        "inputs": {
            "entity_candidate_harvest_json": str(in_entity_candidate_harvest_json),
            "entity_adjudication_recommendations_json": str(in_entity_adjudication_recommendations_json),
            "theme_profile_json": str(in_theme_profile_json),
            "pipeline_config_json": str(in_pipeline_config_json) if in_pipeline_config_json else "",
            "candidate_count": len(recommendations),
            "active_theme_count": len(themes),
        },
        "policy": {
            "theme_match_changes_prior_not_final_decision": True,
            "transitive_thematic_learning_not_transitive_canon": True,
            "human_review_remains_canon_gate": True,
            "model_theme_reclassification_is_review_recommendation": True,
        },
        "summary": {
            "reclassification_count": len(rows),
            "theme_matched_candidate_count": sum(1 for row in rows if row.get("theme_matches")),
            "model_enabled": model_stats["model_enabled"],
            "model_candidate_count": model_stats["model_candidate_count"],
            "model_candidate_limit": model_stats["model_candidate_limit"],
            "model_call_count": model_stats["model_call_count"],
            "model_applied_candidate_count": model_stats["model_applied_candidate_count"],
            "model_failure_count": len(model_stats["failures"]),
        },
        "model": {
            "task": TASK_NAME,
            "provider": model_stats["provider"],
            "api_model": model_stats["api_model"],
            "failures": model_stats["failures"],
        },
        "candidate_reclassifications": rows,
    }
    write_json(out_theme_candidate_reclassification_json, payload)
    logger.info(
        "Stage 07D complete: reclassifications=%d theme_matched=%d active_themes=%d model_calls=%d model_applied=%d",
        len(rows),
        payload["summary"]["theme_matched_candidate_count"],
        len(themes),
        model_stats["model_call_count"],
        model_stats["model_applied_candidate_count"],
    )


def active_themes(theme_profile: dict[str, Any]) -> list[dict[str, Any]]:
    themes = []
    for theme in theme_profile.get("themes", []) or []:
        if not isinstance(theme, dict):
            continue
        if str(theme.get("status", "")).strip() not in {"active", "candidate"}:
            continue
        themes.append(theme)
    return themes


def reclassify_candidate(recommendation: dict[str, Any], candidate: dict[str, Any], themes: list[dict[str, Any]]) -> dict[str, Any]:
    key = candidate_key(recommendation)
    theme_matches = match_themes(recommendation, candidate, themes) if theme_matching_allowed(recommendation, candidate) else []
    base_prior = clamp_float(recommendation.get("local_lore_prior", candidate.get("local_lore_prior", 0.0)))
    external_prior = clamp_float(recommendation.get("external_reference_prior", candidate.get("external_reference_prior", 0.0)))
    theme_boost = round(sum(float(match.get("prior_boost", 0.0) or 0.0) for match in theme_matches), 3)
    penalty = externality_penalty(recommendation)
    adjusted_prior = clamp_float(base_prior + theme_boost - penalty)
    base_action = str(recommendation.get("recommended_action", "needs_author_review"))
    adjusted_action = adjusted_recommendation(base_action, recommendation, theme_matches, adjusted_prior)
    return {
        "reclassification_id": stable_id("theme_reclassification", key),
        "candidate_id": candidate.get("candidate_id", recommendation.get("candidate_id", "")),
        "candidate_name": recommendation.get("candidate_name") or candidate.get("candidate_name", ""),
        "normalized_key": key,
        "base_recommended_action": base_action,
        "base_recommended_track": recommendation.get("recommended_track", "unknown"),
        "base_local_lore_prior": base_prior,
        "base_external_reference_prior": external_prior,
        "externality_class": recommendation.get("externality_class", ""),
        "theme_matches": theme_matches,
        "theme_prior_boost": theme_boost,
        "externality_penalty": penalty,
        "theme_adjusted_lore_prior": adjusted_prior,
        "theme_adjusted_recommended_action": adjusted_action,
        "theme_adjusted_recommended_track": adjusted_track(recommendation, theme_matches, adjusted_action),
        "why_not_auto_promote": "Theme match changes relevance prior only; local evidence and human review still decide canon.",
        "human_review_question": theme_review_question(recommendation, theme_matches),
        "theme_reclassification_source": "deterministic",
        "model_reclassification_status": "not_run",
        "model_reasoning_summary": "",
    }


def annotate_reclassifications_with_model(
    rows: list[dict[str, Any]],
    recommendations: list[dict[str, Any]],
    candidates: dict[str, dict[str, Any]],
    themes: list[dict[str, Any]],
    provider_config: dict[str, Any],
    model_configured: bool,
    logger: Any,
) -> dict[str, Any]:
    task_cfg = stage_task_config(provider_config)
    kwargs = stage_model_kwargs(provider_config, task_cfg)
    base_stats = {
        "rows": rows,
        "model_enabled": False,
        "model_candidate_count": 0,
        "model_candidate_limit": 0,
        "model_call_count": 0,
        "model_applied_candidate_count": 0,
        "provider": kwargs.get("provider", ""),
        "api_model": kwargs.get("api_model", ""),
        "failures": [],
    }
    if not rows:
        return base_stats
    if not model_configured:
        mark_model_status(rows, "not_configured")
        return base_stats
    if not bool(task_cfg.get("enabled", True)):
        mark_model_status(rows, "disabled")
        return base_stats
    if not themes:
        mark_model_status(rows, "no_active_themes")
        return base_stats

    keyed_recommendations = {candidate_key(item): item for item in recommendations if isinstance(item, dict)}
    eligible_rows = [
        row
        for row in rows
        if model_reclassification_allowed(row, keyed_recommendations.get(str(row.get("normalized_key", "")), {}))
    ]
    eligible_rows.sort(key=lambda row: model_candidate_priority_key(row, candidates.get(str(row.get("normalized_key", "")), {})))
    max_model_candidates = max(0, int(task_cfg.get("max_model_candidates_per_run", 0) or 0))
    model_rows = eligible_rows[:max_model_candidates] if max_model_candidates else eligible_rows
    selected_keys = {str(row.get("normalized_key", "")) for row in model_rows if str(row.get("normalized_key", ""))}
    for row in rows:
        if str(row.get("normalized_key", "")) not in selected_keys:
            row["model_reclassification_status"] = "not_selected"
    if not model_rows:
        return {**base_stats, "model_enabled": True}

    max_per_call = max(
        1,
        int(task_cfg.get("max_candidates_per_call", DEFAULT_MAX_MODEL_CANDIDATES_PER_CALL) or DEFAULT_MAX_MODEL_CANDIDATES_PER_CALL),
    )
    batch_count = (len(model_rows) + max_per_call - 1) // max_per_call
    logger.info(
        "Stage 07D: requesting model theme reclassification for %d/%d eligible candidate(s) in %d initial batch(es).",
        len(model_rows),
        len(eligible_rows),
        batch_count,
    )
    annotations_by_key: dict[str, dict[str, Any]] = {}
    failures: list[dict[str, Any]] = []
    model_call_count = 0
    for batch_index, offset in enumerate(range(0, len(model_rows), max_per_call), start=1):
        chunk = model_rows[offset : offset + max_per_call]
        logger.info(
            "Stage 07D model batch %d/%d: candidates=%d offset=%d model=%s",
            batch_index,
            batch_count,
            len(chunk),
            offset,
            kwargs.get("api_model", ""),
        )
        annotations, chunk_failures, chunk_call_count = request_model_reclassifications_for_chunk(
            chunk,
            keyed_recommendations,
            candidates,
            themes,
            kwargs,
            logger,
            batch_label=f"{batch_index}/{batch_count}",
            offset=offset,
        )
        model_call_count += chunk_call_count
        failures.extend(chunk_failures)
        for annotation in annotations:
            key = candidate_key(annotation)
            if key:
                annotations_by_key[key] = annotation

    known_themes = known_theme_lookup(themes)
    applied_count = 0
    out_rows: list[dict[str, Any]] = []
    for row in rows:
        key = str(row.get("normalized_key", ""))
        if key in selected_keys and key not in annotations_by_key:
            row["model_reclassification_status"] = "model_missing_or_invalid"
            row["theme_reclassification_source"] = "deterministic"
        if key in annotations_by_key:
            row = apply_model_reclassification(row, annotations_by_key[key], known_themes)
            if row.get("model_reclassification_status") == "model_applied":
                applied_count += 1
        out_rows.append(row)
    if failures:
        logger.warning(
            "Stage 07D model theme reclassification had %d recoverable failure group(s); deterministic rows were retained. failures=%s",
            len(failures),
            json.dumps(failures[:3], ensure_ascii=False)[:1200],
        )
    return {
        **base_stats,
        "rows": out_rows,
        "model_enabled": True,
        "model_candidate_count": len(model_rows),
        "model_candidate_limit": max_model_candidates,
        "model_call_count": model_call_count,
        "model_applied_candidate_count": applied_count,
        "failures": failures,
    }


def mark_model_status(rows: list[dict[str, Any]], status: str) -> None:
    for row in rows:
        row["model_reclassification_status"] = status
        row["theme_reclassification_source"] = "deterministic"


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
        kwargs["api_model"] = "deepseek/deepseek-v4-flash"
    kwargs["timeout_seconds"] = max(int(kwargs.get("timeout_seconds", 60)), 180)
    kwargs["max_tokens"] = max(int(kwargs.get("max_tokens", 4096)), 8192)
    if not task_cfg or "rate_state_path" not in task_cfg:
        kwargs["rate_state_path"] = Path("artifacts/learning/openrouter_deepseek_stage_07d_theme_reclassification_rate_runtime.json")
    if task_cfg.get("structured_outputs_enabled", True):
        kwargs["json_schema"] = MODEL_RESPONSE_SCHEMA
    return kwargs


def model_reclassification_allowed(row: dict[str, Any], recommendation: dict[str, Any]) -> bool:
    action = str(row.get("base_recommended_action") or recommendation.get("recommended_action") or "").strip()
    track = str(row.get("base_recommended_track") or recommendation.get("recommended_track") or "").strip()
    externality = str(row.get("externality_class") or recommendation.get("externality_class") or "").strip()
    if action in {"demote_meta", "mark_generic", "no_action"}:
        return False
    if track in {"meta", "ignore"}:
        return False
    if externality in {"external_fictional_ip", "real_world_person", "real_world_org", "generic_phrase"}:
        return False
    return True


def model_candidate_priority_key(row: dict[str, Any], candidate: dict[str, Any]) -> tuple[int, float, float, int, str]:
    has_theme_match = int(bool(row.get("theme_matches")))
    externality_bonus = int(str(row.get("externality_class", "")) == "historical_or_mythological")
    lore_prior = float(row.get("base_local_lore_prior", 0.0) or 0.0)
    evidence_count = int(candidate.get("evidence_count", 0) or 0)
    return (-has_theme_match, -externality_bonus, -lore_prior, -evidence_count, str(row.get("candidate_name", "")).lower())


def request_model_reclassifications_for_chunk(
    chunk: list[dict[str, Any]],
    recommendations: dict[str, dict[str, Any]],
    candidates: dict[str, dict[str, Any]],
    themes: list[dict[str, Any]],
    kwargs: dict[str, Any],
    logger: Any,
    batch_label: str,
    offset: int,
    depth: int = 0,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], int]:
    attempts = max(1, int(kwargs.get("reclassification_retry_attempts", DEFAULT_MODEL_CHUNK_RETRY_ATTEMPTS) or DEFAULT_MODEL_CHUNK_RETRY_ATTEMPTS))
    prompt = build_theme_reclassification_prompt(chunk, recommendations, candidates, themes)
    candidate_keys = [str(item.get("normalized_key", "")) for item in chunk]
    attempt_failures: list[dict[str, Any]] = []
    call_count = 0
    for attempt in range(1, attempts + 1):
        call_count += 1
        logger.info(
            "Stage 07D model request %s attempt %d/%d: candidates=%d offset=%d depth=%d",
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
        annotations = normalize_model_reclassification_response(response)
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
        annotation_keys = {candidate_key(annotation) for annotation in annotations if isinstance(annotation, dict)}
        missing = [key for key in candidate_keys if key and key not in annotation_keys]
        if missing:
            logger.warning(
                "Stage 07D model request %s returned %d/%d reclassifications; missing keys retain deterministic rows.",
                batch_label,
                len(annotation_keys),
                len(candidate_keys),
            )
        return annotations, [], call_count

    if len(chunk) > 1:
        midpoint = max(1, len(chunk) // 2)
        logger.warning(
            "Stage 07D model batch %s failed validation after %d attempt(s); splitting %d candidate(s) into %d and %d.",
            batch_label,
            attempts,
            len(chunk),
            len(chunk[:midpoint]),
            len(chunk[midpoint:]),
        )
        left_annotations, left_failures, left_calls = request_model_reclassifications_for_chunk(
            chunk[:midpoint],
            recommendations,
            candidates,
            themes,
            kwargs,
            logger,
            batch_label=f"{batch_label}a",
            offset=offset,
            depth=depth + 1,
        )
        right_annotations, right_failures, right_calls = request_model_reclassifications_for_chunk(
            chunk[midpoint:],
            recommendations,
            candidates,
            themes,
            kwargs,
            logger,
            batch_label=f"{batch_label}b",
            offset=offset + midpoint,
            depth=depth + 1,
        )
        return left_annotations + right_annotations, left_failures + right_failures, call_count + left_calls + right_calls

    return [], attempt_failures, call_count


def normalize_model_reclassification_response(response: Any) -> list[dict[str, Any]] | None:
    if isinstance(response, dict):
        rows = response.get("candidate_reclassifications")
        if rows is None:
            rows = response.get("reclassifications")
        if isinstance(rows, list):
            return [item for item in rows if isinstance(item, dict)]
    if isinstance(response, list):
        return [item for item in response if isinstance(item, dict)]
    return None


def build_theme_reclassification_prompt(
    rows: list[dict[str, Any]],
    recommendations: dict[str, dict[str, Any]],
    candidates: dict[str, dict[str, Any]],
    themes: list[dict[str, Any]],
) -> str:
    theme_rows = [theme_model_row(theme) for theme in themes]
    candidate_rows = [
        model_candidate_row(row, recommendations.get(str(row.get("normalized_key", "")), {}), candidates.get(str(row.get("normalized_key", "")), {}))
        for row in rows
    ]
    return f"""You are Stage 07D of the THERIAC Lore Bible pipeline.
Use the active theme profile to refine candidate entity review recommendations.
Return strict JSON only.

Core rules:
- Do not use web search.
- Web externality has already been checked by Stage 07B.
- Theme match changes the prior only; it is never a canon promotion rule.
- Human/editor review remains the canon gate.
- Transitive thematic learning is allowed; transitive canon is not.
- Externality is not disqualifying for historical or mythological names when local evidence and active themes support in-world use.
- Strong external fictional IP, real-world person/org, generic phrase, or meta context should not be upgraded by theme alone.
- Treat theme_domain as a broad lane. Do not use a mythological/theological theme to upgrade technological or emotional candidates, or vice versa, unless the candidate evidence explicitly bridges those lanes.
- If the active themes do not genuinely help a candidate, return no theme_matches and preserve the base recommendation.
- DISAMBIGUATION RULE: Distinguish between abstract themes and characters named after concepts (e.g., Love, Loss, Fear, Greed, Altruism). Do not boost a candidate simply because a character's name matches a theme word; look for the actual thematic concept.

Active theme profile:
{json.dumps(theme_rows, ensure_ascii=False, indent=2)}

Candidate rows:
{json.dumps(candidate_rows, ensure_ascii=False, indent=2)}

Return exactly one JSON object:
{{
  "candidate_reclassifications": [
    {{
      "candidate_name": "string",
      "normalized_key": "same key from input",
      "theme_matches": [
        {{
          "theme_id": "must be a theme_id from active theme profile",
          "label": "theme label",
          "status": "active|candidate",
          "match_strength": 0.0,
          "matched_indicators": ["short local/theme overlap indicators"],
          "reason": "why this theme changes the candidate prior",
          "prior_boost": 0.0
        }}
      ],
      "theme_prior_boost": 0.0,
      "theme_adjusted_lore_prior": 0.0,
      "theme_adjusted_recommended_action": "needs_author_review|keep_lore_candidate|demote_meta|mark_generic|review_alias|no_action",
      "theme_adjusted_recommended_track": "lore_candidate|meta|mixed|ignore|unknown",
      "why_not_auto_promote": "explain why this remains a review recommendation rather than canon",
      "human_review_question": "specific question for an editor",
      "model_reasoning_summary": "one sentence summary"
    }}
  ]
}}
"""


def theme_model_row(theme: dict[str, Any]) -> dict[str, Any]:
    return {
        "theme_id": theme.get("theme_id", ""),
        "label": theme.get("label", ""),
        "theme_domain": theme.get("theme_domain", ""),
        "theme_type": theme.get("theme_type", ""),
        "status": theme.get("status", ""),
        "confidence": clamp_float(theme.get("confidence", 0.0)),
        "canon_relevance": theme.get("canon_relevance", ""),
        "description": clip_text(theme.get("description", ""), 500),
        "evidence_entities": list(theme.get("evidence_entities", []) or [])[:20],
        "positive_indicators": list(theme.get("positive_indicators", []) or [])[:20],
        "negative_indicators": list(theme.get("negative_indicators", []) or [])[:12],
        "disambiguation_notes": list(theme.get("disambiguation_notes", []) or [])[:12],
    }


def model_candidate_row(row: dict[str, Any], recommendation: dict[str, Any], candidate: dict[str, Any]) -> dict[str, Any]:
    return {
        "candidate_id": row.get("candidate_id", ""),
        "candidate_name": row.get("candidate_name", ""),
        "normalized_key": row.get("normalized_key", ""),
        "sample_texts": [clip_text(text, 300) for text in list(candidate.get("sample_texts", []) or [])[:3]],
        "source_snippet_ids": list(candidate.get("source_snippet_ids", recommendation.get("source_snippet_ids", [])) or [])[:12],
        "evidence_count": int(candidate.get("evidence_count", 0) or 0),
        "base_recommended_action": row.get("base_recommended_action", ""),
        "base_recommended_track": row.get("base_recommended_track", ""),
        "recommended_entity_type": recommendation.get("recommended_entity_type", ""),
        "externality_class": row.get("externality_class", ""),
        "base_local_lore_prior": row.get("base_local_lore_prior", 0.0),
        "base_external_reference_prior": row.get("base_external_reference_prior", 0.0),
        "deterministic_theme_matches": row.get("theme_matches", []),
        "in_world_signals": list(recommendation.get("in_world_signals", []) or [])[:8],
        "meta_signals": list(recommendation.get("meta_signals", []) or [])[:8],
        "web_findings": [
            {
                "query": finding.get("query", ""),
                "finding": clip_text(finding.get("finding", ""), 260),
                "externality_weight": finding.get("externality_weight", 0.0),
            }
            for finding in recommendation.get("web_findings", []) or []
            if isinstance(finding, dict)
        ][:5],
        "reasoning_summary": clip_text(recommendation.get("reasoning_summary", ""), 500),
        "current_human_review_question": row.get("human_review_question", ""),
    }


def apply_model_reclassification(
    row: dict[str, Any],
    annotation: dict[str, Any],
    known_themes: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    out = dict(row)
    matches = normalize_model_theme_matches(annotation.get("theme_matches", []), known_themes)
    computed_boost = round(sum(float(match.get("prior_boost", 0.0) or 0.0) for match in matches), 3)
    theme_boost = min(clamp_float(annotation.get("theme_prior_boost", computed_boost)), computed_boost) if matches else 0.0
    adjusted_prior = clamp_float(annotation.get("theme_adjusted_lore_prior", out.get("base_local_lore_prior", 0.0)))
    action = normalize_model_action(annotation.get("theme_adjusted_recommended_action"), out.get("theme_adjusted_recommended_action", "needs_author_review"))
    track = normalize_model_track(annotation.get("theme_adjusted_recommended_track"), out.get("theme_adjusted_recommended_track", "unknown"))
    if not matches and action == "keep_lore_candidate" and str(out.get("base_recommended_action", "")) in {"demote_meta", "mark_generic", "no_action"}:
        action = str(out.get("base_recommended_action", action))
        track = str(out.get("base_recommended_track", track))
        adjusted_prior = clamp_float(out.get("theme_adjusted_lore_prior", adjusted_prior))
    out.update(
        {
            "theme_matches": matches,
            "theme_prior_boost": theme_boost,
            "theme_adjusted_lore_prior": adjusted_prior,
            "theme_adjusted_recommended_action": action,
            "theme_adjusted_recommended_track": track,
            "why_not_auto_promote": clip_text(
                annotation.get("why_not_auto_promote", ""),
                500,
            )
            or "Theme match changes relevance prior only; local evidence and human review still decide canon.",
            "human_review_question": clip_text(annotation.get("human_review_question", ""), 500)
            or out.get("human_review_question", ""),
            "theme_reclassification_source": "model",
            "model_reclassification_status": "model_applied",
            "model_reasoning_summary": clip_text(annotation.get("model_reasoning_summary", ""), 500),
        }
    )
    return out


def normalize_model_theme_matches(raw_matches: Any, known_themes: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    if not isinstance(raw_matches, list):
        return []
    out: list[dict[str, Any]] = []
    for item in raw_matches:
        if not isinstance(item, dict):
            continue
        raw_theme_id = str(item.get("theme_id", "")).strip()
        raw_label = normalized_name_key(str(item.get("label", "")))
        theme = known_themes.get(raw_theme_id) or known_themes.get(raw_label)
        if not theme:
            continue
        theme_id = str(theme.get("theme_id", raw_theme_id)).strip()
        strength = clamp_float(item.get("match_strength", 0.0))
        prior_boost = min(0.35, clamp_float(item.get("prior_boost", 0.0)))
        if strength <= 0.0 and prior_boost <= 0.0:
            continue
        out.append(
            {
                "theme_id": theme_id,
                "label": str(theme.get("label") or item.get("label") or ""),
                "status": str(theme.get("status") or item.get("status") or ""),
                "match_strength": strength,
                "matched_indicators": coerce_text_list(item.get("matched_indicators", []), limit=12),
                "reason": clip_text(item.get("reason", ""), 500),
                "prior_boost": prior_boost,
            }
        )
    return sorted(out, key=lambda match: -float(match.get("match_strength", 0.0) or 0.0))


def known_theme_lookup(themes: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for theme in themes:
        if not isinstance(theme, dict):
            continue
        theme_id = str(theme.get("theme_id", "")).strip()
        label_key = normalized_name_key(str(theme.get("label", "")))
        if theme_id:
            out[theme_id] = theme
        if label_key:
            out[label_key] = theme
    return out


def normalize_model_action(value: Any, default: Any) -> str:
    text = str(value or "").strip()
    if text in THEME_RECLASSIFICATION_ACTIONS:
        return text
    fallback = str(default or "").strip()
    return fallback if fallback in THEME_RECLASSIFICATION_ACTIONS else "needs_author_review"


def normalize_model_track(value: Any, default: Any) -> str:
    text = str(value or "").strip()
    if text in THEME_RECLASSIFICATION_TRACKS:
        return text
    fallback = str(default or "").strip()
    return fallback if fallback in THEME_RECLASSIFICATION_TRACKS else "unknown"


def coerce_text_list(value: Any, limit: int = 12) -> list[str]:
    if not isinstance(value, list):
        return []
    out: list[str] = []
    for item in value:
        text = clip_text(item, 160)
        if text:
            out.append(text)
        if len(out) >= limit:
            break
    return out


def clip_text(value: Any, limit: int) -> str:
    text = " ".join(str(value or "").split())
    if len(text) <= limit:
        return text
    return text[: max(0, limit - 3)].rstrip() + "..."


def match_themes(recommendation: dict[str, Any], candidate: dict[str, Any], themes: list[dict[str, Any]]) -> list[dict[str, Any]]:
    text = candidate_text(recommendation, candidate)
    out: list[dict[str, Any]] = []
    for theme in themes:
        indicators = theme_indicators(theme)
        matched = sorted({indicator for indicator in indicators if indicator and indicator in text})
        if not matched:
            continue
        confidence = clamp_float(theme.get("confidence", 0.0))
        externality_bonus = 0.2 if str(recommendation.get("externality_class", "")) == "historical_or_mythological" else 0.0
        strength = clamp_float(0.35 + min(0.35, 0.08 * len(matched)) + externality_bonus + (confidence * 0.15))
        out.append(
            {
                "theme_id": theme.get("theme_id", ""),
                "label": theme.get("label", ""),
                "status": theme.get("status", ""),
                "match_strength": strength,
                "matched_indicators": matched[:10],
                "reason": f"Candidate context overlaps with the active theme '{theme.get('label', '')}'.",
                "prior_boost": round(0.18 * strength * max(0.35, confidence), 3),
            }
        )
    return sorted(out, key=lambda item: -float(item.get("match_strength", 0.0) or 0.0))


def theme_matching_allowed(recommendation: dict[str, Any], candidate: dict[str, Any]) -> bool:
    action = str(recommendation.get("recommended_action") or candidate.get("recommended_action") or "").strip()
    track = str(recommendation.get("recommended_track") or candidate.get("recommended_track") or "").strip()
    externality = str(recommendation.get("externality_class") or "").strip()
    if action in {"demote_meta", "mark_generic", "no_action"}:
        return False
    if track in {"meta", "ignore"}:
        return False
    if externality in {"external_fictional_ip", "real_world_person", "real_world_org", "generic_phrase"}:
        return False
    return True


def theme_indicators(theme: dict[str, Any]) -> set[str]:
    raw: set[str] = set()
    raw.update(token_phrases(str(theme.get("label", ""))))
    for value in theme.get("evidence_entities", []) or []:
        raw.update(token_phrases(str(value)))
    label_lower = str(theme.get("label", "")).lower()
    for key, hints in THEME_KEYWORD_HINTS.items():
        if key in label_lower:
            raw.update(hints)
    return {item for item in raw if len(item) >= 3}


def token_phrases(value: str) -> set[str]:
    lower = " ".join(value.lower().split())
    tokens = [token for token in normalized_name_key(value).split() if len(token) >= 4 and token not in THEME_INDICATOR_STOPWORDS]
    parts: set[str] = set()
    if 1 < len(tokens) <= 5:
        parts.add(" ".join(tokens))
    parts.update(tokens)
    return parts


def candidate_text(recommendation: dict[str, Any], candidate: dict[str, Any]) -> str:
    parts: list[str] = [
        str(recommendation.get("candidate_name", "")),
        str(candidate.get("candidate_name", "")),
        str(recommendation.get("reasoning_summary", "")),
        str(candidate.get("model_reasoning_summary", "")),
        " ".join(recommendation.get("in_world_signals", []) or []),
        " ".join(recommendation.get("meta_signals", []) or []),
    ]
    for finding in recommendation.get("web_findings", []) or []:
        if isinstance(finding, dict):
            parts.append(str(finding.get("finding", "")))
    return " ".join(" ".join(parts).lower().split())


def externality_penalty(recommendation: dict[str, Any]) -> float:
    externality = str(recommendation.get("externality_class", ""))
    action = str(recommendation.get("recommended_action", ""))
    if externality == "external_fictional_ip":
        return 0.4
    if externality in {"real_world_person", "real_world_org"}:
        return 0.22
    if externality == "generic_phrase":
        return 0.3
    if action == "demote_meta":
        return 0.18
    return 0.0


def adjusted_recommendation(base_action: str, recommendation: dict[str, Any], theme_matches: list[dict[str, Any]], adjusted_prior: float) -> str:
    externality = str(recommendation.get("externality_class", ""))
    if not theme_matches:
        return base_action
    if externality == "external_fictional_ip":
        return base_action
    if externality in {"historical_or_mythological", "ambiguous", "none_detected"} and adjusted_prior >= 0.55:
        return "needs_author_review"
    return base_action


def adjusted_track(recommendation: dict[str, Any], theme_matches: list[dict[str, Any]], action: str) -> str:
    if theme_matches and action == "needs_author_review" and str(recommendation.get("recommended_track", "")) != "meta":
        return "lore_candidate"
    return str(recommendation.get("recommended_track", "unknown"))


def theme_review_question(recommendation: dict[str, Any], theme_matches: list[dict[str, Any]]) -> str:
    name = str(recommendation.get("candidate_name", "") or "this candidate")
    if theme_matches:
        labels = ", ".join(str(match.get("label", "")) for match in theme_matches[:2] if str(match.get("label", "")).strip())
        return f"Does {name} belong to the established {labels} theme lane in THERIAC, or is it only an external comparison?"
    return str(recommendation.get("human_review_question", "")) or f"Should {name} be treated as lore, meta, alias evidence, or ignored?"


def candidate_key(item: dict[str, Any]) -> str:
    return normalized_name_key(str(item.get("normalized_key") or item.get("normalized_name_key") or item.get("candidate_name") or ""))


def clamp_float(value: Any) -> float:
    try:
        return round(max(0.0, min(1.0, float(value))), 3)
    except (TypeError, ValueError):
        return 0.0


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--in-entity-candidate-harvest-json", type=Path, required=True)
    parser.add_argument("--in-entity-adjudication-recommendations-json", type=Path, required=True)
    parser.add_argument("--in-theme-profile-json", type=Path, required=True)
    parser.add_argument("--out-theme-candidate-reclassification-json", type=Path, required=True)
    parser.add_argument("--in-pipeline-config-json", type=Path, default=None)
    args = parser.parse_args()
    run(
        args.in_entity_candidate_harvest_json,
        args.in_entity_adjudication_recommendations_json,
        args.in_theme_profile_json,
        args.out_theme_candidate_reclassification_json,
        args.in_pipeline_config_json,
    )


if __name__ == "__main__":
    main()
