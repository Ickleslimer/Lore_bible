from __future__ import annotations

import argparse
import json
import re
from collections import Counter
from pathlib import Path
from typing import Any

from pipeline.common import get_logger, now_utc_iso, read_json, read_jsonl, stable_id, write_json, write_jsonl
from pipeline.entity_resolution import load_entity_records, normalized_name_key
from pipeline.model_provider import call_model_chat, model_call_kwargs
from pipeline.stage_04_conversation_segmentation import (
    annotate_dm_pairs,
    build_coarse_windows,
    conversation_config,
    detect_self_user_id,
    load_config,
    split_window_for_model,
)
from pipeline.stage_06_snippet_extraction import META_KEYWORDS, meta_intent_hits


TASK_NAME = "stage_04r_theme_relevance_adjudication"
HARD_RETRY_TASK_NAME = "stage_04r_theme_relevance_hard_retry"
THEME_RERUN_SCHEMA_VERSION = 1

TERM_STOPWORDS = {
    "active",
    "aesthetic",
    "and",
    "apocrypha",
    "chat",
    "deity",
    "divine",
    "external",
    "generic",
    "historical",
    "history",
    "lineage",
    "lore",
    "motif",
    "myth",
    "mythological",
    "mythology",
    "name",
    "only",
    "project",
    "reference",
    "symbolic",
    "theme",
    "the",
    "theriac",
}

AUTHORIAL_LANGUAGE = (
    "canon",
    "character",
    "design",
    "diegetic",
    "faction",
    "lore",
    "motif",
    "mythology",
    "quest",
    "story",
    "worldbuilding",
    "writing",
)

PRODUCTION_ONLY_TERMS = (
    "call",
    "deadline",
    "invoice",
    "meeting",
    "pay",
    "payment",
    "salary",
    "schedule",
    "standup",
)

EXTERNALITY_PENALTY_CLASSES = {"external_fictional_ip", "real_world_person", "real_world_org", "generic_phrase"}


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _unit(value: Any, default: float = 0.0) -> float:
    return max(0.0, min(1.0, _safe_float(value, default)))


def _list_strings(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    out: list[str] = []
    seen: set[str] = set()
    for item in value:
        text = str(item or "").strip()
        if text and text not in seen:
            out.append(text)
            seen.add(text)
    return out


def theme_aware_rerun_config(provider_config: dict[str, Any]) -> dict[str, Any]:
    raw = provider_config.get("theme_aware_rerun", {}) if isinstance(provider_config, dict) else {}
    raw_cfg = raw if isinstance(raw, dict) else {}
    cfg: dict[str, Any] = {
        "enabled": False,
        "max_iterations": 1,
        "rerun_only_previous_rejects": True,
        "require_active_theme": True,
        "require_human_approval_for_new_theme_use": True,
        "min_rescue_confidence": 0.72,
        "prefilter_min_score": 0.32,
        "model_adjudication_enabled": True,
        "fallback_to_heuristic_on_model_failure": True,
        "max_candidate_windows_per_run": 500,
        "max_rescued_conversations_per_run": 100,
        "max_windows_per_model_call": 8,
    }
    cfg.update(raw_cfg)
    cfg["enabled"] = bool(cfg.get("enabled", False))
    cfg["max_iterations"] = max(0, int(cfg.get("max_iterations", 1) or 0))
    cfg["min_rescue_confidence"] = _unit(cfg.get("min_rescue_confidence", 0.72), 0.72)
    cfg["prefilter_min_score"] = _unit(cfg.get("prefilter_min_score", 0.32), 0.32)
    cfg["max_candidate_windows_per_run"] = max(0, int(cfg.get("max_candidate_windows_per_run", 500) or 0))
    cfg["max_rescued_conversations_per_run"] = max(0, int(cfg.get("max_rescued_conversations_per_run", 100) or 0))
    cfg["max_windows_per_model_call"] = max(1, int(cfg.get("max_windows_per_model_call", 8) or 8))
    return cfg


def active_approved_themes(theme_profile: dict[str, Any]) -> list[dict[str, Any]]:
    themes = theme_profile.get("themes", []) if isinstance(theme_profile, dict) else []
    out: list[dict[str, Any]] = []
    for theme in themes if isinstance(themes, list) else []:
        if not isinstance(theme, dict):
            continue
        status = str(theme.get("status") or "").strip().lower()
        canon_relevance = str(theme.get("canon_relevance") or "").strip().lower()
        if status != "active":
            continue
        if canon_relevance in {"rejected", "meta_only"}:
            continue
        out.append(theme)
    return out


def _term_candidates(value: str) -> list[str]:
    text = re.sub(r"\s+", " ", str(value or "")).strip()
    if not text:
        return []
    terms = [text]
    for token in re.findall(r"[A-Za-z][A-Za-z0-9'-]{2,}", text):
        lower = token.lower().strip("'")
        if len(lower) >= 4 and lower not in TERM_STOPWORDS:
            terms.append(token)
    return terms


def _theme_terms(theme: dict[str, Any]) -> list[str]:
    values: list[str] = []
    values.append(str(theme.get("label") or ""))
    values.extend(_list_strings(theme.get("evidence_entities", [])))
    values.extend(_list_strings(theme.get("positive_indicators", [])))
    values.extend(_list_strings(theme.get("related_themes", [])))
    terms: list[str] = []
    seen: set[str] = set()
    for value in values:
        for term in _term_candidates(value):
            key = normalized_name_key(term)
            if len(key) < 3 or key in TERM_STOPWORDS or key in seen:
                continue
            seen.add(key)
            terms.append(term)
    return terms


def _text_for_rows(rows: list[dict[str, Any]], limit: int = 30000) -> str:
    parts: list[str] = []
    total = 0
    for row in rows:
        text = str(row.get("content_normalized") or row.get("content_raw") or "").strip()
        if not text:
            continue
        if total + len(text) > limit:
            remaining = max(0, limit - total)
            if remaining:
                parts.append(text[:remaining])
            break
        parts.append(text)
        total += len(text)
    return "\n".join(parts)


def _matched_terms(text: str, terms: list[str]) -> list[str]:
    lowered = text.lower()
    normalized_blob = f" {normalized_name_key(text)} "
    matches: list[str] = []
    seen: set[str] = set()
    for term in terms:
        clean = str(term or "").strip()
        key = normalized_name_key(clean)
        if not key or key in seen:
            continue
        if " " in key:
            found = key in normalized_blob
        else:
            found = re.search(rf"\b{re.escape(clean.lower())}\b", lowered) is not None
        if found:
            matches.append(clean)
            seen.add(key)
    return matches


def _known_lore_entities(path: Path | None) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for entity in load_entity_records(path):
        name = str(entity.get("canonical_name") or "").strip()
        aliases = _list_strings(entity.get("aliases", []))
        terms = [name, *aliases]
        terms = [term for term in terms if len(normalized_name_key(term)) >= 3]
        if not name or not terms:
            continue
        out.append(
            {
                "entity_id": str(entity.get("entity_id") or ""),
                "canonical_name": name,
                "entity_type": str(entity.get("entity_type") or ""),
                "terms": terms,
            }
        )
    return out


def _externality_terms(path: Path | None) -> list[dict[str, Any]]:
    if path is None or not path.exists():
        return []
    try:
        payload = read_json(path)
    except Exception:
        return []
    entries = payload.get("entries", {}) if isinstance(payload, dict) else {}
    out: list[dict[str, Any]] = []
    for key, entry in entries.items() if isinstance(entries, dict) else []:
        if not isinstance(entry, dict):
            continue
        recommendation = entry.get("recommendation", {}) if isinstance(entry.get("recommendation"), dict) else {}
        externality_class = str(recommendation.get("externality_class") or "").strip()
        if externality_class not in EXTERNALITY_PENALTY_CLASSES:
            continue
        terms = [
            str(entry.get("candidate_name") or ""),
            str(recommendation.get("candidate_name") or ""),
            str(key or ""),
        ]
        out.append(
            {
                "term": next((term for term in terms if normalized_name_key(term)), str(key)),
                "externality_class": externality_class,
                "reasoning_summary": str(recommendation.get("reasoning_summary") or ""),
            }
        )
    return out


def _accepted_message_ids(path: Path | None) -> set[str]:
    if path is None or not path.exists():
        return set()
    try:
        payload = read_json(path)
    except Exception:
        return set()
    accepted: set[str] = set()
    for segment in payload.get("segments", []) if isinstance(payload, dict) else []:
        if not isinstance(segment, dict):
            continue
        for message_id in segment.get("message_ids", []) or []:
            if str(message_id).strip():
                accepted.add(str(message_id).strip())
    return accepted


def _accepted_windows_by_pair(path: Path | None) -> dict[str, list[dict[str, Any]]]:
    if path is None or not path.exists():
        return {}
    try:
        payload = read_json(path)
    except Exception:
        return {}
    out: dict[str, list[dict[str, Any]]] = {}
    for segment in payload.get("segments", []) if isinstance(payload, dict) else []:
        if not isinstance(segment, dict):
            continue
        dm_pair_id = str(segment.get("dm_pair_id") or "").strip()
        if dm_pair_id:
            out.setdefault(dm_pair_id, []).append(segment)
    return out


def _has_authorial_language(text: str) -> bool:
    lowered = text.lower()
    return meta_intent_hits(text) > 0 or any(term in lowered for term in AUTHORIAL_LANGUAGE)


def _production_only_penalty(text: str, has_theme_or_entity: bool) -> float:
    lowered = text.lower()
    if has_theme_or_entity:
        return 0.0
    if any(term in lowered for term in META_KEYWORDS) or any(term in lowered for term in PRODUCTION_ONLY_TERMS):
        return 0.18
    return 0.0


def _proximity_bonus(window: dict[str, Any], accepted_by_pair: dict[str, list[dict[str, Any]]]) -> float:
    dm_pair_id = str(window.get("dm_pair_id") or "")
    if not dm_pair_id or dm_pair_id not in accepted_by_pair:
        return 0.0
    start = str(window.get("timestamp_start_utc") or "")
    end = str(window.get("timestamp_end_utc") or "")
    for segment in accepted_by_pair.get(dm_pair_id, []):
        seg_start = str(segment.get("timestamp_start_utc") or "")
        seg_end = str(segment.get("timestamp_end_utc") or "")
        if (seg_start and end and seg_start >= end) or (seg_end and start and seg_end <= start):
            return 0.08
    return 0.05


def _score_window(
    window: dict[str, Any],
    themes: list[dict[str, Any]],
    known_entities: list[dict[str, Any]],
    externality_terms: list[dict[str, Any]],
    accepted_by_pair: dict[str, list[dict[str, Any]]],
    cfg: dict[str, Any],
) -> dict[str, Any]:
    text = _text_for_rows(list(window.get("rows", [])))
    matched_themes: list[dict[str, Any]] = []
    known_links: list[dict[str, Any]] = []
    warnings: list[dict[str, Any]] = []

    for theme in themes:
        terms = _theme_terms(theme)
        matches = _matched_terms(text, terms)
        if not matches:
            continue
        match_strength = min(1.0, 0.32 + (0.18 * min(len(matches), 4)))
        matched_themes.append(
            {
                "theme_id": str(theme.get("theme_id") or ""),
                "theme_label": str(theme.get("label") or theme.get("theme_id") or ""),
                "theme_type": str(theme.get("theme_type") or ""),
                "match_strength": round(match_strength, 3),
                "matched_terms": matches[:12],
            }
        )

    for entity in known_entities:
        matches = _matched_terms(text, entity["terms"])
        if matches:
            known_links.append(
                {
                    "entity_id": entity["entity_id"],
                    "entity_name": entity["canonical_name"],
                    "entity_type": entity["entity_type"],
                    "relation": "direct_mention_or_alias",
                    "matched_terms": matches[:8],
                }
            )

    for warning in externality_terms:
        matches = _matched_terms(text, [str(warning.get("term") or "")])
        if matches:
            warnings.append(
                {
                    "term": str(warning.get("term") or ""),
                    "externality_class": str(warning.get("externality_class") or ""),
                    "reasoning_summary": str(warning.get("reasoning_summary") or ""),
                }
            )

    has_theme = bool(matched_themes)
    has_entity = bool(known_links)
    theme_score = min(0.36, sum(float(item.get("match_strength", 0.0)) for item in matched_themes) * 0.18)
    entity_score = 0.34 if has_entity else 0.0
    repeated_motif_score = 0.10 if sum(len(item.get("matched_terms", [])) for item in matched_themes) >= 2 else 0.0
    authorial_score = 0.12 if _has_authorial_language(text) else 0.0
    proximity_score = _proximity_bonus(window, accepted_by_pair)
    externality_penalty = 0.16 if warnings and not has_entity else 0.0
    generic_penalty = 0.10 if has_theme and not has_entity and not authorial_score else 0.0
    production_penalty = _production_only_penalty(text, has_theme or has_entity)
    score = max(
        0.0,
        min(
            1.0,
            entity_score
            + theme_score
            + repeated_motif_score
            + authorial_score
            + proximity_score
            - externality_penalty
            - generic_penalty
            - production_penalty,
        ),
    )
    if bool(cfg.get("require_active_theme", True)) and not (has_theme or has_entity):
        score = min(score, 0.18)
    return {
        "candidate_id": stable_id("theme_rescue_candidate", str(window.get("coarse_window_id", "")), str(window.get("model_window_id", ""))),
        "source_coarse_window_id": str(window.get("coarse_window_id", "")),
        "source_model_window_id": str(window.get("model_window_id", window.get("coarse_window_id", ""))),
        "dm_pair_id": str(window.get("dm_pair_id", "")),
        "partner_id": str(window.get("partner_id", "unknown")),
        "partner_label": str(window.get("partner_label", "unknown")),
        "participant_ids": window.get("participant_ids", []),
        "participant_labels": window.get("participant_labels", {}),
        "message_ids": [str(row.get("message_id", "")) for row in window.get("rows", [])],
        "timestamp_start_utc": window.get("timestamp_start_utc"),
        "timestamp_end_utc": window.get("timestamp_end_utc"),
        "message_count": int(window.get("message_count", 0) or 0),
        "theme_relevance_score": round(score, 3),
        "matched_themes": matched_themes,
        "known_entity_links": known_links,
        "externality_warnings": warnings,
        "heuristic_components": {
            "direct_known_entity": round(entity_score, 3),
            "active_theme_indicators": round(theme_score, 3),
            "repeated_project_motifs": round(repeated_motif_score, 3),
            "authorial_or_development_language": round(authorial_score, 3),
            "proximity_to_accepted_lore": round(proximity_score, 3),
            "externality_penalty": round(externality_penalty, 3),
            "generic_theme_penalty": round(generic_penalty, 3),
            "production_only_penalty": round(production_penalty, 3),
        },
        "text_preview": text[:1600],
    }


def _candidate_prompt_rows(candidates: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for candidate in candidates:
        rows.append(
            {
                "candidate_id": candidate["candidate_id"],
                "dm_pair_id": candidate["dm_pair_id"],
                "partner_label": candidate["partner_label"],
                "timestamp_start_utc": candidate["timestamp_start_utc"],
                "timestamp_end_utc": candidate["timestamp_end_utc"],
                "message_count": candidate["message_count"],
                "heuristic_theme_relevance_score": candidate["theme_relevance_score"],
                "matched_themes": candidate["matched_themes"],
                "known_entity_links": candidate["known_entity_links"],
                "externality_warnings": candidate["externality_warnings"],
                "messages_preview": candidate["text_preview"],
            }
        )
    return rows


def build_adjudication_prompt(candidates: list[dict[str, Any]], cfg: dict[str, Any]) -> str:
    return f"""You are running THERIAC Stage 04R: Theme-Aware Relevance Rerun.
Return strict JSON only with no markdown.

You are not deciding canon.
You are deciding whether a previously rejected or ignored conversation deserves a second pass for snippet extraction.

Guardrails:
- Use approved active themes as relevance priors only.
- Do not introduce, infer, or approve a new theme.
- Do not rescue a conversation merely because it contains a mythological, historical, scientific, or technological term.
- Rescue only if the term appears in a context plausibly connected to the THERIAC project, an approved active theme, or a known lore entity.
- Prefer false negatives over flooding the pipeline with unrelated material.
- External IP only, generic mythology/history chat, production-only/team-only discussion, and no relation to accepted lore themes should stay rejected.

Minimum rescue confidence: {cfg.get("min_rescue_confidence", 0.72)}

Candidate missed conversations:
{json.dumps(_candidate_prompt_rows(candidates), ensure_ascii=False, indent=2)}

Return JSON object:
{{
  "decisions": [
    {{
      "candidate_id": "id from input",
      "rerun_decision": "rescue_for_snippet_extraction|keep_rejected",
      "confidence": 0.0,
      "matched_theme_ids": ["approved active theme ids from input only"],
      "reasoning_summary": "brief reason",
      "requires_human_review": true
    }}
  ]
}}
"""


def _normalize_model_decisions(payload: Any, candidate_ids: set[str]) -> dict[str, dict[str, Any]]:
    if not isinstance(payload, dict):
        return {}
    decisions = payload.get("decisions")
    if not isinstance(decisions, list):
        return {}
    out: dict[str, dict[str, Any]] = {}
    for row in decisions:
        if not isinstance(row, dict):
            continue
        candidate_id = str(row.get("candidate_id") or "").strip()
        if candidate_id not in candidate_ids:
            continue
        decision = str(row.get("rerun_decision") or "").strip().lower()
        if decision not in {"rescue_for_snippet_extraction", "keep_rejected"}:
            continue
        out[candidate_id] = {
            "rerun_decision": decision,
            "confidence": _unit(row.get("confidence", 0.0)),
            "matched_theme_ids": _list_strings(row.get("matched_theme_ids", [])),
            "reasoning_summary": str(row.get("reasoning_summary") or "").strip(),
            "requires_human_review": bool(row.get("requires_human_review", True)),
        }
    return out


def _adjudicate_candidates(
    candidates: list[dict[str, Any]],
    provider_config: dict[str, Any],
    cfg: dict[str, Any],
) -> tuple[dict[str, dict[str, Any]], list[dict[str, Any]], int]:
    if not candidates or not bool(cfg.get("model_adjudication_enabled", True)):
        return {}, [], 0
    logger = get_logger(__name__)
    failures: list[dict[str, Any]] = []
    decisions: dict[str, dict[str, Any]] = {}
    call_count = 0
    chunk_size = int(cfg.get("max_windows_per_model_call", 8))
    for offset in range(0, len(candidates), chunk_size):
        chunk = candidates[offset : offset + chunk_size]
        candidate_ids = {str(candidate.get("candidate_id")) for candidate in chunk}
        prompt = build_adjudication_prompt(chunk, cfg)
        kwargs = model_call_kwargs(provider_config, TASK_NAME)
        try:
            payload = call_model_chat(prompt=prompt, **kwargs)
            call_count += 1
        except Exception as exc:
            payload = None
            failures.append({"candidate_ids": sorted(candidate_ids), "reason": f"model_call_failed: {exc}"})
        normalized = _normalize_model_decisions(payload, candidate_ids)
        missing = candidate_ids - set(normalized)
        if missing and bool(cfg.get("hard_retry_enabled", False)):
            retry_prompt = build_adjudication_prompt([candidate for candidate in chunk if candidate["candidate_id"] in missing], cfg)
            retry_kwargs = model_call_kwargs(provider_config, HARD_RETRY_TASK_NAME)
            try:
                retry_payload = call_model_chat(prompt=retry_prompt, **retry_kwargs)
                call_count += 1
                normalized.update(_normalize_model_decisions(retry_payload, missing))
            except Exception as exc:
                failures.append({"candidate_ids": sorted(missing), "reason": f"hard_retry_failed: {exc}"})
        if missing - set(normalized):
            failures.append({"candidate_ids": sorted(missing - set(normalized)), "reason": "missing_or_invalid_model_decision"})
        decisions.update(normalized)
        logger.info(
            "Stage 04R adjudication chunk %d-%d/%d: decisions=%d failures=%d",
            offset + 1,
            min(offset + len(chunk), len(candidates)),
            len(candidates),
            len(normalized),
            len(failures),
        )
    return decisions, failures, call_count


def _fallback_reason(candidate: dict[str, Any], min_confidence: float) -> str:
    if not candidate.get("matched_themes") and not candidate.get("known_entity_links"):
        return "No approved active theme or known lore entity matched this missed conversation."
    if float(candidate.get("theme_relevance_score", 0.0) or 0.0) < min_confidence:
        return "Theme-aware heuristic score is below the rescue threshold."
    return "Approved active theme or known entity signals justify a second snippet-extraction pass."


def _finalize_decisions(
    candidates: list[dict[str, Any]],
    model_decisions: dict[str, dict[str, Any]],
    cfg: dict[str, Any],
) -> list[dict[str, Any]]:
    min_confidence = float(cfg["min_rescue_confidence"])
    out: list[dict[str, Any]] = []
    for candidate in candidates:
        model = model_decisions.get(str(candidate.get("candidate_id")))
        if model:
            decision = model["rerun_decision"]
            confidence = max(_unit(model.get("confidence", 0.0)), float(candidate.get("theme_relevance_score", 0.0) or 0.0) if decision == "rescue_for_snippet_extraction" else _unit(model.get("confidence", 0.0)))
            reasoning = model.get("reasoning_summary") or _fallback_reason(candidate, min_confidence)
            requires_human_review = bool(model.get("requires_human_review", True))
        else:
            confidence = float(candidate.get("theme_relevance_score", 0.0) or 0.0)
            decision = "rescue_for_snippet_extraction" if confidence >= min_confidence else "keep_rejected"
            reasoning = _fallback_reason(candidate, min_confidence)
            requires_human_review = True
        if decision == "rescue_for_snippet_extraction" and confidence < min_confidence:
            decision = "keep_rejected"
            reasoning = f"Model suggested rescue, but confidence {confidence:.2f} is below configured threshold {min_confidence:.2f}."
        out.append(
            {
                **candidate,
                "conversation_id": stable_id("theme_rescue_conversation", candidate["source_model_window_id"], ",".join(candidate["message_ids"])),
                "rerun_decision": decision,
                "confidence": round(confidence, 3),
                "reasoning_summary": reasoning,
                "recommended_next_stage": "stage_06r_snippet_extraction" if decision == "rescue_for_snippet_extraction" else None,
                "requires_human_review": requires_human_review if decision == "rescue_for_snippet_extraction" else False,
            }
        )
    out.sort(
        key=lambda item: (
            item.get("rerun_decision") != "rescue_for_snippet_extraction",
            -float(item.get("confidence", 0.0) or 0.0),
            str(item.get("timestamp_start_utc") or ""),
        )
    )
    return out


def _rescue_anchor_entities(decision: dict[str, Any]) -> list[str]:
    values: list[str] = []
    values.extend(str(link.get("entity_name") or "") for link in decision.get("known_entity_links", []) if isinstance(link, dict))
    values.extend(str(theme.get("theme_label") or "") for theme in decision.get("matched_themes", []) if isinstance(theme, dict))
    values.extend(
        str(term)
        for theme in decision.get("matched_themes", [])
        if isinstance(theme, dict)
        for term in theme.get("matched_terms", []) or []
    )
    out: list[str] = []
    seen: set[str] = set()
    for value in values:
        text = str(value or "").strip()
        if text and text not in seen:
            out.append(text)
            seen.add(text)
    return out[:20]


def _materialize_rescue_rows(rows_by_id: dict[str, dict[str, Any]], decisions: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    rescued = [decision for decision in decisions if decision.get("rerun_decision") == "rescue_for_snippet_extraction"]
    for decision in rescued:
        message_ids = [str(message_id) for message_id in decision.get("message_ids", []) if str(message_id) in rows_by_id]
        anchors = _rescue_anchor_entities(decision)
        theme_labels = [str(theme.get("theme_label") or "") for theme in decision.get("matched_themes", []) if isinstance(theme, dict)]
        topic_label = "Theme rescue"
        if theme_labels:
            topic_label = f"Theme rescue: {theme_labels[0]}"
        for index, message_id in enumerate(message_ids, start=1):
            key = (str(decision.get("conversation_id")), message_id)
            if key in seen:
                continue
            seen.add(key)
            row = dict(rows_by_id[message_id])
            row["conversation_id"] = str(decision.get("conversation_id"))
            row["dm_pair_id"] = str(decision.get("dm_pair_id"))
            row["conversation_message_index"] = index
            row["conversation_topic_label"] = topic_label
            row["conversation_topic_summary"] = str(decision.get("reasoning_summary") or "")
            row["conversation_track"] = "lore"
            row["conversation_anchor_entities"] = anchors
            row["conversation_relevance_type"] = "theme_aware_rescue"
            row["conversation_relevance_rationale"] = str(decision.get("reasoning_summary") or "")
            row["conversation_relevance_confidence"] = float(decision.get("confidence", 0.0) or 0.0)
            row["conversation_model_confidence"] = float(decision.get("confidence", 0.0) or 0.0)
            row["conversation_source_model_window_id"] = str(decision.get("source_model_window_id") or "")
            row["conversation_rescue_source"] = "stage_04r_theme_relevance_rerun"
            row["conversation_rescue_requires_human_review"] = bool(decision.get("requires_human_review", True))
            row["conversation_rescue_matched_themes"] = decision.get("matched_themes", [])
            row["conversation_rescue_known_entity_links"] = decision.get("known_entity_links", [])
            row["conversation_rescue_externality_warnings"] = decision.get("externality_warnings", [])
            row["conversation_rescue_reasoning_summary"] = str(decision.get("reasoning_summary") or "")
            rows.append(row)
    rows.sort(key=lambda item: (str(item.get("timestamp_utc", "")), str(item.get("conversation_id", "")), int(item.get("conversation_message_index", 0) or 0)))
    return rows


def run(
    in_global_timeline_jsonl: Path,
    in_conversation_segments_json: Path,
    in_resolved_entities_json: Path,
    in_theme_profile_json: Path,
    in_externality_cache_json: Path,
    out_rerun_json: Path,
    out_rescued_messages_jsonl: Path,
    out_rescue_segments_json: Path,
    out_failures_json: Path,
    in_pipeline_config_json: Path | None = None,
) -> None:
    logger = get_logger(__name__)
    provider_config = load_config(in_pipeline_config_json)
    rerun_cfg = theme_aware_rerun_config(provider_config)
    generated_at = now_utc_iso()
    if not rerun_cfg.get("enabled", False) or int(rerun_cfg.get("max_iterations", 1)) <= 0:
        payload = {
            "schema_version": THEME_RERUN_SCHEMA_VERSION,
            "generated_at_utc": generated_at,
            "status": "disabled",
            "stage": "04R_theme_aware_relevance_rerun",
            "policy": rerun_cfg,
            "summary": {"candidate_window_count": 0, "rescued_conversation_count": 0, "model_call_count": 0},
            "decisions": [],
        }
        write_json(out_rerun_json, payload)
        write_jsonl(out_rescued_messages_jsonl, [])
        write_json(out_rescue_segments_json, {"generated_at_utc": generated_at, "status": "disabled", "segments": []})
        write_json(out_failures_json, {"generated_at_utc": generated_at, "status": "disabled", "failures": []})
        logger.info("Stage 04R skipped: theme-aware rerun is disabled.")
        return

    theme_profile = read_json(in_theme_profile_json) if in_theme_profile_json.exists() else {}
    themes = active_approved_themes(theme_profile)
    if not themes:
        payload = {
            "schema_version": THEME_RERUN_SCHEMA_VERSION,
            "generated_at_utc": generated_at,
            "status": "no_active_themes",
            "stage": "04R_theme_aware_relevance_rerun",
            "policy": rerun_cfg,
            "summary": {"active_theme_count": 0, "candidate_window_count": 0, "rescued_conversation_count": 0, "model_call_count": 0},
            "decisions": [],
        }
        write_json(out_rerun_json, payload)
        write_jsonl(out_rescued_messages_jsonl, [])
        write_json(out_rescue_segments_json, {"generated_at_utc": generated_at, "status": "no_active_themes", "segments": []})
        write_json(out_failures_json, {"generated_at_utc": generated_at, "status": "no_active_themes", "failures": []})
        logger.info("Stage 04R skipped: no approved active themes are available.")
        return

    conversation_cfg = conversation_config(provider_config)
    rows = read_jsonl(in_global_timeline_jsonl)
    self_user_id = detect_self_user_id(rows, str(conversation_cfg.get("self_user_id", "")))
    annotated_rows = annotate_dm_pairs(rows, self_user_id)
    rows_by_id = {str(row.get("message_id", "")): row for row in annotated_rows if str(row.get("message_id", "")).strip()}
    accepted_ids = _accepted_message_ids(in_conversation_segments_json)
    accepted_by_pair = _accepted_windows_by_pair(in_conversation_segments_json)
    ignored_rows = [row for row in annotated_rows if str(row.get("message_id", "")) not in accepted_ids]
    coarse_windows = build_coarse_windows(ignored_rows, float(conversation_cfg.get("max_gap_hours", 12)))
    known_entities = _known_lore_entities(in_resolved_entities_json)
    externality = _externality_terms(in_externality_cache_json)

    candidates: list[dict[str, Any]] = []
    dropped_by_prefilter = 0
    max_candidate_windows = int(rerun_cfg["max_candidate_windows_per_run"])
    for coarse_window in coarse_windows:
        model_chunks = split_window_for_model(
            coarse_window,
            int(conversation_cfg.get("model_window_max_messages", 80)),
            int(conversation_cfg.get("model_window_max_chars", 14000)),
        )
        for model_window in model_chunks:
            scored = _score_window(model_window, themes, known_entities, externality, accepted_by_pair, rerun_cfg)
            if float(scored.get("theme_relevance_score", 0.0) or 0.0) < float(rerun_cfg["prefilter_min_score"]):
                dropped_by_prefilter += 1
                continue
            if bool(rerun_cfg.get("require_active_theme", True)) and not (scored.get("matched_themes") or scored.get("known_entity_links")):
                dropped_by_prefilter += 1
                continue
            candidates.append(scored)
            if max_candidate_windows and len(candidates) >= max_candidate_windows:
                break
        if max_candidate_windows and len(candidates) >= max_candidate_windows:
            break

    candidates.sort(key=lambda item: (-float(item.get("theme_relevance_score", 0.0) or 0.0), str(item.get("timestamp_start_utc", ""))))
    model_decisions, failures, model_call_count = _adjudicate_candidates(candidates, provider_config, rerun_cfg)
    decisions = _finalize_decisions(candidates, model_decisions, rerun_cfg)
    rescued = [decision for decision in decisions if decision.get("rerun_decision") == "rescue_for_snippet_extraction"]
    max_rescued = int(rerun_cfg["max_rescued_conversations_per_run"])
    if max_rescued and len(rescued) > max_rescued:
        allowed_ids = {str(decision.get("candidate_id")) for decision in rescued[:max_rescued]}
        limited: list[dict[str, Any]] = []
        for decision in decisions:
            if decision.get("rerun_decision") == "rescue_for_snippet_extraction" and str(decision.get("candidate_id")) not in allowed_ids:
                limited.append(
                    {
                        **decision,
                        "rerun_decision": "keep_rejected",
                        "recommended_next_stage": None,
                        "requires_human_review": False,
                        "reasoning_summary": "Rescue candidate exceeded max_rescued_conversations_per_run.",
                    }
                )
            else:
                limited.append(decision)
        decisions = limited
        rescued = [decision for decision in decisions if decision.get("rerun_decision") == "rescue_for_snippet_extraction"]

    rescue_rows = _materialize_rescue_rows(rows_by_id, decisions)
    rescue_segments = [
        {
            "conversation_id": decision["conversation_id"],
            "dm_pair_id": decision["dm_pair_id"],
            "partner_id": decision["partner_id"],
            "partner_label": decision["partner_label"],
            "participant_ids": decision.get("participant_ids", []),
            "participant_labels": decision.get("participant_labels", {}),
            "track": "lore",
            "topic_label": f"Theme rescue: {decision['matched_themes'][0]['theme_label']}" if decision.get("matched_themes") else "Theme rescue",
            "topic_summary": decision.get("reasoning_summary", ""),
            "topic_shift_reason": "Previously ignored by strict Stage 04; rescued by approved active theme or known entity signal.",
            "anchor_entities": _rescue_anchor_entities(decision),
            "relevance_type": "theme_aware_rescue",
            "relevance_rationale": decision.get("reasoning_summary", ""),
            "relevance_confidence": decision.get("confidence", 0.0),
            "message_ids": decision.get("message_ids", []),
            "timestamp_start_utc": decision.get("timestamp_start_utc"),
            "timestamp_end_utc": decision.get("timestamp_end_utc"),
            "message_count": decision.get("message_count", 0),
            "model_confidence": decision.get("confidence", 0.0),
            "source_coarse_window_id": decision.get("source_coarse_window_id", ""),
            "source_model_window_id": decision.get("source_model_window_id", ""),
            "matched_themes": decision.get("matched_themes", []),
            "known_entity_links": decision.get("known_entity_links", []),
            "externality_warnings": decision.get("externality_warnings", []),
        }
        for decision in rescued
    ]
    decision_counts = Counter(str(decision.get("rerun_decision", "unknown")) for decision in decisions)
    payload = {
        "schema_version": THEME_RERUN_SCHEMA_VERSION,
        "generated_at_utc": generated_at,
        "status": "complete",
        "stage": "04R_theme_aware_relevance_rerun",
        "policy": rerun_cfg,
        "inputs": {
            "global_timeline_jsonl": str(in_global_timeline_jsonl),
            "conversation_segments_json": str(in_conversation_segments_json),
            "resolved_entities_json": str(in_resolved_entities_json),
            "theme_profile_json": str(in_theme_profile_json),
            "externality_cache_json": str(in_externality_cache_json),
        },
        "summary": {
            "active_theme_count": len(themes),
            "known_lore_entity_count": len(known_entities),
            "messages_in": len(rows),
            "accepted_strict_message_count": len(accepted_ids),
            "ignored_message_count": len(ignored_rows),
            "ignored_coarse_window_count": len(coarse_windows),
            "candidate_window_count": len(candidates),
            "dropped_by_prefilter_count": dropped_by_prefilter,
            "rescued_conversation_count": len(rescued),
            "rescued_message_count": len(rescue_rows),
            "model_call_count": model_call_count,
            "failure_count": len(failures),
            "decision_counts": dict(sorted(decision_counts.items())),
        },
        "decisions": decisions,
    }
    write_json(out_rerun_json, payload)
    write_jsonl(out_rescued_messages_jsonl, rescue_rows)
    write_json(out_rescue_segments_json, {"generated_at_utc": generated_at, "status": "complete", "segments": rescue_segments})
    write_json(out_failures_json, {"generated_at_utc": generated_at, "status": "complete", "failures": failures})
    logger.info(
        "Stage 04R complete: active_themes=%d candidates=%d rescued=%d messages=%d failures=%d",
        len(themes),
        len(candidates),
        len(rescued),
        len(rescue_rows),
        len(failures),
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--in-global-timeline-jsonl", type=Path, required=True)
    parser.add_argument("--in-conversation-segments-json", type=Path, required=True)
    parser.add_argument("--in-resolved-entities-json", type=Path, required=True)
    parser.add_argument("--in-theme-profile-json", type=Path, required=True)
    parser.add_argument("--in-externality-cache-json", type=Path, required=True)
    parser.add_argument("--out-rerun-json", type=Path, required=True)
    parser.add_argument("--out-rescued-messages-jsonl", type=Path, required=True)
    parser.add_argument("--out-rescue-segments-json", type=Path, required=True)
    parser.add_argument("--out-failures-json", type=Path, required=True)
    parser.add_argument("--in-pipeline-config-json", type=Path, required=False, default=None)
    args = parser.parse_args()
    run(
        args.in_global_timeline_jsonl,
        args.in_conversation_segments_json,
        args.in_resolved_entities_json,
        args.in_theme_profile_json,
        args.in_externality_cache_json,
        args.out_rerun_json,
        args.out_rescued_messages_jsonl,
        args.out_rescue_segments_json,
        args.out_failures_json,
        args.in_pipeline_config_json,
    )


if __name__ == "__main__":
    main()
