from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any

from pipeline.common import get_logger, now_utc_iso, read_json, stable_id, write_json
from pipeline.entity_resolution import normalized_name_key


RECLASSIFICATION_SCHEMA_VERSION = 1
THEME_KEYWORD_HINTS = {
    "sumerian": {"sumerian", "sumer", "mesopotamian", "annunaki", "anunnaki", "enki", "inanna", "ninhursag", "uruk", "gilgamesh", "enkidu"},
    "greek": {"greek", "hellenic", "spartan", "sparta", "krypteia", "leonidas", "olympus"},
    "spartan": {"spartan", "sparta", "krypteia", "leonidas"},
    "biblical": {"biblical", "abrahamic", "enoch", "watchers", "nephilim", "eden", "samael", "eve", "metatron"},
    "abrahamic": {"abrahamic", "biblical", "enoch", "watchers", "eden", "samael", "eve"},
    "japanese": {"japanese", "izanami", "yomi", "kami"},
}


def run(
    in_entity_candidate_harvest_json: Path,
    in_entity_adjudication_recommendations_json: Path,
    in_theme_profile_json: Path,
    out_theme_candidate_reclassification_json: Path,
) -> None:
    logger = get_logger(__name__)
    harvest = read_json(in_entity_candidate_harvest_json) if in_entity_candidate_harvest_json.exists() else {}
    adjudication = read_json(in_entity_adjudication_recommendations_json) if in_entity_adjudication_recommendations_json.exists() else {}
    theme_profile = read_json(in_theme_profile_json) if in_theme_profile_json.exists() else {}
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
    rows.sort(key=lambda item: (-float(item.get("theme_adjusted_lore_prior", 0.0) or 0.0), str(item.get("candidate_name", "")).lower()))
    payload = {
        "schema_version": RECLASSIFICATION_SCHEMA_VERSION,
        "generated_at_utc": now_utc_iso(),
        "stage": "07D_theme_aware_candidate_reclassification",
        "inputs": {
            "entity_candidate_harvest_json": str(in_entity_candidate_harvest_json),
            "entity_adjudication_recommendations_json": str(in_entity_adjudication_recommendations_json),
            "theme_profile_json": str(in_theme_profile_json),
            "candidate_count": len(recommendations),
            "active_theme_count": len(themes),
        },
        "policy": {
            "theme_match_changes_prior_not_final_decision": True,
            "transitive_thematic_learning_not_transitive_canon": True,
            "human_review_remains_canon_gate": True,
        },
        "summary": {
            "reclassification_count": len(rows),
            "theme_matched_candidate_count": sum(1 for row in rows if row.get("theme_matches")),
        },
        "candidate_reclassifications": rows,
    }
    write_json(out_theme_candidate_reclassification_json, payload)
    logger.info(
        "Stage 07D complete: reclassifications=%d theme_matched=%d active_themes=%d",
        len(rows),
        payload["summary"]["theme_matched_candidate_count"],
        len(themes),
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
    theme_matches = match_themes(recommendation, candidate, themes)
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
    }


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


def theme_indicators(theme: dict[str, Any]) -> set[str]:
    raw: set[str] = set()
    for field in ("label", "description", "theme_type"):
        raw.update(token_phrases(str(theme.get(field, ""))))
    for field in ("positive_indicators", "evidence_entities"):
        for value in theme.get(field, []) or []:
            raw.update(token_phrases(str(value)))
    label_lower = str(theme.get("label", "")).lower()
    for key, hints in THEME_KEYWORD_HINTS.items():
        if key in label_lower:
            raw.update(hints)
    return {item for item in raw if len(item) >= 3}


def token_phrases(value: str) -> set[str]:
    lower = " ".join(value.lower().split())
    parts = {lower} if lower else set()
    parts.update(token for token in normalized_name_key(value).split() if len(token) >= 3)
    return parts


def candidate_text(recommendation: dict[str, Any], candidate: dict[str, Any]) -> str:
    parts: list[str] = [
        str(recommendation.get("candidate_name", "")),
        str(candidate.get("candidate_name", "")),
        str(recommendation.get("reasoning_summary", "")),
        str(candidate.get("model_reasoning_summary", "")),
        " ".join(recommendation.get("in_world_signals", []) or []),
        " ".join(recommendation.get("meta_signals", []) or []),
        " ".join(str(text) for text in candidate.get("sample_texts", []) or []),
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
    args = parser.parse_args()
    run(
        args.in_entity_candidate_harvest_json,
        args.in_entity_adjudication_recommendations_json,
        args.in_theme_profile_json,
        args.out_theme_candidate_reclassification_json,
    )


if __name__ == "__main__":
    main()
