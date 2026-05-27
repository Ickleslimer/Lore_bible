"""Family-aware validation for lore development ledger entries."""
from __future__ import annotations

import re
from typing import Any

from pipeline.ledger_quality_metrics import PLACEHOLDER_HEADLINE_PATTERNS

HOMOGENEOUS_FAMILY = "deepseek_v4_flash"
HIGH_RISK_CHANGE_TYPES = frozenset({"canonical_name", "quest", "timeline"})
REVIEW_QUEUE_CHANGE_TYPES = HIGH_RISK_CHANGE_TYPES


def validation_rules_for_family(model_family: str, cfg: dict[str, Any]) -> dict[str, Any]:
    lane = str(model_family or "").strip().lower()
    homogeneous = lane in {HOMOGENEOUS_FAMILY, "", "homogeneous"}
    section = cfg.get("heterogeneous", {}) if isinstance(cfg.get("heterogeneous"), dict) else {}
    hom_section = cfg.get("homogeneous", {}) if isinstance(cfg.get("homogeneous"), dict) else {}
    defaults_hom = {
        "min_confidence": 0.55,
        "max_entries_per_segment": 12,
        "require_entity_id_for_new": "warn",
    }
    defaults_het = {
        "min_confidence": 0.70,
        "max_entries_per_segment": 6,
        "require_entity_id_for_new": "drop",
    }
    base = dict(defaults_het if not homogeneous else defaults_hom)
    overlay = hom_section if homogeneous else section
    base.update(overlay)
    base["homogeneous"] = homogeneous
    return base


def _normalized_headline_key(headline: str) -> str:
    return re.sub(r"\s+", " ", str(headline or "").strip().lower())


def validate_ledger_entries(
    entries: list[dict[str, Any]],
    *,
    model_family: str,
    validation_cfg: dict[str, Any],
    prior_entries: list[dict[str, Any]],
    by_name: dict[str, dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    """
    Returns (accepted, rejected_failures, review_queue_rows).
    rejected_failures are dicts suitable for lore_development_ledger_failures.
    review_queue_rows are dicts for ledger_review_queue.jsonl.
    """
    rules = validation_rules_for_family(model_family, validation_cfg)
    min_confidence = float(rules.get("min_confidence", 0.55))
    max_entries = max(1, int(rules.get("max_entries_per_segment", 12)))
    require_entity_mode = str(rules.get("require_entity_id_for_new", "warn")).strip().lower()

    prior_headlines: set[str] = set()
    for prior in prior_entries:
        key = _normalized_headline_key(str(prior.get("headline", "")))
        if key:
            prior_headlines.add(key)

    accepted: list[dict[str, Any]] = []
    rejected: list[dict[str, Any]] = []
    review_queue: list[dict[str, Any]] = []

    for entry in entries[:max_entries]:
        headline = str(entry.get("headline", "")).strip()
        reasons: list[str] = []
        warnings: list[str] = []

        confidence = float(entry.get("confidence", 0.0) or 0.0)
        if confidence < min_confidence:
            reasons.append(f"confidence {confidence} < min {min_confidence}")

        hkey = _normalized_headline_key(headline)
        if hkey and hkey in prior_headlines:
            reasons.append("duplicate_headline_in_entity_chain")

        if any(pat.search(headline) for pat in PLACEHOLDER_HEADLINE_PATTERNS):
            reasons.append("placeholder_headline_pattern")

        change_type = str(entry.get("change_type", "")).strip().lower()
        if change_type == "canonical_name":
            if not str(entry.get("before", "")).strip() or not str(entry.get("after", "")).strip():
                reasons.append("canonical_name_missing_before_or_after")

        event_kind = str(entry.get("event_kind", "")).strip().lower()
        subject_label = str(entry.get("subject_label", "")).strip()
        subject_entity_id = str(entry.get("subject_entity_id", "")).strip()
        if event_kind == "new" and not subject_entity_id and subject_label:
            name_key = re.sub(r"[^a-z0-9]+", " ", subject_label.lower()).strip()
            if name_key in by_name:
                msg = "new_event_missing_subject_entity_id_when_registry_match"
                if require_entity_mode == "drop":
                    reasons.append(msg)
                else:
                    warnings.append(msg)

        if reasons:
            rejected.append(
                {
                    "reason": "validation_rejected",
                    "validation_errors": reasons,
                    "source_segment_id": entry.get("source_segment_id"),
                    "global_sequence": entry.get("global_sequence"),
                    "headline": headline,
                    "inference_profile": entry.get("inference_profile"),
                    "inference_model_family": entry.get("inference_model_family"),
                    "entry_preview": entry,
                }
            )
            continue

        accepted.append(entry)
        if hkey:
            prior_headlines.add(hkey)

        queue_reasons: list[str] = []
        if warnings:
            queue_reasons.extend(warnings)
        if event_kind == "new":
            queue_reasons.append("first_introduction")
        if change_type in REVIEW_QUEUE_CHANGE_TYPES:
            queue_reasons.append(f"high_risk_change_type:{change_type}")
        if str(entry.get("inference_lane_tier", "")).strip().lower() == "heterogeneous":
            queue_reasons.append("heterogeneous_lane")
        if queue_reasons:
            review_queue.append(
                {
                    "entry_id": entry.get("entry_id"),
                    "source_segment_id": entry.get("source_segment_id"),
                    "global_sequence": entry.get("global_sequence"),
                    "headline": headline,
                    "inference_profile": entry.get("inference_profile"),
                    "inference_lane_tier": entry.get("inference_lane_tier"),
                    "inference_model_family": entry.get("inference_model_family"),
                    "reasons": queue_reasons,
                }
            )

    if len(entries) > max_entries:
        for overflow in entries[max_entries:]:
            rejected.append(
                {
                    "reason": "validation_rejected",
                    "validation_errors": [f"max_entries_per_segment exceeded ({max_entries})"],
                    "source_segment_id": overflow.get("source_segment_id"),
                    "global_sequence": overflow.get("global_sequence"),
                    "headline": overflow.get("headline"),
                    "entry_preview": overflow,
                }
            )

    return accepted, rejected, review_queue


def should_enqueue_review(entry: dict[str, Any], *, warnings: list[str] | None = None) -> bool:
    del warnings
    event_kind = str(entry.get("event_kind", "")).strip().lower()
    change_type = str(entry.get("change_type", "")).strip().lower()
    if event_kind == "new":
        return True
    if change_type in REVIEW_QUEUE_CHANGE_TYPES:
        return True
    if str(entry.get("inference_lane_tier", "")).strip().lower() == "heterogeneous":
        return True
    return False
