"""Quality metrics for lore development ledger entries (batch gates, calibration)."""
from __future__ import annotations

import re
from collections import Counter
from typing import Any

PLACEHOLDER_HEADLINE_PATTERNS = (
    re.compile(r"\b(TBD|TODO|placeholder|unknown entity|N/?A)\b", re.I),
    re.compile(r"^Entity — .+ — introduced as a concept$", re.I),
    re.compile(r"^Change — .+ — updated$", re.I),
)


def ledger_entry_metrics(entries: list[dict[str, Any]]) -> dict[str, Any]:
    if not entries:
        return {"count": 0}

    confidences: list[float] = []
    with_entity = 0
    with_snippets = 0
    with_messages = 0
    placeholder_hits = 0
    short_headlines = 0
    event_kinds: Counter[str] = Counter()
    change_types: Counter[str] = Counter()
    segments: set[str] = set()
    families: Counter[str] = Counter()

    for entry in entries:
        event_kinds[str(entry.get("event_kind", ""))] += 1
        change_types[str(entry.get("change_type", ""))] += 1
        segment_id = str(entry.get("source_segment_id", "")).strip()
        if segment_id:
            segments.add(segment_id)
        families[str(entry.get("inference_model_family", "") or "unknown")] += 1
        try:
            confidences.append(float(entry.get("confidence", 0.0) or 0.0))
        except (TypeError, ValueError):
            confidences.append(0.0)
        if str(entry.get("subject_entity_id", "")).strip():
            with_entity += 1
        snippets = entry.get("supporting_snippet_ids", [])
        if isinstance(snippets, list) and snippets:
            with_snippets += 1
        messages = entry.get("supporting_message_ids", [])
        if isinstance(messages, list) and messages:
            with_messages += 1
        headline = str(entry.get("headline", "")).strip()
        if len(headline) < 40:
            short_headlines += 1
        if any(pat.search(headline) for pat in PLACEHOLDER_HEADLINE_PATTERNS):
            placeholder_hits += 1

    n = len(entries)
    avg_conf = sum(confidences) / n if confidences else 0.0
    return {
        "count": n,
        "unique_segments": len(segments),
        "entries_per_segment": round(n / len(segments), 2) if segments else 0.0,
        "avg_confidence": round(avg_conf, 3),
        "pct_with_subject_entity_id": round(100.0 * with_entity / n, 1),
        "pct_with_supporting_snippets": round(100.0 * with_snippets / n, 1),
        "pct_with_supporting_messages": round(100.0 * with_messages / n, 1),
        "pct_placeholder_headlines": round(100.0 * placeholder_hits / n, 1),
        "pct_short_headlines_lt40": round(100.0 * short_headlines / n, 1),
        "event_kinds": dict(event_kinds.most_common(6)),
        "change_types": dict(change_types.most_common(8)),
        "inference_model_families": dict(families.most_common(8)),
    }


def evaluate_quality_gate(
    batch_metrics: dict[str, Any],
    baseline_metrics: dict[str, Any],
    *,
    placeholder_multiplier: float = 2.0,
    entity_id_margin_pp: float = 15.0,
    entries_per_segment_ratio: float = 0.5,
) -> tuple[bool, list[str]]:
    """Return (passed, list of failure reasons)."""
    if int(batch_metrics.get("count", 0) or 0) == 0:
        return True, []

    reasons: list[str] = []
    base_count = int(baseline_metrics.get("count", 0) or 0)
    if base_count <= 0:
        return True, []

    base_placeholder = float(baseline_metrics.get("pct_placeholder_headlines", 0.0) or 0.0)
    batch_placeholder = float(batch_metrics.get("pct_placeholder_headlines", 0.0) or 0.0)
    threshold_placeholder = max(base_placeholder * placeholder_multiplier, base_placeholder + 1.0)
    if batch_placeholder > threshold_placeholder:
        reasons.append(
            f"pct_placeholder_headlines {batch_placeholder} > threshold {threshold_placeholder:.1f}"
        )

    base_entity = float(baseline_metrics.get("pct_with_subject_entity_id", 0.0) or 0.0)
    batch_entity = float(batch_metrics.get("pct_with_subject_entity_id", 0.0) or 0.0)
    if batch_entity < base_entity - entity_id_margin_pp:
        reasons.append(
            f"pct_with_subject_entity_id {batch_entity} < baseline {base_entity} - {entity_id_margin_pp}"
        )

    base_eps = float(baseline_metrics.get("entries_per_segment", 0.0) or 0.0)
    batch_eps = float(batch_metrics.get("entries_per_segment", 0.0) or 0.0)
    if base_eps > 0 and batch_eps < base_eps * entries_per_segment_ratio:
        reasons.append(
            f"entries_per_segment {batch_eps} < baseline {base_eps} * {entries_per_segment_ratio}"
        )

    return len(reasons) == 0, reasons
