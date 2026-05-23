from __future__ import annotations

import argparse
import re
from pathlib import Path
from typing import Any

from pipeline.common import get_logger, now_utc_iso, read_json, read_jsonl, stable_id, write_json, write_jsonl
from pipeline.model_provider import build_prompt, call_model_chat, load_seed_entities
from pipeline.thematic_profile import update_runtime_profile


LORE_KEYWORDS = {
    "theriac",
    "enoch",
    "krypteia",
    "hectr",
    "ruinr",
    "mycelium wars",
    "olympus",
    "joy",
    "penemue",
    "quest",
    "immortality",
}
META_KEYWORDS = {
    "mechanic",
    "marketing",
    "campaign",
    "fundraising",
    "pitch",
    "steam",
    "devlog",
    "production",
    "roadmap",
    "audience",
}
META_INTENT_REGEXES = [
    re.compile(r"\b(social media|media push|marketing|campaign|pitch(?:ing)?|production|commission|commercial)\b"),
    re.compile(r"\b(work project|deadline|roadmap|devlog|artist|hiatus)\b"),
    re.compile(r"\b(i|we)\b.{0,24}\b(need|should|could|plan|planning|schedule|pause|ship|release)\b"),
]
SENSITIVE_REGEXES = [
    r"\bfuck\b",
    r"\bshit\b",
    r"\bslur\b",
]


def _profile_defaults_from_config(provider_config: dict[str, Any], profile_type: str) -> tuple[str, float, float, int]:
    source_defaults = provider_config.get("source_profile_defaults", {})
    profile_defaults = source_defaults.get(profile_type, {}) if isinstance(source_defaults, dict) else {}

    strictness_level = str(profile_defaults.get("strictness_level", "strict"))
    relevance_min = float(profile_defaults.get("theriac_relevance_min", 0.7))
    meta_split_min = float(profile_defaults.get("meta_lore_split_min", 0.55))
    context_window = int(profile_defaults.get("context_window_messages", 1))

    relevance_min = max(0.0, min(1.0, relevance_min))
    meta_split_min = max(0.0, min(1.0, meta_split_min))
    context_window = max(1, context_window)
    return strictness_level, relevance_min, meta_split_min, context_window


def default_profile(
    thread_id: str,
    partner_id: str,
    partner_label: str,
    provider_config: dict[str, Any],
) -> dict[str, Any]:
    profile_type = "unknown_low_signal"
    strictness_level, relevance_min, meta_split_min, context_window = _profile_defaults_from_config(
        provider_config, profile_type
    )
    return {
        "thread_id": thread_id,
        "partner_id": partner_id,
        "partner_display_name": partner_label,
        "profile_type": profile_type,
        "strictness_level": strictness_level,
        "base_thresholds": {"theriac_relevance_min": relevance_min, "meta_lore_split_min": meta_split_min},
        "context_window_messages": context_window,
        "notes": "Auto-generated default profile.",
        "last_calibrated_at": now_utc_iso(),
        "calibration_examples": [],
    }


def profile_adjustment(profile_type: str) -> float:
    if profile_type == "theriac_dedicated":
        return 0.15
    if profile_type == "mixed_topic":
        return 0.05
    return -0.05


def context_key_for_row(row: dict[str, Any]) -> str:
    return str(row.get("conversation_id") or row.get("thread_id", ""))


def source_profile_key_for_row(row: dict[str, Any]) -> str:
    return str(row.get("dm_pair_id") or row.get("thread_id", ""))


def load_patch_notes_by_conversation(path: Path | None) -> dict[str, dict[str, Any]]:
    notes = load_patch_notes(path)
    return {
        str(note.get("conversation_id", "")): note
        for note in notes
        if isinstance(note, dict) and str(note.get("conversation_id", "")).strip()
    }


def load_patch_notes(path: Path | None) -> list[dict[str, Any]]:
    if path is None or not path.exists():
        return []
    payload = read_json(path)
    notes = payload.get("notes", []) if isinstance(payload, dict) else payload if isinstance(payload, list) else []
    return [note for note in notes if isinstance(note, dict)]


def summarize_patch_items(items: Any, text_key: str, limit: int = 4) -> list[str]:
    if not isinstance(items, list):
        return []
    out: list[str] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        text = str(item.get(text_key, "")).strip()
        if text:
            out.append(text)
        if len(out) >= limit:
            break
    return out


def rows_by_message_id(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    return {str(row.get("message_id", "")): row for row in rows if str(row.get("message_id", ""))}


def rows_by_conversation(rows: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        conversation_id = str(row.get("conversation_id", "")).strip()
        if conversation_id:
            grouped.setdefault(conversation_id, []).append(row)
    for conversation_id, items in grouped.items():
        items.sort(
            key=lambda item: (
                int(item.get("conversation_message_index", 0) or 0),
                str(item.get("timestamp_utc", "")),
                str(item.get("message_id", "")),
            )
        )
    return grouped


def _stable_unique(values: list[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for value in values:
        text = str(value).strip()
        if not text or text in seen:
            continue
        seen.add(text)
        out.append(text)
    return out


def _patch_note_rows(note: dict[str, Any], rows_by_id: dict[str, dict[str, Any]], by_conversation: dict[str, list[dict[str, Any]]]) -> list[dict[str, Any]]:
    message_ids = _list_string_values(note.get("message_ids", []))
    rows = [rows_by_id[mid] for mid in message_ids if mid in rows_by_id]
    if rows:
        return rows
    return by_conversation.get(str(note.get("conversation_id", "")), [])


def _supporting_rows_for_patch_item(
    note: dict[str, Any],
    item: dict[str, Any],
    rows_by_id: dict[str, dict[str, Any]],
    by_conversation: dict[str, list[dict[str, Any]]],
) -> tuple[list[dict[str, Any]], bool]:
    requested_ids = _list_string_values(item.get("supporting_message_ids", []))
    rows = [rows_by_id[mid] for mid in requested_ids if mid in rows_by_id]
    if rows:
        return rows, len(rows) == len(requested_ids)
    fallback_rows = _patch_note_rows(note, rows_by_id, by_conversation)
    return fallback_rows, False


def _patch_item_text(item_type: str, item: dict[str, Any]) -> str:
    if item_type == "lore_development":
        names = ", ".join(_list_string_values(item.get("entity_names", [])))
        prefix = f"{names}: " if names else ""
        return f"{prefix}{str(item.get('description', '')).strip()}".strip()
    if item_type == "meta_development":
        dev_type = str(item.get("development_type", "")).strip()
        prefix = f"{dev_type}: " if dev_type else ""
        return f"{prefix}{str(item.get('description', '')).strip()}".strip()
    if item_type == "entity_update":
        entity = str(item.get("entity_name", "")).strip()
        update_type = str(item.get("update_type", "")).strip()
        prefix_parts = [part for part in (entity, update_type) if part]
        prefix = f"{' / '.join(prefix_parts)}: " if prefix_parts else ""
        return f"{prefix}{str(item.get('description', '')).strip()}".strip()
    if item_type == "relationship_update":
        source = str(item.get("source_entity", "")).strip()
        target = str(item.get("target_entity", "")).strip()
        relation = str(item.get("relationship_type", "")).strip()
        prefix = " -> ".join(part for part in (source, target) if part)
        if relation:
            prefix = f"{prefix} ({relation})" if prefix else relation
        return f"{prefix}: {str(item.get('description', '')).strip()}".strip(": ").strip()
    if item_type == "timeline_update":
        return str(item.get("description", "")).strip()
    if item_type == "open_question":
        return str(item.get("question", "")).strip()
    if item_type == "possible_contradiction":
        return str(item.get("description", "")).strip()
    return str(item.get("description", "") or item.get("question", "")).strip()


def _patch_item_candidates(item_type: str, item: dict[str, Any], note: dict[str, Any], support_rows: list[dict[str, Any]]) -> list[str]:
    candidates: list[str] = []
    if item_type == "lore_development":
        candidates.extend(_list_string_values(item.get("entity_names", [])))
    elif item_type == "entity_update":
        candidates.append(str(item.get("entity_name", "")).strip())
    elif item_type == "relationship_update":
        candidates.extend([str(item.get("source_entity", "")).strip(), str(item.get("target_entity", "")).strip()])

    if not any(str(value).strip() for value in candidates):
        candidates.extend(_list_string_values(note.get("anchor_entities", [])))
        for row in support_rows:
            candidates.extend(_list_string_values(row.get("conversation_anchor_entities", [])))
    return _stable_unique(candidates)[:12]


def _patch_item_topics(item_type: str, item: dict[str, Any]) -> list[str]:
    if item_type == "meta_development":
        dev_type = str(item.get("development_type", "")).strip().lower()
        topics = ["production"]
        if dev_type == "marketing":
            topics.extend(["marketing", "go_to_market", "audience"])
        if dev_type == "design":
            topics.append("mechanic")
        return sorted(set(topics))
    if item_type == "timeline_update":
        return ["event"]
    if item_type == "open_question":
        return ["entity", "theme"]
    if item_type == "possible_contradiction":
        return ["entity", "theme"]
    return ["entity"]


def _patch_item_track(item_type: str, note: dict[str, Any]) -> str:
    if item_type == "meta_development":
        return "meta"
    if item_type in {"open_question", "possible_contradiction"} and str(note.get("track", "")).strip().lower() == "meta":
        return "meta"
    return "lore"


def _patch_item_kind_fields(item_type: str, item: dict[str, Any]) -> dict[str, Any]:
    fields: dict[str, Any] = {
        "source_kind": f"patch_note_{item_type}",
        "patch_item_type": item_type,
        "patch_item_text": _patch_item_text(item_type, item),
        "patch_development_type": str(item.get("development_type", "")),
        "patch_update_type": str(item.get("update_type", "")),
        "patch_relationship_type": str(item.get("relationship_type", "")),
        "patch_conflicts_with_prior_patch_note_ids": _list_string_values(item.get("conflicts_with_prior_patch_note_ids", [])),
    }
    return fields


def _iter_patch_items(note: dict[str, Any]) -> list[tuple[str, int, dict[str, Any]]]:
    fields = [
        ("lore_development", "lore_developments"),
        ("meta_development", "meta_developments"),
        ("entity_update", "entity_updates"),
        ("relationship_update", "relationship_updates"),
        ("timeline_update", "timeline_updates"),
        ("open_question", "open_questions"),
        ("possible_contradiction", "possible_contradictions"),
    ]
    out: list[tuple[str, int, dict[str, Any]]] = []
    for item_type, field in fields:
        raw_items = note.get(field, [])
        if not isinstance(raw_items, list):
            continue
        for index, item in enumerate(raw_items, start=1):
            if isinstance(item, dict):
                out.append((item_type, index, item))
    return out


def materialize_patch_note_snippets(
    *,
    rows: list[dict[str, Any]],
    patch_notes: list[dict[str, Any]],
    profiles: dict[str, dict[str, Any]],
    provider_config: dict[str, Any],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], int, int]:
    rows_by_id = rows_by_message_id(rows)
    by_conversation = rows_by_conversation(rows)
    snippets: list[dict[str, Any]] = []
    review: list[dict[str, Any]] = []
    skipped_no_durable = 0
    skipped_empty_items = 0
    ordered_notes = sorted(
        patch_notes,
        key=lambda note: (
            int(note.get("global_conversation_index", 0) or 0),
            str(note.get("timestamp_start_utc", "")),
            str(note.get("conversation_id", "")),
        ),
    )

    for note in ordered_notes:
        if str(note.get("status", "")).strip().lower() == "no_durable_development":
            skipped_no_durable += 1
            continue
        note_rows = _patch_note_rows(note, rows_by_id, by_conversation)
        if not note_rows:
            skipped_empty_items += 1
            continue
        profile_key = str(note.get("dm_pair_id") or note_rows[0].get("dm_pair_id") or note_rows[0].get("thread_id", ""))
        if profile_key and profile_key not in profiles:
            profiles[profile_key] = default_profile(
                profile_key,
                str(note.get("partner_id") or note_rows[0].get("partner_id", "")),
                str(note.get("partner_label") or note_rows[0].get("partner_label", "")),
                provider_config,
            )

        for item_type, item_index, item in _iter_patch_items(note):
            support_rows, exact_support = _supporting_rows_for_patch_item(note, item, rows_by_id, by_conversation)
            if not support_rows:
                skipped_empty_items += 1
                continue
            support_rows = sorted(
                support_rows,
                key=lambda row: (
                    int(row.get("conversation_message_index", 0) or 0),
                    str(row.get("timestamp_utc", "")),
                    str(row.get("message_id", "")),
                ),
            )
            patch_item_text = _patch_item_text(item_type, item)
            if not patch_item_text:
                skipped_empty_items += 1
                continue
            message_ids = [str(row.get("message_id", "")) for row in support_rows if str(row.get("message_id", ""))]
            raw_text = join_window_text(support_rows, "content_raw")
            normalized_text = join_window_text(support_rows, "content_normalized")
            if not normalized_text:
                normalized_text = raw_text
            display_text = f"Patch note item: {patch_item_text}"
            if normalized_text:
                display_text = f"{display_text}\nSupporting messages:\n{normalized_text}"
            candidates = _patch_item_candidates(item_type, item, note, support_rows)
            confidence = _safe_confidence(item.get("confidence"), _safe_confidence(note.get("confidence"), 0.82))
            snippet = {
                "snippet_id": stable_id("snippet", str(note.get("patch_note_id", "")), item_type, str(item_index), *message_ids),
                "thread_id": str(support_rows[0].get("thread_id", "")),
                "conversation_id": str(note.get("conversation_id") or support_rows[0].get("conversation_id", "")),
                "dm_pair_id": str(note.get("dm_pair_id") or support_rows[0].get("dm_pair_id", "")),
                "conversation_topic_label": str(note.get("topic_label") or support_rows[0].get("conversation_topic_label", "")),
                "conversation_topic_summary": str(note.get("topic_summary") or support_rows[0].get("conversation_topic_summary", "")),
                "conversation_track": str(note.get("track") or support_rows[0].get("conversation_track", "")),
                "conversation_anchor_entities": _stable_unique(
                    _list_string_values(note.get("anchor_entities", []))
                    + [
                        anchor
                        for row in support_rows
                        for anchor in _list_string_values(row.get("conversation_anchor_entities", []))
                    ]
                ),
                "conversation_relevance_type": str(support_rows[0].get("conversation_relevance_type", "")),
                "conversation_relevance_rationale": str(support_rows[0].get("conversation_relevance_rationale", "")),
                "conversation_relevance_confidence": _safe_confidence(
                    support_rows[0].get("conversation_relevance_confidence", support_rows[0].get("conversation_model_confidence", 0.0))
                ),
                "conversation_model_confidence": _safe_confidence(support_rows[0].get("conversation_model_confidence", note.get("confidence", 0.0))),
                "conversation_global_index": int(note.get("global_conversation_index", 0) or 0),
                "conversation_patch_note_id": str(note.get("patch_note_id", "")),
                "conversation_patch_status": str(note.get("status", "")),
                "conversation_patch_summary": str(note.get("summary", "")),
                "conversation_patch_lore_developments": summarize_patch_items(note.get("lore_developments", []), "description"),
                "conversation_patch_meta_developments": summarize_patch_items(note.get("meta_developments", []), "description"),
                "conversation_patch_open_questions": summarize_patch_items(note.get("open_questions", []), "question"),
                "conversation_patch_possible_contradictions": summarize_patch_items(note.get("possible_contradictions", []), "description"),
                "partner_id": str(note.get("partner_id") or support_rows[0].get("partner_id", "")),
                "partner_label": str(note.get("partner_label") or support_rows[0].get("partner_label", "")),
                "message_ids": message_ids,
                "timestamp_start_utc": str(support_rows[0].get("timestamp_utc") or note.get("timestamp_start_utc", "")),
                "timestamp_end_utc": str(support_rows[-1].get("timestamp_utc") or note.get("timestamp_end_utc", "")),
                "speaker": "unknown",
                "raw_text": raw_text,
                "display_text_normalized": display_text,
                "relevance_score": max(0.72, confidence),
                "relevance_reason": (
                    f"provider=stage05_patch_note; item_type={item_type}; "
                    f"patch_note_id={note.get('patch_note_id', '')}; exact_support={str(exact_support).lower()}"
                ),
                "candidate_entities": candidates,
                "knowledge_track": _patch_item_track(item_type, note),
                "candidate_topics": _patch_item_topics(item_type, item),
                "sensitivity_flags": detect_sensitive_flags(raw_text),
                "provenance": support_rows[0].get("provenance", {}),
                "patch_item_index": item_index,
                "patch_candidate_entities": candidates,
                "patch_confidence": confidence,
                "patch_supporting_message_ids": message_ids,
                **_patch_item_kind_fields(item_type, item),
            }
            snippets.append(snippet)
            if not exact_support:
                review.append(snippet)

    return snippets, review, skipped_no_durable, skipped_empty_items


def stage_06_provider_mode(provider_config: dict[str, Any], rows: list[dict[str, Any]]) -> str:
    explicit = str(provider_config.get("stage_06_anchor_provider", "")).strip().lower()
    if explicit:
        return explicit
    if any(str(row.get("conversation_id", "")).strip() for row in rows):
        return "conversation_metadata"
    return str(provider_config.get("anchor_provider", "heuristic")).strip().lower() or "heuristic"


def index_rows_by_thread(rows: list[dict[str, Any]]) -> tuple[dict[str, list[dict[str, Any]]], dict[tuple[str, str], int]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        context_key = context_key_for_row(row)
        grouped.setdefault(context_key, []).append(row)

    for context_key, items in grouped.items():
        items.sort(key=lambda x: (str(x.get("timestamp_utc", "")), str(x.get("message_id", ""))))
        grouped[context_key] = items

    positions: dict[tuple[str, str], int] = {}
    for context_key, items in grouped.items():
        for idx, item in enumerate(items):
            positions[(context_key, str(item.get("message_id", "")))] = idx
    return grouped, positions


def context_rows_for_message(
    row: dict[str, Any],
    grouped_rows: dict[str, list[dict[str, Any]]],
    row_positions: dict[tuple[str, str], int],
    context_window_messages: int,
) -> list[dict[str, Any]]:
    context_key = context_key_for_row(row)
    message_id = str(row.get("message_id", ""))
    thread_rows = grouped_rows.get(context_key, [row])
    current_idx = row_positions.get((context_key, message_id), 0)

    window_size = max(1, int(context_window_messages))
    before = (window_size - 1) // 2
    after = (window_size - 1) - before
    start = max(0, current_idx - before)
    end = min(len(thread_rows), current_idx + after + 1)
    window_rows = thread_rows[start:end]
    return window_rows or [row]


def join_window_text(window_rows: list[dict[str, Any]], field: str) -> str:
    parts: list[str] = []
    for item in window_rows:
        value = str(item.get(field, "")).strip()
        if value:
            parts.append(value)
    return "\n".join(parts)


def score_relevance(text: str, profile: dict[str, Any]) -> tuple[float, str]:
    t = text.lower()
    lore_hits = sum(1 for kw in LORE_KEYWORDS if kw in t)
    meta_hits = sum(1 for kw in META_KEYWORDS if kw in t)
    base = min(1.0, (lore_hits * 0.24) + (meta_hits * 0.12))
    score = max(0.0, min(1.0, base + profile_adjustment(profile.get("profile_type", "unknown_low_signal"))))
    reason = f"lore_hits={lore_hits}, meta_hits={meta_hits}, profile={profile.get('profile_type')}"
    return score, reason


def classify_track(text: str) -> tuple[str, list[str]]:
    t = text.lower()
    lore_hits = [kw for kw in LORE_KEYWORDS if kw in t]
    meta_hits = [kw for kw in META_KEYWORDS if kw in t]
    topics: list[str] = []
    if lore_hits:
        topics.extend(["entity", "theme"])
    if meta_hits:
        topics.extend(["marketing", "production"])
    if lore_hits and not meta_hits:
        return "lore", sorted(set(topics))
    if meta_hits and not lore_hits:
        return "meta", sorted(set(topics))
    if lore_hits and meta_hits:
        return "meta", sorted(set(topics))
    return "unknown", []


def heuristic_anchor_candidates(text: str, seed_entities: list[str]) -> list[str]:
    lower = text.lower()
    matches = [name for name in seed_entities if name.lower() in lower]
    return matches[:10]


def meta_intent_hits(text: str) -> int:
    lowered = text.lower()
    return sum(1 for rx in META_INTENT_REGEXES if rx.search(lowered))


def conservative_bootstrap_without_model(
    text: str,
    heuristic_score: float,
    heuristic_reason: str,
    heuristic_track: str,
    heuristic_topics: list[str],
    anchors: list[str],
) -> tuple[float, str, str, list[str], list[str], list[str], list[str]]:
    # Heuristics are only a weak prior when model output is unavailable.
    meta_hits = meta_intent_hits(text)
    anchor_boost = min(0.2, 0.03 * len(anchors))
    prior_strength = max(0.0, min(1.0, heuristic_score + anchor_boost))

    if heuristic_track == "meta" and (meta_hits >= 2 or (meta_hits >= 1 and prior_strength >= 0.55)):
        score = max(0.38, min(0.72, round(0.20 + (0.16 * meta_hits) + (0.44 * prior_strength), 4)))
        reason = f"{heuristic_reason}; provider_unavailable=bootstrap_meta; meta_intent_hits={meta_hits}"
        topics = sorted(set(heuristic_topics + ["marketing", "production"]))
        return score, reason, "meta", topics, anchors, [], []

    if heuristic_track == "lore" and prior_strength >= 0.62 and meta_hits == 0 and len(anchors) >= 1:
        score = max(0.38, min(0.76, round(0.18 + (0.62 * prior_strength), 4)))
        reason = f"{heuristic_reason}; provider_unavailable=bootstrap_lore; anchor_count={len(anchors)}"
        topics = sorted(set(heuristic_topics + ["entity", "theme"]))
        return score, reason, "lore", topics, anchors, [], []

    score = min(0.34, round(0.20 + (0.20 * prior_strength), 4))
    reason = f"{heuristic_reason}; provider_unavailable=bootstrap_unknown; meta_intent_hits={meta_hits}"
    return score, reason, "unknown", heuristic_topics, anchors, [], []


def _list_string_values(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return [str(item).strip() for item in value if str(item).strip()]


def _safe_confidence(value: Any, default: float = 0.0) -> float:
    try:
        return max(0.0, min(1.0, float(value)))
    except (TypeError, ValueError):
        return default


def _metadata_topics(raw_track: str, heuristic_topics: list[str]) -> list[str]:
    topics = list(heuristic_topics)
    if raw_track in {"lore", "both"}:
        topics.extend(["entity", "theme"])
    if raw_track in {"meta", "both"}:
        topics.extend(["production"])
    return sorted(set(topics))


def classify_with_conversation_metadata(
    row: dict[str, Any],
    heuristic_score: float,
    heuristic_reason: str,
    heuristic_track: str,
    heuristic_topics: list[str],
    anchors: list[str],
) -> tuple[float, str, str, list[str], list[str], list[str], list[str]]:
    raw_track = str(row.get("conversation_track", "")).strip().lower()
    if not str(row.get("conversation_id", "")).strip() or raw_track not in {"lore", "meta", "both"}:
        return heuristic_score, heuristic_reason, heuristic_track, heuristic_topics, anchors, [], []

    snippet_track = "lore" if raw_track == "both" else raw_track
    model_confidence = _safe_confidence(row.get("conversation_model_confidence", 0.0))
    metadata_score = max(0.82, min(1.0, model_confidence or 0.82), heuristic_score)
    metadata_anchors = _list_string_values(row.get("conversation_anchor_entities", []))
    topic_label = str(row.get("conversation_topic_label", "")).strip()
    if topic_label and topic_label.lower() not in {"theriac discussion", "marketing roadmap"}:
        metadata_anchors.append(topic_label)
    merged_anchors = list(dict.fromkeys(metadata_anchors + anchors))[:10]
    topics = _metadata_topics(raw_track, heuristic_topics)
    reason = (
        f"{heuristic_reason}; provider=conversation_metadata; "
        f"conversation_id={row.get('conversation_id')}; conversation_track={raw_track}"
    )
    return metadata_score, reason, snippet_track, topics, merged_anchors, [], []


def classify_with_provider(
    text: str,
    profile: dict[str, Any],
    provider_config: dict[str, Any],
    seed_entities: list[str],
    row: dict[str, Any] | None = None,
) -> tuple[float, str, str, list[str], list[str], list[str], list[str]]:
    logger = get_logger(__name__)
    heuristic_score, heuristic_reason = score_relevance(text, profile)
    heuristic_track, heuristic_topics = classify_track(text)
    anchors = heuristic_anchor_candidates(text, seed_entities)

    mode = str(provider_config.get("anchor_provider", "heuristic")).lower()
    if mode in {"conversation_metadata", "segmented", "b3"}:
        if row is not None:
            return classify_with_conversation_metadata(
                row,
                heuristic_score,
                heuristic_reason,
                heuristic_track,
                heuristic_topics,
                anchors,
            )
        return heuristic_score, heuristic_reason, heuristic_track, heuristic_topics, anchors, [], []
    if mode not in {"model", "hybrid"}:
        return heuristic_score, heuristic_reason, heuristic_track, heuristic_topics, anchors, [], []

    model_provider_cfg = provider_config.get("model_provider", {})
    rate_state_path = Path(str(model_provider_cfg.get("rate_state_path", "artifacts/learning/model_provider_rate_runtime.json")))
    prompt = build_prompt(text, profile, seed_entities, anchors)
    model_response = call_model_chat(
        base_url=str(model_provider_cfg.get("base_url", "http://127.0.0.1:11434")),
        model=str(model_provider_cfg.get("model", "llama3.1")),
        prompt=prompt,
        temperature=float(model_provider_cfg.get("temperature", 0.0)),
        timeout_seconds=int(model_provider_cfg.get("timeout_seconds", 60)),
        provider=str(model_provider_cfg.get("provider", "auto")),
        api_base_url=str(model_provider_cfg.get("api_base_url", "https://openrouter.ai/api/v1")),
        api_model=str(model_provider_cfg.get("api_model", "qwen/qwen3.5-flash-02-23")),
        api_retries=int(model_provider_cfg.get("api_retries", 2)),
        auto_fallback_to_ollama=bool(model_provider_cfg.get("auto_fallback_to_ollama", True)),
        rate_limit_cooldown_seconds=int(model_provider_cfg.get("rate_limit_cooldown_seconds", 90)),
        rate_state_path=rate_state_path,
        min_interval_seconds=float(model_provider_cfg.get("adaptive_min_interval_seconds", 2.0)),
        max_interval_seconds=float(model_provider_cfg.get("adaptive_max_interval_seconds", 120.0)),
        success_decay=float(model_provider_cfg.get("adaptive_success_decay", 0.9)),
        rate_limit_growth=float(model_provider_cfg.get("adaptive_rate_limit_growth", 1.8)),
        ollama_unavailable_cooldown_seconds=int(model_provider_cfg.get("ollama_unavailable_cooldown_seconds", 120)),
        session_id=str(model_provider_cfg.get("session_id") or "theriac-stage-06-snippet-extraction"),
        trace=model_provider_cfg.get(
            "trace",
            {
                "trace_id": "theriac-stage-06-snippet-extraction",
                "trace_name": "THERIAC Stage 06 Snippet Extraction",
                "span_name": "stage_06_snippet_extraction",
                "generation_name": "snippet_relevance_classification",
                "pipeline_task": "stage_06_snippet_extraction",
            },
        ),
    )
    if not isinstance(model_response, dict):
        logger.debug(
            "Stage 06 provider fallback: mode=%s heuristic_score=%.3f heuristic_track=%s",
            mode,
            heuristic_score,
            heuristic_track,
        )
        return conservative_bootstrap_without_model(
            text,
            heuristic_score,
            heuristic_reason,
            heuristic_track,
            heuristic_topics,
            anchors,
        )

    model_score = float(model_response.get("theriac_relevance", heuristic_score))
    model_score = max(0.0, min(1.0, model_score))
    model_track = str(model_response.get("knowledge_track", heuristic_track))
    if model_track not in {"lore", "meta", "unknown"}:
        model_track = heuristic_track
    raw_candidates = model_response.get("anchor_candidates", [])
    model_anchors: list[str] = []
    if isinstance(raw_candidates, list):
        for item in raw_candidates:
            if isinstance(item, dict):
                name = str(item.get("name", "")).strip()
                if name:
                    model_anchors.append(name)
    merged_anchors = list(dict.fromkeys((model_anchors + anchors)))[:10]
    suggested = model_response.get("suggested_thematic_markers", {}) if isinstance(model_response, dict) else {}
    suggested_hist: list[str] = []
    suggested_music: list[str] = []
    if isinstance(suggested, dict):
        suggested_hist = [str(x).strip().lower() for x in (suggested.get("historical") or []) if str(x).strip()]
        suggested_music = [str(x).strip().lower() for x in (suggested.get("music") or []) if str(x).strip()]
    reason = f"{heuristic_reason}; provider={mode}; heuristic_prior_weight=0.15"
    logger.debug(
        "Stage 06 provider result: mode=%s model_score=%.3f model_track=%s model_anchors=%d merged_anchors=%d",
        mode,
        model_score,
        model_track,
        len(model_anchors),
        len(merged_anchors),
    )

    if mode == "hybrid":
        # In hybrid mode, model output dominates; heuristics are only weak priors.
        blended = round((0.85 * model_score) + (0.15 * heuristic_score), 4)
        topics = []
        if model_track == "meta":
            topics = sorted(set(heuristic_topics + ["marketing", "production"]))
        elif model_track == "lore":
            topics = sorted(set(heuristic_topics + ["entity", "theme"]))
        else:
            topics = heuristic_topics
        return blended, reason, model_track, topics, merged_anchors, sorted(set(suggested_hist)), sorted(set(suggested_music))

    topics = []
    if model_track == "meta":
        topics = sorted(set(heuristic_topics + ["marketing", "production"]))
    elif model_track == "lore":
        topics = sorted(set(heuristic_topics + ["entity", "theme"]))
    else:
        topics = heuristic_topics
    return model_score, reason, model_track, topics, merged_anchors, sorted(set(suggested_hist)), sorted(set(suggested_music))


def detect_sensitive_flags(text: str) -> list[str]:
    lower = text.lower()
    flags: list[str] = []
    for rx in SENSITIVE_REGEXES:
        if re.search(rx, lower):
            flags.append("contains_sensitive_language")
            break
    if any(ord(ch) > 127 for ch in text):
        flags.append("contains_unicode_or_emoji")
    return flags


def run(
    in_jsonl: Path,
    in_profiles_json: Path,
    out_snippets_jsonl: Path,
    out_needs_review_jsonl: Path,
    out_profiles_json: Path,
    in_pipeline_config_json: Path | None = None,
    in_seed_json: Path | None = None,
    thematic_runtime_path: Path | None = None,
    in_patch_notes_json: Path | None = None,
) -> None:
    logger = get_logger(__name__)
    rows = read_jsonl(in_jsonl)
    logger.info("Stage 06: loaded %d normalized message row(s).", len(rows))
    grouped_rows, row_positions = index_rows_by_thread(rows)
    profile_payload = {"profiles": []}
    if in_profiles_json.exists():
        profile_payload = read_json(in_profiles_json)
    profiles = {
        p["thread_id"]: p for p in profile_payload.get("profiles", []) if isinstance(p, dict) and "thread_id" in p
    }
    provider_config: dict[str, Any] = {"anchor_provider": "heuristic"}
    if in_pipeline_config_json and in_pipeline_config_json.exists():
        provider_config = read_json(in_pipeline_config_json)
    patch_notes = load_patch_notes(in_patch_notes_json)
    patch_notes_by_conversation = {
        str(note.get("conversation_id", "")): note
        for note in patch_notes
        if str(note.get("conversation_id", "")).strip()
    }
    effective_provider = stage_06_provider_mode(provider_config, rows)
    stage_06_provider_config = dict(provider_config)
    stage_06_provider_config["anchor_provider"] = effective_provider
    seed_entities = load_seed_entities(in_seed_json)
    logger.info(
        "Stage 06: provider=%s, configured_anchor_provider=%s, seed_entities=%d, existing_profiles=%d, patch_notes=%d",
        effective_provider,
        str(provider_config.get("anchor_provider", "heuristic")),
        len(seed_entities),
        len(profiles),
        len(patch_notes_by_conversation),
    )

    if patch_notes:
        snippets, review, skipped_no_durable, skipped_empty_items = materialize_patch_note_snippets(
            rows=rows,
            patch_notes=patch_notes,
            profiles=profiles,
            provider_config=stage_06_provider_config,
        )
        write_jsonl(out_snippets_jsonl, snippets)
        write_jsonl(out_needs_review_jsonl, review)
        write_json(out_profiles_json, {"profiles": list(profiles.values())})
        logger.info(
            "Stage 06 complete: materialized %d patch-note evidence snippet(s), needs_review=%d, profiles=%d, skipped_no_durable=%d, skipped_empty_items=%d",
            len(snippets),
            len(review),
            len(profiles),
            skipped_no_durable,
            skipped_empty_items,
        )
        return

    snippets: list[dict[str, Any]] = []
    review: list[dict[str, Any]] = []
    progress_every = max(1, len(rows) // 10)
    stage_06_hist_markers: list[str] = []
    stage_06_music_markers: list[str] = []
    skipped_no_durable = 0

    for row_index, row in enumerate(rows, start=1):
        thread_id = row["thread_id"]
        profile_key = source_profile_key_for_row(row)
        profile = profiles.get(profile_key)
        if profile is None:
            profile = default_profile(profile_key, row["partner_id"], row["partner_label"], stage_06_provider_config)
            profiles[profile_key] = profile
        context_window = int(profile.get("context_window_messages", 1))
        window_rows = context_rows_for_message(row, grouped_rows, row_positions, context_window)
        context_raw = join_window_text(window_rows, "content_raw")
        context_normalized = join_window_text(window_rows, "content_normalized")
        if not context_normalized:
            context_normalized = row.get("content_normalized", "")
        if not context_raw:
            context_raw = row.get("content_raw", "")
        patch_note = patch_notes_by_conversation.get(str(row.get("conversation_id", "")), {})
        if str(patch_note.get("status", "")).strip().lower() == "no_durable_development":
            skipped_no_durable += 1
            if row_index % progress_every == 0 or row_index == len(rows):
                logger.info(
                    "Stage 06 progress: %d/%d rows, snippets=%d, needs_review=%d, skipped_no_durable=%d",
                    row_index,
                    len(rows),
                    len(snippets),
                    len(review),
                    skipped_no_durable,
                )
            continue

        score, reason, track, topics, anchor_candidates, suggested_hist, suggested_music = classify_with_provider(
            context_normalized,
            profile,
            stage_06_provider_config,
            seed_entities,
            row,
        )
        stage_06_hist_markers.extend(suggested_hist)
        stage_06_music_markers.extend(suggested_music)
        threshold = float(profile["base_thresholds"]["theriac_relevance_min"])
        snippet = {
            "snippet_id": stable_id("snippet", row["message_id"], context_key_for_row(row)),
            "thread_id": row["thread_id"],
            "conversation_id": str(row.get("conversation_id", "")),
            "dm_pair_id": str(row.get("dm_pair_id", "")),
            "conversation_topic_label": str(row.get("conversation_topic_label", "")),
            "conversation_topic_summary": str(row.get("conversation_topic_summary", "")),
            "conversation_track": str(row.get("conversation_track", "")),
            "conversation_anchor_entities": _list_string_values(row.get("conversation_anchor_entities", [])),
            "conversation_relevance_type": str(row.get("conversation_relevance_type", "")),
            "conversation_relevance_rationale": str(row.get("conversation_relevance_rationale", "")),
            "conversation_relevance_confidence": _safe_confidence(row.get("conversation_relevance_confidence", row.get("conversation_model_confidence", 0.0))),
            "conversation_model_confidence": _safe_confidence(row.get("conversation_model_confidence", 0.0)),
            "conversation_global_index": int(patch_note.get("global_conversation_index", 0) or 0),
            "conversation_patch_note_id": str(patch_note.get("patch_note_id", "")),
            "conversation_patch_status": str(patch_note.get("status", "")),
            "conversation_patch_summary": str(patch_note.get("summary", "")),
            "conversation_patch_lore_developments": summarize_patch_items(patch_note.get("lore_developments", []), "description"),
            "conversation_patch_meta_developments": summarize_patch_items(patch_note.get("meta_developments", []), "description"),
            "conversation_patch_open_questions": summarize_patch_items(patch_note.get("open_questions", []), "question"),
            "conversation_patch_possible_contradictions": summarize_patch_items(patch_note.get("possible_contradictions", []), "description"),
            "partner_id": row["partner_id"],
            "partner_label": row["partner_label"],
            "message_ids": [str(item.get("message_id", "")) for item in window_rows if str(item.get("message_id", ""))],
            "timestamp_start_utc": window_rows[0].get("timestamp_utc", row["timestamp_utc"]),
            "timestamp_end_utc": window_rows[-1].get("timestamp_utc", row["timestamp_utc"]),
            "speaker": "unknown",
            "raw_text": context_raw,
            "display_text_normalized": context_normalized,
            "relevance_score": score,
            "relevance_reason": reason,
            "candidate_entities": anchor_candidates,
            "knowledge_track": track,
            "candidate_topics": topics,
            "sensitivity_flags": detect_sensitive_flags(row.get("content_raw", "")),
            "provenance": row.get("provenance", {}),
        }
        if score >= threshold and track != "unknown":
            snippets.append(snippet)
        elif score >= max(0.35, threshold - 0.2):
            review.append(snippet)
        logger.debug(
            "Stage 06 classify: row=%d/%d message_id=%s score=%.3f threshold=%.3f track=%s anchors=%d",
            row_index,
            len(rows),
            row.get("message_id", ""),
            score,
            threshold,
            track,
            len(anchor_candidates),
        )
        if row_index % progress_every == 0 or row_index == len(rows):
            logger.info(
                "Stage 06 progress: %d/%d rows, snippets=%d, needs_review=%d, skipped_no_durable=%d",
                row_index,
                len(rows),
                len(snippets),
                len(review),
                skipped_no_durable,
            )

    write_jsonl(out_snippets_jsonl, snippets)
    write_jsonl(out_needs_review_jsonl, review)
    write_json(out_profiles_json, {"profiles": list(profiles.values())})
    thematic_cfg = provider_config.get("thematic_linking", {})
    runtime_updates_enabled = bool(thematic_cfg.get("runtime_updates_enabled", True))
    if runtime_updates_enabled and thematic_runtime_path is not None:
        update_runtime_profile(
            thematic_runtime_path,
            "stage_06",
            stage_06_hist_markers,
            stage_06_music_markers,
            min_support=int(thematic_cfg.get("runtime_min_support", 2)),
        )
    logger.info(
        "Stage 06 complete: snippets=%d, needs_review=%d, profiles=%d, skipped_no_durable=%d",
        len(snippets),
        len(review),
        len(profiles),
        skipped_no_durable,
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--in-jsonl", type=Path, required=True)
    parser.add_argument("--in-profiles-json", type=Path, required=True)
    parser.add_argument("--out-snippets-jsonl", type=Path, required=True)
    parser.add_argument("--out-needs-review-jsonl", type=Path, required=True)
    parser.add_argument("--out-profiles-json", type=Path, required=True)
    parser.add_argument("--in-pipeline-config-json", type=Path, required=False, default=None)
    parser.add_argument("--in-entity-seed-json", "--in-seed-json", dest="in_seed_json", type=Path, required=False, default=None)
    parser.add_argument("--thematic-runtime-path", type=Path, required=False, default=None)
    parser.add_argument("--in-patch-notes-json", type=Path, required=False, default=None)
    args = parser.parse_args()
    run(
        args.in_jsonl,
        args.in_profiles_json,
        args.out_snippets_jsonl,
        args.out_needs_review_jsonl,
        args.out_profiles_json,
        args.in_pipeline_config_json,
        args.in_seed_json,
        args.thematic_runtime_path,
        args.in_patch_notes_json,
    )


if __name__ == "__main__":
    main()
