from __future__ import annotations

import argparse
import json
import re
import time
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Iterable

from pipeline.common import (
    get_logger,
    now_utc_iso,
    parse_discord_timestamp,
    read_json,
    read_jsonl,
    stable_id,
)
from pipeline.mixtral_anchor_provider import (
    call_gemini_batch_json,
    call_mixtral_chat,
    get_mixtral_runtime_status,
    load_seed_entities,
    model_batch_enabled,
    model_batch_initial_max_requests,
    model_batch_max_requests,
    model_call_kwargs,
)
from pipeline.stage_06_snippet_extraction import LORE_KEYWORDS, META_KEYWORDS, meta_intent_hits


VALID_TRACKS = {"lore", "meta", "both"}
PACING_SKIP_REASONS = {"provider_locked", "adaptive_pacing", "rate_limit_cooldown"}
NEGATIVE_RELEVANCE_VALUES = {"none", "irrelevant", "unrelated", "external_only", "personal_only", "unknown"}
POSITIVE_RELEVANCE_VALUES = {"direct_lore", "direct_project_meta", "direct_inspiration", "both"}
NEGATIVE_RELEVANCE_PHRASES = (
    "unrelated to theriac",
    "no direct connection to theriac",
    "no direct tie to theriac",
    "no theriac relevance",
    "does not mention theriac",
    "doesn't mention theriac",
    "not connected to theriac",
    "without a direct connection to theriac",
    "with no direct connection to theriac",
)
SEED_TOKEN_STOPWORDS = {
    "and",
    "architecture",
    "aesthetic",
    "aesthetics",
    "art",
    "artist",
    "artists",
    "auto",
    "catastrophe",
    "codename",
    "computing",
    "design",
    "develop",
    "developer",
    "developers",
    "development",
    "ecological",
    "federation",
    "film",
    "for",
    "from",
    "game",
    "games",
    "exit",
    "global",
    "heaven",
    "houses",
    "into",
    "marketing",
    "mechanic",
    "mechanics",
    "music",
    "mycelial",
    "name",
    "nation",
    "of",
    "pain",
    "plot",
    "production",
    "project",
    "research",
    "release",
    "roadmap",
    "story",
    "states",
    "sumerian",
    "the",
    "tunnel",
    "wars",
    "with",
}


def _write_json_atomic(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_name(f".{path.name}.tmp")
    tmp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp_path.replace(path)


def _write_jsonl_atomic(path: Path, rows: Iterable[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_name(f".{path.name}.tmp")
    with tmp_path.open("w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")
    tmp_path.replace(path)


def _default_config() -> dict[str, Any]:
    return {
        "enabled": True,
        "max_gap_hours": 12,
        "self_user_id": "",
        "short_digression_max_messages": 5,
        "short_digression_max_minutes": 15,
        "model_window_max_messages": 80,
        "model_window_max_chars": 14000,
        "segmentation_validation_retries": 1,
        "segmentation_provider_retries": 2,
        "segmentation_provider_retry_sleep_seconds": 90,
        "segmentation_validation_retry_sleep_seconds": 2,
        "cheap_prefilter_enabled": False,
    }


def load_config(path: Path | None) -> dict[str, Any]:
    if path is None or not path.exists():
        return {}
    payload = read_json(path)
    return payload if isinstance(payload, dict) else {}


def conversation_config(provider_config: dict[str, Any]) -> dict[str, Any]:
    cfg = _default_config()
    raw = provider_config.get("conversation_segmentation", {})
    raw_cfg = raw if isinstance(raw, dict) else {}
    cfg.update(raw_cfg)
    mixtral_cfg = provider_config.get("mixtral", {}) if isinstance(provider_config.get("mixtral", {}), dict) else {}
    if "segmentation_provider_retry_sleep_seconds" not in raw_cfg and "rate_limit_cooldown_seconds" in mixtral_cfg:
        cfg["segmentation_provider_retry_sleep_seconds"] = int(mixtral_cfg.get("rate_limit_cooldown_seconds", 90))
    if "segmentation_validation_retry_sleep_seconds" not in raw_cfg and "adaptive_min_interval_seconds" in mixtral_cfg:
        cfg["segmentation_validation_retry_sleep_seconds"] = float(mixtral_cfg.get("adaptive_min_interval_seconds", 2.0))

    cfg["max_gap_hours"] = float(cfg.get("max_gap_hours", 12))
    cfg["short_digression_max_messages"] = max(0, int(cfg.get("short_digression_max_messages", 5)))
    cfg["short_digression_max_minutes"] = max(0, int(cfg.get("short_digression_max_minutes", 15)))
    cfg["model_window_max_messages"] = max(1, int(cfg.get("model_window_max_messages", 80)))
    cfg["model_window_max_chars"] = max(1000, int(cfg.get("model_window_max_chars", 14000)))
    cfg["segmentation_validation_retries"] = max(0, int(cfg.get("segmentation_validation_retries", 1)))
    cfg["segmentation_provider_retries"] = max(0, int(cfg.get("segmentation_provider_retries", 2)))
    cfg["segmentation_provider_retry_sleep_seconds"] = max(
        0.0, float(cfg.get("segmentation_provider_retry_sleep_seconds", 90))
    )
    cfg["segmentation_validation_retry_sleep_seconds"] = max(
        0.0, float(cfg.get("segmentation_validation_retry_sleep_seconds", 2))
    )
    return cfg


def is_bot_or_application_row(row: dict[str, Any]) -> bool:
    return bool(
        row.get("is_bot_or_application")
        or row.get("author_is_bot")
        or row.get("application_id")
        or row.get("webhook_id")
    )


def detect_self_user_id(rows: list[dict[str, Any]], configured_self_user_id: str | None = None) -> str:
    configured = str(configured_self_user_id or "").strip()
    if configured:
        return configured

    thread_counts: dict[str, set[str]] = defaultdict(set)
    message_counts: Counter[str] = Counter()
    for row in rows:
        if is_bot_or_application_row(row):
            continue
        author_id = str(row.get("author_id", "unknown"))
        if not author_id or author_id == "unknown":
            continue
        thread_counts[author_id].add(str(row.get("thread_id", "")))
        message_counts[author_id] += 1
    if not thread_counts:
        for row in rows:
            author_id = str(row.get("author_id", "unknown"))
            if author_id and author_id != "unknown":
                message_counts[author_id] += 1
        return message_counts.most_common(1)[0][0] if message_counts else "unknown"

    return sorted(
        thread_counts,
        key=lambda aid: (len(thread_counts[aid]), message_counts[aid], aid),
        reverse=True,
    )[0]


def _labels_by_author(rows: list[dict[str, Any]]) -> dict[str, str]:
    labels: dict[str, str] = {}
    for row in rows:
        author_id = str(row.get("author_id", "unknown"))
        label = str(row.get("author_name") or author_id)
        if author_id and author_id != "unknown":
            labels[author_id] = label
    return labels


def annotate_dm_pairs(rows: list[dict[str, Any]], self_user_id: str) -> list[dict[str, Any]]:
    by_thread: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        by_thread[str(row.get("thread_id", ""))].append(row)

    thread_identity: dict[str, dict[str, Any]] = {}
    for thread_id, thread_rows in by_thread.items():
        counts: Counter[str] = Counter()
        labels = _labels_by_author(thread_rows)
        for row in thread_rows:
            if is_bot_or_application_row(row):
                continue
            author_id = str(row.get("author_id", "unknown"))
            if author_id and author_id != "unknown":
                counts[author_id] += 1

        non_self = [aid for aid in counts if aid != self_user_id]
        if non_self:
            partner_id = sorted(non_self, key=lambda aid: (counts[aid], aid), reverse=True)[0]
        else:
            partner_id = str(thread_rows[0].get("partner_id", "unknown")) if thread_rows else "unknown"

        participant_ids = sorted(aid for aid in counts if aid and aid != "unknown")
        if self_user_id and self_user_id != "unknown" and self_user_id not in participant_ids:
            participant_ids.append(self_user_id)
            participant_ids.sort()
        if partner_id and partner_id != "unknown" and partner_id not in participant_ids:
            participant_ids.append(partner_id)
            participant_ids.sort()

        pair_parts = sorted([self_user_id or "unknown", partner_id or "unknown"])
        dm_pair_id = stable_id("dm_pair", *pair_parts)
        thread_identity[thread_id] = {
            "dm_pair_id": dm_pair_id,
            "partner_id": partner_id,
            "partner_label": labels.get(partner_id, partner_id or "unknown"),
            "participant_ids": participant_ids,
            "participant_labels": {aid: labels.get(aid, aid) for aid in participant_ids},
        }

    annotated: list[dict[str, Any]] = []
    for row in rows:
        identity = thread_identity.get(str(row.get("thread_id", "")), {})
        updated = dict(row)
        updated["dm_pair_id"] = identity.get("dm_pair_id", stable_id("dm_pair", self_user_id, "unknown"))
        updated["partner_id"] = identity.get("partner_id", row.get("partner_id", "unknown"))
        updated["partner_label"] = identity.get("partner_label", row.get("partner_label", "unknown"))
        updated["participant_ids"] = identity.get("participant_ids", [])
        updated["participant_labels"] = identity.get("participant_labels", {})
        annotated.append(updated)

    annotated.sort(key=lambda x: (str(x.get("timestamp_utc", "")), str(x.get("message_id", ""))))
    return annotated


def _window_identity(rows: list[dict[str, Any]], window_idx: int) -> str:
    if not rows:
        return stable_id("coarse_conversation", str(window_idx))
    return stable_id(
        "coarse_conversation",
        str(rows[0].get("dm_pair_id", "")),
        str(rows[0].get("message_id", "")),
        str(rows[-1].get("message_id", "")),
        str(rows[0].get("timestamp_utc", "")),
    )


def _coarse_window_from_rows(rows: list[dict[str, Any]], window_idx: int, previous_gap_seconds: float | None) -> dict[str, Any]:
    return {
        "coarse_window_id": _window_identity(rows, window_idx),
        "dm_pair_id": str(rows[0].get("dm_pair_id", "")) if rows else "",
        "partner_id": str(rows[0].get("partner_id", "unknown")) if rows else "unknown",
        "partner_label": str(rows[0].get("partner_label", "unknown")) if rows else "unknown",
        "participant_ids": rows[0].get("participant_ids", []) if rows else [],
        "participant_labels": rows[0].get("participant_labels", {}) if rows else {},
        "message_ids": [str(row.get("message_id", "")) for row in rows],
        "timestamp_start_utc": rows[0].get("timestamp_utc") if rows else None,
        "timestamp_end_utc": rows[-1].get("timestamp_utc") if rows else None,
        "message_count": len(rows),
        "previous_gap_seconds": previous_gap_seconds,
        "rows": rows,
    }


def build_coarse_windows(rows: list[dict[str, Any]], max_gap_hours: float) -> list[dict[str, Any]]:
    max_gap_seconds = float(max_gap_hours) * 3600.0
    by_pair: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        by_pair[str(row.get("dm_pair_id", ""))].append(row)

    windows: list[dict[str, Any]] = []
    window_idx = 0
    for _dm_pair_id, pair_rows in sorted(by_pair.items()):
        pair_rows.sort(key=lambda x: (str(x.get("timestamp_utc", "")), str(x.get("message_id", ""))))
        current: list[dict[str, Any]] = []
        previous_ts = None
        previous_gap_for_window: float | None = None
        for row in pair_rows:
            ts = parse_discord_timestamp(str(row.get("timestamp_utc")))
            if previous_ts is not None:
                gap_seconds = (ts - previous_ts).total_seconds()
                if gap_seconds > max_gap_seconds and current:
                    window_idx += 1
                    windows.append(_coarse_window_from_rows(current, window_idx, previous_gap_for_window))
                    current = []
                    previous_gap_for_window = gap_seconds
            current.append(row)
            previous_ts = ts
        if current:
            window_idx += 1
            windows.append(_coarse_window_from_rows(current, window_idx, previous_gap_for_window))

    windows.sort(key=lambda x: (str(x.get("timestamp_start_utc", "")), str(x.get("coarse_window_id", ""))))
    return windows


def _window_text(rows: list[dict[str, Any]], max_chars: int | None = None) -> str:
    parts: list[str] = []
    total = 0
    limit = max_chars or 10**9
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


def is_candidate_window(window: dict[str, Any], seed_entities: list[str]) -> bool:
    text = _window_text(window.get("rows", []), max_chars=25000)
    lowered = text.lower()
    if not lowered.strip():
        return False
    if any(keyword in lowered for keyword in LORE_KEYWORDS | META_KEYWORDS):
        return True
    if meta_intent_hits(text) > 0:
        return True
    for name in seed_entities:
        normalized = str(name).strip().lower()
        if len(normalized) >= 3 and normalized in lowered:
            return True
    return False


def split_window_for_model(window: dict[str, Any], max_messages: int, max_chars: int) -> list[dict[str, Any]]:
    rows = list(window.get("rows", []))
    chunks: list[dict[str, Any]] = []
    current: list[dict[str, Any]] = []
    current_chars = 0
    for row in rows:
        row_text = str(row.get("content_normalized") or row.get("content_raw") or "")
        row_chars = max(1, len(row_text))
        should_flush = current and (len(current) >= max_messages or current_chars + row_chars > max_chars)
        if should_flush:
            chunk = dict(window)
            chunk["rows"] = current
            chunk["message_ids"] = [str(item.get("message_id", "")) for item in current]
            chunk["timestamp_start_utc"] = current[0].get("timestamp_utc")
            chunk["timestamp_end_utc"] = current[-1].get("timestamp_utc")
            chunk["message_count"] = len(current)
            chunk["model_window_id"] = f"{window['coarse_window_id']}:part{len(chunks) + 1}"
            chunks.append(chunk)
            current = []
            current_chars = 0
        current.append(row)
        current_chars += row_chars

    if current:
        chunk = dict(window)
        chunk["rows"] = current
        chunk["message_ids"] = [str(item.get("message_id", "")) for item in current]
        chunk["timestamp_start_utc"] = current[0].get("timestamp_utc")
        chunk["timestamp_end_utc"] = current[-1].get("timestamp_utc")
        chunk["message_count"] = len(current)
        chunk["model_window_id"] = f"{window['coarse_window_id']}:part{len(chunks) + 1}"
        chunks.append(chunk)
    return chunks


def build_segmentation_prompt(window: dict[str, Any], cfg: dict[str, Any], seed_entities: list[str]) -> str:
    messages = []
    for idx, row in enumerate(window.get("rows", []), start=1):
        content = str(row.get("content_normalized") or row.get("content_raw") or "")
        if len(content) > 900:
            content = content[:900] + "..."
        messages.append(
            {
                "index": idx,
                "message_id": str(row.get("message_id", "")),
                "timestamp_utc": str(row.get("timestamp_utc", "")),
                "author_id": str(row.get("author_id", "")),
                "author_name": str(row.get("author_name", "")),
                "is_bot_or_application": bool(is_bot_or_application_row(row)),
                "content": content,
            }
        )

    seed_preview = seed_entities[:80]
    return f"""You segment 1:1 Discord DMs for THERIAC lore-card extraction.
Return strict JSON only with no markdown.

THERIAC-relevant means:
- lore/canon/story/worldbuilding discussion; or
- meta/design/production discussion about THERIAC, its plot, mechanics, release, marketing, writing, or canon decisions.

Strict relevance gate:
- Include a segment only when the messages themselves make a direct THERIAC connection.
- External media, other games, anime, music, science, history, career advice, personal updates, relationships, jobs, food, and general life chat are irrelevant unless the same segment explicitly connects them to a THERIAC entity, quest, mechanic, style decision, production decision, or canon question.
- Inspiration/vibes alone are not enough. A segment about another work is relevant only if the speakers explicitly apply it to THERIAC.
- Do not classify another game's internal lore, factions, quests, character builds, faction bonuses, or character relationships as THERIAC lore. If retained because it is explicitly applied to THERIAC, anchor the segment to the THERIAC concept being designed, not to the external-media names.
- For project-wide meta discussion, include "THERIAC" in anchor_entities.
- If the topic summary or rationale would say "unrelated to THERIAC" or "no direct connection to THERIAC", do not emit that segment.

Discard unrelated chat. Do not emit irrelevant segments.
Split the candidate window when the material topic changes: different entity, quest, plot thread, mechanic, production concern, or canon question.
Keep a brief digression inside its surrounding segment only when it is under {cfg["short_digression_max_messages"]} messages,
under {cfg["short_digression_max_minutes"]} minutes, and returns to the same topic.
Return segments in chronological order. Segments must be non-overlapping: do not emit duplicate, nested, or partly overlapping spans.
If one topic is nested inside a broader topic, choose precise adjacent boundaries instead of returning both spans.

Entity/name hints, for relevance only:
{json.dumps(seed_preview, ensure_ascii=False)}

Candidate window:
- source_coarse_window_id: {window.get("coarse_window_id")}
- model_window_id: {window.get("model_window_id", window.get("coarse_window_id"))}
- dm_pair_id: {window.get("dm_pair_id")}
- partner_label: {window.get("partner_label")}
- timestamp_start_utc: {window.get("timestamp_start_utc")}
- timestamp_end_utc: {window.get("timestamp_end_utc")}

Messages:
{json.dumps(messages, ensure_ascii=False, indent=2)}

Return JSON object:
{{
  "segments": [
    {{
      "start_message_index": 1,
      "end_message_index": 3,
      "start_message_id": "first included message id",
      "end_message_id": "last included message id",
      "track": "lore|meta|both",
      "topic_label": "short stable label",
      "topic_summary": "1-3 sentence summary of the relevant discussion",
      "topic_shift_reason": "why this is separate from adjacent material",
      "anchor_entities": ["entity or concept names"],
      "relevance_type": "direct_lore|direct_project_meta|direct_inspiration",
      "relevance_rationale": "specific direct THERIAC tie, naming the entity, quest, mechanic, style decision, production decision, or canon question",
      "relevance_confidence": 0.0,
      "confidence": 0.0
    }}
  ]
}}

Use only message indices and message_id values shown in the Messages list above.
Prefer start_message_index/end_message_index when possible; include message IDs too if you are confident.
If the window contains no THERIAC-relevant material, return {{"segments":[]}}.
"""


def _call_model(prompt: str, provider_config: dict[str, Any], cfg: dict[str, Any]) -> dict[str, Any] | None:
    call_kwargs = model_call_kwargs(provider_config, "stage_04_conversation_segmentation")
    if cfg.get("provider"):
        call_kwargs["provider"] = str(cfg["provider"])
    if cfg.get("api_retries") is not None:
        call_kwargs["api_retries"] = int(cfg["api_retries"])
    return call_mixtral_chat(prompt=prompt, **call_kwargs)


def _provider_wait_seconds(reason: str, status: dict[str, Any], cfg: dict[str, Any]) -> float:
    now_s = time.time()
    next_attempt = float(status.get("next_mistral_attempt_epoch_s") or 0.0)
    rate_limited_until = float(status.get("rate_limited_until_epoch_s") or 0.0)
    target = 0.0
    if reason in {"provider_locked", "adaptive_pacing"}:
        target = next_attempt
    elif reason in {"rate_limit_cooldown", "rate_limited_429"}:
        target = max(rate_limited_until, next_attempt)
    if target > now_s:
        return max(0.1, target - now_s)
    return float(cfg["segmentation_provider_retry_sleep_seconds"])


def _json_preview(payload: Any, limit: int = 600) -> str:
    try:
        text = json.dumps(payload, ensure_ascii=False, sort_keys=True)
    except TypeError:
        text = str(payload)
    if len(text) > limit:
        return text[:limit] + "..."
    return text


def _message_id_from_model_ref(
    raw_value: Any,
    valid_message_ids: set[str],
    index_to_message_id: dict[int, str],
) -> str:
    value = str(raw_value or "").strip()
    if value in valid_message_ids:
        return value
    if value.isdigit():
        return index_to_message_id.get(int(value), "")
    return ""


def _safe_unit_float(value: Any, default: float = 0.0) -> float:
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        numeric = default
    return max(0.0, min(1.0, numeric))


def _relevance_key(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", value.lower())


def _seed_relevance_keys(seed_entities: list[str]) -> set[str]:
    keys: set[str] = set()
    for seed in seed_entities:
        seed_key = _relevance_key(seed)
        if seed_key:
            keys.add(seed_key)
        if "(" in seed:
            base_title_key = _relevance_key(seed.split("(", 1)[0])
            if len(base_title_key) >= 5:
                keys.add(base_title_key)
        for token in re.findall(r"[A-Za-z0-9]+", seed):
            token_key = _relevance_key(token)
            if len(token_key) >= 4 and token_key not in SEED_TOKEN_STOPWORDS:
                keys.add(token_key)
    return keys


def _segment_text_blob(raw: dict[str, Any]) -> str:
    anchor_entities = raw.get("anchor_entities", [])
    anchor_text = " ".join(str(item) for item in anchor_entities) if isinstance(anchor_entities, list) else str(anchor_entities)
    return " ".join(
        [
            str(raw.get("topic_label", "")),
            str(raw.get("topic_summary", "")),
            str(raw.get("topic_shift_reason", "")),
            str(raw.get("relevance_rationale", "")),
            anchor_text,
        ]
    ).lower()


def _rows_text_blob(rows: list[dict[str, Any]]) -> str:
    return " ".join(str(row.get("content_normalized") or row.get("content_raw") or "") for row in rows).lower()


def _has_direct_theriac_signal(raw: dict[str, Any], seed_keys: set[str], segment_rows: list[dict[str, Any]]) -> bool:
    rows_blob = _rows_text_blob(segment_rows)
    normalized_rows_blob = _relevance_key(rows_blob)
    if "theriac" in rows_blob:
        return True

    anchor_entities = raw.get("anchor_entities", [])
    anchors = anchor_entities if isinstance(anchor_entities, list) else []
    anchor_keys = {_relevance_key(str(anchor)) for anchor in anchors if _relevance_key(str(anchor))}
    if seed_keys:
        if any(anchor_key in normalized_rows_blob for anchor_key in anchor_keys & seed_keys):
            return True
        for seed_key in seed_keys:
            if len(seed_key) >= 5 and seed_key in normalized_rows_blob:
                return True
        return False

    return bool(anchor_keys)


def _relevance_drop_reason(raw: dict[str, Any], seed_keys: set[str], segment_rows: list[dict[str, Any]]) -> str:
    relevance_type = str(raw.get("relevance_type", "")).strip().lower()
    if relevance_type in NEGATIVE_RELEVANCE_VALUES:
        return f"model_relevance_type_{relevance_type}"

    confidence_value = raw.get("relevance_confidence")
    if confidence_value not in (None, ""):
        try:
            relevance_confidence = float(confidence_value)
        except (TypeError, ValueError):
            relevance_confidence = 0.0
        if relevance_confidence < 0.5:
            return "low_relevance_confidence"

    blob = _segment_text_blob(raw)
    if any(phrase in blob for phrase in NEGATIVE_RELEVANCE_PHRASES):
        return "negative_relevance_rationale"

    if not _has_direct_theriac_signal(raw, seed_keys, segment_rows):
        return "missing_direct_theriac_signal"

    if relevance_type and relevance_type not in POSITIVE_RELEVANCE_VALUES:
        return f"unsupported_relevance_type_{relevance_type}"

    return ""


def _record_relevance_event(
    relevance_events: list[dict[str, Any]] | None,
    window: dict[str, Any],
    raw: dict[str, Any],
    reason: str,
) -> None:
    if relevance_events is None:
        return
    relevance_events.append(
        {
            "event_type": "model_segment_dropped_by_relevance_gate",
            "reason": reason,
            "source_coarse_window_id": str(window.get("coarse_window_id", "")),
            "source_model_window_id": str(window.get("model_window_id", window.get("coarse_window_id", ""))),
            "dm_pair_id": str(window.get("dm_pair_id", "")),
            "partner_id": str(window.get("partner_id", "unknown")),
            "partner_label": str(window.get("partner_label", "unknown")),
            "track": str(raw.get("track", "")),
            "topic_label": str(raw.get("topic_label", "")),
            "topic_summary": str(raw.get("topic_summary", ""))[:500],
            "anchor_entities": raw.get("anchor_entities", []) if isinstance(raw.get("anchor_entities", []), list) else [],
            "relevance_type": str(raw.get("relevance_type", "")),
            "relevance_rationale": str(raw.get("relevance_rationale", ""))[:500],
            "recorded_at_utc": now_utc_iso(),
        }
    )


def normalize_model_segments(
    payload: dict[str, Any],
    window_rows: list[dict[str, Any]],
    seed_entities: list[str] | None = None,
    relevance_events: list[dict[str, Any]] | None = None,
    window: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    raw_segments = payload.get("segments")
    if not isinstance(raw_segments, list):
        raise ValueError("model response missing segments list")

    gate_window = window or {}
    seed_keys = _seed_relevance_keys(seed_entities or [])
    valid_message_ids = {str(row.get("message_id", "")) for row in window_rows if str(row.get("message_id", "")).strip()}
    positions = {str(row.get("message_id", "")): idx for idx, row in enumerate(window_rows)}
    index_to_message_id = {
        idx: str(row.get("message_id", ""))
        for idx, row in enumerate(window_rows, start=1)
        if str(row.get("message_id", "")).strip()
    }
    normalized: list[dict[str, Any]] = []
    for raw in raw_segments:
        if not isinstance(raw, dict):
            continue
        track = str(raw.get("track", "")).strip().lower()
        if track not in VALID_TRACKS:
            continue
        message_ids = raw.get("message_ids")
        if isinstance(message_ids, list) and message_ids:
            start_message_id = _message_id_from_model_ref(message_ids[0], valid_message_ids, index_to_message_id)
            end_message_id = _message_id_from_model_ref(message_ids[-1], valid_message_ids, index_to_message_id)
        else:
            start_message_id = _message_id_from_model_ref(
                raw.get("start_message_id", ""),
                valid_message_ids,
                index_to_message_id,
            )
            end_message_id = _message_id_from_model_ref(
                raw.get("end_message_id", ""),
                valid_message_ids,
                index_to_message_id,
            )
        if not start_message_id:
            start_message_id = _message_id_from_model_ref(
                raw.get("start_message_index", ""),
                valid_message_ids,
                index_to_message_id,
            )
        if not end_message_id:
            end_message_id = _message_id_from_model_ref(
                raw.get("end_message_index", ""),
                valid_message_ids,
                index_to_message_id,
            )
        if start_message_id not in valid_message_ids or end_message_id not in valid_message_ids:
            raise ValueError(
                "model segment references message id/index outside the candidate window: "
                f"start={raw.get('start_message_id') or raw.get('start_message_index')!r}, "
                f"end={raw.get('end_message_id') or raw.get('end_message_index')!r}"
            )
        start_pos = positions[start_message_id]
        end_pos = positions[end_message_id]
        if end_pos < start_pos:
            start_pos, end_pos = end_pos, start_pos
        segment_rows = window_rows[start_pos : end_pos + 1]
        relevance_drop_reason = _relevance_drop_reason(raw, seed_keys, segment_rows)
        if relevance_drop_reason:
            _record_relevance_event(relevance_events, gate_window, raw, relevance_drop_reason)
            continue
        confidence = _safe_unit_float(raw.get("confidence", 0.0))
        normalized.append(
            {
                "start_message_id": start_message_id,
                "end_message_id": end_message_id,
                "track": track,
                "topic_label": str(raw.get("topic_label", "")).strip()[:120] or "Theriac discussion",
                "topic_summary": str(raw.get("topic_summary", "")).strip(),
                "topic_shift_reason": str(raw.get("topic_shift_reason", "")).strip(),
                "anchor_entities": [
                    str(item).strip()
                    for item in raw.get("anchor_entities", [])
                    if str(item).strip()
                ][:20]
                if isinstance(raw.get("anchor_entities", []), list)
                else [],
                "relevance_type": str(raw.get("relevance_type", "")).strip(),
                "relevance_rationale": str(raw.get("relevance_rationale", "")).strip(),
                "relevance_confidence": _safe_unit_float(raw.get("relevance_confidence", raw.get("confidence", 0.0))),
                "confidence": confidence,
            }
        )
    return normalized


def _segment_message_ids(rows: list[dict[str, Any]], start_idx: int, end_idx: int) -> list[str]:
    if end_idx < start_idx:
        return []
    return [str(row.get("message_id", "")) for row in rows[start_idx : end_idx + 1]]


def _record_overlap_event(
    overlap_events: list[dict[str, Any]] | None,
    window: dict[str, Any],
    model_segment: dict[str, Any],
    rows: list[dict[str, Any]],
    original_start_idx: int,
    original_end_idx: int,
    *,
    event_type: str,
    overlap_kind: str,
    action: str,
    materialized_start_idx: int | None = None,
    materialized_end_idx: int | None = None,
) -> None:
    if overlap_events is None:
        return
    overlap_events.append(
        {
            "event_type": event_type,
            "overlap_kind": overlap_kind,
            "action": action,
            "source_coarse_window_id": str(window.get("coarse_window_id", "")),
            "source_model_window_id": str(window.get("model_window_id", window.get("coarse_window_id", ""))),
            "dm_pair_id": str(window.get("dm_pair_id", "")),
            "partner_id": str(window.get("partner_id", "unknown")),
            "partner_label": str(window.get("partner_label", "unknown")),
            "topic_label": str(model_segment.get("topic_label", "")),
            "track": str(model_segment.get("track", "")),
            "original_message_ids": _segment_message_ids(rows, original_start_idx, original_end_idx),
            "materialized_message_ids": _segment_message_ids(rows, materialized_start_idx, materialized_end_idx)
            if materialized_start_idx is not None and materialized_end_idx is not None
            else [],
            "recorded_at_utc": now_utc_iso(),
        }
    )


def segment_window_with_model(
    window: dict[str, Any],
    provider_config: dict[str, Any],
    cfg: dict[str, Any],
    seed_entities: list[str],
    relevance_events: list[dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    prompt = build_segmentation_prompt(window, cfg, seed_entities)
    provider_attempts = 1 + int(cfg["segmentation_provider_retries"])
    validation_attempts = 1 + int(cfg["segmentation_validation_retries"])
    last_reason = "model_segmentation_failed"
    logger = get_logger(__name__)
    provider_failures = 0
    validation_failures = 0
    while provider_failures < provider_attempts and validation_failures < validation_attempts:
        payload = _call_model(prompt, provider_config, cfg)
        if not isinstance(payload, dict):
            status = get_mixtral_runtime_status()
            reason = str(status.get("last_mistral_skip_reason") or "provider_unavailable_or_rate_limited")
            last_reason = reason
            sleep_s = _provider_wait_seconds(reason, status, cfg)
            if reason in PACING_SKIP_REASONS:
                logger.info(
                    "Stage 04: provider pacing for %s; retrying in %.1fs (%s).",
                    window.get("model_window_id", window.get("coarse_window_id")),
                    sleep_s,
                    reason,
                )
                time.sleep(sleep_s)
                continue

            provider_failures += 1
            if provider_failures < provider_attempts:
                logger.info(
                    "Stage 04: model unavailable for %s; retrying in %.1fs (%d/%d, %s).",
                    window.get("model_window_id", window.get("coarse_window_id")),
                    sleep_s,
                    provider_failures,
                    provider_attempts,
                    reason,
                )
                time.sleep(sleep_s)
            continue
        try:
            return normalize_model_segments(
                payload,
                list(window.get("rows", [])),
                seed_entities,
                relevance_events,
                window,
            )
        except Exception as exc:
            last_reason = f"invalid_model_response: {exc}; payload_preview={_json_preview(payload)}"
            validation_failures += 1
            if validation_failures < validation_attempts:
                sleep_s = float(cfg["segmentation_validation_retry_sleep_seconds"])
                logger.info(
                    "Stage 04: invalid segmentation response for %s; retrying in %.1fs (%d/%d).",
                    window.get("model_window_id", window.get("coarse_window_id")),
                    sleep_s,
                    validation_failures,
                    validation_attempts,
                )
                time.sleep(sleep_s)
    raise RuntimeError(last_reason)


def provider_config_with_task_overrides(
    provider_config: dict[str, Any],
    task_name: str,
    overrides: dict[str, Any],
) -> dict[str, Any]:
    next_config = dict(provider_config or {})
    routing = dict(next_config.get("model_routing", {}) if isinstance(next_config.get("model_routing", {}), dict) else {})
    tasks = dict(routing.get("tasks", {}) if isinstance(routing.get("tasks", {}), dict) else {})
    task_cfg = dict(tasks.get(task_name, {}) if isinstance(tasks.get(task_name, {}), dict) else {})
    task_cfg.update(overrides)
    tasks[task_name] = task_cfg
    routing["tasks"] = tasks
    next_config["model_routing"] = routing
    return next_config


def segment_windows_with_batch(
    windows: list[dict[str, Any]],
    provider_config: dict[str, Any],
    cfg: dict[str, Any],
    seed_entities: list[str],
    relevance_events: list[dict[str, Any]] | None = None,
    batch_status_log_path: Path | None = None,
) -> dict[str, dict[str, Any]]:
    requests = [
        {
            "key": str(window.get("model_window_id", window.get("coarse_window_id", ""))),
            "prompt": build_segmentation_prompt(window, cfg, seed_entities),
        }
        for window in windows
    ]
    batch_provider_config = provider_config
    if batch_status_log_path is not None:
        batch_provider_config = provider_config_with_task_overrides(
            provider_config,
            "stage_04_conversation_segmentation",
            {"batch_status_log_path": str(batch_status_log_path)},
        )
    batch_results = call_gemini_batch_json(batch_provider_config, "stage_04_conversation_segmentation", requests)
    normalized_by_window: dict[str, dict[str, Any]] = {}
    for window in windows:
        key = str(window.get("model_window_id", window.get("coarse_window_id", "")))
        result = batch_results.get(key, {"payload": None, "error": "missing_batch_response"})
        payload = result.get("payload")
        if not isinstance(payload, dict):
            normalized_by_window[key] = {
                "segments": None,
                "error": str(result.get("error") or "batch_response_not_json_object"),
                "payload": payload,
            }
            continue
        try:
            normalized_by_window[key] = {
                "segments": normalize_model_segments(
                    payload,
                    list(window.get("rows", [])),
                    seed_entities,
                    relevance_events,
                    window,
                ),
                "error": "",
                "payload": payload,
            }
        except Exception as exc:
            normalized_by_window[key] = {
                "segments": None,
                "error": f"invalid_model_response: {exc}; payload_preview={_json_preview(payload)}",
                "payload": payload,
            }
    return normalized_by_window


def materialize_segments(
    window: dict[str, Any],
    model_segments: list[dict[str, Any]],
    overlap_events: list[dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    rows = list(window.get("rows", []))
    positions = {str(row.get("message_id", "")): idx for idx, row in enumerate(rows)}
    segments: list[dict[str, Any]] = []
    accepted_spans: set[tuple[int, int]] = set()
    last_end = -1
    for model_segment in sorted(model_segments, key=lambda item: positions[str(item["start_message_id"])]):
        start_idx = positions[str(model_segment["start_message_id"])]
        end_idx = positions[str(model_segment["end_message_id"])]
        if end_idx < start_idx:
            start_idx, end_idx = end_idx, start_idx
        original_start_idx = start_idx
        original_end_idx = end_idx
        if start_idx <= last_end:
            if (start_idx, end_idx) in accepted_spans:
                _record_overlap_event(
                    overlap_events,
                    window,
                    model_segment,
                    rows,
                    original_start_idx,
                    original_end_idx,
                    event_type="overlapping_segment_dropped",
                    overlap_kind="duplicate_span",
                    action="dropped",
                )
                continue
            if end_idx <= last_end:
                _record_overlap_event(
                    overlap_events,
                    window,
                    model_segment,
                    rows,
                    original_start_idx,
                    original_end_idx,
                    event_type="overlapping_segment_dropped",
                    overlap_kind="nested_span",
                    action="dropped",
                )
                continue
            start_idx = last_end + 1
            _record_overlap_event(
                overlap_events,
                window,
                model_segment,
                rows,
                original_start_idx,
                original_end_idx,
                event_type="overlapping_segment_trimmed",
                overlap_kind="partial_prefix",
                action="trimmed",
                materialized_start_idx=start_idx,
                materialized_end_idx=end_idx,
            )
        segment_rows = rows[start_idx : end_idx + 1]
        if not segment_rows:
            continue
        topic_label = str(model_segment["topic_label"])
        conversation_id = stable_id(
            "conversation",
            str(window.get("dm_pair_id", "")),
            str(segment_rows[0].get("message_id", "")),
            str(segment_rows[-1].get("message_id", "")),
            topic_label.lower(),
        )
        segments.append(
            {
                "conversation_id": conversation_id,
                "dm_pair_id": str(window.get("dm_pair_id", "")),
                "partner_id": str(window.get("partner_id", "unknown")),
                "partner_label": str(window.get("partner_label", "unknown")),
                "participant_ids": window.get("participant_ids", []),
                "participant_labels": window.get("participant_labels", {}),
                "track": model_segment["track"],
                "topic_label": topic_label,
                "topic_summary": model_segment["topic_summary"],
                "topic_shift_reason": model_segment["topic_shift_reason"],
                "anchor_entities": model_segment["anchor_entities"],
                "relevance_type": model_segment.get("relevance_type", ""),
                "relevance_rationale": model_segment.get("relevance_rationale", ""),
                "relevance_confidence": float(model_segment.get("relevance_confidence", model_segment["confidence"])),
                "message_ids": [str(row.get("message_id", "")) for row in segment_rows],
                "timestamp_start_utc": segment_rows[0].get("timestamp_utc"),
                "timestamp_end_utc": segment_rows[-1].get("timestamp_utc"),
                "message_count": len(segment_rows),
                "model_confidence": model_segment["confidence"],
                "source_coarse_window_id": str(window.get("coarse_window_id", "")),
                "source_model_window_id": str(window.get("model_window_id", window.get("coarse_window_id", ""))),
            }
        )
        accepted_spans.add((start_idx, end_idx))
        last_end = end_idx
    return segments


def annotate_rows_for_segments(rows_by_id: dict[str, dict[str, Any]], segments: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out_rows: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for segment in sorted(segments, key=lambda x: (str(x.get("timestamp_start_utc", "")), str(x.get("conversation_id", "")))):
        for idx, message_id in enumerate(segment.get("message_ids", []), start=1):
            key = (str(segment["conversation_id"]), str(message_id))
            if key in seen or str(message_id) not in rows_by_id:
                continue
            seen.add(key)
            row = dict(rows_by_id[str(message_id)])
            row["conversation_id"] = str(segment["conversation_id"])
            row["dm_pair_id"] = str(segment["dm_pair_id"])
            row["conversation_message_index"] = idx
            row["conversation_topic_label"] = str(segment["topic_label"])
            row["conversation_topic_summary"] = str(segment.get("topic_summary", ""))
            row["conversation_track"] = str(segment["track"])
            row["conversation_anchor_entities"] = list(segment.get("anchor_entities", []))
            row["conversation_relevance_type"] = str(segment.get("relevance_type", ""))
            row["conversation_relevance_rationale"] = str(segment.get("relevance_rationale", ""))
            row["conversation_relevance_confidence"] = float(segment.get("relevance_confidence", segment.get("model_confidence", 0.0)))
            row["conversation_model_confidence"] = float(segment.get("model_confidence", 0.0))
            row["conversation_source_model_window_id"] = str(segment.get("source_model_window_id", ""))
            out_rows.append(row)
    out_rows.sort(key=lambda x: (str(x.get("timestamp_utc", "")), str(x.get("conversation_id", "")), int(x.get("conversation_message_index", 0))))
    return out_rows


def write_stage_outputs(
    *,
    out_jsonl: Path,
    out_segments_json: Path,
    out_index_json: Path,
    out_failures_json: Path,
    rows_by_id: dict[str, dict[str, Any]],
    segments: list[dict[str, Any]],
    failures: list[dict[str, Any]],
    overlap_events: list[dict[str, Any]],
    relevance_events: list[dict[str, Any]],
    annotated_rows: list[dict[str, Any]],
    self_user_id: str,
    max_gap_hours: float,
    messages_in: int,
    coarse_windows: int,
    completed_coarse_windows: int,
    candidate_windows: int,
    dropped_prefilter: int,
    model_windows: int,
    status: str,
    planned_model_windows: int | None = None,
    batch_status_log_path: Path | None = None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    generated_at = now_utc_iso()
    relevant_rows = annotate_rows_for_segments(rows_by_id, segments)
    overlap_kind_counts = Counter(str(event.get("overlap_kind", "unknown")) for event in overlap_events)
    overlap_action_counts = Counter(str(event.get("action", "unknown")) for event in overlap_events)
    relevance_reason_counts = Counter(str(event.get("reason", "unknown")) for event in relevance_events)
    index_payload = {
        "generated_at_utc": generated_at,
        "status": status,
        "self_user_id": self_user_id,
        "max_gap_hours": max_gap_hours,
        "messages_in": messages_in,
        "messages_out": len(relevant_rows),
        "coarse_windows": coarse_windows,
        "completed_coarse_windows": completed_coarse_windows,
        "candidate_windows": candidate_windows,
        "dropped_prefilter_windows": dropped_prefilter,
        "model_windows": model_windows,
        "planned_model_windows": planned_model_windows,
        "relevant_segments": len(segments),
        "failed_model_windows": len(failures),
        "overlapping_model_segments_total": len(overlap_events),
        "overlapping_model_segments_dropped": int(overlap_action_counts.get("dropped", 0)),
        "overlapping_model_segments_trimmed": int(overlap_action_counts.get("trimmed", 0)),
        "overlapping_model_segment_duplicates_dropped": int(overlap_kind_counts.get("duplicate_span", 0)),
        "overlapping_model_segment_nested_dropped": int(overlap_kind_counts.get("nested_span", 0)),
        "overlapping_model_segment_partial_prefix_trimmed": int(overlap_kind_counts.get("partial_prefix", 0)),
        "overlap_diagnostics_sample": overlap_events[:25],
        "model_segments_dropped_by_relevance": len(relevance_events),
        "model_segments_dropped_by_relevance_reasons": dict(sorted(relevance_reason_counts.items())),
        "relevance_gate_diagnostics_sample": relevance_events[:25],
        "dm_pair_count": len({str(row.get("dm_pair_id", "")) for row in annotated_rows}),
        "outputs": {
            "messages_relevant_conversations": str(out_jsonl),
            "conversation_segments": str(out_segments_json),
            "conversation_index": str(out_index_json),
            "conversation_segmentation_failures": str(out_failures_json),
            "gemini_batch_status": str(batch_status_log_path) if batch_status_log_path else "",
        },
    }
    _write_jsonl_atomic(out_jsonl, relevant_rows)
    _write_json_atomic(
        out_segments_json,
        {
            "generated_at_utc": generated_at,
            "status": status,
            "segments": segments,
            "overlap_diagnostics": overlap_events,
            "relevance_gate_diagnostics": relevance_events,
        },
    )
    _write_json_atomic(out_index_json, index_payload)
    _write_json_atomic(out_failures_json, {"generated_at_utc": generated_at, "status": status, "failures": failures})
    return relevant_rows, index_payload


def build_segmentation_failure(coarse_window: dict[str, Any], model_window: dict[str, Any], error: str) -> dict[str, Any]:
    return {
        "coarse_window_id": str(coarse_window.get("coarse_window_id", "")),
        "model_window_id": str(model_window.get("model_window_id", "")),
        "dm_pair_id": str(coarse_window.get("dm_pair_id", "")),
        "partner_id": str(coarse_window.get("partner_id", "unknown")),
        "partner_label": str(coarse_window.get("partner_label", "unknown")),
        "timestamp_start_utc": coarse_window.get("timestamp_start_utc"),
        "timestamp_end_utc": coarse_window.get("timestamp_end_utc"),
        "message_count": int(model_window.get("message_count", 0)),
        "model_window_message_count": int(model_window.get("message_count", 0)),
        "coarse_window_message_count": int(coarse_window.get("message_count", 0)),
        "reason": "model_conversation_segmentation_failed",
        "error": str(error),
        "recorded_at_utc": now_utc_iso(),
    }


def run(
    in_jsonl: Path,
    out_jsonl: Path,
    out_segments_json: Path,
    out_index_json: Path,
    out_failures_json: Path,
    in_pipeline_config_json: Path | None = None,
    in_entity_seed_json: Path | None = None,
) -> None:
    logger = get_logger(__name__)
    provider_config = load_config(in_pipeline_config_json)
    cfg = conversation_config(provider_config)
    rows = read_jsonl(in_jsonl)
    seed_entities = load_seed_entities(in_entity_seed_json)
    self_user_id = detect_self_user_id(rows, str(cfg.get("self_user_id", "")))
    logger.info(
        "Stage 04: loaded %d timeline row(s), self_user_id=%s, seed_entities=%d.",
        len(rows),
        self_user_id,
        len(seed_entities),
    )

    annotated_rows = annotate_dm_pairs(rows, self_user_id)
    rows_by_id = {str(row.get("message_id", "")): row for row in annotated_rows}
    coarse_windows = build_coarse_windows(annotated_rows, float(cfg["max_gap_hours"]))

    segments: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []
    overlap_events: list[dict[str, Any]] = []
    relevance_events: list[dict[str, Any]] = []
    dropped_prefilter = 0
    candidate_windows = 0
    model_windows = 0
    completed_coarse_windows = 0
    planned_model_windows: int | None = None
    progress_every = max(1, len(coarse_windows) // 10)
    batch_enabled = model_batch_enabled(provider_config, "stage_04_conversation_segmentation")
    batch_status_log_path = out_index_json.parent / "gemini_batch_status.jsonl" if batch_enabled else None
    if batch_status_log_path and batch_status_log_path.exists():
        batch_status_log_path.unlink()

    write_stage_outputs(
        out_jsonl=out_jsonl,
        out_segments_json=out_segments_json,
        out_index_json=out_index_json,
        out_failures_json=out_failures_json,
        rows_by_id=rows_by_id,
        segments=segments,
        failures=failures,
        overlap_events=overlap_events,
        relevance_events=relevance_events,
        annotated_rows=annotated_rows,
        self_user_id=self_user_id,
        max_gap_hours=float(cfg["max_gap_hours"]),
        messages_in=len(rows),
        coarse_windows=len(coarse_windows),
        completed_coarse_windows=completed_coarse_windows,
        candidate_windows=candidate_windows,
        dropped_prefilter=dropped_prefilter,
        model_windows=model_windows,
        status="in_progress",
        batch_status_log_path=batch_status_log_path,
    )

    if batch_enabled:
        model_jobs: list[tuple[dict[str, Any], dict[str, Any], int]] = []
        for window_idx, coarse_window in enumerate(coarse_windows, start=1):
            if bool(cfg.get("cheap_prefilter_enabled", False)) and not is_candidate_window(coarse_window, seed_entities):
                dropped_prefilter += 1
                continue
            candidate_windows += 1
            model_chunks = split_window_for_model(
                coarse_window,
                int(cfg["model_window_max_messages"]),
                int(cfg["model_window_max_chars"]),
            )
            for model_window in model_chunks:
                model_jobs.append((coarse_window, model_window, window_idx))

        planned_model_windows = len(model_jobs)
        max_batch_requests = model_batch_max_requests(provider_config, "stage_04_conversation_segmentation", default=100)
        initial_batch_requests = min(
            max_batch_requests,
            model_batch_initial_max_requests(provider_config, "stage_04_conversation_segmentation", default=max_batch_requests),
        )
        routing_cfg = provider_config.get("model_routing", {}) if isinstance(provider_config, dict) else {}
        tasks_cfg = routing_cfg.get("tasks", {}) if isinstance(routing_cfg, dict) else {}
        task_cfg = tasks_cfg.get("stage_04_conversation_segmentation", {}) if isinstance(tasks_cfg, dict) else {}
        abort_on_chunk_failure = bool(task_cfg.get("batch_abort_on_chunk_failure", True)) if isinstance(task_cfg, dict) else True
        write_stage_outputs(
            out_jsonl=out_jsonl,
            out_segments_json=out_segments_json,
            out_index_json=out_index_json,
            out_failures_json=out_failures_json,
            rows_by_id=rows_by_id,
            segments=segments,
            failures=failures,
            overlap_events=overlap_events,
            relevance_events=relevance_events,
            annotated_rows=annotated_rows,
            self_user_id=self_user_id,
            max_gap_hours=float(cfg["max_gap_hours"]),
            messages_in=len(rows),
            coarse_windows=len(coarse_windows),
            completed_coarse_windows=completed_coarse_windows,
            candidate_windows=candidate_windows,
            dropped_prefilter=dropped_prefilter,
            model_windows=model_windows,
            planned_model_windows=planned_model_windows,
            status="in_progress",
            batch_status_log_path=batch_status_log_path,
        )
        logger.info(
            "Stage 04: batch mode enabled; prepared %d candidate coarse window(s), %d model window(s), first chunk=%d, later chunks=%d.",
            candidate_windows,
            planned_model_windows,
            initial_batch_requests,
            max_batch_requests,
        )
        offset = 0
        chunk_index = 0
        while offset < len(model_jobs):
            chunk_index += 1
            chunk_limit = initial_batch_requests if chunk_index == 1 else max_batch_requests
            job_chunk = model_jobs[offset : offset + chunk_limit]
            logger.info(
                "Stage 04: submitting batch chunk %d (%d-%d of %d model windows).",
                chunk_index,
                offset + 1,
                offset + len(job_chunk),
                len(model_jobs),
            )
            chunk_windows = [model_window for _coarse, model_window, _idx in job_chunk]
            try:
                batch_outputs = segment_windows_with_batch(
                    chunk_windows,
                    provider_config,
                    cfg,
                    seed_entities,
                    relevance_events,
                    batch_status_log_path=batch_status_log_path,
                )
            except Exception as exc:
                model_windows += len(job_chunk)
                for coarse_window, model_window, _window_idx in job_chunk:
                    failures.append(build_segmentation_failure(coarse_window, model_window, f"batch_model_call_failed: {exc}"))
                completed_coarse_windows = max((window_idx for _coarse, _model, window_idx in job_chunk), default=completed_coarse_windows)
                write_stage_outputs(
                    out_jsonl=out_jsonl,
                    out_segments_json=out_segments_json,
                    out_index_json=out_index_json,
                    out_failures_json=out_failures_json,
                    rows_by_id=rows_by_id,
                    segments=segments,
                    failures=failures,
                    overlap_events=overlap_events,
                    relevance_events=relevance_events,
                    annotated_rows=annotated_rows,
                    self_user_id=self_user_id,
                    max_gap_hours=float(cfg["max_gap_hours"]),
                    messages_in=len(rows),
                    coarse_windows=len(coarse_windows),
                    completed_coarse_windows=completed_coarse_windows,
                    candidate_windows=candidate_windows,
                    dropped_prefilter=dropped_prefilter,
                    model_windows=model_windows,
                    planned_model_windows=len(model_jobs),
                    status="in_progress",
                    batch_status_log_path=batch_status_log_path,
                )
                if abort_on_chunk_failure:
                    raise RuntimeError(
                        "Stage 04 batch chunk failed; stopping before submitting additional chunks. "
                        f"chunk={chunk_index}, model_windows={offset + 1}-{offset + len(job_chunk)}, error={exc}"
                    ) from exc
            else:
                for coarse_window, model_window, _window_idx in job_chunk:
                    key = str(model_window.get("model_window_id", model_window.get("coarse_window_id", "")))
                    result = batch_outputs.get(key, {"segments": None, "error": "missing_batch_response"})
                    model_windows += 1
                    logger.info(
                        "Stage 04 model call %d/%d: batch_chunk=%d coarse_window=%s model_window=%s dm_pair=%s partner=%s messages=%d.",
                        model_windows,
                        len(model_jobs),
                        chunk_index,
                        coarse_window.get("coarse_window_id"),
                        model_window.get("model_window_id"),
                        coarse_window.get("dm_pair_id"),
                        coarse_window.get("partner_label"),
                        int(model_window.get("message_count", 0) or 0),
                    )
                    model_segments = result.get("segments")
                    if isinstance(model_segments, list):
                        segments.extend(materialize_segments(model_window, model_segments, overlap_events))
                    else:
                        failures.append(build_segmentation_failure(coarse_window, model_window, str(result.get("error", ""))))

            completed_coarse_windows = max((window_idx for _coarse, _model, window_idx in job_chunk), default=completed_coarse_windows)
            write_stage_outputs(
                out_jsonl=out_jsonl,
                out_segments_json=out_segments_json,
                out_index_json=out_index_json,
                out_failures_json=out_failures_json,
                rows_by_id=rows_by_id,
                segments=segments,
                failures=failures,
                overlap_events=overlap_events,
                relevance_events=relevance_events,
                annotated_rows=annotated_rows,
                self_user_id=self_user_id,
                max_gap_hours=float(cfg["max_gap_hours"]),
                messages_in=len(rows),
                coarse_windows=len(coarse_windows),
                completed_coarse_windows=completed_coarse_windows,
                candidate_windows=candidate_windows,
                dropped_prefilter=dropped_prefilter,
                model_windows=model_windows,
                planned_model_windows=len(model_jobs),
                status="in_progress",
                batch_status_log_path=batch_status_log_path,
            )
            logger.info(
                "Stage 04 batch progress: %d/%d model windows, segments=%d, failures=%d.",
                min(offset + len(job_chunk), len(model_jobs)),
                len(model_jobs),
                len(segments),
                len(failures),
            )
            offset += len(job_chunk)
        completed_coarse_windows = len(coarse_windows)
    else:
        planned_model_windows = 0
        for coarse_window in coarse_windows:
            if bool(cfg.get("cheap_prefilter_enabled", False)) and not is_candidate_window(coarse_window, seed_entities):
                continue
            planned_model_windows += len(
                split_window_for_model(
                    coarse_window,
                    int(cfg["model_window_max_messages"]),
                    int(cfg["model_window_max_chars"]),
                )
            )
        logger.info(
            "Stage 04: synchronous mode enabled; planned %d model window call(s) across %d coarse window(s).",
            planned_model_windows,
            len(coarse_windows),
        )
        for window_idx, coarse_window in enumerate(coarse_windows, start=1):
            if bool(cfg.get("cheap_prefilter_enabled", False)) and not is_candidate_window(coarse_window, seed_entities):
                dropped_prefilter += 1
                completed_coarse_windows = window_idx
                continue
            candidate_windows += 1
            model_chunks = split_window_for_model(
                coarse_window,
                int(cfg["model_window_max_messages"]),
                int(cfg["model_window_max_chars"]),
            )
            for model_window in model_chunks:
                model_windows += 1
                logger.info(
                    "Stage 04 model call %d/%d: coarse_window=%s model_window=%s dm_pair=%s partner=%s messages=%d.",
                    model_windows,
                    planned_model_windows,
                    coarse_window.get("coarse_window_id"),
                    model_window.get("model_window_id"),
                    coarse_window.get("dm_pair_id"),
                    coarse_window.get("partner_label"),
                    int(model_window.get("message_count", 0) or 0),
                )
                try:
                    model_segments = segment_window_with_model(
                        model_window,
                        provider_config,
                        cfg,
                        seed_entities,
                        relevance_events,
                    )
                    segments.extend(materialize_segments(model_window, model_segments, overlap_events))
                except Exception as exc:
                    failures.append(build_segmentation_failure(coarse_window, model_window, str(exc)))
                write_stage_outputs(
                    out_jsonl=out_jsonl,
                    out_segments_json=out_segments_json,
                    out_index_json=out_index_json,
                    out_failures_json=out_failures_json,
                    rows_by_id=rows_by_id,
                    segments=segments,
                    failures=failures,
                    overlap_events=overlap_events,
                    relevance_events=relevance_events,
                    annotated_rows=annotated_rows,
                    self_user_id=self_user_id,
                    max_gap_hours=float(cfg["max_gap_hours"]),
                    messages_in=len(rows),
                    coarse_windows=len(coarse_windows),
                    completed_coarse_windows=completed_coarse_windows,
                    candidate_windows=candidate_windows,
                    dropped_prefilter=dropped_prefilter,
                    model_windows=model_windows,
                    planned_model_windows=planned_model_windows,
                    status="in_progress",
                )
            completed_coarse_windows = window_idx
            write_stage_outputs(
                out_jsonl=out_jsonl,
                out_segments_json=out_segments_json,
                out_index_json=out_index_json,
                out_failures_json=out_failures_json,
                rows_by_id=rows_by_id,
                segments=segments,
                failures=failures,
                overlap_events=overlap_events,
                relevance_events=relevance_events,
                annotated_rows=annotated_rows,
                self_user_id=self_user_id,
                max_gap_hours=float(cfg["max_gap_hours"]),
                messages_in=len(rows),
                coarse_windows=len(coarse_windows),
                completed_coarse_windows=completed_coarse_windows,
                candidate_windows=candidate_windows,
                dropped_prefilter=dropped_prefilter,
                model_windows=model_windows,
                planned_model_windows=planned_model_windows,
                status="in_progress",
            )
            if window_idx % progress_every == 0 or window_idx == len(coarse_windows):
                logger.info(
                    "Stage 04 progress: %d/%d coarse windows, candidates=%d, segments=%d, failures=%d.",
                    window_idx,
                    len(coarse_windows),
                    candidate_windows,
                    len(segments),
                    len(failures),
                )

    relevant_rows, _index_payload = write_stage_outputs(
        out_jsonl=out_jsonl,
        out_segments_json=out_segments_json,
        out_index_json=out_index_json,
        out_failures_json=out_failures_json,
        rows_by_id=rows_by_id,
        segments=segments,
        failures=failures,
        overlap_events=overlap_events,
        relevance_events=relevance_events,
        annotated_rows=annotated_rows,
        self_user_id=self_user_id,
        max_gap_hours=float(cfg["max_gap_hours"]),
        messages_in=len(rows),
        coarse_windows=len(coarse_windows),
        completed_coarse_windows=completed_coarse_windows,
        candidate_windows=candidate_windows,
        dropped_prefilter=dropped_prefilter,
        model_windows=model_windows,
        planned_model_windows=planned_model_windows,
        status="complete",
        batch_status_log_path=batch_status_log_path,
    )

    logger.info(
        "Stage 04 complete: relevant_segments=%d, relevant_messages=%d, dropped_prefilter=%d, failures=%d.",
        len(segments),
        len(relevant_rows),
        dropped_prefilter,
        len(failures),
    )
    if not segments and failures:
        raise RuntimeError(
            "Stage 04 produced no relevant conversation segments and has model segmentation failures; "
            "stopping so the pipeline does not silently miss corpus data."
        )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--in-jsonl", type=Path, required=True)
    parser.add_argument("--out-jsonl", type=Path, required=True)
    parser.add_argument("--out-segments-json", type=Path, required=True)
    parser.add_argument("--out-index-json", type=Path, required=True)
    parser.add_argument("--out-failures-json", type=Path, required=True)
    parser.add_argument("--in-pipeline-config-json", type=Path, required=False, default=None)
    parser.add_argument("--in-entity-seed-json", type=Path, required=False, default=None)
    args = parser.parse_args()
    run(
        args.in_jsonl,
        args.out_jsonl,
        args.out_segments_json,
        args.out_index_json,
        args.out_failures_json,
        args.in_pipeline_config_json,
        args.in_entity_seed_json,
    )


if __name__ == "__main__":
    main()
