from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Any

from pipeline.common import get_logger, now_utc_iso, read_json, read_jsonl, stable_id, write_json, write_jsonl
from pipeline.model_provider import call_model_chat, model_call_kwargs


VALID_EVENT_KINDS = {"new", "change"}
VALID_CHANGE_TYPES = {
    "entity_introduced",
    "canonical_name",
    "quest",
    "relationship",
    "role",
    "background",
    "timeline",
    "meta",
    "open_question",
    "other",
}


def _list_string_values(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    out: list[str] = []
    for item in value:
        text = str(item).strip()
        if text and text not in out:
            out.append(text)
    return out


def _safe_confidence(value: Any, default: float = 0.0) -> float:
    try:
        return max(0.0, min(1.0, float(value)))
    except (TypeError, ValueError):
        return default


def _normalized_name_key(name: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", str(name or "").lower()).strip()


def _rows_by_conversation(rows: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        conversation_id = str(row.get("conversation_id", "")).strip()
        if conversation_id:
            grouped.setdefault(conversation_id, []).append(row)
    for conversation_id, items in grouped.items():
        items.sort(
            key=lambda x: (
                int(x.get("conversation_message_index", 0) or 0),
                str(x.get("timestamp_utc", "")),
                str(x.get("message_id", "")),
            )
        )
        grouped[conversation_id] = items
    return grouped


def ordered_segments(segments_payload: dict[str, Any]) -> list[dict[str, Any]]:
    segments = [segment for segment in segments_payload.get("segments", []) if isinstance(segment, dict)]
    return sorted(
        segments,
        key=lambda segment: (
            str(segment.get("timestamp_start_utc", "")),
            str(segment.get("timestamp_end_utc", "")),
            str(segment.get("dm_pair_id", "")),
            str(segment.get("conversation_id", "")),
        ),
    )


def conversation_messages_for_prompt(
    rows: list[dict[str, Any]],
    *,
    max_messages: int,
    max_message_chars: int,
    max_total_chars: int,
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    used_chars = 0
    for row in rows[:max_messages]:
        text = str(row.get("content_normalized") or row.get("content_raw") or "").strip()
        if max_message_chars > 0 and len(text) > max_message_chars:
            text = text[:max_message_chars].rstrip() + "..."
        used_chars += len(text)
        if max_total_chars > 0 and used_chars > max_total_chars and out:
            break
        out.append(
            {
                "message_id": str(row.get("message_id", "")),
                "timestamp_utc": str(row.get("timestamp_utc", "")),
                "conversation_message_index": row.get("conversation_message_index"),
                "author_id": str(row.get("author_id", "")),
                "author_label": str(row.get("author_label", "")),
                "text": text,
            }
        )
    return out


def build_entity_registry(
    resolved_entities_payload: dict[str, Any],
    alias_payload: dict[str, Any],
    seed_payload: dict[str, Any] | None = None,
) -> tuple[list[dict[str, Any]], dict[str, dict[str, Any]]]:
    entities: list[dict[str, Any]] = []
    for key in ("resolved_entities", "seed_only_entities"):
        for row in resolved_entities_payload.get(key, []) if isinstance(resolved_entities_payload.get(key), list) else []:
            if isinstance(row, dict):
                entities.append(row)
    if seed_payload:
        for row in seed_payload.get("entities", []) if isinstance(seed_payload.get("entities"), list) else []:
            if isinstance(row, dict):
                entities.append(row)

    by_name: dict[str, dict[str, Any]] = {}
    for entity in entities:
        entity_id = str(entity.get("entity_id", "")).strip()
        canonical = str(entity.get("canonical_name", "")).strip()
        if entity_id and canonical:
            by_name[_normalized_name_key(canonical)] = entity
        for alias in entity.get("aliases", []) if isinstance(entity.get("aliases"), list) else []:
            alias_text = str(alias).strip()
            if alias_text:
                by_name[_normalized_name_key(alias_text)] = entity
    for alias in alias_payload.get("aliases", []) if isinstance(alias_payload.get("aliases"), list) else []:
        if not isinstance(alias, dict):
            continue
        alias_text = str(alias.get("alias_text", "")).strip()
        entity_id = str(alias.get("entity_id", "")).strip()
        if not alias_text or not entity_id:
            continue
        entity = next((row for row in entities if str(row.get("entity_id", "")) == entity_id), None)
        if entity is not None:
            by_name[_normalized_name_key(alias_text)] = entity
    return entities, by_name


def build_snippet_index(snippets: list[dict[str, Any]]) -> dict[str, list[str]]:
    by_message: dict[str, list[str]] = {}
    for snippet in snippets:
        snippet_id = str(snippet.get("snippet_id", "")).strip()
        if not snippet_id:
            continue
        message_ids = _list_string_values(snippet.get("message_ids", []))
        if snippet.get("message_id"):
            message_ids.append(str(snippet.get("message_id")))
        for message_id in message_ids:
            by_message.setdefault(message_id, [])
            if snippet_id not in by_message[message_id]:
                by_message[message_id].append(snippet_id)
    return by_message


def merge_segment_streams(
    strict_segments: list[dict[str, Any]],
    rescue_segments: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    merged: list[dict[str, Any]] = []
    for segment in strict_segments:
        row = dict(segment)
        row["source_scope"] = "strict_accept"
        merged.append(row)
    for segment in rescue_segments:
        row = dict(segment)
        row["source_scope"] = "theme_rescue"
        merged.append(row)
    return sorted(
        merged,
        key=lambda segment: (
            str(segment.get("timestamp_start_utc", "")),
            str(segment.get("timestamp_end_utc", "")),
            str(segment.get("source_scope", "")),
            str(segment.get("conversation_id", "")),
        ),
    )


def prior_entity_context(
    entries: list[dict[str, Any]],
    *,
    per_entity_limit: int,
) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for entry in entries:
        entity_id = str(entry.get("subject_entity_id", "")).strip()
        label_key = _normalized_name_key(str(entry.get("subject_label", "")))
        keys = []
        if entity_id:
            keys.append(entity_id)
        if label_key:
            keys.append(label_key)
        for key in keys:
            grouped.setdefault(key, []).append(entry)
    out: dict[str, list[dict[str, Any]]] = {}
    for key, rows in grouped.items():
        out[key] = rows[-per_entity_limit:]
    return out


def build_ledger_prompt(
    *,
    segment: dict[str, Any],
    rows: list[dict[str, Any]],
    global_sequence: int,
    entity_registry: list[dict[str, Any]],
    prior_context: dict[str, list[dict[str, Any]]],
    cfg: dict[str, Any],
    validation_feedback: str = "",
) -> str:
    messages = conversation_messages_for_prompt(
        rows,
        max_messages=int(cfg.get("max_messages_per_prompt", 80)),
        max_message_chars=int(cfg.get("max_message_chars", 700)),
        max_total_chars=int(cfg.get("max_prompt_message_chars", 20000)),
    )
    segment_preview = {
        "conversation_id": segment.get("conversation_id"),
        "source_scope": segment.get("source_scope"),
        "global_sequence": global_sequence,
        "dm_pair_id": segment.get("dm_pair_id"),
        "partner_id": segment.get("partner_id"),
        "partner_label": segment.get("partner_label"),
        "track": segment.get("track"),
        "topic_label": segment.get("topic_label"),
        "topic_summary": segment.get("topic_summary"),
        "anchor_entities": segment.get("anchor_entities", []),
        "timestamp_start_utc": segment.get("timestamp_start_utc"),
        "timestamp_end_utc": segment.get("timestamp_end_utc"),
    }
    compact_entities = [
        {
            "entity_id": entity.get("entity_id"),
            "canonical_name": entity.get("canonical_name"),
            "entity_type": entity.get("entity_type"),
            "aliases": (entity.get("aliases", [])[:8] if isinstance(entity.get("aliases"), list) else []),
        }
        for entity in entity_registry[:120]
    ]
    return f"""Write Theriac lore development ledger entries for one conversation segment.
Return strict JSON only. These entries are machine-facing development history for later card synthesis, not final canon.

Critical rules:
- Emit 0 or more ledger entries. Each entry must be a delta, not a conversation recap.
- event_kind "new" only when a durable entity/concept is first introduced to project lore in chronological order.
- event_kind "change" for renames, quest beats, role shifts, relationship updates, timeline updates, and refinements.
- Use change_type values such as entity_introduced, canonical_name, quest, relationship, role, background, timeline, meta, open_question, other.
- For renames, populate before and after (example: before "Loss", after "Enoch").
- headline must read like: "Entity — Manunggal — initial description …" or "Loss — given canonical name Enoch".
- Link subject_entity_id when the resolved entity registry contains a match; otherwise leave subject_entity_id empty and use subject_label.
- Cite exact supporting_message_ids from this conversation only.
- If nothing durable changed, return an empty entries array.

Segment metadata:
{json.dumps(segment_preview, ensure_ascii=False, indent=2)}

Resolved entity registry:
{json.dumps(compact_entities, ensure_ascii=False, indent=2)}

Prior ledger entries for entities already touched (most recent first within each entity):
{json.dumps(prior_context, ensure_ascii=False, indent=2)}

Conversation messages:
{json.dumps(messages, ensure_ascii=False, indent=2)}

Previous validation feedback to fix:
{validation_feedback or "none"}

Return JSON object:
{{
  "entries": [
    {{
      "event_kind": "new|change",
      "change_type": "entity_introduced|canonical_name|quest|relationship|role|background|timeline|meta|open_question|other",
      "subject_entity_id": "",
      "subject_label": "display name at time of entry",
      "headline": "one-line delta",
      "before": "",
      "after": "",
      "related_entity_ids": [],
      "supporting_message_ids": ["exact message_id values"],
      "confidence": 0.0
    }}
  ]
}}
"""


def _resolve_subject_entity_id(subject_label: str, subject_entity_id: str, by_name: dict[str, dict[str, Any]]) -> str:
    explicit = str(subject_entity_id or "").strip()
    if explicit:
        return explicit
    entity = by_name.get(_normalized_name_key(subject_label))
    if entity is None:
        return ""
    return str(entity.get("entity_id", "")).strip()


def normalize_ledger_entries(
    payload: Any,
    *,
    segment: dict[str, Any],
    global_sequence: int,
    allowed_message_ids: set[str],
    by_name: dict[str, dict[str, Any]],
    snippet_by_message: dict[str, list[str]],
) -> list[dict[str, Any]]:
    if not isinstance(payload, dict):
        return []
    raw_entries = payload.get("entries")
    if not isinstance(raw_entries, list):
        return []
    out: list[dict[str, Any]] = []
    conversation_id = str(segment.get("conversation_id", "")).strip()
    source_scope = str(segment.get("source_scope", "")).strip()
    timestamp_utc = str(segment.get("timestamp_start_utc", "") or segment.get("timestamp_end_utc", ""))
    for raw in raw_entries:
        if not isinstance(raw, dict):
            continue
        event_kind = str(raw.get("event_kind", "")).strip().lower()
        change_type = str(raw.get("change_type", "")).strip().lower()
        subject_label = str(raw.get("subject_label", "")).strip()
        headline = str(raw.get("headline", "")).strip()
        if event_kind not in VALID_EVENT_KINDS or change_type not in VALID_CHANGE_TYPES or not subject_label or not headline:
            continue
        supporting_message_ids = [
            message_id
            for message_id in _list_string_values(raw.get("supporting_message_ids", []))
            if message_id in allowed_message_ids
        ]
        if not supporting_message_ids:
            continue
        subject_entity_id = _resolve_subject_entity_id(
            subject_label,
            str(raw.get("subject_entity_id", "")).strip(),
            by_name,
        )
        supporting_snippet_ids: list[str] = []
        for message_id in supporting_message_ids:
            supporting_snippet_ids.extend(snippet_by_message.get(message_id, []))
        supporting_snippet_ids = _list_string_values(supporting_snippet_ids)
        entry_id = stable_id(
            "ledger_entry",
            str(global_sequence),
            conversation_id,
            event_kind,
            change_type,
            subject_label,
            headline,
            *supporting_message_ids[:3],
        )
        out.append(
            {
                "entry_id": entry_id,
                "global_sequence": global_sequence,
                "timestamp_utc": timestamp_utc,
                "event_kind": event_kind,
                "change_type": change_type,
                "subject_entity_id": subject_entity_id,
                "subject_label": subject_label,
                "headline": headline,
                "before": str(raw.get("before", "")).strip(),
                "after": str(raw.get("after", "")).strip(),
                "related_entity_ids": _list_string_values(raw.get("related_entity_ids", [])),
                "source_scope": source_scope,
                "source_conversation_id": conversation_id,
                "source_segment_id": conversation_id,
                "supporting_message_ids": supporting_message_ids,
                "supporting_snippet_ids": supporting_snippet_ids,
                "confidence": _safe_confidence(raw.get("confidence"), 0.75),
                "recorded_at_utc": now_utc_iso(),
            }
        )
    return out


def extract_ledger_entries_with_model(
    *,
    segment: dict[str, Any],
    rows: list[dict[str, Any]],
    global_sequence: int,
    total_segments: int,
    prior_entries: list[dict[str, Any]],
    entity_registry: list[dict[str, Any]],
    by_name: dict[str, dict[str, Any]],
    snippet_by_message: dict[str, list[str]],
    provider_config: dict[str, Any],
    cfg: dict[str, Any],
) -> list[dict[str, Any]]:
    logger = get_logger(__name__)
    allowed_message_ids = {str(row.get("message_id", "")) for row in rows if str(row.get("message_id", ""))}
    per_entity_limit = max(1, int(cfg.get("previous_context_entries_per_entity", 4)))
    prior_context = prior_entity_context(prior_entries, per_entity_limit=per_entity_limit)
    validation_feedback = ""
    retries = max(0, int(cfg.get("validation_retries", 1)))
    for attempt in range(retries + 1):
        prompt = build_ledger_prompt(
            segment=segment,
            rows=rows,
            global_sequence=global_sequence,
            entity_registry=entity_registry,
            prior_context=prior_context,
            cfg=cfg,
            validation_feedback=validation_feedback,
        )
        logger.info(
            "Stage 07 ledger model call %d/%d: conversation_id=%s scope=%s messages=%d.",
            global_sequence,
            total_segments,
            segment.get("conversation_id"),
            segment.get("source_scope"),
            len(rows),
        )
        response = call_model_chat(
            prompt,
            **model_call_kwargs(provider_config, "stage_05_lore_development_ledger"),
        )
        try:
            payload = json.loads(response) if isinstance(response, str) else response
            entries = normalize_ledger_entries(
                payload,
                segment=segment,
                global_sequence=global_sequence,
                allowed_message_ids=allowed_message_ids,
                by_name=by_name,
                snippet_by_message=snippet_by_message,
            )
            return entries
        except json.JSONDecodeError as exc:
            validation_feedback = f"invalid_json: {exc}"
            if attempt >= retries:
                raise RuntimeError(f"invalid_ledger_json: {exc}") from exc
    return []


def group_entity_history(entries: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for entry in sorted(entries, key=lambda row: (int(row.get("global_sequence", 0) or 0), str(row.get("entry_id", "")))):
        entity_id = str(entry.get("subject_entity_id", "")).strip()
        key = entity_id or f"label:{_normalized_name_key(str(entry.get('subject_label', '')))}"
        grouped.setdefault(key, []).append(entry)
    return grouped


def render_entity_history_lines(entries: list[dict[str, Any]]) -> list[str]:
    lines: list[str] = []
    for entry in entries:
        prefix = "New" if entry.get("event_kind") == "new" else "Change"
        label = str(entry.get("subject_label", "")).strip()
        headline = str(entry.get("headline", "")).strip()
        if label and headline.lower().startswith(label.lower()):
            lines.append(f"{prefix}: {headline}")
        else:
            lines.append(f"{prefix}: {label} — {headline}")
    return lines


def ledger_config(provider_config: dict[str, Any]) -> dict[str, Any]:
    cfg = provider_config.get("lore_development_ledger", {}) if isinstance(provider_config.get("lore_development_ledger"), dict) else {}
    return {
        "max_messages_per_prompt": int(cfg.get("max_messages_per_prompt", 80)),
        "max_message_chars": int(cfg.get("max_message_chars", 700)),
        "max_prompt_message_chars": int(cfg.get("max_prompt_message_chars", 20000)),
        "previous_context_entries_per_entity": int(cfg.get("previous_context_entries_per_entity", 4)),
        "validation_retries": int(cfg.get("validation_retries", 1)),
        "provider_retries": int(cfg.get("provider_retries", 2)),
        "retry_sleep_seconds": float(cfg.get("retry_sleep_seconds", 2)),
        "provider_retry_sleep_seconds": float(cfg.get("provider_retry_sleep_seconds", 15)),
    }


def load_existing_outputs(
    out_index_json: Path,
    out_jsonl: Path,
    out_failures_json: Path,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], set[str]]:
    entries: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []
    completed_segment_ids: set[str] = set()
    if out_jsonl.exists():
        entries = [row for row in read_jsonl(out_jsonl) if isinstance(row, dict)]
    if out_failures_json.exists():
        payload = read_json(out_failures_json)
        failures = payload.get("failures", []) if isinstance(payload, dict) else []
        for failure in failures:
            if isinstance(failure, dict):
                segment_id = str(failure.get("source_segment_id", "")).strip()
                if segment_id:
                    completed_segment_ids.add(segment_id)
    if out_index_json.exists():
        payload = read_json(out_index_json)
        for segment_id in _list_string_values(payload.get("completed_segment_ids", [])):
            completed_segment_ids.add(segment_id)
    return entries, failures, completed_segment_ids


def write_outputs(
    *,
    out_index_json: Path,
    out_jsonl: Path,
    out_history_json: Path,
    out_failures_json: Path,
    entries: list[dict[str, Any]],
    failures: list[dict[str, Any]],
    completed_segment_ids: set[str],
    total_segments: int,
    status: str,
) -> None:
    grouped = group_entity_history(entries)
    history_payload = {
        "generated_at_utc": now_utc_iso(),
        "status": status,
        "entry_count": len(entries),
        "entity_count": len(grouped),
        "by_entity": grouped,
        "grouped": {
            "new": [entry for entry in entries if entry.get("event_kind") == "new"],
            "change": [entry for entry in entries if entry.get("event_kind") == "change"],
        },
    }
    index_payload = {
        "generated_at_utc": now_utc_iso(),
        "status": status,
        "entry_count": len(entries),
        "segment_count": total_segments,
        "completed_segment_count": len(completed_segment_ids),
        "failure_count": len(failures),
        "completed_segment_ids": sorted(completed_segment_ids),
    }
    write_json(out_index_json, index_payload)
    write_jsonl(out_jsonl, entries)
    write_json(out_history_json, history_payload)
    write_json(out_failures_json, {"generated_at_utc": now_utc_iso(), "status": status, "failures": failures})


def run(
    in_relevant_messages_jsonl: Path,
    in_rescue_messages_jsonl: Path | None,
    in_segments_json: Path,
    in_rescue_segments_json: Path | None,
    in_resolved_entities_json: Path,
    in_alias_json: Path,
    in_snippets_jsonl: Path,
    out_index_json: Path,
    out_jsonl: Path,
    out_history_json: Path,
    out_failures_json: Path,
    in_pipeline_config_json: Path | None = None,
    in_seed_json: Path | None = None,
) -> None:
    logger = get_logger(__name__)
    provider_config = read_json(in_pipeline_config_json) if in_pipeline_config_json and in_pipeline_config_json.exists() else {}
    cfg = ledger_config(provider_config)

    relevant_rows = read_jsonl(in_relevant_messages_jsonl)
    rescue_rows = read_jsonl(in_rescue_messages_jsonl) if in_rescue_messages_jsonl and in_rescue_messages_jsonl.exists() else []
    rows_by_conversation = _rows_by_conversation(relevant_rows + rescue_rows)

    strict_segments = ordered_segments(read_json(in_segments_json))
    rescue_segments: list[dict[str, Any]] = []
    if in_rescue_segments_json and in_rescue_segments_json.exists():
        rescue_segments = ordered_segments(read_json(in_rescue_segments_json))
    segments = merge_segment_streams(strict_segments, rescue_segments)

    resolved_payload = read_json(in_resolved_entities_json)
    alias_payload = read_json(in_alias_json)
    seed_payload = read_json(in_seed_json) if in_seed_json and in_seed_json.exists() else None
    entity_registry, by_name = build_entity_registry(resolved_payload, alias_payload, seed_payload)
    snippet_by_message = build_snippet_index(read_jsonl(in_snippets_jsonl))

    entries, failures, completed_segment_ids = load_existing_outputs(out_index_json, out_jsonl, out_failures_json)
    progress_every = max(1, len(segments) // 10)
    logger.info(
        "Stage 07: building lore development ledger for %d segment(s); existing_entries=%d failures=%d completed_segments=%d.",
        len(segments),
        len(entries),
        len(failures),
        len(completed_segment_ids),
    )
    write_outputs(
        out_index_json=out_index_json,
        out_jsonl=out_jsonl,
        out_history_json=out_history_json,
        out_failures_json=out_failures_json,
        entries=entries,
        failures=failures,
        completed_segment_ids=completed_segment_ids,
        total_segments=len(segments),
        status="in_progress",
    )

    for global_sequence, segment in enumerate(segments, start=1):
        segment_id = str(segment.get("conversation_id", "")).strip()
        if not segment_id or segment_id in completed_segment_ids:
            continue
        rows = rows_by_conversation.get(segment_id, [])
        if not rows:
            message_ids = _list_string_values(segment.get("message_ids", []))
            rows = [
                row
                for message_id in message_ids
                for row in [next((item for item in relevant_rows + rescue_rows if str(item.get("message_id", "")) == message_id), None)]
                if row is not None
            ]
        if not rows:
            failures.append(
                {
                    "source_segment_id": segment_id,
                    "global_sequence": global_sequence,
                    "source_scope": segment.get("source_scope"),
                    "reason": "missing_segment_messages",
                    "error": "No message rows found for segment.",
                    "recorded_at_utc": now_utc_iso(),
                }
            )
            completed_segment_ids.add(segment_id)
            continue
        try:
            new_entries = extract_ledger_entries_with_model(
                segment=segment,
                rows=rows,
                global_sequence=global_sequence,
                total_segments=len(segments),
                prior_entries=entries,
                entity_registry=entity_registry,
                by_name=by_name,
                snippet_by_message=snippet_by_message,
                provider_config=provider_config,
                cfg=cfg,
            )
            entries.extend(new_entries)
            completed_segment_ids.add(segment_id)
        except Exception as exc:
            failures.append(
                {
                    "source_segment_id": segment_id,
                    "global_sequence": global_sequence,
                    "source_scope": segment.get("source_scope"),
                    "reason": "model_ledger_failed",
                    "error": str(exc),
                    "recorded_at_utc": now_utc_iso(),
                }
            )
            completed_segment_ids.add(segment_id)
            logger.warning("Stage 07 ledger failed segment_id=%s seq=%d error=%s", segment_id, global_sequence, exc)

        write_outputs(
            out_index_json=out_index_json,
            out_jsonl=out_jsonl,
            out_history_json=out_history_json,
            out_failures_json=out_failures_json,
            entries=entries,
            failures=failures,
            completed_segment_ids=completed_segment_ids,
            total_segments=len(segments),
            status="in_progress",
        )
        if global_sequence % progress_every == 0 or global_sequence == len(segments):
            logger.info(
                "Stage 07 progress: %d/%d segments, entries=%d, failures=%d.",
                global_sequence,
                len(segments),
                len(entries),
                len(failures),
            )

    write_outputs(
        out_index_json=out_index_json,
        out_jsonl=out_jsonl,
        out_history_json=out_history_json,
        out_failures_json=out_failures_json,
        entries=entries,
        failures=failures,
        completed_segment_ids=completed_segment_ids,
        total_segments=len(segments),
        status="complete",
    )
    logger.info("Stage 07 complete: entries=%d failures=%d entities=%d.", len(entries), len(failures), len(group_entity_history(entries)))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--in-relevant-messages-jsonl", type=Path, required=True)
    parser.add_argument("--in-rescue-messages-jsonl", type=Path, required=False, default=None)
    parser.add_argument("--in-segments-json", type=Path, required=True)
    parser.add_argument("--in-rescue-segments-json", type=Path, required=False, default=None)
    parser.add_argument("--in-resolved-entities-json", type=Path, required=True)
    parser.add_argument("--in-alias-json", type=Path, required=True)
    parser.add_argument("--in-snippets-jsonl", type=Path, required=True)
    parser.add_argument("--out-index-json", type=Path, required=True)
    parser.add_argument("--out-jsonl", type=Path, required=True)
    parser.add_argument("--out-history-json", type=Path, required=True)
    parser.add_argument("--out-failures-json", type=Path, required=True)
    parser.add_argument("--in-pipeline-config-json", type=Path, required=False, default=None)
    parser.add_argument("--in-seed-json", type=Path, required=False, default=None)
    args = parser.parse_args()
    run(
        args.in_relevant_messages_jsonl,
        args.in_rescue_messages_jsonl,
        args.in_segments_json,
        args.in_rescue_segments_json,
        args.in_resolved_entities_json,
        args.in_alias_json,
        args.in_snippets_jsonl,
        args.out_index_json,
        args.out_jsonl,
        args.out_history_json,
        args.out_failures_json,
        args.in_pipeline_config_json,
        args.in_seed_json,
    )


if __name__ == "__main__":
    main()
