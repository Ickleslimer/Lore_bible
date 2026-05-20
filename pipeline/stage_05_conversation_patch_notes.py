from __future__ import annotations

import argparse
import json
import re
import time
from pathlib import Path
from typing import Any

from pipeline.common import get_logger, now_utc_iso, read_json, read_jsonl, stable_id, write_json, write_jsonl
from pipeline.mixtral_anchor_provider import call_mixtral_chat, get_mixtral_runtime_status, model_call_kwargs


PACING_SKIP_REASONS = {"provider_locked", "adaptive_pacing", "rate_limit_cooldown"}
WEAK_INDIRECT_MAX_MESSAGES = 3
DIRECT_THERIAC_TERMS = {
    "theriac",
    "game",
    "quest",
    "quests",
    "character",
    "characters",
    "plot",
    "story",
    "canon",
    "lore",
    "route",
    "routes",
    "path",
    "paths",
    "boss",
    "mechanic",
    "mechanics",
    "player",
    "protagonist",
    "antagonist",
    "visual novel",
    "vn",
    "dialogue",
    "scene",
    "ending",
    "lab",
    "hectr",
    "ruinr",
    "achilles",
    "gfns",
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


def _provider_wait_seconds(reason: str, status: dict[str, Any], fallback_seconds: float) -> float:
    now_s = time.time()
    next_attempt = float(status.get("next_mistral_attempt_epoch_s") or 0.0)
    rate_limited_until = float(status.get("rate_limited_until_epoch_s") or 0.0)
    target = next_attempt
    if reason in {"rate_limit_cooldown", "rate_limited_429"}:
        target = max(rate_limited_until, next_attempt)
    if target > now_s:
        return max(0.1, target - now_s)
    return max(0.0, fallback_seconds)


def _rows_by_conversation(rows: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        conversation_id = str(row.get("conversation_id", "")).strip()
        if conversation_id:
            grouped.setdefault(conversation_id, []).append(row)
    for conversation_id, items in grouped.items():
        items.sort(key=lambda x: (str(x.get("conversation_message_index", "")), str(x.get("timestamp_utc", "")), str(x.get("message_id", ""))))
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


def previous_context_for_prompt(notes: list[dict[str, Any]], max_notes: int) -> list[dict[str, Any]]:
    if max_notes <= 0:
        return []
    context = []
    for note in notes[-max_notes:]:
        context.append(
            {
                "patch_note_id": note.get("patch_note_id"),
                "global_conversation_index": note.get("global_conversation_index"),
                "conversation_id": note.get("conversation_id"),
                "partner_label": note.get("partner_label"),
                "timestamp_start_utc": note.get("timestamp_start_utc"),
                "topic_label": note.get("topic_label"),
                "summary": note.get("summary"),
                "lore_developments": [
                    str(item.get("description", "")) for item in note.get("lore_developments", [])[:4] if isinstance(item, dict)
                ],
                "meta_developments": [
                    str(item.get("description", "")) for item in note.get("meta_developments", [])[:3] if isinstance(item, dict)
                ],
                "open_questions": [
                    str(item.get("question", "")) for item in note.get("open_questions", [])[:3] if isinstance(item, dict)
                ],
            }
        )
    return context


def build_patch_note_prompt(
    *,
    segment: dict[str, Any],
    rows: list[dict[str, Any]],
    global_index: int,
    prior_context: list[dict[str, Any]],
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
        "global_conversation_index": global_index,
        "dm_pair_id": segment.get("dm_pair_id"),
        "partner_id": segment.get("partner_id"),
        "partner_label": segment.get("partner_label"),
        "track": segment.get("track"),
        "topic_label": segment.get("topic_label"),
        "topic_summary": segment.get("topic_summary"),
        "topic_shift_reason": segment.get("topic_shift_reason"),
        "anchor_entities": segment.get("anchor_entities", []),
        "timestamp_start_utc": segment.get("timestamp_start_utc"),
        "timestamp_end_utc": segment.get("timestamp_end_utc"),
        "model_confidence": segment.get("model_confidence"),
    }
    return f"""Write chronological development patch notes for one THERIAC-relevant conversation.
Return strict JSON only. These notes are not final canon; they are an ordered evidence map for later claim extraction.

Critical ordering rule:
- This conversation is item {global_index} in the global chronological order across all 1:1 DM pairs.
- Use the prior patch-note context only as earlier context. Never infer from later conversations.
- When this conversation repeats or independently communicates a development from a prior note, mark it as reinforcement instead of contradiction.
- Do not collapse separate partner conversations into a single event. Preserve who received which development and when.
- Cite exact message_id values for every development or open question.
- If the conversation is only a tiny external reference, link, joke, vibe, inspiration fragment, or vague comparison, and the messages do not explicitly connect it to THERIAC, a THERIAC entity, a quest, a mechanic, a scene, a character, canon, lore, production, or design, return status "no_durable_development".
- For status "no_durable_development", use a brief summary explaining why it is not durable, and return empty development/update/question arrays. Do not invent a location, concept, character, or lore entity from an indirect reference.
- Do not create lore_developments from inspiration/vibes alone. A lore development requires explicit in-message canon/entity/plot relevance.
- Real-world team members, artists, writers, composers, consultants, programmers, Discord partners, and production collaborators belong in meta_developments only. Do not put them in entity_updates or relationship_updates unless the messages explicitly define an in-world fictional entity with that name.
- Characters, factions, locations, quests, mechanics, and lore from other media (for example Warframe characters such as Erra, Hunhow, Alad V, Nef Anyo, or Parvos Granum, or Zenless Zone Zero factions such as Sons of Calydon) belong in meta_developments as reference/inspiration context only. Do not put external-media names in lore_developments, entity_updates, relationship_updates, or timeline_updates unless the messages explicitly say the name has become an in-world THERIAC entity.

Segment metadata:
{json.dumps(segment_preview, ensure_ascii=False, indent=2)}

Prior patch-note context, already in global order:
{json.dumps(prior_context, ensure_ascii=False, indent=2)}

Conversation messages:
{json.dumps(messages, ensure_ascii=False, indent=2)}

Previous validation feedback to fix:
{validation_feedback or "none"}

Return JSON object:
{{
  "status": "draft|no_durable_development",
  "summary": "short chronological summary of what this conversation develops",
  "lore_developments": [
    {{
      "development_type": "new|refinement|reinforcement|contradiction|open_question|other",
      "entity_names": ["entity or concept names"],
      "description": "what changed or was established in lore/canon terms",
      "supporting_message_ids": ["exact message_id values"],
      "confidence": 0.0
    }}
  ],
  "meta_developments": [
    {{
      "development_type": "production|design|marketing|scope|staffing|reinforcement|other",
      "description": "what changed or was established about the game/design/process",
      "supporting_message_ids": ["exact message_id values"],
      "confidence": 0.0
    }}
  ],
  "entity_updates": [
    {{
      "entity_name": "name",
      "update_type": "introduced|alias|rename|role_change|relationship_change|classification_change|reinforced|contradicted|other",
      "description": "concise update",
      "supporting_message_ids": ["exact message_id values"],
      "confidence": 0.0
    }}
  ],
  "relationship_updates": [
    {{
      "source_entity": "name",
      "target_entity": "name",
      "relationship_type": "relationship label",
      "description": "concise relationship update",
      "supporting_message_ids": ["exact message_id values"],
      "confidence": 0.0
    }}
  ],
  "timeline_updates": [
    {{
      "description": "chronology or plot-timeline update",
      "supporting_message_ids": ["exact message_id values"],
      "confidence": 0.0
    }}
  ],
  "open_questions": [
    {{
      "question": "unresolved canon/design question",
      "supporting_message_ids": ["exact message_id values"]
    }}
  ],
  "possible_contradictions": [
    {{
      "description": "possible contradiction or tension, only if actually present",
      "conflicts_with_prior_patch_note_ids": ["prior patch_note_id values only"],
      "supporting_message_ids": ["exact message_id values"],
      "confidence": 0.0
    }}
  ],
  "reinforces_prior_patch_note_ids": ["prior patch_note_id values only"],
  "confidence": 0.0
}}
"""


def _filter_supporting_message_ids(value: Any, allowed_ids: set[str]) -> list[str]:
    out: list[str] = []
    if not isinstance(value, list):
        return out
    for item in value:
        message_id = str(item).strip()
        if message_id in allowed_ids and message_id not in out:
            out.append(message_id)
    return out


def _normalize_dict_list(value: Any, allowed_ids: set[str], fields: set[str]) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    out: list[dict[str, Any]] = []
    for item in value:
        if not isinstance(item, dict):
            continue
        cleaned = {key: item.get(key) for key in fields if key in item}
        if "supporting_message_ids" in fields:
            cleaned["supporting_message_ids"] = _filter_supporting_message_ids(item.get("supporting_message_ids"), allowed_ids)
        if "entity_names" in fields:
            cleaned["entity_names"] = _list_string_values(item.get("entity_names", []))
        if "conflicts_with_prior_patch_note_ids" in fields:
            cleaned["conflicts_with_prior_patch_note_ids"] = _list_string_values(item.get("conflicts_with_prior_patch_note_ids", []))
        if "confidence" in fields:
            cleaned["confidence"] = _safe_confidence(item.get("confidence"), 0.5)
        for key, value in list(cleaned.items()):
            if key in {"supporting_message_ids", "entity_names", "conflicts_with_prior_patch_note_ids", "confidence"}:
                continue
            cleaned[key] = str(value or "").strip()
        if any(str(v).strip() for k, v in cleaned.items() if k not in {"supporting_message_ids", "entity_names", "confidence"}):
            out.append(cleaned)
    return out


def _combined_message_text(rows: list[dict[str, Any]]) -> str:
    return "\n".join(str(row.get("content_normalized") or row.get("content_raw") or "") for row in rows).lower()


def _has_direct_theriac_signal(rows: list[dict[str, Any]]) -> bool:
    text = _combined_message_text(rows)
    for term in DIRECT_THERIAC_TERMS:
        if " " in term:
            if term in text:
                return True
        elif re.search(rf"\b{re.escape(term)}\b", text):
            return True
    return False


def _is_weak_indirect_segment(segment: dict[str, Any], rows: list[dict[str, Any]]) -> bool:
    message_count = int(segment.get("message_count", len(rows)) or len(rows))
    if message_count > WEAK_INDIRECT_MAX_MESSAGES:
        return False
    return not _has_direct_theriac_signal(rows)


def _clear_durable_developments(note: dict[str, Any], reason: str) -> None:
    note["status"] = "no_durable_development"
    note["summary"] = reason
    note["lore_developments"] = []
    note["meta_developments"] = []
    note["entity_updates"] = []
    note["relationship_updates"] = []
    note["timeline_updates"] = []
    note["open_questions"] = []
    note["possible_contradictions"] = []
    note["reinforces_prior_patch_note_ids"] = []
    note["confidence"] = min(float(note.get("confidence", 0.5) or 0.5), 0.35)


def normalize_patch_payload(
    payload: dict[str, Any],
    *,
    segment: dict[str, Any],
    rows: list[dict[str, Any]],
    global_index: int,
    prior_note_ids: set[str],
) -> dict[str, Any]:
    allowed_ids = {str(row.get("message_id", "")) for row in rows}
    conversation_id = str(segment.get("conversation_id", ""))
    patch_note_id = stable_id("conversation_patch_note", conversation_id)
    payload_status = str(payload.get("status", "draft")).strip().lower()
    status = payload_status if payload_status in {"draft", "no_durable_development"} else "draft"
    note = {
        "patch_note_id": patch_note_id,
        "conversation_id": conversation_id,
        "global_conversation_index": global_index,
        "dm_pair_id": str(segment.get("dm_pair_id", "")),
        "partner_id": str(segment.get("partner_id", "")),
        "partner_label": str(segment.get("partner_label", "")),
        "track": str(segment.get("track", "")),
        "topic_label": str(segment.get("topic_label", "")),
        "topic_summary": str(segment.get("topic_summary", "")),
        "timestamp_start_utc": str(segment.get("timestamp_start_utc", "")),
        "timestamp_end_utc": str(segment.get("timestamp_end_utc", "")),
        "message_count": int(segment.get("message_count", len(rows)) or len(rows)),
        "message_ids": [str(row.get("message_id", "")) for row in rows if str(row.get("message_id", ""))],
        "anchor_entities": _list_string_values(segment.get("anchor_entities", [])),
        "summary": str(payload.get("summary", "")).strip(),
        "lore_developments": _normalize_dict_list(
            payload.get("lore_developments"),
            allowed_ids,
            {"development_type", "entity_names", "description", "supporting_message_ids", "confidence"},
        ),
        "meta_developments": _normalize_dict_list(
            payload.get("meta_developments"),
            allowed_ids,
            {"development_type", "description", "supporting_message_ids", "confidence"},
        ),
        "entity_updates": _normalize_dict_list(
            payload.get("entity_updates"),
            allowed_ids,
            {"entity_name", "update_type", "description", "supporting_message_ids", "confidence"},
        ),
        "relationship_updates": _normalize_dict_list(
            payload.get("relationship_updates"),
            allowed_ids,
            {"source_entity", "target_entity", "relationship_type", "description", "supporting_message_ids", "confidence"},
        ),
        "timeline_updates": _normalize_dict_list(
            payload.get("timeline_updates"),
            allowed_ids,
            {"description", "supporting_message_ids", "confidence"},
        ),
        "open_questions": _normalize_dict_list(
            payload.get("open_questions"),
            allowed_ids,
            {"question", "supporting_message_ids"},
        ),
        "possible_contradictions": _normalize_dict_list(
            payload.get("possible_contradictions"),
            allowed_ids,
            {"description", "conflicts_with_prior_patch_note_ids", "supporting_message_ids", "confidence"},
        ),
        "reinforces_prior_patch_note_ids": [
            note_id for note_id in _list_string_values(payload.get("reinforces_prior_patch_note_ids", [])) if note_id in prior_note_ids
        ],
        "confidence": _safe_confidence(payload.get("confidence"), _safe_confidence(segment.get("model_confidence"), 0.5)),
        "status": status,
        "created_at_utc": now_utc_iso(),
    }
    for contradiction in note["possible_contradictions"]:
        refs = _list_string_values(contradiction.get("conflicts_with_prior_patch_note_ids", []))
        contradiction["conflicts_with_prior_patch_note_ids"] = [note_id for note_id in refs if note_id in prior_note_ids]
    if not note["summary"]:
        raise RuntimeError("patch note omitted required summary")
    if note["status"] == "no_durable_development":
        _clear_durable_developments(note, "No durable THERIAC development was established in this conversation.")
    elif _is_weak_indirect_segment(segment, rows):
        _clear_durable_developments(
            note,
            "No durable THERIAC development was established; the segment is a brief indirect reference without explicit project, entity, quest, mechanic, canon, lore, production, or design connection.",
        )
    return note


def extract_patch_note_with_model(
    *,
    segment: dict[str, Any],
    rows: list[dict[str, Any]],
    global_index: int,
    total_segments: int,
    prior_notes: list[dict[str, Any]],
    provider_config: dict[str, Any],
    cfg: dict[str, Any],
) -> dict[str, Any]:
    logger = get_logger(__name__)
    validation_retries = max(0, int(cfg.get("validation_retries", 1)))
    provider_retries = max(validation_retries, int(cfg.get("provider_retries", 2)))
    retry_sleep_seconds = max(0.0, float(cfg.get("retry_sleep_seconds", 15)))
    provider_retry_sleep_seconds = max(retry_sleep_seconds, float(cfg.get("provider_retry_sleep_seconds", 30)))
    validation_feedback = ""
    validation_failures = 0
    provider_failures = 0
    prior_context = previous_context_for_prompt(prior_notes, int(cfg.get("previous_context_notes", 6)))
    prior_note_ids = {str(note.get("patch_note_id", "")) for note in prior_notes}
    logger.info(
        "Stage 05 model call %d/%d: conversation_id=%s track=%s topic=%s messages=%d.",
        global_index,
        total_segments,
        segment.get("conversation_id"),
        segment.get("track"),
        segment.get("topic_label"),
        len(rows),
    )

    while True:
        prompt = build_patch_note_prompt(
            segment=segment,
            rows=rows,
            global_index=global_index,
            prior_context=prior_context,
            cfg=cfg,
            validation_feedback=validation_feedback,
        )
        response = call_mixtral_chat(
            prompt=prompt,
            **model_call_kwargs(provider_config, "stage_05_conversation_patch_notes"),
        )
        if isinstance(response, dict):
            try:
                return normalize_patch_payload(
                    response,
                    segment=segment,
                    rows=rows,
                    global_index=global_index,
                    prior_note_ids=prior_note_ids,
                )
            except Exception as exc:
                validation_failures += 1
                if validation_failures > validation_retries:
                    raise RuntimeError(f"invalid_patch_note_json: {exc}") from exc
                validation_feedback = f"Previous response failed validation: {exc}. Return the requested schema only."
                if retry_sleep_seconds:
                    time.sleep(retry_sleep_seconds)
                continue
        status = get_mixtral_runtime_status()
        reason = str(status.get("last_mistral_skip_reason") or "provider_unavailable")
        sleep_s = _provider_wait_seconds(reason, status, provider_retry_sleep_seconds)
        if reason in PACING_SKIP_REASONS:
            if sleep_s:
                logger.info(
                    "Stage 05 provider pacing for conversation=%s; retrying in %.1fs (%s).",
                    segment.get("conversation_id"),
                    sleep_s,
                    reason,
                )
                time.sleep(sleep_s)
            continue
        provider_failures += 1
        if provider_failures > provider_retries:
            raise RuntimeError(f"provider returned no patch-note JSON ({reason})")
        if sleep_s:
            time.sleep(sleep_s)


def patch_note_config(provider_config: dict[str, Any]) -> dict[str, Any]:
    cfg = provider_config.get("conversation_patch_notes", {}) if isinstance(provider_config.get("conversation_patch_notes", {}), dict) else {}
    return {
        "previous_context_notes": int(cfg.get("previous_context_notes", 6)),
        "max_messages_per_prompt": int(cfg.get("max_messages_per_prompt", 80)),
        "max_message_chars": int(cfg.get("max_message_chars", 700)),
        "max_prompt_message_chars": int(cfg.get("max_prompt_message_chars", 20000)),
        "validation_retries": int(cfg.get("validation_retries", 1)),
        "provider_retries": int(cfg.get("provider_retries", 2)),
        "retry_sleep_seconds": float(cfg.get("retry_sleep_seconds", 2)),
        "provider_retry_sleep_seconds": float(cfg.get("provider_retry_sleep_seconds", 15)),
    }


def _safe_read_json(path: Path, default: dict[str, Any]) -> dict[str, Any]:
    if not path.exists():
        return default
    try:
        payload = read_json(path)
    except Exception:
        return default
    return payload if isinstance(payload, dict) else default


def _safe_read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    try:
        lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    except Exception:
        return []
    for line in lines:
        if not line.strip():
            continue
        try:
            row = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(row, dict):
            rows.append(row)
    return rows


def _sort_patch_notes(notes: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen: set[str] = set()
    sorted_notes: list[dict[str, Any]] = []
    for note in sorted(
        (note for note in notes if isinstance(note, dict)),
        key=lambda item: (
            int(item.get("global_conversation_index", 0) or 0),
            str(item.get("timestamp_start_utc", "")),
            str(item.get("conversation_id", "")),
        ),
    ):
        conversation_id = str(note.get("conversation_id", "")).strip()
        if not conversation_id or conversation_id in seen:
            continue
        seen.add(conversation_id)
        sorted_notes.append(note)
    return sorted_notes


def load_existing_outputs(out_json: Path, out_jsonl: Path, out_failures_json: Path) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    payload = _safe_read_json(out_json, {})
    notes = payload.get("notes") if isinstance(payload, dict) else None
    existing_notes = _sort_patch_notes(notes if isinstance(notes, list) else _safe_read_jsonl(out_jsonl))

    failures_payload = _safe_read_json(out_failures_json, {"failures": []})
    failures = failures_payload.get("failures")
    existing_failures = [failure for failure in failures if isinstance(failure, dict)] if isinstance(failures, list) else []
    return existing_notes, existing_failures


def apply_durability_guardrails_to_existing_notes(
    notes: list[dict[str, Any]],
    *,
    segments_by_conversation: dict[str, dict[str, Any]],
    rows_by_conversation: dict[str, list[dict[str, Any]]],
) -> int:
    changed = 0
    for note in notes:
        conversation_id = str(note.get("conversation_id", "")).strip()
        segment = segments_by_conversation.get(conversation_id)
        rows = rows_by_conversation.get(conversation_id, [])
        if not segment or not rows:
            continue
        previous_status = str(note.get("status", "draft"))
        previous_lore_count = len(note.get("lore_developments", [])) if isinstance(note.get("lore_developments"), list) else 0
        if str(note.get("status", "")).strip().lower() == "no_durable_development":
            _clear_durable_developments(note, "No durable THERIAC development was established in this conversation.")
        elif _is_weak_indirect_segment(segment, rows):
            _clear_durable_developments(
                note,
                "No durable THERIAC development was established; the segment is a brief indirect reference without explicit project, entity, quest, mechanic, canon, lore, production, or design connection.",
            )
        if str(note.get("status", "draft")) != previous_status or previous_lore_count != len(note.get("lore_developments", [])):
            changed += 1
    return changed


def write_outputs(
    *,
    out_json: Path,
    out_jsonl: Path,
    out_failures_json: Path,
    notes: list[dict[str, Any]],
    failures: list[dict[str, Any]],
    total_segments: int,
    status: str,
) -> None:
    payload = {
        "generated_at_utc": now_utc_iso(),
        "status": status,
        "notes": notes,
        "conversation_count": total_segments,
        "notes_count": len(notes),
        "failure_count": len(failures),
        "order": "global_chronological_by_conversation_start",
    }
    write_json(out_json, payload)
    write_jsonl(out_jsonl, notes)
    write_json(out_failures_json, {"generated_at_utc": now_utc_iso(), "status": status, "failures": failures})


def run(
    in_messages_jsonl: Path,
    in_segments_json: Path,
    out_patch_notes_json: Path,
    out_patch_notes_jsonl: Path,
    out_failures_json: Path,
    in_pipeline_config_json: Path | None = None,
) -> None:
    logger = get_logger(__name__)
    provider_config = read_json(in_pipeline_config_json) if in_pipeline_config_json and in_pipeline_config_json.exists() else {}
    cfg = patch_note_config(provider_config)
    rows_by_conversation = _rows_by_conversation(read_jsonl(in_messages_jsonl))
    segments = ordered_segments(read_json(in_segments_json))
    segments_by_conversation = {str(segment.get("conversation_id", "")).strip(): segment for segment in segments}
    notes, failures = load_existing_outputs(out_patch_notes_json, out_patch_notes_jsonl, out_failures_json)
    sanitized_existing = apply_durability_guardrails_to_existing_notes(
        notes,
        segments_by_conversation=segments_by_conversation,
        rows_by_conversation=rows_by_conversation,
    )
    completed_conversation_ids = {str(note.get("conversation_id", "")).strip() for note in notes if str(note.get("conversation_id", "")).strip()}
    completed_conversation_ids.update(
        str(failure.get("conversation_id", "")).strip() for failure in failures if str(failure.get("conversation_id", "")).strip()
    )
    progress_every = max(1, len(segments) // 10)
    logger.info("Stage 05: drafting chronological patch notes for %d conversation segment(s).", len(segments))
    if notes or failures:
        logger.info(
            "Stage 05: resuming from checkpoint with notes=%d, failures=%d, completed_conversations=%d, sanitized_no_durable=%d.",
            len(notes),
            len(failures),
            len(completed_conversation_ids),
            sanitized_existing,
        )
    write_outputs(
        out_json=out_patch_notes_json,
        out_jsonl=out_patch_notes_jsonl,
        out_failures_json=out_failures_json,
        notes=notes,
        failures=failures,
        total_segments=len(segments),
        status="in_progress",
    )

    for index, segment in enumerate(segments, start=1):
        conversation_id = str(segment.get("conversation_id", "")).strip()
        if conversation_id in completed_conversation_ids:
            continue
        rows = rows_by_conversation.get(conversation_id, [])
        if not rows:
            failures.append(
                {
                    "conversation_id": conversation_id,
                    "global_conversation_index": index,
                    "reason": "missing_conversation_messages",
                    "error": "No relevant message rows found for conversation segment.",
                    "recorded_at_utc": now_utc_iso(),
                }
            )
            completed_conversation_ids.add(conversation_id)
            continue
        try:
            note = extract_patch_note_with_model(
                segment=segment,
                rows=rows,
                global_index=index,
                total_segments=len(segments),
                prior_notes=notes,
                provider_config=provider_config,
                cfg=cfg,
            )
            notes.append(note)
            completed_conversation_ids.add(conversation_id)
        except Exception as exc:
            failures.append(
                {
                    "conversation_id": conversation_id,
                    "global_conversation_index": index,
                    "reason": "model_patch_note_failed",
                    "error": str(exc),
                    "recorded_at_utc": now_utc_iso(),
                }
            )
            completed_conversation_ids.add(conversation_id)
            logger.warning("Stage 05 patch-note failed conversation_id=%s index=%d error=%s", conversation_id, index, exc)

        write_outputs(
            out_json=out_patch_notes_json,
            out_jsonl=out_patch_notes_jsonl,
            out_failures_json=out_failures_json,
            notes=notes,
            failures=failures,
            total_segments=len(segments),
            status="in_progress",
        )
        if index % progress_every == 0 or index == len(segments):
            logger.info(
                "Stage 05 progress: %d/%d conversations, notes=%d, failures=%d.",
                index,
                len(segments),
                len(notes),
                len(failures),
            )

    write_outputs(
        out_json=out_patch_notes_json,
        out_jsonl=out_patch_notes_jsonl,
        out_failures_json=out_failures_json,
        notes=notes,
        failures=failures,
        total_segments=len(segments),
        status="complete",
    )
    if segments and not notes and failures:
        raise RuntimeError("Stage 05 produced no conversation patch notes and has failures; stopping before downstream extraction.")
    logger.info("Stage 05 complete: patch_notes=%d, failures=%d.", len(notes), len(failures))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--in-messages-jsonl", type=Path, required=True)
    parser.add_argument("--in-segments-json", type=Path, required=True)
    parser.add_argument("--out-patch-notes-json", type=Path, required=True)
    parser.add_argument("--out-patch-notes-jsonl", type=Path, required=True)
    parser.add_argument("--out-failures-json", type=Path, required=True)
    parser.add_argument("--in-pipeline-config-json", type=Path, required=False, default=None)
    args = parser.parse_args()
    run(
        args.in_messages_jsonl,
        args.in_segments_json,
        args.out_patch_notes_json,
        args.out_patch_notes_jsonl,
        args.out_failures_json,
        args.in_pipeline_config_json,
    )


if __name__ == "__main__":
    main()
