from __future__ import annotations

import argparse
import json
import re
import time
from pathlib import Path
from typing import Any

from pipeline.common import get_logger, now_utc_iso, read_json, read_jsonl, stable_id, write_json, write_jsonl
from pipeline.ledger_entry_validation import validate_ledger_entries
from pipeline.ledger_quality_metrics import evaluate_quality_gate, ledger_entry_metrics
from pipeline.model_provider import call_model_chat, get_model_runtime_status, model_call_kwargs
from pipeline.opportunistic_model_router import (
    opportunistic_model_chat,
    opportunistic_route_config,
)


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


class ModelTemporarilyUnavailable(RuntimeError):
    """Raised when the model/provider is temporarily unavailable (e.g., upstream 429)."""


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


def _compact_entry_for_prior_context(entry: dict[str, Any]) -> dict[str, Any]:
    """Shrink prior-context rows so prompts stay within practical token limits."""
    return {
        "event_kind": str(entry.get("event_kind", "")),
        "change_type": str(entry.get("change_type", "")),
        "subject_label": str(entry.get("subject_label", "")),
        "headline": str(entry.get("headline", ""))[:240],
    }


def prior_entity_context(
    entries: list[dict[str, Any]],
    *,
    per_entity_limit: int,
    max_entity_keys: int = 48,
) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for entry in sorted(entries, key=lambda row: int(row.get("global_sequence", 0) or 0)):
        entity_id = str(entry.get("subject_entity_id", "")).strip()
        label_key = _normalized_name_key(str(entry.get("subject_label", "")))
        keys = []
        if entity_id:
            keys.append(entity_id)
        if label_key:
            keys.append(label_key)
        for key in keys:
            grouped.setdefault(key, []).append(entry)
    recent_keys = list(grouped.keys())[-max(1, int(max_entity_keys)) :]
    out: dict[str, list[dict[str, Any]]] = {}
    for key in recent_keys:
        rows = grouped.get(key, [])
        out[key] = [_compact_entry_for_prior_context(row) for row in rows[-per_entity_limit:]]
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
    max_registry_entities = max(16, int(cfg.get("max_registry_entities", 40)))
    compact_entities = [
        {
            "entity_id": entity.get("entity_id"),
            "canonical_name": entity.get("canonical_name"),
            "entity_type": entity.get("entity_type"),
            "aliases": (entity.get("aliases", [])[:4] if isinstance(entity.get("aliases"), list) else []),
        }
        for entity in entity_registry[:max_registry_entities]
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


def segment_has_lore_signal(
    segment: dict[str, Any],
    rows: list[dict[str, Any]],
    snippet_by_message: dict[str, list[str]],
) -> bool:
    anchors = segment.get("anchor_entities", [])
    if isinstance(anchors, list) and anchors:
        return True
    message_ids = {str(row.get("message_id", "")) for row in rows if str(row.get("message_id", ""))}
    for message_id in message_ids:
        if snippet_by_message.get(message_id):
            return True
    return False


def normalize_ledger_entries(
    payload: Any,
    *,
    segment: dict[str, Any],
    global_sequence: int,
    allowed_message_ids: set[str],
    by_name: dict[str, dict[str, Any]],
    snippet_by_message: dict[str, list[str]],
    inference_provenance: dict[str, Any] | None = None,
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
    provenance = inference_provenance if isinstance(inference_provenance, dict) else {}
    recorded_at = now_utc_iso()
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
                "recorded_at_utc": recorded_at,
                "inference_profile": str(provenance.get("routing_profile", "")),
                "inference_provider": str(provenance.get("provider", "")),
                "inference_api_model": str(provenance.get("api_model", "")),
                "inference_lane_tier": str(provenance.get("lane_tier", "")),
                "inference_model_family": str(provenance.get("model_family", "")),
                "inference_recorded_at_utc": recorded_at,
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
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    logger = get_logger(__name__)
    allowed_message_ids = {str(row.get("message_id", "")) for row in rows if str(row.get("message_id", ""))}
    per_entity_limit = max(1, int(cfg.get("previous_context_entries_per_entity", 4)))
    prior_context = prior_entity_context(
        prior_entries,
        per_entity_limit=per_entity_limit,
        max_entity_keys=int(cfg.get("max_prior_entity_keys", 48)),
    )
    validation_feedback = ""
    retries = max(0, int(cfg.get("validation_retries", 1)))
    provider_retries = max(0, int(cfg.get("provider_retries", 2)))
    provider_retry_sleep_seconds = max(0.0, float(cfg.get("provider_retry_sleep_seconds", 15.0) or 0.0))
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
        route_cfg = opportunistic_route_config(provider_config, cfg)
        probe_kwargs = model_call_kwargs(provider_config, "stage_05_lore_development_ledger")
        logger.info(
            "Stage 07 ledger model call %d/%d: conversation_id=%s scope=%s messages=%d prompt_chars=%d timeout_s=%s opportunistic=%s.",
            global_sequence,
            total_segments,
            segment.get("conversation_id"),
            segment.get("source_scope"),
            len(rows),
            len(prompt),
            probe_kwargs.get("timeout_seconds"),
            route_cfg.get("enabled"),
        )
        response = None
        last_reason = ""
        segment_id = str(segment.get("conversation_id", "")).strip()
        if route_cfg.get("enabled"):
            chat_result = opportunistic_model_chat(
                prompt,
                provider_config=provider_config,
                task_name="stage_05_lore_development_ledger",
                route_cfg=route_cfg,
                segment_id=segment_id,
                prior_entries=prior_entries,
            )
            response = chat_result.response
            if response is None:
                last_reason = chat_result.skip_reason or str(
                    get_model_runtime_status().get("last_model_skip_reason") or ""
                )
            inference_provenance = {
                "routing_profile": chat_result.routing_profile,
                "provider": chat_result.provider,
                "api_model": chat_result.api_model,
                "lane_tier": chat_result.lane_tier,
                "model_family": chat_result.model_family,
            }
        else:
            inference_provenance = {}
            call_kwargs = probe_kwargs
            max_provider_attempts = 1 + provider_retries
            retry_on_timeout = bool(cfg.get("retry_on_connection_timeout", True))
            for provider_attempt in range(max_provider_attempts):
                response = call_model_chat(prompt=prompt, **call_kwargs)
                if response is not None:
                    status = get_model_runtime_status()
                    inference_provenance = {
                        "routing_profile": str(call_kwargs.get("routing_profile", "")),
                        "provider": str(call_kwargs.get("provider", "")),
                        "api_model": str(call_kwargs.get("api_model", "")),
                        "lane_tier": "homogeneous",
                        "model_family": "deepseek_v4_flash",
                    }
                    break
                status = get_model_runtime_status()
                last_reason = str(status.get("last_model_skip_reason") or "")
                if provider_attempt >= max_provider_attempts - 1:
                    break
                if _transient_provider_skip_reason(last_reason) or (
                    retry_on_timeout and last_reason in {"connection_error", "attempts_exhausted"}
                ):
                    logger.warning(
                        "Stage 07 ledger provider retry %d/%d after %s (segment_id=%s).",
                        provider_attempt + 1,
                        max_provider_attempts - 1,
                        last_reason,
                        segment.get("conversation_id"),
                    )
                    if provider_retry_sleep_seconds:
                        time.sleep(provider_retry_sleep_seconds)
                    continue
                break
        if response is None:
            raise ModelTemporarilyUnavailable(last_reason or "model_provider_unavailable")
        try:
            payload = json.loads(response) if isinstance(response, str) else response
            raw_entries = normalize_ledger_entries(
                payload,
                segment=segment,
                global_sequence=global_sequence,
                allowed_message_ids=allowed_message_ids,
                by_name=by_name,
                snippet_by_message=snippet_by_message,
                inference_provenance=inference_provenance,
            )
            entity_prior = [
                row
                for row in prior_entries
                if str(row.get("subject_entity_id", "")).strip()
                in {
                    str(e.get("subject_entity_id", "")).strip()
                    for e in raw_entries
                    if str(e.get("subject_entity_id", "")).strip()
                }
                or _normalized_name_key(str(row.get("subject_label", "")))
                in {_normalized_name_key(str(e.get("subject_label", ""))) for e in raw_entries}
            ]
            validation_cfg = cfg.get("ledger_validation", {})
            if not isinstance(validation_cfg, dict):
                validation_cfg = {}
            accepted, rejected, _review = validate_ledger_entries(
                raw_entries,
                model_family=str(inference_provenance.get("model_family", "deepseek_v4_flash")),
                validation_cfg=validation_cfg,
                prior_entries=entity_prior or prior_entries,
                by_name=by_name,
            )
            if rejected:
                logger.warning(
                    "Stage 07 validation rejected %d/%d entries segment_id=%s.",
                    len(rejected),
                    len(raw_entries),
                    segment_id,
                )
            return accepted, rejected, _review
        except json.JSONDecodeError as exc:
            validation_feedback = f"invalid_json: {exc}"
            if attempt >= retries:
                raise RuntimeError(f"invalid_ledger_json: {exc}") from exc
    return [], [], []


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


def _transient_provider_skip_reason(reason: str) -> bool:
    reason = str(reason or "").strip()
    if reason in {"rate_limited_429", "provider_locked", "adaptive_pacing", "connection_error", "attempts_exhausted"}:
        return True
    return reason in {"http_error_502", "http_error_503", "http_error_504"}


def ledger_config(provider_config: dict[str, Any]) -> dict[str, Any]:
    cfg = provider_config.get("lore_development_ledger", {}) if isinstance(provider_config.get("lore_development_ledger"), dict) else {}
    return {
        "max_messages_per_prompt": int(cfg.get("max_messages_per_prompt", 80)),
        "max_message_chars": int(cfg.get("max_message_chars", 700)),
        "max_prompt_message_chars": int(cfg.get("max_prompt_message_chars", 20000)),
        "previous_context_entries_per_entity": int(cfg.get("previous_context_entries_per_entity", 4)),
        "max_prior_entity_keys": int(cfg.get("max_prior_entity_keys", 48)),
        "max_registry_entities": int(cfg.get("max_registry_entities", 60)),
        "validation_retries": int(cfg.get("validation_retries", 1)),
        "provider_retries": int(cfg.get("provider_retries", 2)),
        "retry_sleep_seconds": float(cfg.get("retry_sleep_seconds", 2)),
        "provider_retry_sleep_seconds": float(cfg.get("provider_retry_sleep_seconds", 15)),
        "retry_on_connection_timeout": bool(cfg.get("retry_on_connection_timeout", True)),
        "stop_when_billed": bool(cfg.get("stop_when_billed", False)),
        "stop_when_billed_min_cost_usd": float(cfg.get("stop_when_billed_min_cost_usd", 0.0) or 0.0),
        "max_new_segments_per_run": int(cfg.get("max_new_segments_per_run", 0) or 0),
        "skip_segments_without_lore_signal": bool(cfg.get("skip_segments_without_lore_signal", False)),
        "quality_gate_every_n_segments": int(cfg.get("quality_gate_every_n_segments", 0) or 0),
        "quality_gate_baseline_path": str(cfg.get("quality_gate_baseline_path", "")),
        "ledger_validation": cfg.get("ledger_validation", {}),
        "opportunistic_routing": cfg.get("opportunistic_routing", {}),
    }


def _load_baseline_metrics(baseline_path: str) -> dict[str, Any]:
    path = Path(baseline_path)
    if not baseline_path or not path.exists():
        return {}
    entries = [row for row in read_jsonl(path) if isinstance(row, dict)]
    return ledger_entry_metrics(entries)


def run_quality_gate(
    *,
    entries: list[dict[str, Any]],
    run_started_at_utc: str,
    baseline_metrics: dict[str, Any],
    out_report_path: Path,
) -> tuple[bool, dict[str, Any]]:
    batch = [
        entry
        for entry in entries
        if str(entry.get("recorded_at_utc", "")) >= run_started_at_utc
    ]
    batch_metrics = ledger_entry_metrics(batch)
    passed, reasons = evaluate_quality_gate(batch_metrics, baseline_metrics)
    report = {
        "generated_at_utc": now_utc_iso(),
        "passed": passed,
        "failure_reasons": reasons,
        "batch_metrics": batch_metrics,
        "baseline_metrics": baseline_metrics,
        "run_started_at_utc": run_started_at_utc,
    }
    write_json(out_report_path, report)
    return passed, report


def append_review_queue_rows(path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        for row in rows:
            row_out = dict(row)
            row_out.setdefault("recorded_at_utc", now_utc_iso())
            handle.write(json.dumps(row_out, ensure_ascii=False) + "\n")


def load_existing_outputs(
    out_index_json: Path,
    out_jsonl: Path,
    out_failures_json: Path,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], set[str]]:
    entries: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []
    completed_segment_ids: set[str] = set()
    failed_segment_ids: set[str] = set()
    if out_jsonl.exists():
        entries = [row for row in read_jsonl(out_jsonl) if isinstance(row, dict)]
    if out_failures_json.exists():
        payload = read_json(out_failures_json)
        failures = payload.get("failures", []) if isinstance(payload, dict) else []
        for failure in failures:
            if isinstance(failure, dict):
                segment_id = str(failure.get("source_segment_id", "")).strip()
                if segment_id:
                    failed_segment_ids.add(segment_id)
    if out_index_json.exists():
        payload = read_json(out_index_json)
        for segment_id in _list_string_values(payload.get("completed_segment_ids", [])):
            if segment_id not in failed_segment_ids:
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
    starting_completed = len(completed_segment_ids)
    final_status = "complete"
    max_new_segments = max(0, int(cfg.get("max_new_segments_per_run", 0) or 0))
    progress_every = max(1, len(segments) // 10)
    run_started_at_utc = now_utc_iso()
    baseline_metrics = _load_baseline_metrics(str(cfg.get("quality_gate_baseline_path", "")))
    quality_gate_every = max(0, int(cfg.get("quality_gate_every_n_segments", 0) or 0))
    segments_this_run = 0
    review_queue_path = out_jsonl.parent / "ledger_review_queue.jsonl"
    quality_gate_report_path = out_jsonl.parent / "quality_gate_report.json"
    skip_without_signal = bool(cfg.get("skip_segments_without_lore_signal", False))
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
        if skip_without_signal and not segment_has_lore_signal(segment, rows, snippet_by_message):
            logger.info(
                "Stage 07 skip segment_id=%s seq=%d (no lore signal: no snippets or anchor entities).",
                segment_id,
                global_sequence,
            )
            completed_segment_ids.add(segment_id)
            segments_this_run += 1
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
            if quality_gate_every and segments_this_run % quality_gate_every == 0 and baseline_metrics:
                passed, report = run_quality_gate(
                    entries=entries,
                    run_started_at_utc=run_started_at_utc,
                    baseline_metrics=baseline_metrics,
                    out_report_path=quality_gate_report_path,
                )
                if not passed:
                    logger.warning(
                        "Stage 07 quality gate failed: %s",
                        report.get("failure_reasons"),
                    )
                    final_status = "in_progress"
                    break
            if max_new_segments and (len(completed_segment_ids) - starting_completed) >= max_new_segments:
                break
            continue
        try:
            new_entries, validation_rejected, review_rows = extract_ledger_entries_with_model(
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
            for rejection in validation_rejected:
                rejection["recorded_at_utc"] = now_utc_iso()
                rejection["source_scope"] = segment.get("source_scope")
                failures.append(rejection)
            append_review_queue_rows(review_queue_path, review_rows)
            entries.extend(new_entries)
            failures = [
                failure
                for failure in failures
                if str(failure.get("source_segment_id", "")).strip() != segment_id
            ]
            completed_segment_ids.add(segment_id)
            segments_this_run += 1
        except ModelTemporarilyUnavailable as exc:
            logger.warning(
                "Stage 07 stopping early: model temporarily unavailable (segment_id=%s seq=%d): %s",
                segment_id,
                global_sequence,
                exc,
            )
            final_status = "in_progress"
            break
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
            segments_this_run += 1

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

        if quality_gate_every and segments_this_run > 0 and segments_this_run % quality_gate_every == 0 and baseline_metrics:
            passed, report = run_quality_gate(
                entries=entries,
                run_started_at_utc=run_started_at_utc,
                baseline_metrics=baseline_metrics,
                out_report_path=quality_gate_report_path,
            )
            if not passed:
                logger.warning(
                    "Stage 07 quality gate failed: %s",
                    report.get("failure_reasons"),
                )
                final_status = "in_progress"
                break

        if max_new_segments and (len(completed_segment_ids) - starting_completed) >= max_new_segments:
            logger.warning(
                "Stage 07 stopping early: reached max_new_segments_per_run=%d (completed=%d -> %d).",
                max_new_segments,
                starting_completed,
                len(completed_segment_ids),
            )
            break

        if cfg.get("stop_when_billed"):
            status = get_model_runtime_status()
            billed = float(status.get("last_call_billed_cost_usd", 0.0) or 0.0)
            threshold = float(cfg.get("stop_when_billed_min_cost_usd", 0.0) or 0.0)
            if billed > threshold:
                logger.warning(
                    "Stage 07 stopping early: detected billed model call cost_usd=%.6f model=%s provider=%s",
                    billed,
                    status.get("last_call_api_model", ""),
                    status.get("last_call_provider", ""),
                )
                break

    write_outputs(
        out_index_json=out_index_json,
        out_jsonl=out_jsonl,
        out_history_json=out_history_json,
        out_failures_json=out_failures_json,
        entries=entries,
        failures=failures,
        completed_segment_ids=completed_segment_ids,
        total_segments=len(segments),
        status=final_status,
    )
    logger.info(
        "Stage 07 finished (%s): entries=%d failures=%d entities=%d.",
        final_status,
        len(entries),
        len(failures),
        len(group_entity_history(entries)),
    )


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
