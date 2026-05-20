from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

from pipeline.common import now_utc_iso, read_json, read_jsonl, safe_uuid, stable_id, write_json
from pipeline.entity_resolution import card_id_for_entity, load_entity_records, normalized_name_key
from pipeline.model_provider import call_model_chat, get_model_runtime_status, model_call_kwargs
from pipeline.review_memory import (
    load_review_memory,
    normalize_claim_text,
    remember_story_question_answer,
    save_review_memory,
)


STORY_SESSION_VERSION = 1
STORY_TASK_NAME = "stage_09_story_questions"

STORY_QUESTION_SCHEMA = {
    "type": "object",
    "properties": {
        "question_text": {"type": "string"},
        "focus_type": {"type": "string"},
        "rationale": {"type": "string"},
        "linked_claim_ids": {
            "type": "array",
            "items": {"type": "string"}
        },
        "linked_entities": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "entity_id": {"type": "string"},
                    "name": {"type": "string"}
                },
                "required": ["entity_id", "name"],
                "additionalProperties": False
            }
        },
        "evidence_snippet_ids": {
            "type": "array",
            "items": {"type": "string"}
        },
        "expected_resolution": {"type": "string"}
    },
    "required": [
        "question_text", "focus_type", "rationale", "linked_claim_ids",
        "linked_entities", "evidence_snippet_ids", "expected_resolution"
    ],
    "additionalProperties": False
}

ANSWER_APPLICATION_PROPOSAL_SCHEMA = {
    "type": "object",
    "properties": {
        "summary": {"type": "string"},
        "claim_decisions": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "claim_id": {"type": "string"},
                    "decision": {"type": "string"},
                    "edited_claim_text": {"type": "string"},
                    "confidence": {"type": "number"},
                    "rationale": {"type": "string"}
                },
                "required": ["claim_id", "decision", "confidence", "rationale"],
                "additionalProperties": False
            }
        },
        "author_claims": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "target_entity_id": {"type": "string"},
                    "target_entity_name": {"type": "string"},
                    "claim_type": {"type": "string"},
                    "claim_text": {"type": "string"},
                    "knowledge_track": {"type": "string"},
                    "confidence": {"type": "number"},
                    "rationale": {"type": "string"}
                },
                "required": ["target_entity_name", "claim_type", "claim_text", "knowledge_track", "confidence", "rationale"],
                "additionalProperties": False
            }
        },
        "left_pending": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "claim_id": {"type": "string"},
                    "reason": {"type": "string"}
                },
                "required": ["claim_id", "reason"],
                "additionalProperties": False
            }
        }
    },
    "required": ["summary", "claim_decisions", "author_claims", "left_pending"],
    "additionalProperties": False
}
MAX_QUESTION_CLUSTERS = 12
MAX_CLAIM_TEXT_CHARS = 360
MAX_APPLICATION_CLAIM_TEXT_CHARS = 520
MAX_RELATIONSHIP_HINTS = 2
MAX_SOURCE_IDS_PER_CLAIM = 8
MAX_SUPPORT_WARNINGS = 5
MAX_EVIDENCE_SNIPPETS_FOR_QUESTION = 10
MAX_PATCH_NOTES_FOR_QUESTION = 6
MAX_PATCH_NOTE_ITEMS = 3
VALID_CLAIM_DECISIONS = {"accept", "reject", "defer", "needs_more_context"}
PENDING_CLAIM_DECISIONS = {"defer", "needs_more_context"}
AUTO_REVIEW_REVIEWER_MARKERS = ("auto_review", "gemini_auto")
STORY_REVIEW_STATUS_PRIORITY = {
    "unanswered": 0,
    "human_review_requested": 1,
    "auto_reviewed": 2,
    "pending_decision": 1,
}
STORY_REVIEW_STATUS_LABELS = {
    "unanswered": "unanswered",
    "human_review_requested": "human review requested",
    "auto_reviewed": "auto-reviewed prior",
    "pending_decision": "pending decision",
}
ABSENCE_NEGATIVE_RATIONALE_MARKERS = (
    "no mention",
    "not mention",
    "does not mention",
    "doesn't mention",
    "not addressed",
    "does not address",
    "doesn't address",
    "answer focuses",
    "unrelated",
    "not implied",
    "no prior",
    "no ship",
    ", not ",
)
EXPLICIT_CONTRADICTION_MARKERS = (
    "explicitly contradict",
    "directly contradict",
    "author says",
    "author states",
    "answer explicitly",
    "answer directly",
    "states that this is not",
    "states this is not",
)
VALID_AUTHOR_CLAIM_TYPES = {
    "relationship",
    "role",
    "background",
    "timeline",
    "inspiration",
    "open_question",
    "alias",
    "lore_fact",
    "meta_note",
    "other",
}
VALID_AUTHOR_KNOWLEDGE_TRACKS = {"lore", "meta", "both"}
AFFIRM_ALL_ANSWER_RE = re.compile(
    r"^\s*(?:yes[:,.\s]*)?(?:correct|right|true|confirmed|accurate)(?:\s+on\s+all\s+(?:counts|points))?[.!?\s]*$|"
    r"^\s*(?:yes[:,.\s]*)?all\s+(?:correct|right|true|confirmed|accurate)[.!?\s]*$|"
    r"^\s*yes\s+to\s+all(?:\s+of\s+(?:that|it))?[.!?\s]*$",
    flags=re.IGNORECASE,
)
AUTHOR_META_TEXT_MARKERS = (
    "working name",
    "working title",
    "canonical name",
    "became her canonical",
    "became his canonical",
    "became their canonical",
    "later updated",
    "renamed",
    "originally developed",
    "developed based",
    "development history",
    "design history",
    "design note",
    "inspired by",
    "inspiration",
    "player's",
    "player-facing",
    "gameplay",
    "game mechanic",
    "mechanic",
    "generic reference",
    "likely refer",
    "can refer to",
)


def story_question_paths(root: Path) -> dict[str, Path]:
    review_root = root / "07_review"
    return {
        "session": review_root / "story_question_session.json",
        "questions": review_root / "story_questions.jsonl",
        "answers": review_root / "story_question_answers.jsonl",
        "applications": review_root / "story_question_applications.jsonl",
        "application_proposals": review_root / "story_question_application_proposals.jsonl",
        "failures": review_root / "story_question_failures.json",
        "claim_decisions": review_root / "claim_review_decisions.json",
        "author_claims": review_root / "author_claims.json",
    }


def _read_json_or_default(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        payload = read_json(path)
    except Exception:
        return default
    return payload if isinstance(payload, type(default)) else default


def _clip_text(value: Any, limit: int) -> str:
    text = re.sub(r"\s+", " ", str(value or "")).strip()
    if len(text) <= limit:
        return text
    return text[: max(0, limit - 3)].rstrip() + "..."


def _append_jsonl(path: Path, row: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(row, ensure_ascii=False) + "\n")


def _load_config(path: Path | None) -> dict[str, Any]:
    if path and path.exists():
        payload = read_json(path)
        return payload if isinstance(payload, dict) else {}
    return {}


def story_question_config(provider_config: dict[str, Any]) -> dict[str, Any]:
    cfg = provider_config.get("story_questions", {}) if isinstance(provider_config.get("story_questions", {}), dict) else {}
    return {
        "enabled": bool(cfg.get("enabled", True)),
        "provider": str(cfg.get("provider", "openrouter") or "openrouter"),
        "model": str(cfg.get("model", "qwen/qwen3-235b-a22b-2507") or "qwen/qwen3-235b-a22b-2507"),
        "max_linked_claims_per_question": max(1, int(cfg.get("max_linked_claims_per_question", 8))),
        "application_policy": str(cfg.get("application_policy", "moderate") or "moderate"),
        "linked_claim_min_confidence": float(cfg.get("linked_claim_min_confidence", 0.6)),
        "unlinked_claim_min_confidence": float(cfg.get("unlinked_claim_min_confidence", 0.85)),
        "max_pending_claims_for_application": max(8, int(cfg.get("max_pending_claims_for_application", 40))),
        "generate_all_max_questions": max(1, int(cfg.get("generate_all_max_questions", 300))),
    }


def load_story_session(root: Path) -> dict[str, Any]:
    path = story_question_paths(root)["session"]
    if path.exists():
        payload = _read_json_or_default(path, {})
        if isinstance(payload, dict):
            session = _empty_session(root)
            session.update(payload)
            for key in ["questions", "answers", "applications", "skipped_questions"]:
                if not isinstance(session.get(key), list):
                    session[key] = []
            if session.get("pending_application_proposal") is not None and not isinstance(
                session.get("pending_application_proposal"), dict
            ):
                session["pending_application_proposal"] = None
            return session
    return _empty_session(root)


def _empty_session(root: Path) -> dict[str, Any]:
    return {
        "version": STORY_SESSION_VERSION,
        "session_id": stable_id("story_question_session", str(root.resolve())),
        "status": "active",
        "created_at_utc": now_utc_iso(),
        "updated_at_utc": now_utc_iso(),
        "current_question_id": "",
        "questions": [],
        "answers": [],
        "applications": [],
        "skipped_questions": [],
        "pending_application_proposal": None,
        "last_unresolved_claim_count": 0,
        "last_model_rationale": "",
    }


def save_story_session(root: Path, session: dict[str, Any]) -> None:
    session["version"] = STORY_SESSION_VERSION
    session["updated_at_utc"] = now_utc_iso()
    write_json(story_question_paths(root)["session"], session)


def _decision_ids_with_human_review(decisions_path: Path) -> tuple[dict[str, dict[str, Any]], set[str]]:
    payload = _read_json_or_default(decisions_path, {"decisions": []})
    decisions = payload.get("decisions", []) if isinstance(payload, dict) else []
    latest: dict[str, dict[str, Any]] = {}
    human_reviewed: set[str] = set()
    for decision in decisions:
        if not isinstance(decision, dict):
            continue
        claim_id = str(decision.get("claim_id", "")).strip()
        if not claim_id:
            continue
        latest[claim_id] = decision
        reviewer = str(decision.get("reviewer", "")).strip().lower()
        if reviewer and not _is_auto_review_reviewer(reviewer):
            human_reviewed.add(claim_id)
    return latest, human_reviewed


def _is_auto_review_reviewer(reviewer: str) -> bool:
    lowered = str(reviewer or "").strip().lower()
    return any(marker in lowered for marker in AUTO_REVIEW_REVIEWER_MARKERS)


def _is_auto_review_decision(decision: dict[str, Any] | None) -> bool:
    if not isinstance(decision, dict):
        return False
    return _is_auto_review_reviewer(str(decision.get("reviewer", "")))


def _pending_attention_claim_ids(root: Path, human_reviewed: set[str]) -> set[str]:
    payload = _read_json_or_default(root / "07_review" / "claim_auto_review_attention.json", {"items": []})
    out: set[str] = set()
    for item in payload.get("items", []) if isinstance(payload, dict) else []:
        if not isinstance(item, dict):
            continue
        claim_id = str(item.get("claim_id", "")).strip()
        if claim_id and claim_id not in human_reviewed:
            out.add(claim_id)
    return out


def _claim_attention_by_id(root: Path, human_reviewed: set[str]) -> dict[str, dict[str, Any]]:
    payload = _read_json_or_default(root / "07_review" / "claim_auto_review_attention.json", {"items": []})
    out: dict[str, dict[str, Any]] = {}
    for item in payload.get("items", []) if isinstance(payload, dict) else []:
        if not isinstance(item, dict):
            continue
        claim_id = str(item.get("claim_id", "")).strip()
        if claim_id and claim_id not in human_reviewed:
            out[claim_id] = item
    return out


def load_claims(root: Path) -> list[dict[str, Any]]:
    payload = _read_json_or_default(root / "06_drafts" / "card_drafts" / "claim_drafts.json", {"claims": []})
    claims = payload.get("claims", []) if isinstance(payload, dict) else []
    return [claim for claim in claims if isinstance(claim, dict)]


def _coerce_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _story_review_status_for_claim(decision: dict[str, Any] | None, attention: dict[str, Any] | None) -> str:
    if isinstance(attention, dict) and attention:
        return "human_review_requested"
    if not isinstance(decision, dict) or not decision:
        return "unanswered"
    action = str(decision.get("decision", "")).strip().lower()
    if _is_auto_review_decision(decision):
        if bool(decision.get("human_review_recommended")):
            return "human_review_requested"
        return "auto_reviewed"
    if action in PENDING_CLAIM_DECISIONS:
        return "pending_decision"
    return "decided"


def _story_review_priority(status: str) -> int:
    return STORY_REVIEW_STATUS_PRIORITY.get(str(status or ""), 99)


def _story_review_status_counts(claims: list[dict[str, Any]]) -> dict[str, int]:
    counts = {key: 0 for key in STORY_REVIEW_STATUS_PRIORITY}
    for claim in claims:
        status = str(claim.get("story_review_status", "unanswered") or "unanswered")
        counts[status] = counts.get(status, 0) + 1
    counts["human_review_requested_total"] = counts.get("human_review_requested", 0) + counts.get("pending_decision", 0)
    counts["all_story_candidates"] = len(claims)
    return counts


def _select_question_claim_tier(claims: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], str]:
    if not claims:
        return [], ""
    min_priority = min(_story_review_priority(str(claim.get("story_review_status", ""))) for claim in claims)
    selected = [
        claim
        for claim in claims
        if _story_review_priority(str(claim.get("story_review_status", ""))) == min_priority
    ]
    status = str(selected[0].get("story_review_status", "")) if selected else ""
    return selected, status


def _claim_with_review_context(
    claim: dict[str, Any],
    decision: dict[str, Any] | None,
    attention: dict[str, Any] | None,
) -> dict[str, Any]:
    status = _story_review_status_for_claim(decision, attention)
    base = {
        **claim,
        "story_review_status": status,
        "story_review_label": STORY_REVIEW_STATUS_LABELS.get(status, status or "unknown"),
        "story_review_priority": _story_review_priority(status),
    }
    if not _is_auto_review_decision(decision):
        return base
    action = str((decision or {}).get("decision", "")).strip().lower()
    base_confidence = _coerce_float(claim.get("confidence"), 0.0)
    human_attention = bool((decision or {}).get("human_review_recommended")) or bool(attention)
    if action == "accept":
        review_prior = 0.72 if human_attention else 0.88
        story_confidence = max(base_confidence, review_prior)
        weight = 0.7 if human_attention else 1.0
    elif action == "reject":
        review_prior = 0.42 if human_attention else 0.25
        story_confidence = min(base_confidence or review_prior, review_prior)
        weight = 0.45 if human_attention else 0.75
    else:
        review_prior = 0.55
        story_confidence = max(base_confidence, review_prior)
        weight = 0.35
    return {
        **base,
        "auto_review": {
            "decision": action or "unknown",
            "rationale": _clip_text((decision or {}).get("rationale", ""), 240),
            "human_review_recommended": human_attention,
            "human_review_reason": str((decision or {}).get("human_review_reason") or (attention or {}).get("human_review_reason") or ""),
            "reviewer": (decision or {}).get("reviewer", ""),
            "policy": (decision or {}).get("auto_review_policy", ""),
            "weight": weight,
        },
        "story_question_confidence": story_confidence,
    }


def pending_claims_for_story(root: Path) -> list[dict[str, Any]]:
    decisions_by_claim, human_reviewed = _decision_ids_with_human_review(story_question_paths(root)["claim_decisions"])
    attention_ids = _pending_attention_claim_ids(root, human_reviewed)
    attention_by_id = _claim_attention_by_id(root, human_reviewed)
    pending: list[dict[str, Any]] = []
    for claim in load_claims(root):
        claim_id = str(claim.get("claim_id", "")).strip()
        if not claim_id:
            continue
        if claim_id in human_reviewed:
            continue
        decision = decisions_by_claim.get(claim_id)
        decision_action = str(decision.get("decision", "")).strip().lower() if decision else ""
        if (
            claim_id not in decisions_by_claim
            or _is_auto_review_decision(decision)
            or claim_id in attention_ids
            or decision_action in PENDING_CLAIM_DECISIONS
        ):
            pending.append(_claim_with_review_context(claim, decision, attention_by_id.get(claim_id)))
    return pending


def _snippet_context(root: Path, snippet_ids: list[str], limit: int = 12) -> list[dict[str, Any]]:
    wanted = {str(item).strip() for item in snippet_ids if str(item).strip()}
    if not wanted:
        return []
    out: list[dict[str, Any]] = []
    for row in read_jsonl(root / "03_relevance" / "snippets_candidates.jsonl"):
        snippet_id = str(row.get("snippet_id", "")).strip()
        if snippet_id not in wanted:
            continue
        out.append(
            {
                "snippet_id": snippet_id,
                "conversation_id": row.get("conversation_id", ""),
                "topic": row.get("conversation_topic_label") or row.get("topic_label") or row.get("cluster_key", ""),
                "track": row.get("knowledge_track", ""),
                "text": _clip_text(
                    row.get("display_text_normalized") or row.get("display_text") or row.get("content_normalized") or "",
                    700,
                ),
            }
        )
        if len(out) >= limit:
            break
    return out


def _compact_patch_items(value: Any, limit: int) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for item in value if isinstance(value, list) else []:
        if not isinstance(item, dict):
            continue
        compact: dict[str, Any] = {}
        for key in [
            "entity_name",
            "entity_names",
            "relationship",
            "source_entity",
            "target_entity",
            "development_type",
            "update_type",
            "description",
            "summary",
            "question",
            "confidence",
        ]:
            if key not in item:
                continue
            raw = item.get(key)
            if isinstance(raw, list):
                compact[key] = [str(x)[:80] for x in raw[:4]]
            elif key in {"description", "summary", "question"}:
                compact[key] = _clip_text(raw, 260)
            else:
                compact[key] = raw
        if compact:
            out.append(compact)
        if len(out) >= limit:
            break
    return out


def _patch_note_context(root: Path, conversation_ids: set[str], limit: int = 12) -> list[dict[str, Any]]:
    payload = _read_json_or_default(root / "02_timeline" / "conversation_patch_notes.json", {"notes": []})
    notes = payload.get("notes", []) if isinstance(payload, dict) else []
    out: list[dict[str, Any]] = []
    for note in notes:
        if not isinstance(note, dict):
            continue
        conversation_id = str(note.get("conversation_id", "")).strip()
        if conversation_ids and conversation_id not in conversation_ids:
            continue
        out.append(
            {
                "patch_note_id": note.get("patch_note_id", ""),
                "conversation_id": conversation_id,
                "sequence_index": note.get("sequence_index"),
                "topic_label": note.get("topic_label", ""),
                "summary": _clip_text(note.get("summary") or note.get("conversation_summary") or "", 420),
                "entity_updates": _compact_patch_items(note.get("entity_updates", []), MAX_PATCH_NOTE_ITEMS),
                "relationship_updates": _compact_patch_items(note.get("relationship_updates", []), MAX_PATCH_NOTE_ITEMS),
                "open_questions": _compact_patch_items(note.get("open_questions", []), 2),
            }
        )
        if len(out) >= limit:
            break
    return out


def _relationship_hint_summary(value: Any) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for hint in value if isinstance(value, list) else []:
        if not isinstance(hint, dict):
            continue
        out.append(
            {
                "relation_type": str(hint.get("relation_type", "") or "")[:80],
                "target_name": str(hint.get("target_name") or hint.get("target_entity_name") or "")[:120],
                "note": _clip_text(hint.get("note") or hint.get("description") or "", 220),
                "confidence": hint.get("confidence"),
            }
        )
        if len(out) >= MAX_RELATIONSHIP_HINTS:
            break
    return out


def _claim_summary(claim: dict[str, Any], *, application: bool = False) -> dict[str, Any]:
    claim_text_limit = MAX_APPLICATION_CLAIM_TEXT_CHARS if application else MAX_CLAIM_TEXT_CHARS
    summary = {
        "claim_id": claim.get("claim_id", ""),
        "target_entity_id": claim.get("target_entity_id", ""),
        "target_entity_name": claim.get("target_entity_name", ""),
        "target_card_id": claim.get("target_card_id", ""),
        "knowledge_track": claim.get("knowledge_track", ""),
        "claim_type": claim.get("claim_type", ""),
        "claim_text": _clip_text(claim.get("claim_text", ""), claim_text_limit),
        "confidence": claim.get("confidence"),
        "story_question_confidence": claim.get("story_question_confidence", claim.get("confidence")),
        "story_review_status": claim.get("story_review_status", "unanswered"),
        "story_review_label": claim.get("story_review_label", "unanswered"),
        "source_snippet_ids": [
            str(item)
            for item in (claim.get("source_snippet_ids", []) or [])[:MAX_SOURCE_IDS_PER_CLAIM]
        ],
        "support_warnings": [
            str(item)[:160]
            for item in (claim.get("support_warnings", []) or [])[:MAX_SUPPORT_WARNINGS]
        ]
        if isinstance(claim.get("support_warnings", []), list)
        else [],
        "contradiction_notes": _clip_text(claim.get("contradiction_notes", ""), 260),
        "proposed_relationship_hints": _relationship_hint_summary(claim.get("proposed_relationship_hints", [])),
    }
    auto_review = claim.get("auto_review")
    if isinstance(auto_review, dict) and auto_review:
        summary["auto_review"] = {
            "decision": auto_review.get("decision", ""),
            "human_review_recommended": bool(auto_review.get("human_review_recommended")),
            "weight": auto_review.get("weight", 0),
            "rationale": _clip_text(auto_review.get("rationale", ""), 220),
            "human_review_reason": _clip_text(auto_review.get("human_review_reason", ""), 220),
        }
    return summary


def _cluster_pending_claims(claims: list[dict[str, Any]], max_linked_claims: int) -> list[dict[str, Any]]:
    by_entity: dict[str, list[dict[str, Any]]] = {}
    for claim in claims:
        key = str(claim.get("target_entity_id") or claim.get("target_entity_name") or "unknown")
        by_entity.setdefault(key, []).append(claim)

    clusters: list[dict[str, Any]] = []
    for key, group in by_entity.items():
        group_sorted = sorted(
            group,
            key=lambda claim: (
                -len(claim.get("source_snippet_ids", []) or []),
                -_coerce_float((claim.get("auto_review") or {}).get("weight"), 0.0) if isinstance(claim.get("auto_review"), dict) else 0.0,
                -_coerce_float(claim.get("story_question_confidence", claim.get("confidence")), 0.0),
                str(claim.get("claim_type", "")),
                str(claim.get("claim_id", "")),
            ),
        )
        contradiction_count = sum(1 for claim in group if str(claim.get("contradiction_notes", "")).strip())
        relationship_count = sum(1 for claim in group if str(claim.get("claim_type", "")).lower() == "relationship")
        warning_count = sum(1 for claim in group if claim.get("support_warnings"))
        evidence_count = sum(len(claim.get("source_snippet_ids", []) or []) for claim in group)
        auto_review_weight = sum(
            _coerce_float((claim.get("auto_review") or {}).get("weight"), 0.0)
            for claim in group
            if isinstance(claim.get("auto_review"), dict)
        )
        clusters.append(
            {
                "cluster_id": stable_id("story_claim_cluster", key),
                "target_entity_id": group_sorted[0].get("target_entity_id", ""),
                "target_entity_name": group_sorted[0].get("target_entity_name", key),
                "claim_count": len(group),
                "evidence_count": evidence_count,
                "contradiction_count": contradiction_count,
                "relationship_count": relationship_count,
                "warning_count": warning_count,
                "linked_claim_ids": [str(claim.get("claim_id", "")) for claim in group_sorted[:max_linked_claims]],
                "claims": [_claim_summary(claim) for claim in group_sorted[:max_linked_claims]],
                "score": evidence_count + contradiction_count * 6 + relationship_count * 3 + warning_count * 2 + len(group) + auto_review_weight,
            }
        )
    return sorted(clusters, key=lambda item: (-int(item.get("score", 0)), str(item.get("target_entity_name", ""))))[:MAX_QUESTION_CLUSTERS]


def _prior_qas(session: dict[str, Any], limit: int = 12) -> list[dict[str, Any]]:
    answers_by_question = {
        str(answer.get("question_id", "")): answer
        for answer in session.get("answers", [])[-limit:]
        if isinstance(answer, dict)
    }
    out: list[dict[str, Any]] = []
    for question in session.get("questions", [])[-limit:]:
        if not isinstance(question, dict):
            continue
        answer = answers_by_question.get(str(question.get("question_id", "")))
        out.append(
            {
                "question_id": question.get("question_id", ""),
                "question_text": question.get("question_text", ""),
                "status": question.get("status", ""),
                "answer_text": answer.get("answer_text", "") if answer else "",
                "application_summary": question.get("application_summary", ""),
            }
        )
    return out


def _build_question_state(
    root: Path,
    session: dict[str, Any],
    provider_config: dict[str, Any],
    *,
    excluded_claim_ids: set[str] | None = None,
) -> dict[str, Any]:
    cfg = story_question_config(provider_config)
    excluded = excluded_claim_ids or set()
    all_claims = [
        claim
        for claim in pending_claims_for_story(root)
        if str(claim.get("claim_id", "")).strip() not in excluded
    ]
    claims, active_status = _select_question_claim_tier(all_claims)
    clusters = _cluster_pending_claims(claims, cfg["max_linked_claims_per_question"])
    source_ids: list[str] = []
    for cluster in clusters[:6]:
        for claim in cluster.get("claims", []) if isinstance(cluster.get("claims", []), list) else []:
            source_ids.extend(str(item) for item in claim.get("source_snippet_ids", []) or [])
    snippets = _snippet_context(root, source_ids, limit=MAX_EVIDENCE_SNIPPETS_FOR_QUESTION)
    conversation_ids = {str(item.get("conversation_id", "")) for item in snippets if str(item.get("conversation_id", "")).strip()}
    memory = load_review_memory(Path("canon/review_memory.json"))
    return {
        "unresolved_claim_count": len(claims),
        "story_candidate_count": len(all_claims),
        "story_claim_status_counts": _story_review_status_counts(all_claims),
        "active_story_review_status": active_status,
        "active_story_review_label": STORY_REVIEW_STATUS_LABELS.get(active_status, active_status),
        "eligible_claim_ids": [str(claim.get("claim_id", "")) for claim in claims if str(claim.get("claim_id", ""))],
        "excluded_claim_count": len(excluded),
        "application_policy": cfg["application_policy"],
        "max_linked_claims": cfg["max_linked_claims_per_question"],
        "clusters": clusters,
        "evidence_snippets": snippets,
        "patch_notes": _patch_note_context(root, conversation_ids, limit=MAX_PATCH_NOTES_FOR_QUESTION),
        "prior_story_questions": _prior_qas(session),
        "review_memory_story_answers": memory.get("story_question_answers", [])[-20:],
    }


def _question_prompt(state: dict[str, Any]) -> str:
    return f"""You are acting as a senior lore editor for THERIAC.
Generate exactly one high-value question for the author to answer during claim review.

Use the current unresolved claim state, prior Q/A history, evidence snippets, patch notes, and review memory.
The question should resolve as many uncertain claims as possible while staying answerable in plain English.
Question priority order:
1. Completely unanswered claims with no claim-review decision.
2. Claims whose auto-review or pending-decision state explicitly requests human review.
3. Remaining auto-reviewed claims as lower-priority machine priors.
The current prompt state has already selected the highest-priority tier with unreserved claims; do not reach outside the provided eligible claims.
CRITICAL: The question you generate MUST be broad enough to directly resolve ALL of the claims you include in your `linked_claim_ids` array. Do not link a claim if the answer to your question will not definitively prove or disprove it.
Every linked claim's main subject and predicate must be named or clearly paraphrased in the question text. Do not include linked claims merely because they share the same entity cluster.
Prefer major contradictions, recurring entity relationships, entity identity/type confusion, plot-purpose uncertainty, timeline confusion, and claims with many evidence links.
Claims may include an `auto_review` object and `story_question_confidence`.
Treat auto-review as a machine prior, not a final decision: accepted auto-reviewed
claims are more likely to be valid, rejected auto-reviewed claims are more likely
to be false/noisy, and human_review_recommended lowers certainty. You may still
link auto-reviewed claims when an author answer would usefully confirm, reject,
or clarify them.

Do not ask for a checklist. Ask one focused question. The next question will be generated after the answer is applied to the reduced claim list.

State:
```json
{json.dumps(state, ensure_ascii=False, separators=(",", ":"))}
```

Return strict JSON only:
{{
  "question_text": "one focused author-facing question",
  "focus_type": "relationship|identity|entity_type|plot_role|timeline|contradiction|theme|mechanic|other",
  "rationale": "why this is the highest-value next question",
  "linked_claim_ids": ["claim ids this question is meant to resolve"],
  "linked_entities": [{{"entity_id": "", "name": ""}}],
  "evidence_snippet_ids": ["snippet ids worth showing"],
  "expected_resolution": "what a good answer would clarify"
}}
"""


def _coerce_text_list(value: Any, allowed: set[str] | None = None, limit: int | None = None) -> list[str]:
    out: list[str] = []
    for item in value if isinstance(value, list) else []:
        text = str(item).strip()
        if not text:
            continue
        if allowed is not None and text not in allowed:
            continue
        if text not in out:
            out.append(text)
        if limit is not None and len(out) >= limit:
            break
    return out


def _author_claim_text_is_meta(claim_text: str) -> bool:
    lower = str(claim_text or "").lower()
    return any(marker in lower for marker in AUTHOR_META_TEXT_MARKERS)


def _normalize_author_claim_type(raw_type: Any, claim_text: str) -> str:
    claim_type = str(raw_type or "lore_fact").strip()
    if claim_type not in VALID_AUTHOR_CLAIM_TYPES:
        claim_type = "lore_fact"
    lower = str(claim_text or "").lower()
    if claim_type in {"background", "role", "lore_fact", "other"}:
        if any(marker in lower for marker in ["inspired by", "inspiration", "originally developed", "developed based"]):
            return "inspiration"
        if _author_claim_text_is_meta(claim_text):
            return "meta_note"
    return claim_type


def _normalize_author_claim_track(raw_track: Any, claim_type: str, claim_text: str) -> str:
    track = str(raw_track or "").strip().lower()
    if track not in VALID_AUTHOR_KNOWLEDGE_TRACKS:
        track = "lore"
    if claim_type in {"meta_note", "open_question", "inspiration"}:
        return "meta"
    if _author_claim_text_is_meta(claim_text):
        return "meta"
    return track


def _answered_question_ids(session: dict[str, Any]) -> set[str]:
    return {str(answer.get("question_id", "")) for answer in session.get("answers", []) if isinstance(answer, dict)}


def _skipped_question_ids(session: dict[str, Any]) -> set[str]:
    return {str(item.get("question_id", "")) for item in session.get("skipped_questions", []) if isinstance(item, dict)}


def _question_is_unanswered(question: dict[str, Any], answered: set[str], skipped: set[str]) -> bool:
    question_id = str(question.get("question_id", "")).strip()
    if not question_id or question_id in answered or question_id in skipped:
        return False
    return str(question.get("status", "")).strip().lower() not in {"answered", "skipped", "superseded"}


def _unanswered_questions(session: dict[str, Any]) -> list[dict[str, Any]]:
    answered = _answered_question_ids(session)
    skipped = _skipped_question_ids(session)
    return [
        question
        for question in session.get("questions", [])
        if isinstance(question, dict) and _question_is_unanswered(question, answered, skipped)
    ]


def _current_unanswered_question(session: dict[str, Any]) -> dict[str, Any] | None:
    current_id = str(session.get("current_question_id", "")).strip()
    if not current_id:
        return None
    unanswered_by_id = {
        str(question.get("question_id", "")).strip(): question
        for question in _unanswered_questions(session)
    }
    return unanswered_by_id.get(current_id)


def _next_unanswered_question(session: dict[str, Any], *, exclude_question_id: str = "") -> dict[str, Any] | None:
    exclude = str(exclude_question_id or "").strip()
    for question in _unanswered_questions(session):
        question_id = str(question.get("question_id", "")).strip()
        if question_id and question_id != exclude:
            return question
    return None


def _activate_next_unanswered_question(session: dict[str, Any], *, exclude_question_id: str = "") -> dict[str, Any] | None:
    question = _next_unanswered_question(session, exclude_question_id=exclude_question_id)
    session["current_question_id"] = str(question.get("question_id", "")) if question else ""
    return question


def _reserved_question_claim_ids(session: dict[str, Any], *, exclude_question_id: str = "") -> set[str]:
    exclude = str(exclude_question_id or "").strip()
    out: set[str] = set()
    for question in _unanswered_questions(session):
        if exclude and str(question.get("question_id", "")).strip() == exclude:
            continue
        for claim_id in question.get("linked_claim_ids", []) or []:
            claim_id_text = str(claim_id).strip()
            if claim_id_text:
                out.add(claim_id_text)
    return out


def _available_story_claims(root: Path, session: dict[str, Any], *, excluded_claim_ids: set[str] | None = None) -> list[dict[str, Any]]:
    excluded = set(excluded_claim_ids or set())
    excluded.update(_reserved_question_claim_ids(session))
    return [
        claim
        for claim in pending_claims_for_story(root)
        if str(claim.get("claim_id", "")).strip() not in excluded
    ]


def _activate_existing_question_if_needed(root: Path, session: dict[str, Any]) -> dict[str, Any] | None:
    existing = _current_unanswered_question(session) or _next_unanswered_question(session)
    if existing:
        question_id = str(existing.get("question_id", "")).strip()
        if question_id and session.get("current_question_id") != question_id:
            session["current_question_id"] = question_id
            save_story_session(root, session)
    return existing


def _current_or_queued_question_count(session: dict[str, Any]) -> int:
    return len(_unanswered_questions(session))


def _queued_question_count(session: dict[str, Any]) -> int:
    current = _current_unanswered_question(session)
    current_id = str(current.get("question_id", "")).strip() if current else ""
    return sum(
        1
        for question in _unanswered_questions(session)
        if str(question.get("question_id", "")).strip() != current_id
    )


def generate_next_question(
    root: Path,
    provider_config_path: Path | None = Path("config/pipeline_config.json"),
    *,
    force_regenerate: bool = False,
    excluded_claim_ids: set[str] | None = None,
    respect_existing: bool = True,
    activate: bool = True,
    generation_mode: str = "single",
) -> dict[str, Any]:
    provider_config = _load_config(provider_config_path)
    cfg = story_question_config(provider_config)
    if not cfg["enabled"]:
        raise RuntimeError("Story Questions are disabled in pipeline_config.json.")
    session = load_story_session(root)
    existing = _activate_existing_question_if_needed(root, session) if respect_existing else _current_unanswered_question(session)
    if existing and respect_existing and not force_regenerate:
        return existing
    if existing and force_regenerate:
        existing["status"] = "superseded"
        session["pending_application_proposal"] = None

    excluded = set(excluded_claim_ids or set())
    all_pending_claims = [
        claim
        for claim in pending_claims_for_story(root)
        if str(claim.get("claim_id", "")).strip() not in excluded
    ]
    if not all_pending_claims:
        raise RuntimeError("No pending claims are available for Story Questions.")

    state = _build_question_state(root, session, provider_config, excluded_claim_ids=excluded)
    eligible_claim_ids = {str(item).strip() for item in state.get("eligible_claim_ids", []) if str(item).strip()}
    pending_claims = [
        claim
        for claim in all_pending_claims
        if str(claim.get("claim_id", "")).strip() in eligible_claim_ids
    ]
    if not pending_claims:
        raise RuntimeError("No eligible pending claims are available for the current Story Question priority tier.")
    prompt = _question_prompt(state)
    kwargs = model_call_kwargs(provider_config, STORY_TASK_NAME)
    kwargs["provider"] = cfg["provider"]
    kwargs["api_model"] = cfg["model"]
    kwargs["json_schema"] = STORY_QUESTION_SCHEMA
    response = call_model_chat(prompt=prompt, **kwargs)
    if not response:
        reason = get_model_runtime_status().get("last_model_skip_reason") or "provider_unavailable"
        _record_failure(
            root,
            "generate_question",
            reason,
            {"unresolved_claim_count": len(pending_claims), "prompt_chars": len(prompt)},
        )
        raise RuntimeError(f"Story Question generation failed: {reason}")

    allowed_claim_ids = {str(claim.get("claim_id", "")).strip() for claim in pending_claims}
    linked_claim_ids = _coerce_text_list(
        response.get("linked_claim_ids", []),
        allowed=allowed_claim_ids,
        limit=cfg["max_linked_claims_per_question"],
    )
    if not linked_claim_ids and state.get("clusters"):
        linked_claim_ids = _coerce_text_list(
            state["clusters"][0].get("linked_claim_ids", []),
            allowed=allowed_claim_ids,
            limit=cfg["max_linked_claims_per_question"],
        )
    question_text = re.sub(r"\s+", " ", str(response.get("question_text", "")).strip())
    if not question_text:
        _record_failure(root, "generate_question", "missing_question_text", {"response": response})
        raise RuntimeError("Story Question generation failed: model returned no question_text.")

    question_id = stable_id("story_question", session["session_id"], question_text, now_utc_iso())
    question = {
        "question_id": question_id,
        "session_id": session["session_id"],
        "status": "pending",
        "question_text": question_text,
        "focus_type": str(response.get("focus_type", "other") or "other"),
        "rationale": str(response.get("rationale", "") or ""),
        "linked_claim_ids": linked_claim_ids,
        "linked_entities": response.get("linked_entities", []) if isinstance(response.get("linked_entities", []), list) else [],
        "evidence_snippet_ids": _coerce_text_list(response.get("evidence_snippet_ids", []), limit=12),
        "expected_resolution": str(response.get("expected_resolution", "") or ""),
        "unresolved_claim_count": len(pending_claims),
        "excluded_claim_count": len(excluded),
        "generation_mode": generation_mode,
        "created_at_utc": now_utc_iso(),
        "provider": cfg["provider"],
        "model": cfg["model"],
    }
    session.setdefault("questions", []).append(question)
    if activate and not _current_unanswered_question(session):
        session["current_question_id"] = question_id
    session["last_unresolved_claim_count"] = len(pending_claims)
    session["last_model_rationale"] = question["rationale"]
    save_story_session(root, session)
    _append_jsonl(story_question_paths(root)["questions"], question)
    return question


def generate_all_questions(
    root: Path,
    provider_config_path: Path | None = Path("config/pipeline_config.json"),
    *,
    max_questions: int | None = None,
) -> dict[str, Any]:
    provider_config = _load_config(provider_config_path)
    cfg = story_question_config(provider_config)
    if not cfg["enabled"]:
        raise RuntimeError("Story Questions are disabled in pipeline_config.json.")
    limit = max(1, int(max_questions if max_questions is not None else cfg["generate_all_max_questions"]))
    created: list[dict[str, Any]] = []
    stopped_reason = "question_limit_reached"
    failure: dict[str, Any] | None = None
    while len(created) < limit:
        session = load_story_session(root)
        reserved = _reserved_question_claim_ids(session)
        available_claims = _available_story_claims(root, session)
        if not available_claims:
            stopped_reason = "no_unreserved_pending_claims"
            break
        active_before = _current_unanswered_question(session)
        try:
            question = generate_next_question(
                root,
                provider_config_path,
                excluded_claim_ids=reserved,
                respect_existing=False,
                activate=active_before is None,
                generation_mode="generate_all",
            )
        except RuntimeError as exc:
            stopped_reason = "generation_failed"
            failure = {"error": str(exc), "available_claim_count": len(available_claims)}
            break
        linked_ids = [str(item).strip() for item in question.get("linked_claim_ids", []) or [] if str(item).strip()]
        if not linked_ids:
            stopped_reason = "generated_question_without_linked_claims"
            break
        created.append(question)
    session = load_story_session(root)
    if not _current_unanswered_question(session):
        _activate_next_unanswered_question(session)
        save_story_session(root, session)
    reserved_after = _reserved_question_claim_ids(session)
    remaining = _available_story_claims(root, session)
    result = {
        "created_count": len(created),
        "created_questions": created,
        "reserved_claim_count": len(reserved_after),
        "remaining_unreserved_claim_count": len(remaining),
        "queue_count": _queued_question_count(session),
        "unanswered_question_count": _current_or_queued_question_count(session),
        "stopped_reason": stopped_reason,
        "max_questions": limit,
        "created_at_utc": now_utc_iso(),
    }
    if failure:
        result["failure"] = failure
    return result


def _record_failure(root: Path, operation: str, reason: str, context: dict[str, Any] | None = None) -> None:
    paths = story_question_paths(root)
    payload = _read_json_or_default(paths["failures"], {"failures": []})
    failures = payload.setdefault("failures", [])
    failures.append(
        {
            "failure_id": safe_uuid(),
            "operation": operation,
            "reason": reason,
            "context": context or {},
            "created_at_utc": now_utc_iso(),
        }
    )
    write_json(paths["failures"], payload)


def _claims_by_id(claims: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    return {str(claim.get("claim_id", "")).strip(): claim for claim in claims if str(claim.get("claim_id", "")).strip()}


def _claims_for_application(root: Path, question: dict[str, Any], cfg: dict[str, Any]) -> list[dict[str, Any]]:
    pending = pending_claims_for_story(root)
    by_id = _claims_by_id(pending)
    linked_ids = [str(item) for item in question.get("linked_claim_ids", []) or [] if str(item) in by_id]
    entity_ids = {
        str(by_id[claim_id].get("target_entity_id", "")).strip()
        for claim_id in linked_ids
        if str(by_id[claim_id].get("target_entity_id", "")).strip()
    }
    selected: list[dict[str, Any]] = []
    for claim_id in linked_ids:
        selected.append(by_id[claim_id])
    for claim in pending:
        if len(selected) >= int(cfg["max_pending_claims_for_application"]):
            break
        claim_id = str(claim.get("claim_id", "")).strip()
        if claim_id in linked_ids:
            continue
        if str(claim.get("target_entity_id", "")).strip() in entity_ids:
            selected.append(claim)
    return selected


def _compact_application_proposal(proposal: dict[str, Any] | None) -> dict[str, Any]:
    if not proposal:
        return {}
    return {
        "proposal_id": proposal.get("proposal_id", ""),
        "summary": proposal.get("summary", ""),
        "claim_decisions": [
            {
                "claim_id": item.get("claim_id", ""),
                "decision": item.get("decision", ""),
                "edited_claim_text": item.get("edited_claim_text", ""),
                "confidence": item.get("confidence", item.get("application_confidence", "")),
                "rationale": _clip_text(item.get("rationale", ""), 260),
            }
            for item in proposal.get("claim_decisions", [])[:20]
            if isinstance(item, dict)
        ],
        "author_claims": [
            {
                "target_entity_name": item.get("target_entity_name", ""),
                "claim_type": item.get("claim_type", ""),
                "claim_text": _clip_text(item.get("claim_text", ""), 260),
                "knowledge_track": item.get("knowledge_track", ""),
                "confidence": item.get("confidence", ""),
                "rationale": _clip_text(item.get("rationale", item.get("review_rationale", "")), 260),
            }
            for item in proposal.get("author_claims", [])[:20]
            if isinstance(item, dict)
        ],
        "left_pending": proposal.get("left_pending", [])[:20] if isinstance(proposal.get("left_pending", []), list) else [],
    }


def _application_prompt(
    question: dict[str, Any],
    answer: dict[str, Any],
    claims: list[dict[str, Any]],
    root: Path,
    cfg: dict[str, Any],
    *,
    reviewer_critique: str = "",
    prior_proposal: dict[str, Any] | None = None,
) -> str:
    source_ids: list[str] = []
    for claim in claims:
        source_ids.extend(str(item) for item in claim.get("source_snippet_ids", []) or [])
    snippets = _snippet_context(root, source_ids, limit=16)
    state = {
        "application_policy": cfg["application_policy"],
        "linked_claim_ids": question.get("linked_claim_ids", []),
        "question": question,
        "answer": answer,
        "candidate_claims": [_claim_summary(claim, application=True) for claim in claims],
        "evidence_snippets": snippets,
        "reviewer_critique": reviewer_critique,
        "prior_application_proposal": _compact_application_proposal(prior_proposal),
    }
    return f"""You are applying an authoritative author answer to THERIAC claim review.
Use a moderate application policy:
- Apply the answer to directly linked claims when the answer clearly resolves them.
- Apply it to non-linked candidate claims only when they are close duplicates or clear entailments, with confidence >= 0.85.
- CRITICAL: Do NOT reject or defer claims simply because the answer does not mention them. The answer addresses one focused topic; most candidate claims will be about unrelated topics. Absence of support is NOT grounds for rejection. Only reject a claim when the answer ACTIVELY AND SPECIFICALLY CONTRADICTS it.
- If a claim is unrelated to the question or the answer does not address it, do NOT include it in claim_decisions at all - leave it out entirely so it stays pending.
- Output no more than 16 claim_decisions. Prefer directly linked claims first; omit lower-priority non-linked decisions if the response would be long.
- Output no more than 6 author_claims.
- Keep summary under 60 words and each rationale under 20 words.
- Leave edited_claim_text blank unless the candidate wording is materially misleading.
- Only include left_pending entries for linked claims that the answer does not resolve.
- Do not bulk approve broad categories from vague wording.
- Create author claims only for direct canonical facts from the answer that are not already captured by accepted claim decisions.
- Classify author claims by track carefully: use knowledge_track "lore" only for in-world facts that would be true inside THERIAC's fiction; use "meta" for game mechanics, player-facing design, production/design history, authorial clarification, naming history, working names, inspirations, references, or statements about how concepts were developed. Use "both" only when the same claim explicitly contains inseparable in-world and meta information.
- Do not mark naming/development-history claims as lore just because they concern a lore entity. Claims about working names becoming canonical names, characters originally being developed from a theme, or external inspirations should be "meta" and usually claim_type "meta_note" or "inspiration".
- Leave ambiguous claims pending.
- If reviewer_critique is present, revise the prior proposal in response to that critique rather than defending it.

State:
```json
{json.dumps(state, ensure_ascii=False, separators=(",", ":"))}
```

Return strict JSON only:
{{
  "summary": "short summary of how the answer was applied",
  "claim_decisions": [
    {{
      "claim_id": "candidate claim id",
      "decision": "accept|reject|defer|needs_more_context",
      "edited_claim_text": "optional improved wording when accepting",
      "confidence": 0.0,
      "rationale": "why the answer supports this decision"
    }}
  ],
  "author_claims": [
    {{
      "target_entity_id": "optional resolved entity id",
      "target_entity_name": "entity name",
      "claim_type": "relationship|role|background|timeline|inspiration|open_question|alias|lore_fact|meta_note|other",
      "claim_text": "authoritative fact from the answer",
      "knowledge_track": "lore|meta|both",
      "confidence": 0.0,
      "rationale": "why this should be added as an author claim"
    }}
  ],
  "left_pending": [
    {{"claim_id": "candidate claim id", "reason": "why it remains unresolved"}}
  ]
}}
"""


def _application_retry_prompt(
    question: dict[str, Any],
    answer: dict[str, Any],
    claims: list[dict[str, Any]],
    cfg: dict[str, Any],
    *,
    reviewer_critique: str = "",
    prior_proposal: dict[str, Any] | None = None,
) -> str:
    linked_ids = {str(item) for item in question.get("linked_claim_ids", []) or []}
    linked_claims = [claim for claim in claims if str(claim.get("claim_id", "") or "") in linked_ids]
    unlinked_claims = [claim for claim in claims if str(claim.get("claim_id", "") or "") not in linked_ids]
    retry_claims = linked_claims + unlinked_claims[: max(0, 8 - len(linked_claims))]
    compact_claims: list[dict[str, Any]] = []
    for claim in retry_claims[:16]:
        claim_id = str(claim.get("claim_id", "") or "")
        compact_claims.append(
            {
                "claim_id": claim_id,
                "linked_to_question": claim_id in linked_ids,
                "target_entity_id": claim.get("target_entity_id", ""),
                "target_entity_name": claim.get("target_entity_name", ""),
                "knowledge_track": claim.get("knowledge_track", ""),
                "claim_type": claim.get("claim_type", ""),
                "claim_text": _clip_text(claim.get("claim_text", ""), 300),
                "confidence": claim.get("confidence"),
            }
        )
    state = {
        "application_policy": cfg["application_policy"],
        "question": {
            "question_id": question.get("question_id", ""),
            "question_text": question.get("question_text", ""),
            "linked_claim_ids": question.get("linked_claim_ids", []),
            "linked_entities": question.get("linked_entities", []),
        },
        "answer": {
            "answer_id": answer.get("answer_id", ""),
            "question_id": answer.get("question_id", ""),
            "answer_text": answer.get("answer_text", ""),
        },
        "candidate_claims": compact_claims,
        "reviewer_critique": reviewer_critique,
        "prior_application_proposal": _compact_application_proposal(prior_proposal),
    }
    return f"""The previous story-answer application response was not parseable JSON.
Retry the task using this reduced state. Return exactly one JSON object and no markdown, commentary, explanation, or code fence.

Rules:
- Apply the answer only to claims it directly resolves.
- CRITICAL: Do NOT reject or defer claims simply because the answer does not mention them. Absence of support is NOT grounds for rejection. Only reject a claim when the answer ACTIVELY AND SPECIFICALLY CONTRADICTS it. If a claim is unrelated to the question or answer, omit it from claim_decisions entirely.
- Output at most 12 claim_decisions. Prefer linked claims. Do not decide non-linked claims unless they are exact duplicates.
- Output at most 4 author_claims.
- Keep summary under 40 words and each rationale under 15 words.
- Only include left_pending for linked claims that remain unresolved.
- Create author claims for direct canonical facts in the answer that are not already captured by claim decisions.
- Use knowledge_track "lore" only for in-world facts. Use "meta" for design history, working names, inspirations, references, gameplay, or production notes.
- Leave ambiguous claims pending.

Reduced state JSON:
{json.dumps(state, ensure_ascii=False, separators=(",", ":"))}

Required JSON shape:
{{
  "summary": "short summary of how the answer was applied",
  "claim_decisions": [
    {{"claim_id": "candidate claim id", "decision": "accept|reject|defer|needs_more_context", "edited_claim_text": "", "confidence": 0.0, "rationale": "why"}}
  ],
  "author_claims": [
    {{"target_entity_id": "optional resolved entity id", "target_entity_name": "entity name", "claim_type": "relationship|role|background|timeline|inspiration|open_question|alias|lore_fact|meta_note|other", "claim_text": "authoritative fact from the answer", "knowledge_track": "lore|meta|both", "confidence": 0.0, "rationale": "why"}}
  ],
  "left_pending": [
    {{"claim_id": "candidate claim id", "reason": "why it remains unresolved"}}
  ]
}}
"""


def _affirmative_confirmation_response(
    question: dict[str, Any],
    answer: dict[str, Any],
    candidate_claims: list[dict[str, Any]],
) -> dict[str, Any] | None:
    answer_text = str(answer.get("answer_text", "") or "").strip()
    if not AFFIRM_ALL_ANSWER_RE.match(answer_text):
        return None
    linked_ids = {str(item) for item in question.get("linked_claim_ids", []) or []}
    linked_claims = [
        claim
        for claim in candidate_claims
        if str(claim.get("claim_id", "") or "").strip() in linked_ids
    ]
    if not linked_claims:
        return None
    return {
        "summary": "The author confirmed the question's linked claims as correct.",
        "claim_decisions": [
            {
                "claim_id": str(claim.get("claim_id", "") or ""),
                "decision": "accept",
                "edited_claim_text": "",
                "confidence": 0.95,
                "rationale": "Author confirmed all linked points.",
            }
            for claim in linked_claims[:16]
        ],
        "author_claims": [],
        "left_pending": [],
        "_deterministic_confirmation": True,
    }


def _resolve_author_target(raw: dict[str, Any], entities: list[dict[str, Any]], fallback_claims: list[dict[str, Any]]) -> dict[str, Any] | None:
    by_id = {str(entity.get("entity_id", "")).strip(): entity for entity in entities}
    by_name: dict[str, dict[str, Any]] = {}
    for entity in entities:
        name = str(entity.get("canonical_name", "")).strip()
        if name:
            by_name[normalized_name_key(name)] = entity
        for alias in entity.get("aliases", []) or []:
            alias_text = str(alias).strip()
            if alias_text:
                by_name[normalized_name_key(alias_text)] = entity
    target_id = str(raw.get("target_entity_id", "")).strip()
    if target_id and target_id in by_id:
        return by_id[target_id]
    target_name = str(raw.get("target_entity_name") or raw.get("canonical_name") or "").strip()
    if target_name and normalized_name_key(target_name) in by_name:
        return by_name[normalized_name_key(target_name)]
    fallback_entity_ids = {
        str(claim.get("target_entity_id", "")).strip()
        for claim in fallback_claims
        if str(claim.get("target_entity_id", "")).strip()
    }
    if len(fallback_entity_ids) == 1:
        return by_id.get(next(iter(fallback_entity_ids)))
    return None


def _append_author_claims(root: Path, raw_claims: list[dict[str, Any]], fallback_claims: list[dict[str, Any]], answer: dict[str, Any]) -> list[dict[str, Any]]:
    if not raw_claims:
        return []
    entities = load_entity_records(root / "05_alias" / "resolved_entities.json")
    path = story_question_paths(root)["author_claims"]
    payload = _read_json_or_default(path, {"claims": []})
    claims = payload.setdefault("claims", [])
    existing_ids = {
        str(claim.get("claim_id", "")).strip()
        for claim in claims
        if isinstance(claim, dict) and str(claim.get("claim_id", "")).strip()
    }
    created: list[dict[str, Any]] = []
    for raw in raw_claims:
        if not isinstance(raw, dict):
            continue
        claim_text = re.sub(r"\s+", " ", str(raw.get("claim_text", "")).strip())
        if not claim_text:
            continue
        entity = _resolve_author_target(raw, entities, fallback_claims)
        if not entity:
            continue
        target_entity_id = str(entity.get("entity_id", "")).strip()
        target_name = str(entity.get("canonical_name", raw.get("target_entity_name", ""))).strip()
        claim_type = _normalize_author_claim_type(raw.get("claim_type", "lore_fact"), claim_text)
        knowledge_track = _normalize_author_claim_track(raw.get("knowledge_track", ""), claim_type, claim_text)
        claim_id = stable_id("author_claim", target_entity_id, claim_type, claim_text)
        if claim_id in existing_ids:
            continue
        row = {
            "claim_id": claim_id,
            "target_entity_id": target_entity_id,
            "target_card_id": str(entity.get("card_id") or card_id_for_entity(target_name)),
            "target_entity_name": target_name,
                "knowledge_track": knowledge_track,
            "claim_text": claim_text,
            "claim_type": claim_type,
            "source_snippet_ids": [],
            "confidence": float(raw.get("confidence", 1.0) or 1.0),
            "status": "accepted",
            "contradiction_notes": "",
            "created_at_utc": now_utc_iso(),
            "reviewer": "story_question_answer",
            "review_rationale": str(raw.get("rationale", "") or ""),
            "manual_claim": True,
            "author_claim": True,
            "source_priority": "story_question_answer",
            "story_question_id": answer.get("question_id", ""),
            "answer_id": answer.get("answer_id", ""),
            "normalized_claim_text": normalize_claim_text(claim_text),
        }
        claims.append(row)
        existing_ids.add(claim_id)
        created.append(row)
    write_json(path, payload)
    return created


def _proposed_author_claims(
    root: Path,
    raw_claims: list[dict[str, Any]],
    fallback_claims: list[dict[str, Any]],
    answer: dict[str, Any],
) -> list[dict[str, Any]]:
    if not raw_claims:
        return []
    entities = load_entity_records(root / "05_alias" / "resolved_entities.json")
    existing_payload = _read_json_or_default(story_question_paths(root)["author_claims"], {"claims": []})
    existing_ids = {
        str(claim.get("claim_id", "")).strip()
        for claim in existing_payload.get("claims", [])
        if isinstance(claim, dict) and str(claim.get("claim_id", "")).strip()
    }
    proposed: list[dict[str, Any]] = []
    for raw in raw_claims:
        if not isinstance(raw, dict):
            continue
        claim_text = re.sub(r"\s+", " ", str(raw.get("claim_text", "")).strip())
        if not claim_text:
            continue
        entity = _resolve_author_target(raw, entities, fallback_claims)
        if not entity:
            continue
        target_entity_id = str(entity.get("entity_id", "")).strip()
        target_name = str(entity.get("canonical_name", raw.get("target_entity_name", ""))).strip()
        claim_type = _normalize_author_claim_type(raw.get("claim_type", "lore_fact"), claim_text)
        knowledge_track = _normalize_author_claim_track(raw.get("knowledge_track", ""), claim_type, claim_text)
        claim_id = stable_id("author_claim", target_entity_id, claim_type, claim_text)
        if claim_id in existing_ids:
            continue
        proposed.append(
            {
                "claim_id": claim_id,
                "target_entity_id": target_entity_id,
                "target_card_id": str(entity.get("card_id") or card_id_for_entity(target_name)),
                "target_entity_name": target_name,
            "knowledge_track": knowledge_track,
                "claim_text": claim_text,
                "claim_type": claim_type,
                "confidence": float(raw.get("confidence", 1.0) or 1.0),
                "status": "proposed",
                "rationale": str(raw.get("rationale", "") or ""),
                "reviewer": "story_question_answer",
                "source_priority": "story_question_answer",
                "story_question_id": answer.get("question_id", ""),
                "answer_id": answer.get("answer_id", ""),
            }
        )
    return proposed


def _unsupported_negative_decision_reason(action: str, rationale: str) -> str:
    if action in PENDING_CLAIM_DECISIONS:
        return "pending_decision_left_unwritten"
    if action != "reject":
        return ""
    lower = str(rationale or "").lower()
    has_absence_marker = any(marker in lower for marker in ABSENCE_NEGATIVE_RATIONALE_MARKERS)
    has_explicit_marker = any(marker in lower for marker in EXPLICIT_CONTRADICTION_MARKERS)
    if has_absence_marker and not has_explicit_marker:
        return "negative_decision_based_on_absence"
    if "contradict" in lower and not has_explicit_marker:
        return "negative_decision_lacks_explicit_contradiction"
    return ""


def _proposed_claim_decisions(
    raw_decisions: list[dict[str, Any]],
    candidate_claims: list[dict[str, Any]],
    question: dict[str, Any],
    answer: dict[str, Any],
    cfg: dict[str, Any],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    candidate_by_id = _claims_by_id(candidate_claims)
    linked_ids = {str(item) for item in question.get("linked_claim_ids", []) or []}
    proposed: list[dict[str, Any]] = []
    dropped: list[dict[str, Any]] = []
    for raw in raw_decisions:
        if not isinstance(raw, dict):
            continue
        claim_id = str(raw.get("claim_id", "")).strip()
        if claim_id not in candidate_by_id:
            dropped.append({"claim_id": claim_id, "reason": "claim_id_not_in_candidate_set"})
            continue
        action = str(raw.get("decision", "")).strip().lower()
        if action not in VALID_CLAIM_DECISIONS:
            dropped.append({"claim_id": claim_id, "reason": f"invalid_decision:{action}"})
            continue
        try:
            confidence = float(raw.get("confidence", raw.get("application_confidence", 0.0)) or 0.0)
        except (TypeError, ValueError):
            confidence = 0.0
        min_confidence = float(cfg["linked_claim_min_confidence"]) if claim_id in linked_ids else float(cfg["unlinked_claim_min_confidence"])
        if confidence < min_confidence:
            dropped.append({"claim_id": claim_id, "reason": "below_confidence_threshold", "confidence": confidence})
            continue
        claim = candidate_by_id[claim_id]
        rationale = str(raw.get("rationale", "") or "")
        unsupported_reason = _unsupported_negative_decision_reason(action, rationale)
        if unsupported_reason:
            dropped.append(
                {
                    "claim_id": claim_id,
                    "reason": unsupported_reason,
                    "decision": action,
                    "rationale": rationale,
                }
            )
            continue
        row = {
            "proposal_decision_id": stable_id("story_proposed_claim_decision", answer["answer_id"], claim_id, action),
            "claim_id": claim_id,
            "decision": action,
            "confidence": confidence,
            "application_confidence": confidence,
            "rationale": rationale,
            "target_entity_id": str(claim.get("target_entity_id", "") or ""),
            "target_entity_name": str(claim.get("target_entity_name", "") or ""),
            "claim_type": str(claim.get("claim_type", "") or ""),
            "candidate_claim_text": str(claim.get("claim_text", "") or ""),
            "linked_to_question": claim_id in linked_ids,
            "reviewer": "story_question_answer",
            "human_override": True,
            "story_question_id": question.get("question_id", ""),
            "answer_id": answer.get("answer_id", ""),
        }
        edited_text = re.sub(r"\s+", " ", str(raw.get("edited_claim_text", "")).strip())
        if edited_text and action == "accept":
            row["edited_claim_text"] = edited_text
        proposed.append(row)
    return proposed, dropped


def _append_claim_decisions(
    root: Path,
    raw_decisions: list[dict[str, Any]],
    candidate_claims: list[dict[str, Any]],
    question: dict[str, Any],
    answer: dict[str, Any],
    cfg: dict[str, Any],
) -> list[dict[str, Any]]:
    paths = story_question_paths(root)
    payload = _read_json_or_default(paths["claim_decisions"], {"decisions": []})
    decisions = payload.setdefault("decisions", [])
    candidate_by_id = _claims_by_id(candidate_claims)
    linked_ids = {str(item) for item in question.get("linked_claim_ids", []) or []}
    written: list[dict[str, Any]] = []
    for raw in raw_decisions:
        if not isinstance(raw, dict):
            continue
        claim_id = str(raw.get("claim_id", "")).strip()
        if claim_id not in candidate_by_id:
            continue
        action = str(raw.get("decision", "")).strip().lower()
        if action not in VALID_CLAIM_DECISIONS:
            continue
        if action in PENDING_CLAIM_DECISIONS:
            continue
        if _unsupported_negative_decision_reason(action, str(raw.get("rationale", "") or "")):
            continue
        try:
            confidence = float(raw.get("confidence", 0.0) or 0.0)
        except (TypeError, ValueError):
            confidence = 0.0
        if claim_id in linked_ids:
            if confidence < float(cfg["linked_claim_min_confidence"]):
                continue
        elif confidence < float(cfg["unlinked_claim_min_confidence"]):
            continue
        row = {
            "decision_id": stable_id("claim_decision", answer["answer_id"], claim_id, action),
            "claim_id": claim_id,
            "decision": action,
            "reviewer": "story_question_answer",
            "rationale": str(raw.get("rationale", "") or ""),
            "timestamp_utc": now_utc_iso(),
            "human_override": True,
            "story_question_id": question.get("question_id", ""),
            "answer_id": answer.get("answer_id", ""),
            "application_confidence": confidence,
        }
        edited_text = re.sub(r"\s+", " ", str(raw.get("edited_claim_text", "")).strip())
        if edited_text and action == "accept":
            row["edited_claim_text"] = edited_text
        decisions.append(row)
        written.append(row)
    write_json(paths["claim_decisions"], payload)
    return written


def _append_skipped_question_claim_discards(root: Path, question: dict[str, Any], reason: str, skipped_at_utc: str) -> list[dict[str, Any]]:
    linked_ids = [str(item).strip() for item in question.get("linked_claim_ids", []) or [] if str(item).strip()]
    if not linked_ids:
        return []
    candidate_by_id = _claims_by_id(load_claims(root))
    paths = story_question_paths(root)
    payload = _read_json_or_default(paths["claim_decisions"], {"decisions": []})
    decisions = payload.setdefault("decisions", [])
    written: list[dict[str, Any]] = []
    for claim_id in linked_ids:
        claim = candidate_by_id.get(claim_id, {})
        row = {
            "decision_id": stable_id("claim_decision", str(question.get("question_id", "")), claim_id, "story_question_skip"),
            "claim_id": claim_id,
            "decision": "reject",
            "reviewer": "story_question_skip",
            "rationale": reason or "Question skipped; linked claim discarded.",
            "timestamp_utc": skipped_at_utc,
            "human_override": True,
            "story_question_id": question.get("question_id", ""),
            "skip_discard": True,
            "application_confidence": 1.0,
        }
        if claim:
            row.update(
                {
                    "target_entity_id": str(claim.get("target_entity_id", "") or ""),
                    "target_entity_name": str(claim.get("target_entity_name", "") or ""),
                    "claim_type": str(claim.get("claim_type", "") or ""),
                    "candidate_claim_text": str(claim.get("claim_text", "") or ""),
                }
            )
        decisions.append(row)
        written.append(row)
    write_json(paths["claim_decisions"], payload)
    return written


def _active_question_by_id(session: dict[str, Any], question_id: str | None = None) -> dict[str, Any] | None:
    target_question_id = question_id or str(session.get("current_question_id", "")).strip()
    if not target_question_id:
        return None
    for item in reversed(session.get("questions", [])):
        if isinstance(item, dict) and str(item.get("question_id", "")) == target_question_id:
            return item
    return None


def propose_story_answer_application(
    root: Path,
    answer_text: str,
    provider_config_path: Path | None = Path("config/pipeline_config.json"),
    *,
    question_id: str | None = None,
    reviewer: str = "human_reviewer",
    reviewer_critique: str = "",
) -> dict[str, Any]:
    clean_answer = re.sub(r"\s+", " ", str(answer_text or "").strip())
    if not clean_answer:
        raise ValueError("Answer text is required.")
    provider_config = _load_config(provider_config_path)
    cfg = story_question_config(provider_config)
    session = load_story_session(root)
    question = _active_question_by_id(session, question_id)
    if not question:
        raise RuntimeError("No active story question is available to answer.")

    answer = {
        "answer_id": stable_id("story_answer", str(question.get("question_id", "")), clean_answer, now_utc_iso()),
        "question_id": question.get("question_id", ""),
        "session_id": session.get("session_id", ""),
        "answer_text": clean_answer,
        "reviewer": reviewer or "human_reviewer",
        "created_at_utc": now_utc_iso(),
    }
    candidate_claims = _claims_for_application(root, question, cfg)
    prior_proposal = session.get("pending_application_proposal") if isinstance(session.get("pending_application_proposal"), dict) else None
    response = _affirmative_confirmation_response(question, answer, candidate_claims)
    retry_context: dict[str, Any] | None = None
    if response:
        retry_context = {
            "deterministic_confirmation": True,
            "reason": "affirmative_all_counts_answer",
            "model_call_skipped": True,
        }
    if not response:
        prompt = _application_prompt(
            question,
            answer,
            candidate_claims,
            root,
            cfg,
            reviewer_critique=re.sub(r"\s+", " ", str(reviewer_critique or "").strip()),
            prior_proposal=prior_proposal,
        )
        kwargs = model_call_kwargs(provider_config, STORY_TASK_NAME)
        kwargs["provider"] = cfg["provider"]
        kwargs["api_model"] = cfg["model"]
        kwargs["json_schema"] = ANSWER_APPLICATION_PROPOSAL_SCHEMA
        response = call_model_chat(prompt=prompt, **kwargs)
        if not response:
            reason = get_model_runtime_status().get("last_model_skip_reason") or "provider_unavailable"
            if reason == "content_parse_failed":
                retry_prompt = _application_retry_prompt(
                    question,
                    answer,
                    candidate_claims,
                    cfg,
                    reviewer_critique=re.sub(r"\s+", " ", str(reviewer_critique or "").strip()),
                    prior_proposal=prior_proposal,
                )
                kwargs["json_schema"] = ANSWER_APPLICATION_PROPOSAL_SCHEMA
                response = call_model_chat(prompt=retry_prompt, **kwargs)
                if response:
                    retry_context = {
                        "initial_reason": reason,
                        "initial_prompt_chars": len(prompt),
                        "retry_prompt_chars": len(retry_prompt),
                        "recovered": True,
                    }
                else:
                    retry_reason = get_model_runtime_status().get("last_model_skip_reason") or "provider_unavailable"
                    _record_failure(
                        root,
                        "propose_application",
                        "content_parse_failed_retry_failed",
                        {
                            "question_id": question.get("question_id", ""),
                            "answer_text": clean_answer,
                            "prompt_chars": len(prompt),
                            "retry_prompt_chars": len(retry_prompt),
                            "retry_reason": retry_reason,
                        },
                    )
                    raise RuntimeError(
                        f"Story Question answer application proposal failed: content_parse_failed after compact retry ({retry_reason})"
                    )
            else:
                _record_failure(
                    root,
                    "propose_application",
                    reason,
                    {"question_id": question.get("question_id", ""), "answer_text": clean_answer, "prompt_chars": len(prompt)},
                )
                raise RuntimeError(f"Story Question answer application proposal failed: {reason}")

    raw_decisions = response.get("claim_decisions", []) if isinstance(response.get("claim_decisions", []), list) else []
    claim_decisions, dropped_decisions = _proposed_claim_decisions(
        raw_decisions,
        candidate_claims,
        question,
        answer,
        cfg,
    )
    author_claims = _proposed_author_claims(
        root,
        response.get("author_claims", []) if isinstance(response.get("author_claims", []), list) else [],
        candidate_claims,
        answer,
    )
    proposal = {
        "proposal_id": stable_id("story_application_proposal", answer["answer_id"]),
        "question_id": question.get("question_id", ""),
        "answer_id": answer["answer_id"],
        "answer": answer,
        "answer_text": clean_answer,
        "status": "proposed",
        "summary": str(response.get("summary", "") or ""),
        "claim_decisions": claim_decisions,
        "author_claims": author_claims,
        "left_pending": response.get("left_pending", []) if isinstance(response.get("left_pending", []), list) else [],
        "dropped_decisions": dropped_decisions,
        "candidate_claim_count": len(candidate_claims),
        "unresolved_claim_count_before": len(pending_claims_for_story(root)),
        "reviewer_critique": re.sub(r"\s+", " ", str(reviewer_critique or "").strip()),
        "provider": cfg["provider"],
        "model": cfg["model"],
        "created_at_utc": now_utc_iso(),
    }
    if retry_context:
        proposal["model_retry"] = retry_context
    current_session = load_story_session(root)
    current_session["pending_application_proposal"] = proposal
    save_story_session(root, current_session)
    _append_jsonl(story_question_paths(root)["application_proposals"], proposal)
    return proposal


def discard_story_answer_application(root: Path, reason: str = "") -> dict[str, Any]:
    session = load_story_session(root)
    proposal = session.get("pending_application_proposal") if isinstance(session.get("pending_application_proposal"), dict) else None
    if not proposal:
        raise RuntimeError("No pending story answer proposal is available to discard.")
    proposal["status"] = "discarded"
    proposal["discarded_at_utc"] = now_utc_iso()
    proposal["discard_reason"] = reason
    session["pending_application_proposal"] = None
    save_story_session(root, session)
    _append_jsonl(story_question_paths(root)["application_proposals"], proposal)
    return proposal


def commit_story_answer_application(
    root: Path,
    provider_config_path: Path | None = Path("config/pipeline_config.json"),
    *,
    proposal_id: str | None = None,
) -> dict[str, Any]:
    provider_config = _load_config(provider_config_path)
    cfg = story_question_config(provider_config)
    session = load_story_session(root)
    proposal = session.get("pending_application_proposal") if isinstance(session.get("pending_application_proposal"), dict) else None
    if not proposal:
        raise RuntimeError("No pending story answer proposal is available to approve.")
    if proposal_id and str(proposal.get("proposal_id", "")) != str(proposal_id):
        raise RuntimeError("The pending story answer proposal does not match the requested proposal_id.")
    question = _active_question_by_id(session, str(proposal.get("question_id", "")))
    if not question:
        raise RuntimeError("No active story question is available for the pending proposal.")
    answer = proposal.get("answer") if isinstance(proposal.get("answer"), dict) else {}
    if not answer:
        raise RuntimeError("The pending story answer proposal is missing its answer record.")

    candidate_claims = _claims_for_application(root, question, cfg)
    claim_decisions = _append_claim_decisions(
        root,
        proposal.get("claim_decisions", []) if isinstance(proposal.get("claim_decisions", []), list) else [],
        candidate_claims,
        question,
        answer,
        cfg,
    )
    author_claims = _append_author_claims(
        root,
        proposal.get("author_claims", []) if isinstance(proposal.get("author_claims", []), list) else [],
        candidate_claims,
        answer,
    )
    application = {
        "application_id": stable_id("story_application", answer["answer_id"]),
        "proposal_id": proposal.get("proposal_id", ""),
        "question_id": question.get("question_id", ""),
        "answer_id": answer["answer_id"],
        "summary": str(proposal.get("summary", "") or ""),
        "claim_decisions": claim_decisions,
        "author_claims": author_claims,
        "left_pending": proposal.get("left_pending", []) if isinstance(proposal.get("left_pending", []), list) else [],
        "dropped_decisions": proposal.get("dropped_decisions", []) if isinstance(proposal.get("dropped_decisions", []), list) else [],
        "approved_at_utc": now_utc_iso(),
        "created_at_utc": now_utc_iso(),
        "unresolved_claim_count_after": len(pending_claims_for_story(root)),
    }

    question["status"] = "answered"
    question["answered_at_utc"] = answer["created_at_utc"]
    question["application_summary"] = application["summary"]
    session.setdefault("answers", []).append(answer)
    session.setdefault("applications", []).append(application)
    proposal["status"] = "approved"
    proposal["approved_at_utc"] = application["approved_at_utc"]
    session["pending_application_proposal"] = None
    session["last_unresolved_claim_count"] = application["unresolved_claim_count_after"]
    _activate_next_unanswered_question(session, exclude_question_id=str(question.get("question_id", "")))
    save_story_session(root, session)
    _append_jsonl(story_question_paths(root)["answers"], answer)
    _append_jsonl(story_question_paths(root)["applications"], application)
    _append_jsonl(story_question_paths(root)["application_proposals"], proposal)

    memory_path = Path("canon/review_memory.json")
    memory = load_review_memory(memory_path)
    remember_story_question_answer(
        memory,
        {
            **answer,
            "question_text": question.get("question_text", ""),
            "linked_claim_ids": question.get("linked_claim_ids", []),
            "target_entity_ids": sorted(
                {
                    str(claim.get("target_entity_id", "")).strip()
                    for claim in candidate_claims
                    if str(claim.get("target_entity_id", "")).strip()
                }
            ),
            "target_entity_names": sorted(
                {
                    str(claim.get("target_entity_name", "")).strip()
                    for claim in candidate_claims
                    if str(claim.get("target_entity_name", "")).strip()
                }
            ),
            "claim_decision_ids": [row.get("decision_id", "") for row in claim_decisions],
            "author_claim_ids": [row.get("claim_id", "") for row in author_claims],
            "application_summary": application["summary"],
        },
    )
    save_review_memory(memory_path, memory)
    return application


def apply_story_answer(
    root: Path,
    answer_text: str,
    provider_config_path: Path | None = Path("config/pipeline_config.json"),
    *,
    question_id: str | None = None,
    reviewer: str = "human_reviewer",
) -> dict[str, Any]:
    proposal = propose_story_answer_application(
        root,
        answer_text,
        provider_config_path,
        question_id=question_id,
        reviewer=reviewer,
    )
    return commit_story_answer_application(root, provider_config_path, proposal_id=str(proposal.get("proposal_id", "")))


def skip_current_question(root: Path, reason: str = "") -> dict[str, Any]:
    session = load_story_session(root)
    question = _current_unanswered_question(session)
    if not question:
        raise RuntimeError("No active story question is available to skip.")
    skipped_at_utc = now_utc_iso()
    discard_decisions = _append_skipped_question_claim_discards(root, question, reason, skipped_at_utc)
    record = {
        "question_id": question.get("question_id", ""),
        "reason": reason,
        "skipped_at_utc": skipped_at_utc,
        "discarded_claim_ids": [row.get("claim_id", "") for row in discard_decisions],
        "discarded_claim_count": len(discard_decisions),
    }
    question["status"] = "skipped"
    question["discarded_claim_ids"] = record["discarded_claim_ids"]
    question["discarded_claim_count"] = record["discarded_claim_count"]
    session.setdefault("skipped_questions", []).append(record)
    session["pending_application_proposal"] = None
    session["last_unresolved_claim_count"] = len(pending_claims_for_story(root))
    _activate_next_unanswered_question(session, exclude_question_id=str(question.get("question_id", "")))
    save_story_session(root, session)
    return record


def end_story_session(root: Path) -> dict[str, Any]:
    session = load_story_session(root)
    session["status"] = "ended"
    session["current_question_id"] = ""
    session["pending_application_proposal"] = None
    session["ended_at_utc"] = now_utc_iso()
    save_story_session(root, session)
    return session


def story_question_display(root: Path) -> dict[str, Any]:
    session = load_story_session(root)
    question = _activate_existing_question_if_needed(root, session)
    if question:
        session = load_story_session(root)
        question = _current_unanswered_question(session)
    pending = pending_claims_for_story(root)
    status_counts = _story_review_status_counts(pending)
    linked_claims: list[dict[str, Any]] = []
    snippets: list[dict[str, Any]] = []
    if question:
        by_id = _claims_by_id(pending)
        linked_claims = [_claim_summary(by_id[claim_id]) for claim_id in question.get("linked_claim_ids", []) if claim_id in by_id]
        source_ids = [str(item) for item in question.get("evidence_snippet_ids", []) or []]
        if not source_ids:
            for claim in linked_claims:
                source_ids.extend(str(item) for item in claim.get("source_snippet_ids", []) or [])
        snippets = _snippet_context(root, source_ids, limit=8)
    return {
        "session": session,
        "question": question,
        "pending_application_proposal": session.get("pending_application_proposal")
        if isinstance(session.get("pending_application_proposal"), dict)
        else None,
        "pending_claim_count": len(pending),
        "story_claim_status_counts": status_counts,
        "unanswered_claim_count": status_counts.get("unanswered", 0),
        "human_review_requested_claim_count": status_counts.get("human_review_requested_total", 0),
        "auto_reviewed_claim_count": status_counts.get("auto_reviewed", 0),
        "reserved_claim_count": len(_reserved_question_claim_ids(session)),
        "unanswered_question_count": _current_or_queued_question_count(session),
        "queued_question_count": _queued_question_count(session),
        "linked_claims": linked_claims,
        "evidence_snippets": snippets,
    }
