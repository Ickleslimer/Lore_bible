from __future__ import annotations

import re
from pathlib import Path
from typing import Any

from pipeline.common import now_utc_iso, read_json, write_json
from pipeline.entity_resolution import normalize_entity_type


MEMORY_VERSION = 1


def empty_review_memory() -> dict[str, Any]:
    return {
        "version": MEMORY_VERSION,
        "accepted_claims": [],
        "rejected_claims": [],
        "approved_aliases": [],
        "entity_merges": [],
        "approved_conversation_entities": [],
        "rejected_conversation_entities": [],
        "approved_cards": [],
        "author_directives": [],
        "card_architecture_actions": [],
        "card_redirects": [],
        "story_question_answers": [],
        "style_corrections": [],
        "updated_at_utc": now_utc_iso(),
    }


def load_review_memory(path: Path | None) -> dict[str, Any]:
    if not path or not path.exists():
        return empty_review_memory()
    payload = read_json(path)
    if not isinstance(payload, dict):
        return empty_review_memory()
    memory = empty_review_memory()
    memory.update(payload)
    for key in [
        "accepted_claims",
        "rejected_claims",
        "approved_aliases",
        "entity_merges",
        "approved_conversation_entities",
        "rejected_conversation_entities",
        "approved_cards",
        "author_directives",
        "card_architecture_actions",
        "card_redirects",
        "story_question_answers",
        "style_corrections",
    ]:
        if not isinstance(memory.get(key), list):
            memory[key] = []
    memory["version"] = MEMORY_VERSION
    return memory


def save_review_memory(path: Path, memory: dict[str, Any]) -> None:
    memory["version"] = MEMORY_VERSION
    memory["updated_at_utc"] = now_utc_iso()
    write_json(path, memory)


def normalize_memory_entity_type(entity_type: Any) -> str:
    return normalize_entity_type(entity_type, "term")


def normalize_claim_text(text: str) -> str:
    value = re.sub(r"\s+", " ", str(text).lower()).strip()
    value = re.sub(r"[^a-z0-9]+", " ", value)
    return re.sub(r"\s+", " ", value).strip()


def relevant_memory_for_entity(memory: dict[str, Any], entity_id: str, canonical_name: str = "") -> dict[str, Any]:
    names = {str(entity_id)}
    if canonical_name:
        names.add(canonical_name.lower())

    def matches(row: dict[str, Any]) -> bool:
        row_entity = str(row.get("target_entity_id") or row.get("entity_id") or row.get("card_id") or "")
        row_name = str(row.get("canonical_name") or row.get("entity_name") or "").lower()
        return row_entity in names or row_name in names

    global_author_directives = [
        x
        for x in memory.get("author_directives", [])
        if isinstance(x, dict)
        and (
            bool(x.get("global"))
            or str(x.get("scope", "")).lower() == "global"
            or (not str(x.get("target_card_id", "")).strip() and not str(x.get("target_entity_id", "")).strip())
        )
    ]
    entity_author_directives = [
        x
        for x in memory.get("author_directives", [])
        if isinstance(x, dict)
        and (
            str(x.get("target_card_id", "")) == entity_id
            or str(x.get("target_entity_id", "")) == entity_id
        )
    ]

    return {
        "accepted_claims": [x for x in memory.get("accepted_claims", []) if isinstance(x, dict) and matches(x)],
        "rejected_claims": [x for x in memory.get("rejected_claims", []) if isinstance(x, dict) and matches(x)],
        "approved_aliases": [
            x
            for x in memory.get("approved_aliases", [])
            if isinstance(x, dict)
            and (
                str(x.get("target_entity_id", "")) in names
                or str(x.get("canonical_name", "")).lower() in names
            )
        ],
        "entity_merges": [
            x
            for x in memory.get("entity_merges", [])
            if isinstance(x, dict)
            and (
                str(x.get("target_entity_id", "")) in names
                or str(x.get("source_entity_id", "")) in names
                or str(x.get("target_entity_name", "")).lower() in names
                or str(x.get("source_entity_name", "")).lower() in names
            )
        ],
        "approved_cards": [x for x in memory.get("approved_cards", []) if isinstance(x, dict) and matches(x)],
        "author_directives": global_author_directives + entity_author_directives,
        "card_architecture_actions": [
            x
            for x in memory.get("card_architecture_actions", [])[-100:]
            if isinstance(x, dict)
            and (
                str(x.get("target_entity_id", "")) in names
                or str(x.get("source_entity_id", "")) in names
                or str(x.get("target_entity_name", "")).lower() in names
                or str(x.get("source_entity_name", "")).lower() in names
            )
        ],
        "card_redirects": [
            x
            for x in memory.get("card_redirects", [])[-100:]
            if isinstance(x, dict)
            and (
                str(x.get("target_entity_id", "")) in names
                or str(x.get("source_entity_id", "")) in names
                or str(x.get("target_entity_name", "")).lower() in names
                or str(x.get("source_entity_name", "")).lower() in names
            )
        ],
        "story_question_answers": [
            x
            for x in memory.get("story_question_answers", [])[-50:]
            if isinstance(x, dict)
            and (
                str(x.get("target_entity_id", "")) in names
                or str(x.get("target_entity_name", "")).lower() in names
                or str(x.get("canonical_name", "")).lower() in names
            )
        ],
        "style_corrections": memory.get("style_corrections", [])[-10:],
    }


def rejected_claim_keys(memory: dict[str, Any], entity_id: str) -> set[str]:
    keys: set[str] = set()
    for claim in memory.get("rejected_claims", []) or []:
        if not isinstance(claim, dict):
            continue
        if str(claim.get("target_entity_id", "")) != str(entity_id):
            continue
        key = claim.get("normalized_claim_text") or normalize_claim_text(str(claim.get("claim_text", "")))
        if key:
            keys.add(str(key))
    return keys


def remember_claim_decisions(
    memory: dict[str, Any],
    claims: list[dict[str, Any]],
    decisions: list[dict[str, Any]],
) -> None:
    claim_by_id = {str(c.get("claim_id")): c for c in claims if isinstance(c, dict)}
    existing_accepted = {
        (str(c.get("target_entity_id")), str(c.get("normalized_claim_text") or normalize_claim_text(c.get("claim_text", ""))))
        for c in memory.get("accepted_claims", [])
        if isinstance(c, dict)
    }
    existing_rejected = {
        (str(c.get("target_entity_id")), str(c.get("normalized_claim_text") or normalize_claim_text(c.get("claim_text", ""))))
        for c in memory.get("rejected_claims", [])
        if isinstance(c, dict)
    }
    for decision in decisions:
        claim = claim_by_id.get(str(decision.get("claim_id")))
        if not claim:
            continue
        row = {
            **claim,
            "decision": decision.get("decision"),
            "reviewer": decision.get("reviewer", "reviewer"),
            "rationale": decision.get("rationale", ""),
            "reviewed_at_utc": decision.get("timestamp_utc", now_utc_iso()),
            "normalized_claim_text": normalize_claim_text(str(claim.get("claim_text", ""))),
        }
        key = (str(row.get("target_entity_id")), str(row.get("normalized_claim_text")))
        if decision.get("decision") == "accept" and key not in existing_accepted:
            memory.setdefault("accepted_claims", []).append(row)
            existing_accepted.add(key)
            remember_alias_from_claim(memory, row)
        elif decision.get("decision") == "reject" and key not in existing_rejected:
            memory.setdefault("rejected_claims", []).append(row)
            existing_rejected.add(key)


def remember_alias_from_claim(memory: dict[str, Any], claim: dict[str, Any]) -> None:
    if str(claim.get("claim_type", "")) != "alias":
        return
    alias_text = str(claim.get("alias_text", "")).strip()
    if not alias_text:
        return
    canonical_name = str(claim.get("target_entity_name", "")).strip()
    target_entity_id = str(claim.get("target_entity_id", "")).strip()
    existing = {
        (str(item.get("target_entity_id", "")), str(item.get("alias_text", "")).lower())
        for item in memory.get("approved_aliases", [])
        if isinstance(item, dict)
    }
    key = (target_entity_id, alias_text.lower())
    if key in existing:
        return
    memory.setdefault("approved_aliases", []).append(
        {
            "target_entity_id": target_entity_id,
            "canonical_name": canonical_name,
            "alias_text": alias_text,
            "source_claim_id": claim.get("claim_id", ""),
            "source_snippet_ids": claim.get("source_snippet_ids", []),
            "approved_at_utc": claim.get("reviewed_at_utc", now_utc_iso()),
        }
    )


def normalize_entity_name_key(text: str) -> str:
    value = re.sub(r"\s+", " ", str(text).lower()).strip()
    value = re.sub(r"[^a-z0-9]+", " ", value)
    return re.sub(r"\s+", " ", value).strip()


def remember_conversation_entity_decisions(
    memory: dict[str, Any],
    proposals: list[dict[str, Any]],
    decisions: list[dict[str, Any]],
) -> None:
    proposal_by_id = {
        str(proposal.get("proposal_id", "")): proposal
        for proposal in proposals
        if isinstance(proposal, dict) and str(proposal.get("proposal_id", "")).strip()
    }
    proposal_by_key = {
        normalize_entity_name_key(str(proposal.get("candidate_name") or proposal.get("normalized_name_key") or "")): proposal
        for proposal in proposals
        if isinstance(proposal, dict)
    }
    existing_approved = {
        (
            normalize_entity_name_key(str(item.get("candidate_name") or item.get("canonical_name") or "")),
            normalize_entity_name_key(str(item.get("canonical_name") or item.get("candidate_name") or "")),
        )
        for item in memory.get("approved_conversation_entities", [])
        if isinstance(item, dict)
    }
    existing_rejected = {
        (
            normalize_entity_name_key(str(item.get("candidate_name") or item.get("canonical_name") or "")),
            normalize_entity_name_key(str(item.get("canonical_name") or item.get("candidate_name") or "")),
        )
        for item in memory.get("rejected_conversation_entities", [])
        if isinstance(item, dict)
    }

    for decision in decisions:
        if not isinstance(decision, dict):
            continue
        action = str(decision.get("decision", "")).strip().lower()
        if action not in {"approve", "accept", "reject"}:
            continue
        proposal = proposal_by_id.get(str(decision.get("proposal_id", "")))
        if proposal is None:
            decision_key = normalize_entity_name_key(
                str(decision.get("candidate_name") or decision.get("canonical_name") or "")
            )
            proposal = proposal_by_key.get(decision_key)
        candidate_name = str(
            (proposal or {}).get("candidate_name")
            or decision.get("candidate_name")
            or decision.get("canonical_name")
            or ""
        ).strip()
        if not candidate_name:
            continue
        canonical_name = str(decision.get("canonical_name") or candidate_name).strip()
        entity_type = normalize_memory_entity_type(decision.get("entity_type") or (proposal or {}).get("proposed_entity_type") or "term")
        row = {
            "proposal_id": str(decision.get("proposal_id") or (proposal or {}).get("proposal_id") or ""),
            "candidate_name": candidate_name,
            "canonical_name": canonical_name,
            "entity_type": entity_type,
            "secondary_entity_types": [
                normalize_memory_entity_type(item)
                for item in (
                    decision.get("secondary_entity_types", [])
                    if isinstance(decision.get("secondary_entity_types", []), list)
                    else []
                )
            ],
            "aliases": decision.get("aliases", []) if isinstance(decision.get("aliases", []), list) else [],
            "source_snippet_ids": (proposal or {}).get("source_snippet_ids", []),
            "sample_texts": (proposal or {}).get("sample_texts", []),
            "decision": action,
            "reviewer": decision.get("reviewer", "reviewer"),
            "rationale": decision.get("rationale", ""),
            "reviewed_at_utc": decision.get("timestamp_utc", now_utc_iso()),
            "normalized_name_key": normalize_entity_name_key(canonical_name),
        }
        if action in {"approve", "accept"}:
            approved_key = (normalize_entity_name_key(candidate_name), normalize_entity_name_key(canonical_name))
            if approved_key and approved_key not in existing_approved:
                memory.setdefault("approved_conversation_entities", []).append(row)
                existing_approved.add(approved_key)
        elif action == "reject":
            rejected_key = (normalize_entity_name_key(candidate_name), normalize_entity_name_key(canonical_name))
            if rejected_key and rejected_key not in existing_rejected:
                memory.setdefault("rejected_conversation_entities", []).append(row)
                existing_rejected.add(rejected_key)


def remember_author_directives(memory: dict[str, Any], directives: list[dict[str, Any]]) -> None:
    existing = {str(x.get("directive_id")) for x in memory.get("author_directives", []) if isinstance(x, dict)}
    for directive in directives:
        if not isinstance(directive, dict):
            continue
        directive_id = str(directive.get("directive_id", ""))
        if directive_id and directive_id not in existing:
            memory.setdefault("author_directives", []).append(directive)
            existing.add(directive_id)


def remember_approved_cards(
    memory: dict[str, Any],
    cards: list[dict[str, Any]],
    decisions: list[dict[str, Any]],
) -> None:
    approved_ids = {
        str(d.get("card_id") or d.get("target_entity_id"))
        for d in decisions
        if str(d.get("decision", "")).lower() in {"accept", "approve"}
    }
    for card in cards:
        card_id = str(card.get("card_id", ""))
        entity_id = str(card.get("details", {}).get("entity_id", ""))
        if card_id in approved_ids or entity_id in approved_ids:
            memory.setdefault("approved_cards", []).append(
                {
                    "card_id": card_id,
                    "entity_id": card.get("details", {}).get("entity_id", ""),
                    "canonical_name": card.get("canonical_name", ""),
                    "summary": card.get("summary", ""),
                    "sections": card.get("details", {}).get("sections", {}),
                    "approved_at_utc": now_utc_iso(),
                }
            )


def remember_story_question_answer(memory: dict[str, Any], answer_record: dict[str, Any]) -> None:
    if not isinstance(answer_record, dict):
        return
    answer_id = str(answer_record.get("answer_id", "")).strip()
    existing = {
        str(item.get("answer_id", "")).strip()
        for item in memory.get("story_question_answers", [])
        if isinstance(item, dict)
    }
    if answer_id and answer_id in existing:
        return
    memory.setdefault("story_question_answers", []).append(answer_record)
