from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

from pipeline.artifact_paths import ArtifactPaths
from pipeline.common import get_logger, now_utc_iso, read_json, read_jsonl, safe_uuid, stable_id, write_json, write_jsonl
from pipeline.entity_resolution import card_id_for_entity, normalized_name_key
from pipeline.model_provider import call_model_chat, model_call_kwargs


CARD_EDIT_REQUESTS_FILENAME = "card_edit_requests.jsonl"
CARD_ARCHITECTURE_PROPOSALS_FILENAME = "card_architecture_proposals.json"
CARD_ARCHITECTURE_DECISIONS_FILENAME = "card_architecture_decisions.json"
CARD_ARCHITECTURE_APPLIED_FILENAME = "card_architecture_applied.json"
CARD_ARCHITECTURE_FAILURES_FILENAME = "card_architecture_failures.json"
CARD_REDIRECTS_FILENAME = "card_redirects.json"

VALID_CARD_ARCHITECTURE_ACTIONS = {
    "demote_card_to_section",
    "mark_not_standalone",
    "move_claims_to_card",
    "add_author_claim",
    "add_author_directive",
    "rename_card",
    "add_alias",
    "merge_cards",
    "create_relationship",
    "request_human_clarification",
}
VALID_CARD_ARCHITECTURE_DECISIONS = {"approve", "accept", "reject", "defer", "needs_more_context"}


def card_architecture_paths(review_dir: Path) -> dict[str, Path]:
    return {
        "requests": review_dir / CARD_EDIT_REQUESTS_FILENAME,
        "proposals": review_dir / CARD_ARCHITECTURE_PROPOSALS_FILENAME,
        "decisions": review_dir / CARD_ARCHITECTURE_DECISIONS_FILENAME,
        "applied": review_dir / CARD_ARCHITECTURE_APPLIED_FILENAME,
        "failures": review_dir / CARD_ARCHITECTURE_FAILURES_FILENAME,
        "redirects": review_dir / CARD_REDIRECTS_FILENAME,
    }


def ensure_card_architecture_files(review_dir: Path) -> None:
    paths = card_architecture_paths(review_dir)
    if not paths["requests"].exists():
        write_jsonl(paths["requests"], [])
    if not paths["proposals"].exists():
        write_json(paths["proposals"], {"generated_at_utc": now_utc_iso(), "proposals": [], "failures": []})
    if not paths["decisions"].exists():
        write_json(paths["decisions"], {"decisions": []})
    if not paths["applied"].exists():
        write_json(paths["applied"], {"generated_at_utc": now_utc_iso(), "applied_actions": []})
    if not paths["redirects"].exists():
        write_json(paths["redirects"], {"generated_at_utc": now_utc_iso(), "redirects": []})
    if not paths["failures"].exists():
        write_json(paths["failures"], {"generated_at_utc": now_utc_iso(), "failures": []})


def load_card_edit_requests(path: Path) -> list[dict[str, Any]]:
    return [row for row in read_jsonl(path) if isinstance(row, dict)]


def load_card_architecture_proposals(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    payload = read_json(path)
    rows = payload.get("proposals", []) if isinstance(payload, dict) else []
    return [row for row in rows if isinstance(row, dict)]


def load_card_architecture_decisions(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    payload = read_json(path)
    rows = payload.get("decisions", []) if isinstance(payload, dict) else []
    return [row for row in rows if isinstance(row, dict)]


def write_card_architecture_decision(
    decisions_path: Path,
    action: dict[str, Any],
    decision: str,
    reviewer: str,
    rationale: str,
    timestamp_utc: str | None = None,
    override_source: str = "desktop",
) -> dict[str, Any]:
    action_id = str(action.get("action_id", "")).strip()
    if not action_id:
        raise ValueError("Card architecture action is missing action_id.")
    normalized_decision = "approve" if decision == "accept" else str(decision or "").strip().lower()
    if normalized_decision not in VALID_CARD_ARCHITECTURE_DECISIONS:
        normalized_decision = "defer"
    payload = {
        "action_id": action_id,
        "proposal_id": action.get("proposal_id", ""),
        "request_id": action.get("request_id", ""),
        "action_type": action.get("action_type", ""),
        "decision": normalized_decision,
        "reviewer": reviewer or "human_reviewer",
        "rationale": rationale,
        "timestamp_utc": timestamp_utc or now_utc_iso(),
        "human_override": True,
        "override_source": override_source,
    }
    data = read_json(decisions_path) if decisions_path.exists() else {"decisions": []}
    decisions = data.setdefault("decisions", [])
    if not isinstance(decisions, list):
        decisions = []
        data["decisions"] = decisions
    decisions.append(payload)
    write_json(decisions_path, data)
    return payload


def append_card_edit_request(
    artifacts_root: Path,
    instruction_text: str,
    requester: str = "author",
    target_text: str = "",
    rationale: str = "",
    source: str = "",
    timestamp_utc: str | None = None,
) -> dict[str, Any]:
    clean = re.sub(r"\s+", " ", str(instruction_text or "")).strip()
    if not clean:
        raise ValueError("Card Agent request text is required.")
    created_at = timestamp_utc or now_utc_iso()
    row = {
        "request_id": stable_id("card_edit_request", clean, str(target_text or ""), created_at),
        "instruction_text": clean,
        "target_text": re.sub(r"\s+", " ", str(target_text or "")).strip(),
        "rationale": re.sub(r"\s+", " ", str(rationale or "")).strip(),
        "requester": requester or "author",
        "status": "pending",
        "created_at_utc": created_at,
    }
    clean_source = re.sub(r"\s+", " ", str(source or "")).strip()
    if clean_source:
        row["source"] = clean_source
    path = ArtifactPaths(artifacts_root).card_edit_requests
    rows = load_card_edit_requests(path) if path.exists() else []
    rows.append(row)
    write_jsonl(path, rows)
    return row


def _latest_decisions_by_action(decisions: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    latest: dict[str, dict[str, Any]] = {}
    for decision in decisions:
        action_id = str(decision.get("action_id", "")).strip()
        if action_id:
            latest[action_id] = decision
    return latest


def _entity_indexes(entities: list[dict[str, Any]]) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]], dict[str, dict[str, Any]]]:
    by_id: dict[str, dict[str, Any]] = {}
    by_card: dict[str, dict[str, Any]] = {}
    by_name: dict[str, dict[str, Any]] = {}
    for entity in entities:
        if not isinstance(entity, dict):
            continue
        entity_id = str(entity.get("entity_id", "")).strip()
        canonical_name = str(entity.get("canonical_name", "")).strip()
        card_id = str(entity.get("card_id") or card_id_for_entity(canonical_name)).strip()
        if entity_id:
            by_id[entity_id] = entity
        if card_id:
            by_card[card_id] = entity
        if canonical_name:
            by_name[normalized_name_key(canonical_name)] = entity
        for alias in entity.get("aliases", []) or []:
            alias_text = str(alias).strip()
            if alias_text:
                by_name[normalized_name_key(alias_text)] = entity
    return by_id, by_card, by_name


def _resolve_entity_ref(action: dict[str, Any], prefix: str, entities: list[dict[str, Any]]) -> dict[str, Any] | None:
    by_id, by_card, by_name = _entity_indexes(entities)
    field_candidates = [
        f"{prefix}_entity_id",
        f"{prefix}_card_id",
        f"{prefix}_entity_name",
        f"{prefix}_card_name",
        f"{prefix}_name",
        prefix,
    ]
    for field in field_candidates:
        raw = str(action.get(field, "")).strip()
        if not raw:
            continue
        entity = by_id.get(raw) or by_card.get(raw) or by_name.get(normalized_name_key(raw))
        if entity:
            return entity
    return None


def _claim_by_id(claims: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    return {
        str(claim.get("claim_id", "")).strip(): claim
        for claim in claims
        if isinstance(claim, dict) and str(claim.get("claim_id", "")).strip()
    }


def _as_text_list(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    if value is None:
        return []
    text = str(value).strip()
    return [text] if text else []


def _card_ref_payload(entity: dict[str, Any] | None, prefix: str) -> dict[str, Any]:
    if not entity:
        return {}
    canonical_name = str(entity.get("canonical_name", "")).strip()
    return {
        f"{prefix}_entity_id": str(entity.get("entity_id", "")).strip(),
        f"{prefix}_card_id": str(entity.get("card_id") or card_id_for_entity(canonical_name)).strip(),
        f"{prefix}_entity_name": canonical_name,
    }


def _action_identity_parts(action: dict[str, Any]) -> list[str]:
    keys = [
        "request_id",
        "action_type",
        "source_entity_id",
        "source_card_id",
        "target_entity_id",
        "target_card_id",
        "target_section",
        "new_canonical_name",
        "alias_text",
        "claim_text",
        "instruction_text",
        "relationship_type",
    ]
    parts = [str(action.get(key, "")) for key in keys]
    parts.extend(_as_text_list(action.get("claim_ids")))
    return parts


def validate_card_architecture_action(
    raw_action: dict[str, Any],
    proposal_id: str,
    request_id: str,
    entities: list[dict[str, Any]],
    accepted_claims: list[dict[str, Any]],
) -> dict[str, Any]:
    action_type = str(raw_action.get("action_type", "")).strip()
    action = {
        **raw_action,
        "proposal_id": proposal_id,
        "request_id": request_id,
        "action_type": action_type,
    }
    failures: list[str] = []
    warnings: list[str] = []
    if action_type not in VALID_CARD_ARCHITECTURE_ACTIONS:
        failures.append(f"unknown_action_type:{action_type or '(missing)'}")

    source_entity = _resolve_entity_ref(action, "source", entities)
    target_entity = _resolve_entity_ref(action, "target", entities)
    if source_entity:
        action.update(_card_ref_payload(source_entity, "source"))
    if target_entity:
        action.update(_card_ref_payload(target_entity, "target"))

    claim_index = _claim_by_id(accepted_claims)
    claim_ids = _as_text_list(action.get("claim_ids"))
    invalid_claim_ids = [claim_id for claim_id in claim_ids if claim_id not in claim_index]
    if invalid_claim_ids:
        failures.append("invalid_claim_ids:" + ",".join(invalid_claim_ids[:12]))
    action["claim_ids"] = [claim_id for claim_id in claim_ids if claim_id in claim_index]

    if action_type in {"demote_card_to_section", "merge_cards"}:
        if not source_entity:
            failures.append("missing_or_unknown_source_card")
        if not target_entity:
            failures.append("missing_or_unknown_target_card")
    elif action_type == "mark_not_standalone":
        if not source_entity and not target_entity:
            failures.append("missing_or_unknown_source_card")
        if not source_entity and target_entity:
            source_entity = target_entity
            action.update(_card_ref_payload(source_entity, "source"))
    elif action_type == "move_claims_to_card":
        if not target_entity:
            failures.append("missing_or_unknown_target_card")
        if not action.get("claim_ids"):
            failures.append("missing_valid_claim_ids")
    elif action_type in {"add_author_directive", "rename_card", "add_alias"}:
        if not target_entity and not source_entity:
            failures.append("missing_or_unknown_target_card")
        if source_entity and not target_entity:
            target_entity = source_entity
            action.update(_card_ref_payload(target_entity, "target"))
    elif action_type == "add_author_claim":
        if target_entity:
            action.update(_card_ref_payload(target_entity, "target"))
        elif not str(action.get("claim_text", "")).strip():
            failures.append("missing_claim_text")
        if not str(action.get("claim_text", "")).strip():
            failures.append("missing_claim_text")
    elif action_type == "create_relationship":
        if not source_entity:
            failures.append("missing_or_unknown_source_card")
        if not target_entity:
            failures.append("missing_or_unknown_target_card")
    elif action_type == "request_human_clarification":
        if not str(action.get("clarification_question") or action.get("rationale") or action.get("instruction_text") or "").strip():
            warnings.append("no_clarification_text")

    if action_type == "demote_card_to_section" and not str(action.get("target_section", "")).strip():
        action["target_section"] = "background"
    if action_type == "move_claims_to_card" and not str(action.get("target_section", "")).strip():
        action["target_section"] = "background"
    if action_type == "add_author_directive" and not str(action.get("instruction_text", "")).strip():
        failures.append("missing_instruction_text")
    if action_type == "rename_card" and not str(action.get("new_canonical_name", "")).strip():
        failures.append("missing_new_canonical_name")
    if action_type == "add_alias" and not str(action.get("alias_text", "")).strip():
        failures.append("missing_alias_text")

    action_id = str(action.get("action_id", "")).strip()
    if not action_id:
        action_id = stable_id("card_arch_action", *_action_identity_parts(action))
    action["action_id"] = action_id
    action["validation_status"] = "invalid" if failures else "valid"
    action["validation_errors"] = failures
    action["validation_warnings"] = warnings
    return action


def annotate_card_architecture_proposals(
    proposals: list[dict[str, Any]],
    decisions: list[dict[str, Any]],
    entities: list[dict[str, Any]],
    accepted_claims: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    decision_by_action = _latest_decisions_by_action(decisions)
    failures: list[dict[str, Any]] = []
    annotated_proposals: list[dict[str, Any]] = []
    for proposal in proposals:
        proposal_id = str(proposal.get("proposal_id", "")).strip() or stable_id(
            "card_arch_proposal",
            str(proposal.get("request_id", "")),
            json.dumps(proposal.get("actions", []), sort_keys=True, ensure_ascii=False),
        )
        request_id = str(proposal.get("request_id", "")).strip()
        annotated_actions: list[dict[str, Any]] = []
        for raw_action in proposal.get("actions", []) or []:
            if not isinstance(raw_action, dict):
                continue
            action = validate_card_architecture_action(raw_action, proposal_id, request_id, entities, accepted_claims)
            decision = decision_by_action.get(str(action.get("action_id", "")))
            if decision:
                action["review_status"] = str(decision.get("decision", "defer")).strip().lower() or "defer"
                action["reviewer"] = decision.get("reviewer", "reviewer")
                action["review_rationale"] = decision.get("rationale", "")
                action["reviewed_at_utc"] = decision.get("timestamp_utc", now_utc_iso())
            elif action.get("validation_status") == "invalid":
                action["review_status"] = "invalid"
            else:
                action["review_status"] = "pending"
            if action.get("validation_status") == "invalid":
                failures.append(
                    {
                        "failure_id": safe_uuid(),
                        "proposal_id": proposal_id,
                        "request_id": request_id,
                        "action_id": action.get("action_id", ""),
                        "action_type": action.get("action_type", ""),
                        "validation_errors": action.get("validation_errors", []),
                        "action": raw_action,
                        "created_at_utc": now_utc_iso(),
                    }
                )
            annotated_actions.append(action)
        annotated_proposals.append({**proposal, "proposal_id": proposal_id, "request_id": request_id, "actions": annotated_actions})
    return annotated_proposals, failures


def card_architecture_actions_from_proposals(proposals: list[dict[str, Any]]) -> list[dict[str, Any]]:
    actions: list[dict[str, Any]] = []
    for proposal in proposals:
        for action in proposal.get("actions", []) or []:
            if isinstance(action, dict):
                actions.append(action)
    return actions


def pending_card_architecture_actions(proposals_path: Path, decisions_path: Path) -> list[dict[str, Any]]:
    proposals = load_card_architecture_proposals(proposals_path)
    decisions = load_card_architecture_decisions(decisions_path)
    decision_by_action = _latest_decisions_by_action(decisions)
    pending: list[dict[str, Any]] = []
    for proposal in proposals:
        for action in proposal.get("actions", []) or []:
            if not isinstance(action, dict):
                continue
            action_id = str(action.get("action_id", "")).strip()
            if not action_id:
                continue
            if str(action.get("validation_status", "valid")) == "invalid":
                continue
            if action_id not in decision_by_action and str(action.get("review_status", "pending")) == "pending":
                pending.append({**action, "proposal_id": proposal.get("proposal_id"), "request_id": proposal.get("request_id")})
    return pending


def build_card_architecture_prompt(
    requests: list[dict[str, Any]],
    accepted_claims: list[dict[str, Any]],
    entities: list[dict[str, Any]],
    existing_canonical_cards: list[dict[str, Any]],
    review_memory: dict[str, Any],
    source_snippets_by_id: dict[str, dict[str, Any]],
) -> str:
    entity_rows = [
        {
            "entity_id": entity.get("entity_id"),
            "card_id": entity.get("card_id") or card_id_for_entity(str(entity.get("canonical_name", ""))),
            "canonical_name": entity.get("canonical_name"),
            "entity_type": entity.get("entity_type"),
            "aliases": entity.get("aliases", []),
        }
        for entity in entities
    ]
    claim_rows = [
        {
            "claim_id": claim.get("claim_id"),
            "target_entity_id": claim.get("target_entity_id"),
            "target_card_id": claim.get("target_card_id"),
            "target_entity_name": claim.get("target_entity_name"),
            "claim_type": claim.get("claim_type"),
            "knowledge_track": claim.get("knowledge_track"),
            "claim_text": claim.get("claim_text"),
            "source_snippet_ids": claim.get("source_snippet_ids", []),
            "manual_claim": bool(claim.get("manual_claim") or claim.get("author_claim")),
        }
        for claim in accepted_claims
    ]
    snippet_rows = []
    wanted_snippets: set[str] = set()
    for claim in accepted_claims:
        wanted_snippets.update(_as_text_list(claim.get("source_snippet_ids")))
    for snippet_id in sorted(wanted_snippets)[:160]:
        snippet = source_snippets_by_id.get(snippet_id)
        if not isinstance(snippet, dict):
            continue
        snippet_rows.append(
            {
                "snippet_id": snippet_id,
                "conversation_id": snippet.get("conversation_id", ""),
                "conversation_global_index": snippet.get("conversation_global_index"),
                "topic": snippet.get("conversation_topic_label") or snippet.get("conversation_patch_topic_label") or "",
                "patch_summary": snippet.get("conversation_patch_summary", ""),
                "text": str(snippet.get("display_text_normalized") or snippet.get("patch_item_text") or "")[:900],
            }
        )
    relevant_memory = {
        "card_architecture_actions": review_memory.get("card_architecture_actions", [])[-100:],
        "card_redirects": review_memory.get("card_redirects", [])[-100:],
        "author_directives": review_memory.get("author_directives", [])[-80:],
        "entity_merges": review_memory.get("entity_merges", [])[-80:],
        "approved_aliases": review_memory.get("approved_aliases", [])[-120:],
    }
    return f"""You are the THERIAC card-base architecture agent.
Your job is to propose structural edits to the card base before prose drafting. Return strict JSON only.

Only propose actions that are directly responsive to the user requests and supported by existing accepted claims, author claims, aliases, memory, or source snippets. Do not write final card prose here.
Human approval is required later, so prefer explicit operations with concise rationale over vague advice.

Allowed action_type values:
- demote_card_to_section: source card/entity is not standalone; move its claims into a target card section.
- mark_not_standalone: suppress future standalone promotion unless stronger evidence appears.
- move_claims_to_card: reassign specific accepted claim IDs to another target card for synthesis.
- add_author_claim: add a user-authoritative claim after approval.
- add_author_directive: add a synthesis instruction for a target card.
- rename_card: change canonical display name for a draft card.
- add_alias: add alias/working-name metadata.
- merge_cards: combine two draft cards at card-boundary level while preserving aliases/evidence.
- create_relationship: add an explicit wiki-style relationship between cards.
- request_human_clarification: surface unresolved structural ambiguity instead of guessing.

Reference rules:
- Use existing entity_id/card_id/claim_id values whenever possible.
- For add_author_claim, include claim_text, claim_type, knowledge_track, and a target entity/card if the target exists.
- For demote_card_to_section, include source_entity_id or source_card_id, target_entity_id or target_card_id, target_section, rationale, affected_claim_ids when known, and confidence.
- For move_claims_to_card, include target_entity_id or target_card_id and exact claim_ids.
- For mark_not_standalone, include source_entity_id or source_card_id.
- Do not use this path for same-entity identity merges; those belong to identity merge review.
- If a request is ambiguous, propose request_human_clarification rather than inventing facts.

User card edit requests:
{json.dumps(requests, ensure_ascii=False, indent=2)}

Available cards/entities:
{json.dumps(entity_rows, ensure_ascii=False, indent=2)}

Accepted claims and author claims:
{json.dumps(claim_rows, ensure_ascii=False, indent=2)}

Source snippet context for accepted claims:
{json.dumps(snippet_rows, ensure_ascii=False, indent=2)}

Existing canonical cards:
{json.dumps(existing_canonical_cards, ensure_ascii=False, indent=2)}

Relevant review memory:
{json.dumps(relevant_memory, ensure_ascii=False, indent=2)}

Return JSON object:
{{
  "proposals": [
    {{
      "request_id": "request id being answered",
      "summary": "short proposal summary",
      "actions": [
        {{
          "action_type": "one allowed value",
          "source_entity_id": "",
          "source_card_id": "",
          "target_entity_id": "",
          "target_card_id": "",
          "target_section": "background|role_in_story|relationships|timeline|inspirations|open_questions",
          "claim_ids": ["claim id"],
          "claim_text": "",
          "claim_type": "lore_fact|relationship|role|background|timeline|inspiration|open_question|meta_note|other",
          "knowledge_track": "lore|meta|both",
          "instruction_text": "",
          "alias_text": "",
          "new_canonical_name": "",
          "relationship_type": "",
          "clarification_question": "",
          "rationale": "why this action is needed",
          "affected_cards": ["card id"],
          "affected_claim_ids": ["claim id"],
          "confidence": 0.0
        }}
      ]
    }}
  ]
}}
"""


def generate_card_architecture_proposals(
    requests: list[dict[str, Any]],
    accepted_claims: list[dict[str, Any]],
    entities: list[dict[str, Any]],
    existing_canonical_cards: list[dict[str, Any]],
    review_memory: dict[str, Any],
    source_snippets_by_id: dict[str, dict[str, Any]],
    config: dict[str, Any],
) -> list[dict[str, Any]]:
    if not requests:
        return []
    prompt = build_card_architecture_prompt(
        requests,
        accepted_claims,
        entities,
        existing_canonical_cards,
        review_memory,
        source_snippets_by_id,
    )
    response = call_model_chat(prompt=prompt, **model_call_kwargs(config, "stage_11_card_architecture_agent"))
    if response is None:
        raise RuntimeError("Stage 10A Card Architecture Agent requires model output; provider returned no response.")
    if not isinstance(response, dict) or not isinstance(response.get("proposals"), list):
        raise RuntimeError("Stage 10A Card Architecture Agent returned invalid JSON; expected object with proposals array.")
    generated: list[dict[str, Any]] = []
    for proposal in response.get("proposals", []) or []:
        if not isinstance(proposal, dict):
            continue
        request_id = str(proposal.get("request_id", "")).strip()
        proposal_id = str(proposal.get("proposal_id", "")).strip() or stable_id(
            "card_arch_proposal",
            request_id,
            json.dumps(proposal.get("actions", []), sort_keys=True, ensure_ascii=False),
        )
        generated.append(
            {
                **proposal,
                "proposal_id": proposal_id,
                "request_id": request_id,
                "created_at_utc": proposal.get("created_at_utc") or now_utc_iso(),
                "status": "proposed",
                "model_task": "stage_11_card_architecture_agent",
            }
        )
    return generated


def prepare_card_architecture_review(
    review_dir: Path,
    accepted_claims: list[dict[str, Any]],
    entities: list[dict[str, Any]],
    existing_canonical_cards: list[dict[str, Any]],
    review_memory: dict[str, Any],
    source_snippets_by_id: dict[str, dict[str, Any]],
    config: dict[str, Any],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    ensure_card_architecture_files(review_dir)
    paths = card_architecture_paths(review_dir)
    requests = [
        row
        for row in load_card_edit_requests(paths["requests"])
        if str(row.get("status", "pending")).strip().lower() == "pending"
    ]
    existing_proposals = load_card_architecture_proposals(paths["proposals"])
    covered_request_ids = {
        str(proposal.get("request_id", "")).strip()
        for proposal in existing_proposals
        if str(proposal.get("request_id", "")).strip()
    }
    requests_to_generate = [
        request
        for request in requests
        if str(request.get("request_id", "")).strip() and str(request.get("request_id", "")).strip() not in covered_request_ids
    ]
    generated: list[dict[str, Any]] = []
    if requests_to_generate:
        generated = generate_card_architecture_proposals(
            requests_to_generate,
            accepted_claims,
            entities,
            existing_canonical_cards,
            review_memory,
            source_snippets_by_id,
            config,
        )
    decisions = load_card_architecture_decisions(paths["decisions"])
    proposals, failures = annotate_card_architecture_proposals(
        existing_proposals + generated,
        decisions,
        entities,
        accepted_claims,
    )
    write_json(
        paths["proposals"],
        {
            "generated_at_utc": now_utc_iso(),
            "proposals": proposals,
            "decisions_path": str(paths["decisions"]),
            "failures_path": str(paths["failures"]),
        },
    )
    write_json(paths["failures"], {"generated_at_utc": now_utc_iso(), "failures": failures})
    pending = [
        action
        for action in card_architecture_actions_from_proposals(proposals)
        if str(action.get("review_status", "pending")) == "pending"
        and str(action.get("validation_status", "valid")) != "invalid"
    ]
    return proposals, pending, failures


def _append_unique_alias(entity: dict[str, Any], alias_text: str) -> None:
    clean = re.sub(r"\s+", " ", str(alias_text or "")).strip()
    if not clean:
        return
    aliases = [str(alias).strip() for alias in entity.get("aliases", []) or [] if str(alias).strip()]
    existing = {alias.lower() for alias in aliases}
    if clean.lower() not in existing and clean.lower() != str(entity.get("canonical_name", "")).strip().lower():
        aliases.append(clean)
    entity["aliases"] = sorted(aliases, key=str.lower)


def _retarget_claim(claim: dict[str, Any], target_entity: dict[str, Any], action: dict[str, Any]) -> dict[str, Any]:
    target_name = str(target_entity.get("canonical_name", "")).strip()
    out = {
        **claim,
        "card_architecture_action_ids": sorted(
            set(_as_text_list(claim.get("card_architecture_action_ids")) + [str(action.get("action_id", ""))])
        ),
        "architecture_original_target_entity_id": claim.get("architecture_original_target_entity_id") or claim.get("target_entity_id", ""),
        "architecture_original_target_card_id": claim.get("architecture_original_target_card_id") or claim.get("target_card_id", ""),
        "architecture_original_target_entity_name": claim.get("architecture_original_target_entity_name") or claim.get("target_entity_name", ""),
        "target_entity_id": target_entity.get("entity_id", ""),
        "target_card_id": target_entity.get("card_id") or card_id_for_entity(target_name),
        "target_entity_name": target_name,
        "preferred_card_section": action.get("target_section", claim.get("preferred_card_section", "")),
        "card_architecture_applied": True,
    }
    return out


def _make_author_claim_from_action(action: dict[str, Any], target_entity: dict[str, Any]) -> dict[str, Any]:
    claim_text = re.sub(r"\s+", " ", str(action.get("claim_text", "")).strip())
    claim_type = str(action.get("claim_type", "lore_fact") or "lore_fact").strip() or "lore_fact"
    knowledge_track = str(action.get("knowledge_track", "lore") or "lore").strip().lower()
    if knowledge_track not in {"lore", "meta", "both"}:
        knowledge_track = "lore"
    target_name = str(target_entity.get("canonical_name", "")).strip()
    claim_id = str(action.get("claim_id", "")).strip() or stable_id(
        "author_claim",
        str(target_entity.get("entity_id", "")),
        claim_type,
        claim_text,
    )
    return {
        "claim_id": claim_id,
        "target_entity_id": target_entity.get("entity_id", ""),
        "target_card_id": target_entity.get("card_id") or card_id_for_entity(target_name),
        "target_entity_name": target_name,
        "knowledge_track": knowledge_track,
        "claim_text": claim_text,
        "claim_type": claim_type,
        "source_snippet_ids": [],
        "confidence": float(action.get("confidence", 1.0) or 1.0),
        "status": "accepted",
        "contradiction_notes": "",
        "created_at_utc": now_utc_iso(),
        "reviewer": action.get("reviewer") or "card_architecture_agent",
        "review_rationale": action.get("review_rationale") or action.get("rationale", ""),
        "manual_claim": True,
        "author_claim": True,
        "source_priority": "card_architecture_action",
        "card_architecture_action_ids": [action.get("action_id", "")],
    }


def _append_author_claim_record(author_claims_path: Path, claim: dict[str, Any]) -> None:
    payload = read_json(author_claims_path) if author_claims_path.exists() else {"claims": []}
    rows = payload.setdefault("claims", [])
    if not isinstance(rows, list):
        rows = []
        payload["claims"] = rows
    claim_id = str(claim.get("claim_id", "")).strip()
    for index, existing in enumerate(rows):
        if isinstance(existing, dict) and str(existing.get("claim_id", "")).strip() == claim_id:
            rows[index] = {**existing, **claim}
            break
    else:
        rows.append(claim)
    payload["updated_at_utc"] = now_utc_iso()
    write_json(author_claims_path, payload)


def _append_directive_record(directives_path: Path, directive: dict[str, Any]) -> None:
    payload = read_json(directives_path) if directives_path.exists() else {"directives": []}
    rows = payload.setdefault("directives", [])
    if not isinstance(rows, list):
        rows = []
        payload["directives"] = rows
    directive_id = str(directive.get("directive_id", "")).strip()
    existing_ids = {str(row.get("directive_id", "")).strip() for row in rows if isinstance(row, dict)}
    if directive_id and directive_id not in existing_ids:
        rows.append(directive)
    write_json(directives_path, payload)


def _approved_actions_from_memory(memory: dict[str, Any]) -> list[dict[str, Any]]:
    rows = memory.get("card_architecture_actions", [])
    return [row for row in rows if isinstance(row, dict) and str(row.get("review_status", "approve")) in {"approve", "accept", "applied"}]


def apply_card_architecture_actions(
    review_dir: Path,
    accepted_claims: list[dict[str, Any]],
    entities: list[dict[str, Any]],
    directives: list[dict[str, Any]],
    review_memory: dict[str, Any],
    author_claims_path: Path,
    directives_path: Path,
) -> dict[str, Any]:
    ensure_card_architecture_files(review_dir)
    paths = card_architecture_paths(review_dir)
    proposals = load_card_architecture_proposals(paths["proposals"])
    decisions = load_card_architecture_decisions(paths["decisions"])
    proposals, validation_failures = annotate_card_architecture_proposals(proposals, decisions, entities, accepted_claims)
    actions = [
        action
        for action in card_architecture_actions_from_proposals(proposals)
        if str(action.get("review_status", "")).lower() in {"approve", "accept"}
        and str(action.get("validation_status", "valid")) != "invalid"
    ]
    memory_actions = _approved_actions_from_memory(review_memory)
    seen_action_ids = {str(action.get("action_id", "")).strip() for action in actions if str(action.get("action_id", "")).strip()}
    for action in memory_actions:
        action_id = str(action.get("action_id", "")).strip()
        if action_id and action_id not in seen_action_ids:
            validated = validate_card_architecture_action(action, str(action.get("proposal_id", "")), str(action.get("request_id", "")), entities, accepted_claims)
            if str(validated.get("validation_status", "valid")) != "invalid":
                actions.append({**validated, "review_status": str(action.get("review_status", "approve"))})
                seen_action_ids.add(action_id)

    entity_by_id = {str(entity.get("entity_id", "")): {**entity, "aliases": list(entity.get("aliases", []) or [])} for entity in entities}
    claim_by_id = _claim_by_id(accepted_claims)
    claims = [dict(claim) for claim in accepted_claims]
    suppress_entity_ids: set[str] = set()
    redirects: list[dict[str, Any]] = []
    applied_actions: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = list(validation_failures)
    move_all_actions = {"demote_card_to_section", "merge_cards"}

    for action in actions:
        action_type = str(action.get("action_type", "")).strip()
        action_id = str(action.get("action_id", "")).strip()
        source_id = str(action.get("source_entity_id", "")).strip()
        target_id = str(action.get("target_entity_id", "")).strip()
        source_entity = entity_by_id.get(source_id)
        target_entity = entity_by_id.get(target_id)
        try:
            if action_type in move_all_actions:
                if not source_entity or not target_entity:
                    raise ValueError("source or target entity missing during application")
                for index, claim in enumerate(claims):
                    if str(claim.get("target_entity_id", "")) == source_id:
                        claims[index] = _retarget_claim(claim, target_entity, action)
                suppress_entity_ids.add(source_id)
                if action_type == "merge_cards":
                    _append_unique_alias(target_entity, str(source_entity.get("canonical_name", "")))
                    for alias in source_entity.get("aliases", []) or []:
                        _append_unique_alias(target_entity, str(alias))
                redirect = {
                    "redirect_id": stable_id("card_redirect", source_id, target_id, action_type),
                    "source_entity_id": source_id,
                    "source_card_id": action.get("source_card_id", ""),
                    "source_entity_name": action.get("source_entity_name", ""),
                    "target_entity_id": target_id,
                    "target_card_id": action.get("target_card_id", ""),
                    "target_entity_name": action.get("target_entity_name", ""),
                    "target_section": action.get("target_section", "background"),
                    "status": "merged_into_card" if action_type == "merge_cards" else "demoted_to_section",
                    "action_id": action_id,
                    "rationale": action.get("rationale", ""),
                    "created_at_utc": now_utc_iso(),
                }
                redirects.append(redirect)
            elif action_type == "mark_not_standalone":
                if not source_entity:
                    raise ValueError("source entity missing during mark_not_standalone")
                suppress_entity_ids.add(source_id)
                redirects.append(
                    {
                        "redirect_id": stable_id("card_redirect", source_id, str(action.get("target_entity_id", "")), "not_standalone"),
                        "source_entity_id": source_id,
                        "source_card_id": action.get("source_card_id", ""),
                        "source_entity_name": action.get("source_entity_name", ""),
                        "target_entity_id": action.get("target_entity_id", ""),
                        "target_card_id": action.get("target_card_id", ""),
                        "target_entity_name": action.get("target_entity_name", ""),
                        "target_section": action.get("target_section", ""),
                        "status": "not_standalone",
                        "action_id": action_id,
                        "rationale": action.get("rationale", ""),
                        "created_at_utc": now_utc_iso(),
                    }
                )
            elif action_type == "move_claims_to_card":
                if not target_entity:
                    raise ValueError("target entity missing during move_claims_to_card")
                wanted = set(_as_text_list(action.get("claim_ids")))
                for index, claim in enumerate(claims):
                    if str(claim.get("claim_id", "")) in wanted:
                        claims[index] = _retarget_claim(claim, target_entity, action)
            elif action_type == "add_author_claim":
                if not target_entity:
                    raise ValueError("target entity missing during add_author_claim")
                claim = _make_author_claim_from_action(action, target_entity)
                if not claim.get("claim_text"):
                    raise ValueError("author claim has no text")
                if str(claim.get("claim_id", "")) not in {str(c.get("claim_id", "")) for c in claims}:
                    claims.append(claim)
                _append_author_claim_record(author_claims_path, claim)
            elif action_type == "add_author_directive":
                if not target_entity:
                    raise ValueError("target entity missing during add_author_directive")
                directive = {
                    "directive_id": stable_id("author_directive", action_id, str(action.get("instruction_text", ""))),
                    "target_entity_id": target_entity.get("entity_id", ""),
                    "target_card_id": target_entity.get("card_id") or card_id_for_entity(str(target_entity.get("canonical_name", ""))),
                    "instruction_text": str(action.get("instruction_text", "")).strip(),
                    "author": action.get("reviewer") or "card_architecture_agent",
                    "created_at_utc": now_utc_iso(),
                    "source": "card_architecture_action",
                    "card_architecture_action_id": action_id,
                }
                directives.append(directive)
                _append_directive_record(directives_path, directive)
            elif action_type == "rename_card":
                if not target_entity:
                    raise ValueError("target entity missing during rename_card")
                old_name = str(target_entity.get("canonical_name", ""))
                new_name = re.sub(r"\s+", " ", str(action.get("new_canonical_name", "")).strip())
                if not new_name:
                    raise ValueError("rename_card missing new_canonical_name")
                _append_unique_alias(target_entity, old_name)
                target_entity["canonical_name"] = new_name
                for index, claim in enumerate(claims):
                    if str(claim.get("target_entity_id", "")) == str(target_entity.get("entity_id", "")):
                        claims[index] = {
                            **claim,
                            "target_entity_name": new_name,
                            "card_architecture_action_ids": sorted(
                                set(_as_text_list(claim.get("card_architecture_action_ids")) + [action_id])
                            ),
                        }
            elif action_type == "add_alias":
                if not target_entity:
                    raise ValueError("target entity missing during add_alias")
                _append_unique_alias(target_entity, str(action.get("alias_text", "")))
            elif action_type == "create_relationship":
                if not source_entity or not target_entity:
                    raise ValueError("source or target entity missing during create_relationship")
                relationship_type = str(action.get("relationship_type") or "relationship").strip()
                source_name = str(source_entity.get("canonical_name", "")).strip()
                target_name = str(target_entity.get("canonical_name", "")).strip()
                claim_text = str(action.get("claim_text", "")).strip() or f"{source_name} has a {relationship_type} relationship with {target_name}."
                relationship_action = {
                    **action,
                    "claim_text": claim_text,
                    "claim_type": "relationship",
                    "knowledge_track": action.get("knowledge_track", "lore"),
                }
                claim = _make_author_claim_from_action(relationship_action, source_entity)
                if str(claim.get("claim_id", "")) not in {str(c.get("claim_id", "")) for c in claims}:
                    claims.append(claim)
                _append_author_claim_record(author_claims_path, claim)
            elif action_type == "request_human_clarification":
                pass
            else:
                continue
            applied_actions.append({**action, "applied_at_utc": now_utc_iso(), "review_status": action.get("review_status", "approve")})
        except Exception as exc:
            failures.append(
                {
                    "failure_id": safe_uuid(),
                    "action_id": action_id,
                    "action_type": action_type,
                    "reason": str(exc),
                    "action": action,
                    "created_at_utc": now_utc_iso(),
                }
            )

    suppressed_claim_ids = {
        str(claim.get("claim_id", ""))
        for claim in claims
        if str(claim.get("target_entity_id", "")) in suppress_entity_ids
    }
    claims = [claim for claim in claims if str(claim.get("target_entity_id", "")) not in suppress_entity_ids]
    entities_out = [
        entity
        for entity_id, entity in entity_by_id.items()
        if entity_id not in suppress_entity_ids
    ]
    existing_applied_payload = read_json(paths["applied"]) if paths["applied"].exists() else {"applied_actions": []}
    existing_applied_actions = [
        row
        for row in existing_applied_payload.get("applied_actions", [])
        if isinstance(row, dict) and str(row.get("source", "")) == "cardbase_agent"
    ] if isinstance(existing_applied_payload, dict) else []
    existing_redirect_payload = read_json(paths["redirects"]) if paths["redirects"].exists() else {"redirects": []}
    existing_redirects = [
        row
        for row in existing_redirect_payload.get("redirects", [])
        if isinstance(row, dict) and str(row.get("card_agent_transaction_id", "")).strip()
    ] if isinstance(existing_redirect_payload, dict) else []
    write_json(
        paths["applied"],
        {
            "generated_at_utc": now_utc_iso(),
            "applied_actions": existing_applied_actions + applied_actions,
            "suppressed_entity_ids": sorted(suppress_entity_ids),
            "suppressed_claim_ids": sorted(suppressed_claim_ids),
        },
    )
    write_json(paths["redirects"], {"generated_at_utc": now_utc_iso(), "redirects": existing_redirects + redirects})
    write_json(paths["failures"], {"generated_at_utc": now_utc_iso(), "failures": failures})

    existing_action_ids = {
        str(row.get("action_id", "")).strip()
        for row in review_memory.get("card_architecture_actions", [])
        if isinstance(row, dict)
    }
    for action in applied_actions:
        action_id = str(action.get("action_id", "")).strip()
        if action_id and action_id not in existing_action_ids:
            review_memory.setdefault("card_architecture_actions", []).append(action)
            existing_action_ids.add(action_id)

    existing_redirect_ids = {
        str(row.get("redirect_id", "")).strip()
        for row in review_memory.get("card_redirects", [])
        if isinstance(row, dict)
    }
    for redirect in redirects:
        redirect_id = str(redirect.get("redirect_id", "")).strip()
        if redirect_id and redirect_id not in existing_redirect_ids:
            review_memory.setdefault("card_redirects", []).append(redirect)
            existing_redirect_ids.add(redirect_id)

    merge_log_rows = [
        {
            "decision_id": action.get("action_id", ""),
            "claim_id": ",".join(_as_text_list(action.get("claim_ids"))),
            "card_id": action.get("target_card_id") or action.get("source_card_id"),
            "target_entity_id": action.get("target_entity_id") or action.get("source_entity_id"),
            "knowledge_track": "architecture",
            "decision": action.get("action_type", ""),
            "reviewer": action.get("reviewer", "reviewer"),
            "rationale": action.get("rationale", ""),
            "timestamp_utc": action.get("reviewed_at_utc") or action.get("applied_at_utc") or now_utc_iso(),
            "source_priority": "card_architecture_action",
            "claim_payload": action,
        }
        for action in applied_actions
    ]
    logger = get_logger(__name__)
    logger.info(
        "Stage 10B Card Architecture Application: applied=%d redirects=%d suppressed_cards=%d failures=%d",
        len(applied_actions),
        len(redirects),
        len(suppress_entity_ids),
        len(failures),
    )
    return {
        "accepted_claims": claims,
        "entities": sorted(entities_out, key=lambda entity: str(entity.get("canonical_name", ""))),
        "directives": directives,
        "redirects": redirects,
        "applied_actions": applied_actions,
        "failures": failures,
        "merge_log_rows": merge_log_rows,
    }


def _decision_for_action(action: dict[str, Any], decisions_by_action: dict[str, dict[str, Any]]) -> dict[str, Any]:
    return decisions_by_action.get(str(action.get("action_id", "")).strip(), {})


def card_architecture_browser_rows(proposals_path: Path, decisions_path: Path, redirects_path: Path | None = None) -> list[dict[str, Any]]:
    proposals = load_card_architecture_proposals(proposals_path)
    decisions = load_card_architecture_decisions(decisions_path)
    decisions_by_action = _latest_decisions_by_action(decisions)
    redirects_payload = read_json(redirects_path) if redirects_path and redirects_path.exists() else {"redirects": []}
    redirected_action_ids = {
        str(row.get("action_id", "")).strip()
        for row in redirects_payload.get("redirects", []) if isinstance(row, dict)
    }
    rows: list[dict[str, Any]] = []
    for proposal in proposals:
        for action in proposal.get("actions", []) or []:
            if not isinstance(action, dict):
                continue
            decision = _decision_for_action(action, decisions_by_action)
            action_id = str(action.get("action_id", "")).strip()
            status = str(decision.get("decision") or action.get("review_status", "pending")).strip().lower()
            if action_id in redirected_action_ids:
                bucket = "redirected"
            else:
                bucket = {
                    "approve": "approved",
                    "accept": "approved",
                    "reject": "rejected",
                    "defer": "deferred",
                    "needs_more_context": "needs context",
                    "invalid": "invalid",
                }.get(status, "pending")
            action_type = str(action.get("action_type", ""))
            source = str(action.get("source_entity_name") or action.get("source_card_id") or "")
            target = str(action.get("target_entity_name") or action.get("target_card_id") or "")
            if action_type == "add_author_claim":
                display = str(action.get("claim_text", ""))[:140] or "Add author claim"
            elif source and target:
                display = f"{source} -> {target}"
            elif source:
                display = source
            elif target:
                display = target
            else:
                display = action_type or "(action)"
            rows.append(
                {
                    "row_id": f"card_architecture:{action_id or len(rows)}",
                    "row_kind": "card_architecture",
                    "bucket": bucket,
                    "source_bucket": "card_architecture",
                    "category": "mixed",
                    "candidate_name": display,
                    "raw_candidate_name": display,
                    "canonical_name": target or source,
                    "proposed_entity_type": action_type,
                    "evidence_count": len(_as_text_list(action.get("claim_ids") or action.get("affected_claim_ids"))),
                    "topics": [action_type],
                    "tracks": [],
                    "triage_reason": str(action.get("rationale") or proposal.get("summary") or ""),
                    "review_priority": "architecture",
                    "decision": str(decision.get("decision", "") or action.get("review_status", "")),
                    "item": {**action, "proposal_summary": proposal.get("summary", ""), "latest_decision": decision},
                    "latest_decision": decision,
                }
            )
    rows.sort(key=lambda row: (str(row.get("bucket", "")), str(row.get("proposed_entity_type", "")), str(row.get("candidate_name", "")).lower()))
    return rows
