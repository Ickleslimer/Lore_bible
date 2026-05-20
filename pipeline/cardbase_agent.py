from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from pipeline.artifact_paths import ArtifactPaths
from pipeline.card_architecture_agent import (
    CARD_ARCHITECTURE_APPLIED_FILENAME,
    CARD_ARCHITECTURE_FAILURES_FILENAME,
    CARD_EDIT_REQUESTS_FILENAME,
    CARD_REDIRECTS_FILENAME,
    load_card_edit_requests,
)
from pipeline.common import now_utc_iso, read_json, read_jsonl, safe_uuid, stable_id, write_json, write_jsonl
from pipeline.entity_resolution import card_id_for_entity, normalize_entity_type, normalized_name_key
from pipeline.model_provider import call_model_chat, model_call_kwargs
from pipeline.review_memory import load_review_memory, save_review_memory


CARD_AGENT_TRANSACTIONS_FILENAME = "card_agent_transactions.jsonl"
CARD_AGENT_STEP_SCHEMA = {
    "type": "object",
    "properties": {
        "tool_name": {
            "type": "string",
            "enum": [
                "search_entities",
                "get_entity",
                "get_card",
                "get_claims",
                "get_relationships",
                "get_redirects",
                "apply_identity_merge",
                "write_author_claim",
                "rewrite_references",
                "synthesize_affected_cards",
                "check_consistency",
                "finish",
            ],
        },
        "arguments": {"type": "object", "additionalProperties": True},
        "rationale": {"type": "string"},
        "final_response": {"type": "string"},
    },
    "required": ["tool_name", "arguments"],
    "additionalProperties": False,
}


def card_agent_transactions_path(review_dir: Path) -> Path:
    return review_dir / CARD_AGENT_TRANSACTIONS_FILENAME


def load_card_agent_transactions(review_dir: Path) -> list[dict[str, Any]]:
    return [row for row in read_jsonl(card_agent_transactions_path(review_dir)) if isinstance(row, dict)]


def _append_jsonl(path: Path, row: dict[str, Any]) -> None:
    rows = read_jsonl(path)
    rows.append(row)
    write_jsonl(path, rows)


def _read_file_snapshot(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"exists": False, "text": None}
    return {"exists": True, "text": path.read_text(encoding="utf-8")}


def _restore_file_snapshot(path: Path, snapshot: dict[str, Any]) -> None:
    if snapshot.get("exists"):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(str(snapshot.get("text") or ""), encoding="utf-8")
    elif path.exists():
        path.unlink()


class CardAgentTransaction:
    def __init__(self, review_dir: Path, request: dict[str, Any]) -> None:
        self.review_dir = review_dir
        self.transaction_id = stable_id(
            "card_agent_tx",
            str(request.get("request_id", "")),
            str(request.get("instruction_text", "")),
            now_utc_iso(),
            safe_uuid(),
        )
        self.request = request
        self.started_at_utc = now_utc_iso()
        self.steps: list[dict[str, Any]] = []
        self.read_set: list[dict[str, Any]] = []
        self._before_by_path: dict[str, dict[str, Any]] = {}

    def _key(self, path: Path) -> str:
        return str(path.resolve())

    def track_read(self, path: Path, purpose: str = "") -> None:
        snapshot = _read_file_snapshot(path)
        self.read_set.append(
            {
                "path": str(path),
                "purpose": purpose,
                "exists": bool(snapshot.get("exists")),
                "bytes": len(str(snapshot.get("text") or "")) if snapshot.get("exists") else 0,
            }
        )

    def _track_before(self, path: Path) -> None:
        key = self._key(path)
        if key not in self._before_by_path:
            self._before_by_path[key] = {"path": str(path), "before": _read_file_snapshot(path)}

    def read_json(self, path: Path, default: dict[str, Any]) -> dict[str, Any]:
        self.track_read(path)
        if not path.exists():
            return default
        payload = read_json(path)
        return payload if isinstance(payload, dict) else default

    def read_jsonl(self, path: Path) -> list[dict[str, Any]]:
        self.track_read(path)
        return read_jsonl(path)

    def write_json(self, path: Path, payload: Any) -> None:
        self._track_before(path)
        write_json(path, payload)

    def write_jsonl(self, path: Path, rows: list[dict[str, Any]]) -> None:
        self._track_before(path)
        write_jsonl(path, rows)

    def record_step(self, tool_name: str, arguments: dict[str, Any], result: Any = None, error: str = "") -> None:
        row: dict[str, Any] = {
            "step_index": len(self.steps) + 1,
            "tool_name": tool_name,
            "arguments": arguments,
            "timestamp_utc": now_utc_iso(),
        }
        if error:
            row["error"] = error
        else:
            row["result"] = result
        self.steps.append(row)

    def write_set(self) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for entry in self._before_by_path.values():
            path = Path(entry["path"])
            before = entry["before"]
            after = _read_file_snapshot(path)
            rows.append(
                {
                    "path": str(path),
                    "before": before,
                    "after": after,
                    "changed": before != after,
                }
            )
        return rows

    def affected_summary(self) -> dict[str, list[str]]:
        entities: set[str] = set()
        cards: set[str] = set()
        claims: set[str] = set()

        def add_values(target: set[str], value: Any) -> None:
            if isinstance(value, list):
                for item in value:
                    add_values(target, item)
                return
            text = str(value or "").strip()
            if text:
                target.add(text)

        def visit(value: Any) -> None:
            if isinstance(value, dict):
                for key, child in value.items():
                    key_text = str(key)
                    if key_text == "entity_id" or key_text.endswith("_entity_id") or key_text.endswith("_entity_ids"):
                        add_values(entities, child)
                    elif key_text == "card_id" or key_text.endswith("_card_id") or key_text.endswith("_card_ids"):
                        add_values(cards, child)
                    elif key_text == "claim_id" or key_text.endswith("_claim_id") or key_text.endswith("_claim_ids"):
                        add_values(claims, child)
                    visit(child)
            elif isinstance(value, list):
                for item in value:
                    visit(item)

        visit(self.steps)
        return {
            "entities": sorted(entities),
            "cards": sorted(cards),
            "claims": sorted(claims),
        }

    def rollback(self) -> None:
        for entry in reversed(list(self._before_by_path.values())):
            _restore_file_snapshot(Path(entry["path"]), entry["before"])

    def finalize(self, status: str, *, rationale: str = "", error: str = "", validation: dict[str, Any] | None = None) -> dict[str, Any]:
        row = {
            "transaction_id": self.transaction_id,
            "request_id": self.request.get("request_id", ""),
            "request_text": self.request.get("instruction_text", ""),
            "status": status,
            "started_at_utc": self.started_at_utc,
            "finished_at_utc": now_utc_iso(),
            "rationale": rationale,
            "error": error,
            "steps": self.steps,
            "read_set": self.read_set,
            "write_set": self.write_set(),
            "affected": self.affected_summary(),
            "validation": validation or {},
        }
        _append_jsonl(card_agent_transactions_path(self.review_dir), row)
        return row


def _json_payload(path: Path, default: dict[str, Any]) -> dict[str, Any]:
    if not path.exists():
        return default
    payload = read_json(path)
    return payload if isinstance(payload, dict) else default


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


def _resolve_entity_ref(entities: list[dict[str, Any]], value: Any = "", *, entity_id: Any = "", card_id: Any = "", name: Any = "") -> dict[str, Any] | None:
    by_id, by_card, by_name = _entity_indexes(entities)
    candidates = [entity_id, card_id, name, value]
    for candidate in candidates:
        text = str(candidate or "").strip()
        if not text:
            continue
        entity = by_id.get(text) or by_card.get(text) or by_name.get(normalized_name_key(text))
        if entity:
            return entity
    return None


def _clean_text_list(values: Any) -> list[str]:
    if not isinstance(values, list):
        values = [] if values in (None, "") else [values]
    out: list[str] = []
    seen: set[str] = set()
    for value in values:
        text = str(value or "").strip()
        if not text:
            continue
        key = normalized_name_key(text)
        if key in seen:
            continue
        seen.add(key)
        out.append(text)
    return out


def _upsert_by_id(rows: list[dict[str, Any]], row: dict[str, Any], id_key: str) -> None:
    row_id = str(row.get(id_key, "")).strip()
    for index, existing in enumerate(rows):
        if isinstance(existing, dict) and str(existing.get(id_key, "")).strip() == row_id:
            rows[index] = {**existing, **row}
            return
    rows.append(row)


class CardbaseAgentRuntime:
    def __init__(
        self,
        *,
        review_dir: Path,
        entities: list[dict[str, Any]],
        accepted_claims: list[dict[str, Any]],
        review_memory_path: Path,
        author_claims_path: Path,
        card_drafts_path: Path,
        canonical_cards_path: Path,
        config: dict[str, Any],
        transaction: CardAgentTransaction,
    ) -> None:
        self.review_dir = review_dir
        self.entities = entities
        self.accepted_claims = accepted_claims
        self.review_memory_path = review_memory_path
        self.author_claims_path = author_claims_path
        self.card_drafts_path = card_drafts_path
        self.canonical_cards_path = canonical_cards_path
        self.config = config
        self.tx = transaction
        self.paths = {
            "requests": review_dir / CARD_EDIT_REQUESTS_FILENAME,
            "applied": review_dir / CARD_ARCHITECTURE_APPLIED_FILENAME,
            "failures": review_dir / CARD_ARCHITECTURE_FAILURES_FILENAME,
            "redirects": review_dir / CARD_REDIRECTS_FILENAME,
            "transactions": review_dir / CARD_AGENT_TRANSACTIONS_FILENAME,
        }
        self.last_merge: dict[str, Any] | None = None

    def tool_search_entities(self, arguments: dict[str, Any]) -> dict[str, Any]:
        query = str(arguments.get("query") or arguments.get("text") or "").strip()
        query_key = normalized_name_key(query)
        rows = []
        for entity in self.entities:
            names = [str(entity.get("canonical_name", "")), *[str(alias) for alias in entity.get("aliases", []) or []]]
            haystack = " ".join(names)
            haystack_key = normalized_name_key(haystack)
            if not query_key or query_key in haystack_key or any(query_key in normalized_name_key(name) for name in names):
                rows.append(
                    {
                        "entity_id": entity.get("entity_id", ""),
                        "card_id": entity.get("card_id") or card_id_for_entity(str(entity.get("canonical_name", ""))),
                        "canonical_name": entity.get("canonical_name", ""),
                        "entity_type": normalize_entity_type(entity.get("entity_type", "term")),
                        "aliases": _clean_text_list(entity.get("aliases", [])),
                    }
                )
        return {"matches": rows[:25], "match_count": len(rows)}

    def tool_get_entity(self, arguments: dict[str, Any]) -> dict[str, Any]:
        entity = _resolve_entity_ref(
            self.entities,
            arguments.get("entity_ref", ""),
            entity_id=arguments.get("entity_id", ""),
            card_id=arguments.get("card_id", ""),
            name=arguments.get("name", ""),
        )
        if not entity:
            raise ValueError("entity_not_found")
        return {"entity": entity}

    def _load_cards(self, path: Path) -> list[dict[str, Any]]:
        payload = self.tx.read_json(path, {"cards": []})
        return [card for card in payload.get("cards", []) if isinstance(card, dict)] if isinstance(payload, dict) else []

    def tool_get_card(self, arguments: dict[str, Any]) -> dict[str, Any]:
        entity = _resolve_entity_ref(
            self.entities,
            arguments.get("entity_ref", ""),
            entity_id=arguments.get("entity_id", ""),
            card_id=arguments.get("card_id", ""),
            name=arguments.get("name", ""),
        )
        refs = {
            str(arguments.get("card_id") or "").strip(),
            str(arguments.get("entity_id") or "").strip(),
            str(arguments.get("name") or "").strip(),
        }
        if entity:
            refs.update(
                {
                    str(entity.get("entity_id", "")).strip(),
                    str(entity.get("card_id") or card_id_for_entity(str(entity.get("canonical_name", "")))).strip(),
                    str(entity.get("canonical_name", "")).strip(),
                }
            )
        refs = {ref for ref in refs if ref}
        cards = []
        for source, path in [("draft", self.card_drafts_path), ("canonical", self.canonical_cards_path)]:
            for card in self._load_cards(path):
                names = {str(card.get("card_id", "")), str(card.get("canonical_name", "")), str(card.get("details", {}).get("entity_id", ""))}
                if names & refs or any(normalized_name_key(name) == normalized_name_key(ref) for name in names for ref in refs):
                    cards.append({"source": source, "card": card})
        return {"cards": cards}

    def tool_get_claims(self, arguments: dict[str, Any]) -> dict[str, Any]:
        entity = _resolve_entity_ref(
            self.entities,
            arguments.get("entity_ref", ""),
            entity_id=arguments.get("entity_id", ""),
            card_id=arguments.get("card_id", ""),
            name=arguments.get("name", ""),
        )
        if not entity:
            raise ValueError("entity_not_found")
        entity_id = str(entity.get("entity_id", ""))
        claims = [claim for claim in self.accepted_claims if str(claim.get("target_entity_id", "")) == entity_id]
        return {"entity_id": entity_id, "claims": claims, "claim_count": len(claims)}

    def tool_get_relationships(self, arguments: dict[str, Any]) -> dict[str, Any]:
        entity = _resolve_entity_ref(
            self.entities,
            arguments.get("entity_ref", ""),
            entity_id=arguments.get("entity_id", ""),
            card_id=arguments.get("card_id", ""),
            name=arguments.get("name", ""),
        )
        if not entity:
            raise ValueError("entity_not_found")
        entity_id = str(entity.get("entity_id", ""))
        card_id = str(entity.get("card_id") or card_id_for_entity(str(entity.get("canonical_name", ""))))
        relationships: list[dict[str, Any]] = []
        for source, path in [("draft", self.card_drafts_path), ("canonical", self.canonical_cards_path)]:
            for card in self._load_cards(path):
                source_matches = str(card.get("card_id", "")) == card_id or str(card.get("details", {}).get("entity_id", "")) == entity_id
                for rel in card.get("relationships", []) or []:
                    if not isinstance(rel, dict):
                        continue
                    target_matches = str(rel.get("target_card_id", "")) == card_id or str(rel.get("target_entity_id", "")) == entity_id
                    if source_matches or target_matches:
                        relationships.append({"source": source, "card_id": card.get("card_id"), "relationship": rel})
                details = card.get("details") if isinstance(card.get("details"), dict) else {}
                for link in details.get("wiki_links", []) or []:
                    if not isinstance(link, dict):
                        continue
                    target_matches = str(link.get("target_card_id", "")) == card_id or str(link.get("target_entity_id", "")) == entity_id
                    if source_matches or target_matches:
                        relationships.append({"source": source, "card_id": card.get("card_id"), "wiki_link": link})
        return {"relationships": relationships, "relationship_count": len(relationships)}

    def tool_get_redirects(self, _arguments: dict[str, Any]) -> dict[str, Any]:
        payload = self.tx.read_json(self.paths["redirects"], {"redirects": []})
        memory = self.tx.read_json(self.review_memory_path, load_review_memory(None))
        return {
            "redirects": payload.get("redirects", []) if isinstance(payload.get("redirects"), list) else [],
            "memory_redirects": memory.get("card_redirects", []) if isinstance(memory.get("card_redirects"), list) else [],
        }

    def _append_author_claim(self, action: dict[str, Any], target_entity: dict[str, Any]) -> dict[str, Any]:
        claim_text = str(action.get("claim_text") or "").strip()
        if not claim_text:
            raise ValueError("claim_text_required")
        target_name = str(target_entity.get("canonical_name", "")).strip()
        claim_type = str(action.get("claim_type") or "lore_fact").strip() or "lore_fact"
        knowledge_track = str(action.get("knowledge_track") or "lore").strip().lower()
        if knowledge_track not in {"lore", "meta", "both"}:
            knowledge_track = "lore"
        claim = {
            "claim_id": str(action.get("claim_id") or "").strip()
            or stable_id("author_claim", str(target_entity.get("entity_id", "")), claim_type, claim_text),
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
            "reviewer": "cardbase_agent",
            "review_rationale": str(action.get("rationale", "")),
            "manual_claim": True,
            "author_claim": True,
            "source_priority": "cardbase_agent",
            "card_agent_transaction_id": self.tx.transaction_id,
        }
        payload = self.tx.read_json(self.author_claims_path, {"claims": []})
        rows = payload.setdefault("claims", [])
        if not isinstance(rows, list):
            rows = []
            payload["claims"] = rows
        _upsert_by_id(rows, claim, "claim_id")
        payload["updated_at_utc"] = now_utc_iso()
        self.tx.write_json(self.author_claims_path, payload)
        return claim

    def tool_write_author_claim(self, arguments: dict[str, Any]) -> dict[str, Any]:
        target = _resolve_entity_ref(
            self.entities,
            arguments.get("target_entity_ref", ""),
            entity_id=arguments.get("target_entity_id", ""),
            card_id=arguments.get("target_card_id", ""),
            name=arguments.get("target_entity_name", ""),
        )
        if not target:
            raise ValueError("target_entity_not_found")
        claim = self._append_author_claim(arguments, target)
        return {"written_claim": claim}

    def tool_apply_identity_merge(self, arguments: dict[str, Any]) -> dict[str, Any]:
        source = _resolve_entity_ref(
            self.entities,
            arguments.get("source_entity_ref", ""),
            entity_id=arguments.get("source_entity_id", ""),
            card_id=arguments.get("source_card_id", ""),
            name=arguments.get("source_entity_name", ""),
        )
        target = _resolve_entity_ref(
            self.entities,
            arguments.get("target_entity_ref", ""),
            entity_id=arguments.get("target_entity_id", ""),
            card_id=arguments.get("target_card_id", ""),
            name=arguments.get("target_entity_name", ""),
        )
        if not source:
            raise ValueError("source_entity_not_found")
        if not target:
            raise ValueError("target_entity_not_found")
        source_id = str(source.get("entity_id", "")).strip()
        target_id = str(target.get("entity_id", "")).strip()
        if not source_id or not target_id or source_id == target_id:
            raise ValueError("identity_merge_requires_distinct_entities")
        source_name = str(source.get("canonical_name", "")).strip()
        target_name = str(target.get("canonical_name", "")).strip()
        source_card_id = str(source.get("card_id") or card_id_for_entity(source_name))
        target_card_id = str(target.get("card_id") or card_id_for_entity(target_name))
        action_id = str(arguments.get("action_id") or "").strip() or stable_id("card_agent_identity_merge", self.tx.transaction_id, source_id, target_id)
        rationale = str(arguments.get("rationale") or "").strip()

        memory = self.tx.read_json(self.review_memory_path, load_review_memory(None))
        merges = memory.setdefault("entity_merges", [])
        if not isinstance(merges, list):
            merges = []
            memory["entity_merges"] = merges
        merge = {
            "merge_id": stable_id("entity_merge", action_id, source_id, target_id),
            "source_entity_id": source_id,
            "source_card_id": source_card_id,
            "source_entity_name": source_name,
            "target_entity_id": target_id,
            "target_card_id": target_card_id,
            "target_entity_name": target_name,
            "canonical_name": target_name,
            "alias_text": source_name,
            "merge_type": "cardbase_agent_identity_merge",
            "source_claim_ids": [str(claim.get("claim_id", "")) for claim in self.accepted_claims if str(claim.get("target_entity_id", "")) == source_id],
            "source_snippet_ids": _clean_text_list([sid for claim in self.accepted_claims if str(claim.get("target_entity_id", "")) == source_id for sid in claim.get("source_snippet_ids", []) or []]),
            "approved_by": "cardbase_agent",
            "rationale": rationale,
            "approved_at_utc": now_utc_iso(),
            "card_agent_transaction_id": self.tx.transaction_id,
            "card_agent_action_id": action_id,
        }
        if not any(str(item.get("source_entity_id", "")) == source_id and str(item.get("target_entity_id", "")) == target_id for item in merges if isinstance(item, dict)):
            merges.append(merge)

        aliases = memory.setdefault("approved_aliases", [])
        if not isinstance(aliases, list):
            aliases = []
            memory["approved_aliases"] = aliases
        alias_candidates = _clean_text_list([source_name, *list(source.get("aliases", []) or [])])
        existing_aliases = {
            (str(item.get("target_entity_id", "")), normalized_name_key(str(item.get("alias_text", ""))))
            for item in aliases
            if isinstance(item, dict)
        }
        for alias_text in alias_candidates:
            alias_key = (target_id, normalized_name_key(alias_text))
            if alias_text and alias_key not in existing_aliases and normalized_name_key(alias_text) != normalized_name_key(target_name):
                aliases.append(
                    {
                        "target_entity_id": target_id,
                        "canonical_name": target_name,
                        "alias_text": alias_text,
                        "source_claim_id": ",".join(merge["source_claim_ids"]),
                        "source_snippet_ids": merge["source_snippet_ids"],
                        "approved_at_utc": now_utc_iso(),
                        "card_agent_transaction_id": self.tx.transaction_id,
                    }
                )
                existing_aliases.add(alias_key)

        redirect = {
            "redirect_id": stable_id("card_redirect", action_id, source_id, target_id),
            "source_entity_id": source_id,
            "source_card_id": source_card_id,
            "source_entity_name": source_name,
            "target_entity_id": target_id,
            "target_card_id": target_card_id,
            "target_entity_name": target_name,
            "target_section": str(arguments.get("target_section") or "background"),
            "status": "merged_into_card",
            "action_id": action_id,
            "rationale": rationale,
            "created_at_utc": now_utc_iso(),
            "card_agent_transaction_id": self.tx.transaction_id,
        }
        redirects_payload = self.tx.read_json(self.paths["redirects"], {"generated_at_utc": now_utc_iso(), "redirects": []})
        redirects = redirects_payload.setdefault("redirects", [])
        if not isinstance(redirects, list):
            redirects = []
            redirects_payload["redirects"] = redirects
        _upsert_by_id(redirects, redirect, "redirect_id")
        memory_redirects = memory.setdefault("card_redirects", [])
        if isinstance(memory_redirects, list):
            _upsert_by_id(memory_redirects, redirect, "redirect_id")

        applied_payload = self.tx.read_json(self.paths["applied"], {"generated_at_utc": now_utc_iso(), "applied_actions": []})
        applied = applied_payload.setdefault("applied_actions", [])
        if not isinstance(applied, list):
            applied = []
            applied_payload["applied_actions"] = applied
        applied_action = {
            "action_id": action_id,
            "action_type": "apply_identity_merge",
            "source_entity_id": source_id,
            "source_card_id": source_card_id,
            "source_entity_name": source_name,
            "target_entity_id": target_id,
            "target_card_id": target_card_id,
            "target_entity_name": target_name,
            "claim_text": str(arguments.get("claim_text") or self.tx.request.get("instruction_text") or ""),
            "rationale": rationale,
            "confidence": arguments.get("confidence", 1.0),
            "applied_at_utc": now_utc_iso(),
            "review_status": "approve",
            "source": "cardbase_agent",
            "card_agent_transaction_id": self.tx.transaction_id,
        }
        _upsert_by_id(applied, applied_action, "action_id")
        card_actions = memory.setdefault("card_architecture_actions", [])
        if isinstance(card_actions, list):
            _upsert_by_id(card_actions, applied_action, "action_id")

        self.tx.write_json(self.review_memory_path, memory)
        self.tx.write_json(self.paths["redirects"], redirects_payload)
        self.tx.write_json(self.paths["applied"], applied_payload)

        claim_text = str(arguments.get("claim_text") or self.tx.request.get("instruction_text") or "").strip()
        written_claim = None
        if claim_text:
            written_claim = self._append_author_claim(
                {
                    "claim_text": claim_text,
                    "claim_type": str(arguments.get("claim_type") or "lore_fact"),
                    "knowledge_track": str(arguments.get("knowledge_track") or "lore"),
                    "confidence": arguments.get("confidence", 1.0),
                    "rationale": rationale,
                },
                target,
            )
        self.last_merge = {"source": source, "target": target, "merge": merge, "redirect": redirect, "author_claim": written_claim}
        return {"merge": merge, "redirect": redirect, "author_claim": written_claim}

    def _rewrite_cards_in_path(self, path: Path, source: dict[str, Any], target: dict[str, Any]) -> dict[str, Any]:
        if not path.exists():
            return {"path": str(path), "changed": False, "reason": "missing"}
        payload = self.tx.read_json(path, {"cards": []})
        cards = [card for card in payload.get("cards", []) if isinstance(card, dict)] if isinstance(payload, dict) else []
        source_id = str(source.get("entity_id", "")).strip()
        source_card_id = str(source.get("card_id") or card_id_for_entity(str(source.get("canonical_name", ""))))
        source_name = str(source.get("canonical_name", "")).strip()
        target_id = str(target.get("entity_id", "")).strip()
        target_card_id = str(target.get("card_id") or card_id_for_entity(str(target.get("canonical_name", ""))))
        target_name = str(target.get("canonical_name", "")).strip()
        changed = False
        out_cards: list[dict[str, Any]] = []

        def rewrite_ref(item: dict[str, Any]) -> None:
            nonlocal changed
            target_match = (
                str(item.get("target_entity_id", "")).strip() == source_id
                or str(item.get("target_card_id", "")).strip() == source_card_id
                or normalized_name_key(str(item.get("target_entity_name", ""))) == normalized_name_key(source_name)
            )
            if target_match:
                item["target_entity_id"] = target_id
                item["target_card_id"] = target_card_id
                item["target_entity_name"] = target_name
                changed = True

        for card in cards:
            details = card.get("details") if isinstance(card.get("details"), dict) else {}
            card_entity_id = str(details.get("entity_id", "")).strip()
            card_id = str(card.get("card_id", "")).strip()
            card_name = str(card.get("canonical_name", "")).strip()
            if card_entity_id == source_id or card_id == source_card_id or normalized_name_key(card_name) == normalized_name_key(source_name):
                changed = True
                continue
            for rel in card.get("relationships", []) or []:
                if isinstance(rel, dict):
                    rewrite_ref(rel)
            for link in details.get("wiki_links", []) or []:
                if isinstance(link, dict):
                    rewrite_ref(link)
            out_cards.append(card)
        if changed:
            payload["cards"] = out_cards
            self.tx.write_json(path, payload)
        return {"path": str(path), "changed": changed, "card_count": len(out_cards)}

    def tool_rewrite_references(self, arguments: dict[str, Any]) -> dict[str, Any]:
        source = _resolve_entity_ref(
            self.entities,
            arguments.get("source_entity_ref", ""),
            entity_id=arguments.get("source_entity_id", ""),
            card_id=arguments.get("source_card_id", ""),
            name=arguments.get("source_entity_name", ""),
        )
        target = _resolve_entity_ref(
            self.entities,
            arguments.get("target_entity_ref", ""),
            entity_id=arguments.get("target_entity_id", ""),
            card_id=arguments.get("target_card_id", ""),
            name=arguments.get("target_entity_name", ""),
        )
        if (not source or not target) and self.last_merge:
            source = self.last_merge["source"]
            target = self.last_merge["target"]
        if not source or not target:
            raise ValueError("rewrite_references_requires_source_and_target")
        results = [
            self._rewrite_cards_in_path(self.card_drafts_path, source, target),
            self._rewrite_cards_in_path(self.canonical_cards_path, source, target),
        ]
        return {"rewrites": results}

    def tool_synthesize_affected_cards(self, arguments: dict[str, Any]) -> dict[str, Any]:
        return {
            "status": "deferred_to_stage_11",
            "note": "Stage 11 card synthesis will run after the cardbase agent transaction completes.",
            "arguments": arguments,
        }

    def tool_check_consistency(self, arguments: dict[str, Any]) -> dict[str, Any]:
        source = _resolve_entity_ref(
            self.entities,
            arguments.get("source_entity_ref", ""),
            entity_id=arguments.get("source_entity_id", ""),
            card_id=arguments.get("source_card_id", ""),
            name=arguments.get("source_entity_name", ""),
        )
        target = _resolve_entity_ref(
            self.entities,
            arguments.get("target_entity_ref", ""),
            entity_id=arguments.get("target_entity_id", ""),
            card_id=arguments.get("target_card_id", ""),
            name=arguments.get("target_entity_name", ""),
        )
        if (not source or not target) and self.last_merge:
            source = self.last_merge["source"]
            target = self.last_merge["target"]
        issues: list[str] = []
        if source and target:
            source_id = str(source.get("entity_id", "")).strip()
            source_card_id = str(source.get("card_id") or card_id_for_entity(str(source.get("canonical_name", ""))))
            source_name_key = normalized_name_key(str(source.get("canonical_name", "")))
            target_id = str(target.get("entity_id", "")).strip()
            memory = self.tx.read_json(self.review_memory_path, load_review_memory(None))
            if not any(str(item.get("source_entity_id", "")) == source_id and str(item.get("target_entity_id", "")) == target_id for item in memory.get("entity_merges", []) if isinstance(item, dict)):
                issues.append("missing_entity_merge_record")
            redirects = self.tx.read_json(self.paths["redirects"], {"redirects": []}).get("redirects", [])
            if not any(str(item.get("source_entity_id", "")) == source_id and str(item.get("target_entity_id", "")) == target_id for item in redirects if isinstance(item, dict)):
                issues.append("missing_redirect")
            for path in [self.card_drafts_path, self.canonical_cards_path]:
                if not path.exists():
                    continue
                payload = self.tx.read_json(path, {"cards": []})
                for card in payload.get("cards", []) if isinstance(payload.get("cards", []), list) else []:
                    if not isinstance(card, dict):
                        continue
                    details = card.get("details") if isinstance(card.get("details"), dict) else {}
                    if (
                        str(card.get("card_id", "")) == source_card_id
                        or str(details.get("entity_id", "")) == source_id
                        or normalized_name_key(str(card.get("canonical_name", ""))) == source_name_key
                    ):
                        issues.append(f"stale_source_card:{path.name}:{card.get('card_id')}")
                    for rel in card.get("relationships", []) or []:
                        if isinstance(rel, dict) and str(rel.get("target_card_id", "")) == source_card_id:
                            issues.append(f"stale_relationship_target:{path.name}:{card.get('card_id')}")
                    for link in details.get("wiki_links", []) or []:
                        if isinstance(link, dict) and str(link.get("target_card_id", "")) == source_card_id:
                            issues.append(f"stale_wiki_link_target:{path.name}:{card.get('card_id')}")
        return {"ok": not issues, "issues": issues}

    def execute_tool(self, tool_name: str, arguments: dict[str, Any]) -> Any:
        tools = {
            "search_entities": self.tool_search_entities,
            "get_entity": self.tool_get_entity,
            "get_card": self.tool_get_card,
            "get_claims": self.tool_get_claims,
            "get_relationships": self.tool_get_relationships,
            "get_redirects": self.tool_get_redirects,
            "apply_identity_merge": self.tool_apply_identity_merge,
            "write_author_claim": self.tool_write_author_claim,
            "rewrite_references": self.tool_rewrite_references,
            "synthesize_affected_cards": self.tool_synthesize_affected_cards,
            "check_consistency": self.tool_check_consistency,
        }
        if tool_name not in tools:
            raise ValueError(f"unknown_tool:{tool_name}")
        return tools[tool_name](arguments)

    def prompt(self, request: dict[str, Any], steps: list[dict[str, Any]]) -> str:
        entity_preview = [
            {
                "entity_id": entity.get("entity_id"),
                "card_id": entity.get("card_id") or card_id_for_entity(str(entity.get("canonical_name", ""))),
                "canonical_name": entity.get("canonical_name"),
                "entity_type": entity.get("entity_type"),
                "aliases": entity.get("aliases", []),
            }
            for entity in self.entities[:240]
        ]
        return f"""You are a true Cardbase Agent for the THERIAC lore cardbase.
You may inspect and mutate the cardbase only by choosing one tool call at a time.
The user request is authoritative, but you must explore first when needed. You make semantic decisions; deterministic code only validates and applies your tool calls.
Do not use a deterministic parser. If a request means two cardbase entities are the same identity, call apply_identity_merge with explicit source and target IDs, then rewrite_references, then check_consistency, then finish.
If entity references are unclear, inspect with search/get tools. If still impossible, finish with a clear final_response and no writes.

Available tools:
- search_entities(query)
- get_entity(entity_id|card_id|name|entity_ref)
- get_card(entity_id|card_id|name|entity_ref)
- get_claims(entity_id|card_id|name|entity_ref)
- get_relationships(entity_id|card_id|name|entity_ref)
- get_redirects()
- apply_identity_merge(source_entity_id, target_entity_id, claim_text, rationale, confidence)
- write_author_claim(target_entity_id, claim_text, claim_type, knowledge_track, rationale, confidence)
- rewrite_references(source_entity_id, target_entity_id)
- synthesize_affected_cards(entity_ids)
- check_consistency(source_entity_id, target_entity_id)
- finish(final_response)

User request:
{json.dumps(request, ensure_ascii=False, indent=2)}

Entity index:
{json.dumps(entity_preview, ensure_ascii=False, indent=2)}

Prior tool steps and results:
{json.dumps(steps, ensure_ascii=False, indent=2)}

Return exactly one JSON tool call matching the schema.
"""

    def run(self, request: dict[str, Any], max_steps: int = 16) -> dict[str, Any]:
        rationale = ""
        validation: dict[str, Any] = {}
        for _ in range(max_steps):
            response = call_model_chat(
                prompt=self.prompt(request, self.tx.steps),
                json_schema=CARD_AGENT_STEP_SCHEMA,
                **model_call_kwargs(self.config, "stage_11_card_architecture_agent"),
            )
            if response is None or not isinstance(response, dict):
                raise RuntimeError("cardbase_agent_model_returned_no_tool_call")
            tool_name = str(response.get("tool_name", "")).strip()
            arguments = response.get("arguments") if isinstance(response.get("arguments"), dict) else {}
            rationale = str(response.get("rationale") or rationale or "")
            if tool_name == "finish":
                final_response = str(response.get("final_response") or arguments.get("final_response") or "")
                self.tx.record_step(tool_name, arguments, {"final_response": final_response})
                if self.last_merge:
                    validation = self.tool_check_consistency(
                        {
                            "source_entity_id": self.last_merge["source"].get("entity_id", ""),
                            "target_entity_id": self.last_merge["target"].get("entity_id", ""),
                        }
                    )
                    if not validation.get("ok", False):
                        raise RuntimeError("cardbase_agent_consistency_failed:" + ",".join(validation.get("issues", [])))
                return {"status": "completed", "rationale": rationale or final_response, "validation": validation}
            try:
                result = self.execute_tool(tool_name, arguments)
                self.tx.record_step(tool_name, arguments, result)
                if tool_name == "check_consistency":
                    validation = result if isinstance(result, dict) else {}
            except Exception as exc:
                self.tx.record_step(tool_name, arguments, error=str(exc))
                raise
        raise RuntimeError("cardbase_agent_exceeded_max_steps")


def _update_request_status(
    review_dir: Path,
    request_id: str,
    status: str,
    transaction_id: str,
    error: str = "",
    transaction: CardAgentTransaction | None = None,
) -> None:
    path = review_dir / CARD_EDIT_REQUESTS_FILENAME
    rows = transaction.read_jsonl(path) if transaction else (load_card_edit_requests(path) if path.exists() else [])
    for row in rows:
        if str(row.get("request_id", "")).strip() == request_id:
            row["status"] = status
            row["card_agent_transaction_id"] = transaction_id
            row["card_agent_error"] = error
            row["updated_at_utc"] = now_utc_iso()
    if transaction:
        transaction.write_jsonl(path, rows)
    else:
        write_jsonl(path, rows)


def _append_failure(review_dir: Path, request: dict[str, Any], transaction_id: str, error: str) -> None:
    path = review_dir / CARD_ARCHITECTURE_FAILURES_FILENAME
    payload = _json_payload(path, {"generated_at_utc": now_utc_iso(), "failures": []})
    rows = payload.setdefault("failures", [])
    if not isinstance(rows, list):
        rows = []
        payload["failures"] = rows
    rows.append(
        {
            "failure_id": safe_uuid(),
            "request_id": request.get("request_id", ""),
            "instruction_text": request.get("instruction_text", ""),
            "transaction_id": transaction_id,
            "reason": error,
            "source": "cardbase_agent",
            "created_at_utc": now_utc_iso(),
        }
    )
    write_json(path, payload)


def run_pending_card_agent_requests(
    *,
    review_dir: Path,
    entities: list[dict[str, Any]],
    accepted_claims: list[dict[str, Any]],
    review_memory_path: Path,
    author_claims_path: Path,
    card_drafts_path: Path,
    canonical_cards_path: Path,
    config: dict[str, Any],
) -> dict[str, Any]:
    requests_path = review_dir / CARD_EDIT_REQUESTS_FILENAME
    requests = [
        row
        for row in (load_card_edit_requests(requests_path) if requests_path.exists() else [])
        if str(row.get("status", "pending")).strip().lower() == "pending"
        and str(row.get("instruction_text", "")).strip()
    ]
    completed: list[str] = []
    failed: list[str] = []
    for request in requests:
        tx = CardAgentTransaction(review_dir, request)
        runtime = CardbaseAgentRuntime(
            review_dir=review_dir,
            entities=entities,
            accepted_claims=accepted_claims,
            review_memory_path=review_memory_path,
            author_claims_path=author_claims_path,
            card_drafts_path=card_drafts_path,
            canonical_cards_path=canonical_cards_path,
            config=config,
            transaction=tx,
        )
        try:
            result = runtime.run(request)
            _update_request_status(review_dir, str(request.get("request_id", "")), "applied", tx.transaction_id, transaction=tx)
            tx.finalize("completed", rationale=str(result.get("rationale", "")), validation=result.get("validation", {}))
            completed.append(tx.transaction_id)
        except Exception as exc:
            error = str(exc)
            tx.rollback()
            tx.finalize("failed_rolled_back", error=error)
            _update_request_status(review_dir, str(request.get("request_id", "")), "failed", tx.transaction_id, error)
            _append_failure(review_dir, request, tx.transaction_id, error)
            failed.append(tx.transaction_id)
    return {"completed": completed, "failed": failed, "processed_count": len(completed) + len(failed)}


def undo_card_agent_transaction(review_dir: Path, transaction_id: str, reviewer: str = "user", rationale: str = "") -> dict[str, Any]:
    transactions = load_card_agent_transactions(review_dir)
    original = next((row for row in transactions if str(row.get("transaction_id", "")) == transaction_id), None)
    if not original:
        raise ValueError(f"Unknown card agent transaction: {transaction_id}")
    if str(original.get("status", "")) not in {"completed"}:
        raise ValueError(f"Only completed transactions can be undone; status={original.get('status')}")
    reversal_id = stable_id("card_agent_undo", transaction_id, now_utc_iso(), safe_uuid())
    before_rows: list[dict[str, Any]] = []
    for item in reversed(original.get("write_set", []) or []):
        if not isinstance(item, dict):
            continue
        path = Path(str(item.get("path", "")))
        before_rows.append({"path": str(path), "before": _read_file_snapshot(path), "restore_to": item.get("before", {})})
        _restore_file_snapshot(path, item.get("before", {}))
    write_set = [
        {
            "path": row["path"],
            "before": row["before"],
            "after": _read_file_snapshot(Path(row["path"])),
            "changed": row["before"] != _read_file_snapshot(Path(row["path"])),
        }
        for row in before_rows
    ]
    reversal = {
        "transaction_id": reversal_id,
        "status": "completed_reversal",
        "reverses_transaction_id": transaction_id,
        "request_id": original.get("request_id", ""),
        "request_text": original.get("request_text", ""),
        "started_at_utc": now_utc_iso(),
        "finished_at_utc": now_utc_iso(),
        "reviewer": reviewer,
        "rationale": rationale,
        "steps": [{"tool_name": "undo_card_agent_transaction", "arguments": {"transaction_id": transaction_id}}],
        "read_set": [{"path": str(card_agent_transactions_path(review_dir)), "purpose": "transaction_lookup"}],
        "write_set": write_set,
        "affected": original.get("affected", {}),
        "validation": {"ok": True},
    }
    _append_jsonl(card_agent_transactions_path(review_dir), reversal)
    return reversal


def card_agent_activity_payload(root: Path) -> dict[str, Any]:
    paths = ArtifactPaths(root)
    transactions = load_card_agent_transactions(paths.stage11)
    return {
        "active_root": str(root),
        "transactions": list(reversed(transactions)),
        "total": len(transactions),
        "source_path": str(paths.card_agent_transactions),
    }
