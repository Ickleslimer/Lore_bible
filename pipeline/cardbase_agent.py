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
    append_card_edit_request,
    ensure_card_architecture_files,
    load_card_edit_requests,
)
from pipeline.common import now_utc_iso, read_json, read_jsonl, safe_uuid, stable_id, write_json, write_jsonl
from pipeline.entity_resolution import card_id_for_entity, load_entity_records, normalize_entity_type, normalized_name_key
from pipeline.model_provider import call_model_chat, model_call_kwargs
from pipeline.review_memory import load_review_memory, save_review_memory


CARD_AGENT_TRANSACTIONS_FILENAME = "card_agent_transactions.jsonl"
CARD_AGENT_PROGRESS_FILENAME = "card_agent_progress.jsonl"
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
                "apply_canonical_rename",
                "remove_entity_from_cardbase",
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


def card_agent_progress_path(review_dir: Path) -> Path:
    return review_dir / CARD_AGENT_PROGRESS_FILENAME


def load_card_agent_transactions(review_dir: Path) -> list[dict[str, Any]]:
    return [row for row in read_jsonl(card_agent_transactions_path(review_dir)) if isinstance(row, dict)]


def _append_jsonl(path: Path, row: dict[str, Any]) -> None:
    rows = read_jsonl(path)
    rows.append(row)
    write_jsonl(path, rows)


def _append_progress_event(review_dir: Path, row: dict[str, Any]) -> None:
    path = card_agent_progress_path(review_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(row, ensure_ascii=False) + "\n")


def _progress_result_summary(result: Any) -> str:
    if isinstance(result, dict):
        for key in ("message", "final_response", "summary", "status", "rationale"):
            text = str(result.get(key) or "").strip()
            if text:
                return text[:240]
        parts: list[str] = []
        for key, value in result.items():
            if isinstance(value, list):
                parts.append(f"{len(value)} {key}")
            elif isinstance(value, dict):
                parts.append(f"{len(value)} {key}")
            elif key.endswith("_id") or key.endswith("_card_id") or key.endswith("_entity_id"):
                text = str(value or "").strip()
                if text:
                    parts.append(f"{key}={text}")
        return ", ".join(parts[:4])[:240]
    if isinstance(result, list):
        return f"{len(result)} item(s)"
    text = str(result or "").strip()
    return text[:240]


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
        self._progress(
            "started",
            "Agent transaction started.",
            status="running",
        )

    def _key(self, path: Path) -> str:
        return str(path.resolve())

    def _progress(self, event: str, message: str, **fields: Any) -> None:
        row = {
            "timestamp_utc": now_utc_iso(),
            "transaction_id": self.transaction_id,
            "request_id": self.request.get("request_id", ""),
            "request_text": self.request.get("instruction_text", ""),
            "event": event,
            "message": message,
        }
        for key, value in fields.items():
            if value is not None and value != "":
                row[key] = value
        _append_progress_event(self.review_dir, row)

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
        if error:
            self._progress(
                "tool_error",
                f"{tool_name} failed: {error}",
                step_index=row["step_index"],
                tool_name=tool_name,
                status="error",
            )
        else:
            summary = _progress_result_summary(result)
            self._progress(
                "tool_step",
                f"{tool_name}: {summary}" if summary else f"{tool_name} completed.",
                step_index=row["step_index"],
                tool_name=tool_name,
                status="running",
            )

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
        self._progress("rollback", "Restoring before-state for failed transaction.", status="rolling_back")
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
        if status == "completed":
            message = "Agent transaction completed."
            event = "completed"
        elif status == "completed_reversal":
            message = "Agent transaction reversal completed."
            event = "completed_reversal"
        else:
            message = f"Agent transaction ended with status {status}."
            event = "failed" if "failed" in status else "finished"
        if error:
            message = f"{message} {error}"
        self._progress(event, message, status=status)
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
        claim_review_decisions_path: Path | None = None,
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
        self.claim_review_decisions_path = claim_review_decisions_path or (review_dir.parent / "09_claim_drafting" / "claim_review_decisions.json")
        self.claim_drafts_path = self.claim_review_decisions_path.with_name("claim_drafts.json")
        self.resolved_entities_path = review_dir.parent / "07_entity_resolution" / "resolved_entities.json"
        self.identity_preview_path = review_dir.parent / "10_identity_merge" / "identity_merged_entities_preview.json"
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
        self.last_removal: dict[str, Any] | None = None
        self.last_rename: dict[str, Any] | None = None

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

    def _rename_entity_rows_in_path(self, path: Path, entity: dict[str, Any], new_name: str, new_card_id: str, list_key: str) -> dict[str, Any]:
        if not path.exists():
            return {"path": str(path), "changed": False, "reason": "missing", "renamed_entity_ids": []}
        payload = self.tx.read_json(path, {list_key: []})
        rows = payload.get(list_key, []) if isinstance(payload.get(list_key), list) else []
        entity_id = str(entity.get("entity_id", "")).strip()
        old_name = str(entity.get("canonical_name", "")).strip()
        old_card_id = str(entity.get("card_id") or card_id_for_entity(old_name))
        old_name_key = normalized_name_key(old_name)
        changed = False
        renamed_ids: list[str] = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            row_matches = (
                str(row.get("entity_id", "")).strip() == entity_id
                or str(row.get("card_id", "")).strip() == old_card_id
                or normalized_name_key(str(row.get("canonical_name", ""))) == old_name_key
            )
            if not row_matches:
                continue
            aliases = _clean_text_list([old_name, *list(row.get("aliases", []) or []), *list(entity.get("aliases", []) or [])])
            row["canonical_name"] = new_name
            row["card_id"] = new_card_id
            row["aliases"] = [alias for alias in aliases if normalized_name_key(alias) != normalized_name_key(new_name)]
            row["resolution_status"] = str(row.get("resolution_status") or "resolved")
            renamed_ids.append(str(row.get("entity_id", "") or entity_id))
            changed = True
        if changed:
            payload["updated_at_utc"] = now_utc_iso()
            self.tx.write_json(path, payload)
        return {"path": str(path), "changed": changed, "renamed_entity_ids": renamed_ids}

    def _rename_claim_targets_in_path(self, path: Path, entity: dict[str, Any], new_name: str, new_card_id: str, list_key: str) -> dict[str, Any]:
        if not path.exists():
            return {"path": str(path), "changed": False, "reason": "missing", "renamed_claim_ids": []}
        payload = self.tx.read_json(path, {list_key: []})
        rows = payload.get(list_key, []) if isinstance(payload.get(list_key), list) else []
        entity_id = str(entity.get("entity_id", "")).strip()
        old_name = str(entity.get("canonical_name", "")).strip()
        old_card_id = str(entity.get("card_id") or card_id_for_entity(old_name))
        old_name_key = normalized_name_key(old_name)
        changed = False
        renamed_claim_ids: list[str] = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            row_matches = (
                str(row.get("target_entity_id", "")).strip() == entity_id
                or str(row.get("target_card_id", "")).strip() == old_card_id
                or normalized_name_key(str(row.get("target_entity_name", ""))) == old_name_key
            )
            if not row_matches:
                continue
            row["target_entity_id"] = entity_id
            row["target_card_id"] = new_card_id
            row["target_entity_name"] = new_name
            if str(row.get("canonical_name", "")).strip():
                row["canonical_name"] = new_name
            renamed_claim_ids.append(str(row.get("claim_id", "")).strip())
            changed = True
        if changed:
            payload["updated_at_utc"] = now_utc_iso()
            self.tx.write_json(path, payload)
        return {"path": str(path), "changed": changed, "renamed_claim_ids": [claim_id for claim_id in renamed_claim_ids if claim_id]}

    def _rename_cards_in_path(self, path: Path, entity: dict[str, Any], new_name: str, new_card_id: str) -> dict[str, Any]:
        if not path.exists():
            return {"path": str(path), "changed": False, "reason": "missing"}
        payload = self.tx.read_json(path, {"cards": []})
        cards = [card for card in payload.get("cards", []) if isinstance(card, dict)] if isinstance(payload, dict) else []
        entity_id = str(entity.get("entity_id", "")).strip()
        old_name = str(entity.get("canonical_name", "")).strip()
        old_card_id = str(entity.get("card_id") or card_id_for_entity(old_name))
        old_name_key = normalized_name_key(old_name)
        changed = False
        renamed_card_ids: list[str] = []
        reference_updates = 0

        def rewrite_ref(item: dict[str, Any]) -> None:
            nonlocal changed, reference_updates
            target_match = (
                str(item.get("target_entity_id", "")).strip() == entity_id
                or str(item.get("target_card_id", "")).strip() == old_card_id
                or normalized_name_key(str(item.get("target_entity_name", ""))) == old_name_key
            )
            if not target_match:
                return
            if (
                str(item.get("target_entity_id", "")).strip() != entity_id
                or str(item.get("target_card_id", "")).strip() != new_card_id
                or str(item.get("target_entity_name", "")).strip() != new_name
            ):
                reference_updates += 1
            item["target_entity_id"] = entity_id
            item["target_card_id"] = new_card_id
            item["target_entity_name"] = new_name
            changed = True

        for card in cards:
            details = card.get("details") if isinstance(card.get("details"), dict) else {}
            card_matches = (
                str(card.get("card_id", "")).strip() == old_card_id
                or str(details.get("entity_id", "")).strip() == entity_id
                or normalized_name_key(str(card.get("canonical_name", ""))) == old_name_key
            )
            if card_matches:
                aliases = _clean_text_list([old_name, *list(card.get("aliases", []) or []), *list(entity.get("aliases", []) or [])])
                card["card_id"] = new_card_id
                card["canonical_name"] = new_name
                card["aliases"] = [alias for alias in aliases if normalized_name_key(alias) != normalized_name_key(new_name)]
                details["entity_id"] = entity_id
                card["details"] = details
                renamed_card_ids.append(old_card_id)
                changed = True
            for rel in card.get("relationships", []) or []:
                if isinstance(rel, dict):
                    rewrite_ref(rel)
            for link in details.get("wiki_links", []) or []:
                if isinstance(link, dict):
                    rewrite_ref(link)
        if changed:
            payload["cards"] = cards
            payload["updated_at_utc"] = now_utc_iso()
            self.tx.write_json(path, payload)
        return {
            "path": str(path),
            "changed": changed,
            "renamed_card_ids": renamed_card_ids,
            "new_card_id": new_card_id,
            "reference_updates": reference_updates,
            "card_count": len(cards),
        }

    def _rename_memory_targets(
        self,
        memory: dict[str, Any],
        entity: dict[str, Any],
        new_name: str,
        new_card_id: str,
        rename: dict[str, Any],
    ) -> dict[str, Any]:
        entity_id = str(entity.get("entity_id", "")).strip()
        old_name = str(entity.get("canonical_name", "")).strip()
        old_card_id = str(entity.get("card_id") or card_id_for_entity(old_name))
        old_name_key = normalized_name_key(old_name)
        changed_counts: dict[str, int] = {}

        def target_matches(row: dict[str, Any]) -> bool:
            return (
                str(row.get("target_entity_id", "")).strip() == entity_id
                or str(row.get("target_card_id", "")).strip() == old_card_id
                or normalized_name_key(str(row.get("target_entity_name", ""))) == old_name_key
            )

        def source_matches(row: dict[str, Any]) -> bool:
            return (
                str(row.get("source_entity_id", "")).strip() == entity_id
                or str(row.get("source_card_id", "")).strip() == old_card_id
                or normalized_name_key(str(row.get("source_entity_name", ""))) == old_name_key
            )

        for list_key in ("accepted_claims", "rejected_claims"):
            rows = memory.get(list_key, [])
            if not isinstance(rows, list):
                continue
            count = 0
            for row in rows:
                if isinstance(row, dict) and target_matches(row):
                    row["target_entity_id"] = entity_id
                    row["target_card_id"] = new_card_id
                    row["target_entity_name"] = new_name
                    count += 1
            if count:
                changed_counts[list_key] = count

        for list_key in ("entity_merges", "card_redirects", "card_architecture_actions", "approved_cards"):
            rows = memory.get(list_key, [])
            if not isinstance(rows, list):
                continue
            count = 0
            for row in rows:
                if not isinstance(row, dict):
                    continue
                if target_matches(row):
                    row["target_entity_id"] = entity_id
                    row["target_card_id"] = new_card_id
                    row["target_entity_name"] = new_name
                    if str(row.get("canonical_name", "")).strip():
                        row["canonical_name"] = new_name
                    count += 1
                if source_matches(row):
                    row["source_entity_id"] = entity_id
                    row["source_card_id"] = new_card_id
                    row["source_entity_name"] = new_name
                    count += 1
            if count:
                changed_counts[list_key] = count

        aliases = memory.setdefault("approved_aliases", [])
        if not isinstance(aliases, list):
            aliases = []
            memory["approved_aliases"] = aliases
        alias_candidates = _clean_text_list([old_name, *list(entity.get("aliases", []) or [])])
        existing_aliases = {
            (str(item.get("target_entity_id", "")), normalized_name_key(str(item.get("alias_text", ""))))
            for item in aliases
            if isinstance(item, dict)
        }
        added_aliases = 0
        for alias_text in alias_candidates:
            alias_key = (entity_id, normalized_name_key(alias_text))
            if alias_text and alias_key not in existing_aliases and normalized_name_key(alias_text) != normalized_name_key(new_name):
                aliases.append(
                    {
                        "target_entity_id": entity_id,
                        "canonical_name": new_name,
                        "alias_text": alias_text,
                        "source_claim_id": ",".join(rename["source_claim_ids"]),
                        "source_snippet_ids": rename["source_snippet_ids"],
                        "approved_at_utc": now_utc_iso(),
                        "card_agent_transaction_id": self.tx.transaction_id,
                    }
                )
                existing_aliases.add(alias_key)
                added_aliases += 1
        for row in aliases:
            if isinstance(row, dict) and str(row.get("target_entity_id", "")).strip() == entity_id:
                row["canonical_name"] = new_name
        if added_aliases:
            changed_counts["approved_aliases"] = added_aliases

        renames = memory.setdefault("canonical_renames", [])
        if not isinstance(renames, list):
            renames = []
            memory["canonical_renames"] = renames
        _upsert_by_id(renames, rename, "rename_id")
        changed_counts["canonical_renames"] = 1
        memory["updated_at_utc"] = now_utc_iso()
        return changed_counts

    def tool_apply_canonical_rename(self, arguments: dict[str, Any]) -> dict[str, Any]:
        entity = _resolve_entity_ref(
            self.entities,
            arguments.get("entity_ref", ""),
            entity_id=arguments.get("entity_id", ""),
            card_id=arguments.get("card_id", ""),
            name=arguments.get("name", "") or arguments.get("current_canonical_name", ""),
        )
        if not entity:
            raise ValueError("entity_not_found")
        new_name = str(arguments.get("canonical_name") or arguments.get("new_canonical_name") or "").strip()
        if not new_name:
            raise ValueError("canonical_name_required")
        entity_id = str(entity.get("entity_id", "")).strip()
        old_name = str(entity.get("canonical_name", "")).strip()
        old_card_id = str(entity.get("card_id") or card_id_for_entity(old_name))
        new_card_id = str(arguments.get("target_card_id") or card_id_for_entity(new_name)).strip()
        if not entity_id:
            raise ValueError("entity_id_required")
        if normalized_name_key(old_name) == normalized_name_key(new_name) and old_card_id == new_card_id:
            raise ValueError("canonical_name_already_current")
        existing = _resolve_entity_ref(self.entities, name=new_name)
        if existing and str(existing.get("entity_id", "")).strip() != entity_id:
            raise ValueError("canonical_name_conflicts_with_existing_entity")
        existing_card = next(
            (
                candidate
                for candidate in self.entities
                if str(candidate.get("card_id") or card_id_for_entity(str(candidate.get("canonical_name", "")))) == new_card_id
                and str(candidate.get("entity_id", "")).strip() != entity_id
            ),
            None,
        )
        if existing_card:
            raise ValueError("canonical_card_id_conflicts_with_existing_entity")

        action_id = str(arguments.get("action_id") or "").strip() or stable_id("card_agent_canonical_rename", self.tx.transaction_id, entity_id, new_name)
        rationale = str(arguments.get("rationale") or self.tx.request.get("instruction_text") or "").strip()
        entity_claims = self._claims_for_entity(entity)
        source_claim_ids = [str(claim.get("claim_id", "")) for claim in entity_claims if str(claim.get("claim_id", "")).strip()]
        source_snippet_ids = _clean_text_list(
            [sid for claim in entity_claims for sid in claim.get("source_snippet_ids", []) or []]
        )
        rename = {
            "rename_id": stable_id("canonical_rename", action_id, entity_id, old_name, new_name),
            "entity_id": entity_id,
            "old_card_id": old_card_id,
            "old_canonical_name": old_name,
            "target_entity_id": entity_id,
            "target_card_id": new_card_id,
            "target_entity_name": new_name,
            "canonical_name": new_name,
            "alias_text": old_name,
            "source_claim_ids": source_claim_ids,
            "source_snippet_ids": source_snippet_ids,
            "rename_type": "cardbase_agent_canonical_rename",
            "approved_by": "cardbase_agent",
            "rationale": rationale,
            "approved_at_utc": now_utc_iso(),
            "card_agent_transaction_id": self.tx.transaction_id,
            "card_agent_action_id": action_id,
        }
        memory = self.tx.read_json(self.review_memory_path, load_review_memory(None))
        memory_changes = self._rename_memory_targets(memory, entity, new_name, new_card_id, rename)
        self.tx.write_json(self.review_memory_path, memory)

        entity_results = [
            self._rename_entity_rows_in_path(self.resolved_entities_path, entity, new_name, new_card_id, "resolved_entities"),
            self._rename_entity_rows_in_path(self.identity_preview_path, entity, new_name, new_card_id, "entities"),
        ]
        claim_results = [
            self._rename_claim_targets_in_path(self.claim_drafts_path, entity, new_name, new_card_id, "claims"),
            self._rename_claim_targets_in_path(self.author_claims_path, entity, new_name, new_card_id, "claims"),
        ]
        card_results = [
            self._rename_cards_in_path(self.card_drafts_path, entity, new_name, new_card_id),
            self._rename_cards_in_path(self.canonical_cards_path, entity, new_name, new_card_id),
        ]

        redirect = {
            "redirect_id": stable_id("card_redirect", action_id, entity_id, old_card_id, new_card_id),
            "source_entity_id": entity_id,
            "source_card_id": old_card_id,
            "source_entity_name": old_name,
            "target_entity_id": entity_id,
            "target_card_id": new_card_id,
            "target_entity_name": new_name,
            "target_section": str(arguments.get("target_section") or "summary"),
            "status": "renamed_to_card",
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
        self.tx.write_json(self.paths["redirects"], redirects_payload)

        applied_payload = self.tx.read_json(self.paths["applied"], {"generated_at_utc": now_utc_iso(), "applied_actions": []})
        applied = applied_payload.setdefault("applied_actions", [])
        if not isinstance(applied, list):
            applied = []
            applied_payload["applied_actions"] = applied
        applied_action = {
            "action_id": action_id,
            "action_type": "apply_canonical_rename",
            "target_entity_id": entity_id,
            "target_card_id": new_card_id,
            "target_entity_name": new_name,
            "old_card_id": old_card_id,
            "old_canonical_name": old_name,
            "canonical_name": new_name,
            "claim_text": str(arguments.get("claim_text") or self.tx.request.get("instruction_text") or ""),
            "rationale": rationale,
            "confidence": arguments.get("confidence", 1.0),
            "applied_at_utc": now_utc_iso(),
            "review_status": "approve",
            "source": "cardbase_agent",
            "card_agent_transaction_id": self.tx.transaction_id,
        }
        _upsert_by_id(applied, applied_action, "action_id")
        self.tx.write_json(self.paths["applied"], applied_payload)

        memory = self.tx.read_json(self.review_memory_path, load_review_memory(None))
        memory_redirects = memory.setdefault("card_redirects", [])
        if isinstance(memory_redirects, list):
            _upsert_by_id(memory_redirects, redirect, "redirect_id")
        card_actions = memory.setdefault("card_architecture_actions", [])
        if isinstance(card_actions, list):
            _upsert_by_id(card_actions, applied_action, "action_id")
        memory["updated_at_utc"] = now_utc_iso()
        self.tx.write_json(self.review_memory_path, memory)

        claim_text = str(arguments.get("claim_text") or self.tx.request.get("instruction_text") or "").strip()
        updated_entity = {**entity, "canonical_name": new_name, "card_id": new_card_id}
        updated_entity["aliases"] = [
            alias
            for alias in _clean_text_list([old_name, *list(entity.get("aliases", []) or [])])
            if normalized_name_key(alias) != normalized_name_key(new_name)
        ]
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
                updated_entity,
            )
        for index, candidate in enumerate(self.entities):
            if str(candidate.get("entity_id", "")).strip() == entity_id:
                self.entities[index] = {**candidate, **updated_entity}
        for claim in self.accepted_claims:
            if self._claim_targets_entity(claim, entity):
                claim["target_entity_id"] = entity_id
                claim["target_card_id"] = new_card_id
                claim["target_entity_name"] = new_name
        self.last_rename = {
            "entity": {**entity},
            "renamed_entity": updated_entity,
            "rename": rename,
            "redirect": redirect,
            "author_claim": written_claim,
            "entity_results": entity_results,
            "claim_results": claim_results,
            "card_results": card_results,
            "memory_changes": memory_changes,
        }
        return self.last_rename

    def _claim_targets_entity(self, claim: dict[str, Any], entity: dict[str, Any]) -> bool:
        entity_id = str(entity.get("entity_id", "")).strip()
        card_id = str(entity.get("card_id") or card_id_for_entity(str(entity.get("canonical_name", ""))))
        name_key = normalized_name_key(str(entity.get("canonical_name", "")))
        return (
            str(claim.get("target_entity_id", "")).strip() == entity_id
            or str(claim.get("target_card_id", "")).strip() == card_id
            or normalized_name_key(str(claim.get("target_entity_name", ""))) == name_key
        )

    def _claims_for_entity(self, entity: dict[str, Any]) -> list[dict[str, Any]]:
        claims = [claim for claim in self.accepted_claims if self._claim_targets_entity(claim, entity)]
        if self.claim_drafts_path.exists():
            payload = self.tx.read_json(self.claim_drafts_path, {"claims": []})
            claims.extend(
                claim
                for claim in payload.get("claims", [])
                if isinstance(claim, dict) and self._claim_targets_entity(claim, entity)
            )
        return _dedupe_claims(claims)

    def _append_claim_rejections(self, entity: dict[str, Any], rationale: str) -> list[dict[str, Any]]:
        target_claims = self._claims_for_entity(entity)
        if not target_claims:
            return []
        payload = self.tx.read_json(self.claim_review_decisions_path, {"decisions": []})
        decisions = payload.setdefault("decisions", [])
        if not isinstance(decisions, list):
            decisions = []
            payload["decisions"] = decisions
        written: list[dict[str, Any]] = []
        for claim in target_claims:
            claim_id = str(claim.get("claim_id", "")).strip()
            if not claim_id:
                continue
            decision = {
                "decision_id": stable_id("card_agent_claim_reject", self.tx.transaction_id, claim_id),
                "claim_id": claim_id,
                "decision": "reject",
                "reviewer": "cardbase_agent",
                "rationale": rationale,
                "timestamp_utc": now_utc_iso(),
                "card_agent_transaction_id": self.tx.transaction_id,
            }
            decisions.append(decision)
            written.append(decision)
        if written:
            self.tx.write_json(self.claim_review_decisions_path, payload)
        return written

    def _remove_author_claims_for_entity(self, entity: dict[str, Any]) -> list[dict[str, Any]]:
        if not self.author_claims_path.exists():
            return []
        payload = self.tx.read_json(self.author_claims_path, {"claims": []})
        rows = payload.get("claims", []) if isinstance(payload.get("claims"), list) else []
        kept: list[dict[str, Any]] = []
        removed: list[dict[str, Any]] = []
        for row in rows:
            if isinstance(row, dict) and self._claim_targets_entity(row, entity):
                removed.append(row)
            elif isinstance(row, dict):
                kept.append(row)
        if removed:
            payload["claims"] = kept
            payload["updated_at_utc"] = now_utc_iso()
            self.tx.write_json(self.author_claims_path, payload)
        return removed

    def _remove_cards_in_path(self, path: Path, entity: dict[str, Any]) -> dict[str, Any]:
        if not path.exists():
            return {"path": str(path), "changed": False, "reason": "missing", "removed_card_ids": [], "pruned_reference_count": 0}
        payload = self.tx.read_json(path, {"cards": []})
        cards = [card for card in payload.get("cards", []) if isinstance(card, dict)] if isinstance(payload, dict) else []
        entity_id = str(entity.get("entity_id", "")).strip()
        card_id = str(entity.get("card_id") or card_id_for_entity(str(entity.get("canonical_name", ""))))
        name_key = normalized_name_key(str(entity.get("canonical_name", "")))
        changed = False
        pruned_reference_count = 0
        removed_card_ids: list[str] = []
        out_cards: list[dict[str, Any]] = []

        def is_target_ref(item: dict[str, Any]) -> bool:
            return (
                str(item.get("target_entity_id", "")).strip() == entity_id
                or str(item.get("target_card_id", "")).strip() == card_id
                or normalized_name_key(str(item.get("target_entity_name", ""))) == name_key
            )

        for card in cards:
            details = card.get("details") if isinstance(card.get("details"), dict) else {}
            card_matches = (
                str(card.get("card_id", "")).strip() == card_id
                or str(details.get("entity_id", "")).strip() == entity_id
                or normalized_name_key(str(card.get("canonical_name", ""))) == name_key
            )
            if card_matches:
                removed_card_ids.append(str(card.get("card_id", "") or card_id))
                changed = True
                continue
            relationships = card.get("relationships", []) if isinstance(card.get("relationships", []), list) else []
            kept_relationships = [rel for rel in relationships if not (isinstance(rel, dict) and is_target_ref(rel))]
            if len(kept_relationships) != len(relationships):
                pruned_reference_count += len(relationships) - len(kept_relationships)
                card["relationships"] = kept_relationships
                changed = True
            links = details.get("wiki_links", []) if isinstance(details.get("wiki_links", []), list) else []
            kept_links = [link for link in links if not (isinstance(link, dict) and is_target_ref(link))]
            if len(kept_links) != len(links):
                pruned_reference_count += len(links) - len(kept_links)
                details["wiki_links"] = kept_links
                card["details"] = details
                changed = True
            out_cards.append(card)
        if changed:
            payload["cards"] = out_cards
            self.tx.write_json(path, payload)
        return {
            "path": str(path),
            "changed": changed,
            "removed_card_ids": removed_card_ids,
            "pruned_reference_count": pruned_reference_count,
            "card_count": len(out_cards),
        }

    def _remove_entity_rows_in_path(self, path: Path, entity: dict[str, Any], list_key: str) -> dict[str, Any]:
        if not path.exists():
            return {"path": str(path), "changed": False, "reason": "missing", "removed_entity_ids": []}
        payload = self.tx.read_json(path, {list_key: []})
        rows = payload.get(list_key, []) if isinstance(payload.get(list_key), list) else []
        entity_id = str(entity.get("entity_id", "")).strip()
        card_id = str(entity.get("card_id") or card_id_for_entity(str(entity.get("canonical_name", ""))))
        name_key = normalized_name_key(str(entity.get("canonical_name", "")))
        kept: list[dict[str, Any]] = []
        removed_ids: list[str] = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            row_id = str(row.get("entity_id", "")).strip()
            row_card_id = str(row.get("card_id", "")).strip()
            row_name_key = normalized_name_key(str(row.get("canonical_name", "")))
            if row_id == entity_id or row_card_id == card_id or row_name_key == name_key:
                removed_ids.append(row_id or entity_id)
                continue
            kept.append(row)
        if not removed_ids:
            return {"path": str(path), "changed": False, "removed_entity_ids": []}
        payload[list_key] = kept
        if list_key == "entities":
            payload["source_entity_count"] = max(0, int(payload.get("source_entity_count", len(rows)) or len(rows)) - len(removed_ids))
            payload["merged_entity_count"] = len(kept)
            target_map = payload.get("target_map", {}) if isinstance(payload.get("target_map"), dict) else {}
            payload["target_map"] = {
                str(source_id): str(target_id)
                for source_id, target_id in target_map.items()
                if str(source_id) != entity_id and str(target_id) != entity_id
            }
            sources_by_target = payload.get("sources_by_target", {}) if isinstance(payload.get("sources_by_target"), dict) else {}
            payload["sources_by_target"] = {
                str(target_id): [str(source_id) for source_id in source_ids if str(source_id) != entity_id]
                for target_id, source_ids in sources_by_target.items()
                if str(target_id) != entity_id and isinstance(source_ids, list)
            }
        payload["updated_at_utc"] = now_utc_iso()
        self.tx.write_json(path, payload)
        return {"path": str(path), "changed": True, "removed_entity_ids": removed_ids, "entity_count": len(kept)}

    def tool_remove_entity_from_cardbase(self, arguments: dict[str, Any]) -> dict[str, Any]:
        entity = _resolve_entity_ref(
            self.entities,
            arguments.get("entity_ref", ""),
            entity_id=arguments.get("entity_id", ""),
            card_id=arguments.get("card_id", ""),
            name=arguments.get("name", ""),
        )
        if not entity:
            raise ValueError("entity_not_found")
        entity_id = str(entity.get("entity_id", "")).strip()
        entity_name = str(entity.get("canonical_name", "")).strip()
        card_id = str(entity.get("card_id") or card_id_for_entity(entity_name))
        rationale = str(arguments.get("rationale") or self.tx.request.get("instruction_text") or "").strip()
        action_id = str(arguments.get("action_id") or "").strip() or stable_id("card_agent_remove_entity", self.tx.transaction_id, entity_id, card_id)
        rejected_claim_decisions = self._append_claim_rejections(entity, rationale)
        removed_author_claims = self._remove_author_claims_for_entity(entity)
        card_results = [
            self._remove_cards_in_path(self.card_drafts_path, entity),
            self._remove_cards_in_path(self.canonical_cards_path, entity),
        ]
        entity_results = [
            self._remove_entity_rows_in_path(self.resolved_entities_path, entity, "resolved_entities"),
            self._remove_entity_rows_in_path(self.identity_preview_path, entity, "entities"),
        ]
        entity_claims = self._claims_for_entity(entity)
        source_claim_ids = [str(claim.get("claim_id", "")) for claim in entity_claims if str(claim.get("claim_id", "")).strip()]
        memory = self.tx.read_json(self.review_memory_path, load_review_memory(None))
        memory["accepted_claims"] = [
            claim
            for claim in memory.get("accepted_claims", [])
            if not (isinstance(claim, dict) and self._claim_targets_entity(claim, entity))
        ]
        rejected_memory = memory.setdefault("rejected_claims", [])
        if not isinstance(rejected_memory, list):
            rejected_memory = []
            memory["rejected_claims"] = rejected_memory
        existing_rejected_ids = {str(claim.get("claim_id", "")) for claim in rejected_memory if isinstance(claim, dict)}
        for claim in entity_claims:
            claim_id = str(claim.get("claim_id", "")).strip()
            if claim_id and self._claim_targets_entity(claim, entity) and claim_id not in existing_rejected_ids:
                rejected_memory.append(
                    {
                        **claim,
                        "decision": "reject",
                        "reviewer": "cardbase_agent",
                        "rationale": rationale,
                        "reviewed_at_utc": now_utc_iso(),
                        "card_agent_transaction_id": self.tx.transaction_id,
                    }
                )
                existing_rejected_ids.add(claim_id)
        removal = {
            "removal_id": stable_id("removed_entity", action_id, entity_id, card_id),
            "entity_id": entity_id,
            "card_id": card_id,
            "canonical_name": entity_name,
            "reason": rationale,
            "source_claim_ids": source_claim_ids,
            "removed_author_claim_ids": [str(claim.get("claim_id", "")) for claim in removed_author_claims],
            "removed_entity_artifacts": entity_results,
            "removed_at_utc": now_utc_iso(),
            "card_agent_transaction_id": self.tx.transaction_id,
            "card_agent_action_id": action_id,
        }
        removed_entities = memory.setdefault("removed_entities", [])
        if not isinstance(removed_entities, list):
            removed_entities = []
            memory["removed_entities"] = removed_entities
        _upsert_by_id(removed_entities, removal, "removal_id")
        applied_action = {
            "action_id": action_id,
            "action_type": "remove_entity_from_cardbase",
            "target_entity_id": entity_id,
            "target_card_id": card_id,
            "target_entity_name": entity_name,
            "claim_text": str(arguments.get("claim_text") or self.tx.request.get("instruction_text") or ""),
            "rationale": rationale,
            "confidence": arguments.get("confidence", 1.0),
            "applied_at_utc": now_utc_iso(),
            "review_status": "approve",
            "source": "cardbase_agent",
            "card_agent_transaction_id": self.tx.transaction_id,
        }
        card_actions = memory.setdefault("card_architecture_actions", [])
        if isinstance(card_actions, list):
            _upsert_by_id(card_actions, applied_action, "action_id")
        applied_payload = self.tx.read_json(self.paths["applied"], {"generated_at_utc": now_utc_iso(), "applied_actions": []})
        applied = applied_payload.setdefault("applied_actions", [])
        if not isinstance(applied, list):
            applied = []
            applied_payload["applied_actions"] = applied
        _upsert_by_id(applied, applied_action, "action_id")
        self.tx.write_json(self.review_memory_path, memory)
        self.tx.write_json(self.paths["applied"], applied_payload)
        self.last_removal = {
            "entity": entity,
            "removal": removal,
            "rejected_claim_decisions": rejected_claim_decisions,
            "removed_author_claims": removed_author_claims,
            "card_results": card_results,
            "entity_results": entity_results,
        }
        return self.last_removal

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
        removed = _resolve_entity_ref(
            self.entities,
            arguments.get("removed_entity_ref", ""),
            entity_id=arguments.get("removed_entity_id", ""),
            card_id=arguments.get("removed_card_id", ""),
            name=arguments.get("removed_entity_name", ""),
        )
        if not removed and self.last_removal:
            removed = self.last_removal["entity"]
        if removed and not (
            str(arguments.get("source_entity_id", "")).strip()
            or str(arguments.get("source_entity_ref", "")).strip()
            or str(arguments.get("target_entity_id", "")).strip()
            or str(arguments.get("target_entity_ref", "")).strip()
        ):
            return self._check_removed_entity_consistency(removed)
        renamed = _resolve_entity_ref(
            self.entities,
            arguments.get("renamed_entity_ref", "") or arguments.get("entity_ref", ""),
            entity_id=arguments.get("renamed_entity_id", "") or arguments.get("entity_id", ""),
            card_id=arguments.get("renamed_card_id", "") or arguments.get("card_id", ""),
            name=arguments.get("renamed_entity_name", "") or arguments.get("name", ""),
        )
        canonical_name = str(arguments.get("canonical_name") or arguments.get("new_canonical_name") or "").strip()
        if (renamed or self.last_rename) and canonical_name and not (
            str(arguments.get("source_entity_id", "")).strip()
            or str(arguments.get("source_entity_ref", "")).strip()
            or str(arguments.get("target_entity_id", "")).strip()
            or str(arguments.get("target_entity_ref", "")).strip()
        ):
            return self._check_canonical_rename_consistency(renamed or self.last_rename["renamed_entity"], canonical_name)
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

    def _check_canonical_rename_consistency(self, entity: dict[str, Any], canonical_name: str) -> dict[str, Any]:
        entity_id = str(entity.get("entity_id", "")).strip()
        new_name = str(canonical_name or entity.get("canonical_name", "")).strip()
        new_card_id = str(entity.get("card_id") or card_id_for_entity(new_name))
        old_entity = self.last_rename.get("entity", {}) if self.last_rename else {}
        old_name = str(old_entity.get("canonical_name", "")).strip()
        old_card_id = str(old_entity.get("card_id") or (card_id_for_entity(old_name) if old_name else ""))
        old_name_key = normalized_name_key(old_name)
        issues: list[str] = []

        for path, list_key in [(self.resolved_entities_path, "resolved_entities"), (self.identity_preview_path, "entities")]:
            if not path.exists():
                continue
            payload = self.tx.read_json(path, {list_key: []})
            rows = payload.get(list_key, []) if isinstance(payload.get(list_key, []), list) else []
            matched = False
            for row in rows:
                if not isinstance(row, dict):
                    continue
                row_matches = str(row.get("entity_id", "")).strip() == entity_id
                if row_matches:
                    matched = True
                    if str(row.get("canonical_name", "")).strip() != new_name:
                        issues.append(f"renamed_entity_name_not_updated:{path.name}:{entity_id}")
                    if str(row.get("card_id", "")).strip() != new_card_id:
                        issues.append(f"renamed_entity_card_id_not_updated:{path.name}:{entity_id}")
                elif old_name and (
                    str(row.get("card_id", "")).strip() == old_card_id
                    or normalized_name_key(str(row.get("canonical_name", ""))) == old_name_key
                ):
                    issues.append(f"stale_old_entity_row:{path.name}:{row.get('entity_id')}")
            if not matched and list_key == "resolved_entities":
                issues.append(f"renamed_entity_missing:{path.name}:{entity_id}")

        for path in [self.card_drafts_path, self.canonical_cards_path]:
            if not path.exists():
                continue
            payload = self.tx.read_json(path, {"cards": []})
            for card in payload.get("cards", []) if isinstance(payload.get("cards", []), list) else []:
                if not isinstance(card, dict):
                    continue
                details = card.get("details") if isinstance(card.get("details"), dict) else {}
                card_entity_id = str(details.get("entity_id", "")).strip()
                if card_entity_id == entity_id:
                    if str(card.get("card_id", "")).strip() != new_card_id:
                        issues.append(f"renamed_card_id_not_updated:{path.name}:{card.get('card_id')}")
                    if str(card.get("canonical_name", "")).strip() != new_name:
                        issues.append(f"renamed_card_name_not_updated:{path.name}:{card.get('card_id')}")
                elif old_name and (
                    str(card.get("card_id", "")).strip() == old_card_id
                    or normalized_name_key(str(card.get("canonical_name", ""))) == old_name_key
                ):
                    issues.append(f"stale_old_card:{path.name}:{card.get('card_id')}")
                for rel in card.get("relationships", []) or []:
                    if not isinstance(rel, dict):
                        continue
                    if str(rel.get("target_entity_id", "")).strip() == entity_id:
                        if str(rel.get("target_card_id", "")).strip() != new_card_id:
                            issues.append(f"renamed_relationship_card_id_not_updated:{path.name}:{card.get('card_id')}")
                        if str(rel.get("target_entity_name", "")).strip() != new_name:
                            issues.append(f"renamed_relationship_name_not_updated:{path.name}:{card.get('card_id')}")
                    elif old_card_id and str(rel.get("target_card_id", "")).strip() == old_card_id:
                        issues.append(f"stale_old_relationship_target:{path.name}:{card.get('card_id')}")
                for link in details.get("wiki_links", []) or []:
                    if not isinstance(link, dict):
                        continue
                    if str(link.get("target_entity_id", "")).strip() == entity_id:
                        if str(link.get("target_card_id", "")).strip() != new_card_id:
                            issues.append(f"renamed_wiki_link_card_id_not_updated:{path.name}:{card.get('card_id')}")
                        if str(link.get("target_entity_name", "")).strip() != new_name:
                            issues.append(f"renamed_wiki_link_name_not_updated:{path.name}:{card.get('card_id')}")
                    elif old_card_id and str(link.get("target_card_id", "")).strip() == old_card_id:
                        issues.append(f"stale_old_wiki_link_target:{path.name}:{card.get('card_id')}")

        for path, list_key in [(self.claim_drafts_path, "claims"), (self.author_claims_path, "claims")]:
            if not path.exists():
                continue
            payload = self.tx.read_json(path, {list_key: []})
            for row in payload.get(list_key, []) if isinstance(payload.get(list_key, []), list) else []:
                if not isinstance(row, dict):
                    continue
                if str(row.get("target_entity_id", "")).strip() == entity_id:
                    if str(row.get("target_card_id", "")).strip() != new_card_id:
                        issues.append(f"renamed_claim_card_id_not_updated:{path.name}:{row.get('claim_id')}")
                    if str(row.get("target_entity_name", "")).strip() != new_name:
                        issues.append(f"renamed_claim_name_not_updated:{path.name}:{row.get('claim_id')}")

        memory = self.tx.read_json(self.review_memory_path, load_review_memory(None))
        renames = memory.get("canonical_renames", []) if isinstance(memory.get("canonical_renames", []), list) else []
        if not any(str(item.get("entity_id", "")).strip() == entity_id and str(item.get("canonical_name", "")).strip() == new_name for item in renames if isinstance(item, dict)):
            issues.append("missing_canonical_rename_record")
        aliases = memory.get("approved_aliases", []) if isinstance(memory.get("approved_aliases", []), list) else []
        if old_name and not any(str(item.get("target_entity_id", "")).strip() == entity_id and normalized_name_key(str(item.get("alias_text", ""))) == old_name_key for item in aliases if isinstance(item, dict)):
            issues.append("missing_old_name_alias")
        redirects = self.tx.read_json(self.paths["redirects"], {"redirects": []}).get("redirects", [])
        if old_name and not any(
            str(item.get("source_entity_id", "")).strip() == entity_id
            and str(item.get("source_card_id", "")).strip() == old_card_id
            and str(item.get("target_card_id", "")).strip() == new_card_id
            for item in redirects
            if isinstance(item, dict)
        ):
            issues.append("missing_canonical_rename_redirect")
        return {"ok": not issues, "issues": issues, "renamed_entity_id": entity_id, "canonical_name": new_name, "card_id": new_card_id}

    def _check_removed_entity_consistency(self, entity: dict[str, Any]) -> dict[str, Any]:
        entity_id = str(entity.get("entity_id", "")).strip()
        card_id = str(entity.get("card_id") or card_id_for_entity(str(entity.get("canonical_name", ""))))
        name_key = normalized_name_key(str(entity.get("canonical_name", "")))
        issues: list[str] = []
        for path in [self.card_drafts_path, self.canonical_cards_path]:
            if not path.exists():
                continue
            payload = self.tx.read_json(path, {"cards": []})
            for card in payload.get("cards", []) if isinstance(payload.get("cards", []), list) else []:
                if not isinstance(card, dict):
                    continue
                details = card.get("details") if isinstance(card.get("details"), dict) else {}
                if (
                    str(card.get("card_id", "")).strip() == card_id
                    or str(details.get("entity_id", "")).strip() == entity_id
                    or normalized_name_key(str(card.get("canonical_name", ""))) == name_key
                ):
                    issues.append(f"stale_removed_card:{path.name}:{card.get('card_id')}")
                for rel in card.get("relationships", []) or []:
                    if not isinstance(rel, dict):
                        continue
                    if str(rel.get("target_entity_id", "")).strip() == entity_id or str(rel.get("target_card_id", "")).strip() == card_id:
                        issues.append(f"stale_removed_relationship:{path.name}:{card.get('card_id')}")
                for link in details.get("wiki_links", []) or []:
                    if not isinstance(link, dict):
                        continue
                    if str(link.get("target_entity_id", "")).strip() == entity_id or str(link.get("target_card_id", "")).strip() == card_id:
                        issues.append(f"stale_removed_wiki_link:{path.name}:{card.get('card_id')}")
        for path, list_key in [(self.resolved_entities_path, "resolved_entities"), (self.identity_preview_path, "entities")]:
            if not path.exists():
                continue
            payload = self.tx.read_json(path, {list_key: []})
            for row in payload.get(list_key, []) if isinstance(payload.get(list_key, []), list) else []:
                if not isinstance(row, dict):
                    continue
                if (
                    str(row.get("entity_id", "")).strip() == entity_id
                    or str(row.get("card_id", "")).strip() == card_id
                    or normalized_name_key(str(row.get("canonical_name", ""))) == name_key
                ):
                    issues.append(f"stale_removed_entity:{path.name}:{entity_id}")
        target_claim_ids = {
            str(claim.get("claim_id", "")).strip()
            for claim in self._claims_for_entity(entity)
            if self._claim_targets_entity(claim, entity) and str(claim.get("claim_id", "")).strip()
        }
        if target_claim_ids:
            decisions_payload = self.tx.read_json(self.claim_review_decisions_path, {"decisions": []})
            latest: dict[str, str] = {}
            for decision in decisions_payload.get("decisions", []) if isinstance(decisions_payload.get("decisions", []), list) else []:
                if not isinstance(decision, dict):
                    continue
                claim_id = str(decision.get("claim_id", "")).strip()
                if claim_id:
                    latest[claim_id] = str(decision.get("decision", "")).strip().lower()
            for claim_id in sorted(target_claim_ids):
                if latest.get(claim_id) != "reject":
                    issues.append(f"claim_not_rejected:{claim_id}")
        return {"ok": not issues, "issues": issues, "removed_entity_id": entity_id, "removed_card_id": card_id}

    def execute_tool(self, tool_name: str, arguments: dict[str, Any]) -> Any:
        tools = {
            "search_entities": self.tool_search_entities,
            "get_entity": self.tool_get_entity,
            "get_card": self.tool_get_card,
            "get_claims": self.tool_get_claims,
            "get_relationships": self.tool_get_relationships,
            "get_redirects": self.tool_get_redirects,
            "apply_identity_merge": self.tool_apply_identity_merge,
            "apply_canonical_rename": self.tool_apply_canonical_rename,
            "remove_entity_from_cardbase": self.tool_remove_entity_from_cardbase,
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
If a request asks to make a name the canonical name/title for an existing entity, inspect the entity and its card/claims, then call apply_canonical_rename with the entity ID and canonical_name, then check_consistency with renamed_entity_id and canonical_name, then finish. Use apply_identity_merge instead if the requested canonical name already belongs to a different entity.
If a request says an entity is not part of THERIAC, is not a THERIAC character, should be removed, deleted, excluded, or no longer belongs in the cardbase, inspect the entity and its claims, then call remove_entity_from_cardbase, then check_consistency with removed_entity_id, then finish.
If entity references are unclear, inspect with search/get tools. If still impossible, finish with a clear final_response and no writes.

Available tools:
- search_entities(query)
- get_entity(entity_id|card_id|name|entity_ref)
- get_card(entity_id|card_id|name|entity_ref)
- get_claims(entity_id|card_id|name|entity_ref)
- get_relationships(entity_id|card_id|name|entity_ref)
- get_redirects()
- apply_identity_merge(source_entity_id, target_entity_id, claim_text, rationale, confidence)
- apply_canonical_rename(entity_id|card_id|name|entity_ref, canonical_name, claim_text, rationale, confidence)
- remove_entity_from_cardbase(entity_id|card_id|name|entity_ref, rationale, confidence)
- write_author_claim(target_entity_id, claim_text, claim_type, knowledge_track, rationale, confidence)
- rewrite_references(source_entity_id, target_entity_id)
- synthesize_affected_cards(entity_ids)
- check_consistency(source_entity_id, target_entity_id | renamed_entity_id, canonical_name | removed_entity_id)
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
                elif self.last_removal:
                    validation = self.tool_check_consistency(
                        {
                            "removed_entity_id": self.last_removal["entity"].get("entity_id", ""),
                        }
                    )
                    if not validation.get("ok", False):
                        raise RuntimeError("cardbase_agent_consistency_failed:" + ",".join(validation.get("issues", [])))
                elif self.last_rename:
                    validation = self.tool_check_consistency(
                        {
                            "renamed_entity_id": self.last_rename["renamed_entity"].get("entity_id", ""),
                            "canonical_name": self.last_rename["renamed_entity"].get("canonical_name", ""),
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


def _run_card_agent_request(
    *,
    request: dict[str, Any],
    review_dir: Path,
    entities: list[dict[str, Any]],
    accepted_claims: list[dict[str, Any]],
    review_memory_path: Path,
    author_claims_path: Path,
    card_drafts_path: Path,
    canonical_cards_path: Path,
    claim_review_decisions_path: Path | None = None,
    config: dict[str, Any],
    max_steps: int = 16,
) -> dict[str, Any]:
    tx = CardAgentTransaction(review_dir, request)
    runtime = CardbaseAgentRuntime(
        review_dir=review_dir,
        entities=entities,
        accepted_claims=accepted_claims,
        review_memory_path=review_memory_path,
        author_claims_path=author_claims_path,
        card_drafts_path=card_drafts_path,
        canonical_cards_path=canonical_cards_path,
        claim_review_decisions_path=claim_review_decisions_path,
        config=config,
        transaction=tx,
    )
    request_id = str(request.get("request_id", ""))
    try:
        result = runtime.run(request, max_steps=max_steps)
        _update_request_status(review_dir, request_id, "applied", tx.transaction_id, transaction=tx)
        transaction = tx.finalize("completed", rationale=str(result.get("rationale", "")), validation=result.get("validation", {}))
        return {"status": "completed", "transaction_id": tx.transaction_id, "transaction": transaction}
    except Exception as exc:
        error = str(exc)
        tx.rollback()
        transaction = tx.finalize("failed_rolled_back", error=error)
        _update_request_status(review_dir, request_id, "failed", tx.transaction_id, error)
        _append_failure(review_dir, request, tx.transaction_id, error)
        return {"status": "failed_rolled_back", "transaction_id": tx.transaction_id, "transaction": transaction, "error": error}


def _find_repo_root_for_artifacts(artifacts_root: Path) -> Path:
    for candidate in [artifacts_root, *artifacts_root.parents]:
        if (candidate / "config" / "pipeline_config.json").exists() or (candidate / "canon" / "review_memory.json").exists():
            return candidate
    return Path.cwd()


def _dedupe_claims(claims: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    seen: set[str] = set()
    for claim in claims:
        claim_id = str(claim.get("claim_id", "")).strip()
        key = claim_id or stable_id("claim", str(claim.get("target_entity_id", "")), str(claim.get("claim_text", "")))
        if key in seen:
            continue
        seen.add(key)
        out.append(claim)
    return out


def _load_on_demand_context(
    artifacts_root: Path,
    *,
    review_memory_path: Path | None = None,
    config_path: Path | None = None,
) -> dict[str, Any]:
    from pipeline.stage_11_card_synthesis import (
        _load_decisions,
        apply_claim_decisions,
        default_author_claim_decisions,
        load_author_claims,
    )

    root = artifacts_root.resolve()
    paths = ArtifactPaths(root)
    repo_root = _find_repo_root_for_artifacts(root)
    resolved_review_memory_path = review_memory_path or (repo_root / "canon" / "review_memory.json")
    resolved_config_path = config_path or (repo_root / "config" / "pipeline_config.json")
    config = read_json(resolved_config_path) if resolved_config_path.exists() else {}
    config = config if isinstance(config, dict) else {}
    entities = load_entity_records(paths.resolved_entities)
    claim_payload = read_json(paths.claim_drafts) if paths.claim_drafts.exists() else {"claims": []}
    claims = [claim for claim in claim_payload.get("claims", []) if isinstance(claim, dict)] if isinstance(claim_payload, dict) else []
    claim_decisions = _load_decisions(paths.claim_review_decisions)
    author_claims, author_claim_failures = load_author_claims(paths.author_claims, entities)
    author_claim_decisions = default_author_claim_decisions(author_claims, claim_decisions)
    accepted_claims, _merge_log = apply_claim_decisions(claims + author_claims, claim_decisions + author_claim_decisions)
    accepted_claim_ids = {str(claim.get("claim_id", "")).strip() for claim in accepted_claims}
    accepted_claims.extend(
        {
            **claim,
            "status": "accepted",
        }
        for claim in claims
        if str(claim.get("status", "")).strip().lower() == "accepted"
        and str(claim.get("claim_id", "")).strip() not in accepted_claim_ids
    )
    memory = load_review_memory(resolved_review_memory_path)
    accepted_claims.extend(claim for claim in memory.get("accepted_claims", []) if isinstance(claim, dict))
    return {
        "paths": paths,
        "review_dir": paths.stage11,
        "entities": entities,
        "accepted_claims": _dedupe_claims(accepted_claims),
        "review_memory_path": resolved_review_memory_path,
        "author_claims_path": paths.author_claims,
        "claim_review_decisions_path": paths.claim_review_decisions,
        "card_drafts_path": paths.card_drafts,
        "canonical_cards_path": paths.canonical_cards,
        "config": config,
        "author_claim_failures": author_claim_failures,
    }


def run_card_agent_request(
    *,
    artifacts_root: Path,
    instruction_text: str,
    requester: str = "author",
    target_text: str = "",
    rationale: str = "",
    review_memory_path: Path | None = None,
    config_path: Path | None = None,
    max_steps: int = 16,
) -> dict[str, Any]:
    context = _load_on_demand_context(
        artifacts_root,
        review_memory_path=review_memory_path,
        config_path=config_path,
    )
    review_dir = context["review_dir"]
    ensure_card_architecture_files(review_dir)
    request = append_card_edit_request(
        artifacts_root,
        instruction_text,
        requester=requester,
        target_text=target_text,
        rationale=rationale,
        source="on_demand",
    )
    result = _run_card_agent_request(
        request=request,
        review_dir=review_dir,
        entities=context["entities"],
        accepted_claims=context["accepted_claims"],
        review_memory_path=context["review_memory_path"],
        author_claims_path=context["author_claims_path"],
        card_drafts_path=context["card_drafts_path"],
        canonical_cards_path=context["canonical_cards_path"],
        claim_review_decisions_path=context["claim_review_decisions_path"],
        config=context["config"],
        max_steps=max_steps,
    )
    result["request"] = request
    result["author_claim_failures"] = context["author_claim_failures"]
    return result


def run_pending_card_agent_requests(
    *,
    review_dir: Path,
    entities: list[dict[str, Any]],
    accepted_claims: list[dict[str, Any]],
    review_memory_path: Path,
    author_claims_path: Path,
    card_drafts_path: Path,
    canonical_cards_path: Path,
    claim_review_decisions_path: Path | None = None,
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
        result = _run_card_agent_request(
            request=request,
            review_dir=review_dir,
            entities=entities,
            accepted_claims=accepted_claims,
            review_memory_path=review_memory_path,
            author_claims_path=author_claims_path,
            card_drafts_path=card_drafts_path,
            canonical_cards_path=canonical_cards_path,
            claim_review_decisions_path=claim_review_decisions_path,
            config=config,
        )
        if result.get("status") == "completed":
            completed.append(str(result.get("transaction_id", "")))
        else:
            failed.append(str(result.get("transaction_id", "")))
    return {"completed": completed, "failed": failed, "processed_count": len(completed) + len(failed)}


def undo_card_agent_transaction(review_dir: Path, transaction_id: str, reviewer: str = "user", rationale: str = "") -> dict[str, Any]:
    transactions = load_card_agent_transactions(review_dir)
    original = next((row for row in transactions if str(row.get("transaction_id", "")) == transaction_id), None)
    if not original:
        raise ValueError(f"Unknown card agent transaction: {transaction_id}")
    if str(original.get("status", "")) not in {"completed"}:
        raise ValueError(f"Only completed transactions can be undone; status={original.get('status')}")
    reversal_id = stable_id("card_agent_undo", transaction_id, now_utc_iso(), safe_uuid())
    _append_progress_event(
        review_dir,
        {
            "timestamp_utc": now_utc_iso(),
            "transaction_id": reversal_id,
            "request_id": original.get("request_id", ""),
            "request_text": original.get("request_text", ""),
            "event": "undo_started",
            "status": "running",
            "message": f"Undo started for transaction {transaction_id}.",
            "reverses_transaction_id": transaction_id,
        },
    )
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
    _append_progress_event(
        review_dir,
        {
            "timestamp_utc": now_utc_iso(),
            "transaction_id": reversal_id,
            "request_id": original.get("request_id", ""),
            "request_text": original.get("request_text", ""),
            "event": "undo_completed",
            "status": "completed_reversal",
            "message": f"Undo completed for transaction {transaction_id}.",
            "reverses_transaction_id": transaction_id,
        },
    )
    return reversal


CHANGE_ITEM_ID_KEYS = (
    "rename_id",
    "merge_id",
    "redirect_id",
    "action_id",
    "claim_id",
    "request_id",
    "transaction_id",
    "proposal_id",
    "decision_id",
    "card_id",
    "entity_id",
)
CHANGE_DETAIL_KEYS = (
    "status",
    "decision",
    "action_type",
    "merge_type",
    "source_entity_id",
    "source_entity_name",
    "source_card_id",
    "target_entity_id",
    "target_entity_name",
    "target_card_id",
    "canonical_name",
    "old_canonical_name",
    "old_card_id",
    "alias_text",
    "rename_type",
    "claim_id",
    "claim_text",
    "request_id",
    "instruction_text",
    "rationale",
    "card_agent_transaction_id",
)


def _snapshot_text(snapshot: Any) -> str | None:
    if not isinstance(snapshot, dict) or not snapshot.get("exists"):
        return None
    text = snapshot.get("text")
    return str(text) if text is not None else ""


def _snapshot_meta(snapshot: Any) -> dict[str, Any]:
    text = _snapshot_text(snapshot)
    return {
        "exists": bool(isinstance(snapshot, dict) and snapshot.get("exists")),
        "chars": len(text) if text is not None else 0,
    }


def _display_change_path(path_text: str, root: Path) -> str:
    path = Path(path_text)
    for base in (root, root.parent, root.parent.parent, Path.cwd()):
        try:
            return str(path.relative_to(base))
        except ValueError:
            continue
    return path.name


def _parse_snapshot_payload(path_text: str, text: str | None) -> Any:
    if text is None:
        return None
    suffix = Path(path_text).suffix.lower()
    try:
        if suffix == ".jsonl":
            return [json.loads(line) for line in text.splitlines() if line.strip()]
        if suffix == ".json":
            return json.loads(text or "{}")
    except Exception:
        return None
    return None


def _change_item_id(item: Any) -> str:
    if isinstance(item, dict):
        for key in CHANGE_ITEM_ID_KEYS:
            text = str(item.get(key, "")).strip()
            if text:
                return text
        if item.get("target_entity_id") and item.get("alias_text"):
            return stable_id("change_item", str(item.get("target_entity_id")), str(item.get("alias_text")))
    return stable_id("change_item", json.dumps(item, sort_keys=True, ensure_ascii=False, default=str))


def _compact_text(value: Any, limit: int = 220) -> str:
    text = " ".join(str(value or "").split())
    return text if len(text) <= limit else text[: limit - 3] + "..."


def _change_item_label(item: Any) -> str:
    if not isinstance(item, dict):
        return _compact_text(item)
    source = str(item.get("source_entity_name") or item.get("source_entity_id") or "").strip()
    target = str(item.get("target_entity_name") or item.get("target_entity_id") or "").strip()
    if source and target:
        return f"{source} -> {target}"
    if item.get("alias_text") and item.get("canonical_name"):
        return f"{item.get('alias_text')} -> {item.get('canonical_name')}"
    if item.get("claim_text"):
        target_name = str(item.get("target_entity_name") or item.get("canonical_name") or "").strip()
        return f"{target_name}: {_compact_text(item.get('claim_text'))}" if target_name else _compact_text(item.get("claim_text"))
    if item.get("instruction_text"):
        return _compact_text(item.get("instruction_text"))
    return str(item.get("canonical_name") or item.get("card_id") or item.get("entity_id") or _change_item_id(item))


def _change_item_details(item: Any) -> dict[str, Any]:
    if not isinstance(item, dict):
        return {"value": _compact_text(item)}
    details: dict[str, Any] = {}
    for key in CHANGE_DETAIL_KEYS:
        value = item.get(key)
        if isinstance(value, (str, int, float, bool)) and str(value).strip():
            details[key] = _compact_text(value)
        elif isinstance(value, list) and value:
            details[key] = [_compact_text(v, 120) for v in value[:20]]
            if len(value) > 20:
                details[f"{key}_omitted_count"] = len(value) - 20
    return details


def _selected_field_changes(before: Any, after: Any) -> list[dict[str, str]]:
    if not isinstance(before, dict) or not isinstance(after, dict):
        return []
    changes: list[dict[str, str]] = []
    for key in CHANGE_DETAIL_KEYS:
        if before.get(key) == after.get(key):
            continue
        if key not in before and key not in after:
            continue
        changes.append(
            {
                "field": key,
                "before": _compact_text(before.get(key), 160),
                "after": _compact_text(after.get(key), 160),
            }
        )
    return changes


def _entity_display_name(item: Any, prefix: str) -> str:
    if not isinstance(item, dict):
        return ""
    for suffix in ("entity_name", "entity_id", "card_id"):
        text = str(item.get(f"{prefix}_{suffix}", "")).strip()
        if text:
            return text
    return ""


def _target_display_name(item: Any) -> str:
    if not isinstance(item, dict):
        return ""
    return str(
        item.get("target_entity_name")
        or item.get("canonical_name")
        or item.get("target_entity_id")
        or item.get("target_card_id")
        or ""
    ).strip()


def _sentence(text: str) -> str:
    text = " ".join(str(text or "").split())
    if not text:
        return ""
    return text if text.endswith((".", "!", "?")) else text + "."


def _status_change(before: Any, after: Any, key: str) -> tuple[str, str] | None:
    if not isinstance(before, dict) or not isinstance(after, dict):
        return None
    before_text = str(before.get(key, "")).strip()
    after_text = str(after.get(key, "")).strip()
    if before_text != after_text:
        return before_text, after_text
    return None


def _card_ref_display(item: dict[str, Any]) -> str:
    return str(item.get("target_entity_name") or item.get("target_entity_id") or item.get("target_card_id") or "").strip()


def _card_reference_redirects(before: Any, after: Any) -> list[str]:
    if not isinstance(before, dict) or not isinstance(after, dict):
        return []
    pairs: list[str] = []

    def visit(before_items: Any, after_items: Any) -> None:
        if not isinstance(before_items, list) or not isinstance(after_items, list):
            return
        for before_item, after_item in zip(before_items, after_items):
            if not isinstance(before_item, dict) or not isinstance(after_item, dict):
                continue
            before_ref = _card_ref_display(before_item)
            after_ref = _card_ref_display(after_item)
            if before_ref and after_ref and before_ref != after_ref:
                pairs.append(f"{before_ref} -> {after_ref}")

    visit(before.get("relationships", []), after.get("relationships", []))
    before_details = before.get("details") if isinstance(before.get("details"), dict) else {}
    after_details = after.get("details") if isinstance(after.get("details"), dict) else {}
    visit(before_details.get("wiki_links", []), after_details.get("wiki_links", []))
    return list(dict.fromkeys(pairs))


def _change_item_sentence(collection_name: str, bucket: str, item: Any, before: Any = None) -> str:
    if not isinstance(item, dict):
        return _sentence(f"{collection_name} {bucket}: {_compact_text(item)}")
    source = _entity_display_name(item, "source")
    target = _target_display_name(item)
    label = _change_item_label(item)
    claim_text = _compact_text(item.get("claim_text"), 180)
    instruction_text = _compact_text(item.get("instruction_text"), 180)

    if collection_name == "entity_merges":
        if bucket == "added" and source and target:
            return _sentence(f"{source} merged into {target}")
        if bucket == "removed" and source and target:
            return _sentence(f"{source} no longer merges into {target}")
    if collection_name == "canonical_renames":
        old_name = str(item.get("old_canonical_name") or item.get("alias_text") or "").strip()
        new_name = str(item.get("canonical_name") or target or "").strip()
        if bucket == "added" and old_name and new_name:
            return _sentence(f"{old_name} renamed to {new_name}")
        if bucket == "removed" and old_name and new_name:
            return _sentence(f"{old_name} rename to {new_name} removed")
    if collection_name == "approved_aliases":
        alias = str(item.get("alias_text") or source or "").strip()
        if bucket == "added" and alias and target:
            return _sentence(f"{alias} added as an alias for {target}")
        if bucket == "removed" and alias and target:
            return _sentence(f"{alias} removed as an alias for {target}")
    if collection_name in {"card_redirects", "redirects"} and source and target:
        if bucket == "added":
            return _sentence(f"{source} redirected to {target}")
        if bucket == "removed":
            return _sentence(f"{source} redirect to {target} removed")
    if collection_name in {"card_architecture_actions", "applied_actions"}:
        action_type = str(item.get("action_type") or "action").replace("_", " ")
        if str(item.get("action_type")) == "apply_identity_merge" and source and target:
            return _sentence(f"Identity merge action recorded for {source} -> {target}")
        if str(item.get("action_type")) == "apply_canonical_rename":
            old_name = str(item.get("old_canonical_name") or "").strip()
            new_name = str(item.get("canonical_name") or target or "").strip()
            if old_name and new_name:
                return _sentence(f"Canonical rename action recorded for {old_name} -> {new_name}")
        return _sentence(f"{action_type.capitalize()} {bucket}: {label}")
    if collection_name in {"claims", "accepted_claims", "author_claims"}:
        before_target = _target_display_name(before)
        after_target = target
        if bucket == "added":
            prefix = f"Claim added to {after_target}" if after_target else "Claim added"
            return _sentence(f"{prefix}: {claim_text or label}")
        if bucket == "removed":
            prefix = f"Claim removed from {after_target}" if after_target else "Claim removed"
            return _sentence(f"{prefix}: {claim_text or label}")
        if before_target and after_target and before_target != after_target:
            return _sentence(f"Claim redirected from {before_target} to {after_target}: {claim_text or label}")
        status = _status_change(before, item, "status") or _status_change(before, item, "decision")
        if status:
            return _sentence(f"Claim status changed from {status[0] or 'blank'} to {status[1] or 'blank'}: {claim_text or label}")
        return _sentence(f"Claim updated for {after_target}: {claim_text or label}" if after_target else f"Claim updated: {claim_text or label}")
    if collection_name == "rows" and (item.get("request_id") or item.get("instruction_text")):
        status = _status_change(before, item, "status")
        if status:
            return _sentence(f"Request marked {status[1] or 'blank'}: {instruction_text or label}")
        if bucket == "added":
            return _sentence(f"Request logged: {instruction_text or label}")
        if bucket == "removed":
            return _sentence(f"Request removed: {instruction_text or label}")
    if collection_name == "cards":
        card_name = str(item.get("canonical_name") or item.get("card_id") or label).strip()
        if bucket == "added":
            return _sentence(f"Card added: {card_name}")
        if bucket == "removed":
            return _sentence(f"Card removed: {card_name}")
        redirects = _card_reference_redirects(before, item)
        if redirects:
            return _sentence(f"Card references updated for {card_name}: {', '.join(redirects[:4])}")
        return _sentence(f"Card updated: {card_name}")
    return _sentence(f"{collection_name.replace('_', ' ')} {bucket}: {label}")


def _summarize_change_item(
    collection_name: str,
    bucket: str,
    item_id: str,
    item: Any,
    *,
    before: Any = None,
) -> dict[str, Any]:
    row = {
        "id": item_id,
        "label": _change_item_label(item),
        "sentence": _change_item_sentence(collection_name, bucket, item, before),
        "details": _change_item_details(item),
    }
    field_changes = _selected_field_changes(before, item)
    if field_changes:
        row["field_changes"] = field_changes
    return row


def _diff_change_list(name: str, before_items: Any, after_items: Any) -> dict[str, Any] | None:
    if not isinstance(before_items, list) or not isinstance(after_items, list):
        return None
    before_by_id = {_change_item_id(item): item for item in before_items}
    after_by_id = {_change_item_id(item): item for item in after_items}
    added_ids = sorted(set(after_by_id) - set(before_by_id))
    removed_ids = sorted(set(before_by_id) - set(after_by_id))
    updated_ids = sorted(
        item_id
        for item_id in set(before_by_id) & set(after_by_id)
        if before_by_id[item_id] != after_by_id[item_id]
    )
    if not added_ids and not removed_ids and not updated_ids:
        return None
    return {
        "name": name,
        "added": [_summarize_change_item(name, "added", item_id, after_by_id[item_id]) for item_id in added_ids],
        "removed": [_summarize_change_item(name, "removed", item_id, before_by_id[item_id]) for item_id in removed_ids],
        "updated": [
            _summarize_change_item(name, "updated", item_id, after_by_id[item_id], before=before_by_id[item_id])
            for item_id in updated_ids
        ],
    }


def _artifact_change_summary(root: Path, item: dict[str, Any]) -> dict[str, Any]:
    path_text = str(item.get("path") or "")
    before = item.get("before", {}) if isinstance(item.get("before"), dict) else {}
    after = item.get("after", {}) if isinstance(item.get("after"), dict) else {}
    before_text = _snapshot_text(before)
    after_text = _snapshot_text(after)
    before_payload = _parse_snapshot_payload(path_text, before_text)
    after_payload = _parse_snapshot_payload(path_text, after_text)
    before_exists = bool(before.get("exists"))
    after_exists = bool(after.get("exists"))
    change_type = "updated"
    if not before_exists and after_exists:
        change_type = "created"
    elif before_exists and not after_exists:
        change_type = "deleted"
    elif not item.get("changed"):
        change_type = "unchanged"
    summary: dict[str, Any] = {
        "path": path_text,
        "display_path": _display_change_path(path_text, root),
        "changed": bool(item.get("changed")),
        "change_type": change_type,
        "before": _snapshot_meta(before),
        "after": _snapshot_meta(after),
        "collections": [],
        "changed_fields": [],
    }
    collections: list[dict[str, Any]] = []
    if isinstance(before_payload, list) or isinstance(after_payload, list):
        collection = _diff_change_list("rows", before_payload or [], after_payload or [])
        if collection:
            collections.append(collection)
    elif isinstance(before_payload, dict) or isinstance(after_payload, dict):
        before_dict = before_payload if isinstance(before_payload, dict) else {}
        after_dict = after_payload if isinstance(after_payload, dict) else {}
        for key in sorted(set(before_dict) | set(after_dict)):
            before_value = before_dict.get(key)
            after_value = after_dict.get(key)
            if isinstance(before_value, list) or isinstance(after_value, list):
                collection = _diff_change_list(key, before_value or [], after_value or [])
                if collection:
                    collections.append(collection)
            elif before_value != after_value:
                summary["changed_fields"].append(key)
    summary["collections"] = collections
    return summary


def _transaction_change_lines(artifacts: list[dict[str, Any]]) -> list[dict[str, str]]:
    lines: list[dict[str, str]] = []
    seen: set[str] = set()
    for artifact in artifacts:
        artifact_path = str(artifact.get("display_path") or artifact.get("path") or "")
        for collection in artifact.get("collections", []) or []:
            if not isinstance(collection, dict):
                continue
            collection_name = str(collection.get("name") or "")
            for bucket in ("added", "updated", "removed"):
                rows = collection.get(bucket, [])
                if not isinstance(rows, list):
                    continue
                for row in rows:
                    if not isinstance(row, dict):
                        continue
                    sentence = str(row.get("sentence") or row.get("label") or "").strip()
                    if not sentence or sentence in seen:
                        continue
                    seen.add(sentence)
                    lines.append(
                        {
                            "sentence": sentence,
                            "kind": bucket,
                            "collection": collection_name,
                            "artifact": artifact_path,
                            "id": str(row.get("id") or ""),
                        }
                    )
    if not lines:
        for artifact in artifacts:
            if not artifact.get("changed"):
                continue
            sentence = _sentence(f"{artifact.get('display_path') or artifact.get('path')} {artifact.get('change_type') or 'updated'}")
            if sentence and sentence not in seen:
                seen.add(sentence)
                lines.append(
                    {
                        "sentence": sentence,
                        "kind": str(artifact.get("change_type") or "updated"),
                        "collection": "artifact",
                        "artifact": str(artifact.get("display_path") or artifact.get("path") or ""),
                        "id": "",
                    }
                )
    return lines


def _sanitized_write_set(root: Path, write_set: list[dict[str, Any]]) -> list[dict[str, Any]]:
    sanitized: list[dict[str, Any]] = []
    for item in write_set:
        if not isinstance(item, dict):
            continue
        summary = _artifact_change_summary(root, item)
        sanitized.append(
            {
                "path": summary["path"],
                "display_path": summary["display_path"],
                "changed": summary["changed"],
                "change_type": summary["change_type"],
                "before": summary["before"],
                "after": summary["after"],
            }
        )
    return sanitized


def _transaction_change_summary(root: Path, transaction: dict[str, Any]) -> dict[str, Any]:
    write_set = [item for item in transaction.get("write_set", []) or [] if isinstance(item, dict)]
    artifacts = [_artifact_change_summary(root, item) for item in write_set]
    affected = transaction.get("affected", {}) if isinstance(transaction.get("affected"), dict) else {}
    return {
        "affected": {
            "entities": [str(item) for item in affected.get("entities", []) or [] if str(item).strip()],
            "cards": [str(item) for item in affected.get("cards", []) or [] if str(item).strip()],
            "claims": [str(item) for item in affected.get("claims", []) or [] if str(item).strip()],
        },
        "artifacts": artifacts,
        "lines": _transaction_change_lines(artifacts),
    }


def _transaction_activity_row(root: Path, transaction: dict[str, Any]) -> dict[str, Any]:
    row = dict(transaction)
    write_set = [item for item in transaction.get("write_set", []) or [] if isinstance(item, dict)]
    row["write_set"] = _sanitized_write_set(root, write_set)
    row["change_summary"] = _transaction_change_summary(root, transaction)
    return row


def _format_progress_event(row: dict[str, Any]) -> str:
    timestamp = str(row.get("timestamp_utc") or "").strip()
    time_text = timestamp[11:19] if len(timestamp) >= 19 else timestamp
    step = row.get("step_index")
    tool_name = str(row.get("tool_name") or "").strip()
    event = str(row.get("event") or "").strip()
    message = str(row.get("message") or "").strip()
    label = tool_name or event or "agent"
    if step:
        label = f"{step}. {label}"
    pieces = [piece for piece in [time_text, label, message] if piece]
    return " | ".join(pieces)[:700]


def card_agent_progress_payload(root: Path, max_lines: int = 80) -> dict[str, Any]:
    paths = ArtifactPaths(root)
    limit = max(1, min(int(max_lines or 80), 300))
    source_path = paths.card_agent_progress
    try:
        events = [row for row in read_jsonl(source_path) if isinstance(row, dict)]
    except Exception:
        events = []
    tail_events = events[-limit:]
    lines = [_format_progress_event(row) for row in tail_events]
    updated_at = str(source_path.stat().st_mtime) if source_path.exists() else "0"
    latest = lines[-1] if lines else ""
    return {
        "active_root": str(root),
        "source_path": str(source_path),
        "latest_line": latest,
        "latest_progress_line": latest,
        "lines": lines,
        "events": tail_events,
        "total_scanned": len(events),
        "updated_at_epoch": updated_at,
    }


def card_agent_activity_payload(root: Path) -> dict[str, Any]:
    paths = ArtifactPaths(root)
    transactions = load_card_agent_transactions(paths.stage11)
    return {
        "active_root": str(root),
        "transactions": [_transaction_activity_row(root, transaction) for transaction in reversed(transactions)],
        "total": len(transactions),
        "source_path": str(paths.card_agent_transactions),
    }
