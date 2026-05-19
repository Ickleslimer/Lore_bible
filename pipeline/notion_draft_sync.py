from __future__ import annotations

import json
import os
import re
import time
from pathlib import Path
from typing import Any, Callable

import requests

from pipeline.common import get_logger, now_utc_iso, read_json, write_json
from pipeline.ui_review_app import card_review_sections


NOTION_VERSION = "2022-06-28"
NOTION_API_BASE = "https://api.notion.com/v1"
RICH_TEXT_LIMIT = 1900
APPEND_BLOCK_LIMIT = 100
STATE_PATH = Path("artifacts/learning/notion_draft_cards_state.json")
REPORT_PATH = Path("08_notion/notion_draft_sync_report.json")
DATABASE_TITLE = "THERIAC Draft Lore Cards"


class NotionSyncError(RuntimeError):
    pass


def _read_json_or_default(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        return read_json(path)
    except Exception:
        return default


def _read_env_file(path: Path) -> dict[str, str]:
    if not path.exists():
        return {}
    values: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key, value = stripped.split("=", 1)
        clean = value.strip().strip('"').strip("'")
        values[key.strip()] = clean
    return values


def _env_value(env_file: dict[str, str], *keys: str) -> str:
    for key in keys:
        value = os.environ.get(key, "").strip().strip('"').strip("'")
        if value:
            return value
        value = env_file.get(key, "").strip().strip('"').strip("'")
        if value:
            return value
    return ""


def normalize_notion_id(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    matches = re.findall(r"[0-9a-fA-F]{32}|[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}", text)
    raw = matches[-1] if matches else text
    raw = re.sub(r"[^0-9a-fA-F]", "", raw)
    if len(raw) != 32:
        return text
    return "-".join([raw[:8], raw[8:12], raw[12:16], raw[16:20], raw[20:]])


def notion_draft_config(config_path: Path | None = Path("config/pipeline_config.json"), env_path: Path | None = Path(".env")) -> dict[str, Any]:
    config = _read_json_or_default(config_path, {}) if config_path else {}
    notion_config = config.get("notion", {}) if isinstance(config.get("notion", {}), dict) else {}
    env_file = _read_env_file(env_path) if env_path else {}
    token = _env_value(env_file, "NOTION_API_KEY", "NOTION_ACCESS_TOKEN", "NOTION_TOKEN") or str(notion_config.get("api_key", "")).strip()
    parent_page_id = normalize_notion_id(
        _env_value(env_file, "NOTION_DRAFT_PARENT_PAGE_ID", "NOTION_PAGE_ID", "NOTION_PARENT_PAGE_ID")
        or notion_config.get("draft_parent_page_id", "")
    )
    database_id = normalize_notion_id(
        _env_value(env_file, "NOTION_DRAFT_CARDS_DATABASE_ID", "NOTION_DATABASE_ID", "NOTION_DRAFT_DATABASE_ID")
        or notion_config.get("draft_cards_database_id", "")
    )
    enabled = bool(notion_config.get("draft_sync_enabled", True))
    return {
        "enabled": enabled,
        "api_key": token,
        "parent_page_id": parent_page_id,
        "database_id": database_id,
    }


def _text(content: Any, *, bold: bool = False, code: bool = False) -> dict[str, Any]:
    return {
        "type": "text",
        "text": {"content": str(content or "")[:RICH_TEXT_LIMIT]},
        "annotations": {"bold": bold, "code": code},
    }


def _rich_text_chunks(text: Any) -> list[dict[str, Any]]:
    clean = re.sub(r"\s+", " ", str(text or "")).strip()
    if not clean:
        return []
    chunks = [clean[i : i + RICH_TEXT_LIMIT] for i in range(0, len(clean), RICH_TEXT_LIMIT)]
    return [_text(chunk) for chunk in chunks]


def _block(block_type: str, text: Any) -> dict[str, Any]:
    rich_text = _rich_text_chunks(text) or [_text(" ")]
    return {"object": "block", "type": block_type, block_type: {"rich_text": rich_text}}


def _paragraph_blocks(text: Any) -> list[dict[str, Any]]:
    clean = str(text or "").strip()
    if not clean:
        return []
    parts = [part.strip() for part in re.split(r"\n\s*\n", clean) if part.strip()]
    blocks: list[dict[str, Any]] = []
    for part in parts:
        compact = re.sub(r"[ \t]+", " ", part)
        for chunk in [compact[i : i + RICH_TEXT_LIMIT] for i in range(0, len(compact), RICH_TEXT_LIMIT)]:
            blocks.append(_block("paragraph", chunk))
    return blocks


def _bullet(text: Any) -> dict[str, Any]:
    return _block("bulleted_list_item", text)


def _word_count(text: Any) -> int:
    return len(re.findall(r"\b\w+\b", str(text or "")))


def card_word_count(card: dict[str, Any]) -> int:
    total = _word_count(card.get("summary", ""))
    for section in card_review_sections(card):
        total += _word_count(section.get("text", ""))
    return total


def _latest_card_decisions(decisions_path: Path) -> dict[str, dict[str, Any]]:
    payload = _read_json_or_default(decisions_path, {"decisions": []})
    latest: dict[str, dict[str, Any]] = {}
    for decision in payload.get("decisions", []) if isinstance(payload, dict) else []:
        if not isinstance(decision, dict):
            continue
        for key in (decision.get("card_id"), decision.get("target_card_id")):
            card_id = str(key or "").strip()
            if card_id:
                latest[card_id] = decision
    return latest


def review_status_for_card(card: dict[str, Any], decisions: dict[str, dict[str, Any]]) -> str:
    decision = decisions.get(str(card.get("card_id", "")))
    if not decision:
        return "pending review"
    action = str(decision.get("decision", "")).strip().lower()
    if action in {"approve", "accept"}:
        return "approved"
    if action == "reject":
        return "rejected"
    return action or "reviewed"


def render_card_blocks(card: dict[str, Any], run_id: str, review_status: str) -> list[dict[str, Any]]:
    details = card.get("details") if isinstance(card.get("details"), dict) else {}
    blocks: list[dict[str, Any]] = [
        _block("heading_1", str(card.get("canonical_name") or card.get("card_id") or "Draft Card")),
        _block("paragraph", "Draft preview only. Approve or reject this card in the THERIAC desktop app; Notion is not the source of truth."),
        _block("heading_2", "Review Metadata"),
        _bullet(f"Run: {run_id}"),
        _bullet(f"Card ID: {card.get('card_id', '')}"),
        _bullet(f"Review status: {review_status}"),
        _bullet(f"Entity type: {card.get('entity_type', '')}"),
        _bullet(f"Accepted claims: {len(details.get('accepted_claim_ids', []) or [])}"),
        _bullet(f"Source evidence items: {len(card.get('source_evidence', []) or [])}"),
        _block("heading_2", "Summary"),
        *_paragraph_blocks(card.get("summary", "")),
    ]

    for section in card_review_sections(card):
        blocks.append(_block("heading_2", section.get("title", "")))
        blocks.extend(_paragraph_blocks(section.get("text", "")))

    relationships = card.get("relationships", []) if isinstance(card.get("relationships", []), list) else []
    if relationships:
        blocks.append(_block("heading_2", "Structured Relationships"))
        for rel in relationships[:40]:
            if isinstance(rel, dict):
                blocks.append(_bullet(f"{rel.get('relation_type', 'related')} -> {rel.get('target_card_id', '')}: {rel.get('note', '')}"))

    timeline = card.get("timeline", []) if isinstance(card.get("timeline", []), list) else []
    if timeline:
        blocks.append(_block("heading_2", "Timeline"))
        for item in timeline[:40]:
            if isinstance(item, dict):
                blocks.append(_bullet(f"{item.get('timestamp_utc', '')}: {item.get('description', '')}"))

    wiki_links = details.get("wiki_links", []) if isinstance(details.get("wiki_links", []), list) else []
    if wiki_links:
        blocks.append(_block("heading_2", "Wiki Links"))
        for link in wiki_links[:60]:
            if isinstance(link, dict):
                blocks.append(_bullet(f"{link.get('relation_type', 'related')} -> {link.get('target_entity_name') or link.get('target_card_id', '')} ({link.get('section', '')})"))

    unresolved = details.get("unresolved_conflicts", []) if isinstance(details.get("unresolved_conflicts", []), list) else []
    if unresolved:
        blocks.append(_block("heading_2", "Unresolved Conflicts"))
        for item in unresolved[:30]:
            blocks.append(_bullet(json.dumps(item, ensure_ascii=False) if isinstance(item, dict) else str(item)))

    return blocks


class NotionDraftClient:
    def __init__(self, token: str, session: requests.Session | None = None) -> None:
        self.session = session or requests.Session()
        self.min_interval_seconds = 0.35
        self._last_request_epoch = 0.0
        self.headers = {
            "Authorization": f"Bearer {token}",
            "Notion-Version": NOTION_VERSION,
            "Content-Type": "application/json",
        }

    def request(self, method: str, path: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        response: requests.Response | None = None
        for attempt in range(5):
            elapsed = time.monotonic() - self._last_request_epoch
            if elapsed < self.min_interval_seconds:
                time.sleep(self.min_interval_seconds - elapsed)
            response = self.session.request(
                method,
                f"{NOTION_API_BASE}{path}",
                headers=self.headers,
                json=payload,
                timeout=60,
            )
            self._last_request_epoch = time.monotonic()
            if response.status_code != 429 and response.status_code < 500:
                break
            if attempt >= 4:
                break
            retry_after = response.headers.get("Retry-After", "")
            try:
                delay = float(retry_after)
            except ValueError:
                delay = min(2.0 * (attempt + 1), 20.0)
            time.sleep(max(delay, self.min_interval_seconds))
        if response is None:
            raise NotionSyncError(f"Notion API {method} {path} failed before sending a request.")
        if response.status_code >= 400:
            raise NotionSyncError(f"Notion API {method} {path} failed: HTTP {response.status_code} {response.text[:500]}")
        if not response.text:
            return {}
        return response.json()

    def create_database(self, parent_page_id: str) -> dict[str, Any]:
        return self.request(
            "POST",
            "/databases",
            {
                "parent": {"type": "page_id", "page_id": parent_page_id},
                "title": [{"type": "text", "text": {"content": DATABASE_TITLE}}],
                "properties": notion_database_properties(),
            },
        )

    def update_database_schema(self, database_id: str) -> None:
        self.request("PATCH", f"/databases/{database_id}", {"properties": notion_database_properties()})

    def query_existing_page(self, database_id: str, card_id: str, run_id: str) -> dict[str, Any] | None:
        payload = {
            "filter": {
                "and": [
                    {"property": "Card ID", "rich_text": {"equals": card_id}},
                    {"property": "Run ID", "rich_text": {"equals": run_id}},
                ]
            },
            "page_size": 1,
        }
        result = self.request("POST", f"/databases/{database_id}/query", payload)
        rows = result.get("results", []) if isinstance(result, dict) else []
        return rows[0] if rows else None

    def create_page(self, database_id: str, properties: dict[str, Any], children: list[dict[str, Any]]) -> dict[str, Any]:
        return self.request(
            "POST",
            "/pages",
            {"parent": {"database_id": database_id}, "properties": properties, "children": children[:APPEND_BLOCK_LIMIT]},
        )

    def update_page(self, page_id: str, properties: dict[str, Any]) -> dict[str, Any]:
        return self.request("PATCH", f"/pages/{page_id}", {"properties": properties})

    def list_block_children(self, block_id: str) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        cursor = ""
        while True:
            suffix = f"?page_size=100&start_cursor={cursor}" if cursor else "?page_size=100"
            result = self.request("GET", f"/blocks/{block_id}/children{suffix}")
            out.extend(result.get("results", []) if isinstance(result, dict) else [])
            if not result.get("has_more"):
                return out
            cursor = str(result.get("next_cursor") or "")

    def delete_block(self, block_id: str) -> None:
        self.request("DELETE", f"/blocks/{block_id}")

    def append_children(self, block_id: str, children: list[dict[str, Any]]) -> None:
        for i in range(0, len(children), APPEND_BLOCK_LIMIT):
            self.request("PATCH", f"/blocks/{block_id}/children", {"children": children[i : i + APPEND_BLOCK_LIMIT]})


def notion_database_properties() -> dict[str, Any]:
    return {
        "Name": {"title": {}},
        "Card ID": {"rich_text": {}},
        "Canonical Name": {"rich_text": {}},
        "Entity Type": {"select": {"options": []}},
        "Draft Status": {"select": {"options": []}},
        "Review Status": {"select": {"options": []}},
        "Run ID": {"rich_text": {}},
        "Claim Count": {"number": {"format": "number"}},
        "Evidence Count": {"number": {"format": "number"}},
        "Word Count": {"number": {"format": "number"}},
        "Last Synced": {"date": {}},
        "Local Artifact Path": {"rich_text": {}},
    }


def _select_property(value: Any) -> dict[str, Any]:
    name = re.sub(r"\s+", " ", str(value or "unknown")).strip()[:100] or "unknown"
    return {"select": {"name": name}}


def _rich_property(value: Any) -> dict[str, Any]:
    return {"rich_text": _rich_text_chunks(value)[:1]}


def page_properties_for_card(card: dict[str, Any], run_id: str, review_status: str, run_root: Path) -> dict[str, Any]:
    details = card.get("details") if isinstance(card.get("details"), dict) else {}
    return {
        "Name": {"title": _rich_text_chunks(card.get("canonical_name") or card.get("card_id", ""))[:1]},
        "Card ID": _rich_property(card.get("card_id", "")),
        "Canonical Name": _rich_property(card.get("canonical_name", "")),
        "Entity Type": _select_property(card.get("entity_type", "")),
        "Draft Status": _select_property(card.get("status", "draft")),
        "Review Status": _select_property(review_status),
        "Run ID": _rich_property(run_id),
        "Claim Count": {"number": len(details.get("accepted_claim_ids", []) or [])},
        "Evidence Count": {"number": len(card.get("source_evidence", []) or [])},
        "Word Count": {"number": card_word_count(card)},
        "Last Synced": {"date": {"start": now_utc_iso()}},
        "Local Artifact Path": _rich_property(str(run_root / "07_review" / "card_drafts.json")),
    }


def _state_database_id(config: dict[str, Any], parent_page_id: str, state_path: Path) -> str:
    explicit = normalize_notion_id(config.get("database_id", ""))
    if explicit:
        return explicit
    state = _read_json_or_default(state_path, {})
    state_parent = normalize_notion_id(state.get("draft_parent_page_id", "")) if isinstance(state, dict) else ""
    state_database = normalize_notion_id(state.get("draft_cards_database_id", "")) if isinstance(state, dict) else ""
    if parent_page_id and state_parent == parent_page_id and state_database:
        return state_database
    return ""


def ensure_draft_database(
    client: NotionDraftClient,
    config: dict[str, Any],
    state_path: Path = STATE_PATH,
) -> tuple[str, bool]:
    parent_page_id = str(config.get("parent_page_id", "") or "")
    database_id = _state_database_id(config, parent_page_id, state_path)
    if database_id:
        client.update_database_schema(database_id)
        return database_id, False
    if not parent_page_id:
        raise NotionSyncError("NOTION_DRAFT_PARENT_PAGE_ID is required to create the draft-card database.")
    created = client.create_database(parent_page_id)
    database_id = normalize_notion_id(created.get("id", ""))
    if not database_id:
        raise NotionSyncError("Notion database creation succeeded but returned no database id.")
    write_json(
        state_path,
        {
            "draft_cards_database_id": database_id,
            "draft_parent_page_id": parent_page_id,
            "created_at_utc": now_utc_iso(),
            "database_title": DATABASE_TITLE,
        },
    )
    return database_id, True


def replace_page_children(client: NotionDraftClient, page_id: str, children: list[dict[str, Any]]) -> None:
    for child in client.list_block_children(page_id):
        child_id = str(child.get("id", "")).strip()
        if child_id:
            client.delete_block(child_id)
    client.append_children(page_id, children)


def _run_id(run_root: Path) -> str:
    return run_root.name or str(run_root.resolve())


def sync_draft_cards_to_notion(
    run_root: Path,
    config_path: Path | None = Path("config/pipeline_config.json"),
    env_path: Path | None = Path(".env"),
    *,
    client: NotionDraftClient | None = None,
    state_path: Path = STATE_PATH,
    progress_callback: Callable[[str], None] | None = None,
) -> dict[str, Any]:
    logger = get_logger(__name__)
    out_report = run_root / REPORT_PATH

    def log(message: str) -> None:
        logger.info(message)
        if progress_callback:
            progress_callback(message)

    card_drafts_path = run_root / "07_review" / "card_drafts.json"
    decisions_path = run_root / "07_review" / "card_review_decisions.json"
    payload = _read_json_or_default(card_drafts_path, {"cards": []})
    cards = [card for card in payload.get("cards", []) if isinstance(card, dict)] if isinstance(payload, dict) else []
    report: dict[str, Any] = {
        "status": "skipped",
        "reason": "",
        "run_root": str(run_root),
        "run_id": _run_id(run_root),
        "card_count": len(cards),
        "created_pages": 0,
        "updated_pages": 0,
        "failed_pages": [],
        "pages": [],
        "database_id": "",
        "database_created": False,
        "created_at_utc": now_utc_iso(),
    }
    if not cards:
        report["reason"] = "No draft cards found at 07_review/card_drafts.json."
        write_json(out_report, report)
        return report

    config = notion_draft_config(config_path, env_path)
    if not config.get("enabled", True):
        report["reason"] = "Notion draft sync is disabled in config."
        write_json(out_report, report)
        return report
    if not config.get("api_key"):
        report["reason"] = "Missing NOTION_API_KEY."
        write_json(out_report, report)
        return report
    if not config.get("parent_page_id") and not config.get("database_id") and not _state_database_id(config, "", state_path):
        report["reason"] = "Missing NOTION_DRAFT_PARENT_PAGE_ID or NOTION_DRAFT_CARDS_DATABASE_ID."
        write_json(out_report, report)
        return report

    notion = client or NotionDraftClient(str(config["api_key"]))
    try:
        database_id, database_created = ensure_draft_database(notion, config, state_path)
        report["database_id"] = database_id
        report["database_created"] = database_created
    except Exception as exc:
        report["status"] = "failed"
        report["reason"] = str(exc)
        write_json(out_report, report)
        return report

    decisions = _latest_card_decisions(decisions_path)
    run_id = _run_id(run_root)
    for index, card in enumerate(cards, 1):
        card_id = str(card.get("card_id", "")).strip()
        if not card_id:
            report["failed_pages"].append({"card_id": "", "error": "missing card_id"})
            continue
        review_status = review_status_for_card(card, decisions)
        properties = page_properties_for_card(card, run_id, review_status, run_root)
        blocks = render_card_blocks(card, run_id, review_status)
        try:
            existing = notion.query_existing_page(database_id, card_id, run_id)
            if existing:
                page_id = str(existing.get("id", ""))
                updated = notion.update_page(page_id, properties)
                replace_page_children(notion, page_id, blocks)
                action = "updated"
                report["updated_pages"] += 1
                url = updated.get("url") or existing.get("url", "")
            else:
                created = notion.create_page(database_id, properties, blocks)
                page_id = str(created.get("id", ""))
                remaining = blocks[APPEND_BLOCK_LIMIT:]
                if remaining and page_id:
                    notion.append_children(page_id, remaining)
                action = "created"
                report["created_pages"] += 1
                url = created.get("url", "")
            report["pages"].append(
                {
                    "card_id": card_id,
                    "canonical_name": card.get("canonical_name", ""),
                    "page_id": page_id,
                    "url": url,
                    "action": action,
                }
            )
            log(f"Notion draft sync {index}/{len(cards)} {action}: {card.get('canonical_name') or card_id}")
        except Exception as exc:
            report["failed_pages"].append({"card_id": card_id, "canonical_name": card.get("canonical_name", ""), "error": str(exc)})
            log(f"Notion draft sync failed for {card.get('canonical_name') or card_id}: {exc}")

    report["status"] = "complete" if not report["failed_pages"] else "partial"
    report["reason"] = ""
    write_json(out_report, report)
    return report
