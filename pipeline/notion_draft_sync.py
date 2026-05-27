from __future__ import annotations

import json
import os
import re
import time
from pathlib import Path
from typing import Any, Callable

import requests

from pipeline.artifact_paths import ArtifactPaths, migrate_run_artifacts_to_numbered
from pipeline.common import get_logger, now_utc_iso, read_json, write_json
from pipeline.entity_resolution import normalized_name_key
from pipeline.ui_review_app import card_review_sections


NOTION_VERSION = "2022-06-28"
NOTION_API_BASE = "https://api.notion.com/v1"
RICH_TEXT_LIMIT = 1900
APPEND_BLOCK_LIMIT = 100
STATE_PATH = Path("artifacts/learning/notion_cards_state.json")
LEGACY_STATE_PATH = Path("artifacts/learning/notion_draft_cards_state.json")
DRAFT_DATABASE_TITLE = "Theriac Draft Lore Cards"
CANONICAL_DATABASE_TITLE = "Theriac Canon Lore Cards"
NotionSyncTarget = str  # "draft" | "canonical"


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


def _target_enabled(notion_config: dict[str, Any], target: str) -> bool:
    if target == "canonical":
        return bool(notion_config.get("canonical_sync_enabled", notion_config.get("final_sync_enabled", True)))
    return bool(notion_config.get("draft_sync_enabled", True))


def notion_sync_config(
    config_path: Path | None = Path("config/pipeline_config.json"),
    env_path: Path | None = Path(".env"),
    *,
    target: str = "draft",
) -> dict[str, Any]:
    config = _read_json_or_default(config_path, {}) if config_path else {}
    notion_config = config.get("notion", {}) if isinstance(config.get("notion", {}), dict) else {}
    env_file = _read_env_file(env_path) if env_path else {}
    token = _env_value(env_file, "NOTION_API_KEY", "NOTION_ACCESS_TOKEN", "NOTION_TOKEN") or str(notion_config.get("api_key", "")).strip()
    if target == "canonical":
        parent_page_id = normalize_notion_id(
            _env_value(
                env_file,
                "NOTION_CANONICAL_PARENT_PAGE_ID",
                "NOTION_FINAL_PARENT_PAGE_ID",
                "NOTION_CANON_PARENT_PAGE_ID",
            )
            or notion_config.get("canonical_parent_page_id", "")
            or notion_config.get("final_parent_page_id", "")
        )
        database_id = normalize_notion_id(
            _env_value(
                env_file,
                "NOTION_CANONICAL_CARDS_DATABASE_ID",
                "NOTION_FINAL_CARDS_DATABASE_ID",
                "NOTION_CANON_DATABASE_ID",
            )
            or notion_config.get("canonical_cards_database_id", "")
            or notion_config.get("final_cards_database_id", "")
        )
    else:
        parent_page_id = normalize_notion_id(
            _env_value(env_file, "NOTION_DRAFT_PARENT_PAGE_ID", "NOTION_PAGE_ID", "NOTION_PARENT_PAGE_ID")
            or notion_config.get("draft_parent_page_id", "")
        )
        database_id = normalize_notion_id(
            _env_value(env_file, "NOTION_DRAFT_CARDS_DATABASE_ID", "NOTION_DATABASE_ID", "NOTION_DRAFT_DATABASE_ID")
            or notion_config.get("draft_cards_database_id", "")
        )
    return {
        "target": target,
        "enabled": _target_enabled(notion_config, target),
        "api_key": token,
        "parent_page_id": parent_page_id,
        "database_id": database_id,
        "database_title": CANONICAL_DATABASE_TITLE if target == "canonical" else DRAFT_DATABASE_TITLE,
    }


def notion_draft_config(config_path: Path | None = Path("config/pipeline_config.json"), env_path: Path | None = Path(".env")) -> dict[str, Any]:
    """Backward-compatible draft-only config shape."""
    config = notion_sync_config(config_path, env_path, target="draft")
    return {
        "enabled": config["enabled"],
        "api_key": config["api_key"],
        "parent_page_id": config["parent_page_id"],
        "database_id": config["database_id"],
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
    from pipeline.prose_alias_registry import sanitize_card_prose_whitespace

    clean = sanitize_card_prose_whitespace(str(text or ""))
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


def work_review_sections(work_card: dict[str, Any]) -> list[dict[str, str]]:
    from pipeline.work_card_sections import work_review_section_order

    sections = work_card.get("sections") if isinstance(work_card.get("sections"), dict) else {}
    blocks: list[dict[str, str]] = []
    for key, title in work_review_section_order():
        text = str(sections.get(key, "")).strip()
        if text:
            blocks.append({"key": key, "title": title, "text": text})
    return blocks


def render_work_card_blocks(work_card: dict[str, Any], run_id: str) -> list[dict[str, Any]]:
    title = str(work_card.get("title") or work_card.get("work_id") or "Narrative Work")
    blocks: list[dict[str, Any]] = [_block("heading_1", title)]
    blocks.extend([_block("heading_2", "Summary"), *_paragraph_blocks(work_card.get("summary", ""))])
    for section in work_review_sections(work_card):
        blocks.append(_block("heading_2", section.get("title", "")))
        blocks.extend(_paragraph_blocks(section.get("text", "")))
    blocks.append(_bullet(f"Run: {run_id}"))
    blocks.append(_bullet(f"Work ID: {work_card.get('work_id', '')}"))
    return blocks


def sync_work_cards_to_notion(
    run_root: Path,
    config_path: Path | None = Path("config/pipeline_config.json"),
    env_path: Path | None = Path(".env"),
    *,
    progress_callback: Callable[[str], None] | None = None,
) -> dict[str, Any]:
    from pipeline.narrative_works import work_cards_path

    paths = ArtifactPaths(run_root)
    work_path = work_cards_path(run_root)
    report: dict[str, Any] = {
        "status": "skipped",
        "reason": "no_work_cards",
        "created_pages": 0,
        "updated_pages": 0,
        "failed_pages": [],
        "pages": [],
    }
    if not work_path.exists():
        return report
    payload = read_json(work_path)
    works = [row for row in payload.get("works", []) or [] if isinstance(row, dict)]
    if not works:
        return report
    notion_config = notion_sync_config(config_path, env_path, target="draft")
    if not notion_config.get("enabled") or not notion_config.get("api_key"):
        report["reason"] = "notion_disabled"
        return report
    log = progress_callback or (lambda _message: None)
    notion = NotionDraftClient(str(notion_config["api_key"]))
    database_id, _database_created = ensure_cards_database(notion, notion_config, target="draft", state_path=STATE_PATH)
    run_id = _run_id(run_root)
    for index, work_card in enumerate(works, start=1):
        work_id = str(work_card.get("work_id", "")).strip()
        page_card_id = f"work_{work_id}"
        pseudo_card = {
            "card_id": page_card_id,
            "canonical_name": work_card.get("title", work_id),
            "entity_type": "narrative_work",
            "status": work_card.get("status", "draft"),
            "summary": work_card.get("summary", ""),
            "details": {"sections": work_card.get("sections", {})},
        }
        properties = card_page_properties(pseudo_card, run_id, "pending review", run_root, target="draft")
        blocks = render_work_card_blocks(work_card, run_id)
        try:
            existing = notion.query_existing_page(database_id, page_card_id, run_id, match_run_id=True)
            if existing:
                page_id = str(existing.get("id", ""))
                notion.update_page(page_id, properties)
                replace_page_children(notion, page_id, blocks)
                report["updated_pages"] += 1
            else:
                created = notion.create_page(database_id, properties, blocks)
                page_id = str(created.get("id", ""))
                report["created_pages"] += 1
            report["pages"].append({"work_id": work_id, "page_id": page_id})
            log(f"Notion work sync {index}/{len(works)}: {work_card.get('title', work_id)}")
        except Exception as exc:
            report["failed_pages"].append({"work_id": work_id, "error": str(exc)})
    report["status"] = "complete" if not report["failed_pages"] else "partial"
    report["reason"] = ""
    return report


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


def is_public_facing_card(card: dict[str, Any], *, target: str) -> bool:
    """Approved/canonical lore pages are reader-facing and omit pipeline review chrome."""
    if target == "canonical":
        return True
    return str(card.get("status", "")).strip().lower() == "canonical"


def render_card_blocks(
    card: dict[str, Any],
    run_id: str,
    review_status: str,
    *,
    preview_mode: bool = True,
    public_article: bool = False,
) -> list[dict[str, Any]]:
    details = card.get("details") if isinstance(card.get("details"), dict) else {}
    title = str(card.get("canonical_name") or card.get("card_id") or "Lore Card")
    blocks: list[dict[str, Any]] = [_block("heading_1", title)]
    if preview_mode and not public_article:
        blocks.extend(
            [
                _block(
                    "paragraph",
                    "Draft preview only. Approve or reject this card in the Theriac desktop app; Notion is not the source of truth.",
                ),
                _block("heading_2", "Review Metadata"),
                _bullet(f"Run: {run_id}"),
                _bullet(f"Card ID: {card.get('card_id', '')}"),
                _bullet(f"Review status: {review_status}"),
                _bullet(f"Entity type: {card.get('entity_type', '')}"),
                _bullet(f"Accepted claims: {len(details.get('accepted_claim_ids', []) or [])}"),
                _bullet(f"Source evidence items: {len(card.get('source_evidence', []) or [])}"),
            ]
        )
    blocks.extend([_block("heading_2", "Summary"), *_paragraph_blocks(card.get("summary", ""))])

    for section in card_review_sections(card):
        blocks.append(_block("heading_2", section.get("title", "")))
        blocks.extend(_paragraph_blocks(section.get("text", "")))

    if public_article:
        wiki_links = details.get("wiki_links", []) if isinstance(details.get("wiki_links", []), list) else []
        if wiki_links:
            blocks.append(_block("heading_2", "See Also"))
            for link in wiki_links[:60]:
                if isinstance(link, dict):
                    target_name = link.get("target_entity_name") or link.get("target_card_id", "")
                    relation = link.get("relation_type", "related")
                    blocks.append(_bullet(f"{target_name} ({relation})"))
        return blocks

    relationships = card.get("relationships", []) if isinstance(card.get("relationships", []), list) else []
    if relationships:
        blocks.append(_block("heading_2", "Structured Relationships"))
        for rel in relationships[:40]:
            if isinstance(rel, dict):
                blocks.append(_bullet(f"{rel.get('relation_type', 'related')} -> {rel.get('target_card_id', '')}: {rel.get('note', '')}"))

    timeline = card.get("timeline", []) if isinstance(card.get("timeline", []), list) else []
    if timeline and not public_article:
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

    def create_database(self, parent_page_id: str, *, title: str = DRAFT_DATABASE_TITLE) -> dict[str, Any]:
        return self.request(
            "POST",
            "/databases",
            {
                "parent": {"type": "page_id", "page_id": parent_page_id},
                "title": [{"type": "text", "text": {"content": title[:100]}}],
                "properties": notion_database_properties(),
            },
        )

    def update_database_schema(self, database_id: str) -> None:
        self.request("PATCH", f"/databases/{database_id}", {"properties": notion_database_properties()})

    def retrieve_database(self, database_id: str) -> dict[str, Any]:
        return self.request("GET", f"/databases/{database_id}")

    def count_database_pages(self, database_id: str) -> int:
        result = self.request("POST", f"/databases/{database_id}/query", {"page_size": 1})
        rows = result.get("results", []) if isinstance(result, dict) else []
        if rows:
            return max(len(rows), 1)
        return 0

    def find_databases_on_parent(self, parent_page_id: str, database_title: str) -> list[str]:
        target_key = _normalize_database_title_key(database_title)
        matches: list[str] = []
        for child in self.list_block_children(parent_page_id):
            if str(child.get("type", "")).strip() != "child_database":
                continue
            database_id = normalize_notion_id(child.get("id", ""))
            if not database_id:
                continue
            try:
                payload = self.retrieve_database(database_id)
            except NotionSyncError:
                continue
            if _database_title_key(payload) == target_key:
                matches.append(database_id)
        return matches

    def _query_all_pages(self, database_id: str, filter_payload: dict[str, Any]) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        cursor = ""
        while True:
            body: dict[str, Any] = {"filter": filter_payload, "page_size": 100}
            if cursor:
                body["start_cursor"] = cursor
            result = self.request("POST", f"/databases/{database_id}/query", body)
            rows.extend(result.get("results", []) if isinstance(result, dict) else [])
            if not result.get("has_more"):
                return rows
            cursor = str(result.get("next_cursor") or "")

    def query_existing_page(
        self,
        database_id: str,
        card_id: str,
        run_id: str = "",
        *,
        match_run_id: bool = True,
    ) -> dict[str, Any] | None:
        page, _duplicates = self.find_page_for_card_sync(
            database_id,
            {"card_id": card_id, "canonical_name": ""},
            run_id,
            match_run_id=match_run_id,
        )
        return page

    def find_page_for_card_sync(
        self,
        database_id: str,
        card: dict[str, Any],
        run_id: str = "",
        *,
        match_run_id: bool = False,
    ) -> tuple[dict[str, Any] | None, list[dict[str, Any]]]:
        """Locate an existing Notion page to update (newest wins); return duplicate pages to archive."""
        card_id = str(card.get("card_id", "")).strip()
        canonical = str(card.get("canonical_name", "")).strip()
        seen_ids: set[str] = set()
        candidates: list[dict[str, Any]] = []

        def add_rows(rows: list[dict[str, Any]]) -> None:
            for row in rows:
                page_id = str(row.get("id", "")).strip()
                if page_id and page_id not in seen_ids:
                    seen_ids.add(page_id)
                    candidates.append(row)

        if card_id:
            id_filters: list[dict[str, Any]] = [{"property": "Card ID", "rich_text": {"equals": card_id}}]
            if match_run_id and run_id:
                id_filters.append({"property": "Run ID", "rich_text": {"equals": run_id}})
            id_filter: dict[str, Any] = id_filters[0] if len(id_filters) == 1 else {"and": id_filters}
            add_rows(self._query_all_pages(database_id, id_filter))

        if canonical:
            add_rows(
                self._query_all_pages(
                    database_id,
                    {"property": "Canonical Name", "rich_text": {"equals": canonical}},
                )
            )
            add_rows(
                self._query_all_pages(
                    database_id,
                    {"property": "Name", "title": {"equals": canonical}},
                )
            )

        if not candidates:
            return None, []
        best = max(candidates, key=lambda row: str(row.get("last_edited_time", "")))
        best_id = str(best.get("id", "")).strip()
        duplicates = [row for row in candidates if str(row.get("id", "")).strip() != best_id]
        return best, duplicates

    def archive_page(self, page_id: str) -> None:
        self.request("PATCH", f"/pages/{normalize_notion_id(page_id)}", {"archived": True})

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


def page_properties_for_card(
    card: dict[str, Any],
    run_id: str,
    review_status: str,
    run_root: Path,
    *,
    target: str = "draft",
    public_article: bool = False,
) -> dict[str, Any]:
    details = card.get("details") if isinstance(card.get("details"), dict) else {}
    paths = ArtifactPaths(run_root)
    artifact_path = paths.canonical_cards if target == "canonical" else paths.card_drafts
    properties: dict[str, Any] = {
        "Name": {"title": _rich_text_chunks(card.get("canonical_name") or card.get("card_id", ""))[:1]},
        "Canonical Name": _rich_property(card.get("canonical_name", "")),
        "Entity Type": _select_property(card.get("entity_type", "")),
        "Draft Status": _select_property(card.get("status", "draft")),
        "Word Count": {"number": card_word_count(card)},
        "Last Synced": {"date": {"start": now_utc_iso()}},
    }
    if not public_article:
        properties.update(
            {
                "Card ID": _rich_property(card.get("card_id", "")),
                "Review Status": _select_property(review_status),
                "Run ID": _rich_property(run_id),
                "Claim Count": {"number": len(details.get("accepted_claim_ids", []) or [])},
                "Evidence Count": {"number": len(card.get("source_evidence", []) or [])},
                "Local Artifact Path": _rich_property(str(artifact_path)),
            }
        )
    return properties


def _normalize_database_title_key(title: str) -> str:
    return re.sub(r"\s+", " ", str(title or "")).strip().casefold()


def _database_title_key(database_payload: dict[str, Any]) -> str:
    title = database_payload.get("title", [])
    if not isinstance(title, list):
        return ""
    parts: list[str] = []
    for item in title:
        if not isinstance(item, dict):
            continue
        plain = item.get("plain_text")
        if plain:
            parts.append(str(plain))
            continue
        text = item.get("text", {}) if isinstance(item.get("text"), dict) else {}
        content = text.get("content")
        if content:
            parts.append(str(content))
    return _normalize_database_title_key(" ".join(parts))


def _choose_database_id(client: NotionDraftClient, database_ids: list[str]) -> str:
    if not database_ids:
        return ""
    if len(database_ids) == 1:
        return database_ids[0]
    scored: list[tuple[str, str, int]] = []
    for database_id in database_ids:
        payload = client.retrieve_database(database_id)
        scored.append(
            (
                database_id,
                str(payload.get("created_time", "")),
                client.count_database_pages(database_id),
            )
        )
    # Prefer the oldest database (original) when duplicate titles exist under one parent page.
    scored.sort(key=lambda row: (row[1], -row[2], row[0]))
    return scored[0][0]


def _load_notion_state(state_path: Path) -> dict[str, Any]:
    state = _read_json_or_default(state_path, {})
    if isinstance(state, dict) and state:
        return state
    legacy = _read_json_or_default(LEGACY_STATE_PATH, {})
    return legacy if isinstance(legacy, dict) else {}


def _state_keys_for_target(target: str) -> tuple[str, str, str]:
    if target == "canonical":
        return ("canonical_parent_page_id", "canonical_cards_database_id", "canonical_database_title")
    return ("draft_parent_page_id", "draft_cards_database_id", "draft_database_title")


def _config_database_id(config: dict[str, Any]) -> str:
    return normalize_notion_id(config.get("database_id", ""))


def _state_database_id(parent_page_id: str, state_path: Path, *, target: str) -> str:
    state = _load_notion_state(state_path)
    parent_key, database_key, _ = _state_keys_for_target(target)
    state_parent = normalize_notion_id(state.get(parent_key, "")) if isinstance(state, dict) else ""
    state_database = normalize_notion_id(state.get(database_key, "")) if isinstance(state, dict) else ""
    if parent_page_id and state_parent == parent_page_id and state_database:
        return state_database
    return ""


def _persist_database_state(state_path: Path, *, target: str, parent_page_id: str, database_id: str, database_title: str) -> None:
    state = _load_notion_state(state_path)
    parent_key, database_key, title_key = _state_keys_for_target(target)
    state[parent_key] = parent_page_id
    state[database_key] = database_id
    state[title_key] = database_title
    state["updated_at_utc"] = now_utc_iso()
    if "created_at_utc" not in state:
        state["created_at_utc"] = now_utc_iso()
    write_json(state_path, state)


def ensure_cards_database(
    client: NotionDraftClient,
    config: dict[str, Any],
    *,
    target: str = "draft",
    state_path: Path = STATE_PATH,
) -> tuple[str, bool]:
    logger = get_logger(__name__)
    parent_page_id = str(config.get("parent_page_id", "") or "")
    database_title = str(config.get("database_title", DRAFT_DATABASE_TITLE))
    if target == "canonical":
        default_title = CANONICAL_DATABASE_TITLE
    else:
        default_title = DRAFT_DATABASE_TITLE
    if not database_title:
        database_title = default_title

    database_id = _config_database_id(config)
    if database_id:
        client.update_database_schema(database_id)
        _persist_database_state(
            state_path,
            target=target,
            parent_page_id=parent_page_id,
            database_id=database_id,
            database_title=database_title,
        )
        return database_id, False

    discovered: list[str] = []
    if parent_page_id:
        discovered = client.find_databases_on_parent(parent_page_id, database_title)
        if len(discovered) > 1:
            logger.warning(
                "Notion %s sync found %d databases titled %r under parent %s; reusing the established one (%s). "
                "Set NOTION_%s_CARDS_DATABASE_ID to pin a specific database.",
                target,
                len(discovered),
                database_title,
                parent_page_id,
                _choose_database_id(client, discovered),
                "DRAFT" if target == "draft" else "CANONICAL",
            )
        if discovered:
            database_id = _choose_database_id(client, discovered)
            client.update_database_schema(database_id)
            _persist_database_state(
                state_path,
                target=target,
                parent_page_id=parent_page_id,
                database_id=database_id,
                database_title=database_title,
            )
            logger.info(
                "Notion %s sync reusing existing database %s (%r) on parent %s",
                target,
                database_id,
                database_title,
                parent_page_id,
            )
            return database_id, False

    state_database_id = _state_database_id(parent_page_id, state_path, target=target)
    if state_database_id:
        client.update_database_schema(state_database_id)
        return state_database_id, False

    if not parent_page_id:
        env_name = "NOTION_CANONICAL_PARENT_PAGE_ID" if target == "canonical" else "NOTION_DRAFT_PARENT_PAGE_ID"
        raise NotionSyncError(f"{env_name} is required to create the {target} card database.")
    created = client.create_database(parent_page_id, title=database_title)
    database_id = normalize_notion_id(created.get("id", ""))
    if not database_id:
        raise NotionSyncError("Notion database creation succeeded but returned no database id.")
    _persist_database_state(state_path, target=target, parent_page_id=parent_page_id, database_id=database_id, database_title=database_title)
    logger.info("Notion %s sync created database %s (%r) on parent %s", target, database_id, database_title, parent_page_id)
    return database_id, True


def ensure_draft_database(
    client: NotionDraftClient,
    config: dict[str, Any],
    state_path: Path = STATE_PATH,
) -> tuple[str, bool]:
    return ensure_cards_database(client, config, target="draft", state_path=state_path)


def replace_page_children(client: NotionDraftClient, page_id: str, children: list[dict[str, Any]]) -> None:
    for child in client.list_block_children(page_id):
        child_id = str(child.get("id", "")).strip()
        if child_id:
            client.delete_block(child_id)
    client.append_children(page_id, children)


def _run_id(run_root: Path) -> str:
    return run_root.name or str(run_root.resolve())


def _canonical_cards_by_id(paths: ArtifactPaths) -> dict[str, dict[str, Any]]:
    payload = _read_json_or_default(paths.canonical_cards, {"cards": []})
    cards = payload.get("cards", []) if isinstance(payload, dict) else []
    out: dict[str, dict[str, Any]] = {}
    for card in cards:
        if not isinstance(card, dict):
            continue
        if str(card.get("status", "")).strip().lower() != "canonical":
            continue
        card_id = str(card.get("card_id", "")).strip()
        if card_id:
            out[card_id] = card
    return out


def _cards_for_target(paths: ArtifactPaths, target: str) -> tuple[Path, list[dict[str, Any]], str]:
    if target == "canonical":
        source_path = paths.canonical_cards
        canonical_by_id = _canonical_cards_by_id(paths)
        cards = list(canonical_by_id.values())
        empty_reason = "No canonical cards found at 11_card_synthesis/canonical_cards.json."
        return source_path, cards, empty_reason

    source_path = paths.card_drafts
    draft_payload = _read_json_or_default(source_path, {"cards": []})
    draft_cards = [card for card in draft_payload.get("cards", []) if isinstance(card, dict)] if isinstance(draft_payload, dict) else []
    canonical_by_id = _canonical_cards_by_id(paths)
    merged: list[dict[str, Any]] = []
    seen: set[str] = set()
    for card in draft_cards:
        card_id = str(card.get("card_id", "")).strip()
        if not card_id:
            continue
        seen.add(card_id)
        merged.append(canonical_by_id.get(card_id, card))
    for card_id, card in canonical_by_id.items():
        if card_id not in seen:
            merged.append(card)
    cards = merged
    empty_reason = "No draft cards found at 11_card_synthesis/card_drafts.json."
    return source_path, cards, empty_reason


def sync_cards_to_notion(
    run_root: Path,
    config_path: Path | None = Path("config/pipeline_config.json"),
    env_path: Path | None = Path(".env"),
    *,
    target: str = "draft",
    client: NotionDraftClient | None = None,
    state_path: Path = STATE_PATH,
    progress_callback: Callable[[str], None] | None = None,
) -> dict[str, Any]:
    logger = get_logger(__name__)
    migrate_run_artifacts_to_numbered(run_root)
    paths = ArtifactPaths(run_root)
    out_report = paths.notion_canonical_sync_report if target == "canonical" else paths.notion_draft_sync_report
    # Match existing pages by card_id and canonical name (not run_id) so re-synthesis updates in place.
    match_run_id_on_lookup = False
    label = "canonical" if target == "canonical" else "draft"

    def log(message: str) -> None:
        logger.info(message)
        if progress_callback:
            progress_callback(message)

    _, cards, empty_reason = _cards_for_target(paths, target)
    report: dict[str, Any] = {
        "status": "skipped",
        "reason": "",
        "target": target,
        "run_root": str(run_root),
        "run_id": _run_id(run_root),
        "card_count": len(cards),
        "created_pages": 0,
        "updated_pages": 0,
        "failed_pages": [],
        "archived_pages": [],
        "pages": [],
        "database_id": "",
        "database_created": False,
        "parent_page_id": "",
        "created_at_utc": now_utc_iso(),
    }
    if not cards:
        report["reason"] = empty_reason
        write_json(out_report, report)
        return report

    config = notion_sync_config(config_path, env_path, target=target)
    report["parent_page_id"] = config.get("parent_page_id", "")
    if not config.get("enabled", True):
        report["reason"] = f"Notion {label} sync is disabled in config."
        write_json(out_report, report)
        return report
    if not config.get("api_key"):
        report["reason"] = "Missing NOTION_API_KEY."
        write_json(out_report, report)
        return report
    if not config.get("parent_page_id") and not _config_database_id(config) and not _state_database_id("", state_path, target=target):
        missing = "NOTION_CANONICAL_PARENT_PAGE_ID" if target == "canonical" else "NOTION_DRAFT_PARENT_PAGE_ID"
        report["reason"] = f"Missing {missing} or matching database id env var."
        write_json(out_report, report)
        return report

    notion = client or NotionDraftClient(str(config["api_key"]))
    try:
        database_id, database_created = ensure_cards_database(notion, config, target=target, state_path=state_path)
        report["database_id"] = database_id
        report["database_created"] = database_created
    except Exception as exc:
        report["status"] = "failed"
        report["reason"] = str(exc)
        write_json(out_report, report)
        return report

    decisions = _latest_card_decisions(paths.card_review_decisions)
    run_id = _run_id(run_root)
    for index, card in enumerate(cards, 1):
        card_id = str(card.get("card_id", "")).strip()
        if not card_id:
            report["failed_pages"].append({"card_id": "", "error": "missing card_id"})
            continue
        public_article = is_public_facing_card(card, target=target)
        review_status = "canonical" if public_article else review_status_for_card(card, decisions)
        properties = page_properties_for_card(
            card,
            run_id,
            review_status,
            run_root,
            target=target,
            public_article=public_article,
        )
        blocks = render_card_blocks(
            card,
            run_id,
            review_status,
            preview_mode=not public_article,
            public_article=public_article,
        )
        try:
            existing, duplicates = notion.find_page_for_card_sync(
                database_id,
                card,
                run_id,
                match_run_id=match_run_id_on_lookup,
            )
            if existing:
                page_id = str(existing.get("id", ""))
                updated = notion.update_page(page_id, properties)
                replace_page_children(notion, page_id, blocks)
                action = "updated"
                report["updated_pages"] += 1
                url = updated.get("url") or existing.get("url", "")
                for dup in duplicates:
                    dup_id = str(dup.get("id", "")).strip()
                    if not dup_id:
                        continue
                    try:
                        notion.archive_page(dup_id)
                        report["archived_pages"].append(
                            {
                                "page_id": dup_id,
                                "canonical_name": card.get("canonical_name", ""),
                                "card_id": card_id,
                            }
                        )
                    except Exception as archive_exc:
                        report["failed_pages"].append(
                            {
                                "card_id": card_id,
                                "canonical_name": card.get("canonical_name", ""),
                                "error": f"archive duplicate failed: {archive_exc}",
                            }
                        )
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
            log(f"Notion {label} sync {index}/{len(cards)} {action}: {card.get('canonical_name') or card_id}")
        except Exception as exc:
            report["failed_pages"].append({"card_id": card_id, "canonical_name": card.get("canonical_name", ""), "error": str(exc)})
            log(f"Notion {label} sync failed for {card.get('canonical_name') or card_id}: {exc}")

    report["status"] = "complete" if not report["failed_pages"] else "partial"
    report["reason"] = ""
    write_json(out_report, report)
    return report


def sync_draft_cards_to_notion(
    run_root: Path,
    config_path: Path | None = Path("config/pipeline_config.json"),
    env_path: Path | None = Path(".env"),
    *,
    client: NotionDraftClient | None = None,
    state_path: Path = STATE_PATH,
    progress_callback: Callable[[str], None] | None = None,
) -> dict[str, Any]:
    return sync_cards_to_notion(
        run_root,
        config_path,
        env_path,
        target="draft",
        client=client,
        state_path=state_path,
        progress_callback=progress_callback,
    )


_NOTION_IMPORT_SKIP_HEADINGS = frozenset(
    {
        "Review Metadata",
        "Structured Relationships",
        "Timeline",
        "Wiki Links",
        "Unresolved Conflicts",
        "See Also",
    }
)


def block_to_plain_text(block: dict[str, Any]) -> str:
    block_type = str(block.get("type", "")).strip()
    payload = block.get(block_type)
    if not isinstance(payload, dict):
        return ""
    rich = payload.get("rich_text", [])
    if not isinstance(rich, list):
        return ""
    parts: list[str] = []
    for item in rich:
        if not isinstance(item, dict):
            continue
        text_obj = item.get("text")
        if isinstance(text_obj, dict) and text_obj.get("content") is not None:
            parts.append(str(text_obj.get("content", "")))
        elif isinstance(item.get("plain_text"), str):
            parts.append(item["plain_text"])
    return "".join(parts).strip()


def _section_title_to_key(title: str, config: dict[str, Any] | None = None) -> str:
    from pipeline.card_sections import card_review_section_order

    clean = str(title or "").strip()
    for key, display in card_review_section_order(config):
        if display.strip().lower() == clean.lower():
            return key
    slug = re.sub(r"[^a-z0-9]+", "_", clean.lower()).strip("_")
    return slug or "section"


def parse_card_page_blocks(
    blocks: list[dict[str, Any]],
    *,
    config: dict[str, Any] | None = None,
) -> tuple[str, dict[str, str]]:
    """Extract summary + section prose from a Notion lore-card page (draft or canonical layout)."""
    summary_parts: list[str] = []
    sections: dict[str, list[str]] = {}
    current_section_key: str | None = None
    in_summary = False
    seen_summary_heading = False

    for block in blocks:
        block_type = str(block.get("type", "")).strip()
        text = block_to_plain_text(block)
        if block_type == "heading_2":
            if text.strip().lower() == "summary":
                in_summary = True
                seen_summary_heading = True
                current_section_key = None
                continue
            in_summary = False
            if text in _NOTION_IMPORT_SKIP_HEADINGS:
                current_section_key = None
                continue
            current_section_key = _section_title_to_key(text, config)
            sections.setdefault(current_section_key, [])
            continue
        if block_type != "paragraph":
            continue
        if not text:
            continue
        if in_summary or (not seen_summary_heading and not sections and not summary_parts):
            summary_parts.append(text)
            continue
        if current_section_key:
            sections[current_section_key].append(text)

    summary = "\n\n".join(summary_parts).strip()
    section_out = {key: "\n\n".join(parts).strip() for key, parts in sections.items() if "\n\n".join(parts).strip()}
    return summary, section_out


def apply_notion_prose_to_card(
    card: dict[str, Any],
    *,
    summary: str,
    sections: dict[str, str],
) -> dict[str, Any]:
    updated = dict(card)
    if summary:
        updated["summary"] = summary
    details = updated.get("details")
    if not isinstance(details, dict):
        details = {}
    existing_sections = details.get("sections")
    if not isinstance(existing_sections, dict):
        existing_sections = {}
    merged_sections = dict(existing_sections)
    merged_sections.update(sections)
    updated["details"] = {**details, "sections": merged_sections}
    return updated


def resolve_notion_pages_by_canonical_name(
    canonical_names: list[str],
    *,
    config_path: Path | None = Path("config/pipeline_config.json"),
    env_path: Path | None = Path(".env"),
    client: NotionDraftClient | None = None,
    target: str = "draft",
) -> list[dict[str, Any]]:
    """Find Notion database pages by Canonical Name property (newest edit wins if duplicates)."""
    notion_config = notion_sync_config(config_path, env_path, target=target)
    token = str(notion_config.get("api_key", "")).strip()
    database_id = str(notion_config.get("database_id", "")).strip()
    if not token or not database_id:
        raise NotionSyncError("Missing Notion token or draft cards database id.")
    notion = client or NotionDraftClient(token)
    refs: list[dict[str, Any]] = []
    for name in canonical_names:
        clean = str(name or "").strip()
        if not clean:
            continue
        result = notion.request(
            "POST",
            f"/databases/{database_id}/query",
            {
                "filter": {"property": "Canonical Name", "rich_text": {"equals": clean}},
                "page_size": 10,
            },
        )
        rows = result.get("results", []) if isinstance(result, dict) else []
        if not rows:
            continue
        best = max(rows, key=lambda row: str(row.get("last_edited_time", "")))
        page_id = normalize_notion_id(best.get("id", ""))
        props = best.get("properties", {}) if isinstance(best.get("properties"), dict) else {}
        card_rt = props.get("Card ID", {}).get("rich_text", [])
        card_id = ""
        if isinstance(card_rt, list) and card_rt and isinstance(card_rt[0], dict):
            card_id = str(card_rt[0].get("plain_text", "")).strip()
        refs.append(
            {
                "canonical_name": clean,
                "page_id": page_id,
                "card_id": card_id,
                "last_edited_time": str(best.get("last_edited_time", "")),
            }
        )
    return refs


def pull_cards_from_notion(
    run_root: Path,
    page_refs: list[dict[str, Any]],
    existing_cards: list[dict[str, Any]],
    *,
    config_path: Path | None = Path("config/pipeline_config.json"),
    env_path: Path | None = Path(".env"),
    client: NotionDraftClient | None = None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Pull latest page body prose from Notion into local card dicts (by card_id)."""
    config = _read_json_or_default(config_path, {}) if config_path else {}
    notion_config = notion_sync_config(config_path, env_path, target="draft")
    token = str(notion_config.get("api_key", "")).strip()
    if not token:
        raise NotionSyncError("Missing Notion API token for import.")
    notion = client or NotionDraftClient(token)
    by_id = {str(card.get("card_id", "")).strip(): dict(card) for card in existing_cards if str(card.get("card_id", "")).strip()}
    pulled: list[dict[str, str]] = []
    failures: list[dict[str, str]] = []

    for ref in page_refs:
        card_id = str(ref.get("card_id", "")).strip()
        page_id = normalize_notion_id(ref.get("page_id", ""))
        if not card_id or not page_id:
            continue
        base = by_id.get(card_id)
        if base is None:
            failures.append({"card_id": card_id, "error": "no local card for page"})
            continue
        try:
            blocks = notion.list_block_children(page_id)
            summary, sections = parse_card_page_blocks(blocks, config=config if isinstance(config, dict) else None)
            by_id[card_id] = apply_notion_prose_to_card(base, summary=summary, sections=sections)
            pulled.append({"card_id": card_id, "canonical_name": str(base.get("canonical_name", ""))})
        except NotionSyncError as exc:
            failures.append({"card_id": card_id, "error": str(exc)})

    report = {
        "status": "complete" if not failures else "partial",
        "pulled": pulled,
        "failures": failures,
        "card_count": len(pulled),
    }
    return list(by_id.values()), report


def sync_canonical_cards_to_notion(
    run_root: Path,
    config_path: Path | None = Path("config/pipeline_config.json"),
    env_path: Path | None = Path(".env"),
    *,
    client: NotionDraftClient | None = None,
    state_path: Path = STATE_PATH,
    progress_callback: Callable[[str], None] | None = None,
) -> dict[str, Any]:
    return sync_cards_to_notion(
        run_root,
        config_path,
        env_path,
        target="canonical",
        client=client,
        state_path=state_path,
        progress_callback=progress_callback,
    )


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description="Sync Theriac lore cards to Notion draft or canonical databases.")
    parser.add_argument("--artifacts-root", required=True, help="Run artifacts root, e.g. artifacts/runs/<run_id>")
    parser.add_argument(
        "--target",
        choices=("draft", "canonical", "both"),
        default="draft",
        help="draft=preview database under draft parent page; canonical=final cards under canonical parent page",
    )
    parser.add_argument("--config", default="config/pipeline_config.json")
    parser.add_argument("--env", default=".env")
    args = parser.parse_args()
    run_root = Path(args.artifacts_root)
    targets = ("draft", "canonical") if args.target == "both" else (args.target,)
    for target in targets:
        report = sync_cards_to_notion(run_root, Path(args.config), Path(args.env), target=target)
        print(json.dumps(report, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
