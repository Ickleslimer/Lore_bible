"""Build static Fandom-style HTML wiki preview sites from pipeline cards."""

from __future__ import annotations

import html
import json
import re
import shutil
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from pipeline.artifact_paths import ArtifactPaths, migrate_run_artifacts_to_numbered
from pipeline.branding import GAME_NAME
from pipeline.common import read_json, write_json
from pipeline.prose_alias_registry import sanitize_card_prose_whitespace
from pipeline.work_card_sections import WORK_SECTION_DISPLAY_TITLES, work_section_display_title

ASSETS_DIR = Path(__file__).resolve().parent / "wiki_site_assets"
WIKI_FAVICON_NAME = "icon.png"
WIKI_SITE_TITLE = f"{GAME_NAME} Wiki"


@dataclass
class WikiPageEntry:
    name: str
    slug: str
    path: str
    page_kind: str  # entity | work
    type_label: str
    excerpt: str
    status: str
    payload: dict[str, Any] = field(default_factory=dict)

    @property
    def is_work(self) -> bool:
        return self.page_kind == "work"


def slugify(value: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", str(value or "").strip().lower()).strip("-")
    return slug or "page"


def asset_prefix(depth: int) -> str:
    return "../" * depth if depth > 0 else "./"


def normalize_paragraphs(text: str) -> str:
    clean = sanitize_card_prose_whitespace(text)
    if not clean:
        return ""
    parts = [p.strip() for p in re.split(r"\n\s*\n", clean) if p.strip()]
    return "".join(f"<p>{html.escape(' '.join(part.split()))}</p>" for part in parts)


def excerpt_from_text(text: str, limit: int = 160) -> str:
    clean = " ".join(sanitize_card_prose_whitespace(text).split())
    if len(clean) <= limit:
        return clean
    return clean[: limit - 1].rsplit(" ", 1)[0] + "…"


def load_entity_cards(run_root: Path, *, prefer_canonical: bool) -> list[dict[str, Any]]:
    paths = ArtifactPaths(run_root)
    sources = [paths.canonical_cards, paths.card_drafts] if prefer_canonical else [paths.card_drafts, paths.canonical_cards]
    for path in sources:
        if not path.exists():
            continue
        payload = read_json(path)
        cards = [row for row in payload.get("cards", []) if isinstance(row, dict)]
        if cards:
            return cards
    return []


def load_work_cards(run_root: Path) -> list[dict[str, Any]]:
    path = ArtifactPaths(run_root).work_cards
    if not path.exists():
        return []
    payload = read_json(path)
    return [row for row in payload.get("works", []) if isinstance(row, dict)]


def entity_entry_from_card(card: dict[str, Any]) -> WikiPageEntry:
    name = str(card.get("canonical_name", "")).strip() or "Unknown"
    slug = slugify(name)
    summary = str(card.get("summary", "")).strip()
    return WikiPageEntry(
        name=name,
        slug=slug,
        path=f"pages/{slug}.html",
        page_kind="entity",
        type_label=str(card.get("entity_type", "") or "entity").strip(),
        excerpt=excerpt_from_text(summary),
        status=str(card.get("status", "draft")).strip(),
        payload=card,
    )


def work_entry_from_card(work: dict[str, Any]) -> WikiPageEntry:
    work_id = str(work.get("work_id", "")).strip() or "work"
    title = str(work.get("title", work_id)).strip()
    slug = slugify(work_id)
    summary = str(work.get("summary", "")).strip()
    return WikiPageEntry(
        name=title,
        slug=slug,
        path=f"works/{slug}.html",
        page_kind="work",
        type_label=str(work.get("kind", "work")).strip(),
        excerpt=excerpt_from_text(summary),
        status=str(work.get("status", "draft")).strip(),
        payload=work,
    )


def build_search_index(entries: list[WikiPageEntry]) -> list[dict[str, str]]:
    return [
        {
            "name": entry.name,
            "slug": entry.slug,
            "path": entry.path,
            "page_kind": entry.page_kind,
            "type": entry.type_label,
            "excerpt": entry.excerpt,
        }
        for entry in entries
    ]


def slug_by_name(entries: list[WikiPageEntry]) -> dict[str, str]:
    out: dict[str, str] = {}
    for entry in entries:
        out[entry.name.strip().lower()] = entry.slug
        if entry.is_work:
            work_id = str(entry.payload.get("work_id", "")).strip().lower()
            if work_id:
                out[work_id] = entry.slug
    return out


def build_href_by_name(
    entries: list[WikiPageEntry],
    *,
    config: dict[str, Any] | None = None,
) -> dict[str, str]:
    from pipeline.wiki_autolink import build_autolink_index, build_quest_autolink_targets, href_by_name

    index = build_autolink_index(
        entries,
        quest_targets=build_quest_autolink_targets(),
        config=config,
    )
    return href_by_name(index)


def _infobox_row(label: str, value_html: str) -> str:
    if not value_html.strip():
        return ""
    return (
        f'<tr><th scope="row">{html.escape(label)}</th>'
        f'<td>{value_html}</td></tr>'
    )


def _link_html(name: str, href_by_name: dict[str, str], depth: int) -> str:
    key = name.strip().lower()
    rel_path = href_by_name.get(key)
    if not rel_path:
        return html.escape(name)
    prefix = asset_prefix(depth)
    path = f"{prefix}{rel_path}"
    return f'<a href="{html.escape(path)}">{html.escape(name)}</a>'


def build_infobox_html(entry: WikiPageEntry, href_by_name: dict[str, str], *, depth: int = 1) -> str:
    title = html.escape(entry.name)
    type_label = html.escape(entry.type_label.replace("_", " ").title())
    status = html.escape(entry.status or "draft")
    image_label = html.escape(entry.type_label.replace("_", " ").title() or "Lore")

    rows: list[str] = []
    if entry.is_work:
        rows.append(_infobox_row("Kind", type_label))
        rows.append(_infobox_row("Status", status))
        work_id = str(entry.payload.get("work_id", "")).strip().lower()
        if work_id == "theriac_coda":
            prefix = asset_prefix(depth)
            map_href = f"{prefix}quests/theriac-coda-quest-map.html"
            rows.append(
                _infobox_row(
                    "Quest map",
                    f'<a href="{html.escape(map_href)}">Character quests (Path B)</a>',
                )
            )
        if entry.excerpt:
            rows.append(_infobox_row("Overview", html.escape(entry.excerpt)))
    else:
        card = entry.payload
        rows.append(_infobox_row("Type", type_label))
        rows.append(_infobox_row("Status", status))
        aliases = [str(a).strip() for a in card.get("aliases", []) or [] if str(a).strip()]
        if aliases:
            shown = aliases[:6]
            alias_text = ", ".join(html.escape(a) for a in shown)
            if len(aliases) > 6:
                alias_text += f' <span class="infobox-more">+{len(aliases) - 6} more</span>'
            rows.append(_infobox_row("Aliases", alias_text))
        if entry.excerpt:
            rows.append(_infobox_row("Overview", html.escape(entry.excerpt)))

        links: list[str] = []
        details = card.get("details") if isinstance(card.get("details"), dict) else {}
        for link in (details.get("wiki_links") or [])[:12]:
            if not isinstance(link, dict):
                continue
            target = str(link.get("target_entity_name") or link.get("target_card_id") or "").strip()
            rel = str(link.get("relation_type", "related")).strip()
            if target:
                links.append(f'<li><span class="rel">{html.escape(rel)}</span> {_link_html(target, href_by_name, depth)}</li>')
        for rel in (card.get("relationships") or [])[:8]:
            if not isinstance(rel, dict):
                continue
            target = str(rel.get("target_entity_name") or rel.get("target_card_id") or "").strip()
            rel_type = str(rel.get("relation_type", "related")).strip()
            if target:
                links.append(f'<li><span class="rel">{html.escape(rel_type)}</span> {_link_html(target, href_by_name, depth)}</li>')
        if links:
            rows.append(_infobox_row("Links", f'<ul class="infobox-links">{"".join(links)}</ul>'))

    body_rows = "".join(rows)
    return f"""
<aside class="wiki-infobox" aria-label="Page facts">
  <div class="infobox-image" aria-hidden="true">{image_label}</div>
  <h2 class="infobox-title">{title}</h2>
  <table class="infobox-table"><tbody>{body_rows}</tbody></table>
</aside>
"""


def render_header_html(*, depth: int, active: str = "") -> str:
    prefix = asset_prefix(depth)
    home_href = f"{prefix}index.html"
    return f"""
<header class="wiki-global-header">
  <a class="wiki-brand" href="{html.escape(home_href)}">
    <span class="wiki-brand-mark">T</span>
    <span class="wiki-brand-text">{html.escape(WIKI_SITE_TITLE)}</span>
  </a>
  <div class="wiki-search-wrap" data-wiki-search>
    <input type="search" class="wiki-search-input" placeholder="Search the wiki…" autocomplete="off"
      aria-label="Search wiki" aria-expanded="false" aria-controls="wiki-search-results" />
    <div id="wiki-search-results" class="wiki-search-results" role="listbox" hidden></div>
  </div>
</header>
"""


def _resolve_favicon_source() -> Path | None:
    repo_root = Path(__file__).resolve().parents[1]
    for candidate in (
        ASSETS_DIR / WIKI_FAVICON_NAME,
        repo_root / "desktop-tauri" / "public" / WIKI_FAVICON_NAME,
        repo_root / "desktop-tauri" / "src-tauri" / "icons" / "32x32.png",
    ):
        if candidate.exists():
            return candidate
    return None


def _page_shell(
    *,
    title: str,
    depth: int,
    body_main: str,
    body_class: str = "",
) -> str:
    prefix = asset_prefix(depth)
    favicon_href = f"{prefix}{WIKI_FAVICON_NAME}"
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{html.escape(title)} — {html.escape(WIKI_SITE_TITLE)}</title>
  <link rel="icon" type="image/png" href="{html.escape(favicon_href)}">
  <link rel="stylesheet" href="{prefix}wiki.css">
</head>
<body data-wiki-root="{html.escape(prefix)}" class="{body_class}">
{render_header_html(depth=depth)}
{body_main}
<script src="{prefix}wiki.js" defer></script>
</body>
</html>
"""


def _paragraphs_html(
    text: str,
    *,
    autolink_index: Any | None = None,
    current_slug: str = "",
    depth: int = 1,
    autolink_state: Any | None = None,
    section: str = "",
    page_slug: str = "",
) -> str:
    if autolink_index is not None:
        from pipeline.wiki_autolink import autolink_paragraphs

        return autolink_paragraphs(
            text,
            autolink_index,
            current_slug=current_slug,
            depth=depth,
            state=autolink_state,
            section=section,
            page_slug=page_slug,
        )
    return normalize_paragraphs(text)


def entity_sections_html(
    card: dict[str, Any],
    config_path: Path | None,
    *,
    autolink_index: Any | None = None,
    current_slug: str = "",
    depth: int = 1,
    autolink_state: Any | None = None,
    page_slug: str = "",
) -> str:
    from pipeline.ui_review_app import card_review_sections

    blocks: list[str] = []
    for section in card_review_sections(card):
        title = html.escape(str(section.get("title", "")))
        section_key = str(section.get("key", "")).strip() or title
        body = _paragraphs_html(
            str(section.get("text", "")),
            autolink_index=autolink_index,
            current_slug=current_slug,
            depth=depth,
            autolink_state=autolink_state,
            section=section_key,
            page_slug=page_slug,
        )
        if body:
            blocks.append(f'<section class="wiki-section"><h2>{title}</h2>{body}</section>')
    return "\n".join(blocks)


def work_sections_html(
    work: dict[str, Any],
    *,
    autolink_index: Any | None = None,
    current_slug: str = "",
    depth: int = 1,
    autolink_state: Any | None = None,
    page_slug: str = "",
) -> str:
    sections = work.get("sections") if isinstance(work.get("sections"), dict) else {}
    blocks: list[str] = []
    for key, display in WORK_SECTION_DISPLAY_TITLES.items():
        if key == "summary":
            continue
        text = str(sections.get(key, "")).strip()
        if not text:
            continue
        title = html.escape(work_section_display_title(key))
        body = _paragraphs_html(
            text,
            autolink_index=autolink_index,
            current_slug=current_slug,
            depth=depth,
            autolink_state=autolink_state,
            section=key,
            page_slug=page_slug,
        )
        if body:
            blocks.append(f'<section class="wiki-section"><h2>{title}</h2>{body}</section>')
    return "\n".join(blocks)


def render_article_html(
    entry: WikiPageEntry,
    *,
    sections_html: str,
    infobox_html: str,
    depth: int,
    autolink_index: Any | None = None,
    autolink_state: Any | None = None,
) -> str:
    title = html.escape(entry.name)
    lead = _paragraphs_html(
        str(entry.payload.get("summary", "")),
        autolink_index=autolink_index,
        current_slug=entry.slug,
        depth=depth,
        autolink_state=autolink_state,
        section="summary",
        page_slug=entry.slug,
    )
    main = f"""
<main class="wiki-page">
  <div class="wiki-page-grid">
    <article class="wiki-article">
      <h1 class="wiki-page-title">{title}</h1>
      <div class="wiki-lead">{lead}</div>
      {sections_html}
      <footer class="wiki-footer">Preview build — not the production wiki export.</footer>
    </article>
    {infobox_html}
  </div>
</main>
"""
    return _page_shell(title=entry.name, depth=depth, body_main=main, body_class="wiki-article-page")


def _landing_tile(entry: WikiPageEntry) -> str:
    return f"""
<a class="wiki-tile" href="{html.escape(entry.path)}">
  <span class="wiki-tile-name">{html.escape(entry.name)}</span>
  <span class="wiki-tile-type">{html.escape(entry.type_label.replace('_', ' ').title())}</span>
  <span class="wiki-tile-excerpt">{html.escape(entry.excerpt or 'No summary yet.')}</span>
</a>
"""


def render_landing_html(entries: list[WikiPageEntry]) -> str:
    entities = [e for e in entries if not e.is_work]
    works = [e for e in entries if e.is_work]

    by_type: dict[str, list[WikiPageEntry]] = {}
    for entry in sorted(entities, key=lambda e: e.name.lower()):
        key = entry.type_label.replace("_", " ").title() or "Other"
        by_type.setdefault(key, []).append(entry)

    type_sections = []
    for type_name in sorted(by_type.keys()):
        tiles = "".join(_landing_tile(e) for e in by_type[type_name])
        type_sections.append(
            f'<section class="wiki-browse-section"><h2>{html.escape(type_name)}</h2>'
            f'<div class="wiki-tile-grid">{tiles}</div></section>'
        )

    works_section = ""
    if works:
        tiles = "".join(_landing_tile(e) for e in sorted(works, key=lambda e: e.name.lower()))
        works_section = (
            '<section class="wiki-browse-section"><h2>Narrative works</h2>'
            f'<div class="wiki-tile-grid">{tiles}</div></section>'
        )

    body = f"""
<main class="wiki-landing">
  <section class="wiki-hero">
    <h1>Welcome to the {html.escape(WIKI_SITE_TITLE)}</h1>
    <p class="wiki-hero-lead">A fan-style lore encyclopedia for the {html.escape(GAME_NAME)} setting. Browse characters, factions, and narrative works.</p>
    <p class="wiki-hero-note">Static preview from pipeline cards — use the search bar above or the grids below.</p>
  </section>
  {works_section}
  {"".join(type_sections) if type_sections else '<p class="wiki-empty">No lore pages in this build.</p>'}
</main>
"""
    return _page_shell(title=WIKI_SITE_TITLE, depth=0, body_main=body, body_class="wiki-landing-page")


def copy_static_assets(out_dir: Path) -> None:
    for name in ("wiki.css", "wiki.js"):
        src = ASSETS_DIR / name
        if not src.exists():
            raise FileNotFoundError(f"Missing wiki site asset: {src}")
        shutil.copy2(src, out_dir / name)
    favicon_src = _resolve_favicon_source()
    if favicon_src is not None:
        shutil.copy2(favicon_src, out_dir / WIKI_FAVICON_NAME)


def _load_pipeline_config(config_path: Path | None) -> dict[str, Any]:
    if config_path and config_path.exists():
        return read_json(config_path)
    default = Path("config/pipeline_config.json")
    if default.exists():
        return read_json(default)
    return {}


def build_wiki_site(
    run_root: Path,
    out_dir: Path,
    *,
    entity_filter: set[str] | None = None,
    prefer_canonical: bool = True,
    config_path: Path | None = None,
) -> list[WikiPageEntry]:
    from pipeline.wiki_autolink import (
        AutolinkState,
        build_autolink_index,
        build_quest_autolink_targets,
        collect_unlinked_candidates,
        wiki_site_autolink_config,
    )

    migrate_run_artifacts_to_numbered(run_root)
    entries: list[WikiPageEntry] = []

    for card in load_entity_cards(run_root, prefer_canonical=prefer_canonical):
        name = str(card.get("canonical_name", "")).strip()
        if entity_filter and name.lower() not in entity_filter:
            continue
        entries.append(entity_entry_from_card(card))

    for work in load_work_cards(run_root):
        entries.append(work_entry_from_card(work))

    if not entries:
        raise RuntimeError("No wiki pages to build (no entity cards or work cards found).")

    config = _load_pipeline_config(config_path)
    autolink_cfg = wiki_site_autolink_config(config)
    autolink_enabled = bool(autolink_cfg.get("enabled"))
    write_report = bool(autolink_cfg.get("write_report"))

    href_by_name = build_href_by_name(entries, config=config)
    autolink_index = None
    if autolink_enabled:
        autolink_index = build_autolink_index(
            entries,
            quest_targets=build_quest_autolink_targets(),
            config=config,
        )

    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "pages").mkdir(exist_ok=True)
    (out_dir / "works").mkdir(exist_ok=True)
    copy_static_assets(out_dir)

    quest_search_rows: list[dict[str, str]] = []
    try:
        from pipeline.quest_map_builder import build_quest_map_site

        work_title = f"{GAME_NAME} Coda"
        for entry in entries:
            if entry.is_work and str(entry.payload.get("work_id", "")).strip().lower() == "theriac_coda":
                work_title = entry.name
                break
        quest_search_rows = build_quest_map_site(
            out_dir,
            href_by_name=href_by_name,
            work_title=work_title,
            autolink_index=autolink_index,
        )
    except FileNotFoundError:
        pass

    search_payload = build_search_index(entries)
    if quest_search_rows:
        search_payload.extend(quest_search_rows)
    write_json(out_dir / "search-index.json", search_payload)

    report_linked: list[dict[str, str]] = []
    report_unlinked: list[dict[str, str]] = []

    for entry in entries:
        autolink_state = AutolinkState() if autolink_index is not None else None
        infobox = build_infobox_html(entry, href_by_name, depth=1)
        if entry.is_work:
            sections = work_sections_html(
                entry.payload,
                autolink_index=autolink_index,
                current_slug=entry.slug,
                depth=1,
                autolink_state=autolink_state,
                page_slug=entry.slug,
            )
            html_doc = render_article_html(
                entry,
                sections_html=sections,
                infobox_html=infobox,
                depth=1,
                autolink_index=autolink_index,
                autolink_state=autolink_state,
            )
            (out_dir / "works" / f"{entry.slug}.html").write_text(html_doc, encoding="utf-8")
        else:
            sections = entity_sections_html(
                entry.payload,
                config_path,
                autolink_index=autolink_index,
                current_slug=entry.slug,
                depth=1,
                autolink_state=autolink_state,
                page_slug=entry.slug,
            )
            html_doc = render_article_html(
                entry,
                sections_html=sections,
                infobox_html=infobox,
                depth=1,
                autolink_index=autolink_index,
                autolink_state=autolink_state,
            )
            (out_dir / "pages" / f"{entry.slug}.html").write_text(html_doc, encoding="utf-8")

        if autolink_index is not None and autolink_state is not None:
            report_linked.extend(autolink_state.linked)
            prose_chunks = [str(entry.payload.get("summary", ""))]
            if entry.is_work:
                work_sections = entry.payload.get("sections")
                if isinstance(work_sections, dict):
                    prose_chunks.extend(str(v) for v in work_sections.values())
            else:
                from pipeline.ui_review_app import card_review_sections

                for section in card_review_sections(entry.payload):
                    prose_chunks.append(str(section.get("text", "")))
            for phrase in collect_unlinked_candidates("\n".join(prose_chunks), autolink_index):
                report_unlinked.append({"page_slug": entry.slug, "phrase": phrase})

    if autolink_enabled and write_report:
        report_path = out_dir / "autolink_report.json"
        write_json(
            report_path,
            {
                "autolink_enabled": True,
                "linked": report_linked,
                "candidates_unlinked": report_unlinked,
            },
        )

    (out_dir / "index.html").write_text(render_landing_html(entries), encoding="utf-8")
    return entries
