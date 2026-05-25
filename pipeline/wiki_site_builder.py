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


def _infobox_row(label: str, value_html: str) -> str:
    if not value_html.strip():
        return ""
    return (
        f'<tr><th scope="row">{html.escape(label)}</th>'
        f'<td>{value_html}</td></tr>'
    )


def _link_html(name: str, slug_map: dict[str, str], depth: int) -> str:
    key = name.strip().lower()
    slug = slug_map.get(key)
    if not slug:
        return html.escape(name)
    prefix = asset_prefix(depth)
    path = f"{prefix}pages/{slug}.html"
    return f'<a href="{html.escape(path)}">{html.escape(name)}</a>'


def build_infobox_html(entry: WikiPageEntry, slug_map: dict[str, str], *, depth: int = 1) -> str:
    title = html.escape(entry.name)
    type_label = html.escape(entry.type_label.replace("_", " ").title())
    status = html.escape(entry.status or "draft")
    image_label = html.escape(entry.type_label.replace("_", " ").title() or "Lore")

    rows: list[str] = []
    if entry.is_work:
        rows.append(_infobox_row("Kind", type_label))
        rows.append(_infobox_row("Status", status))
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
                links.append(f'<li><span class="rel">{html.escape(rel)}</span> {_link_html(target, slug_map, depth)}</li>')
        for rel in (card.get("relationships") or [])[:8]:
            if not isinstance(rel, dict):
                continue
            target = str(rel.get("target_entity_name") or rel.get("target_card_id") or "").strip()
            rel_type = str(rel.get("relation_type", "related")).strip()
            if target:
                links.append(f'<li><span class="rel">{html.escape(rel_type)}</span> {_link_html(target, slug_map, depth)}</li>')
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


def _page_shell(
    *,
    title: str,
    depth: int,
    body_main: str,
    body_class: str = "",
) -> str:
    prefix = asset_prefix(depth)
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{html.escape(title)} — {html.escape(WIKI_SITE_TITLE)}</title>
  <link rel="stylesheet" href="{prefix}wiki.css">
</head>
<body data-wiki-root="{html.escape(prefix)}" class="{body_class}">
{render_header_html(depth=depth)}
{body_main}
<script src="{prefix}wiki.js" defer></script>
</body>
</html>
"""


def entity_sections_html(card: dict[str, Any], config_path: Path | None) -> str:
    from pipeline.ui_review_app import card_review_sections

    blocks: list[str] = []
    for section in card_review_sections(card):
        title = html.escape(str(section.get("title", "")))
        body = normalize_paragraphs(str(section.get("text", "")))
        if body:
            blocks.append(f'<section class="wiki-section"><h2>{title}</h2>{body}</section>')
    return "\n".join(blocks)


def work_sections_html(work: dict[str, Any]) -> str:
    sections = work.get("sections") if isinstance(work.get("sections"), dict) else {}
    blocks: list[str] = []
    for key, display in WORK_SECTION_DISPLAY_TITLES.items():
        if key == "summary":
            continue
        text = str(sections.get(key, "")).strip()
        if not text:
            continue
        title = html.escape(work_section_display_title(key))
        body = normalize_paragraphs(text)
        if body:
            blocks.append(f'<section class="wiki-section"><h2>{title}</h2>{body}</section>')
    return "\n".join(blocks)


def render_article_html(
    entry: WikiPageEntry,
    *,
    sections_html: str,
    infobox_html: str,
    depth: int,
) -> str:
    title = html.escape(entry.name)
    lead = normalize_paragraphs(str(entry.payload.get("summary", "")))
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


def build_wiki_site(
    run_root: Path,
    out_dir: Path,
    *,
    entity_filter: set[str] | None = None,
    prefer_canonical: bool = True,
    config_path: Path | None = None,
) -> list[WikiPageEntry]:
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

    slug_map = slug_by_name(entries)
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "pages").mkdir(exist_ok=True)
    (out_dir / "works").mkdir(exist_ok=True)
    copy_static_assets(out_dir)
    write_json(out_dir / "search-index.json", build_search_index(entries))

    for entry in entries:
        infobox = build_infobox_html(entry, slug_map, depth=1)
        if entry.is_work:
            sections = work_sections_html(entry.payload)
            html_doc = render_article_html(entry, sections_html=sections, infobox_html=infobox, depth=1)
            (out_dir / "works" / f"{entry.slug}.html").write_text(html_doc, encoding="utf-8")
        else:
            sections = entity_sections_html(entry.payload, config_path)
            html_doc = render_article_html(entry, sections_html=sections, infobox_html=infobox, depth=1)
            (out_dir / "pages" / f"{entry.slug}.html").write_text(html_doc, encoding="utf-8")

    (out_dir / "index.html").write_text(render_landing_html(entries), encoding="utf-8")
    return entries
