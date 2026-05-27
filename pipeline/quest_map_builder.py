"""Static HTML quest map (character chains × in-game year) for wiki preview."""

from __future__ import annotations

import html
from typing import Any

from pathlib import Path

from pipeline.branding import GAME_NAME
from pipeline.quest_catalog import load_quest_catalog, quests_for_work, slugify
from pipeline.wiki_site_builder import _link_html, _page_shell, asset_prefix


QUEST_MAP_SLUG = "theriac-coda-quest-map"
QUEST_MAP_TITLE = f"{GAME_NAME} Coda — Character Quest Map"
QUEST_MAP_PATH = f"quests/{QUEST_MAP_SLUG}.html"


def _year_label(year: int | None) -> str:
    if year is None:
        return "—"
    return f"Year {year}"


def _route_label(route: str) -> str:
    mapping = {
        "path_b": "Path B (main)",
        "path_a": "Path A",
        "both": "Both routes",
    }
    return mapping.get(str(route or "").strip().lower(), str(route or "—"))


def _quest_row(
    quest: dict[str, Any],
    *,
    href_by_name: dict[str, str],
    depth: int,
    show_character: bool = False,
) -> str:
    title = str(quest.get("quest_title", "")).strip()
    quest_id = str(quest.get("quest_id", "")).strip()
    prefix = asset_prefix(depth)
    title_cell = html.escape(title)
    if quest_id:
        href = f"{prefix}quests/{quest_id}.html"
        title_cell = f'<a href="{html.escape(href)}">{html.escape(title)}</a>'
    char = str(quest.get("main_character", "")).strip()
    char_cell = ""
    if show_character and char:
        char_cell = f"<td>{_link_html(char, href_by_name, depth)}</td>"
    seq = int(quest.get("pool_sequence") or 0)
    synopsis = html.escape(str(quest.get("synopsis", "")).strip())
    route = html.escape(_route_label(str(quest.get("route_scope", ""))))
    year = html.escape(_year_label(quest.get("earliest_year") if isinstance(quest.get("earliest_year"), int) else None))
    unlock = str(quest.get("unlock_note") or "").strip()
    gate = quest.get("year_gate")
    if gate and not unlock:
        unlock = f"Year gate: {gate}"
    unlock_cell = html.escape(unlock) if unlock else "—"
    return (
        f"<tr>"
        f"<td class=\"quest-seq\">{seq}</td>"
        f"{char_cell}"
        f"<td class=\"quest-title\">{title_cell}</td>"
        f"<td>{year}</td>"
        f"<td>{route}</td>"
        f"<td class=\"quest-synopsis\">{synopsis}</td>"
        f"<td class=\"quest-unlock\">{unlock_cell}</td>"
        f"</tr>"
    )


def render_character_sections(
    catalog: dict[str, Any],
    *,
    href_by_name: dict[str, str],
    depth: int,
) -> str:
    blocks: list[str] = []
    by_character = catalog.get("by_character", {})
    for character in sorted(by_character.keys(), key=lambda c: c.lower()):
        quests = by_character[character]
        rows = "".join(_quest_row(q, href_by_name=href_by_name, depth=depth) for q in quests)
        char_anchor = html.escape(slugify_character(character))
        blocks.append(
            f'<section class="wiki-quest-character" id="{char_anchor}">'
            f"<h2>{_link_html(character, href_by_name, depth)}</h2>"
            f'<p class="wiki-quest-note">Pool order: one active quest at a time; completing unlocks the next in this chain.</p>'
            f'<table class="wiki-quest-table"><thead><tr>'
            f"<th>#</th><th>Quest</th><th>Earliest year</th><th>Route</th><th>Synopsis</th><th>Unlock notes</th>"
            f"</tr></thead><tbody>{rows}</tbody></table></section>"
        )
    return "\n".join(blocks)


def slugify_character(name: str) -> str:
    return slugify(name)


def render_year_sections(
    catalog: dict[str, Any],
    *,
    href_by_name: dict[str, str],
    depth: int,
) -> str:
    blocks: list[str] = []
    by_year = catalog.get("by_year", {})
    for year in sorted(by_year.keys()):
        quests = sorted(
            by_year[year],
            key=lambda q: (str(q.get("main_character", "")).lower(), int(q.get("pool_sequence") or 0)),
        )
        rows = "".join(_quest_row(q, href_by_name=href_by_name, depth=depth, show_character=True) for q in quests)
        blocks.append(
            f'<section class="wiki-quest-year" id="year-{year}">'
            f"<h2>Year {year}</h2>"
            f"<p>Quests that can become available during this in-game year (subject to main-story gates and prior pool progress).</p>"
            f'<table class="wiki-quest-table"><thead><tr>'
            f"<th>#</th><th>Character</th><th>Quest</th><th>Earliest year</th><th>Route</th><th>Synopsis</th><th>Unlock notes</th>"
            f"</tr></thead><tbody>{rows}</tbody></table></section>"
        )

    unassigned = [
        q
        for q in catalog.get("quests", [])
        if not isinstance(q.get("earliest_year"), int)
    ]
    if unassigned:
        rows = "".join(
            _quest_row(q, href_by_name=href_by_name, depth=depth, show_character=True) for q in sorted(
                unassigned,
                key=lambda q: (str(q.get("main_character", "")).lower(), int(q.get("pool_sequence") or 0)),
            )
        )
        blocks.append(
            '<section class="wiki-quest-year" id="year-tbd">'
            "<h2>Year not set in catalog</h2>"
            "<p>Earliest in-game year still needs authoring in <code>config/quest_song_seed.json</code>.</p>"
            f'<table class="wiki-quest-table"><thead><tr>'
            f"<th>#</th><th>Character</th><th>Quest</th><th>Earliest year</th><th>Route</th><th>Synopsis</th><th>Unlock notes</th>"
            f"</tr></thead><tbody>{rows}</tbody></table></section>"
        )
    return "\n".join(blocks)


def render_quest_stub_html(
    quest: dict[str, Any],
    *,
    href_by_name: dict[str, str],
    work_title: str,
    depth: int = 1,
    autolink_index: Any | None = None,
) -> str:
    title = str(quest.get("quest_title", "")).strip()
    char = str(quest.get("main_character", "")).strip()
    prefix = asset_prefix(depth)
    map_link = f'{prefix}{QUEST_MAP_PATH}'
    work_link = f'{prefix}works/theriac-coda.html'
    lead_bits = [
        f'<p><strong>Main character:</strong> {_link_html(char, href_by_name, depth)}</p>',
        f"<p><strong>Pool step:</strong> {int(quest.get('pool_sequence') or 0)} in {html.escape(char)}&rsquo;s chain</p>",
        f"<p><strong>Earliest year:</strong> {html.escape(_year_label(quest.get('earliest_year') if isinstance(quest.get('earliest_year'), int) else None))}</p>",
        f"<p><strong>Route:</strong> {html.escape(_route_label(str(quest.get('route_scope', ''))))}</p>",
    ]
    prereq = str(quest.get("prerequisite_quest_id") or "").strip()
    if prereq:
        lead_bits.append(
            f'<p><strong>Previous in pool:</strong> <a href="{html.escape(prefix)}quests/{html.escape(prereq)}.html">{html.escape(prereq)}</a></p>'
        )
    synopsis_raw = str(quest.get("synopsis", "")).strip()
    quest_id = str(quest.get("quest_id", "")).strip()
    if autolink_index is not None and synopsis_raw:
        from pipeline.wiki_autolink import autolink_paragraphs

        synopsis_body = autolink_paragraphs(
            synopsis_raw,
            autolink_index,
            current_slug=quest_id,
            depth=depth,
            section="synopsis",
            page_slug=quest_id,
        )
    else:
        synopsis_body = f"<p>{html.escape(synopsis_raw)}</p>" if synopsis_raw else "<p>—</p>"
    body = f"""
<main class="wiki-page wiki-quest-stub">
  <article class="wiki-article">
    <p class="wiki-breadcrumb"><a href="{html.escape(work_link)}">{html.escape(work_title)}</a> · <a href="{html.escape(map_link)}">Quest map</a></p>
    <h1 class="wiki-page-title">{html.escape(title)}</h1>
    <div class="wiki-lead">{"".join(lead_bits)}</div>
    <section class="wiki-section"><h2>Synopsis</h2>{synopsis_body}</section>
    <footer class="wiki-footer">Quest stub from catalog seed — synthesis not run yet.</footer>
  </article>
</main>
"""
    return _page_shell(title=title, depth=depth, body_main=body, body_class="wiki-quest-page")


def render_quest_map_html(
    catalog: dict[str, Any],
    *,
    href_by_name: dict[str, str],
    work_title: str = f"{GAME_NAME} Coda",
    depth: int = 1,
) -> str:
    prefix = asset_prefix(depth)
    work_link = f"{prefix}works/theriac-coda.html"
    char_nav = "".join(
        f'<a class="wiki-quest-nav-link" href="#{html.escape(slugify_character(c))}">{html.escape(c)}</a>'
        for c in sorted(catalog.get("by_character", {}).keys(), key=str.lower)
    )
    year_nav = "".join(
        f'<a class="wiki-quest-nav-link" href="#year-{y}">Year {y}</a>'
        for y in sorted(catalog.get("by_year", {}).keys())
    )
    if any(not isinstance(q.get("earliest_year"), int) for q in catalog.get("quests", [])):
        year_nav += '<a class="wiki-quest-nav-link" href="#year-tbd">TBD</a>'

    body = f"""
<main class="wiki-page wiki-quest-map">
  <article class="wiki-article">
    <p class="wiki-breadcrumb"><a href="{html.escape(work_link)}">{html.escape(work_title)}</a></p>
    <h1 class="wiki-page-title">{html.escape(QUEST_MAP_TITLE)}</h1>
    <div class="wiki-lead">
      <p>Path B quest map structure: each character has a <strong>pool</strong> (one available quest at a time).
      Completing a quest unlocks the next in that character&rsquo;s chain. <strong>In-game year</strong> (roughly 5–6 years)
      gates when later pool entries can appear, often after main-story beats in that year.</p>
      <p>Catalog source: <code>config/quest_song_seed.json</code>. Earliest year and unlock notes are partial until authored.</p>
    </div>
    <nav class="wiki-quest-tabs" aria-label="Quest map views">
      <a href="#view-by-character">By character</a>
      <a href="#view-by-year">By year</a>
    </nav>
    <section id="view-by-character" class="wiki-quest-view">
      <h2>By character</h2>
      <nav class="wiki-quest-jump">{char_nav}</nav>
      {render_character_sections(catalog, href_by_name=href_by_name, depth=depth)}
    </section>
    <section id="view-by-year" class="wiki-quest-view">
      <h2>By year</h2>
      <nav class="wiki-quest-jump">{year_nav}</nav>
      {render_year_sections(catalog, href_by_name=href_by_name, depth=depth)}
    </section>
    <footer class="wiki-footer">Quest map preview — not production wiki export.</footer>
  </article>
</main>
"""
    return _page_shell(title=QUEST_MAP_TITLE, depth=depth, body_main=body, body_class="wiki-quest-map-page")


def _resolve_href_by_name(
    *,
    href_by_name: dict[str, str] | None = None,
    slug_map: dict[str, str] | None = None,
) -> dict[str, str]:
    if href_by_name:
        return href_by_name
    if slug_map:
        return {key: f"pages/{slug}.html" for key, slug in slug_map.items()}
    return {}


def build_quest_map_site(
    out_dir: Path,
    *,
    href_by_name: dict[str, str] | None = None,
    slug_map: dict[str, str] | None = None,
    seed_path: Path | None = None,
    work_title: str = f"{GAME_NAME} Coda",
    autolink_index: Any | None = None,
) -> list[dict[str, str]]:
    links = _resolve_href_by_name(href_by_name=href_by_name, slug_map=slug_map)
    catalog = load_quest_catalog(seed_path)
    quests = quests_for_work(catalog)
    if not quests:
        return []

    quest_dir = out_dir / "quests"
    quest_dir.mkdir(parents=True, exist_ok=True)
    (quest_dir / f"{QUEST_MAP_SLUG}.html").write_text(
        render_quest_map_html(catalog, href_by_name=links, work_title=work_title, depth=1),
        encoding="utf-8",
    )
    search_rows: list[dict[str, str]] = [
        {
            "name": QUEST_MAP_TITLE,
            "slug": QUEST_MAP_SLUG,
            "path": QUEST_MAP_PATH,
            "page_kind": "quest_map",
            "type": "quest map",
            "excerpt": "Character quest pools and year gates for Theriac Coda (Path B).",
        }
    ]
    for quest in quests:
        quest_id = str(quest.get("quest_id", "")).strip()
        if not quest_id:
            continue
        (quest_dir / f"{quest_id}.html").write_text(
            render_quest_stub_html(
                quest,
                href_by_name=links,
                work_title=work_title,
                depth=1,
                autolink_index=autolink_index,
            ),
            encoding="utf-8",
        )
        search_rows.append(
            {
                "name": str(quest.get("quest_title", quest_id)),
                "slug": quest_id,
                "path": f"quests/{quest_id}.html",
                "page_kind": "quest",
                "type": "quest",
                "excerpt": str(quest.get("synopsis", ""))[:160],
            }
        )
    return search_rows
