from __future__ import annotations

from pathlib import Path

from pipeline.quest_catalog import load_quest_catalog
from pipeline.quest_map_builder import QUEST_MAP_SLUG, build_quest_map_site, render_quest_map_html
from pipeline.wiki_site_builder import build_href_by_name, entity_entry_from_card


def test_render_quest_map_contains_character_and_year_views(tmp_path: Path) -> None:
    seed = Path(__file__).resolve().parent / "fixtures" / "quest_song_seed.json"
    catalog = load_quest_catalog(seed)
    href_by_name = build_href_by_name(
        [entity_entry_from_card({"canonical_name": "Oyuun", "entity_type": "character", "summary": ""})]
    )
    html_doc = render_quest_map_html(catalog, href_by_name=href_by_name, depth=1)
    assert "view-by-character" in html_doc
    assert "view-by-year" in html_doc
    assert "Pool order" in html_doc


def test_build_quest_map_site_writes_map_and_stubs(tmp_path: Path) -> None:
    seed = Path(__file__).resolve().parent / "fixtures" / "quest_song_seed.json"
    rows = build_quest_map_site(tmp_path, href_by_name={}, seed_path=seed)
    assert (tmp_path / "quests" / f"{QUEST_MAP_SLUG}.html").exists()
    assert len(rows) >= 40
    map_rows = [r for r in rows if r.get("page_kind") == "quest_map"]
    assert len(map_rows) == 1
