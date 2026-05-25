from __future__ import annotations

from pipeline.wiki_site_builder import (
    WikiPageEntry,
    asset_prefix,
    build_infobox_html,
    build_search_index,
    entity_entry_from_card,
    normalize_paragraphs,
    slug_by_name,
    slugify,
    work_entry_from_card,
)


def test_slugify() -> None:
    assert slugify("Enoch Faust") == "enoch-faust"


def test_search_index_entity_and_work() -> None:
    entries = [
        entity_entry_from_card({"canonical_name": "Enoch", "entity_type": "character", "summary": "Lead."}),
        work_entry_from_card({"work_id": "theriac_coda", "title": "Theriac Coda", "kind": "main", "summary": "Game."}),
    ]
    index = build_search_index(entries)
    assert len(index) == 2
    kinds = {row["page_kind"] for row in index}
    assert kinds == {"entity", "work"}
    slugs = [row["slug"] for row in index]
    assert len(slugs) == len(set(slugs))


def test_infobox_links_enoch_when_wiki_link_present() -> None:
    enoch = entity_entry_from_card({"canonical_name": "Enoch", "entity_type": "character", "summary": "x"})
    krypteia = entity_entry_from_card(
        {
            "canonical_name": "Krypteia",
            "entity_type": "faction",
            "summary": "Antagonists.",
            "details": {
                "wiki_links": [
                    {
                        "target_entity_name": "Enoch",
                        "relation_type": "antagonist_of",
                    }
                ]
            },
        }
    )
    slug_map = slug_by_name([enoch, krypteia])
    box = build_infobox_html(krypteia, slug_map, depth=1)
    assert 'href="../pages/enoch.html"' in box
    assert "Enoch" in box


def test_asset_prefix_depth() -> None:
    assert asset_prefix(0) == "./"
    assert asset_prefix(1) == "../"


def test_normalize_paragraphs_preserves_letter_t() -> None:
    html_out = normalize_paragraphs("The secret society that operates behind the scenes.")
    assert "The" in html_out
    assert "secret" in html_out
    assert "society" in html_out
    assert "he " not in html_out or "The" in html_out


def test_work_entry_path() -> None:
    entry = work_entry_from_card({"work_id": "theriac_coda", "title": "Theriac Coda", "kind": "main"})
    assert entry.path == "works/theriac-coda.html"
    assert entry.is_work
