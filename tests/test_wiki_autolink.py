from __future__ import annotations

from pipeline.wiki_autolink import (
    AutolinkIndex,
    AutolinkPattern,
    AutolinkState,
    AutolinkTarget,
    autolink_paragraphs,
    autolink_plain_text,
    build_autolink_index,
    collect_unlinked_candidates,
    href_by_name,
)
from pipeline.wiki_site_builder import WikiPageEntry, entity_entry_from_card, work_entry_from_card


def _index_from_targets(targets: list[AutolinkTarget]) -> AutolinkIndex:
    patterns = [
        AutolinkPattern(surface=t.label, target=t)
        for t in sorted(targets, key=lambda t: len(t.label), reverse=True)
    ]
    return AutolinkIndex(patterns=patterns, targets_by_slug={t.slug: t for t in targets})


def test_longest_match_wins() -> None:
    targets = [
        AutolinkTarget("Enoch Faust", "enoch-faust", "pages/enoch-faust.html", "entity"),
        AutolinkTarget("Enoch", "enoch", "pages/enoch.html", "entity"),
    ]
    index = _index_from_targets(targets)
    out = autolink_plain_text(
        "Enoch Faust led the lab.",
        index,
        current_slug="other",
        depth=1,
    )
    assert 'href="../pages/enoch-faust.html"' in out
    assert 'href="../pages/enoch.html"' not in out
    assert out.count("<a ") == 1


def test_two_slugs_each_link_once() -> None:
    targets = [
        AutolinkTarget("Enoch Faust", "enoch-faust", "pages/enoch-faust.html", "entity"),
        AutolinkTarget("Enoch", "enoch", "pages/enoch.html", "entity"),
    ]
    index = _index_from_targets(targets)
    out = autolink_plain_text(
        "Enoch Faust met Enoch.",
        index,
        current_slug="other",
        depth=1,
    )
    assert out.count("<a ") == 2


def test_first_mention_only_across_paragraphs() -> None:
    targets = [AutolinkTarget("Krypteia", "krypteia", "pages/krypteia.html", "entity")]
    index = _index_from_targets(targets)
    state = AutolinkState()
    autolink_plain_text("Krypteia appeared.", index, current_slug="enoch", depth=1, state=state)
    out = autolink_plain_text("Krypteia returned.", index, current_slug="enoch", depth=1, state=state)
    assert out.count("<a ") == 0
    assert "Krypteia returned." in out


def test_self_link_skipped() -> None:
    targets = [AutolinkTarget("Enoch", "enoch", "pages/enoch.html", "entity")]
    index = _index_from_targets(targets)
    out = autolink_plain_text("Enoch leads the lab.", index, current_slug="enoch", depth=1)
    assert "<a " not in out
    assert "Enoch leads" in out


def test_work_href_at_depth_one() -> None:
    targets = [AutolinkTarget("Theriac Coda", "theriac-coda", "works/theriac-coda.html", "work")]
    index = _index_from_targets(targets)
    out = autolink_plain_text("Set in Theriac Coda.", index, current_slug="enoch", depth=1)
    assert 'href="../works/theriac-coda.html"' in out


def test_quest_href() -> None:
    targets = [
        AutolinkTarget("Bat Out Of Hell", "oyuun_bat-out-of-hell", "quests/oyuun_bat-out-of-hell.html", "quest"),
    ]
    index = _index_from_targets(targets)
    out = autolink_plain_text("Quest Bat Out Of Hell unlocks.", index, current_slug="enoch", depth=1)
    assert 'href="../quests/oyuun_bat-out-of-hell.html"' in out


def test_config_alias_resolves_to_entity() -> None:
    entries = [
        entity_entry_from_card({"canonical_name": "Enoch", "entity_type": "character", "summary": ""}),
    ]
    config = {"card_first_synthesis": {"prose_canonical_aliases": [{"alias": "Loss", "canonical": "Enoch"}]}}
    index = build_autolink_index(entries, quest_targets=[], config=config)
    out = autolink_plain_text("Loss fled.", index, current_slug="other", depth=1)
    assert 'href="../pages/enoch.html"' in out


def test_html_escape_in_surrounding_text() -> None:
    targets = [AutolinkTarget("Enoch", "enoch", "pages/enoch.html", "entity")]
    index = _index_from_targets(targets)
    out = autolink_plain_text('Enoch & "allies" <lab>.', index, current_slug="other", depth=1)
    assert "&amp;" in out
    assert "&lt;lab&gt;" in out
    assert "<a " in out


def test_build_autolink_index_includes_work_and_aliases() -> None:
    entries = [
        entity_entry_from_card(
            {
                "canonical_name": "Enoch Faust",
                "entity_type": "character",
                "summary": "",
                "aliases": ["Loss"],
            }
        ),
        work_entry_from_card({"work_id": "theriac_coda", "title": "Theriac Coda", "kind": "main"}),
    ]
    index = build_autolink_index(entries, quest_targets=[], config={})
    hrefs = href_by_name(index)
    assert hrefs["enoch faust"] == "pages/enoch-faust.html"
    assert hrefs["loss"] == "pages/enoch-faust.html"
    assert hrefs["theriac coda"] == "works/theriac-coda.html"
    assert hrefs["theriac_coda"] == "works/theriac-coda.html"


def test_autolink_paragraphs_wraps_p_tags() -> None:
    targets = [AutolinkTarget("Enoch", "enoch", "pages/enoch.html", "entity")]
    index = _index_from_targets(targets)
    html_out = autolink_paragraphs("Enoch leads.\n\nEnoch again.", index, current_slug="other", depth=1)
    assert html_out.startswith("<p>")
    assert html_out.count("<p>") == 2


def test_collect_unlinked_candidates() -> None:
    targets = [AutolinkTarget("Enoch", "enoch", "pages/enoch.html", "entity")]
    index = _index_from_targets(targets)
    found = collect_unlinked_candidates("Enoch met Unknown Faction leaders.", index)
    assert any("Unknown Faction" in phrase for phrase in found)
