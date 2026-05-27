from __future__ import annotations

from pipeline.quest_catalog import quest_key_from_label
from pipeline.quest_tagging import (
    apply_override_tags,
    build_artist_review_queue,
    has_quest_design_signals,
    heuristic_tags_for_snippet,
    merge_discovered_quests,
    parse_chronology_guesses,
    select_snippets_for_tagging,
)


def test_parse_chronology_year_only_when_explicit() -> None:
    assert parse_chronology_guesses("Year 2 siege begins.")["earliest_year_guess"] == 2
    assert parse_chronology_guesses("They discuss quest pacing.")["earliest_year_guess"] is None


def test_has_quest_design_signals_meta_song() -> None:
    snippet = {
        "knowledge_track": "meta",
        "display_text_normalized": 'Quest title might be "Fake Plastic Trees" by Radiohead for Oyuun.',
    }
    assert has_quest_design_signals(snippet)


def test_heuristic_exact_known_title() -> None:
    snippet = {
        "snippet_id": "s1",
        "knowledge_track": "lore",
        "display_text_normalized": 'We locked "Cochise" as an Oyuun quest by Audioslave.',
    }
    examples_index = {
        quest_key_from_label("Cochise"): {
            "quest_label": "Cochise",
            "main_character": "Oyuun",
            "motif_id": "audioslave",
        }
    }
    tags = heuristic_tags_for_snippet(
        snippet,
        narrative_work_id="theriac_coda",
        examples_index=examples_index,
        discovered_index={},
        motifs=[],
        character_names=["Oyuun", "Enoch"],
        artist_bindings={},
        known_titles=["Cochise"],
    )
    assert len(tags) == 1
    assert tags[0]["match_kind"] == "exact_known"
    assert tags[0]["main_character"] == "Oyuun"


def test_merge_discovered_quests_appends_snippet_ids() -> None:
    merged = merge_discovered_quests(
        None,
        [
            {
                "quest_label": "Cochise",
                "quest_key": "cochise",
                "snippet_id": "s1",
                "main_character": "Oyuun",
                "confidence": 0.8,
            },
            {
                "quest_label": "Cochise",
                "quest_key": "cochise",
                "snippet_id": "s2",
                "main_character": "Oyuun",
                "confidence": 0.7,
            },
        ],
    )
    assert len(merged["quests"]) == 1
    assert set(merged["quests"][0]["snippet_ids"]) == {"s1", "s2"}


def test_build_artist_review_queue_groups_other() -> None:
    queue = build_artist_review_queue(
        [
            {
                "snippet_id": "s1",
                "quest_label": "Mystery Track",
                "motif_id": "other",
                "character_confidence": 0.4,
                "artist_attributions": ["The Rolling Stones"],
                "main_character": None,
            },
            {
                "snippet_id": "s2",
                "quest_label": "Another Track",
                "motif_id": "other",
                "character_confidence": 0.35,
                "artist_attributions": ["The Rolling Stones"],
                "main_character": "Pandora",
            },
        ],
        threshold=0.65,
    )
    assert len(queue) == 1
    assert queue[0]["artist_label"] == "The Rolling Stones"
    assert set(queue[0]["snippet_ids"]) == {"s1", "s2"}


def test_apply_override_tags() -> None:
    snippet = {"snippet_id": "s9", "knowledge_track": "meta", "display_text_normalized": "x"}
    rows = apply_override_tags(
        snippet,
        [{"quest_label": "Custom Quest", "main_character": "Enoch", "motif_id": "nine_inch_nails"}],
        "theriac_coda",
    )
    assert rows[0]["source"] == "review_override"
    assert rows[0]["main_character"] == "Enoch"


def test_select_snippets_includes_meta_when_enabled() -> None:
    snippets = [
        {"snippet_id": "a", "knowledge_track": "meta", "display_text_normalized": "quest song naming"},
        {"snippet_id": "b", "knowledge_track": "lore", "display_text_normalized": "unrelated lore"},
    ]
    selected = select_snippets_for_tagging(snippets, {}, {"include_meta_track": True, "max_snippets": 10})
    ids = {s["snippet_id"] for s in selected}
    assert "a" in ids
    assert "b" not in ids
