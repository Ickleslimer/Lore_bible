from __future__ import annotations

from pathlib import Path

from pipeline.quest_catalog import (
    infer_earliest_year,
    infer_route_scope,
    load_quest_catalog,
    normalize_quest_seeds,
    quest_id_for_record,
)


def test_infer_earliest_year_from_synopsis() -> None:
    assert infer_earliest_year("Year 1 Siege.") == 1
    assert infer_earliest_year("Ramasinta returns during the Year 2 siege.") == 2
    assert infer_earliest_year("No calendar mention.") is None


def test_infer_route_scope() -> None:
    assert infer_route_scope("Oyuun battles to the death (destructive path).") == "path_a"
    assert infer_route_scope("Peaceful lab siding.") == "path_b"


def test_normalize_assigns_pool_sequence_and_prerequisite() -> None:
    seeds = [
        {"quest_title": "Alpha", "main_character": "Oyuun", "synopsis": "First."},
        {"quest_title": "Beta", "main_character": "Oyuun", "synopsis": "Second."},
    ]
    rows = normalize_quest_seeds(seeds)
    assert len(rows) == 2
    assert rows[0]["pool_sequence"] == 1
    assert rows[1]["pool_sequence"] == 2
    assert rows[0]["prerequisite_quest_id"] is None
    assert rows[1]["prerequisite_quest_id"] == rows[0]["quest_id"]
    assert rows[0]["quest_id"].startswith("oyuun_")


def test_quest_id_unique_collision() -> None:
    used: set[str] = set()
    a = quest_id_for_record("Enoch", "Test", used=used)
    b = quest_id_for_record("Enoch", "Test", used=used)
    assert a == "enoch_test"
    assert b == "enoch_test_2"


def test_load_catalog_from_fixture_seed() -> None:
    seed = Path(__file__).resolve().parent / "fixtures" / "quest_song_seed.json"
    catalog = load_quest_catalog(seed)
    assert catalog["meta"]["quest_count"] >= 40
    assert "Enoch" in catalog["by_character"]
    for quests in catalog["by_character"].values():
        sequences = [int(q["pool_sequence"]) for q in quests]
        assert sequences == sorted(sequences)
