from __future__ import annotations

from pipeline.stage_11_card_synthesis import merge_canonical_cards, merge_draft_cards_with_existing


def test_merge_canonical_cards_replaces_same_canonical_name() -> None:
    existing = [
        {"card_id": "card_old", "canonical_name": "Enoch", "summary": "old"},
        {"card_id": "card_other", "canonical_name": "Krypteia", "summary": "k"},
    ]
    revisions = [{"card_id": "card_new", "canonical_name": "Enoch", "summary": "new"}]
    merged = merge_canonical_cards(existing, revisions)
    assert len(merged) == 2
    by_name = {c["canonical_name"]: c for c in merged}
    assert by_name["Enoch"]["card_id"] == "card_new"
    assert by_name["Enoch"]["summary"] == "new"
    assert by_name["Krypteia"]["card_id"] == "card_other"


def test_merge_draft_cards_preserves_unsynthesized_entities() -> None:
    existing = [
        {"card_id": "card_a", "canonical_name": "Alpha", "summary": "a"},
        {"card_id": "card_b", "canonical_name": "Beta", "summary": "b"},
    ]
    new = [{"card_id": "card_b2", "canonical_name": "Beta", "summary": "b2"}]
    merged = merge_draft_cards_with_existing(existing, new)
    assert len(merged) == 2
    by_name = {c["canonical_name"]: c for c in merged}
    assert by_name["Alpha"]["summary"] == "a"
    assert by_name["Beta"]["card_id"] == "card_b2"
