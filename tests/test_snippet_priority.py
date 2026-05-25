from pipeline.card_first_review import rank_snippet_ids, snippet_priority_key


def test_character_incidental_snippet_ranks_below_core_character_lore() -> None:
    entity = {"entity_type": "character", "canonical_name": "Enoch"}
    core = {
        "snippet_id": "snippet_core",
        "display_text_normalized": "Enoch struggles with thanatophobia during a chess game with Oyuun.",
        "conversation_patch_summary": "",
    }
    incidental = {
        "snippet_id": "snippet_incidental",
        "display_text_normalized": "Enoch gives approval for the early specifications of RUINR, built with embezzled funds.",
        "conversation_patch_summary": "Secretive organization lore and embezzled funding.",
    }
    source = {"snippet_core": core, "snippet_incidental": incidental}
    ranked = rank_snippet_ids(["snippet_incidental", "snippet_core"], source, entity)
    assert ranked[0] == "snippet_core"


def test_incidental_penalty_absent_for_non_character_entities() -> None:
    entity = {"entity_type": "faction", "canonical_name": "RUINR"}
    snippet = {
        "snippet_id": "snippet_a",
        "display_text_normalized": "Built with embezzled funds and early specifications.",
    }
    key = snippet_priority_key(snippet, entity)
    key_no_entity = snippet_priority_key(snippet, None)
    assert key[2] == key_no_entity[2]
