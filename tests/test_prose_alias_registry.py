from pipeline.prose_alias_registry import (
    build_global_prose_alias_pairs,
    normalize_prose_for_entity,
    sanitize_card_prose_whitespace,
)


def test_build_global_pairs_includes_entity_aliases_and_config():
    entities = [
        {
            "canonical_name": "Enoch",
            "aliases": ["Loss"],
        },
        {
            "canonical_name": "Oyuun",
            "aliases": ["Oyu"],
        },
    ]
    memory = {
        "approved_aliases": [{"alias_text": "Loss", "canonical_name": "Enoch"}],
    }
    config = {
        "card_first_synthesis": {
            "prose_canonical_aliases": [{"alias": "Fear", "canonical": "Oyuun"}],
        }
    }
    pairs = build_global_prose_alias_pairs(entities, memory, config)
    alias_to_canonical = dict(pairs)
    assert alias_to_canonical["Loss"] == "Enoch"
    assert alias_to_canonical["Oyu"] == "Oyuun"
    assert alias_to_canonical["Fear"] == "Oyuun"


def test_normalize_prose_for_entity_replaces_other_characters():
    entity = {"canonical_name": "Enoch", "aliases": ["Loss"]}
    global_pairs = [("Fear", "Oyuun")]
    text = "He taught Fear chess at the lab."
    result = normalize_prose_for_entity(text, entity, global_pairs)
    assert "Fear" not in result
    assert "Oyuun" in result


def test_sanitize_card_prose_whitespace_fixes_mid_word_line_break():
    broken = "feeling somethin\ng barely human."
    assert sanitize_card_prose_whitespace(broken) == "feeling somethin g barely human."
