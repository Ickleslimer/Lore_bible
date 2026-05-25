from pipeline.card_sections import (
    CARD_SECTION_DISPLAY_TITLES,
    find_path_a_in_summary,
    find_path_section_crossovers,
    normalize_card_sections,
    should_use_path_split_sections,
    synthesis_section_keys,
    validate_path_section_isolation,
    validate_peaceful_path_summary,
)


def test_protagonist_character_uses_history_sections() -> None:
    entity = {"entity_type": "character", "canonical_name": "Enoch"}
    assert should_use_path_split_sections(entity, approved_snippet_count=120)
    keys = synthesis_section_keys(entity, approved_snippet_count=120)
    assert "history_theriac_coda" in keys
    assert "history_path_a_side_route" in keys
    assert "timeline" not in keys


def test_sparse_faction_keeps_legacy_timeline_section() -> None:
    entity = {"entity_type": "faction", "canonical_name": "RUINR"}
    keys = synthesis_section_keys(entity, approved_snippet_count=5)
    assert "timeline" in keys
    assert "history_theriac_coda" not in keys


def test_legacy_path_sections_normalize_to_history() -> None:
    sections = {
        "path_b_main": "Main route prose.",
        "path_a_destructive": "Side route prose.",
    }
    normalized = normalize_card_sections(sections)
    assert normalized["history_theriac_coda"] == "Main route prose."
    assert normalized["history_path_a_side_route"] == "Side route prose."
    assert "path_b_main" not in normalized


def test_path_crossover_validation() -> None:
    synthesis = {
        "summary": "Enoch leads the lab on the peaceful main route.",
        "sections": {
            "history_theriac_coda": "On the peaceful path the player sides with the lab for the main route.",
            "history_path_a_side_route": "On Path A the player executes the lab in the destructive path.",
        },
    }
    assert not find_path_section_crossovers(synthesis)

    bad = {
        "summary": "Peaceful arc only.",
        "sections": {
            "history_theriac_coda": "The player enters a cyberpsychotic state on the destructive path.",
            "history_path_a_side_route": "A short side note.",
        },
    }
    try:
        validate_path_section_isolation(bad)
        raise AssertionError("expected crossover rejection")
    except RuntimeError as exc:
        assert "route history sections must stay isolated" in str(exc).lower()


def test_path_a_forbidden_in_summary() -> None:
    synthesis = {
        "summary": "On Path A the player executes the lab in a cyberpsychotic assault.",
        "sections": {"history_theriac_coda": "Main route."},
    }
    assert find_path_a_in_summary(synthesis)
    try:
        validate_peaceful_path_summary(synthesis)
        raise AssertionError("expected summary rejection")
    except RuntimeError as exc:
        assert "peaceful-path" in str(exc).lower()


def test_display_titles_for_notion_headings() -> None:
    assert "Main Route" in CARD_SECTION_DISPLAY_TITLES["history_theriac_coda"]
    assert "Path A" in CARD_SECTION_DISPLAY_TITLES["history_path_a_side_route"]
