from pathlib import Path

from pipeline.narrative_works import (
    active_works,
    heuristic_narrative_work_tag,
    load_narrative_works,
    narrative_work_for_history_section,
    work_by_id,
)


def test_registry_includes_theriac_coda_and_path_a_stub() -> None:
    works = load_narrative_works(Path("canon/narrative_works.json"))
    assert work_by_id(works, "theriac_coda") is not None
    path_a = work_by_id(works, "theriac_coda_path_a")
    assert path_a is not None
    assert path_a.get("kind") == "side_route"
    assert active_works(works)
    assert work_by_id(works, "wedding_ramasinta") is not None


def test_history_section_maps_to_work_id() -> None:
    assert narrative_work_for_history_section("history_theriac_coda") == "theriac_coda"
    assert narrative_work_for_history_section("history_path_a_side_route") == "theriac_coda_path_a"


def test_heuristic_tags_path_a_snippet() -> None:
    works = load_narrative_works(Path("canon/narrative_works.json"))
    snippet = {
        "display_text_normalized": "On Path A the player follows orders against the lab and executes members.",
        "conversation_patch_summary": "",
    }
    assert heuristic_narrative_work_tag(snippet, works) == "theriac_coda_path_a"
