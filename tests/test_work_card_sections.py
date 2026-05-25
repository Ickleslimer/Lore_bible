from pipeline.work_card_sections import (
    PEACEFUL_WORK_LEDE_RULE,
    WORK_SECTION_WRITE_ORDER,
    work_synthesis_section_keys,
    work_word_target_plan,
)


def test_work_section_order_includes_branching_before_cast() -> None:
    assert WORK_SECTION_WRITE_ORDER.index("branching") < WORK_SECTION_WRITE_ORDER.index("cast")
    assert "summary" == WORK_SECTION_WRITE_ORDER[0]


def test_work_synthesis_keys_match_body_sections() -> None:
    keys = work_synthesis_section_keys()
    assert "premise" in keys
    assert "branching" in keys
    assert "summary" not in keys


def test_work_word_targets_reference_peaceful_lede() -> None:
    plan = work_word_target_plan()
    summary_target = plan["section_word_targets"]["summary"]
    assert "Path B" in summary_target or "peaceful" in summary_target.lower()
    assert "Path B" in PEACEFUL_WORK_LEDE_RULE or "peaceful" in PEACEFUL_WORK_LEDE_RULE.lower()
