"""Narrative work page section keys (franchise hub cards)."""

from __future__ import annotations

from typing import Any

WORK_CARD_SECTION_KEYS = [
    "premise",
    "setting",
    "plot_structure",
    "branching",
    "other_works",
    "cast",
    "production",
]

WORK_SECTION_WRITE_ORDER = [
    "summary",
    "premise",
    "setting",
    "plot_structure",
    "branching",
    "other_works",
    "cast",
    "production",
]

WORK_SECTION_DISPLAY_TITLES: dict[str, str] = {
    "summary": "Summary",
    "premise": "Premise",
    "setting": "Setting",
    "plot_structure": "Plot and Structure",
    "branching": "Branching and Routes",
    "other_works": "Other Works and Timelines",
    "cast": "Cast and Characters",
    "production": "Production and Design Notes",
}

PEACEFUL_WORK_LEDE_RULE = """
Work-page summary rule:
- Frame Theriac Coda as the default player experience (Path B / peaceful main route, lab siding, ~40+ hours).
- Do not put Path A assault/cyberpsychosis detail in the summary; Path A belongs in branching.
"""


def work_section_display_title(key: str) -> str:
    return WORK_SECTION_DISPLAY_TITLES.get(key, str(key).replace("_", " ").title())


def work_review_section_order() -> list[tuple[str, str]]:
    return [(key, work_section_display_title(key)) for key in WORK_CARD_SECTION_KEYS]


def work_synthesis_section_keys() -> list[str]:
    return list(WORK_CARD_SECTION_KEYS)


def work_word_target_plan() -> dict[str, Any]:
    return {
        "synthesis_tier": "narrative_work",
        "total_word_target": {"min": 900, "max": 1600},
        "section_word_targets": {
            "summary": "80-120 words: default Path B player experience (peaceful main route)",
            "premise": "120-200 words",
            "setting": "120-200 words",
            "plot_structure": "200-350 words",
            "branching": "150-250 words: Path B main; Path A side route detail here only",
            "other_works": "80-150 words: spin-offs and alt timelines at high level",
            "cast": "120-200 words",
            "production": "80-160 words: meta/design only",
        },
    }
