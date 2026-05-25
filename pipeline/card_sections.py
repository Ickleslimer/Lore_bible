"""Canonical wiki card section keys, display titles, path/work history rules."""

from __future__ import annotations

from typing import Any

from pipeline.entity_resolution import normalize_entity_type

# Path-neutral sections (all cards).
BASE_CARD_SECTION_KEYS = [
    "background",
    "role_in_story",
    "relationships",
    "inspirations",
    "open_questions",
]

# Theriac Coda route histories on character cards (Path B = main; Path A = side route like a spin-off).
CODA_CHARACTER_HISTORY_KEYS = [
    "history_theriac_coda",
    "history_path_a_side_route",
]

# Deprecated path section keys (older synthesized cards).
LEGACY_PATH_SECTION_KEYS = ["path_b_main", "path_a_destructive"]

LEGACY_PATH_TO_HISTORY = {
    "path_b_main": "history_theriac_coda",
    "path_a_destructive": "history_path_a_side_route",
}

# Older cards may still have a single blended timeline section.
LEGACY_CARD_SECTION_KEYS = ["timeline"]

CARD_SECTION_KEYS = [
    "background",
    "role_in_story",
    "relationships",
    *CODA_CHARACTER_HISTORY_KEYS,
    "inspirations",
    "open_questions",
]

SECTION_WRITE_ORDER = [
    "summary",
    "background",
    "role_in_story",
    "relationships",
    "history_theriac_coda",
    "history_path_a_side_route",
    "inspirations",
]


def section_write_order(config: dict[str, Any] | None = None) -> list[str]:
    """Section-chained writer order including registry spin-off history stubs."""
    order = list(SECTION_WRITE_ORDER)
    insert_at = order.index("inspirations") if "inspirations" in order else len(order)
    for key in character_work_history_keys(config):
        if key in order or key in CODA_CHARACTER_HISTORY_KEYS:
            continue
        order.insert(insert_at, key)
        insert_at += 1
    return order

CARD_SECTION_DISPLAY_TITLES: dict[str, str] = {
    "background": "Background",
    "role_in_story": "Role In Story",
    "relationships": "Relationships",
    "history_theriac_coda": "Theriac Coda — Main Route (Peaceful)",
    "history_path_a_side_route": "Theriac Coda — Path A (Side Route)",
    "path_b_main": "Theriac Coda — Main Route (Peaceful)",
    "path_a_destructive": "Theriac Coda — Path A (Side Route)",
    "timeline": "Timeline",
    "inspirations": "Inspirations",
    "open_questions": "Open Questions",
}

PATH_BRANCH_CONTEXT = """
Theriac branch structure (for playable character cards):
- About one hour into the game, the player chooses to side with the lab (expected) or follow original orders against the lab.
- Path B (peaceful / main route): player sides with the lab; this is the primary storyline (~40+ hours of content).
- Path A (destructive / side route): player executes lab members per original orders; a shorter branch (~6 hours), not the main arc.
- The summary lede reflects Path B / the peaceful main route only (default playthrough). Do not put Path A plot in the summary.
- history_theriac_coda holds main-route (Path B) events. history_path_a_side_route holds Path A only (optional, like a spin-off history block).
- role_in_story covers only pre-branch presence and the branch choice framing—not full route walkthroughs.
"""

PEACEFUL_LEDE_RULE = """
Peaceful-path lede rule (summary only):
- Write the summary as if the character on the expected Path B / peaceful main route (lab siding, ~40+ hour arc).
- Do not describe Path A violence, cyberpsychotic assault, execute-lab orders, or dual-ending contrast in the summary.
- At most one short clause elsewhere may note an alternate destructive route exists; details belong in history_path_a_side_route.
"""

_PATH_A_MARKERS = (
    "path a",
    "destructive path",
    "side route",
    "against the lab",
    "execute the lab",
    "executes the lab",
    "execution of the lab",
    "cyberpsychotic",
    "cyberpsychosis",
    "olympus assault",
    "brutal assault",
    "~6 hour",
    "6 hour",
    "six hour",
)

_PATH_B_MARKERS = (
    "path b",
    "peaceful path",
    "main route",
    "main path",
    "side with the lab",
    "sides with the lab",
    "40+ hour",
    "40 hour",
    "forty hour",
)


def card_section_display_title(key: str) -> str:
    if key in CARD_SECTION_DISPLAY_TITLES:
        return CARD_SECTION_DISPLAY_TITLES[key]
    if key.startswith("history_"):
        work_part = key.removeprefix("history_").replace("_", " ").title()
        return f"{work_part} — Appearances"
    return str(key).replace("_", " ").title()


def normalize_card_sections(sections: dict[str, Any]) -> dict[str, Any]:
    """Map legacy path_* section keys to history_* for display and synthesis."""
    if not isinstance(sections, dict):
        return {}
    out = dict(sections)
    for legacy, modern in LEGACY_PATH_TO_HISTORY.items():
        legacy_text = str(out.get(legacy, "")).strip()
        if not legacy_text:
            continue
        existing = str(out.get(modern, "")).strip()
        if not existing:
            out[modern] = legacy_text
        out.pop(legacy, None)
    return out


def character_work_history_keys(config: dict[str, Any] | None = None) -> list[str]:
    try:
        from pipeline.narrative_works import character_history_section_keys

        return character_history_section_keys(config)
    except Exception:
        return []


def card_review_section_order(config: dict[str, Any] | None = None) -> list[tuple[str, str]]:
    order: list[tuple[str, str]] = [
        ("background", card_section_display_title("background")),
        ("role_in_story", card_section_display_title("role_in_story")),
        ("relationships", card_section_display_title("relationships")),
        ("history_theriac_coda", card_section_display_title("history_theriac_coda")),
        ("history_path_a_side_route", card_section_display_title("history_path_a_side_route")),
    ]
    for key in character_work_history_keys(config):
        if key in {item[0] for item in order} or key in CODA_CHARACTER_HISTORY_KEYS:
            continue
        order.append((key, card_section_display_title(key)))
    for legacy_key, title_key in (
        ("path_b_main", "path_b_main"),
        ("path_a_destructive", "path_a_destructive"),
        ("timeline", "timeline"),
    ):
        order.append((legacy_key, card_section_display_title(title_key)))
    order.extend(
        [
            ("inspirations", card_section_display_title("inspirations")),
            ("open_questions", card_section_display_title("open_questions")),
        ]
    )
    return order


def should_use_path_split_sections(
    entity: dict[str, Any] | None,
    *,
    approved_snippet_count: int = 0,
    config: dict[str, Any] | None = None,
) -> bool:
    if not isinstance(entity, dict):
        return False
    if normalize_entity_type(entity.get("entity_type", "term")) != "character":
        return False
    if approved_snippet_count >= 80:
        return True
    if config:
        from pipeline.card_first_review import card_first_synthesis_config, is_protagonist_tier_entity

        cfg = card_first_synthesis_config(config)
        if is_protagonist_tier_entity(entity, approved_snippet_count, cfg):
            return True
    return approved_snippet_count >= 12


def synthesis_section_keys(
    entity: dict[str, Any] | None = None,
    *,
    approved_snippet_count: int = 0,
    config: dict[str, Any] | None = None,
) -> list[str]:
    if should_use_path_split_sections(entity, approved_snippet_count=approved_snippet_count, config=config):
        keys = [
            "background",
            "role_in_story",
            "relationships",
            *CODA_CHARACTER_HISTORY_KEYS,
        ]
        for work_key in character_work_history_keys(config):
            if work_key not in keys:
                keys.append(work_key)
        keys.extend(["inspirations", "open_questions"])
        return keys
    return [*BASE_CARD_SECTION_KEYS[:3], "timeline", *BASE_CARD_SECTION_KEYS[3:]]


def support_map_section_keys(
    entity: dict[str, Any] | None = None,
    *,
    approved_snippet_count: int = 0,
    config: dict[str, Any] | None = None,
) -> list[str]:
    keys = synthesis_section_keys(entity, approved_snippet_count=approved_snippet_count, config=config)
    legacy = list(LEGACY_CARD_SECTION_KEYS) + list(LEGACY_PATH_SECTION_KEYS)
    return keys + [k for k in legacy if k not in keys]


def _section_text(synthesis: dict[str, Any], section_key: str) -> str:
    sections = synthesis.get("sections")
    if isinstance(sections, dict):
        normalized = normalize_card_sections(sections)
        text = str(normalized.get(section_key, "")).strip()
        if text:
            return text.lower()
    return ""


def _summary_text(synthesis: dict[str, Any]) -> str:
    return str(synthesis.get("summary", "")).strip().lower()


def find_path_a_in_summary(synthesis: dict[str, Any]) -> list[str]:
    summary = _summary_text(synthesis)
    if not summary:
        return []
    return [f"summary contains Path A marker '{marker}'" for marker in _PATH_A_MARKERS if marker in summary]


def find_path_section_crossovers(synthesis: dict[str, Any]) -> list[str]:
    path_b = _section_text(synthesis, "history_theriac_coda")
    path_a = _section_text(synthesis, "history_path_a_side_route")
    if not path_b and not path_a:
        return []

    issues: list[str] = []
    for marker in _PATH_A_MARKERS:
        if marker in path_b:
            issues.append(f"Main-route history reads like Path A (matched '{marker}').")
    for marker in _PATH_B_MARKERS:
        if marker in path_a:
            issues.append(f"Path A history reads like Path B (matched '{marker}').")
    return issues


def validate_peaceful_path_summary(synthesis: dict[str, Any]) -> None:
    issues = find_path_a_in_summary(synthesis)
    if issues:
        detail = " ".join(issues[:4])
        raise RuntimeError(
            "Stage 11 synthesis rejected: summary must be a peaceful-path (Path B) lede only. "
            f"{detail} Move Path A material to history_path_a_side_route."
        )


def validate_path_section_isolation(synthesis: dict[str, Any]) -> None:
    validate_peaceful_path_summary(synthesis)
    issues = find_path_section_crossovers(synthesis)
    if issues:
        detail = " ".join(issues[:4])
        raise RuntimeError(
            "Stage 11 synthesis rejected: route history sections must stay isolated. "
            f"{detail}"
        )


def validate_work_history_isolation(
    synthesis: dict[str, Any],
    work_markers: dict[str, tuple[str, ...]],
) -> None:
    """Reject when prose for work A appears in history section for work B."""
    if not work_markers:
        return
    for section_key, markers in work_markers.items():
        if not section_key.startswith("history_"):
            continue
        text = _section_text(synthesis, section_key)
        if not text:
            continue
        for other_key, other_markers in work_markers.items():
            if other_key == section_key:
                continue
            for marker in other_markers:
                if marker and marker in text:
                    raise RuntimeError(
                        f"Stage 11 synthesis rejected: section `{section_key}` contains markers for `{other_key}` ('{marker}')."
                    )
