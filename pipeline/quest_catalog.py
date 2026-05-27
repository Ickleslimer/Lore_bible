"""Character quest catalog for Theriac Coda (song-title quests, year + pool axes)."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

from pipeline.common import read_json

DEFAULT_SEED_PATH = Path("config/quest_song_seed.json")


def slugify(value: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", str(value or "").strip().lower()).strip("-")
    return slug or "page"


THERIAC_CODA_WORK_ID = "theriac_coda"
ROUTE_PATH_B = "path_b"
ROUTE_PATH_A = "path_a"
ROUTE_BOTH = "both"

_YEAR_RE = re.compile(r"\bYear\s*(\d+)\b", re.IGNORECASE)


def quest_slug_from_title(title: str) -> str:
    return slugify(title)


def quest_key_from_label(label: str) -> str:
    return quest_slug_from_title(label)


def load_quest_examples(examples_path: Path | None = None) -> list[dict[str, Any]]:
    """Known quest examples from seed (titles + optional pinned metadata)."""
    path = examples_path or DEFAULT_SEED_PATH
    if not path.exists():
        return []
    payload = read_json(path)
    seeds = payload.get("quest_song_seeds", []) if isinstance(payload, dict) else []
    examples: list[dict[str, Any]] = []
    for row in normalize_quest_seeds([r for r in seeds if isinstance(r, dict)]):
        title = str(row.get("quest_title", "")).strip()
        if not title:
            continue
        examples.append(
            {
                "quest_label": title,
                "quest_key": quest_key_from_label(title),
                "quest_id": str(row.get("quest_id", "")).strip() or None,
                "main_character": str(row.get("main_character", "")).strip() or None,
                "motif_id": str(row.get("band_hint", "")).strip() or None,
                "earliest_year_guess": row.get("earliest_year"),
                "pool_sequence_guess": row.get("pool_sequence"),
                "source": "quest_song_seed",
            }
        )
    return examples


def examples_by_title_key(examples: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for ex in examples:
        key = str(ex.get("quest_key", "")).strip().lower()
        label_key = quest_key_from_label(str(ex.get("quest_label", "")))
        if key:
            out[key] = ex
        if label_key:
            out[label_key] = ex
    return out


def quest_id_for_record(main_character: str, quest_title: str, *, used: set[str] | None = None) -> str:
    char = slugify(main_character) or "unknown"
    title = quest_slug_from_title(quest_title) or "quest"
    base = f"{char}_{title}"
    candidate = base
    n = 2
    taken = set() if used is None else used
    while candidate in taken:
        candidate = f"{base}_{n}"
        n += 1
    taken.add(candidate)
    return candidate


def infer_earliest_year(synopsis: str) -> int | None:
    match = _YEAR_RE.search(str(synopsis or ""))
    if not match:
        return None
    try:
        return int(match.group(1))
    except ValueError:
        return None


def infer_route_scope(synopsis: str) -> str:
    text = str(synopsis or "").lower()
    if "destructive path" in text or "path a" in text:
        return ROUTE_PATH_A
    return ROUTE_PATH_B


def normalize_quest_seeds(
    seeds: list[dict[str, Any]],
    *,
    narrative_work_id: str = THERIAC_CODA_WORK_ID,
) -> list[dict[str, Any]]:
    """Fill quest_id, pool_sequence, prerequisites, and inferred year/route on seed rows."""
    pool_counters: dict[str, int] = {}
    used_ids: set[str] = set()
    normalized: list[dict[str, Any]] = []
    last_id_by_character: dict[str, str] = {}

    for raw in seeds:
        if not isinstance(raw, dict):
            continue
        row = dict(raw)
        title = str(row.get("quest_title", "")).strip()
        character = str(row.get("main_character", "")).strip()
        if not title or not character:
            continue

        char_key = character.lower()
        pool_counters[char_key] = pool_counters.get(char_key, 0) + 1
        pool_sequence = int(row.get("pool_sequence") or pool_counters[char_key])

        quest_id = str(row.get("quest_id", "")).strip()
        if not quest_id:
            quest_id = quest_id_for_record(character, title, used=used_ids)
        else:
            used_ids.add(quest_id)

        synopsis = str(row.get("synopsis", "")).strip()
        earliest_year = row.get("earliest_year")
        if earliest_year is None or earliest_year == "":
            earliest_year = infer_earliest_year(synopsis)
        elif earliest_year is not None:
            try:
                earliest_year = int(earliest_year)
            except (TypeError, ValueError):
                earliest_year = infer_earliest_year(synopsis)

        route_scope = str(row.get("route_scope", "")).strip() or infer_route_scope(synopsis)
        prerequisite = str(row.get("prerequisite_quest_id", "")).strip()
        if not prerequisite:
            prerequisite = last_id_by_character.get(char_key, "")

        row.update(
            {
                "quest_id": quest_id,
                "narrative_work_id": str(row.get("narrative_work_id", "")).strip() or narrative_work_id,
                "main_character": character,
                "pool_sequence": pool_sequence,
                "earliest_year": earliest_year,
                "route_scope": route_scope,
                "prerequisite_quest_id": prerequisite or None,
                "year_gate": row.get("year_gate"),
                "unlock_note": str(row.get("unlock_note", "")).strip() or None,
            }
        )
        last_id_by_character[char_key] = quest_id
        normalized.append(row)

    return normalized


def load_quest_catalog(seed_path: Path | None = None) -> dict[str, Any]:
    path = seed_path or DEFAULT_SEED_PATH
    if not path.exists():
        return {"quests": [], "by_character": {}, "by_year": {}, "meta": {"seed_path": str(path)}}
    payload = read_json(path)
    seeds = payload.get("quest_song_seeds", []) if isinstance(payload, dict) else []
    quests = normalize_quest_seeds([row for row in seeds if isinstance(row, dict)])
    by_character: dict[str, list[dict[str, Any]]] = {}
    by_year: dict[int, list[dict[str, Any]]] = {}
    for quest in quests:
        char = str(quest.get("main_character", "")).strip()
        by_character.setdefault(char, []).append(quest)
        year = quest.get("earliest_year")
        if isinstance(year, int):
            by_year.setdefault(year, []).append(quest)
    for char in by_character:
        by_character[char].sort(key=lambda q: int(q.get("pool_sequence") or 0))
    return {
        "quests": quests,
        "by_character": by_character,
        "by_year": {k: by_year[k] for k in sorted(by_year)},
        "meta": {
            "seed_path": str(path),
            "narrative_work_id": THERIAC_CODA_WORK_ID,
            "quest_count": len(quests),
            "character_count": len(by_character),
        },
    }


def quests_for_work(catalog: dict[str, Any], work_id: str = THERIAC_CODA_WORK_ID) -> list[dict[str, Any]]:
    key = work_id.strip().lower()
    return [
        q
        for q in catalog.get("quests", [])
        if str(q.get("narrative_work_id", "")).strip().lower() == key
    ]
