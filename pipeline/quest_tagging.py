"""Quest tagging heuristics, discovery merge, and review queue (Stage 08Q core)."""

from __future__ import annotations

import re
from typing import Any

from pipeline.entity_resolution import normalized_name_key
from pipeline.quest_catalog import (
    _YEAR_RE,
    examples_by_title_key,
    infer_earliest_year,
    load_quest_examples,
    quest_key_from_label,
)
from pipeline.quest_motifs import match_artist_to_motif, normalize_artist_key
from pipeline.quest_tagging_paths import CODA_WORK_IDS
from pipeline.stage_07_entity_resolution import ARTIST_BY_PATTERN, MUSIC_EVIDENCE_MARKERS, MUSIC_QUEST_CONTEXT_MARKERS
from pipeline.stage_08_snippet_grouping import extract_artist_names, extract_music_signals

_POOL_FIRST_RE = re.compile(r"\bfirst\s+(?:quest|mission|arc)\b", re.IGNORECASE)
_POOL_NEXT_RE = re.compile(r"\bnext\s+(?:quest|mission|arc|in\s+(?:her|his|their)\s+(?:pool|line))\b", re.IGNORECASE)
_POOL_STEP_RE = re.compile(r"\b(?:pool\s+)?step\s+(\d+)\b", re.IGNORECASE)

QUEST_DESIGN_KEYWORDS = (
    "quest",
    "mission",
    "path b",
    "path a",
    "side route",
    "song title",
    "quest line",
    "quest pool",
    "quest map",
    "main route",
)


def quest_tagging_config(config: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(config, dict):
        return {"enabled": True}
    block = config.get("quest_tagging")
    return block if isinstance(block, dict) else {"enabled": False}


def snippet_text(snippet: dict[str, Any]) -> str:
    return " ".join(
        [
            str(snippet.get("display_text_normalized", "")),
            str(snippet.get("conversation_patch_summary", "")),
            " ".join(str(item) for item in snippet.get("conversation_patch_lore_developments", []) or []),
            str(snippet.get("patch_item_text", "")),
        ]
    ).strip()


def has_quest_design_signals(snippet: dict[str, Any]) -> bool:
    text = snippet_text(snippet).lower()
    if any(kw in text for kw in QUEST_DESIGN_KEYWORDS):
        return True
    if extract_music_signals(snippet_text(snippet)):
        return True
    thematic = snippet.get("thematic_tags", []) or []
    if any(str(t).startswith("music:") for t in thematic):
        return True
    if "possible_song_title_reference" in thematic or "possible_artist_reference" in thematic:
        return True
    for field in ("patch_item_type", "patch_update_type"):
        if str(snippet.get(field, "")).strip().lower() in {"quest", "quest_update", "path", "route", "mission"}:
            return True
    return False


def extract_artists_from_text(text: str) -> list[str]:
    artists: list[str] = extract_artist_names(text)
    lower = text.lower()
    for marker in MUSIC_EVIDENCE_MARKERS:
        if marker in lower and marker not in {"song", "track", "album", "music", "playlist", "ost", "soundtrack", "lyrics"}:
            # music markers like radiohead in config - skip generic
            pass
    for match in ARTIST_BY_PATTERN.finditer(text):
        artist = match.group(1).strip(" .,!?:;")
        if artist and len(artist) >= 3:
            artists.append(artist)
    # music: tags from snippet stored separately; caller merges
    return sorted(set(artists))


def artists_from_snippet(snippet: dict[str, Any]) -> list[str]:
    text = snippet_text(snippet)
    artists = extract_artists_from_text(text)
    for tag in snippet.get("thematic_tags", []) or []:
        if isinstance(tag, str) and tag.startswith("music:"):
            marker = tag[len("music:") :].strip()
            if marker:
                artists.append(marker.replace("_", " ").title())
    return sorted(set(artists))


def parse_chronology_guesses(text: str) -> dict[str, Any]:
    year = infer_earliest_year(text)
    pool_sequence = None
    chronology_source = None
    chronology_confidence = None
    if year is not None:
        chronology_source = "explicit_text"
        chronology_confidence = 0.85
    step_match = _POOL_STEP_RE.search(text)
    if step_match:
        try:
            pool_sequence = int(step_match.group(1))
            chronology_source = chronology_source or "explicit_text"
            chronology_confidence = max(chronology_confidence or 0, 0.75)
        except ValueError:
            pass
    elif _POOL_FIRST_RE.search(text):
        pool_sequence = 1
        chronology_source = chronology_source or "explicit_text"
        chronology_confidence = max(chronology_confidence or 0, 0.7)
    elif _POOL_NEXT_RE.search(text):
        chronology_source = chronology_source or "explicit_text"
        chronology_confidence = max(chronology_confidence or 0, 0.55)
    return {
        "earliest_year_guess": year,
        "pool_sequence_guess": pool_sequence,
        "chronology_source": chronology_source,
        "chronology_confidence": chronology_confidence,
    }


def _title_in_text(title: str, text: str) -> bool:
    title_lower = title.lower()
    blob = f" {normalized_name_key(text)} "
    if " " in title_lower:
        norm_title = normalized_name_key(title_lower)
        return bool(norm_title and f" {norm_title} " in blob)
    return bool(re.search(rf"\b{re.escape(title_lower)}\b", text.lower()))


def _characters_in_text(character_names: list[str], text: str) -> list[str]:
    lower = text.lower()
    found: list[str] = []
    for name in character_names:
        if name and name.lower() in lower:
            found.append(name)
    return found


def _base_tag_row(
    snippet: dict[str, Any],
    *,
    narrative_work_id: str,
    quest_label: str,
    source: str,
    match_kind: str,
    confidence: float,
    rationale: str,
    **extra: Any,
) -> dict[str, Any]:
    chronology = parse_chronology_guesses(snippet_text(snippet))
    row: dict[str, Any] = {
        "snippet_id": str(snippet.get("snippet_id", "")).strip(),
        "knowledge_track": str(snippet.get("knowledge_track", "lore")).strip() or "lore",
        "narrative_work_id": narrative_work_id,
        "quest_label": quest_label,
        "quest_key": quest_key_from_label(quest_label),
        "main_character": extra.get("main_character"),
        "character_confidence": extra.get("character_confidence"),
        "motif_id": extra.get("motif_id"),
        "artist_attributions": extra.get("artist_attributions") or [],
        "match_kind": match_kind,
        "confidence": round(float(confidence), 3),
        "earliest_year_guess": extra.get("earliest_year_guess", chronology.get("earliest_year_guess")),
        "pool_sequence_guess": extra.get("pool_sequence_guess", chronology.get("pool_sequence_guess")),
        "chronology_confidence": extra.get("chronology_confidence", chronology.get("chronology_confidence")),
        "chronology_source": extra.get("chronology_source", chronology.get("chronology_source")),
        "external_lookup_used": bool(extra.get("external_lookup_used", False)),
        "needs_external_lookup": bool(extra.get("needs_external_lookup", False)),
        "source": source,
        "rationale": rationale,
    }
    return row


def resolve_character_from_motif(
    motif: dict[str, Any] | None,
    *,
    cast_characters: list[str],
) -> tuple[str | None, float, str | None]:
    if not motif:
        return None, 0.0, None
    motif_id = str(motif.get("motif_id", "")).strip()
    primary = str(motif.get("primary_character", "") or "").strip() or None
    if primary:
        return primary, 0.82, motif_id
    if cast_characters:
        return cast_characters[0], 0.58, motif_id
    return None, 0.35, motif_id


def heuristic_tags_for_snippet(
    snippet: dict[str, Any],
    *,
    narrative_work_id: str,
    examples_index: dict[str, dict[str, Any]],
    discovered_index: dict[str, dict[str, Any]],
    motifs: list[dict[str, Any]],
    character_names: list[str],
    artist_bindings: dict[str, dict[str, Any]],
    known_titles: list[str],
) -> list[dict[str, Any]]:
    text = snippet_text(snippet)
    if not text:
        return []
    tags: list[dict[str, Any]] = []
    artists = artists_from_snippet(snippet)
    cast = _characters_in_text(character_names, text)

    matched_titles: list[str] = []
    for title in known_titles:
        if _title_in_text(title, text):
            matched_titles.append(title)

    for title in matched_titles:
        key = quest_key_from_label(title)
        ex = examples_index.get(key) or discovered_index.get(key)
        main_character = str(ex.get("main_character", "")).strip() if ex else None
        motif_id = str(ex.get("motif_id", "")).strip() if ex else None
        char_conf = 0.9 if main_character else 0.5
        if not main_character and cast:
            main_character = cast[0]
            char_conf = 0.65
        tags.append(
            _base_tag_row(
                snippet,
                narrative_work_id=narrative_work_id,
                quest_label=title,
                source="heuristic",
                match_kind="exact_known",
                confidence=0.88 if main_character else 0.62,
                rationale=f"Exact quest title '{title}' found in snippet text.",
                main_character=main_character,
                character_confidence=char_conf,
                motif_id=motif_id or None,
                artist_attributions=artists,
            )
        )

    if tags:
        return tags

    # Motif inference: artists in text → character
    for artist in artists:
        binding = artist_bindings.get(normalize_artist_key(artist))
        if binding:
            motif = {"motif_id": binding.get("motif_id"), "primary_character": binding.get("primary_character")}
        else:
            motif = match_artist_to_motif(artist, motifs)
        character, char_conf, motif_id = resolve_character_from_motif(motif, cast_characters=cast)
        # Try to extract quoted title or use artist-only tag with low label
        quoted = re.findall(r'"([^"]{3,80})"', text)
        quest_label = quoted[0] if quoted else artist
        if character and char_conf >= 0.55:
            tags.append(
                _base_tag_row(
                    snippet,
                    narrative_work_id=narrative_work_id,
                    quest_label=quest_label,
                    source="heuristic",
                    match_kind="motif_inferred",
                    confidence=min(0.85, char_conf + 0.05),
                    rationale=f"Artist '{artist}' maps to motif '{motif_id}' → {character}.",
                    main_character=character,
                    character_confidence=char_conf,
                    motif_id=motif_id,
                    artist_attributions=[artist],
                )
            )
        elif motif_id == "other" or (motif and motif.get("requires_review")):
            tags.append(
                _base_tag_row(
                    snippet,
                    narrative_work_id=narrative_work_id,
                    quest_label=quest_label,
                    source="heuristic",
                    match_kind="motif_inferred",
                    confidence=0.42,
                    rationale=f"Artist '{artist}' in other/review motif; character unresolved.",
                    main_character=cast[0] if cast else None,
                    character_confidence=0.45 if cast else 0.25,
                    motif_id="other",
                    artist_attributions=[artist],
                    needs_external_lookup=True,
                )
            )

    if tags:
        return tags

    # Cast inference when quest context + characters but no title
    lower = text.lower()
    has_quest_ctx = any(re.search(rf"\b{re.escape(m)}\b", lower) for m in MUSIC_QUEST_CONTEXT_MARKERS)
    has_music = any(re.search(rf"\b{re.escape(m)}\b", lower) for m in MUSIC_EVIDENCE_MARKERS) or bool(
        extract_music_signals(text)
    )
    if has_quest_ctx and has_music and cast:
        tags.append(
            _base_tag_row(
                snippet,
                narrative_work_id=narrative_work_id,
                quest_label=cast[0] + " quest (untitled)",
                source="heuristic",
                match_kind="cast_inferred",
                confidence=0.48,
                rationale=f"Quest/music context with characters {', '.join(cast[:3])}; no title matched.",
                main_character=cast[0],
                character_confidence=0.5,
                motif_id=None,
                artist_attributions=artists,
                needs_external_lookup=not artists,
            )
        )
    return tags


def select_snippets_for_tagging(
    snippets: list[dict[str, Any]],
    work_tags: dict[str, str],
    cfg: dict[str, Any],
) -> list[dict[str, Any]]:
    max_snippets = int(cfg.get("max_snippets", 600) or 600)
    include_meta = bool(cfg.get("include_meta_track", True))
    selected: list[dict[str, Any]] = []
    for snippet in snippets:
        track = str(snippet.get("knowledge_track", "")).strip().lower()
        if track == "meta" and not include_meta:
            continue
        if track not in {"lore", "meta"}:
            continue
        snippet_id = str(snippet.get("snippet_id", "")).strip()
        work_id = work_tags.get(snippet_id, "").strip()
        if work_id in CODA_WORK_IDS or has_quest_design_signals(snippet):
            selected.append(snippet)
    # meta first for quest design rescue, then lore
    meta = [s for s in selected if str(s.get("knowledge_track", "")).lower() == "meta"]
    lore = [s for s in selected if s not in meta]
    ordered = meta + lore
    return ordered[:max_snippets]


def merge_discovered_quests(
    existing: dict[str, Any] | None,
    tag_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    quests: dict[str, dict[str, Any]] = {}
    if isinstance(existing, dict):
        for row in existing.get("quests", []) or []:
            if isinstance(row, dict):
                key = str(row.get("quest_key", "")).strip()
                if key:
                    quests[key] = dict(row)

    for tag in tag_rows:
        label = str(tag.get("quest_label", "")).strip()
        key = str(tag.get("quest_key", "")).strip() or quest_key_from_label(label)
        if not label or not key:
            continue
        if "untitled" in label.lower():
            continue
        row = quests.get(key, {})
        snippet_id = str(tag.get("snippet_id", "")).strip()
        snippet_ids = list(row.get("snippet_ids", []) or [])
        if snippet_id and snippet_id not in snippet_ids:
            snippet_ids.append(snippet_id)
        aliases = list(row.get("aliases", []) or [])
        if label not in aliases and label != row.get("quest_label"):
            aliases.append(label)
        quests[key] = {
            "quest_key": key,
            "quest_label": row.get("quest_label") or label,
            "main_character": tag.get("main_character") or row.get("main_character"),
            "motif_id": tag.get("motif_id") or row.get("motif_id"),
            "snippet_ids": snippet_ids,
            "aliases": aliases,
            "earliest_year_guess": tag.get("earliest_year_guess") if tag.get("earliest_year_guess") is not None else row.get("earliest_year_guess"),
            "pool_sequence_guess": tag.get("pool_sequence_guess") if tag.get("pool_sequence_guess") is not None else row.get("pool_sequence_guess"),
            "chronology_pinned": bool(row.get("chronology_pinned", False)),
            "confidence": max(float(row.get("confidence", 0) or 0), float(tag.get("confidence", 0) or 0)),
        }
    return {"quests": sorted(quests.values(), key=lambda q: str(q.get("quest_label", "")).lower())}


def build_artist_review_queue(
    tag_rows: list[dict[str, Any]],
    *,
    threshold: float = 0.65,
) -> list[dict[str, Any]]:
    groups: dict[str, dict[str, Any]] = {}
    for tag in tag_rows:
        char_conf = float(tag.get("character_confidence", 0) or 0)
        motif_id = str(tag.get("motif_id", "")).strip()
        if char_conf >= threshold and motif_id != "other":
            continue
        for artist in tag.get("artist_attributions", []) or []:
            artist_label = str(artist).strip()
            if not artist_label:
                continue
            key = normalize_artist_key(artist_label)
            if key not in groups:
                groups[key] = {
                    "artist_label": artist_label,
                    "artist_normalized": key,
                    "snippet_ids": [],
                    "example_quest_titles": [],
                    "candidate_characters": [],
                    "question": f"Is artist '{artist_label}' associated with a Theriac character quest line? If so, which character?",
                }
            g = groups[key]
            sid = str(tag.get("snippet_id", "")).strip()
            if sid and sid not in g["snippet_ids"]:
                g["snippet_ids"].append(sid)
            title = str(tag.get("quest_label", "")).strip()
            if title and title not in g["example_quest_titles"]:
                g["example_quest_titles"].append(title)
            char = str(tag.get("main_character", "")).strip()
            if char and char not in g["candidate_characters"]:
                g["candidate_characters"].append(char)
    return sorted(groups.values(), key=lambda row: row["artist_label"].lower())


def apply_override_tags(
    snippet: dict[str, Any],
    overrides: list[dict[str, Any]],
    narrative_work_id: str,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for ov in overrides:
        label = str(ov.get("quest_label", "")).strip()
        if not label:
            continue
        rows.append(
            _base_tag_row(
                snippet,
                narrative_work_id=str(ov.get("narrative_work_id", "")).strip() or narrative_work_id,
                quest_label=label,
                source="review_override",
                match_kind="exact_known",
                confidence=1.0,
                rationale="review_memory quest_tag_overrides",
                main_character=str(ov.get("main_character", "")).strip() or None,
                character_confidence=1.0 if ov.get("main_character") else None,
                motif_id=str(ov.get("motif_id", "")).strip() or None,
                artist_attributions=ov.get("artist_attributions") or [],
            )
        )
    return rows


def known_titles_from_indexes(
    examples_index: dict[str, dict[str, Any]],
    discovered_index: dict[str, dict[str, Any]],
) -> list[str]:
    titles: set[str] = set()
    for idx in (examples_index, discovered_index):
        for row in idx.values():
            title = str(row.get("quest_label", "")).strip()
            if title:
                titles.add(title)
    return sorted(titles)
