from __future__ import annotations

import re
from collections import defaultdict
from pathlib import Path
from typing import Any

from pipeline.common import read_json, stable_id


BLOCKED_NAME_KEYS = {
    "ai",
    "ai infrastructure",
    "ai systems",
    "alternate geography context",
    "core premise",
    "history",
    "key organizations",
    "placeholders",
    "project overview",
    "remaining questions",
    "satellite character",
    "terminology",
    "the lab crew 4",
    "theriac coda lore bible 1",
    "world ai systems",
}

CANONICAL_OVERRIDES = {
    "gfns": "Global Federation of Nation States",
    "global federation of nation states": "Global Federation of Nation States",
    "hectr": "HECTR",
    "lab": "The Lab",
    "the lab": "The Lab",
    "ruinr": "RUINR",
    "joy": "Joy Roberts",
    "joy roberts": "Joy Roberts",
}

ENTITY_TYPE_ALIASES = {"ai_system": "character", "ai system": "character", "ai systems": "character"}
DISALLOWED_ENTITY_TYPES = {"theme"}
ENTITY_TYPES = {"character", "faction", "organization", "location", "quest", "event", "timeline_node", "term"}


def normalize_entity_type(entity_type: Any, default: str = "term") -> str:
    raw = str(entity_type or "").strip().lower()
    raw = ENTITY_TYPE_ALIASES.get(raw, raw)
    if raw in ENTITY_TYPES:
        return raw
    return default if default in ENTITY_TYPES else "term"


def is_disallowed_entity_type(entity_type: Any) -> bool:
    raw = str(entity_type or "").strip().lower()
    raw = ENTITY_TYPE_ALIASES.get(raw, raw)
    return raw in DISALLOWED_ENTITY_TYPES


def display_name(value: str) -> str:
    cleaned = clean_candidate_name(value)
    key = normalized_name_key(cleaned)
    if key in CANONICAL_OVERRIDES:
        return CANONICAL_OVERRIDES[key]
    if cleaned.isupper() and len(cleaned) <= 8:
        return cleaned
    return cleaned.title()


def clean_candidate_name(value: str) -> str:
    text = re.sub(r"\s+", " ", str(value)).strip()
    text = text.strip(" \t\r\n-_/\\:;,.")
    text = re.sub(r"\s+[:/]+$", "", text).strip()
    text = re.sub(r"\s+", " ", text)
    parts = text.split()
    deduped: list[str] = []
    for part in parts:
        if deduped and deduped[-1].lower().strip(":") == part.lower().strip(":"):
            continue
        deduped.append(part)
    return " ".join(deduped).strip(" \t\r\n-_/\\:;,.")


def normalized_name_key(value: str) -> str:
    text = clean_candidate_name(value).lower()
    text = re.sub(r"[^a-z0-9]+", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def is_blocked_seed_name(value: str) -> bool:
    key = normalized_name_key(value)
    if not key or key in BLOCKED_NAME_KEYS:
        return True
    if len(key) <= 2:
        return True
    if key.isdigit():
        return True
    if key in {"chapter", "section", "notes", "todo", "questions"}:
        return True
    return False


def entity_seed_id(name: str) -> str:
    return stable_id("entity_seed", normalized_name_key(name))


def entity_id(name: str) -> str:
    return stable_id("entity", normalized_name_key(name))


def card_id_for_entity(name: str) -> str:
    return stable_id("card", normalized_name_key(name))


def load_entity_records(path: Path | None) -> list[dict[str, Any]]:
    if not path or not path.exists():
        return []
    payload = read_json(path)
    if isinstance(payload, dict):
        if isinstance(payload.get("resolved_entities"), list):
            return [
                {**x, "entity_type": normalize_entity_type(x.get("entity_type", "term"))}
                for x in payload["resolved_entities"]
                if isinstance(x, dict) and not is_disallowed_entity_type(x.get("entity_type"))
            ]
        if isinstance(payload.get("entities"), list):
            return [
                {**x, "entity_type": normalize_entity_type(x.get("entity_type", "term"))}
                for x in payload["entities"]
                if isinstance(x, dict) and not is_disallowed_entity_type(x.get("entity_type"))
            ]
        if isinstance(payload.get("cards"), list):
            # Backwards-compatible reader for old artifacts.
            out: list[dict[str, Any]] = []
            for card in payload["cards"]:
                if not isinstance(card, dict):
                    continue
                name = str(card.get("canonical_name", "")).strip()
                if not name:
                    continue
                if is_disallowed_entity_type(card.get("entity_type", "term")):
                    continue
                out.append(
                    {
                        "entity_id": entity_id(name),
                        "entity_seed_id": entity_seed_id(name),
                        "canonical_name": name,
                        "entity_type": normalize_entity_type(card.get("entity_type", "term")),
                        "aliases": card.get("aliases", []),
                        "seed_status": "active",
                    }
                )
            return out
    return []


def load_entity_names(path: Path | None) -> list[str]:
    names: list[str] = []
    for entity in load_entity_records(path):
        if str(entity.get("seed_status", "active")) == "blocked_seed":
            continue
        name = str(entity.get("canonical_name", "")).strip()
        if name:
            names.append(name)
        for alias in entity.get("aliases", []) or []:
            alias_text = str(alias).strip()
            if alias_text:
                names.append(alias_text)
    return sorted(dict.fromkeys(names), key=lambda x: x.lower())


def resolve_entities(seed_entities: list[dict[str, Any]], review_memory: dict[str, Any] | None = None) -> dict[str, Any]:
    memory = review_memory or {}
    approved_merges = memory.get("approved_aliases", []) or []
    alias_overrides: dict[str, str] = {}
    for item in approved_merges:
        if not isinstance(item, dict):
            continue
        alias = normalized_name_key(str(item.get("alias_text", "")))
        target = str(item.get("canonical_name", "")).strip()
        if alias and target:
            alias_overrides[alias] = display_name(target)
    for item in memory.get("entity_merges", []) or []:
        if not isinstance(item, dict):
            continue
        target = str(item.get("target_entity_name", "")).strip()
        if not target:
            continue
        for alias_value in [item.get("source_entity_name", ""), item.get("alias_text", "")]:
            alias = normalized_name_key(str(alias_value))
            if alias:
                alias_overrides[alias] = display_name(target)

    seed_alias_targets: dict[str, set[str]] = defaultdict(set)
    for seed in seed_entities:
        seed_status = str(seed.get("seed_status", "active"))
        raw_name = str(seed.get("canonical_name", "")).strip()
        cleaned = clean_candidate_name(raw_name)
        if seed_status == "blocked_seed" or is_blocked_seed_name(cleaned):
            continue
        canonical = CANONICAL_OVERRIDES.get(normalized_name_key(cleaned)) or display_name(cleaned)
        canonical_key = normalized_name_key(canonical)
        for alias in seed.get("aliases", []) or []:
            alias_key = normalized_name_key(str(alias))
            if alias_key and alias_key != canonical_key:
                seed_alias_targets[alias_key].add(canonical)
    seed_alias_overrides = {
        alias_key: next(iter(targets))
        for alias_key, targets in seed_alias_targets.items()
        if len(targets) == 1
    }

    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    blocked: list[dict[str, Any]] = []
    for seed in seed_entities:
        raw_name = str(seed.get("canonical_name", "")).strip()
        cleaned = clean_candidate_name(raw_name)
        key = normalized_name_key(cleaned)
        canonical = alias_overrides.get(key) or CANONICAL_OVERRIDES.get(key) or seed_alias_overrides.get(key) or display_name(cleaned)
        canonical_key = normalized_name_key(canonical)
        seed_status = str(seed.get("seed_status", "active"))
        if is_disallowed_entity_type(seed.get("entity_type", "")):
            blocked.append({**seed, "seed_status": "blocked_seed", "blocked_reason": "themes_are_profile_data_not_entities"})
            continue
        if seed_status == "blocked_seed" or is_blocked_seed_name(cleaned):
            blocked.append({**seed, "seed_status": "blocked_seed", "blocked_reason": "generic_or_malformed_heading"})
            continue
        grouped[canonical_key].append({**seed, "canonical_name": cleaned})

    resolved: list[dict[str, Any]] = []
    for key, items in grouped.items():
        canonical_name = CANONICAL_OVERRIDES.get(key) or display_name(items[0].get("canonical_name", key))
        aliases = set()
        seed_ids = []
        relationship_hints: list[dict[str, Any]] = []
        type_counts: dict[str, int] = defaultdict(int)
        for item in items:
            name = display_name(str(item.get("canonical_name", "")))
            if name and name != canonical_name:
                aliases.add(name)
            for alias in item.get("aliases", []) or []:
                alias_text = display_name(str(alias))
                if alias_text and alias_text != canonical_name:
                    aliases.add(alias_text)
            if item.get("entity_seed_id"):
                seed_ids.append(str(item["entity_seed_id"]))
            for hint in item.get("relationship_hints", []) or []:
                if isinstance(hint, dict):
                    relationship_hints.append(hint)
            entity_type = normalize_entity_type(item.get("entity_type", "term"))
            type_counts[entity_type] += 1
        entity_type = sorted(type_counts.items(), key=lambda x: (-x[1], x[0]))[0][0] if type_counts else "term"
        resolved.append(
            {
                "entity_id": entity_id(canonical_name),
                "card_id": card_id_for_entity(canonical_name),
                "canonical_name": canonical_name,
                "entity_type": entity_type,
                "aliases": sorted(aliases),
                "seed_entity_ids": sorted(set(seed_ids)),
                "relationship_hints": relationship_hints,
                "resolution_status": "resolved",
            }
        )

    return {
        "resolved_entities": sorted(resolved, key=lambda x: (x["entity_type"], x["canonical_name"])),
        "blocked_entities": sorted(blocked, key=lambda x: str(x.get("canonical_name", ""))),
    }
