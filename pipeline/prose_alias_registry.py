from __future__ import annotations

import re
from typing import Any

from pipeline.entity_resolution import normalized_name_key


def _stage_11():
    from pipeline import stage_11_card_synthesis as stage_11

    return stage_11


def prose_canonical_aliases_from_config(config: dict[str, Any] | None) -> list[dict[str, str]]:
    if not isinstance(config, dict):
        return []
    rows = config.get("prose_canonical_aliases")
    if not isinstance(rows, list):
        card_first = config.get("card_first_synthesis")
        if isinstance(card_first, dict):
            rows = card_first.get("prose_canonical_aliases")
    if not isinstance(rows, list):
        return []
    cleaned: list[dict[str, str]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        alias = str(row.get("alias", "")).strip()
        canonical = str(row.get("canonical", "") or row.get("canonical_name", "")).strip()
        if alias and canonical:
            cleaned.append({"alias": alias, "canonical": canonical})
    return cleaned


def build_global_prose_alias_pairs(
    entities: list[dict[str, Any]],
    review_memory: dict[str, Any] | None = None,
    config: dict[str, Any] | None = None,
) -> list[tuple[str, str]]:
    """Alias → canonical replacements for other entities mentioned in card/snippet prose."""
    memory = review_memory if isinstance(review_memory, dict) else {}
    seen_keys: set[str] = set()
    pairs: list[tuple[str, str]] = []

    def add(alias: str, canonical: str) -> None:
        alias_text = str(alias).strip()
        canonical_text = str(canonical).strip()
        if not alias_text or not canonical_text:
            return
        alias_key = normalized_name_key(alias_text)
        canonical_key = normalized_name_key(canonical_text)
        if alias_key == canonical_key or alias_key in seen_keys:
            return
        seen_keys.add(alias_key)
        pairs.append((alias_text, canonical_text))

    for entity in entities:
        if not isinstance(entity, dict):
            continue
        canonical = str(entity.get("canonical_name", "")).strip()
        if not canonical:
            continue
        for term in _stage_11().entity_alias_terms_for_normalization(entity):
            add(term, canonical)

    for item in memory.get("approved_aliases", []) or []:
        if not isinstance(item, dict):
            continue
        add(str(item.get("alias_text", "")), str(item.get("canonical_name", "")))

    for item in memory.get("entity_merges", []) or []:
        if not isinstance(item, dict):
            continue
        canonical = str(item.get("canonical_name", "") or item.get("target_entity_name", "")).strip()
        if not canonical:
            continue
        for field in ("source_entity_name", "alias_text"):
            add(str(item.get(field, "")), canonical)

    for item in prose_canonical_aliases_from_config(config):
        add(item["alias"], item["canonical"])

    pairs.sort(key=lambda pair: len(pair[0]), reverse=True)
    return pairs


def normalize_prose_with_alias_pairs(text: str, alias_pairs: list[tuple[str, str]]) -> str:
    if not text or not alias_pairs:
        return str(text or "")
    stage_11 = _stage_11()
    result = str(text)
    for alias, canonical in alias_pairs:
        if not stage_11.should_normalize_entity_aliases_in_text(result, canonical, [alias]):
            continue
        pattern = re.compile(r"(?<!\w)" + re.escape(alias) + r"(?!\w)", re.IGNORECASE)
        result = pattern.sub(canonical, result)
    return result


def normalize_prose_for_entity(
    text: str,
    entity: dict[str, Any],
    global_alias_pairs: list[tuple[str, str]] | None = None,
) -> str:
    stage_11 = _stage_11()
    canonical = str(entity.get("canonical_name", "")).strip()
    subject_terms = stage_11.entity_alias_terms_for_normalization(entity)
    result = stage_11.normalize_prose_to_canonical_name(str(text or ""), canonical, subject_terms)
    if not global_alias_pairs:
        return result
    subject_keys = {normalized_name_key(canonical)} | {normalized_name_key(term) for term in subject_terms}
    filtered = [
        (alias, target)
        for alias, target in global_alias_pairs
        if normalized_name_key(alias) not in subject_keys
    ]
    return normalize_prose_with_alias_pairs(result, filtered)


def sanitize_card_prose_whitespace(text: str) -> str:
    """Collapse stray line breaks inside paragraphs (fixes mid-word splits in Notion)."""
    if not text:
        return ""
    cleaned = str(text).replace("\r\n", "\n").replace("\r", "\n")
    cleaned = re.sub(r"[^\S\n]+", " ", cleaned)
    cleaned = re.sub(r"(?<!\n)\n(?!\n)", " ", cleaned)
    return re.sub(r" +", " ", cleaned).strip()


_PERSONAL_AUTHOR_PATTERNS = (
    r"\b(on a personal level)\b",
    r"\b(in my life)\b",
    r"\b(personally)\b",
    r"\bfor me\b",
    r"\bhelped me\b",
    r"\bmade me\b",
    r"\bi felt\b",
    r"\bi was\b",
    r"\bi am\b",
    r"\bi\b",
    r"\bmy\b",
    r"\bme\b",
    r"\bmine\b",
)


def scrub_personal_author_references(text: str) -> str:
    """Remove first-person / author-personal-life sentences from meta prose.

    This is intentionally conservative: it only removes whole sentences/lines that
    look like autobiographical commentary, and leaves other factual inspiration notes intact.
    """
    clean = sanitize_card_prose_whitespace(text)
    if not clean:
        return ""
    pattern = re.compile("|".join(_PERSONAL_AUTHOR_PATTERNS), re.IGNORECASE)

    # Split on sentence-ish boundaries while keeping reasonable fidelity.
    parts = re.split(r"(?<=[.!?])\s+", clean)
    kept: list[str] = []
    for part in parts:
        candidate = part.strip()
        if not candidate:
            continue
        if pattern.search(candidate):
            continue
        kept.append(candidate)
    return " ".join(kept).strip()


def apply_prose_normalization_to_synthesis(
    synthesis: dict[str, Any],
    entity: dict[str, Any],
    global_alias_pairs: list[tuple[str, str]] | None = None,
) -> None:
    if not isinstance(synthesis, dict):
        return
    pairs = global_alias_pairs or []
    synthesis["summary"] = sanitize_card_prose_whitespace(
        normalize_prose_for_entity(str(synthesis.get("summary", "")), entity, pairs)
    )
    sections = synthesis.get("sections")
    if isinstance(sections, dict):
        normalized: dict[str, str] = {}
        for key, value in sections.items():
            out = sanitize_card_prose_whitespace(normalize_prose_for_entity(str(value), entity, pairs))
            if key == "inspirations":
                out = scrub_personal_author_references(out)
            normalized[key] = out
        synthesis["sections"] = normalized
    for rel in synthesis.get("relationships", []) or []:
        if not isinstance(rel, dict):
            continue
        rel["target_entity_name"] = normalize_prose_for_entity(
            str(rel.get("target_entity_name", "")),
            entity,
            pairs,
        )
        rel["note"] = sanitize_card_prose_whitespace(
            normalize_prose_for_entity(str(rel.get("note", "")), entity, pairs)
        )
    for item in synthesis.get("timeline", []) or []:
        if not isinstance(item, dict):
            continue
        item["description"] = sanitize_card_prose_whitespace(
            normalize_prose_for_entity(str(item.get("description", "")), entity, pairs)
        )
    for link in synthesis.get("wiki_links", []) or []:
        if not isinstance(link, dict):
            continue
        link["target_entity_name"] = normalize_prose_for_entity(
            str(link.get("target_entity_name", "")),
            entity,
            pairs,
        )


def normalize_card_prose_aliases(
    card: dict[str, Any],
    entity: dict[str, Any],
    global_alias_pairs: list[tuple[str, str]] | None = None,
) -> dict[str, Any]:
    pairs = global_alias_pairs or []
    card = dict(card)
    card["summary"] = sanitize_card_prose_whitespace(
        normalize_prose_for_entity(str(card.get("summary", "")), entity, pairs)
    )
    details = card.get("details")
    if isinstance(details, dict):
        sections = details.get("sections")
        if isinstance(sections, dict):
            details = {**details, "sections": {
                key: sanitize_card_prose_whitespace(normalize_prose_for_entity(str(value), entity, pairs))
                for key, value in sections.items()
            }}
            card["details"] = details
    for rel in card.get("relationships", []) or []:
        if not isinstance(rel, dict):
            continue
        rel["target_entity_name"] = normalize_prose_for_entity(
            str(rel.get("target_entity_name", "")),
            entity,
            pairs,
        )
        rel["note"] = sanitize_card_prose_whitespace(
            normalize_prose_for_entity(str(rel.get("note", "")), entity, pairs)
        )
    for item in card.get("timeline", []) or []:
        if not isinstance(item, dict):
            continue
        item["description"] = sanitize_card_prose_whitespace(
            normalize_prose_for_entity(str(item.get("description", "")), entity, pairs)
        )
    return card
