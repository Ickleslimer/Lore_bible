from __future__ import annotations

import argparse
import re
import zipfile
from collections import defaultdict
from pathlib import Path
from typing import Any

from pipeline.common import get_logger, read_json, write_json
from pipeline.entity_resolution import (
    clean_candidate_name,
    display_name,
    entity_seed_id,
    is_blocked_seed_name,
    normalized_name_key,
)
from pipeline.mixtral_anchor_provider import build_stage_a_prompt, call_mixtral_chat, model_call_kwargs
from pipeline.thematic_profile import update_runtime_profile


SECTION_TO_ENTITY_TYPE = {
    "KEY ORGANIZATIONS": "organization",
    "THE KRYPTEIA": "organization",
    "AI INFRASTRUCTURE": "character",
    "KEY QUEST": "quest",
    "QUEST": "quest",
    "THEME": "theme",
    "TIMELINE": "timeline_node",
    "CHARACTER": "character",
    "FACTION": "faction",
    "TERMINOLOGY": "term"
}


def read_docx_text(docx_path: Path) -> str:
    with zipfile.ZipFile(docx_path) as z:
        raw = z.read("word/document.xml").decode("utf-8", errors="replace")
    chunks = re.findall(r"<w:t[^>]*>(.*?)</w:t>", raw)
    return " ".join(chunks)


def infer_entities(text: str) -> list[dict[str, Any]]:
    entities: list[dict[str, Any]] = []
    seen: set[str] = set()
    section_hits: defaultdict[str, list[str]] = defaultdict(list)

    # Heading-like sequences.
    for match in re.finditer(r"\b([A-Z][A-Z0-9'/: -]{3,})\b", text):
        token = re.sub(r"\s+", " ", match.group(1)).strip(" -")
        if len(token) < 4 or token.isdigit():
            continue
        section_hits[token].append(token)

    for section in section_hits:
        normalized = clean_candidate_name(section)
        key = normalized_name_key(normalized)
        if not key or key in seen:
            continue
        seen.add(key)
        if is_blocked_seed_name(normalized):
            continue
        entity_type = "term"
        for key, value in SECTION_TO_ENTITY_TYPE.items():
            if key in normalized:
                entity_type = value
                break
        entities.append(
            {
                "entity_seed_id": entity_seed_id(normalized),
                "entity_type": entity_type,
                "canonical_name": display_name(normalized),
                "aliases": [],
                "seed_status": "active",
                "source_section_hints": [normalized],
                "relationship_hints": [],
                "bootstrap_origin": "lore_bible_heading",
                "confidence": {"score": 0.45, "reviewer_note": "Ontology seed only; not canon evidence."},
            }
        )

    return sorted(entities, key=lambda x: (x["entity_type"], x["canonical_name"]))


def infer_entities_mixtral(text: str, config: dict[str, Any]) -> dict[str, Any] | None:
    logger = get_logger(__name__)
    excerpt_chars = int(config.get("stage_a_mixtral_excerpt_chars", 24000))
    prompt = build_stage_a_prompt(text[:excerpt_chars])
    call_kwargs = model_call_kwargs(config, "stage_a_bootstrap")
    response = call_mixtral_chat(
        prompt=prompt,
        **call_kwargs,
    )
    if not isinstance(response, dict):
        logger.debug("Stage 01 model response fallback: provider returned no JSON object.")
        return None
    raw_entities = response.get("entities")
    if not isinstance(raw_entities, list):
        logger.debug("Stage 01 model response fallback: missing/invalid `entities` list. keys=%s", sorted(response.keys()))
        return None
    allowed_entity_types = {
        "character",
        "faction",
        "organization",
        "location",
        "quest",
        "event",
        "timeline_node",
        "theme",
        "term",
    }
    entities: list[dict[str, Any]] = []
    for item in raw_entities:
        if not isinstance(item, dict):
            continue
        name = str(item.get("canonical_name", "")).strip()
        entity_type = str(item.get("entity_type", "term")).strip()
        if entity_type == "ai_system":
            entity_type = "character"
        source_section_hint = str(item.get("source_section_hint") or item.get("summary", "")).strip()
        aliases_raw = item.get("aliases", [])
        aliases = []
        if isinstance(aliases_raw, list):
            for alias in aliases_raw:
                alias_text = str(alias).strip()
                if alias_text and alias_text.lower() != name.lower():
                    aliases.append(alias_text)
        rels_raw = item.get("relationship_hints", [])
        relationship_hints = []
        if isinstance(rels_raw, list):
            for rel in rels_raw:
                if not isinstance(rel, dict):
                    continue
                target_name = str(rel.get("target_name", "")).strip()
                relation_type = str(rel.get("relation_type", "")).strip()
                note = str(rel.get("note", "")).strip()
                if not target_name or not relation_type:
                    continue
                relationship_hints.append(
                    {
                        "target_name": target_name,
                        "relation_type": relation_type,
                        "note": note or "Relationship hint from Stage 01 model extraction.",
                    }
                )
        if not name:
            continue
        cleaned_name = clean_candidate_name(name)
        if is_blocked_seed_name(cleaned_name):
            continue
        if entity_type not in allowed_entity_types:
            entity_type = "term"
        entities.append(
            {
                "entity_seed_id": entity_seed_id(cleaned_name),
                "entity_type": entity_type if entity_type else "term",
                "canonical_name": display_name(cleaned_name),
                "aliases": sorted(set(aliases)),
                "seed_status": "active",
                "source_section_hints": [source_section_hint] if source_section_hint else [],
                "relationship_hints": relationship_hints,
                "bootstrap_origin": "lore_bible_bootstrap_model",
                "confidence": {"score": 0.65, "reviewer_note": "Ontology seed only; not canon evidence."},
            }
        )
    if not entities:
        logger.debug("Stage 01 model response fallback: `entities` parsed but no usable entity seeds were produced.")
        return None
    dedup: dict[str, dict[str, Any]] = {}
    for c in entities:
        dedup[normalized_name_key(c["canonical_name"])] = c
    suggested = response.get("suggested_thematic_markers", {}) if isinstance(response, dict) else {}
    historical = []
    music = []
    if isinstance(suggested, dict):
        historical = [str(x).strip().lower() for x in (suggested.get("historical") or []) if str(x).strip()]
        music = [str(x).strip().lower() for x in (suggested.get("music") or []) if str(x).strip()]
    return {
            "entities": sorted(dedup.values(), key=lambda x: (x["entity_type"], x["canonical_name"])),
        "suggested_historical_markers": sorted(set(historical)),
        "suggested_music_markers": sorted(set(music)),
    }


def run(
    docx_path: Path,
    out_seed: Path,
    out_schema_descriptor: Path,
    in_pipeline_config_json: Path | None = None,
    thematic_runtime_path: Path | None = None,
) -> None:
    logger = get_logger(__name__)
    logger.info("Stage 01: loading lore bible DOCX from %s", docx_path)
    text = read_docx_text(docx_path)
    logger.debug("Stage 01: extracted %d characters from DOCX text.", len(text))
    config: dict[str, Any] = {}
    if in_pipeline_config_json and in_pipeline_config_json.exists():
        config = read_json(in_pipeline_config_json)
    stage_a_provider = str(config.get("stage_a_anchor_provider", "heuristic")).lower()
    logger.info("Stage 01: provider mode is '%s'.", stage_a_provider)
    entities = infer_entities(text)
    logger.debug("Stage 01: heuristic extraction produced %d entity seed(s).", len(entities))
    if stage_a_provider in {"mixtral", "hybrid"}:
        logger.info("Stage 01: requesting model bootstrap extraction...")
        model_result = infer_entities_mixtral(text, config)
        if model_result and isinstance(model_result, dict):
            model_cards = list(model_result.get("entities", model_result.get("cards", [])))
            logger.info("Stage 01: model extraction produced %d entity seed(s).", len(model_cards))
            if stage_a_provider == "mixtral":
                entities = model_cards
            else:
                merged: dict[str, dict[str, Any]] = {normalized_name_key(c["canonical_name"]): c for c in entities}
                for c in model_cards:
                    merged[normalized_name_key(c["canonical_name"])] = c
                entities = sorted(merged.values(), key=lambda x: (x["entity_type"], x["canonical_name"]))
                logger.info("Stage 01: hybrid merge produced %d combined entity seed(s).", len(entities))
            thematic_cfg = config.get("thematic_linking", {})
            runtime_updates_enabled = bool(thematic_cfg.get("runtime_updates_enabled", True))
            if runtime_updates_enabled and thematic_runtime_path is not None:
                min_support = int(thematic_cfg.get("runtime_min_support", 2))
                update_runtime_profile(
                    thematic_runtime_path,
                    "stage_a",
                    list(model_result.get("suggested_historical_markers", [])),
                    list(model_result.get("suggested_music_markers", [])),
                    min_support=min_support,
                )
        else:
            logger.warning("Stage 01: model extraction returned no usable output; keeping heuristic entity seeds.")
    payload = {
        "source": str(docx_path),
        "entity_count": len(entities),
        "provider_mode": stage_a_provider,
        "entities": entities,
    }
    write_json(out_seed, payload)
    write_json(
        out_schema_descriptor,
        {
            "entity_types": [
                "character",
                "faction",
                "organization",
                "location",
                "quest",
                "event",
                "timeline_node",
                "theme",
                "term"
            ],
            "notes": "Ontology seed for THERIAC lore cards. Not canon evidence."
        }
    )
    logger.info("Stage 01 complete: wrote %d entity seed(s).", len(entities))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--docx", type=Path, required=True)
    parser.add_argument("--out-entity-seed", "--out-seed", dest="out_seed", type=Path, required=True)
    parser.add_argument("--out-schema-descriptor", type=Path, required=True)
    parser.add_argument("--in-pipeline-config-json", type=Path, required=False, default=None)
    parser.add_argument("--thematic-runtime-path", type=Path, required=False, default=None)
    args = parser.parse_args()
    run(args.docx, args.out_seed, args.out_schema_descriptor, args.in_pipeline_config_json, args.thematic_runtime_path)


if __name__ == "__main__":
    main()
