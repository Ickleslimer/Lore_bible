from __future__ import annotations

import argparse
import re
import zipfile
from collections import defaultdict
from pathlib import Path
from typing import Any

from pipeline.common import get_logger, read_json, stable_id, write_json
from pipeline.mixtral_anchor_provider import build_stage_a_prompt, call_mixtral_chat
from pipeline.thematic_profile import update_runtime_profile


SECTION_TO_ENTITY_TYPE = {
    "KEY ORGANIZATIONS": "organization",
    "THE KRYPTEIA": "organization",
    "AI INFRASTRUCTURE": "ai_system",
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
        normalized = section.strip()
        if normalized in seen:
            continue
        seen.add(normalized)
        entity_type = "term"
        for key, value in SECTION_TO_ENTITY_TYPE.items():
            if key in normalized:
                entity_type = value
                break
        entities.append(
            {
                "card_id": stable_id("card", normalized),
                "entity_type": entity_type,
                "canonical_name": normalized.title(),
                "aliases": [],
                "status": "canonical",
                "summary": f"Bootstrap entry derived from lore bible section: {normalized}.",
                "details": {"origin": "lore_bible_bootstrap"},
                "timeline": [],
                "relationships": [],
                "source_evidence": ["lore_bible_seed"],
                "confidence": {"score": 0.7, "reviewer_note": "Auto-derived from heading extraction."},
                "revision_history": []
            }
        )

    return sorted(entities, key=lambda x: (x["entity_type"], x["canonical_name"]))


def infer_entities_mixtral(text: str, config: dict[str, Any]) -> dict[str, Any] | None:
    logger = get_logger(__name__)
    mixtral_cfg = config.get("mixtral", {})
    rate_state_path = Path(str(mixtral_cfg.get("rate_state_path", "artifacts/learning/mixtral_rate_runtime.json")))
    excerpt_chars = int(config.get("stage_a_mixtral_excerpt_chars", 24000))
    prompt = build_stage_a_prompt(text[:excerpt_chars])
    response = call_mixtral_chat(
        base_url=str(mixtral_cfg.get("base_url", "http://127.0.0.1:11434")),
        model=str(mixtral_cfg.get("model", "mixtral")),
        prompt=prompt,
        temperature=float(mixtral_cfg.get("temperature", 0.0)),
        timeout_seconds=int(mixtral_cfg.get("timeout_seconds", 60)),
        provider=str(mixtral_cfg.get("provider", "auto")),
        api_base_url=str(mixtral_cfg.get("api_base_url", "https://api.mistral.ai/v1")),
        api_model=str(mixtral_cfg.get("api_model", "mistral-large-latest")),
        api_retries=int(mixtral_cfg.get("api_retries", 2)),
        auto_fallback_to_ollama=bool(mixtral_cfg.get("auto_fallback_to_ollama", True)),
        rate_limit_cooldown_seconds=int(mixtral_cfg.get("rate_limit_cooldown_seconds", 90)),
        rate_state_path=rate_state_path,
        min_interval_seconds=float(mixtral_cfg.get("adaptive_min_interval_seconds", 2.0)),
        max_interval_seconds=float(mixtral_cfg.get("adaptive_max_interval_seconds", 120.0)),
        success_decay=float(mixtral_cfg.get("adaptive_success_decay", 0.9)),
        rate_limit_growth=float(mixtral_cfg.get("adaptive_rate_limit_growth", 1.8)),
        ollama_unavailable_cooldown_seconds=int(mixtral_cfg.get("ollama_unavailable_cooldown_seconds", 120)),
    )
    if not isinstance(response, dict):
        logger.debug("Stage A Mixtral response fallback: provider returned no JSON object.")
        return None
    raw_entities = response.get("entities")
    if not isinstance(raw_entities, list):
        logger.debug("Stage A Mixtral response fallback: missing/invalid `entities` list. keys=%s", sorted(response.keys()))
        return None
    allowed_entity_types = {
        "character",
        "faction",
        "organization",
        "ai_system",
        "quest",
        "event",
        "timeline_node",
        "theme",
        "term",
    }
    cards: list[dict[str, Any]] = []
    for item in raw_entities:
        if not isinstance(item, dict):
            continue
        name = str(item.get("canonical_name", "")).strip()
        entity_type = str(item.get("entity_type", "term")).strip()
        summary = str(item.get("summary", "")).strip()
        aliases_raw = item.get("aliases", [])
        aliases = []
        if isinstance(aliases_raw, list):
            for alias in aliases_raw:
                alias_text = str(alias).strip()
                if alias_text and alias_text.lower() != name.lower():
                    aliases.append(alias_text)
        rels_raw = item.get("relationship_hints", [])
        relationships = []
        relationship_hints_unresolved = []
        if isinstance(rels_raw, list):
            for rel in rels_raw:
                if not isinstance(rel, dict):
                    continue
                target_name = str(rel.get("target_name", "")).strip()
                relation_type = str(rel.get("relation_type", "")).strip()
                note = str(rel.get("note", "")).strip()
                if not target_name or not relation_type:
                    continue
                relationships.append(
                    {
                        # Resolved to card IDs later when additional entities exist.
                        "target_card_id": stable_id("card", target_name),
                        "relation_type": relation_type,
                        "note": note or "Relationship hint from Stage A Mixtral extraction.",
                    }
                )
                relationship_hints_unresolved.append(
                    {
                        "target_name": target_name,
                        "relation_type": relation_type,
                        "note": note or "Relationship hint from Stage A Mixtral extraction.",
                    }
                )
        if not name:
            continue
        if entity_type not in allowed_entity_types:
            entity_type = "term"
        cards.append(
            {
                "card_id": stable_id("card", name),
                "entity_type": entity_type if entity_type else "term",
                "canonical_name": name,
                "aliases": sorted(set(aliases)),
                "status": "canonical",
                "summary": summary or f"Bootstrap entry inferred by Mixtral: {name}.",
                "details": {
                    "origin": "lore_bible_bootstrap_mixtral",
                    "relationship_hints_unresolved": relationship_hints_unresolved,
                },
                "timeline": [],
                "relationships": relationships,
                "source_evidence": ["lore_bible_seed"],
                "confidence": {"score": 0.8, "reviewer_note": "Model-derived from lore bible."},
                "revision_history": [],
            }
        )
    if not cards:
        logger.debug("Stage A Mixtral response fallback: `entities` parsed but no usable cards were produced.")
        return None
    dedup: dict[str, dict[str, Any]] = {}
    for c in cards:
        dedup[c["canonical_name"].lower()] = c
    suggested = response.get("suggested_thematic_markers", {}) if isinstance(response, dict) else {}
    historical = []
    music = []
    if isinstance(suggested, dict):
        historical = [str(x).strip().lower() for x in (suggested.get("historical") or []) if str(x).strip()]
        music = [str(x).strip().lower() for x in (suggested.get("music") or []) if str(x).strip()]
    return {
        "cards": sorted(dedup.values(), key=lambda x: (x["entity_type"], x["canonical_name"])),
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
    logger.info("Stage A: loading lore bible DOCX from %s", docx_path)
    text = read_docx_text(docx_path)
    logger.debug("Stage A: extracted %d characters from DOCX text.", len(text))
    config: dict[str, Any] = {}
    if in_pipeline_config_json and in_pipeline_config_json.exists():
        config = read_json(in_pipeline_config_json)
    stage_a_provider = str(config.get("stage_a_anchor_provider", "heuristic")).lower()
    logger.info("Stage A: provider mode is '%s'.", stage_a_provider)
    cards = infer_entities(text)
    logger.debug("Stage A: heuristic extraction produced %d card(s).", len(cards))
    if stage_a_provider in {"mixtral", "hybrid"}:
        logger.info("Stage A: requesting Mixtral bootstrap extraction...")
        model_result = infer_entities_mixtral(text, config)
        if model_result and isinstance(model_result, dict):
            model_cards = list(model_result.get("cards", []))
            logger.info("Stage A: Mixtral extraction produced %d card(s).", len(model_cards))
            if stage_a_provider == "mixtral":
                cards = model_cards
            else:
                merged: dict[str, dict[str, Any]] = {c["canonical_name"].lower(): c for c in cards}
                for c in model_cards:
                    merged[c["canonical_name"].lower()] = c
                cards = sorted(merged.values(), key=lambda x: (x["entity_type"], x["canonical_name"]))
                logger.info("Stage A: hybrid merge produced %d combined card(s).", len(cards))
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
            logger.warning("Stage A: Mixtral extraction returned no usable output; keeping heuristic cards.")
    payload = {
        "source": str(docx_path),
        "entity_count": len(cards),
        "provider_mode": stage_a_provider,
        "cards": cards
    }
    write_json(out_seed, payload)
    write_json(
        out_schema_descriptor,
        {
            "entity_types": [
                "character",
                "faction",
                "organization",
                "ai_system",
                "quest",
                "event",
                "timeline_node",
                "theme",
                "term"
            ],
            "notes": "Seed ontology for THERIAC lore cards."
        }
    )
    logger.info("Stage A complete: wrote %d seed cards.", len(cards))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--docx", type=Path, required=True)
    parser.add_argument("--out-seed", type=Path, required=True)
    parser.add_argument("--out-schema-descriptor", type=Path, required=True)
    parser.add_argument("--in-pipeline-config-json", type=Path, required=False, default=None)
    parser.add_argument("--thematic-runtime-path", type=Path, required=False, default=None)
    args = parser.parse_args()
    run(args.docx, args.out_seed, args.out_schema_descriptor, args.in_pipeline_config_json, args.thematic_runtime_path)


if __name__ == "__main__":
    main()
