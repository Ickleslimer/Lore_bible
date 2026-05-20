from __future__ import annotations

import argparse
import re
from pathlib import Path
from typing import Any

from pipeline.common import get_logger, read_json, read_jsonl, stable_id, write_json
from pipeline.entity_resolution import load_entity_records, normalized_name_key
from pipeline.thematic_profile import load_runtime_profile, merge_thematic_config


def tokenize(text: str) -> list[str]:
    return [t for t in re.split(r"[^a-zA-Z0-9]+", text.lower()) if t]


def infer_cluster_key(snippet: dict[str, Any], canon_names: list[str] | dict[str, str]) -> str:
    if isinstance(canon_names, dict):
        name_lookup = {key: value for key, value in canon_names.items() if key and value}
    else:
        name_lookup = {normalized_name_key(name): name for name in canon_names if normalized_name_key(name)}
    for candidate in snippet.get("candidate_entities", []) or []:
        key = normalized_name_key(str(candidate))
        if key in name_lookup:
            return name_lookup[key]

    text = normalized_name_key(str(snippet.get("display_text_normalized", "")))
    for name_key, canonical_name in sorted(name_lookup.items(), key=lambda x: len(x[0]), reverse=True):
        pattern = r"(?<![a-z0-9])" + r"\s+".join(re.escape(part) for part in name_key.split()) + r"(?![a-z0-9])"
        if re.search(pattern, text):
            return canonical_name
    return "unmapped"


def extract_music_signals(text: str) -> list[str]:
    signals: list[str] = []
    # Song-like title hint: quoted strings or title with parentheses.
    if re.search(r"\"[^\"]{3,}\"", text) or re.search(r"[A-Za-z0-9][^\n]{2,}\([^)]{2,}\)", text):
        signals.append("possible_song_title_reference")
    # "by <artist>" pattern.
    if re.search(r"\bby\s+[A-Z][A-Za-z0-9&' .-]{2,}\b", text):
        signals.append("possible_artist_reference")
    return sorted(set(signals))


def extract_artist_names(text: str) -> list[str]:
    # Conservative extraction: only explicit "by <Artist>" pattern.
    matches = re.findall(r"\bby\s+([A-Z][A-Za-z0-9&' .-]{2,})\b", text)
    cleaned = [m.strip(" .,!?:;") for m in matches if m.strip()]
    return sorted(set(cleaned))


def extract_thematic_tags(text: str, historical_markers: list[str], music_markers: list[str]) -> list[str]:
    lower = text.lower()
    tags: list[str] = []
    for marker in historical_markers:
        if marker in lower:
            tags.append(f"historical:{marker}")
    for marker in music_markers:
        if marker in lower:
            tags.append(f"music:{marker}")
    tags.extend(extract_music_signals(text))
    return sorted(set(tags))


def run(
    in_snippets_jsonl: Path,
    in_seed_json: Path,
    out_lore_json: Path,
    out_meta_json: Path,
    in_pipeline_config_json: Path | None = None,
    thematic_runtime_path: Path | None = None,
) -> None:
    logger = get_logger(__name__)
    snippets = read_jsonl(in_snippets_jsonl)
    entities = load_entity_records(in_seed_json)
    active_entities = [e for e in entities if e.get("resolution_status", "resolved") != "blocked_seed" and e.get("seed_status", "active") != "blocked_seed"]
    canon_names: dict[str, str] = {}
    for entity in active_entities:
        canonical_name = str(entity.get("canonical_name", "")).strip()
        if canonical_name:
            canon_names[normalized_name_key(canonical_name)] = canonical_name
        for alias in entity.get("aliases", []) or []:
            if str(alias).strip() and canonical_name:
                canon_names[normalized_name_key(str(alias))] = canonical_name
    quest_names = [e.get("canonical_name", "") for e in active_entities if e.get("entity_type") == "quest"]
    character_names = [e.get("canonical_name", "") for e in active_entities if e.get("entity_type") == "character"]
    config: dict[str, Any] = {}
    if in_pipeline_config_json and in_pipeline_config_json.exists():
        config = read_json(in_pipeline_config_json)
    runtime_profile = load_runtime_profile(thematic_runtime_path)
    thematic_cfg = merge_thematic_config(config, runtime_profile)
    thematic_enabled = bool(thematic_cfg.get("enabled", True))
    historical_markers = list(thematic_cfg.get("historical_markers", []))
    music_markers = list(thematic_cfg.get("music_markers", []))

    lore_clusters: dict[str, dict[str, Any]] = {}
    meta_clusters: dict[str, dict[str, Any]] = {}
    thematic_memory: dict[str, dict[str, Any]] = {}
    logger.info(
        "Stage 08: grouping %d snippet(s) with thematic_linking=%s",
        len(snippets),
        thematic_enabled,
    )

    heartbeat_every = max(1, min(1000, max(100, len(snippets) // 20 or 1)))
    for snippet_index, snip in enumerate(snippets, start=1):
        snippet_text = str(snip.get("display_text_normalized", ""))
        key = infer_cluster_key(snip, canon_names)
        cluster_id = stable_id("cluster", snip.get("knowledge_track", "unknown"), key)
        target = lore_clusters if snip.get("knowledge_track") == "lore" else meta_clusters
        if cluster_id not in target:
            target[cluster_id] = {
                "cluster_id": cluster_id,
                "cluster_key": key,
                "snippet_ids": [],
                "topics": [],
                "knowledge_track": snip.get("knowledge_track", "unknown"),
                "thematic_tags": [],
            }
        target[cluster_id]["snippet_ids"].append(snip["snippet_id"])
        target[cluster_id]["topics"] = sorted(set(target[cluster_id]["topics"] + snip.get("candidate_topics", [])))
        if thematic_enabled:
            tags = extract_thematic_tags(
                snippet_text,
                historical_markers,
                music_markers,
            )
            target[cluster_id]["thematic_tags"] = sorted(set(target[cluster_id]["thematic_tags"] + tags))

            # Continuity memory: repeated artist mentions + co-occurring character/quest names.
            artists = extract_artist_names(snippet_text)
            if artists:
                lower_text = snippet_text.lower()
                co_chars = [name for name in character_names if name and name.lower() in lower_text]
                co_quests = [name for name in quest_names if name and name.lower() in lower_text]
                for artist in artists:
                    key_artist = artist.lower()
                    if key_artist not in thematic_memory:
                        thematic_memory[key_artist] = {
                            "artist_name": artist,
                            "mention_count": 0,
                            "character_mentions": {},
                            "quest_mentions": {},
                            "snippet_ids": [],
                        }
                    mem = thematic_memory[key_artist]
                    mem["mention_count"] += 1
                    mem["snippet_ids"].append(snip["snippet_id"])
                    for cname in co_chars:
                        mem["character_mentions"][cname] = int(mem["character_mentions"].get(cname, 0)) + 1
                    for qname in co_quests:
                        mem["quest_mentions"][qname] = int(mem["quest_mentions"].get(qname, 0)) + 1
        if snippet_index == len(snippets) or snippet_index % heartbeat_every == 0:
            logger.info(
                "Stage 08 progress: %d/%d grouping snippets lore_clusters=%d meta_clusters=%d",
                snippet_index,
                len(snippets),
                len(lore_clusters),
                len(meta_clusters),
            )

    write_json(
        out_lore_json,
        {
            "clusters": list(lore_clusters.values()),
            "thematic_memory": {
                "artists": sorted(
                    thematic_memory.values(),
                    key=lambda x: (x["mention_count"], x["artist_name"]),
                    reverse=True,
                )
            },
        },
    )
    write_json(out_meta_json, {"clusters": list(meta_clusters.values())})
    logger.info(
        "Stage 08 complete: lore_clusters=%d, meta_clusters=%d, remembered_artists=%d",
        len(lore_clusters),
        len(meta_clusters),
        len(thematic_memory),
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--in-snippets-jsonl", type=Path, required=True)
    parser.add_argument("--in-entities-json", "--in-seed-json", dest="in_entities_json", type=Path, required=True)
    parser.add_argument("--out-lore-json", type=Path, required=True)
    parser.add_argument("--out-meta-json", type=Path, required=True)
    parser.add_argument("--in-pipeline-config-json", type=Path, required=False, default=None)
    parser.add_argument("--thematic-runtime-path", type=Path, required=False, default=None)
    args = parser.parse_args()
    run(
        args.in_snippets_jsonl,
        args.in_entities_json,
        args.out_lore_json,
        args.out_meta_json,
        args.in_pipeline_config_json,
        args.thematic_runtime_path,
    )


if __name__ == "__main__":
    main()
