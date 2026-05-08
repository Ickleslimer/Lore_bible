from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any

from pipeline.common import get_logger, now_utc_iso, read_json, read_jsonl, safe_uuid, stable_id, write_json


def infer_thematic_relationship_hints(
    cluster: dict[str, Any],
    card_by_id: dict[str, dict[str, Any]],
    thematic_memory: dict[str, Any],
) -> list[dict[str, Any]]:
    tags = set(cluster.get("thematic_tags", []))
    hints: list[dict[str, Any]] = []
    if "possible_artist_reference" in tags or any(t.startswith("music:") for t in tags):
        quest_targets = [c for c in card_by_id.values() if c.get("entity_type") == "quest"]
        char_targets = [c for c in card_by_id.values() if c.get("entity_type") == "character"]
        continuity_candidates: list[dict[str, Any]] = []
        for artist_row in thematic_memory.get("artists", []):
            if not isinstance(artist_row, dict):
                continue
            if int(artist_row.get("mention_count", 0)) < 2:
                continue
            top_chars = sorted(
                (artist_row.get("character_mentions") or {}).items(),
                key=lambda x: x[1],
                reverse=True,
            )[:3]
            top_quests = sorted(
                (artist_row.get("quest_mentions") or {}).items(),
                key=lambda x: x[1],
                reverse=True,
            )[:3]
            if top_chars or top_quests:
                continuity_candidates.append(
                    {
                        "artist_name": artist_row.get("artist_name"),
                        "mention_count": artist_row.get("mention_count", 0),
                        "top_character_links": top_chars,
                        "top_quest_links": top_quests,
                    }
                )
        if quest_targets and char_targets:
            hints.append(
                {
                    "relation_type": "possible_quest_character_music_link",
                    "confidence": 0.6 if continuity_candidates else 0.45,
                    "note": "Music/artist thematic markers suggest quest-to-character association candidate."
                    + (" Continuity memory raised confidence." if continuity_candidates else ""),
                    "candidate_quest_cards": [q.get("card_id") for q in quest_targets[:5]],
                    "candidate_character_cards": [c.get("card_id") for c in char_targets[:5]],
                    "continuity_memory_candidates": continuity_candidates[:5],
                }
            )
    if any(t.startswith("historical:") for t in tags):
        hints.append(
            {
                "relation_type": "possible_historical_theming_link",
                "confidence": 0.4,
                "note": "Historical naming markers present; consider linking to similarly themed entities.",
            }
        )
    return hints


def run(
    in_seed_json: Path,
    in_lore_clusters_json: Path,
    in_meta_clusters_json: Path,
    in_alias_json: Path,
    in_snippets_jsonl: Path,
    out_draft_dir: Path,
) -> None:
    logger = get_logger(__name__)
    seed = read_json(in_seed_json)
    lore_payload = read_json(in_lore_clusters_json)
    lore_clusters = lore_payload.get("clusters", [])
    thematic_memory = lore_payload.get("thematic_memory", {})
    meta_clusters = read_json(in_meta_clusters_json).get("clusters", [])
    alias_payload = read_json(in_alias_json).get("aliases", [])
    snippets = {s["snippet_id"]: s for s in read_jsonl(in_snippets_jsonl)}

    out_draft_dir.mkdir(parents=True, exist_ok=True)

    card_by_id = {c["card_id"]: c for c in seed.get("cards", []) if isinstance(c, dict) and "card_id" in c}
    logger.info(
        "Stage F: drafting from %d lore cluster(s), %d meta cluster(s), %d snippet(s)",
        len(lore_clusters),
        len(meta_clusters),
        len(snippets),
    )

    lore_patches = []
    for cluster in lore_clusters:
        snippet_ids = cluster.get("snippet_ids", [])
        evidence = [snippets[sid] for sid in snippet_ids if sid in snippets]
        if not evidence:
            continue
        target_card_id = None
        cluster_key = str(cluster.get("cluster_key", "")).lower()
        for card in card_by_id.values():
            if card.get("canonical_name", "").lower() == cluster_key:
                target_card_id = card["card_id"]
                break
        if target_card_id is None:
            target_card_id = stable_id("card", cluster_key)

        lore_patches.append(
            {
                "claim_id": safe_uuid(),
                "card_id": target_card_id,
                "knowledge_track": "lore",
                "proposed_summary_append": " ".join(e["display_text_normalized"] for e in evidence[:3]),
                "source_snippet_ids": snippet_ids,
                "thematic_tags": cluster.get("thematic_tags", []),
                "proposed_relationship_hints": infer_thematic_relationship_hints(cluster, card_by_id, thematic_memory),
                "confidence": round(sum(e.get("relevance_score", 0.5) for e in evidence) / len(evidence), 3),
                "created_at_utc": now_utc_iso(),
            }
        )

    meta_cards = []
    for cluster in meta_clusters:
        snippet_ids = cluster.get("snippet_ids", [])
        evidence = [snippets[sid] for sid in snippet_ids if sid in snippets]
        if not evidence:
            continue
        meta_cards.append(
            {
                "meta_id": stable_id("meta", cluster["cluster_id"]),
                "meta_type": "production",
                "title": str(cluster.get("cluster_key", "Meta Cluster")).title(),
                "summary": " ".join(e["display_text_normalized"] for e in evidence[:2]),
                "details": {
                    "cluster_id": cluster["cluster_id"],
                    "topics": cluster.get("topics", []),
                    "thematic_tags": cluster.get("thematic_tags", []),
                },
                "linked_lore_cards": [],
                "source_evidence": snippet_ids,
                "status": "draft",
            }
        )

    write_json(out_draft_dir / "lore_patches.json", {"patches": lore_patches})
    write_json(out_draft_dir / "meta_cards_draft.json", {"meta_cards": meta_cards})
    write_json(out_draft_dir / "alias_snapshot.json", {"aliases": alias_payload})
    logger.info(
        "Stage F complete: lore_patches=%d, meta_cards=%d",
        len(lore_patches),
        len(meta_cards),
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--in-seed-json", type=Path, required=True)
    parser.add_argument("--in-lore-clusters-json", type=Path, required=True)
    parser.add_argument("--in-meta-clusters-json", type=Path, required=True)
    parser.add_argument("--in-alias-json", type=Path, required=True)
    parser.add_argument("--in-snippets-jsonl", type=Path, required=True)
    parser.add_argument("--out-draft-dir", type=Path, required=True)
    args = parser.parse_args()
    run(
        args.in_seed_json,
        args.in_lore_clusters_json,
        args.in_meta_clusters_json,
        args.in_alias_json,
        args.in_snippets_jsonl,
        args.out_draft_dir,
    )


if __name__ == "__main__":
    main()
