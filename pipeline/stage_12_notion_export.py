from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from pipeline.common import get_logger, read_json, read_jsonl


def to_lore_record(card: dict[str, Any]) -> dict[str, Any]:
    return {
        "database": "LoreCards",
        "id": card.get("card_id"),
        "properties": {
            "Name": card.get("canonical_name"),
            "EntityType": card.get("entity_type"),
            "Status": card.get("status"),
            "Summary": card.get("summary", ""),
            "SourceEvidence": card.get("source_evidence", []),
        },
    }


def to_meta_record(card: dict[str, Any]) -> dict[str, Any]:
    return {
        "database": "MetaCards",
        "id": card.get("meta_id"),
        "properties": {
            "Title": card.get("title"),
            "MetaType": card.get("meta_type"),
            "Status": card.get("status"),
            "Summary": card.get("summary"),
            "SourceEvidence": card.get("source_evidence", []),
        },
    }


def run(
    in_cards_json: Path,
    in_meta_cards_json: Path,
    in_alias_json: Path,
    in_snippets_jsonl: Path,
    in_profiles_json: Path,
    in_merge_log_jsonl: Path,
    out_ndjson: Path,
) -> None:
    logger = get_logger(__name__)
    cards = [card for card in read_json(in_cards_json).get("cards", []) if card.get("status") == "canonical"]
    meta_cards = read_json(in_meta_cards_json).get("meta_cards", [])
    aliases = read_json(in_alias_json).get("aliases", [])
    snippets = read_jsonl(in_snippets_jsonl)
    profiles = read_json(in_profiles_json).get("profiles", [])
    merge_log = read_jsonl(in_merge_log_jsonl)
    logger.info(
        "Stage 11: exporting lore=%d meta=%d aliases=%d snippets=%d profiles=%d decisions=%d",
        len(cards),
        len(meta_cards),
        len(aliases),
        len(snippets),
        len(profiles),
        len(merge_log),
    )

    out_ndjson.parent.mkdir(parents=True, exist_ok=True)
    with out_ndjson.open("w", encoding="utf-8") as f:
        for card in cards:
            f.write(json.dumps(to_lore_record(card), ensure_ascii=False) + "\n")
        for card in meta_cards:
            f.write(json.dumps(to_meta_record(card), ensure_ascii=False) + "\n")
        for alias in aliases:
            f.write(json.dumps({"database": "Aliases", "id": alias.get("alias_id"), "properties": alias}, ensure_ascii=False) + "\n")
        for snip in snippets:
            f.write(
                json.dumps(
                    {"database": "EvidenceSnippets", "id": snip.get("snippet_id"), "properties": snip},
                    ensure_ascii=False,
                )
                + "\n"
            )
        for profile in profiles:
            f.write(
                json.dumps(
                    {"database": "DMSourceProfiles", "id": profile.get("thread_id"), "properties": profile},
                    ensure_ascii=False,
                )
                + "\n"
            )
        for decision in merge_log:
            f.write(
                json.dumps({"database": "RevisionLog", "id": decision.get("decision_id"), "properties": decision}, ensure_ascii=False)
                + "\n"
            )
    logger.info(
        "Stage 11 complete: wrote %d total NDJSON records to %s",
        len(cards) + len(meta_cards) + len(aliases) + len(snippets) + len(profiles) + len(merge_log),
        out_ndjson,
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--in-cards-json", type=Path, required=True)
    parser.add_argument("--in-meta-cards-json", type=Path, required=True)
    parser.add_argument("--in-alias-json", type=Path, required=True)
    parser.add_argument("--in-snippets-jsonl", type=Path, required=True)
    parser.add_argument("--in-profiles-json", type=Path, required=True)
    parser.add_argument("--in-merge-log-jsonl", type=Path, required=True)
    parser.add_argument("--out-ndjson", type=Path, required=True)
    args = parser.parse_args()
    run(
        args.in_cards_json,
        args.in_meta_cards_json,
        args.in_alias_json,
        args.in_snippets_jsonl,
        args.in_profiles_json,
        args.in_merge_log_jsonl,
        args.out_ndjson,
    )


if __name__ == "__main__":
    main()
