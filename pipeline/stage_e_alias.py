from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any

from pipeline.common import get_logger, read_json, read_jsonl, stable_id, write_json


def run(
    in_snippets_jsonl: Path,
    in_seed_json: Path,
    out_alias_json: Path,
    out_timeline_json: Path,
) -> None:
    logger = get_logger(__name__)
    snippets = read_jsonl(in_snippets_jsonl)
    snippets.sort(key=lambda x: (x.get("timestamp_start_utc", ""), x.get("snippet_id", "")))
    seed = read_json(in_seed_json)
    cards = seed.get("cards", [])
    canonical_names = {c.get("canonical_name", "").lower(): c.get("card_id") for c in cards if isinstance(c, dict)}

    alias_entries: list[dict[str, Any]] = []
    timelines: dict[str, list[dict[str, Any]]] = {}
    seen_aliases: dict[tuple[str, str], dict[str, Any]] = {}

    for snip in snippets:
        text = snip.get("display_text_normalized", "")
        lower = text.lower()
        linked_card_id = None
        linked_name = None
        for name, card_id in canonical_names.items():
            if name and name in lower:
                linked_card_id = card_id
                linked_name = name
                break
        if not linked_card_id:
            continue
        key = (linked_card_id, linked_name or "")
        if key not in seen_aliases:
            seen_aliases[key] = {
                "alias_id": stable_id("alias", linked_card_id, linked_name or ""),
                "entity_card_id": linked_card_id,
                "alias_text": linked_name or "",
                "alias_type": "working_name",
                "first_seen_timestamp_utc": snip["timestamp_start_utc"],
                "last_seen_timestamp_utc": snip["timestamp_end_utc"],
                "source_snippet_ids": [snip["snippet_id"]],
                "resolution_confidence": snip.get("relevance_score", 0.5),
                "resolution_status": "resolved",
                "notes": "Auto-linked by canonical name mention.",
            }
        else:
            entry = seen_aliases[key]
            entry["last_seen_timestamp_utc"] = snip["timestamp_end_utc"]
            entry["source_snippet_ids"].append(snip["snippet_id"])

        timelines.setdefault(linked_card_id, []).append(
            {
                "timestamp_utc": snip["timestamp_start_utc"],
                "snippet_id": snip["snippet_id"],
                "text": text,
                "status": "revision_candidate",
            }
        )

    alias_entries = sorted(seen_aliases.values(), key=lambda x: (x["entity_card_id"], x["alias_text"]))
    write_json(out_alias_json, {"aliases": alias_entries})
    write_json(out_timeline_json, {"entity_timelines": timelines})
    logger.info(
        "Stage E complete: aliases=%d, entity_timelines=%d",
        len(alias_entries),
        len(timelines),
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--in-snippets-jsonl", type=Path, required=True)
    parser.add_argument("--in-seed-json", type=Path, required=True)
    parser.add_argument("--out-alias-json", type=Path, required=True)
    parser.add_argument("--out-timeline-json", type=Path, required=True)
    args = parser.parse_args()
    run(args.in_snippets_jsonl, args.in_seed_json, args.out_alias_json, args.out_timeline_json)


if __name__ == "__main__":
    main()
