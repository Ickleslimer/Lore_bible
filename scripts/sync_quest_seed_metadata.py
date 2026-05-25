"""
Write quest_id, pool_sequence, prerequisites, and inferred fields into config/quest_song_seed.json.

Run after editing quest rows so wiki quest map and pipeline markers stay aligned.
"""

from __future__ import annotations

import argparse
from pathlib import Path

from pipeline.common import read_json, write_json
from pipeline.quest_catalog import DEFAULT_SEED_PATH, normalize_quest_seeds


def main() -> None:
    parser = argparse.ArgumentParser(description="Sync quest catalog metadata into quest_song_seed.json")
    parser.add_argument("--seed-path", type=Path, default=DEFAULT_SEED_PATH)
    parser.add_argument("--dry-run", action="store_true", help="Print counts only; do not write")
    args = parser.parse_args()

    path = args.seed_path.resolve()
    payload = read_json(path)
    seeds = payload.get("quest_song_seeds", []) if isinstance(payload, dict) else []
    normalized = normalize_quest_seeds([row for row in seeds if isinstance(row, dict)])
    payload["quest_song_seeds"] = normalized
    payload["seed_count"] = len(normalized)

    if args.dry_run:
        print(f"Would write {len(normalized)} quest(s) to {path}")
        return

    write_json(path, payload)
    print(f"Wrote {len(normalized)} quest(s) with quest_id and pool_sequence to {path}")


if __name__ == "__main__":
    main()
