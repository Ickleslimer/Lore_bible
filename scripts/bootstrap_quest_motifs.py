"""Generate canon/quest_motifs.json from config/quest_song_seed.json band hints."""

from __future__ import annotations

import argparse
from pathlib import Path

from pipeline.common import write_json
from pipeline.quest_motifs import DEFAULT_MOTIFS_PATH, DEFAULT_SEED_PATH, bootstrap_motifs_from_seed


def main() -> None:
    parser = argparse.ArgumentParser(description="Bootstrap quest motif registry from quest seed file.")
    parser.add_argument("--seed-path", type=Path, default=DEFAULT_SEED_PATH)
    parser.add_argument("--out-path", type=Path, default=DEFAULT_MOTIFS_PATH)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    payload = bootstrap_motifs_from_seed(args.seed_path)
    if args.dry_run:
        print(f"Would write {len(payload.get('motifs', []))} motif(s) to {args.out_path}")
        return
    args.out_path.parent.mkdir(parents=True, exist_ok=True)
    write_json(args.out_path, payload)
    print(f"Wrote {len(payload.get('motifs', []))} motif(s) to {args.out_path}")


if __name__ == "__main__":
    main()
