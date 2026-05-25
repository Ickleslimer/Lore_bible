"""
Build a local static Fandom-style HTML wiki preview from pipeline cards.

Phase 1 has no production wiki site export; this script is for layout/readability trials only.
"""

from __future__ import annotations

import argparse
import webbrowser
from pathlib import Path

from pipeline.artifact_paths import migrate_run_artifacts_to_numbered
from pipeline.wiki_site_builder import build_wiki_site


def main() -> None:
    parser = argparse.ArgumentParser(description="Build static Fandom-style HTML wiki preview from pipeline cards.")
    parser.add_argument("--artifacts-root", type=Path, required=True, help="Pipeline run root")
    parser.add_argument("--out-dir", type=Path, default=Path("artifacts/wiki_preview"))
    parser.add_argument("--entities", default="", help="Comma-separated canonical names (default: all cards)")
    parser.add_argument("--drafts", action="store_true", help="Prefer card_drafts.json over canonical_cards.json")
    parser.add_argument("--open", action="store_true", help="Open index.html in the default browser")
    parser.add_argument("--config", type=Path, default=Path("config/pipeline_config.json"))
    args = parser.parse_args()

    run_root = args.artifacts_root.resolve()
    migrate_run_artifacts_to_numbered(run_root)

    entity_filter: set[str] | None = None
    if args.entities.strip():
        entity_filter = {part.strip().lower() for part in args.entities.split(",") if part.strip()}

    entries = build_wiki_site(
        run_root,
        args.out_dir.resolve(),
        entity_filter=entity_filter,
        prefer_canonical=not args.drafts,
        config_path=args.config,
    )

    out_dir = args.out_dir.resolve()
    index_path = out_dir / "index.html"
    print(f"Wrote {len(entries)} page(s) to {out_dir}")
    for entry in entries:
        print(f"  - {entry.name}: {entry.path}")
    print(f"  - search-index.json, wiki.css, wiki.js")

    if args.open:
        webbrowser.open(index_path.as_uri())


if __name__ == "__main__":
    main()
