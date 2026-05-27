"""
Pull lore card prose from Notion draft pages (Enoch, Krypteia, etc.) into local cards and rebuild the wiki preview.

Uses page IDs recorded in the run's notion_draft_sync_report.json.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from pipeline.artifact_paths import ArtifactPaths, migrate_run_artifacts_to_numbered
from pipeline.common import read_json, write_json
from pipeline.notion_draft_sync import pull_cards_from_notion, resolve_notion_pages_by_canonical_name
from pipeline.wiki_site_builder import build_wiki_site


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Import card prose from Notion draft pages and rebuild the static wiki preview.",
    )
    parser.add_argument(
        "--artifacts-root",
        type=Path,
        default=Path("artifacts/runs/wiki_seed_enoch_krypteia"),
        help="Run root with canonical_cards.json and notion_draft_sync_report.json",
    )
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=Path("artifacts/wiki_preview_enoch_krypteia"),
        help="Wiki HTML output directory",
    )
    parser.add_argument(
        "--entities",
        default="Enoch,Krypteia",
        help="Comma-separated canonical names to import (must match Notion sync report)",
    )
    parser.add_argument("--config", type=Path, default=Path("config/pipeline_config.json"))
    parser.add_argument("--env", type=Path, default=Path(".env"))
    parser.add_argument("--open", action="store_true", help="Open index.html after build")
    args = parser.parse_args()

    run_root = args.artifacts_root.resolve()
    migrate_run_artifacts_to_numbered(run_root)
    paths = ArtifactPaths(run_root)

    target_names = [part.strip() for part in args.entities.split(",") if part.strip()]
    if not target_names:
        raise SystemExit("Provide at least one entity via --entities")

    page_refs = resolve_notion_pages_by_canonical_name(
        target_names,
        config_path=args.config,
        env_path=args.env,
    )
    if len(page_refs) != len(target_names):
        found = {str(ref.get("canonical_name", "")).strip().lower() for ref in page_refs}
        missing = [name for name in target_names if name.lower() not in found]
        raise SystemExit(f"No Notion draft page found for: {', '.join(missing)}")

    canonical_path = paths.canonical_cards
    drafts_path = paths.card_drafts
    source_path = canonical_path if canonical_path.exists() else drafts_path
    if not source_path.exists():
        raise SystemExit(f"No local cards at {canonical_path} or {drafts_path}")

    payload = read_json(source_path)
    existing = [row for row in payload.get("cards", []) if isinstance(row, dict)]

    # Only pull cards we asked for; keep other local cards unchanged.
    name_to_card = {str(c.get("canonical_name", "")).strip().lower(): c for c in existing}
    cards_to_pull: list[dict] = []
    refs_for_pull: list[dict] = []
    for ref in page_refs:
        cname = str(ref.get("canonical_name", "")).strip().lower()
        card = name_to_card.get(cname)
        if card is None:
            raise SystemExit(f"No local card for Notion page {ref.get('canonical_name')!r}")
        card_id = str(card.get("card_id", "")).strip()
        refs_for_pull.append({**ref, "card_id": card_id or ref.get("card_id", "")})
        cards_to_pull.append(card)

    updated_subset, import_report = pull_cards_from_notion(
        run_root,
        refs_for_pull,
        cards_to_pull,
        config_path=args.config,
        env_path=args.env,
    )
    updated_by_id = {str(c.get("card_id", "")).strip(): c for c in updated_subset}

    merged_cards: list[dict] = []
    for card in existing:
        card_id = str(card.get("card_id", "")).strip()
        if card_id in updated_by_id:
            merged_cards.append(updated_by_id[card_id])
        else:
            merged_cards.append(card)

    merged_cards.sort(key=lambda c: str(c.get("canonical_name", "")).lower())
    card_payload = {"cards": merged_cards}
    canonical_path.parent.mkdir(parents=True, exist_ok=True)
    write_json(canonical_path, card_payload)
    write_json(drafts_path, card_payload)

    import_out = paths.stage12 / "notion_import_report.json"
    write_json(import_out, import_report)
    print(f"Imported {import_report.get('card_count', 0)} card(s) from Notion -> {canonical_path}")
    if import_report.get("failures"):
        print(json.dumps(import_report["failures"], indent=2))

    entity_filter = {name.lower() for name in target_names}
    entries = build_wiki_site(
        run_root,
        args.out_dir.resolve(),
        entity_filter=entity_filter,
        prefer_canonical=True,
        config_path=args.config,
    )
    print(f"Rebuilt wiki: {len(entries)} page(s) -> {args.out_dir.resolve()}")
    for entry in entries:
        print(f"  - {entry.name}: {entry.path}")

    if args.open:
        import webbrowser

        webbrowser.open((args.out_dir.resolve() / "index.html").as_uri())


if __name__ == "__main__":
    main()
