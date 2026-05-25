"""
Synthesize narrative work cards (phase 1: Theriac Coda) from an existing pipeline run.

Requires Stage 08W tags (runs 08W if missing) and writes 11_work_synthesis/work_cards.json.
"""

from __future__ import annotations

import argparse
import shutil
from pathlib import Path

from pipeline.artifact_paths import ArtifactPaths, migrate_run_artifacts_to_numbered
from pipeline.common import get_logger, read_json, setup_logging
from pipeline.narrative_works import snippet_tag_path, work_cards_path
from pipeline.stage_08w_narrative_work_tagging import run as run_stage_08w
from pipeline.stage_11w_work_card_synthesis import run as run_stage_11w


def _copy_if_exists(src: Path, dst: Path) -> bool:
    if not src.exists():
        return False
    dst.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src, dst)
    return True


def prepare_run_tree(source_root: Path, dest_root: Path) -> ArtifactPaths:
    migrate_run_artifacts_to_numbered(source_root)
    migrate_run_artifacts_to_numbered(dest_root)
    source = ArtifactPaths(source_root)
    dest = ArtifactPaths(dest_root)
    for path in (
        source.effective_snippets(),
        source.resolved_entities,
        source.snippet_clusters_meta,
        source.snippet_clusters_lore,
    ):
        rel = path.relative_to(source_root)
        _copy_if_exists(path, dest_root / rel)
    return dest


def main() -> None:
    parser = argparse.ArgumentParser(description="Targeted Theriac Coda work card synthesis")
    parser.add_argument("--source-run", type=Path, required=True)
    parser.add_argument("--dest-run", type=Path, required=True)
    parser.add_argument("--config", type=Path, default=Path("config/pipeline_config.json"))
    parser.add_argument("--review-memory", type=Path, default=Path("canon/review_memory.json"))
    parser.add_argument("--skip-08w", action="store_true")
    args = parser.parse_args()
    setup_logging()
    logger = get_logger(__name__)
    paths = prepare_run_tree(args.source_run.resolve(), args.dest_run.resolve())
    tags_path = snippet_tag_path(paths.root)
    work_path = work_cards_path(paths.root)

    if not args.skip_08w or not tags_path.exists():
        run_stage_08w(
            paths.effective_snippets(),
            tags_path,
            args.config,
            args.review_memory,
        )
    run_stage_11w(
        paths.effective_snippets(),
        tags_path,
        work_path,
        args.config,
    )
    payload = read_json(work_path)
    logger.info("Work card run complete: works=%d -> %s", len(payload.get("works", []) or []), work_path)


if __name__ == "__main__":
    main()
