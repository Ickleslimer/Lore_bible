"""Re-adjudicate Stage 04R candidate windows that failed model adjudication."""

from __future__ import annotations

import argparse
from pathlib import Path

from pipeline.artifact_paths import ArtifactPaths, migrate_run_artifacts_to_numbered
from pipeline.common import get_logger, read_json, setup_logging
from pipeline.stage_04r_theme_relevance_rerun import load_failed_candidate_ids, run_retry_failed_adjudication
from pipeline.stage_06r_theme_rescue_snippet_extraction import run as run_stage_06r


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Retry Stage 04R model adjudication for failed candidate windows and optionally refresh 06R merge.",
    )
    parser.add_argument("--artifacts-root", type=Path, required=True)
    parser.add_argument("--log-level", type=str, default=None)
    parser.add_argument(
        "--skip-06r",
        action="store_true",
        help="Only patch 04R artifacts; do not rerun Stage 06R snippet merge.",
    )
    parser.add_argument("--in-pipeline-config-json", type=Path, default=Path("config/pipeline_config.json"))
    args = parser.parse_args()
    setup_logging(args.log_level)
    logger = get_logger(__name__)

    root = args.artifacts_root
    migrate_run_artifacts_to_numbered(root)
    paths = ArtifactPaths(root)
    failed_ids = load_failed_candidate_ids(paths.theme_relevance_rerun_failures)
    if not failed_ids:
        logger.info("No failed Stage 04R candidates found in %s.", paths.theme_relevance_rerun_failures)
        return
    if not paths.theme_relevance_rerun.exists():
        raise FileNotFoundError(f"Missing Stage 04R artifact: {paths.theme_relevance_rerun}")

    attempted = run_retry_failed_adjudication(
        paths.global_timeline,
        paths.theme_relevance_rerun,
        paths.theme_relevance_rerun_failures,
        paths.theme_relevance_rerun,
        paths.theme_rescue_messages,
        paths.theme_rescue_segments,
        paths.theme_relevance_rerun_failures,
        args.in_pipeline_config_json,
    )
    if attempted <= 0:
        return

    if args.skip_06r:
        logger.info("Stage 06R skipped (--skip-06r). Rerun 06R before downstream stages if rescue outputs changed.")
        return

    run_stage_06r(
        paths.theme_rescue_messages,
        paths.source_profiles,
        paths.snippets,
        paths.snippets_needs_review,
        paths.theme_rescue_snippets,
        paths.theme_rescue_snippets_needs_review,
        paths.theme_rescue_source_profiles,
        paths.snippets_with_theme_rescue,
        paths.snippets_needs_review_with_theme_rescue,
        paths.theme_rescue_snippet_merge_report,
        args.in_pipeline_config_json,
        paths.entity_seed,
        root / "learning" / "thematic_profile_runtime.json",
    )
    merge_report = read_json(paths.theme_rescue_snippet_merge_report) if paths.theme_rescue_snippet_merge_report.exists() else {}
    summary = merge_report.get("summary", {}) if isinstance(merge_report, dict) else {}
    logger.info(
        "Stage 06R refresh complete: combined_snippets=%s rescue_snippets=%s",
        summary.get("combined_snippet_count"),
        summary.get("rescue_snippet_count"),
    )


if __name__ == "__main__":
    main()
