from __future__ import annotations

import argparse
from pathlib import Path
from time import perf_counter

from pipeline.artifact_paths import ArtifactPaths, migrate_run_artifacts_to_numbered
from pipeline.common import get_logger, read_json, read_jsonl, setup_logging
from pipeline.stage_05_lore_development_ledger import run as run_stage_05_ledger
from pipeline.stage_06_snippet_extraction import run as run_stage_06
from pipeline.stage_08_snippet_grouping import run as run_stage_08
from pipeline.stage_07a_entity_candidate_harvest import run as run_stage_07
from pipeline.stage_07b_entity_adjudication import run as run_stage_07b
from pipeline.stage_07c_theme_miner import run as run_stage_07c
from pipeline.stage_07d_theme_reclassification import run as run_stage_07d
from pipeline.stage_04r_theme_relevance_rerun import run as run_stage_04r
from pipeline.stage_06r_theme_rescue_snippet_extraction import run as run_stage_06r
from pipeline.stage_09_claim_drafting import run as run_stage_09


REVIEW_REQUIRED_EXIT_CODE = 2
REVIEW_GATE_MARKERS = (
    "requiring review",
    "conversation entity proposal",
    "identity merge proposal",
)


def _count_jsonl(path: Path) -> int:
    if not path.exists():
        return 0
    return len(read_jsonl(path))


def _is_review_gate_error(exc: BaseException) -> bool:
    text = str(exc).lower()
    return any(marker in text for marker in REVIEW_GATE_MARKERS)


def _theme_rerun_enabled() -> bool:
    path = Path("config/pipeline_config.json")
    if not path.exists():
        return False
    try:
        payload = read_json(path)
    except Exception:
        return False
    raw = payload.get("theme_aware_rerun", {}) if isinstance(payload, dict) else {}
    return bool(raw.get("enabled", False)) if isinstance(raw, dict) else False


def _effective_snippets_path(paths: ArtifactPaths) -> Path:
    return paths.effective_snippets()


def _run_stage(logger, stage_idx: int, total_stages: int, stage_name: str, fn, *args) -> float:
    logger.info("[%d/%d] START %s", stage_idx, total_stages, stage_name)
    start = perf_counter()
    try:
        fn(*args)
    except RuntimeError as exc:
        if _is_review_gate_error(exc):
            elapsed = perf_counter() - start
            logger.warning("[%d/%d] REVIEW %s (%.2fs)", stage_idx, total_stages, stage_name, elapsed)
            logger.warning("Pipeline paused for review: %s", exc)
            raise SystemExit(REVIEW_REQUIRED_EXIT_CODE) from None
        raise
    elapsed = perf_counter() - start
    logger.info("[%d/%d] DONE  %s (%.2fs)", stage_idx, total_stages, stage_name, elapsed)
    return elapsed


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Resume the Theriac pipeline from Stage 05 (snippet extraction) using completed Stage 04 artifacts."
    )
    parser.add_argument("--artifacts-root", type=Path, required=True)
    parser.add_argument("--log-level", type=str, default=None)
    args = parser.parse_args()
    setup_logging(args.log_level)
    logger = get_logger(__name__)
    root = args.artifacts_root
    migrate_run_artifacts_to_numbered(root)
    paths = ArtifactPaths(root)
    thematic_runtime_path = root / "learning" / "thematic_profile_runtime.json"
    snippets_for_downstream = _effective_snippets_path(paths)
    total_stages = 12

    _run_stage(
        logger,
        5,
        total_stages,
        "Stage 05 Snippet Extraction",
        run_stage_06,
        paths.relevant_messages,
        paths.source_profiles,
        paths.snippets,
        paths.snippets_needs_review,
        paths.source_profiles,
        Path("config/pipeline_config.json"),
        paths.entity_seed,
        thematic_runtime_path,
        None,
    )
    logger.info(
        "Stage 05 summary: snippets=%d, needs_review=%d, profiles=%d",
        _count_jsonl(paths.snippets),
        _count_jsonl(paths.snippets_needs_review),
        len(read_json(paths.source_profiles).get("profiles", [])),
    )

    snippets_for_07a = _effective_snippets_path(paths)
    logger.info(
        "Stage 06A will harvest entity candidates from %s (%d snippet(s)).",
        snippets_for_07a,
        _count_jsonl(snippets_for_07a),
    )
    _run_stage(
        logger,
        6,
        total_stages,
        "Stage 06A Entity Candidate Harvest",
        run_stage_07,
        snippets_for_07a,
        paths.entity_seed,
        paths.alias_map,
        paths.entity_timelines,
        paths.resolved_entities,
        Path("canon/review_memory.json"),
        paths.entity_candidate_harvest,
        Path("config/pipeline_config.json"),
    )

    _run_stage(
        logger,
        6,
        total_stages,
        "Stage 06B Entity Adjudication",
        run_stage_07b,
        paths.entity_candidate_harvest,
        paths.entity_adjudication_recommendations,
        paths.externality_cache,
        Path("config/pipeline_config.json"),
        Path("canon/theme_profile.json"),
    )

    _run_stage(
        logger,
        6,
        total_stages,
        "Stage 06C Theme Miner",
        run_stage_07c,
        paths.entity_candidate_harvest,
        paths.entity_adjudication_recommendations,
        paths.resolved_entities,
        Path("canon/review_memory.json"),
        Path("canon/theme_profile.json"),
        paths.theme_profile_update_report,
        Path("config/pipeline_config.json"),
    )

    _run_stage(
        logger,
        6,
        total_stages,
        "Stage 06D Theme-Aware Candidate Reclassification",
        run_stage_07d,
        paths.entity_candidate_harvest,
        paths.entity_adjudication_recommendations,
        Path("canon/theme_profile.json"),
        paths.theme_candidate_reclassification,
        Path("config/pipeline_config.json"),
    )

    from pipeline.stage_07e_theme_lineage_web import run as run_stage_07e

    _run_stage(
        logger,
        6,
        total_stages,
        "Stage 06E Theme Lineage Web",
        run_stage_07e,
        Path("canon/theme_profile.json"),
        paths.theme_lineage_web_report,
        paths.theme_lineage_cache,
        Path("config/pipeline_config.json"),
    )

    if _theme_rerun_enabled():
        _run_stage(
            logger,
            6,
            total_stages,
            "Stage 04R Theme-Aware Relevance Rerun",
            run_stage_04r,
            paths.global_timeline,
            paths.conversation_segments,
            paths.resolved_entities,
            Path("canon/theme_profile.json"),
            paths.externality_cache,
            paths.theme_relevance_rerun,
            paths.theme_rescue_messages,
            paths.theme_rescue_segments,
            paths.theme_relevance_rerun_failures,
            Path("config/pipeline_config.json"),
        )
        _run_stage(
            logger,
            6,
            total_stages,
            "Stage 06R Theme Rescue Snippet Extraction",
            run_stage_06r,
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
            Path("config/pipeline_config.json"),
            paths.entity_seed,
            thematic_runtime_path,
        )
        snippets_for_downstream = _effective_snippets_path(paths)

    _run_stage(
        logger,
        7,
        total_stages,
        "Stage 07 Lore Development Ledger",
        run_stage_05_ledger,
        paths.relevant_messages,
        paths.theme_rescue_messages,
        paths.conversation_segments,
        paths.theme_rescue_segments,
        paths.resolved_entities,
        paths.alias_map,
        snippets_for_downstream,
        paths.lore_development_ledger_index,
        paths.lore_development_ledger_jsonl,
        paths.entity_development_history,
        paths.lore_development_ledger_failures,
        Path("config/pipeline_config.json"),
        paths.entity_seed,
    )
    stage_07_index = read_json(paths.lore_development_ledger_index)
    logger.info(
        "Stage 07 summary: entries=%d, segments=%d, failures=%d",
        int(stage_07_index.get("entry_count", 0)),
        int(stage_07_index.get("segment_count", 0)),
        int(stage_07_index.get("failure_count", 0)),
    )

    _run_stage(
        logger,
        8,
        total_stages,
        "Stage 08 Snippet Grouping",
        run_stage_08,
        snippets_for_downstream,
        paths.resolved_entities,
        paths.snippet_clusters_lore,
        paths.snippet_clusters_meta,
        Path("config/pipeline_config.json"),
        thematic_runtime_path,
    )

    _run_stage(
        logger,
        9,
        total_stages,
        "Stage 09 Claim Drafting",
        run_stage_09,
        paths.resolved_entities,
        paths.snippet_clusters_lore,
        paths.snippet_clusters_meta,
        paths.alias_map,
        snippets_for_downstream,
        paths.claim_drafting_dir,
        Path("config/pipeline_config.json"),
        Path("canon/review_memory.json"),
    )
    logger.info("Stage 05 resume complete. Claim draft outputs written under: %s", root)


if __name__ == "__main__":
    main()
