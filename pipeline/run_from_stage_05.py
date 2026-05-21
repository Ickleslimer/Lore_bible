from __future__ import annotations

import argparse
from pathlib import Path
from time import perf_counter

from pipeline.artifact_paths import ArtifactPaths, migrate_run_artifacts_to_numbered
from pipeline.common import get_logger, read_json, read_jsonl, setup_logging
from pipeline.stage_05_conversation_patch_notes import run as run_stage_05
from pipeline.stage_06_snippet_extraction import run as run_stage_06
from pipeline.stage_08_snippet_grouping import run as run_stage_08
from pipeline.stage_07a_entity_candidate_harvest import run as run_stage_07
from pipeline.stage_07b_entity_adjudication import run as run_stage_07b
from pipeline.stage_07c_theme_miner import run as run_stage_07c
from pipeline.stage_07d_theme_reclassification import run as run_stage_07d
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
    parser = argparse.ArgumentParser(description="Resume the THERIAC pipeline from Stage 05 using completed Stage 04 artifacts.")
    parser.add_argument("--artifacts-root", type=Path, required=True)
    parser.add_argument("--log-level", type=str, default=None)
    args = parser.parse_args()
    setup_logging(args.log_level)
    logger = get_logger(__name__)
    root = args.artifacts_root
    migrate_run_artifacts_to_numbered(root)
    paths = ArtifactPaths(root)
    thematic_runtime_path = root / "learning" / "thematic_profile_runtime.json"
    total_stages = 12

    _run_stage(
        logger,
        5,
        total_stages,
        "Stage 05 Conversation Patch Notes",
        run_stage_05,
        paths.relevant_messages,
        paths.conversation_segments,
        paths.conversation_patch_notes,
        paths.conversation_patch_notes_jsonl,
        paths.conversation_patch_note_failures,
        Path("config/pipeline_config.json"),
    )
    stage_05_index = read_json(paths.conversation_patch_notes)
    logger.info(
        "Stage 05 summary: patch_notes=%d, conversations=%d, failures=%d",
        int(stage_05_index.get("notes_count", 0)),
        int(stage_05_index.get("conversation_count", 0)),
        int(stage_05_index.get("failure_count", 0)),
    )

    _run_stage(
        logger,
        6,
        total_stages,
        "Stage 06 Snippet Extraction",
        run_stage_06,
        paths.relevant_messages,
        paths.source_profiles,
        paths.snippets,
        paths.snippets_needs_review,
        paths.source_profiles,
        Path("config/pipeline_config.json"),
        paths.entity_seed,
        thematic_runtime_path,
        paths.conversation_patch_notes,
    )
    logger.info(
        "Stage 06 summary: snippets=%d, needs_review=%d, profiles=%d",
        _count_jsonl(paths.snippets),
        _count_jsonl(paths.snippets_needs_review),
        len(read_json(paths.source_profiles).get("profiles", [])),
    )

    _run_stage(
        logger,
        7,
        total_stages,
        "Stage 07A Entity Candidate Harvest",
        run_stage_07,
        paths.snippets,
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
        7,
        total_stages,
        "Stage 07B Entity Adjudication",
        run_stage_07b,
        paths.entity_candidate_harvest,
        paths.entity_adjudication_recommendations,
        paths.externality_cache,
        Path("config/pipeline_config.json"),
        Path("canon/theme_profile.json"),
    )

    _run_stage(
        logger,
        7,
        total_stages,
        "Stage 07C Theme Miner",
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
        7,
        total_stages,
        "Stage 07D Theme-Aware Candidate Reclassification",
        run_stage_07d,
        paths.entity_candidate_harvest,
        paths.entity_adjudication_recommendations,
        Path("canon/theme_profile.json"),
        paths.theme_candidate_reclassification,
    )

    _run_stage(
        logger,
        8,
        total_stages,
        "Stage 08 Snippet Grouping",
        run_stage_08,
        paths.snippets,
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
        paths.snippets,
        paths.claim_drafting_dir,
        Path("config/pipeline_config.json"),
        Path("canon/review_memory.json"),
    )
    logger.info("Stage 05 resume complete. Claim draft outputs written under: %s", root)


if __name__ == "__main__":
    main()
