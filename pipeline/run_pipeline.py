from __future__ import annotations

import argparse
from time import perf_counter
from pathlib import Path

from pipeline.common import get_logger, read_json, read_jsonl, setup_logging
from pipeline.stage_a_bootstrap import run as run_stage_a
from pipeline.stage_b_normalize import run as run_stage_b
from pipeline.stage_b2_global_merge import run as run_stage_b2
from pipeline.stage_c_extract import run as run_stage_c
from pipeline.stage_d_group import run as run_stage_d
from pipeline.stage_e_alias import run as run_stage_e
from pipeline.stage_f_draft import run as run_stage_f


def _count_jsonl(path: Path) -> int:
    if not path.exists():
        return 0
    return len(read_jsonl(path))


def _run_stage(logger, stage_idx: int, total_stages: int, stage_name: str, fn, *args) -> float:
    logger.info("[%d/%d] START %s", stage_idx, total_stages, stage_name)
    start = perf_counter()
    fn(*args)
    elapsed = perf_counter() - start
    logger.info("[%d/%d] DONE  %s (%.2fs)", stage_idx, total_stages, stage_name, elapsed)
    return elapsed


def main() -> None:
    parser = argparse.ArgumentParser(description="Run THERIAC pipeline stages up to drafts.")
    parser.add_argument("--docx", type=Path, required=True)
    parser.add_argument("--conversations-root", type=Path, required=True)
    parser.add_argument("--artifacts-root", type=Path, required=True)
    parser.add_argument("--log-level", type=str, default=None, help="Logging level (DEBUG, INFO, WARNING, ERROR).")
    args = parser.parse_args()
    setup_logging(args.log_level)
    logger = get_logger(__name__)

    root = args.artifacts_root
    thematic_runtime_path = root / "learning" / "thematic_profile_runtime.json"
    total_stages = 7
    _run_stage(
        logger,
        1,
        total_stages,
        "Stage A Bootstrap",
        run_stage_a,
        args.docx,
        root / "01_bootstrap" / "canon_seed.json",
        root / "01_bootstrap" / "schema_descriptor.json",
        Path("config/pipeline_config.json"),
        thematic_runtime_path,
    )
    seed_payload = read_json(root / "01_bootstrap" / "canon_seed.json")
    logger.info(
        "Stage A summary: provider=%s, entities=%d",
        seed_payload.get("provider_mode", "unknown"),
        int(seed_payload.get("entity_count", 0)),
    )

    _run_stage(
        logger,
        2,
        total_stages,
        "Stage B Normalize",
        run_stage_b,
        args.conversations_root,
        root / "02_timeline" / "messages_normalized_per_thread.jsonl",
        root / "02_timeline" / "summary.json",
    )
    stage_b_summary = read_json(root / "02_timeline" / "summary.json")
    logger.info(
        "Stage B summary: files=%d, normalized_messages=%d, rejected=%d",
        int(stage_b_summary.get("input_files", 0)),
        int(stage_b_summary.get("normalized_messages", 0)),
        int(stage_b_summary.get("rejected_before_cutoff_or_invalid", 0)),
    )

    _run_stage(
        logger,
        3,
        total_stages,
        "Stage B2 Global Merge",
        run_stage_b2,
        root / "02_timeline" / "messages_normalized_per_thread.jsonl",
        root / "02_timeline" / "messages_global_timeline.jsonl",
        root / "02_timeline" / "global_index.json",
    )
    stage_b2_index = read_json(root / "02_timeline" / "global_index.json")
    logger.info(
        "Stage B2 summary: global_messages=%d, threads=%d",
        int(stage_b2_index.get("message_count", 0)),
        len(stage_b2_index.get("thread_counts", {})),
    )

    _run_stage(
        logger,
        4,
        total_stages,
        "Stage C Extract",
        run_stage_c,
        root / "02_timeline" / "messages_global_timeline.jsonl",
        root / "03_relevance" / "dm_source_profiles.json",
        root / "03_relevance" / "snippets_candidates.jsonl",
        root / "03_relevance" / "snippets_needs_review.jsonl",
        root / "03_relevance" / "dm_source_profiles.json",
        Path("config/pipeline_config.json"),
        root / "01_bootstrap" / "canon_seed.json",
        thematic_runtime_path,
    )
    logger.info(
        "Stage C summary: snippets=%d, needs_review=%d, profiles=%d",
        _count_jsonl(root / "03_relevance" / "snippets_candidates.jsonl"),
        _count_jsonl(root / "03_relevance" / "snippets_needs_review.jsonl"),
        len(read_json(root / "03_relevance" / "dm_source_profiles.json").get("profiles", [])),
    )

    _run_stage(
        logger,
        5,
        total_stages,
        "Stage D Group",
        run_stage_d,
        root / "03_relevance" / "snippets_candidates.jsonl",
        root / "01_bootstrap" / "canon_seed.json",
        root / "04_grouping" / "snippet_clusters_lore.json",
        root / "04_grouping" / "snippet_clusters_meta.json",
        Path("config/pipeline_config.json"),
        thematic_runtime_path,
    )
    logger.info(
        "Stage D summary: lore_clusters=%d, meta_clusters=%d",
        len(read_json(root / "04_grouping" / "snippet_clusters_lore.json").get("clusters", [])),
        len(read_json(root / "04_grouping" / "snippet_clusters_meta.json").get("clusters", [])),
    )

    _run_stage(
        logger,
        6,
        total_stages,
        "Stage E Alias",
        run_stage_e,
        root / "03_relevance" / "snippets_candidates.jsonl",
        root / "01_bootstrap" / "canon_seed.json",
        root / "05_alias" / "alias_map.json",
        root / "05_alias" / "entity_timelines.json",
    )
    logger.info(
        "Stage E summary: aliases=%d, entity_timelines=%d",
        len(read_json(root / "05_alias" / "alias_map.json").get("aliases", [])),
        len(read_json(root / "05_alias" / "entity_timelines.json").get("entity_timelines", {})),
    )

    _run_stage(
        logger,
        7,
        total_stages,
        "Stage F Draft",
        run_stage_f,
        root / "01_bootstrap" / "canon_seed.json",
        root / "04_grouping" / "snippet_clusters_lore.json",
        root / "04_grouping" / "snippet_clusters_meta.json",
        root / "05_alias" / "alias_map.json",
        root / "03_relevance" / "snippets_candidates.jsonl",
        root / "06_drafts" / "card_drafts",
    )
    logger.info(
        "Stage F summary: lore_patches=%d, meta_cards=%d",
        len(read_json(root / "06_drafts" / "card_drafts" / "lore_patches.json").get("patches", [])),
        len(read_json(root / "06_drafts" / "card_drafts" / "meta_cards_draft.json").get("meta_cards", [])),
    )
    logger.info("Pipeline complete. Draft outputs written under: %s", root)


if __name__ == "__main__":
    main()
