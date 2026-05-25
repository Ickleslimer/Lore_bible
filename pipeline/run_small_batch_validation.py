from __future__ import annotations

import argparse
import shutil
from pathlib import Path
from time import perf_counter

from pipeline.artifact_paths import ArtifactPaths
from pipeline.common import CUTOFF_UTC, get_logger, parse_discord_timestamp, read_json, read_jsonl, setup_logging, write_json
from pipeline.stage_01_entity_bootstrap import run as run_stage_01
from pipeline.stage_02_message_normalization import run as run_stage_02
from pipeline.stage_03_timeline_merge import run as run_stage_03
from pipeline.stage_04_conversation_segmentation import run as run_stage_04
from pipeline.stage_05_lore_development_ledger import run as run_stage_05_ledger
from pipeline.stage_06_snippet_extraction import run as run_stage_06
from pipeline.stage_08_snippet_grouping import run as run_stage_08
from pipeline.stage_07a_entity_candidate_harvest import run as run_stage_07
from pipeline.stage_07b_entity_adjudication import run as run_stage_07b
from pipeline.stage_07c_theme_miner import run as run_stage_07c
from pipeline.stage_07d_theme_reclassification import run as run_stage_07d
from pipeline.stage_09_claim_drafting import run as run_stage_09
from pipeline.stage_10_identity_merge import run as run_stage_10
from pipeline.stage_11_card_synthesis import run as run_stage_11
from pipeline.stage_12_notion_export import run as run_stage_12


REVIEW_REQUIRED_EXIT_CODE = 2
REVIEW_GATE_MARKERS = (
    "requiring review",
    "conversation entity proposal",
    "identity merge proposal",
)


DEFAULT_BASE_DIR = Path("artifacts")
DEFAULT_CONVERSATIONS_ROOT = Path("discord_conversations")
DEFAULT_DOCX_CANDIDATES = [
    Path("theriac-coda---lore-bible.docx"),
]


def _count_jsonl(path: Path) -> int:
    if not path.exists():
        return 0
    return len(read_jsonl(path))


def _run_stage(logger, stage_idx: int, total_stages: int, stage_name: str, fn, *args) -> float:
    logger.info("[%d/%d] START %s", stage_idx, total_stages, stage_name)
    start = perf_counter()
    try:
        fn(*args)
    except RuntimeError as exc:
        if any(marker in str(exc).lower() for marker in REVIEW_GATE_MARKERS):
            elapsed = perf_counter() - start
            logger.warning("[%d/%d] REVIEW %s (%.2fs)", stage_idx, total_stages, stage_name, elapsed)
            logger.warning("Pipeline paused for review: %s", exc)
            raise SystemExit(REVIEW_REQUIRED_EXIT_CODE) from None
        raise
    elapsed = perf_counter() - start
    logger.info("[%d/%d] DONE  %s (%.2fs)", stage_idx, total_stages, stage_name, elapsed)
    return elapsed


def _resolve_docx(docx_path: Path | None) -> Path:
    if docx_path is not None:
        return docx_path
    for candidate in DEFAULT_DOCX_CANDIDATES:
        if candidate.exists():
            return candidate
    discovered = sorted(Path(".").glob("*.docx"))
    if len(discovered) == 1:
        return discovered[0]
    raise SystemExit(
        "Could not locate lore bible DOCX. Pass --docx with the file path."
    )


def _resolve_inputs(
    base_dir: Path | None,
    conversations_root: Path | None,
    docx_path: Path | None,
) -> tuple[Path, Path, Path]:
    resolved_base = base_dir or DEFAULT_BASE_DIR
    resolved_conversations = conversations_root or DEFAULT_CONVERSATIONS_ROOT
    resolved_docx = _resolve_docx(docx_path)

    if not resolved_conversations.exists():
        raise SystemExit(
            f"Conversations root not found: {resolved_conversations}. "
            "Pass --conversations-root."
        )
    if not resolved_docx.exists():
        raise SystemExit(f"DOCX file not found: {resolved_docx}")

    return resolved_base, resolved_conversations, resolved_docx


def _file_recency_score(json_path: Path) -> float:
    """
    Use latest message timestamp as recency score.
    Returns unix timestamp; very old fallback when unreadable.
    """
    try:
        payload = read_json(json_path)
        if not isinstance(payload, list) or not payload:
            return 0.0
        latest = None
        for msg in payload:
            if not isinstance(msg, dict):
                continue
            raw_ts = msg.get("timestamp")
            if not raw_ts:
                continue
            ts = parse_discord_timestamp(str(raw_ts))
            if latest is None or ts > latest:
                latest = ts
        if latest is None:
            return 0.0
        return latest.timestamp()
    except Exception:
        return 0.0


def _looks_post_cutoff(json_path: Path) -> bool:
    try:
        payload = read_json(json_path)
        if not isinstance(payload, list) or not payload:
            return False
        for msg in payload:
            if not isinstance(msg, dict):
                continue
            raw_ts = msg.get("timestamp")
            if not raw_ts:
                continue
            if parse_discord_timestamp(str(raw_ts)) >= CUTOFF_UTC:
                return True
        return False
    except Exception:
        return False


def create_auto_decisions(claim_drafts_path: Path, out_decisions_path: Path) -> None:
    payload = read_json(claim_drafts_path)
    decisions = []
    for claim in payload.get("claims", [])[:20]:
        decision = "accept" if claim.get("confidence", 0) >= 0.65 else "defer"
        decisions.append(
            {
                "claim_id": claim["claim_id"],
                "decision": decision,
                "reviewer": "auto_small_batch_validation",
                "rationale": "Automated claim validation decision for pipeline smoke test.",
            }
        )
    write_json(out_decisions_path, {"decisions": decisions})


def run(base_dir: Path, conversations_root: Path, docx_path: Path, sample_limit_files: int) -> None:
    logger = get_logger(__name__)
    work = base_dir / "small_batch"
    paths = ArtifactPaths(work)
    thematic_runtime_path = work / "learning" / "thematic_profile_runtime.json"
    if work.exists():
        logger.info("Removing previous small-batch workspace: %s", work)
        shutil.rmtree(work)
    work.mkdir(parents=True, exist_ok=True)
    logger.info("Small-batch workspace ready: %s", work)

    # Optional small-batch subset
    subset_root = work / "subset_conversations"
    subset_root.mkdir(parents=True, exist_ok=True)
    candidate_files = sorted(conversations_root.rglob("*.json"))
    post_cutoff_files = [p for p in candidate_files if _looks_post_cutoff(p)]
    ranked = sorted(post_cutoff_files if post_cutoff_files else candidate_files, key=_file_recency_score, reverse=True)
    all_files = ranked[:sample_limit_files]
    logger.info(
        "Preparing subset conversations: selected %d of requested %d file(s) (post_cutoff_pool=%d total_pool=%d)",
        len(all_files),
        sample_limit_files,
        len(post_cutoff_files),
        len(candidate_files),
    )
    for src in all_files:
        dst = subset_root / src.relative_to(conversations_root)
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, dst)
        logger.debug("Copied subset file: %s -> %s", src, dst)

    total_stages = 12
    _run_stage(
        logger,
        1,
        total_stages,
        "Stage 01 Entity Bootstrap",
        run_stage_01,
        docx_path,
        paths.entity_seed,
        paths.schema_descriptor,
        base_dir.parent / "config" / "pipeline_config.json",
        thematic_runtime_path,
    )
    seed_payload = read_json(paths.entity_seed)
    logger.info(
        "Stage 01 summary: provider=%s, entities=%d",
        seed_payload.get("provider_mode", "unknown"),
        int(seed_payload.get("entity_count", 0)),
    )

    _run_stage(
        logger,
        2,
        total_stages,
        "Stage 02 Message Normalization",
        run_stage_02,
        subset_root,
        paths.normalized_messages,
        paths.normalization_summary,
    )
    stage_02_summary = read_json(paths.normalization_summary)
    logger.info(
        "Stage 02 summary: files=%d, normalized_messages=%d, rejected=%d",
        int(stage_02_summary.get("input_files", 0)),
        int(stage_02_summary.get("normalized_messages", 0)),
        int(stage_02_summary.get("rejected_before_cutoff_or_invalid", 0)),
    )

    _run_stage(
        logger,
        3,
        total_stages,
        "Stage 03 Timeline Merge",
        run_stage_03,
        paths.normalized_messages,
        paths.global_timeline,
        paths.global_index,
    )
    stage_03_index = read_json(paths.global_index)
    logger.info(
        "Stage 03 summary: global_messages=%d, threads=%d",
        int(stage_03_index.get("message_count", 0)),
        len(stage_03_index.get("thread_counts", {})),
    )

    _run_stage(
        logger,
        4,
        total_stages,
        "Stage 04 Relevant Conversation Segmentation",
        run_stage_04,
        paths.global_timeline,
        paths.relevant_messages,
        paths.conversation_segments,
        paths.conversation_index,
        paths.conversation_segmentation_failures,
        base_dir.parent / "config" / "pipeline_config.json",
        paths.entity_seed,
    )
    stage_04_index = read_json(paths.conversation_index)
    logger.info(
        "Stage 04 summary: relevant_segments=%d, relevant_messages=%d, dropped=%d, failures=%d",
        int(stage_04_index.get("relevant_segments", 0)),
        int(stage_04_index.get("messages_out", 0)),
        int(stage_04_index.get("dropped_prefilter_windows", 0)),
        int(stage_04_index.get("failed_model_windows", 0)),
    )

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
        base_dir.parent / "config" / "pipeline_config.json",
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

    _run_stage(
        logger,
        6,
        total_stages,
        "Stage 06A Entity Candidate Harvest",
        run_stage_07,
        paths.snippets,
        paths.entity_seed,
        paths.alias_map,
        paths.entity_timelines,
        paths.resolved_entities,
        base_dir.parent / "canon" / "review_memory.json",
        paths.entity_candidate_harvest,
        base_dir.parent / "config" / "pipeline_config.json",
    )
    logger.info(
        "Stage 06A summary: resolved_entities=%d seed_only_entities=%d entity_candidates=%d aliases=%d entity_timelines=%d",
        len(read_json(paths.resolved_entities).get("resolved_entities", [])),
        len(read_json(paths.resolved_entities).get("seed_only_entities", [])),
        len(read_json(paths.entity_candidate_harvest).get("candidates", [])),
        len(read_json(paths.alias_map).get("aliases", [])),
        len(read_json(paths.entity_timelines).get("entity_timelines", {})),
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
        base_dir.parent / "config" / "pipeline_config.json",
        base_dir.parent / "canon" / "theme_profile.json",
    )
    stage_07b = read_json(paths.entity_adjudication_recommendations)
    logger.info(
        "Stage 06B summary: recommendations=%d web_selected=%d web_calls=%d cache_hits=%d failures=%d",
        len(stage_07b.get("recommendations", [])),
        int(stage_07b.get("summary", {}).get("web_selected_candidate_count", 0)),
        int(stage_07b.get("summary", {}).get("web_call_count", 0)),
        int(stage_07b.get("summary", {}).get("cache_hit_count", 0)),
        int(stage_07b.get("summary", {}).get("failure_count", 0)),
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
        base_dir.parent / "canon" / "review_memory.json",
        base_dir.parent / "canon" / "theme_profile.json",
        paths.theme_profile_update_report,
        base_dir.parent / "config" / "pipeline_config.json",
    )
    stage_07c = read_json(paths.theme_profile_update_report)
    logger.info(
        "Stage 06C summary: themes=%d evidence_packets=%d applied_updates=%d failures=%d",
        int(stage_07c.get("summary", {}).get("theme_count", 0)),
        int(stage_07c.get("inputs", {}).get("evidence_packet_count", 0)),
        int(stage_07c.get("summary", {}).get("applied_update_count", 0)),
        int(stage_07c.get("summary", {}).get("failure_count", 0)),
    )
    _run_stage(
        logger,
        6,
        total_stages,
        "Stage 06D Theme-Aware Candidate Reclassification",
        run_stage_07d,
        paths.entity_candidate_harvest,
        paths.entity_adjudication_recommendations,
        base_dir.parent / "canon" / "theme_profile.json",
        paths.theme_candidate_reclassification,
        base_dir.parent / "config" / "pipeline_config.json",
    )
    stage_07d = read_json(paths.theme_candidate_reclassification)
    logger.info(
        "Stage 06D summary: reclassifications=%d theme_matched=%d",
        len(stage_07d.get("candidate_reclassifications", [])),
        int(stage_07d.get("summary", {}).get("theme_matched_candidate_count", 0)),
    )

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
        paths.snippets,
        paths.lore_development_ledger_index,
        paths.lore_development_ledger_jsonl,
        paths.entity_development_history,
        paths.lore_development_ledger_failures,
        base_dir.parent / "config" / "pipeline_config.json",
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
        paths.snippets,
        paths.resolved_entities,
        paths.snippet_clusters_lore,
        paths.snippet_clusters_meta,
        base_dir.parent / "config" / "pipeline_config.json",
        thematic_runtime_path,
    )
    logger.info(
        "Stage 08 summary: lore_clusters=%d, meta_clusters=%d",
        len(read_json(paths.snippet_clusters_lore).get("clusters", [])),
        len(read_json(paths.snippet_clusters_meta).get("clusters", [])),
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
        base_dir.parent / "config" / "pipeline_config.json",
        base_dir.parent / "canon" / "review_memory.json",
    )
    logger.info(
        "Stage 09 summary: claim_drafts=%d, meta_cards=%d",
        len(read_json(paths.claim_drafts).get("claims", [])),
        len(read_json(paths.meta_cards_draft).get("meta_cards", [])),
    )
    create_auto_decisions(
        paths.claim_drafts,
        paths.claim_review_decisions,
    )
    write_json(paths.card_review_decisions, {"decisions": []})
    logger.info(
        "Auto claim decisions summary: decisions=%d",
        len(read_json(paths.claim_review_decisions).get("decisions", [])),
    )

    _run_stage(
        logger,
        10,
        total_stages,
        "Stage 10 Identity Merge",
        run_stage_10,
        paths.resolved_entities,
        paths.claim_drafts,
        paths.claim_review_decisions,
        base_dir.parent / "canon" / "review_memory.json",
        paths.identity_merge_proposals,
        paths.identity_merge_decisions,
        base_dir.parent / "config" / "pipeline_config.json",
    )
    logger.info(
        "Stage 10 summary: identity_merge_proposals=%d",
        len(read_json(paths.identity_merge_proposals).get("proposals", [])),
    )

    _run_stage(
        logger,
        11,
        total_stages,
        "Stage 11 Card Synthesis and Canon Merge",
        run_stage_11,
        paths.resolved_entities,
        paths.claim_drafts,
        paths.claim_review_decisions,
        paths.card_review_decisions,
        paths.author_directives,
        base_dir.parent / "canon" / "review_memory.json",
        paths.card_drafts,
        paths.canonical_cards,
        paths.merge_log,
        base_dir.parent / "config" / "pipeline_config.json",
        paths.snippets,
    )
    logger.info(
        "Stage 11 summary: draft_cards=%d canonical_cards=%d merge_log=%d",
        len(read_json(paths.card_drafts).get("cards", [])),
        len(read_json(paths.canonical_cards).get("cards", [])),
        _count_jsonl(paths.merge_log),
    )

    _run_stage(
        logger,
        12,
        total_stages,
        "Stage 12 Notion Export",
        run_stage_12,
        paths.canonical_cards,
        paths.meta_cards_draft,
        paths.alias_map,
        paths.snippets,
        paths.source_profiles,
        paths.merge_log,
        paths.notion_import,
    )
    out_ndjson = paths.notion_import
    exported_rows = 0
    if out_ndjson.exists():
        with out_ndjson.open("r", encoding="utf-8") as f:
            exported_rows = sum(1 for _ in f)
    logger.info("Stage 12 summary: notion_records=%d", exported_rows)
    logger.info("Small-batch validation complete. Output root: %s", work)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run a small-batch end-to-end validation pipeline."
    )
    parser.add_argument("--base-dir", type=Path, required=False)
    parser.add_argument("--conversations-root", type=Path, required=False)
    parser.add_argument("--docx", type=Path, required=False)
    parser.add_argument("--sample-limit-files", type=int, default=6)
    parser.add_argument("--log-level", type=str, default=None, help="Logging level (DEBUG, INFO, WARNING, ERROR).")
    args = parser.parse_args()
    setup_logging(args.log_level)
    base_dir, conversations_root, docx = _resolve_inputs(
        args.base_dir,
        args.conversations_root,
        args.docx,
    )
    run(base_dir, conversations_root, docx, args.sample_limit_files)


if __name__ == "__main__":
    main()
