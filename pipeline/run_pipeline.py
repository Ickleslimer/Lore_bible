from __future__ import annotations

import argparse
from time import perf_counter
from pathlib import Path

from pipeline.common import get_logger, read_json, read_jsonl, setup_logging
from pipeline.stage_a_bootstrap import run as run_stage_a
from pipeline.stage_b_normalize import run as run_stage_b
from pipeline.stage_b2_global_merge import run as run_stage_b2
from pipeline.stage_b3_segment_conversations import run as run_stage_b3
from pipeline.stage_b4_conversation_patch_notes import run as run_stage_b4
from pipeline.stage_c_extract import run as run_stage_c
from pipeline.stage_d_group import run as run_stage_d
from pipeline.stage_e_alias import run as run_stage_e
from pipeline.stage_f_draft import run as run_stage_f
from pipeline.stage_g_merge_engine import run as run_stage_g
from pipeline.stage_h_notion_export import run as run_stage_h


REVIEW_REQUIRED_EXIT_CODE = 2
REVIEW_GATE_MARKERS = (
    "requiring review",
    "conversation entity proposal",
    "identity merge proposal",
    "claim review",
    "card review",
)


STAGE_TOTAL = 11


def _count_jsonl(path: Path) -> int:
    if not path.exists():
        return 0
    return len(read_jsonl(path))


def _mtime(path: Path) -> float:
    return path.stat().st_mtime if path.exists() else 0.0


def _missing(paths: list[Path]) -> bool:
    return any(not path.exists() for path in paths)


def _newer_than_outputs(inputs: list[Path], outputs: list[Path]) -> bool:
    existing_outputs = [path for path in outputs if path.exists()]
    if not existing_outputs:
        return True
    latest_input = max((_mtime(path) for path in inputs if path.exists()), default=0.0)
    earliest_output = min(_mtime(path) for path in existing_outputs)
    return latest_input > earliest_output


def _has_decisions(path: Path, id_field: str = "proposal_id") -> bool:
    if not path.exists():
        return False
    try:
        payload = read_json(path)
    except Exception:
        return False
    for row in payload.get("decisions", []) if isinstance(payload, dict) else []:
        if isinstance(row, dict) and str(row.get(id_field, "")).strip():
            return True
    return False


def _decision_ids(path: Path, fields: list[str]) -> set[str]:
    if not path.exists():
        return set()
    try:
        payload = read_json(path)
    except Exception:
        return set()
    decisions = payload.get("decisions", []) if isinstance(payload, dict) else []
    out: set[str] = set()
    for decision in decisions:
        if not isinstance(decision, dict):
            continue
        for field in fields:
            value = str(decision.get(field, "")).strip()
            if value:
                out.add(value)
    return out


def _pending_claim_count(root: Path) -> int:
    claims_path = root / "06_drafts" / "card_drafts" / "claim_drafts.json"
    if not claims_path.exists():
        return 0
    try:
        claims = read_json(claims_path).get("claims", [])
    except Exception:
        return 0
    decisions = _decision_ids(root / "07_review" / "claim_review_decisions.json", ["claim_id"])
    return sum(
        1
        for claim in claims
        if isinstance(claim, dict)
        and str(claim.get("claim_id", "")).strip()
        and str(claim.get("claim_id", "")).strip() not in decisions
    )


def _pending_identity_merge_count(root: Path) -> int:
    proposals_path = root / "07_review" / "identity_merge_proposals.json"
    if not proposals_path.exists():
        return 0
    try:
        proposals = read_json(proposals_path).get("proposals", [])
    except Exception:
        return 0
    decisions = _decision_ids(root / "07_review" / "identity_merge_decisions.json", ["proposal_id", "merge_id"])
    return sum(
        1
        for proposal in proposals
        if isinstance(proposal, dict)
        and str(proposal.get("proposal_id", "")).strip()
        and str(proposal.get("proposal_id", "")).strip() not in decisions
        and str(proposal.get("review_status", "pending")).strip().lower() == "pending"
    )


def _pending_card_count(root: Path) -> int:
    cards_path = root / "07_review" / "card_drafts.json"
    if not cards_path.exists():
        return 0
    try:
        cards = read_json(cards_path).get("cards", [])
    except Exception:
        return 0
    decisions = _decision_ids(root / "07_review" / "card_review_decisions.json", ["card_id", "target_card_id"])
    return sum(
        1
        for card in cards
        if isinstance(card, dict)
        and str(card.get("card_id", "")).strip()
        and str(card.get("card_id", "")).strip() not in decisions
    )


def _json_field(path: Path, key: str, default: object = None) -> object:
    if not path.exists():
        return default
    try:
        payload = read_json(path)
    except Exception:
        return default
    return payload.get(key, default) if isinstance(payload, dict) else default


def determine_resume_start_stage(root: Path) -> tuple[int, str]:
    """Return the earliest stage that must run for an existing artifact root.

    A return value of 0 means the current artifacts are up to date through
    Stage 11, or the run is paused for human review and no pipeline stage
    should be started yet.
    """
    stage1 = [root / "01_bootstrap" / "entity_seed.json"]
    stage2 = [root / "02_timeline" / "messages_normalized_per_thread.jsonl", root / "02_timeline" / "summary.json"]
    stage3 = [root / "02_timeline" / "messages_global_timeline.jsonl", root / "02_timeline" / "global_index.json"]
    stage4 = [
        root / "02_timeline" / "messages_relevant_conversations.jsonl",
        root / "02_timeline" / "conversation_segments.json",
        root / "02_timeline" / "conversation_index.json",
    ]
    stage5 = [root / "02_timeline" / "conversation_patch_notes.json"]
    stage6 = [root / "03_relevance" / "snippets_candidates.jsonl", root / "03_relevance" / "dm_source_profiles.json"]
    stage7 = [
        root / "05_alias" / "resolved_entities.json",
        root / "05_alias" / "alias_map.json",
        root / "05_alias" / "entity_timelines.json",
        root / "05_alias" / "conversation_entity_proposals.json",
    ]
    stage8 = [root / "04_grouping" / "snippet_clusters_lore.json", root / "04_grouping" / "snippet_clusters_meta.json"]
    stage9 = [root / "06_drafts" / "card_drafts" / "claim_drafts.json"]
    stage10 = [
        root / "07_review" / "card_drafts.json",
        root / "07_review" / "canonical_cards.json",
        root / "07_review" / "merge_log.jsonl",
    ]
    stage11 = [root / "08_notion" / "notion_import.ndjson"]

    if _missing(stage1):
        return 1, "Stage 01 bootstrap artifacts are missing."
    if _missing(stage2):
        return 2, "Stage 02 normalized message artifacts are missing."
    if _missing(stage3):
        return 3, "Stage 03 merged timeline artifacts are missing."
    if _missing(stage4):
        return 4, "Stage 04 relevant conversation artifacts are missing."
    if _missing(stage5) or str(_json_field(stage5[0], "status", "")).strip().lower() != "complete":
        return 5, "Stage 05 patch notes are missing or incomplete."
    if _missing(stage6) or _newer_than_outputs(stage5 + stage4, stage6):
        return 6, "Stage 06 snippets are missing or older than conversation patch notes."

    conversation_decisions = root / "05_alias" / "conversation_entity_decisions.json"
    if _missing(stage7):
        return 7, "Stage 07 entity resolution artifacts are missing."
    if _has_decisions(conversation_decisions) and _newer_than_outputs([conversation_decisions], stage7):
        return 7, "Stage 07 decisions changed after entity resolution; rerunning entity resolution."

    if _missing(stage8) or _newer_than_outputs(stage6 + [stage7[0]], stage8):
        return 8, "Stage 08 grouping artifacts are missing or stale."
    if _missing(stage9) or _newer_than_outputs(stage8 + [stage7[0], root / "05_alias" / "alias_map.json"], stage9):
        return 9, "Stage 09 claim drafts are missing or stale."

    if _pending_claim_count(root) > 0:
        return 0, "Paused for claim review; approve/reject draft claims before Stage 10 card synthesis."

    claim_decisions = root / "07_review" / "claim_review_decisions.json"
    author_directives = root / "07_review" / "author_directives.json"
    identity_merge_decisions = root / "07_review" / "identity_merge_decisions.json"
    card_decisions = root / "07_review" / "card_review_decisions.json"
    identity_merge_proposals = root / "07_review" / "identity_merge_proposals.json"
    if _pending_identity_merge_count(root) > 0:
        return 0, "Paused for identity merge review; approve/reject identity merge proposals before rerunning Stage 10."
    if _pending_card_count(root) > 0:
        return 0, "Paused for card review; approve/reject synthesized card drafts before canonical merge."
    if _missing(stage10):
        return 10, "Stage 10 card synthesis/canon merge artifacts are missing."
    if _newer_than_outputs(
        [
            stage9[0],
            root / "05_alias" / "resolved_entities.json",
            root / "03_relevance" / "snippets_candidates.jsonl",
            claim_decisions,
            author_directives,
            identity_merge_decisions,
            card_decisions,
        ],
        stage10,
    ):
        return 10, "Stage 10 card synthesis/canon merge artifacts are stale after review decisions."
    if identity_merge_proposals.exists() and _newer_than_outputs([identity_merge_proposals], stage10):
        return 10, "Stage 10 identity merge proposals changed after card synthesis."

    if _missing(stage11) or _newer_than_outputs(
        [
            root / "07_review" / "canonical_cards.json",
            root / "06_drafts" / "card_drafts" / "meta_cards_draft.json",
            root / "05_alias" / "alias_map.json",
            root / "03_relevance" / "snippets_candidates.jsonl",
            root / "03_relevance" / "dm_source_profiles.json",
            root / "07_review" / "merge_log.jsonl",
        ],
        stage11,
    ):
        return 11, "Stage 11 Notion export is missing or stale."

    return 0, "Artifacts are current through Stage 11; no pipeline stage needs to run."


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


def _pause_for_review(logger, stage_idx: int, total_stages: int, stage_name: str, message: str) -> None:
    logger.warning("[%d/%d] REVIEW %s", stage_idx, total_stages, stage_name)
    logger.warning("Pipeline paused for review: %s", message)
    raise SystemExit(REVIEW_REQUIRED_EXIT_CODE)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the full THERIAC lore card pipeline.")
    parser.add_argument("--docx", type=Path, required=True)
    parser.add_argument("--conversations-root", type=Path, required=True)
    parser.add_argument("--artifacts-root", type=Path, required=True)
    parser.add_argument("--log-level", type=str, default=None, help="Logging level (DEBUG, INFO, WARNING, ERROR).")
    parser.add_argument("--resume", action="store_true", help="Resume an existing artifact folder from the earliest stale stage.")
    parser.add_argument("--start-stage", type=int, default=1, help="Expert override: first stage to run, 1-11.")
    args = parser.parse_args()
    setup_logging(args.log_level)
    logger = get_logger(__name__)

    root = args.artifacts_root
    thematic_runtime_path = root / "learning" / "thematic_profile_runtime.json"
    total_stages = STAGE_TOTAL
    start_stage = max(1, min(total_stages, int(args.start_stage or 1)))
    if args.resume:
        start_stage, resume_reason = determine_resume_start_stage(root)
        logger.info("Resume mode selected for %s: %s", root, resume_reason)
        if start_stage <= 0:
            if "review" in resume_reason.lower():
                logger.warning("Pipeline paused for review: %s", resume_reason)
                raise SystemExit(REVIEW_REQUIRED_EXIT_CODE)
            logger.info("Resume complete: %s", resume_reason)
            return

    if start_stage <= 1:
        _run_stage(
            logger,
            1,
            total_stages,
            "Stage 01 Entity Bootstrap",
            run_stage_a,
            args.docx,
            root / "01_bootstrap" / "entity_seed.json",
            root / "01_bootstrap" / "schema_descriptor.json",
            Path("config/pipeline_config.json"),
            thematic_runtime_path,
        )
        seed_payload = read_json(root / "01_bootstrap" / "entity_seed.json")
        logger.info(
            "Stage 01 summary: provider=%s, entities=%d",
            seed_payload.get("provider_mode", "unknown"),
            int(seed_payload.get("entity_count", 0)),
        )
    else:
        logger.info("[1/%d] SKIP  Stage 01 Entity Bootstrap (resume starts at Stage %02d)", total_stages, start_stage)

    if start_stage <= 2:
        _run_stage(
            logger,
            2,
            total_stages,
            "Stage 02 Message Normalization",
            run_stage_b,
            args.conversations_root,
            root / "02_timeline" / "messages_normalized_per_thread.jsonl",
            root / "02_timeline" / "summary.json",
        )
        stage_b_summary = read_json(root / "02_timeline" / "summary.json")
        logger.info(
            "Stage 02 summary: files=%d, normalized_messages=%d, rejected=%d",
            int(stage_b_summary.get("input_files", 0)),
            int(stage_b_summary.get("normalized_messages", 0)),
            int(stage_b_summary.get("rejected_before_cutoff_or_invalid", 0)),
        )
    else:
        logger.info("[2/%d] SKIP  Stage 02 Message Normalization (resume starts at Stage %02d)", total_stages, start_stage)

    if start_stage <= 3:
        _run_stage(
            logger,
            3,
            total_stages,
            "Stage 03 Timeline Merge",
            run_stage_b2,
            root / "02_timeline" / "messages_normalized_per_thread.jsonl",
            root / "02_timeline" / "messages_global_timeline.jsonl",
            root / "02_timeline" / "global_index.json",
        )
        stage_b2_index = read_json(root / "02_timeline" / "global_index.json")
        logger.info(
            "Stage 03 summary: global_messages=%d, threads=%d",
            int(stage_b2_index.get("message_count", 0)),
            len(stage_b2_index.get("thread_counts", {})),
        )
    else:
        logger.info("[3/%d] SKIP  Stage 03 Timeline Merge (resume starts at Stage %02d)", total_stages, start_stage)

    if start_stage <= 4:
        _run_stage(
            logger,
            4,
            total_stages,
            "Stage 04 Relevant Conversation Segmentation",
            run_stage_b3,
            root / "02_timeline" / "messages_global_timeline.jsonl",
            root / "02_timeline" / "messages_relevant_conversations.jsonl",
            root / "02_timeline" / "conversation_segments.json",
            root / "02_timeline" / "conversation_index.json",
            root / "02_timeline" / "conversation_segmentation_failures.json",
            Path("config/pipeline_config.json"),
            root / "01_bootstrap" / "entity_seed.json",
        )
        stage_b3_index = read_json(root / "02_timeline" / "conversation_index.json")
        logger.info(
            "Stage 04 summary: relevant_segments=%d, relevant_messages=%d, dropped=%d, failures=%d",
            int(stage_b3_index.get("relevant_segments", 0)),
            int(stage_b3_index.get("messages_out", 0)),
            int(stage_b3_index.get("dropped_prefilter_windows", 0)),
            int(stage_b3_index.get("failed_model_windows", 0)),
        )
    else:
        logger.info("[4/%d] SKIP  Stage 04 Relevant Conversation Segmentation (resume starts at Stage %02d)", total_stages, start_stage)

    if start_stage <= 5:
        _run_stage(
            logger,
            5,
            total_stages,
            "Stage 05 Conversation Patch Notes",
            run_stage_b4,
            root / "02_timeline" / "messages_relevant_conversations.jsonl",
            root / "02_timeline" / "conversation_segments.json",
            root / "02_timeline" / "conversation_patch_notes.json",
            root / "02_timeline" / "conversation_patch_notes.jsonl",
            root / "02_timeline" / "conversation_patch_note_failures.json",
            Path("config/pipeline_config.json"),
        )
        stage_b4_index = read_json(root / "02_timeline" / "conversation_patch_notes.json")
        logger.info(
            "Stage 05 summary: patch_notes=%d, conversations=%d, failures=%d",
            int(stage_b4_index.get("notes_count", 0)),
            int(stage_b4_index.get("conversation_count", 0)),
            int(stage_b4_index.get("failure_count", 0)),
        )
    else:
        logger.info("[5/%d] SKIP  Stage 05 Conversation Patch Notes (resume starts at Stage %02d)", total_stages, start_stage)

    if start_stage <= 6:
        _run_stage(
            logger,
            6,
            total_stages,
            "Stage 06 Snippet Extraction",
            run_stage_c,
            root / "02_timeline" / "messages_relevant_conversations.jsonl",
            root / "03_relevance" / "dm_source_profiles.json",
            root / "03_relevance" / "snippets_candidates.jsonl",
            root / "03_relevance" / "snippets_needs_review.jsonl",
            root / "03_relevance" / "dm_source_profiles.json",
            Path("config/pipeline_config.json"),
            root / "01_bootstrap" / "entity_seed.json",
            thematic_runtime_path,
            root / "02_timeline" / "conversation_patch_notes.json",
        )
        logger.info(
            "Stage 06 summary: snippets=%d, needs_review=%d, profiles=%d",
            _count_jsonl(root / "03_relevance" / "snippets_candidates.jsonl"),
            _count_jsonl(root / "03_relevance" / "snippets_needs_review.jsonl"),
            len(read_json(root / "03_relevance" / "dm_source_profiles.json").get("profiles", [])),
        )
    else:
        logger.info("[6/%d] SKIP  Stage 06 Snippet Extraction (resume starts at Stage %02d)", total_stages, start_stage)

    if start_stage <= 7:
        _run_stage(
            logger,
            7,
            total_stages,
            "Stage 07 Entity Resolution",
            run_stage_e,
            root / "03_relevance" / "snippets_candidates.jsonl",
            root / "01_bootstrap" / "entity_seed.json",
            root / "05_alias" / "alias_map.json",
            root / "05_alias" / "entity_timelines.json",
            root / "05_alias" / "resolved_entities.json",
            Path("canon/review_memory.json"),
            root / "05_alias" / "conversation_entity_proposals.json",
            root / "05_alias" / "conversation_entity_decisions.json",
            Path("config/pipeline_config.json"),
        )
        logger.info(
            "Stage 07 summary: resolved_entities=%d seed_only_entities=%d conversation_entity_proposals=%d aliases=%d entity_timelines=%d",
            len(read_json(root / "05_alias" / "resolved_entities.json").get("resolved_entities", [])),
            len(read_json(root / "05_alias" / "resolved_entities.json").get("seed_only_entities", [])),
            len(read_json(root / "05_alias" / "conversation_entity_proposals.json").get("proposals", [])),
            len(read_json(root / "05_alias" / "alias_map.json").get("aliases", [])),
            len(read_json(root / "05_alias" / "entity_timelines.json").get("entity_timelines", {})),
        )
    else:
        logger.info("[7/%d] SKIP  Stage 07 Entity Resolution (resume starts at Stage %02d)", total_stages, start_stage)

    if start_stage <= 8:
        _run_stage(
            logger,
            8,
            total_stages,
            "Stage 08 Snippet Grouping",
            run_stage_d,
            root / "03_relevance" / "snippets_candidates.jsonl",
            root / "05_alias" / "resolved_entities.json",
            root / "04_grouping" / "snippet_clusters_lore.json",
            root / "04_grouping" / "snippet_clusters_meta.json",
            Path("config/pipeline_config.json"),
            thematic_runtime_path,
        )
        logger.info(
            "Stage 08 summary: lore_clusters=%d, meta_clusters=%d",
            len(read_json(root / "04_grouping" / "snippet_clusters_lore.json").get("clusters", [])),
            len(read_json(root / "04_grouping" / "snippet_clusters_meta.json").get("clusters", [])),
        )
    else:
        logger.info("[8/%d] SKIP  Stage 08 Snippet Grouping (resume starts at Stage %02d)", total_stages, start_stage)

    if start_stage <= 9:
        _run_stage(
            logger,
            9,
            total_stages,
            "Stage 09 Claim Drafting",
            run_stage_f,
            root / "05_alias" / "resolved_entities.json",
            root / "04_grouping" / "snippet_clusters_lore.json",
            root / "04_grouping" / "snippet_clusters_meta.json",
            root / "05_alias" / "alias_map.json",
            root / "03_relevance" / "snippets_candidates.jsonl",
            root / "06_drafts" / "card_drafts",
            Path("config/pipeline_config.json"),
            Path("canon/review_memory.json"),
        )
        logger.info(
            "Stage 09 summary: claim_drafts=%d, meta_cards=%d",
            len(read_json(root / "06_drafts" / "card_drafts" / "claim_drafts.json").get("claims", [])),
            len(read_json(root / "06_drafts" / "card_drafts" / "meta_cards_draft.json").get("meta_cards", [])),
        )
    else:
        logger.info("[9/%d] SKIP  Stage 09 Claim Drafting (resume starts at Stage %02d)", total_stages, start_stage)

    pending_claims = _pending_claim_count(root)
    if pending_claims:
        _pause_for_review(
            logger,
            9,
            total_stages,
            "Stage 09 Claim Drafting",
            f"Stage 09 produced {pending_claims} claim(s) requiring claim review before Stage 10.",
        )

    if start_stage <= 10:
        _run_stage(
            logger,
            10,
            total_stages,
            "Stage 10 Card Synthesis",
            run_stage_g,
            root / "05_alias" / "resolved_entities.json",
            root / "06_drafts" / "card_drafts" / "claim_drafts.json",
            root / "07_review" / "claim_review_decisions.json",
            root / "07_review" / "card_review_decisions.json",
            root / "07_review" / "author_directives.json",
            Path("canon/review_memory.json"),
            root / "07_review" / "card_drafts.json",
            root / "07_review" / "canonical_cards.json",
            root / "07_review" / "merge_log.jsonl",
            Path("config/pipeline_config.json"),
            root / "03_relevance" / "snippets_candidates.jsonl",
        )
        logger.info(
            "Stage 10 summary: card_drafts=%d canonical_cards=%d merge_log=%d",
            len(read_json(root / "07_review" / "card_drafts.json").get("cards", [])),
            len(read_json(root / "07_review" / "canonical_cards.json").get("cards", [])),
            _count_jsonl(root / "07_review" / "merge_log.jsonl"),
        )
    else:
        logger.info("[10/%d] SKIP  Stage 10 Card Synthesis (resume starts at Stage %02d)", total_stages, start_stage)

    pending_cards = _pending_card_count(root)
    if pending_cards:
        _pause_for_review(
            logger,
            10,
            total_stages,
            "Stage 10 Card Synthesis",
            f"Stage 10 produced {pending_cards} card draft(s) requiring card review before Stage 11 export.",
        )

    if start_stage <= 11:
        _run_stage(
            logger,
            11,
            total_stages,
            "Stage 11 Notion Export",
            run_stage_h,
            root / "07_review" / "canonical_cards.json",
            root / "06_drafts" / "card_drafts" / "meta_cards_draft.json",
            root / "05_alias" / "alias_map.json",
            root / "03_relevance" / "snippets_candidates.jsonl",
            root / "03_relevance" / "dm_source_profiles.json",
            root / "07_review" / "merge_log.jsonl",
            root / "08_notion" / "notion_import.ndjson",
        )
        logger.info(
            "Stage 11 summary: notion_records=%d",
            _count_jsonl(root / "08_notion" / "notion_import.ndjson"),
        )
    else:
        logger.info("[11/%d] SKIP  Stage 11 Notion Export (resume starts at Stage %02d)", total_stages, start_stage)
    logger.info("Pipeline complete. Notion export written under: %s", root / "08_notion")


if __name__ == "__main__":
    main()
