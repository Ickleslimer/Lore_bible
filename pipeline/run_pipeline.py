from __future__ import annotations

import argparse
from datetime import datetime, timezone
from time import perf_counter
from pathlib import Path

from pipeline.artifact_paths import ArtifactPaths, migrate_run_artifacts_to_numbered
from pipeline.common import get_logger, read_json, read_jsonl, setup_logging, write_json
from pipeline.stage_01_entity_bootstrap import run as run_stage_01
from pipeline.stage_02_message_normalization import run as run_stage_02
from pipeline.stage_03_timeline_merge import run as run_stage_03
from pipeline.stage_04_conversation_segmentation import run as run_stage_04
from pipeline.stage_05_conversation_patch_notes import run as run_stage_05
from pipeline.stage_06_snippet_extraction import run as run_stage_06
from pipeline.stage_08_snippet_grouping import run as run_stage_08
from pipeline.stage_07a_entity_candidate_harvest import run as run_stage_07
from pipeline.stage_07b_entity_adjudication import run as run_stage_07b
from pipeline.stage_07c_theme_miner import run as run_stage_07c
from pipeline.stage_07d_theme_reclassification import run as run_stage_07d
from pipeline.stage_04r_theme_relevance_rerun import run as run_stage_04r
from pipeline.stage_06r_theme_rescue_snippet_extraction import run as run_stage_06r
from pipeline.stage_09_claim_drafting import run as run_stage_09
from pipeline.stage_10_identity_merge import run as run_stage_10
from pipeline.stage_11_card_synthesis import run as run_stage_11
from pipeline.stage_12_notion_export import run as run_stage_12
from pipeline.notion_draft_sync import sync_draft_cards_to_notion
from pipeline.card_architecture_agent import load_card_edit_requests, load_card_architecture_proposals, pending_card_architecture_actions


REVIEW_REQUIRED_EXIT_CODE = 2
REVIEW_GATE_MARKERS = (
    "requiring review",
    "conversation entity proposal",
    "identity merge proposal",
    "card architecture proposal",
    "claim review",
    "card review",
)


STAGE_TOTAL = 12


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
    p = ArtifactPaths(root)
    claims_path = p.claim_drafts
    if not claims_path.exists():
        return 0
    try:
        claims = read_json(claims_path).get("claims", [])
    except Exception:
        return 0
    decisions = _decision_ids(p.claim_review_decisions, ["claim_id"])
    return sum(
        1
        for claim in claims
        if isinstance(claim, dict)
        and str(claim.get("claim_id", "")).strip()
        and str(claim.get("claim_id", "")).strip() not in decisions
    )


def _pending_identity_merge_count(root: Path) -> int:
    p = ArtifactPaths(root)
    proposals_path = p.identity_merge_proposals
    if not proposals_path.exists():
        return 0
    try:
        proposals = read_json(proposals_path).get("proposals", [])
    except Exception:
        return 0
    decisions = _decision_ids(p.identity_merge_decisions, ["proposal_id", "merge_id"])
    return sum(
        1
        for proposal in proposals
        if isinstance(proposal, dict)
        and str(proposal.get("proposal_id", "")).strip()
        and str(proposal.get("proposal_id", "")).strip() not in decisions
        and str(proposal.get("review_status", "pending")).strip().lower() == "pending"
    )


def _pending_card_count(root: Path) -> int:
    p = ArtifactPaths(root)
    cards_path = p.card_drafts
    if not cards_path.exists():
        return 0
    try:
        cards = read_json(cards_path).get("cards", [])
    except Exception:
        return 0
    decisions = _decision_ids(p.card_review_decisions, ["card_id", "target_card_id"])
    return sum(
        1
        for card in cards
        if isinstance(card, dict)
        and str(card.get("card_id", "")).strip()
        and str(card.get("card_id", "")).strip() not in decisions
    )


def _pending_card_architecture_count(root: Path) -> int:
    p = ArtifactPaths(root)
    try:
        return len(
            pending_card_architecture_actions(
                p.card_architecture_proposals,
                p.card_architecture_decisions,
            )
        )
    except Exception:
        return 0


def _unproposed_card_edit_request_count(root: Path) -> int:
    p = ArtifactPaths(root)
    requests_path = p.card_edit_requests
    proposals_path = p.card_architecture_proposals
    if not requests_path.exists():
        return 0
    try:
        requests = load_card_edit_requests(requests_path)
        proposals = load_card_architecture_proposals(proposals_path)
    except Exception:
        return 0
    covered = {
        str(proposal.get("request_id", "")).strip()
        for proposal in proposals
        if isinstance(proposal, dict) and str(proposal.get("request_id", "")).strip()
    }
    return sum(
        1
        for request in requests
        if isinstance(request, dict)
        and str(request.get("status", "pending")).strip().lower() == "pending"
        and str(request.get("request_id", "")).strip()
        and str(request.get("request_id", "")).strip() not in covered
    )


def _json_field(path: Path, key: str, default: object = None) -> object:
    if not path.exists():
        return default
    try:
        payload = read_json(path)
    except Exception:
        return default
    return payload.get(key, default) if isinstance(payload, dict) else default


def _load_pipeline_config() -> dict[str, object]:
    path = Path("config/pipeline_config.json")
    if not path.exists():
        return {}
    try:
        payload = read_json(path)
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _theme_rerun_enabled() -> bool:
    cfg = _load_pipeline_config()
    raw = cfg.get("theme_aware_rerun", {}) if isinstance(cfg, dict) else {}
    return bool(raw.get("enabled", False)) if isinstance(raw, dict) else False


def _effective_snippets_path(paths: ArtifactPaths) -> Path:
    return paths.snippets_with_theme_rescue if paths.snippets_with_theme_rescue.exists() else paths.snippets


def _effective_snippets_review_path(paths: ArtifactPaths) -> Path:
    return paths.snippets_needs_review_with_theme_rescue if paths.snippets_needs_review_with_theme_rescue.exists() else paths.snippets_needs_review


def determine_resume_start_stage(root: Path, ignore_pending: bool = False) -> tuple[int, str]:
    """Return the earliest stage that must run for an existing artifact root.

    A return value of 0 means the current artifacts are up to date through
    Stage 12, or the run is paused for human review and no pipeline stage
    should be started yet.
    """
    migrate_run_artifacts_to_numbered(root)
    p = ArtifactPaths(root)
    stage1 = [p.entity_seed]
    stage2 = [p.normalized_messages, p.normalization_summary]
    stage3 = [p.global_timeline, p.global_index]
    stage4 = [
        p.relevant_messages,
        p.conversation_segments,
        p.conversation_index,
    ]
    stage5 = [p.conversation_patch_notes]
    stage6 = [p.snippets, p.source_profiles]
    stage7 = [
        p.resolved_entities,
        p.alias_map,
        p.entity_timelines,
        p.entity_candidate_harvest,
        p.entity_adjudication_recommendations,
        p.externality_cache,
        p.theme_profile_update_report,
        p.theme_candidate_reclassification,
    ]
    theme_rerun_enabled = _theme_rerun_enabled()
    if theme_rerun_enabled:
        stage7.extend([p.theme_relevance_rerun, p.snippets_with_theme_rescue])
    effective_snippets = _effective_snippets_path(p)
    stage8 = [p.snippet_clusters_lore, p.snippet_clusters_meta]
    stage9 = [p.claim_drafts]
    stage10 = [p.identity_merge_proposals]
    stage11 = [
        p.card_drafts,
        p.canonical_cards,
        p.merge_log,
    ]
    stage12 = [p.notion_import]

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

    if _missing(stage7):
        return 7, "Stage 07A/07B entity candidate harvest/adjudication artifacts are missing."

    if _missing(stage8) or _newer_than_outputs([effective_snippets, p.resolved_entities], stage8):
        return 8, "Stage 08 grouping artifacts are missing or stale."
    if _missing(stage9) or _newer_than_outputs(stage8 + [p.resolved_entities, p.alias_map, effective_snippets], stage9):
        return 9, "Stage 09 claim drafts are missing or stale."

    if not ignore_pending and _pending_claim_count(root) > 0:
        return 0, "Paused for claim review; approve/reject draft claims before Stage 10 identity merge."

    claim_decisions = p.claim_review_decisions
    author_claims = p.author_claims
    author_directives = p.author_directives
    identity_merge_decisions = p.identity_merge_decisions
    card_decisions = p.card_review_decisions
    identity_merge_proposals = p.identity_merge_proposals
    card_edit_requests = p.card_edit_requests
    card_architecture_proposals = p.card_architecture_proposals
    card_architecture_decisions = p.card_architecture_decisions
    identity_merge_inputs = [
        stage9[0],
        p.resolved_entities,
        claim_decisions,
        author_claims,
    ]
    if _missing(stage10) or _newer_than_outputs(identity_merge_inputs, stage10):
        return 10, "Stage 10 identity merge proposals are missing or stale."
    if not ignore_pending and _pending_identity_merge_count(root) > 0:
        return 0, "Paused for identity cluster review; approve/reject identity clusters before Stage 11 card synthesis."
    if not ignore_pending and _pending_card_architecture_count(root) > 0:
        return 0, "Paused for card architecture review; approve/reject card architecture proposals before Stage 11 card synthesis."
    if _unproposed_card_edit_request_count(root) > 0:
        return 11, "Pending Cardbase Agent requests need Stage 11 processing."
    if _missing(stage11):
        return 11, "Stage 11 card synthesis/canon merge artifacts are missing."
    if _newer_than_outputs(
        [
            stage9[0],
            p.resolved_entities,
            effective_snippets,
            claim_decisions,
            author_claims,
            author_directives,
            identity_merge_decisions,
            card_decisions,
            card_architecture_proposals,
            card_architecture_decisions,
            identity_merge_proposals,
        ],
        stage11,
    ):
        return 11, "Stage 11 card synthesis/canon merge artifacts are stale after review decisions."
    if not ignore_pending and _pending_card_count(root) > 0:
        return 0, "Paused for card review; approve/reject synthesized card drafts before Stage 12 export."

    if _missing(stage12) or _newer_than_outputs(
        [
            p.canonical_cards,
            p.meta_cards_draft,
            p.alias_map,
            effective_snippets,
            p.source_profiles,
            p.merge_log,
        ],
        stage12,
    ):
        return 12, "Stage 12 Notion export is missing or stale."

    return 0, "Artifacts are current through Stage 12; no pipeline stage needs to run."


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
    parser.add_argument("--ignore-pending", action="store_true", help="Ignore pending review items and force continuation.")
    parser.add_argument("--start-stage", type=int, default=1, help="Expert override: first stage to run, 1-12.")
    args = parser.parse_args()
    setup_logging(args.log_level)
    logger = get_logger(__name__)

    root = args.artifacts_root
    migrate_run_artifacts_to_numbered(root)
    p = ArtifactPaths(root)
    thematic_runtime_path = root / "learning" / "thematic_profile_runtime.json"
    if args.ignore_pending:
        bypass_path = p.review_gate_bypass
        existing_bypass = read_json(bypass_path) if bypass_path.exists() else {}
        if not isinstance(existing_bypass, dict):
            existing_bypass = {}
        existing_bypass.update(
            {
                "claim_review": True,
                "claim_review_bypassed_at_utc": datetime.now(timezone.utc).isoformat(),
                "reason": "User selected --ignore-pending / force past pending review gates.",
            }
        )
        write_json(bypass_path, existing_bypass)
    total_stages = STAGE_TOTAL
    start_stage = max(1, min(total_stages, int(args.start_stage or 1)))
    snippets_for_downstream = _effective_snippets_path(p)
    snippets_review_for_downstream = _effective_snippets_review_path(p)
    if args.resume:
        start_stage, resume_reason = determine_resume_start_stage(root, ignore_pending=args.ignore_pending)
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
            run_stage_01,
            args.docx,
            p.entity_seed,
            p.schema_descriptor,
            Path("config/pipeline_config.json"),
            thematic_runtime_path,
        )
        seed_payload = read_json(p.entity_seed)
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
            run_stage_02,
            args.conversations_root,
            p.normalized_messages,
            p.normalization_summary,
        )
        stage_02_summary = read_json(p.normalization_summary)
        logger.info(
            "Stage 02 summary: files=%d, normalized_messages=%d, rejected=%d",
            int(stage_02_summary.get("input_files", 0)),
            int(stage_02_summary.get("normalized_messages", 0)),
            int(stage_02_summary.get("rejected_before_cutoff_or_invalid", 0)),
        )
    else:
        logger.info("[2/%d] SKIP  Stage 02 Message Normalization (resume starts at Stage %02d)", total_stages, start_stage)

    if start_stage <= 3:
        _run_stage(
            logger,
            3,
            total_stages,
            "Stage 03 Timeline Merge",
            run_stage_03,
            p.normalized_messages,
            p.global_timeline,
            p.global_index,
        )
        stage_03_index = read_json(p.global_index)
        logger.info(
            "Stage 03 summary: global_messages=%d, threads=%d",
            int(stage_03_index.get("message_count", 0)),
            len(stage_03_index.get("thread_counts", {})),
        )
    else:
        logger.info("[3/%d] SKIP  Stage 03 Timeline Merge (resume starts at Stage %02d)", total_stages, start_stage)

    if start_stage <= 4:
        _run_stage(
            logger,
            4,
            total_stages,
            "Stage 04 Relevant Conversation Segmentation",
            run_stage_04,
            p.global_timeline,
            p.relevant_messages,
            p.conversation_segments,
            p.conversation_index,
            p.conversation_segmentation_failures,
            Path("config/pipeline_config.json"),
            p.entity_seed,
        )
        stage_04_index = read_json(p.conversation_index)
        logger.info(
            "Stage 04 summary: relevant_segments=%d, relevant_messages=%d, dropped=%d, failures=%d",
            int(stage_04_index.get("relevant_segments", 0)),
            int(stage_04_index.get("messages_out", 0)),
            int(stage_04_index.get("dropped_prefilter_windows", 0)),
            int(stage_04_index.get("failed_model_windows", 0)),
        )
    else:
        logger.info("[4/%d] SKIP  Stage 04 Relevant Conversation Segmentation (resume starts at Stage %02d)", total_stages, start_stage)

    if start_stage <= 5:
        _run_stage(
            logger,
            5,
            total_stages,
            "Stage 05 Conversation Patch Notes",
            run_stage_05,
            p.relevant_messages,
            p.conversation_segments,
            p.conversation_patch_notes,
            p.conversation_patch_notes_jsonl,
            p.conversation_patch_note_failures,
            Path("config/pipeline_config.json"),
        )
        stage_05_index = read_json(p.conversation_patch_notes)
        logger.info(
            "Stage 05 summary: patch_notes=%d, conversations=%d, failures=%d",
            int(stage_05_index.get("notes_count", 0)),
            int(stage_05_index.get("conversation_count", 0)),
            int(stage_05_index.get("failure_count", 0)),
        )
    else:
        logger.info("[5/%d] SKIP  Stage 05 Conversation Patch Notes (resume starts at Stage %02d)", total_stages, start_stage)

    if start_stage <= 6:
        _run_stage(
            logger,
            6,
            total_stages,
            "Stage 06 Snippet Extraction",
            run_stage_06,
            p.relevant_messages,
            p.source_profiles,
            p.snippets,
            p.snippets_needs_review,
            p.source_profiles,
            Path("config/pipeline_config.json"),
            p.entity_seed,
            thematic_runtime_path,
            p.conversation_patch_notes,
        )
        logger.info(
            "Stage 06 summary: snippets=%d, needs_review=%d, profiles=%d",
            _count_jsonl(p.snippets),
            _count_jsonl(p.snippets_needs_review),
            len(read_json(p.source_profiles).get("profiles", [])),
        )
    else:
        logger.info("[6/%d] SKIP  Stage 06 Snippet Extraction (resume starts at Stage %02d)", total_stages, start_stage)

    if start_stage <= 7:
        _run_stage(
            logger,
            7,
            total_stages,
            "Stage 07A Entity Candidate Harvest",
            run_stage_07,
            p.snippets,
            p.entity_seed,
            p.alias_map,
            p.entity_timelines,
            p.resolved_entities,
            Path("canon/review_memory.json"),
            p.entity_candidate_harvest,
            Path("config/pipeline_config.json"),
        )
        logger.info(
            "Stage 07A summary: resolved_entities=%d seed_only_entities=%d entity_candidates=%d aliases=%d entity_timelines=%d",
            len(read_json(p.resolved_entities).get("resolved_entities", [])),
            len(read_json(p.resolved_entities).get("seed_only_entities", [])),
            len(read_json(p.entity_candidate_harvest).get("candidates", [])),
            len(read_json(p.alias_map).get("aliases", [])),
            len(read_json(p.entity_timelines).get("entity_timelines", {})),
        )
        _run_stage(
            logger,
            7,
            total_stages,
            "Stage 07B Entity Adjudication",
            run_stage_07b,
            p.entity_candidate_harvest,
            p.entity_adjudication_recommendations,
            p.externality_cache,
            Path("config/pipeline_config.json"),
            Path("canon/theme_profile.json"),
        )
        stage_07b = read_json(p.entity_adjudication_recommendations)
        logger.info(
            "Stage 07B summary: recommendations=%d web_selected=%d web_calls=%d cache_hits=%d failures=%d",
            len(stage_07b.get("recommendations", [])),
            int(stage_07b.get("summary", {}).get("web_selected_candidate_count", 0)),
            int(stage_07b.get("summary", {}).get("web_call_count", 0)),
            int(stage_07b.get("summary", {}).get("cache_hit_count", 0)),
            int(stage_07b.get("summary", {}).get("failure_count", 0)),
        )
        _run_stage(
            logger,
            7,
            total_stages,
            "Stage 07C Theme Miner",
            run_stage_07c,
            p.entity_candidate_harvest,
            p.entity_adjudication_recommendations,
            p.resolved_entities,
            Path("canon/review_memory.json"),
            Path("canon/theme_profile.json"),
            p.theme_profile_update_report,
            Path("config/pipeline_config.json"),
        )
        stage_07c = read_json(p.theme_profile_update_report)
        logger.info(
            "Stage 07C summary: themes=%d evidence_packets=%d applied_updates=%d failures=%d",
            int(stage_07c.get("summary", {}).get("theme_count", 0)),
            int(stage_07c.get("inputs", {}).get("evidence_packet_count", 0)),
            int(stage_07c.get("summary", {}).get("applied_update_count", 0)),
            int(stage_07c.get("summary", {}).get("failure_count", 0)),
        )
        _run_stage(
            logger,
            7,
            total_stages,
            "Stage 07D Theme-Aware Candidate Reclassification",
            run_stage_07d,
            p.entity_candidate_harvest,
            p.entity_adjudication_recommendations,
            Path("canon/theme_profile.json"),
            p.theme_candidate_reclassification,
            Path("config/pipeline_config.json"),
        )
        stage_07d = read_json(p.theme_candidate_reclassification)
        logger.info(
            "Stage 07D summary: reclassifications=%d theme_matched=%d",
            len(stage_07d.get("candidate_reclassifications", [])),
            int(stage_07d.get("summary", {}).get("theme_matched_candidate_count", 0)),
        )
        if _theme_rerun_enabled():
            _run_stage(
                logger,
                7,
                total_stages,
                "Stage 04R Theme-Aware Relevance Rerun",
                run_stage_04r,
                p.global_timeline,
                p.conversation_segments,
                p.resolved_entities,
                Path("canon/theme_profile.json"),
                p.externality_cache,
                p.theme_relevance_rerun,
                p.theme_rescue_messages,
                p.theme_rescue_segments,
                p.theme_relevance_rerun_failures,
                Path("config/pipeline_config.json"),
            )
            stage_04r = read_json(p.theme_relevance_rerun)
            logger.info(
                "Stage 04R summary: candidates=%d rescued=%d rescued_messages=%d",
                int(stage_04r.get("summary", {}).get("candidate_window_count", 0)),
                int(stage_04r.get("summary", {}).get("rescued_conversation_count", 0)),
                int(stage_04r.get("summary", {}).get("rescued_message_count", 0)),
            )
            _run_stage(
                logger,
                7,
                total_stages,
                "Stage 06R Theme Rescue Snippet Extraction",
                run_stage_06r,
                p.theme_rescue_messages,
                p.source_profiles,
                p.snippets,
                p.snippets_needs_review,
                p.theme_rescue_snippets,
                p.theme_rescue_snippets_needs_review,
                p.theme_rescue_source_profiles,
                p.snippets_with_theme_rescue,
                p.snippets_needs_review_with_theme_rescue,
                p.theme_rescue_snippet_merge_report,
                Path("config/pipeline_config.json"),
                p.entity_seed,
                thematic_runtime_path,
            )
            merge_report = read_json(p.theme_rescue_snippet_merge_report)
            logger.info(
                "Stage 06R summary: rescue_snippets=%d combined_snippets=%d",
                int(merge_report.get("summary", {}).get("rescue_snippet_count", 0)),
                int(merge_report.get("summary", {}).get("combined_snippet_count", 0)),
            )
            snippets_for_downstream = _effective_snippets_path(p)
            snippets_review_for_downstream = _effective_snippets_review_path(p)
    else:
        logger.info("[7/%d] SKIP  Stage 07A-07D Entity Candidate Harvest + Adjudication + Themes (resume starts at Stage %02d)", total_stages, start_stage)
        snippets_for_downstream = _effective_snippets_path(p)
        snippets_review_for_downstream = _effective_snippets_review_path(p)

    if start_stage <= 8:
        _run_stage(
            logger,
            8,
            total_stages,
            "Stage 08 Snippet Grouping",
            run_stage_08,
            snippets_for_downstream,
            p.resolved_entities,
            p.snippet_clusters_lore,
            p.snippet_clusters_meta,
            Path("config/pipeline_config.json"),
            thematic_runtime_path,
        )
        logger.info(
            "Stage 08 summary: lore_clusters=%d, meta_clusters=%d",
            len(read_json(p.snippet_clusters_lore).get("clusters", [])),
            len(read_json(p.snippet_clusters_meta).get("clusters", [])),
        )
    else:
        logger.info("[8/%d] SKIP  Stage 08 Snippet Grouping (resume starts at Stage %02d)", total_stages, start_stage)

    if start_stage <= 9:
        _run_stage(
            logger,
            9,
            total_stages,
            "Stage 09 Claim Drafting",
            run_stage_09,
            p.resolved_entities,
            p.snippet_clusters_lore,
            p.snippet_clusters_meta,
            p.alias_map,
            snippets_for_downstream,
            p.claim_drafting_dir,
            Path("config/pipeline_config.json"),
            Path("canon/review_memory.json"),
        )
        logger.info(
            "Stage 09 summary: claim_drafts=%d, meta_cards=%d",
            len(read_json(p.claim_drafts).get("claims", [])),
            len(read_json(p.meta_cards_draft).get("meta_cards", [])),
        )
    else:
        logger.info("[9/%d] SKIP  Stage 09 Claim Drafting (resume starts at Stage %02d)", total_stages, start_stage)

    pending_claims = _pending_claim_count(root)
    if pending_claims and not args.ignore_pending:
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
            "Stage 10 Identity Merge",
            run_stage_10,
            p.resolved_entities,
            p.claim_drafts,
            p.claim_review_decisions,
            Path("canon/review_memory.json"),
            p.identity_merge_proposals,
            p.identity_merge_decisions,
            Path("config/pipeline_config.json"),
        )
        logger.info(
            "Stage 10 summary: identity_merge_proposals=%d pending_identity_merges=%d",
            len(read_json(p.identity_merge_proposals).get("proposals", [])),
            _pending_identity_merge_count(root),
        )
    else:
        logger.info("[10/%d] SKIP  Stage 10 Identity Merge (resume starts at Stage %02d)", total_stages, start_stage)

    pending_identity_merges = _pending_identity_merge_count(root)
    if pending_identity_merges and not args.ignore_pending:
        _pause_for_review(
            logger,
            10,
            total_stages,
            "Stage 10 Identity Merge",
            f"Stage 10 produced {pending_identity_merges} identity cluster proposal(s) requiring review before Stage 11.",
        )

    if start_stage <= 11:
        _run_stage(
            logger,
            11,
            total_stages,
            "Stage 11 Card Synthesis",
            run_stage_11,
            p.resolved_entities,
            p.claim_drafts,
            p.claim_review_decisions,
            p.card_review_decisions,
            p.author_directives,
            Path("canon/review_memory.json"),
            p.card_drafts,
            p.canonical_cards,
            p.merge_log,
            Path("config/pipeline_config.json"),
            snippets_for_downstream,
        )
        logger.info(
            "Stage 11 summary: card_drafts=%d canonical_cards=%d merge_log=%d",
            len(read_json(p.card_drafts).get("cards", [])),
            len(read_json(p.canonical_cards).get("cards", [])),
            _count_jsonl(p.merge_log),
        )
        draft_sync_report = sync_draft_cards_to_notion(
            root,
            Path("config/pipeline_config.json"),
            Path(".env"),
            progress_callback=lambda message: logger.info(message),
        )
        logger.info(
            "Stage 11 Notion draft sync: status=%s created=%d updated=%d failed=%d report=%s reason=%s",
            draft_sync_report.get("status", "unknown"),
            int(draft_sync_report.get("created_pages", 0) or 0),
            int(draft_sync_report.get("updated_pages", 0) or 0),
            len(draft_sync_report.get("failed_pages", []) or []),
            p.notion_draft_sync_report,
            draft_sync_report.get("reason", ""),
        )
    else:
        logger.info("[11/%d] SKIP  Stage 11 Card Synthesis (resume starts at Stage %02d)", total_stages, start_stage)

    pending_cards = _pending_card_count(root)
    if pending_cards and not args.ignore_pending:
        _pause_for_review(
            logger,
            11,
            total_stages,
            "Stage 11 Card Synthesis",
            f"Stage 11 produced {pending_cards} card draft(s) requiring card review before Stage 12 export.",
        )

    if start_stage <= 12:
        _run_stage(
            logger,
            12,
            total_stages,
            "Stage 12 Notion Export",
            run_stage_12,
            p.canonical_cards,
            p.meta_cards_draft,
            p.alias_map,
            snippets_for_downstream,
            p.source_profiles,
            p.merge_log,
            p.notion_import,
        )
        logger.info(
            "Stage 12 summary: notion_records=%d",
            _count_jsonl(p.notion_import),
        )
    else:
        logger.info("[12/%d] SKIP  Stage 12 Notion Export (resume starts at Stage %02d)", total_stages, start_stage)
    logger.info("Pipeline complete. Notion export written under: %s", p.stage12)


if __name__ == "__main__":
    main()
