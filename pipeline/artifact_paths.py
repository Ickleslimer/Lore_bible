from __future__ import annotations

import shutil
from dataclasses import dataclass
from pathlib import Path


STAGE_DIRS: dict[int, str] = {
    1: "01_entity_bootstrap",
    2: "02_message_normalization",
    3: "03_timeline_merge",
    4: "04_conversation_segmentation",
    5: "05_snippet_extraction",
    6: "06_entity_resolution",
    7: "07_lore_development_ledger",
    8: "08_snippet_grouping",
    9: "09_claim_drafting",
    10: "10_identity_merge",
    11: "11_card_synthesis",
    12: "12_notion_export",
}


@dataclass(frozen=True)
class ArtifactPaths:
    root: Path

    @property
    def stage01(self) -> Path:
        return self.root / STAGE_DIRS[1]

    @property
    def stage02(self) -> Path:
        return self.root / STAGE_DIRS[2]

    @property
    def stage03(self) -> Path:
        return self.root / STAGE_DIRS[3]

    @property
    def stage04(self) -> Path:
        return self.root / STAGE_DIRS[4]

    @property
    def stage05(self) -> Path:
        return self.root / STAGE_DIRS[5]

    @property
    def stage06(self) -> Path:
        return self.root / STAGE_DIRS[6]

    @property
    def stage07(self) -> Path:
        return self.root / STAGE_DIRS[7]

    @property
    def stage08(self) -> Path:
        return self.root / STAGE_DIRS[8]

    @property
    def stage09(self) -> Path:
        return self.root / STAGE_DIRS[9]

    @property
    def stage10(self) -> Path:
        return self.root / STAGE_DIRS[10]

    @property
    def stage11(self) -> Path:
        return self.root / STAGE_DIRS[11]

    @property
    def stage12(self) -> Path:
        return self.root / STAGE_DIRS[12]

    @property
    def entity_seed(self) -> Path:
        return self.stage01 / "entity_seed.json"

    @property
    def schema_descriptor(self) -> Path:
        return self.stage01 / "schema_descriptor.json"

    @property
    def normalized_messages(self) -> Path:
        return self.stage02 / "messages_normalized_per_thread.jsonl"

    @property
    def normalization_summary(self) -> Path:
        return self.stage02 / "summary.json"

    @property
    def global_timeline(self) -> Path:
        return self.stage03 / "messages_global_timeline.jsonl"

    @property
    def global_index(self) -> Path:
        return self.stage03 / "global_index.json"

    @property
    def relevant_messages(self) -> Path:
        return self.stage04 / "messages_relevant_conversations.jsonl"

    @property
    def conversation_segments(self) -> Path:
        return self.stage04 / "conversation_segments.json"

    @property
    def conversation_index(self) -> Path:
        return self.stage04 / "conversation_index.json"

    @property
    def conversation_segmentation_failures(self) -> Path:
        return self.stage04 / "conversation_segmentation_failures.json"

    @property
    def theme_relevance_rerun(self) -> Path:
        return self.stage04 / "theme_relevance_rerun.json"

    @property
    def theme_rescue_messages(self) -> Path:
        return self.stage04 / "messages_theme_rescued_conversations.jsonl"

    @property
    def theme_rescue_segments(self) -> Path:
        return self.stage04 / "theme_rescue_segments.json"

    @property
    def theme_relevance_rerun_failures(self) -> Path:
        return self.stage04 / "theme_relevance_rerun_failures.json"

    @property
    def lore_development_ledger_index(self) -> Path:
        return self.stage07 / "lore_development_ledger_index.json"

    @property
    def lore_development_ledger_jsonl(self) -> Path:
        return self.stage07 / "lore_development_ledger.jsonl"

    @property
    def entity_development_history(self) -> Path:
        return self.stage07 / "entity_development_history.json"

    @property
    def lore_development_ledger_failures(self) -> Path:
        return self.stage07 / "lore_development_ledger_failures.json"

    @property
    def ledger_review_queue(self) -> Path:
        return self.stage07 / "ledger_review_queue.jsonl"

    @property
    def ledger_quality_gate_report(self) -> Path:
        return self.stage07 / "quality_gate_report.json"

    @property
    def snippets(self) -> Path:
        return self.stage05 / "snippets_candidates.jsonl"

    @property
    def snippets_needs_review(self) -> Path:
        return self.stage05 / "snippets_needs_review.jsonl"

    @property
    def theme_rescue_snippets(self) -> Path:
        return self.stage05 / "snippets_theme_rescue.jsonl"

    @property
    def theme_rescue_snippets_needs_review(self) -> Path:
        return self.stage05 / "snippets_theme_rescue_needs_review.jsonl"

    @property
    def theme_rescue_source_profiles(self) -> Path:
        return self.stage05 / "dm_source_profiles_theme_rescue.json"

    @property
    def snippets_with_theme_rescue(self) -> Path:
        return self.stage05 / "snippets_candidates_with_theme_rescue.jsonl"

    def effective_snippets(self) -> Path:
        """Prefer theme-rescue merged snippets when Stage 06R output exists."""
        if self.snippets_with_theme_rescue.exists():
            return self.snippets_with_theme_rescue
        return self.snippets

    @property
    def snippets_needs_review_with_theme_rescue(self) -> Path:
        return self.stage05 / "snippets_needs_review_with_theme_rescue.jsonl"

    def effective_snippets_needs_review(self) -> Path:
        if self.snippets_needs_review_with_theme_rescue.exists():
            return self.snippets_needs_review_with_theme_rescue
        return self.snippets_needs_review

    @property
    def theme_rescue_snippet_merge_report(self) -> Path:
        return self.stage05 / "theme_rescue_snippet_merge_report.json"

    @property
    def source_profiles(self) -> Path:
        return self.stage05 / "dm_source_profiles.json"

    @property
    def alias_map(self) -> Path:
        return self.stage06 / "alias_map.json"

    @property
    def entity_timelines(self) -> Path:
        return self.stage06 / "entity_timelines.json"

    @property
    def resolved_entities(self) -> Path:
        return self.stage06 / "resolved_entities.json"

    @property
    def entity_candidate_harvest(self) -> Path:
        return self.stage06 / "entity_candidate_harvest.json"

    @property
    def entity_inventory_browser_cache(self) -> Path:
        return self.stage06 / "entity_inventory_browser_cache.json"

    @property
    def entity_adjudication_recommendations(self) -> Path:
        return self.stage06 / "entity_adjudication_recommendations.json"

    @property
    def externality_cache(self) -> Path:
        return self.stage06 / "externality_cache.json"

    @property
    def theme_profile_update_report(self) -> Path:
        return self.stage06 / "theme_profile_update_report.json"

    @property
    def theme_candidate_reclassification(self) -> Path:
        return self.stage06 / "theme_candidate_reclassification.json"

    @property
    def theme_lineage_web_report(self) -> Path:
        return self.stage06 / "theme_lineage_web_report.json"

    @property
    def theme_lineage_cache(self) -> Path:
        return self.stage06 / "theme_lineage_cache.json"

    @property
    def theme_rescue_approval(self) -> Path:
        return self.stage06 / "theme_rescue_approval.json"

    @property
    def theme_rescue_baseline(self) -> Path:
        return self.stage06 / "theme_rescue_baseline.json"

    @property
    def conversation_entity_proposals(self) -> Path:
        return self.stage06 / "conversation_entity_proposals.json"

    @property
    def conversation_entity_decisions(self) -> Path:
        return self.stage06 / "conversation_entity_decisions.json"

    @property
    def snippet_clusters_lore(self) -> Path:
        return self.stage08 / "snippet_clusters_lore.json"

    @property
    def snippet_clusters_meta(self) -> Path:
        return self.stage08 / "snippet_clusters_meta.json"

    @property
    def stage08w(self) -> Path:
        return self.root / "08_narrative_work_tagging"

    @property
    def narrative_work_tags(self) -> Path:
        return self.stage08w / "snippet_narrative_work_tags.jsonl"

    @property
    def narrative_work_tagging_summary(self) -> Path:
        return self.stage08w / "tagging_summary.json"

    @property
    def stage08q(self) -> Path:
        return self.root / "08_quest_tagging"

    @property
    def snippet_quest_tags(self) -> Path:
        return self.stage08q / "snippet_quest_tags.jsonl"

    @property
    def discovered_quests(self) -> Path:
        return self.stage08q / "discovered_quests.json"

    @property
    def quest_tagging_summary(self) -> Path:
        return self.stage08q / "tagging_summary.json"

    @property
    def artist_character_review_queue(self) -> Path:
        return self.stage08q / "artist_character_review_queue.jsonl"

    @property
    def stage11w(self) -> Path:
        return self.root / "11_work_synthesis"

    @property
    def work_cards(self) -> Path:
        return self.stage11w / "work_cards.json"

    @property
    def claim_drafting_dir(self) -> Path:
        return self.stage09

    @property
    def claim_drafts(self) -> Path:
        return self.stage09 / "claim_drafts.json"

    @property
    def meta_cards_draft(self) -> Path:
        return self.stage09 / "meta_cards_draft.json"

    @property
    def claim_review_decisions(self) -> Path:
        return self.stage09 / "claim_review_decisions.json"

    @property
    def claim_auto_review_attention(self) -> Path:
        return self.stage09 / "claim_auto_review_attention.json"

    @property
    def review_gate_bypass(self) -> Path:
        return self.stage09 / "review_gate_bypass.json"

    @property
    def author_claims(self) -> Path:
        return self.stage09 / "author_claims.json"

    @property
    def identity_merge_proposals(self) -> Path:
        return self.stage10 / "identity_merge_proposals.json"

    @property
    def identity_merge_decisions(self) -> Path:
        return self.stage10 / "identity_merge_decisions.json"

    @property
    def identity_merged_entities_preview(self) -> Path:
        return self.stage10 / "identity_merged_entities_preview.json"

    @property
    def card_edit_requests(self) -> Path:
        return self.stage11 / "card_edit_requests.jsonl"

    @property
    def card_agent_transactions(self) -> Path:
        return self.stage11 / "card_agent_transactions.jsonl"

    @property
    def card_agent_progress(self) -> Path:
        return self.stage11 / "card_agent_progress.jsonl"

    @property
    def card_architecture_proposals(self) -> Path:
        return self.stage11 / "card_architecture_proposals.json"

    @property
    def card_architecture_decisions(self) -> Path:
        return self.stage11 / "card_architecture_decisions.json"

    @property
    def card_drafts(self) -> Path:
        return self.stage11 / "card_drafts.json"

    @property
    def card_review_decisions(self) -> Path:
        return self.stage11 / "card_review_decisions.json"

    @property
    def author_directives(self) -> Path:
        return self.stage11 / "author_directives.json"

    @property
    def canonical_cards(self) -> Path:
        return self.stage11 / "canonical_cards.json"

    @property
    def merge_log(self) -> Path:
        return self.stage11 / "merge_log.jsonl"

    @property
    def notion_import(self) -> Path:
        return self.stage12 / "notion_import.ndjson"

    @property
    def notion_draft_sync_report(self) -> Path:
        return self.stage12 / "notion_draft_sync_report.json"

    @property
    def notion_canonical_sync_report(self) -> Path:
        return self.stage12 / "notion_canonical_sync_report.json"


def first_existing(*paths: Path) -> Path:
    for path in paths:
        if path.exists():
            return path
    return paths[0]


def _copy_file(src: Path, dst: Path, *, overwrite: bool) -> bool:
    if not src.exists() or not src.is_file():
        return False
    if dst.exists() and not overwrite:
        try:
            if src.stat().st_mtime <= dst.stat().st_mtime:
                return False
        except OSError:
            return False
    dst.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src, dst)
    return True


def _copy_tree(src: Path, dst: Path, *, overwrite: bool) -> int:
    if not src.exists():
        return 0
    copied = 0
    for path in src.rglob("*"):
        if not path.is_file():
            continue
        rel = path.relative_to(src)
        if _copy_file(path, dst / rel, overwrite=overwrite):
            copied += 1
    return copied


def migrate_run_artifacts_to_numbered(root: Path, *, overwrite: bool = False) -> dict[str, int]:
    """Copy legacy artifact folders into stage-numbered folders.

    This intentionally copies instead of moving. Existing runs can resume from
    the new structure without losing the historical layout that produced them.
    """

    root = Path(root)
    p = ArtifactPaths(root)
    copied_by_stage: dict[str, int] = {str(stage): 0 for stage in range(1, 13)}

    copied_by_stage["1"] += _copy_tree(root / "01_bootstrap", p.stage01, overwrite=overwrite)
    copied_by_stage["5"] += _copy_tree(root / "03_relevance", p.stage05, overwrite=overwrite)
    copied_by_stage["5"] += _copy_tree(root / "06_snippet_extraction", p.stage05, overwrite=overwrite)
    copied_by_stage["6"] += _copy_tree(root / "05_alias", p.stage06, overwrite=overwrite)
    copied_by_stage["6"] += _copy_tree(root / "07_entity_resolution", p.stage06, overwrite=overwrite)
    copied_by_stage["7"] += _copy_tree(root / "05_conversation_patch_notes", p.stage07, overwrite=overwrite)
    copied_by_stage["7"] += _copy_tree(root / "05_lore_development_ledger", p.stage07, overwrite=overwrite)
    copied_by_stage["8"] += _copy_tree(root / "04_grouping", p.stage08, overwrite=overwrite)
    copied_by_stage["9"] += _copy_tree(root / "06_drafts" / "card_drafts", p.stage09, overwrite=overwrite)
    copied_by_stage["12"] += _copy_tree(root / "08_notion", p.stage12, overwrite=overwrite)

    legacy_timeline = root / "02_timeline"
    timeline_map = {
        "summary.json": p.normalization_summary,
        "messages_normalized_per_thread.jsonl": p.normalized_messages,
        "messages_global_timeline.jsonl": p.global_timeline,
        "global_index.json": p.global_index,
        "messages_relevant_conversations.jsonl": p.relevant_messages,
        "conversation_segments.json": p.conversation_segments,
        "conversation_index.json": p.conversation_index,
        "conversation_segmentation_failures.json": p.conversation_segmentation_failures,
        "lore_development_ledger_index.json": p.lore_development_ledger_index,
        "lore_development_ledger.jsonl": p.lore_development_ledger_jsonl,
        "entity_development_history.json": p.entity_development_history,
        "lore_development_ledger_failures.json": p.lore_development_ledger_failures,
    }
    for name, dst in timeline_map.items():
        stage_key = str(int(dst.relative_to(root).parts[0].split("_", 1)[0]))
        if _copy_file(legacy_timeline / name, dst, overwrite=overwrite):
            copied_by_stage[stage_key] += 1

    legacy_review = root / "07_review"
    review_stage09_prefixes = (
        "claim_",
        "story_question",
        "author_claim",
        "review_gate_bypass",
    )
    review_stage10_prefixes = ("identity_merge",)
    review_stage11_prefixes = (
        "card_",
        "canonical_cards",
        "merge_log",
        "author_directives",
    )
    if legacy_review.exists():
        for path in legacy_review.rglob("*"):
            if not path.is_file():
                continue
            name = path.name
            rel = path.relative_to(legacy_review)
            if name.startswith(review_stage09_prefixes):
                target_stage = "9"
                dst = p.stage09 / rel
            elif name.startswith(review_stage10_prefixes):
                target_stage = "10"
                dst = p.stage10 / rel
            elif name.startswith(review_stage11_prefixes):
                target_stage = "11"
                dst = p.stage11 / rel
            else:
                target_stage = "11"
                dst = p.stage11 / rel
            if _copy_file(path, dst, overwrite=overwrite):
                copied_by_stage[target_stage] += 1

    return copied_by_stage
