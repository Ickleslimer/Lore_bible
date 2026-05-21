from __future__ import annotations

import json
import os
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from unittest.mock import patch

from pipeline.artifact_paths import ArtifactPaths
from pipeline.cardbase_agent import card_agent_activity_payload, card_agent_progress_payload, load_card_agent_transactions, run_card_agent_request, run_pending_card_agent_requests, undo_card_agent_transaction
from pipeline.common import read_jsonl, stable_id
from pipeline.auto_review import AutoReviewResult, _auto_review_claims, _auto_review_conversation_entities
from pipeline.entity_resolution import card_id_for_entity, normalized_name_key, resolve_entities
from pipeline.model_provider import (
    _call_gemini_chat,
    _call_openrouter_chat,
    _extract_inline_responses,
    _gemini_batch_state,
    _inline_response_payload,
    build_stage_01_prompt,
    model_call_kwargs,
)
from pipeline.review_memory import relevant_memory_for_entity
from pipeline.run_pipeline import determine_resume_start_stage
from pipeline.story_questions import (
    apply_story_answer,
    commit_story_answer_application,
    generate_all_questions,
    generate_next_question,
    pending_claims_for_story,
    propose_story_answer_application,
    skip_current_question,
    story_question_display,
)
from pipeline.stage_01_entity_bootstrap import infer_entities
from pipeline.stage_04_conversation_segmentation import normalize_model_segments, run as run_stage_04
from pipeline.stage_05_conversation_patch_notes import run as run_stage_05
from pipeline.stage_06_snippet_extraction import run as run_stage_06
from pipeline.stage_08_snippet_grouping import run as run_stage_08
from pipeline.stage_07a_entity_candidate_harvest import run as run_stage_07a
from pipeline.stage_07b_entity_adjudication import run as run_stage_07b
from pipeline.stage_07c_theme_miner import run as run_stage_07c
from pipeline.stage_07d_theme_reclassification import run as run_stage_07d
from pipeline.stage_07_entity_resolution import annotate_conversation_entity_proposals, infer_type_evidence_for_candidate, normalize_entity_type, run as run_stage_07
from pipeline.stage_09_claim_drafting import build_claim_extraction_prompt, run as run_stage_09
from pipeline.stage_10_identity_merge import run as run_stage_10
from pipeline.stage_11_card_synthesis import (
    _build_identity_cluster_proposals,
    apply_entity_merges_to_entities,
    build_card_synthesis_prompt,
    find_unsupported_acronym_expansions,
    find_verbatim_claim_reuse,
    remember_identity_merge_decisions,
    run as run_stage_11,
)
from pipeline.stage_12_notion_export import run as run_stage_12
from pipeline.notion_draft_sync import notion_draft_config, sync_draft_cards_to_notion
from pipeline.review_inventory import (
    append_author_claim,
    attach_log_paths_for_run,
    candidate_inventory_browser_rows,
    candidate_inventory_category,
    choose_initial_artifacts_root,
    claim_inventory_browser_rows,
    ctrl_backspace_delete_start,
    ctrl_delete_delete_end,
    load_project_env,
    sort_candidate_inventory_rows,
    write_claim_inventory_override_decision,
    write_candidate_inventory_override_decision,
)
from pipeline.ui_review_app import (
    app_state_path,
    build_app,
    discover_review_runs,
    is_pipeline_progress_log_line,
    load_last_open_artifacts_root,
    new_run_artifacts_root,
    pending_review_counts_for_root,
    pipeline_progress_artifact_snapshot,
    pipeline_progress_from_logs,
    render_run_selector_html,
    render_pipeline_progress_html,
    save_last_open_artifacts_root,
)
from pipeline.tauri_bridge import handle_request


def write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(json.dumps(row) for row in rows) + "\n", encoding="utf-8")


def qwen_harvest_response(keys: list[str], entity_type_by_key: dict[str, str] | None = None) -> dict[str, Any]:
    entity_type_by_key = entity_type_by_key or {}
    return {
        "candidates": [
            {
                "normalized_name_key": key,
                "candidate_name": key.title(),
                "proposed_entity_type": entity_type_by_key.get(key, "term"),
                "denotation_class": "mixed_or_uncertain",
                "recommended_track": "unknown",
                "local_lore_prior": 0.5,
                "external_reference_prior": 0.1,
                "confidence": 0.75,
                "canonical_name": None,
                "alias_of": None,
                "signal_flags": {},
                "reasoning_summary": f"Qwen local-evidence annotation for {key}.",
                "human_review_question": f"Should {key} be treated as THERIAC lore, meta, or something else?",
            }
            for key in keys
        ]
    }


def web_adjudication_response(
    key: str,
    *,
    candidate_name: str | None = None,
    externality_class: str = "external_fictional_ip",
    recommended_action: str = "demote_meta",
) -> dict[str, Any]:
    return {
        "recommendations": [
            {
                "candidate_name": candidate_name or key.title(),
                "normalized_key": key,
                "recommended_action": recommended_action,
                "recommended_track": "meta" if recommended_action == "demote_meta" else "mixed",
                "recommended_entity_type": "inspiration_reference",
                "canonical_name": None,
                "alias_of": None,
                "confidence": 0.96,
                "externality_class": externality_class,
                "local_lore_prior": 0.15,
                "external_reference_prior": 0.98,
                "theme_matches": [],
                "in_world_signals": [],
                "meta_signals": ["Strong external match"],
                "web_findings": [
                    {
                        "query": candidate_name or key.title(),
                        "finding": "Known external reference.",
                        "externality_weight": 0.98,
                    }
                ],
                "reasoning_summary": "Externality detected by web search; human review still decides canon.",
                "human_review_question": f"Is {candidate_name or key.title()} a THERIAC lore element or only an external reference?",
            }
        ]
    }


def theme_miner_response() -> dict[str, Any]:
    return {
        "theme_updates": [
            {
                "action": "create_theme",
                "theme_id": "theme_sumerian_mythology",
                "label": "Sumerian mythology",
                "theme_type": "mythological_lineage",
                "status": "active",
                "confidence": 0.87,
                "canon_relevance": "lore_pattern",
                "description": "Sumerian mythological names and motifs are used for AI systems or lab architecture.",
                "evidence_entities": ["Ninhursag", "Inanna"],
                "evidence_claim_ids": [],
                "evidence_snippet_ids": ["s_ninhursag"],
                "positive_indicators": ["Sumerian deity name", "Mesopotamian religious term", "Enki"],
                "negative_indicators": ["Used only as external mythology comparison"],
                "related_themes": [],
                "disambiguation_notes": ["Sumerian origin alone is not enough for canon promotion."],
                "pattern_notes": ["Treat future Sumerian names as plausible only when local context suggests in-world use."],
                "provenance_summary": "Local adjudication treats Sumerian deity names as plausible THERIAC lore candidates.",
            }
        ]
    }


def write_pipeline_artifacts_through_stage9(root: Path, claims: list[dict] | None = None) -> None:
    write_json(root / "01_bootstrap" / "entity_seed.json", {"entities": []})
    write_json(root / "02_timeline" / "summary.json", {})
    write_jsonl(root / "02_timeline" / "messages_normalized_per_thread.jsonl", [{"message_id": "m1"}])
    write_json(root / "02_timeline" / "global_index.json", {})
    write_jsonl(root / "02_timeline" / "messages_global_timeline.jsonl", [{"message_id": "m1"}])
    write_json(root / "02_timeline" / "conversation_segments.json", {"segments": []})
    write_json(root / "02_timeline" / "conversation_index.json", {})
    write_jsonl(root / "02_timeline" / "messages_relevant_conversations.jsonl", [{"message_id": "m1"}])
    write_json(root / "02_timeline" / "conversation_patch_notes.json", {"status": "complete"})
    write_jsonl(root / "03_relevance" / "snippets_candidates.jsonl", [{"snippet_id": "s1"}])
    write_json(root / "03_relevance" / "dm_source_profiles.json", {"profiles": []})
    write_json(root / "05_alias" / "resolved_entities.json", {"resolved_entities": []})
    write_json(root / "05_alias" / "alias_map.json", {"aliases": []})
    write_json(root / "05_alias" / "entity_timelines.json", {"entity_timelines": {}})
    write_json(root / "05_alias" / "entity_candidate_harvest.json", {"schema_version": 1, "candidates": []})
    write_json(root / "05_alias" / "entity_adjudication_recommendations.json", {"schema_version": 1, "recommendations": []})
    write_json(root / "05_alias" / "externality_cache.json", {"schema_version": 1, "entries": {}})
    write_json(root / "05_alias" / "theme_profile_update_report.json", {"schema_version": 1, "summary": {"theme_count": 0}})
    write_json(root / "05_alias" / "theme_candidate_reclassification.json", {"schema_version": 1, "candidate_reclassifications": []})
    write_json(root / "05_alias" / "conversation_entity_proposals.json", {"proposals": []})
    write_json(root / "04_grouping" / "snippet_clusters_lore.json", {"clusters": []})
    write_json(root / "04_grouping" / "snippet_clusters_meta.json", {"clusters": []})
    write_json(root / "06_drafts" / "card_drafts" / "claim_drafts.json", {"claims": claims or []})
    write_json(root / "06_drafts" / "card_drafts" / "meta_cards_draft.json", {"meta_cards": []})


def draft_card_payload(summary: str = "HECTR is a synthetic intelligence.") -> dict[str, Any]:
    return {
        "card_id": "entity_hectr",
        "canonical_name": "HECTR",
        "entity_type": "character",
        "status": "draft",
        "summary": summary,
        "details": {
            "accepted_claim_ids": ["claim_1"],
            "sections": {
                "background": "HECTR begins as an AI presence in the project record.",
                "role_in_story": "HECTR acts as a pressure point for questions about machine agency.",
                "relationships": "HECTR is linked to RUINR through later identity development.",
                "timeline": "Early references frame HECTR before later renaming work.",
                "open_questions": "The final limits of HECTR's autonomy remain unsettled.",
            },
            "wiki_links": [
                {"target_entity_name": "RUINR", "relation_type": "later identity", "section": "relationships"},
            ],
        },
        "relationships": [{"relation_type": "alias-development", "target_card_id": "entity_ruinr", "note": "Later identity."}],
        "source_evidence": [{"source_snippet_id": "snippet_1"}],
    }


class FakeNotionDraftClient:
    def __init__(self) -> None:
        self.database_id = "db_fake"
        self.created_databases: list[str] = []
        self.updated_databases: list[str] = []
        self.pages: dict[str, dict[str, Any]] = {}
        self.deleted_blocks: list[str] = []

    def _prop_text(self, properties: dict[str, Any], key: str) -> str:
        prop = properties.get(key, {})
        for value_key in ("rich_text", "title"):
            texts = prop.get(value_key, []) if isinstance(prop, dict) else []
            if texts:
                first = texts[0]
                return str(first.get("plain_text") or first.get("text", {}).get("content", ""))
        return ""

    def _with_block_ids(self, page_id: str, blocks: list[dict[str, Any]]) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        for index, block in enumerate(blocks, 1):
            out.append({**block, "id": f"{page_id}_block_{len(out) + index}"})
        return out

    def create_database(self, parent_page_id: str) -> dict[str, Any]:
        self.created_databases.append(parent_page_id)
        return {"id": self.database_id}

    def update_database_schema(self, database_id: str) -> None:
        self.updated_databases.append(database_id)

    def query_existing_page(self, database_id: str, card_id: str, run_id: str) -> dict[str, Any] | None:
        for page in self.pages.values():
            properties = page.get("properties", {})
            if self._prop_text(properties, "Card ID") == card_id and self._prop_text(properties, "Run ID") == run_id:
                return page
        return None

    def create_page(self, database_id: str, properties: dict[str, Any], children: list[dict[str, Any]]) -> dict[str, Any]:
        page_id = f"page_{len(self.pages) + 1}"
        page = {
            "id": page_id,
            "url": f"https://notion.example/{page_id}",
            "properties": properties,
            "children": self._with_block_ids(page_id, children),
        }
        self.pages[page_id] = page
        return page

    def update_page(self, page_id: str, properties: dict[str, Any]) -> dict[str, Any]:
        self.pages[page_id]["properties"] = properties
        return {"id": page_id, "url": self.pages[page_id]["url"]}

    def list_block_children(self, block_id: str) -> list[dict[str, Any]]:
        return list(self.pages[block_id]["children"])

    def delete_block(self, block_id: str) -> None:
        self.deleted_blocks.append(block_id)
        for page in self.pages.values():
            page["children"] = [child for child in page.get("children", []) if child.get("id") != block_id]

    def append_children(self, block_id: str, children: list[dict[str, Any]]) -> None:
        self.pages[block_id]["children"].extend(self._with_block_ids(block_id, children))


def notion_block_texts(blocks: list[dict[str, Any]]) -> list[str]:
    out: list[str] = []
    for block in blocks:
        block_type = block.get("type")
        rich_text = block.get(block_type, {}).get("rich_text", []) if isinstance(block_type, str) else []
        for text in rich_text:
            out.append(str(text.get("plain_text") or text.get("text", {}).get("content", "")))
    return out


def iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def msg_row(
    message_id: str,
    timestamp_utc: str,
    thread_id: str = "thread_a",
    author_id: str = "partner",
    author_name: str = "Partner",
    content: str = "HECTR lore discussion.",
    partner_id: str = "partner",
    partner_label: str = "Partner",
    is_bot: bool = False,
) -> dict:
    return {
        "message_id": message_id,
        "thread_id": thread_id,
        "partner_id": partner_id,
        "partner_label": partner_label,
        "channel_id": "channel",
        "timestamp_utc": timestamp_utc,
        "author_id": author_id,
        "author_name": author_name,
        "author_is_bot": is_bot,
        "is_bot_or_application": is_bot,
        "application_id": "app" if is_bot else "",
        "webhook_id": "",
        "content_raw": content,
        "content_normalized": content,
        "attachments_count": 0,
        "embeds_count": 0,
        "sensitivity_flags": [],
        "provenance": {"json_path": "x.json", "export_batch": "test", "parser_version": "test", "content_hash": message_id},
    }


def write_b3_config(root: Path) -> Path:
    path = root / "config.json"
    write_json(
        path,
        {
            "conversation_segmentation": {
                "max_gap_hours": 12,
                "self_user_id": "self",
                "segmentation_provider_retries": 0,
                "segmentation_validation_retries": 0,
                "segmentation_provider_retry_sleep_seconds": 0,
                "segmentation_validation_retry_sleep_seconds": 0,
            }
        },
    )
    return path


def run_b3_for_test(
    root: Path,
    rows: list[dict],
    model_payloads: list[dict],
    seed_entities: list[str] | None = None,
) -> tuple[list[dict], dict, dict]:
    write_jsonl(root / "messages.jsonl", rows)
    config_path = write_b3_config(root)
    seed_path = None
    if seed_entities is not None:
        seed_path = root / "entity_seed.json"
        write_json(
            seed_path,
            {
                "entities": [
                    {"canonical_name": name, "entity_type": "term", "aliases": [], "seed_status": "active"}
                    for name in seed_entities
                ]
            },
        )
    with patch("pipeline.stage_04_conversation_segmentation.call_model_chat", side_effect=model_payloads):
        run_stage_04(
            root / "messages.jsonl",
            root / "relevant.jsonl",
            root / "segments.json",
            root / "index.json",
            root / "failures.json",
            config_path,
            seed_path,
        )
    return (
        [json.loads(line) for line in (root / "relevant.jsonl").read_text(encoding="utf-8").splitlines() if line.strip()],
        json.loads((root / "segments.json").read_text(encoding="utf-8")),
        json.loads((root / "index.json").read_text(encoding="utf-8")),
    )


class PipelineV2Tests(unittest.TestCase):
    def test_stage_05_conversation_patch_notes_preserve_global_chronological_order(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            early = msg_row("m1", "2025-01-10T10:00:00Z", thread_id="thread_b", partner_id="partner_b", partner_label="Beta")
            early.update(
                {
                    "conversation_id": "conv_early",
                    "dm_pair_id": "pair_b",
                    "conversation_message_index": 0,
                    "content_normalized": "HECTR is introduced to Beta as the lab AI.",
                    "content_raw": "HECTR is introduced to Beta as the lab AI.",
                }
            )
            late = msg_row("m2", "2025-01-11T10:00:00Z", thread_id="thread_a", partner_id="partner_a", partner_label="Alpha")
            late.update(
                {
                    "conversation_id": "conv_late",
                    "dm_pair_id": "pair_a",
                    "conversation_message_index": 0,
                    "content_normalized": "HECTR is then explained to Alpha with the same lab AI role.",
                    "content_raw": "HECTR is then explained to Alpha with the same lab AI role.",
                }
            )
            write_jsonl(root / "messages.jsonl", [late, early])
            write_json(
                root / "segments.json",
                {
                    "segments": [
                        {
                            "conversation_id": "conv_late",
                            "dm_pair_id": "pair_a",
                            "partner_id": "partner_a",
                            "partner_label": "Alpha",
                            "track": "lore",
                            "topic_label": "HECTR role",
                            "topic_summary": "Later HECTR explanation.",
                            "topic_shift_reason": "topic",
                            "anchor_entities": ["HECTR"],
                            "message_ids": ["m2"],
                            "timestamp_start_utc": "2025-01-11T10:00:00Z",
                            "timestamp_end_utc": "2025-01-11T10:00:00Z",
                            "message_count": 1,
                            "model_confidence": 0.9,
                            "source_coarse_window_id": "coarse_late",
                        },
                        {
                            "conversation_id": "conv_early",
                            "dm_pair_id": "pair_b",
                            "partner_id": "partner_b",
                            "partner_label": "Beta",
                            "track": "lore",
                            "topic_label": "HECTR role",
                            "topic_summary": "Earlier HECTR explanation.",
                            "topic_shift_reason": "topic",
                            "anchor_entities": ["HECTR"],
                            "message_ids": ["m1"],
                            "timestamp_start_utc": "2025-01-10T10:00:00Z",
                            "timestamp_end_utc": "2025-01-10T10:00:00Z",
                            "message_count": 1,
                            "model_confidence": 0.9,
                            "source_coarse_window_id": "coarse_early",
                        },
                    ]
                },
            )
            write_json(
                root / "config.json",
                {
                    "conversation_patch_notes": {
                        "previous_context_notes": 6,
                        "retry_sleep_seconds": 0,
                        "provider_retry_sleep_seconds": 0,
                    }
                },
            )
            prompts: list[str] = []

            def fake_patch_note(prompt: str, **_kwargs: object) -> dict:
                prompts.append(prompt)
                segment_meta = prompt.split("Segment metadata:", 1)[1].split("Prior patch-note context", 1)[0]
                if '"conversation_id": "conv_early"' in segment_meta:
                    return {
                        "summary": "Early HECTR role communicated to Beta.",
                        "lore_developments": [
                            {
                                "development_type": "new",
                                "entity_names": ["HECTR"],
                                "description": "HECTR is framed as the lab AI.",
                                "supporting_message_ids": ["m1"],
                                "confidence": 0.9,
                            }
                        ],
                        "meta_developments": [],
                        "entity_updates": [],
                        "relationship_updates": [],
                        "timeline_updates": [],
                        "open_questions": [],
                        "possible_contradictions": [],
                        "reinforces_prior_patch_note_ids": [],
                        "confidence": 0.9,
                    }
                return {
                    "summary": "Later HECTR role communicated to Alpha.",
                    "lore_developments": [
                        {
                            "development_type": "reinforcement",
                            "entity_names": ["HECTR"],
                            "description": "The same HECTR role is repeated to another partner.",
                            "supporting_message_ids": ["m2"],
                            "confidence": 0.85,
                        }
                    ],
                    "meta_developments": [],
                    "entity_updates": [],
                    "relationship_updates": [],
                    "timeline_updates": [],
                    "open_questions": [],
                    "possible_contradictions": [],
                    "reinforces_prior_patch_note_ids": [stable_id("conversation_patch_note", "conv_early")],
                    "confidence": 0.85,
                }

            with patch("pipeline.stage_05_conversation_patch_notes.call_model_chat", side_effect=fake_patch_note):
                run_stage_05(
                    root / "messages.jsonl",
                    root / "segments.json",
                    root / "patch_notes.json",
                    root / "patch_notes.jsonl",
                    root / "patch_failures.json",
                    root / "config.json",
                )

            payload = json.loads((root / "patch_notes.json").read_text(encoding="utf-8"))
            notes = payload["notes"]
            self.assertEqual([note["conversation_id"] for note in notes], ["conv_early", "conv_late"])
            self.assertEqual([note["global_conversation_index"] for note in notes], [1, 2])
            self.assertIn("Early HECTR role communicated to Beta.", prompts[1])
            self.assertNotIn("Later HECTR role communicated to Alpha.", prompts[0])
            self.assertEqual(notes[1]["reinforces_prior_patch_note_ids"], [stable_id("conversation_patch_note", "conv_early")])

    def test_stage_05_resumes_existing_checkpoint_without_restarting(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            first = msg_row("m1", "2025-01-10T10:00:00Z", partner_label="Beta")
            first.update(
                {
                    "conversation_id": "conv_first",
                    "dm_pair_id": "pair_b",
                    "conversation_message_index": 0,
                    "content_normalized": "HECTR is introduced to Beta as the lab AI.",
                }
            )
            second = msg_row("m2", "2025-01-11T10:00:00Z", partner_label="Alpha")
            second.update(
                {
                    "conversation_id": "conv_second",
                    "dm_pair_id": "pair_a",
                    "conversation_message_index": 0,
                    "content_normalized": "HECTR's lab role is reinforced with Alpha.",
                }
            )
            write_jsonl(root / "messages.jsonl", [first, second])
            write_json(
                root / "segments.json",
                {
                    "segments": [
                        {
                            "conversation_id": "conv_first",
                            "dm_pair_id": "pair_b",
                            "partner_id": "partner_b",
                            "partner_label": "Beta",
                            "track": "lore",
                            "topic_label": "HECTR role",
                            "topic_summary": "First explanation.",
                            "topic_shift_reason": "topic",
                            "anchor_entities": ["HECTR"],
                            "message_ids": ["m1"],
                            "timestamp_start_utc": "2025-01-10T10:00:00Z",
                            "timestamp_end_utc": "2025-01-10T10:00:00Z",
                            "message_count": 1,
                            "model_confidence": 0.9,
                        },
                        {
                            "conversation_id": "conv_second",
                            "dm_pair_id": "pair_a",
                            "partner_id": "partner_a",
                            "partner_label": "Alpha",
                            "track": "lore",
                            "topic_label": "HECTR role",
                            "topic_summary": "Second explanation.",
                            "topic_shift_reason": "topic",
                            "anchor_entities": ["HECTR"],
                            "message_ids": ["m2"],
                            "timestamp_start_utc": "2025-01-11T10:00:00Z",
                            "timestamp_end_utc": "2025-01-11T10:00:00Z",
                            "message_count": 1,
                            "model_confidence": 0.9,
                        },
                    ]
                },
            )
            write_json(
                root / "config.json",
                {
                    "conversation_patch_notes": {
                        "previous_context_notes": 6,
                        "retry_sleep_seconds": 0,
                        "provider_retry_sleep_seconds": 0,
                    }
                },
            )
            existing_note = {
                "patch_note_id": stable_id("conversation_patch_note", "conv_first"),
                "conversation_id": "conv_first",
                "global_conversation_index": 1,
                "summary": "Existing first HECTR note.",
                "timestamp_start_utc": "2025-01-10T10:00:00Z",
                "lore_developments": [],
                "meta_developments": [],
                "open_questions": [],
            }
            write_json(
                root / "patch_notes.json",
                {
                    "status": "in_progress",
                    "notes": [existing_note],
                    "conversation_count": 2,
                    "notes_count": 1,
                    "failure_count": 0,
                },
            )
            write_jsonl(root / "patch_notes.jsonl", [existing_note])
            write_json(root / "patch_failures.json", {"status": "in_progress", "failures": []})
            prompts: list[str] = []

            def fake_patch_note(prompt: str, **_kwargs: object) -> dict:
                prompts.append(prompt)
                self.assertIn("Existing first HECTR note.", prompt)
                self.assertIn('"conversation_id": "conv_second"', prompt)
                self.assertNotIn('"conversation_id": "conv_first"', prompt.split("Segment metadata:", 1)[1].split("Prior patch-note context", 1)[0])
                return {
                    "summary": "Second HECTR note.",
                    "lore_developments": [],
                    "meta_developments": [],
                    "entity_updates": [],
                    "relationship_updates": [],
                    "timeline_updates": [],
                    "open_questions": [],
                    "possible_contradictions": [],
                    "reinforces_prior_patch_note_ids": [stable_id("conversation_patch_note", "conv_first")],
                    "confidence": 0.85,
                }

            with patch("pipeline.stage_05_conversation_patch_notes.call_model_chat", side_effect=fake_patch_note):
                run_stage_05(
                    root / "messages.jsonl",
                    root / "segments.json",
                    root / "patch_notes.json",
                    root / "patch_notes.jsonl",
                    root / "patch_failures.json",
                    root / "config.json",
                )

            payload = json.loads((root / "patch_notes.json").read_text(encoding="utf-8"))
            self.assertEqual(len(prompts), 1)
            self.assertEqual([note["conversation_id"] for note in payload["notes"]], ["conv_first", "conv_second"])
            self.assertEqual(payload["status"], "complete")

    def test_stage_05_demotes_tiny_indirect_reference_to_no_durable_development(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            first = msg_row("m1", "2025-01-10T10:00:00Z", content="https://youtu.be/example")
            first.update({"conversation_id": "conv_weak", "dm_pair_id": "pair_1", "conversation_message_index": 1})
            second = msg_row("m2", "2025-01-10T10:01:00Z", content="literally alternate reality florida")
            second.update({"conversation_id": "conv_weak", "dm_pair_id": "pair_1", "conversation_message_index": 2})
            write_jsonl(root / "messages.jsonl", [first, second])
            write_json(
                root / "segments.json",
                {
                    "segments": [
                        {
                            "conversation_id": "conv_weak",
                            "dm_pair_id": "pair_1",
                            "partner_id": "partner",
                            "partner_label": "Partner",
                            "track": "lore",
                            "topic_label": "Alternate Reality Florida",
                            "topic_summary": "A brief external reference.",
                            "topic_shift_reason": "topic",
                            "anchor_entities": ["Alternate Reality Florida"],
                            "message_ids": ["m1", "m2"],
                            "timestamp_start_utc": "2025-01-10T10:00:00Z",
                            "timestamp_end_utc": "2025-01-10T10:01:00Z",
                            "message_count": 2,
                            "model_confidence": 0.9,
                        }
                    ]
                },
            )
            write_json(root / "config.json", {"conversation_patch_notes": {"retry_sleep_seconds": 0, "provider_retry_sleep_seconds": 0}})

            with patch(
                "pipeline.stage_05_conversation_patch_notes.call_model_chat",
                return_value={
                    "status": "draft",
                    "summary": "The segment suggests Alternate Reality Florida as a possible location.",
                    "lore_developments": [
                        {
                            "development_type": "new",
                            "entity_names": ["Alternate Reality Florida"],
                            "description": "A possible location is introduced.",
                            "supporting_message_ids": ["m1", "m2"],
                            "confidence": 0.8,
                        }
                    ],
                    "meta_developments": [],
                    "entity_updates": [],
                    "relationship_updates": [],
                    "timeline_updates": [],
                    "open_questions": [],
                    "possible_contradictions": [],
                    "reinforces_prior_patch_note_ids": [],
                    "confidence": 0.8,
                },
            ):
                run_stage_05(
                    root / "messages.jsonl",
                    root / "segments.json",
                    root / "patch_notes.json",
                    root / "patch_notes.jsonl",
                    root / "patch_failures.json",
                    root / "config.json",
                )

            note = json.loads((root / "patch_notes.json").read_text(encoding="utf-8"))["notes"][0]
            self.assertEqual(note["status"], "no_durable_development")
            self.assertIn("No durable THERIAC development", note["summary"])
            self.assertEqual(note["lore_developments"], [])
            self.assertEqual(note["entity_updates"], [])

    def test_stage_06_attaches_conversation_patch_note_context(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            row = msg_row("m1", "2025-01-10T10:00:00Z", content="HECTR coordinates the lab.")
            row.update(
                {
                    "conversation_id": "conv_1",
                    "dm_pair_id": "pair_1",
                    "conversation_message_index": 0,
                    "conversation_topic_label": "HECTR role",
                    "conversation_topic_summary": "HECTR role discussion.",
                    "conversation_track": "lore",
                    "conversation_anchor_entities": ["HECTR"],
                    "conversation_model_confidence": 0.91,
                }
            )
            write_jsonl(root / "messages.jsonl", [row])
            write_json(root / "profiles.json", {"profiles": []})
            write_json(root / "seed.json", {"entities": [{"canonical_name": "HECTR", "entity_type": "character", "aliases": []}]})
            write_json(root / "config.json", {"stage_06_anchor_provider": "conversation_metadata"})
            write_json(
                root / "patch_notes.json",
                {
                    "notes": [
                        {
                            "patch_note_id": "patch_1",
                            "conversation_id": "conv_1",
                            "global_conversation_index": 7,
                            "status": "draft",
                            "summary": "HECTR's lab role is developed.",
                            "lore_developments": [{"description": "HECTR coordinates the lab."}],
                            "meta_developments": [],
                            "open_questions": [{"question": "How autonomous is HECTR?"}],
                            "possible_contradictions": [],
                        }
                    ]
                },
            )

            run_stage_06(
                root / "messages.jsonl",
                root / "profiles.json",
                root / "snippets.jsonl",
                root / "review.jsonl",
                root / "profiles_out.json",
                root / "config.json",
                root / "seed.json",
                None,
                root / "patch_notes.json",
            )

            snippets = [json.loads(line) for line in (root / "snippets.jsonl").read_text(encoding="utf-8").splitlines()]
            self.assertEqual(len(snippets), 2)
            by_type = {snippet["patch_item_type"]: snippet for snippet in snippets}
            snippet = by_type["lore_development"]
            self.assertEqual(snippet["conversation_global_index"], 7)
            self.assertEqual(snippet["conversation_patch_note_id"], "patch_1")
            self.assertEqual(snippet["conversation_patch_status"], "draft")
            self.assertEqual(snippet["conversation_patch_summary"], "HECTR's lab role is developed.")
            self.assertEqual(snippet["conversation_patch_lore_developments"], ["HECTR coordinates the lab."])
            self.assertEqual(snippet["conversation_patch_open_questions"], ["How autonomous is HECTR?"])
            self.assertEqual(snippet["source_kind"], "patch_note_lore_development")
            self.assertEqual(snippet["patch_item_type"], "lore_development")
            self.assertEqual(snippet["patch_item_text"], "HECTR coordinates the lab.")
            self.assertEqual(snippet["candidate_entities"], ["HECTR"])
            self.assertIn("Patch note item:", snippet["display_text_normalized"])
            self.assertEqual(by_type["open_question"]["patch_item_text"], "How autonomous is HECTR?")

    def test_stage_06_materializes_patch_note_items_as_evidence_snippets(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            first = msg_row("m1", "2025-01-10T10:00:00Z", content="HECTR coordinates the lab.")
            first.update({"conversation_id": "conv_1", "dm_pair_id": "pair_1", "conversation_message_index": 0})
            second = msg_row("m2", "2025-01-10T10:02:00Z", content="ACHILLES is later treated as the same system as RUINR.")
            second.update({"conversation_id": "conv_1", "dm_pair_id": "pair_1", "conversation_message_index": 1})
            write_jsonl(root / "messages.jsonl", [first, second])
            write_json(root / "profiles.json", {"profiles": []})
            write_json(root / "config.json", {"stage_06_anchor_provider": "conversation_metadata"})
            write_json(
                root / "patch_notes.json",
                {
                    "notes": [
                        {
                            "patch_note_id": "patch_1",
                            "conversation_id": "conv_1",
                            "global_conversation_index": 2,
                            "dm_pair_id": "pair_1",
                            "partner_id": "partner",
                            "partner_label": "Partner",
                            "track": "both",
                            "topic_label": "HECTR and RUINR identity",
                            "topic_summary": "Lab role plus identity relationship.",
                            "status": "draft",
                            "summary": "The conversation develops HECTR and the ACHILLES/RUINR identity.",
                            "message_ids": ["m1", "m2"],
                            "anchor_entities": ["HECTR", "RUINR"],
                            "confidence": 0.91,
                            "lore_developments": [
                                {
                                    "development_type": "new",
                                    "entity_names": ["HECTR"],
                                    "description": "HECTR coordinates the lab.",
                                    "supporting_message_ids": ["m1"],
                                    "confidence": 0.9,
                                }
                            ],
                            "meta_developments": [],
                            "entity_updates": [],
                            "relationship_updates": [
                                {
                                    "source_entity": "ACHILLES",
                                    "target_entity": "RUINR",
                                    "relationship_type": "rename",
                                    "description": "ACHILLES is treated as the same system as RUINR.",
                                    "supporting_message_ids": ["m2"],
                                    "confidence": 0.88,
                                }
                            ],
                            "timeline_updates": [],
                            "open_questions": [],
                            "possible_contradictions": [],
                        }
                    ]
                },
            )

            run_stage_06(
                root / "messages.jsonl",
                root / "profiles.json",
                root / "snippets.jsonl",
                root / "review.jsonl",
                root / "profiles_out.json",
                root / "config.json",
                None,
                None,
                root / "patch_notes.json",
            )

            snippets = [json.loads(line) for line in (root / "snippets.jsonl").read_text(encoding="utf-8").splitlines() if line.strip()]
            self.assertEqual([snippet["patch_item_type"] for snippet in snippets], ["lore_development", "relationship_update"])
            self.assertEqual(snippets[0]["message_ids"], ["m1"])
            self.assertEqual(snippets[1]["message_ids"], ["m2"])
            self.assertEqual(snippets[1]["candidate_entities"], ["ACHILLES", "RUINR"])
            self.assertEqual(snippets[1]["patch_relationship_type"], "rename")
            self.assertEqual(snippets[1]["knowledge_track"], "lore")
            self.assertEqual((root / "review.jsonl").read_text(encoding="utf-8"), "")

    def test_stage_06_skips_no_durable_patch_note_conversations(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            row = msg_row("m1", "2025-01-10T10:00:00Z", content="literally alternate reality florida")
            row.update(
                {
                    "conversation_id": "conv_weak",
                    "dm_pair_id": "pair_1",
                    "conversation_message_index": 1,
                    "conversation_topic_label": "Alternate Reality Florida",
                    "conversation_topic_summary": "A brief external reference.",
                    "conversation_track": "lore",
                    "conversation_anchor_entities": ["Alternate Reality Florida"],
                    "conversation_model_confidence": 0.91,
                }
            )
            write_jsonl(root / "messages.jsonl", [row])
            write_json(root / "profiles.json", {"profiles": []})
            write_json(root / "seed.json", {"entities": []})
            write_json(root / "config.json", {"stage_06_anchor_provider": "conversation_metadata"})
            write_json(
                root / "patch_notes.json",
                {
                    "notes": [
                        {
                            "patch_note_id": "patch_weak",
                            "conversation_id": "conv_weak",
                            "global_conversation_index": 3,
                            "status": "no_durable_development",
                            "summary": "No durable THERIAC development was established.",
                            "lore_developments": [],
                            "meta_developments": [],
                            "open_questions": [],
                            "possible_contradictions": [],
                        }
                    ]
                },
            )

            run_stage_06(
                root / "messages.jsonl",
                root / "profiles.json",
                root / "snippets.jsonl",
                root / "review.jsonl",
                root / "profiles_out.json",
                root / "config.json",
                root / "seed.json",
                None,
                root / "patch_notes.json",
            )

            self.assertEqual((root / "snippets.jsonl").read_text(encoding="utf-8"), "")
            self.assertEqual((root / "review.jsonl").read_text(encoding="utf-8"), "")

    def test_gemini_provider_uses_generate_content_json_mode(self) -> None:
        captured: dict[str, object] = {}

        class FakeResponse:
            def __enter__(self) -> "FakeResponse":
                return self

            def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
                return None

            def read(self) -> bytes:
                return json.dumps(
                    {
                        "candidates": [
                            {
                                "content": {
                                    "parts": [
                                        {
                                            "text": json.dumps(
                                                {"segments": [], "ok": True},
                                                separators=(",", ":"),
                                            )
                                        }
                                    ]
                                }
                            }
                        ]
                    }
                ).encode("utf-8")

        def fake_urlopen(req: object, timeout: int) -> FakeResponse:
            captured["url"] = getattr(req, "full_url")
            captured["body"] = json.loads(getattr(req, "data").decode("utf-8"))
            captured["timeout"] = timeout
            return FakeResponse()

        with patch("pipeline.model_provider.urllib.request.urlopen", side_effect=fake_urlopen):
            payload = _call_gemini_chat(
                "https://generativelanguage.googleapis.com/v1beta",
                "fake-key",
                "gemini-2.5-flash-lite",
                'Return {"segments":[]}',
                0.0,
                12,
            )

        self.assertEqual(payload, {"segments": [], "ok": True})
        self.assertIn("/models/gemini-2.5-flash-lite:generateContent", str(captured["url"]))
        self.assertEqual(captured["timeout"], 12)
        body = captured["body"]
        self.assertEqual(body["generationConfig"]["responseMimeType"], "application/json")

    def test_openrouter_provider_uses_chat_completions_json_mode(self) -> None:
        captured: dict[str, object] = {}

        class FakeResponse:
            def __enter__(self) -> "FakeResponse":
                return self

            def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
                return None

            def read(self) -> bytes:
                return json.dumps(
                    {
                        "choices": [
                            {
                                "message": {
                                    "content": json.dumps({"segments": [], "ok": True}, separators=(",", ":")),
                                }
                            }
                        ]
                    }
                ).encode("utf-8")

        def fake_urlopen(req: object, timeout: int) -> FakeResponse:
            captured["url"] = getattr(req, "full_url")
            captured["body"] = json.loads(getattr(req, "data").decode("utf-8"))
            captured["timeout"] = timeout
            return FakeResponse()

        with tempfile.TemporaryDirectory() as tmp:
            with patch("pipeline.model_provider.urllib.request.urlopen", side_effect=fake_urlopen):
                with patch("pipeline.model_provider._NEXT_MODEL_ATTEMPT_EPOCH_S", 0.0):
                    with patch("pipeline.model_provider._RATE_LIMITED_UNTIL_EPOCH_S", 0.0):
                        payload = _call_openrouter_chat(
                            "https://openrouter.ai/api/v1",
                            "fake-key",
                            "qwen/qwen3.5-flash-02-23",
                            'Return {"segments":[]}',
                            0.0,
                            12,
                            retries=0,
                            rate_state_path=Path(tmp) / "rate.json",
                            min_interval_seconds=0.0,
                            max_tokens=1234,
                            tools=[{"type": "openrouter:web_search", "parameters": {"max_results": 3}}],
                        )

        self.assertEqual(payload, {"segments": [], "ok": True})
        self.assertIn("/chat/completions", str(captured["url"]))
        self.assertEqual(captured["timeout"], 12)
        body = captured["body"]
        self.assertEqual(body["model"], "qwen/qwen3.5-flash-02-23")
        self.assertEqual(body["response_format"], {"type": "json_object"})
        self.assertEqual(body["max_tokens"], 1234)
        self.assertEqual(body["tools"], [{"type": "openrouter:web_search", "parameters": {"max_results": 3}}])

    def test_model_routing_selects_qwen_instruct_for_synthesis(self) -> None:
        config = {
            "model_provider": {
                "provider": "openrouter",
                "api_model": "qwen/qwen3.5-flash-02-23",
                "rate_state_path": "artifacts/learning/openrouter_qwen_flash_rate_runtime.json",
            },
            "model_routing": {
                "profiles": {
                    "cheap": {
                        "api_model": "qwen/qwen3.5-flash-02-23",
                        "rate_state_path": "artifacts/learning/openrouter_qwen_flash_rate_runtime.json",
                    },
                    "deep_reasoning": {
                        "provider": "openrouter",
                        "api_model": "qwen/qwen3-235b-a22b-2507",
                        "api_base_url": "https://openrouter.ai/api/v1",
                        "rate_state_path": "artifacts/learning/openrouter_qwen_instruct_rate_runtime.json",
                    },
                },
                "tasks": {
                    "stage_09_claim_drafting": {"profile": "cheap", "batch_enabled": True},
                    "stage_11_card_synthesis": {"profile": "deep_reasoning", "batch_enabled": False},
                },
            },
        }

        claim_kwargs = model_call_kwargs(config, "stage_09_claim_drafting")
        synthesis_kwargs = model_call_kwargs(config, "stage_11_card_synthesis")

        self.assertEqual(claim_kwargs["api_model"], "qwen/qwen3.5-flash-02-23")
        self.assertEqual(synthesis_kwargs["provider"], "openrouter")
        self.assertEqual(synthesis_kwargs["api_model"], "qwen/qwen3-235b-a22b-2507")
        self.assertIn("openrouter_qwen_instruct_rate_runtime", str(synthesis_kwargs["rate_state_path"]))

    def test_default_pipeline_config_routes_agentic_reasoning_to_gemini_31(self) -> None:
        config = json.loads((Path("config") / "pipeline_config.json").read_text(encoding="utf-8"))

        agent_kwargs = model_call_kwargs(config, "stage_11_card_architecture_agent")
        identity_kwargs = model_call_kwargs(config, "stage_10_identity_merge_cluster_judgement")
        harvest_kwargs = model_call_kwargs(config, "stage_07a_entity_candidate_harvest")
        adjudication_kwargs = model_call_kwargs(config, "stage_07b_entity_adjudication_web")
        theme_miner_kwargs = model_call_kwargs(config, "stage_07c_theme_miner")

        self.assertEqual(agent_kwargs["provider"], "openrouter")
        self.assertEqual(agent_kwargs["api_model"], "google/gemini-3.1-pro-preview")
        self.assertIn("openrouter_gemini_31_deep_reasoning", str(agent_kwargs["rate_state_path"]))
        self.assertEqual(identity_kwargs["api_model"], "google/gemini-3.1-pro-preview")
        self.assertEqual(harvest_kwargs["provider"], "openrouter")
        self.assertEqual(harvest_kwargs["api_model"], "qwen/qwen3-235b-a22b-2507")
        self.assertEqual(adjudication_kwargs["api_model"], "google/gemini-3.1-pro-preview")
        self.assertEqual(adjudication_kwargs["tools"][0]["type"], "openrouter:web_search")
        self.assertEqual(theme_miner_kwargs["api_model"], "google/gemini-3.1-pro-preview")
        self.assertEqual(config["story_questions"]["provider"], "openrouter")
        self.assertEqual(config["story_questions"]["model"], "google/gemini-3.1-pro-preview")

    def test_gemini_batch_response_parser_handles_nested_inline_responses(self) -> None:
        body = {
            "metadata": {
                "state": "BATCH_STATE_SUCCEEDED",
                "output": {
                    "inlinedResponses": {
                        "inlinedResponses": [
                            {
                                "response": {
                                    "candidates": [
                                        {
                                            "content": {
                                                "parts": [{"text": json.dumps({"segments": []})}]
                                            }
                                        }
                                    ]
                                },
                                "metadata": {"key": "window_1"},
                            }
                        ]
                    }
                },
            },
            "done": True,
        }

        self.assertEqual(_gemini_batch_state(body), "BATCH_STATE_SUCCEEDED")
        responses = _extract_inline_responses(body)
        self.assertEqual(len(responses), 1)
        payload, error = _inline_response_payload(responses[0], __import__("logging").getLogger("test"))
        self.assertEqual(payload, {"segments": []})
        self.assertEqual(error, "")

    def test_song_title_quest_domain_rule_is_in_bootstrap_prompt(self) -> None:
        prompt = build_stage_01_prompt("Exit Music (For A Film) is the destructive path conclusion.")

        self.assertIn("quest titles may be named after songs", prompt.lower())
        self.assertIn("classify it as quest", prompt.lower())

    def test_global_author_directives_apply_to_entity_memory(self) -> None:
        memory = {
            "accepted_claims": [],
            "rejected_claims": [],
            "approved_cards": [],
            "author_directives": [
                {
                    "directive_id": "directive_song_title_quest_names",
                    "scope": "global",
                    "target_entity_id": "",
                    "target_card_id": "",
                    "instruction_text": "Quest titles in THERIAC may be named after songs.",
                }
            ],
            "style_corrections": [],
        }

        entity_memory = relevant_memory_for_entity(memory, "entity_exit_music", "Exit Music (For A Film)")

        self.assertEqual(
            entity_memory["author_directives"][0]["directive_id"],
            "directive_song_title_quest_names",
        )

    def test_stage_04_splits_only_after_more_than_12_hour_gap(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            start = datetime(2026, 4, 1, 0, 0, tzinfo=timezone.utc)
            rows = [
                msg_row("m1", iso(start), author_id="self", author_name="Me"),
                msg_row("m2", iso(start + timedelta(hours=11, minutes=59))),
                msg_row("m3", iso(start + timedelta(hours=23, minutes=59))),
                msg_row("m4", iso(start + timedelta(hours=36, seconds=1))),
            ]
            relevant, segments_payload, index = run_b3_for_test(
                root,
                rows,
                [
                    {"segments": [{"start_message_id": "m1", "end_message_id": "m3", "track": "lore", "topic_label": "HECTR", "topic_summary": "HECTR lore.", "topic_shift_reason": "First coarse window.", "anchor_entities": ["HECTR"], "confidence": 0.9}]},
                    {"segments": [{"start_message_id": "m4", "end_message_id": "m4", "track": "lore", "topic_label": "HECTR follow-up", "topic_summary": "HECTR follow-up.", "topic_shift_reason": "Gap over 12 hours.", "anchor_entities": ["HECTR"], "confidence": 0.9}]},
                ],
            )

            self.assertEqual(index["coarse_windows"], 2)
            self.assertEqual(len(segments_payload["segments"]), 2)
            self.assertEqual(len(relevant), 4)

    def test_stage_04_uses_batch_mode_for_model_windows_when_enabled(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            rows = [
                msg_row("m1", "2026-04-01T00:00:00Z", author_id="self", author_name="Me", content="HECTR starts here."),
                msg_row("m2", "2026-04-01T00:01:00Z", content="HECTR lore continues."),
            ]
            write_jsonl(root / "messages.jsonl", rows)
            write_json(
                root / "config.json",
                {
                    "conversation_segmentation": {
                        "max_gap_hours": 12,
                        "self_user_id": "self",
                        "segmentation_provider_retries": 0,
                        "segmentation_validation_retries": 0,
                        "segmentation_provider_retry_sleep_seconds": 0,
                        "segmentation_validation_retry_sleep_seconds": 0,
                    },
                    "model_routing": {
                        "profiles": {"cheap": {"provider": "gemini", "api_model": "gemini-2.5-flash-lite"}},
                        "tasks": {"stage_04_conversation_segmentation": {"profile": "cheap", "batch_enabled": True, "batch_max_requests": 10}},
                    },
                },
            )

            def fake_batch(_config: dict, _task_name: str, requests: list[dict]) -> dict:
                self.assertEqual(len(requests), 1)
                key = requests[0]["key"]
                return {
                    key: {
                        "payload": {
                            "segments": [
                                {
                                    "start_message_id": "m1",
                                    "end_message_id": "m2",
                                    "track": "lore",
                                    "topic_label": "HECTR",
                                    "topic_summary": "HECTR lore.",
                                    "topic_shift_reason": "Single relevant topic.",
                                    "anchor_entities": ["HECTR"],
                                    "confidence": 0.9,
                                }
                            ]
                        },
                        "error": "",
                    }
                }

            with patch("pipeline.stage_04_conversation_segmentation.call_gemini_batch_json", side_effect=fake_batch) as batch:
                with patch("pipeline.stage_04_conversation_segmentation.call_model_chat", side_effect=AssertionError("sync model should not be used")):
                    run_stage_04(
                        root / "messages.jsonl",
                        root / "relevant.jsonl",
                        root / "segments.json",
                        root / "index.json",
                        root / "failures.json",
                        root / "config.json",
                        None,
                    )

            index = json.loads((root / "index.json").read_text(encoding="utf-8"))
            self.assertEqual(batch.call_count, 1)
            self.assertEqual(index["model_windows"], 1)
            self.assertEqual(index["relevant_segments"], 1)
            self.assertEqual(index["failed_model_windows"], 0)

    def test_stage_04_drops_irrelevant_model_returned_spans(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            rows = [msg_row("m1", "2026-04-01T00:00:00Z", content="HECTR aside in otherwise irrelevant chatter.")]
            relevant, segments_payload, index = run_b3_for_test(
                root,
                rows,
                [{"segments": [{"start_message_id": "m1", "end_message_id": "m1", "track": "irrelevant", "topic_label": "Nope", "topic_summary": "", "topic_shift_reason": "", "anchor_entities": [], "confidence": 0.2}]}],
            )

            self.assertEqual(relevant, [])
            self.assertEqual(segments_payload["segments"], [])
            self.assertEqual(index["failed_model_windows"], 0)

    def test_stage_04_generic_seed_tokens_do_not_create_direct_signal(self) -> None:
        rows = [
            msg_row(
                "m1",
                "2026-04-01T00:00:00Z",
                content="We talked about a working name, early development, and aesthetics for another project.",
            )
        ]
        relevance_events: list[dict] = []

        segments = normalize_model_segments(
            {
                "segments": [
                    {
                        "start_message_id": "m1",
                        "end_message_id": "m1",
                        "track": "meta",
                        "topic_label": "Project codename",
                        "topic_summary": "Working name and aesthetics discussion.",
                        "topic_shift_reason": "Production topic.",
                        "anchor_entities": ["development", "aesthetics"],
                        "relevance_type": "direct_project_meta",
                        "relevance_confidence": 0.9,
                        "confidence": 0.9,
                    }
                ]
            },
            rows,
            ["Seismic Weaponry Development", "Hellenic Aesthetics"],
            relevance_events,
            {"coarse_window_id": "w1", "model_window_id": "mw1"},
        )

        self.assertEqual(segments, [])
        self.assertEqual(relevance_events[0]["reason"], "missing_direct_theriac_signal")

    def test_stage_04_keeps_lore_and_meta_topic_shift_segments(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            rows = [
                msg_row("m1", "2026-04-01T00:00:00Z", author_id="self", author_name="Me", content="HECTR is central to the Krypteia."),
                msg_row("m2", "2026-04-01T00:02:00Z", content="HECTR hides RUINR from the plot."),
                msg_row("m3", "2026-04-01T00:04:00Z", author_id="self", author_name="Me", content="Theriac marketing campaign planning."),
                msg_row("m4", "2026-04-01T00:06:00Z", content="The production roadmap needs a devlog."),
            ]
            _relevant, segments_payload, _index = run_b3_for_test(
                root,
                rows,
                [
                    {
                        "segments": [
                            {"start_message_id": "m1", "end_message_id": "m2", "track": "lore", "topic_label": "HECTR plot role", "topic_summary": "HECTR lore.", "topic_shift_reason": "Entity focus.", "anchor_entities": ["HECTR", "RUINR"], "confidence": 0.91},
                            {"start_message_id": "m3", "end_message_id": "m4", "track": "meta", "topic_label": "Marketing roadmap", "topic_summary": "Marketing planning.", "topic_shift_reason": "Production concern.", "anchor_entities": ["Theriac"], "confidence": 0.88},
                        ]
                    }
                ],
            )

            tracks = [segment["track"] for segment in segments_payload["segments"]]
            self.assertEqual(tracks, ["lore", "meta"])
            self.assertEqual(len({segment["conversation_id"] for segment in segments_payload["segments"]}), 2)

    def test_stage_04_different_dm_pairs_never_merge(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            rows = [
                msg_row("a1", "2026-04-01T00:00:00Z", thread_id="thread_a", author_id="self", author_name="Me", partner_id="alice", partner_label="Alice", content="HECTR question for Alice."),
                msg_row("a2", "2026-04-01T00:01:00Z", thread_id="thread_a", author_id="alice", author_name="Alice", partner_id="alice", partner_label="Alice", content="HECTR answer from Alice."),
                msg_row("b1", "2026-04-01T00:00:30Z", thread_id="thread_b", author_id="self", author_name="Me", partner_id="bob", partner_label="Bob", content="HECTR question for Bob."),
                msg_row("b2", "2026-04-01T00:01:30Z", thread_id="thread_b", author_id="bob", author_name="Bob", partner_id="bob", partner_label="Bob", content="HECTR answer from Bob."),
            ]
            _relevant, segments_payload, _index = run_b3_for_test(
                root,
                rows,
                [
                    {"segments": [{"start_message_id": "a1", "end_message_id": "a2", "track": "lore", "topic_label": "Alice HECTR", "topic_summary": "Alice thread.", "topic_shift_reason": "One DM pair.", "anchor_entities": ["HECTR"], "confidence": 0.9}]},
                    {"segments": [{"start_message_id": "b1", "end_message_id": "b2", "track": "lore", "topic_label": "Bob HECTR", "topic_summary": "Bob thread.", "topic_shift_reason": "Different DM pair.", "anchor_entities": ["HECTR"], "confidence": 0.9}]},
                ],
            )

            self.assertEqual(len({segment["dm_pair_id"] for segment in segments_payload["segments"]}), 2)
            self.assertEqual({segment["partner_label"] for segment in segments_payload["segments"]}, {"Alice", "Bob"})

    def test_stage_04_bot_author_does_not_change_dm_pair(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            rows = [
                msg_row("m1", "2026-04-01T00:00:00Z", author_id="self", author_name="Me", content="HECTR starts here."),
                msg_row("m2", "2026-04-01T00:01:00Z", author_id="partner", author_name="Partner", content="HECTR continues."),
                msg_row("m3", "2026-04-01T00:02:00Z", author_id="bot", author_name="Bot", content="HECTR bot context.", is_bot=True),
            ]
            relevant, segments_payload, _index = run_b3_for_test(
                root,
                rows,
                [{"segments": [{"start_message_id": "m1", "end_message_id": "m3", "track": "lore", "topic_label": "HECTR with bot context", "topic_summary": "Bot is context only.", "topic_shift_reason": "Single discussion.", "anchor_entities": ["HECTR"], "confidence": 0.9}]}],
            )

            self.assertEqual(len(relevant), 3)
            self.assertNotIn("bot", set(segments_payload["segments"][0]["participant_ids"]))
            self.assertEqual(len({row["dm_pair_id"] for row in relevant}), 1)

    def test_stage_04_accepts_model_returned_message_indexes(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            rows = [
                msg_row("m1", "2026-04-01T00:00:00Z", author_id="self", author_name="Me", content="HECTR starts here."),
                msg_row("m2", "2026-04-01T00:01:00Z", content="RUINR appears in the same topic."),
                msg_row("m3", "2026-04-01T00:02:00Z", content="Unrelated scheduling aside."),
            ]
            relevant, segments_payload, _index = run_b3_for_test(
                root,
                rows,
                [
                    {
                        "segments": [
                            {
                                "start_message_index": 1,
                                "end_message_index": 2,
                                "track": "lore",
                                "topic_label": "HECTR and RUINR",
                                "topic_summary": "HECTR and RUINR lore.",
                                "topic_shift_reason": "Topic before the aside.",
                                "anchor_entities": ["HECTR", "RUINR"],
                                "confidence": 0.9,
                            }
                        ]
                    }
                ],
            )

            self.assertEqual([row["message_id"] for row in relevant], ["m1", "m2"])
            self.assertEqual(segments_payload["segments"][0]["message_ids"], ["m1", "m2"])

    def test_stage_04_accepts_numeric_message_id_fields_as_indexes(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            rows = [
                msg_row("m1", "2026-04-01T00:00:00Z", author_id="self", author_name="Me", content="HECTR starts here."),
                msg_row("m2", "2026-04-01T00:01:00Z", content="RUINR appears in the same topic."),
            ]
            relevant, segments_payload, _index = run_b3_for_test(
                root,
                rows,
                [
                    {
                        "segments": [
                            {
                                "start_message_id": "1",
                                "end_message_id": "2",
                                "track": "lore",
                                "topic_label": "HECTR and RUINR",
                                "topic_summary": "HECTR and RUINR lore.",
                                "topic_shift_reason": "Single model-indexed topic.",
                                "anchor_entities": ["HECTR", "RUINR"],
                                "confidence": 0.9,
                            }
                        ]
                    }
                ],
            )

            self.assertEqual([row["message_id"] for row in relevant], ["m1", "m2"])
            self.assertEqual(segments_payload["segments"][0]["message_ids"], ["m1", "m2"])

    def test_stage_04_reports_and_handles_overlapping_model_segments(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            rows = [
                msg_row("m1", "2026-04-01T00:00:00Z", author_id="self", author_name="Me", content="HECTR starts here."),
                msg_row("m2", "2026-04-01T00:01:00Z", content="RUINR appears in the same topic."),
                msg_row("m3", "2026-04-01T00:02:00Z", content="Krypteia plot escalation."),
                msg_row("m4", "2026-04-01T00:03:00Z", content="The next adjacent topic lands here."),
            ]
            relevant, segments_payload, index = run_b3_for_test(
                root,
                rows,
                [
                    {
                        "segments": [
                            {
                                "start_message_id": "m1",
                                "end_message_id": "m3",
                                "track": "lore",
                                "topic_label": "HECTR plot",
                                "topic_summary": "HECTR plot discussion.",
                                "topic_shift_reason": "Initial broad span.",
                                "anchor_entities": ["HECTR"],
                                "confidence": 0.9,
                            },
                            {
                                "start_message_id": "m1",
                                "end_message_id": "m3",
                                "track": "lore",
                                "topic_label": "HECTR duplicate",
                                "topic_summary": "Duplicate broad span.",
                                "topic_shift_reason": "Duplicate.",
                                "anchor_entities": ["HECTR"],
                                "confidence": 0.9,
                            },
                            {
                                "start_message_id": "m2",
                                "end_message_id": "m2",
                                "track": "lore",
                                "topic_label": "Nested RUINR",
                                "topic_summary": "Nested inside the broad span.",
                                "topic_shift_reason": "Nested.",
                                "anchor_entities": ["RUINR"],
                                "confidence": 0.8,
                            },
                            {
                                "start_message_id": "m3",
                                "end_message_id": "m4",
                                "track": "lore",
                                "topic_label": "Partial overlap",
                                "topic_summary": "Overlaps the broad span and continues.",
                                "topic_shift_reason": "Partial overlap.",
                                "anchor_entities": ["Krypteia"],
                                "confidence": 0.8,
                            },
                        ]
                    }
                ],
            )

            self.assertEqual([segment["message_ids"] for segment in segments_payload["segments"]], [["m1", "m2", "m3"], ["m4"]])
            self.assertEqual([row["message_id"] for row in relevant], ["m1", "m2", "m3", "m4"])
            self.assertEqual(index["overlapping_model_segments_total"], 3)
            self.assertEqual(index["overlapping_model_segments_dropped"], 2)
            self.assertEqual(index["overlapping_model_segments_trimmed"], 1)
            self.assertEqual(index["overlapping_model_segment_duplicates_dropped"], 1)
            self.assertEqual(index["overlapping_model_segment_nested_dropped"], 1)
            self.assertEqual(index["overlapping_model_segment_partial_prefix_trimmed"], 1)
            self.assertEqual(
                [event["overlap_kind"] for event in segments_payload["overlap_diagnostics"]],
                ["duplicate_span", "nested_span", "partial_prefix"],
            )
            self.assertEqual(segments_payload["overlap_diagnostics"][-1]["materialized_message_ids"], ["m4"])

    def test_stage_04_relevance_gate_drops_external_media_without_theriac_tie(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            rows = [
                msg_row("m1", "2026-04-01T00:00:00Z", author_id="self", author_name="Me", content="Evangelion rewatch thoughts."),
                msg_row("m2", "2026-04-01T00:01:00Z", content="The character psychology is interesting."),
            ]
            relevant, segments_payload, index = run_b3_for_test(
                root,
                rows,
                [
                    {
                        "segments": [
                            {
                                "start_message_id": "m1",
                                "end_message_id": "m2",
                                "track": "meta",
                                "topic_label": "Evangelion Discussion",
                                "topic_summary": "The conversation discusses Evangelion's themes and character psychology.",
                                "topic_shift_reason": "This segment focuses on external media with no direct connection to THERIAC.",
                                "anchor_entities": ["Evangelion"],
                                "relevance_type": "direct_inspiration",
                                "relevance_rationale": "No direct connection to THERIAC is stated.",
                                "relevance_confidence": 0.8,
                                "confidence": 0.9,
                            }
                        ]
                    }
                ],
                seed_entities=["HECTR", "OYUUN", "THERIAC"],
            )

            self.assertEqual(relevant, [])
            self.assertEqual(segments_payload["segments"], [])
            self.assertEqual(index["model_segments_dropped_by_relevance"], 1)
            self.assertEqual(
                index["model_segments_dropped_by_relevance_reasons"],
                {"negative_relevance_rationale": 1},
            )
            self.assertEqual(
                segments_payload["relevance_gate_diagnostics"][0]["topic_label"],
                "Evangelion Discussion",
            )

    def test_stage_04_relevance_gate_keeps_external_inspiration_with_seed_anchor(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            rows = [
                msg_row("m1", "2026-04-01T00:00:00Z", author_id="self", author_name="Me", content="Alice in Chains maps onto OYUUN."),
                msg_row("m2", "2026-04-01T00:01:00Z", content="That grief should inform OYUUN's tone."),
            ]
            relevant, segments_payload, index = run_b3_for_test(
                root,
                rows,
                [
                    {
                        "segments": [
                            {
                                "start_message_id": "m1",
                                "end_message_id": "m2",
                                "track": "meta",
                                "topic_label": "Alice in Chains and OYUUN",
                                "topic_summary": "The speakers explicitly connect Alice in Chains' themes to OYUUN's tone in THERIAC.",
                                "topic_shift_reason": "External media is applied to a THERIAC character.",
                                "anchor_entities": ["OYUUN"],
                                "relevance_type": "direct_inspiration",
                                "relevance_rationale": "OYUUN is a THERIAC entity and the music is being used as style inspiration for that character.",
                                "relevance_confidence": 0.94,
                                "confidence": 0.9,
                            }
                        ]
                    }
                ],
                seed_entities=["HECTR", "OYUUN", "THERIAC"],
            )

            self.assertEqual([row["message_id"] for row in relevant], ["m1", "m2"])
            self.assertEqual(segments_payload["segments"][0]["relevance_type"], "direct_inspiration")
            self.assertEqual(index["model_segments_dropped_by_relevance"], 0)

    def test_stage_04_relevance_gate_accepts_distinctive_seed_name_token(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            rows = [
                msg_row("m1", "2026-04-01T00:00:00Z", author_id="self", author_name="Me", content="Enoch needs a specific voice."),
                msg_row("m2", "2026-04-01T00:01:00Z", content="That casting idea fits Enoch."),
            ]
            relevant, segments_payload, index = run_b3_for_test(
                root,
                rows,
                [
                    {
                        "segments": [
                            {
                                "start_message_id": "m1",
                                "end_message_id": "m2",
                                "track": "meta",
                                "topic_label": "Enoch voice casting",
                                "topic_summary": "The speakers discuss voice casting for Enoch.",
                                "topic_shift_reason": "Production discussion for a THERIAC character.",
                                "anchor_entities": ["Enoch Faust Ersetzen"],
                                "relevance_type": "direct_project_meta",
                                "relevance_rationale": "Enoch Faust Ersetzen is a THERIAC seed entity.",
                                "relevance_confidence": 0.92,
                                "confidence": 0.9,
                            }
                        ]
                    }
                ],
                seed_entities=["Enoch Faust Ersetzen"],
            )

            self.assertEqual([row["message_id"] for row in relevant], ["m1", "m2"])
            self.assertEqual(segments_payload["segments"][0]["anchor_entities"], ["Enoch Faust Ersetzen"])
            self.assertEqual(index["model_segments_dropped_by_relevance"], 0)

    def test_stage_04_failure_records_model_window_count_and_payload_preview(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            rows = [
                msg_row("m1", "2026-04-01T00:00:00Z", author_id="self", author_name="Me", content="HECTR starts here."),
                msg_row("m2", "2026-04-01T00:01:00Z", content="RUINR appears in the same topic."),
                msg_row("m3", "2026-04-01T00:02:00Z", content="A second model chunk."),
            ]
            write_jsonl(root / "messages.jsonl", rows)
            write_json(
                root / "config.json",
                {
                    "conversation_segmentation": {
                        "max_gap_hours": 12,
                        "self_user_id": "self",
                        "model_window_max_messages": 2,
                        "segmentation_provider_retries": 0,
                        "segmentation_validation_retries": 0,
                        "segmentation_provider_retry_sleep_seconds": 0,
                        "segmentation_validation_retry_sleep_seconds": 0,
                    }
                },
            )
            invalid_payload = {
                "segments": [
                    {
                        "start_message_id": "outside",
                        "end_message_id": "m2",
                        "track": "lore",
                        "topic_label": "Bad reference",
                        "topic_summary": "Invalid test payload.",
                        "topic_shift_reason": "Validation failure.",
                        "anchor_entities": ["HECTR"],
                        "confidence": 0.9,
                    }
                ]
            }

            with patch("pipeline.stage_04_conversation_segmentation.call_model_chat", side_effect=[invalid_payload, {"segments": []}]):
                with self.assertRaises(RuntimeError):
                    run_stage_04(
                        root / "messages.jsonl",
                        root / "relevant.jsonl",
                        root / "segments.json",
                        root / "index.json",
                        root / "failures.json",
                        root / "config.json",
                        None,
                    )

            failures = json.loads((root / "failures.json").read_text(encoding="utf-8"))
            self.assertEqual(len(failures["failures"]), 1)
            failure = failures["failures"][0]
            self.assertEqual(failure["message_count"], 2)
            self.assertEqual(failure["model_window_message_count"], 2)
            self.assertEqual(failure["coarse_window_message_count"], 3)
            self.assertIn("outside", failure["error"])
            self.assertIn("payload_preview", failure["error"])

    def test_stage_04_checkpoints_completed_model_windows_before_interrupt(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            rows = [
                msg_row("m1", "2026-04-01T00:00:00Z", author_id="self", author_name="Me", content="HECTR starts here."),
                msg_row("m2", "2026-04-01T13:00:01Z", content="HECTR resumes after a gap."),
            ]
            write_jsonl(root / "messages.jsonl", rows)
            config_path = write_b3_config(root)
            first_payload = {
                "segments": [
                    {
                        "start_message_id": "m1",
                        "end_message_id": "m1",
                        "track": "lore",
                        "topic_label": "HECTR opening",
                        "topic_summary": "HECTR opening discussion.",
                        "topic_shift_reason": "First coarse window.",
                        "anchor_entities": ["HECTR"],
                        "confidence": 0.9,
                    }
                ]
            }

            with patch("pipeline.stage_04_conversation_segmentation.call_model_chat", side_effect=[first_payload, KeyboardInterrupt]):
                with self.assertRaises(KeyboardInterrupt):
                    run_stage_04(
                        root / "messages.jsonl",
                        root / "relevant.jsonl",
                        root / "segments.json",
                        root / "index.json",
                        root / "failures.json",
                        config_path,
                        None,
                    )

            segments_payload = json.loads((root / "segments.json").read_text(encoding="utf-8"))
            relevant = [json.loads(line) for line in (root / "relevant.jsonl").read_text(encoding="utf-8").splitlines() if line.strip()]
            index = json.loads((root / "index.json").read_text(encoding="utf-8"))
            self.assertEqual(segments_payload["status"], "in_progress")
            self.assertEqual(len(segments_payload["segments"]), 1)
            self.assertEqual(relevant[0]["conversation_anchor_entities"], ["HECTR"])
            self.assertEqual(index["relevant_segments"], 1)

    def test_stage_06_context_windows_do_not_cross_conversation_id(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write_jsonl(
                root / "messages.jsonl",
                [
                    dict(msg_row("m1", "2026-04-01T00:00:00Z", content="HECTR lore details."), conversation_id="conv_1", dm_pair_id="pair_1", conversation_topic_label="HECTR", conversation_track="lore"),
                    dict(msg_row("m2", "2026-04-01T00:01:00Z", content="RUINR lore details."), conversation_id="conv_2", dm_pair_id="pair_1", conversation_topic_label="RUINR", conversation_track="lore"),
                ],
            )
            write_json(
                root / "config.json",
                {
                    "anchor_provider": "heuristic",
                    "source_profile_defaults": {
                        "unknown_low_signal": {
                            "strictness_level": "strict",
                            "theriac_relevance_min": 0.01,
                            "meta_lore_split_min": 0.65,
                            "context_window_messages": 3,
                        }
                    },
                },
            )

            run_stage_06(
                root / "messages.jsonl",
                root / "profiles.json",
                root / "snippets.jsonl",
                root / "needs_review.jsonl",
                root / "profiles.json",
                root / "config.json",
                None,
                None,
            )

            snippets = [json.loads(line) for line in (root / "snippets.jsonl").read_text(encoding="utf-8").splitlines() if line.strip()]
            by_id = {snippet["message_ids"][0]: snippet for snippet in snippets}
            self.assertEqual(by_id["m1"]["message_ids"], ["m1"])
            self.assertEqual(by_id["m2"]["message_ids"], ["m2"])
            self.assertEqual(by_id["m1"]["conversation_id"], "conv_1")
            self.assertEqual(by_id["m2"]["conversation_id"], "conv_2")

    def test_stage_06_uses_conversation_metadata_without_per_message_model_call(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write_jsonl(
                root / "messages.jsonl",
                [
                    dict(
                        msg_row("m1", "2026-04-01T00:00:00Z", content="She opens the first door and refuses the easy explanation."),
                        conversation_id="conv_hectr",
                        dm_pair_id="pair_1",
                        conversation_topic_label="HECTR plot role",
                        conversation_topic_summary="HECTR is discussed as part of a plot sequence.",
                        conversation_track="lore",
                        conversation_anchor_entities=["HECTR"],
                        conversation_model_confidence=0.91,
                    )
                ],
            )
            write_json(
                root / "config.json",
                {
                    "anchor_provider": "hybrid",
                    "source_profile_defaults": {
                        "unknown_low_signal": {
                            "strictness_level": "strict",
                            "theriac_relevance_min": 0.7,
                            "meta_lore_split_min": 0.65,
                            "context_window_messages": 1,
                        }
                    },
                },
            )

            with patch("pipeline.stage_06_snippet_extraction.call_model_chat", side_effect=AssertionError("Stage 06 should not call the model")) as mocked_model:
                run_stage_06(
                    root / "messages.jsonl",
                    root / "profiles.json",
                    root / "snippets.jsonl",
                    root / "needs_review.jsonl",
                    root / "profiles.json",
                    root / "config.json",
                    None,
                    None,
                )

            snippets = [json.loads(line) for line in (root / "snippets.jsonl").read_text(encoding="utf-8").splitlines() if line.strip()]
            self.assertEqual(mocked_model.call_count, 0)
            self.assertEqual(len(snippets), 1)
            self.assertEqual(snippets[0]["knowledge_track"], "lore")
            self.assertIn("provider=conversation_metadata", snippets[0]["relevance_reason"])
            self.assertIn("HECTR", snippets[0]["candidate_entities"])

    def test_stage_01_outputs_entity_seeds_not_canonical_cards(self) -> None:
        entities = infer_entities("HECTR. PROJECT OVERVIEW. REMAINING QUESTIONS. JOY ROBERTS.")
        names = {entity["canonical_name"] for entity in entities}

        self.assertIn("HECTR", names)
        self.assertIn("Joy Roberts", names)
        self.assertNotIn("Project Overview", names)
        self.assertNotIn("Remaining Questions", names)
        for entity in entities:
            self.assertIn("entity_seed_id", entity)
            self.assertNotIn("summary", entity)
            self.assertNotIn("source_evidence", entity)
            self.assertNotEqual(entity.get("status"), "canonical")

    def test_entity_resolution_merges_duplicates_and_blocks_headings(self) -> None:
        seeds = [
            {"entity_seed_id": "1", "canonical_name": "HECTR", "entity_type": "character", "aliases": [], "seed_status": "active"},
            {"entity_seed_id": "2", "canonical_name": "Hectr :", "entity_type": "term", "aliases": [], "seed_status": "active"},
            {"entity_seed_id": "3", "canonical_name": "Hectr Hectr", "entity_type": "term", "aliases": [], "seed_status": "active"},
            {"entity_seed_id": "4", "canonical_name": "RUINR", "entity_type": "character", "aliases": [], "seed_status": "active"},
            {"entity_seed_id": "5", "canonical_name": "Ruinr:", "entity_type": "term", "aliases": [], "seed_status": "active"},
            {"entity_seed_id": "6", "canonical_name": "Gfns", "entity_type": "term", "aliases": [], "seed_status": "active"},
            {"entity_seed_id": "7", "canonical_name": "Global Federation of Nation States", "entity_type": "organization", "aliases": [], "seed_status": "active"},
            {"entity_seed_id": "8", "canonical_name": "Joy", "entity_type": "character", "aliases": [], "seed_status": "active"},
            {"entity_seed_id": "9", "canonical_name": "Joy Roberts", "entity_type": "character", "aliases": [], "seed_status": "active"},
            {"entity_seed_id": "10", "canonical_name": "Project Overview", "entity_type": "term", "aliases": [], "seed_status": "active"},
        ]
        payload = resolve_entities(seeds)
        names = [entity["canonical_name"] for entity in payload["resolved_entities"]]

        self.assertEqual(names.count("HECTR"), 1)
        self.assertEqual(names.count("RUINR"), 1)
        self.assertEqual(names.count("Global Federation of Nation States"), 1)
        self.assertEqual(names.count("Joy Roberts"), 1)
        self.assertIn("Project Overview", {entity["canonical_name"] for entity in payload["blocked_entities"]})

    def test_entity_resolution_uses_reviewed_entity_merges_from_memory(self) -> None:
        seeds = [
            {"entity_seed_id": "1", "canonical_name": "RUINR", "entity_type": "character", "aliases": [], "seed_status": "active"},
            {"entity_seed_id": "2", "canonical_name": "ACHILLES", "entity_type": "character", "aliases": [], "seed_status": "active"},
        ]
        payload = resolve_entities(
            seeds,
            {
                "entity_merges": [
                    {
                        "source_entity_name": "ACHILLES",
                        "target_entity_name": "RUINR",
                        "alias_text": "ACHILLES",
                    }
                ],
                "approved_aliases": [],
            },
        )
        entities = payload["resolved_entities"]

        self.assertEqual(len(entities), 1)
        self.assertEqual(entities[0]["canonical_name"], "RUINR")
        self.assertEqual(entities[0]["aliases"], ["ACHILLES"])

    def test_entity_resolution_merges_seed_names_that_match_existing_aliases(self) -> None:
        seeds = [
            {
                "entity_seed_id": "1",
                "canonical_name": "Enoch",
                "entity_type": "character",
                "aliases": ["Enoch Faust Ersetzen"],
                "seed_status": "active",
            },
            {
                "entity_seed_id": "2",
                "canonical_name": "Enoch Faust Ersetzen",
                "entity_type": "term",
                "aliases": [],
                "seed_status": "active",
            },
        ]

        payload = resolve_entities(seeds)
        entities = payload["resolved_entities"]

        self.assertEqual(len(entities), 1)
        self.assertEqual(entities[0]["canonical_name"], "Enoch")
        self.assertEqual(entities[0]["entity_type"], "character")
        self.assertEqual(entities[0]["aliases"], ["Enoch Faust Ersetzen"])

    def test_stage_07_promotes_only_currently_observed_seed_entities(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write_json(
                root / "entity_seed.json",
                {
                    "entities": [
                        {"entity_seed_id": "1", "canonical_name": "HECTR", "entity_type": "character", "aliases": [], "seed_status": "active"},
                        {"entity_seed_id": "2", "canonical_name": "RUINR", "entity_type": "character", "aliases": [], "seed_status": "active"},
                        {"entity_seed_id": "3", "canonical_name": "Late Development Entity", "entity_type": "term", "aliases": [], "seed_status": "active"},
                    ]
                },
            )
            write_jsonl(
                root / "snippets.jsonl",
                [
                    {
                        "snippet_id": "s1",
                        "timestamp_start_utc": "2026-04-01T00:00:00Z",
                        "timestamp_end_utc": "2026-04-01T00:01:00Z",
                        "display_text_normalized": "HECTR is mentioned in this early conversation.",
                        "candidate_entities": ["HECTR"],
                        "relevance_score": 0.9,
                    }
                ],
            )

            run_stage_07(
                root / "snippets.jsonl",
                root / "entity_seed.json",
                root / "aliases.json",
                root / "timelines.json",
                root / "resolved_entities.json",
                None,
            )

            payload = json.loads((root / "resolved_entities.json").read_text(encoding="utf-8"))
            self.assertEqual([entity["canonical_name"] for entity in payload["resolved_entities"]], ["HECTR"])
            self.assertEqual(
                {entity["canonical_name"] for entity in payload["seed_only_entities"]},
                {"Late Development Entity", "RUINR"},
            )

    def test_stage_07_candidate_metadata_can_map_literal_concise_entity_anchor(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write_json(
                root / "entity_seed.json",
                {
                    "entities": [
                        {
                            "entity_seed_id": "1",
                            "canonical_name": "Path A: Destructive Path",
                            "entity_type": "quest",
                            "aliases": [],
                            "seed_status": "active",
                        },
                        {"entity_seed_id": "2", "canonical_name": "HECTR", "entity_type": "character", "aliases": [], "seed_status": "active"},
                    ]
                },
            )
            write_jsonl(
                root / "snippets.jsonl",
                [
                    {
                        "snippet_id": "s1",
                        "timestamp_start_utc": "2026-04-01T00:00:00Z",
                        "timestamp_end_utc": "2026-04-01T00:01:00Z",
                        "display_text_normalized": "Path A is being discussed here.",
                        "candidate_entities": ["Path A"],
                        "relevance_score": 0.9,
                    }
                ],
            )

            run_stage_07(
                root / "snippets.jsonl",
                root / "entity_seed.json",
                root / "aliases.json",
                root / "timelines.json",
                root / "resolved_entities.json",
                None,
            )

            payload = json.loads((root / "resolved_entities.json").read_text(encoding="utf-8"))
            timelines = json.loads((root / "timelines.json").read_text(encoding="utf-8"))["entity_timelines"]
            self.assertEqual([entity["canonical_name"] for entity in payload["resolved_entities"]], ["Path A: Destructive Path"])
            entity_id = payload["resolved_entities"][0]["entity_id"]
            self.assertEqual(timelines[entity_id][0]["match_type"], "candidate_entity_metadata")
            self.assertEqual(json.loads((root / "aliases.json").read_text(encoding="utf-8"))["aliases"], [])

    def test_stage_07_ignores_unbacked_candidate_metadata_anchor(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write_json(
                root / "entity_seed.json",
                {
                    "entities": [
                        {
                            "entity_seed_id": "1",
                            "canonical_name": "Path A: Destructive Path",
                            "entity_type": "quest",
                            "aliases": [],
                            "seed_status": "active",
                        }
                    ]
                },
            )
            write_jsonl(
                root / "snippets.jsonl",
                [
                    {
                        "snippet_id": "s1",
                        "timestamp_start_utc": "2026-04-01T00:00:00Z",
                        "timestamp_end_utc": "2026-04-01T00:01:00Z",
                        "display_text_normalized": "A boss fight structure is being discussed without naming the route.",
                        "candidate_entities": ["Path A"],
                        "relevance_score": 0.9,
                    }
                ],
            )

            run_stage_07(
                root / "snippets.jsonl",
                root / "entity_seed.json",
                root / "aliases.json",
                root / "timelines.json",
                root / "resolved_entities.json",
                None,
            )

            payload = json.loads((root / "resolved_entities.json").read_text(encoding="utf-8"))
            self.assertEqual(payload["resolved_entities"], [])
            self.assertEqual(payload["seed_only_entities"][0]["canonical_name"], "Path A: Destructive Path")

    def test_stage_07a_emits_qwen_annotated_harvest_without_review_gate(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write_json(root / "entity_seed.json", {"entities": []})
            write_json(
                root / "config.json",
                {
                    "model_routing": {
                        "tasks": {
                            "stage_07a_entity_candidate_harvest": {
                                "provider": "openrouter",
                                "api_model": "qwen/qwen3-235b-a22b-2507",
                                "api_base_url": "https://openrouter.ai/api/v1",
                                "temperature": 0.0,
                                "max_tokens": 8192,
                                "max_candidates_per_call": 24,
                            }
                        }
                    }
                },
            )
            write_jsonl(
                root / "snippets.jsonl",
                [
                    {
                        "snippet_id": "s_glass",
                        "timestamp_start_utc": "2026-04-01T00:00:00Z",
                        "timestamp_end_utc": "2026-04-01T00:01:00Z",
                        "display_text_normalized": "The Glass Orchard is a possible early route concept.",
                        "candidate_entities": ["Glass Orchard"],
                        "candidate_topics": ["quest"],
                        "knowledge_track": "lore",
                        "relevance_score": 0.82,
                    }
                ],
            )

            with patch(
                "pipeline.stage_07a_entity_candidate_harvest.call_model_chat",
                return_value=qwen_harvest_response(["glass orchard"], {"glass orchard": "quest"}),
            ) as model:
                run_stage_07a(
                    root / "snippets.jsonl",
                    root / "entity_seed.json",
                    root / "aliases.json",
                    root / "timelines.json",
                    root / "resolved_entities.json",
                    None,
                    root / "entity_candidate_harvest.json",
                    root / "config.json",
                )

            harvest = json.loads((root / "entity_candidate_harvest.json").read_text(encoding="utf-8"))
            resolved = json.loads((root / "resolved_entities.json").read_text(encoding="utf-8"))
            self.assertEqual(model.call_count, 1)
            self.assertEqual(model.call_args.kwargs["api_model"], "qwen/qwen3-235b-a22b-2507")
            self.assertEqual(harvest["summary"]["candidate_count"], 1)
            self.assertEqual(harvest["summary"]["model_annotated_candidate_count"], 1)
            self.assertEqual(harvest["policy"]["model_name"], "qwen/qwen3-235b-a22b-2507")
            self.assertEqual(harvest["candidates"][0]["candidate_name"], "Glass Orchard")
            self.assertEqual(harvest["candidates"][0]["model_annotation_status"], "annotated")
            self.assertEqual(harvest["candidates"][0]["proposed_entity_type"], "quest")
            self.assertEqual(harvest["candidates"][0]["legacy_triage_hint"]["triage_status"], "review_required")
            self.assertEqual(resolved["resolved_entities"], [])
            self.assertFalse((root / "conversation_entity_proposals.json").exists())

    def test_stage_07a_harvests_broad_candidates_with_local_signal_flags(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write_json(
                root / "entity_seed.json",
                {
                    "entities": [
                        {"entity_seed_id": "seed_hectr", "canonical_name": "HECTR", "entity_type": "character", "aliases": [], "seed_status": "active"}
                    ]
                },
            )
            write_jsonl(
                root / "snippets.jsonl",
                [
                    {
                        "snippet_id": "s_generic",
                        "timestamp_start_utc": "2026-04-01T00:00:00Z",
                        "timestamp_end_utc": "2026-04-01T00:01:00Z",
                        "display_text_normalized": "NPC is used here as a generic label near HECTR.",
                        "candidate_entities": ["NPC"],
                        "candidate_topics": ["entity"],
                        "knowledge_track": "lore",
                        "relevance_score": 0.5,
                    },
                    {
                        "snippet_id": "s_adam",
                        "timestamp_start_utc": "2026-04-01T00:02:00Z",
                        "timestamp_end_utc": "2026-04-01T00:03:00Z",
                        "display_text_normalized": "Adam Smasher is an inspiration reference for the boss silhouette.",
                        "candidate_entities": ["Adam Smasher"],
                        "candidate_topics": ["production"],
                        "knowledge_track": "meta",
                        "patch_relationship_type": "inspiration",
                        "relevance_score": 0.7,
                    },
                    {
                        "snippet_id": "s_alad",
                        "timestamp_start_utc": "2026-04-01T00:04:00Z",
                        "timestamp_end_utc": "2026-04-01T00:05:00Z",
                        "display_text_normalized": "Warframe's Alad V is an external media reference, not a THERIAC person.",
                        "candidate_entities": ["Alad V"],
                        "candidate_topics": ["production"],
                        "knowledge_track": "meta",
                        "relevance_score": 0.7,
                    },
                    {
                        "snippet_id": "s_artist",
                        "timestamp_start_utc": "2026-04-01T00:06:00Z",
                        "timestamp_end_utc": "2026-04-01T00:07:00Z",
                        "display_text_normalized": "Mira is the character artist for the game and is on board for visuals.",
                        "candidate_entities": ["Mira"],
                        "candidate_topics": ["production"],
                        "knowledge_track": "meta",
                        "relevance_score": 0.6,
                    },
                    {
                        "snippet_id": "s_name",
                        "timestamp_start_utc": "2026-04-01T00:08:00Z",
                        "timestamp_end_utc": "2026-04-01T00:09:00Z",
                        "display_text_normalized": "Game Name might change later.",
                        "candidate_entities": ["Game Name"],
                        "candidate_topics": ["production"],
                        "knowledge_track": "meta",
                        "relevance_score": 0.4,
                    },
                ],
            )

            with patch(
                "pipeline.stage_07a_entity_candidate_harvest.call_model_chat",
                return_value=qwen_harvest_response(["npc", "adam smasher", "alad v", "mira", "game name"]),
            ):
                run_stage_07a(
                    root / "snippets.jsonl",
                    root / "entity_seed.json",
                    root / "aliases.json",
                    root / "timelines.json",
                    root / "resolved_entities.json",
                    None,
                    root / "entity_candidate_harvest.json",
                )

            harvest = json.loads((root / "entity_candidate_harvest.json").read_text(encoding="utf-8"))
            candidates = {item["normalized_name_key"]: item for item in harvest["candidates"]}
            self.assertTrue(candidates["npc"]["signal_flags"]["generic_phrase"])
            self.assertTrue(candidates["adam smasher"]["signal_flags"]["inspiration_marker"])
            self.assertTrue(candidates["alad v"]["signal_flags"]["external_media_marker"])
            self.assertTrue(candidates["mira"]["signal_flags"]["meta_team_marker"])
            self.assertTrue(candidates["game name"]["signal_flags"]["generic_phrase"])
            self.assertIn("HECTR", [item["canonical_name"] for item in candidates["npc"]["known_entities_co_mentioned"]])

    def test_stage_07a_prior_approved_memory_entity_resolves_from_review_memory(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write_json(root / "entity_seed.json", {"entities": []})
            write_json(
                root / "review_memory.json",
                {
                    "version": 1,
                    "approved_conversation_entities": [
                        {
                            "proposal_id": "proposal_glass",
                            "candidate_name": "Glass Orchard",
                            "canonical_name": "The Glass Orchard",
                            "entity_type": "quest",
                            "aliases": ["Glass Orchard"],
                        }
                    ],
                    "rejected_conversation_entities": [],
                },
            )
            write_jsonl(
                root / "snippets.jsonl",
                [
                    {
                        "snippet_id": "s_glass",
                        "timestamp_start_utc": "2026-04-01T00:00:00Z",
                        "timestamp_end_utc": "2026-04-01T00:01:00Z",
                        "display_text_normalized": "The Glass Orchard opens as a possible early route.",
                        "candidate_entities": ["Glass Orchard"],
                        "candidate_topics": ["quest"],
                        "knowledge_track": "lore",
                        "relevance_score": 0.82,
                    }
                ],
            )

            with patch(
                "pipeline.stage_07a_entity_candidate_harvest.call_model_chat",
                return_value=qwen_harvest_response(["glass orchard"], {"glass orchard": "quest"}),
            ):
                run_stage_07a(
                    root / "snippets.jsonl",
                    root / "entity_seed.json",
                    root / "aliases.json",
                    root / "timelines.json",
                    root / "resolved_entities.json",
                    root / "review_memory.json",
                    root / "entity_candidate_harvest.json",
                )

            resolved = json.loads((root / "resolved_entities.json").read_text(encoding="utf-8"))["resolved_entities"]
            harvest = json.loads((root / "entity_candidate_harvest.json").read_text(encoding="utf-8"))
            self.assertEqual([entity["canonical_name"] for entity in resolved], ["The Glass Orchard"])
            self.assertTrue(harvest["candidates"][0]["signal_flags"]["prior_approved_memory_match"])

    def test_stage_07b_web_adjudicates_only_selected_externality_candidates(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write_json(
                root / "entity_candidate_harvest.json",
                {
                    "schema_version": 1,
                    "candidates": [
                        {
                            "candidate_id": "candidate_adam",
                            "candidate_name": "Adam Smasher",
                            "normalized_name_key": "adam smasher",
                            "source_snippet_ids": ["s_adam"],
                            "evidence_count": 1,
                            "sample_texts": ["Adam Smasher is an inspiration reference for the boss silhouette."],
                            "proposed_entity_type": "term",
                            "type_conflicts": [],
                            "signal_flags": {"inspiration_marker": True, "external_media_marker": True},
                            "model_denotation_class": "likely_external_reference",
                            "recommended_track": "meta",
                            "local_lore_prior": 0.15,
                            "external_reference_prior": 0.7,
                            "model_confidence": 0.83,
                            "model_reasoning_summary": "Local text frames this as inspiration.",
                        },
                        {
                            "candidate_id": "candidate_lab",
                            "candidate_name": "The Lab",
                            "normalized_name_key": "the lab",
                            "source_snippet_ids": ["s_lab"],
                            "evidence_count": 1,
                            "sample_texts": ["The Lab stores the prototype body."],
                            "proposed_entity_type": "location",
                            "type_conflicts": [],
                            "signal_flags": {},
                            "model_denotation_class": "likely_lore_entity",
                            "recommended_track": "lore",
                            "local_lore_prior": 0.82,
                            "external_reference_prior": 0.05,
                            "model_confidence": 0.91,
                            "model_reasoning_summary": "Local text treats this as an in-world place.",
                        },
                    ],
                },
            )
            write_json(
                root / "config.json",
                {
                    "model_routing": {
                        "tasks": {
                            "stage_07b_entity_adjudication_web": {
                                "provider": "openrouter",
                                "api_model": "google/gemini-3.1-pro-preview",
                                "api_base_url": "https://openrouter.ai/api/v1",
                                "temperature": 0.0,
                                "max_tokens": 3500,
                                "tools": [{"type": "openrouter:web_search", "parameters": {"max_results": 3}}],
                            }
                        }
                    }
                },
            )

            with patch(
                "pipeline.stage_07b_entity_adjudication.call_model_chat",
                return_value=web_adjudication_response("adam smasher", candidate_name="Adam Smasher"),
            ) as model:
                run_stage_07b(
                    root / "entity_candidate_harvest.json",
                    root / "entity_adjudication_recommendations.json",
                    root / "externality_cache.json",
                    root / "config.json",
                )

            self.assertEqual(model.call_count, 1)
            self.assertEqual(model.call_args.kwargs["tools"][0]["type"], "openrouter:web_search")
            self.assertIn("json_schema", model.call_args.kwargs)
            payload = json.loads((root / "entity_adjudication_recommendations.json").read_text(encoding="utf-8"))
            recommendations = {item["normalized_key"]: item for item in payload["recommendations"]}
            self.assertEqual(payload["summary"]["web_selected_candidate_count"], 1)
            self.assertEqual(payload["summary"]["web_call_count"], 1)
            self.assertTrue(payload["policy"]["web_search_detects_externality_not_canon"])
            self.assertEqual(recommendations["adam smasher"]["externality_class"], "external_fictional_ip")
            self.assertEqual(recommendations["adam smasher"]["recommended_action"], "demote_meta")
            self.assertEqual(recommendations["adam smasher"]["adjudication_status"], "web_adjudicated")
            self.assertEqual(recommendations["the lab"]["adjudication_status"], "local_only_not_selected")
            self.assertEqual(recommendations["the lab"]["web_findings"], [])

    def test_stage_07b_reuses_externality_cache_without_web_call(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write_json(
                root / "entity_candidate_harvest.json",
                {
                    "schema_version": 1,
                    "candidates": [
                        {
                            "candidate_id": "candidate_adam",
                            "candidate_name": "Adam Smasher",
                            "normalized_name_key": "adam smasher",
                            "source_snippet_ids": ["s_adam"],
                            "evidence_count": 1,
                            "sample_texts": ["Adam Smasher is an inspiration reference for the boss silhouette."],
                            "proposed_entity_type": "term",
                            "type_conflicts": [],
                            "signal_flags": {"inspiration_marker": True},
                            "model_denotation_class": "likely_external_reference",
                            "recommended_track": "meta",
                            "local_lore_prior": 0.15,
                            "external_reference_prior": 0.7,
                            "model_confidence": 0.83,
                        }
                    ],
                },
            )
            write_json(
                root / "config.json",
                {
                    "model_routing": {
                        "tasks": {
                            "stage_07b_entity_adjudication_web": {
                                "provider": "openrouter",
                                "api_model": "google/gemini-3.1-pro-preview",
                                "tools": [{"type": "openrouter:web_search"}],
                            }
                        }
                    }
                },
            )

            with patch(
                "pipeline.stage_07b_entity_adjudication.call_model_chat",
                return_value=web_adjudication_response("adam smasher", candidate_name="Adam Smasher"),
            ):
                run_stage_07b(
                    root / "entity_candidate_harvest.json",
                    root / "entity_adjudication_recommendations.json",
                    root / "externality_cache.json",
                    root / "config.json",
                )

            with patch(
                "pipeline.stage_07b_entity_adjudication.call_model_chat",
                side_effect=AssertionError("cached externality should not call web model"),
            ):
                run_stage_07b(
                    root / "entity_candidate_harvest.json",
                    root / "entity_adjudication_recommendations.json",
                    root / "externality_cache.json",
                    root / "config.json",
                )

            payload = json.loads((root / "entity_adjudication_recommendations.json").read_text(encoding="utf-8"))
            self.assertEqual(payload["summary"]["cache_hit_count"], 1)
            self.assertEqual(payload["summary"]["web_call_count"], 0)
            self.assertEqual(payload["recommendations"][0]["cache_status"], "hit")

    def test_stage_07c_theme_miner_updates_persistent_theme_profile(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write_json(
                root / "entity_candidate_harvest.json",
                {
                    "schema_version": 1,
                    "candidates": [
                        {
                            "candidate_id": "candidate_ninhursag",
                            "candidate_name": "Ninhursag",
                            "normalized_name_key": "ninhursag",
                            "source_snippet_ids": ["s_ninhursag"],
                            "sample_texts": ["Ninhursag is being used as a Sumerian lab architecture name."],
                            "proposed_entity_type": "term",
                            "local_lore_prior": 0.68,
                        }
                    ],
                },
            )
            write_json(
                root / "entity_adjudication_recommendations.json",
                {
                    "schema_version": 1,
                    "recommendations": [
                        {
                            "candidate_name": "Ninhursag",
                            "normalized_key": "ninhursag",
                            "recommended_action": "needs_author_review",
                            "recommended_track": "lore_candidate",
                            "recommended_entity_type": "term",
                            "externality_class": "historical_or_mythological",
                            "local_lore_prior": 0.68,
                            "external_reference_prior": 0.8,
                            "source_snippet_ids": ["s_ninhursag"],
                            "in_world_signals": ["Local text suggests an in-world lab architecture name"],
                            "web_findings": [{"query": "Ninhursag", "finding": "Ninhursag is a Sumerian mother goddess.", "externality_weight": 0.9}],
                            "reasoning_summary": "Sumerian mythological externality, but local usage looks in-world.",
                        }
                    ],
                },
            )
            write_json(root / "resolved_entities.json", {"resolved_entities": []})
            write_json(root / "review_memory.json", {"accepted_claims": []})
            write_json(root / "config.json", {"model_routing": {"tasks": {"stage_07c_theme_miner": {"provider": "openrouter", "api_model": "google/gemini-3.1-pro-preview"}}}})

            with patch("pipeline.stage_07c_theme_miner.call_model_chat", return_value=theme_miner_response()) as model:
                run_stage_07c(
                    root / "entity_candidate_harvest.json",
                    root / "entity_adjudication_recommendations.json",
                    root / "resolved_entities.json",
                    root / "review_memory.json",
                    root / "theme_profile.json",
                    root / "theme_profile_update_report.json",
                    root / "config.json",
                )

            self.assertEqual(model.call_count, 1)
            profile = json.loads((root / "theme_profile.json").read_text(encoding="utf-8"))
            report = json.loads((root / "theme_profile_update_report.json").read_text(encoding="utf-8"))
            self.assertTrue(profile["policy"]["transitive_thematic_learning_not_transitive_canon"])
            self.assertEqual(profile["themes"][0]["theme_id"], "theme_sumerian_mythology")
            self.assertEqual(profile["themes"][0]["status"], "active")
            self.assertIn("Ninhursag", profile["themes"][0]["evidence_entities"])
            self.assertEqual(report["summary"]["applied_update_count"], 1)

    def test_stage_07d_theme_reclassification_boosts_prior_without_promoting_canon(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write_json(
                root / "entity_candidate_harvest.json",
                {
                    "schema_version": 1,
                    "candidates": [
                        {
                            "candidate_id": "candidate_enki",
                            "candidate_name": "Enki",
                            "normalized_name_key": "enki",
                            "sample_texts": ["Enki may be a name for a non-human cognition subsystem."],
                            "local_lore_prior": 0.52,
                        }
                    ],
                },
            )
            write_json(
                root / "entity_adjudication_recommendations.json",
                {
                    "schema_version": 1,
                    "recommendations": [
                        {
                            "candidate_id": "candidate_enki",
                            "candidate_name": "Enki",
                            "normalized_key": "enki",
                            "recommended_action": "needs_author_review",
                            "recommended_track": "mixed",
                            "recommended_entity_type": "term",
                            "externality_class": "historical_or_mythological",
                            "local_lore_prior": 0.52,
                            "external_reference_prior": 0.82,
                            "web_findings": [{"query": "Enki", "finding": "Enki is a Sumerian deity.", "externality_weight": 0.88}],
                            "reasoning_summary": "Historical/mythological externality with local in-world hints.",
                            "human_review_question": "Is Enki lore or comparison?",
                        }
                    ],
                },
            )
            write_json(
                root / "theme_profile.json",
                {
                    "schema_version": 1,
                    "policy": {"theme_match_is_not_promotion_rule": True},
                    "themes": [
                        {
                            "theme_id": "theme_sumerian_mythology",
                            "label": "Sumerian mythology",
                            "theme_type": "mythological_lineage",
                            "status": "active",
                            "confidence": 0.87,
                            "canon_relevance": "lore_pattern",
                            "description": "Sumerian deity names are used for AI systems.",
                            "evidence_entities": ["Ninhursag", "Inanna"],
                            "positive_indicators": ["Sumerian deity name", "Enki"],
                            "negative_indicators": [],
                            "related_themes": [],
                            "disambiguation_notes": ["Sumerian origin alone is not enough for canon promotion."],
                            "last_updated": "2026-05-21T00:00:00Z",
                        }
                    ],
                },
            )

            run_stage_07d(
                root / "entity_candidate_harvest.json",
                root / "entity_adjudication_recommendations.json",
                root / "theme_profile.json",
                root / "theme_candidate_reclassification.json",
            )

            payload = json.loads((root / "theme_candidate_reclassification.json").read_text(encoding="utf-8"))
            row = payload["candidate_reclassifications"][0]
            self.assertTrue(payload["policy"]["theme_match_changes_prior_not_final_decision"])
            self.assertEqual(row["normalized_key"], "enki")
            self.assertEqual(row["theme_matches"][0]["theme_id"], "theme_sumerian_mythology")
            self.assertGreater(row["theme_adjusted_lore_prior"], row["base_local_lore_prior"])
            self.assertEqual(row["theme_adjusted_recommended_action"], "needs_author_review")
            self.assertIn("Theme match changes relevance prior only", row["why_not_auto_promote"])

    def test_run_from_stage_05_uses_stage_07a_to_07d_without_entity_review_gate(self) -> None:
        import sys
        import pipeline.run_from_stage_05 as resume

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            paths = ArtifactPaths(root)
            write_json(paths.entity_seed, {"entities": []})
            write_jsonl(paths.relevant_messages, [])
            write_json(paths.conversation_segments, {"segments": []})

            def fake_stage_05(_messages: Path, _segments: Path, out_json: Path, out_jsonl: Path, out_failures: Path, _config: Path) -> None:
                write_json(out_json, {"status": "complete", "conversation_count": 1, "notes_count": 1, "failure_count": 0, "notes": []})
                write_jsonl(out_jsonl, [])
                write_json(out_failures, {"failures": []})

            def fake_stage_06(
                _messages: Path,
                _profiles_in: Path,
                out_snippets: Path,
                out_needs_review: Path,
                out_profiles: Path,
                *_args: Any,
            ) -> None:
                write_jsonl(
                    out_snippets,
                    [
                        {
                            "snippet_id": "s_glass",
                            "timestamp_start_utc": "2026-04-01T00:00:00Z",
                            "timestamp_end_utc": "2026-04-01T00:01:00Z",
                            "display_text_normalized": "The Glass Orchard is a possible early route concept.",
                            "candidate_entities": ["Glass Orchard"],
                            "candidate_topics": ["quest"],
                            "knowledge_track": "lore",
                            "relevance_score": 0.82,
                        }
                    ],
                )
                write_jsonl(out_needs_review, [])
                write_json(out_profiles, {"profiles": []})

            def fake_stage_08(_snippets: Path, _entities: Path, out_lore: Path, out_meta: Path, *_args: Any) -> None:
                write_json(out_lore, {"clusters": []})
                write_json(out_meta, {"clusters": []})

            def fake_stage_09(_entities: Path, _lore: Path, _meta: Path, _aliases: Path, _snippets: Path, out_dir: Path, *_args: Any) -> None:
                write_json(out_dir / "claim_drafts.json", {"claims": []})
                write_json(out_dir / "meta_cards_draft.json", {"meta_cards": []})

            def fake_stage_07c(
                _harvest: Path,
                _adjudication: Path,
                _resolved: Path,
                _memory: Path,
                _theme_profile: Path,
                out_report: Path,
                *_args: Any,
            ) -> None:
                write_json(out_report, {"schema_version": 1, "summary": {"theme_count": 0, "applied_update_count": 0}, "inputs": {"evidence_packet_count": 0}})

            def fake_stage_07d(_harvest: Path, _adjudication: Path, _theme_profile: Path, out_reclassification: Path) -> None:
                write_json(out_reclassification, {"schema_version": 1, "summary": {"theme_matched_candidate_count": 0}, "candidate_reclassifications": []})

            with patch.object(sys, "argv", ["run_from_stage_05", "--artifacts-root", str(root)]), patch.object(
                resume, "run_stage_05", side_effect=fake_stage_05
            ), patch.object(resume, "run_stage_06", side_effect=fake_stage_06), patch.object(
                resume, "run_stage_08", side_effect=fake_stage_08
            ), patch.object(
                resume, "run_stage_09", side_effect=fake_stage_09
            ), patch.object(
                resume, "run_stage_07c", side_effect=fake_stage_07c
            ), patch.object(
                resume, "run_stage_07d", side_effect=fake_stage_07d
            ), patch(
                "pipeline.stage_07a_entity_candidate_harvest.call_model_chat",
                return_value=qwen_harvest_response(["glass orchard"], {"glass orchard": "quest"}),
            ), patch(
                "pipeline.stage_07b_entity_adjudication.call_model_chat",
                return_value=web_adjudication_response("glass orchard", candidate_name="Glass Orchard", externality_class="none_detected", recommended_action="needs_author_review"),
            ):
                resume.main()

            harvest = json.loads(paths.entity_candidate_harvest.read_text(encoding="utf-8"))
            adjudication = json.loads(paths.entity_adjudication_recommendations.read_text(encoding="utf-8"))
            self.assertEqual(harvest["candidates"][0]["candidate_name"], "Glass Orchard")
            self.assertEqual(adjudication["summary"]["web_call_count"], 1)
            self.assertTrue(paths.theme_profile_update_report.exists())
            self.assertTrue(paths.theme_candidate_reclassification.exists())
            self.assertFalse(paths.conversation_entity_proposals.exists())

    def test_entity_candidate_harvest_schema_contract_lists_required_fields(self) -> None:
        schema = json.loads(Path("schema/entity_candidate_harvest_schema.json").read_text(encoding="utf-8"))
        self.assertIn("candidates", schema["required"])
        candidate_required = set(schema["properties"]["candidates"]["items"]["required"])
        for field in (
            "candidate_id",
            "candidate_name",
            "normalized_name_key",
            "surface_forms",
            "source_snippet_ids",
            "signal_flags",
            "legacy_triage_hint",
        ):
            self.assertIn(field, candidate_required)
        candidate_properties = schema["properties"]["candidates"]["items"]["properties"]
        self.assertIn("model_annotation", candidate_properties)
        self.assertIn("model_denotation_class", candidate_properties)
        self.assertIn("human_review_question", candidate_properties)

    def test_entity_adjudication_schema_contract_lists_required_fields(self) -> None:
        schema = json.loads(Path("schema/entity_adjudication_schema.json").read_text(encoding="utf-8"))
        self.assertIn("recommendations", schema["required"])
        recommendation_required = set(schema["properties"]["recommendations"]["items"]["required"])
        for field in (
            "candidate_name",
            "normalized_key",
            "recommended_action",
            "recommended_track",
            "externality_class",
            "web_findings",
            "human_review_question",
        ):
            self.assertIn(field, recommendation_required)
        externality_enum = schema["properties"]["recommendations"]["items"]["properties"]["externality_class"]["enum"]
        self.assertIn("external_fictional_ip", externality_enum)
        self.assertIn("historical_or_mythological", externality_enum)

    def test_theme_profile_schema_contract_lists_required_fields(self) -> None:
        schema = json.loads(Path("schema/theme_profile_schema.json").read_text(encoding="utf-8"))
        self.assertIn("themes", schema["required"])
        theme_required = set(schema["properties"]["themes"]["items"]["required"])
        for field in (
            "theme_id",
            "label",
            "status",
            "confidence",
            "evidence_entities",
            "positive_indicators",
            "disambiguation_notes",
        ):
            self.assertIn(field, theme_required)
        self.assertIn("active", schema["properties"]["themes"]["items"]["properties"]["status"]["enum"])
        self.assertIn("meta_only", schema["properties"]["themes"]["items"]["properties"]["status"]["enum"])

    def test_theme_reclassification_schema_contract_lists_required_fields(self) -> None:
        schema = json.loads(Path("schema/theme_candidate_reclassification_schema.json").read_text(encoding="utf-8"))
        self.assertIn("candidate_reclassifications", schema["required"])
        row_required = set(schema["properties"]["candidate_reclassifications"]["items"]["required"])
        self.assertIn("theme_matches", row_required)
        self.assertIn("theme_adjusted_lore_prior", row_required)
        self.assertIn("why_not_auto_promote", row_required)

    def test_stage_07_proposes_text_observed_conversation_entity_not_in_seed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write_json(
                root / "entity_seed.json",
                {
                    "entities": [
                        {"entity_seed_id": "1", "canonical_name": "HECTR", "entity_type": "character", "aliases": [], "seed_status": "active"}
                    ]
                },
            )
            write_jsonl(
                root / "snippets.jsonl",
                [
                    {
                        "snippet_id": "s1",
                        "timestamp_start_utc": "2026-04-01T00:00:00Z",
                        "timestamp_end_utc": "2026-04-01T00:01:00Z",
                        "display_text_normalized": "The Glass Orchard is a possible early route concept.",
                        "candidate_entities": ["Glass Orchard"],
                        "candidate_topics": ["quest"],
                        "knowledge_track": "lore",
                        "relevance_score": 0.82,
                    }
                ],
            )

            with self.assertRaisesRegex(RuntimeError, "conversation entity proposal"):
                run_stage_07(
                    root / "snippets.jsonl",
                    root / "entity_seed.json",
                    root / "aliases.json",
                    root / "timelines.json",
                    root / "resolved_entities.json",
                    None,
                    root / "conversation_entity_proposals.json",
                    root / "conversation_entity_decisions.json",
                )

            payload = json.loads((root / "resolved_entities.json").read_text(encoding="utf-8"))
            proposals = json.loads((root / "conversation_entity_proposals.json").read_text(encoding="utf-8"))["proposals"]
            self.assertEqual(payload["resolved_entities"], [])
            self.assertEqual(proposals[0]["candidate_name"], "Glass Orchard")
            self.assertEqual(proposals[0]["proposed_entity_type"], "quest")
            self.assertEqual(proposals[0]["review_status"], "pending")

    def test_stage_07_proposes_entity_from_patch_note_evidence_text(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write_json(root / "entity_seed.json", {"entities": []})
            loss_snippets = []
            for idx in range(5):
                loss_snippets.append(
                    {
                        "snippet_id": f"s_patch_loss_{idx}",
                        "timestamp_start_utc": f"2026-04-01T00:0{idx}:00Z",
                        "timestamp_end_utc": f"2026-04-01T00:0{idx}:30Z",
                        "display_text_normalized": "Patch note item: Loss / classification_change: Loss is treated as a character rather than a theme.\nSupporting messages:\nThe working name is changing because they keep being referred to with pronouns.",
                        "patch_item_text": "Loss / classification_change: Loss is treated as a character rather than a theme.",
                        "patch_item_type": "entity_update",
                        "patch_update_type": "classification_change",
                        "patch_candidate_entities": ["Loss"],
                        "candidate_entities": ["Loss"],
                        "candidate_topics": ["entity"],
                        "knowledge_track": "lore",
                        "relevance_score": 0.86,
                    }
                )
            write_jsonl(
                root / "snippets.jsonl",
                loss_snippets,
            )

            with self.assertRaisesRegex(RuntimeError, "conversation entity proposal"):
                run_stage_07(
                    root / "snippets.jsonl",
                    root / "entity_seed.json",
                    root / "aliases.json",
                    root / "timelines.json",
                    root / "resolved_entities.json",
                    None,
                    root / "conversation_entity_proposals.json",
                    root / "conversation_entity_decisions.json",
                )

            proposal = json.loads((root / "conversation_entity_proposals.json").read_text(encoding="utf-8"))["proposals"][0]
            self.assertEqual(proposal["candidate_name"], "Loss")
            self.assertEqual(proposal["proposed_entity_type"], "character")
            self.assertIn("Loss / classification_change", proposal["sample_texts"][0])

    def test_stage_07_triages_low_evidence_phrase_to_candidate_inventory(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write_json(root / "entity_seed.json", {"entities": []})
            write_jsonl(
                root / "snippets.jsonl",
                [
                    {
                        "snippet_id": "s_phrase",
                        "timestamp_start_utc": "2026-04-01T00:00:00Z",
                        "timestamp_end_utc": "2026-04-01T00:01:00Z",
                        "display_text_normalized": "Impactful Consequences are discussed as a design goal.",
                        "candidate_entities": ["Impactful Consequences"],
                        "candidate_topics": ["entity"],
                        "knowledge_track": "lore",
                        "relevance_score": 0.82,
                    }
                ],
            )

            run_stage_07(
                root / "snippets.jsonl",
                root / "entity_seed.json",
                root / "aliases.json",
                root / "timelines.json",
                root / "resolved_entities.json",
                None,
                root / "conversation_entity_proposals.json",
                root / "conversation_entity_decisions.json",
            )

            payload = json.loads((root / "conversation_entity_proposals.json").read_text(encoding="utf-8"))
            self.assertEqual(payload["proposals"], [])
            self.assertEqual(payload["candidate_inventory"][0]["candidate_name"], "Impactful Consequences")
            self.assertEqual(payload["candidate_inventory"][0]["triage_status"], "candidate_inventory")

    def test_stage_07_triages_team_contributor_to_meta_inventory(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write_json(root / "entity_seed.json", {"entities": []})
            rows = []
            for idx in range(5):
                rows.append(
                    {
                        "snippet_id": f"s_artist_{idx}",
                        "timestamp_start_utc": f"2026-04-01T00:0{idx}:00Z",
                        "timestamp_end_utc": f"2026-04-01T00:0{idx}:30Z",
                        "display_text_normalized": (
                            "Patch note item: Corinah / role_change: Corinah is assigned as an artist "
                            "for THERIAC and joins the project art team.\nSupporting messages:\n"
                            "Corinah says she can do art for the game and help with animation."
                        ),
                        "patch_item_text": "Corinah / role_change: Corinah is assigned as an artist for THERIAC.",
                        "patch_item_type": "entity_update",
                        "patch_update_type": "role_change",
                        "patch_candidate_entities": ["Corinah"],
                        "candidate_entities": ["Corinah"],
                        "candidate_topics": ["entity"],
                        "knowledge_track": "lore",
                        "relevance_score": 0.9,
                    }
                )
            write_jsonl(root / "snippets.jsonl", rows)

            run_stage_07(
                root / "snippets.jsonl",
                root / "entity_seed.json",
                root / "aliases.json",
                root / "timelines.json",
                root / "resolved_entities.json",
                None,
                root / "conversation_entity_proposals.json",
                root / "conversation_entity_decisions.json",
            )

            payload = json.loads((root / "conversation_entity_proposals.json").read_text(encoding="utf-8"))
            self.assertEqual(payload["proposals"], [])
            self.assertEqual(payload["candidate_inventory"][0]["candidate_name"], "Corinah")
            self.assertEqual(payload["candidate_inventory"][0]["triage_status"], "candidate_inventory")
            self.assertIn("project/team contributor", payload["candidate_inventory"][0]["triage_reason"])
            self.assertEqual(payload["candidate_inventory"][0]["knowledge_track_counts"], {"lore": 5})

    def test_stage_07_triages_external_media_characters_to_inventory(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write_json(root / "entity_seed.json", {"entities": []})
            rows = []
            external_examples = {
                "Erra": "Warframe's New War quest, alongside Corpus, Orokin, Grineer, and Narmer.",
                "Hunhow": "Warframe's New War quest, alongside Corpus, Orokin, Grineer, and Narmer.",
                "Alad V": "Warframe's Corpus faction and Orokin history.",
                "Nef Anyo": "Warframe's Corpus faction and Orokin history.",
                "Parvos Granum": "Warframe's Corpus faction and Orokin history.",
                "Sons of Calydon": "Zenless Zone Zero, ZZZ faction bonuses, Billy, Caesar, and bangboo details.",
                "Sons of Calydon Quest": "Zenless Zone Zero, ZZZ faction bonuses, Billy, Caesar, and bangboo details.",
            }
            for name, source_text in external_examples.items():
                for idx in range(5):
                    rows.append(
                        {
                            "snippet_id": f"s_{name.replace(' ', '_')}_{idx}",
                            "timestamp_start_utc": f"2026-04-01T00:{idx:02d}:00Z",
                            "timestamp_end_utc": f"2026-04-01T00:{idx:02d}:30Z",
                            "display_text_normalized": (
                                f"Patch note item: {name} / role_change: {name} is discussed as part of "
                                f"{source_text}"
                            ),
                            "patch_item_text": (
                                f"{name} / role_change: {name} is discussed as part of {source_text}"
                            ),
                            "patch_item_type": "entity_update",
                            "patch_update_type": "role_change",
                            "patch_candidate_entities": [name],
                            "candidate_entities": [name],
                            "candidate_topics": ["entity"],
                            "knowledge_track": "lore",
                            "relevance_score": 0.9,
                        }
                    )
            write_jsonl(root / "snippets.jsonl", rows)

            run_stage_07(
                root / "snippets.jsonl",
                root / "entity_seed.json",
                root / "aliases.json",
                root / "timelines.json",
                root / "resolved_entities.json",
                None,
                root / "conversation_entity_proposals.json",
                root / "conversation_entity_decisions.json",
            )

            payload = json.loads((root / "conversation_entity_proposals.json").read_text(encoding="utf-8"))
            self.assertEqual(payload["proposals"], [])
            inventory_by_key = {item["normalized_name_key"]: item for item in payload["candidate_inventory"]}
            self.assertEqual(set(inventory_by_key), {normalized_name_key(name) for name in external_examples})
            for name in external_examples:
                item = inventory_by_key[normalized_name_key(name)]
                self.assertEqual(item["triage_status"], "candidate_inventory")
                self.assertIn("external-media", item["triage_reason"])

    def test_stage_07_keeps_external_name_for_review_when_adopted_into_theriac(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write_json(root / "entity_seed.json", {"entities": []})
            rows = []
            for idx in range(5):
                rows.append(
                    {
                        "snippet_id": f"s_erra_theriac_{idx}",
                        "timestamp_start_utc": f"2026-04-01T00:{idx:02d}:00Z",
                        "timestamp_end_utc": f"2026-04-01T00:{idx:02d}:30Z",
                        "display_text_normalized": (
                            "Patch note item: Erra / introduced: Erra is explicitly introduced as a THERIAC character."
                        ),
                        "patch_item_text": "Erra / introduced: Erra is explicitly introduced as a THERIAC character.",
                        "patch_item_type": "entity_update",
                        "patch_update_type": "introduced",
                        "patch_candidate_entities": ["Erra"],
                        "candidate_entities": ["Erra"],
                        "candidate_topics": ["entity"],
                        "knowledge_track": "lore",
                        "relevance_score": 0.9,
                    }
                )
            write_jsonl(root / "snippets.jsonl", rows)

            with self.assertRaisesRegex(RuntimeError, "conversation entity proposal"):
                run_stage_07(
                    root / "snippets.jsonl",
                    root / "entity_seed.json",
                    root / "aliases.json",
                    root / "timelines.json",
                    root / "resolved_entities.json",
                    None,
                    root / "conversation_entity_proposals.json",
                    root / "conversation_entity_decisions.json",
                )

            payload = json.loads((root / "conversation_entity_proposals.json").read_text(encoding="utf-8"))
            self.assertEqual(payload["proposals"][0]["candidate_name"], "Erra")
            self.assertEqual(payload["proposals"][0]["triage_status"], "review_required")

    def test_stage_07_triages_reference_inspirations_to_inventory(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write_json(root / "entity_seed.json", {"entities": []})
            rows = []
            reference_examples = {
                "Adam Smasher": "Golok is more akin to Adam Smasher in ethos; this is inspiration for Golok.",
                "Aubrey de Grey": "Enoch's wife is inspired by figures like Aubrey de Grey.",
                "Gendo": "Enoch's relationship with Fear is compared to Gendo and Rei.",
                "Gendo Ikari": "The character design uses a female Gendo Ikari archetype as inspiration.",
                "Mamoru Oshii": "Mamoru Oshii's visual style influenced THERIAC's art style.",
            }
            for name, text in reference_examples.items():
                for idx in range(5):
                    rows.append(
                        {
                            "snippet_id": f"s_{normalized_name_key(name).replace(' ', '_')}_{idx}",
                            "timestamp_start_utc": f"2026-04-01T00:{idx:02d}:00Z",
                            "timestamp_end_utc": f"2026-04-01T00:{idx:02d}:30Z",
                            "display_text_normalized": f"Patch note item: {name} / role_change: {text}",
                            "patch_item_text": f"{name} / role_change: {text}",
                            "patch_item_type": "entity_update",
                            "patch_update_type": "role_change",
                            "patch_relationship_type": "inspiration",
                            "patch_candidate_entities": [name],
                            "candidate_entities": [name],
                            "candidate_topics": ["entity"],
                            "knowledge_track": "lore",
                            "relevance_score": 0.9,
                        }
                    )
            write_jsonl(root / "snippets.jsonl", rows)

            run_stage_07(
                root / "snippets.jsonl",
                root / "entity_seed.json",
                root / "aliases.json",
                root / "timelines.json",
                root / "resolved_entities.json",
                None,
                root / "conversation_entity_proposals.json",
                root / "conversation_entity_decisions.json",
            )

            payload = json.loads((root / "conversation_entity_proposals.json").read_text(encoding="utf-8"))
            self.assertEqual(payload["proposals"], [])
            inventory_by_key = {item["normalized_name_key"]: item for item in payload["candidate_inventory"]}
            self.assertEqual(set(inventory_by_key), {normalized_name_key(name) for name in reference_examples})
            for name in reference_examples:
                item = inventory_by_key[normalized_name_key(name)]
                self.assertEqual(item["triage_status"], "candidate_inventory")
                self.assertIn("reference/inspiration", item["triage_reason"])

    def test_stage_07_keeps_reference_name_for_review_when_adopted_into_theriac(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write_json(root / "entity_seed.json", {"entities": []})
            rows = []
            for idx in range(5):
                rows.append(
                    {
                        "snippet_id": f"s_adam_theriac_{idx}",
                        "timestamp_start_utc": f"2026-04-01T00:{idx:02d}:00Z",
                        "timestamp_end_utc": f"2026-04-01T00:{idx:02d}:30Z",
                        "display_text_normalized": (
                            "Patch note item: Adam Smasher / introduced: "
                            "Adam Smasher is explicitly introduced as a THERIAC character."
                        ),
                        "patch_item_text": (
                            "Adam Smasher / introduced: Adam Smasher is explicitly introduced as a THERIAC character."
                        ),
                        "patch_item_type": "entity_update",
                        "patch_update_type": "introduced",
                        "patch_candidate_entities": ["Adam Smasher"],
                        "candidate_entities": ["Adam Smasher"],
                        "candidate_topics": ["entity"],
                        "knowledge_track": "lore",
                        "relevance_score": 0.9,
                    }
                )
            write_jsonl(root / "snippets.jsonl", rows)

            with self.assertRaisesRegex(RuntimeError, "conversation entity proposal"):
                run_stage_07(
                    root / "snippets.jsonl",
                    root / "entity_seed.json",
                    root / "aliases.json",
                    root / "timelines.json",
                    root / "resolved_entities.json",
                    None,
                    root / "conversation_entity_proposals.json",
                    root / "conversation_entity_decisions.json",
                )

            payload = json.loads((root / "conversation_entity_proposals.json").read_text(encoding="utf-8"))
            self.assertEqual(payload["proposals"][0]["candidate_name"], "Adam Smasher")
            self.assertEqual(payload["proposals"][0]["triage_status"], "review_required")

    def test_stage_07_approved_candidate_alias_attaches_to_existing_entity(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write_json(
                root / "entity_seed.json",
                {
                    "entities": [
                        {
                            "entity_seed_id": "seed_oyuun",
                            "canonical_name": "Oyuun",
                            "entity_type": "character",
                            "aliases": [],
                            "seed_status": "active",
                        }
                    ]
                },
            )
            rows = []
            for idx in range(5):
                rows.append(
                    {
                        "snippet_id": f"s_fear_{idx}",
                        "timestamp_start_utc": f"2026-04-01T00:{idx:02d}:00Z",
                        "timestamp_end_utc": f"2026-04-01T00:{idx:02d}:30Z",
                        "display_text_normalized": "Patch note item: Fear / reinforced: Fear is developed as a character.",
                        "patch_item_text": "Fear / reinforced: Fear is developed as a character.",
                        "patch_item_type": "entity_update",
                        "patch_update_type": "reinforced",
                        "patch_candidate_entities": ["Fear"],
                        "candidate_entities": ["Fear"],
                        "candidate_topics": ["entity"],
                        "knowledge_track": "lore",
                        "relevance_score": 0.9,
                    }
                )
            write_jsonl(root / "snippets.jsonl", rows)
            write_json(
                root / "conversation_entity_decisions.json",
                {
                    "decisions": [
                        {
                            "candidate_name": "Fear",
                            "decision": "approve",
                            "canonical_name": "Oyuun",
                            "entity_type": "character",
                            "reviewer": "r",
                            "rationale": "working name",
                            "timestamp_utc": "2026-05-17T00:00:00Z",
                        }
                    ]
                },
            )

            run_stage_07(
                root / "snippets.jsonl",
                root / "entity_seed.json",
                root / "aliases.json",
                root / "timelines.json",
                root / "resolved_entities.json",
                root / "memory.json",
                root / "conversation_entity_proposals.json",
                root / "conversation_entity_decisions.json",
            )

            resolved = json.loads((root / "resolved_entities.json").read_text(encoding="utf-8"))["resolved_entities"]
            oyuun = next(entity for entity in resolved if entity["canonical_name"] == "Oyuun")
            self.assertIn("Fear", oyuun["aliases"])
            alias_map = json.loads((root / "aliases.json").read_text(encoding="utf-8"))["aliases"]
            self.assertTrue(any(alias["alias_text"] == "Fear" and alias["entity_id"] == oyuun["entity_id"] for alias in alias_map))
            memory = json.loads((root / "memory.json").read_text(encoding="utf-8"))
            self.assertEqual(memory["approved_conversation_entities"][0]["candidate_name"], "Fear")
            self.assertEqual(memory["approved_conversation_entities"][0]["canonical_name"], "Oyuun")

    def test_stage_07_model_alias_resolution_prefills_review_proposal(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write_json(
                root / "entity_seed.json",
                {
                    "entities": [
                        {
                            "entity_seed_id": "seed_enoch",
                            "canonical_name": "Enoch",
                            "entity_type": "character",
                            "aliases": [],
                            "seed_status": "active",
                        }
                    ]
                },
            )
            write_jsonl(
                root / "snippets.jsonl",
                [
                    {
                        "snippet_id": "s_loss_alias",
                        "timestamp_start_utc": "2026-04-01T00:00:00Z",
                        "timestamp_end_utc": "2026-04-01T00:01:00Z",
                        "display_text_normalized": "Patch note item: Loss / alias: Loss is an early working name for Enoch.",
                        "patch_item_text": "Loss / alias: Loss is an early working name for Enoch.",
                        "patch_item_type": "entity_update",
                        "patch_update_type": "alias",
                        "patch_candidate_entities": ["Loss"],
                        "candidate_entities": ["Loss"],
                        "candidate_topics": ["entity"],
                        "knowledge_track": "lore",
                        "relevance_score": 0.92,
                    }
                ],
            )
            write_json(
                root / "config.json",
                {
                    "model_routing": {
                        "profiles": {"balanced_reasoning": {"provider": "gemini", "api_model": "gemini-2.5-flash"}},
                        "tasks": {
                            "stage_07_entity_resolution": {
                                "profile": "balanced_reasoning",
                                "enabled": True,
                                "max_evidence_per_call": 10,
                            }
                        },
                    },
                    "model_provider": {"provider": "gemini", "timeout_seconds": 60},
                },
            )

            with patch("pipeline.stage_07_entity_resolution.call_model_chat") as mock_model:
                mock_model.return_value = {
                    "alias_mappings": [
                        {
                            "alias_text": "Loss",
                            "canonical_name": "Enoch",
                            "target_entity_id": "",
                            "entity_type": "character",
                            "source_snippet_ids": ["s_loss_alias"],
                            "confidence": 0.94,
                            "rationale": "The patch note identifies Loss as an early working name for Enoch.",
                        }
                    ]
                }
                with self.assertRaisesRegex(RuntimeError, "conversation entity proposal"):
                    run_stage_07(
                        root / "snippets.jsonl",
                        root / "entity_seed.json",
                        root / "aliases.json",
                        root / "timelines.json",
                        root / "resolved_entities.json",
                        None,
                        root / "conversation_entity_proposals.json",
                        root / "conversation_entity_decisions.json",
                        root / "config.json",
                    )

            mock_model.assert_called_once()
            payload = json.loads((root / "conversation_entity_proposals.json").read_text(encoding="utf-8"))
            proposal = payload["proposals"][0]
            self.assertEqual(proposal["candidate_name"], "Loss")
            self.assertEqual(proposal["suggested_canonical_name"], "Enoch")
            self.assertEqual(proposal["proposed_entity_type"], "character")
            self.assertIn("llm_alias_resolution", proposal["proposal_kinds"])
            self.assertEqual(proposal["triage_status"], "review_required")
            self.assertEqual(proposal["review_priority"], "high")
            self.assertIn("alias/rename evidence", proposal["triage_reason"])

            rows = candidate_inventory_browser_rows(root / "conversation_entity_proposals.json")
            self.assertEqual(rows[0]["candidate_name"], "Enoch (alias: Loss)")
            self.assertEqual(rows[0]["raw_candidate_name"], "Loss")
            self.assertEqual(rows[0]["canonical_name"], "Enoch")

    def test_stage_07_decision_rerun_skips_prior_alias_grouping(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write_json(
                root / "entity_seed.json",
                {
                    "entities": [
                        {
                            "entity_seed_id": "seed_enoch",
                            "canonical_name": "Enoch",
                            "entity_type": "character",
                            "aliases": [],
                            "seed_status": "active",
                        }
                    ]
                },
            )
            write_jsonl(
                root / "snippets.jsonl",
                [
                    {
                        "snippet_id": "s_loss_alias",
                        "timestamp_start_utc": "2026-04-01T00:00:00Z",
                        "timestamp_end_utc": "2026-04-01T00:01:00Z",
                        "display_text_normalized": "Patch note item: Loss / alias: Loss is an early working name for Enoch.",
                        "patch_item_text": "Loss / alias: Loss is an early working name for Enoch.",
                        "patch_item_type": "entity_update",
                        "patch_update_type": "alias",
                        "patch_candidate_entities": ["Loss"],
                        "candidate_entities": ["Loss"],
                        "candidate_topics": ["entity"],
                        "knowledge_track": "lore",
                        "relevance_score": 0.92,
                    }
                ],
            )
            proposal_id = stable_id("conversation_entity_proposal", "loss")
            write_json(
                root / "conversation_entity_proposals.json",
                {
                    "proposals": [
                        {
                            "proposal_id": proposal_id,
                            "candidate_name": "Loss",
                            "normalized_name_key": "loss",
                            "proposed_entity_type": "character",
                            "suggested_canonical_name": "Enoch",
                            "alias_resolution_confidence": 0.94,
                            "proposal_kinds": ["llm_alias_resolution"],
                            "proposal_reason": "Model alias resolution suggests Loss -> Enoch.",
                            "review_status": "pending",
                        }
                    ]
                },
            )
            write_json(
                root / "conversation_entity_decisions.json",
                {
                    "decisions": [
                        {
                            "proposal_id": proposal_id,
                            "candidate_name": "Loss",
                            "canonical_name": "Enoch",
                            "decision": "approve",
                            "entity_type": "character",
                            "aliases": ["Loss"],
                            "reviewer": "human_reviewer",
                        }
                    ]
                },
            )
            write_json(
                root / "config.json",
                {
                    "model_routing": {
                        "profiles": {"balanced_reasoning": {"provider": "gemini", "api_model": "gemini-2.5-flash"}},
                        "tasks": {"stage_07_entity_resolution": {"profile": "balanced_reasoning", "enabled": True}},
                    },
                    "model_provider": {"provider": "gemini", "timeout_seconds": 60},
                },
            )

            with patch("pipeline.stage_07_entity_resolution.call_model_chat") as mock_model:
                mock_model.side_effect = AssertionError("alias grouping should not rerun after decisions")
                run_stage_07(
                    root / "snippets.jsonl",
                    root / "entity_seed.json",
                    root / "aliases.json",
                    root / "timelines.json",
                    root / "resolved_entities.json",
                    root / "memory.json",
                    root / "conversation_entity_proposals.json",
                    root / "conversation_entity_decisions.json",
                    root / "config.json",
                )

            mock_model.assert_not_called()
            payload = json.loads((root / "conversation_entity_proposals.json").read_text(encoding="utf-8"))
            self.assertEqual(payload["proposals"][0]["review_status"], "approved")
            self.assertEqual(payload["proposals"][0]["suggested_canonical_name"], "Enoch")
            aliases = json.loads((root / "aliases.json").read_text(encoding="utf-8"))["aliases"]
            self.assertTrue(any(alias["alias_text"] == "Loss" for alias in aliases))

    def test_stage_07_model_candidate_alias_resolution_handles_name_variants(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write_json(
                root / "entity_seed.json",
                {
                    "entities": [
                        {
                            "entity_seed_id": "seed_enoch",
                            "canonical_name": "Enoch",
                            "entity_type": "character",
                            "aliases": [],
                            "seed_status": "active",
                        }
                    ]
                },
            )
            write_jsonl(
                root / "snippets.jsonl",
                [
                    {
                        "snippet_id": "s_enoch_variant",
                        "timestamp_start_utc": "2026-04-01T00:00:00Z",
                        "timestamp_end_utc": "2026-04-01T00:01:00Z",
                        "display_text_normalized": "Professor Enoch Ersatzen, who designed the protocol, returns in the later arc.",
                        "patch_item_text": "Professor Enoch Ersatzen / role_change: returns in the later arc.",
                        "patch_item_type": "entity_update",
                        "patch_update_type": "role_change",
                        "patch_candidate_entities": ["Professor Enoch Ersatzen"],
                        "candidate_entities": ["Professor Enoch Ersatzen"],
                        "candidate_topics": ["entity"],
                        "knowledge_track": "lore",
                        "relevance_score": 0.86,
                    }
                ],
            )
            write_json(
                root / "config.json",
                {
                    "model_routing": {
                        "profiles": {"balanced_reasoning": {"provider": "gemini", "api_model": "gemini-2.5-flash"}},
                        "tasks": {
                            "stage_07_entity_resolution": {
                                "profile": "balanced_reasoning",
                                "enabled": True,
                                "max_evidence_per_call": 10,
                                "max_candidates_per_call": 10,
                            }
                        },
                    },
                    "model_provider": {"provider": "gemini", "timeout_seconds": 60},
                },
            )

            with patch("pipeline.stage_07_entity_resolution.call_model_chat") as mock_model:
                mock_model.return_value = {
                    "alias_mappings": [
                        {
                            "alias_text": "Professor Enoch Ersatzen",
                            "canonical_name": "Enoch",
                            "target_entity_id": "",
                            "entity_type": "character",
                            "source_snippet_ids": ["s_enoch_variant"],
                            "confidence": 0.91,
                            "rationale": "The candidate is a title/name variant of Enoch in character evidence.",
                        }
                    ]
                }
                with self.assertRaisesRegex(RuntimeError, "conversation entity proposal"):
                    run_stage_07(
                        root / "snippets.jsonl",
                        root / "entity_seed.json",
                        root / "aliases.json",
                        root / "timelines.json",
                        root / "resolved_entities.json",
                        None,
                        root / "conversation_entity_proposals.json",
                        root / "conversation_entity_decisions.json",
                        root / "config.json",
                    )

            self.assertEqual(mock_model.call_count, 1)
            payload = json.loads((root / "conversation_entity_proposals.json").read_text(encoding="utf-8"))
            proposal = payload["proposals"][0]
            self.assertEqual(proposal["candidate_name"], "Professor Enoch Ersatzen")
            self.assertEqual(proposal["suggested_canonical_name"], "Enoch")
            self.assertIn("llm_candidate_alias_resolution", proposal["proposal_kinds"])
            self.assertIn("alias/rename evidence", proposal["triage_reason"])

    def test_stage_07_model_candidate_alias_resolution_accepts_list_response(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write_json(
                root / "entity_seed.json",
                {
                    "entities": [
                        {
                            "entity_seed_id": "seed_enoch",
                            "canonical_name": "Enoch",
                            "entity_type": "character",
                            "aliases": [],
                            "seed_status": "active",
                        }
                    ]
                },
            )
            write_jsonl(
                root / "snippets.jsonl",
                [
                    {
                        "snippet_id": "s_enoch_variant",
                        "timestamp_start_utc": "2026-04-01T00:00:00Z",
                        "timestamp_end_utc": "2026-04-01T00:01:00Z",
                        "display_text_normalized": "Enoch Faust Ersatzen, who designed the protocol, returns.",
                        "patch_item_text": "Enoch Faust Ersatzen / role_change: returns.",
                        "patch_item_type": "entity_update",
                        "patch_update_type": "role_change",
                        "patch_candidate_entities": ["Enoch Faust Ersatzen"],
                        "candidate_entities": ["Enoch Faust Ersatzen"],
                        "candidate_topics": ["entity"],
                        "knowledge_track": "lore",
                        "relevance_score": 0.86,
                    }
                ],
            )
            write_json(
                root / "config.json",
                {
                    "model_routing": {
                        "tasks": {
                            "stage_07_entity_resolution": {
                                "enabled": True,
                                "max_candidates_per_call": 10,
                            }
                        }
                    },
                    "model_provider": {"provider": "gemini", "timeout_seconds": 60},
                },
            )

            with patch("pipeline.stage_07_entity_resolution.call_model_chat") as mock_model:
                mock_model.return_value = [
                    {
                        "alias_text": "Enoch Faust Ersatzen",
                        "canonical_name": "Enoch",
                        "target_entity_id": "",
                        "entity_type": "character",
                        "source_snippet_ids": ["s_enoch_variant"],
                        "confidence": 0.91,
                        "rationale": "Bare-list response is still a valid alias mapping list.",
                    }
                ]
                with self.assertRaisesRegex(RuntimeError, "conversation entity proposal"):
                    run_stage_07(
                        root / "snippets.jsonl",
                        root / "entity_seed.json",
                        root / "aliases.json",
                        root / "timelines.json",
                        root / "resolved_entities.json",
                        None,
                        root / "conversation_entity_proposals.json",
                        root / "conversation_entity_decisions.json",
                        root / "config.json",
                    )

            payload = json.loads((root / "conversation_entity_proposals.json").read_text(encoding="utf-8"))
            self.assertEqual(payload["triage_summary"]["alias_resolution_failures"], 0)
            self.assertEqual(payload["proposals"][0]["suggested_canonical_name"], "Enoch")

    def test_stage_07_model_candidate_alias_resolution_accepts_provider_list_wrapper(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write_json(
                root / "entity_seed.json",
                {
                    "entities": [
                        {
                            "entity_seed_id": "seed_enoch",
                            "canonical_name": "Enoch",
                            "entity_type": "character",
                            "aliases": [],
                            "seed_status": "active",
                        }
                    ]
                },
            )
            write_jsonl(
                root / "snippets.jsonl",
                [
                    {
                        "snippet_id": "s_enoch_variant",
                        "timestamp_start_utc": "2026-04-01T00:00:00Z",
                        "timestamp_end_utc": "2026-04-01T00:01:00Z",
                        "display_text_normalized": "Enoch Faust Ersatzen, who designed the protocol, returns.",
                        "patch_item_text": "Enoch Faust Ersatzen / role_change: returns.",
                        "patch_item_type": "entity_update",
                        "patch_update_type": "role_change",
                        "patch_candidate_entities": ["Enoch Faust Ersatzen"],
                        "candidate_entities": ["Enoch Faust Ersatzen"],
                        "candidate_topics": ["entity"],
                        "knowledge_track": "lore",
                        "relevance_score": 0.86,
                    }
                ],
            )
            write_json(root / "config.json", {"model_routing": {"tasks": {"stage_07_entity_resolution": {"enabled": True}}}})

            with patch("pipeline.stage_07_entity_resolution.call_model_chat") as mock_model:
                mock_model.return_value = {
                    "_json_root": [
                        {
                            "alias_text": "Enoch Faust Ersatzen",
                            "canonical_name": "Enoch",
                            "source_snippet_ids": ["s_enoch_variant"],
                            "confidence": 0.91,
                            "rationale": "Provider wrapped a bare list response.",
                        }
                    ]
                }
                with self.assertRaisesRegex(RuntimeError, "conversation entity proposal"):
                    run_stage_07(
                        root / "snippets.jsonl",
                        root / "entity_seed.json",
                        root / "aliases.json",
                        root / "timelines.json",
                        root / "resolved_entities.json",
                        None,
                        root / "conversation_entity_proposals.json",
                        root / "conversation_entity_decisions.json",
                        root / "config.json",
                    )

            payload = json.loads((root / "conversation_entity_proposals.json").read_text(encoding="utf-8"))
            self.assertEqual(payload["triage_summary"]["alias_resolution_failures"], 0)
            self.assertEqual(payload["proposals"][0]["suggested_canonical_name"], "Enoch")

    def test_stage_07_reconsiders_candidate_type_from_aggregated_usage(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write_json(root / "entity_seed.json", {"entities": []})
            write_jsonl(
                root / "snippets.jsonl",
                [
                    {
                        "snippet_id": "s1",
                        "timestamp_start_utc": "2026-04-01T00:00:00Z",
                        "timestamp_end_utc": "2026-04-01T00:01:00Z",
                        "display_text_normalized": "Null Crown is a recurring motif in the story.",
                        "candidate_entities": ["Null Crown"],
                        "candidate_topics": ["theme"],
                        "knowledge_track": "lore",
                        "relevance_score": 0.7,
                    },
                    {
                        "snippet_id": "s2",
                        "timestamp_start_utc": "2026-04-01T00:02:00Z",
                        "timestamp_end_utc": "2026-04-01T00:03:00Z",
                        "display_text_normalized": "Null Crown, who refuses the offer, returns later.",
                        "candidate_entities": ["Null Crown"],
                        "candidate_topics": ["theme"],
                        "knowledge_track": "lore",
                        "relevance_score": 0.8,
                    },
                    {
                        "snippet_id": "s3",
                        "timestamp_start_utc": "2026-04-01T00:04:00Z",
                        "timestamp_end_utc": "2026-04-01T00:05:00Z",
                        "display_text_normalized": "Null Crown says he remembers the previous route.",
                        "candidate_entities": ["Null Crown"],
                        "candidate_topics": ["theme"],
                        "knowledge_track": "lore",
                        "relevance_score": 0.8,
                    },
                    {
                        "snippet_id": "s4",
                        "timestamp_start_utc": "2026-04-01T00:06:00Z",
                        "timestamp_end_utc": "2026-04-01T00:07:00Z",
                        "display_text_normalized": "Null Crown says they cannot accept the old ending.",
                        "candidate_entities": ["Null Crown"],
                        "candidate_topics": ["theme"],
                        "knowledge_track": "lore",
                        "relevance_score": 0.8,
                    },
                    {
                        "snippet_id": "s5",
                        "timestamp_start_utc": "2026-04-01T00:08:00Z",
                        "timestamp_end_utc": "2026-04-01T00:09:00Z",
                        "display_text_normalized": "Null Crown, who appears again, changes the route.",
                        "candidate_entities": ["Null Crown"],
                        "candidate_topics": ["theme"],
                        "knowledge_track": "lore",
                        "relevance_score": 0.8,
                    },
                ],
            )

            with self.assertRaisesRegex(RuntimeError, "conversation entity proposal"):
                run_stage_07(
                    root / "snippets.jsonl",
                    root / "entity_seed.json",
                    root / "aliases.json",
                    root / "timelines.json",
                    root / "resolved_entities.json",
                    None,
                    root / "conversation_entity_proposals.json",
                    root / "conversation_entity_decisions.json",
                )

            proposal = json.loads((root / "conversation_entity_proposals.json").read_text(encoding="utf-8"))["proposals"][0]
            self.assertEqual(proposal["initial_proposed_entity_type"], "theme")
            self.assertEqual(proposal["proposed_entity_type"], "character")
            self.assertTrue(proposal["type_reconsidered"])
            self.assertIn("theme", {item["entity_type"] for item in proposal["type_conflicts"]})
            self.assertIn("reconsidered", proposal["type_review_notes"])

    def test_stage_07_recent_specific_character_evidence_reaches_review_with_four_mentions(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write_json(root / "entity_seed.json", {"entities": []})
            rows = []
            for idx in range(4):
                rows.append(
                    {
                        "snippet_id": f"s_old_general_{idx}",
                        "timestamp_start_utc": f"2025-01-23T14:{idx:02d}:00Z",
                        "timestamp_end_utc": f"2025-01-23T14:{idx:02d}:30Z",
                        "display_text_normalized": "Old General, who appears in an early sketch, might be involved.",
                        "patch_item_text": "Old General / role_change: Old General might be involved.",
                        "patch_item_type": "entity_update",
                        "patch_update_type": "role_change",
                        "patch_candidate_entities": ["Old General"],
                        "candidate_entities": ["Old General"],
                        "candidate_topics": ["entity"],
                        "knowledge_track": "lore",
                        "relevance_score": 0.7,
                    }
                )
                rows.append(
                    {
                        "snippet_id": f"s_halayudtha_{idx}",
                        "timestamp_start_utc": f"2026-04-23T14:{idx:02d}:00Z",
                        "timestamp_end_utc": f"2026-04-23T14:{idx:02d}:30Z",
                        "display_text_normalized": "Halayudtha, who is Ramasinta's general, starts the revolution.",
                        "patch_item_text": "Halayudtha / role_change: Halayudtha is Ramasinta's general and starts the revolution.",
                        "patch_item_type": "entity_update",
                        "patch_update_type": "role_change",
                        "patch_candidate_entities": ["Halayudtha"],
                        "candidate_entities": ["Halayudtha"],
                        "candidate_topics": ["entity"],
                        "knowledge_track": "lore",
                        "relevance_score": 0.9,
                    }
                )
            write_jsonl(root / "snippets.jsonl", rows)

            with self.assertRaisesRegex(RuntimeError, "conversation entity proposal"):
                run_stage_07(
                    root / "snippets.jsonl",
                    root / "entity_seed.json",
                    root / "aliases.json",
                    root / "timelines.json",
                    root / "resolved_entities.json",
                    None,
                    root / "conversation_entity_proposals.json",
                    root / "conversation_entity_decisions.json",
                )

            payload = json.loads((root / "conversation_entity_proposals.json").read_text(encoding="utf-8"))
            proposals_by_name = {item["candidate_name"]: item for item in payload["proposals"]}
            inventory_by_name = {item["candidate_name"]: item for item in payload["candidate_inventory"]}
            self.assertIn("Halayudtha", proposals_by_name)
            self.assertGreaterEqual(proposals_by_name["Halayudtha"]["recency_adjusted_evidence_count"], 5)
            self.assertIn("recentness boost", proposals_by_name["Halayudtha"]["triage_reason"])
            self.assertIn("Old General", inventory_by_name)
            self.assertEqual(inventory_by_name["Old General"]["recency_evidence_multiplier"], 1.0)

    def test_stage_07_approved_conversation_entity_promotes_to_resolved_and_groups(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write_json(root / "entity_seed.json", {"entities": []})
            write_jsonl(
                root / "snippets.jsonl",
                [
                    {
                        "snippet_id": "s1",
                        "timestamp_start_utc": "2026-04-01T00:00:00Z",
                        "timestamp_end_utc": "2026-04-01T00:01:00Z",
                        "display_text_normalized": "The Glass Orchard is a possible early route concept.",
                        "candidate_entities": ["Glass Orchard"],
                        "candidate_topics": ["quest"],
                        "knowledge_track": "lore",
                        "relevance_score": 0.82,
                    }
                ],
            )
            write_json(
                root / "conversation_entity_decisions.json",
                {
                    "decisions": [
                        {
                            "proposal_id": "",
                            "candidate_name": "Glass Orchard",
                            "decision": "approve",
                            "canonical_name": "The Glass Orchard",
                            "entity_type": "quest",
                            "reviewer": "r",
                            "rationale": "real early route concept",
                            "timestamp_utc": "2026-04-01T00:02:00Z",
                        }
                    ]
                },
            )

            run_stage_07(
                root / "snippets.jsonl",
                root / "entity_seed.json",
                root / "aliases.json",
                root / "timelines.json",
                root / "resolved_entities.json",
                None,
                root / "conversation_entity_proposals.json",
                root / "conversation_entity_decisions.json",
            )
            run_stage_08(
                root / "snippets.jsonl",
                root / "resolved_entities.json",
                root / "lore_clusters.json",
                root / "meta_clusters.json",
                None,
                None,
            )

            payload = json.loads((root / "resolved_entities.json").read_text(encoding="utf-8"))
            clusters = json.loads((root / "lore_clusters.json").read_text(encoding="utf-8"))["clusters"]
            self.assertEqual([entity["canonical_name"] for entity in payload["resolved_entities"]], ["The Glass Orchard"])
            self.assertEqual(payload["resolved_entities"][0]["resolution_status"], "conversation_candidate_approved")
            self.assertEqual(clusters[0]["cluster_key"], "The Glass Orchard")

    def test_stage_07_persists_conversation_entity_decisions_to_memory(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write_json(root / "entity_seed.json", {"entities": []})
            write_json(
                root / "memory.json",
                {
                    "version": 1,
                    "accepted_claims": [],
                    "rejected_claims": [],
                    "approved_aliases": [],
                    "entity_merges": [],
                    "approved_cards": [],
                    "author_directives": [],
                    "style_corrections": [],
                    "updated_at_utc": "2026-05-16T00:00:00Z",
                },
            )
            write_jsonl(
                root / "snippets.jsonl",
                [
                    {
                        "snippet_id": "s1",
                        "timestamp_start_utc": "2026-04-01T00:00:00Z",
                        "timestamp_end_utc": "2026-04-01T00:01:00Z",
                        "display_text_normalized": "The Glass Orchard is a possible early route concept.",
                        "candidate_entities": ["Glass Orchard"],
                        "candidate_topics": ["quest"],
                        "knowledge_track": "lore",
                        "relevance_score": 0.82,
                    },
                    {
                        "snippet_id": "s2",
                        "timestamp_start_utc": "2026-04-01T00:02:00Z",
                        "timestamp_end_utc": "2026-04-01T00:03:00Z",
                        "display_text_normalized": "Design is just generic production chatter.",
                        "candidate_entities": ["Design"],
                        "candidate_topics": ["production"],
                        "knowledge_track": "meta",
                        "relevance_score": 0.7,
                    },
                ],
            )
            write_json(
                root / "conversation_entity_decisions.json",
                {
                    "decisions": [
                        {
                            "proposal_id": "",
                            "candidate_name": "Glass Orchard",
                            "decision": "approve",
                            "canonical_name": "The Glass Orchard",
                            "entity_type": "quest",
                            "reviewer": "r",
                            "rationale": "real early route concept",
                            "timestamp_utc": "2026-04-01T00:02:00Z",
                        }
                    ]
                },
            )

            run_stage_07(
                root / "snippets.jsonl",
                root / "entity_seed.json",
                root / "aliases.json",
                root / "timelines.json",
                root / "resolved_entities.json",
                root / "memory.json",
                root / "conversation_entity_proposals.json",
                root / "conversation_entity_decisions.json",
            )

            memory = json.loads((root / "memory.json").read_text(encoding="utf-8"))
            self.assertEqual(memory["approved_conversation_entities"][0]["canonical_name"], "The Glass Orchard")

            write_json(root / "conversation_entity_decisions.json", {"decisions": []})
            run_stage_07(
                root / "snippets.jsonl",
                root / "entity_seed.json",
                root / "aliases_2.json",
                root / "timelines_2.json",
                root / "resolved_entities_2.json",
                root / "memory.json",
                root / "conversation_entity_proposals_2.json",
                root / "conversation_entity_decisions.json",
            )
            payload = json.loads((root / "resolved_entities_2.json").read_text(encoding="utf-8"))
            proposals = json.loads((root / "conversation_entity_proposals_2.json").read_text(encoding="utf-8"))["proposals"]
            self.assertEqual([entity["canonical_name"] for entity in payload["resolved_entities"]], ["The Glass Orchard"])
            self.assertEqual(proposals, [])

    def test_stage_07_persists_rejected_conversation_entity_and_suppresses_future_proposal(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write_json(root / "entity_seed.json", {"entities": []})
            write_json(
                root / "memory.json",
                {
                    "version": 1,
                    "accepted_claims": [],
                    "rejected_claims": [],
                    "approved_aliases": [],
                    "entity_merges": [],
                    "approved_cards": [],
                    "author_directives": [],
                    "style_corrections": [],
                    "updated_at_utc": "2026-05-16T00:00:00Z",
                },
            )
            write_jsonl(
                root / "snippets.jsonl",
                [
                    {
                        "snippet_id": "s1",
                        "timestamp_start_utc": "2026-04-01T00:00:00Z",
                        "timestamp_end_utc": "2026-04-01T00:01:00Z",
                        "display_text_normalized": "Moodboard Alpha is just a discarded label.",
                        "candidate_entities": ["Moodboard Alpha"],
                        "candidate_topics": ["production"],
                        "knowledge_track": "meta",
                        "relevance_score": 0.82,
                    }
                ],
            )
            write_json(
                root / "conversation_entity_decisions.json",
                {
                    "decisions": [
                        {
                            "proposal_id": "",
                            "candidate_name": "Moodboard Alpha",
                            "decision": "reject",
                            "reviewer": "r",
                            "rationale": "not an entity",
                            "timestamp_utc": "2026-04-01T00:02:00Z",
                        }
                    ]
                },
            )

            run_stage_07(
                root / "snippets.jsonl",
                root / "entity_seed.json",
                root / "aliases.json",
                root / "timelines.json",
                root / "resolved_entities.json",
                root / "memory.json",
                root / "conversation_entity_proposals.json",
                root / "conversation_entity_decisions.json",
            )
            memory = json.loads((root / "memory.json").read_text(encoding="utf-8"))
            self.assertEqual(memory["rejected_conversation_entities"][0]["candidate_name"], "Moodboard Alpha")

            write_json(root / "conversation_entity_decisions.json", {"decisions": []})
            run_stage_07(
                root / "snippets.jsonl",
                root / "entity_seed.json",
                root / "aliases_2.json",
                root / "timelines_2.json",
                root / "resolved_entities_2.json",
                root / "memory.json",
                root / "conversation_entity_proposals_2.json",
                root / "conversation_entity_decisions.json",
            )
            proposals = json.loads((root / "conversation_entity_proposals_2.json").read_text(encoding="utf-8"))["proposals"]
            self.assertEqual(proposals, [])

    def test_stage_08_uses_entity_anchors_instead_of_first_word_clusters(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write_json(
                root / "resolved_entities.json",
                {
                    "resolved_entities": [
                        {
                            "entity_id": "entity_oyuun",
                            "card_id": "card_oyuun",
                            "canonical_name": "OYUUN",
                            "entity_type": "character",
                            "aliases": [],
                            "resolution_status": "resolved",
                        }
                    ]
                },
            )
            write_jsonl(
                root / "snippets.jsonl",
                [
                    {
                        "snippet_id": "s1",
                        "knowledge_track": "lore",
                        "display_text_normalized": "and this is general Theriac chatter without an entity anchor",
                        "candidate_entities": [],
                        "candidate_topics": [],
                    },
                    {
                        "snippet_id": "s2",
                        "knowledge_track": "lore",
                        "display_text_normalized": "Oyuun can switch out her prosthetic arm.",
                        "candidate_entities": ["OYUUN"],
                        "candidate_topics": [],
                    },
                ],
            )

            run_stage_08(
                root / "snippets.jsonl",
                root / "resolved_entities.json",
                root / "lore_clusters.json",
                root / "meta_clusters.json",
            )

            keys = {cluster["cluster_key"] for cluster in json.loads((root / "lore_clusters.json").read_text(encoding="utf-8"))["clusters"]}
            self.assertIn("OYUUN", keys)
            self.assertIn("unmapped", keys)
            self.assertNotIn("and", keys)

    def test_stage_09_prompt_preserves_external_references_as_inspiration_claims(self) -> None:
        prompt = build_claim_extraction_prompt(
            {"canonical_name": "Golok", "entity_type": "character"},
            {"cluster_id": "cluster_golok", "cluster_key": "Golok"},
            [
                {
                    "snippet_id": "s1",
                    "display_text_normalized": "Golok is more akin to Adam Smasher in ethos.",
                }
            ],
            {},
        )

        self.assertIn('claim_type "inspiration"', prompt)
        self.assertIn("should not become card subjects", prompt)

    def test_stage_09_extracts_atomic_claims_without_raw_append(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write_json(
                root / "resolved_entities.json",
                {
                    "resolved_entities": [
                        {
                            "entity_id": "entity_hectr",
                            "card_id": "card_hectr",
                            "canonical_name": "HECTR",
                            "entity_type": "character",
                            "aliases": [],
                            "resolution_status": "resolved",
                        }
                    ]
                },
            )
            write_json(root / "lore_clusters.json", {"clusters": [{"cluster_id": "c1", "cluster_key": "HECTR", "snippet_ids": ["s1", "s2"], "thematic_tags": []}], "thematic_memory": {}})
            write_json(root / "meta_clusters.json", {"clusters": []})
            write_json(root / "alias.json", {"aliases": []})
            write_jsonl(
                root / "snippets.jsonl",
                [
                    {
                        "snippet_id": "s1",
                        "timestamp_start_utc": "2026-04-09T05:57:53Z",
                        "timestamp_end_utc": "2026-04-09T05:58:03Z",
                        "display_text_normalized": "so his face is going to be in basically every Krypteia AI",
                        "candidate_entities": ["HECTR"],
                        "relevance_score": 0.8,
                    },
                    {
                        "snippet_id": "s2",
                        "timestamp_start_utc": "2026-04-09T05:59:53Z",
                        "timestamp_end_utc": "2026-04-09T06:00:03Z",
                        "display_text_normalized": "unrelated HECTR mention without the extracted fact",
                        "candidate_entities": ["HECTR"],
                        "relevance_score": 0.8,
                    },
                ],
            )
            write_json(root / "memory.json", {"version": 1, "accepted_claims": [], "rejected_claims": [], "approved_aliases": [], "entity_merges": [], "approved_cards": [], "author_directives": [], "style_corrections": [], "updated_at_utc": "2026-05-16T00:00:00Z"})

            with patch("pipeline.stage_09_claim_drafting.call_model_chat", return_value={"claims": [{"claim_text": "HECTR is a template ancestor for Krypteia AI systems.", "claim_type": "relationship", "source_snippet_ids": ["s1"], "confidence": 0.82, "contradiction_notes": ""}]}):
                run_stage_09(
                    root / "resolved_entities.json",
                    root / "lore_clusters.json",
                    root / "meta_clusters.json",
                    root / "alias.json",
                    root / "snippets.jsonl",
                    root / "drafts",
                    None,
                    root / "memory.json",
                )

            claims = json.loads((root / "drafts" / "claim_drafts.json").read_text(encoding="utf-8"))["claims"]
            self.assertEqual(len(claims), 1)
            self.assertEqual(claims[0]["claim_text"], "HECTR is a template ancestor for Krypteia AI systems.")
            self.assertNotIn("proposed_summary_append", claims[0])
            self.assertEqual(claims[0]["source_snippet_ids"], ["s1"])

    def test_stage_09_uses_batch_mode_for_claim_extraction_when_enabled(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write_json(
                root / "resolved_entities.json",
                {
                    "resolved_entities": [
                        {
                            "entity_id": "entity_hectr",
                            "card_id": "card_hectr",
                            "canonical_name": "HECTR",
                            "entity_type": "character",
                            "aliases": [],
                            "resolution_status": "resolved",
                        }
                    ]
                },
            )
            write_json(root / "lore_clusters.json", {"clusters": [{"cluster_id": "c1", "cluster_key": "HECTR", "snippet_ids": ["s1"], "thematic_tags": []}], "thematic_memory": {}})
            write_json(root / "meta_clusters.json", {"clusters": []})
            write_json(root / "alias.json", {"aliases": []})
            write_jsonl(
                root / "snippets.jsonl",
                [
                    {
                        "snippet_id": "s1",
                        "timestamp_start_utc": "2026-04-09T05:57:53Z",
                        "timestamp_end_utc": "2026-04-09T05:58:03Z",
                        "display_text_normalized": "HECTR is a template ancestor for Krypteia AI systems.",
                        "candidate_entities": ["HECTR"],
                        "relevance_score": 0.8,
                    }
                ],
            )
            write_json(root / "memory.json", {"version": 1, "accepted_claims": [], "rejected_claims": [], "approved_aliases": [], "entity_merges": [], "approved_cards": [], "author_directives": [], "style_corrections": [], "updated_at_utc": "2026-05-16T00:00:00Z"})
            write_json(
                root / "config.json",
                {
                    "model_routing": {
                        "profiles": {"cheap": {"provider": "gemini", "api_model": "gemini-2.5-flash-lite"}},
                        "tasks": {"stage_09_claim_drafting": {"profile": "cheap", "batch_enabled": True, "batch_max_requests": 10}},
                    }
                },
            )

            def fake_batch(_config: dict, _task_name: str, requests: list[dict]) -> dict:
                self.assertEqual(len(requests), 1)
                key = requests[0]["key"]
                return {
                    key: {
                        "payload": {
                            "claims": [
                                {
                                    "claim_text": "HECTR is a template ancestor for Krypteia AI systems.",
                                    "claim_type": "relationship",
                                    "source_snippet_ids": ["s1"],
                                    "confidence": 0.82,
                                    "contradiction_notes": "",
                                }
                            ]
                        },
                        "error": "",
                    }
                }

            with patch("pipeline.stage_09_claim_drafting.call_gemini_batch_json", side_effect=fake_batch) as batch:
                with patch("pipeline.stage_09_claim_drafting.call_model_chat", side_effect=AssertionError("sync model should not be used")):
                    run_stage_09(
                        root / "resolved_entities.json",
                        root / "lore_clusters.json",
                        root / "meta_clusters.json",
                        root / "alias.json",
                        root / "snippets.jsonl",
                        root / "drafts",
                        root / "config.json",
                        root / "memory.json",
                    )

            claims = json.loads((root / "drafts" / "claim_drafts.json").read_text(encoding="utf-8"))["claims"]
            self.assertEqual(batch.call_count, 1)
            self.assertEqual(len(claims), 1)
            self.assertEqual(claims[0]["claim_text"], "HECTR is a template ancestor for Krypteia AI systems.")

    def test_stage_09_logs_failed_claim_extraction_and_continues(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write_json(
                root / "resolved_entities.json",
                {
                    "resolved_entities": [
                        {
                            "entity_id": "entity_hectr",
                            "card_id": "card_hectr",
                            "canonical_name": "HECTR",
                            "entity_type": "character",
                            "aliases": [],
                            "resolution_status": "resolved",
                        }
                    ]
                },
            )
            write_json(
                root / "lore_clusters.json",
                {
                    "clusters": [
                        {"cluster_id": "c1", "cluster_key": "HECTR", "snippet_ids": ["s1"], "thematic_tags": []},
                        {"cluster_id": "c2", "cluster_key": "HECTR", "snippet_ids": ["s2"], "thematic_tags": []},
                    ],
                    "thematic_memory": {},
                },
            )
            write_json(root / "meta_clusters.json", {"clusters": []})
            write_json(root / "alias.json", {"aliases": []})
            write_jsonl(
                root / "snippets.jsonl",
                [
                    {
                        "snippet_id": "s1",
                        "timestamp_start_utc": "2026-04-09T05:57:53Z",
                        "display_text_normalized": "HECTR noise that makes the provider answer with the wrong shape.",
                        "candidate_entities": ["HECTR"],
                        "relevance_score": 0.8,
                    },
                    {
                        "snippet_id": "s2",
                        "timestamp_start_utc": "2026-04-09T05:59:53Z",
                        "display_text_normalized": "HECTR is related to Krypteia AI systems.",
                        "candidate_entities": ["HECTR"],
                        "relevance_score": 0.8,
                    },
                ],
            )
            write_json(root / "memory.json", {"version": 1, "accepted_claims": [], "rejected_claims": [], "approved_aliases": [], "entity_merges": [], "approved_cards": [], "author_directives": [], "style_corrections": [], "updated_at_utc": "2026-05-16T00:00:00Z"})
            write_json(root / "config.json", {"model_provider": {"claim_extraction_validation_retries": 0}})

            with patch(
                "pipeline.stage_09_claim_drafting.call_model_chat",
                side_effect=[
                    {"not_claims": []},
                    {"claims": [{"claim_text": "HECTR is related to Krypteia AI systems.", "claim_type": "relationship", "confidence": 0.82, "contradiction_notes": ""}]},
                ],
            ):
                run_stage_09(
                    root / "resolved_entities.json",
                    root / "lore_clusters.json",
                    root / "meta_clusters.json",
                    root / "alias.json",
                    root / "snippets.jsonl",
                    root / "drafts",
                    root / "config.json",
                    root / "memory.json",
                )

            claims = json.loads((root / "drafts" / "claim_drafts.json").read_text(encoding="utf-8"))["claims"]
            failures = json.loads((root / "drafts" / "claim_extraction_failures.json").read_text(encoding="utf-8"))["failures"]
            self.assertEqual(len(claims), 1)
            self.assertEqual(claims[0]["source_snippet_ids"], ["s2"])
            self.assertEqual(len(failures), 1)
            self.assertEqual(failures[0]["reason"], "model_claim_extraction_failed")

    def test_stage_11_synthesizes_drafts_and_requires_card_approval_for_canon(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write_json(
                root / "resolved_entities.json",
                {
                    "resolved_entities": [
                        {
                            "entity_id": "entity_hectr",
                            "card_id": "card_hectr",
                            "canonical_name": "HECTR",
                            "entity_type": "character",
                            "aliases": [],
                            "resolution_status": "resolved",
                        }
                    ]
                },
            )
            claim = {
                "claim_id": "claim1",
                "target_entity_id": "entity_hectr",
                "target_card_id": "card_hectr",
                "target_entity_name": "HECTR",
                "knowledge_track": "lore",
                "claim_text": "HECTR is a template ancestor for Krypteia AI systems.",
                "claim_type": "relationship",
                "source_snippet_ids": ["s1"],
                "confidence": 0.82,
                "status": "draft",
                "contradiction_notes": "",
                "created_at_utc": "2026-05-16T00:00:00Z",
            }
            write_json(root / "claim_drafts.json", {"claims": [claim]})
            write_json(root / "claim_decisions.json", {"decisions": [{"claim_id": "claim1", "decision": "accept", "reviewer": "r", "rationale": "ok"}]})
            write_json(root / "card_decisions.json", {"decisions": []})
            write_json(root / "directives.json", {"directives": []})
            write_json(root / "memory.json", {"version": 1, "accepted_claims": [], "rejected_claims": [], "approved_aliases": [], "entity_merges": [], "approved_cards": [], "author_directives": [], "style_corrections": [], "updated_at_utc": "2026-05-16T00:00:00Z"})
            model_card = {
                "summary": "HECTR is a Krypteia AI ancestor synthesized from reviewed claims.",
                "sections": {"background": "Reviewed claim background.", "role_in_story": "It shapes Krypteia systems.", "relationships": "Linked to Krypteia AI.", "timeline": "", "open_questions": ""},
                "relationships": [],
                "timeline": [],
                "support_map": {"summary": ["claim1"], "background": ["claim1"], "role_in_story": ["claim1"], "relationships": ["claim1"], "timeline": [], "open_questions": []},
            }

            with patch("pipeline.stage_11_card_synthesis.call_model_chat", return_value=model_card):
                run_stage_11(
                    root / "resolved_entities.json",
                    root / "claim_drafts.json",
                    root / "claim_decisions.json",
                    root / "card_decisions.json",
                    root / "directives.json",
                    root / "memory.json",
                    root / "card_drafts.json",
                    root / "canonical_cards.json",
                    root / "merge_log.jsonl",
                    None,
                )

            draft_cards = json.loads((root / "card_drafts.json").read_text(encoding="utf-8"))["cards"]
            canonical_cards = json.loads((root / "canonical_cards.json").read_text(encoding="utf-8"))["cards"]
            self.assertEqual(len(draft_cards), 1)
            self.assertEqual(draft_cards[0]["status"], "draft")
            self.assertEqual(draft_cards[0]["source_evidence"], ["s1"])
            self.assertEqual(canonical_cards, [])

            write_json(root / "card_decisions.json", {"decisions": [{"card_id": "card_hectr", "decision": "approve", "reviewer": "r", "rationale": "canon"}]})
            with patch("pipeline.stage_11_card_synthesis.call_model_chat", return_value=model_card):
                run_stage_11(
                    root / "resolved_entities.json",
                    root / "claim_drafts.json",
                    root / "claim_decisions.json",
                    root / "card_decisions.json",
                    root / "directives.json",
                    root / "memory.json",
                    root / "card_drafts.json",
                    root / "canonical_cards.json",
                    root / "merge_log.jsonl",
                    None,
                )

            canonical_cards = json.loads((root / "canonical_cards.json").read_text(encoding="utf-8"))["cards"]
            self.assertEqual(len(canonical_cards), 1)
            self.assertEqual(canonical_cards[0]["status"], "canonical")
            self.assertNotIn("lore_bible_seed", canonical_cards[0]["source_evidence"])
            self.assertEqual(canonical_cards[0]["details"]["support_map"]["summary"], ["claim1"])

    def test_stage_11_checkpoints_partial_card_drafts_before_interrupt(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write_json(
                root / "resolved_entities.json",
                {
                    "resolved_entities": [
                        {
                            "entity_id": "entity_hectr",
                            "card_id": "card_hectr",
                            "canonical_name": "HECTR",
                            "entity_type": "character",
                            "aliases": [],
                            "resolution_status": "resolved",
                        },
                        {
                            "entity_id": "entity_ruinr",
                            "card_id": "card_ruinr",
                            "canonical_name": "RUINR",
                            "entity_type": "character",
                            "aliases": [],
                            "resolution_status": "resolved",
                        },
                    ]
                },
            )
            claims = [
                {
                    "claim_id": "claim_hectr",
                    "target_entity_id": "entity_hectr",
                    "target_card_id": "card_hectr",
                    "target_entity_name": "HECTR",
                    "knowledge_track": "lore",
                    "claim_text": "HECTR is a Krypteia AI ancestor.",
                    "claim_type": "relationship",
                    "source_snippet_ids": ["s1"],
                    "confidence": 0.82,
                    "status": "draft",
                    "contradiction_notes": "",
                    "created_at_utc": "2026-05-16T00:00:00Z",
                },
                {
                    "claim_id": "claim_ruinr",
                    "target_entity_id": "entity_ruinr",
                    "target_card_id": "card_ruinr",
                    "target_entity_name": "RUINR",
                    "knowledge_track": "lore",
                    "claim_text": "RUINR is tied to ACHILLES.",
                    "claim_type": "relationship",
                    "source_snippet_ids": ["s2"],
                    "confidence": 0.82,
                    "status": "draft",
                    "contradiction_notes": "",
                    "created_at_utc": "2026-05-16T00:00:00Z",
                },
            ]
            write_json(root / "claim_drafts.json", {"claims": claims})
            write_json(
                root / "claim_decisions.json",
                {
                    "decisions": [
                        {"claim_id": "claim_hectr", "decision": "accept", "reviewer": "r", "rationale": "ok"},
                        {"claim_id": "claim_ruinr", "decision": "accept", "reviewer": "r", "rationale": "ok"},
                    ]
                },
            )
            write_json(root / "card_decisions.json", {"decisions": []})
            write_json(root / "directives.json", {"directives": []})
            write_json(root / "memory.json", {"version": 1, "accepted_claims": [], "rejected_claims": [], "approved_aliases": [], "entity_merges": [], "approved_cards": [], "author_directives": [], "style_corrections": [], "updated_at_utc": "2026-05-16T00:00:00Z"})
            model_card = {
                "summary": "HECTR is a Krypteia AI ancestor synthesized from reviewed claims.",
                "sections": {
                    "background": "Reviewed claim background.",
                    "role_in_story": "It shapes Krypteia systems.",
                    "relationships": "Linked to Krypteia AI.",
                    "timeline": "",
                    "inspirations": "",
                    "open_questions": "",
                },
                "relationships": [],
                "timeline": [],
                "support_map": {
                    "summary": ["claim_hectr"],
                    "background": ["claim_hectr"],
                    "role_in_story": ["claim_hectr"],
                    "relationships": ["claim_hectr"],
                    "timeline": [],
                    "inspirations": [],
                    "open_questions": [],
                },
            }

            with patch("pipeline.stage_11_card_synthesis.synthesize_card_with_model", side_effect=[model_card, ValueError("interrupted")]):
                with self.assertRaises(ValueError):
                    run_stage_11(
                        root / "resolved_entities.json",
                        root / "claim_drafts.json",
                        root / "claim_decisions.json",
                        root / "card_decisions.json",
                        root / "directives.json",
                        root / "memory.json",
                        root / "card_drafts.json",
                        root / "canonical_cards.json",
                        root / "merge_log.jsonl",
                        None,
                    )

            self.assertFalse((root / "card_drafts.json").exists())
            checkpoint = json.loads((root / "card_synthesis_checkpoint.json").read_text(encoding="utf-8"))
            partial = json.loads((root / "card_drafts.partial.json").read_text(encoding="utf-8"))
            self.assertEqual(checkpoint["status"], "running")
            self.assertEqual(checkpoint["processed_count"], 1)
            self.assertEqual(checkpoint["total_count"], 2)
            self.assertEqual(checkpoint["draft_card_count"], 1)
            self.assertEqual(checkpoint["cards"][0]["canonical_name"], "HECTR")
            self.assertEqual(partial["cards"][0]["card_id"], "card_hectr")

    def test_stage_11_card_synthesis_prompt_requests_full_fandom_wiki_style_entry(self) -> None:
        prompt = build_card_synthesis_prompt(
            {"canonical_name": "HECTR", "entity_type": "character"},
            [{"claim_id": "claim1", "claim_text": "HECTR matters to Krypteia.", "claim_type": "relationship"}],
            {},
        )

        self.assertIn("comparable in shape and density to a strong fandom wiki page", prompt)
        self.assertIn("Write polished article prose", prompt)
        self.assertIn("compact lead paragraph", prompt)
        self.assertIn("Do not paste accepted claim_text verbatim", prompt)
        self.assertIn("Word target plan", prompt)
        self.assertIn("section_word_targets", prompt)

    def test_stage_11_detects_long_verbatim_claim_reuse_in_card_prose(self) -> None:
        claim_text = (
            "HECTR is the supervising intelligence that anchors the Krypteia lab sequence "
            "and frames the player's first contact with the system."
        )
        reused = find_verbatim_claim_reuse(
            [{"claim_id": "claim_verbatim", "claim_text": claim_text}],
            {
                "summary": claim_text,
                "sections": {
                    "background": "",
                    "role_in_story": "",
                    "relationships": "",
                    "timeline": "",
                    "inspirations": "",
                    "open_questions": "",
                },
            },
        )
        paraphrased = find_verbatim_claim_reuse(
            [{"claim_id": "claim_verbatim", "claim_text": claim_text}],
            {
                "summary": (
                    "HECTR frames the player's first encounter with Krypteia, serving as the intelligence "
                    "that gives the lab sequence its initial point of contact."
                ),
                "sections": {},
            },
        )

        self.assertEqual(reused, ["claim_verbatim"])
        self.assertEqual(paraphrased, [])

    def test_stage_11_accepts_author_claims_for_card_refactoring(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write_json(
                root / "resolved_entities.json",
                {
                    "resolved_entities": [
                        {
                            "entity_id": "entity_tunnel_wars",
                            "card_id": "card_tunnel_wars",
                            "canonical_name": "Tunnel Wars",
                            "entity_type": "event",
                            "aliases": [],
                            "resolution_status": "resolved",
                        },
                        {
                            "entity_id": "entity_mycelium_wars",
                            "card_id": "card_mycelium_wars",
                            "canonical_name": "Mycelium Wars",
                            "entity_type": "event",
                            "aliases": [],
                            "resolution_status": "resolved",
                        },
                    ]
                },
            )
            write_json(root / "claim_drafts.json", {"claims": []})
            write_json(root / "claim_decisions.json", {"decisions": []})
            write_json(
                root / "author_claims.json",
                {
                    "claims": [
                        {
                            "claim_id": "author_claim_tunnel_mycelium",
                            "target_entity_name": "Tunnel Wars",
                            "claim_text": "The tunnel wars are part of the Mycelium Wars.",
                            "claim_type": "relationship",
                            "created_at_utc": "2026-05-18T12:00:00Z",
                        }
                    ]
                },
            )
            write_json(root / "card_decisions.json", {"decisions": []})
            write_json(root / "directives.json", {"directives": []})
            write_json(root / "memory.json", {"version": 1, "accepted_claims": [], "rejected_claims": [], "approved_aliases": [], "entity_merges": [], "approved_cards": [], "author_directives": [], "style_corrections": [], "updated_at_utc": "2026-05-16T00:00:00Z"})
            model_card = {
                "summary": "The Tunnel Wars are a conflict within the wider Mycelium Wars.",
                "sections": {"background": "They are treated as part of the Mycelium Wars.", "role_in_story": "", "relationships": "The Tunnel Wars sit inside the Mycelium Wars.", "timeline": "", "inspirations": "", "open_questions": ""},
                "relationships": [{"target_entity_name": "Mycelium Wars", "relation_type": "part_of", "note": "The Tunnel Wars are part of the Mycelium Wars.", "support_claim_ids": ["author_claim_tunnel_mycelium"]}],
                "timeline": [],
                "wiki_links": [{"target_card_id": "card_mycelium_wars", "target_entity_name": "Mycelium Wars", "relation_type": "part_of", "section": "relationships", "support_claim_ids": ["author_claim_tunnel_mycelium"]}],
                "support_map": {"summary": ["author_claim_tunnel_mycelium"], "background": ["author_claim_tunnel_mycelium"], "role_in_story": [], "relationships": ["author_claim_tunnel_mycelium"], "timeline": [], "inspirations": [], "open_questions": []},
            }
            prompts: list[str] = []

            def fake_model(prompt: str, **_kwargs: Any) -> dict[str, Any]:
                prompts.append(prompt)
                return model_card

            with patch("pipeline.stage_11_card_synthesis.call_model_chat", side_effect=fake_model):
                run_stage_11(
                    root / "resolved_entities.json",
                    root / "claim_drafts.json",
                    root / "claim_decisions.json",
                    root / "card_decisions.json",
                    root / "directives.json",
                    root / "memory.json",
                    root / "card_drafts.json",
                    root / "canonical_cards.json",
                    root / "merge_log.jsonl",
                    None,
                )

            draft_card = json.loads((root / "card_drafts.json").read_text(encoding="utf-8"))["cards"][0]
            memory = json.loads((root / "memory.json").read_text(encoding="utf-8"))
            self.assertIn("The tunnel wars are part of the Mycelium Wars.", prompts[0])
            self.assertIn("Author-supplied manual claims are authoritative", prompts[0])
            self.assertEqual(draft_card["details"]["accepted_claim_ids"], ["author_claim_tunnel_mycelium"])
            self.assertEqual(draft_card["details"]["support_map"]["summary"], ["author_claim_tunnel_mycelium"])
            self.assertEqual(memory["accepted_claims"][0]["claim_id"], "author_claim_tunnel_mycelium")

    def test_cardbase_agent_identity_merge_transaction_and_undo(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            paths = ArtifactPaths(root)
            entities = [
                {
                    "entity_id": "entity_pandoras_mother",
                    "card_id": "card_pandoras_mother",
                    "canonical_name": "Pandora's mother",
                    "entity_type": "character",
                    "aliases": [],
                    "resolution_status": "resolved",
                },
                {
                    "entity_id": "entity_izanami",
                    "card_id": "card_izanami",
                    "canonical_name": "Izanami",
                    "entity_type": "character",
                    "aliases": [],
                    "resolution_status": "resolved",
                },
                {
                    "entity_id": "entity_pandora",
                    "card_id": "card_pandora",
                    "canonical_name": "Pandora",
                    "entity_type": "character",
                    "aliases": [],
                    "resolution_status": "resolved",
                },
            ]
            accepted_claims = [
                {
                    "claim_id": "claim_pandoras_mother_threshold",
                    "target_entity_id": "entity_pandoras_mother",
                    "target_card_id": "card_pandoras_mother",
                    "target_entity_name": "Pandora's mother",
                    "knowledge_track": "lore",
                    "claim_text": "Pandora's mother guards the threshold to Yomi.",
                    "claim_type": "background",
                    "source_snippet_ids": ["snippet_mother"],
                    "confidence": 0.92,
                    "status": "accepted",
                    "created_at_utc": "2026-05-20T00:00:00Z",
                }
            ]
            memory_path = root / "canon" / "review_memory.json"
            pre_memory = {
                "version": 1,
                "accepted_claims": [],
                "rejected_claims": [],
                "approved_aliases": [],
                "entity_merges": [],
                "approved_cards": [],
                "author_directives": [],
                "style_corrections": [],
                "updated_at_utc": "2026-05-20T00:00:00Z",
            }
            pre_cards = {
                "cards": [
                    {
                        "card_id": "card_pandoras_mother",
                        "canonical_name": "Pandora's mother",
                        "entity_type": "character",
                        "status": "canonical",
                        "summary": "Pandora's mother guards the threshold.",
                        "details": {"entity_id": "entity_pandoras_mother", "accepted_claim_ids": ["claim_pandoras_mother_threshold"], "wiki_links": []},
                        "relationships": [],
                    },
                    {
                        "card_id": "card_izanami",
                        "canonical_name": "Izanami",
                        "entity_type": "character",
                        "status": "canonical",
                        "summary": "Izanami is a surviving card.",
                        "details": {"entity_id": "entity_izanami", "accepted_claim_ids": [], "wiki_links": []},
                        "relationships": [],
                    },
                    {
                        "card_id": "card_pandora",
                        "canonical_name": "Pandora",
                        "entity_type": "character",
                        "status": "canonical",
                        "summary": "Pandora has a mother.",
                        "details": {
                            "entity_id": "entity_pandora",
                            "accepted_claim_ids": [],
                            "wiki_links": [
                                {
                                    "target_entity_id": "entity_pandoras_mother",
                                    "target_card_id": "card_pandoras_mother",
                                    "target_entity_name": "Pandora's mother",
                                    "relation_type": "mother",
                                    "section": "relationships",
                                }
                            ],
                        },
                        "relationships": [
                            {
                                "target_entity_id": "entity_pandoras_mother",
                                "target_card_id": "card_pandoras_mother",
                                "target_entity_name": "Pandora's mother",
                                "relation_type": "mother",
                                "note": "Pandora's mother is referenced before the merge.",
                            }
                        ],
                    },
                ]
            }
            write_json(memory_path, pre_memory)
            write_json(paths.author_claims, {"claims": []})
            write_json(paths.card_drafts, {"cards": []})
            write_json(paths.canonical_cards, pre_cards)
            write_jsonl(
                paths.card_edit_requests,
                [
                    {
                        "request_id": "req_pandoras_mother_is_izanami",
                        "instruction_text": "Pandora's mother is Izanami",
                        "status": "pending",
                        "created_at_utc": "2026-05-20T00:01:00Z",
                    }
                ],
            )
            pre_request_text = paths.card_edit_requests.read_text(encoding="utf-8")

            tool_calls = [
                {"tool_name": "search_entities", "arguments": {"query": "Pandora's mother"}, "rationale": "Find the source entity."},
                {"tool_name": "search_entities", "arguments": {"query": "Izanami"}, "rationale": "Find the surviving entity."},
                {"tool_name": "get_card", "arguments": {"entity_id": "entity_pandoras_mother"}, "rationale": "Inspect the source card."},
                {"tool_name": "get_card", "arguments": {"entity_id": "entity_izanami"}, "rationale": "Inspect the target card."},
                {"tool_name": "get_claims", "arguments": {"entity_id": "entity_pandoras_mother"}, "rationale": "Collect source claims."},
                {"tool_name": "get_relationships", "arguments": {"entity_id": "entity_pandoras_mother"}, "rationale": "Collect source relationships."},
                {
                    "tool_name": "apply_identity_merge",
                    "arguments": {
                        "source_entity_id": "entity_pandoras_mother",
                        "target_entity_id": "entity_izanami",
                        "claim_text": "Pandora's mother is Izanami",
                        "rationale": "The author directly states these refer to the same identity.",
                        "confidence": 1.0,
                    },
                    "rationale": "Apply the author-directed identity merge.",
                },
                {
                    "tool_name": "rewrite_references",
                    "arguments": {"source_entity_id": "entity_pandoras_mother", "target_entity_id": "entity_izanami"},
                    "rationale": "Point surviving references at Izanami.",
                },
                {"tool_name": "synthesize_affected_cards", "arguments": {"entity_ids": ["entity_izanami"]}, "rationale": "Defer card prose to Stage 11."},
                {
                    "tool_name": "check_consistency",
                    "arguments": {"source_entity_id": "entity_pandoras_mother", "target_entity_id": "entity_izanami"},
                    "rationale": "Verify the merge.",
                },
                {
                    "tool_name": "finish",
                    "arguments": {"final_response": "Merged Pandora's mother into Izanami."},
                    "final_response": "Merged Pandora's mother into Izanami.",
                    "rationale": "The cardbase is consistent.",
                },
            ]

            with patch("pipeline.cardbase_agent.call_model_chat", side_effect=tool_calls) as model:
                result = run_pending_card_agent_requests(
                    review_dir=paths.stage11,
                    entities=entities,
                    accepted_claims=accepted_claims,
                    review_memory_path=memory_path,
                    author_claims_path=paths.author_claims,
                    card_drafts_path=paths.card_drafts,
                    canonical_cards_path=paths.canonical_cards,
                    config={},
                )

            self.assertEqual(result["processed_count"], 1)
            self.assertEqual(len(result["completed"]), 1)
            self.assertEqual(model.call_count, len(tool_calls))
            memory = json.loads(memory_path.read_text(encoding="utf-8"))
            self.assertEqual(memory["entity_merges"][0]["source_entity_name"], "Pandora's mother")
            self.assertEqual(memory["entity_merges"][0]["target_entity_name"], "Izanami")
            self.assertEqual(memory["approved_aliases"][0]["alias_text"], "Pandora's mother")
            redirects = json.loads((paths.stage11 / "card_redirects.json").read_text(encoding="utf-8"))["redirects"]
            self.assertEqual(redirects[0]["source_entity_name"], "Pandora's mother")
            self.assertEqual(redirects[0]["target_entity_name"], "Izanami")
            author_claims = json.loads(paths.author_claims.read_text(encoding="utf-8"))["claims"]
            self.assertEqual(author_claims[0]["target_entity_name"], "Izanami")
            self.assertEqual(author_claims[0]["claim_text"], "Pandora's mother is Izanami")

            cards_after = json.loads(paths.canonical_cards.read_text(encoding="utf-8"))["cards"]
            self.assertNotIn("card_pandoras_mother", {card["card_id"] for card in cards_after})
            pandora_card = next(card for card in cards_after if card["card_id"] == "card_pandora")
            self.assertEqual(pandora_card["relationships"][0]["target_entity_name"], "Izanami")
            self.assertEqual(pandora_card["relationships"][0]["target_card_id"], "card_izanami")
            self.assertEqual(pandora_card["details"]["wiki_links"][0]["target_entity_name"], "Izanami")
            self.assertEqual(pandora_card["details"]["wiki_links"][0]["target_card_id"], "card_izanami")

            transactions = load_card_agent_transactions(paths.stage11)
            self.assertEqual(len(transactions), 1)
            transaction = transactions[0]
            self.assertEqual(transaction["status"], "completed")
            self.assertEqual([step["tool_name"] for step in transaction["steps"]], [call["tool_name"] for call in tool_calls])
            self.assertIn("entity_izanami", transaction["affected"]["entities"])
            self.assertIn("entity_pandoras_mother", transaction["affected"]["entities"])
            self.assertIn("card_izanami", transaction["affected"]["cards"])
            self.assertIn("claim_pandoras_mother_threshold", transaction["affected"]["claims"])
            changed_paths = {Path(item["path"]).name for item in transaction["write_set"] if item.get("changed")}
            self.assertIn("review_memory.json", changed_paths)
            self.assertIn("author_claims.json", changed_paths)
            self.assertIn("canonical_cards.json", changed_paths)
            self.assertIn("card_redirects.json", changed_paths)
            self.assertIn("card_edit_requests.jsonl", changed_paths)

            activity = card_agent_activity_payload(root)
            activity_transaction = next(item for item in activity["transactions"] if item["transaction_id"] == transaction["transaction_id"])
            self.assertIn("change_summary", activity_transaction)
            self.assertTrue(all("text" not in item.get("before", {}) for item in activity_transaction["write_set"]))
            memory_artifact = next(
                item
                for item in activity_transaction["change_summary"]["artifacts"]
                if item["display_path"].endswith("canon\\review_memory.json") or item["display_path"].endswith("canon/review_memory.json")
            )
            changed_collections = {collection["name"]: collection for collection in memory_artifact["collections"]}
            self.assertEqual(changed_collections["entity_merges"]["added"][0]["label"], "Pandora's mother -> Izanami")
            self.assertEqual(changed_collections["approved_aliases"]["added"][0]["details"]["alias_text"], "Pandora's mother")
            change_lines = [line["sentence"] for line in activity_transaction["change_summary"]["lines"]]
            self.assertIn("Pandora's mother merged into Izanami.", change_lines)
            self.assertIn("Pandora's mother added as an alias for Izanami.", change_lines)
            self.assertIn("Claim added to Izanami: Pandora's mother is Izanami.", change_lines)

            undo = undo_card_agent_transaction(paths.stage11, transaction["transaction_id"], reviewer="test", rationale="Undo test.")
            self.assertEqual(undo["status"], "completed_reversal")
            self.assertEqual(undo["reverses_transaction_id"], transaction["transaction_id"])
            self.assertEqual(json.loads(memory_path.read_text(encoding="utf-8")), pre_memory)
            self.assertEqual(json.loads(paths.author_claims.read_text(encoding="utf-8")), {"claims": []})
            self.assertEqual(json.loads(paths.canonical_cards.read_text(encoding="utf-8")), pre_cards)
            self.assertEqual(paths.card_edit_requests.read_text(encoding="utf-8"), pre_request_text)
            self.assertFalse((paths.stage11 / "card_redirects.json").exists())
            self.assertFalse((paths.stage11 / "card_architecture_applied.json").exists())
            self.assertEqual(len(load_card_agent_transactions(paths.stage11)), 2)

    def test_cardbase_agent_runs_on_demand_without_stage_11_resume(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            paths = ArtifactPaths(root)
            write_json(
                paths.resolved_entities,
                {
                    "resolved_entities": [
                        {
                            "entity_id": "entity_pandoras_mother",
                            "card_id": "card_pandoras_mother",
                            "canonical_name": "Pandora's mother",
                            "entity_type": "character",
                            "aliases": [],
                            "resolution_status": "resolved",
                        },
                        {
                            "entity_id": "entity_izanami",
                            "card_id": "card_izanami",
                            "canonical_name": "Izanami",
                            "entity_type": "character",
                            "aliases": [],
                            "resolution_status": "resolved",
                        },
                    ]
                },
            )
            claim = {
                "claim_id": "claim_pandoras_mother_threshold",
                "target_entity_id": "entity_pandoras_mother",
                "target_card_id": "card_pandoras_mother",
                "target_entity_name": "Pandora's mother",
                "knowledge_track": "lore",
                "claim_text": "Pandora's mother guards the threshold to Yomi.",
                "claim_type": "background",
                "source_snippet_ids": ["snippet_mother"],
                "confidence": 0.92,
                "status": "draft",
            }
            write_json(paths.claim_drafts, {"claims": [claim]})
            write_json(paths.claim_review_decisions, {"decisions": [{"claim_id": claim["claim_id"], "decision": "accept"}]})
            write_json(paths.author_claims, {"claims": []})
            write_json(paths.card_drafts, {"cards": []})
            write_json(
                paths.canonical_cards,
                {
                    "cards": [
                        {
                            "card_id": "card_pandoras_mother",
                            "canonical_name": "Pandora's mother",
                            "details": {"entity_id": "entity_pandoras_mother", "wiki_links": []},
                            "relationships": [],
                        },
                        {
                            "card_id": "card_izanami",
                            "canonical_name": "Izanami",
                            "details": {"entity_id": "entity_izanami", "wiki_links": []},
                            "relationships": [],
                        },
                    ]
                },
            )
            memory_path = root / "canon" / "review_memory.json"
            write_json(
                memory_path,
                {
                    "version": 1,
                    "accepted_claims": [],
                    "rejected_claims": [],
                    "approved_aliases": [],
                    "entity_merges": [],
                    "approved_cards": [],
                    "author_directives": [],
                    "style_corrections": [],
                    "updated_at_utc": "2026-05-20T00:00:00Z",
                },
            )
            tool_calls = [
                {"tool_name": "search_entities", "arguments": {"query": "Pandora's mother"}, "rationale": "Find source."},
                {"tool_name": "search_entities", "arguments": {"query": "Izanami"}, "rationale": "Find target."},
                {"tool_name": "get_claims", "arguments": {"entity_id": "entity_pandoras_mother"}, "rationale": "Read source claims."},
                {
                    "tool_name": "apply_identity_merge",
                    "arguments": {
                        "source_entity_id": "entity_pandoras_mother",
                        "target_entity_id": "entity_izanami",
                        "claim_text": "Pandora's mother is Izanami",
                        "rationale": "Direct author identity assertion.",
                        "confidence": 1.0,
                    },
                    "rationale": "Apply the merge now.",
                },
                {"tool_name": "rewrite_references", "arguments": {"source_entity_id": "entity_pandoras_mother", "target_entity_id": "entity_izanami"}, "rationale": "Remove stale cards."},
                {"tool_name": "check_consistency", "arguments": {"source_entity_id": "entity_pandoras_mother", "target_entity_id": "entity_izanami"}, "rationale": "Verify."},
                {"tool_name": "finish", "arguments": {"final_response": "Done."}, "final_response": "Done.", "rationale": "Verified."},
            ]

            with patch("pipeline.cardbase_agent.call_model_chat", side_effect=tool_calls) as model:
                result = run_card_agent_request(
                    artifacts_root=root,
                    instruction_text="Pandora's mother is Izanami",
                    requester="test",
                    review_memory_path=memory_path,
                    config_path=root / "missing_config.json",
                )

            self.assertEqual(result["status"], "completed")
            self.assertEqual(model.call_count, len(tool_calls))
            requests = read_jsonl(paths.card_edit_requests)
            self.assertEqual(len(requests), 1)
            self.assertEqual(requests[0]["status"], "applied")
            self.assertEqual(requests[0]["source"], "on_demand")
            memory = json.loads(memory_path.read_text(encoding="utf-8"))
            self.assertEqual(memory["entity_merges"][0]["source_entity_name"], "Pandora's mother")
            self.assertEqual(memory["entity_merges"][0]["target_entity_name"], "Izanami")
            self.assertEqual(memory["approved_aliases"][0]["alias_text"], "Pandora's mother")
            author_claims = json.loads(paths.author_claims.read_text(encoding="utf-8"))["claims"]
            self.assertEqual(author_claims[0]["target_entity_name"], "Izanami")
            cards_after = json.loads(paths.canonical_cards.read_text(encoding="utf-8"))["cards"]
            self.assertEqual([card["canonical_name"] for card in cards_after], ["Izanami"])
            transactions = load_card_agent_transactions(paths.stage11)
            self.assertEqual(len(transactions), 1)
            self.assertEqual(transactions[0]["request_text"], "Pandora's mother is Izanami")
            self.assertEqual([step["tool_name"] for step in transactions[0]["steps"]], [call["tool_name"] for call in tool_calls])

    def test_cardbase_agent_canonical_rename_updates_targets_and_undo(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            paths = ArtifactPaths(root)
            old_name = "Hierarchical Embedded Cognitive Template for Recursive Architectures"
            old_card_id = card_id_for_entity(old_name)
            new_card_id = card_id_for_entity("HECTR")
            entities = [
                {
                    "entity_id": "entity_hectr",
                    "card_id": old_card_id,
                    "canonical_name": old_name,
                    "entity_type": "character",
                    "aliases": ["HECTR"],
                    "resolution_status": "resolved",
                },
                {
                    "entity_id": "entity_ruinr",
                    "card_id": "card_ruinr",
                    "canonical_name": "RUINR",
                    "entity_type": "character",
                    "aliases": [],
                    "resolution_status": "resolved",
                },
            ]
            claim = {
                "claim_id": "claim_hectr_role",
                "target_entity_id": "entity_hectr",
                "target_card_id": old_card_id,
                "target_entity_name": old_name,
                "knowledge_track": "lore",
                "claim_text": "HECTR is the repurposed war AI of the Krypteia.",
                "claim_type": "background",
                "source_snippet_ids": ["snippet_hectr"],
                "confidence": 0.95,
                "status": "draft",
            }
            write_json(paths.resolved_entities, {"resolved_entities": entities})
            write_json(
                paths.identity_merged_entities_preview,
                {
                    "source_entity_count": 2,
                    "merged_entity_count": 2,
                    "merge_record_count": 0,
                    "target_map": {},
                    "sources_by_target": {},
                    "entities": entities,
                },
            )
            write_json(paths.claim_drafts, {"claims": [claim]})
            write_json(paths.claim_review_decisions, {"decisions": [{"claim_id": claim["claim_id"], "decision": "accept"}]})
            write_json(
                paths.author_claims,
                {
                    "claims": [
                        {
                            "claim_id": "author_claim_old_hectr",
                            "target_entity_id": "entity_hectr",
                            "target_card_id": old_card_id,
                            "target_entity_name": old_name,
                            "knowledge_track": "lore",
                            "claim_text": "The canonical name is being shortened.",
                            "claim_type": "lore_fact",
                            "status": "accepted",
                        }
                    ]
                },
            )
            write_json(paths.card_drafts, {"cards": []})
            write_json(
                paths.canonical_cards,
                {
                    "cards": [
                        {
                            "card_id": old_card_id,
                            "canonical_name": old_name,
                            "entity_type": "character",
                            "aliases": ["HECTR"],
                            "status": "canonical",
                            "summary": "HECTR has a long generated title.",
                            "details": {"entity_id": "entity_hectr", "accepted_claim_ids": ["claim_hectr_role"], "wiki_links": []},
                            "relationships": [],
                        },
                        {
                            "card_id": "card_ruinr",
                            "canonical_name": "RUINR",
                            "entity_type": "character",
                            "aliases": [],
                            "status": "canonical",
                            "summary": "RUINR relies on HECTR.",
                            "details": {
                                "entity_id": "entity_ruinr",
                                "wiki_links": [
                                    {
                                        "target_entity_id": "entity_hectr",
                                        "target_card_id": old_card_id,
                                        "target_entity_name": old_name,
                                        "relation_type": "depends_on",
                                        "section": "relationships",
                                    }
                                ],
                            },
                            "relationships": [
                                {
                                    "target_entity_id": "entity_hectr",
                                    "target_card_id": old_card_id,
                                    "target_entity_name": old_name,
                                    "relation_type": "depends_on",
                                    "note": "RUINR is connected to the old HECTR title.",
                                }
                            ],
                        },
                    ]
                },
            )
            memory_path = root / "canon" / "review_memory.json"
            write_json(
                memory_path,
                {
                    "version": 1,
                    "accepted_claims": [],
                    "rejected_claims": [],
                    "approved_aliases": [],
                    "entity_merges": [],
                    "approved_cards": [],
                    "author_directives": [],
                    "style_corrections": [],
                    "updated_at_utc": "2026-05-20T00:00:00Z",
                },
            )
            pre_memory = json.loads(memory_path.read_text(encoding="utf-8"))
            pre_entities = json.loads(paths.resolved_entities.read_text(encoding="utf-8"))
            pre_claims = json.loads(paths.claim_drafts.read_text(encoding="utf-8"))
            pre_author_claims = json.loads(paths.author_claims.read_text(encoding="utf-8"))
            pre_cards = json.loads(paths.canonical_cards.read_text(encoding="utf-8"))
            tool_calls = [
                {"tool_name": "search_entities", "arguments": {"query": "HECTR"}, "rationale": "Find the entity currently known by the HECTR alias."},
                {"tool_name": "get_entity", "arguments": {"entity_id": "entity_hectr"}, "rationale": "Confirm the current canonical name."},
                {"tool_name": "get_claims", "arguments": {"entity_id": "entity_hectr"}, "rationale": "Inspect claims before renaming."},
                {"tool_name": "get_relationships", "arguments": {"entity_id": "entity_hectr"}, "rationale": "Inspect inbound references."},
                {
                    "tool_name": "apply_canonical_rename",
                    "arguments": {
                        "entity_id": "entity_hectr",
                        "canonical_name": "HECTR",
                        "claim_text": "HECTR is the canonical name.",
                        "rationale": "The author requests HECTR as the canonical card name.",
                        "confidence": 1.0,
                    },
                    "rationale": "Apply the canonical rename.",
                },
                {"tool_name": "check_consistency", "arguments": {"renamed_entity_id": "entity_hectr", "canonical_name": "HECTR"}, "rationale": "Verify the rename."},
                {"tool_name": "finish", "arguments": {"final_response": "HECTR is now canonical."}, "final_response": "HECTR is now canonical.", "rationale": "Rename verified."},
            ]

            with patch("pipeline.cardbase_agent.call_model_chat", side_effect=tool_calls):
                result = run_card_agent_request(
                    artifacts_root=root,
                    instruction_text="Can you make HECTR the canonical name?",
                    requester="test",
                    review_memory_path=memory_path,
                    config_path=root / "missing_config.json",
                )

            self.assertEqual(result["status"], "completed")
            entities_after = json.loads(paths.resolved_entities.read_text(encoding="utf-8"))["resolved_entities"]
            hectr = next(entity for entity in entities_after if entity["entity_id"] == "entity_hectr")
            self.assertEqual(hectr["canonical_name"], "HECTR")
            self.assertEqual(hectr["card_id"], new_card_id)
            self.assertIn(old_name, hectr["aliases"])
            claims_after = json.loads(paths.claim_drafts.read_text(encoding="utf-8"))["claims"]
            self.assertEqual(claims_after[0]["target_entity_name"], "HECTR")
            self.assertEqual(claims_after[0]["target_card_id"], new_card_id)
            author_claims_after = json.loads(paths.author_claims.read_text(encoding="utf-8"))["claims"]
            self.assertTrue(all(claim["target_entity_name"] == "HECTR" for claim in author_claims_after))
            cards_after = json.loads(paths.canonical_cards.read_text(encoding="utf-8"))["cards"]
            self.assertIn(new_card_id, {card["card_id"] for card in cards_after})
            self.assertNotIn(old_card_id, {card["card_id"] for card in cards_after})
            ruinr = next(card for card in cards_after if card["card_id"] == "card_ruinr")
            self.assertEqual(ruinr["relationships"][0]["target_entity_name"], "HECTR")
            self.assertEqual(ruinr["relationships"][0]["target_card_id"], new_card_id)
            self.assertEqual(ruinr["details"]["wiki_links"][0]["target_entity_name"], "HECTR")
            self.assertEqual(ruinr["details"]["wiki_links"][0]["target_card_id"], new_card_id)
            memory = json.loads(memory_path.read_text(encoding="utf-8"))
            self.assertEqual(memory["canonical_renames"][0]["old_canonical_name"], old_name)
            self.assertEqual(memory["canonical_renames"][0]["canonical_name"], "HECTR")
            self.assertEqual(memory["approved_aliases"][0]["alias_text"], old_name)
            redirects = json.loads((paths.stage11 / "card_redirects.json").read_text(encoding="utf-8"))["redirects"]
            self.assertEqual(redirects[0]["source_entity_name"], old_name)
            self.assertEqual(redirects[0]["target_entity_name"], "HECTR")
            transaction = load_card_agent_transactions(paths.stage11)[0]
            self.assertEqual(transaction["status"], "completed")
            self.assertIn("apply_canonical_rename", [step["tool_name"] for step in transaction["steps"]])
            self.assertTrue(transaction["validation"]["ok"])

            undo = undo_card_agent_transaction(paths.stage11, transaction["transaction_id"], reviewer="test", rationale="Undo rename.")
            self.assertEqual(undo["status"], "completed_reversal")
            self.assertEqual(json.loads(memory_path.read_text(encoding="utf-8")), pre_memory)
            self.assertEqual(json.loads(paths.resolved_entities.read_text(encoding="utf-8")), pre_entities)
            self.assertEqual(json.loads(paths.claim_drafts.read_text(encoding="utf-8")), pre_claims)
            self.assertEqual(json.loads(paths.author_claims.read_text(encoding="utf-8")), pre_author_claims)
            self.assertEqual(json.loads(paths.canonical_cards.read_text(encoding="utf-8")), pre_cards)
            self.assertEqual(json.loads((paths.stage11 / "card_redirects.json").read_text(encoding="utf-8"))["redirects"], [])

    def test_cardbase_agent_removal_rejects_claims_and_removes_card(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            paths = ArtifactPaths(root)
            write_json(
                paths.resolved_entities,
                {
                    "resolved_entities": [
                        {
                            "entity_id": "entity_morgan_blackhand",
                            "card_id": "card_morgan_blackhand",
                            "canonical_name": "Morgan Blackhand",
                            "entity_type": "character",
                            "aliases": [],
                            "resolution_status": "resolved",
                        },
                        {
                            "entity_id": "entity_theriac",
                            "card_id": "card_theriac",
                            "canonical_name": "Theriac",
                            "entity_type": "organization",
                            "aliases": [],
                            "resolution_status": "resolved",
                        },
                    ]
                },
            )
            claims = [
                {
                    "claim_id": "claim_morgan_role",
                    "target_entity_id": "entity_morgan_blackhand",
                    "target_card_id": "card_morgan_blackhand",
                    "target_entity_name": "Morgan Blackhand",
                    "knowledge_track": "lore",
                    "claim_text": "Morgan Blackhand is involved with THERIAC operations.",
                    "claim_type": "role",
                    "source_snippet_ids": ["snippet_morgan"],
                    "confidence": 0.81,
                    "status": "draft",
                },
                {
                    "claim_id": "claim_theriac",
                    "target_entity_id": "entity_theriac",
                    "target_card_id": "card_theriac",
                    "target_entity_name": "Theriac",
                    "knowledge_track": "lore",
                    "claim_text": "Theriac is the main project.",
                    "claim_type": "background",
                    "source_snippet_ids": ["snippet_theriac"],
                    "confidence": 0.9,
                    "status": "draft",
                },
            ]
            write_json(paths.claim_drafts, {"claims": claims})
            write_json(
                paths.claim_review_decisions,
                {
                    "decisions": [
                        {"claim_id": "claim_morgan_role", "decision": "accept", "reviewer": "r", "rationale": "old"},
                        {"claim_id": "claim_theriac", "decision": "accept", "reviewer": "r", "rationale": "ok"},
                    ]
                },
            )
            write_json(paths.author_claims, {"claims": []})
            write_json(paths.card_drafts, {"cards": []})
            write_json(
                paths.canonical_cards,
                {
                    "cards": [
                        {
                            "card_id": "card_morgan_blackhand",
                            "canonical_name": "Morgan Blackhand",
                            "details": {"entity_id": "entity_morgan_blackhand", "wiki_links": []},
                            "relationships": [],
                        },
                        {
                            "card_id": "card_theriac",
                            "canonical_name": "Theriac",
                            "details": {
                                "entity_id": "entity_theriac",
                                "wiki_links": [
                                    {
                                        "target_entity_id": "entity_morgan_blackhand",
                                        "target_card_id": "card_morgan_blackhand",
                                        "target_entity_name": "Morgan Blackhand",
                                        "relation_type": "mentions",
                                    }
                                ],
                            },
                            "relationships": [
                                {
                                    "target_entity_id": "entity_morgan_blackhand",
                                    "target_card_id": "card_morgan_blackhand",
                                    "target_entity_name": "Morgan Blackhand",
                                    "relation_type": "mentions",
                                }
                            ],
                        },
                    ]
                },
            )
            memory_path = root / "canon" / "review_memory.json"
            write_json(
                memory_path,
                {
                    "version": 1,
                    "accepted_claims": [claims[0]],
                    "rejected_claims": [],
                    "approved_aliases": [],
                    "entity_merges": [],
                    "approved_cards": [],
                    "author_directives": [],
                    "style_corrections": [],
                    "updated_at_utc": "2026-05-21T00:00:00Z",
                },
            )
            tool_calls = [
                {"tool_name": "get_entity", "arguments": {"name": "Morgan Blackhand"}, "rationale": "Confirm the entity."},
                {"tool_name": "get_claims", "arguments": {"entity_id": "entity_morgan_blackhand"}, "rationale": "Inspect claims before removal."},
                {
                    "tool_name": "remove_entity_from_cardbase",
                    "arguments": {
                        "entity_id": "entity_morgan_blackhand",
                        "rationale": "Author says Morgan Blackhand is not a THERIAC character and should be removed.",
                        "confidence": 1.0,
                    },
                    "rationale": "Remove the non-THERIAC character and reject its claims.",
                },
                {"tool_name": "check_consistency", "arguments": {"removed_entity_id": "entity_morgan_blackhand"}, "rationale": "Verify removal."},
                {"tool_name": "finish", "arguments": {"final_response": "Removed Morgan Blackhand."}, "final_response": "Removed Morgan Blackhand.", "rationale": "Removal verified."},
            ]

            with patch("pipeline.cardbase_agent.call_model_chat", side_effect=tool_calls):
                result = run_card_agent_request(
                    artifacts_root=root,
                    instruction_text="Morgan Blackhand is not a Theriac character and should be removed.",
                    requester="test",
                    review_memory_path=memory_path,
                    config_path=root / "missing_config.json",
                )

            self.assertEqual(result["status"], "completed")
            decisions = json.loads(paths.claim_review_decisions.read_text(encoding="utf-8"))["decisions"]
            self.assertEqual(decisions[-1]["claim_id"], "claim_morgan_role")
            self.assertEqual(decisions[-1]["decision"], "reject")
            self.assertEqual(decisions[-1]["reviewer"], "cardbase_agent")
            self.assertNotEqual(decisions[-1]["claim_id"], "claim_theriac")
            cards_after = json.loads(paths.canonical_cards.read_text(encoding="utf-8"))["cards"]
            self.assertNotIn("card_morgan_blackhand", {card["card_id"] for card in cards_after})
            resolved_after = json.loads(paths.resolved_entities.read_text(encoding="utf-8"))["resolved_entities"]
            self.assertNotIn("Morgan Blackhand", [entity["canonical_name"] for entity in resolved_after])
            theriac_card = next(card for card in cards_after if card["card_id"] == "card_theriac")
            self.assertEqual(theriac_card["relationships"], [])
            self.assertEqual(theriac_card["details"]["wiki_links"], [])
            self.assertEqual(json.loads(paths.author_claims.read_text(encoding="utf-8"))["claims"], [])
            memory = json.loads(memory_path.read_text(encoding="utf-8"))
            self.assertEqual(memory["accepted_claims"], [])
            self.assertEqual(memory["rejected_claims"][0]["claim_id"], "claim_morgan_role")
            self.assertEqual(memory["removed_entities"][0]["canonical_name"], "Morgan Blackhand")
            transaction = load_card_agent_transactions(paths.stage11)[0]
            self.assertEqual(transaction["status"], "completed")
            self.assertIn("remove_entity_from_cardbase", [step["tool_name"] for step in transaction["steps"]])
            progress = card_agent_progress_payload(root, max_lines=20)
            self.assertGreaterEqual(progress["total_scanned"], len(tool_calls) + 2)
            self.assertIn("completed", progress["latest_line"])
            self.assertTrue(any("remove_entity_from_cardbase" in line for line in progress["lines"]))
            changed_paths = {Path(item["path"]).name for item in transaction["write_set"] if item.get("changed")}
            self.assertIn("claim_review_decisions.json", changed_paths)
            self.assertIn("canonical_cards.json", changed_paths)
            self.assertIn("resolved_entities.json", changed_paths)

    def test_desktop_merged_entity_inventory_hides_removed_entities_from_memory(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            repo = Path(tmp)
            active = repo / "artifacts" / "runs" / "run_removed"
            paths = ArtifactPaths(active)
            write_json(
                paths.identity_merged_entities_preview,
                {
                    "generated_at_utc": "2026-05-21T00:00:00Z",
                    "mode": "test",
                    "source_entity_count": 2,
                    "merged_entity_count": 2,
                    "merge_record_count": 0,
                    "target_map": {},
                    "sources_by_target": {},
                    "entities": [
                        {
                            "entity_id": "entity_morgan_blackhand",
                            "card_id": "card_morgan_blackhand",
                            "canonical_name": "Morgan Blackhand",
                            "entity_type": "character",
                            "aliases": [],
                        },
                        {
                            "entity_id": "entity_theriac",
                            "card_id": "card_theriac",
                            "canonical_name": "Theriac",
                            "entity_type": "organization",
                            "aliases": [],
                        },
                    ],
                },
            )
            write_json(paths.claim_drafts, {"claims": []})
            write_json(paths.claim_review_decisions, {"decisions": []})
            write_json(paths.author_claims, {"claims": []})
            write_json(
                repo / "canon" / "review_memory.json",
                {
                    "removed_entities": [
                        {
                            "entity_id": "entity_morgan_blackhand",
                            "card_id": "card_morgan_blackhand",
                            "canonical_name": "Morgan Blackhand",
                        }
                    ]
                },
            )

            inventory = handle_request(
                {
                    "repo_root": str(repo),
                    "command": "entity_inventory",
                    "payload": {"artifacts_root": str(active)},
                }
            )

            merged_names = [row["candidate_name"] for row in inventory["merged_rows"]]
            self.assertNotIn("Morgan Blackhand", merged_names)
            self.assertIn("Theriac", merged_names)

    def test_tauri_app_config_saves_bootstrap_doc_and_openrouter_key(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            repo = Path(tmp)
            config_path = repo / "config" / "pipeline_config.json"
            bootstrap_doc = repo / "docs" / "lore.docx"
            bootstrap_doc.parent.mkdir(parents=True)
            bootstrap_doc.write_bytes(b"not a real docx; config validation only")
            write_json(
                config_path,
                {
                    "paths": {
                        "docx_lore_bible": "old.docx",
                        "discord_conversations_root": "discord_conversations",
                        "artifacts_root": "artifacts",
                    },
                    "model_provider": {"provider": "openrouter"},
                },
            )
            (repo / ".env").write_text("OPENROUTER_KEY=old-key\nOTHER_VALUE=kept\n", encoding="utf-8")

            saved = handle_request(
                {
                    "repo_root": str(repo),
                    "command": "save_app_config",
                    "payload": {
                        "bootstrap_doc_path": str(bootstrap_doc),
                        "openrouter_api_key": "sk-or-test-key",
                    },
                }
            )

            self.assertEqual(saved["bootstrap_doc_config_value"], "docs/lore.docx")
            self.assertTrue(saved["bootstrap_doc_exists"])
            self.assertTrue(saved["openrouter_key_present"])
            self.assertNotIn("sk-or-test-key", json.dumps(saved))
            config = json.loads(config_path.read_text(encoding="utf-8"))
            self.assertEqual(config["paths"]["docx_lore_bible"], "docs/lore.docx")
            env_text = (repo / ".env").read_text(encoding="utf-8")
            self.assertIn("OPENROUTER_API_KEY=sk-or-test-key", env_text)
            self.assertNotIn("OPENROUTER_KEY=old-key", env_text)
            self.assertIn("OTHER_VALUE=kept", env_text)

            loaded = handle_request({"repo_root": str(repo), "command": "app_config", "payload": {}})
            self.assertEqual(loaded["bootstrap_doc_config_value"], "docs/lore.docx")
            self.assertEqual(loaded["openrouter_key_preview"], "sk-or-...-key")

    def test_stage_11_cardbase_agent_merges_pandoras_mother_into_izanami_before_synthesis(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            paths = ArtifactPaths(root)
            write_json(
                paths.resolved_entities,
                {
                    "resolved_entities": [
                        {
                            "entity_id": "entity_pandoras_mother",
                            "card_id": "card_pandoras_mother",
                            "canonical_name": "Pandora's mother",
                            "entity_type": "character",
                            "aliases": [],
                            "resolution_status": "resolved",
                        },
                        {
                            "entity_id": "entity_izanami",
                            "card_id": "card_izanami",
                            "canonical_name": "Izanami",
                            "entity_type": "character",
                            "aliases": [],
                            "resolution_status": "resolved",
                        },
                    ]
                },
            )
            claim = {
                "claim_id": "claim_pandoras_mother_threshold",
                "target_entity_id": "entity_pandoras_mother",
                "target_card_id": "card_pandoras_mother",
                "target_entity_name": "Pandora's mother",
                "knowledge_track": "lore",
                "claim_text": "Pandora's mother guards the threshold to Yomi.",
                "claim_type": "background",
                "source_snippet_ids": ["snippet_mother"],
                "confidence": 0.92,
                "status": "draft",
                "created_at_utc": "2026-05-20T00:00:00Z",
            }
            write_json(paths.claim_drafts, {"claims": [claim]})
            write_json(paths.claim_review_decisions, {"decisions": [{"claim_id": claim["claim_id"], "decision": "accept", "reviewer": "r", "rationale": "ok"}]})
            write_json(paths.card_review_decisions, {"decisions": []})
            write_json(paths.author_directives, {"directives": []})
            write_json(paths.canonical_cards, {"cards": []})
            memory_path = root / "canon" / "review_memory.json"
            write_json(memory_path, {"version": 1, "accepted_claims": [], "rejected_claims": [], "approved_aliases": [], "entity_merges": [], "approved_cards": [], "author_directives": [], "style_corrections": [], "updated_at_utc": "2026-05-20T00:00:00Z"})
            write_jsonl(
                paths.card_edit_requests,
                [
                    {
                        "request_id": "req_pandoras_mother_is_izanami",
                        "instruction_text": "Pandora's mother is Izanami",
                        "status": "pending",
                        "created_at_utc": "2026-05-20T00:01:00Z",
                    }
                ],
            )
            agent_calls = [
                {"tool_name": "search_entities", "arguments": {"query": "Pandora's mother"}, "rationale": "Find source."},
                {"tool_name": "search_entities", "arguments": {"query": "Izanami"}, "rationale": "Find target."},
                {"tool_name": "get_claims", "arguments": {"entity_id": "entity_pandoras_mother"}, "rationale": "Read source claims."},
                {"tool_name": "get_relationships", "arguments": {"entity_id": "entity_pandoras_mother"}, "rationale": "Read source relationships."},
                {
                    "tool_name": "apply_identity_merge",
                    "arguments": {
                        "source_entity_id": "entity_pandoras_mother",
                        "target_entity_id": "entity_izanami",
                        "claim_text": "Pandora's mother is Izanami",
                        "rationale": "Direct author identity assertion.",
                        "confidence": 1.0,
                    },
                    "rationale": "Merge the source into the surviving entity.",
                },
                {"tool_name": "rewrite_references", "arguments": {"source_entity_id": "entity_pandoras_mother", "target_entity_id": "entity_izanami"}, "rationale": "Rewrite references."},
                {"tool_name": "check_consistency", "arguments": {"source_entity_id": "entity_pandoras_mother", "target_entity_id": "entity_izanami"}, "rationale": "Verify."},
                {"tool_name": "finish", "arguments": {"final_response": "Done."}, "final_response": "Done.", "rationale": "Verified."},
            ]
            model_card = {
                "summary": "Izanami guards the threshold to Yomi.",
                "sections": {
                    "background": "Izanami guards the threshold to Yomi.",
                    "role_in_story": "",
                    "relationships": "",
                    "timeline": "",
                    "inspirations": "",
                    "open_questions": "",
                },
                "relationships": [],
                "timeline": [],
                "wiki_links": [],
                "support_map": {
                    "summary": ["claim_pandoras_mother_threshold"],
                    "background": ["claim_pandoras_mother_threshold"],
                    "role_in_story": [],
                    "relationships": [],
                    "timeline": [],
                    "inspirations": [],
                    "open_questions": [],
                },
            }
            synthesis_prompts: list[str] = []

            def fake_synthesis(prompt: str, **_kwargs: Any) -> dict[str, Any]:
                synthesis_prompts.append(prompt)
                return model_card

            with patch("pipeline.cardbase_agent.call_model_chat", side_effect=agent_calls):
                with patch("pipeline.stage_11_card_synthesis.call_model_chat", side_effect=fake_synthesis):
                    run_stage_11(
                        paths.resolved_entities,
                        paths.claim_drafts,
                        paths.claim_review_decisions,
                        paths.card_review_decisions,
                        paths.author_directives,
                        memory_path,
                        paths.card_drafts,
                        paths.canonical_cards,
                        paths.merge_log,
                        None,
                    )

            draft_cards = json.loads(paths.card_drafts.read_text(encoding="utf-8"))["cards"]
            self.assertEqual(len(draft_cards), 1)
            self.assertEqual(draft_cards[0]["canonical_name"], "Izanami")
            self.assertIn("Pandora's mother", draft_cards[0]["aliases"])
            self.assertIn("claim_pandoras_mother_threshold", draft_cards[0]["details"]["accepted_claim_ids"])
            self.assertNotIn("Pandora's mother", [card["canonical_name"] for card in draft_cards])
            self.assertIn("Pandora's mother guards the threshold to Yomi.", synthesis_prompts[0])
            self.assertIn("Izanami", synthesis_prompts[0])
            memory = json.loads(memory_path.read_text(encoding="utf-8"))
            self.assertEqual(memory["entity_merges"][0]["source_entity_name"], "Pandora's mother")
            self.assertEqual(memory["entity_merges"][0]["target_entity_name"], "Izanami")
            transactions = load_card_agent_transactions(paths.stage11)
            self.assertEqual(transactions[0]["status"], "completed")
            self.assertIn("apply_identity_merge", [step["tool_name"] for step in transactions[0]["steps"]])

    def test_stage_11_uses_preexisting_memory_merges_to_skip_duplicate_card_calls(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            paths = ArtifactPaths(root)
            write_json(
                paths.resolved_entities,
                {
                    "resolved_entities": [
                        {
                            "entity_id": "entity_laser",
                            "card_id": "card_laser",
                            "canonical_name": "Orbital Laser Weapon",
                            "entity_type": "location",
                            "aliases": ["Space Warship"],
                        },
                        {
                            "entity_id": "entity_garuda",
                            "card_id": "card_garuda",
                            "canonical_name": "The Garuda",
                            "entity_type": "location",
                            "aliases": ["Garuda"],
                        },
                    ]
                },
            )
            write_json(
                paths.claim_drafts,
                {
                    "claims": [
                        {
                            "claim_id": "claim_laser",
                            "target_entity_id": "entity_laser",
                            "target_card_id": "card_laser",
                            "target_entity_name": "Orbital Laser Weapon",
                            "knowledge_track": "lore",
                            "claim_type": "background",
                            "claim_text": "The orbital laser weapon melts roads.",
                        },
                        {
                            "claim_id": "claim_garuda",
                            "target_entity_id": "entity_garuda",
                            "target_card_id": "card_garuda",
                            "target_entity_name": "The Garuda",
                            "knowledge_track": "lore",
                            "claim_type": "background",
                            "claim_text": "The Garuda is a space-based weapon system.",
                        },
                    ]
                },
            )
            write_json(
                paths.claim_review_decisions,
                {
                    "decisions": [
                        {"claim_id": "claim_laser", "decision": "accept"},
                        {"claim_id": "claim_garuda", "decision": "accept"},
                    ]
                },
            )
            write_json(paths.card_review_decisions, {"decisions": []})
            write_json(paths.author_directives, {"directives": []})
            write_json(paths.canonical_cards, {"cards": []})
            write_json(paths.identity_merge_proposals, {"proposals": []})
            write_json(paths.identity_merge_decisions, {"decisions": []})
            memory_path = root / "canon" / "review_memory.json"
            write_json(
                memory_path,
                {
                    "version": 1,
                    "accepted_claims": [],
                    "rejected_claims": [],
                    "approved_aliases": [],
                    "entity_merges": [
                        {
                            "merge_id": "merge_laser_garuda",
                            "source_entity_id": "entity_laser",
                            "source_card_id": "card_laser",
                            "source_entity_name": "Orbital Laser Weapon",
                            "target_entity_id": "entity_garuda",
                            "target_card_id": "card_garuda",
                            "target_entity_name": "The Garuda",
                            "canonical_name": "The Garuda",
                            "alias_text": "Orbital Laser Weapon",
                            "merge_type": "cardbase_agent_identity_merge",
                        }
                    ],
                    "approved_cards": [],
                    "author_directives": [],
                    "style_corrections": [],
                    "updated_at_utc": "2026-05-20T00:00:00Z",
                },
            )
            synthesis_prompts: list[str] = []

            def fake_synthesis(prompt: str, **_kwargs: Any) -> dict[str, Any]:
                synthesis_prompts.append(prompt)
                return {
                    "summary": "The Garuda is a space-based weapon that melts roads.",
                    "sections": {
                        "background": "The Garuda is a space-based weapon system that melts roads.",
                        "role_in_story": "",
                        "relationships": "",
                        "timeline": "",
                        "inspirations": "",
                        "open_questions": "",
                    },
                    "relationships": [],
                    "timeline": [],
                    "wiki_links": [],
                    "support_map": {
                        "summary": ["claim_laser", "claim_garuda"],
                        "background": ["claim_laser", "claim_garuda"],
                        "role_in_story": [],
                        "relationships": [],
                        "timeline": [],
                        "inspirations": [],
                        "open_questions": [],
                    },
                }

            with patch("pipeline.stage_11_card_synthesis.call_model_chat", side_effect=fake_synthesis):
                run_stage_11(
                    paths.resolved_entities,
                    paths.claim_drafts,
                    paths.claim_review_decisions,
                    paths.card_review_decisions,
                    paths.author_directives,
                    memory_path,
                    paths.card_drafts,
                    paths.canonical_cards,
                    paths.merge_log,
                    None,
                )

            self.assertEqual(len(synthesis_prompts), 1)
            draft_cards = json.loads(paths.card_drafts.read_text(encoding="utf-8"))["cards"]
            self.assertEqual(len(draft_cards), 1)
            self.assertEqual(draft_cards[0]["canonical_name"], "The Garuda")
            self.assertIn("Orbital Laser Weapon", draft_cards[0]["aliases"])
            self.assertNotIn("Orbital Laser Weapon", [card["canonical_name"] for card in draft_cards])
            self.assertEqual(
                set(draft_cards[0]["details"]["accepted_claim_ids"]),
                {"claim_laser", "claim_garuda"},
            )

    def test_stage_11_card_synthesis_prompt_includes_original_source_snippet_context(self) -> None:
        prompt = build_card_synthesis_prompt(
            {"canonical_name": "HECTR", "entity_type": "character"},
            [
                {
                    "claim_id": "claim1",
                    "claim_text": "HECTR oversees the Krypteia lab sequence.",
                    "claim_type": "role",
                    "source_snippet_ids": ["s1"],
                }
            ],
            {},
            source_snippets_by_id={
                "s1": {
                    "snippet_id": "s1",
                    "conversation_global_index": 12,
                    "conversation_id": "conversation_a",
                    "conversation_topic_label": "HECTR lab role",
                    "timestamp_start_utc": "2026-05-01T00:00:00Z",
                    "conversation_patch_summary": "The conversation develops HECTR's lab role.",
                    "display_text_normalized": "HECTR watches the lab sequence and frames the Krypteia reveal.",
                    "patch_item_type": "entity_update",
                    "patch_update_type": "role_change",
                }
            },
        )

        self.assertIn("Source snippet evidence for accepted claims", prompt)
        self.assertIn("HECTR watches the lab sequence", prompt)
        self.assertIn("Do not merely summarize summaries", prompt)

    def test_stage_11_card_synthesis_prompt_scales_word_targets_for_developed_entities(self) -> None:
        claims = [
            {"claim_id": f"claim{i}", "claim_text": f"Enoch development detail {i}.", "claim_type": "role"}
            for i in range(1, 10)
        ]
        prompt = build_card_synthesis_prompt({"canonical_name": "Enoch", "entity_type": "character"}, claims, {})

        self.assertIn('"min": 400', prompt)
        self.assertIn('"max": 800', prompt)
        self.assertIn("Heavily developed characters", prompt)

    def test_stage_11_draft_cards_include_wiki_links_to_related_cards(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write_json(
                root / "resolved_entities.json",
                {
                    "resolved_entities": [
                        {
                            "entity_id": "entity_hectr",
                            "card_id": "card_hectr",
                            "canonical_name": "HECTR",
                            "entity_type": "character",
                            "aliases": [],
                            "resolution_status": "resolved",
                        },
                        {
                            "entity_id": "entity_krypteia",
                            "card_id": "card_krypteia",
                            "canonical_name": "Krypteia",
                            "entity_type": "organization",
                            "aliases": [],
                            "resolution_status": "resolved",
                        },
                    ]
                },
            )
            claim = {
                "claim_id": "claim_link",
                "target_entity_id": "entity_hectr",
                "target_card_id": "card_hectr",
                "target_entity_name": "HECTR",
                "knowledge_track": "lore",
                "claim_text": "HECTR is related to Krypteia AI systems.",
                "claim_type": "relationship",
                "source_snippet_ids": ["s1"],
                "confidence": 0.82,
                "status": "draft",
                "contradiction_notes": "",
                "created_at_utc": "2026-05-16T00:00:00Z",
            }
            write_json(root / "claim_drafts.json", {"claims": [claim]})
            write_json(root / "claim_decisions.json", {"decisions": [{"claim_id": "claim_link", "decision": "accept"}]})
            write_json(root / "card_decisions.json", {"decisions": []})
            write_json(root / "directives.json", {"directives": []})
            write_json(root / "memory.json", {"version": 1, "accepted_claims": [], "rejected_claims": [], "approved_aliases": [], "entity_merges": [], "approved_cards": [], "author_directives": [], "style_corrections": [], "updated_at_utc": "2026-05-16T00:00:00Z"})
            model_card = {
                "summary": "HECTR is discussed in relation to Krypteia AI systems.",
                "sections": {"background": "HECTR's background is tied to Krypteia.", "role_in_story": "", "relationships": "HECTR is connected to Krypteia through accepted relationship evidence.", "timeline": "", "inspirations": "", "open_questions": ""},
                "relationships": [{"target_entity_name": "Krypteia", "relation_type": "related_to", "note": "HECTR is related to Krypteia AI systems.", "support_claim_ids": ["claim_link"]}],
                "timeline": [],
                "wiki_links": [{"target_entity_name": "Krypteia", "relation_type": "related_to", "section": "relationships", "support_claim_ids": ["claim_link"]}],
                "support_map": {"summary": ["claim_link"], "background": ["claim_link"], "role_in_story": [], "relationships": ["claim_link"], "timeline": [], "inspirations": [], "open_questions": []},
            }

            with patch("pipeline.stage_11_card_synthesis.call_model_chat", return_value=model_card):
                run_stage_11(
                    root / "resolved_entities.json",
                    root / "claim_drafts.json",
                    root / "claim_decisions.json",
                    root / "card_decisions.json",
                    root / "directives.json",
                    root / "memory.json",
                    root / "card_drafts.json",
                    root / "canonical_cards.json",
                    root / "merge_log.jsonl",
                    None,
                )

            draft_card = json.loads((root / "card_drafts.json").read_text(encoding="utf-8"))["cards"][0]
            self.assertEqual(draft_card["relationships"][0]["target_card_id"], "card_krypteia")
            self.assertEqual(draft_card["details"]["wiki_links"][0]["target_card_id"], "card_krypteia")
            self.assertIn("section_word_counts", draft_card["details"])

    def test_stage_11_writes_inspiration_section_from_accepted_inspiration_claims(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write_json(
                root / "resolved_entities.json",
                {
                    "resolved_entities": [
                        {
                            "entity_id": "entity_golok",
                            "card_id": "card_golok",
                            "canonical_name": "Golok",
                            "entity_type": "character",
                            "aliases": [],
                            "resolution_status": "resolved",
                        }
                    ]
                },
            )
            claim = {
                "claim_id": "claim_inspiration",
                "target_entity_id": "entity_golok",
                "target_card_id": "card_golok",
                "target_entity_name": "Golok",
                "knowledge_track": "lore",
                "claim_text": "Golok's ethos is compared to Adam Smasher as an external inspiration.",
                "claim_type": "inspiration",
                "source_snippet_ids": ["s_inspiration"],
                "confidence": 0.82,
                "status": "draft",
                "contradiction_notes": "",
                "created_at_utc": "2026-05-16T00:00:00Z",
            }
            write_json(root / "claim_drafts.json", {"claims": [claim]})
            write_json(
                root / "claim_decisions.json",
                {"decisions": [{"claim_id": "claim_inspiration", "decision": "accept", "reviewer": "r", "rationale": "ok"}]},
            )
            write_json(root / "card_decisions.json", {"decisions": []})
            write_json(root / "directives.json", {"directives": []})
            write_json(root / "memory.json", {"version": 1, "accepted_claims": [], "rejected_claims": [], "approved_aliases": [], "entity_merges": [], "approved_cards": [], "author_directives": [], "style_corrections": [], "updated_at_utc": "2026-05-16T00:00:00Z"})
            model_card = {
                "summary": "Golok is framed through accepted inspiration evidence.",
                "sections": {
                    "background": "",
                    "role_in_story": "",
                    "relationships": "",
                    "timeline": "",
                    "inspirations": "Golok's ethos is compared to Adam Smasher as an external inspiration.",
                    "open_questions": "",
                },
                "relationships": [],
                "timeline": [],
                "resolved_conflicts": [],
                "unresolved_conflicts": [],
                "support_map": {
                    "summary": ["claim_inspiration"],
                    "background": [],
                    "role_in_story": [],
                    "relationships": [],
                    "timeline": [],
                    "inspirations": ["claim_inspiration"],
                    "open_questions": [],
                    "resolved_conflicts": [],
                    "unresolved_conflicts": [],
                },
            }

            with patch("pipeline.stage_11_card_synthesis.call_model_chat", return_value=model_card):
                run_stage_11(
                    root / "resolved_entities.json",
                    root / "claim_drafts.json",
                    root / "claim_decisions.json",
                    root / "card_decisions.json",
                    root / "directives.json",
                    root / "memory.json",
                    root / "card_drafts.json",
                    root / "canonical_cards.json",
                    root / "merge_log.jsonl",
                    None,
                )

            draft_cards = json.loads((root / "card_drafts.json").read_text(encoding="utf-8"))["cards"]
            self.assertEqual(draft_cards[0]["details"]["sections"]["inspirations"], model_card["sections"]["inspirations"])
            self.assertEqual(draft_cards[0]["details"]["support_map"]["inspirations"], ["claim_inspiration"])
            self.assertEqual(draft_cards[0]["source_evidence"], ["s_inspiration"])

    def test_stage_11_revises_from_full_accepted_claim_history(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write_json(
                root / "resolved_entities.json",
                {
                    "resolved_entities": [
                        {
                            "entity_id": "entity_hectr",
                            "card_id": "card_hectr",
                            "canonical_name": "HECTR",
                            "entity_type": "character",
                            "aliases": [],
                            "resolution_status": "resolved",
                        }
                    ]
                },
            )
            old_claim = {
                "claim_id": "old_claim",
                "target_entity_id": "entity_hectr",
                "target_card_id": "card_hectr",
                "target_entity_name": "HECTR",
                "knowledge_track": "lore",
                "claim_text": "HECTR is a template ancestor for Krypteia AI systems.",
                "claim_type": "relationship",
                "source_snippet_ids": ["old_snip"],
                "confidence": 0.8,
                "status": "accepted",
                "normalized_claim_text": "hectr is a template ancestor for krypteia ai systems",
                "created_at_utc": "2026-05-15T00:00:00Z",
            }
            new_claim = {
                "claim_id": "new_claim",
                "target_entity_id": "entity_hectr",
                "target_card_id": "card_hectr",
                "target_entity_name": "HECTR",
                "knowledge_track": "lore",
                "claim_text": "HECTR is unaware of RUINR because Krypteia firewalls hide RUINR from him.",
                "claim_type": "role",
                "source_snippet_ids": ["new_snip"],
                "confidence": 0.9,
                "status": "draft",
                "contradiction_notes": "",
                "created_at_utc": "2026-05-16T00:00:00Z",
            }
            write_json(root / "claim_drafts.json", {"claims": [new_claim]})
            write_json(root / "claim_decisions.json", {"decisions": [{"claim_id": "new_claim", "decision": "accept", "reviewer": "r", "rationale": "ok"}]})
            write_json(root / "card_decisions.json", {"decisions": [{"card_id": "card_hectr", "decision": "approve", "reviewer": "r", "rationale": "revise"}]})
            write_json(root / "directives.json", {"directives": []})
            write_json(
                root / "memory.json",
                {
                    "version": 1,
                    "accepted_claims": [old_claim],
                    "rejected_claims": [],
                    "approved_aliases": [],
                    "entity_merges": [],
                    "approved_cards": [],
                    "author_directives": [],
                    "style_corrections": [],
                    "updated_at_utc": "2026-05-16T00:00:00Z",
                },
            )
            write_json(root / "canonical_cards.json", {"cards": [{"card_id": "card_other", "canonical_name": "Other", "entity_type": "term", "aliases": [], "status": "canonical", "summary": "Keep me.", "details": {}, "timeline": [], "relationships": [], "source_evidence": [], "confidence": {"score": 1}, "revision_history": []}]})
            model_card = {
                "summary": "HECTR predates Krypteia's AI lineage and remains cut off from RUINR by Krypteia's own firewalls.",
                "sections": {"background": "Krypteia's AI systems treat HECTR as an ancestral template.", "role_in_story": "Krypteia's firewalling keeps RUINR hidden from HECTR, limiting what HECTR understands about that threat.", "relationships": "", "timeline": "", "open_questions": ""},
                "relationships": [],
                "timeline": [],
                "resolved_conflicts": [],
                "unresolved_conflicts": [],
                "support_map": {"summary": ["old_claim", "new_claim"], "background": ["old_claim"], "role_in_story": ["new_claim"], "relationships": [], "timeline": [], "open_questions": [], "resolved_conflicts": [], "unresolved_conflicts": []},
            }

            with patch("pipeline.stage_11_card_synthesis.call_model_chat", return_value=model_card):
                run_stage_11(
                    root / "resolved_entities.json",
                    root / "claim_drafts.json",
                    root / "claim_decisions.json",
                    root / "card_decisions.json",
                    root / "directives.json",
                    root / "memory.json",
                    root / "card_drafts.json",
                    root / "canonical_cards.json",
                    root / "merge_log.jsonl",
                    None,
                )

            canonical_cards = json.loads((root / "canonical_cards.json").read_text(encoding="utf-8"))["cards"]
            by_id = {card["card_id"]: card for card in canonical_cards}
            self.assertIn("card_other", by_id)
            self.assertIn("card_hectr", by_id)
            self.assertEqual(by_id["card_hectr"]["source_evidence"], ["new_snip", "old_snip"])
            self.assertEqual(by_id["card_hectr"]["details"]["accepted_claim_ids"], ["old_claim", "new_claim"])

    def test_stage_11_stores_accepted_alias_claims_for_future_resolution(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write_json(
                root / "resolved_entities.json",
                {
                    "resolved_entities": [
                        {
                            "entity_id": "entity_hectr",
                            "card_id": "card_hectr",
                            "canonical_name": "HECTR",
                            "entity_type": "character",
                            "aliases": [],
                            "resolution_status": "resolved",
                        }
                    ]
                },
            )
            alias_claim = {
                "claim_id": "alias_claim",
                "target_entity_id": "entity_hectr",
                "target_card_id": "card_hectr",
                "target_entity_name": "HECTR",
                "knowledge_track": "lore",
                "claim_text": "HECTR is also called the Warden.",
                "claim_type": "alias",
                "alias_text": "the Warden",
                "source_snippet_ids": ["s1"],
                "confidence": 0.8,
                "status": "draft",
                "contradiction_notes": "",
                "created_at_utc": "2026-05-16T00:00:00Z",
            }
            write_json(root / "claim_drafts.json", {"claims": [alias_claim]})
            write_json(root / "claim_decisions.json", {"decisions": [{"claim_id": "alias_claim", "decision": "accept", "reviewer": "r", "rationale": "ok"}]})
            write_json(root / "card_decisions.json", {"decisions": []})
            write_json(root / "directives.json", {"directives": []})
            write_json(root / "memory.json", {"version": 1, "accepted_claims": [], "rejected_claims": [], "approved_aliases": [], "entity_merges": [], "approved_cards": [], "author_directives": [], "style_corrections": [], "updated_at_utc": "2026-05-16T00:00:00Z"})
            model_card = {
                "summary": "HECTR is also called the Warden.",
                "sections": {"background": "HECTR is also called the Warden.", "role_in_story": "", "relationships": "", "timeline": "", "open_questions": ""},
                "relationships": [],
                "timeline": [],
                "support_map": {"summary": ["alias_claim"], "background": ["alias_claim"], "role_in_story": [], "relationships": [], "timeline": [], "open_questions": [], "resolved_conflicts": [], "unresolved_conflicts": []},
            }

            with patch("pipeline.stage_11_card_synthesis.call_model_chat", return_value=model_card):
                run_stage_11(
                    root / "resolved_entities.json",
                    root / "claim_drafts.json",
                    root / "claim_decisions.json",
                    root / "card_decisions.json",
                    root / "directives.json",
                    root / "memory.json",
                    root / "card_drafts.json",
                    root / "canonical_cards.json",
                    root / "merge_log.jsonl",
                    None,
                )

            memory = json.loads((root / "memory.json").read_text(encoding="utf-8"))
            self.assertEqual(memory["approved_aliases"][0]["alias_text"], "the Warden")
            self.assertEqual(memory["approved_aliases"][0]["canonical_name"], "HECTR")

    def test_stage_10_requires_review_then_stage_11_applies_identity_merge_from_rename_claim(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write_json(
                root / "resolved_entities.json",
                {
                    "resolved_entities": [
                        {
                            "entity_id": "entity_ruinr",
                            "card_id": "card_ruinr",
                            "canonical_name": "RUINR",
                            "entity_type": "character",
                            "aliases": [],
                            "resolution_status": "resolved",
                        },
                        {
                            "entity_id": "entity_achilles",
                            "card_id": "card_achilles",
                            "canonical_name": "ACHILLES",
                            "entity_type": "character",
                            "aliases": [],
                            "resolution_status": "resolved",
                        },
                    ]
                },
            )
            ruinr_claim = {
                "claim_id": "ruinr_claim",
                "target_entity_id": "entity_ruinr",
                "target_card_id": "card_ruinr",
                "target_entity_name": "RUINR",
                "knowledge_track": "lore",
                "claim_text": "RUINR is introduced through a neurally integrated suit sequence.",
                "claim_type": "background",
                "source_snippet_ids": ["s_ruinr"],
                "confidence": 0.9,
                "status": "draft",
                "created_at_utc": "2026-05-16T00:00:00Z",
            }
            rename_claim = {
                "claim_id": "rename_claim",
                "target_entity_id": "entity_achilles",
                "target_card_id": "card_achilles",
                "target_entity_name": "ACHILLES",
                "knowledge_track": "lore",
                "claim_text": "ACHILLES renames itself to RUINR during the second quest.",
                "claim_type": "timeline",
                "source_snippet_ids": ["s_rename"],
                "confidence": 0.95,
                "status": "draft",
                "created_at_utc": "2026-05-16T00:01:00Z",
            }
            write_json(root / "claim_drafts.json", {"claims": [ruinr_claim, rename_claim]})
            write_json(
                root / "claim_decisions.json",
                {
                    "decisions": [
                        {"claim_id": "ruinr_claim", "decision": "accept", "reviewer": "r", "rationale": "ok"},
                        {"claim_id": "rename_claim", "decision": "accept", "reviewer": "r", "rationale": "ok"},
                    ]
                },
            )
            write_json(root / "card_decisions.json", {"decisions": []})
            write_json(root / "directives.json", {"directives": []})
            write_json(root / "memory.json", {"version": 1, "accepted_claims": [], "rejected_claims": [], "approved_aliases": [], "entity_merges": [], "approved_cards": [], "author_directives": [], "style_corrections": [], "updated_at_utc": "2026-05-16T00:00:00Z"})

            with patch("pipeline.stage_11_card_synthesis.call_model_chat") as model:
                with self.assertRaisesRegex(RuntimeError, "identity cluster proposal"):
                    run_stage_10(
                        root / "resolved_entities.json",
                        root / "claim_drafts.json",
                        root / "claim_decisions.json",
                        root / "memory.json",
                        root / "identity_merge_proposals.json",
                        root / "identity_merge_decisions.json",
                        None,
                    )
                model.assert_not_called()

            proposals = json.loads((root / "identity_merge_proposals.json").read_text(encoding="utf-8"))["proposals"]
            self.assertEqual(len(proposals), 1)
            proposal = proposals[0]
            self.assertEqual(proposal["source_entity_name"], "ACHILLES")
            self.assertEqual(proposal["target_entity_name"], "RUINR")
            self.assertEqual(proposal["review_status"], "pending")
            self.assertEqual(proposal["evidence_claim_ids"], ["rename_claim"])
            preview = json.loads((root / "identity_merged_entities_preview.json").read_text(encoding="utf-8"))
            self.assertEqual(preview["source_entity_count"], 2)
            self.assertEqual(preview["merged_entity_count"], 1)
            self.assertEqual(preview["pending_identity_merge_count"], 1)
            self.assertEqual(preview["entities"][0]["canonical_name"], "RUINR")
            self.assertIn("ACHILLES", preview["entities"][0]["aliases"])
            self.assertEqual(preview["entities"][0]["identity_merge_preview_status"], "pending")

            write_json(
                root / "identity_merge_decisions.json",
                {
                    "decisions": [
                        {
                            "proposal_id": proposal["proposal_id"],
                            "decision": "approve",
                            "reviewer": "r",
                            "rationale": "same entity",
                            "timestamp_utc": "2026-05-16T00:02:00Z",
                        }
                    ]
                },
            )
            run_stage_10(
                root / "resolved_entities.json",
                root / "claim_drafts.json",
                root / "claim_decisions.json",
                root / "memory.json",
                root / "identity_merge_proposals.json",
                root / "identity_merge_decisions.json",
                None,
            )
            approved_preview = json.loads((root / "identity_merged_entities_preview.json").read_text(encoding="utf-8"))
            self.assertEqual(approved_preview["pending_identity_merge_count"], 0)
            self.assertEqual(approved_preview["approved_identity_merge_count"], 1)
            self.assertEqual(approved_preview["entities"][0]["identity_merge_preview_status"], "approve")
            model_card = {
                "summary": "RUINR is introduced through a neurally integrated suit sequence, after beginning as ACHILLES.",
                "sections": {
                    "background": "RUINR is introduced through a neurally integrated suit sequence.",
                    "role_in_story": "",
                    "relationships": "",
                    "timeline": "ACHILLES renames itself to RUINR during the second quest.",
                    "open_questions": "",
                },
                "relationships": [],
                "timeline": [],
                "resolved_conflicts": [],
                "unresolved_conflicts": [],
                "support_map": {
                    "summary": ["ruinr_claim", "rename_claim"],
                    "background": ["ruinr_claim"],
                    "role_in_story": [],
                    "relationships": [],
                    "timeline": ["rename_claim"],
                    "open_questions": [],
                    "resolved_conflicts": [],
                    "unresolved_conflicts": [],
                },
            }

            with patch("pipeline.stage_11_card_synthesis.call_model_chat", return_value=model_card):
                run_stage_11(
                    root / "resolved_entities.json",
                    root / "claim_drafts.json",
                    root / "claim_decisions.json",
                    root / "card_decisions.json",
                    root / "directives.json",
                    root / "memory.json",
                    root / "card_drafts.json",
                    root / "canonical_cards.json",
                    root / "merge_log.jsonl",
                    None,
                )

            draft_cards = json.loads((root / "card_drafts.json").read_text(encoding="utf-8"))["cards"]
            self.assertEqual(len(draft_cards), 1)
            self.assertEqual(draft_cards[0]["canonical_name"], "RUINR")
            self.assertEqual(draft_cards[0]["aliases"], ["ACHILLES"])
            self.assertEqual(draft_cards[0]["details"]["accepted_claim_ids"], ["ruinr_claim", "rename_claim"])
            self.assertEqual(draft_cards[0]["source_evidence"], ["s_rename", "s_ruinr"])

            memory = json.loads((root / "memory.json").read_text(encoding="utf-8"))
            self.assertEqual(memory["entity_merges"][0]["source_entity_name"], "ACHILLES")
            self.assertEqual(memory["entity_merges"][0]["target_entity_name"], "RUINR")
            self.assertEqual(memory["approved_aliases"][0]["alias_text"], "ACHILLES")

    def test_stage_11_reads_identity_merge_decisions_from_stage_10_artifacts(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            paths = ArtifactPaths(root)
            write_json(
                paths.resolved_entities,
                {
                    "resolved_entities": [
                        {
                            "entity_id": "entity_ruinr",
                            "card_id": "card_ruinr",
                            "canonical_name": "RUINR",
                            "entity_type": "character",
                            "aliases": [],
                            "resolution_status": "resolved",
                        },
                        {
                            "entity_id": "entity_achilles",
                            "card_id": "card_achilles",
                            "canonical_name": "ACHILLES",
                            "entity_type": "character",
                            "aliases": [],
                            "resolution_status": "resolved",
                        },
                    ]
                },
            )
            claims = [
                {
                    "claim_id": "ruinr_claim",
                    "target_entity_id": "entity_ruinr",
                    "target_card_id": "card_ruinr",
                    "target_entity_name": "RUINR",
                    "knowledge_track": "lore",
                    "claim_text": "RUINR is introduced through a neurally integrated suit sequence.",
                    "claim_type": "background",
                    "source_snippet_ids": ["s_ruinr"],
                    "confidence": 0.9,
                    "status": "draft",
                    "created_at_utc": "2026-05-16T00:00:00Z",
                },
                {
                    "claim_id": "rename_claim",
                    "target_entity_id": "entity_achilles",
                    "target_card_id": "card_achilles",
                    "target_entity_name": "ACHILLES",
                    "knowledge_track": "lore",
                    "claim_text": "ACHILLES renames itself to RUINR during the second quest.",
                    "claim_type": "timeline",
                    "source_snippet_ids": ["s_rename"],
                    "confidence": 0.95,
                    "status": "draft",
                    "created_at_utc": "2026-05-16T00:01:00Z",
                },
            ]
            write_json(paths.claim_drafts, {"claims": claims})
            write_json(
                paths.claim_review_decisions,
                {
                    "decisions": [
                        {"claim_id": "ruinr_claim", "decision": "accept", "reviewer": "r", "rationale": "ok"},
                        {"claim_id": "rename_claim", "decision": "accept", "reviewer": "r", "rationale": "ok"},
                    ]
                },
            )
            write_json(paths.card_review_decisions, {"decisions": []})
            write_json(paths.author_directives, {"directives": []})
            memory_path = root / "canon" / "review_memory.json"
            write_json(memory_path, {"version": 1, "accepted_claims": [], "rejected_claims": [], "approved_aliases": [], "entity_merges": [], "approved_cards": [], "author_directives": [], "style_corrections": [], "updated_at_utc": "2026-05-16T00:00:00Z"})
            proposal = {
                "proposal_id": "identity_cluster_ruinr_achilles",
                "cluster_id": "identity_cluster_ruinr_achilles",
                "proposal_kind": "identity_cluster",
                "member_entity_ids": ["entity_achilles", "entity_ruinr"],
                "member_entities": [
                    {"entity_id": "entity_achilles", "card_id": "card_achilles", "canonical_name": "ACHILLES", "entity_type": "character", "aliases": []},
                    {"entity_id": "entity_ruinr", "card_id": "card_ruinr", "canonical_name": "RUINR", "entity_type": "character", "aliases": []},
                ],
                "canonical_entity_id": "entity_ruinr",
                "canonical_name": "RUINR",
                "target_entity_id": "entity_ruinr",
                "target_card_id": "card_ruinr",
                "target_entity_name": "RUINR",
                "source_entity_id": "entity_achilles",
                "source_card_id": "card_achilles",
                "source_entity_name": "ACHILLES",
                "alias_texts": ["ACHILLES"],
                "former_names": ["ACHILLES"],
                "working_names": [],
                "formal_names": [],
                "merge_type": "identity_cluster",
                "review_status": "pending",
                "evidence_claim_ids": ["rename_claim"],
                "source_snippet_ids": ["s_rename"],
                "member_edges": [
                    {
                        "proposal_id": "edge_achilles_ruinr",
                        "source_entity_id": "entity_achilles",
                        "source_entity_name": "ACHILLES",
                        "target_entity_id": "entity_ruinr",
                        "target_entity_name": "RUINR",
                        "evidence_claim_ids": ["rename_claim"],
                        "source_snippet_ids": ["s_rename"],
                    }
                ],
            }
            write_json(paths.identity_merge_proposals, {"proposals": [proposal]})
            write_json(
                paths.identity_merge_decisions,
                {
                    "decisions": [
                        {
                            "proposal_id": "identity_cluster_ruinr_achilles",
                            "decision": "approve",
                            "reviewer": "r",
                            "rationale": "same entity",
                            "timestamp_utc": "2026-05-16T00:02:00Z",
                        }
                    ]
                },
            )
            model_card = {
                "summary": "RUINR is introduced through a neurally integrated suit sequence, after beginning as ACHILLES.",
                "sections": {
                    "background": "RUINR is introduced through a neurally integrated suit sequence.",
                    "role_in_story": "",
                    "relationships": "",
                    "timeline": "ACHILLES renames itself to RUINR during the second quest.",
                    "open_questions": "",
                },
                "relationships": [],
                "timeline": [],
                "support_map": {
                    "summary": ["ruinr_claim", "rename_claim"],
                    "background": ["ruinr_claim"],
                    "role_in_story": [],
                    "relationships": [],
                    "timeline": ["rename_claim"],
                    "open_questions": [],
                },
            }

            with patch("pipeline.stage_11_card_synthesis.call_model_chat", return_value=model_card):
                run_stage_11(
                    paths.resolved_entities,
                    paths.claim_drafts,
                    paths.claim_review_decisions,
                    paths.card_review_decisions,
                    paths.author_directives,
                    memory_path,
                    paths.card_drafts,
                    paths.canonical_cards,
                    paths.merge_log,
                    None,
                )

            self.assertFalse((paths.stage11 / "identity_merge_decisions.json").exists())
            draft_cards = json.loads(paths.card_drafts.read_text(encoding="utf-8"))["cards"]
            self.assertEqual(len(draft_cards), 1)
            self.assertEqual(draft_cards[0]["canonical_name"], "RUINR")
            self.assertEqual(draft_cards[0]["aliases"], ["ACHILLES"])
            self.assertEqual(draft_cards[0]["details"]["accepted_claim_ids"], ["ruinr_claim", "rename_claim"])

    def test_stage_11_collates_identity_chain_into_one_canonical_cluster(self) -> None:
        entities = [
            {
                "entity_id": "entity_loss",
                "card_id": "card_loss",
                "canonical_name": "Loss",
                "entity_type": "character",
                "aliases": [],
            },
            {
                "entity_id": "entity_enoch",
                "card_id": "card_enoch",
                "canonical_name": "Enoch",
                "entity_type": "character",
                "aliases": [],
            },
            {
                "entity_id": "entity_enoch_full",
                "card_id": "card_enoch_full",
                "canonical_name": "Enoch Faust Ersatzen",
                "entity_type": "character",
                "aliases": [],
            },
        ]
        edges = [
            {
                "proposal_id": "edge_loss_enoch",
                "source_entity_id": "entity_loss",
                "source_entity_name": "Loss",
                "target_entity_id": "entity_enoch",
                "target_entity_name": "Enoch",
                "evidence_claim_ids": ["claim_loss"],
                "source_snippet_ids": ["snippet_loss"],
                "evidence": [{"claim_id": "claim_loss", "claim_text": "Loss later becomes Enoch."}],
            },
            {
                "proposal_id": "edge_enoch_full",
                "source_entity_id": "entity_enoch",
                "source_entity_name": "Enoch",
                "target_entity_id": "entity_enoch_full",
                "target_entity_name": "Enoch Faust Ersatzen",
                "evidence_claim_ids": ["claim_full"],
                "source_snippet_ids": ["snippet_full"],
                "evidence": [{"claim_id": "claim_full", "claim_text": "Enoch's full name is Enoch Faust Ersatzen."}],
            },
        ]
        config = {
            "model_routing": {
                "default_profile": "deep_reasoning",
                "profiles": {"deep_reasoning": {"provider": "openrouter", "api_model": "deepseek/deepseek-v4-flash"}},
                "tasks": {"stage_10_identity_merge_cluster_judgement": {"profile": "deep_reasoning"}},
            },
            "model_provider": {"synthesis_provider_retries": 0},
        }
        judgement = {
            "clusters": [
                {
                    "cluster_index": 0,
                    "canonical_entity_id": "entity_enoch",
                    "canonical_name": "Enoch",
                    "aliases": ["Loss", "Enoch Faust Ersatzen"],
                    "former_names": ["Loss"],
                    "working_names": ["Loss"],
                    "formal_names": ["Enoch Faust Ersatzen"],
                    "do_not_merge_entity_ids": [],
                    "status": "ready_for_review",
                    "confidence": 0.91,
                    "rationale": "Loss is a working name and Enoch Faust Ersatzen is a full name; Enoch is the best page title.",
                }
            ]
        }
        with patch("pipeline.stage_11_card_synthesis.call_model_chat", return_value=judgement):
            proposals = _build_identity_cluster_proposals(edges, entities, config)

        self.assertEqual(len(proposals), 1)
        proposal = proposals[0]
        self.assertEqual(proposal["proposal_kind"], "identity_cluster")
        self.assertEqual(proposal["canonical_entity_id"], "entity_enoch")
        self.assertEqual(proposal["canonical_name"], "Enoch")
        self.assertEqual(proposal["target_entity_name"], "Enoch")
        self.assertEqual(set(proposal["member_entity_ids"]), {"entity_loss", "entity_enoch", "entity_enoch_full"})
        self.assertEqual(set(proposal["edge_proposal_ids"]), {"edge_loss_enoch", "edge_enoch_full"})
        self.assertIn("Loss", proposal["alias_texts"])
        self.assertIn("Enoch Faust Ersatzen", proposal["formal_names"])

    def test_stage_11_rejects_unsupported_acronym_expansion(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write_json(
                root / "resolved_entities.json",
                {
                    "resolved_entities": [
                        {
                            "entity_id": "entity_hectr",
                            "card_id": "card_hectr",
                            "canonical_name": "HECTR",
                            "entity_type": "character",
                            "aliases": [],
                            "resolution_status": "resolved",
                        }
                    ]
                },
            )
            claim = {
                "claim_id": "claim1",
                "target_entity_id": "entity_hectr",
                "target_card_id": "card_hectr",
                "target_entity_name": "HECTR",
                "knowledge_track": "lore",
                "claim_text": "HECTR is a template ancestor for Krypteia AI systems.",
                "claim_type": "relationship",
                "source_snippet_ids": ["s1"],
                "confidence": 0.82,
                "status": "draft",
                "contradiction_notes": "",
                "created_at_utc": "2026-05-16T00:00:00Z",
            }
            write_json(root / "claim_drafts.json", {"claims": [claim]})
            write_json(root / "claim_decisions.json", {"decisions": [{"claim_id": "claim1", "decision": "accept", "reviewer": "r", "rationale": "ok"}]})
            write_json(root / "card_decisions.json", {"decisions": []})
            write_json(root / "directives.json", {"directives": []})
            write_json(root / "memory.json", {"version": 1, "accepted_claims": [], "rejected_claims": [], "approved_aliases": [], "entity_merges": [], "approved_cards": [], "author_directives": [], "style_corrections": [], "updated_at_utc": "2026-05-16T00:00:00Z"})
            model_card = {
                "summary": "HECTR (Hierarchical Embedded Cognitive Template for Recursive Architectures) is a Krypteia AI ancestor.",
                "sections": {"background": "It shapes Krypteia systems.", "role_in_story": "", "relationships": "", "timeline": "", "open_questions": ""},
                "relationships": [],
                "timeline": [],
                "support_map": {"summary": ["claim1"], "background": ["claim1"], "role_in_story": [], "relationships": [], "timeline": [], "open_questions": []},
            }

            with patch("pipeline.stage_11_card_synthesis.call_model_chat", return_value=model_card):
                with self.assertRaisesRegex(RuntimeError, "unsupported acronym expansion"):
                    run_stage_11(
                        root / "resolved_entities.json",
                        root / "claim_drafts.json",
                        root / "claim_decisions.json",
                        root / "card_decisions.json",
                        root / "directives.json",
                        root / "memory.json",
                        root / "card_drafts.json",
                        root / "canonical_cards.json",
                        root / "merge_log.jsonl",
                        None,
                    )

    def test_stage_11_acronym_guard_allows_parenthetical_continuity(self) -> None:
        entity = {"canonical_name": "ACHILLES", "aliases": []}
        claims = [
            {
                "claim_id": "claim1",
                "claim_text": "ACHILLES renames itself to RUINR during the second quest.",
            }
        ]
        synthesis = {
            "summary": "ACHILLES (and later RUINR) begins as a Krypteia AI patch.",
            "sections": {},
        }

        self.assertEqual(find_unsupported_acronym_expansions(entity, claims, {}, synthesis), [])

    def test_stage_11_drops_unclaimed_open_questions(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write_json(
                root / "resolved_entities.json",
                {
                    "resolved_entities": [
                        {
                            "entity_id": "entity_hectr",
                            "card_id": "card_hectr",
                            "canonical_name": "HECTR",
                            "entity_type": "character",
                            "aliases": [],
                            "resolution_status": "resolved",
                        }
                    ]
                },
            )
            claim = {
                "claim_id": "claim1",
                "target_entity_id": "entity_hectr",
                "target_card_id": "card_hectr",
                "target_entity_name": "HECTR",
                "knowledge_track": "lore",
                "claim_text": "HECTR is a template ancestor for Krypteia AI systems.",
                "claim_type": "relationship",
                "source_snippet_ids": ["s1"],
                "confidence": 0.82,
                "status": "draft",
                "contradiction_notes": "",
                "created_at_utc": "2026-05-16T00:00:00Z",
            }
            write_json(root / "claim_drafts.json", {"claims": [claim]})
            write_json(root / "claim_decisions.json", {"decisions": [{"claim_id": "claim1", "decision": "accept", "reviewer": "r", "rationale": "ok"}]})
            write_json(root / "card_decisions.json", {"decisions": []})
            write_json(root / "directives.json", {"directives": []})
            write_json(root / "memory.json", {"version": 1, "accepted_claims": [], "rejected_claims": [], "approved_aliases": [], "entity_merges": [], "approved_cards": [], "author_directives": [], "style_corrections": [], "updated_at_utc": "2026-05-16T00:00:00Z"})
            model_card = {
                "summary": "HECTR is a template ancestor for Krypteia AI systems.",
                "sections": {"background": "HECTR is a template ancestor for Krypteia AI systems.", "role_in_story": "", "relationships": "", "timeline": "", "open_questions": "What other systems use HECTR?"},
                "relationships": [],
                "timeline": [],
                "support_map": {"summary": ["claim1"], "background": ["claim1"], "role_in_story": [], "relationships": [], "timeline": [], "open_questions": ["claim1"]},
            }

            with patch("pipeline.stage_11_card_synthesis.call_model_chat", return_value=model_card):
                run_stage_11(
                    root / "resolved_entities.json",
                    root / "claim_drafts.json",
                    root / "claim_decisions.json",
                    root / "card_decisions.json",
                    root / "directives.json",
                    root / "memory.json",
                    root / "card_drafts.json",
                    root / "canonical_cards.json",
                    root / "merge_log.jsonl",
                    None,
                )

            draft_cards = json.loads((root / "card_drafts.json").read_text(encoding="utf-8"))["cards"]
            self.assertEqual(draft_cards[0]["details"]["sections"]["open_questions"], "")
            self.assertEqual(draft_cards[0]["details"]["support_map"]["open_questions"], [])

    def test_notion_export_skips_unapproved_draft_cards(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write_json(root / "cards.json", {"cards": [{"card_id": "draft", "canonical_name": "Draft", "entity_type": "term", "status": "draft", "summary": "Nope", "source_evidence": []}]})
            write_json(root / "meta.json", {"meta_cards": []})
            write_json(root / "aliases.json", {"aliases": []})
            write_json(root / "profiles.json", {"profiles": []})
            write_jsonl(root / "snips.jsonl", [])
            write_jsonl(root / "log.jsonl", [])

            run_stage_12(root / "cards.json", root / "meta.json", root / "aliases.json", root / "snips.jsonl", root / "profiles.json", root / "log.jsonl", root / "notion.ndjson")

            self.assertEqual((root / "notion.ndjson").read_text(encoding="utf-8"), "")

    def test_notion_draft_config_reads_existing_env_aliases(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            env_path = root / ".env"
            env_path.write_text(
                "NOTION_ACCESS_TOKEN=secret-token\n"
                "NOTION_PAGE_ID=0123456789abcdef0123456789abcdef\n",
                encoding="utf-8",
            )

            with patch.dict(os.environ, {}, clear=True):
                config = notion_draft_config(None, env_path)

            self.assertEqual(config["api_key"], "secret-token")
            self.assertEqual(config["parent_page_id"], "01234567-89ab-cdef-0123-456789abcdef")

    def test_notion_draft_sync_skips_when_credentials_missing(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write_json(root / "07_review" / "card_drafts.json", {"cards": [draft_card_payload()]})

            with patch.dict(os.environ, {}, clear=True):
                report = sync_draft_cards_to_notion(
                    root,
                    config_path=None,
                    env_path=root / "missing.env",
                    state_path=root / "state.json",
                )

            self.assertEqual(report["status"], "skipped")
            self.assertEqual(report["reason"], "Missing NOTION_API_KEY.")
            self.assertTrue((ArtifactPaths(root).notion_draft_sync_report).exists())

    def test_notion_draft_sync_creates_database_and_page(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            env_path = root / ".env"
            env_path.write_text(
                "NOTION_ACCESS_TOKEN=secret-token\n"
                "NOTION_PAGE_ID=0123456789abcdef0123456789abcdef\n",
                encoding="utf-8",
            )
            write_json(root / "07_review" / "card_drafts.json", {"cards": [draft_card_payload()]})
            fake_client = FakeNotionDraftClient()

            with patch.dict(os.environ, {}, clear=True):
                report = sync_draft_cards_to_notion(
                    root,
                    config_path=None,
                    env_path=env_path,
                    client=fake_client,
                    state_path=root / "state.json",
                )

            self.assertEqual(report["status"], "complete")
            self.assertEqual(report["created_pages"], 1)
            self.assertEqual(report["updated_pages"], 0)
            self.assertTrue(report["database_created"])
            self.assertEqual(fake_client.created_databases, ["01234567-89ab-cdef-0123-456789abcdef"])
            page = next(iter(fake_client.pages.values()))
            self.assertEqual(fake_client._prop_text(page["properties"], "Card ID"), "entity_hectr")
            self.assertEqual(fake_client._prop_text(page["properties"], "Run ID"), root.name)
            text = "\n".join(notion_block_texts(page["children"]))
            self.assertIn("Draft preview only", text)
            self.assertIn("HECTR is a synthetic intelligence.", text)
            self.assertIn("Structured Relationships", text)
            self.assertIn("Wiki Links", text)

    def test_notion_draft_sync_updates_existing_page_in_place(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            env_path = root / ".env"
            env_path.write_text(
                "NOTION_ACCESS_TOKEN=secret-token\n"
                "NOTION_PAGE_ID=0123456789abcdef0123456789abcdef\n",
                encoding="utf-8",
            )
            write_json(root / "07_review" / "card_drafts.json", {"cards": [draft_card_payload("Old summary.") ]})
            fake_client = FakeNotionDraftClient()

            with patch.dict(os.environ, {}, clear=True):
                sync_draft_cards_to_notion(
                    root,
                    config_path=None,
                    env_path=env_path,
                    client=fake_client,
                    state_path=root / "state.json",
                )
                write_json(root / "07_review" / "card_drafts.json", {"cards": [draft_card_payload("New expanded summary.") ]})
                report = sync_draft_cards_to_notion(
                    root,
                    config_path=None,
                    env_path=env_path,
                    client=fake_client,
                    state_path=root / "state.json",
                )

            self.assertEqual(report["status"], "complete")
            self.assertEqual(report["created_pages"], 0)
            self.assertEqual(report["updated_pages"], 1)
            self.assertTrue(fake_client.deleted_blocks)
            page = next(iter(fake_client.pages.values()))
            text = "\n".join(notion_block_texts(page["children"]))
            self.assertIn("New expanded summary.", text)
            self.assertNotIn("Old summary.", text)

    def test_ui_pipeline_progress_marks_running_stage(self) -> None:
        progress = pipeline_progress_from_logs(
            [
                "2026-05-16 12:00:00 | INFO | [1/9] START Stage 01 Entity Bootstrap",
                "2026-05-16 12:00:01 | INFO | [1/9] DONE  Stage 01 Entity Bootstrap (1.00s)",
                "2026-05-16 12:00:02 | INFO | [2/9] START Stage 02 Message Normalization",
            ],
            "running",
            "Pipeline run started.",
        )

        states = {stage["index"]: stage["state"] for stage in progress["stages"]}
        self.assertEqual(states[1], "done")
        self.assertEqual(states[2], "current")
        self.assertEqual(states[3], "waiting")
        self.assertEqual(progress["summary"], "Running stage 2/12: Message Normalization")

        html = render_pipeline_progress_html(progress)
        self.assertIn('id="pipeline-progress"', html)
        self.assertIn('data-stage-index="2"', html)
        self.assertIn("pipeline-stage current", html)

    def test_ui_pipeline_progress_uses_stage05_model_call_heartbeat(self) -> None:
        line = (
            "13:37:51 | INFO | pipeline.stage_05_conversation_patch_notes | "
            "Stage 05 model call 2601/2644: conversation_id=conversation_1 track=lore topic=Ramasinta art messages=41."
        )
        self.assertTrue(is_pipeline_progress_log_line(line))
        progress = pipeline_progress_from_logs(
            [
                "11:00:59 | INFO | __main__ | [5/9] START Stage 05 Conversation Patch Notes",
                line,
            ],
            "running",
            "Attached to worker.",
        )

        states = {stage["index"]: stage["state"] for stage in progress["stages"]}
        self.assertEqual(states[5], "current")
        self.assertIn("model call 2601/2644", progress["summary"])

    def test_desktop_attach_finds_stage05_resume_logs(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            older = root / "run_from_stage_05_old.err.log"
            newer = root / "run_from_stage05_new.err.log"
            older.write_text("old", encoding="utf-8")
            newer.write_text("new", encoding="utf-8")

            paths = attach_log_paths_for_run(root, "run_from_stage_05")

            self.assertIn(newer, paths)
            self.assertIn(older, paths)

    def test_ui_pipeline_progress_inferrs_paused_stage07_from_artifacts(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write_json(root / "01_bootstrap" / "entity_seed.json", {"entities": []})
            write_jsonl(root / "02_timeline" / "messages_normalized_per_thread.jsonl", [])
            write_jsonl(root / "02_timeline" / "messages_global_timeline.jsonl", [])
            write_json(root / "02_timeline" / "conversation_segments.json", {"segments": []})
            write_json(root / "02_timeline" / "conversation_patch_notes.json", {"status": "complete", "conversation_count": 1, "notes_count": 1, "failure_count": 0, "notes": []})
            write_jsonl(root / "03_relevance" / "snippets_candidates.jsonl", [])
            write_json(root / "05_alias" / "resolved_entities.json", {"resolved_entities": []})
            write_json(root / "05_alias" / "conversation_entity_proposals.json", {"proposals": [{"proposal_id": "p1", "review_status": "pending"}]})
            write_json(root / "05_alias" / "conversation_entity_decisions.json", {"decisions": []})

            snapshot = pipeline_progress_artifact_snapshot(root)
            progress = pipeline_progress_from_logs(snapshot["logs"], snapshot["status"], snapshot["message"])

            states = {stage["index"]: stage["state"] for stage in progress["stages"]}
            self.assertEqual(snapshot["status"], "review_required")
            self.assertEqual(states[6], "done")
            self.assertEqual(states[7], "attention")
        self.assertIn("stage 7/12", progress["summary"])

    def test_ui_pipeline_progress_marks_review_gate_as_attention(self) -> None:
        progress = pipeline_progress_from_logs(
            [
                "2026-05-16 12:00:00 | INFO | [1/9] START Stage 01 Entity Bootstrap",
                "2026-05-16 12:00:01 | INFO | [1/9] DONE  Stage 01 Entity Bootstrap (1.00s)",
                "2026-05-16 12:00:02 | INFO | [2/9] START Stage 02 Message Normalization",
                "2026-05-16 12:00:03 | INFO | [2/9] DONE  Stage 02 Message Normalization (1.00s)",
                "2026-05-16 12:00:04 | INFO | [3/9] START Stage 03 Timeline Merge",
                "2026-05-16 12:00:05 | INFO | [3/9] DONE  Stage 03 Timeline Merge (1.00s)",
                "2026-05-16 12:00:06 | INFO | [4/9] START Stage 04 Conversation Segmentation",
                "2026-05-16 12:00:07 | INFO | [4/9] DONE  Stage 04 Conversation Segmentation (1.00s)",
                "2026-05-16 12:00:08 | INFO | [5/9] START Stage 05 Conversation Patch Notes",
                "2026-05-16 12:00:09 | INFO | [5/9] DONE  Stage 05 Conversation Patch Notes (1.00s)",
                "2026-05-16 12:00:10 | INFO | [6/9] START Stage 06 Snippet Extraction",
                "2026-05-16 12:00:11 | INFO | [6/9] DONE  Stage 06 Snippet Extraction (1.00s)",
                "2026-05-16 12:00:12 | INFO | [7/9] START Stage 07 Entity Resolution",
                "RuntimeError: Stage 07 found 2 conversation entity proposal(s) requiring review",
            ],
            "failed",
            "Pipeline failed with exit code 1.",
            1,
        )

        states = {stage["index"]: stage["state"] for stage in progress["stages"]}
        self.assertEqual(states[6], "done")
        self.assertEqual(states[7], "attention")
        self.assertEqual(states[8], "waiting")
        self.assertTrue(progress["review_gate"])
        self.assertEqual(progress["summary"], "Paused for review at stage 7/12: Entity Resolution")

    def test_ui_pipeline_progress_shows_identity_merge_as_distinct_done_stage(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write_json(root / "01_bootstrap" / "entity_seed.json", {"entities": []})
            write_jsonl(root / "02_timeline" / "messages_normalized_per_thread.jsonl", [])
            write_jsonl(root / "02_timeline" / "messages_global_timeline.jsonl", [])
            write_json(root / "02_timeline" / "conversation_segments.json", {"segments": []})
            write_json(
                root / "02_timeline" / "conversation_patch_notes.json",
                {"status": "complete", "conversation_count": 1, "notes_count": 1, "failure_count": 0, "notes": []},
            )
            write_jsonl(root / "03_relevance" / "snippets_candidates.jsonl", [])
            write_json(root / "05_alias" / "resolved_entities.json", {"resolved_entities": []})
            write_json(root / "06_drafts" / "card_drafts" / "claim_drafts.json", {"claims": []})
            write_json(
                root / "07_review" / "identity_merge_proposals.json",
                {"proposals": [{"proposal_id": "merge_1", "review_status": "approved"}]},
            )
            write_json(
                root / "07_review" / "identity_merge_decisions.json",
                {"decisions": [{"proposal_id": "merge_1", "decision": "approve"}]},
            )

            snapshot = pipeline_progress_artifact_snapshot(root)
            progress = pipeline_progress_from_logs(snapshot["logs"], snapshot["status"], snapshot["message"])

        states = {stage["index"]: stage["state"] for stage in progress["stages"]}
        names = {stage["index"]: stage["name"] for stage in progress["stages"]}
        self.assertEqual(progress["total_stages"], 12)
        self.assertEqual(names[10], "Identity Merge")
        self.assertEqual(states[10], "done")
        self.assertEqual(states[11], "waiting")

    def test_ui_pipeline_progress_uses_worker_failure_for_card_synthesis_state(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write_json(root / "01_bootstrap" / "entity_seed.json", {"entities": []})
            write_jsonl(root / "02_timeline" / "messages_normalized_per_thread.jsonl", [])
            write_jsonl(root / "02_timeline" / "messages_global_timeline.jsonl", [])
            write_json(root / "02_timeline" / "conversation_segments.json", {"segments": []})
            write_json(
                root / "02_timeline" / "conversation_patch_notes.json",
                {"status": "complete", "conversation_count": 1, "notes_count": 1, "failure_count": 0, "notes": []},
            )
            write_jsonl(root / "03_relevance" / "snippets_candidates.jsonl", [])
            write_json(root / "05_alias" / "resolved_entities.json", {"resolved_entities": []})
            write_json(root / "06_drafts" / "card_drafts" / "claim_drafts.json", {"claims": []})
            write_json(
                root / "07_review" / "identity_merge_proposals.json",
                {"proposals": [{"proposal_id": "merge_1", "review_status": "approved"}]},
            )
            write_json(
                root / "07_review" / "identity_merge_decisions.json",
                {"decisions": [{"proposal_id": "merge_1", "decision": "approve"}]},
            )
            (root / "tauri_pipeline_worker.log").write_text(
                "\n".join(
                    [
                        "03:24:22 | INFO | __main__ | [10/11] START Stage 10 Card Synthesis",
                        "03:29:06 | INFO | pipeline.stage_g_merge_engine | Stage 10 progress: 5/242 synthesizing cards draft_cards=4 failures=1",
                        "1779244235 | desktop: Pipeline stopped with exit code 1.",
                    ]
                ),
                encoding="utf-8",
            )

            snapshot = pipeline_progress_artifact_snapshot(root)
            progress = pipeline_progress_from_logs(snapshot["logs"], snapshot["status"], snapshot["message"])

        states = {stage["index"]: stage["state"] for stage in progress["stages"]}
        self.assertEqual(progress["status"], "failed")
        self.assertEqual(progress["summary"], "Failed at stage 11/12: Card Synthesis")
        self.assertEqual(states[10], "done")
        self.assertEqual(states[11], "failed")

    def test_tauri_draft_card_viewer_reads_stage11_partial_checkpoint(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            repo = Path(tmp)
            root = repo / "artifacts" / "runs" / "draft_run"
            paths = ArtifactPaths(root)
            write_json(
                paths.stage11 / "card_drafts.partial.json",
                {
                    "status": "running",
                    "processed_count": 2,
                    "total_count": 5,
                    "current_entity_name": "HECTR",
                    "cards": [draft_card_payload("Live partial summary.")],
                },
            )
            write_json(
                paths.stage11 / "card_synthesis_failures.json",
                {"failures": [{"entity_id": "entity_x", "error": "model timeout"}]},
            )

            result = handle_request(
                {
                    "repo_root": str(repo),
                    "command": "draft_cards",
                    "payload": {"artifacts_root": str(root)},
                }
            )

        self.assertEqual(result["total"], 1)
        self.assertEqual(result["metadata"]["source_kind"], "partial")
        self.assertEqual(result["metadata"]["processed_count"], 2)
        self.assertEqual(result["metadata"]["failure_count"], 1)
        self.assertEqual(result["cards"][0]["summary"], "Live partial summary.")
        self.assertGreater(result["cards"][0]["word_count"], 0)

    def test_ui_pipeline_progress_marks_claim_step_complete_when_review_is_bypassed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write_json(root / "01_bootstrap" / "entity_seed.json", {"entities": []})
            write_jsonl(root / "02_timeline" / "messages_normalized_per_thread.jsonl", [])
            write_jsonl(root / "02_timeline" / "messages_global_timeline.jsonl", [])
            write_json(root / "02_timeline" / "conversation_segments.json", {"segments": []})
            write_json(
                root / "02_timeline" / "conversation_patch_notes.json",
                {"status": "complete", "conversation_count": 1, "notes_count": 1, "failure_count": 0, "notes": []},
            )
            write_jsonl(root / "03_relevance" / "snippets_candidates.jsonl", [])
            write_json(root / "05_alias" / "resolved_entities.json", {"resolved_entities": []})
            write_json(
                root / "06_drafts" / "card_drafts" / "claim_drafts.json",
                {"claims": [{"claim_id": "claim_1", "claim_text": "A pending claim."}]},
            )
            write_json(ArtifactPaths(root).claim_review_decisions, {"decisions": []})
            write_json(root / "07_review" / "review_gate_bypass.json", {"claim_review": True})

            snapshot = pipeline_progress_artifact_snapshot(root)
            progress = pipeline_progress_from_logs(snapshot["logs"], snapshot["status"], snapshot["message"])

        states = {stage["index"]: stage["state"] for stage in progress["stages"]}
        self.assertEqual(states[9], "done")
        self.assertEqual(states[10], "waiting")

    def test_desktop_candidate_inventory_browser_rows_bucket_and_categorize(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            proposals_path = root / "conversation_entity_proposals.json"
            write_json(
                proposals_path,
                {
                    "proposals": [
                        {
                            "proposal_id": "p_loss",
                            "candidate_name": "Loss",
                            "proposed_entity_type": "character",
                            "candidate_topics": ["entity"],
                            "knowledge_tracks": ["lore"],
                            "evidence_count": 7,
                            "triage_reason": "recurring or type-reconsidered character candidate",
                            "sample_texts": ["Loss is referred to with pronouns in a fight scene."],
                        },
                        {
                            "proposal_id": "p_fear",
                            "candidate_name": "Fear",
                            "proposed_entity_type": "character",
                            "candidate_topics": ["entity"],
                            "knowledge_tracks": ["lore"],
                            "evidence_count": 428,
                            "triage_reason": "kept because review_status is approved",
                            "review_status": "approved",
                            "latest_decision": {"canonical_name": "Oyuun", "decision": "approve"},
                        }
                    ],
                    "candidate_inventory": [
                        {
                            "proposal_id": "p_corinah",
                            "candidate_name": "Corinah",
                            "proposed_entity_type": "character",
                            "candidate_topics": ["entity"],
                            "knowledge_tracks": ["lore"],
                            "knowledge_track_counts": {"lore": 8},
                            "evidence_count": 8,
                            "triage_reason": "project/team contributor evidence retained as meta inventory, not lore entity review",
                            "sample_texts": ["Corinah is assigned as an artist for THERIAC and joins the art team."],
                        }
                    ],
                    "suppressed_candidates": [
                        {
                            "proposal_id": "p_player",
                            "candidate_name": "Player",
                            "proposed_entity_type": "term",
                            "candidate_topics": ["entity"],
                            "evidence_count": 2,
                            "triage_reason": "generic scaffold term",
                        }
                    ],
                },
            )

            rows = candidate_inventory_browser_rows(proposals_path)
            by_name = {row["candidate_name"]: row for row in rows}

            self.assertEqual(by_name["Loss"]["bucket"], "promoted")
            self.assertEqual(by_name["Loss"]["category"], "lore")
            self.assertEqual(by_name["Oyuun (alias: Fear)"]["raw_candidate_name"], "Fear")
            self.assertEqual(by_name["Oyuun (alias: Fear)"]["canonical_name"], "Oyuun")
            self.assertEqual(by_name["Corinah"]["bucket"], "demoted")
            self.assertEqual(by_name["Corinah"]["category"], "meta")
            self.assertEqual(by_name["Player"]["bucket"], "suppressed")
            self.assertEqual(candidate_inventory_category(by_name["Corinah"]["item"]), "meta")

    def test_desktop_candidate_inventory_groups_alias_proposals(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            proposals_path = root / "conversation_entity_proposals.json"
            write_json(
                proposals_path,
                {
                    "alias_review_groups": [
                        {
                            "proposal_id": "alias_review_group_khava",
                            "group_kind": "alias_review_group",
                            "candidate_name": "Khava Zimov aliases (2)",
                            "suggested_canonical_name": "Khava Zimov",
                            "proposed_entity_type": "character",
                            "evidence_count": 121,
                            "triage_reason": "2 alias candidates proposed for Khava Zimov",
                            "review_priority": "high",
                            "child_proposal_ids": ["p_eve", "p_khava"],
                            "alias_candidates": [
                                {"proposal_id": "p_eve", "candidate_name": "Eve", "evidence_count": 40},
                                {"proposal_id": "p_khava", "candidate_name": "Khava", "evidence_count": 81},
                            ],
                        }
                    ],
                    "proposals": [
                        {
                            "proposal_id": "p_eve",
                            "candidate_name": "Eve",
                            "suggested_canonical_name": "Khava Zimov",
                            "proposed_entity_type": "character",
                            "evidence_count": 40,
                            "triage_reason": "alias/rename evidence suggests this is an alias of Khava Zimov",
                        },
                        {
                            "proposal_id": "p_khava",
                            "candidate_name": "Khava",
                            "suggested_canonical_name": "Khava Zimov",
                            "proposed_entity_type": "character",
                            "evidence_count": 81,
                            "triage_reason": "alias/rename evidence suggests this is an alias of Khava Zimov",
                        },
                    ],
                    "candidate_inventory": [],
                    "suppressed_candidates": [],
                },
            )

            rows = candidate_inventory_browser_rows(proposals_path)
            names = [row["candidate_name"] for row in rows]

            self.assertEqual(names, ["Khava Zimov aliases (2)"])
            self.assertEqual(rows[0]["bucket"], "promoted")
            self.assertEqual(rows[0]["canonical_name"], "Khava Zimov")
            self.assertEqual(rows[0]["evidence_count"], 121)

    def test_desktop_candidate_inventory_override_writes_human_decision(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            proposals_path = root / "conversation_entity_proposals.json"
            decisions_path = root / "conversation_entity_decisions.json"
            write_json(
                proposals_path,
                {
                    "proposals": [],
                    "candidate_inventory": [
                        {
                            "proposal_id": "p_corinah",
                            "candidate_name": "Corinah",
                            "proposed_entity_type": "character",
                            "candidate_topics": ["entity"],
                            "knowledge_tracks": ["lore"],
                            "evidence_count": 8,
                        }
                    ],
                    "suppressed_candidates": [],
                },
            )
            write_json(decisions_path, {"decisions": []})
            row = candidate_inventory_browser_rows(proposals_path, decisions_path)[0]

            written = write_candidate_inventory_override_decision(
                decisions_path,
                row,
                "reject",
                "Corinah",
                "character",
                "Dylan",
                "Team artist, not an in-world entity.",
                timestamp_utc="2026-05-18T00:00:00Z",
            )

            self.assertEqual(written, 1)
            decisions = json.loads(decisions_path.read_text(encoding="utf-8"))["decisions"]
            self.assertEqual(decisions[0]["proposal_id"], "p_corinah")
            self.assertEqual(decisions[0]["decision"], "reject")
            self.assertTrue(decisions[0]["human_override"])

            rows = candidate_inventory_browser_rows(proposals_path, decisions_path)
            self.assertEqual(rows[0]["decision"], "reject")

    def test_desktop_candidate_inventory_override_writes_alias_group_children(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            proposals_path = root / "conversation_entity_proposals.json"
            decisions_path = root / "conversation_entity_decisions.json"
            write_json(
                proposals_path,
                {
                    "alias_review_groups": [
                        {
                            "proposal_id": "alias_group_enoch",
                            "group_kind": "alias_review_group",
                            "candidate_name": "Enoch aliases (2)",
                            "suggested_canonical_name": "Enoch",
                            "proposed_entity_type": "character",
                            "child_proposal_ids": ["p_metatron", "p_loss"],
                            "alias_candidates": [
                                {"proposal_id": "p_metatron", "candidate_name": "Metatron"},
                                {"proposal_id": "p_loss", "candidate_name": "Loss"},
                            ],
                        }
                    ],
                    "proposals": [
                        {"proposal_id": "p_metatron", "candidate_name": "Metatron", "suggested_canonical_name": "Enoch"},
                        {"proposal_id": "p_loss", "candidate_name": "Loss", "suggested_canonical_name": "Enoch"},
                    ],
                    "candidate_inventory": [],
                    "suppressed_candidates": [],
                },
            )
            write_json(decisions_path, {"decisions": []})
            row = candidate_inventory_browser_rows(proposals_path, decisions_path)[0]

            written = write_candidate_inventory_override_decision(
                decisions_path,
                row,
                "approve",
                "Enoch",
                "character",
                "Dylan",
                "Both names are aliases.",
                timestamp_utc="2026-05-18T00:00:00Z",
            )

            self.assertEqual(written, 2)
            decisions = json.loads(decisions_path.read_text(encoding="utf-8"))["decisions"]
            self.assertEqual({decision["proposal_id"] for decision in decisions}, {"p_metatron", "p_loss"})
            self.assertTrue(all(decision["canonical_name"] == "Enoch" for decision in decisions))

    def test_stage_07_prefers_human_override_over_later_auto_review(self) -> None:
        proposals = [{"proposal_id": "p_loss", "candidate_name": "Loss", "normalized_name_key": "loss"}]
        decisions = [
            {
                "proposal_id": "p_loss",
                "candidate_name": "Loss",
                "decision": "approve",
                "canonical_name": "Enoch",
                "entity_type": "character",
                "reviewer": "Dylan",
                "human_override": True,
            },
            {
                "proposal_id": "p_loss",
                "candidate_name": "Loss",
                "decision": "reject",
                "canonical_name": "Loss",
                "entity_type": "theme",
                "reviewer": "gemini_auto_review",
            },
        ]

        annotated = annotate_conversation_entity_proposals(proposals, decisions)

        self.assertEqual(annotated[0]["review_status"], "approved")
        self.assertEqual(annotated[0]["latest_decision"]["canonical_name"], "Enoch")

    def test_desktop_candidate_inventory_sort_orders_evidence_numerically(self) -> None:
        rows = [
            {"candidate_name": "Beta", "evidence_count": 2, "bucket": "demoted"},
            {"candidate_name": "Alpha", "evidence_count": 12, "bucket": "promoted"},
            {"candidate_name": "Gamma", "evidence_count": 5, "bucket": "demoted"},
        ]

        descending = sort_candidate_inventory_rows(rows, "evidence", True)
        ascending = sort_candidate_inventory_rows(rows, "evidence", False)

        self.assertEqual([row["candidate_name"] for row in descending], ["Alpha", "Gamma", "Beta"])
        self.assertEqual([row["candidate_name"] for row in ascending], ["Beta", "Gamma", "Alpha"])

    def test_entity_type_ai_system_legacy_values_fold_into_character(self) -> None:
        self.assertEqual(normalize_entity_type("ai_system"), "character")
        self.assertEqual(normalize_entity_type("ai system"), "character")
        self.assertEqual(normalize_entity_type("ai systems"), "character")

    def test_uppercase_candidate_does_not_create_ai_system_vote(self) -> None:
        votes = infer_type_evidence_for_candidate(
            "RAM",
            {
                "snippet_id": "s_ram",
                "candidate_topics": ["entity"],
                "knowledge_track": "meta",
                "display_text_normalized": "We may need more RAM for local model testing.",
            },
        )

        self.assertNotIn("ai_system", {vote["entity_type"] for vote in votes})

    def test_desktop_candidate_inventory_includes_auto_review_attention_bucket(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            proposals_path = root / "conversation_entity_proposals.json"
            write_json(
                proposals_path,
                {
                    "proposals": [
                        {
                            "proposal_id": "p_lab",
                            "candidate_name": "The Lab",
                            "proposed_entity_type": "location",
                            "evidence_count": 42,
                            "candidate_topics": ["entity"],
                            "knowledge_tracks": ["lore"],
                            "sample_texts": ["The Lab is a facility and an institution."],
                        }
                    ],
                    "alias_review_groups": [],
                    "candidate_inventory": [],
                    "suppressed_candidates": [],
                },
            )
            write_json(
                root / "conversation_entity_auto_review_attention.json",
                {
                    "items": [
                        {
                            "proposal_id": "p_lab",
                            "candidate_name": "The Lab",
                            "decision": "approve",
                            "canonical_name": "The Lab",
                            "entity_type": "location",
                            "human_review_reason": "type-conflicted evidence needs human review: faction",
                        }
                    ]
                },
            )

            rows = candidate_inventory_browser_rows(proposals_path)
            attention_rows = [row for row in rows if row["bucket"] == "attention"]

            self.assertEqual(len(attention_rows), 1)
            self.assertEqual(attention_rows[0]["candidate_name"], "The Lab")
            self.assertEqual(attention_rows[0]["evidence_count"], 42)
            self.assertIn("type-conflicted", attention_rows[0]["triage_reason"])

    def test_auto_review_writes_low_evidence_best_guess_with_attention_flag(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write_json(
                root / "conversation_entity_proposals.json",
                {
                    "proposals": [
                        {
                            "proposal_id": "p_supply_road",
                            "candidate_name": "Supply Road",
                            "review_status": "pending",
                            "proposed_entity_type": "term",
                            "evidence_count": 1,
                            "sample_texts": ["A supply road is mentioned once as part of the facility layout."],
                        }
                    ],
                    "alias_review_groups": [],
                },
            )
            write_json(root / "conversation_entity_decisions.json", {"decisions": []})
            result = AutoReviewResult()

            with patch(
                "pipeline.auto_review._gemini_generate",
                return_value={
                    "decision": "approve",
                    "canonical_name": "Supply Road",
                    "entity_type": "location",
                    "human_review_recommended": False,
                    "human_review_reason": "",
                    "rationale": "Best guess is a physical location.",
                },
            ) as model:
                _auto_review_conversation_entities(
                    {
                        "conversation_entity_proposals": root / "conversation_entity_proposals.json",
                        "conversation_entity_decisions": root / "conversation_entity_decisions.json",
                    },
                    "fake-key",
                    "gemini-test",
                    0,
                    result,
                    lambda _line: None,
                )
                model.assert_called_once()

            decisions = json.loads((root / "conversation_entity_decisions.json").read_text(encoding="utf-8"))["decisions"]
            self.assertEqual(len(decisions), 1)
            self.assertEqual(decisions[0]["decision"], "approve")
            self.assertEqual(decisions[0]["entity_type"], "location")
            self.assertTrue(decisions[0]["human_review_recommended"])
            self.assertIn("only 1 evidence", decisions[0]["human_review_reason"])
            attention = json.loads((root / "conversation_entity_auto_review_attention.json").read_text(encoding="utf-8"))
            self.assertEqual(attention["items"][0]["proposal_id"], "p_supply_road")
            self.assertEqual(result.skipped, 1)

    def test_auto_review_writes_type_conflicted_best_guess_to_attention_bucket(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write_json(
                root / "conversation_entity_proposals.json",
                {
                    "proposals": [
                        {
                            "proposal_id": "p_lab",
                            "candidate_name": "The Lab",
                            "review_status": "pending",
                            "proposed_entity_type": "location",
                            "evidence_count": 42,
                            "type_conflicts": [{"entity_type": "faction", "score": 6.0}],
                            "sample_texts": ["The Lab is a facility and institution."],
                        }
                    ],
                    "alias_review_groups": [],
                },
            )
            write_json(root / "conversation_entity_decisions.json", {"decisions": []})
            result = AutoReviewResult()

            with patch(
                "pipeline.auto_review._gemini_generate",
                return_value={
                    "decision": "approve",
                    "canonical_name": "The Lab",
                    "entity_type": "location",
                    "human_review_recommended": False,
                    "human_review_reason": "",
                    "rationale": "Best guess is location.",
                },
            ):
                _auto_review_conversation_entities(
                    {
                        "conversation_entity_proposals": root / "conversation_entity_proposals.json",
                        "conversation_entity_decisions": root / "conversation_entity_decisions.json",
                    },
                    "fake-key",
                    "gemini-test",
                    0,
                    result,
                    lambda _line: None,
                )

            decisions = json.loads((root / "conversation_entity_decisions.json").read_text(encoding="utf-8"))["decisions"]
            self.assertEqual(decisions[0]["decision"], "approve")
            self.assertTrue(decisions[0]["human_review_recommended"])
            self.assertIn("type-conflicted evidence", decisions[0]["human_review_reason"])
            attention = json.loads((root / "conversation_entity_auto_review_attention.json").read_text(encoding="utf-8"))
            self.assertEqual(attention["items"][0]["candidate_name"], "The Lab")
            self.assertEqual(result.skipped, 1)

    def test_auto_review_needs_more_context_response_writes_best_guess_attention_decision(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write_json(
                root / "conversation_entity_proposals.json",
                {
                    "proposals": [
                        {
                            "proposal_id": "p_lab",
                            "candidate_name": "Longevity Clinic",
                            "review_status": "pending",
                            "proposed_entity_type": "organization",
                            "evidence_count": 6,
                            "sample_texts": ["The clinic appears in several lore notes."],
                        }
                    ],
                    "alias_review_groups": [],
                },
            )
            write_json(root / "conversation_entity_decisions.json", {"decisions": []})
            result = AutoReviewResult()

            with patch(
                "pipeline.auto_review._gemini_generate",
                return_value={
                    "decision": "needs_more_context",
                    "canonical_name": "Longevity Clinic",
                    "entity_type": "organization",
                    "rationale": "Evidence is still ambiguous.",
                },
            ):
                _auto_review_conversation_entities(
                    {
                        "conversation_entity_proposals": root / "conversation_entity_proposals.json",
                        "conversation_entity_decisions": root / "conversation_entity_decisions.json",
                    },
                    "fake-key",
                    "gemini-test",
                    0,
                    result,
                    lambda _line: None,
                )

            decisions = json.loads((root / "conversation_entity_decisions.json").read_text(encoding="utf-8"))["decisions"]
            self.assertEqual(len(decisions), 1)
            self.assertEqual(decisions[0]["decision"], "approve")
            self.assertEqual(decisions[0]["entity_type"], "location")
            self.assertTrue(decisions[0]["human_review_recommended"])
            self.assertIn("model requested human review", decisions[0]["human_review_reason"])
            self.assertIn("organization", decisions[0]["secondary_entity_types"])
            self.assertEqual(result.skipped, 1)

    def test_auto_review_writes_thin_alias_group_best_guess_with_attention_flag(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write_json(
                root / "conversation_entity_proposals.json",
                {
                    "alias_review_groups": [
                        {
                            "proposal_id": "alias_group_enoch",
                            "group_kind": "alias_review_group",
                            "candidate_name": "Enoch aliases (2)",
                            "suggested_canonical_name": "Enoch",
                            "proposed_entity_type": "character",
                            "review_status": "pending",
                            "child_proposal_ids": ["p_metatron", "p_son_of_man"],
                            "alias_candidates": [
                                {"proposal_id": "p_metatron", "candidate_name": "Metatron", "evidence_count": 12},
                                {"proposal_id": "p_son_of_man", "candidate_name": "Son Of Man", "evidence_count": 1},
                            ],
                        }
                    ],
                    "proposals": [
                        {"proposal_id": "p_metatron", "candidate_name": "Metatron", "review_status": "pending", "evidence_count": 12},
                        {"proposal_id": "p_son_of_man", "candidate_name": "Son Of Man", "review_status": "pending", "evidence_count": 1},
                    ],
                },
            )
            write_json(root / "conversation_entity_decisions.json", {"decisions": []})
            result = AutoReviewResult()

            with patch(
                "pipeline.auto_review._gemini_generate",
                return_value={
                    "decision": "approve",
                    "canonical_name": "Enoch",
                    "entity_type": "character",
                    "human_review_recommended": False,
                    "human_review_reason": "",
                    "rationale": "Best guess is that these are aliases for Enoch.",
                },
            ) as model:
                _auto_review_conversation_entities(
                    {
                        "conversation_entity_proposals": root / "conversation_entity_proposals.json",
                        "conversation_entity_decisions": root / "conversation_entity_decisions.json",
                    },
                    "fake-key",
                    "gemini-test",
                    0,
                    result,
                    lambda _line: None,
                )
                model.assert_called_once()

            decisions = json.loads((root / "conversation_entity_decisions.json").read_text(encoding="utf-8"))["decisions"]
            self.assertEqual(len(decisions), 2)
            self.assertTrue(all(decision["human_review_recommended"] for decision in decisions))
            self.assertTrue(all("low-evidence child" in decision["human_review_reason"] for decision in decisions))
            self.assertEqual(result.skipped, 1)

    def test_auto_review_claims_include_source_context_and_group_duplicates(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            claims_path = root / "06_drafts" / "card_drafts" / "claim_drafts.json"
            decisions_path = ArtifactPaths(root).claim_review_decisions
            snippets_path = root / "03_relevance" / "snippets_candidates.jsonl"
            claims = [
                {
                    "claim_id": "claim_lab_1",
                    "target_entity_id": "entity_lab",
                    "target_entity_name": "The Lab",
                    "claim_text": "The Lab researches age-related disease cures.",
                    "normalized_claim_text": "the lab researches age related disease cures",
                    "claim_type": "background",
                    "confidence": 0.91,
                    "source_snippet_ids": ["snippet_lab"],
                    "support_warnings": ["proper_name_not_in_evidence:Uncited Name"],
                },
                {
                    "claim_id": "claim_lab_2",
                    "target_entity_id": "entity_lab",
                    "target_entity_name": "The Lab",
                    "claim_text": "The Lab researches age-related disease cures.",
                    "normalized_claim_text": "the lab researches age related disease cures",
                    "claim_type": "background",
                    "confidence": 0.91,
                    "source_snippet_ids": ["snippet_lab"],
                    "support_warnings": [],
                },
                {
                    "claim_id": "claim_lab_3",
                    "target_entity_id": "entity_lab",
                    "target_entity_name": "The Lab",
                    "claim_text": "The Lab gives the player an ethical dilemma.",
                    "normalized_claim_text": "the lab gives the player an ethical dilemma",
                    "claim_type": "theme",
                    "confidence": 0.88,
                    "source_snippet_ids": ["snippet_lab"],
                    "support_warnings": [],
                },
            ]
            write_json(claims_path, {"claims": claims})
            write_json(decisions_path, {"decisions": []})
            snippets_path.parent.mkdir(parents=True, exist_ok=True)
            snippets_path.write_text(
                json.dumps(
                    {
                        "snippet_id": "snippet_lab",
                        "conversation_id": "conversation_1",
                        "conversation_global_index": 4,
                        "timestamp_start_utc": "2025-01-01T00:00:00Z",
                        "conversation_topic_label": "Lab ethics",
                        "conversation_topic_summary": "The team discusses the lab's longevity research dilemma.",
                        "knowledge_track": "lore",
                        "source_kind": "patch_note_lore_development",
                        "patch_item_type": "lore_development",
                        "patch_item_text": "The Lab researches cures for age-related diseases and complicates the player's mission.",
                        "raw_text": "The lab is working to cure the very disease your friend is dying of.",
                    },
                    ensure_ascii=False,
                )
                + "\n",
                encoding="utf-8",
            )
            prompts: list[str] = []

            def fake_model(_api_key: str, prompt: str, *, model: str) -> dict[str, object]:
                prompts.append(prompt)
                return {
                    "decision": "accept",
                    "human_review_recommended": False,
                    "human_review_reason": "",
                    "rationale": "The source directly supports the claim.",
                }

            result = AutoReviewResult()
            with patch("pipeline.auto_review._gemini_generate", side_effect=fake_model) as model:
                _auto_review_claims(
                    {"patches": claims_path, "decisions": decisions_path},
                    "fake-key",
                    "gemini-test",
                    0,
                    result,
                    lambda _line: None,
                )

            self.assertEqual(model.call_count, 2)
            self.assertIn("The Lab researches cures for age-related diseases", prompts[0])
            self.assertIn('"duplicate_group_size": 2', prompts[0])
            self.assertIn('"source_set_group_size": 3', prompts[0])
            self.assertIn("The Lab gives the player an ethical dilemma", prompts[0])
            decisions = json.loads(decisions_path.read_text(encoding="utf-8"))["decisions"]
            self.assertEqual({decision["claim_id"] for decision in decisions}, {"claim_lab_1", "claim_lab_2", "claim_lab_3"})
            first_decision = next(decision for decision in decisions if decision["claim_id"] == "claim_lab_1")
            self.assertTrue(first_decision["human_review_recommended"])
            self.assertIn("support warnings", first_decision["human_review_reason"])
            self.assertIn("exact duplicate group", first_decision["human_review_reason"])
            attention = json.loads((decisions_path.parent / "claim_auto_review_attention.json").read_text(encoding="utf-8"))
            self.assertEqual({item["claim_id"] for item in attention["items"]}, {"claim_lab_1", "claim_lab_2"})
            self.assertEqual(result.total, 3)
            self.assertEqual(result.accepted, 3)
            self.assertEqual(result.skipped, 2)

    def test_auto_review_claims_skips_story_answered_and_author_claim_duplicates(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            claims_path = root / "06_drafts" / "card_drafts" / "claim_drafts.json"
            decisions_path = ArtifactPaths(root).claim_review_decisions
            claims = [
                {
                    "claim_id": "claim_story_answered",
                    "target_entity_name": "HECTR",
                    "claim_text": "HECTR has already been resolved by a story answer.",
                    "normalized_claim_text": "hectr has already been resolved by a story answer",
                    "claim_type": "role",
                    "source_snippet_ids": [],
                },
                {
                    "claim_id": "claim_author_duplicate",
                    "target_entity_name": "HECTR",
                    "claim_text": "The tunnel wars are part of the mycelium wars.",
                    "normalized_claim_text": "the tunnel wars are part of the mycelium wars",
                    "claim_type": "relationship",
                    "source_snippet_ids": [],
                },
                {
                    "claim_id": "claim_open",
                    "target_entity_name": "HECTR",
                    "claim_text": "HECTR still needs auto-review.",
                    "normalized_claim_text": "hectr still needs auto review",
                    "claim_type": "role",
                    "source_snippet_ids": [],
                },
            ]
            write_json(claims_path, {"claims": claims})
            write_json(decisions_path, {"decisions": []})
            write_json(
                ArtifactPaths(root).stage09 / "story_question_session.json",
                {
                    "version": 1,
                    "session_id": "story_session",
                    "status": "active",
                    "created_at_utc": "2026-05-19T00:00:00Z",
                    "updated_at_utc": "2026-05-19T00:00:00Z",
                    "current_question_id": "",
                    "questions": [
                        {
                            "question_id": "question_1",
                            "status": "answered",
                            "question_text": "Resolve HECTR?",
                            "linked_claim_ids": ["claim_story_answered"],
                        }
                    ],
                    "answers": [{"question_id": "question_1", "answer_text": "Resolved."}],
                    "applications": [],
                    "skipped_questions": [],
                    "pending_application_proposal": None,
                },
            )
            write_json(
                ArtifactPaths(root).author_claims,
                {
                    "claims": [
                        {
                            "claim_id": "author_claim_tunnel_mycelium",
                            "claim_text": "The tunnel wars are part of the mycelium wars.",
                            "normalized_claim_text": "the tunnel wars are part of the mycelium wars",
                        }
                    ]
                },
            )

            def fake_model(_api_key: str, prompt: str, *, model: str) -> dict[str, object]:
                self.assertIn("HECTR still needs auto-review", prompt)
                self.assertNotIn("already been resolved by a story answer", prompt)
                self.assertNotIn("tunnel wars are part", prompt.lower())
                return {
                    "decision": "accept",
                    "human_review_recommended": False,
                    "human_review_reason": "",
                    "rationale": "Supported.",
                }

            result = AutoReviewResult()
            with patch("pipeline.auto_review._gemini_generate", side_effect=fake_model) as model:
                _auto_review_claims(
                    {"patches": claims_path, "decisions": decisions_path},
                    "fake-key",
                    "deepseek/deepseek-v4-flash",
                    0,
                    result,
                    lambda _line: None,
                )

            self.assertEqual(model.call_count, 1)
            decisions = json.loads(decisions_path.read_text(encoding="utf-8"))["decisions"]
            self.assertEqual([decision["claim_id"] for decision in decisions], ["claim_open"])

    def test_claim_attention_queue_keeps_auto_reviewed_claim_pending_until_human_override(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            claim = {"claim_id": "claim_attention", "claim_text": "A claim needing human eyes."}
            write_json(root / "06_drafts" / "card_drafts" / "claim_drafts.json", {"claims": [claim]})
            write_json(
                ArtifactPaths(root).claim_review_decisions,
                {
                    "decisions": [
                        {
                            "claim_id": "claim_attention",
                            "decision": "accept",
                            "reviewer": "gemini_auto_review",
                            "human_review_recommended": True,
                        }
                    ]
                },
            )
            write_json(
                root / "07_review" / "claim_auto_review_attention.json",
                {"items": [{"claim_id": "claim_attention", "human_review_reason": "support warnings"}]},
            )

            self.assertEqual(pending_review_counts_for_root(root)["claims"], 1)

            write_json(
                ArtifactPaths(root).claim_review_decisions,
                {
                    "decisions": [
                        {
                            "claim_id": "claim_attention",
                            "decision": "accept",
                            "reviewer": "gemini_auto_review",
                            "human_review_recommended": True,
                        },
                        {
                            "claim_id": "claim_attention",
                            "decision": "accept",
                            "reviewer": "human_reviewer",
                            "rationale": "Confirmed.",
                        },
                    ]
                },
            )
            self.assertEqual(pending_review_counts_for_root(root)["claims"], 0)

    def test_candidate_inventory_claim_rows_and_override(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            claim = {
                "claim_id": "claim_attention",
                "target_entity_name": "The Lab",
                "knowledge_track": "lore",
                "claim_text": "The Lab researches age-related disease cures.",
                "claim_type": "background",
                "confidence": 0.9,
                "source_snippet_ids": ["snippet_lab", "snippet_lab_2"],
                "support_warnings": ["proper_name_not_in_evidence:Example"],
                "thematic_tags": ["longevity"],
            }
            claims_path = root / "06_drafts" / "card_drafts" / "claim_drafts.json"
            decisions_path = ArtifactPaths(root).claim_review_decisions
            write_json(claims_path, {"claims": [claim]})
            write_json(
                decisions_path,
                {
                    "decisions": [
                        {
                            "claim_id": "claim_attention",
                            "decision": "accept",
                            "reviewer": "gemini_auto_review",
                            "human_review_recommended": True,
                        }
                    ]
                },
            )
            write_json(
                root / "07_review" / "claim_auto_review_attention.json",
                {
                    "items": [
                        {
                            "claim_id": "claim_attention",
                            "decision": "accept",
                            "human_review_reason": "support warnings",
                        }
                    ]
                },
            )

            rows = claim_inventory_browser_rows(claims_path, decisions_path, root)
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["row_kind"], "claim")
            self.assertEqual(rows[0]["bucket"], "attention")
            self.assertEqual(rows[0]["category"], "lore")
            self.assertEqual(rows[0]["evidence_count"], 2)
            self.assertIn("support warnings", rows[0]["triage_reason"])

            written = write_claim_inventory_override_decision(
                decisions_path,
                rows[0],
                "reject",
                "human_reviewer",
                "This was too broad.",
                timestamp_utc="2026-01-01T00:00:00Z",
            )
            self.assertEqual(written, 1)
            rows_after = claim_inventory_browser_rows(claims_path, decisions_path, root)
            self.assertEqual(rows_after[0]["bucket"], "rejected")
            self.assertEqual(rows_after[0]["decision"], "reject")

    def test_author_claims_are_visible_in_claim_inventory(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write_json(
                root / "05_alias" / "resolved_entities.json",
                {
                    "resolved_entities": [
                        {
                            "entity_id": "entity_tunnel_wars",
                            "card_id": "card_tunnel_wars",
                            "canonical_name": "Tunnel Wars",
                            "entity_type": "event",
                            "aliases": ["The Tunnel War"],
                        }
                    ]
                },
            )
            claims_path = root / "06_drafts" / "card_drafts" / "claim_drafts.json"
            decisions_path = ArtifactPaths(root).claim_review_decisions
            write_json(claims_path, {"claims": []})
            write_json(decisions_path, {"decisions": []})

            author_claim = append_author_claim(
                root,
                "The Tunnel War",
                "relationship",
                "The tunnel wars are part of the Mycelium Wars.",
                "human_reviewer",
                "Author correction.",
                timestamp_utc="2026-05-18T12:00:00Z",
            )
            rows = claim_inventory_browser_rows(claims_path, decisions_path, root)

            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["source_bucket"], "author_claims")
            self.assertEqual(rows[0]["bucket"], "accepted")
            self.assertEqual(rows[0]["decision"], "accept")
            self.assertEqual(rows[0]["canonical_name"], "Tunnel Wars")
            self.assertEqual(rows[0]["item"]["claim_id"], author_claim["claim_id"])

    def test_ui_discovers_runs_with_pending_review_items(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            repo = Path(tmp)
            run_a = repo / "artifacts" / "run_a"
            run_b = repo / "artifacts" / "run_b"
            write_json(
                run_a / "06_drafts" / "card_drafts" / "claim_drafts.json",
                {"claims": [{"claim_id": "claim_a"}, {"claim_id": "claim_b"}]},
            )
            write_json(
                run_a / "07_review" / "claim_review_decisions.json",
                {"decisions": [{"claim_id": "claim_a", "decision": "accept"}]},
            )
            write_json(
                run_b / "05_alias" / "conversation_entity_proposals.json",
                {"proposals": [{"proposal_id": "entity_prop", "review_status": "pending"}]},
            )
            write_json(run_b / "05_alias" / "conversation_entity_decisions.json", {"decisions": []})

            counts_a = pending_review_counts_for_root(run_a)
            self.assertEqual(counts_a["claims"], 1)
            self.assertEqual(pending_review_counts_for_root(run_b)["conversation_entities"], 1)

            runs = discover_review_runs(repo, run_a)
            labels = {run["label"]: run["pending_total"] for run in runs}
            self.assertEqual(labels[str(Path("artifacts") / "run_a")], 1)
            self.assertEqual(labels[str(Path("artifacts") / "run_b")], 1)

    def test_ui_discovers_completed_review_runs_with_no_pending_items(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            repo = Path(tmp)
            active = repo / "artifacts"
            completed = repo / "artifacts" / "runs" / "completed_full"
            write_json(
                completed / "05_alias" / "conversation_entity_proposals.json",
                {"proposals": [{"proposal_id": "p_done", "review_status": "pending"}]},
            )
            write_json(
                completed / "05_alias" / "conversation_entity_decisions.json",
                {"decisions": [{"proposal_id": "p_done", "decision": "approve"}]},
            )

            runs = discover_review_runs(repo, active)
            labels = {run["label"]: run["pending_total"] for run in runs}

            self.assertEqual(labels[str(Path("artifacts") / "runs" / "completed_full")], 0)

    def test_ui_run_selector_includes_new_run_option(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            repo = Path(tmp)
            run_a = repo / "artifacts" / "run_a"
            write_json(run_a / "06_drafts" / "card_drafts" / "claim_drafts.json", {"claims": [{"claim_id": "claim_a"}]})
            runs = discover_review_runs(repo, run_a)

            html = render_run_selector_html(runs, run_a, repo, new_run_selected=True)

            self.assertIn("New Run - create a fresh timestamped artifact folder", html)
            self.assertIn("__theriac_new_run__", html)
            self.assertIn("New run selected", html)

    def test_pipeline_resume_ignores_legacy_entity_decisions_after_07a_harvest(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write_json(root / "01_bootstrap" / "entity_seed.json", {"entities": []})
            write_json(root / "02_timeline" / "summary.json", {})
            write_jsonl(root / "02_timeline" / "messages_normalized_per_thread.jsonl", [{"message_id": "m1"}])
            write_json(root / "02_timeline" / "global_index.json", {})
            write_jsonl(root / "02_timeline" / "messages_global_timeline.jsonl", [{"message_id": "m1"}])
            write_json(root / "02_timeline" / "conversation_segments.json", {"segments": []})
            write_json(root / "02_timeline" / "conversation_index.json", {})
            write_jsonl(root / "02_timeline" / "messages_relevant_conversations.jsonl", [{"message_id": "m1"}])
            write_json(root / "02_timeline" / "conversation_patch_notes.json", {"status": "complete"})
            write_jsonl(root / "03_relevance" / "snippets_candidates.jsonl", [{"snippet_id": "s1"}])
            write_json(root / "03_relevance" / "dm_source_profiles.json", {"profiles": []})
            write_json(root / "05_alias" / "resolved_entities.json", {"resolved_entities": []})
            write_json(root / "05_alias" / "alias_map.json", {"aliases": []})
            write_json(root / "05_alias" / "entity_timelines.json", {"entity_timelines": {}})
            write_json(root / "05_alias" / "entity_candidate_harvest.json", {"schema_version": 1, "candidates": []})
            write_json(root / "05_alias" / "entity_adjudication_recommendations.json", {"schema_version": 1, "recommendations": []})
            write_json(root / "05_alias" / "externality_cache.json", {"schema_version": 1, "entries": {}})
            write_json(root / "05_alias" / "theme_profile_update_report.json", {"schema_version": 1, "summary": {"theme_count": 0}})
            write_json(root / "05_alias" / "theme_candidate_reclassification.json", {"schema_version": 1, "candidate_reclassifications": []})
            write_json(root / "05_alias" / "conversation_entity_proposals.json", {"proposals": [{"proposal_id": "p1"}]})
            write_json(root / "04_grouping" / "snippet_clusters_lore.json", {"clusters": []})
            write_json(root / "04_grouping" / "snippet_clusters_meta.json", {"clusters": []})
            write_json(root / "06_drafts" / "card_drafts" / "claim_drafts.json", {"claims": []})

            # Make the human/AI decision newer than the Stage 07 outputs.
            decisions_path = root / "05_alias" / "conversation_entity_decisions.json"
            write_json(
                decisions_path,
                {"decisions": [{"proposal_id": "p1", "decision": "approve"}]},
            )
            future_mtime = max(path.stat().st_mtime for path in (root / "05_alias").glob("*.json")) + 10
            os.utime(decisions_path, (future_mtime, future_mtime))

            stage, reason = determine_resume_start_stage(root)

            self.assertEqual(stage, 10)
            self.assertIn("Stage 10", reason)

    def test_pipeline_resume_starts_at_patch_notes_when_stage_04_is_done(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write_json(root / "01_bootstrap" / "entity_seed.json", {"entities": []})
            write_json(root / "02_timeline" / "summary.json", {})
            write_jsonl(root / "02_timeline" / "messages_normalized_per_thread.jsonl", [{"message_id": "m1"}])
            write_json(root / "02_timeline" / "global_index.json", {})
            write_jsonl(root / "02_timeline" / "messages_global_timeline.jsonl", [{"message_id": "m1"}])
            write_json(root / "02_timeline" / "conversation_segments.json", {"segments": []})
            write_json(root / "02_timeline" / "conversation_index.json", {})
            write_jsonl(root / "02_timeline" / "messages_relevant_conversations.jsonl", [{"message_id": "m1"}])

            stage, reason = determine_resume_start_stage(root)

            self.assertEqual(stage, 5)
            self.assertIn("patch notes", reason)

    def test_pipeline_resume_pauses_for_claim_review_before_card_synthesis(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write_pipeline_artifacts_through_stage9(root, [{"claim_id": "claim_a"}])

            stage, reason = determine_resume_start_stage(root)

            self.assertEqual(stage, 0)
            self.assertIn("claim review", reason)

    def test_pipeline_resume_starts_at_card_synthesis_after_claim_review(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write_pipeline_artifacts_through_stage9(root, [{"claim_id": "claim_a"}])
            write_json(ArtifactPaths(root).claim_review_decisions, {"decisions": [{"claim_id": "claim_a", "decision": "accept"}]})

            stage, reason = determine_resume_start_stage(root)

            self.assertEqual(stage, 10)
            self.assertIn("Stage 10", reason)

    def test_pipeline_resume_reruns_card_synthesis_after_author_claim_added(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write_pipeline_artifacts_through_stage9(root, [{"claim_id": "claim_a"}])
            write_json(ArtifactPaths(root).claim_review_decisions, {"decisions": [{"claim_id": "claim_a", "decision": "accept"}]})
            write_json(root / "07_review" / "card_drafts.json", {"cards": [{"card_id": "card_a", "status": "draft"}]})
            write_json(root / "07_review" / "canonical_cards.json", {"cards": [{"card_id": "card_a", "status": "canonical"}]})
            write_jsonl(root / "07_review" / "merge_log.jsonl", [{"decision_id": "d1"}])
            write_json(ArtifactPaths(root).author_claims, {"claims": [{"claim_id": "author_claim_a"}]})
            future_mtime = max(path.stat().st_mtime for path in (root / "07_review").glob("*")) + 10
            os.utime(ArtifactPaths(root).author_claims, (future_mtime, future_mtime))

            stage, reason = determine_resume_start_stage(root)

            self.assertEqual(stage, 10)
            self.assertIn("Stage 10", reason)

    def test_pipeline_resume_pauses_for_card_review_before_export(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write_pipeline_artifacts_through_stage9(root, [{"claim_id": "claim_a"}])
            write_json(ArtifactPaths(root).claim_review_decisions, {"decisions": [{"claim_id": "claim_a", "decision": "accept"}]})
            write_json(root / "07_review" / "identity_merge_proposals.json", {"proposals": []})
            write_json(root / "07_review" / "card_drafts.json", {"cards": [{"card_id": "card_a", "status": "draft"}]})
            write_json(root / "07_review" / "canonical_cards.json", {"cards": []})
            write_jsonl(root / "07_review" / "merge_log.jsonl", [{"decision_id": "d1"}])

            stage, reason = determine_resume_start_stage(root)

            self.assertEqual(stage, 0)
            self.assertIn("card review", reason)

    def test_pipeline_resume_starts_at_export_after_card_review(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write_pipeline_artifacts_through_stage9(root, [{"claim_id": "claim_a"}])
            write_json(ArtifactPaths(root).claim_review_decisions, {"decisions": [{"claim_id": "claim_a", "decision": "accept"}]})
            write_json(root / "07_review" / "card_review_decisions.json", {"decisions": [{"card_id": "card_a", "decision": "approve"}]})
            write_json(root / "07_review" / "identity_merge_proposals.json", {"proposals": []})
            write_json(root / "07_review" / "card_drafts.json", {"cards": [{"card_id": "card_a", "status": "draft"}]})
            write_json(root / "07_review" / "canonical_cards.json", {"cards": [{"card_id": "card_a", "status": "canonical"}]})
            write_jsonl(root / "07_review" / "merge_log.jsonl", [{"decision_id": "d1"}])

            stage, reason = determine_resume_start_stage(root)

            self.assertEqual(stage, 12)
            self.assertIn("Stage 12", reason)

    def test_pipeline_resume_ignores_applied_cardbase_agent_requests(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            paths = ArtifactPaths(root)
            write_pipeline_artifacts_through_stage9(root, [{"claim_id": "claim_a"}])
            write_json(paths.claim_review_decisions, {"decisions": [{"claim_id": "claim_a", "decision": "accept"}]})
            write_json(paths.card_review_decisions, {"decisions": [{"card_id": "card_a", "decision": "approve"}]})
            write_json(paths.identity_merge_proposals, {"proposals": []})
            write_json(paths.card_drafts, {"cards": [{"card_id": "card_a", "status": "draft"}]})
            write_json(paths.canonical_cards, {"cards": [{"card_id": "card_a", "status": "canonical"}]})
            write_jsonl(paths.merge_log, [{"decision_id": "d1"}])
            write_jsonl(
                paths.card_edit_requests,
                [
                    {
                        "request_id": "req_applied",
                        "instruction_text": "Pandora's mother is Izanami",
                        "status": "applied",
                        "card_agent_transaction_id": "card_agent_tx_applied",
                    }
                ],
            )

            stage, reason = determine_resume_start_stage(root)

            self.assertEqual(stage, 12)
            self.assertIn("Stage 12", reason)

    def test_new_run_artifacts_root_creates_unique_run_folder(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            repo = Path(tmp)
            first = new_run_artifacts_root(repo)
            second = new_run_artifacts_root(repo)

            self.assertTrue(first.exists())
            self.assertTrue(second.exists())
            self.assertNotEqual(first, second)
            self.assertEqual(first.parent, repo / "artifacts" / "runs")

    def test_app_state_remembers_last_open_run_for_desktop_startup(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            repo = Path(tmp)
            run_a = repo / "artifacts" / "run_a"
            run_b = repo / "artifacts" / "run_b"
            write_json(run_a / "06_drafts" / "card_drafts" / "claim_drafts.json", {"claims": []})
            write_json(run_b / "06_drafts" / "card_drafts" / "claim_drafts.json", {"claims": []})

            save_last_open_artifacts_root(repo, run_b)

            self.assertEqual(load_last_open_artifacts_root(repo), run_b.resolve())
            self.assertEqual(choose_initial_artifacts_root(repo), run_b.resolve())
            self.assertEqual(json.loads(app_state_path(repo).read_text(encoding="utf-8"))["last_open_artifacts_root"], str(run_b.resolve()))
            self.assertEqual(choose_initial_artifacts_root(repo, run_a), run_a.resolve())

    def test_desktop_launcher_loads_project_env_for_frozen_worker(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            repo = Path(tmp)
            (repo / ".env").write_text(
                'GEMINI_API_KEY="fake-gemini"\nMODEL_API_KEY: "fake-model"\n',
                encoding="utf-8",
            )
            with patch.dict("os.environ", {}, clear=True):
                load_project_env(repo)
                self.assertEqual(__import__("os").environ["GEMINI_API_KEY"], "fake-gemini")
                self.assertEqual(__import__("os").environ["MODEL_API_KEY"], "fake-model")

    def test_tauri_entity_views_include_card_agent_memory_merges(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            repo = Path(tmp)
            active = repo / "artifacts" / "runs" / "run_1"
            paths = ArtifactPaths(active)
            write_json(
                paths.resolved_entities,
                {
                    "resolved_entities": [
                        {
                            "entity_id": "entity_laser",
                            "card_id": "card_laser",
                            "canonical_name": "Orbital Laser Weapon",
                            "entity_type": "location",
                            "aliases": ["Space Warship"],
                        },
                        {
                            "entity_id": "entity_garuda",
                            "card_id": "card_garuda",
                            "canonical_name": "The Garuda",
                            "entity_type": "location",
                            "aliases": ["Garuda"],
                        },
                        {
                            "entity_id": "entity_majapahit",
                            "card_id": "card_majapahit",
                            "canonical_name": "Majapahit",
                            "entity_type": "faction",
                            "aliases": [],
                        },
                    ]
                },
            )
            write_json(paths.conversation_entity_proposals, {"proposals": []})
            write_json(paths.conversation_entity_decisions, {"decisions": []})
            write_json(paths.identity_merge_proposals, {"proposals": []})
            write_json(paths.identity_merge_decisions, {"decisions": []})
            write_json(
                paths.claim_drafts,
                {
                    "claims": [
                        {
                            "claim_id": "claim_laser",
                            "target_entity_id": "entity_laser",
                            "target_card_id": "card_laser",
                            "target_entity_name": "Orbital Laser Weapon",
                            "claim_type": "relationship",
                            "knowledge_track": "lore",
                            "claim_text": "The orbital laser weapon was built by Majapahit.",
                            "proposed_relationship_hints": [
                                {
                                    "target_entity_id": "entity_majapahit",
                                    "target_card_id": "card_majapahit",
                                    "target_entity_name": "Majapahit",
                                    "relation_type": "built_by",
                                }
                            ],
                        }
                    ]
                },
            )
            write_json(paths.claim_review_decisions, {"decisions": [{"claim_id": "claim_laser", "decision": "accept"}]})
            write_json(paths.author_claims, {"claims": []})
            write_json(
                repo / "canon" / "review_memory.json",
                {
                    "entity_merges": [
                        {
                            "merge_id": "merge_laser_garuda",
                            "source_entity_id": "entity_laser",
                            "source_card_id": "card_laser",
                            "source_entity_name": "Orbital Laser Weapon",
                            "target_entity_id": "entity_garuda",
                            "target_card_id": "card_garuda",
                            "target_entity_name": "The Garuda",
                            "canonical_name": "The Garuda",
                            "alias_text": "Orbital Laser Weapon",
                            "merge_type": "cardbase_agent_identity_merge",
                        }
                    ]
                },
            )

            inventory = handle_request(
                {
                    "repo_root": str(repo),
                    "command": "entity_inventory",
                    "payload": {"artifacts_root": str(active)},
                }
            )

            merged_names = [row["candidate_name"] for row in inventory["merged_rows"]]
            self.assertIn("The Garuda", merged_names)
            self.assertNotIn("Orbital Laser Weapon", merged_names)
            garuda = next(row for row in inventory["merged_rows"] if row["candidate_name"] == "The Garuda")
            self.assertIn("Orbital Laser Weapon", garuda["item"]["aliases"])
            self.assertIn("Space Warship", garuda["item"]["aliases"])
            self.assertIn("Garuda", garuda["item"]["aliases"])
            self.assertEqual(garuda["topics"], [])
            self.assertNotIn("Merged from 1", garuda["triage_reason"])
            self.assertEqual([source["canonical_name"] for source in garuda["item"]["merged_from_entities"]], ["Orbital Laser Weapon"])

            graph = handle_request(
                {
                    "repo_root": str(repo),
                    "command": "entity_relationships",
                    "payload": {"artifacts_root": str(active)},
                }
            )

            node_names = [node["name"] for node in graph["nodes"]]
            self.assertIn("The Garuda", node_names)
            self.assertNotIn("Orbital Laser Weapon", node_names)
            garuda_node = next(node for node in graph["nodes"] if node["name"] == "The Garuda")
            self.assertIn("Orbital Laser Weapon", garuda_node["aliases"])

    def test_desktop_text_word_delete_boundaries(self) -> None:
        text = "The Lab answers quickly"

        self.assertEqual(ctrl_backspace_delete_start(text), len("The Lab answers "))
        self.assertEqual(ctrl_backspace_delete_start("The Lab answers   "), len("The Lab "))
        self.assertEqual(ctrl_backspace_delete_start("The Lab..."), len("The Lab"))
        self.assertEqual(ctrl_delete_delete_end("   next word"), len("   next"))
        self.assertEqual(ctrl_delete_delete_end("... next"), len("..."))

    def test_ui_run_selector_switches_active_artifact_root(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            repo = Path(tmp)
            run_a = repo / "artifacts" / "run_a"
            run_b = repo / "artifacts" / "run_b"
            claim_a = {
                "claim_id": "claim_a",
                "target_card_id": "card_a",
                "target_entity_name": "Alpha Run",
                "confidence": 0.7,
            }
            claim_b = {
                "claim_id": "claim_b",
                "target_card_id": "card_b",
                "target_entity_name": "Beta Run",
                "confidence": 0.8,
            }
            for run, claim in [(run_a, claim_a), (run_b, claim_b)]:
                write_json(run / "06_drafts" / "card_drafts" / "claim_drafts.json", {"claims": [claim]})
                write_json(run / "07_review" / "claim_review_decisions.json", {"decisions": []})
                write_json(run / "07_review" / "author_directives.json", {"directives": []})
                write_json(run / "07_review" / "card_review_decisions.json", {"decisions": []})
                write_json(run / "07_review" / "identity_merge_decisions.json", {"decisions": []})
                write_json(run / "05_alias" / "conversation_entity_decisions.json", {"decisions": []})

            app = build_app(
                run_a / "06_drafts" / "card_drafts" / "claim_drafts.json",
                run_a / "07_review" / "claim_review_decisions.json",
                run_a / "07_review" / "author_directives.json",
                run_a / "07_review" / "card_drafts.json",
                run_a / "07_review" / "card_review_decisions.json",
                run_a / "07_review" / "identity_merge_proposals.json",
                run_a / "07_review" / "identity_merge_decisions.json",
                run_a / "05_alias" / "conversation_entity_proposals.json",
                run_a / "05_alias" / "conversation_entity_decisions.json",
                run_a,
                None,
                repo / "discord_conversations",
                repo_root_override=repo,
            )

            with app.test_client() as client:
                response_a = client.get("/")
                self.assertIn(b"Alpha Run", response_a.data)
                self.assertIn(str(Path("artifacts") / "run_b").encode("utf-8"), response_a.data)

                response_b = client.post(
                    "/select_run",
                    data={"artifacts_root": str(run_b.resolve())},
                    follow_redirects=True,
                )
                self.assertIn(b"Beta Run", response_b.data)
                self.assertNotIn(b"Alpha Run", response_b.data)
                self.assertEqual(load_last_open_artifacts_root(repo), run_b.resolve())

    def test_story_question_proposal_waits_for_approval_and_uses_configured_openrouter(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            claims = [
                {
                    "claim_id": "claim_lab_kind",
                    "target_entity_id": "entity_lab",
                    "target_card_id": "card_lab",
                    "target_entity_name": "The Lab",
                    "knowledge_track": "lore",
                    "claim_type": "entity_type",
                    "claim_text": "The Lab may be a location or a faction.",
                    "source_snippet_ids": ["snippet_lab"],
                    "confidence": 0.7,
                    "status": "draft",
                }
            ]
            write_pipeline_artifacts_through_stage9(root, claims)
            write_json(
                root / "05_alias" / "resolved_entities.json",
                {
                    "resolved_entities": [
                        {"entity_id": "entity_lab", "card_id": "card_lab", "canonical_name": "The Lab", "aliases": []}
                    ]
                },
            )
            write_jsonl(
                root / "03_relevance" / "snippets_candidates.jsonl",
                [
                    {
                        "snippet_id": "snippet_lab",
                        "conversation_id": "conv_lab",
                        "conversation_topic_label": "The Lab",
                        "knowledge_track": "lore",
                        "display_text_normalized": "The Lab is discussed as both a place and an organization.",
                    }
                ],
            )
            config_path = root / "config.json"
            write_json(
                config_path,
                {
                    "story_questions": {
                        "enabled": True,
                        "provider": "openrouter",
                        "model": "deepseek/deepseek-v4-flash",
                        "max_linked_claims_per_question": 1,
                        "application_policy": "moderate",
                    }
                },
            )

            mock_payloads = [
                {
                    "question_text": "Is The Lab a location, faction, or both?",
                    "focus_type": "entity_type",
                    "rationale": "It resolves entity type confusion.",
                    "linked_claim_ids": ["claim_lab_kind"],
                    "linked_entities": [{"entity_id": "entity_lab", "name": "The Lab"}],
                    "evidence_snippet_ids": ["snippet_lab"],
                    "expected_resolution": "Clarify the entity type.",
                },
                {
                    "summary": "The Lab is both a place and an organization.",
                    "claim_decisions": [
                        {
                            "claim_id": "claim_lab_kind",
                            "decision": "accept",
                            "edited_claim_text": "The Lab is both a physical location and an organized faction.",
                            "confidence": 0.95,
                            "rationale": "The author answer directly resolves the type.",
                        }
                    ],
                    "author_claims": [],
                    "left_pending": [],
                },
            ]

            with patch("pipeline.story_questions.call_model_chat", side_effect=mock_payloads) as model_call:
                with patch("pipeline.story_questions.load_review_memory", return_value={"story_question_answers": []}):
                    with patch("pipeline.story_questions.save_review_memory"):
                        generate_next_question(root, config_path)
                        proposal = propose_story_answer_application(root, "Both: it is a facility and the group inside it.", config_path)
                        decisions_path = ArtifactPaths(root).claim_review_decisions
                        decisions_before = (
                            json.loads(decisions_path.read_text(encoding="utf-8"))["decisions"]
                            if decisions_path.exists()
                            else []
                        )
                        application = commit_story_answer_application(root, config_path, proposal_id=proposal["proposal_id"])

            self.assertEqual(decisions_before, [])
            self.assertEqual(len(proposal["claim_decisions"]), 1)
            self.assertEqual(story_question_display(root)["pending_application_proposal"], None)
            decisions_after = json.loads((ArtifactPaths(root).claim_review_decisions).read_text(encoding="utf-8"))[
                "decisions"
            ]
            self.assertEqual(len(decisions_after), 1)
            self.assertEqual(application["claim_decisions"][0]["edited_claim_text"], "The Lab is both a physical location and an organized faction.")
            self.assertEqual(model_call.call_count, 2)
            self.assertEqual(model_call.call_args_list[1].kwargs["provider"], "openrouter")
            self.assertEqual(model_call.call_args_list[1].kwargs["api_model"], "deepseek/deepseek-v4-flash")

    def test_story_question_author_claims_classify_naming_history_as_meta(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            claims = [
                {
                    "claim_id": "claim_lab_names",
                    "target_entity_id": "entity_lab",
                    "target_card_id": "card_lab",
                    "target_entity_name": "The Lab",
                    "knowledge_track": "lore",
                    "claim_type": "background",
                    "claim_text": "The lab characters may have working names.",
                    "source_snippet_ids": [],
                    "confidence": 0.7,
                    "status": "draft",
                }
            ]
            write_pipeline_artifacts_through_stage9(root, claims)
            write_json(
                root / "05_alias" / "resolved_entities.json",
                {"resolved_entities": [{"entity_id": "entity_lab", "card_id": "card_lab", "canonical_name": "The Lab", "aliases": []}]},
            )
            write_json(
                ArtifactPaths(root).stage09 / "story_question_session.json",
                {
                    "version": 1,
                    "session_id": "story_session",
                    "status": "active",
                    "created_at_utc": "2026-05-18T00:00:00Z",
                    "updated_at_utc": "2026-05-18T00:00:00Z",
                    "current_question_id": "story_question_names",
                    "questions": [
                        {
                            "question_id": "story_question_names",
                            "session_id": "story_session",
                            "status": "pending",
                            "question_text": "Are the emotion names final names or working names?",
                            "linked_claim_ids": ["claim_lab_names"],
                            "linked_entities": [{"entity_id": "entity_lab", "name": "The Lab"}],
                        }
                    ],
                    "answers": [],
                    "applications": [],
                    "skipped_questions": [],
                    "pending_application_proposal": None,
                    "last_unresolved_claim_count": 1,
                    "last_model_rationale": "",
                },
            )
            config_path = root / "config.json"
            write_json(config_path, {"story_questions": {"enabled": True, "provider": "openrouter", "model": "deepseek/deepseek-v4-flash"}})
            model_payload = {
                "summary": "Adds naming history as an author note.",
                "claim_decisions": [],
                "author_claims": [
                    {
                        "target_entity_id": "entity_lab",
                        "target_entity_name": "The Lab",
                        "claim_type": "background",
                        "claim_text": "Loss, Love, Fear, Altruism, and Greed were working names that were later updated with canonical names.",
                        "knowledge_track": "lore",
                        "confidence": 1.0,
                        "rationale": "The author answer states this naming history.",
                    }
                ],
                "left_pending": [],
            }

            with patch("pipeline.story_questions.call_model_chat", return_value=model_payload) as model_call:
                proposal = propose_story_answer_application(root, "Those were working names later updated.", config_path)

            prompt = model_call.call_args.kwargs["prompt"]
            self.assertIn("naming history", prompt)
            self.assertEqual(proposal["author_claims"][0]["knowledge_track"], "meta")
            self.assertEqual(proposal["author_claims"][0]["claim_type"], "meta_note")

    def test_story_question_application_retries_with_compact_prompt_after_content_parse_failure(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            claims = [
                {
                    "claim_id": "claim_krypteia_leader",
                    "target_entity_id": "entity_leonidas",
                    "target_card_id": "card_leonidas",
                    "target_entity_name": "Leonidas",
                    "knowledge_track": "lore",
                    "claim_type": "role",
                    "claim_text": "Leonidas may be connected to the Krypteia, but his rank is unclear.",
                    "source_snippet_ids": ["snippet_leonidas"],
                    "confidence": 0.72,
                    "status": "draft",
                }
            ]
            write_pipeline_artifacts_through_stage9(root, claims)
            write_json(
                root / "05_alias" / "resolved_entities.json",
                {
                    "resolved_entities": [
                        {"entity_id": "entity_leonidas", "card_id": "card_leonidas", "canonical_name": "Leonidas", "aliases": []}
                    ]
                },
            )
            write_jsonl(
                root / "03_relevance" / "snippets_candidates.jsonl",
                [
                    {
                        "snippet_id": "snippet_leonidas",
                        "conversation_id": "conv_leonidas",
                        "conversation_topic_label": "Leonidas and the Krypteia",
                        "knowledge_track": "lore",
                        "display_text_normalized": "Leonidas and the Krypteia are discussed. " * 80,
                    }
                ],
            )
            write_json(
                ArtifactPaths(root).stage09 / "story_question_session.json",
                {
                    "version": 1,
                    "session_id": "story_session",
                    "status": "active",
                    "created_at_utc": "2026-05-18T00:00:00Z",
                    "updated_at_utc": "2026-05-18T00:00:00Z",
                    "current_question_id": "story_question_leonidas",
                    "questions": [
                        {
                            "question_id": "story_question_leonidas",
                            "session_id": "story_session",
                            "status": "pending",
                            "question_text": "Is Leonidas the current leader or founder of the Krypteia?",
                            "linked_claim_ids": ["claim_krypteia_leader"],
                            "linked_entities": [{"entity_id": "entity_leonidas", "name": "Leonidas"}],
                        }
                    ],
                    "answers": [],
                    "applications": [],
                    "skipped_questions": [],
                    "pending_application_proposal": None,
                    "last_unresolved_claim_count": 1,
                    "last_model_rationale": "",
                },
            )
            config_path = root / "config.json"
            write_json(config_path, {"story_questions": {"enabled": True, "provider": "openrouter", "model": "deepseek/deepseek-v4-flash"}})
            model_payload = {
                "summary": "Leonidas is confirmed as founder and current leader.",
                "claim_decisions": [
                    {
                        "claim_id": "claim_krypteia_leader",
                        "decision": "accept",
                        "edited_claim_text": "Leonidas is the founder and current leader of the Krypteia.",
                        "confidence": 0.96,
                        "rationale": "The author answer directly states this.",
                    }
                ],
                "author_claims": [],
                "left_pending": [],
            }

            with patch("pipeline.story_questions.call_model_chat", side_effect=[None, model_payload]) as model_call:
                with patch("pipeline.story_questions.get_model_runtime_status", return_value={"last_model_skip_reason": "content_parse_failed"}):
                    proposal = propose_story_answer_application(root, "Yes, Leonidas is the founder and current leader.", config_path)

            self.assertEqual(model_call.call_count, 2)
            first_prompt = model_call.call_args_list[0].kwargs["prompt"]
            retry_prompt = model_call.call_args_list[1].kwargs["prompt"]
            self.assertLess(len(retry_prompt), len(first_prompt))
            self.assertIn("Reduced state JSON", retry_prompt)
            self.assertEqual(proposal["model_retry"]["initial_reason"], "content_parse_failed")
            self.assertTrue(proposal["model_retry"]["recovered"])
            self.assertEqual(proposal["claim_decisions"][0]["edited_claim_text"], "Leonidas is the founder and current leader of the Krypteia.")

    def test_story_question_correct_on_all_counts_accepts_linked_claims_without_model(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            claims = [
                {
                    "claim_id": "claim_leonidas_leader",
                    "target_entity_id": "entity_leonidas",
                    "target_card_id": "card_leonidas",
                    "target_entity_name": "Leonidas",
                    "knowledge_track": "lore",
                    "claim_type": "role",
                    "claim_text": "Leonidas is the current leader of the Krypteia.",
                    "source_snippet_ids": [],
                    "confidence": 0.8,
                    "status": "draft",
                },
                {
                    "claim_id": "claim_hectr_commission",
                    "target_entity_id": "entity_hectr",
                    "target_card_id": "card_hectr",
                    "target_entity_name": "HECTR",
                    "knowledge_track": "lore",
                    "claim_type": "relationship",
                    "claim_text": "HECTR was commissioned from Penemue for the Krypteia.",
                    "source_snippet_ids": [],
                    "confidence": 0.8,
                    "status": "draft",
                },
                {
                    "claim_id": "claim_unlinked",
                    "target_entity_id": "entity_hectr",
                    "target_card_id": "card_hectr",
                    "target_entity_name": "HECTR",
                    "knowledge_track": "lore",
                    "claim_type": "background",
                    "claim_text": "HECTR has unrelated unresolved background.",
                    "source_snippet_ids": [],
                    "confidence": 0.8,
                    "status": "draft",
                },
            ]
            write_pipeline_artifacts_through_stage9(root, claims)
            write_json(
                root / "05_alias" / "resolved_entities.json",
                {
                    "resolved_entities": [
                        {"entity_id": "entity_leonidas", "card_id": "card_leonidas", "canonical_name": "Leonidas", "aliases": []},
                        {"entity_id": "entity_hectr", "card_id": "card_hectr", "canonical_name": "HECTR", "aliases": []},
                    ]
                },
            )
            write_json(
                ArtifactPaths(root).stage09 / "story_question_session.json",
                {
                    "version": 1,
                    "session_id": "story_session",
                    "status": "active",
                    "created_at_utc": "2026-05-18T00:00:00Z",
                    "updated_at_utc": "2026-05-18T00:00:00Z",
                    "current_question_id": "story_question_confirm_all",
                    "questions": [
                        {
                            "question_id": "story_question_confirm_all",
                            "session_id": "story_session",
                            "status": "pending",
                            "question_text": "Is Leonidas the active Krypteia leader, and did he commission HECTR from Penemue?",
                            "linked_claim_ids": ["claim_leonidas_leader", "claim_hectr_commission"],
                            "linked_entities": [{"entity_id": "entity_leonidas", "name": "Leonidas"}],
                        }
                    ],
                    "answers": [],
                    "applications": [],
                    "skipped_questions": [],
                    "pending_application_proposal": None,
                    "last_unresolved_claim_count": 3,
                    "last_model_rationale": "",
                },
            )
            config_path = root / "config.json"
            write_json(config_path, {"story_questions": {"enabled": True, "provider": "openrouter", "model": "deepseek/deepseek-v4-flash"}})

            with patch("pipeline.story_questions.call_model_chat", side_effect=AssertionError("confirmation should not call model")):
                proposal = propose_story_answer_application(root, "Correct on all counts.", config_path)

            self.assertEqual({item["claim_id"] for item in proposal["claim_decisions"]}, {"claim_leonidas_leader", "claim_hectr_commission"})
            self.assertTrue(all(item["decision"] == "accept" for item in proposal["claim_decisions"]))
            self.assertTrue(proposal["model_retry"]["deterministic_confirmation"])
            self.assertEqual(proposal["left_pending"], [])

    def test_story_question_drops_absence_based_rejections(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            claims = [
                {
                    "claim_id": "claim_ramasinta_relation",
                    "target_entity_id": "entity_transhumanist",
                    "target_card_id": "card_transhumanist",
                    "target_entity_name": "Transhumanist Character",
                    "knowledge_track": "lore",
                    "claim_type": "relationship",
                    "claim_text": "The Transhumanist Character may have influenced Ramasinta's entry into the military program.",
                    "source_snippet_ids": [],
                    "confidence": 0.8,
                    "status": "draft",
                }
            ]
            write_pipeline_artifacts_through_stage9(root, claims)
            write_json(
                root / "05_alias" / "resolved_entities.json",
                {
                    "resolved_entities": [
                        {
                            "entity_id": "entity_transhumanist",
                            "card_id": "card_transhumanist",
                            "canonical_name": "Transhumanist Character",
                            "aliases": [],
                        }
                    ]
                },
            )
            write_json(
                ArtifactPaths(root).stage09 / "story_question_session.json",
                {
                    "version": 1,
                    "session_id": "story_session",
                    "status": "active",
                    "created_at_utc": "2026-05-19T00:00:00Z",
                    "updated_at_utc": "2026-05-19T00:00:00Z",
                    "current_question_id": "story_question_transhumanist",
                    "questions": [
                        {
                            "question_id": "story_question_transhumanist",
                            "session_id": "story_session",
                            "status": "pending",
                            "question_text": "Who is the Transhumanist Character?",
                            "linked_claim_ids": ["claim_ramasinta_relation"],
                            "linked_entities": [{"entity_id": "entity_transhumanist", "name": "Transhumanist Character"}],
                        }
                    ],
                    "answers": [],
                    "applications": [],
                    "skipped_questions": [],
                    "pending_application_proposal": None,
                    "last_unresolved_claim_count": 1,
                    "last_model_rationale": "",
                },
            )
            config_path = root / "config.json"
            write_json(config_path, {"story_questions": {"enabled": True, "provider": "openrouter", "model": "deepseek/deepseek-v4-flash"}})
            model_payload = {
                "summary": "Named the character.",
                "claim_decisions": [
                    {
                        "claim_id": "claim_ramasinta_relation",
                        "decision": "reject",
                        "confidence": 0.95,
                        "rationale": "No mention of Ramasinta or influence on her entry.",
                    }
                ],
                "author_claims": [
                    {
                        "target_entity_id": "entity_transhumanist",
                        "target_entity_name": "Transhumanist Character",
                        "claim_type": "alias",
                        "claim_text": "The Transhumanist Character is Halayudtha.",
                        "knowledge_track": "lore",
                        "confidence": 1.0,
                        "rationale": "Author named the character.",
                    }
                ],
                "left_pending": [],
            }

            with patch("pipeline.story_questions.call_model_chat", return_value=model_payload):
                proposal = propose_story_answer_application(root, "This is Halayudtha.", config_path)

            self.assertEqual(proposal["claim_decisions"], [])
            self.assertEqual(proposal["dropped_decisions"][0]["reason"], "negative_decision_based_on_absence")
            self.assertEqual(len(proposal["author_claims"]), 1)

    def test_skipping_story_question_discards_linked_claims(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            claims = [
                {
                    "claim_id": "claim_bad_bundle_a",
                    "target_entity_id": "entity_bundle",
                    "target_card_id": "card_bundle",
                    "target_entity_name": "Bad Bundle",
                    "knowledge_track": "lore",
                    "claim_type": "role",
                    "claim_text": "Bad Bundle has an unwanted role claim.",
                    "source_snippet_ids": [],
                    "confidence": 0.7,
                    "status": "draft",
                },
                {
                    "claim_id": "claim_bad_bundle_b",
                    "target_entity_id": "entity_bundle",
                    "target_card_id": "card_bundle",
                    "target_entity_name": "Bad Bundle",
                    "knowledge_track": "lore",
                    "claim_type": "relationship",
                    "claim_text": "Bad Bundle has an unwanted relationship claim.",
                    "source_snippet_ids": [],
                    "confidence": 0.7,
                    "status": "draft",
                },
                {
                    "claim_id": "claim_keep",
                    "target_entity_id": "entity_keep",
                    "target_card_id": "card_keep",
                    "target_entity_name": "Keep",
                    "knowledge_track": "lore",
                    "claim_type": "background",
                    "claim_text": "Keep this unrelated claim pending.",
                    "source_snippet_ids": [],
                    "confidence": 0.7,
                    "status": "draft",
                },
            ]
            write_pipeline_artifacts_through_stage9(root, claims)
            write_json(
                ArtifactPaths(root).stage09 / "story_question_session.json",
                {
                    "version": 1,
                    "session_id": "story_session",
                    "status": "active",
                    "created_at_utc": "2026-05-19T00:00:00Z",
                    "updated_at_utc": "2026-05-19T00:00:00Z",
                    "current_question_id": "story_question_bad_bundle",
                    "questions": [
                        {
                            "question_id": "story_question_bad_bundle",
                            "session_id": "story_session",
                            "status": "pending",
                            "question_text": "Bad question with bad linked claims?",
                            "linked_claim_ids": ["claim_bad_bundle_a", "claim_bad_bundle_b"],
                            "linked_entities": [{"entity_id": "entity_bundle", "name": "Bad Bundle"}],
                        }
                    ],
                    "answers": [],
                    "applications": [],
                    "skipped_questions": [],
                    "pending_application_proposal": {"proposal_id": "proposal_to_clear"},
                    "last_unresolved_claim_count": 3,
                    "last_model_rationale": "",
                },
            )

            record = skip_current_question(root, "Bad question; discard linked claims.")

            self.assertEqual(record["discarded_claim_count"], 2)
            self.assertEqual(set(record["discarded_claim_ids"]), {"claim_bad_bundle_a", "claim_bad_bundle_b"})
            decisions = json.loads((ArtifactPaths(root).claim_review_decisions).read_text(encoding="utf-8"))["decisions"]
            self.assertEqual({item["claim_id"] for item in decisions}, {"claim_bad_bundle_a", "claim_bad_bundle_b"})
            self.assertTrue(all(item["reviewer"] == "story_question_skip" for item in decisions))
            self.assertTrue(all(item["decision"] == "reject" for item in decisions))
            display = story_question_display(root)
            self.assertIsNone(display["question"])
            self.assertEqual(display["pending_claim_count"], 1)
            self.assertEqual(display["session"]["pending_application_proposal"], None)

    def test_generate_all_story_questions_reserves_linked_claims(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            claims = []
            for idx, entity in enumerate(["Alpha", "Alpha", "Beta", "Beta", "Gamma"], start=1):
                claims.append(
                    {
                        "claim_id": f"claim_{idx}",
                        "target_entity_id": f"entity_{entity.lower()}",
                        "target_card_id": f"card_{entity.lower()}",
                        "target_entity_name": entity,
                        "knowledge_track": "lore",
                        "claim_type": "background",
                        "claim_text": f"{entity} unresolved claim {idx}.",
                        "source_snippet_ids": [],
                        "confidence": 0.7,
                        "status": "draft",
                    }
                )
            write_pipeline_artifacts_through_stage9(root, claims)
            write_json(
                ArtifactPaths(root).stage09 / "story_question_session.json",
                {
                    "version": 1,
                    "session_id": "story_session",
                    "status": "active",
                    "created_at_utc": "2026-05-19T00:00:00Z",
                    "updated_at_utc": "2026-05-19T00:00:00Z",
                    "current_question_id": "",
                    "questions": [],
                    "answers": [],
                    "applications": [],
                    "skipped_questions": [],
                    "pending_application_proposal": None,
                    "last_unresolved_claim_count": 5,
                    "last_model_rationale": "",
                },
            )
            config_path = root / "config.json"
            write_json(
                config_path,
                {
                    "story_questions": {
                        "enabled": True,
                        "provider": "openrouter",
                        "model": "deepseek/deepseek-v4-flash",
                        "max_linked_claims_per_question": 2,
                        "generate_all_max_questions": 10,
                    }
                },
            )
            model_payloads = [
                {
                    "question_text": "Resolve Alpha claims?",
                    "focus_type": "other",
                    "rationale": "Alpha first.",
                    "linked_claim_ids": ["claim_1", "claim_2"],
                    "linked_entities": [{"entity_id": "entity_alpha", "name": "Alpha"}],
                    "evidence_snippet_ids": [],
                    "expected_resolution": "Resolve Alpha.",
                },
                {
                    "question_text": "Resolve Beta claims?",
                    "focus_type": "other",
                    "rationale": "Beta next.",
                    "linked_claim_ids": ["claim_3", "claim_4"],
                    "linked_entities": [{"entity_id": "entity_beta", "name": "Beta"}],
                    "evidence_snippet_ids": [],
                    "expected_resolution": "Resolve Beta.",
                },
                {
                    "question_text": "Resolve Gamma claim?",
                    "focus_type": "other",
                    "rationale": "Gamma last.",
                    "linked_claim_ids": ["claim_5"],
                    "linked_entities": [{"entity_id": "entity_gamma", "name": "Gamma"}],
                    "evidence_snippet_ids": [],
                    "expected_resolution": "Resolve Gamma.",
                },
            ]

            with patch("pipeline.story_questions.call_model_chat", side_effect=model_payloads) as model_call:
                result = generate_all_questions(root, config_path)

            self.assertEqual(result["created_count"], 3)
            self.assertEqual(model_call.call_count, 3)
            session = json.loads((ArtifactPaths(root).stage09 / "story_question_session.json").read_text(encoding="utf-8"))
            linked_claims = [
                claim_id
                for question in session["questions"]
                for claim_id in question.get("linked_claim_ids", [])
            ]
            self.assertEqual(sorted(linked_claims), ["claim_1", "claim_2", "claim_3", "claim_4", "claim_5"])
            self.assertEqual(len(linked_claims), len(set(linked_claims)))
            self.assertEqual(session["current_question_id"], session["questions"][0]["question_id"])
            display = story_question_display(root)
            self.assertEqual(display["queued_question_count"], 2)
            self.assertEqual(display["reserved_claim_count"], 5)

            skipped = skip_current_question(root, "Discard first generated question.")
            self.assertEqual(set(skipped["discarded_claim_ids"]), {"claim_1", "claim_2"})
            next_display = story_question_display(root)
            self.assertEqual(next_display["question"]["question_text"], "Resolve Beta claims?")
            self.assertEqual(next_display["queued_question_count"], 1)

    def test_story_questions_can_link_auto_reviewed_claims_with_review_prior(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            claims = [
                {
                    "claim_id": "claim_auto_reviewed",
                    "target_entity_id": "entity_enoch",
                    "target_card_id": "card_enoch",
                    "target_entity_name": "Enoch",
                    "knowledge_track": "lore",
                    "claim_type": "role",
                    "claim_text": "Enoch has an unresolved role claim.",
                    "source_snippet_ids": ["s1"],
                    "confidence": 0.4,
                    "status": "draft",
                },
                {
                    "claim_id": "claim_human_reviewed",
                    "target_entity_id": "entity_enoch",
                    "target_card_id": "card_enoch",
                    "target_entity_name": "Enoch",
                    "knowledge_track": "lore",
                    "claim_type": "role",
                    "claim_text": "A human already handled this claim.",
                    "source_snippet_ids": ["s1"],
                    "confidence": 0.9,
                    "status": "draft",
                },
            ]
            write_pipeline_artifacts_through_stage9(root, claims)
            write_json(
                ArtifactPaths(root).claim_review_decisions,
                {
                    "decisions": [
                        {
                            "claim_id": "claim_auto_reviewed",
                            "decision": "accept",
                            "reviewer": "openrouter_auto_review",
                            "rationale": "[AI auto-review] Looks supported.",
                            "human_review_recommended": False,
                        },
                        {
                            "claim_id": "claim_human_reviewed",
                            "decision": "accept",
                            "reviewer": "story_question_answer",
                            "human_override": True,
                            "rationale": "Author already answered.",
                        },
                    ]
                },
            )
            config_path = root / "config.json"
            write_json(
                config_path,
                {
                    "story_questions": {
                        "enabled": True,
                        "provider": "openrouter",
                        "model": "deepseek/deepseek-v4-flash",
                        "max_linked_claims_per_question": 2,
                    }
                },
            )

            pending = pending_claims_for_story(root)
            self.assertEqual([claim["claim_id"] for claim in pending], ["claim_auto_reviewed"])
            self.assertEqual(pending[0]["auto_review"]["decision"], "accept")
            self.assertGreater(pending[0]["story_question_confidence"], pending[0]["confidence"])

            def fake_question(prompt: str, **_kwargs: object) -> dict[str, object]:
                self.assertIn('"auto_review"', prompt)
                self.assertIn('"story_question_confidence"', prompt)
                self.assertIn("claim_auto_reviewed", prompt)
                self.assertNotIn("claim_human_reviewed", prompt)
                return {
                    "question_text": "Can you confirm Enoch's unresolved role?",
                    "focus_type": "plot_role",
                    "rationale": "It confirms the auto-reviewed prior.",
                    "linked_claim_ids": ["claim_auto_reviewed"],
                    "linked_entities": [{"entity_id": "entity_enoch", "name": "Enoch"}],
                    "evidence_snippet_ids": ["s1"],
                    "expected_resolution": "Confirm or reject the role claim.",
                }

            with patch("pipeline.story_questions.call_model_chat", side_effect=fake_question) as model_call:
                question = generate_next_question(root, config_path)

            self.assertEqual(model_call.call_count, 1)
            self.assertEqual(question["linked_claim_ids"], ["claim_auto_reviewed"])

    def test_generate_all_story_questions_prioritizes_unanswered_then_attention_then_auto_reviewed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            claims = [
                {
                    "claim_id": "claim_unanswered",
                    "target_entity_id": "entity_alpha",
                    "target_card_id": "card_alpha",
                    "target_entity_name": "Alpha",
                    "knowledge_track": "lore",
                    "claim_type": "role",
                    "claim_text": "Alpha has a completely unanswered claim.",
                    "source_snippet_ids": [],
                    "confidence": 0.4,
                    "status": "draft",
                },
                {
                    "claim_id": "claim_attention",
                    "target_entity_id": "entity_beta",
                    "target_card_id": "card_beta",
                    "target_entity_name": "Beta",
                    "knowledge_track": "lore",
                    "claim_type": "role",
                    "claim_text": "Beta has an auto-review claim needing human review.",
                    "source_snippet_ids": [],
                    "confidence": 0.4,
                    "status": "draft",
                },
                {
                    "claim_id": "claim_auto",
                    "target_entity_id": "entity_gamma",
                    "target_card_id": "card_gamma",
                    "target_entity_name": "Gamma",
                    "knowledge_track": "lore",
                    "claim_type": "role",
                    "claim_text": "Gamma has a clean auto-reviewed prior.",
                    "source_snippet_ids": [],
                    "confidence": 0.4,
                    "status": "draft",
                },
            ]
            write_pipeline_artifacts_through_stage9(root, claims)
            write_json(
                ArtifactPaths(root).claim_review_decisions,
                {
                    "decisions": [
                        {
                            "claim_id": "claim_attention",
                            "decision": "accept",
                            "reviewer": "openrouter_auto_review",
                            "rationale": "[AI auto-review] Needs human attention.",
                            "human_review_recommended": True,
                            "human_review_reason": "support warnings",
                        },
                        {
                            "claim_id": "claim_auto",
                            "decision": "accept",
                            "reviewer": "openrouter_auto_review",
                            "rationale": "[AI auto-review] Clean prior.",
                            "human_review_recommended": False,
                        },
                    ]
                },
            )
            write_json(
                root / "07_review" / "claim_auto_review_attention.json",
                {"items": [{"claim_id": "claim_attention", "human_review_reason": "support warnings"}]},
            )
            config_path = root / "config.json"
            write_json(
                config_path,
                {
                    "story_questions": {
                        "enabled": True,
                        "provider": "openrouter",
                        "model": "deepseek/deepseek-v4-flash",
                        "max_linked_claims_per_question": 1,
                        "generate_all_max_questions": 3,
                    }
                },
            )
            expected_order = [
                ("claim_unanswered", "unanswered"),
                ("claim_attention", "human_review_requested"),
                ("claim_auto", "auto_reviewed"),
            ]

            def fake_question(prompt: str, **_kwargs: object) -> dict[str, object]:
                expected_claim_id, expected_status = expected_order.pop(0)
                self.assertIn(expected_claim_id, prompt)
                self.assertIn(f'"active_story_review_status":"{expected_status}"', prompt)
                for other_claim_id, _status in expected_order:
                    if expected_status == "unanswered":
                        self.assertNotIn(other_claim_id, prompt)
                    if expected_status == "human_review_requested" and other_claim_id == "claim_auto":
                        self.assertNotIn(other_claim_id, prompt)
                return {
                    "question_text": f"Resolve {expected_claim_id}?",
                    "focus_type": "other",
                    "rationale": "Priority tier.",
                    "linked_claim_ids": [expected_claim_id],
                    "linked_entities": [],
                    "evidence_snippet_ids": [],
                    "expected_resolution": "Resolve this tier.",
                }

            with patch("pipeline.story_questions.call_model_chat", side_effect=fake_question) as model_call:
                result = generate_all_questions(root, config_path)

            self.assertEqual(model_call.call_count, 3)
            self.assertEqual(result["created_count"], 3)
            self.assertEqual(expected_order, [])
            session = json.loads((ArtifactPaths(root).stage09 / "story_question_session.json").read_text(encoding="utf-8"))
            linked_order = [question["linked_claim_ids"][0] for question in session["questions"]]
            self.assertEqual(linked_order, ["claim_unanswered", "claim_attention", "claim_auto"])

    def test_story_questions_iterate_after_answer_application(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            claims = [
                {
                    "claim_id": "claim_khava_role",
                    "target_entity_id": "entity_khava",
                    "target_card_id": "card_khava",
                    "target_entity_name": "Khava",
                    "knowledge_track": "lore",
                    "claim_type": "relationship",
                    "claim_text": "Khava is connected to Enoch, but the relationship is unclear.",
                    "source_snippet_ids": ["snippet_1"],
                    "confidence": 0.72,
                    "status": "draft",
                },
                {
                    "claim_id": "claim_enoch_role",
                    "target_entity_id": "entity_enoch",
                    "target_card_id": "card_enoch",
                    "target_entity_name": "Enoch",
                    "knowledge_track": "lore",
                    "claim_type": "role",
                    "claim_text": "Enoch has a plot role that may depend on Khava.",
                    "source_snippet_ids": ["snippet_2"],
                    "confidence": 0.69,
                    "status": "draft",
                },
            ]
            write_pipeline_artifacts_through_stage9(root, claims)
            write_json(
                root / "05_alias" / "resolved_entities.json",
                {
                    "resolved_entities": [
                        {"entity_id": "entity_khava", "card_id": "card_khava", "canonical_name": "Khava", "aliases": []},
                        {"entity_id": "entity_enoch", "card_id": "card_enoch", "canonical_name": "Enoch", "aliases": []},
                    ]
                },
            )
            write_jsonl(
                root / "03_relevance" / "snippets_candidates.jsonl",
                [
                    {
                        "snippet_id": "snippet_1",
                        "conversation_id": "conv_1",
                        "conversation_topic_label": "Khava and Enoch",
                        "knowledge_track": "lore",
                        "display_text_normalized": "Khava's relationship to Enoch is discussed.",
                    },
                    {
                        "snippet_id": "snippet_2",
                        "conversation_id": "conv_1",
                        "conversation_topic_label": "Khava and Enoch",
                        "knowledge_track": "lore",
                        "display_text_normalized": "Enoch's plot role is discussed in relation to Khava.",
                    },
                ],
            )
            write_json(
                root / "02_timeline" / "conversation_patch_notes.json",
                {
                    "status": "complete",
                    "notes": [
                        {
                            "patch_note_id": "note_1",
                            "conversation_id": "conv_1",
                            "sequence_index": 1,
                            "topic_label": "Khava and Enoch",
                            "summary": "The conversation raises how Khava and Enoch relate.",
                        }
                    ],
                },
            )
            config_path = root / "config.json"
            write_json(
                config_path,
                {
                    "story_questions": {
                        "enabled": True,
                        "provider": "openrouter",
                        "model": "deepseek/deepseek-v4-flash",
                        "max_linked_claims_per_question": 2,
                        "application_policy": "moderate",
                    }
                },
            )
            model_payloads = [
                {
                    "question_text": "What is the relationship between Khava and Enoch?",
                    "focus_type": "relationship",
                    "rationale": "It resolves the densest linked uncertainty.",
                    "linked_claim_ids": ["claim_khava_role", "claim_enoch_role"],
                    "linked_entities": [{"entity_id": "entity_khava", "name": "Khava"}],
                    "evidence_snippet_ids": ["snippet_1", "snippet_2"],
                    "expected_resolution": "Clarify whether Khava protects, opposes, or created Enoch.",
                },
                {
                    "summary": "Resolved Khava's side of the relationship and left Enoch's broader role pending.",
                    "claim_decisions": [
                        {
                            "claim_id": "claim_khava_role",
                            "decision": "accept",
                            "edited_claim_text": "Khava is Enoch's protector.",
                            "confidence": 0.92,
                            "rationale": "The author's answer directly states this.",
                        }
                    ],
                    "author_claims": [
                        {
                            "target_entity_id": "entity_khava",
                            "target_entity_name": "Khava",
                            "claim_type": "relationship",
                            "claim_text": "Khava is Enoch's protector.",
                            "knowledge_track": "lore",
                            "confidence": 1.0,
                            "rationale": "Author clarified it in Story Questions.",
                        }
                    ],
                    "left_pending": [{"claim_id": "claim_enoch_role", "reason": "Plot role still needs detail."}],
                },
                {
                    "question_text": "What purpose does Enoch serve in the plot?",
                    "focus_type": "plot_role",
                    "rationale": "Only Enoch's unresolved role remains.",
                    "linked_claim_ids": ["claim_enoch_role"],
                    "linked_entities": [{"entity_id": "entity_enoch", "name": "Enoch"}],
                    "evidence_snippet_ids": ["snippet_2"],
                    "expected_resolution": "Clarify Enoch's role.",
                },
            ]

            with patch("pipeline.story_questions.call_model_chat", side_effect=model_payloads):
                with patch("pipeline.story_questions.load_review_memory", return_value={"story_question_answers": []}):
                    with patch("pipeline.story_questions.save_review_memory"):
                        first_question = generate_next_question(root, config_path)
                        application = apply_story_answer(root, "Khava is Enoch's protector.", config_path)
                        second_question = generate_next_question(root, config_path)

            self.assertEqual(first_question["linked_claim_ids"], ["claim_khava_role", "claim_enoch_role"])
            self.assertEqual(len(application["claim_decisions"]), 1)
            self.assertEqual(application["claim_decisions"][0]["reviewer"], "story_question_answer")
            self.assertTrue(application["claim_decisions"][0]["human_override"])
            self.assertEqual(application["claim_decisions"][0]["edited_claim_text"], "Khava is Enoch's protector.")
            self.assertEqual(len(application["author_claims"]), 1)
            self.assertEqual(application["unresolved_claim_count_after"], 1)
            self.assertEqual(second_question["unresolved_claim_count"], 1)
            self.assertEqual(second_question["linked_claim_ids"], ["claim_enoch_role"])

            decisions = json.loads((ArtifactPaths(root).claim_review_decisions).read_text(encoding="utf-8"))["decisions"]
            self.assertEqual(decisions[-1]["answer_id"], application["answer_id"])
            author_claims = json.loads((ArtifactPaths(root).author_claims).read_text(encoding="utf-8"))["claims"]
            self.assertEqual(author_claims[0]["source_priority"], "story_question_answer")
            display = story_question_display(root)
            self.assertEqual(display["pending_claim_count"], 1)
            self.assertEqual(display["question"]["question_text"], "What purpose does Enoch serve in the plot?")


class TestIdentityMergeGUIReview(unittest.TestCase):
    def test_identity_merge_inventory_browser_rows_and_overrides(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            proposals_path = root / "identity_merge_proposals.json"
            decisions_path = root / "identity_merge_decisions.json"

            # Create test proposals
            proposals_data = {
                "proposals": [
                    {
                        "proposal_id": "prop_1",
                        "source_entity_id": "entity_achilles",
                        "source_entity_name": "ACHILLES",
                        "target_entity_id": "entity_ruinr",
                        "target_entity_name": "RUINR",
                        "merge_type": "renamed",
                        "review_status": "pending",
                        "confidence": 0.9,
                        "rationale": "Renamed in sequence.",
                        "evidence_claim_ids": ["claim_1", "claim_2"]
                    }
                ]
            }
            write_json(proposals_path, proposals_data)

            # Test identity_merge_inventory_browser_rows with no decisions
            from pipeline.review_inventory import identity_merge_inventory_browser_rows, write_identity_merge_override_decision
            rows = identity_merge_inventory_browser_rows(proposals_path, decisions_path)
            self.assertEqual(len(rows), 1)
            row = rows[0]
            self.assertEqual(row["row_id"], "identity_merge:prop_1")
            self.assertEqual(row["row_kind"], "identity_merge")
            self.assertEqual(row["bucket"], "pending")
            self.assertEqual(row["candidate_name"], "ACHILLES -> RUINR")
            self.assertEqual(row["canonical_name"], "RUINR")
            self.assertEqual(row["proposed_entity_type"], "renamed")
            self.assertEqual(row["evidence_count"], 2)
            self.assertEqual(row["triage_reason"], "Renamed in sequence.")

            # Test write_identity_merge_override_decision
            write_identity_merge_override_decision(
                decisions_path,
                row,
                "approve",
                "human_reviewer",
                "manually confirmed renaming"
            )

            # Re-read decisions to verify
            decisions = json.loads(decisions_path.read_text(encoding="utf-8"))["decisions"]
            self.assertEqual(len(decisions), 1)
            self.assertEqual(decisions[0]["proposal_id"], "prop_1")
            self.assertEqual(decisions[0]["decision"], "approve")
            self.assertEqual(decisions[0]["reviewer"], "human_reviewer")
            self.assertEqual(decisions[0]["rationale"], "manually confirmed renaming")
            self.assertTrue(decisions[0]["human_override"])

            # Test reload rows with decisions
            rows = identity_merge_inventory_browser_rows(proposals_path, decisions_path)
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["bucket"], "approved")
            self.assertEqual(rows[0]["decision"], "approve")

    def test_identity_edge_refutation_excludes_disconnected_branch_on_cluster_approval(self) -> None:
        proposal = {
            "proposal_id": "cluster_1",
            "cluster_id": "cluster_1",
            "proposal_kind": "identity_cluster",
            "member_entity_ids": ["entity_a", "entity_b", "entity_c"],
            "member_entities": [
                {"entity_id": "entity_a", "card_id": "card_a", "canonical_name": "A", "aliases": []},
                {"entity_id": "entity_b", "card_id": "card_b", "canonical_name": "B", "aliases": []},
                {"entity_id": "entity_c", "card_id": "card_c", "canonical_name": "C", "aliases": []},
            ],
            "canonical_entity_id": "entity_c",
            "canonical_name": "C",
            "target_entity_id": "entity_c",
            "target_entity_name": "C",
            "member_edges": [
                {
                    "proposal_id": "edge_ab",
                    "source_entity_id": "entity_a",
                    "source_entity_name": "A",
                    "target_entity_id": "entity_b",
                    "target_entity_name": "B",
                    "evidence_claim_ids": ["claim_ab"],
                },
                {
                    "proposal_id": "edge_bc",
                    "source_entity_id": "entity_b",
                    "source_entity_name": "B",
                    "target_entity_id": "entity_c",
                    "target_entity_name": "C",
                    "evidence_claim_ids": ["claim_bc"],
                },
            ],
            "evidence_claim_ids": ["claim_ab", "claim_bc"],
            "source_snippet_ids": [],
            "alias_texts": ["A", "B"],
            "merge_type": "identity_cluster",
        }
        decisions = [
            {
                "decision_scope": "identity_edge",
                "cluster_id": "cluster_1",
                "edge_proposal_id": "edge_ab",
                "decision": "reject",
                "reviewer": "r",
                "rationale": "A is only related to B.",
                "timestamp_utc": "2026-05-19T00:00:00Z",
            },
            {
                "proposal_id": "cluster_1",
                "decision": "approve",
                "reviewer": "r",
                "rationale": "B and C are same entity.",
                "timestamp_utc": "2026-05-19T00:01:00Z",
            },
        ]
        memory = {"entity_merges": [], "approved_aliases": []}

        remember_identity_merge_decisions(memory, [proposal], decisions)

        merge_pairs = {(item["source_entity_id"], item["target_entity_id"]) for item in memory["entity_merges"]}
        self.assertEqual(merge_pairs, {("entity_b", "entity_c")})
        alias_texts = {item["alias_text"] for item in memory["approved_aliases"]}
        self.assertIn("B", alias_texts)
        self.assertNotIn("A", alias_texts)


if __name__ == "__main__":
    unittest.main()
