from __future__ import annotations

import json
import os
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

from pipeline.common import stable_id
from pipeline.auto_review import AutoReviewResult, _auto_review_conversation_entities
from pipeline.entity_resolution import normalized_name_key, resolve_entities
from pipeline.mixtral_anchor_provider import (
    _call_gemini_chat,
    _extract_inline_responses,
    _gemini_batch_state,
    _inline_response_payload,
    build_stage_a_prompt,
    model_call_kwargs,
)
from pipeline.review_memory import relevant_memory_for_entity
from pipeline.run_pipeline import determine_resume_start_stage
from pipeline.stage_a_bootstrap import infer_entities
from pipeline.stage_b3_segment_conversations import normalize_model_segments, run as run_stage_b3
from pipeline.stage_b4_conversation_patch_notes import run as run_stage_b4
from pipeline.stage_c_extract import run as run_stage_c
from pipeline.stage_d_group import run as run_stage_d
from pipeline.stage_e_alias import annotate_conversation_entity_proposals, infer_type_evidence_for_candidate, normalize_entity_type, run as run_stage_e
from pipeline.stage_f_draft import build_claim_extraction_prompt, run as run_stage_f
from pipeline.stage_g_merge_engine import build_card_synthesis_prompt, find_unsupported_acronym_expansions, run as run_stage_g
from pipeline.stage_h_notion_export import run as run_stage_h
from theriac_lore_desktop import (
    attach_log_paths_for_run,
    candidate_inventory_browser_rows,
    candidate_inventory_category,
    load_project_env,
    sort_candidate_inventory_rows,
    write_candidate_inventory_override_decision,
)
from pipeline.ui_review_app import (
    build_app,
    discover_review_runs,
    is_pipeline_progress_log_line,
    new_run_artifacts_root,
    pending_review_counts_for_root,
    pipeline_progress_artifact_snapshot,
    pipeline_progress_from_logs,
    render_run_selector_html,
    render_pipeline_progress_html,
)


def write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(json.dumps(row) for row in rows) + "\n", encoding="utf-8")


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
    write_json(root / "05_alias" / "conversation_entity_proposals.json", {"proposals": []})
    write_json(root / "04_grouping" / "snippet_clusters_lore.json", {"clusters": []})
    write_json(root / "04_grouping" / "snippet_clusters_meta.json", {"clusters": []})
    write_json(root / "06_drafts" / "card_drafts" / "claim_drafts.json", {"claims": claims or []})
    write_json(root / "06_drafts" / "card_drafts" / "meta_cards_draft.json", {"meta_cards": []})


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
    with patch("pipeline.stage_b3_segment_conversations.call_mixtral_chat", side_effect=model_payloads):
        run_stage_b3(
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
    def test_stage_b4_patch_notes_preserve_global_chronological_order(self) -> None:
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

            with patch("pipeline.stage_b4_conversation_patch_notes.call_mixtral_chat", side_effect=fake_patch_note):
                run_stage_b4(
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

    def test_stage_b4_resumes_existing_checkpoint_without_restarting(self) -> None:
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

            with patch("pipeline.stage_b4_conversation_patch_notes.call_mixtral_chat", side_effect=fake_patch_note):
                run_stage_b4(
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

    def test_stage_b4_demotes_tiny_indirect_reference_to_no_durable_development(self) -> None:
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
                "pipeline.stage_b4_conversation_patch_notes.call_mixtral_chat",
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
                run_stage_b4(
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

    def test_stage_c_attaches_conversation_patch_note_context(self) -> None:
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
            write_json(root / "config.json", {"stage_c_anchor_provider": "conversation_metadata"})
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

            run_stage_c(
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

    def test_stage_c_materializes_patch_note_items_as_evidence_snippets(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            first = msg_row("m1", "2025-01-10T10:00:00Z", content="HECTR coordinates the lab.")
            first.update({"conversation_id": "conv_1", "dm_pair_id": "pair_1", "conversation_message_index": 0})
            second = msg_row("m2", "2025-01-10T10:02:00Z", content="ACHILLES is later treated as the same system as RUINR.")
            second.update({"conversation_id": "conv_1", "dm_pair_id": "pair_1", "conversation_message_index": 1})
            write_jsonl(root / "messages.jsonl", [first, second])
            write_json(root / "profiles.json", {"profiles": []})
            write_json(root / "config.json", {"stage_c_anchor_provider": "conversation_metadata"})
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

            run_stage_c(
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

    def test_stage_c_skips_no_durable_patch_note_conversations(self) -> None:
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
            write_json(root / "config.json", {"stage_c_anchor_provider": "conversation_metadata"})
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

            run_stage_c(
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

        with patch("pipeline.mixtral_anchor_provider.urllib.request.urlopen", side_effect=fake_urlopen):
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

    def test_model_routing_selects_regular_flash_for_synthesis(self) -> None:
        config = {
            "mixtral": {
                "provider": "gemini",
                "api_model": "gemini-2.5-flash-lite",
                "rate_state_path": "artifacts/learning/gemini_flash_lite_rate_runtime.json",
            },
            "model_routing": {
                "profiles": {
                    "cheap": {
                        "api_model": "gemini-2.5-flash-lite",
                        "rate_state_path": "artifacts/learning/gemini_flash_lite_rate_runtime.json",
                    },
                    "reasoning": {
                        "api_model": "gemini-2.5-flash",
                        "rate_state_path": "artifacts/learning/gemini_flash_rate_runtime.json",
                    },
                },
                "tasks": {
                    "stage_f_claim_extraction": {"profile": "cheap", "batch_enabled": True},
                    "stage_g_card_synthesis": {"profile": "reasoning", "batch_enabled": False},
                },
            },
        }

        claim_kwargs = model_call_kwargs(config, "stage_f_claim_extraction")
        synthesis_kwargs = model_call_kwargs(config, "stage_g_card_synthesis")

        self.assertEqual(claim_kwargs["api_model"], "gemini-2.5-flash-lite")
        self.assertEqual(synthesis_kwargs["api_model"], "gemini-2.5-flash")
        self.assertIn("gemini_flash_rate_runtime", str(synthesis_kwargs["rate_state_path"]))

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
        prompt = build_stage_a_prompt("Exit Music (For A Film) is the destructive path conclusion.")

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

    def test_stage_b3_splits_only_after_more_than_12_hour_gap(self) -> None:
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

    def test_stage_b3_uses_batch_mode_for_model_windows_when_enabled(self) -> None:
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
                        "tasks": {"stage_b3_segmentation": {"profile": "cheap", "batch_enabled": True, "batch_max_requests": 10}},
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

            with patch("pipeline.stage_b3_segment_conversations.call_gemini_batch_json", side_effect=fake_batch) as batch:
                with patch("pipeline.stage_b3_segment_conversations.call_mixtral_chat", side_effect=AssertionError("sync model should not be used")):
                    run_stage_b3(
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

    def test_stage_b3_drops_irrelevant_model_returned_spans(self) -> None:
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

    def test_stage_b3_generic_seed_tokens_do_not_create_direct_signal(self) -> None:
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

    def test_stage_b3_keeps_lore_and_meta_topic_shift_segments(self) -> None:
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

    def test_stage_b3_different_dm_pairs_never_merge(self) -> None:
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

    def test_stage_b3_bot_author_does_not_change_dm_pair(self) -> None:
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

    def test_stage_b3_accepts_model_returned_message_indexes(self) -> None:
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

    def test_stage_b3_accepts_numeric_message_id_fields_as_indexes(self) -> None:
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

    def test_stage_b3_reports_and_handles_overlapping_model_segments(self) -> None:
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

    def test_stage_b3_relevance_gate_drops_external_media_without_theriac_tie(self) -> None:
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

    def test_stage_b3_relevance_gate_keeps_external_inspiration_with_seed_anchor(self) -> None:
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

    def test_stage_b3_relevance_gate_accepts_distinctive_seed_name_token(self) -> None:
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

    def test_stage_b3_failure_records_model_window_count_and_payload_preview(self) -> None:
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

            with patch("pipeline.stage_b3_segment_conversations.call_mixtral_chat", side_effect=[invalid_payload, {"segments": []}]):
                with self.assertRaises(RuntimeError):
                    run_stage_b3(
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

    def test_stage_b3_checkpoints_completed_model_windows_before_interrupt(self) -> None:
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

            with patch("pipeline.stage_b3_segment_conversations.call_mixtral_chat", side_effect=[first_payload, KeyboardInterrupt]):
                with self.assertRaises(KeyboardInterrupt):
                    run_stage_b3(
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

    def test_stage_c_context_windows_do_not_cross_conversation_id(self) -> None:
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

            run_stage_c(
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

    def test_stage_c_uses_conversation_metadata_without_per_message_model_call(self) -> None:
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

            with patch("pipeline.stage_c_extract.call_mixtral_chat", side_effect=AssertionError("Stage 06 should not call the model")) as mocked_model:
                run_stage_c(
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

    def test_stage_a_outputs_entity_seeds_not_canonical_cards(self) -> None:
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

    def test_stage_e_promotes_only_currently_observed_seed_entities(self) -> None:
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

            run_stage_e(
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

    def test_stage_e_candidate_metadata_can_map_literal_concise_entity_anchor(self) -> None:
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

            run_stage_e(
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

    def test_stage_e_ignores_unbacked_candidate_metadata_anchor(self) -> None:
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

            run_stage_e(
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

    def test_stage_e_proposes_text_observed_conversation_entity_not_in_seed(self) -> None:
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
                run_stage_e(
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

    def test_stage_e_proposes_entity_from_patch_note_evidence_text(self) -> None:
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
                run_stage_e(
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

    def test_stage_e_triages_low_evidence_phrase_to_candidate_inventory(self) -> None:
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

            run_stage_e(
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

    def test_stage_e_triages_team_contributor_to_meta_inventory(self) -> None:
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

            run_stage_e(
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

    def test_stage_e_triages_external_media_characters_to_inventory(self) -> None:
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

            run_stage_e(
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

    def test_stage_e_keeps_external_name_for_review_when_adopted_into_theriac(self) -> None:
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
                run_stage_e(
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

    def test_stage_e_triages_reference_inspirations_to_inventory(self) -> None:
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

            run_stage_e(
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

    def test_stage_e_keeps_reference_name_for_review_when_adopted_into_theriac(self) -> None:
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
                run_stage_e(
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

    def test_stage_e_approved_candidate_alias_attaches_to_existing_entity(self) -> None:
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

            run_stage_e(
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

    def test_stage_e_model_alias_resolution_prefills_review_proposal(self) -> None:
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
                        "profiles": {"flash_regular": {"provider": "gemini", "api_model": "gemini-2.5-flash"}},
                        "tasks": {
                            "stage_e_alias_resolution": {
                                "profile": "flash_regular",
                                "enabled": True,
                                "max_evidence_per_call": 10,
                            }
                        },
                    },
                    "mixtral": {"provider": "gemini", "timeout_seconds": 60},
                },
            )

            with patch("pipeline.stage_e_alias.call_mixtral_chat") as mock_model:
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
                    run_stage_e(
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

    def test_stage_e_decision_rerun_skips_prior_alias_grouping(self) -> None:
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
                        "profiles": {"flash_regular": {"provider": "gemini", "api_model": "gemini-2.5-flash"}},
                        "tasks": {"stage_e_alias_resolution": {"profile": "flash_regular", "enabled": True}},
                    },
                    "mixtral": {"provider": "gemini", "timeout_seconds": 60},
                },
            )

            with patch("pipeline.stage_e_alias.call_mixtral_chat") as mock_model:
                mock_model.side_effect = AssertionError("alias grouping should not rerun after decisions")
                run_stage_e(
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

    def test_stage_e_model_candidate_alias_resolution_handles_name_variants(self) -> None:
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
                        "profiles": {"flash_regular": {"provider": "gemini", "api_model": "gemini-2.5-flash"}},
                        "tasks": {
                            "stage_e_alias_resolution": {
                                "profile": "flash_regular",
                                "enabled": True,
                                "max_evidence_per_call": 10,
                                "max_candidates_per_call": 10,
                            }
                        },
                    },
                    "mixtral": {"provider": "gemini", "timeout_seconds": 60},
                },
            )

            with patch("pipeline.stage_e_alias.call_mixtral_chat") as mock_model:
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
                    run_stage_e(
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

    def test_stage_e_model_candidate_alias_resolution_accepts_list_response(self) -> None:
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
                            "stage_e_alias_resolution": {
                                "enabled": True,
                                "max_candidates_per_call": 10,
                            }
                        }
                    },
                    "mixtral": {"provider": "gemini", "timeout_seconds": 60},
                },
            )

            with patch("pipeline.stage_e_alias.call_mixtral_chat") as mock_model:
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
                    run_stage_e(
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

    def test_stage_e_model_candidate_alias_resolution_accepts_provider_list_wrapper(self) -> None:
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
            write_json(root / "config.json", {"model_routing": {"tasks": {"stage_e_alias_resolution": {"enabled": True}}}})

            with patch("pipeline.stage_e_alias.call_mixtral_chat") as mock_model:
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
                    run_stage_e(
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

    def test_stage_e_reconsiders_candidate_type_from_aggregated_usage(self) -> None:
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
                run_stage_e(
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

    def test_stage_e_recent_specific_character_evidence_reaches_review_with_four_mentions(self) -> None:
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
                run_stage_e(
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

    def test_stage_e_approved_conversation_entity_promotes_to_resolved_and_groups(self) -> None:
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

            run_stage_e(
                root / "snippets.jsonl",
                root / "entity_seed.json",
                root / "aliases.json",
                root / "timelines.json",
                root / "resolved_entities.json",
                None,
                root / "conversation_entity_proposals.json",
                root / "conversation_entity_decisions.json",
            )
            run_stage_d(
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

    def test_stage_e_persists_conversation_entity_decisions_to_memory(self) -> None:
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

            run_stage_e(
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
            run_stage_e(
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

    def test_stage_e_persists_rejected_conversation_entity_and_suppresses_future_proposal(self) -> None:
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

            run_stage_e(
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
            run_stage_e(
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

    def test_stage_d_uses_entity_anchors_instead_of_first_word_clusters(self) -> None:
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

            run_stage_d(
                root / "snippets.jsonl",
                root / "resolved_entities.json",
                root / "lore_clusters.json",
                root / "meta_clusters.json",
            )

            keys = {cluster["cluster_key"] for cluster in json.loads((root / "lore_clusters.json").read_text(encoding="utf-8"))["clusters"]}
            self.assertIn("OYUUN", keys)
            self.assertIn("unmapped", keys)
            self.assertNotIn("and", keys)

    def test_stage_f_prompt_preserves_external_references_as_inspiration_claims(self) -> None:
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

    def test_stage_f_extracts_atomic_claims_without_raw_append(self) -> None:
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

            with patch("pipeline.stage_f_draft.call_mixtral_chat", return_value={"claims": [{"claim_text": "HECTR is a template ancestor for Krypteia AI systems.", "claim_type": "relationship", "source_snippet_ids": ["s1"], "confidence": 0.82, "contradiction_notes": ""}]}):
                run_stage_f(
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

    def test_stage_f_uses_batch_mode_for_claim_extraction_when_enabled(self) -> None:
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
                        "tasks": {"stage_f_claim_extraction": {"profile": "cheap", "batch_enabled": True, "batch_max_requests": 10}},
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

            with patch("pipeline.stage_f_draft.call_gemini_batch_json", side_effect=fake_batch) as batch:
                with patch("pipeline.stage_f_draft.call_mixtral_chat", side_effect=AssertionError("sync model should not be used")):
                    run_stage_f(
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

    def test_stage_f_logs_failed_claim_extraction_and_continues(self) -> None:
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
            write_json(root / "config.json", {"mixtral": {"claim_extraction_validation_retries": 0}})

            with patch(
                "pipeline.stage_f_draft.call_mixtral_chat",
                side_effect=[
                    {"not_claims": []},
                    {"claims": [{"claim_text": "HECTR is related to Krypteia AI systems.", "claim_type": "relationship", "confidence": 0.82, "contradiction_notes": ""}]},
                ],
            ):
                run_stage_f(
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

    def test_stage_g_synthesizes_drafts_and_requires_card_approval_for_canon(self) -> None:
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

            with patch("pipeline.stage_g_merge_engine.call_mixtral_chat", return_value=model_card):
                run_stage_g(
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
            with patch("pipeline.stage_g_merge_engine.call_mixtral_chat", return_value=model_card):
                run_stage_g(
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

    def test_stage_g_card_synthesis_prompt_requests_full_fandom_wiki_style_entry(self) -> None:
        prompt = build_card_synthesis_prompt(
            {"canonical_name": "HECTR", "entity_type": "character"},
            [{"claim_id": "claim1", "claim_text": "HECTR matters to Krypteia.", "claim_type": "relationship"}],
            {},
        )

        self.assertIn("comparable in shape and density to a strong fandom wiki page", prompt)
        self.assertIn("Write polished article prose", prompt)
        self.assertIn("compact lead paragraph", prompt)
        self.assertIn("Word target plan", prompt)
        self.assertIn("section_word_targets", prompt)

    def test_stage_g_card_synthesis_prompt_includes_original_source_snippet_context(self) -> None:
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

    def test_stage_g_card_synthesis_prompt_scales_word_targets_for_developed_entities(self) -> None:
        claims = [
            {"claim_id": f"claim{i}", "claim_text": f"Enoch development detail {i}.", "claim_type": "role"}
            for i in range(1, 10)
        ]
        prompt = build_card_synthesis_prompt({"canonical_name": "Enoch", "entity_type": "character"}, claims, {})

        self.assertIn('"min": 400', prompt)
        self.assertIn('"max": 800', prompt)
        self.assertIn("Heavily developed characters", prompt)

    def test_stage_g_draft_cards_include_wiki_links_to_related_cards(self) -> None:
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

            with patch("pipeline.stage_g_merge_engine.call_mixtral_chat", return_value=model_card):
                run_stage_g(
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

    def test_stage_g_writes_inspiration_section_from_accepted_inspiration_claims(self) -> None:
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

            with patch("pipeline.stage_g_merge_engine.call_mixtral_chat", return_value=model_card):
                run_stage_g(
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

    def test_stage_g_revises_from_full_accepted_claim_history(self) -> None:
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
                "summary": "HECTR is a template ancestor for Krypteia AI systems. HECTR is unaware of RUINR because Krypteia firewalls hide RUINR from him.",
                "sections": {"background": "HECTR is a template ancestor for Krypteia AI systems.", "role_in_story": "HECTR is unaware of RUINR because Krypteia firewalls hide RUINR from him.", "relationships": "", "timeline": "", "open_questions": ""},
                "relationships": [],
                "timeline": [],
                "resolved_conflicts": [],
                "unresolved_conflicts": [],
                "support_map": {"summary": ["old_claim", "new_claim"], "background": ["old_claim"], "role_in_story": ["new_claim"], "relationships": [], "timeline": [], "open_questions": [], "resolved_conflicts": [], "unresolved_conflicts": []},
            }

            with patch("pipeline.stage_g_merge_engine.call_mixtral_chat", return_value=model_card):
                run_stage_g(
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

    def test_stage_g_stores_accepted_alias_claims_for_future_resolution(self) -> None:
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

            with patch("pipeline.stage_g_merge_engine.call_mixtral_chat", return_value=model_card):
                run_stage_g(
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

    def test_stage_g_requires_review_then_applies_identity_merge_from_rename_claim(self) -> None:
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

            with patch("pipeline.stage_g_merge_engine.call_mixtral_chat") as model:
                with self.assertRaisesRegex(RuntimeError, "identity merge proposal"):
                    run_stage_g(
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
                model.assert_not_called()

            proposals = json.loads((root / "identity_merge_proposals.json").read_text(encoding="utf-8"))["proposals"]
            self.assertEqual(len(proposals), 1)
            proposal = proposals[0]
            self.assertEqual(proposal["source_entity_name"], "ACHILLES")
            self.assertEqual(proposal["target_entity_name"], "RUINR")
            self.assertEqual(proposal["review_status"], "pending")
            self.assertEqual(proposal["evidence_claim_ids"], ["rename_claim"])

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

            with patch("pipeline.stage_g_merge_engine.call_mixtral_chat", return_value=model_card):
                run_stage_g(
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

    def test_stage_g_rejects_unsupported_acronym_expansion(self) -> None:
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

            with patch("pipeline.stage_g_merge_engine.call_mixtral_chat", return_value=model_card):
                with self.assertRaisesRegex(RuntimeError, "unsupported acronym expansion"):
                    run_stage_g(
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

    def test_stage_g_acronym_guard_allows_parenthetical_continuity(self) -> None:
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

    def test_stage_g_drops_unclaimed_open_questions(self) -> None:
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

            with patch("pipeline.stage_g_merge_engine.call_mixtral_chat", return_value=model_card):
                run_stage_g(
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

            run_stage_h(root / "cards.json", root / "meta.json", root / "aliases.json", root / "snips.jsonl", root / "profiles.json", root / "log.jsonl", root / "notion.ndjson")

            self.assertEqual((root / "notion.ndjson").read_text(encoding="utf-8"), "")

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
        self.assertEqual(progress["summary"], "Running stage 2/11: Message Normalization")

        html = render_pipeline_progress_html(progress)
        self.assertIn('id="pipeline-progress"', html)
        self.assertIn('data-stage-index="2"', html)
        self.assertIn("pipeline-stage current", html)

    def test_ui_pipeline_progress_uses_stage05_model_call_heartbeat(self) -> None:
        line = (
            "13:37:51 | INFO | pipeline.stage_b4_conversation_patch_notes | "
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
            older = root / "run_from_b4_old.err.log"
            newer = root / "run_from_stage05_new.err.log"
            older.write_text("old", encoding="utf-8")
            newer.write_text("new", encoding="utf-8")

            paths = attach_log_paths_for_run(root, "run_from_b4")

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
        self.assertIn("stage 7/11", progress["summary"])

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
        self.assertEqual(progress["summary"], "Paused for review at stage 7/11: Entity Resolution")

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

    def test_stage_e_prefers_human_override_over_later_auto_review(self) -> None:
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

    def test_pipeline_resume_starts_at_entity_resolution_after_entity_decisions(self) -> None:
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

            self.assertEqual(stage, 7)
            self.assertIn("decisions changed", reason)

    def test_pipeline_resume_starts_at_patch_notes_when_stage_four_is_done(self) -> None:
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
            write_json(root / "07_review" / "claim_review_decisions.json", {"decisions": [{"claim_id": "claim_a", "decision": "accept"}]})

            stage, reason = determine_resume_start_stage(root)

            self.assertEqual(stage, 10)
            self.assertIn("Stage 10", reason)

    def test_pipeline_resume_pauses_for_card_review_before_export(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write_pipeline_artifacts_through_stage9(root, [{"claim_id": "claim_a"}])
            write_json(root / "07_review" / "claim_review_decisions.json", {"decisions": [{"claim_id": "claim_a", "decision": "accept"}]})
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
            write_json(root / "07_review" / "claim_review_decisions.json", {"decisions": [{"claim_id": "claim_a", "decision": "accept"}]})
            write_json(root / "07_review" / "card_review_decisions.json", {"decisions": [{"card_id": "card_a", "decision": "approve"}]})
            write_json(root / "07_review" / "card_drafts.json", {"cards": [{"card_id": "card_a", "status": "draft"}]})
            write_json(root / "07_review" / "canonical_cards.json", {"cards": [{"card_id": "card_a", "status": "canonical"}]})
            write_jsonl(root / "07_review" / "merge_log.jsonl", [{"decision_id": "d1"}])

            stage, reason = determine_resume_start_stage(root)

            self.assertEqual(stage, 11)
            self.assertIn("Stage 11", reason)

    def test_new_run_artifacts_root_creates_unique_run_folder(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            repo = Path(tmp)
            first = new_run_artifacts_root(repo)
            second = new_run_artifacts_root(repo)

            self.assertTrue(first.exists())
            self.assertTrue(second.exists())
            self.assertNotEqual(first, second)
            self.assertEqual(first.parent, repo / "artifacts" / "runs")

    def test_desktop_launcher_loads_project_env_for_frozen_worker(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            repo = Path(tmp)
            (repo / ".env").write_text(
                'GEMINI_API_KEY="fake-gemini"\nMixtral_API_Key: "fake-mixtral"\n',
                encoding="utf-8",
            )
            with patch.dict("os.environ", {}, clear=True):
                load_project_env(repo)
                self.assertEqual(__import__("os").environ["GEMINI_API_KEY"], "fake-gemini")
                self.assertEqual(__import__("os").environ["Mixtral_API_Key"], "fake-mixtral")

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


if __name__ == "__main__":
    unittest.main()
