from __future__ import annotations

import unittest

from pipeline.ledger_entry_validation import validate_ledger_entries


class LedgerEntryValidationTests(unittest.TestCase):
    def test_low_confidence_rejected_for_heterogeneous(self) -> None:
        entries = [
            {
                "entry_id": "e1",
                "event_kind": "change",
                "change_type": "other",
                "subject_label": "Foo",
                "headline": "Foo — meaningful headline with enough detail for validation",
                "confidence": 0.5,
                "source_segment_id": "seg1",
                "global_sequence": 1,
                "supporting_message_ids": ["m1"],
            }
        ]
        accepted, rejected, _queue = validate_ledger_entries(
            entries,
            model_family="openrouter_free_auto",
            validation_cfg={
                "heterogeneous": {"min_confidence": 0.70, "max_entries_per_segment": 6},
            },
            prior_entries=[],
            by_name={},
        )
        self.assertEqual(len(accepted), 0)
        self.assertEqual(len(rejected), 1)
        self.assertIn("confidence", rejected[0]["validation_errors"][0])

    def test_provenance_fields_preserved_on_accept(self) -> None:
        entries = [
            {
                "entry_id": "e1",
                "event_kind": "new",
                "change_type": "entity_introduced",
                "subject_label": "Foo",
                "subject_entity_id": "entity_1",
                "headline": "Entity — Foo — introduced with a sufficiently long headline",
                "confidence": 0.9,
                "source_segment_id": "seg1",
                "global_sequence": 1,
                "supporting_message_ids": ["m1"],
                "inference_profile": "nim_deepseek_flash",
                "inference_lane_tier": "heterogeneous",
            }
        ]
        accepted, rejected, queue = validate_ledger_entries(
            entries,
            model_family="deepseek_v4_flash",
            validation_cfg={},
            prior_entries=[],
            by_name={},
        )
        self.assertEqual(len(rejected), 0)
        self.assertEqual(len(accepted), 1)
        self.assertTrue(queue)

    def test_canonical_name_requires_before_after(self) -> None:
        entries = [
            {
                "entry_id": "e1",
                "event_kind": "change",
                "change_type": "canonical_name",
                "subject_label": "Loss",
                "headline": "Loss — given canonical name Enoch with enough detail here",
                "confidence": 0.9,
                "source_segment_id": "seg1",
                "global_sequence": 1,
                "supporting_message_ids": ["m1"],
                "before": "",
                "after": "Enoch",
            }
        ]
        _, rejected, _ = validate_ledger_entries(
            entries,
            model_family="deepseek_v4_flash",
            validation_cfg={},
            prior_entries=[],
            by_name={},
        )
        self.assertEqual(len(rejected), 1)


if __name__ == "__main__":
    unittest.main()
