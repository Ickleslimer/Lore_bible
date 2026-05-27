from __future__ import annotations

import unittest

from pipeline.ledger_quality_metrics import evaluate_quality_gate, ledger_entry_metrics


def _sample_entries(count: int, *, placeholder: bool = False) -> list[dict]:
    rows = []
    for i in range(count):
        headline = (
            "Entity — Foo — placeholder TBD concept"
            if placeholder
            else f"Entity — Foo — introduced with sufficient detail number {i} for metrics"
        )
        rows.append(
            {
                "headline": headline,
                "confidence": 0.85,
                "subject_entity_id": f"entity_{i}",
                "supporting_message_ids": ["m1"],
                "supporting_snippet_ids": ["s1"],
                "source_segment_id": f"seg_{i}",
            }
        )
    return rows


class LedgerQualityGateTests(unittest.TestCase):
    def test_metrics_basic(self) -> None:
        metrics = ledger_entry_metrics(_sample_entries(4))
        self.assertEqual(metrics["count"], 4)
        self.assertEqual(metrics["unique_segments"], 4)

    def test_quality_gate_passes_similar_batch(self) -> None:
        baseline = ledger_entry_metrics(_sample_entries(10))
        batch = ledger_entry_metrics(_sample_entries(5))
        passed, reasons = evaluate_quality_gate(batch, baseline)
        self.assertTrue(passed)
        self.assertEqual(reasons, [])

    def test_quality_gate_fails_on_placeholder_spike(self) -> None:
        baseline = ledger_entry_metrics(_sample_entries(10))
        batch = ledger_entry_metrics(_sample_entries(5, placeholder=True))
        passed, reasons = evaluate_quality_gate(batch, baseline)
        self.assertFalse(passed)
        self.assertTrue(any("placeholder" in r for r in reasons))


if __name__ == "__main__":
    unittest.main()
