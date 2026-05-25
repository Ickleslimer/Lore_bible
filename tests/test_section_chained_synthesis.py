import unittest

from pipeline.card_first_review import (
    card_first_synthesis_config,
    is_protagonist_tier_entity,
    protagonist_word_target_plan,
    rank_snippet_ids,
    section_word_targets_for_entity,
    should_use_section_chained_synthesis,
    snippet_ids_for_entity,
)
from pipeline.section_chained_synthesis import snippet_ids_for_section


class SectionChainedSynthesisTests(unittest.TestCase):
    def test_protagonist_tier_detects_dense_character(self) -> None:
        entity = {"canonical_name": "Enoch", "entity_type": "character"}
        cfg = card_first_synthesis_config({"card_first_synthesis": {"protagonist_tier": {"min_approved_snippets": 80}}})
        self.assertTrue(is_protagonist_tier_entity(entity, 1017, cfg))
        self.assertTrue(should_use_section_chained_synthesis(entity, 1017, cfg))

    def test_protagonist_tier_skips_sparse_term(self) -> None:
        entity = {"canonical_name": "Some Term", "entity_type": "term"}
        cfg = card_first_synthesis_config({})
        self.assertFalse(is_protagonist_tier_entity(entity, 200, cfg))
        self.assertFalse(should_use_section_chained_synthesis(entity, 200, cfg))

    def test_protagonist_word_targets_gendo_scale(self) -> None:
        plan = protagonist_word_target_plan(120, 7)
        self.assertEqual(plan["synthesis_tier"], "protagonist")
        self.assertGreaterEqual(plan["total_word_target"]["min"], 1200)
        self.assertGreaterEqual(plan["total_word_target"]["max"], 1800)
        self.assertIn("400-600", plan["section_word_targets"]["background"])

    def test_section_word_targets_uses_protagonist_for_enoch(self) -> None:
        entity = {"canonical_name": "Enoch", "entity_type": "character"}
        plan = section_word_targets_for_entity([], 1017, {"card_first_synthesis": {}}, entity=entity)
        self.assertEqual(plan["synthesis_tier"], "protagonist")

    def test_rank_snippet_ids_prefers_patch_notes(self) -> None:
        source = {
            "snippet_meta": {
                "snippet_id": "snippet_meta",
                "display_text_normalized": "Radiohead playlist working title for Enoch.",
                "conversation_global_index": 1,
            },
            "snippet_patch": {
                "snippet_id": "snippet_patch",
                "display_text_normalized": "Enoch founded the lab after the moratorium.",
                "patch_item_type": "role_change",
                "conversation_global_index": 2,
            },
        }
        ranked = rank_snippet_ids(["snippet_meta", "snippet_patch"], source)
        self.assertEqual(ranked[0], "snippet_patch")

    def test_snippet_ids_for_section_prioritizes_hints(self) -> None:
        source = {
            "snippet_a": {
                "snippet_id": "snippet_a",
                "display_text_normalized": "Enoch enjoys music references.",
                "conversation_global_index": 1,
            },
            "snippet_b": {
                "snippet_id": "snippet_b",
                "display_text_normalized": "Khava and Enoch share a close relationship in the lab.",
                "conversation_global_index": 2,
            },
        }
        chosen = snippet_ids_for_section("relationships", ["snippet_a", "snippet_b"], source, limit=1)
        self.assertEqual(chosen, ["snippet_b"])

    def test_backfill_support_map_from_section_drafts(self) -> None:
        from pipeline.section_chained_synthesis import _backfill_support_map_from_section_drafts

        merged = {"support_map": {"background": []}, "sections": {"background": "text"}}
        section_results = {
            "background": {"prose": "text", "support_ids": ["snippet_a", "snippet_b"]},
        }
        _backfill_support_map_from_section_drafts(merged, section_results)
        self.assertEqual(merged["support_map"]["background"], ["snippet_a", "snippet_b"])

    def test_snippet_ids_for_entity_unbounded_cluster(self) -> None:
        entity = {"canonical_name": "Enoch", "aliases": []}
        clusters = [{"cluster_key": "Enoch", "snippet_ids": [f"snippet_{index}" for index in range(5)]}]
        ids = snippet_ids_for_entity(entity, clusters, max_snippets=10_000)
        self.assertEqual(len(ids), 5)


if __name__ == "__main__":
    unittest.main()
