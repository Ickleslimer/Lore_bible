import unittest

from pipeline.card_first_review import (
    build_entity_evidence_bundle,
    entities_for_card_synthesis,
    should_auto_accept_claim,
    supplement_claim_decisions,
)
from pipeline.stage_11_card_synthesis import build_card_synthesis_prompt, validate_synthesis_support


class CardFirstReviewTests(unittest.TestCase):
    def test_should_auto_accept_lore_claim_with_snippets(self) -> None:
        claim = {
            "claim_id": "claim_1",
            "target_entity_id": "entity_enoch",
            "claim_text": "Enoch designed the lab.",
            "claim_type": "role",
            "knowledge_track": "lore",
            "confidence": 0.9,
            "source_snippet_ids": ["snippet_abc"],
        }
        ok, reason = should_auto_accept_claim(claim, set(), {"min_confidence": 0.0, "require_source_snippets_for_auto_accept": True})
        self.assertTrue(ok)
        self.assertIn("low_risk", reason)

    def test_should_not_auto_accept_theme_claim(self) -> None:
        claim = {
            "claim_id": "claim_2",
            "target_entity_id": "entity_enoch",
            "claim_text": "Enoch Theriac sounds anime-like.",
            "claim_type": "theme",
            "knowledge_track": "lore",
            "source_snippet_ids": ["snippet_abc"],
        }
        ok, _reason = should_auto_accept_claim(claim, set(), {})
        self.assertFalse(ok)

    def test_supplement_claim_decisions_adds_accept_for_pending_low_risk(self) -> None:
        claims = [
            {
                "claim_id": "claim_1",
                "target_entity_id": "entity_enoch",
                "claim_text": "Enoch designed the lab.",
                "claim_type": "background",
                "knowledge_track": "lore",
                "source_snippet_ids": ["snippet_abc"],
            }
        ]
        decisions, report = supplement_claim_decisions(claims, [], {}, {"enabled": True, "auto_accept_claims": True})
        self.assertEqual(len(report["auto_accepted"]), 1)
        self.assertEqual(decisions[0]["decision"], "accept")
        self.assertEqual(decisions[0]["reviewer"], "card_first_auto_accept")

    def test_entities_for_card_synthesis_includes_snippet_only_entity(self) -> None:
        entity = {
            "entity_id": "entity_enoch",
            "canonical_name": "Enoch",
            "aliases": [],
        }
        lore_clusters = [
            {"cluster_key": "Enoch", "snippet_ids": ["snippet_1", "snippet_2"]},
        ]
        out = entities_for_card_synthesis([entity], {}, lore_clusters, {"enabled": True})
        self.assertIn("entity_enoch", out)
        self.assertEqual(out["entity_enoch"], [])

    def test_build_card_synthesis_prompt_includes_evidence_bundle(self) -> None:
        entity = {"entity_id": "entity_enoch", "canonical_name": "Enoch", "entity_type": "character", "aliases": []}
        lore_clusters = [{"cluster_key": "Enoch", "snippet_ids": ["snippet_1"]}]
        bundle = build_entity_evidence_bundle(entity, [], lore_clusters, {"enabled": True})
        prompt = build_card_synthesis_prompt(
            entity,
            [],
            {},
            evidence_bundle=bundle,
            config={"card_first_synthesis": {"enabled": True}},
            source_snippets_by_id={
                "snippet_1": {
                    "snippet_id": "snippet_1",
                    "display_text_normalized": "Enoch oversees the lab sequence.",
                }
            },
        )
        self.assertIn("Approved entity evidence bundle", prompt)
        self.assertIn("Card-first evidence rule", prompt)
        self.assertIn("snippet_1", prompt)
        self.assertIn("Enoch oversees the lab sequence.", prompt)

    def test_validate_synthesis_support_accepts_snippet_ids(self) -> None:
        entity = {"canonical_name": "Enoch", "entity_type": "character"}
        bundle = {
            "approved_snippet_ids": ["snippet_abc"],
            "approved_claim_ids": [],
            "card_first_synthesis": True,
        }
        synthesis = {
            "summary": "Enoch is the engineer behind the lab.",
            "sections": {
                "background": "Enoch built the facility.",
                "role_in_story": "",
                "relationships": "",
                "timeline": "",
                "inspirations": "",
                "open_questions": "",
            },
            "support_map": {
                "summary": ["snippet_abc"],
                "background": ["snippet_abc"],
                "role_in_story": [],
                "relationships": [],
                "timeline": [],
                "inspirations": [],
                "open_questions": [],
            },
            "relationships": [],
            "timeline": [],
            "wiki_links": [],
            "resolved_conflicts": [],
            "unresolved_conflicts": [],
        }
        validate_synthesis_support(
            entity,
            [],
            {},
            synthesis,
            evidence_bundle=bundle,
            config={"card_first_synthesis": {"enabled": True}},
        )


if __name__ == "__main__":
    unittest.main()
