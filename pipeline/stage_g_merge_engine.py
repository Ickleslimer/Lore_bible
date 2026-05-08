from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any

from pipeline.author_directives import apply_directive_to_card, parse_author_instruction
from pipeline.common import get_logger, now_utc_iso, read_json, safe_uuid, write_json, write_jsonl


VALID_DECISIONS = {"accept", "reject", "defer", "needs_more_context"}
SOURCE_PRIORITY = ["discord_inference_draft", "lore_bible_seed", "approved_canon", "author_directive"]


def apply_decisions(
    seed_cards: list[dict[str, Any]],
    lore_patches: list[dict[str, Any]],
    decisions: list[dict[str, Any]],
    author_directives: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    cards = {c["card_id"]: c for c in seed_cards}
    decision_by_claim = {d["claim_id"]: d for d in decisions}
    merge_log: list[dict[str, Any]] = []

    # Highest source of truth: direct author directives.
    for directive in author_directives:
        card_id = directive.get("target_card_id")
        if not card_id:
            continue
        if card_id not in cards:
            cards[card_id] = {
                "card_id": card_id,
                "entity_type": "term",
                "canonical_name": card_id,
                "aliases": [],
                "status": "draft",
                "summary": "",
                "details": {},
                "timeline": [],
                "relationships": [],
                "source_evidence": [],
                "confidence": {"score": 1.0, "reviewer_note": "Author directive."},
                "revision_history": [],
            }
        if "parsed_payload" not in directive:
            directive["parsed_payload"] = parse_author_instruction(str(directive.get("instruction_text", "")))
        cards[card_id], directive_note = apply_directive_to_card(cards[card_id], directive)
        cards[card_id]["revision_history"].append(
            {
                "timestamp_utc": now_utc_iso(),
                "action": "author_directive_applied",
                "actor": directive.get("author", "author"),
                "decision": "accept",
                "rationale": str(directive.get("instruction_text", "")),
            }
        )
        merge_log.append(
            {
                "decision_id": safe_uuid(),
                "claim_id": directive.get("directive_id", safe_uuid()),
                "card_id": card_id,
                "knowledge_track": "lore",
                "decision": "accept",
                "reviewer": directive.get("author", "author"),
                "rationale": f"Author directive applied ({directive_note}).",
                "timestamp_utc": now_utc_iso(),
                "source_snippet_ids": [],
                "source_priority": "author_directive",
                "patch_payload": {"directive": directive},
            }
        )

    for patch in lore_patches:
        claim_id = patch["claim_id"]
        decision = decision_by_claim.get(claim_id)
        if not decision:
            continue
        action = decision.get("decision", "defer")
        if action not in VALID_DECISIONS:
            action = "defer"
        if action == "accept":
            card_id = patch["card_id"]
            if card_id not in cards:
                cards[card_id] = {
                    "card_id": card_id,
                    "entity_type": "term",
                    "canonical_name": card_id,
                    "aliases": [],
                    "status": "draft",
                    "summary": "",
                    "details": {},
                    "timeline": [],
                    "relationships": [],
                    "source_evidence": [],
                    "confidence": {"score": patch.get("confidence", 0.5), "reviewer_note": "Merged from patch."},
                    "revision_history": [],
                }
            cards[card_id]["summary"] = (cards[card_id].get("summary", "") + " " + patch.get("proposed_summary_append", "")).strip()
            cards[card_id]["source_evidence"] = sorted(
                set(cards[card_id].get("source_evidence", []) + patch.get("source_snippet_ids", []))
            )
            cards[card_id]["revision_history"].append(
                {
                    "timestamp_utc": now_utc_iso(),
                    "action": "merge_patch_accept",
                    "actor": decision.get("reviewer", "reviewer"),
                    "decision": "accept",
                    "rationale": decision.get("rationale", ""),
                }
            )
        merge_log.append(
            {
                "decision_id": safe_uuid(),
                "claim_id": claim_id,
                "card_id": patch["card_id"],
                "knowledge_track": "lore",
                "decision": action,
                "reviewer": decision.get("reviewer", "reviewer"),
                "rationale": decision.get("rationale", ""),
                "timestamp_utc": now_utc_iso(),
                "source_snippet_ids": patch.get("source_snippet_ids", []),
                "source_priority": "discord_inference_draft",
                "patch_payload": patch,
            }
        )

    return list(cards.values()), merge_log


def run(
    in_seed_json: Path,
    in_lore_patches_json: Path,
    in_decisions_json: Path,
    in_author_directives_json: Path,
    out_cards_json: Path,
    out_merge_log_jsonl: Path,
) -> None:
    logger = get_logger(__name__)
    seed = read_json(in_seed_json)
    patches = read_json(in_lore_patches_json).get("patches", [])
    decisions = read_json(in_decisions_json).get("decisions", [])
    author_directives = []
    if in_author_directives_json.exists():
        author_directives = read_json(in_author_directives_json).get("directives", [])
    logger.info(
        "Stage G: merging seed_cards=%d patches=%d decisions=%d author_directives=%d",
        len(seed.get("cards", [])),
        len(patches),
        len(decisions),
        len(author_directives),
    )
    merged_cards, merge_log = apply_decisions(seed.get("cards", []), patches, decisions, author_directives)
    write_json(out_cards_json, {"cards": merged_cards})
    write_jsonl(out_merge_log_jsonl, merge_log)
    logger.info(
        "Stage G complete: merged_cards=%d merge_log_entries=%d",
        len(merged_cards),
        len(merge_log),
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--in-seed-json", type=Path, required=True)
    parser.add_argument("--in-lore-patches-json", type=Path, required=True)
    parser.add_argument("--in-decisions-json", type=Path, required=True)
    parser.add_argument("--in-author-directives-json", type=Path, required=False, default=Path("artifacts/07_review/author_directives.json"))
    parser.add_argument("--out-cards-json", type=Path, required=True)
    parser.add_argument("--out-merge-log-jsonl", type=Path, required=True)
    args = parser.parse_args()
    run(
        args.in_seed_json,
        args.in_lore_patches_json,
        args.in_decisions_json,
        args.in_author_directives_json,
        args.out_cards_json,
        args.out_merge_log_jsonl,
    )


if __name__ == "__main__":
    main()
