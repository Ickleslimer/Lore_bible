from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any

from pipeline.common import get_logger, now_utc_iso, read_json, write_json
from pipeline.entity_resolution import load_entity_records
from pipeline.review_memory import (
    load_review_memory,
    remember_claim_decisions,
    save_review_memory,
)
from pipeline.stage_11_card_synthesis import (
    _identity_merge_proposals_are_fresh,
    _load_decisions,
    _load_identity_merge_decisions,
    _load_identity_merge_proposals,
    annotate_identity_merge_proposals,
    apply_claim_decisions,
    default_author_claim_decisions,
    default_author_claims_path,
    detect_identity_merge_proposals,
    load_author_claims,
    remember_identity_merge_decisions,
)


def _load_claims(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    payload = read_json(path)
    rows = payload.get("claims", []) if isinstance(payload, dict) else []
    return [row for row in rows if isinstance(row, dict)]


def _load_config(path: Path | None) -> dict[str, Any]:
    if path is None or not path.exists():
        return {}
    payload = read_json(path)
    return payload if isinstance(payload, dict) else {}


def run(
    in_entities_json: Path,
    in_claim_drafts_json: Path,
    in_claim_decisions_json: Path,
    in_review_memory_json: Path,
    out_identity_merge_proposals_json: Path,
    in_identity_merge_decisions_json: Path,
    in_pipeline_config_json: Path | None = None,
) -> None:
    logger = get_logger(__name__)
    if not in_identity_merge_decisions_json.exists():
        write_json(in_identity_merge_decisions_json, {"decisions": []})

    entities = load_entity_records(in_entities_json)
    claims = _load_claims(in_claim_drafts_json)
    claim_decisions = _load_decisions(in_claim_decisions_json)
    author_claims_path = default_author_claims_path(in_claim_decisions_json)
    author_claims, author_claim_failures = load_author_claims(author_claims_path, entities)
    write_json(
        out_identity_merge_proposals_json.with_name("author_claim_failures.json"),
        {"generated_at_utc": now_utc_iso(), "failures": author_claim_failures},
    )
    if author_claim_failures:
        raise RuntimeError(
            f"Stage 10 found {len(author_claim_failures)} author claim(s) requiring review because their target "
            f"entities could not be resolved; fix {author_claims_path} and rerun Stage 10."
        )

    author_claim_decisions = default_author_claim_decisions(author_claims, claim_decisions)
    all_claims = claims + author_claims
    all_claim_decisions = claim_decisions + author_claim_decisions
    accepted_claims, _merge_log = apply_claim_decisions(all_claims, all_claim_decisions)

    config = _load_config(in_pipeline_config_json)
    memory = load_review_memory(in_review_memory_json)
    remember_claim_decisions(memory, all_claims, all_claim_decisions)

    identity_merge_decisions = _load_identity_merge_decisions(in_identity_merge_decisions_json)
    identity_merge_inputs = [
        in_entities_json,
        in_claim_drafts_json,
        in_claim_decisions_json,
        author_claims_path,
    ]
    existing_identity_merge_proposals = (
        _load_identity_merge_proposals(out_identity_merge_proposals_json)
        if _identity_merge_proposals_are_fresh(out_identity_merge_proposals_json, identity_merge_inputs)
        else None
    )
    if existing_identity_merge_proposals is not None:
        identity_merge_proposals = existing_identity_merge_proposals
        logger.info(
            "Stage 10 Identity Merge: reusing %d existing proposal(s); no newer claim/entity inputs detected.",
            len(identity_merge_proposals),
        )
    else:
        identity_merge_proposals = detect_identity_merge_proposals(accepted_claims, entities, config)

    remember_identity_merge_decisions(memory, identity_merge_proposals, identity_merge_decisions)
    identity_merge_proposals = annotate_identity_merge_proposals(identity_merge_proposals, identity_merge_decisions)
    write_json(
        out_identity_merge_proposals_json,
        {
            "generated_at_utc": now_utc_iso(),
            "proposals": identity_merge_proposals,
            "decisions_path": str(in_identity_merge_decisions_json),
        },
    )

    pending_identity_merges = [
        proposal for proposal in identity_merge_proposals if str(proposal.get("review_status", "pending")) == "pending"
    ]
    save_review_memory(in_review_memory_json, memory)
    logger.info(
        "Stage 10 complete: accepted_claims=%d identity_merge_proposals=%d pending_identity_merges=%d",
        len(accepted_claims),
        len(identity_merge_proposals),
        len(pending_identity_merges),
    )
    if pending_identity_merges:
        raise RuntimeError(
            f"Stage 10 found {len(pending_identity_merges)} identity cluster proposal(s) requiring review; "
            f"review {out_identity_merge_proposals_json} and save decisions to {in_identity_merge_decisions_json}, then rerun Stage 10."
        )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--in-entities-json", type=Path, required=True)
    parser.add_argument("--in-claim-drafts-json", type=Path, required=True)
    parser.add_argument("--in-claim-decisions-json", type=Path, required=True)
    parser.add_argument("--in-review-memory-json", type=Path, required=True)
    parser.add_argument("--out-identity-merge-proposals-json", type=Path, required=True)
    parser.add_argument("--in-identity-merge-decisions-json", type=Path, required=True)
    parser.add_argument("--in-pipeline-config-json", type=Path, required=False, default=None)
    args = parser.parse_args()
    run(
        args.in_entities_json,
        args.in_claim_drafts_json,
        args.in_claim_decisions_json,
        args.in_review_memory_json,
        args.out_identity_merge_proposals_json,
        args.in_identity_merge_decisions_json,
        args.in_pipeline_config_json,
    )


if __name__ == "__main__":
    main()
