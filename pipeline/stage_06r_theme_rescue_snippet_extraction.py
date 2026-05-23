from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any

from pipeline.common import get_logger, now_utc_iso, read_jsonl, write_json, write_jsonl
from pipeline.stage_06_snippet_extraction import run as run_stage_06


def _dedupe_rows(rows: list[dict[str, Any]], key_field: str) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    seen: set[str] = set()
    for row in rows:
        if not isinstance(row, dict):
            continue
        key = str(row.get(key_field) or "")
        if key and key in seen:
            continue
        if key:
            seen.add(key)
        out.append(row)
    return out


def _tag_rescue_snippets(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    tagged: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        tagged.append(
            {
                **row,
                "rescue_source": "stage_04r_theme_relevance_rerun",
                "conversation_rescue_source": row.get("conversation_rescue_source", "stage_04r_theme_relevance_rerun"),
            }
        )
    return tagged


def run(
    in_rescued_messages_jsonl: Path,
    in_profiles_json: Path,
    in_strict_snippets_jsonl: Path,
    in_strict_needs_review_jsonl: Path,
    out_rescue_snippets_jsonl: Path,
    out_rescue_needs_review_jsonl: Path,
    out_rescue_profiles_json: Path,
    out_combined_snippets_jsonl: Path,
    out_combined_needs_review_jsonl: Path,
    out_merge_report_json: Path,
    in_pipeline_config_json: Path | None = None,
    in_seed_json: Path | None = None,
    thematic_runtime_path: Path | None = None,
) -> None:
    logger = get_logger(__name__)
    rescued_messages = read_jsonl(in_rescued_messages_jsonl)
    strict_snippets = read_jsonl(in_strict_snippets_jsonl)
    strict_review = read_jsonl(in_strict_needs_review_jsonl)
    if rescued_messages:
        run_stage_06(
            in_rescued_messages_jsonl,
            in_profiles_json,
            out_rescue_snippets_jsonl,
            out_rescue_needs_review_jsonl,
            out_rescue_profiles_json,
            in_pipeline_config_json,
            in_seed_json,
            thematic_runtime_path,
            None,
        )
        rescue_snippets = _tag_rescue_snippets(read_jsonl(out_rescue_snippets_jsonl))
        rescue_review = _tag_rescue_snippets(read_jsonl(out_rescue_needs_review_jsonl))
        write_jsonl(out_rescue_snippets_jsonl, rescue_snippets)
        write_jsonl(out_rescue_needs_review_jsonl, rescue_review)
    else:
        rescue_snippets = []
        rescue_review = []
        write_jsonl(out_rescue_snippets_jsonl, [])
        write_jsonl(out_rescue_needs_review_jsonl, [])
        write_json(out_rescue_profiles_json, {"profiles": []})

    combined_snippets = _dedupe_rows([*strict_snippets, *rescue_snippets], "snippet_id")
    combined_review = _dedupe_rows([*strict_review, *rescue_review], "snippet_id")
    write_jsonl(out_combined_snippets_jsonl, combined_snippets)
    write_jsonl(out_combined_needs_review_jsonl, combined_review)
    report = {
        "generated_at_utc": now_utc_iso(),
        "stage": "06R_theme_rescue_snippet_extraction",
        "inputs": {
            "rescued_messages_jsonl": str(in_rescued_messages_jsonl),
            "strict_snippets_jsonl": str(in_strict_snippets_jsonl),
            "strict_needs_review_jsonl": str(in_strict_needs_review_jsonl),
        },
        "outputs": {
            "rescue_snippets_jsonl": str(out_rescue_snippets_jsonl),
            "rescue_needs_review_jsonl": str(out_rescue_needs_review_jsonl),
            "combined_snippets_jsonl": str(out_combined_snippets_jsonl),
            "combined_needs_review_jsonl": str(out_combined_needs_review_jsonl),
        },
        "summary": {
            "rescued_message_count": len(rescued_messages),
            "strict_snippet_count": len(strict_snippets),
            "rescue_snippet_count": len(rescue_snippets),
            "combined_snippet_count": len(combined_snippets),
            "strict_needs_review_count": len(strict_review),
            "rescue_needs_review_count": len(rescue_review),
            "combined_needs_review_count": len(combined_review),
        },
    }
    write_json(out_merge_report_json, report)
    logger.info(
        "Stage 06R complete: rescued_messages=%d rescue_snippets=%d combined_snippets=%d",
        len(rescued_messages),
        len(rescue_snippets),
        len(combined_snippets),
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--in-rescued-messages-jsonl", type=Path, required=True)
    parser.add_argument("--in-profiles-json", type=Path, required=True)
    parser.add_argument("--in-strict-snippets-jsonl", type=Path, required=True)
    parser.add_argument("--in-strict-needs-review-jsonl", type=Path, required=True)
    parser.add_argument("--out-rescue-snippets-jsonl", type=Path, required=True)
    parser.add_argument("--out-rescue-needs-review-jsonl", type=Path, required=True)
    parser.add_argument("--out-rescue-profiles-json", type=Path, required=True)
    parser.add_argument("--out-combined-snippets-jsonl", type=Path, required=True)
    parser.add_argument("--out-combined-needs-review-jsonl", type=Path, required=True)
    parser.add_argument("--out-merge-report-json", type=Path, required=True)
    parser.add_argument("--in-pipeline-config-json", type=Path, required=False, default=None)
    parser.add_argument("--in-seed-json", type=Path, required=False, default=None)
    parser.add_argument("--thematic-runtime-path", type=Path, required=False, default=None)
    args = parser.parse_args()
    run(
        args.in_rescued_messages_jsonl,
        args.in_profiles_json,
        args.in_strict_snippets_jsonl,
        args.in_strict_needs_review_jsonl,
        args.out_rescue_snippets_jsonl,
        args.out_rescue_needs_review_jsonl,
        args.out_rescue_profiles_json,
        args.out_combined_snippets_jsonl,
        args.out_combined_needs_review_jsonl,
        args.out_merge_report_json,
        args.in_pipeline_config_json,
        args.in_seed_json,
        args.thematic_runtime_path,
    )


if __name__ == "__main__":
    main()
