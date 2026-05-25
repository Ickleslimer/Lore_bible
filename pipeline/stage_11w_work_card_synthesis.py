"""Stage 11W: synthesize narrative work hub cards (phase 1: active works only)."""

from __future__ import annotations

import argparse
from pathlib import Path

from pipeline.common import get_logger, now_utc_iso, read_json, write_json
from pipeline.narrative_works import load_snippet_narrative_work_tags, snippet_tag_path, work_cards_path
from pipeline.stage_11_card_synthesis import load_source_snippets_by_id
from pipeline.work_card_synthesis import synthesize_active_work_cards


def run(
    in_snippets_jsonl: Path,
    in_narrative_work_tags_jsonl: Path,
    out_work_cards_json: Path,
    in_pipeline_config_json: Path | None = None,
) -> None:
    logger = get_logger(__name__)
    config = read_json(in_pipeline_config_json) if in_pipeline_config_json and in_pipeline_config_json.exists() else {}
    snippets_by_id = load_source_snippets_by_id(in_snippets_jsonl)
    tags = load_snippet_narrative_work_tags(in_narrative_work_tags_jsonl)
    cards = synthesize_active_work_cards(snippets_by_id, tags, config)
    write_json(
        out_work_cards_json,
        {
            "generated_at_utc": now_utc_iso(),
            "works": cards,
        },
    )
    logger.info("Stage 11W complete: work_cards=%d", len(cards))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--in-snippets-jsonl", type=Path, required=True)
    parser.add_argument("--in-narrative-work-tags-jsonl", type=Path, required=True)
    parser.add_argument("--out-work-cards-json", type=Path, required=True)
    parser.add_argument("--in-pipeline-config-json", type=Path, default=Path("config/pipeline_config.json"))
    args = parser.parse_args()
    run(
        args.in_snippets_jsonl,
        args.in_narrative_work_tags_jsonl,
        args.out_work_cards_json,
        args.in_pipeline_config_json,
    )


if __name__ == "__main__":
    main()
