"""Stage 08W: tag snippets with narrative_work_id (model batch + heuristics)."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from pipeline.common import get_logger, now_utc_iso, read_json, read_jsonl, write_json, write_jsonl
from pipeline.model_provider import call_model_chat, model_call_kwargs
from pipeline.narrative_works import (
    heuristic_narrative_work_tag,
    load_narrative_work_tag_overrides,
    load_narrative_works,
    narrative_works_config,
)


def _snippet_text(snippet: dict[str, Any]) -> str:
    return " ".join(
        [
            str(snippet.get("display_text_normalized", "")),
            str(snippet.get("conversation_patch_summary", "")),
            " ".join(str(item) for item in snippet.get("conversation_patch_lore_developments", []) or []),
        ]
    ).strip()


def _select_snippets_for_tagging(snippets: list[dict[str, Any]], cfg: dict[str, Any]) -> list[dict[str, Any]]:
    max_snippets = int(cfg.get("max_snippets", 400) or 400)
    meta_first = [row for row in snippets if str(row.get("knowledge_track", "")).strip().lower() == "meta"]
    lore = [row for row in snippets if row not in meta_first]
    ordered = meta_first + lore
    return ordered[:max_snippets]


def _build_batch_prompt(batch: list[dict[str, Any]], works: list[dict[str, Any]]) -> str:
    work_lines = []
    for work in works:
        work_lines.append(
            {
                "work_id": work.get("work_id"),
                "title": work.get("title"),
                "kind": work.get("kind"),
                "synopsis_seed": work.get("synopsis_seed", ""),
                "keyword_hints": work.get("keyword_hints", []),
            }
        )
    items = [
        {
            "snippet_id": row.get("snippet_id"),
            "knowledge_track": row.get("knowledge_track", "lore"),
            "text": _snippet_text(row)[:1200],
        }
        for row in batch
    ]
    return f"""Classify each snippet with exactly one narrative_work_id from the registry.
Return strict JSON only: {{"tags": [{{"snippet_id": "", "narrative_work_id": "", "confidence": 0.0, "rationale": ""}}]}}

Rules:
- theriac_coda: main game / Path B / lab siding / primary RPG scope.
- theriac_coda_path_a: destructive side route only (~6h, against the lab, execute orders).
- wedding_ramasinta, vengeful_theriac, dead_prophets_fallen_angels: only when text clearly references that spin-off or alt timeline.
- Prefer theriac_coda when ambiguous lore snippets lack explicit Path A or spin-off markers.

Registry:
{json.dumps(work_lines, ensure_ascii=False, indent=2)}

Snippets:
{json.dumps(items, ensure_ascii=False, indent=2)}
"""


def _model_tag_batch(batch: list[dict[str, Any]], works: list[dict[str, Any]], config: dict[str, Any]) -> dict[str, dict[str, Any]]:
    prompt = _build_batch_prompt(batch, works)
    response = call_model_chat(prompt=prompt, **model_call_kwargs(config, "stage_08w_narrative_work_tagging"))
    if not isinstance(response, dict):
        return {}
    out: dict[str, dict[str, Any]] = {}
    for row in response.get("tags", []) or []:
        if not isinstance(row, dict):
            continue
        snippet_id = str(row.get("snippet_id", "")).strip()
        work_id = str(row.get("narrative_work_id", "")).strip()
        if snippet_id and work_id:
            out[snippet_id] = row
    return out


def run(
    in_snippets_jsonl: Path,
    out_tags_jsonl: Path,
    in_pipeline_config_json: Path | None = None,
    in_review_memory_json: Path | None = None,
    *,
    in_narrative_works_json: Path | None = None,
) -> None:
    logger = get_logger(__name__)
    config = read_json(in_pipeline_config_json) if in_pipeline_config_json and in_pipeline_config_json.exists() else {}
    nw_cfg = narrative_works_config(config)
    works = load_narrative_works(in_narrative_works_json)
    snippets = read_jsonl(in_snippets_jsonl)
    selected = _select_snippets_for_tagging(snippets, nw_cfg)
    overrides = load_narrative_work_tag_overrides(in_review_memory_json)

    batch_size = max(1, int(nw_cfg.get("batch_size", 12) or 12))
    model_enabled = bool(nw_cfg.get("model_batch_enabled", True))
    model_tags: dict[str, dict[str, Any]] = {}

    if model_enabled:
        for start in range(0, len(selected), batch_size):
            batch = selected[start : start + batch_size]
            model_tags.update(_model_tag_batch(batch, works, config))

    rows: list[dict[str, Any]] = []
    for snippet in selected:
        snippet_id = str(snippet.get("snippet_id", "")).strip()
        if not snippet_id:
            continue
        if snippet_id in overrides:
            work_id = overrides[snippet_id]
            source = "review_memory_override"
            confidence = 1.0
            rationale = "review_memory narrative_work_tag_overrides"
        elif snippet_id in model_tags:
            tagged = model_tags[snippet_id]
            work_id = str(tagged.get("narrative_work_id", "")).strip() or heuristic_narrative_work_tag(snippet, works)
            source = "model_batch"
            confidence = float(tagged.get("confidence", 0.75) or 0.75)
            rationale = str(tagged.get("rationale", "")).strip()
        else:
            work_id = heuristic_narrative_work_tag(snippet, works)
            source = "heuristic"
            confidence = 0.55
            rationale = "keyword_hints fallback"
        rows.append(
            {
                "snippet_id": snippet_id,
                "narrative_work_id": work_id,
                "confidence": confidence,
                "source": source,
                "rationale": rationale,
                "tagged_at_utc": now_utc_iso(),
            }
        )

    out_tags_jsonl.parent.mkdir(parents=True, exist_ok=True)
    write_jsonl(out_tags_jsonl, rows)
    summary_path = out_tags_jsonl.parent / "tagging_summary.json"
    counts: dict[str, int] = {}
    for row in rows:
        key = str(row.get("narrative_work_id", "unknown"))
        counts[key] = counts.get(key, 0) + 1
    write_json(
        summary_path,
        {
            "generated_at_utc": now_utc_iso(),
            "snippet_count": len(rows),
            "counts_by_work": counts,
            "model_batch_enabled": model_enabled,
            "override_count": len(overrides),
        },
    )
    logger.info("Stage 08W complete: tagged=%d works=%d", len(rows), len(works))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--in-snippets-jsonl", type=Path, required=True)
    parser.add_argument("--out-tags-jsonl", type=Path, required=True)
    parser.add_argument("--in-pipeline-config-json", type=Path, default=Path("config/pipeline_config.json"))
    parser.add_argument("--in-review-memory-json", type=Path, default=Path("canon/review_memory.json"))
    parser.add_argument("--in-narrative-works-json", type=Path, default=Path("canon/narrative_works.json"))
    args = parser.parse_args()
    run(
        args.in_snippets_jsonl,
        args.out_tags_jsonl,
        args.in_pipeline_config_json,
        args.in_review_memory_json,
        in_narrative_works_json=args.in_narrative_works_json,
    )


if __name__ == "__main__":
    main()
