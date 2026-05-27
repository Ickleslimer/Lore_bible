"""Run one Stage 07 ledger extraction with opportunistic multi-provider routing."""
from __future__ import annotations

import argparse
import json
import time
from pathlib import Path

from pipeline.artifact_paths import ArtifactPaths
from pipeline.common import read_json, read_jsonl
from pipeline.model_provider import get_model_runtime_status
from pipeline.stage_05_lore_development_ledger import (
    build_entity_registry,
    build_snippet_index,
    extract_ledger_entries_with_model,
    ledger_config,
    load_existing_outputs,
    merge_segment_streams,
    ordered_segments,
)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--run-root",
        type=Path,
        default=Path("artifacts/runs/20260517_032555635445_full"),
    )
    parser.add_argument(
        "--segment-id",
        default="conversation_891d6619530c67a3",
        help="Conversation id for segment to test (default: smaller strict_accept segment).",
    )
    args = parser.parse_args()

    repo = Path(__file__).resolve().parents[1]
    paths = ArtifactPaths(args.run_root.resolve())
    provider_config = read_json(repo / "config" / "pipeline_config.json")
    cfg = ledger_config(provider_config)

    relevant_rows = read_jsonl(paths.relevant_messages)
    rescue_rows = (
        read_jsonl(paths.theme_rescue_messages) if paths.theme_rescue_messages.exists() else []
    )
    rows_by_conversation: dict[str, list] = {}
    for row in relevant_rows + rescue_rows:
        cid = str(row.get("conversation_id", "")).strip()
        if cid:
            rows_by_conversation.setdefault(cid, []).append(row)

    strict_segments = ordered_segments(read_json(paths.conversation_segments))
    rescue_segments = (
        ordered_segments(read_json(paths.theme_rescue_segments))
        if paths.theme_rescue_segments.exists()
        else []
    )
    segments = merge_segment_streams(strict_segments, rescue_segments)
    segment = next(
        (s for s in segments if str(s.get("conversation_id", "")).strip() == args.segment_id.strip()),
        None,
    )
    if segment is None:
        raise SystemExit(f"Segment not found: {args.segment_id}")

    entries, _, completed = load_existing_outputs(
        paths.lore_development_ledger_index,
        paths.lore_development_ledger_jsonl,
        paths.lore_development_ledger_failures,
    )
    resolved_payload = read_json(paths.resolved_entities)
    alias_payload = read_json(paths.alias_map)
    effective_snippets = paths.effective_snippets()
    snippets = read_jsonl(effective_snippets) if effective_snippets.exists() else []
    entity_registry, by_name = build_entity_registry(resolved_payload, alias_payload, None)
    snippet_by_message = build_snippet_index(snippets)

    segment_id = str(segment.get("conversation_id", "")).strip()
    rows = rows_by_conversation.get(segment_id, [])
    global_sequence = next(
        (idx + 1 for idx, s in enumerate(segments) if str(s.get("conversation_id", "")).strip() == segment_id),
        0,
    )
    print("segment_id", segment_id)
    print("scope", segment.get("source_scope"))
    print("messages", len(rows))
    print("opportunistic", cfg.get("opportunistic_routing"))
    print("already_completed", segment_id in completed)

    started = time.time()
    new_entries, validation_rejected, _review = extract_ledger_entries_with_model(
        segment=segment,
        rows=rows,
        global_sequence=global_sequence,
        total_segments=len(segments),
        prior_entries=entries,
        entity_registry=entity_registry,
        by_name=by_name,
        snippet_by_message=snippet_by_message,
        provider_config=provider_config,
        cfg=cfg,
    )
    elapsed = time.time() - started
    status = get_model_runtime_status()
    print("elapsed_s", round(elapsed, 1))
    print("entries_emitted", len(new_entries))
    print("validation_rejected", len(validation_rejected))
    print("runtime_status", json.dumps(status, indent=2))
    if new_entries:
        print("sample_headline", new_entries[0].get("headline"))
    else:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
