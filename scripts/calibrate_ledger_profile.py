"""Run N Stage 07 segments pinned to one profile for calibration."""
from __future__ import annotations

import argparse
import json
import time
from pathlib import Path

from pipeline.artifact_paths import ArtifactPaths
from pipeline.common import read_json, read_jsonl
from pipeline.ledger_quality_metrics import ledger_entry_metrics
from pipeline.model_provider import model_call_kwargs
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
    parser.add_argument("--profile", required=True, help="model_routing profile name to pin.")
    parser.add_argument("--segments", type=int, default=20, help="Max segments to process.")
    args = parser.parse_args()

    repo = Path(__file__).resolve().parents[1]
    paths = ArtifactPaths(args.run_root.resolve())
    provider_config = read_json(repo / "config" / "pipeline_config.json")
    cfg = ledger_config(provider_config)
    cfg["opportunistic_routing"] = {
        "enabled": True,
        "free_only": True,
        "tiers": [{"name": "calibration", "profiles": [args.profile]}],
    }

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

    kwargs = model_call_kwargs(provider_config, "stage_05_lore_development_ledger", profile_override=args.profile)
    print("profile", args.profile)
    print("provider", kwargs.get("provider"), "model", kwargs.get("api_model"))

    batch_entries: list[dict] = []
    processed = 0
    started = time.time()
    for global_sequence, segment in enumerate(segments, start=1):
        segment_id = str(segment.get("conversation_id", "")).strip()
        if not segment_id or segment_id in completed:
            continue
        rows = rows_by_conversation.get(segment_id, [])
        if not rows:
            continue
        try:
            new_entries, rejected, _review = extract_ledger_entries_with_model(
                segment=segment,
                rows=rows,
                global_sequence=global_sequence,
                total_segments=len(segments),
                prior_entries=entries + batch_entries,
                entity_registry=entity_registry,
                by_name=by_name,
                snippet_by_message=snippet_by_message,
                provider_config=provider_config,
                cfg=cfg,
            )
        except Exception as exc:
            print("segment_failed", segment_id, exc)
            continue
        batch_entries.extend(new_entries)
        processed += 1
        print(
            f"segment {processed}/{args.segments} id={segment_id} entries={len(new_entries)} rejected={len(rejected)}"
        )
        if processed >= args.segments:
            break

    elapsed = time.time() - started
    metrics = ledger_entry_metrics(batch_entries)
    print("\n=== Calibration metrics ===")
    print(json.dumps(metrics, indent=2))
    print("elapsed_s", round(elapsed, 1))
    print("\n--- Sample headlines ---")
    for entry in batch_entries[:5]:
        print(f"  [{entry.get('inference_profile')}] {entry.get('headline', '')[:100]}")


if __name__ == "__main__":
    main()
