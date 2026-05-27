"""Probe NIM latency for a real Stage 07 ledger prompt (timing + timeout diagnosis)."""
from __future__ import annotations

import json
import time
from pathlib import Path

from pipeline.artifact_paths import ArtifactPaths
from pipeline.common import read_json, read_jsonl
from pipeline.model_provider import call_model_chat, get_model_runtime_status, model_call_kwargs
from pipeline.stage_05_lore_development_ledger import (
    build_entity_registry,
    build_ledger_prompt,
    build_snippet_index,
    ledger_config,
    load_existing_outputs,
    merge_segment_streams,
    ordered_segments,
    prior_entity_context,
)


def main() -> None:
    repo = Path(__file__).resolve().parents[1]
    run_root = repo / "artifacts" / "runs" / "20260517_032555635445_full"
    paths = ArtifactPaths(run_root)
    provider_config = read_json(repo / "config/pipeline_config.json")
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

    entries, _, completed = load_existing_outputs(
        paths.lore_development_ledger_index,
        paths.lore_development_ledger_jsonl,
        paths.lore_development_ledger_failures,
    )
    resolved_payload = read_json(paths.resolved_entities)
    alias_payload = read_json(paths.alias_map)
    entity_registry, by_name = build_entity_registry(resolved_payload, alias_payload, None)

    target_id = "theme_rescue_conversation_eaa6b570fef71e14"
    segment = next(
        (s for s in segments if str(s.get("conversation_id", "")).strip() == target_id),
        segments[264] if len(segments) > 264 else segments[0],
    )
    segment_id = str(segment.get("conversation_id", "")).strip()
    rows = rows_by_conversation.get(segment_id, [])
    global_sequence = 265
    prior_context = prior_entity_context(entries, per_entity_limit=4)
    prompt = build_ledger_prompt(
        segment=segment,
        rows=rows,
        global_sequence=global_sequence,
        entity_registry=entity_registry,
        prior_context=prior_context,
        cfg=cfg,
    )
    print("segment_id", segment_id)
    print("message_rows", len(rows))
    print("prompt_chars", len(prompt))

    kwargs = model_call_kwargs(provider_config, "stage_05_lore_development_ledger")
    print("model", kwargs.get("api_model"), "base", kwargs.get("api_base_url"))
    print("configured_timeout_s", kwargs.get("timeout_seconds"))
    print("api_retries", kwargs.get("api_retries"))

    trial_kwargs = dict(kwargs)
    trial_kwargs["api_retries"] = 0
    timeout_s = int(trial_kwargs.get("timeout_seconds", 600))
    print(f"\n--- trial timeout={timeout_s}s api_retries=0 ---")
    started = time.time()
    result = call_model_chat(prompt=prompt, **trial_kwargs)
    elapsed = time.time() - started
    status = get_model_runtime_status()
    print("elapsed_s", round(elapsed, 1))
    print("skip_reason", status.get("last_model_skip_reason"))
    if result:
        if isinstance(result, dict):
            keys = sorted(result.keys())
            print("response_keys", keys[:10])
            if "entries" in result:
                print("entry_count", len(result.get("entries", [])))
        else:
            print("response_type", type(result).__name__)
        print("SUCCESS")
        return
    print("FAILED")
    raise SystemExit(1)


if __name__ == "__main__":
    main()
