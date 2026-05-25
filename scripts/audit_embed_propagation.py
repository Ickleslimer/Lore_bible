#!/usr/bin/env python3
"""Audit embed text propagation through pipeline stages."""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from pipeline.common import CUTOFF_UTC, parse_discord_timestamp
from scripts.patch_embeds_into_stage02 import extract_embed_text

EMBED_SEP = " — "
YT_RE = re.compile(r"youtube|youtu\.be", re.I)


def load_jsonl(path: Path) -> list[dict]:
    if not path.exists():
        return []
    rows: list[dict] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.strip():
            rows.append(json.loads(line))
    return rows


def message_text(row: dict) -> str:
    return str(row.get("content_raw") or row.get("content_normalized") or row.get("raw_text") or row.get("display_text_normalized") or "")


def main() -> None:
    run = ROOT / "artifacts" / "runs" / "20260517_032555635445_full"
    conv = ROOT / "discord_conversations"
    stage02 = run / "02_message_normalization" / "messages_normalized_per_thread.jsonl"
    stage03 = run / "03_timeline_merge" / "messages_global_timeline.jsonl"
    stage04 = run / "04_conversation_segmentation" / "messages_relevant_conversations.jsonl"
    stage05 = run / "05_snippet_extraction" / "snippets_candidates.jsonl"

    # Raw post-cutoff embed stats
    raw_embed = raw_yt = 0
    for fpath in conv.rglob("*.json"):
        try:
            data = json.loads(fpath.read_text(encoding="utf-8"))
        except Exception:
            continue
        if not isinstance(data, list):
            continue
        for msg in data:
            if not isinstance(msg, dict):
                continue
            ts = msg.get("timestamp")
            if not ts:
                continue
            try:
                if parse_discord_timestamp(str(ts)) < CUTOFF_UTC:
                    continue
            except Exception:
                continue
            embeds = msg.get("embeds")
            if isinstance(embeds, list) and embeds:
                raw_embed += 1
            if YT_RE.search(str(msg.get("content") or "")):
                raw_yt += 1
            elif isinstance(embeds, list):
                for emb in embeds:
                    if isinstance(emb, dict) and YT_RE.search(str(emb.get("url") or "")):
                        raw_yt += 1
                        break

    rows02 = load_jsonl(stage02)
    rows03 = load_jsonl(stage03)
    rows04 = load_jsonl(stage04)
    rows05 = load_jsonl(stage05)

    def count(rows: list[dict], pred) -> int:
        return sum(1 for row in rows if pred(message_text(row), row))

    stats = {
        "raw_post_cutoff_youtube": raw_yt,
        "raw_post_cutoff_embed_array": raw_embed,
        "stage02_total": len(rows02),
        "stage02_embeds_count_gt0": count(rows02, lambda t, r: int(r.get("embeds_count") or 0) > 0),
        "stage02_embed_dash": count(rows02, lambda t, r: EMBED_SEP in t),
        "stage02_embed_patched_flag": sum(1 for r in rows02 if r.get("_embed_text_appended")),
        "stage03_embed_dash": count(rows03, lambda t, r: EMBED_SEP in t),
        "stage03_youtube": count(rows03, lambda t, r: bool(YT_RE.search(t))),
        "stage04_embed_dash": count(rows04, lambda t, r: EMBED_SEP in t),
        "stage04_youtube": count(rows04, lambda t, r: bool(YT_RE.search(t))),
        "stage05_embed_dash": count(rows05, lambda t, r: EMBED_SEP in t),
        "stage05_youtube": count(rows05, lambda t, r: bool(YT_RE.search(t))),
    }

    # Patch script outcome simulation
    raw_embed_by_id: dict[str, str] = {}
    for fpath in conv.rglob("*.json"):
        try:
            data = json.loads(fpath.read_text(encoding="utf-8"))
        except Exception:
            continue
        if not isinstance(data, list):
            continue
        for msg in data:
            if not isinstance(msg, dict):
                continue
            mid = str(msg.get("id", "")).strip()
            et = extract_embed_text(msg)
            if mid and et:
                raw_embed_by_id[mid] = et

    patched = unchanged = need_patch = 0
    for row in rows02:
        mid = str(row.get("message_id", "")).strip()
        et = raw_embed_by_id.get(mid, "")
        if not et:
            continue
        raw = str(row.get("content_raw", ""))
        norm = str(row.get("content_normalized", ""))
        if et.lower() in raw.lower() and et.lower() in norm.lower():
            unchanged += 1
        else:
            need_patch += 1
            if patched < 5:
                patched += 1
                print(f"NEEDS PATCH {mid}:")
                print(f"  embed_text: {et[:120]}")
                print(f"  content_raw: {raw[:120]}")

    stats["patch_unchanged"] = unchanged
    stats["patch_need"] = need_patch

    # Trace stage03 embed-dash messages into stage04
    s04_by_id = {str(r.get("message_id")): r for r in rows04}
    in04 = dash_kept = 0
    for row in rows03:
        raw = message_text(row)
        if EMBED_SEP not in raw:
            continue
        mid = str(row.get("message_id"))
        s4 = s04_by_id.get(mid)
        if not s4:
            continue
        in04 += 1
        s4text = message_text(s4)
        if EMBED_SEP in s4text:
            dash_kept += 1

    stats["stage03_embed_dash_in_stage04"] = in04
    stats["stage03_embed_dash_preserved_in_stage04"] = dash_kept

    # Quest song title hits in snippets
    seed = json.loads((ROOT / "config" / "quest_song_seed.json").read_text(encoding="utf-8"))
    titles = [str(s.get("quest_title", "")).lower() for s in seed.get("quest_song_seeds", []) if s.get("quest_title")]
    quest_hits = 0
    for row in rows05:
        text = message_text(row).lower()
        if any(title in text for title in titles):
            quest_hits += 1
    stats["stage05_quest_song_title_hits"] = quest_hits

    print("\n=== EMBED PROPAGATION AUDIT ===")
    for key, value in stats.items():
        print(f"{key}: {value}")


if __name__ == "__main__":
    main()
