#!/usr/bin/env python3
"""One-off script: patch embed titles/descriptions into existing Stage 02 output.

The latest Stage 02 normalization now extracts embed text (song titles from
YouTube links, etc.), but existing runs were processed before that change.

This script reads the raw Discord JSON files, re-extracts embed text using
the same logic as the updated Stage 02, and patches the normalized JSONL
so that downstream stages (04R, 07C, 07D) can detect quest-song markers.

Usage:
    python scripts/patch_embeds_into_stage02.py \\
        --conversations-root discord_conversations \\
        --normalized-jsonl artifacts/runs/<run_id>/02_message_normalization/messages_normalized_per_thread.jsonl \\
        [--dry-run]

This modifies the normalized JSONL in-place unless --dry-run is given.
"""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Any


def extract_embed_text(msg: dict[str, Any]) -> str:
    """Identical logic to stage_02_message_normalization._extract_embed_text."""
    embeds = msg.get("embeds")
    if not isinstance(embeds, list) or not embeds:
        return ""
    parts: list[str] = []
    for embed in embeds:
        if not isinstance(embed, dict):
            continue
        title = str(embed.get("title") or "").strip()
        if title:
            parts.append(title)
        description = str(embed.get("description") or "").strip()
        if description:
            parts.append(description)
        author_name = ""
        author = embed.get("author")
        if isinstance(author, dict):
            author_name = str(author.get("name") or "").strip()
        if author_name:
            parts.append(author_name)
        provider_name = ""
        provider = embed.get("provider")
        if isinstance(provider, dict):
            provider_name = str(provider.get("name") or "").strip()
        if provider_name:
            parts.append(provider_name)
    return " — ".join(parts)


def main() -> None:
    parser = argparse.ArgumentParser(description="Patch embed text into existing Stage 02 normalized output.")
    parser.add_argument("--conversations-root", type=Path, required=True, help="Root of raw Discord JSON files.")
    parser.add_argument("--normalized-jsonl", type=Path, required=True, help="Existing Stage 02 normalized messages JSONL.")
    parser.add_argument("--dry-run", action="store_true", help="Show changes without writing.")
    args = parser.parse_args()

    if not args.conversations_root.exists():
        print(f"ERROR: conversations root does not exist: {args.conversations_root}")
        return 1
    if not args.normalized_jsonl.exists():
        print(f"ERROR: normalized JSONL does not exist: {args.normalized_jsonl}")
        return 1

    # Build a lookup: message_id -> embed_text from raw Discord JSON
    print(f"Scanning raw conversations in {args.conversations_root} ...")
    raw_embed_by_id: dict[str, str] = {}
    raw_files = sorted(args.conversations_root.rglob("*.json"))
    for fpath in raw_files:
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
            if not mid:
                continue
            embed_text = extract_embed_text(msg)
            if embed_text:
                raw_embed_by_id[mid] = embed_text

    print(f"Found {len(raw_embed_by_id)} message(s) with embed text across {len(raw_files)} raw file(s).")

    # Read and patch the normalized JSONL
    lines: list[dict[str, Any]] = []
    patched = 0
    unchanged = 0
    missing_raw = 0

    with open(args.normalized_jsonl, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            row = json.loads(line)
            lines.append(row)

    print(f"Loaded {len(lines)} normalized messages from Stage 02 output.")

    updated_lines: list[dict[str, Any]] = []
    for row in lines:
        mid = str(row.get("message_id", "")).strip()
        if not mid:
            updated_lines.append(row)
            continue

        embed_text = raw_embed_by_id.get(mid, "")
        if not embed_text:
            # No raw embed data for this message
            missing_raw += 1
            updated_lines.append(row)
            continue

        existing_raw = str(row.get("content_raw", "")).strip()
        existing_norm = str(row.get("content_normalized", "")).strip()

        # If embed text is already present in content, skip
        if embed_text.lower() in existing_raw.lower() and embed_text.lower() in existing_norm.lower():
            unchanged += 1
            updated_lines.append(row)
            continue

        # Prepend embed text if content is empty, otherwise append with separator
        new_raw = embed_text if not existing_raw else existing_raw + " " + embed_text
        new_norm = embed_text if not existing_norm else existing_norm + " " + embed_text

        old_row = dict(row)
        row["content_raw"] = new_raw
        row["content_normalized"] = new_norm
        row["embeds_count"] = max(int(row.get("embeds_count", 0)), 1)
        row["_embed_text_appended"] = embed_text

        if args.dry_run:
            safe = lambda s: s.encode("utf-8", errors="replace").decode("utf-8", errors="replace")
            print(f"WOULD PATCH message_id={mid}")
            print(f"  content_raw was:    \"{safe(str(old_row.get('content_raw', ''))[:100])}\"")
            print(f"  content_raw now:    \"{safe(str(row['content_raw'])[:100])}\"")
            print(f"  embed_text:         \"{safe(str(embed_text)[:100])}\"")
            print()

        patched += 1
        updated_lines.append(row)

    print(f"\nSummary:")
    print(f"  Total normalized messages: {len(lines)}")
    print(f"  Patched with embed text:  {patched}")
    print(f"  Already had embed text:   {unchanged}")
    print(f"  No raw embed data found:  {missing_raw}")

    if not args.dry_run and patched > 0:
        with open(args.normalized_jsonl, "w", encoding="utf-8") as f:
            for row in updated_lines:
                f.write(json.dumps(row, ensure_ascii=False) + "\n")
        print(f"\n  Wrote {len(updated_lines)} lines back to {args.normalized_jsonl}")
    elif args.dry_run:
        print(f"\n  DRY RUN — no files were modified.")
    else:
        print(f"\n  No patches needed.")


if __name__ == "__main__":
    main()