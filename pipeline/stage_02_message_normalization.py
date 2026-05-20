from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from pipeline.common import (
    CUTOFF_UTC,
    PARSER_VERSION,
    get_logger,
    isoformat_utc,
    normalize_display_text,
    parse_discord_timestamp,
    read_json,
    safe_uuid,
    stable_id,
    text_hash,
    write_json,
    write_jsonl,
)

DEBUG_LOG_PATH = Path("debug-f7d16c.log")
DEBUG_SESSION_ID = "f7d16c"


# region agent log
def _debug_log(run_id: str, hypothesis_id: str, location: str, message: str, data: dict[str, Any]) -> None:
    try:
        payload = {
            "sessionId": DEBUG_SESSION_ID,
            "runId": run_id,
            "hypothesisId": hypothesis_id,
            "location": location,
            "message": message,
            "data": data,
            "timestamp": int(__import__("time").time() * 1000),
        }
        with DEBUG_LOG_PATH.open("a", encoding="utf-8") as f:
            f.write(json.dumps(payload, ensure_ascii=False) + "\n")
    except Exception:
        pass
# endregion


def infer_thread_identity(message: dict[str, Any], root_name: str) -> tuple[str, str]:
    author = message.get("author", {}) or {}
    author_id = str(author.get("id", "unknown"))
    author_name = author.get("global_name") or author.get("username") or "unknown"
    return author_id, str(author_name)


def infer_partner_from_messages(messages: list[dict[str, Any]]) -> tuple[str, str]:
    counts: dict[str, int] = {}
    labels: dict[str, str] = {}
    for msg in messages:
        author = msg.get("author", {}) or {}
        aid = str(author.get("id", "unknown"))
        if aid not in counts:
            counts[aid] = 0
        counts[aid] += 1
        labels[aid] = str(author.get("global_name") or author.get("username") or aid)
    if not counts:
        return "unknown", "unknown"
    partner_id = sorted(counts.items(), key=lambda x: x[1], reverse=True)[0][0]
    return partner_id, labels.get(partner_id, "unknown")


def normalize_one_message(
    msg: dict[str, Any],
    json_path: Path,
    thread_id: str,
    partner_id: str,
    partner_label: str,
) -> dict[str, Any] | None:
    timestamp_raw = msg.get("timestamp")
    if not timestamp_raw:
        return None
    ts = parse_discord_timestamp(str(timestamp_raw))
    if ts < CUTOFF_UTC:
        return None
    author = msg.get("author", {}) or {}
    content = msg.get("content", "")
    content = content if isinstance(content, str) else str(content)
    message_id = str(msg.get("id", safe_uuid()))
    is_bot_or_application = bool(
        msg.get("application_id")
        or msg.get("webhook_id")
        or author.get("bot") is True
    )
    return {
        "message_id": message_id,
        "thread_id": thread_id,
        "partner_id": partner_id,
        "partner_label": partner_label,
        "channel_id": str(msg.get("channel_id", "")),
        "timestamp_utc": isoformat_utc(ts),
        "author_id": str(author.get("id", "unknown")),
        "author_name": str(author.get("global_name") or author.get("username") or "unknown"),
        "author_is_bot": bool(author.get("bot") is True),
        "is_bot_or_application": is_bot_or_application,
        "application_id": str(msg.get("application_id", "")) if msg.get("application_id") else "",
        "webhook_id": str(msg.get("webhook_id", "")) if msg.get("webhook_id") else "",
        "content_raw": content,
        "content_normalized": normalize_display_text(content),
        "attachments_count": len(msg.get("attachments", []) or []),
        "embeds_count": len(msg.get("embeds", []) or []),
        "sensitivity_flags": [],
        "provenance": {
            "json_path": str(json_path),
            "export_batch": "discord_conversations",
            "parser_version": PARSER_VERSION,
            "content_hash": text_hash(message_id, content),
        },
    }


def iter_json_pages(conversations_root: Path) -> list[Path]:
    return sorted(conversations_root.rglob("*.json"))


def run(conversations_root: Path, out_jsonl: Path, out_summary_json: Path) -> None:
    logger = get_logger(__name__)
    all_rows: list[dict[str, Any]] = []
    rejected = 0
    files = iter_json_pages(conversations_root)
    # region agent log
    _debug_log(
        "stage02-debug",
        "H4",
        "stage_02_message_normalization.py:run",
        "Stage 02 run inputs",
        {
            "conversations_root": str(conversations_root),
            "input_files": len(files),
            "cutoff_utc": isoformat_utc(CUTOFF_UTC),
        },
    )
    # endregion
    logger.info("Stage 02: starting normalization from %d JSON file(s).", len(files))
    progress_every = max(1, len(files) // 10)
    global_min_ts = None
    global_max_ts = None
    sample_file_logs = 0
    for processed_files, json_path in enumerate(files, start=1):
        try:
            messages = read_json(json_path)
            if not isinstance(messages, list):
                logger.debug("Skipping non-list JSON payload: %s", json_path)
                continue
        except Exception:
            logger.warning("Skipping unreadable JSON file: %s", json_path)
            continue
        thread_id = stable_id("thread", str(json_path.parent))
        partner_id, partner_label = infer_partner_from_messages(messages)
        logger.debug(
            "Normalizing file=%s thread_id=%s partner=%s(%s) messages=%d",
            json_path,
            thread_id,
            partner_label,
            partner_id,
            len(messages),
        )
        file_min_ts = None
        file_max_ts = None
        file_missing_ts = 0
        file_kept = 0
        file_rejected_cutoff = 0
        for msg in messages:
            raw_ts = msg.get("timestamp")
            if raw_ts is None:
                file_missing_ts += 1
            else:
                try:
                    ts_obj = parse_discord_timestamp(str(raw_ts))
                    if file_min_ts is None or ts_obj < file_min_ts:
                        file_min_ts = ts_obj
                    if file_max_ts is None or ts_obj > file_max_ts:
                        file_max_ts = ts_obj
                    if global_min_ts is None or ts_obj < global_min_ts:
                        global_min_ts = ts_obj
                    if global_max_ts is None or ts_obj > global_max_ts:
                        global_max_ts = ts_obj
                    if ts_obj < CUTOFF_UTC:
                        file_rejected_cutoff += 1
                except Exception:
                    pass
            row = normalize_one_message(msg, json_path, thread_id, partner_id, partner_label)
            if row is None:
                rejected += 1
                continue
            all_rows.append(row)
            file_kept += 1
        if sample_file_logs < 5:
            # region agent log
            _debug_log(
                "stage02-debug",
                "H1",
                "stage_02_message_normalization.py:run",
                "Per-file timestamp and keep/reject stats",
                {
                    "file": str(json_path),
                    "messages": len(messages),
                    "file_min_ts": isoformat_utc(file_min_ts) if file_min_ts is not None else None,
                    "file_max_ts": isoformat_utc(file_max_ts) if file_max_ts is not None else None,
                    "file_kept": file_kept,
                    "file_missing_ts": file_missing_ts,
                    "file_rejected_cutoff": file_rejected_cutoff,
                },
            )
            # endregion
            sample_file_logs += 1
        if processed_files % progress_every == 0 or processed_files == len(files):
            logger.info(
                "Stage 02 progress: %d/%d files, normalized=%d, rejected=%d",
                processed_files,
                len(files),
                len(all_rows),
                rejected,
            )

    all_rows.sort(key=lambda x: (x["timestamp_utc"], x["message_id"]))
    write_jsonl(out_jsonl, all_rows)
    write_json(
        out_summary_json,
        {
            "input_files": len(files),
            "normalized_messages": len(all_rows),
            "rejected_before_cutoff_or_invalid": rejected,
            "cutoff_utc": isoformat_utc(CUTOFF_UTC),
            "global_min_timestamp_seen": isoformat_utc(global_min_ts) if global_min_ts is not None else None,
            "global_max_timestamp_seen": isoformat_utc(global_max_ts) if global_max_ts is not None else None,
        },
    )
    # region agent log
    _debug_log(
        "stage02-debug",
        "H2",
        "stage_02_message_normalization.py:run",
        "Stage 02 global timestamp summary",
        {
            "global_min_timestamp_seen": isoformat_utc(global_min_ts) if global_min_ts is not None else None,
            "global_max_timestamp_seen": isoformat_utc(global_max_ts) if global_max_ts is not None else None,
            "normalized_messages": len(all_rows),
            "rejected_total": rejected,
        },
    )
    # endregion
    logger.info(
        "Stage 02 complete: wrote %d normalized messages to %s (rejected=%d).",
        len(all_rows),
        out_jsonl,
        rejected,
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--conversations-root", type=Path, required=True)
    parser.add_argument("--out-jsonl", type=Path, required=True)
    parser.add_argument("--out-summary-json", type=Path, required=True)
    args = parser.parse_args()
    run(args.conversations_root, args.out_jsonl, args.out_summary_json)


if __name__ == "__main__":
    main()
