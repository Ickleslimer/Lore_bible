from __future__ import annotations

import argparse
from pathlib import Path

from pipeline.common import get_logger, read_jsonl, write_json, write_jsonl


def run(in_jsonl: Path, out_jsonl: Path, out_index_json: Path) -> None:
    logger = get_logger(__name__)
    logger.info("Stage 03: loading normalized timeline from %s", in_jsonl)
    rows = read_jsonl(in_jsonl)
    rows.sort(key=lambda x: (x.get("timestamp_utc", ""), x.get("message_id", "")))

    thread_counts: dict[str, int] = {}
    for row in rows:
        thread_id = str(row.get("thread_id", "unknown"))
        thread_counts[thread_id] = thread_counts.get(thread_id, 0) + 1

    write_jsonl(out_jsonl, rows)
    write_json(
        out_index_json,
        {
            "message_count": len(rows),
            "thread_counts": thread_counts,
            "ordering": "timestamp_utc,message_id",
        },
    )
    logger.info(
        "Stage 03 complete: wrote %d global rows across %d thread(s).",
        len(rows),
        len(thread_counts),
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--in-jsonl", type=Path, required=True)
    parser.add_argument("--out-jsonl", type=Path, required=True)
    parser.add_argument("--out-index-json", type=Path, required=True)
    args = parser.parse_args()
    run(args.in_jsonl, args.out_jsonl, args.out_index_json)


if __name__ == "__main__":
    main()
