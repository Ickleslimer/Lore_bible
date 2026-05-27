"""Compare lore development ledger entry quality: baseline vs new batch."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

from pipeline.common import read_jsonl
from pipeline.ledger_quality_metrics import ledger_entry_metrics


def _split_by_recorded_at(entries: list, cutoff: str) -> tuple[list, list]:
    before: list = []
    after: list = []
    for entry in entries:
        recorded = str(entry.get("recorded_at_utc", ""))
        if recorded >= cutoff:
            after.append(entry)
        else:
            before.append(entry)
    return before, after


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--jsonl",
        type=Path,
        default=Path("artifacts/runs/20260517_032555635445_full/07_lore_development_ledger/lore_development_ledger.jsonl"),
    )
    parser.add_argument(
        "--cutoff",
        type=str,
        default="",
        help="ISO timestamp; entries recorded_at_utc >= cutoff are treated as the new batch.",
    )
    parser.add_argument("--sample", type=int, default=5, help="Headlines to print from each batch.")
    args = parser.parse_args()

    entries = [row for row in read_jsonl(args.jsonl) if isinstance(row, dict)]
    if args.cutoff:
        baseline, new_batch = _split_by_recorded_at(entries, args.cutoff)
    else:
        ordered = sorted(entries, key=lambda r: str(r.get("recorded_at_utc", "")))
        baseline = [e for e in ordered if str(e.get("recorded_at_utc", "")) < "2026-05-26T16:00:00"]
        new_batch = [e for e in ordered if str(e.get("recorded_at_utc", "")) >= "2026-05-26T16:00:00"]

    print("=== Baseline (paid-era sample) ===")
    print(json.dumps(ledger_entry_metrics(baseline), indent=2))
    print("\n=== New batch ===")
    print(json.dumps(ledger_entry_metrics(new_batch), indent=2))

    print("\n--- Sample baseline headlines ---")
    for entry in baseline[: args.sample]:
        print(f"  [{entry.get('confidence')}] {entry.get('headline', '')[:120]}")

    print("\n--- Sample new-batch headlines ---")
    for entry in new_batch[: args.sample]:
        print(f"  [{entry.get('confidence')}] {entry.get('headline', '')[:120]}")


if __name__ == "__main__":
    main()
