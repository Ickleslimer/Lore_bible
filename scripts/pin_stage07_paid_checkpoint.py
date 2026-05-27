"""Pin or restore Stage 07 lore ledger at the last paid-model checkpoint.

Paid-era batch: OpenRouter DeepSeek V4 Flash (pre free/NIM experiment).
Experiment entries are those with recorded_at_utc >= 2026-05-26T16:00:00Z
(global sequences 265–266 in run 20260517_032555635445_full).
"""
from __future__ import annotations

import argparse
import json
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pipeline.common import read_json, read_jsonl, write_json, write_jsonl
from pipeline.stage_05_lore_development_ledger import group_entity_history

DEFAULT_RUN_ROOT = Path("artifacts/runs/20260517_032555635445_full")
DEFAULT_BACKUP_ROOT = Path("artifacts/backups/stage07_paid_openrouter_flash_20260526")
PAID_ENTRY_CUTOFF_UTC = "2026-05-26T16:00:00"
EXPERIMENT_SEGMENT_IDS = frozenset(
    {
        "theme_rescue_conversation_eaa6b570fef71e14",
        "conversation_891d6619530c67a3",
    }
)
STAGE07_FILES = (
    "lore_development_ledger.jsonl",
    "lore_development_ledger_index.json",
    "entity_development_history.json",
    "lore_development_ledger_failures.json",
)


def _stage07_dir(run_root: Path) -> Path:
    return run_root / "07_lore_development_ledger"


def _load_entries(jsonl_path: Path) -> list[dict[str, Any]]:
    return [row for row in read_jsonl(jsonl_path) if isinstance(row, dict)]


def paid_checkpoint_entries(entries: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        entry
        for entry in entries
        if str(entry.get("recorded_at_utc", "")).strip() < PAID_ENTRY_CUTOFF_UTC
    ]


def build_paid_index(
    *,
    entries: list[dict[str, Any]],
    failures: list[dict[str, Any]],
    completed_segment_ids: set[str],
    total_segments: int,
) -> dict[str, Any]:
    return {
        "generated_at_utc": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "status": "complete",
        "entry_count": len(entries),
        "segment_count": total_segments,
        "completed_segment_count": len(completed_segment_ids),
        "failure_count": len(failures),
        "completed_segment_ids": sorted(completed_segment_ids),
        "checkpoint_note": (
            "Pinned paid OpenRouter Flash checkpoint before free/NIM Stage 07 experiment. "
            f"Entries recorded before {PAID_ENTRY_CUTOFF_UTC}Z."
        ),
    }


def build_paid_history(entries: list[dict[str, Any]]) -> dict[str, Any]:
    grouped = group_entity_history(entries)
    return {
        "generated_at_utc": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "status": "complete",
        "entry_count": len(entries),
        "entity_count": len(grouped),
        "by_entity": grouped,
        "grouped": {
            "new": [entry for entry in entries if entry.get("event_kind") == "new"],
            "change": [entry for entry in entries if entry.get("event_kind") == "change"],
        },
        "checkpoint_note": (
            "Rebuilt from paid-era ledger entries only "
            f"(recorded_at_utc < {PAID_ENTRY_CUTOFF_UTC}Z)."
        ),
    }


def pin_checkpoint(
    *,
    run_root: Path,
    backup_root: Path,
    overwrite: bool = False,
) -> dict[str, Any]:
    stage07 = _stage07_dir(run_root)
    jsonl_path = stage07 / "lore_development_ledger.jsonl"
    index_path = stage07 / "lore_development_ledger_index.json"
    failures_path = stage07 / "lore_development_ledger_failures.json"

    if not jsonl_path.exists():
        raise FileNotFoundError(jsonl_path)
    if backup_root.exists():
        if not overwrite:
            raise FileExistsError(f"Backup already exists: {backup_root} (pass --overwrite)")
        shutil.rmtree(backup_root)
    backup_root.mkdir(parents=True, exist_ok=True)

    all_entries = _load_entries(jsonl_path)
    paid_entries = paid_checkpoint_entries(all_entries)
    experiment_entries = [e for e in all_entries if e not in paid_entries]

    current_index = read_json(index_path) if index_path.exists() else {}
    total_segments = int(current_index.get("segment_count", 4399) or 4399)
    completed_ids = {
        str(segment_id).strip()
        for segment_id in current_index.get("completed_segment_ids", [])
        if str(segment_id).strip()
    }
    paid_completed_ids = completed_ids - EXPERIMENT_SEGMENT_IDS

    failures_payload = read_json(failures_path) if failures_path.exists() else {"failures": []}
    failures = failures_payload.get("failures", []) if isinstance(failures_payload, dict) else []
    if not isinstance(failures, list):
        failures = []

    paid_index = build_paid_index(
        entries=paid_entries,
        failures=failures,
        completed_segment_ids=paid_completed_ids,
        total_segments=total_segments,
    )
    paid_history = build_paid_history(paid_entries)
    paid_failures_payload = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "status": "complete",
        "failures": failures,
        "checkpoint_note": (
            "Failures file copied at pin time; experiment segments had no failure rows."
        ),
    }

    write_jsonl(backup_root / "lore_development_ledger.jsonl", paid_entries)
    write_json(backup_root / "lore_development_ledger_index.json", paid_index)
    write_json(backup_root / "entity_development_history.json", paid_history)
    write_json(backup_root / "lore_development_ledger_failures.json", paid_failures_payload)

    manifest = {
        "created_at_utc": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "label": "stage07_paid_openrouter_flash_before_free_experiment",
        "run_root": str(run_root.resolve()),
        "backup_root": str(backup_root.resolve()),
        "paid_entry_cutoff_utc": PAID_ENTRY_CUTOFF_UTC,
        "experiment_segment_ids_removed": sorted(EXPERIMENT_SEGMENT_IDS),
        "stats": {
            "paid_entry_count": len(paid_entries),
            "experiment_entry_count": len(experiment_entries),
            "paid_completed_segment_count": len(paid_completed_ids),
            "failure_count": len(failures),
            "segment_count": total_segments,
        },
        "restore_command": (
            f"python scripts/pin_stage07_paid_checkpoint.py --restore "
            f"--run-root {run_root.as_posix()} "
            f"--backup-root {backup_root.as_posix()}"
        ),
        "files": list(STAGE07_FILES),
    }
    write_json(backup_root / "MANIFEST.json", manifest)
    return manifest


def restore_checkpoint(*, run_root: Path, backup_root: Path) -> dict[str, Any]:
    stage07 = _stage07_dir(run_root)
    stage07.mkdir(parents=True, exist_ok=True)
    manifest_path = backup_root / "MANIFEST.json"
    if not manifest_path.exists():
        raise FileNotFoundError(manifest_path)

    pre_restore = stage07 / "_pre_restore_snapshot"
    if pre_restore.exists():
        shutil.rmtree(pre_restore)
    pre_restore.mkdir(parents=True, exist_ok=True)
    for name in STAGE07_FILES:
        live = stage07 / name
        if live.exists():
            shutil.copy2(live, pre_restore / name)

    for name in STAGE07_FILES:
        src = backup_root / name
        if not src.exists():
            raise FileNotFoundError(src)
        shutil.copy2(src, stage07 / name)

    manifest = read_json(manifest_path)
    return {
        "restored_from": str(backup_root.resolve()),
        "run_root": str(run_root.resolve()),
        "pre_restore_snapshot": str(pre_restore.resolve()),
        "manifest_stats": manifest.get("stats", {}),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-root", type=Path, default=DEFAULT_RUN_ROOT)
    parser.add_argument("--backup-root", type=Path, default=DEFAULT_BACKUP_ROOT)
    parser.add_argument("--restore", action="store_true", help="Restore live Stage 07 from backup.")
    parser.add_argument("--overwrite", action="store_true", help="Replace an existing backup directory.")
    args = parser.parse_args()

    if args.restore:
        result = restore_checkpoint(run_root=args.run_root.resolve(), backup_root=args.backup_root.resolve())
        print("Restored Stage 07 paid checkpoint.")
        print(json.dumps(result, indent=2))
        return

    manifest = pin_checkpoint(
        run_root=args.run_root.resolve(),
        backup_root=args.backup_root.resolve(),
        overwrite=args.overwrite,
    )
    print("Pinned Stage 07 paid checkpoint.")
    print(json.dumps(manifest, indent=2))


if __name__ == "__main__":
    main()
