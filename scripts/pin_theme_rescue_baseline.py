#!/usr/bin/env python3
"""Pin theme rescue as current for the canon theme profile (skip 04R until profile changes)."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from pipeline.run_pipeline import determine_resume_start_stage, load_max_execution_stage
from pipeline.theme_rescue_status import (
    rescue_artifacts_stale,
    theme_rescue_status_payload,
    write_theme_rescue_baseline,
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Accept current 04R/06R for the canon theme profile (no full 04R rerun until profile changes).",
    )
    parser.add_argument("--run-root", type=Path, required=True)
    parser.add_argument("--repo-root", type=Path, default=_REPO_ROOT)
    parser.add_argument("--note", type=str, default="")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)

    run_root = args.run_root.resolve()
    baseline = write_theme_rescue_baseline(run_root, note=args.note, repo_root=args.repo_root.resolve())
    from pipeline.artifact_paths import ArtifactPaths

    paths = ArtifactPaths(run_root)
    stale = rescue_artifacts_stale(paths, repo_root=args.repo_root.resolve())
    resume = determine_resume_start_stage(
        run_root,
        ignore_pending=True,
        max_stage=load_max_execution_stage(),
    )
    out = {
        "ok": True,
        "baseline": baseline,
        "rescue_stale": stale,
        "resume": {"stage": resume[0], "reason": resume[1]},
        "theme_rescue": theme_rescue_status_payload(run_root, args.repo_root.resolve()),
    }
    if args.json:
        print(json.dumps(out, indent=2))
    else:
        print(f"pinned theme_profile updated_at={baseline.get('theme_profile_updated_at_utc')}")
        print(f"rescue_stale={stale}")
        print(f"resume -> stage {resume[0]}: {resume[1]}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
