"""Standalone Stage 08Q quest tagging on an existing run folder."""

from __future__ import annotations

import argparse
from pathlib import Path

from pipeline.artifact_paths import ArtifactPaths, migrate_run_artifacts_to_numbered
from pipeline.stage_08q_quest_tagging import run as run_stage_08q


def main() -> None:
    parser = argparse.ArgumentParser(description="Run Stage 08Q quest tagging on a pipeline run.")
    parser.add_argument("--artifacts-root", type=Path, required=True)
    parser.add_argument("--config", type=Path, default=Path("config/pipeline_config.json"))
    parser.add_argument("--review-memory", type=Path, default=Path("canon/review_memory.json"))
    args = parser.parse_args()

    run_root = args.artifacts_root.resolve()
    migrate_run_artifacts_to_numbered(run_root)
    paths = ArtifactPaths(run_root)

    run_stage_08q(
        paths.effective_snippets(),
        paths.snippet_quest_tags,
        paths.narrative_work_tags if paths.narrative_work_tags.exists() else None,
        args.config,
        args.review_memory,
        paths.entity_seed,
        paths.discovered_quests if paths.discovered_quests.exists() else None,
    )
    print(f"Wrote {paths.snippet_quest_tags}")
    print(f"Wrote {paths.discovered_quests}")
    print(f"Wrote {paths.artist_character_review_queue}")


if __name__ == "__main__":
    main()
