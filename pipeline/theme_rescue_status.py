from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from pipeline.artifact_paths import ArtifactPaths, migrate_run_artifacts_to_numbered
from pipeline.common import now_utc_iso, read_json, read_jsonl, write_json
from pipeline.stage_04r_theme_relevance_rerun import theme_aware_rerun_config


def load_pipeline_config(repo_root: Path | None = None) -> dict[str, Any]:
    candidates: list[Path] = []
    if repo_root is not None:
        candidates.append(repo_root / "config" / "pipeline_config.json")
    candidates.append(Path("config/pipeline_config.json"))
    for path in candidates:
        if not path.exists():
            continue
        try:
            payload = read_json(path)
        except Exception:
            continue
        if isinstance(payload, dict):
            return payload
    return {}


def theme_rerun_enabled(config: dict[str, Any] | None = None) -> bool:
    cfg = theme_aware_rerun_config(config or load_pipeline_config())
    return bool(cfg.get("enabled", False))


def require_rescue_approval(config: dict[str, Any] | None = None) -> bool:
    cfg = theme_aware_rerun_config(config or load_pipeline_config())
    if not bool(cfg.get("enabled", False)):
        return False
    return bool(cfg.get("require_human_approval_to_start_rescue", True))


def theme_rescue_approved(root: Path) -> bool:
    path = ArtifactPaths(root).theme_rescue_approval
    if not path.exists():
        return False
    try:
        payload = read_json(path)
    except Exception:
        return False
    return bool(str(payload.get("approved_at_utc", "")).strip()) if isinstance(payload, dict) else False


def write_theme_rescue_approval(root: Path, *, approved_by: str = "desktop_user", note: str = "") -> dict[str, Any]:
    paths = ArtifactPaths(root)
    paths.stage06.mkdir(parents=True, exist_ok=True)
    payload = {
        "schema_version": 1,
        "approved_at_utc": now_utc_iso(),
        "approved_by": approved_by,
        "note": note.strip(),
    }
    write_json(paths.theme_rescue_approval, payload)
    return payload


def _json_field(path: Path, key: str, default: Any = "") -> Any:
    if not path.exists():
        return default
    try:
        payload = read_json(path)
    except Exception:
        return default
    return payload.get(key, default) if isinstance(payload, dict) else default


def _json_summary(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = read_json(path)
    except Exception:
        return {}
    if not isinstance(payload, dict):
        return {}
    summary = payload.get("summary")
    return summary if isinstance(summary, dict) else {}


def _artifact_status(path: Path) -> str:
    if not path.exists():
        return "missing"
    status = str(_json_field(path, "status", "complete")).strip().lower()
    return status or "complete"


def _count_jsonl(path: Path) -> int:
    if not path.exists():
        return 0
    return len(read_jsonl(path))


def theme_learning_complete(paths: ArtifactPaths) -> bool:
    required = [paths.theme_profile_update_report, paths.theme_candidate_reclassification]
    return all(path.exists() for path in required)


def _mtime(path: Path) -> float:
    return path.stat().st_mtime if path.exists() else 0.0


def _newer_than_outputs(inputs: list[Path], outputs: list[Path]) -> bool:
    existing_outputs = [path for path in outputs if path.exists()]
    if not existing_outputs:
        return True
    latest_input = max((_mtime(path) for path in inputs if path.exists()), default=0.0)
    earliest_output = min(_mtime(path) for path in existing_outputs)
    return latest_input > earliest_output


def _theme_learning_outputs(paths: ArtifactPaths) -> list[Path]:
    return [paths.theme_profile_update_report, paths.theme_candidate_reclassification]


def _rescue_outputs(paths: ArtifactPaths) -> list[Path]:
    return [paths.theme_relevance_rerun, paths.snippets_with_theme_rescue]


def rescue_artifacts_stale(paths: ArtifactPaths) -> bool:
    """True when 06C/06D outputs are newer than 04R/06R rescue artifacts."""
    learning = _theme_learning_outputs(paths)
    rescue = _rescue_outputs(paths)
    if not all(path.exists() for path in learning):
        return False
    return _newer_than_outputs(learning, rescue)


def _process_state(
    *,
    complete: bool,
    ready: bool,
    blocked: bool,
    skipped: bool = False,
    stale: bool = False,
) -> str:
    if skipped:
        return "skipped"
    if stale:
        return "stale"
    if complete:
        return "done"
    if blocked:
        return "waiting"
    if ready:
        return "ready"
    return "waiting"


def theme_rescue_status_payload(root: Path, repo_root: Path | None = None) -> dict[str, Any]:
    migrate_run_artifacts_to_numbered(root)
    paths = ArtifactPaths(root)
    config = load_pipeline_config(repo_root)
    rerun_cfg = theme_aware_rerun_config(config)
    enabled = bool(rerun_cfg.get("enabled", False))
    approval_required = require_rescue_approval(config)
    approved = theme_rescue_approved(root)
    learning_complete = theme_learning_complete(paths)

    rerun_summary = _json_summary(paths.theme_relevance_rerun)
    merge_summary = _json_summary(paths.theme_rescue_snippet_merge_report)
    rerun_status = _artifact_status(paths.theme_relevance_rerun)
    merge_status = _artifact_status(paths.theme_rescue_snippet_merge_report)
    rerun_complete = paths.theme_relevance_rerun.exists() and rerun_status == "complete"
    merge_complete = paths.snippets_with_theme_rescue.exists() and (
        paths.theme_rescue_snippet_merge_report.exists() or _count_jsonl(paths.snippets_with_theme_rescue) > 0
    )
    rescue_stale = rescue_artifacts_stale(paths)

    approval_blocked = approval_required and not approved
    rerun_ready = enabled and learning_complete and not approval_blocked and not rerun_complete
    rerun_blocked = enabled and (not learning_complete or approval_blocked)
    merge_ready = enabled and rerun_complete and not approval_blocked and not merge_complete
    merge_blocked = enabled and (not rerun_complete or approval_blocked)

    processes: list[dict[str, Any]] = [
        {
            "id": "04R",
            "short_label": "04R",
            "name": "Theme Relevance Rerun",
            "description": "Rescore previously rejected conversation windows and rescan accepted segments using learned theme priors.",
            "state": _process_state(
                complete=rerun_complete,
                ready=rerun_ready,
                blocked=rerun_blocked,
                skipped=not enabled,
                stale=rescue_stale and rerun_complete,
            ),
            "artifact_path": str(paths.theme_relevance_rerun),
            "summary": {
                "status": rerun_status if paths.theme_relevance_rerun.exists() else "missing",
                "candidate_window_count": int(rerun_summary.get("candidate_window_count", 0) or 0),
                "rescued_conversation_count": int(rerun_summary.get("rescued_conversation_count", 0) or 0),
                "rescued_message_count": int(rerun_summary.get("rescued_message_count", 0) or 0),
                "failure_count": int(rerun_summary.get("failure_count", 0) or 0),
                "generated_at_utc": str(_json_field(paths.theme_relevance_rerun, "generated_at_utc", "")),
            },
        },
        {
            "id": "06R",
            "short_label": "06R",
            "name": "Theme Rescue Snippet Extraction",
            "description": "Extract supplemental snippets from rescued conversations and merge them into the downstream snippet corpus.",
            "state": _process_state(
                complete=merge_complete,
                ready=merge_ready,
                blocked=merge_blocked,
                skipped=not enabled,
                stale=rescue_stale and merge_complete,
            ),
            "artifact_path": str(paths.theme_rescue_snippet_merge_report),
            "summary": {
                "status": merge_status if paths.theme_rescue_snippet_merge_report.exists() else "missing",
                "rescue_snippet_count": int(merge_summary.get("rescue_snippet_count", 0) or 0),
                "combined_snippet_count": int(merge_summary.get("combined_snippet_count", 0) or 0),
                "strict_snippet_count": int(merge_summary.get("strict_snippet_count", 0) or 0),
                "generated_at_utc": str(_json_field(paths.theme_rescue_snippet_merge_report, "generated_at_utc", "")),
            },
        },
    ]

    show_prompt = enabled and learning_complete and approval_blocked
    rescue_pending = enabled and learning_complete and (
        approval_blocked or rescue_stale or not rerun_complete or not merge_complete
    )

    if show_prompt and rescue_stale and (rerun_complete or merge_complete):
        prompt_title = "Theme learning updated — refresh rescue?"
        prompt_message = (
            "04R/06R finished earlier, but Stage 06C/06D updated the theme profile afterward. "
            "Review themes on the Themes tab, approve when satisfied, then use Run 04R / 06R on this tab "
            "to refresh rescue artifacts before the lore ledger continues."
        )
        prompt_confirm = (
            "Record theme rescue approval for this run?\n\n"
            "04R/06R will not start until you choose Run 04R / 06R."
        )
        prompt_action = "Approve theme rescue"
    elif show_prompt and rerun_complete and merge_complete:
        prompt_title = "Confirm theme rescue"
        prompt_message = (
            "04R and 06R are complete for this run. Approve to record your sign-off and unlock "
            "Stage 07 (Lore Development Ledger) on the next pipeline resume."
        )
        prompt_confirm = (
            "Record theme rescue approval for this run?\n\n"
            "The pipeline will not start automatically. Resume from the Pipeline tab when ready."
        )
        prompt_action = "Approve theme rescue"
    else:
        prompt_title = "Theme learning complete — approve rescue?"
        prompt_message = (
            "Stage 06C/06D finished updating the theme profile. "
            "Review themes, approve when satisfied, then use Run 04R / 06R when you want relevance rerun "
            "and rescue snippet extraction."
        )
        prompt_confirm = (
            "Record theme rescue approval for this run?\n\n"
            "04R/06R will not start until you choose Run 04R / 06R."
        )
        prompt_action = "Approve theme rescue"

    return {
        "active_root": str(root),
        "enabled": enabled,
        "approval_required": approval_required,
        "approved": approved,
        "theme_learning_complete": learning_complete,
        "rescue_stale": rescue_stale,
        "rescue_pending": rescue_pending,
        "prompt": {
            "show": show_prompt,
            "title": prompt_title,
            "message": prompt_message,
            "action_label": prompt_action,
            "confirm_message": prompt_confirm,
        },
        "processes": processes,
        "policy": {
            "rerun_include_previous_accepts": bool(rerun_cfg.get("rerun_include_previous_accepts", False)),
            "require_active_theme": bool(rerun_cfg.get("require_active_theme", True)),
            "min_rescue_confidence": rerun_cfg.get("min_rescue_confidence"),
        },
    }
