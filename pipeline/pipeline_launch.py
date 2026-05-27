from __future__ import annotations

import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pipeline.common import read_json, write_json
from pipeline.pipeline_watch import (
    resolve_repo_root,
    resolve_run_root,
    start_watch_job,
    worker_log_path,
    watches_dir,
)
from pipeline.review_inventory import process_id_exists
from pipeline.ui_review_app import (
    load_last_open_artifacts_root,
    new_run_artifacts_root,
    save_last_open_artifacts_root,
)


def _now_local() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def configured_pipeline_inputs(repo_root: Path) -> tuple[Path, Path]:
    config_path = repo_root / "config" / "pipeline_config.json"
    docx_raw = "theriac-coda---lore-bible.docx"
    conversations_raw = "discord_conversations"
    if config_path.exists():
        try:
            config = read_json(config_path)
            if isinstance(config, dict):
                paths = config.get("paths", {})
                if isinstance(paths, dict):
                    docx_raw = str(paths.get("docx_lore_bible") or docx_raw)
                    conversations_raw = str(paths.get("discord_conversations_root") or conversations_raw)
        except Exception:
            pass

    def _resolve(raw: str) -> Path:
        path = Path(raw)
        if not path.is_absolute():
            path = repo_root / path
        return path.resolve()

    return _resolve(docx_raw), _resolve(conversations_raw)


def build_run_pipeline_command(
    repo_root: Path,
    run_root: Path,
    *,
    resume: bool = True,
    ignore_pending: bool = False,
    start_stage: int | None = None,
    python_exe: str | None = None,
) -> list[str]:
    docx, conversations = configured_pipeline_inputs(repo_root)
    cmd = [
        python_exe or sys.executable,
        "-u",
        "-m",
        "pipeline.run_pipeline",
        "--docx",
        str(docx),
        "--conversations-root",
        str(conversations),
        "--artifacts-root",
        str(run_root.resolve()),
        "--log-level",
        "INFO",
    ]
    if resume:
        cmd.append("--resume")
    if ignore_pending:
        cmd.append("--ignore-pending")
    if start_stage is not None and 1 <= start_stage <= 12:
        cmd.extend(["--start-stage", str(start_stage)])
    return cmd


def _append_worker_log(run_root: Path, line: str) -> None:
    path = worker_log_path(run_root)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(line.rstrip() + "\n")


def _write_worker_pid(run_root: Path, pid: int) -> None:
    path = run_root / "pipeline_worker.pid"
    path.write_text(str(pid), encoding="utf-8")


def pipeline_worker_running(run_root: Path) -> bool:
    pid_path = run_root / "pipeline_worker.pid"
    if pid_path.exists():
        try:
            pid = int(pid_path.read_text(encoding="utf-8").strip())
        except ValueError:
            pid = 0
        if process_id_exists(pid):
            return True
    from pipeline.pipeline_watch import worker_log_still_active

    return worker_log_still_active(run_root)


def start_pipeline_worker(
    repo_root: Path,
    run_root: Path,
    *,
    resume: bool = True,
    ignore_pending: bool = False,
    start_stage: int | None = None,
    force: bool = False,
) -> dict[str, Any]:
    """Headless equivalent of Tauri 'Run / Resume Full Pipeline'."""
    run_root = run_root.resolve()
    repo_root = repo_root.resolve()
    run_root.mkdir(parents=True, exist_ok=True)
    save_last_open_artifacts_root(repo_root, run_root)

    if pipeline_worker_running(run_root) and not force:
        return {
            "ok": True,
            "started": False,
            "reason": "already_running",
            "run_root": str(run_root),
            "log_path": str(worker_log_path(run_root)),
        }

    docx, conversations = configured_pipeline_inputs(repo_root)
    if not docx.exists():
        return {"ok": False, "error": f"Lore bible DOCX not found: {docx}"}
    if not conversations.exists():
        return {"ok": False, "error": f"Conversations folder not found: {conversations}"}

    cmd = build_run_pipeline_command(
        repo_root,
        run_root,
        resume=resume,
        ignore_pending=ignore_pending,
        start_stage=start_stage,
    )
    log_path = worker_log_path(run_root)
    log_path.parent.mkdir(parents=True, exist_ok=True)
    env = os.environ.copy()
    env["PYTHONIOENCODING"] = "utf-8"
    env["PYTHONUNBUFFERED"] = "1"

    creationflags = 0
    if os.name == "nt":
        creationflags = subprocess.CREATE_NEW_PROCESS_GROUP | subprocess.DETACHED_PROCESS

    _append_worker_log(
        run_root,
        f"{_now_local()} | desktop: starting pipeline worker resume={str(resume).lower()} ignore_pending={str(ignore_pending).lower()}",
    )
    with log_path.open("a", encoding="utf-8") as log_handle:
        proc = subprocess.Popen(
            cmd,
            cwd=str(repo_root),
            stdout=log_handle,
            stderr=subprocess.STDOUT,
            env=env,
            creationflags=creationflags,
            close_fds=os.name != "nt",
        )
    pid = int(proc.pid)
    _write_worker_pid(run_root, pid)
    _append_worker_log(run_root, f"{_now_local()} | desktop: Started pipeline process {pid}.")
    return {
        "ok": True,
        "started": True,
        "pid": pid,
        "run_root": str(run_root),
        "log_path": str(log_path),
        "command": cmd,
    }


def sentinel_paths(repo_root: Path) -> dict[str, Path]:
    base = watches_dir(repo_root)
    return {
        "pid": base / "sentinel.pid",
        "log": base / "sentinel.log",
    }


def sentinel_running(repo_root: Path) -> bool:
    pid_path = sentinel_paths(repo_root)["pid"]
    if not pid_path.exists():
        return False
    try:
        pid = int(pid_path.read_text(encoding="utf-8").strip())
    except ValueError:
        return False
    return process_id_exists(pid)


def start_sentinel(
    repo_root: Path,
    *,
    interval_seconds: int = 60,
    force: bool = False,
) -> dict[str, Any]:
    """Start pipeline_watch_sentinel.py --loop in a detached background process."""
    repo_root = repo_root.resolve()
    if sentinel_running(repo_root) and not force:
        paths = sentinel_paths(repo_root)
        return {
            "ok": True,
            "started": False,
            "reason": "already_running",
            "pid_path": str(paths["pid"]),
            "log_path": str(paths["log"]),
        }

    script = repo_root / "scripts" / "pipeline_watch_sentinel.py"
    if not script.exists():
        return {"ok": False, "error": f"Sentinel script not found: {script}"}

    paths = sentinel_paths(repo_root)
    paths["log"].parent.mkdir(parents=True, exist_ok=True)
    cmd = [
        sys.executable,
        str(script),
        "--repo-root",
        str(repo_root),
        "--loop",
        "--interval",
        str(max(15, int(interval_seconds))),
    ]
    env = os.environ.copy()
    env["PYTHONIOENCODING"] = "utf-8"
    env["PYTHONUNBUFFERED"] = "1"
    creationflags = 0
    if os.name == "nt":
        creationflags = subprocess.CREATE_NEW_PROCESS_GROUP | subprocess.DETACHED_PROCESS

    with paths["log"].open("a", encoding="utf-8") as log_handle:
        log_handle.write(f"{_now_local()} | starting sentinel interval={interval_seconds}s\n")
        proc = subprocess.Popen(
            cmd,
            cwd=str(repo_root),
            stdout=log_handle,
            stderr=subprocess.STDOUT,
            env=env,
            creationflags=creationflags,
            close_fds=os.name != "nt",
        )
    paths["pid"].write_text(str(proc.pid), encoding="utf-8")
    return {
        "ok": True,
        "started": True,
        "pid": int(proc.pid),
        "pid_path": str(paths["pid"]),
        "log_path": str(paths["log"]),
        "command": cmd,
    }


def stop_sentinel(repo_root: Path) -> dict[str, Any]:
    from pipeline.review_inventory import stop_process_tree_by_pid

    paths = sentinel_paths(repo_root)
    if not paths["pid"].exists():
        return {"ok": True, "stopped": False, "reason": "not_running"}
    try:
        pid = int(paths["pid"].read_text(encoding="utf-8").strip())
    except ValueError:
        paths["pid"].unlink(missing_ok=True)
        return {"ok": True, "stopped": False, "reason": "invalid_pid_file"}
    stop_process_tree_by_pid(pid)
    paths["pid"].unlink(missing_ok=True)
    return {"ok": True, "stopped": True, "pid": pid}


def resolve_handoff_run_root(
    repo_root: Path,
    run_root: str | Path | None = None,
    *,
    new_run: bool = False,
) -> Path:
    if new_run:
        root = new_run_artifacts_root(repo_root)
        save_last_open_artifacts_root(repo_root, root)
        return root.resolve()
    if run_root:
        root = resolve_run_root(repo_root, run_root)
        save_last_open_artifacts_root(repo_root, root)
        return root
    last = load_last_open_artifacts_root(repo_root)
    if last is not None:
        return last.resolve()
    return resolve_run_root(repo_root, None)


def pipeline_handoff(
    repo_root: Path,
    *,
    run_root: str | Path | None = None,
    new_run: bool = False,
    start_pipeline: bool = True,
    resume: bool = True,
    ignore_pending: bool = False,
    start_stage: int | None = None,
    start_sentinel_daemon: bool = False,
    sentinel_interval_seconds: int = 60,
    start_watch: bool = False,
    watcher: str = "antigravity_flash",
    poll_interval_seconds: int = 300,
    on_watcher_lost: str = "alert",
    force_pipeline: bool = False,
    force_sentinel: bool = False,
) -> dict[str, Any]:
    """
    Autonomous steps 1–2 of pipeline watch handoff:
    start pipeline worker (Tauri-equivalent) + sentinel loop + optional watch job record.
    """
    repo_root = repo_root.resolve()
    root = resolve_handoff_run_root(repo_root, run_root, new_run=new_run)
    result: dict[str, Any] = {
        "ok": True,
        "repo_root": str(repo_root),
        "run_root": str(root),
    }

    if start_pipeline:
        pipeline_result = start_pipeline_worker(
            repo_root,
            root,
            resume=resume,
            ignore_pending=ignore_pending,
            start_stage=start_stage,
            force=force_pipeline,
        )
        result["pipeline"] = pipeline_result
        if not pipeline_result.get("ok"):
            result["ok"] = False
            return result

    if start_sentinel_daemon:
        sentinel_result = start_sentinel(
            repo_root,
            interval_seconds=sentinel_interval_seconds,
            force=force_sentinel,
        )
        result["sentinel"] = sentinel_result
        if not sentinel_result.get("ok"):
            result["ok"] = False
            return result

    if start_watch:
        job = start_watch_job(
            repo_root,
            run_root=root,
            watcher=watcher,
            poll_interval_seconds=poll_interval_seconds,
            on_watcher_lost=on_watcher_lost,
        )
        result["watch_job"] = job
        result["next_steps"] = [
            "Antigravity Flash: poll theriac_watch_status every "
            f"{poll_interval_seconds}s with checked_by antigravity_flash (job_id={job['job_id']}).",
            "Cursor: do not poll; read watch_report.md or watch_alert.json when the user returns.",
        ]
    else:
        result["next_steps"] = [
            "Call theriac_watch_start if a watch job is still needed.",
            "Antigravity Flash polls theriac_watch_status.",
        ]

    return result
