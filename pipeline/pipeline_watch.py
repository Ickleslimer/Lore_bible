from __future__ import annotations

import json
import os
import re
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pipeline.common import now_utc_iso, read_json, write_json
from pipeline.review_inventory import process_id_exists, stop_process_tree_by_pid
from pipeline.ui_review_app import (
    STAGE_HEARTBEAT_RE,
    STAGE_LOG_RE,
    WORKER_FAILURE_RE,
    is_pipeline_progress_log_line,
    load_last_open_artifacts_root,
    pipeline_progress_artifact_snapshot,
    pipeline_progress_from_logs,
)

TERMINAL_UNTIL_DEFAULT = ("succeeded", "failed", "review_required", "cancelled")
WATCHER_LOST_ACTIONS = ("alert", "cancel_run", "none")
WATCHERS = ("antigravity_flash", "sentinel", "cursor")

WORKER_START_RE = re.compile(
    r"desktop:\s+starting pipeline worker",
    re.IGNORECASE,
)
WORKER_COMPLETED_RE = re.compile(
    r"desktop:\s+Pipeline completed\.?",
    re.IGNORECASE,
)
WORKER_CANCEL_RE = re.compile(
    r"desktop:\s+.*cancellation",
    re.IGNORECASE,
)
PID_IN_LOG_RE = re.compile(r"Started pipeline process\s+(\d+)", re.IGNORECASE)

OPENROUTER_AUTO_TOPUP_DEFAULT = True


def resolve_repo_root(start: Path | None = None) -> Path:
    env = str(os.environ.get("THERIAC_REPO_ROOT", "")).strip()
    if env:
        return Path(env).resolve()
    cursor = start or Path.cwd()
    for candidate in [cursor, *cursor.parents]:
        if (candidate / "pipeline").is_dir() and (candidate / "config").is_dir():
            return candidate.resolve()
    return cursor.resolve()


def resolve_run_root(repo_root: Path, run_root: str | Path | None = None) -> Path:
    if run_root:
        path = Path(run_root)
        if not path.is_absolute():
            path = repo_root / path
        return path.resolve()
    last = load_last_open_artifacts_root(repo_root)
    if last is None:
        raise FileNotFoundError("No active run root; pass run_root or set last_open in app state.")
    return last.resolve()


def watches_dir(repo_root: Path) -> Path:
    path = repo_root / "artifacts" / "pipeline_watches"
    path.mkdir(parents=True, exist_ok=True)
    return path


def quota_snapshots_dir(repo_root: Path) -> Path:
    path = repo_root / "artifacts" / "quota_snapshots"
    path.mkdir(parents=True, exist_ok=True)
    return path


def worker_log_path(run_root: Path) -> Path:
    return run_root / "tauri_pipeline_worker.log"


def read_worker_log_lines(run_root: Path, max_lines: int = 240) -> list[str]:
    path = worker_log_path(run_root)
    if not path.exists():
        return []
    try:
        lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    except OSError:
        return []
    return lines[-max_lines:]


def progress_lines_from_worker(run_root: Path, max_lines: int = 240) -> list[str]:
    return [line for line in read_worker_log_lines(run_root, max_lines) if is_pipeline_progress_log_line(line)]


def infer_worker_terminal(worker_lines: list[str]) -> tuple[bool, str | None, int | None]:
    for line in reversed(worker_lines):
        failure = WORKER_FAILURE_RE.search(line)
        if failure:
            try:
                return True, "failed", int(failure.group(1))
            except ValueError:
                return True, "failed", 1
        if WORKER_COMPLETED_RE.search(line):
            return True, "succeeded", 0
        if WORKER_CANCEL_RE.search(line):
            return True, "cancelled", None
    return False, None, None


def _last_worker_start_index(worker_lines: list[str]) -> int:
    for idx in range(len(worker_lines) - 1, -1, -1):
        if WORKER_START_RE.search(worker_lines[idx]):
            return idx
    return -1


def worker_log_still_active(run_root: Path, stale_seconds: float = 600.0) -> bool:
    path = worker_log_path(run_root)
    if not path.exists():
        return False
    try:
        mtime = path.stat().st_mtime
    except OSError:
        return False
    age = datetime.now(timezone.utc).timestamp() - mtime
    if age > stale_seconds:
        return False
    lines = read_worker_log_lines(run_root, 80)
    terminal, _, _ = infer_worker_terminal(lines)
    if terminal:
        return False
    if _last_worker_start_index(lines) >= 0:
        return True
    for line in reversed(lines):
        if STAGE_HEARTBEAT_RE.search(line) or STAGE_LOG_RE.search(line):
            return True
    return age < 120.0


def parse_pid_from_worker_log(worker_lines: list[str]) -> int | None:
    for line in reversed(worker_lines):
        match = PID_IN_LOG_RE.search(line)
        if match:
            try:
                return int(match.group(1))
            except ValueError:
                return None
    return None


def infer_run_lifecycle(run_root: Path) -> dict[str, Any]:
    worker_lines = read_worker_log_lines(run_root)
    progress_logs = progress_lines_from_worker(run_root)
    terminal, terminal_kind, exit_code = infer_worker_terminal(worker_lines)

    snapshot = pipeline_progress_artifact_snapshot(run_root)
    artifact_status = str(snapshot.get("status", "idle"))
    artifact_logs = [str(line) for line in snapshot.get("logs", [])]

    if terminal and terminal_kind:
        status = terminal_kind
        if terminal_kind == "failed" and artifact_status == "review_required":
            status = "review_required"
        logs = progress_logs or artifact_logs
        message = str(snapshot.get("message", ""))
        if terminal_kind == "failed" and exit_code not in (None, 0, 2):
            message = message or f"Pipeline stopped with exit code {exit_code}."
        return {
            "lifecycle": status,
            "running": False,
            "message": message,
            "logs": logs,
            "exit_code": exit_code,
            "source": "worker_log",
        }

    if worker_log_still_active(run_root):
        logs = progress_logs or artifact_logs
        return {
            "lifecycle": "running",
            "running": True,
            "message": "Pipeline worker appears active.",
            "logs": logs,
            "exit_code": None,
            "source": "worker_log",
        }

    if artifact_status in {"review_required", "failed", "succeeded"} and artifact_logs:
        return {
            "lifecycle": artifact_status,
            "running": False,
            "message": str(snapshot.get("message", "")),
            "logs": artifact_logs,
            "exit_code": exit_code,
            "source": "artifacts",
        }

    return {
        "lifecycle": artifact_status if artifact_status else "idle",
        "running": artifact_status == "running",
        "message": str(snapshot.get("message", "")),
        "logs": artifact_logs or progress_logs,
        "exit_code": exit_code,
        "source": "artifacts",
    }


def build_progress_payload(run_root: Path) -> dict[str, Any]:
    lifecycle = infer_run_lifecycle(run_root)
    status = str(lifecycle.get("lifecycle", "idle"))
    if lifecycle.get("running"):
        status = "running"
    progress = pipeline_progress_from_logs(
        [str(line) for line in lifecycle.get("logs", [])],
        status,
        str(lifecycle.get("message", "")),
        lifecycle.get("exit_code"),
    )
    return {
        "run_root": str(run_root),
        "lifecycle": lifecycle,
        "progress": progress,
    }


def _job_path(repo_root: Path, job_id: str) -> Path:
    return watches_dir(repo_root) / f"{job_id}.json"


def new_job_id() -> str:
    return uuid.uuid4().hex[:12]


def load_watch_job(repo_root: Path, job_id: str) -> dict[str, Any]:
    path = _job_path(repo_root, job_id)
    if not path.exists():
        raise FileNotFoundError(f"Watch job not found: {job_id}")
    payload = read_json(path)
    if not isinstance(payload, dict):
        raise ValueError(f"Invalid watch job payload: {job_id}")
    return payload


def save_watch_job(repo_root: Path, job: dict[str, Any]) -> None:
    job_id = str(job.get("job_id", "")).strip()
    if not job_id:
        raise ValueError("job_id is required")
    write_json(_job_path(repo_root, job_id), job)


def list_watch_jobs(repo_root: Path, include_terminal: bool = True) -> list[dict[str, Any]]:
    root = watches_dir(repo_root)
    jobs: list[dict[str, Any]] = []
    for path in sorted(root.glob("*.json"), key=lambda p: p.stat().st_mtime, reverse=True):
        try:
            payload = read_json(path)
        except Exception:
            continue
        if not isinstance(payload, dict):
            continue
        if not include_terminal and payload.get("terminal_status"):
            continue
        jobs.append(payload)
    return jobs


def start_watch_job(
    repo_root: Path,
    *,
    run_root: Path,
    until: tuple[str, ...] | None = None,
    watcher: str = "antigravity_flash",
    poll_interval_seconds: int = 300,
    on_watcher_lost: str = "alert",
    preflight_snapshot_at: str | None = None,
    failover_pool: str | None = None,
) -> dict[str, Any]:
    if watcher not in WATCHERS:
        raise ValueError(f"Invalid watcher: {watcher}")
    if on_watcher_lost not in WATCHER_LOST_ACTIONS:
        raise ValueError(f"Invalid on_watcher_lost: {on_watcher_lost}")
    job_id = new_job_id()
    now = now_utc_iso()
    job = {
        "job_id": job_id,
        "run_root": str(run_root.resolve()),
        "until": list(until or TERMINAL_UNTIL_DEFAULT),
        "watcher": watcher,
        "poll_interval_seconds": int(poll_interval_seconds),
        "on_watcher_lost": on_watcher_lost,
        "preflight_snapshot_at": preflight_snapshot_at,
        "failover_pool": failover_pool,
        "created_at": now,
        "last_checked_at": None,
        "last_checked_by": None,
        "last_progress_signature": None,
        "stuck_poll_count": 0,
        "terminal_status": None,
        "report_path": None,
        "cancelled": False,
    }
    save_watch_job(repo_root, job)
    return job


def _progress_signature(progress: dict[str, Any]) -> str:
    idx = progress.get("current_stage_index")
    summary = str(progress.get("summary", ""))
    return f"{idx}|{summary}"


def maybe_write_watch_report(run_root: Path, progress: dict[str, Any], terminal_status: str) -> Path:
    report_path = run_root / "watch_report.md"
    lines = [
        "# Pipeline watch report",
        "",
        f"- **Status:** {terminal_status}",
        f"- **Summary:** {progress.get('summary', '')}",
        f"- **Completed stages:** {progress.get('completed_count', 0)}/{progress.get('total_stages', 0)}",
        f"- **Review gate:** {progress.get('review_gate', False)}",
        f"- **Finished at:** {now_utc_iso()}",
        "",
    ]
    report_path.write_text("\n".join(lines), encoding="utf-8")
    done_path = run_root / ".watch_done"
    done_path.write_text(now_utc_iso(), encoding="utf-8")
    return report_path


def write_watch_alert(
    run_root: Path,
    *,
    job_id: str,
    reason: str,
    message: str,
    extra: dict[str, Any] | None = None,
) -> Path:
    alert_path = run_root / "watch_alert.json"
    payload: dict[str, Any] = {
        "job_id": job_id,
        "reason": reason,
        "message": message,
        "run_root": str(run_root),
        "alerted_at_utc": now_utc_iso(),
    }
    if extra:
        payload.update(extra)
    write_json(alert_path, payload)
    return alert_path


def cancel_pipeline_run(run_root: Path) -> dict[str, Any]:
    worker_lines = read_worker_log_lines(run_root)
    pid = parse_pid_from_worker_log(worker_lines)
    lock_path = run_root / "pipeline_worker.pid"
    if lock_path.exists():
        try:
            pid = int(lock_path.read_text(encoding="utf-8").strip())
        except ValueError:
            pass
    if pid and process_id_exists(pid):
        stop_process_tree_by_pid(pid)
        return {"ok": True, "pid": pid, "message": f"Cancellation requested for PID {pid}."}
    return {"ok": False, "pid": pid, "message": "No live pipeline worker PID found."}


def watch_status_update(
    repo_root: Path,
    job_id: str,
    *,
    checked_by: str = "unknown",
    stuck_threshold_polls: int = 6,
) -> dict[str, Any]:
    job = load_watch_job(repo_root, job_id)
    if job.get("cancelled"):
        return {"job": job, "terminal": True, "cancelled": True}

    run_root = Path(str(job["run_root"])).resolve()
    payload = build_progress_payload(run_root)
    progress = payload["progress"]
    lifecycle = payload["lifecycle"]
    lifecycle_status = str(lifecycle.get("lifecycle", "idle"))

    signature = _progress_signature(progress)
    prev_signature = job.get("last_progress_signature")
    if lifecycle.get("running") and signature == prev_signature:
        job["stuck_poll_count"] = int(job.get("stuck_poll_count", 0) or 0) + 1
    else:
        job["stuck_poll_count"] = 0
    job["last_progress_signature"] = signature
    job["last_checked_at"] = now_utc_iso()
    job["last_checked_by"] = checked_by

    until = {str(item) for item in job.get("until", TERMINAL_UNTIL_DEFAULT)}
    terminal = lifecycle_status in until
    stuck_suspected = (
        bool(lifecycle.get("running"))
        and int(job.get("stuck_poll_count", 0) or 0) >= stuck_threshold_polls
    )

    report_path: str | None = None
    if terminal and not job.get("terminal_status"):
        report = maybe_write_watch_report(run_root, progress, lifecycle_status)
        job["terminal_status"] = lifecycle_status
        job["report_path"] = str(report)
        report_path = str(report)

    save_watch_job(repo_root, job)

    return {
        "job_id": job_id,
        "terminal": terminal,
        "status": lifecycle_status,
        "summary": progress.get("summary", ""),
        "current_stage_index": progress.get("current_stage_index"),
        "heartbeat": progress.get("summary", ""),
        "stuck_suspected": stuck_suspected,
        "report_path": report_path or job.get("report_path"),
        "suggested_poll_seconds": int(job.get("poll_interval_seconds", 300)),
        "progress": progress,
        "job": job,
    }


def cancel_watch_job(repo_root: Path, job_id: str) -> dict[str, Any]:
    job = load_watch_job(repo_root, job_id)
    job["cancelled"] = True
    job["terminal_status"] = job.get("terminal_status") or "cancelled"
    save_watch_job(repo_root, job)
    return job


def check_stale_watchers(
    repo_root: Path,
    *,
    apply_alerts: bool = True,
    apply_cancel: bool = True,
) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    now = datetime.now(timezone.utc)
    for job in list_watch_jobs(repo_root, include_terminal=False):
        if job.get("terminal_status") or job.get("cancelled"):
            continue
        last = job.get("last_checked_at")
        poll = int(job.get("poll_interval_seconds", 300) or 300)
        if not last:
            continue
        try:
            last_dt = datetime.fromisoformat(str(last).replace("Z", "+00:00"))
        except ValueError:
            continue
        age = (now - last_dt).total_seconds()
        if age <= 2 * poll:
            continue

        run_root = Path(str(job["run_root"])).resolve()
        lifecycle = infer_run_lifecycle(run_root)
        if not lifecycle.get("running"):
            continue

        job_id = str(job["job_id"])
        alert = {
            "job_id": job_id,
            "reason": "watcher_stale",
            "age_seconds": age,
            "poll_interval_seconds": poll,
            "watcher": job.get("watcher"),
        }
        if apply_alerts:
            write_watch_alert(
                run_root,
                job_id=job_id,
                reason="watcher_stale",
                message=(
                    f"Watch job {job_id} stale for {int(age)}s "
                    f"(>{2 * poll}s). Last checker: {job.get('last_checked_by')}."
                ),
                extra=alert,
            )
        action = str(job.get("on_watcher_lost", "alert"))
        if apply_cancel and action == "cancel_run":
            cancel_pipeline_run(run_root)
        results.append(alert)
    return results


def openrouter_auto_topup_enabled() -> bool:
    raw = os.environ.get("THERIAC_OPENROUTER_AUTO_TOPUP", "1").strip().lower()
    return raw not in {"0", "false", "no", "off"}


def check_openrouter_health() -> dict[str, Any]:
    from pipeline.model_provider import _resolve_openrouter_api_key

    key = _resolve_openrouter_api_key()
    if not key:
        return {"openrouter_health": "missing_key", "informational_only": True}

    try:
        import urllib.error
        import urllib.request

        request = urllib.request.Request(
            "https://openrouter.ai/api/v1/auth/key",
            headers={"Authorization": f"Bearer {key}"},
            method="GET",
        )
        with urllib.request.urlopen(request, timeout=15) as response:
            body = json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        if exc.code in {401, 403}:
            return {"openrouter_health": "auth_failed", "informational_only": True}
        return {"openrouter_health": "error", "informational_only": True, "detail": str(exc)}
    except Exception as exc:
        return {"openrouter_health": "error", "informational_only": True, "detail": str(exc)}

    data = body.get("data") if isinstance(body, dict) else {}
    if not isinstance(data, dict):
        data = {}
    limit_remaining = data.get("limit_remaining")
    usage = data.get("usage")
    return {
        "openrouter_health": "ok",
        "informational_only": True,
        "limit_remaining": limit_remaining,
        "usage": usage,
        "auto_topup": openrouter_auto_topup_enabled(),
        "note": "Balance fields are informational only; auto-topup is enabled for this project.",
    }


def recommendation_from_antigravity_assessment(
    assessment: dict[str, Any],
    *,
    openrouter_health: str = "ok",
) -> dict[str, Any]:
    reasons: list[str] = []
    if openrouter_health in {"missing_key", "auth_failed"}:
        return {
            "recommendation": "quota_unknown",
            "reasons": [f"OpenRouter configuration blocker: {openrouter_health}"],
            "openrouter_blocker": openrouter_health,
        }

    gemini = assessment.get("gemini_bars_filled")
    gpt_pool = assessment.get("gpt_pool_bars_filled")
    if gemini is None:
        return {
            "recommendation": "quota_unknown",
            "reasons": ["Antigravity quota assessment missing gemini_bars_filled."],
        }

    try:
        gemini_i = int(gemini)
    except (TypeError, ValueError):
        return {
            "recommendation": "quota_unknown",
            "reasons": ["Invalid gemini_bars_filled value."],
        }

    gpt_i: int | None = None
    if gpt_pool is not None:
        try:
            gpt_i = int(gpt_pool)
        except (TypeError, ValueError):
            gpt_i = None

    if gemini_i >= 3:
        rec = "run_pipeline_and_flash_watch"
        reasons.append("Gemini pool has 3/3 bars available for Flash watch.")
    elif gemini_i == 2:
        rec = "run_pipeline_sentinel_only"
        reasons.append("Gemini pool cautious (2/3); prefer sentinel with optional short-interval Flash.")
    elif gemini_i <= 1 and gpt_i is not None and gpt_i >= 2:
        rec = "failover_to_gpt_pool_watch"
        reasons.append("Gemini pool low; second Antigravity pool has capacity for watch failover.")
    elif gemini_i <= 1:
        rec = "wait_for_gemini_reset"
        reasons.append("Gemini pool low (0-1/3); defer Flash watch or use sentinel-only.")
    else:
        rec = "quota_unknown"
        reasons.append("Could not classify Gemini pool level.")

    return {
        "recommendation": rec,
        "reasons": reasons,
        "gemini_bars_filled": gemini_i,
        "gpt_pool_bars_filled": gpt_i,
    }


def quota_preflight(
    repo_root: Path,
    *,
    run_capture: bool = False,
    antigravity_assessment: dict[str, Any] | None = None,
    check_openrouter: bool = True,
) -> dict[str, Any]:
    capture: dict[str, Any] = {"ok": False}
    if run_capture:
        from pipeline.quota_capture import run_quota_capture
        from pipeline.quota_worker import quota_worker_enabled, run_quota_capture_via_worker

        if quota_worker_enabled(None):
            capture = run_quota_capture_via_worker(repo_root, auto_navigate=True)
        else:
            capture = run_quota_capture(repo_root, auto_navigate=True)

    snap_dir = quota_snapshots_dir(repo_root)
    meta_path = snap_dir / "latest.meta.json"
    meta: dict[str, Any] = {}
    if meta_path.exists():
        try:
            loaded = read_json(meta_path)
            if isinstance(loaded, dict):
                meta = loaded
        except Exception:
            meta = {}

    or_health = check_openrouter_health() if check_openrouter else {"openrouter_health": "skipped"}
    or_status = str(or_health.get("openrouter_health", "ok"))

    recommendation_payload: dict[str, Any]
    if antigravity_assessment:
        recommendation_payload = recommendation_from_antigravity_assessment(
            antigravity_assessment,
            openrouter_health=or_status,
        )
    else:
        recommendation_payload = {
            "recommendation": "quota_unknown",
            "reasons": [
                "No antigravity_assessment provided. Run check_quota.py, read latest.png, "
                "then call preflight with gemini_bars_filled / gpt_pool_bars_filled."
            ],
        }

    return {
        "antigravity_capture": {
            **capture,
            "snapshot_dir": str(snap_dir),
            "image_path": str(snap_dir / "latest.png") if (snap_dir / "latest.png").exists() else None,
            "meta": meta,
        },
        "openrouter": or_health,
        **recommendation_payload,
    }
