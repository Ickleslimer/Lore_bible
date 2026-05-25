from __future__ import annotations

import os
import time
import uuid
from pathlib import Path
from typing import Any

from pipeline.common import now_utc_iso, read_json, write_json
from pipeline.quota_capture import quota_auto_navigate_enabled, run_quota_capture

CAPTURE_REQUEST_NAME = "capture.request.json"
CAPTURE_RESPONSE_NAME = "capture.response.json"
CAPTURE_LOCK_NAME = "capture.lock.json"
CAPTURE_REQUEST_TMP = "capture.request.json.tmp"
WORKER_READY_NAME = "worker.ready.json"
SHUTDOWN_REQUEST_NAME = "shutdown.request.json"


def quota_worker_dir(repo_root: Path) -> Path:
    path = repo_root / "artifacts" / "quota_snapshots" / "worker"
    path.mkdir(parents=True, exist_ok=True)
    return path


def resolve_capture_repo_root(host_repo_root: Path) -> Path:
    """
    Repo path the VM/guest worker uses for UI capture (run_quota_capture).
    Protocol files (request/response) still live under host_repo_root on shared storage.
    Set THERIAC_QUOTA_VM_REPO_ROOT when the guest drive letter differs (e.g. Z:\\Lore_bible).
    """
    guest = os.environ.get("THERIAC_QUOTA_VM_REPO_ROOT", "").strip()
    if guest:
        return Path(guest)
    return host_repo_root


def quota_worker_enabled(explicit: bool | None = None) -> bool:
    if explicit is not None:
        return explicit
    raw = os.environ.get("THERIAC_QUOTA_WORKER", "").strip().lower()
    return raw in ("1", "true", "yes", "on")


def _paths(repo_root: Path) -> dict[str, Path]:
    base = quota_worker_dir(repo_root)
    return {
        "request": base / CAPTURE_REQUEST_NAME,
        "request_tmp": base / CAPTURE_REQUEST_TMP,
        "response": base / CAPTURE_RESPONSE_NAME,
        "lock": base / CAPTURE_LOCK_NAME,
        "ready": base / WORKER_READY_NAME,
        "shutdown": base / SHUTDOWN_REQUEST_NAME,
        "rdp": base / "quota_worker.rdp",
    }


def _read_json_if_exists(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        payload = read_json(path)
    except Exception:
        return None
    return payload if isinstance(payload, dict) else None


def _atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    write_json(tmp, payload)
    tmp.replace(path)


def build_capture_request(
    repo_root: Path,
    *,
    auto_navigate: bool | None = None,
    request_id: str | None = None,
) -> dict[str, Any]:
    return {
        "request_id": request_id or str(uuid.uuid4()),
        "requested_at_utc": now_utc_iso(),
        "repo_root": str(repo_root.resolve()),
        "auto_navigate": quota_auto_navigate_enabled(auto_navigate),
    }


def submit_capture_request(repo_root: Path, request: dict[str, Any]) -> Path:
    paths = _paths(repo_root)
    _atomic_write_json(paths["request"], request)
    return paths["request"]


def acquire_worker_lock(repo_root: Path, *, worker_pid: int | None = None) -> bool:
    paths = _paths(repo_root)
    lock_path = paths["lock"]
    if lock_path.exists() and paths["request"].exists():
        existing = _read_json_if_exists(lock_path)
        if existing and int(existing.get("worker_pid", -1)) != int(worker_pid or os.getpid()):
            return False
    if lock_path.exists() and not paths["request"].exists():
        try:
            lock_path.unlink(missing_ok=True)
        except OSError:
            pass
    payload = {
        "worker_pid": worker_pid or os.getpid(),
        "started_at_utc": now_utc_iso(),
    }
    _atomic_write_json(lock_path, payload)
    return True


def write_worker_ready(repo_root: Path, *, extra: dict[str, Any] | None = None) -> None:
    payload: dict[str, Any] = {
        "pid": os.getpid(),
        "started_at_utc": now_utc_iso(),
        "repo_root": str(repo_root.resolve()),
    }
    if extra:
        payload.update(extra)
    _atomic_write_json(_paths(repo_root)["ready"], payload)


def clear_worker_ready(repo_root: Path) -> None:
    try:
        _paths(repo_root)["ready"].unlink(missing_ok=True)
    except OSError:
        pass


def worker_ready_payload(repo_root: Path) -> dict[str, Any] | None:
    return _read_json_if_exists(_paths(repo_root)["ready"])


def is_worker_ready(repo_root: Path) -> bool:
    return worker_ready_payload(repo_root) is not None


def request_worker_shutdown(repo_root: Path) -> Path:
    payload = {"requested_at_utc": now_utc_iso()}
    path = _paths(repo_root)["shutdown"]
    _atomic_write_json(path, payload)
    return path


def shutdown_requested(repo_root: Path) -> bool:
    return _paths(repo_root)["shutdown"].exists()


def clear_shutdown_request(repo_root: Path) -> None:
    try:
        _paths(repo_root)["shutdown"].unlink(missing_ok=True)
    except OSError:
        pass


def release_worker_lock(repo_root: Path) -> None:
    try:
        _paths(repo_root)["lock"].unlink(missing_ok=True)
    except OSError:
        pass


def process_capture_request(repo_root: Path) -> dict[str, Any]:
    """Session B: run one capture if capture.request.json is present."""
    paths = _paths(repo_root)
    request = _read_json_if_exists(paths["request"])
    if request is None:
        return {"processed": False, "reason": "no_request"}

    request_id = str(request.get("request_id", ""))
    req_repo = Path(str(request.get("repo_root", repo_root)))
    auto_navigate = request.get("auto_navigate")
    auto_flag: bool | None
    if isinstance(auto_navigate, bool):
        auto_flag = auto_navigate
    else:
        auto_flag = None

    if not acquire_worker_lock(req_repo, worker_pid=os.getpid()):
        return {"processed": False, "reason": "lock_held", "request_id": request_id}

    try:
        capture = run_quota_capture(req_repo, auto_navigate=auto_flag)
        response = {
            "request_id": request_id,
            "completed_at_utc": now_utc_iso(),
            "ok": bool(capture.get("ok")),
            "capture": capture,
            "error": capture.get("error"),
        }
        _atomic_write_json(paths["response"], response)
        try:
            paths["request"].unlink(missing_ok=True)
        except OSError:
            pass
        return {"processed": True, "request_id": request_id, "ok": response["ok"]}
    finally:
        release_worker_lock(req_repo)


def wait_for_capture_response(
    repo_root: Path,
    request_id: str,
    *,
    timeout_seconds: float = 120.0,
    poll_interval_seconds: float = 0.5,
) -> dict[str, Any]:
    """Session A: poll until capture.response.json matches request_id or timeout."""
    paths = _paths(repo_root)
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        response = _read_json_if_exists(paths["response"])
        if response and str(response.get("request_id", "")) == request_id:
            return response
        if not paths["request"].exists():
            response = _read_json_if_exists(paths["response"])
            if response and str(response.get("request_id", "")) == request_id:
                return response
        time.sleep(poll_interval_seconds)
    return {
        "request_id": request_id,
        "ok": False,
        "error": (
            f"Quota worker did not respond within {timeout_seconds:.0f}s. "
            "Start session B: theriac-pipeline-ops/scripts/quota_worker.py --loop (see docs/antigravity/ops-repo.md)."
        ),
        "capture": None,
    }


def run_quota_capture_via_worker(
    repo_root: Path,
    *,
    auto_navigate: bool | None = None,
    timeout_seconds: float | None = None,
) -> dict[str, Any]:
    """Drop capture.request.json and wait for worker in another session."""
    host_root = repo_root.resolve()
    capture_root = resolve_capture_repo_root(host_root)
    timeout = timeout_seconds
    if timeout is None:
        timeout = float(os.environ.get("THERIAC_QUOTA_WORKER_TIMEOUT", "120"))
    request = build_capture_request(capture_root, auto_navigate=auto_navigate)
    request_id = str(request["request_id"])

    # Clear stale response from a prior run.
    paths = _paths(host_root)
    stale = _read_json_if_exists(paths["response"])
    if stale and str(stale.get("request_id", "")) != request_id:
        try:
            paths["response"].unlink(missing_ok=True)
        except OSError:
            pass

    submit_capture_request(host_root, request)
    response = wait_for_capture_response(
        host_root,
        request_id,
        timeout_seconds=timeout,
    )
    if not response.get("ok"):
        return {
            "ok": False,
            "error": response.get("error") or "Worker capture failed.",
            "meta_path": str(host_root / "artifacts" / "quota_snapshots" / "latest.meta.json"),
            "worker": {"request_id": request_id, "response": response},
        }
    capture = response.get("capture")
    if isinstance(capture, dict):
        capture = {
            **capture,
            "worker": {
                "request_id": request_id,
                "mode": "shared_folder",
                "host_repo_root": str(host_root),
                "capture_repo_root": str(capture_root),
            },
        }
        return capture
    return {
        "ok": False,
        "error": "Worker response missing capture payload.",
        "worker": {"request_id": request_id, "response": response},
    }
