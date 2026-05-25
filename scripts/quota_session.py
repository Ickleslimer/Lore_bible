#!/usr/bin/env python3
"""
Start/stop an on-demand RDP quota session (Windows Pro+).

Session A (Cursor) runs:  python scripts/quota_session.py capture
Session B is opened via RDP as THERIAC_QUOTA_RDP_USER; quota_worker.py --loop starts on login.

Requires one-time setup: dedicated Windows user, RDP enabled, Antigravity installed for that user.
See docs/antigravity/quota-rdp-session.md
"""

from __future__ import annotations

import argparse
import os
import re
import subprocess
import sys
import time
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from pipeline.pipeline_watch import resolve_repo_root
from pipeline.quota_worker import (
    is_worker_ready,
    request_worker_shutdown,
    run_quota_capture_via_worker,
)


def _quota_rdp_user() -> str:
    user = os.environ.get("THERIAC_QUOTA_RDP_USER", "").strip()
    if not user:
        raise ValueError(
            "Set THERIAC_QUOTA_RDP_USER to the dedicated quota Windows account name."
        )
    return user


def _quota_rdp_host() -> str:
    return os.environ.get("THERIAC_QUOTA_RDP_HOST", "localhost").strip() or "localhost"


def _python_exe() -> str:
    return os.environ.get("THERIAC_QUOTA_PYTHON", sys.executable)


def _find_session_id(username: str) -> int | None:
    if os.name != "nt":
        return None
    result = subprocess.run(
        ["query", "user"],
        capture_output=True,
        text=True,
        creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
        check=False,
    )
    if result.returncode != 0:
        return None
    name = username.split("\\")[-1].lower()
    for line in result.stdout.splitlines():
        if name not in line.lower():
            continue
        match = re.search(r"\s+(\d+)\s+(Active|Disc|Disconnect)", line)
        if match:
            return int(match.group(1))
    return None


def _store_rdp_credential(host: str, user: str, password: str) -> None:
    target = f"TERMSRV/{host}"
    subprocess.run(
        ["cmdkey", f"/generic:{target}", f"/user:{user}", f"/pass:{password}"],
        check=False,
        creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
    )


def _write_rdp_file(repo_root: Path, rdp_path: Path, host: str, user: str) -> None:
    worker_cmd = (
        f'"{_python_exe()}" "{repo_root / "scripts" / "quota_worker.py"}" --loop'
    )
    lines = [
        f"full address:s:{host}",
        f"username:s:{user}",
        f"alternate shell:s:{worker_cmd}",
        f"shell working directory:s:{repo_root}",
        "screen mode id:i:2",
        "desktopwidth:i:1280",
        "desktopheight:i:800",
        "redirectclipboard:i:0",
        "audiomode:i:2",
        "compression:i:1",
    ]
    rdp_path.parent.mkdir(parents=True, exist_ok=True)
    rdp_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def cmd_start(repo_root: Path, *, wait_ready_seconds: float) -> int:
    user = _quota_rdp_user()
    host = _quota_rdp_host()
    if is_worker_ready(repo_root):
        print(f"Worker already ready (session user {user}).")
        return 0

    password = os.environ.get("THERIAC_QUOTA_RDP_PASSWORD", "")
    if password:
        _store_rdp_credential(host, user, password)

    rdp_path = repo_root / "artifacts" / "quota_snapshots" / "worker" / "quota_worker.rdp"
    _write_rdp_file(repo_root, rdp_path, host, user)

    print(f"Opening RDP to {host} as {user} (minimized). Worker starts on login.", flush=True)
    subprocess.Popen(
        ["mstsc.exe", str(rdp_path)],
        creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
    )

    deadline = time.monotonic() + wait_ready_seconds
    while time.monotonic() < deadline:
        if is_worker_ready(repo_root):
            from pipeline.quota_worker import worker_ready_payload

            ready = worker_ready_payload(repo_root) or {}
            print(f"Worker ready (pid={ready.get('pid', '?')}).", flush=True)
            return 0
        time.sleep(1.0)

    print(
        "Timed out waiting for worker.ready.json. "
        "Complete RDP login in the quota user session if prompted.",
        file=sys.stderr,
        flush=True,
    )
    return 1


def cmd_stop(repo_root: Path, *, wait_seconds: float) -> int:
    user = _quota_rdp_user()
    if is_worker_ready(repo_root):
        print("Requesting worker shutdown...", flush=True)
        request_worker_shutdown(repo_root)
        deadline = time.monotonic() + wait_seconds
        while time.monotonic() < deadline and is_worker_ready(repo_root):
            time.sleep(0.5)

    session_id = _find_session_id(user)
    if session_id is not None:
        print(f"Logging off RDP session {session_id} ({user})...", flush=True)
        subprocess.run(
            ["logoff", str(session_id)],
            check=False,
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
        )
        return 0

    print(f"No active session found for {user}.", flush=True)
    return 0


def cmd_status(repo_root: Path) -> int:
    user = _quota_rdp_user()
    session_id = _find_session_id(user)
    ready = is_worker_ready(repo_root)
    print(f"user={user} session_id={session_id} worker_ready={ready}")
    if ready:
        from pipeline.quota_worker import worker_ready_payload

        print(worker_ready_payload(repo_root))
    return 0


def cmd_capture(repo_root: Path, *, stop_after: bool, wait_ready_seconds: float) -> int:
    if not is_worker_ready(repo_root):
        code = cmd_start(repo_root, wait_ready_seconds=wait_ready_seconds)
        if code != 0:
            return code

    result = run_quota_capture_via_worker(repo_root, auto_navigate=True)
    if result.get("ok"):
        print(result.get("image_path", ""))
        if stop_after:
            return cmd_stop(repo_root, wait_seconds=30.0)
        return 0
    print(result.get("error", "capture failed"), file=sys.stderr)
    if stop_after:
        cmd_stop(repo_root, wait_seconds=30.0)
    return 1


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="On-demand RDP quota session (Windows).")
    parser.add_argument("--repo-root", type=Path, default=None)
    parser.add_argument(
        "--wait-ready",
        type=float,
        default=120.0,
        help="Seconds to wait for worker.ready.json after start (default 120).",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("start", help="Open RDP; worker loop starts via alternate shell.")
    sub.add_parser("status", help="Show session id and worker.ready state.")
    stop_p = sub.add_parser("stop", help="Signal worker shutdown and log off RDP session.")
    stop_p.add_argument("--wait", type=float, default=30.0, help="Wait for worker exit (default 30).")

    cap_p = sub.add_parser("capture", help="Ensure session up, run check_quota via worker.")
    cap_p.add_argument(
        "--stop-after",
        action="store_true",
        help="Stop RDP session after capture completes.",
    )

    args = parser.parse_args(argv)
    try:
        repo_root = resolve_repo_root(args.repo_root)
    except Exception as exc:
        print(exc, file=sys.stderr)
        return 2

    if os.name != "nt":
        print("quota_session.py requires Windows.", file=sys.stderr)
        return 2

    if args.command == "start":
        return cmd_start(repo_root, wait_ready_seconds=args.wait_ready)
    if args.command == "stop":
        return cmd_stop(repo_root, wait_seconds=args.wait)
    if args.command == "status":
        return cmd_status(repo_root)
    if args.command == "capture":
        return cmd_capture(
            repo_root,
            stop_after=args.stop_after,
            wait_ready_seconds=args.wait_ready,
        )
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
