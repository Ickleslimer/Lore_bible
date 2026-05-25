#!/usr/bin/env python3
"""
VirtualBox VM quota session (Windows Home friendly).

Host (Cursor):  python scripts/quota_vm_session.py capture
Guest (VM):     python scripts/quota_worker.py --loop  (see guest startup script)

Requires one-time VM setup: docs/antigravity/quota-vm-session.md
"""

from __future__ import annotations

import argparse
import os
import shutil
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


def _vm_name() -> str:
    name = os.environ.get("THERIAC_QUOTA_VM_NAME", "").strip()
    if not name:
        raise ValueError("Set THERIAC_QUOTA_VM_NAME to your VirtualBox VM name.")
    return name


def _find_vboxmanage() -> str:
    override = os.environ.get("THERIAC_VBOX_MANAGE", "").strip()
    if override and Path(override).exists():
        return override
    found = shutil.which("VBoxManage")
    if found:
        return found
    for candidate in (
        Path(r"C:\Program Files\Oracle\VirtualBox\VBoxManage.exe"),
        Path(r"C:\Program Files (x86)\Oracle\VirtualBox\VBoxManage.exe"),
    ):
        if candidate.exists():
            return str(candidate)
    raise FileNotFoundError(
        "VBoxManage not found. Install VirtualBox or set THERIAC_VBOX_MANAGE to VBoxManage.exe."
    )


def _vbox_run(*args: str) -> subprocess.CompletedProcess[str]:
    cmd = [_find_vboxmanage(), *args]
    return subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
        check=False,
    )


def vm_running(vm: str) -> bool:
    result = _vbox_run("list", "runningvms")
    if result.returncode != 0:
        return False
    return f'"{vm}"' in result.stdout or vm in result.stdout


def cmd_vm_start(*, headless: bool) -> int:
    vm = _vm_name()
    if vm_running(vm):
        print(f"VM already running: {vm}", flush=True)
        return 0
    mode = "headless" if headless else "gui"
    print(f"Starting VM {vm} ({mode})...", flush=True)
    result = _vbox_run("startvm", vm, "--type", mode)
    if result.returncode != 0:
        print(result.stderr or result.stdout, file=sys.stderr)
        return result.returncode
    return 0


def cmd_vm_stop(*, save_state: bool) -> int:
    vm = _vm_name()
    if not vm_running(vm):
        print(f"VM not running: {vm}", flush=True)
        return 0
    action = "savestate" if save_state else "acpipowerbutton"
    print(f"Stopping VM {vm} ({action})...", flush=True)
    result = _vbox_run("controlvm", vm, action)
    if result.returncode != 0:
        print(result.stderr or result.stdout, file=sys.stderr)
        return result.returncode
    return 0


def wait_worker_ready(repo_root: Path, timeout_seconds: float) -> bool:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        if is_worker_ready(repo_root):
            return True
        time.sleep(1.0)
    return False


def cmd_status(repo_root: Path) -> int:
    vm = os.environ.get("THERIAC_QUOTA_VM_NAME", "").strip() or "(unset)"
    running = False
    if os.environ.get("THERIAC_QUOTA_VM_NAME", "").strip():
        try:
            running = vm_running(_vm_name())
        except (ValueError, FileNotFoundError) as exc:
            print(f"vm_check_error={exc}")
    guest_root = os.environ.get("THERIAC_QUOTA_VM_REPO_ROOT", "").strip() or "(same as host)"
    print(f"vm_name={vm} vm_running={running} worker_ready={is_worker_ready(repo_root)}")
    print(f"host_repo={repo_root}")
    print(f"guest_repo={guest_root}")
    return 0


def cmd_capture(
    repo_root: Path,
    *,
    start_vm: bool,
    headless: bool,
    stop_after: bool,
    wait_ready_seconds: float,
) -> int:
    if not is_worker_ready(repo_root):
        if start_vm:
            code = cmd_vm_start(headless=headless)
            if code != 0:
                return code
        if not wait_worker_ready(repo_root, wait_ready_seconds):
            print(
                "Worker not ready. Inside the VM run:\n"
                f"  python scripts\\quota_worker.py --loop --repo-root <guest-repo-path>\n"
                "Or install guest startup: scripts\\install_quota_vm_guest_startup.ps1",
                file=sys.stderr,
            )
            return 1

    result = run_quota_capture_via_worker(repo_root, auto_navigate=True)
    if result.get("ok"):
        print(result.get("image_path", ""))
        if stop_after:
            repo_root  # host paths for shutdown file
            request_worker_shutdown(repo_root)
            time.sleep(3.0)
            return cmd_vm_stop(save_state=True)
        return 0
    print(result.get("error", "capture failed"), file=sys.stderr)
    return 1


def cmd_stop(repo_root: Path) -> int:
    request_worker_shutdown(repo_root)
    time.sleep(2.0)
    return cmd_vm_stop(save_state=True)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="VirtualBox VM quota capture (host).")
    parser.add_argument("--repo-root", type=Path, default=None)
    parser.add_argument("--wait-ready", type=float, default=180.0)
    sub = parser.add_subparsers(dest="command", required=True)

    start_p = sub.add_parser("start", help="Start the VirtualBox VM.")
    start_p.add_argument("--gui", action="store_true", help="Show VM window (default: headless).")

    sub.add_parser("status", help="VM running state + worker.ready.json.")
    sub.add_parser("stop", help="Signal worker shutdown and stop/save VM.")

    cap_p = sub.add_parser("capture", help="Capture via VM worker (optional VM start).")
    cap_p.add_argument("--no-start-vm", action="store_true", help="Do not start VM if worker not ready.")
    cap_p.add_argument("--gui", action="store_true", help="Start VM with window visible.")
    cap_p.add_argument("--stop-after", action="store_true", help="Stop VM after capture.")

    sub.add_parser(
        "print-guest-setup",
        help="Print guest install commands (Python path, repo mount).",
    )

    args = parser.parse_args(argv)
    try:
        repo_root = resolve_repo_root(args.repo_root)
    except Exception as exc:
        print(exc, file=sys.stderr)
        return 2

    if args.command == "start":
        try:
            return cmd_vm_start(headless=not args.gui)
        except FileNotFoundError as exc:
            print(exc, file=sys.stderr)
            return 2
    if args.command == "status":
        return cmd_status(repo_root)
    if args.command == "stop":
        try:
            return cmd_stop(repo_root)
        except (ValueError, FileNotFoundError) as exc:
            print(exc, file=sys.stderr)
            return 2
    if args.command == "capture":
        try:
            return cmd_capture(
                repo_root,
                start_vm=not args.no_start_vm,
                headless=not args.gui,
                stop_after=args.stop_after,
                wait_ready_seconds=args.wait_ready,
            )
        except (ValueError, FileNotFoundError) as exc:
            print(exc, file=sys.stderr)
            return 2
    if args.command == "print-guest-setup":
        _print_guest_setup(repo_root)
        return 0
    return 2


def _print_guest_setup(repo_root: Path) -> None:
    guest = os.environ.get("THERIAC_QUOTA_VM_REPO_ROOT", "Z:\\Lore_bible")
    py = os.environ.get("THERIAC_QUOTA_PYTHON", r"C:\Python312\python.exe")
    print("=== Guest VM (run inside the VM) ===")
    print(f"1. Mount shared folder to: {guest}")
    print(f"2. Install Python deps: pip install -r \"{guest}\\requirements.txt\"")
    print(f"3. Install Antigravity; open project at {guest}")
    print("4. Start worker:")
    print(f'   "{py}" "{guest}\\scripts\\quota_worker.py" --loop --repo-root "{guest}"')
    print("5. Optional auto-start at logon (inside VM):")
    print(f'   powershell -File "{guest}\\scripts\\install_quota_vm_guest_startup.ps1"')
    print("")
    print("=== Host (Cursor) ===")
    print(f"set THERIAC_QUOTA_VM_NAME=YourVmName")
    print(f"set THERIAC_QUOTA_VM_REPO_ROOT={guest}")
    print(f'python scripts\\quota_vm_session.py capture')


if __name__ == "__main__":
    raise SystemExit(main())
