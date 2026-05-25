from __future__ import annotations

import argparse
import os
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

from pipeline.common import now_utc_iso, write_json

# Relative click targets within the Antigravity main window (Agent Manager UI).
_SETTINGS_GEAR = (0.06, 0.965)
_MODELS_TAB = (0.215, 0.268)


def quota_snapshots_dir(repo_root: Path) -> Path:
    path = repo_root / "artifacts" / "quota_snapshots"
    path.mkdir(parents=True, exist_ok=True)
    return path


def _is_antigravity_window_title(title: str) -> bool:
    lowered = (title or "").strip().lower()
    if not lowered or "antigravity" not in lowered:
        return False
    # Avoid false positives such as Cursor chats that mention Antigravity in the tab title.
    blocked = ("cursor", "visual studio code", "vscode", "codex", "windsurf")
    return not any(token in lowered for token in blocked)


def _antigravity_main_window_titles() -> list[str]:
    """Windows: titles from Antigravity.exe processes (e.g. 'Exploring …')."""
    if os.name != "nt":
        return []
    script = (
        "Get-Process -Name Antigravity -ErrorAction SilentlyContinue | "
        "Where-Object { $_.MainWindowTitle } | "
        "ForEach-Object { $_.MainWindowTitle }"
    )
    result = subprocess.run(
        ["powershell", "-NoProfile", "-Command", script],
        capture_output=True,
        text=True,
        creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
        check=False,
    )
    return [line.strip() for line in result.stdout.splitlines() if line.strip()]


def _find_antigravity_window() -> Any | None:
    try:
        import pygetwindow as gw
    except ImportError:
        return None

    for title in _antigravity_main_window_titles():
        try:
            matches = gw.getWindowsWithTitle(title)
        except Exception:
            matches = []
        for window in matches:
            if int(window.width or 0) > 200 and int(window.height or 0) > 200:
                return window

    candidates: list[Any] = []
    for window in gw.getAllWindows():
        title = window.title or ""
        if not _is_antigravity_window_title(title):
            continue
        if int(window.width or 0) <= 200 or int(window.height or 0) <= 200:
            continue
        candidates.append(window)
    if not candidates:
        return None
    candidates.sort(
        key=lambda w: (
            0 if (w.title or "").lower().startswith("antigravity") else 1,
            -(int(w.width or 0) * int(w.height or 0)),
        ),
    )
    return candidates[0]


def quota_auto_navigate_enabled(explicit: bool | None = None) -> bool:
    if explicit is not None:
        return explicit
    raw = os.environ.get("THERIAC_QUOTA_AUTO_NAVIGATE", "1").strip().lower()
    return raw in ("1", "true", "yes", "on")


def _window_rel_click(window: Any, fx: float, fy: float) -> tuple[int, int]:
    left = int(window.left)
    top = int(window.top)
    width = int(window.width)
    height = int(window.height)
    return (left + int(width * fx), top + int(height * fy))


def navigate_to_model_quota(window: Any) -> dict[str, Any]:
    """Open Antigravity Settings → Models (quota bars). Windows UI automation only."""
    try:
        import pyautogui
    except ImportError:
        return {
            "ok": False,
            "method": None,
            "error": "pyautogui is required for auto-navigation (pip install pyautogui).",
        }

    pyautogui.FAILSAFE = True
    pyautogui.PAUSE = 0.12

    try:
        window.activate()
    except Exception:
        pass
    time.sleep(0.75)

    # Dismiss command palette / stray overlays without closing the whole app.
    pyautogui.press("escape")
    time.sleep(0.2)

    settings_xy = _window_rel_click(window, *_SETTINGS_GEAR)
    models_xy = _window_rel_click(window, *_MODELS_TAB)
    pyautogui.click(*settings_xy)
    time.sleep(0.9)
    pyautogui.click(*models_xy)
    time.sleep(0.85)

    return {
        "ok": True,
        "method": "settings_gear_then_models",
        "clicks": [
            {"target": "settings_gear", "x": settings_xy[0], "y": settings_xy[1]},
            {"target": "models_tab", "x": models_xy[0], "y": models_xy[1]},
        ],
        "error": None,
    }


def _capture_window_png(window: Any, out_path: Path) -> None:
    import mss
    from PIL import Image

    left = int(window.left)
    top = int(window.top)
    width = int(window.width)
    height = int(window.height)
    if width <= 0 or height <= 0:
        raise RuntimeError("Antigravity window has invalid dimensions.")
    with mss.mss() as sct:
        shot = sct.grab({"left": left, "top": top, "width": width, "height": height})
        image = Image.frombytes("RGB", shot.size, shot.bgra, "raw", "BGRX")
        out_path.parent.mkdir(parents=True, exist_ok=True)
        image.save(out_path, format="PNG")


def run_quota_capture(
    repo_root: Path,
    *,
    auto_navigate: bool | None = None,
) -> dict[str, Any]:
    snap_dir = quota_snapshots_dir(repo_root)
    out_path = snap_dir / "latest.png"
    meta_path = snap_dir / "latest.meta.json"
    captured_at = now_utc_iso()
    do_navigate = quota_auto_navigate_enabled(auto_navigate)
    navigation: dict[str, Any] | None = None

    window = _find_antigravity_window()
    if window is None:
        meta = {
            "captured_at_utc": captured_at,
            "ok": False,
            "auto_navigate": do_navigate,
            "navigation": None,
            "error": (
                "Antigravity window not found. Open Antigravity, then re-run capture "
                "(use --auto-navigate or THERIAC_QUOTA_AUTO_NAVIGATE=1 for hands-free navigation)."
            ),
        }
        write_json(meta_path, meta)
        return {"ok": False, "error": meta["error"], "meta_path": str(meta_path)}

    if do_navigate and os.name == "nt":
        navigation = navigate_to_model_quota(window)
        if not navigation.get("ok"):
            meta = {
                "captured_at_utc": captured_at,
                "ok": False,
                "auto_navigate": True,
                "navigation": navigation,
                "error": navigation.get("error") or "Auto-navigation failed.",
            }
            write_json(meta_path, meta)
            return {"ok": False, "error": meta["error"], "meta_path": str(meta_path)}
    elif do_navigate:
        navigation = {
            "ok": False,
            "method": None,
            "error": "Auto-navigation is only supported on Windows.",
        }

    try:
        window.activate()
    except Exception:
        pass
    time.sleep(0.35)

    try:
        _capture_window_png(window, out_path)
    except Exception as exc:
        meta = {
            "captured_at_utc": captured_at,
            "ok": False,
            "error": f"Capture failed: {exc}",
        }
        write_json(meta_path, meta)
        return {"ok": False, "error": meta["error"], "meta_path": str(meta_path)}

    meta = {
        "captured_at_utc": captured_at,
        "ok": True,
        "error": None,
        "auto_navigate": do_navigate,
        "navigation": navigation,
        "window_title": getattr(window, "title", ""),
        "image_path": str(out_path),
    }
    write_json(meta_path, meta)
    return {"ok": True, "image_path": str(out_path), "meta_path": str(meta_path), "meta": meta}


def main(argv: list[str] | None = None) -> int:
    from pipeline.pipeline_watch import resolve_repo_root
    from pipeline.quota_worker import quota_worker_enabled, run_quota_capture_via_worker

    parser = argparse.ArgumentParser(
        description=(
            "Capture Antigravity model quota (Settings → Models). "
            "On Windows, auto-navigation is on by default (steals focus). "
            "Use --worker when a quota_worker.py loop runs in another session (shared folder)."
        ),
    )
    parser.add_argument("--repo-root", type=Path, default=None)
    nav = parser.add_mutually_exclusive_group()
    nav.add_argument(
        "--auto-navigate",
        action="store_true",
        default=None,
        help="Click Settings → Models before capture (default: env THERIAC_QUOTA_AUTO_NAVIGATE or on).",
    )
    nav.add_argument(
        "--no-auto-navigate",
        action="store_true",
        help="Skip UI automation; Model Quota / Models panel must already be visible.",
    )
    worker = parser.add_mutually_exclusive_group()
    worker.add_argument(
        "--worker",
        action="store_true",
        default=None,
        help="Drop capture.request.json and wait for theriac-pipeline-ops/scripts/quota_worker.py in session B.",
    )
    worker.add_argument(
        "--no-worker",
        action="store_true",
        help="Capture in this process (default unless THERIAC_QUOTA_WORKER=1).",
    )
    args = parser.parse_args(argv)
    repo_root = resolve_repo_root(args.repo_root)
    auto_navigate: bool | None = False if args.no_auto_navigate else args.auto_navigate
    use_worker = False if args.no_worker else (True if args.worker else quota_worker_enabled(None))
    if use_worker:
        result = run_quota_capture_via_worker(repo_root, auto_navigate=auto_navigate)
    else:
        result = run_quota_capture(repo_root, auto_navigate=auto_navigate)
    if result.get("ok"):
        print(result.get("image_path", ""))
        return 0
    print(result.get("error", "capture failed"), file=sys.stderr)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
