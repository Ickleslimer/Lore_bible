from __future__ import annotations

import argparse
import os
import socket
import sys
import threading
import webbrowser
from pathlib import Path


def looks_like_project_root(path: Path) -> bool:
    return (
        (path / "config" / "pipeline_config.json").exists()
        and (path / "theriac-coda---lore-bible.docx").exists()
    )


def candidate_project_roots() -> list[Path]:
    starts = [Path.cwd()]
    if getattr(sys, "frozen", False):
        starts.append(Path(sys.executable).resolve().parent)
    else:
        starts.append(Path(__file__).resolve().parent)
    roots: list[Path] = []
    for start in starts:
        for candidate in (start, *start.parents):
            if candidate not in roots:
                roots.append(candidate)
    return roots


def find_project_root(explicit_root: Path | None = None) -> Path:
    if explicit_root is not None:
        return explicit_root.resolve()
    env_root = os.environ.get("THERIAC_LORE_ROOT")
    if env_root:
        return Path(env_root).resolve()
    for candidate in candidate_project_roots():
        if looks_like_project_root(candidate):
            return candidate
    return Path.cwd().resolve()


def port_is_available(host: str, port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.settimeout(0.2)
        return sock.connect_ex((host, port)) != 0


def first_available_port(host: str, preferred_port: int) -> int:
    for port in range(preferred_port, preferred_port + 20):
        if port_is_available(host, port):
            return port
    return preferred_port


def strip_launcher_args(argv: list[str]) -> list[str]:
    stripped: list[str] = []
    skip_next = False
    for arg in argv:
        if skip_next:
            skip_next = False
            continue
        if arg in {"--pipeline-worker", "--no-open"}:
            continue
        if arg == "--project-root":
            skip_next = True
            continue
        if arg.startswith("--project-root="):
            continue
        stripped.append(arg)
    return stripped


def has_port_arg(argv: list[str]) -> bool:
    return "--port" in argv or any(arg.startswith("--port=") for arg in argv)


def has_artifacts_root_arg(argv: list[str]) -> bool:
    return "--artifacts-root" in argv or any(arg.startswith("--artifacts-root=") for arg in argv)


def candidate_artifact_roots(project_root: Path) -> list[Path]:
    artifacts_root = project_root / "artifacts"
    if not artifacts_root.exists():
        return []
    roots = [artifacts_root]
    for candidate in artifacts_root.rglob("*"):
        if not candidate.is_dir():
            continue
        if (
            (candidate / "05_alias").exists()
            or (candidate / "06_drafts").exists()
            or (candidate / "07_review").exists()
        ):
            roots.append(candidate)
    return roots


def artifact_root_sort_key(root: Path) -> tuple[int, float]:
    review_markers = [
        root / "05_alias" / "conversation_entity_proposals.json",
        root / "07_review" / "identity_merge_proposals.json",
        root / "06_drafts" / "card_drafts" / "claim_drafts.json",
        root / "07_review" / "card_drafts.json",
    ]
    existing = [path for path in review_markers if path.exists()]
    if not existing:
        return (0, 0.0)
    has_review_gate = int(
        (root / "05_alias" / "conversation_entity_proposals.json").exists()
        or (root / "07_review" / "identity_merge_proposals.json").exists()
    )
    return (100 * has_review_gate + len(existing), max(path.stat().st_mtime for path in existing))


def discover_default_artifacts_root(project_root: Path) -> Path | None:
    candidates = [
        root
        for root in candidate_artifact_roots(project_root)
        if artifact_root_sort_key(root)[0] > 0
    ]
    if not candidates:
        return None
    return max(candidates, key=artifact_root_sort_key)


def parse_launcher_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--pipeline-worker", action="store_true")
    parser.add_argument("--project-root", type=Path, required=False)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8787)
    parser.add_argument("--no-open", action="store_true")
    args, _ = parser.parse_known_args(argv)
    return args


def open_browser_later(host: str, port: int) -> None:
    url = f"http://{host}:{port}/"
    timer = threading.Timer(1.25, lambda: webbrowser.open(url))
    timer.daemon = True
    timer.start()


def main() -> None:
    launcher_args = parse_launcher_args(sys.argv[1:])
    project_root = find_project_root(launcher_args.project_root)
    os.chdir(project_root)

    forwarded_args = strip_launcher_args(sys.argv[1:])
    if launcher_args.pipeline_worker:
        from pipeline.run_pipeline import main as pipeline_main

        sys.argv = ["pipeline.run_pipeline", *forwarded_args]
        pipeline_main()
        return

    if not has_artifacts_root_arg(forwarded_args):
        default_artifacts_root = discover_default_artifacts_root(project_root)
        if default_artifacts_root is not None:
            forwarded_args.extend(["--artifacts-root", str(default_artifacts_root)])

    port = launcher_args.port
    if not has_port_arg(forwarded_args):
        port = first_available_port(launcher_args.host, launcher_args.port)
        if port != launcher_args.port:
            forwarded_args.extend(["--port", str(port)])

    if not launcher_args.no_open:
        open_browser_later(launcher_args.host, port)

    from pipeline.ui_review_app import main as ui_main

    sys.argv = ["pipeline.ui_review_app", *forwarded_args]
    ui_main()


if __name__ == "__main__":
    main()
