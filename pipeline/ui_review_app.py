from __future__ import annotations

import argparse
import os
import subprocess
import sys
import threading
from pathlib import Path
from typing import Any

from flask import Flask, jsonify, render_template_string, request

from pipeline.author_directives import parse_author_instruction
from pipeline.common import now_utc_iso, read_json, safe_uuid, write_json


HTML = """
<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <title>THERIAC Patch Review</title>
  <style>
    body { font-family: Arial, sans-serif; margin: 16px; }
    .row { display: flex; gap: 16px; }
    .panel { flex: 1; border: 1px solid #ddd; padding: 12px; border-radius: 6px; }
    textarea { width: 100%; height: 80px; }
    button { margin-right: 8px; }
    .status { margin: 12px 0; padding: 10px; border-radius: 6px; background: #f6f8fa; white-space: pre-wrap; }
    .logs { background: #0b1020; color: #d7e0ff; padding: 10px; border-radius: 6px; max-height: 280px; overflow-y: auto; white-space: pre-wrap; }
  </style>
</head>
<body>
  <h2>THERIAC Patch Review UI</h2>
  <p>Claim {{ claim.claim_id }} | Card {{ claim.card_id }} | Confidence {{ claim.confidence }}</p>
  <div class="row">
    <div class="panel">
      <h4>Proposed Patch</h4>
      <pre>{{ claim | tojson(indent=2) }}</pre>
    </div>
  </div>
  <form method="post" action="/decision">
    <input type="hidden" name="claim_id" value="{{ claim.claim_id }}" />
    <label>Reviewer</label>
    <input type="text" name="reviewer" value="human_reviewer" />
    <br /><br />
    <label>Rationale</label>
    <textarea name="rationale"></textarea>
    <br />
    <button name="decision" value="accept">Accept</button>
    <button name="decision" value="reject">Reject</button>
    <button name="decision" value="defer">Defer</button>
    <button name="decision" value="needs_more_context">Needs More Context</button>
  </form>
  <hr />
  <h3>Author Directive (Highest Priority)</h3>
  <form method="post" action="/directive">
    <label>Target Card ID</label>
    <input type="text" name="target_card_id" value="{{ claim.card_id }}" />
    <br /><br />
    <label>Author</label>
    <input type="text" name="author" value="author" />
    <br /><br />
    <label>Directive Type</label>
    <select name="directive_type">
      <option value="replace">replace</option>
      <option value="append">append</option>
      <option value="remove">remove</option>
      <option value="alias">alias</option>
      <option value="status_change">status_change</option>
      <option value="timeline_fix">timeline_fix</option>
    </select>
    <br /><br />
    <label>Instruction (natural language)</label>
    <textarea name="instruction_text" placeholder="replace summary with: ..."></textarea>
    <br />
    <button type="submit">Save Author Directive</button>
  </form>
  <hr />
  <h3>Pipeline Run Logs</h3>
  <form method="post" action="/run_full_pipeline">
    <button id="run-pipeline-btn" type="submit" {% if pipeline_status == "running" %}disabled{% endif %}>Run Full Pipeline (Stages A-F)</button>
  </form>
  <div class="status" id="pipeline-status">Status: {{ pipeline_status }}{% if pipeline_message %} | {{ pipeline_message }}{% endif %}</div>
  <pre class="logs" id="pipeline-logs">{{ pipeline_logs }}</pre>
  <script>
    async function refreshPipelineStatus() {
      const response = await fetch("/api/pipeline_run_status");
      if (!response.ok) return;
      const payload = await response.json();
      const statusEl = document.getElementById("pipeline-status");
      const logsEl = document.getElementById("pipeline-logs");
      const runBtn = document.getElementById("run-pipeline-btn");
      if (statusEl) {
        const msg = payload.message ? ` | ${payload.message}` : "";
        statusEl.textContent = `Status: ${payload.status}${msg}`;
      }
      if (logsEl) {
        logsEl.textContent = payload.logs || "(no logs yet)";
        logsEl.scrollTop = logsEl.scrollHeight;
      }
      if (runBtn) {
        runBtn.disabled = payload.status === "running";
      }
    }
    setInterval(refreshPipelineStatus, 1500);
    refreshPipelineStatus();
  </script>
</body>
</html>
"""

HTML_BOOTSTRAP = """
<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <title>THERIAC Patch Review</title>
  <style>
    body { font-family: Arial, sans-serif; margin: 16px; }
    .panel { border: 1px solid #ddd; padding: 12px; border-radius: 6px; max-width: 860px; }
    .status { margin: 12px 0; padding: 10px; border-radius: 6px; background: #f6f8fa; white-space: pre-wrap; }
    .logs { background: #0b1020; color: #d7e0ff; padding: 10px; border-radius: 6px; max-height: 320px; overflow-y: auto; white-space: pre-wrap; }
    code { background: #f6f8fa; padding: 2px 4px; border-radius: 4px; }
  </style>
</head>
<body>
  <h2>THERIAC Patch Review UI</h2>
  <div class="panel">
    <p>{{ bootstrap_reason }}</p>
    <p>Expected path: <code>{{ patches_path }}</code></p>
    <p>Full pipeline target: <code>{{ artifacts_root }}</code></p>
    <p>DOCX: <code>{{ docx_hint }}</code></p>
    <p>Conversations: <code>{{ conversations_root }}</code></p>
    <form method="post" action="/run_full_pipeline">
      <button id="run-pipeline-btn" type="submit" {% if pipeline_status == "running" %}disabled{% endif %}>Run Full Pipeline (Stages A-F)</button>
    </form>
    <div class="status" id="pipeline-status">Status: {{ pipeline_status }}{% if pipeline_message %} | {{ pipeline_message }}{% endif %}</div>
    <pre class="logs" id="pipeline-logs">{{ pipeline_logs }}</pre>
  </div>
  <script>
    async function refreshPipelineStatus() {
      const response = await fetch("/api/pipeline_run_status");
      if (!response.ok) return;
      const payload = await response.json();
      const statusEl = document.getElementById("pipeline-status");
      const logsEl = document.getElementById("pipeline-logs");
      const runBtn = document.getElementById("run-pipeline-btn");
      if (statusEl) {
        const msg = payload.message ? ` | ${payload.message}` : "";
        statusEl.textContent = `Status: ${payload.status}${msg}`;
      }
      if (logsEl) {
        logsEl.textContent = payload.logs || "(no logs yet)";
        logsEl.scrollTop = logsEl.scrollHeight;
      }
      if (runBtn) {
        runBtn.disabled = payload.status === "running";
      }
    }
    setInterval(refreshPipelineStatus, 1500);
    refreshPipelineStatus();
  </script>
</body>
</html>
"""


DEFAULT_PATCHES_CANDIDATES = [
    Path("artifacts/06_drafts/card_drafts/lore_patches.json"),
    Path("artifacts/small_batch/06_drafts/card_drafts/lore_patches.json"),
]

DEFAULT_DECISIONS_CANDIDATES = [
    Path("artifacts/07_review/merge_decisions.json"),
    Path("artifacts/small_batch/07_review/merge_decisions.json"),
]

DEFAULT_DIRECTIVES_CANDIDATES = [
    Path("artifacts/07_review/author_directives.json"),
    Path("artifacts/small_batch/07_review/author_directives.json"),
]

DEFAULT_DOCX_CANDIDATES = [
    Path("theriac-coda---lore-bible.docx"),
]


def _first_existing(candidates: list[Path]) -> Path | None:
    for path in candidates:
        if path.exists():
            return path
    return None


def _resolve_input_paths(
    patches: Path | None,
    decisions: Path | None,
    directives: Path | None,
    artifacts_root: Path | None,
) -> tuple[Path, Path, Path, Path]:
    if artifacts_root is not None:
        root_patches = artifacts_root / "06_drafts" / "card_drafts" / "lore_patches.json"
        root_decisions = artifacts_root / "07_review" / "merge_decisions.json"
        root_directives = artifacts_root / "07_review" / "author_directives.json"
        patches = patches or root_patches
        decisions = decisions or root_decisions
        directives = directives or root_directives
        resolved_artifacts_root = artifacts_root
    else:
        patches = patches or _first_existing(DEFAULT_PATCHES_CANDIDATES)
        decisions = decisions or _first_existing(DEFAULT_DECISIONS_CANDIDATES)
        directives = directives or _first_existing(DEFAULT_DIRECTIVES_CANDIDATES)
        if patches is None:
            patches = DEFAULT_PATCHES_CANDIDATES[0]
        if decisions is None and patches.parent.name == "card_drafts":
            decisions = patches.parent.parent.parent / "07_review" / "merge_decisions.json"
        if decisions is None:
            decisions = DEFAULT_DECISIONS_CANDIDATES[0]
        if directives is None and decisions is not None:
            directives = decisions.parent / "author_directives.json"
        if directives is None:
            directives = DEFAULT_DIRECTIVES_CANDIDATES[0]
        if patches.parent.name == "card_drafts":
            resolved_artifacts_root = patches.parent.parent.parent
        else:
            resolved_artifacts_root = Path("artifacts")

    return patches, decisions, directives, resolved_artifacts_root


def _ensure_review_files(decisions_path: Path, directives_path: Path) -> None:
    if not decisions_path.exists():
        write_json(decisions_path, {"decisions": []})
    if not directives_path.exists():
        write_json(directives_path, {"directives": []})


def _resolve_docx(repo_root: Path, docx_hint: Path | None) -> Path | None:
    if docx_hint is not None:
        candidate = docx_hint if docx_hint.is_absolute() else (repo_root / docx_hint)
        if candidate.exists():
            return candidate
        return None
    for candidate in DEFAULT_DOCX_CANDIDATES:
        full = repo_root / candidate
        if full.exists():
            return full
    discovered = sorted(repo_root.glob("*.docx"))
    if len(discovered) == 1:
        return discovered[0]
    return None


def _load_patches_or_reason(patches_path: Path) -> tuple[list[dict[str, Any]] | None, str]:
    if not patches_path.exists():
        return None, "No draft patches file was found yet."
    try:
        payload = read_json(patches_path)
    except Exception:
        return None, "Draft patches file exists but is unreadable JSON."
    patches = payload.get("patches")
    if not isinstance(patches, list):
        return None, "Draft patches file exists but does not contain a valid `patches` array."
    if len(patches) == 0:
        return None, "Draft patches file currently contains 0 patches."
    return patches, ""


def _pipeline_state_snapshot(
    state_lock: threading.Lock,
    pipeline_state: dict[str, Any],
) -> dict[str, Any]:
    with state_lock:
        logs = list(pipeline_state.get("logs", []))
        return {
            "status": str(pipeline_state.get("status", "idle")),
            "message": str(pipeline_state.get("message", "")),
            "logs": "\n".join(logs),
            "line_count": len(logs),
            "last_exit_code": pipeline_state.get("last_exit_code"),
            "started_at_utc": pipeline_state.get("started_at_utc"),
            "finished_at_utc": pipeline_state.get("finished_at_utc"),
        }


def build_app(
    patches_path: Path,
    decisions_path: Path,
    directives_path: Path,
    artifacts_root: Path,
    docx_hint: Path | None,
    conversations_root: Path | None,
) -> Flask:
    app = Flask(__name__)
    _ensure_review_files(decisions_path, directives_path)
    repo_root = Path(__file__).resolve().parents[1]
    resolved_conversations_root = (
        conversations_root if conversations_root is not None else (repo_root / "discord_conversations")
    )
    pipeline_state: dict[str, Any] = {
        "status": "idle",
        "message": "",
        "logs": [],
        "last_exit_code": None,
        "started_at_utc": None,
        "finished_at_utc": None,
    }
    state_lock = threading.Lock()
    max_log_lines = 1200

    def set_pipeline_state(**kwargs: Any) -> None:
        with state_lock:
            pipeline_state.update(kwargs)

    def append_pipeline_log(line: str) -> None:
        with state_lock:
            logs = pipeline_state.setdefault("logs", [])
            logs.append(line.rstrip())
            if len(logs) > max_log_lines:
                del logs[:-max_log_lines]

    def run_full_pipeline_worker(cmd: list[str]) -> None:
        set_pipeline_state(
            status="running",
            started_at_utc=now_utc_iso(),
            finished_at_utc=None,
            last_exit_code=None,
            message="Pipeline run started.",
            logs=[f"$ {' '.join(cmd)}"],
        )
        env = os.environ.copy()
        env["PYTHONUNBUFFERED"] = "1"
        try:
            with subprocess.Popen(
                cmd,
                cwd=repo_root,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
                env=env,
            ) as process:
                if process.stdout is not None:
                    for line in process.stdout:
                        append_pipeline_log(line)
                exit_code = process.wait()
            if exit_code == 0:
                _ensure_review_files(decisions_path, directives_path)
                set_pipeline_state(
                    status="succeeded",
                    message=f"Pipeline completed successfully. Patches path: {patches_path}",
                    last_exit_code=0,
                    finished_at_utc=now_utc_iso(),
                )
            else:
                set_pipeline_state(
                    status="failed",
                    message=f"Pipeline failed with exit code {exit_code}.",
                    last_exit_code=exit_code,
                    finished_at_utc=now_utc_iso(),
                )
        except Exception as exc:
            append_pipeline_log(f"[ui] Failed to start or stream pipeline: {exc}")
            set_pipeline_state(
                status="failed",
                message="Pipeline run failed before completion.",
                finished_at_utc=now_utc_iso(),
            )

    @app.get("/")
    def index() -> str:
        patches, bootstrap_reason = _load_patches_or_reason(patches_path)
        pipeline_snapshot = _pipeline_state_snapshot(state_lock, pipeline_state)
        if patches is None:
            return render_template_string(
                HTML_BOOTSTRAP,
                bootstrap_reason=bootstrap_reason,
                patches_path=str(patches_path),
                artifacts_root=str(artifacts_root),
                docx_hint=str(docx_hint) if docx_hint is not None else "(auto-discover)",
                conversations_root=str(resolved_conversations_root),
                pipeline_status=pipeline_snapshot["status"],
                pipeline_message=pipeline_snapshot["message"],
                pipeline_logs=pipeline_snapshot["logs"] or "(no logs yet)",
            )
        decisions_data = read_json(decisions_path) if decisions_path.exists() else {"decisions": []}
        decided = {d["claim_id"] for d in decisions_data.get("decisions", [])}
        pending = [p for p in patches if p["claim_id"] not in decided]
        if not pending:
            return "All patches reviewed."
        return render_template_string(
            HTML,
            claim=pending[0],
            pipeline_status=pipeline_snapshot["status"],
            pipeline_message=pipeline_snapshot["message"],
            pipeline_logs=pipeline_snapshot["logs"] or "(no logs yet)",
        )

    @app.post("/decision")
    def decision():
        patches, _ = _load_patches_or_reason(patches_path)
        if patches is None:
            return index()
        decisions_data = read_json(decisions_path) if decisions_path.exists() else {"decisions": []}
        payload = {
            "claim_id": request.form["claim_id"],
            "decision": request.form["decision"],
            "reviewer": request.form.get("reviewer", "reviewer"),
            "rationale": request.form.get("rationale", ""),
            "timestamp_utc": now_utc_iso(),
        }
        decisions_data.setdefault("decisions", []).append(payload)
        write_json(decisions_path, decisions_data)
        return index()

    @app.post("/directive")
    def directive():
        directives_data = read_json(directives_path) if directives_path.exists() else {"directives": []}
        instruction_text = request.form.get("instruction_text", "")
        payload = {
            "directive_id": safe_uuid(),
            "target_card_id": request.form.get("target_card_id", ""),
            "instruction_text": instruction_text,
            "directive_type": request.form.get("directive_type", "append"),
            "effective_timestamp_utc": now_utc_iso(),
            "author": request.form.get("author", "author"),
            "priority": "author_directive",
            "resolution_state": "pending",
            "parsed_payload": parse_author_instruction(instruction_text),
        }
        directives_data.setdefault("directives", []).append(payload)
        write_json(directives_path, directives_data)
        return index()

    @app.get("/api/decisions")
    def api_decisions():
        decisions_data = read_json(decisions_path) if decisions_path.exists() else {"decisions": []}
        return jsonify(decisions_data)

    @app.get("/api/directives")
    def api_directives():
        directives_data = read_json(directives_path) if directives_path.exists() else {"directives": []}
        return jsonify(directives_data)

    @app.get("/api/pipeline_run_status")
    def api_pipeline_run_status():
        return jsonify(_pipeline_state_snapshot(state_lock, pipeline_state))

    @app.post("/run_full_pipeline")
    def run_full_pipeline():
        pipeline_snapshot = _pipeline_state_snapshot(state_lock, pipeline_state)
        if pipeline_snapshot["status"] == "running":
            set_pipeline_state(message="Pipeline is already running.")
            return index()

        docx_path = _resolve_docx(repo_root, docx_hint)
        if docx_path is None:
            set_pipeline_state(
                status="failed",
                message=(
                    "Full pipeline could not start: DOCX file was not found. "
                    "Pass --docx explicitly when starting the UI."
                ),
            )
            return index()
        if not resolved_conversations_root.exists():
            set_pipeline_state(
                status="failed",
                message=(
                    "Full pipeline could not start: conversations root was not found. "
                    f"Expected: {resolved_conversations_root}"
                ),
            )
            return index()

        cmd = [
            sys.executable,
            "-m",
            "pipeline.run_pipeline",
            "--docx",
            str(docx_path),
            "--conversations-root",
            str(resolved_conversations_root),
            "--artifacts-root",
            str(artifacts_root),
            "--log-level",
            "INFO",
        ]
        worker = threading.Thread(
            target=run_full_pipeline_worker,
            args=(cmd,),
            daemon=True,
        )
        worker.start()
        return index()

    return app


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run the THERIAC patch review UI."
    )
    parser.add_argument("--patches", type=Path, required=False)
    parser.add_argument("--decisions", type=Path, required=False)
    parser.add_argument("--directives", type=Path, required=False)
    parser.add_argument("--docx", type=Path, required=False)
    parser.add_argument("--conversations-root", type=Path, required=False)
    parser.add_argument(
        "--artifacts-root",
        type=Path,
        required=False,
        help="Artifact root containing 06_drafts and 07_review (e.g. artifacts/small_batch).",
    )
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8787)
    args = parser.parse_args()

    patches_path, decisions_path, directives_path, artifacts_root = _resolve_input_paths(
        args.patches,
        args.decisions,
        args.directives,
        args.artifacts_root,
    )
    app = build_app(
        patches_path,
        decisions_path,
        directives_path,
        artifacts_root,
        args.docx,
        args.conversations_root,
    )
    app.run(host=args.host, port=args.port, debug=False)


if __name__ == "__main__":
    main()
