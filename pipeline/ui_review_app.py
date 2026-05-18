from __future__ import annotations

import argparse
import html as html_lib
import os
import re
import subprocess
import sys
import threading
from pathlib import Path
from typing import Any

from flask import Flask, jsonify, redirect, render_template_string, request

from pipeline.author_directives import parse_author_instruction
from pipeline.common import now_utc_iso, read_json, safe_uuid, write_json

PIPELINE_STAGES = [
    {"index": 1, "short_label": "01", "name": "Entity Bootstrap"},
    {"index": 2, "short_label": "02", "name": "Message Normalization"},
    {"index": 3, "short_label": "03", "name": "Timeline Merge"},
    {"index": 4, "short_label": "04", "name": "Conversation Segmentation"},
    {"index": 5, "short_label": "05", "name": "Conversation Patch Notes"},
    {"index": 6, "short_label": "06", "name": "Snippet Extraction"},
    {"index": 7, "short_label": "07", "name": "Entity Resolution"},
    {"index": 8, "short_label": "08", "name": "Snippet Grouping"},
    {"index": 9, "short_label": "09", "name": "Claim Drafting"},
    {"index": 10, "short_label": "10", "name": "Card Synthesis"},
    {"index": 11, "short_label": "11", "name": "Notion Export"},
]

NEW_RUN_SELECTOR_VALUE = "__theriac_new_run__"

PIPELINE_PROGRESS_CSS = """
    .run-selector { margin: 10px 0 14px; padding: 12px; border: 1px solid #d8dee4; border-radius: 6px; background: #f6f8fa; }
    .run-selector form { display: flex; gap: 8px; align-items: center; flex-wrap: wrap; margin: 0; }
    .run-selector label { font-weight: 700; }
    .run-selector select { min-width: min(680px, 100%); max-width: 100%; }
    .run-selector .run-meta { color: #57606a; font-size: 12px; margin-top: 6px; }
    .pipeline-progress { margin: 14px 0; padding: 12px; border: 1px solid #d8dee4; border-radius: 6px; background: #fff; }
    .pipeline-progress-header { display: flex; justify-content: space-between; gap: 12px; align-items: baseline; margin-bottom: 12px; }
    .pipeline-progress-title { margin: 0; font-size: 16px; }
    .pipeline-progress-meta { color: #57606a; font-size: 13px; }
    .pipeline-stages { display: grid; grid-template-columns: repeat(11, minmax(72px, 1fr)); gap: 8px; }
    .pipeline-stage { position: relative; min-width: 0; text-align: center; }
    .pipeline-dot { width: 22px; height: 22px; border-radius: 50%; margin: 0 auto 6px; border: 2px solid #c9d1d9; background: #fff; box-sizing: border-box; }
    .pipeline-stage.done .pipeline-dot { background: #238636; border-color: #238636; }
    .pipeline-stage.current .pipeline-dot { background: #0969da; border-color: #0969da; box-shadow: 0 0 0 4px rgba(9, 105, 218, 0.16); }
    .pipeline-stage.attention .pipeline-dot { background: #bf8700; border-color: #bf8700; box-shadow: 0 0 0 4px rgba(191, 135, 0, 0.18); }
    .pipeline-stage.failed .pipeline-dot { background: #cf222e; border-color: #cf222e; box-shadow: 0 0 0 4px rgba(207, 34, 46, 0.14); }
    .pipeline-stage.waiting .pipeline-dot { background: #f6f8fa; }
    .pipeline-label { font-size: 12px; font-weight: 700; color: #24292f; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
    .pipeline-sub { font-size: 11px; color: #57606a; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
    .pipeline-stage:not(:last-child)::after { content: ""; position: absolute; left: calc(50% + 15px); right: calc(-50% + 15px); top: 10px; height: 2px; background: #d8dee4; z-index: 0; }
    .pipeline-stage.done:not(:last-child)::after { background: #238636; }
    .pipeline-stage > * { position: relative; z-index: 1; }
    @media (max-width: 760px) { .pipeline-stages { grid-template-columns: repeat(4, minmax(72px, 1fr)); row-gap: 14px; } .pipeline-stage::after { display: none; } }
"""

PIPELINE_PROGRESS_JS = """
    function renderPipelineProgress(progress) {
      if (!progress) return;
      const metaEl = document.getElementById("pipeline-progress-meta");
      if (metaEl) metaEl.textContent = progress.summary || "";
      for (const stage of progress.stages || []) {
        const el = document.querySelector(`[data-stage-index="${stage.index}"]`);
        if (!el) continue;
        el.className = `pipeline-stage ${stage.state || "waiting"}`;
        const dot = el.querySelector(".pipeline-dot");
        if (dot) dot.title = `${stage.name}: ${stage.state}`;
      }
    }
"""

STAGE_LOG_RE = re.compile(r"\[(\d+)/(\d+)\]\s+(START|DONE|REVIEW)\s+(.+?)(?:\s+\(|$)")
STAGE_HEARTBEAT_RE = re.compile(r"Stage\s+(\d+)\s+(model call|progress):?\s+(\d+)/(\d+)\b(?:\s+([^.\n]+))?", re.IGNORECASE)
REVIEW_GATE_LOG_MARKERS = (
    "requiring review",
    "conversation entity proposal",
    "identity merge proposal",
)


def is_pipeline_progress_log_line(line: str) -> bool:
    lowered = line.lower()
    return (
        STAGE_LOG_RE.search(line) is not None
        or STAGE_HEARTBEAT_RE.search(line) is not None
        or any(marker in lowered for marker in REVIEW_GATE_LOG_MARKERS)
    )


def pipeline_progress_from_logs(
    logs: list[str],
    status: str,
    message: str = "",
    last_exit_code: int | None = None,
) -> dict[str, Any]:
    completed: set[int] = set()
    current_index: int | None = None
    latest_index = 0
    latest_heartbeat: dict[str, Any] | None = None
    for line in logs:
        match = STAGE_LOG_RE.search(line)
        if match:
            idx = int(match.group(1))
            action = match.group(3)
            latest_index = max(latest_index, idx)
            if action == "START":
                current_index = idx
            elif action == "DONE":
                completed.add(idx)
                if current_index == idx:
                    current_index = None
            elif action == "REVIEW":
                current_index = idx
            continue
        heartbeat = STAGE_HEARTBEAT_RE.search(line)
        if heartbeat:
            idx = int(heartbeat.group(1))
            latest_index = max(latest_index, idx)
            current_index = idx
            latest_heartbeat = {
                "stage_index": idx,
                "kind": heartbeat.group(2).lower(),
                "current": int(heartbeat.group(3)),
                "total": int(heartbeat.group(4)),
                "detail": (heartbeat.group(5) or "").strip(),
            }

    total_stages = len(PIPELINE_STAGES)
    if status == "succeeded":
        completed = {stage["index"] for stage in PIPELINE_STAGES}
        current_index = None
    elif status == "running" and current_index is None and len(completed) < total_stages:
        current_index = min(len(completed) + 1, total_stages)
    elif status in {"failed", "review_required"} and current_index is None:
        if latest_index and latest_index not in completed:
            current_index = latest_index
        elif len(completed) < total_stages:
            current_index = len(completed) + 1
        else:
            current_index = total_stages

    logs_text = "\n".join(logs).lower()
    review_gate = status == "review_required" or (status == "failed" and any(marker in logs_text for marker in REVIEW_GATE_LOG_MARKERS))
    stages: list[dict[str, Any]] = []
    current_name = ""
    for stage in PIPELINE_STAGES:
        idx = int(stage["index"])
        state = "waiting"
        if idx in completed:
            state = "done"
        if current_index == idx:
            if status in {"failed", "review_required"}:
                state = "attention" if review_gate else "failed"
            elif status == "running":
                state = "current"
        if current_index == idx:
            current_name = str(stage["name"])
        stages.append({**stage, "state": state})

    if status == "running" and current_index:
        summary = f"Running stage {current_index}/{total_stages}: {current_name}"
        if latest_heartbeat and int(latest_heartbeat.get("stage_index", 0)) == current_index:
            summary += (
                f" ({latest_heartbeat['kind']} "
                f"{latest_heartbeat['current']}/{latest_heartbeat['total']})"
            )
    elif status == "succeeded":
        summary = f"Complete: {total_stages}/{total_stages} stages"
    elif status in {"failed", "review_required"} and review_gate and current_index:
        summary = f"Paused for review at stage {current_index}/{total_stages}: {current_name}"
    elif status == "failed" and current_index:
        summary = f"Failed at stage {current_index}/{total_stages}: {current_name}"
    elif status == "cancelled":
        summary = message or "Pipeline cancelled"
    elif status == "idle":
        summary = "No pipeline run in progress"
    elif last_exit_code not in (None, 0):
        summary = f"Stopped with exit code {last_exit_code}"
    else:
        summary = message or "Pipeline status unavailable"

    return {
        "status": status,
        "summary": summary,
        "current_stage_index": current_index,
        "completed_count": len(completed),
        "total_stages": total_stages,
        "review_gate": review_gate,
        "stages": stages,
    }


def pipeline_progress_artifact_snapshot(root: Path) -> dict[str, Any]:
    logs: list[str] = []
    stage_total = len(PIPELINE_STAGES)

    def mark_done(index: int, name: str) -> None:
        logs.append(f"[{index}/{stage_total}] START Stage {index:02d} {name}")
        logs.append(f"[{index}/{stage_total}] DONE  Stage {index:02d} {name}")

    def mark_start(index: int, name: str) -> None:
        logs.append(f"[{index}/{stage_total}] START Stage {index:02d} {name}")

    if (root / "01_bootstrap" / "entity_seed.json").exists():
        mark_done(1, "Entity Bootstrap")
    if (root / "02_timeline" / "messages_normalized_per_thread.jsonl").exists():
        mark_done(2, "Message Normalization")
    if (root / "02_timeline" / "messages_global_timeline.jsonl").exists():
        mark_done(3, "Timeline Merge")
    if (root / "02_timeline" / "conversation_segments.json").exists():
        mark_done(4, "Conversation Segmentation")

    patch_notes_path = root / "02_timeline" / "conversation_patch_notes.json"
    if patch_notes_path.exists():
        mark_start(5, "Conversation Patch Notes")
        try:
            patch_payload = read_json(patch_notes_path)
        except Exception:
            patch_payload = {}
        if isinstance(patch_payload, dict):
            conversation_total = int(patch_payload.get("conversation_count", 0) or 0)
            processed = int(patch_payload.get("notes_count", 0) or 0) + int(patch_payload.get("failure_count", 0) or 0)
            if conversation_total:
                logs.append(f"Stage 05 progress: {min(processed, conversation_total)}/{conversation_total} conversations")
            if str(patch_payload.get("status", "")).strip().lower() == "complete":
                logs.append(f"[5/{stage_total}] DONE  Stage 05 Conversation Patch Notes")

    if (root / "03_relevance" / "snippets_candidates.jsonl").exists():
        mark_done(6, "Snippet Extraction")

    counts = pending_review_counts_for_root(root)
    if (root / "05_alias" / "resolved_entities.json").exists() or (root / "05_alias" / "conversation_entity_proposals.json").exists():
        mark_start(7, "Entity Resolution")
        if counts.get("conversation_entities", 0) > 0:
            logs.append(f"[7/{stage_total}] REVIEW Stage 07 Entity Resolution")
            return {
                "status": "review_required",
                "message": f"Paused at Stage 07 Entity Resolution: {counts['conversation_entities']} conversation entity proposal(s) need review.",
                "logs": logs,
            }
        logs.append(f"[7/{stage_total}] DONE  Stage 07 Entity Resolution")

    if (root / "04_grouping" / "snippet_clusters_lore.json").exists() or (root / "04_grouping" / "snippet_clusters_meta.json").exists():
        mark_done(8, "Snippet Grouping")

    if (root / "06_drafts" / "card_drafts" / "claim_drafts.json").exists():
        mark_start(9, "Claim Drafting")
        if counts.get("claims", 0) > 0:
            logs.append(f"[9/{stage_total}] REVIEW Stage 09 Claim Drafting")
            return {
                "status": "review_required",
                "message": f"Paused after Stage 09 Claim Drafting: {counts['claims']} claim(s) need review.",
                "logs": logs,
            }
        logs.append(f"[9/{stage_total}] DONE  Stage 09 Claim Drafting")

    if counts.get("identity_merges", 0) > 0:
        mark_start(10, "Card Synthesis")
        logs.append(f"[10/{stage_total}] REVIEW Stage 10 Card Synthesis")
        return {
            "status": "review_required",
            "message": f"Paused for identity review: {counts['identity_merges']} identity merge proposal(s) need review.",
            "logs": logs,
        }
    if (root / "07_review" / "card_drafts.json").exists() or (root / "07_review" / "canonical_cards.json").exists():
        mark_start(10, "Card Synthesis")
        if counts.get("cards", 0) > 0:
            logs.append(f"[10/{stage_total}] REVIEW Stage 10 Card Synthesis")
            return {
                "status": "review_required",
                "message": f"Paused for card review: {counts['cards']} card draft(s) need review.",
                "logs": logs,
            }
        logs.append(f"[10/{stage_total}] DONE  Stage 10 Card Synthesis")
    if counts.get("cards", 0) > 0:
        mark_start(10, "Card Synthesis")
        logs.append(f"[10/{stage_total}] REVIEW Stage 10 Card Synthesis")
        return {
            "status": "review_required",
            "message": f"Paused for card review: {counts['cards']} card draft(s) need review.",
            "logs": logs,
        }
    if (root / "08_notion" / "notion_import.ndjson").exists():
        mark_done(11, "Notion Export")
    if logs:
        status = "succeeded" if any(f"[11/{stage_total}] DONE" in line for line in logs) else "idle"
        return {"status": status, "message": "", "logs": logs}
    return {"status": "idle", "message": "", "logs": []}


def render_pipeline_progress_html(progress: dict[str, Any]) -> str:
    stages_html: list[str] = []
    for stage in progress.get("stages", []):
        state = html_lib.escape(str(stage.get("state", "waiting")))
        index = html_lib.escape(str(stage.get("index", "")))
        label = html_lib.escape(str(stage.get("short_label", "")))
        name = html_lib.escape(str(stage.get("name", "")))
        stages_html.append(
            f'<div class="pipeline-stage {state}" data-stage-index="{index}">'
            f'<div class="pipeline-dot" title="{name}: {state}"></div>'
            f'<div class="pipeline-label">{label}</div>'
            f'<div class="pipeline-sub">{name}</div>'
            "</div>"
        )
    summary = html_lib.escape(str(progress.get("summary", "")))
    return (
        '<div class="pipeline-progress" id="pipeline-progress" aria-label="Pipeline progress tracker">'
        '<div class="pipeline-progress-header">'
        '<h3 class="pipeline-progress-title">Pipeline Progress</h3>'
        f'<div class="pipeline-progress-meta" id="pipeline-progress-meta">{summary}</div>'
        "</div>"
        '<div class="pipeline-stages">'
        + "".join(stages_html)
        + "</div></div>"
    )


HTML = """
<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <title>THERIAC Claim Review</title>
  <style>
    body { font-family: Arial, sans-serif; margin: 16px; }
    .row { display: flex; gap: 16px; }
    .panel { flex: 1; border: 1px solid #ddd; padding: 12px; border-radius: 6px; }
    textarea { width: 100%; height: 80px; }
    button { margin-right: 8px; }
    .status { margin: 12px 0; padding: 10px; border-radius: 6px; background: #f6f8fa; white-space: pre-wrap; }
    .logs { background: #0b1020; color: #d7e0ff; padding: 10px; border-radius: 6px; max-height: 280px; overflow-y: auto; white-space: pre-wrap; }
    {{ pipeline_progress_css | safe }}
  </style>
</head>
<body>
  <h2>THERIAC Claim Review UI</h2>
  {{ run_selector_html | safe }}
  <p>Claim {{ claim.claim_id }} | Entity {{ claim.target_entity_name }} | Confidence {{ claim.confidence }}</p>
  <div class="row">
    <div class="panel">
      <h4>Proposed Claim</h4>
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
    <input type="text" name="target_card_id" value="{{ claim.target_card_id }}" />
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
  {{ pipeline_progress_html | safe }}
  <pre class="logs" id="pipeline-logs">{{ pipeline_logs }}</pre>
  <script>
    {{ pipeline_progress_js | safe }}
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
      renderPipelineProgress(payload.progress);
    }
    setInterval(refreshPipelineStatus, 1500);
    refreshPipelineStatus();
  </script>
</body>
</html>
"""

HTML_CARD = """
<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <title>THERIAC Card Review</title>
  <style>
    body { font-family: Arial, sans-serif; margin: 16px; }
    .row { display: flex; gap: 16px; }
    .panel { flex: 1; border: 1px solid #ddd; padding: 12px; border-radius: 6px; }
    textarea { width: 100%; height: 100px; }
    button { margin-right: 8px; }
    .status { margin: 12px 0; padding: 10px; border-radius: 6px; background: #f6f8fa; white-space: pre-wrap; }
    {{ pipeline_progress_css | safe }}
  </style>
</head>
<body>
  <h2>THERIAC Card Review UI</h2>
  {{ run_selector_html | safe }}
  <p>Card {{ card.card_id }} | {{ card.canonical_name }} | Status {{ card.status }}</p>
  {{ pipeline_progress_html | safe }}
  <div class="row">
    <div class="panel">
      <h4>Synthesized Draft Card</h4>
      <pre>{{ card | tojson(indent=2) }}</pre>
    </div>
  </div>
  <form method="post" action="/card_decision">
    <input type="hidden" name="card_id" value="{{ card.card_id }}" />
    <label>Reviewer</label>
    <input type="text" name="reviewer" value="human_reviewer" />
    <br /><br />
    <label>Rationale</label>
    <textarea name="rationale"></textarea>
    <br />
    <label>Edited Summary (optional)</label>
    <textarea name="edited_summary"></textarea>
    <br />
    <button name="decision" value="approve">Approve Canonical</button>
    <button name="decision" value="reject">Reject</button>
    <button name="decision" value="defer">Defer</button>
    <button name="decision" value="needs_more_context">Needs More Context</button>
  </form>
  <hr />
  <p class="status">After card review decisions are saved, rerun Stage 10 to write approved cards to canonical_cards.json and persistent review memory.</p>
</body>
</html>
"""

HTML_IDENTITY_MERGE = """
<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <title>THERIAC Entity Merge Review</title>
  <style>
    body { font-family: Arial, sans-serif; margin: 16px; }
    .row { display: flex; gap: 16px; }
    .panel { flex: 1; border: 1px solid #ddd; padding: 12px; border-radius: 6px; }
    textarea { width: 100%; height: 100px; }
    button { margin-right: 8px; }
    .status { margin: 12px 0; padding: 10px; border-radius: 6px; background: #f6f8fa; white-space: pre-wrap; }
    {{ pipeline_progress_css | safe }}
  </style>
</head>
<body>
  <h2>THERIAC Entity Merge Review</h2>
  {{ run_selector_html | safe }}
  <p>{{ proposal.source_entity_name }} → {{ proposal.target_entity_name }} | {{ proposal.merge_type }}</p>
  {{ pipeline_progress_html | safe }}
  <div class="row">
    <div class="panel">
      <h4>Proposed Identity Merge</h4>
      <pre>{{ proposal | tojson(indent=2) }}</pre>
    </div>
  </div>
  <form method="post" action="/identity_merge_decision">
    <input type="hidden" name="proposal_id" value="{{ proposal.proposal_id }}" />
    <label>Reviewer</label>
    <input type="text" name="reviewer" value="human_reviewer" />
    <br /><br />
    <label>Rationale</label>
    <textarea name="rationale"></textarea>
    <br />
    <button name="decision" value="approve">Approve Merge</button>
    <button name="decision" value="reject">Reject</button>
    <button name="decision" value="defer">Defer</button>
    <button name="decision" value="needs_more_context">Needs More Context</button>
  </form>
  <hr />
  <p class="status">After entity merge decisions are saved, rerun Stage 10 so approved merges can regroup claims before card synthesis.</p>
</body>
</html>
"""

HTML_CONVERSATION_ENTITY = """
<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <title>THERIAC Conversation Entity Review</title>
  <style>
    body { font-family: Arial, sans-serif; margin: 16px; }
    .row { display: flex; gap: 16px; }
    .panel { flex: 1; border: 1px solid #ddd; padding: 12px; border-radius: 6px; }
    textarea { width: 100%; height: 100px; }
    input, select { min-width: 260px; }
    button { margin-right: 8px; }
    .status { margin: 12px 0; padding: 10px; border-radius: 6px; background: #f6f8fa; white-space: pre-wrap; }
    {{ pipeline_progress_css | safe }}
  </style>
</head>
<body>
  <h2>THERIAC Conversation Entity Review</h2>
  {{ run_selector_html | safe }}
  <p>{{ proposal.candidate_name }} | proposed type {{ proposal.proposed_entity_type }} | evidence {{ proposal.evidence_count }}</p>
  {{ pipeline_progress_html | safe }}
  <div class="row">
    <div class="panel">
      <h4>Proposed Conversation Entity</h4>
      <pre>{{ proposal | tojson(indent=2) }}</pre>
    </div>
  </div>
  <form method="post" action="/conversation_entity_decision">
    <input type="hidden" name="proposal_id" value="{{ proposal.proposal_id }}" />
    <input type="hidden" name="candidate_name" value="{{ proposal.candidate_name }}" />
    <label>Canonical Name</label>
    <input type="text" name="canonical_name" value="{{ proposal.candidate_name }}" />
    <br /><br />
    <label>Entity Type</label>
    <select name="entity_type">
      {% for entity_type in entity_types %}
        <option value="{{ entity_type }}" {% if entity_type == proposal.proposed_entity_type %}selected{% endif %}>{{ entity_type }}</option>
      {% endfor %}
    </select>
    <br /><br />
    <label>Reviewer</label>
    <input type="text" name="reviewer" value="human_reviewer" />
    <br /><br />
    <label>Rationale</label>
    <textarea name="rationale"></textarea>
    <br />
    <button name="decision" value="approve">Approve Entity</button>
    <button name="decision" value="reject">Reject</button>
    <button name="decision" value="defer">Defer</button>
    <button name="decision" value="needs_more_context">Needs More Context</button>
  </form>
  <hr />
  <p class="status">After conversation entity decisions are saved, rerun the pipeline from Stage 07 or rerun the full pipeline so approved entities can be grouped and drafted into claims.</p>
</body>
</html>
"""

HTML_BOOTSTRAP = """
<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <title>THERIAC Claim Review</title>
  <style>
    body { font-family: Arial, sans-serif; margin: 16px; }
    .panel { border: 1px solid #ddd; padding: 12px; border-radius: 6px; max-width: 860px; }
    .status { margin: 12px 0; padding: 10px; border-radius: 6px; background: #f6f8fa; white-space: pre-wrap; }
    .logs { background: #0b1020; color: #d7e0ff; padding: 10px; border-radius: 6px; max-height: 320px; overflow-y: auto; white-space: pre-wrap; }
    code { background: #f6f8fa; padding: 2px 4px; border-radius: 4px; }
    {{ pipeline_progress_css | safe }}
  </style>
</head>
<body>
  <h2>THERIAC Claim Review UI</h2>
  {{ run_selector_html | safe }}
  <div class="panel">
    <p>{{ bootstrap_reason }}</p>
    <p>Expected claim draft path: <code>{{ patches_path }}</code></p>
    <p>Full pipeline target: <code>{{ artifacts_root }}</code></p>
    <p>DOCX: <code>{{ docx_hint }}</code></p>
    <p>Conversations: <code>{{ conversations_root }}</code></p>
    <form method="post" action="/run_full_pipeline">
      <button id="run-pipeline-btn" type="submit" {% if pipeline_status == "running" %}disabled{% endif %}>Run Full Pipeline (Stages A-F)</button>
    </form>
    <div class="status" id="pipeline-status">Status: {{ pipeline_status }}{% if pipeline_message %} | {{ pipeline_message }}{% endif %}</div>
    {{ pipeline_progress_html | safe }}
    <pre class="logs" id="pipeline-logs">{{ pipeline_logs }}</pre>
  </div>
  <script>
    {{ pipeline_progress_js | safe }}
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
      renderPipelineProgress(payload.progress);
    }
    setInterval(refreshPipelineStatus, 1500);
    refreshPipelineStatus();
  </script>
</body>
</html>
"""

HTML_MESSAGE = """
<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <title>THERIAC Review</title>
  <style>
    body { font-family: Arial, sans-serif; margin: 16px; }
    .status { margin: 12px 0; padding: 10px; border-radius: 6px; background: #f6f8fa; white-space: pre-wrap; }
    {{ pipeline_progress_css | safe }}
  </style>
</head>
<body>
  <h2>THERIAC Review</h2>
  {{ run_selector_html | safe }}
  <div class="status">{{ message }}</div>
</body>
</html>
"""


DEFAULT_PATCHES_CANDIDATES = [
    Path("artifacts/06_drafts/card_drafts/claim_drafts.json"),
    Path("artifacts/small_batch/06_drafts/card_drafts/claim_drafts.json"),
]

DEFAULT_DECISIONS_CANDIDATES = [
    Path("artifacts/07_review/claim_review_decisions.json"),
    Path("artifacts/small_batch/07_review/claim_review_decisions.json"),
]

DEFAULT_CARD_DRAFTS_CANDIDATES = [
    Path("artifacts/07_review/card_drafts.json"),
    Path("artifacts/small_batch/07_review/card_drafts.json"),
]

DEFAULT_CARD_DECISIONS_CANDIDATES = [
    Path("artifacts/07_review/card_review_decisions.json"),
    Path("artifacts/small_batch/07_review/card_review_decisions.json"),
]

DEFAULT_IDENTITY_MERGE_PROPOSALS_CANDIDATES = [
    Path("artifacts/07_review/identity_merge_proposals.json"),
    Path("artifacts/small_batch/07_review/identity_merge_proposals.json"),
]

DEFAULT_IDENTITY_MERGE_DECISIONS_CANDIDATES = [
    Path("artifacts/07_review/identity_merge_decisions.json"),
    Path("artifacts/small_batch/07_review/identity_merge_decisions.json"),
]

DEFAULT_CONVERSATION_ENTITY_PROPOSALS_CANDIDATES = [
    Path("artifacts/05_alias/conversation_entity_proposals.json"),
    Path("artifacts/small_batch/05_alias/conversation_entity_proposals.json"),
]

DEFAULT_CONVERSATION_ENTITY_DECISIONS_CANDIDATES = [
    Path("artifacts/05_alias/conversation_entity_decisions.json"),
    Path("artifacts/small_batch/05_alias/conversation_entity_decisions.json"),
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
) -> tuple[Path, Path, Path, Path, Path, Path, Path, Path, Path, Path]:
    if artifacts_root is not None:
        root_patches = artifacts_root / "06_drafts" / "card_drafts" / "claim_drafts.json"
        root_decisions = artifacts_root / "07_review" / "claim_review_decisions.json"
        root_directives = artifacts_root / "07_review" / "author_directives.json"
        root_card_drafts = artifacts_root / "07_review" / "card_drafts.json"
        root_card_decisions = artifacts_root / "07_review" / "card_review_decisions.json"
        root_identity_merge_proposals = artifacts_root / "07_review" / "identity_merge_proposals.json"
        root_identity_merge_decisions = artifacts_root / "07_review" / "identity_merge_decisions.json"
        root_conversation_entity_proposals = artifacts_root / "05_alias" / "conversation_entity_proposals.json"
        root_conversation_entity_decisions = artifacts_root / "05_alias" / "conversation_entity_decisions.json"
        patches = patches or root_patches
        decisions = decisions or root_decisions
        directives = directives or root_directives
        card_drafts = root_card_drafts
        card_decisions = root_card_decisions
        identity_merge_proposals = root_identity_merge_proposals
        identity_merge_decisions = root_identity_merge_decisions
        conversation_entity_proposals = root_conversation_entity_proposals
        conversation_entity_decisions = root_conversation_entity_decisions
        resolved_artifacts_root = artifacts_root
    else:
        patches = patches or _first_existing(DEFAULT_PATCHES_CANDIDATES)
        decisions = decisions or _first_existing(DEFAULT_DECISIONS_CANDIDATES)
        directives = directives or _first_existing(DEFAULT_DIRECTIVES_CANDIDATES)
        card_drafts = _first_existing(DEFAULT_CARD_DRAFTS_CANDIDATES)
        card_decisions = _first_existing(DEFAULT_CARD_DECISIONS_CANDIDATES)
        identity_merge_proposals = _first_existing(DEFAULT_IDENTITY_MERGE_PROPOSALS_CANDIDATES)
        identity_merge_decisions = _first_existing(DEFAULT_IDENTITY_MERGE_DECISIONS_CANDIDATES)
        conversation_entity_proposals = _first_existing(DEFAULT_CONVERSATION_ENTITY_PROPOSALS_CANDIDATES)
        conversation_entity_decisions = _first_existing(DEFAULT_CONVERSATION_ENTITY_DECISIONS_CANDIDATES)
        if patches is None:
            patches = DEFAULT_PATCHES_CANDIDATES[0]
        if decisions is None and patches.parent.name == "card_drafts":
            decisions = patches.parent.parent.parent / "07_review" / "claim_review_decisions.json"
        if decisions is None:
            decisions = DEFAULT_DECISIONS_CANDIDATES[0]
        if directives is None and decisions is not None:
            directives = decisions.parent / "author_directives.json"
        if directives is None:
            directives = DEFAULT_DIRECTIVES_CANDIDATES[0]
        if card_drafts is None and decisions is not None:
            card_drafts = decisions.parent / "card_drafts.json"
        if card_drafts is None:
            card_drafts = DEFAULT_CARD_DRAFTS_CANDIDATES[0]
        if card_decisions is None and decisions is not None:
            card_decisions = decisions.parent / "card_review_decisions.json"
        if card_decisions is None:
            card_decisions = DEFAULT_CARD_DECISIONS_CANDIDATES[0]
        if identity_merge_proposals is None and decisions is not None:
            identity_merge_proposals = decisions.parent / "identity_merge_proposals.json"
        if identity_merge_proposals is None:
            identity_merge_proposals = DEFAULT_IDENTITY_MERGE_PROPOSALS_CANDIDATES[0]
        if identity_merge_decisions is None and decisions is not None:
            identity_merge_decisions = decisions.parent / "identity_merge_decisions.json"
        if identity_merge_decisions is None:
            identity_merge_decisions = DEFAULT_IDENTITY_MERGE_DECISIONS_CANDIDATES[0]
        if conversation_entity_proposals is None:
            conversation_entity_proposals = DEFAULT_CONVERSATION_ENTITY_PROPOSALS_CANDIDATES[0]
        if conversation_entity_decisions is None and conversation_entity_proposals is not None:
            conversation_entity_decisions = conversation_entity_proposals.with_name("conversation_entity_decisions.json")
        if conversation_entity_decisions is None:
            conversation_entity_decisions = DEFAULT_CONVERSATION_ENTITY_DECISIONS_CANDIDATES[0]
        if patches.parent.name == "card_drafts":
            resolved_artifacts_root = patches.parent.parent.parent
        else:
            resolved_artifacts_root = Path("artifacts")

    return (
        patches,
        decisions,
        directives,
        card_drafts,
        card_decisions,
        identity_merge_proposals,
        identity_merge_decisions,
        conversation_entity_proposals,
        conversation_entity_decisions,
        resolved_artifacts_root,
    )


def _ensure_review_files(
    decisions_path: Path,
    directives_path: Path,
    card_decisions_path: Path,
    identity_merge_decisions_path: Path,
    conversation_entity_decisions_path: Path,
) -> None:
    if not decisions_path.exists():
        write_json(decisions_path, {"decisions": []})
    if not directives_path.exists():
        write_json(directives_path, {"directives": []})
    if not card_decisions_path.exists():
        write_json(card_decisions_path, {"decisions": []})
    if not identity_merge_decisions_path.exists():
        write_json(identity_merge_decisions_path, {"decisions": []})
    if not conversation_entity_decisions_path.exists():
        write_json(conversation_entity_decisions_path, {"decisions": []})


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
    patches = payload.get("claims")
    if not isinstance(patches, list):
        return None, "Claim drafts file exists but does not contain a valid `claims` array."
    if len(patches) == 0:
        return None, "Claim drafts file currently contains 0 claims."
    return patches, ""


def _load_card_drafts_or_reason(card_drafts_path: Path) -> tuple[list[dict[str, Any]] | None, str]:
    if not card_drafts_path.exists():
        return None, "No synthesized card drafts file was found yet. Run Stage 10 after reviewing claims."
    try:
        payload = read_json(card_drafts_path)
    except Exception:
        return None, "Card drafts file exists but is unreadable JSON."
    cards = payload.get("cards")
    if not isinstance(cards, list):
        return None, "Card drafts file exists but does not contain a valid `cards` array."
    if len(cards) == 0:
        return None, "Card drafts file currently contains 0 cards."
    return cards, ""


def _read_json_or_default(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        return read_json(path)
    except Exception:
        return default


def _decision_ids(path: Path, id_fields: list[str]) -> set[str]:
    payload = _read_json_or_default(path, {"decisions": []})
    ids: set[str] = set()
    for decision in payload.get("decisions", []):
        for field in id_fields:
            value = str(decision.get(field, "")).strip()
            if value:
                ids.add(value)
    return ids


def pending_review_counts_for_root(root: Path) -> dict[str, int]:
    conversation_payload = _read_json_or_default(
        root / "05_alias" / "conversation_entity_proposals.json",
        {"proposals": [], "alias_review_groups": []},
    )
    conversation_proposals = conversation_payload.get("proposals", [])
    conversation_decisions = _decision_ids(
        root / "05_alias" / "conversation_entity_decisions.json",
        ["proposal_id"],
    )
    grouped_child_ids: set[str] = set()
    pending_alias_groups = []
    for group in conversation_payload.get("alias_review_groups", []) or []:
        if not isinstance(group, dict):
            continue
        pending_child_ids = [
            str(child_id)
            for child_id in group.get("child_proposal_ids", []) or []
            if str(child_id).strip() and str(child_id) not in conversation_decisions
        ]
        if pending_child_ids:
            grouped_child_ids.update(pending_child_ids)
            pending_alias_groups.append(group)
    pending_conversation_entities = [
        proposal
        for proposal in conversation_proposals
        if str(proposal.get("proposal_id", "")).strip()
        and str(proposal.get("proposal_id", "")) not in conversation_decisions
        and str(proposal.get("proposal_id", "")) not in grouped_child_ids
        and str(proposal.get("review_status", "pending")) == "pending"
    ]

    claims = _read_json_or_default(
        root / "06_drafts" / "card_drafts" / "claim_drafts.json",
        {"claims": []},
    ).get("claims", [])
    claim_decisions = _decision_ids(root / "07_review" / "claim_review_decisions.json", ["claim_id"])
    pending_claims = [
        claim
        for claim in claims
        if str(claim.get("claim_id", "")).strip() and str(claim.get("claim_id", "")) not in claim_decisions
    ]

    merge_proposals = _read_json_or_default(
        root / "07_review" / "identity_merge_proposals.json",
        {"proposals": []},
    ).get("proposals", [])
    merge_decisions = _decision_ids(
        root / "07_review" / "identity_merge_decisions.json",
        ["proposal_id", "merge_id"],
    )
    pending_merges = [
        proposal
        for proposal in merge_proposals
        if str(proposal.get("proposal_id", "")).strip()
        and str(proposal.get("proposal_id", "")) not in merge_decisions
        and str(proposal.get("review_status", "pending")) == "pending"
    ]

    cards = _read_json_or_default(root / "07_review" / "card_drafts.json", {"cards": []}).get("cards", [])
    card_decisions = _decision_ids(root / "07_review" / "card_review_decisions.json", ["card_id", "target_card_id"])
    pending_cards = [
        card
        for card in cards
        if str(card.get("card_id", "")).strip() and str(card.get("card_id", "")) not in card_decisions
    ]

    return {
        "conversation_entities": len(pending_alias_groups) + len(pending_conversation_entities),
        "claims": len(pending_claims),
        "identity_merges": len(pending_merges),
        "cards": len(pending_cards),
    }


def pending_review_total(counts: dict[str, int]) -> int:
    return sum(int(value) for value in counts.values())


def pending_review_summary(counts: dict[str, int]) -> str:
    labels = [
        ("conversation_entities", "conversation entities"),
        ("claims", "claims"),
        ("identity_merges", "identity merges"),
        ("cards", "cards"),
    ]
    parts = [f"{counts[key]} {label}" for key, label in labels if counts.get(key, 0)]
    return ", ".join(parts) if parts else "no pending review items"


def _latest_review_mtime(root: Path) -> float:
    markers = [
        root / "05_alias" / "conversation_entity_proposals.json",
        root / "05_alias" / "conversation_entity_decisions.json",
        root / "06_drafts" / "card_drafts" / "claim_drafts.json",
        root / "07_review" / "claim_review_decisions.json",
        root / "07_review" / "identity_merge_proposals.json",
        root / "07_review" / "identity_merge_decisions.json",
        root / "07_review" / "card_drafts.json",
        root / "07_review" / "card_review_decisions.json",
    ]
    existing = [path.stat().st_mtime for path in markers if path.exists()]
    return max(existing) if existing else 0.0


def _looks_like_artifacts_root(path: Path) -> bool:
    return (
        (path / "05_alias").exists()
        or (path / "06_drafts").exists()
        or (path / "07_review").exists()
        or (path / "01_bootstrap").exists()
    )


def _display_path(path: Path, repo_root: Path) -> str:
    try:
        return str(path.resolve().relative_to(repo_root.resolve()))
    except ValueError:
        return str(path.resolve())


def new_run_artifacts_root(repo_root: Path) -> Path:
    stamp = re.sub(r"[^0-9A-Za-z_]+", "", now_utc_iso().replace("T", "_").replace("Z", ""))
    base = repo_root / "artifacts" / "runs"
    for index in range(1, 1000):
        suffix = "" if index == 1 else f"_{index:03d}"
        candidate = base / f"{stamp}_full{suffix}"
        try:
            candidate.mkdir(parents=True, exist_ok=False)
            return candidate
        except FileExistsError:
            continue
    raise RuntimeError("Could not create a unique new run artifact folder.")


def discover_review_runs(repo_root: Path, active_root: Path) -> list[dict[str, Any]]:
    artifacts_dir = repo_root / "artifacts"
    candidates: list[Path] = [active_root]
    if artifacts_dir.exists():
        candidates.append(artifacts_dir)
        for child in artifacts_dir.rglob("*"):
            if child.is_dir() and _looks_like_artifacts_root(child):
                candidates.append(child)

    seen: set[str] = set()
    runs: list[dict[str, Any]] = []
    for candidate in candidates:
        root = candidate.resolve()
        key = str(root).lower()
        if key in seen:
            continue
        seen.add(key)
        counts = pending_review_counts_for_root(root)
        total = pending_review_total(counts)
        is_active = root == active_root.resolve()
        latest_mtime = _latest_review_mtime(root)
        if total == 0 and not is_active and latest_mtime <= 0:
            continue
        runs.append(
            {
                "artifacts_root": root,
                "label": _display_path(root, repo_root),
                "counts": counts,
                "pending_total": total,
                "summary": pending_review_summary(counts),
                "is_active": is_active,
                "latest_mtime": latest_mtime,
            }
        )
    return sorted(runs, key=lambda item: (not item["is_active"], -item["pending_total"], -item["latest_mtime"], item["label"]))


def render_run_selector_html(
    runs: list[dict[str, Any]],
    active_root: Path,
    repo_root: Path,
    disabled: bool = False,
    new_run_selected: bool = False,
) -> str:
    active_label = "New run" if new_run_selected else html_lib.escape(_display_path(active_root, repo_root))
    disabled_attr = " disabled" if disabled else ""
    options_html: list[str] = []
    options_html.append(
        f'<option value="{NEW_RUN_SELECTOR_VALUE}"{" selected" if new_run_selected else ""}>'
        "New Run - create a fresh timestamped artifact folder"
        "</option>"
    )
    for run in runs:
        root = html_lib.escape(str(run["artifacts_root"]))
        label = html_lib.escape(str(run["label"]))
        summary = html_lib.escape(str(run["summary"]))
        selected = (
            " selected"
            if not new_run_selected and run["artifacts_root"].resolve() == active_root.resolve()
            else ""
        )
        options_html.append(
            f'<option value="{root}"{selected}>{label} - {run["pending_total"]} pending ({summary})</option>'
        )
    meta = (
        "New run selected. Press Run Full Pipeline to create a fresh folder under artifacts/runs."
        if new_run_selected
        else f"Active: {active_label}. Runs shown here have review artifacts, including completed runs."
    )
    return (
        '<div class="run-selector">'
        '<form method="post" action="/select_run">'
        '<label for="artifacts-root-select">Run</label>'
        f'<select id="artifacts-root-select" name="artifacts_root"{disabled_attr}>'
        + "".join(options_html)
        + "</select>"
        f'<button type="submit"{disabled_attr}>Switch</button>'
        "</form>"
        f'<div class="run-meta">{meta}</div>'
        "</div>"
    )


def _looks_like_project_root(path: Path) -> bool:
    return (
        (path / "config" / "pipeline_config.json").exists()
        and (path / "theriac-coda---lore-bible.docx").exists()
    )


def _project_root() -> Path:
    if getattr(sys, "frozen", False):
        for start in (Path.cwd(), Path(sys.executable).resolve().parent):
            for candidate in (start, *start.parents):
                if _looks_like_project_root(candidate):
                    return candidate
        return Path.cwd()
    return Path(__file__).resolve().parents[1]


def _pipeline_worker_command(
    docx_path: Path,
    conversations_root: Path,
    artifacts_root: Path,
    resume: bool = False,
) -> list[str]:
    args = [
        "--docx",
        str(docx_path),
        "--conversations-root",
        str(conversations_root),
        "--artifacts-root",
        str(artifacts_root),
        "--log-level",
        "INFO",
    ]
    if resume:
        args.append("--resume")
    if getattr(sys, "frozen", False):
        return [sys.executable, "--pipeline-worker", *args]
    return [sys.executable, "-m", "pipeline.run_pipeline", *args]


def _pipeline_state_snapshot(
    state_lock: threading.Lock,
    pipeline_state: dict[str, Any],
) -> dict[str, Any]:
    with state_lock:
        logs = list(pipeline_state.get("logs", []))
        progress_logs = list(pipeline_state.get("progress_logs", [])) or logs
        status = str(pipeline_state.get("status", "idle"))
        message = str(pipeline_state.get("message", ""))
        last_exit_code = pipeline_state.get("last_exit_code")
        return {
            "status": status,
            "message": message,
            "logs": "\n".join(logs),
            "line_count": len(logs),
            "last_exit_code": last_exit_code,
            "started_at_utc": pipeline_state.get("started_at_utc"),
            "finished_at_utc": pipeline_state.get("finished_at_utc"),
            "progress": pipeline_progress_from_logs(
                progress_logs,
                status,
                message,
                last_exit_code if isinstance(last_exit_code, int) else None,
            ),
        }


def build_app(
    patches_path: Path,
    decisions_path: Path,
    directives_path: Path,
    card_drafts_path: Path,
    card_decisions_path: Path,
    identity_merge_proposals_path: Path,
    identity_merge_decisions_path: Path,
    conversation_entity_proposals_path: Path,
    conversation_entity_decisions_path: Path,
    artifacts_root: Path,
    docx_hint: Path | None,
    conversations_root: Path | None,
    repo_root_override: Path | None = None,
) -> Flask:
    app = Flask(__name__)
    _ensure_review_files(
        decisions_path,
        directives_path,
        card_decisions_path,
        identity_merge_decisions_path,
        conversation_entity_decisions_path,
    )
    repo_root = repo_root_override if repo_root_override is not None else _project_root()
    resolved_conversations_root = (
        conversations_root if conversations_root is not None else (repo_root / "discord_conversations")
    )
    pipeline_state: dict[str, Any] = {
        "status": "idle",
        "message": "",
        "logs": [],
        "progress_logs": [],
        "last_exit_code": None,
        "started_at_utc": None,
        "finished_at_utc": None,
    }
    state_lock = threading.Lock()
    max_log_lines = 1200
    new_run_selected = False

    def switch_artifacts_root(next_root: Path) -> None:
        nonlocal new_run_selected
        nonlocal patches_path
        nonlocal decisions_path
        nonlocal directives_path
        nonlocal card_drafts_path
        nonlocal card_decisions_path
        nonlocal identity_merge_proposals_path
        nonlocal identity_merge_decisions_path
        nonlocal conversation_entity_proposals_path
        nonlocal conversation_entity_decisions_path
        nonlocal artifacts_root
        (
            patches_path,
            decisions_path,
            directives_path,
            card_drafts_path,
            card_decisions_path,
            identity_merge_proposals_path,
            identity_merge_decisions_path,
            conversation_entity_proposals_path,
            conversation_entity_decisions_path,
            artifacts_root,
        ) = _resolve_input_paths(None, None, None, next_root)
        new_run_selected = False
        _ensure_review_files(
            decisions_path,
            directives_path,
            card_decisions_path,
            identity_merge_decisions_path,
            conversation_entity_decisions_path,
        )

    def set_pipeline_state(**kwargs: Any) -> None:
        with state_lock:
            pipeline_state.update(kwargs)
            if "logs" in kwargs:
                pipeline_state["progress_logs"] = [
                    str(line).rstrip()
                    for line in kwargs.get("logs", [])
                    if is_pipeline_progress_log_line(str(line))
                ]

    def append_pipeline_log(line: str) -> None:
        with state_lock:
            clean_line = line.rstrip()
            logs = pipeline_state.setdefault("logs", [])
            logs.append(clean_line)
            if len(logs) > max_log_lines:
                del logs[:-max_log_lines]
            if is_pipeline_progress_log_line(clean_line):
                pipeline_state.setdefault("progress_logs", []).append(clean_line)

    def current_run_selector_html(pipeline_snapshot: dict[str, Any]) -> str:
        return render_run_selector_html(
            discover_review_runs(repo_root, artifacts_root),
            artifacts_root,
            repo_root,
            disabled=pipeline_snapshot["status"] == "running",
            new_run_selected=new_run_selected,
        )

    def common_template_vars(pipeline_snapshot: dict[str, Any]) -> dict[str, Any]:
        return {
            "run_selector_html": current_run_selector_html(pipeline_snapshot),
            "pipeline_progress_css": PIPELINE_PROGRESS_CSS,
            "pipeline_progress_html": render_pipeline_progress_html(pipeline_snapshot["progress"]),
            "pipeline_progress_js": PIPELINE_PROGRESS_JS,
        }

    def render_message(message: str, pipeline_snapshot: dict[str, Any]) -> str:
        return render_template_string(
            HTML_MESSAGE,
            message=message,
            **common_template_vars(pipeline_snapshot),
        )

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
                _ensure_review_files(
                    decisions_path,
                    directives_path,
                    card_decisions_path,
                    identity_merge_decisions_path,
                    conversation_entity_decisions_path,
                )
                set_pipeline_state(
                    status="succeeded",
                    message=f"Pipeline completed successfully. Notion export path: {artifacts_root / '08_notion' / 'notion_import.ndjson'}",
                    last_exit_code=0,
                    finished_at_utc=now_utc_iso(),
                )
            elif exit_code == 2:
                set_pipeline_state(
                    status="review_required",
                    message="Pipeline paused for review. Review the pending decisions, then rerun from this artifact folder.",
                    last_exit_code=exit_code,
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

    def pending_conversation_entity_proposals() -> list[dict[str, Any]]:
        proposals_payload = (
            read_json(conversation_entity_proposals_path)
            if conversation_entity_proposals_path.exists()
            else {"proposals": []}
        )
        decisions_payload = (
            read_json(conversation_entity_decisions_path)
            if conversation_entity_decisions_path.exists()
            else {"decisions": []}
        )
        reviewed = {
            str(decision.get("proposal_id", ""))
            for decision in decisions_payload.get("decisions", [])
            if str(decision.get("proposal_id", "")).strip()
        }
        return [
            proposal
            for proposal in proposals_payload.get("proposals", [])
            if str(proposal.get("proposal_id", "")).strip()
            and str(proposal.get("proposal_id", "")) not in reviewed
            and str(proposal.get("review_status", "pending")) == "pending"
        ]

    @app.post("/select_run")
    def select_run():
        nonlocal new_run_selected
        pipeline_snapshot = _pipeline_state_snapshot(state_lock, pipeline_state)
        if pipeline_snapshot["status"] == "running":
            set_pipeline_state(message="Finish or stop the active pipeline run before switching runs.")
            return redirect("/")

        selected_root = request.form.get("artifacts_root", "").strip()
        if selected_root == NEW_RUN_SELECTOR_VALUE:
            new_run_selected = True
            set_pipeline_state(
                status="idle",
                message="New run selected. Press Run Full Pipeline to create a fresh artifact folder.",
                logs=[],
                last_exit_code=None,
                started_at_utc=None,
                finished_at_utc=None,
            )
            return redirect("/")
        allowed = {
            str(run["artifacts_root"].resolve()).lower(): run["artifacts_root"].resolve()
            for run in discover_review_runs(repo_root, artifacts_root)
        }
        target = Path(selected_root).resolve() if selected_root else None
        if target is None or str(target).lower() not in allowed:
            set_pipeline_state(message="Selected run was not found among runs with pending review items.")
            return redirect("/")

        switch_artifacts_root(allowed[str(target).lower()])
        set_pipeline_state(
            status="idle",
            message=f"Selected run: {_display_path(artifacts_root, repo_root)}",
            logs=[],
            last_exit_code=None,
            started_at_utc=None,
            finished_at_utc=None,
        )
        return redirect("/")

    @app.get("/")
    def index() -> str:
        pipeline_snapshot = _pipeline_state_snapshot(state_lock, pipeline_state)
        if new_run_selected:
            return render_message(
                "New run selected. Press Run Full Pipeline to create a fresh artifact folder under artifacts/runs.",
                pipeline_snapshot,
            )
        pending_entities = pending_conversation_entity_proposals()
        if pending_entities:
            return render_template_string(
                HTML_CONVERSATION_ENTITY,
                proposal=pending_entities[0],
                entity_types=["term", "theme", "quest", "event", "character", "faction", "organization", "location", "timeline_node"],
                **common_template_vars(pipeline_snapshot),
            )

        patches, bootstrap_reason = _load_patches_or_reason(patches_path)
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
                **common_template_vars(pipeline_snapshot),
            )
        decisions_data = read_json(decisions_path) if decisions_path.exists() else {"decisions": []}
        decided = {d["claim_id"] for d in decisions_data.get("decisions", [])}
        pending = [p for p in patches if p["claim_id"] not in decided]
        if not pending:
            identity_merge_payload = read_json(identity_merge_proposals_path) if identity_merge_proposals_path.exists() else {"proposals": []}
            identity_merge_decisions_data = read_json(identity_merge_decisions_path) if identity_merge_decisions_path.exists() else {"decisions": []}
            reviewed_merges = {
                str(d.get("proposal_id") or d.get("merge_id"))
                for d in identity_merge_decisions_data.get("decisions", [])
            }
            pending_merges = [
                p
                for p in identity_merge_payload.get("proposals", [])
                if str(p.get("proposal_id", "")) and str(p.get("proposal_id", "")) not in reviewed_merges
            ]
            if pending_merges:
                return render_template_string(
                    HTML_IDENTITY_MERGE,
                    proposal=pending_merges[0],
                    **common_template_vars(pipeline_snapshot),
                )
            cards, card_reason = _load_card_drafts_or_reason(card_drafts_path)
            if cards is None:
                return render_message(f"All claims reviewed. {card_reason}", pipeline_snapshot)
            card_decisions_data = read_json(card_decisions_path) if card_decisions_path.exists() else {"decisions": []}
            reviewed_cards = {str(d.get("card_id") or d.get("target_card_id")) for d in card_decisions_data.get("decisions", [])}
            pending_cards = [c for c in cards if str(c.get("card_id")) not in reviewed_cards]
            if not pending_cards:
                return render_message("All claims and synthesized card drafts reviewed.", pipeline_snapshot)
            return render_template_string(
                HTML_CARD,
                card=pending_cards[0],
                **common_template_vars(pipeline_snapshot),
            )
        return render_template_string(
            HTML,
            claim=pending[0],
            pipeline_status=pipeline_snapshot["status"],
            pipeline_message=pipeline_snapshot["message"],
            pipeline_logs=pipeline_snapshot["logs"] or "(no logs yet)",
            **common_template_vars(pipeline_snapshot),
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

    @app.post("/card_decision")
    def card_decision():
        card_decisions_data = read_json(card_decisions_path) if card_decisions_path.exists() else {"decisions": []}
        payload = {
            "card_id": request.form["card_id"],
            "decision": request.form["decision"],
            "reviewer": request.form.get("reviewer", "reviewer"),
            "rationale": request.form.get("rationale", ""),
            "edited_summary": request.form.get("edited_summary", "").strip(),
            "timestamp_utc": now_utc_iso(),
        }
        if not payload["edited_summary"]:
            payload.pop("edited_summary")
        card_decisions_data.setdefault("decisions", []).append(payload)
        write_json(card_decisions_path, card_decisions_data)
        return index()

    @app.post("/identity_merge_decision")
    def identity_merge_decision():
        decisions_data = read_json(identity_merge_decisions_path) if identity_merge_decisions_path.exists() else {"decisions": []}
        payload = {
            "proposal_id": request.form["proposal_id"],
            "decision": request.form["decision"],
            "reviewer": request.form.get("reviewer", "reviewer"),
            "rationale": request.form.get("rationale", ""),
            "timestamp_utc": now_utc_iso(),
        }
        decisions_data.setdefault("decisions", []).append(payload)
        write_json(identity_merge_decisions_path, decisions_data)
        return index()

    @app.post("/conversation_entity_decision")
    def conversation_entity_decision():
        decisions_data = read_json(conversation_entity_decisions_path) if conversation_entity_decisions_path.exists() else {"decisions": []}
        payload = {
            "proposal_id": request.form["proposal_id"],
            "candidate_name": request.form.get("candidate_name", ""),
            "decision": request.form["decision"],
            "canonical_name": request.form.get("canonical_name", "").strip(),
            "entity_type": request.form.get("entity_type", "term"),
            "reviewer": request.form.get("reviewer", "reviewer"),
            "rationale": request.form.get("rationale", ""),
            "timestamp_utc": now_utc_iso(),
        }
        decisions_data.setdefault("decisions", []).append(payload)
        write_json(conversation_entity_decisions_path, decisions_data)
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

    @app.get("/api/card_decisions")
    def api_card_decisions():
        decisions_data = read_json(card_decisions_path) if card_decisions_path.exists() else {"decisions": []}
        return jsonify(decisions_data)

    @app.get("/api/identity_merge_proposals")
    def api_identity_merge_proposals():
        proposals_data = read_json(identity_merge_proposals_path) if identity_merge_proposals_path.exists() else {"proposals": []}
        return jsonify(proposals_data)

    @app.get("/api/identity_merge_decisions")
    def api_identity_merge_decisions():
        decisions_data = read_json(identity_merge_decisions_path) if identity_merge_decisions_path.exists() else {"decisions": []}
        return jsonify(decisions_data)

    @app.get("/api/conversation_entity_proposals")
    def api_conversation_entity_proposals():
        proposals_data = read_json(conversation_entity_proposals_path) if conversation_entity_proposals_path.exists() else {"proposals": []}
        return jsonify(proposals_data)

    @app.get("/api/conversation_entity_decisions")
    def api_conversation_entity_decisions():
        decisions_data = read_json(conversation_entity_decisions_path) if conversation_entity_decisions_path.exists() else {"decisions": []}
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
        nonlocal new_run_selected
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

        resume_existing = not new_run_selected
        if new_run_selected:
            switch_artifacts_root(new_run_artifacts_root(repo_root))
            new_run_selected = False
            set_pipeline_state(message=f"Created new run: {_display_path(artifacts_root, repo_root)}")

        cmd = _pipeline_worker_command(docx_path, resolved_conversations_root, artifacts_root, resume=resume_existing)
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

    (
        patches_path,
        decisions_path,
        directives_path,
        card_drafts_path,
        card_decisions_path,
        identity_merge_proposals_path,
        identity_merge_decisions_path,
        conversation_entity_proposals_path,
        conversation_entity_decisions_path,
        artifacts_root,
    ) = _resolve_input_paths(
        args.patches,
        args.decisions,
        args.directives,
        args.artifacts_root,
    )
    app = build_app(
        patches_path,
        decisions_path,
        directives_path,
        card_drafts_path,
        card_decisions_path,
        identity_merge_proposals_path,
        identity_merge_decisions_path,
        conversation_entity_proposals_path,
        conversation_entity_decisions_path,
        artifacts_root,
        args.docx,
        args.conversations_root,
    )
    app.run(host=args.host, port=args.port, debug=False)


if __name__ == "__main__":
    main()
