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
from pipeline.card_architecture_agent import pending_card_architecture_actions
from pipeline.common import now_utc_iso, read_json, read_jsonl, safe_uuid, write_json
from pipeline.entity_resolution import normalized_name_key

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
    {"index": 10, "short_label": "10", "name": "Identity Merge"},
    {"index": 11, "short_label": "11", "name": "Card Synthesis"},
    {"index": 12, "short_label": "12", "name": "Notion Export"},
]

NEW_RUN_SELECTOR_VALUE = "__theriac_new_run__"
APP_STATE_FILENAME = "theriac_lore_app_state.json"

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
    .pipeline-stages { display: grid; grid-template-columns: repeat(auto-fit, minmax(82px, 1fr)); gap: 8px; }
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

REVIEW_UI_CSS = """
    :root { color-scheme: light; }
    body { margin: 0; background: #f6f8fa; color: #24292f; font-family: "Segoe UI", Arial, sans-serif; }
    .page { max-width: 1180px; margin: 0 auto; padding: 22px; }
    .page-header { margin: 0 0 14px; }
    .page-header h2 { margin: 0 0 4px; font-size: 22px; font-weight: 700; }
    .subtitle { color: #57606a; margin: 0; line-height: 1.4; }
    .review-panel { border: 1px solid #d8dee4; border-radius: 8px; background: #fff; padding: 18px; box-shadow: 0 1px 2px rgba(31, 35, 40, 0.06); }
    .panel-heading { display: flex; justify-content: space-between; gap: 12px; align-items: flex-start; margin-bottom: 12px; }
    .panel-heading h3 { margin: 2px 0 0; font-size: 24px; line-height: 1.2; }
    .eyebrow { display: block; color: #57606a; font-size: 12px; font-weight: 700; text-transform: uppercase; }
    .pill { display: inline-flex; align-items: center; min-height: 24px; padding: 2px 8px; border-radius: 999px; border: 1px solid #bfdbfe; background: #eff6ff; color: #1d4ed8; font-size: 12px; font-weight: 700; }
    .lead-text { margin: 12px 0; padding: 14px 16px; border-left: 4px solid #0969da; background: #f6f8fa; border-radius: 6px; font-size: 17px; line-height: 1.5; }
    .summary-text { margin: 12px 0; font-size: 16px; line-height: 1.55; }
    .meta-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(170px, 1fr)); gap: 8px; margin: 14px 0; }
    .meta { padding: 9px 10px; border: 1px solid #d8dee4; border-radius: 6px; background: #fff; min-width: 0; }
    .meta b { display: block; color: #57606a; font-size: 12px; margin-bottom: 3px; }
    .meta span { overflow-wrap: anywhere; }
    .notice { margin: 12px 0; padding: 10px 12px; border: 1px solid #f2cc60; border-radius: 6px; background: #fff8c5; color: #3b2300; white-space: pre-wrap; }
    .section { margin-top: 18px; }
    .section h4 { margin: 0 0 8px; font-size: 15px; }
    .section-body { margin: 0; line-height: 1.55; white-space: pre-wrap; }
    .evidence, .sample, .json-drawer { margin-top: 8px; padding: 11px 12px; border: 1px solid #d8dee4; border-radius: 6px; background: #fff; }
    .evidence-title, .sample-title { font-weight: 700; margin-bottom: 4px; overflow-wrap: anywhere; }
    .evidence p, .sample p { margin: 0; line-height: 1.5; white-space: pre-wrap; }
    details.raw-json { margin-top: 16px; }
    details.raw-json summary { cursor: pointer; color: #57606a; font-weight: 700; }
    pre { white-space: pre-wrap; overflow: auto; background: #0b1020; color: #d7e0ff; padding: 10px; border-radius: 6px; line-height: 1.45; }
    textarea { width: 100%; min-height: 86px; box-sizing: border-box; border: 1px solid #d8dee4; border-radius: 6px; padding: 8px; font-family: inherit; }
    input, select { border: 1px solid #d8dee4; border-radius: 6px; padding: 6px 8px; font-family: inherit; }
    label { font-weight: 700; color: #24292f; }
    button { margin: 8px 8px 0 0; border: 1px solid #0969da; border-radius: 6px; background: #0969da; color: #fff; padding: 7px 11px; font-weight: 700; cursor: pointer; }
    button:disabled { opacity: 0.55; cursor: not-allowed; }
    .status { margin: 12px 0; padding: 10px; border-radius: 6px; border: 1px solid #d8dee4; background: #fff; white-space: pre-wrap; }
    .logs { background: #0b1020; color: #d7e0ff; padding: 10px; border-radius: 6px; max-height: 280px; overflow-y: auto; white-space: pre-wrap; }
    .decision-form { margin-top: 18px; padding-top: 14px; border-top: 1px solid #d8dee4; }
    .form-row { margin: 10px 0; }
    .empty { color: #57606a; font-style: italic; }
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
    "card architecture proposal",
)


def is_pipeline_progress_log_line(line: str) -> bool:
    lowered = line.lower()
    return (
        STAGE_LOG_RE.search(line) is not None
        or STAGE_HEARTBEAT_RE.search(line) is not None
        or any(marker in lowered for marker in REVIEW_GATE_LOG_MARKERS)
    )


def _display_stage_index(raw_index: int, stage_text: str) -> int:
    lowered = stage_text.lower()
    if "identity merge" in lowered or "identity cluster" in lowered:
        return 10
    if "card synthesis" in lowered or "card architecture" in lowered or "draft sync" in lowered:
        return 11
    if "notion export" in lowered:
        return 12
    return raw_index


def pipeline_progress_from_logs(
    logs: list[str],
    status: str,
    message: str = "",
    last_exit_code: int | None = None,
) -> dict[str, Any]:
    completed: set[int] = set()
    current_index: int | None = None
    review_index: int | None = None
    latest_index = 0
    latest_heartbeat: dict[str, Any] | None = None
    for line in logs:
        match = STAGE_LOG_RE.search(line)
        if match:
            idx = _display_stage_index(int(match.group(1)), match.group(4))
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
                review_index = idx
            continue
        heartbeat = STAGE_HEARTBEAT_RE.search(line)
        if heartbeat:
            idx = _display_stage_index(int(heartbeat.group(1)), line)
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
    elif status in {"failed", "review_required"} and review_index is not None:
        current_index = review_index
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
    bypass_payload = _read_json_or_default(root / "07_review" / "review_gate_bypass.json", {})
    claim_review_bypassed = isinstance(bypass_payload, dict) and bool(bypass_payload.get("claim_review"))

    def mark_done(index: int, name: str) -> None:
        logs.append(f"[{index}/{stage_total}] START Stage {index:02d} {name}")
        logs.append(f"[{index}/{stage_total}] DONE  Stage {index:02d} {name}")

    def mark_start(index: int, name: str) -> None:
        logs.append(f"[{index}/{stage_total}] START Stage {index:02d} {name}")

    def mark_identity_merge_if_known() -> bool:
        proposals_path = root / "07_review" / "identity_merge_proposals.json"
        decisions_path = root / "07_review" / "identity_merge_decisions.json"
        if not proposals_path.exists() and not decisions_path.exists():
            return False
        mark_start(10, "Identity Merge")
        pending_identity = int(counts.get("identity_merges", 0) or 0)
        if pending_identity > 0:
            logs.append(f"[10/{stage_total}] REVIEW Stage 10 Identity Merge")
            return True
        logs.append(f"[10/{stage_total}] DONE  Stage 10 Identity Merge")
        return False

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
            if claim_review_bypassed:
                logs.append(f"[9/{stage_total}] DONE  Stage 09 Claim Drafting")
            else:
                logs.append(f"[9/{stage_total}] REVIEW Stage 09 Claim Drafting")
                identity_pending = mark_identity_merge_if_known()
                if identity_pending:
                    return {
                        "status": "review_required",
                        "message": f"Paused for identity review: {counts['identity_merges']} identity cluster proposal(s) need review.",
                        "logs": logs,
                    }
                return {
                    "status": "review_required",
                    "message": f"Paused after Stage 09 Claim Drafting: {counts['claims']} claim(s) need review.",
                    "logs": logs,
                }
        else:
            logs.append(f"[9/{stage_total}] DONE  Stage 09 Claim Drafting")

    if not claim_review_bypassed and counts.get("claims", 0) > 0 and not (root / "06_drafts" / "card_drafts" / "claim_drafts.json").exists():
        mark_start(9, "Claim Drafting")
        logs.append(f"[9/{stage_total}] REVIEW Stage 09 Claim Drafting")
        return {
            "status": "review_required",
            "message": f"Paused after Stage 09 Claim Drafting: {counts['claims']} claim(s) need review.",
            "logs": logs,
        }

    if mark_identity_merge_if_known() or counts.get("identity_merges", 0) > 0:
        return {
            "status": "review_required",
            "message": f"Paused for identity review: {counts['identity_merges']} identity cluster proposal(s) need review.",
            "logs": logs,
        }
    if counts.get("card_architecture", 0) > 0:
        mark_start(11, "Card Synthesis")
        logs.append(f"[11/{stage_total}] REVIEW Stage 11 Card Synthesis")
        return {
            "status": "review_required",
            "message": f"Paused for card architecture review: {counts['card_architecture']} architecture action(s) need review.",
            "logs": logs,
        }
    if (root / "07_review" / "card_drafts.json").exists() or (root / "07_review" / "canonical_cards.json").exists():
        mark_start(11, "Card Synthesis")
        if counts.get("cards", 0) > 0:
            logs.append(f"[11/{stage_total}] REVIEW Stage 11 Card Synthesis")
            return {
                "status": "review_required",
                "message": f"Paused for card review: {counts['cards']} card draft(s) need review.",
                "logs": logs,
            }
        logs.append(f"[11/{stage_total}] DONE  Stage 11 Card Synthesis")
    if counts.get("cards", 0) > 0:
        mark_start(11, "Card Synthesis")
        logs.append(f"[11/{stage_total}] REVIEW Stage 11 Card Synthesis")
        return {
            "status": "review_required",
            "message": f"Paused for card review: {counts['cards']} card draft(s) need review.",
            "logs": logs,
        }
    if (root / "08_notion" / "notion_import.ndjson").exists():
        mark_done(12, "Notion Export")
    if logs:
        status = "succeeded" if any(f"[12/{stage_total}] DONE" in line for line in logs) else "idle"
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
    .review-card { max-width: 1080px; }
    .claim-text { font-size: 18px; line-height: 1.45; padding: 14px; border-left: 4px solid #0969da; background: #f6f8fa; border-radius: 6px; }
    .meta-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 8px; margin: 14px 0; }
    .meta { padding: 8px 10px; border: 1px solid #d8dee4; border-radius: 6px; background: #fff; }
    .meta b { display: block; font-size: 12px; color: #57606a; margin-bottom: 3px; }
    .evidence { margin-top: 10px; padding: 10px; border: 1px solid #d8dee4; border-radius: 6px; background: #fff; }
    .evidence-title { font-weight: 700; color: #24292f; }
    details { margin-top: 12px; }
    pre { white-space: pre-wrap; overflow: auto; background: #f6f8fa; padding: 10px; border-radius: 6px; }
    textarea { width: 100%; height: 80px; }
    button { margin-right: 8px; }
    .status { margin: 12px 0; padding: 10px; border-radius: 6px; background: #f6f8fa; white-space: pre-wrap; }
    .logs { background: #0b1020; color: #d7e0ff; padding: 10px; border-radius: 6px; max-height: 280px; overflow-y: auto; white-space: pre-wrap; }
    {{ review_ui_css | safe }}
    {{ pipeline_progress_css | safe }}
  </style>
</head>
<body>
  <div class="page">
  <div class="page-header">
    <h2>THERIAC Claim Review</h2>
    <p class="subtitle">Review one atomic claim at a time, with source snippets visible before the raw data.</p>
  </div>
  {{ run_selector_html | safe }}
  <div class="row">
    <div class="panel review-card review-panel">
      <div class="panel-heading">
        <div>
          <span class="eyebrow">Proposed Claim</span>
          <h3>{{ claim.target_entity_name }}</h3>
        </div>
        <span class="pill">{{ claim.confidence }}</span>
      </div>
      <div class="lead-text">{{ claim.claim_text }}</div>
      <div class="meta-grid">
        <div class="meta"><b>Claim Type</b><span>{{ claim.claim_type }}</span></div>
        <div class="meta"><b>Knowledge Track</b><span>{{ claim.knowledge_track }}</span></div>
        <div class="meta"><b>Status</b><span>{{ claim.status }}</span></div>
        <div class="meta"><b>Sources</b><span>{{ claim.source_snippet_ids|length }}</span></div>
        <div class="meta"><b>Claim ID</b><span>{{ claim.claim_id }}</span></div>
      </div>
      {% if claim.support_warnings %}
        <div class="notice"><b>Cautions:</b> {{ claim.support_warnings|join(", ") }}</div>
      {% endif %}
      {% if claim.contradiction_notes %}
        <div class="notice"><b>Contradiction notes:</b> {{ claim.contradiction_notes }}</div>
      {% endif %}
      {% if claim.auto_review_attention %}
        <div class="notice">
          <b>Auto-review requested human attention:</b>
          {{ claim.auto_review_attention.human_review_reason or "No reason supplied." }}
        </div>
      {% endif %}
      <div class="section">
        <h4>Evidence Preview</h4>
        {% for evidence in claim_evidence %}
          <div class="evidence">
            <div class="evidence-title">{{ evidence.snippet_id }}{% if evidence.topic %} | {{ evidence.topic }}{% endif %}</div>
            <p>{{ evidence.text }}</p>
          </div>
        {% else %}
          <p class="empty">{{ claim.source_snippet_ids|join(", ") }}</p>
        {% endfor %}
      </div>
      {% if claim.proposed_relationship_hints %}
      <div class="section">
        <h4>Relationship Hints</h4>
        {% for hint in claim.proposed_relationship_hints[:3] %}
          <div class="sample">
            <div class="sample-title">{{ hint.relation_type }}{% if hint.confidence %} | {{ hint.confidence }}{% endif %}</div>
            <p>{{ hint.note }}</p>
          </div>
        {% endfor %}
      </div>
      {% endif %}
      <details class="raw-json">
        <summary>Raw claim JSON</summary>
        <pre>{{ claim | tojson(indent=2) }}</pre>
      </details>
    </div>
  </div>
  <form class="decision-form" method="post" action="/decision">
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
    <button id="run-pipeline-btn" type="submit" {% if pipeline_status == "running" %}disabled{% endif %}>Run / Resume Full Pipeline</button>
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
  </div>
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
    {{ review_ui_css | safe }}
    {{ pipeline_progress_css | safe }}
  </style>
</head>
<body>
  <div class="page">
  <div class="page-header">
    <h2>THERIAC Card Review</h2>
    <p class="subtitle">Review the synthesized wiki-style card before promotion to canonical output.</p>
  </div>
  {{ run_selector_html | safe }}
  {{ pipeline_progress_html | safe }}
  <div class="review-panel">
    <div class="panel-heading">
      <div>
        <span class="eyebrow">Synthesized Draft Card</span>
        <h3>{{ card.canonical_name }}</h3>
      </div>
      <span class="pill">{{ card.status }}</span>
    </div>
    <div class="meta-grid">
      <div class="meta"><b>Entity Type</b><span>{{ card.entity_type }}</span></div>
      <div class="meta"><b>Card ID</b><span>{{ card.card_id }}</span></div>
      <div class="meta"><b>Evidence Items</b><span>{{ card.source_evidence|length }}</span></div>
      <div class="meta"><b>Sections</b><span>{{ card_sections|length }}</span></div>
    </div>
    <div class="section">
      <h4>Lead Summary</h4>
      <p class="summary-text">{{ card.summary }}</p>
    </div>
    {% for section in card_sections %}
      <div class="section">
        <h4>{{ section.title }}</h4>
        <p class="section-body">{{ section.text }}</p>
      </div>
    {% else %}
      <p class="empty">No expanded sections were included in this draft.</p>
    {% endfor %}
    {% if word_counts %}
      <div class="section">
        <h4>Section Word Counts</h4>
        <div class="meta-grid">
          {% for key, value in word_counts.items() %}
            <div class="meta"><b>{{ key.replace("_", " ").title() }}</b><span>{{ value }}</span></div>
          {% endfor %}
        </div>
      </div>
    {% endif %}
    <details class="raw-json">
      <summary>Raw card JSON</summary>
      <pre>{{ card | tojson(indent=2) }}</pre>
    </details>
  </div>
  <form class="decision-form" method="post" action="/card_decision">
    <input type="hidden" name="card_id" value="{{ card.card_id }}" />
    <div class="form-row">
      <label>Reviewer</label>
      <input type="text" name="reviewer" value="human_reviewer" />
    </div>
    <div class="form-row">
      <label>Rationale</label>
      <textarea name="rationale"></textarea>
    </div>
    <div class="form-row">
      <label>Edited Summary (optional)</label>
      <textarea name="edited_summary"></textarea>
    </div>
    <button name="decision" value="approve">Approve Canonical</button>
    <button name="decision" value="reject">Reject</button>
    <button name="decision" value="defer">Defer</button>
    <button name="decision" value="needs_more_context">Needs More Context</button>
  </form>
  <hr />
  <p class="status">After card review decisions are saved, rerun Stage 11 to write approved cards to canonical_cards.json and persistent review memory.</p>
  </div>
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
    {{ review_ui_css | safe }}
    {{ pipeline_progress_css | safe }}
  </style>
</head>
<body>
  <div class="page">
  <div class="page-header">
    <h2>THERIAC Entity Merge Review</h2>
    <p class="subtitle">Approve identity clusters, choosing one canonical wiki-page title before card synthesis.</p>
  </div>
  {{ run_selector_html | safe }}
  {{ pipeline_progress_html | safe }}
  <div class="review-panel">
    <div class="panel-heading">
      <div>
        <span class="eyebrow">Proposed Identity Cluster</span>
        <h3>
          {% if proposal.member_entities %}
            {% for member in proposal.member_entities[:4] %}{{ member.canonical_name or member.entity_id }}{% if not loop.last %} + {% endif %}{% endfor %}
            -> {{ proposal.canonical_name or proposal.target_entity_name or proposal.target_entity_id }}
          {% else %}
            {{ proposal.source_entity_name or proposal.source_entity_id }} -> {{ proposal.target_entity_name or proposal.target_entity_id }}
          {% endif %}
        </h3>
      </div>
      <span class="pill">{{ proposal.confidence }}</span>
    </div>
    <div class="meta-grid">
      <div class="meta"><b>Merge Type</b><span>{{ proposal.merge_type }}</span></div>
      <div class="meta"><b>Proposal ID</b><span>{{ proposal.proposal_id }}</span></div>
      <div class="meta"><b>Canonical Entity ID</b><span>{{ proposal.canonical_entity_id or proposal.target_entity_id }}</span></div>
      <div class="meta"><b>Members</b><span>{{ proposal.member_entity_ids|join(", ") if proposal.member_entity_ids else proposal.source_entity_id ~ ", " ~ proposal.target_entity_id }}</span></div>
    </div>
    {% if proposal.alias_texts %}
      <div class="section">
        <h4>Aliases / Working Names</h4>
        <p class="section-body">{{ proposal.alias_texts|join(", ") }}</p>
      </div>
    {% endif %}
    {% if proposal.suggested_split_entity_ids %}
      <div class="section">
        <h4>Suggested Exclusions</h4>
        <p class="section-body">{{ proposal.suggested_split_entity_ids|join(", ") }}</p>
      </div>
    {% endif %}
    {% if proposal.member_edges %}
      <div class="section">
        <h4>Identity Evidence Edges</h4>
        <p class="section-body">
          {% for edge in proposal.member_edges %}
            {{ edge.source_entity_name }} -> {{ edge.target_entity_name }}{% if not loop.last %}<br />{% endif %}
          {% endfor %}
        </p>
      </div>
    {% endif %}
    <div class="section">
      <h4>Rationale</h4>
      <p class="section-body">{{ proposal.rationale or proposal.reason }}</p>
    </div>
    {% if proposal.evidence_claim_ids %}
      <div class="section">
        <h4>Evidence Claim IDs</h4>
        <p class="section-body">{{ proposal.evidence_claim_ids|join(", ") }}</p>
      </div>
    {% endif %}
    <details class="raw-json">
      <summary>Raw merge JSON</summary>
      <pre>{{ proposal | tojson(indent=2) }}</pre>
    </details>
  </div>
  <form class="decision-form" method="post" action="/identity_merge_decision">
    <input type="hidden" name="proposal_id" value="{{ proposal.proposal_id }}" />
    <div class="form-row">
      <label>Reviewer</label>
      <input type="text" name="reviewer" value="human_reviewer" />
    </div>
    <div class="form-row">
      <label>Canonical Name</label>
      <input type="text" name="canonical_name" value="{{ proposal.canonical_name or proposal.target_entity_name or '' }}" />
    </div>
    <div class="form-row">
      <label>Rationale</label>
      <textarea name="rationale"></textarea>
    </div>
    <button name="decision" value="approve">Approve Cluster</button>
    <button name="decision" value="reject">Reject</button>
    <button name="decision" value="defer">Defer</button>
    <button name="decision" value="needs_more_context">Needs More Context</button>
  </form>
  <hr />
  <p class="status">After entity merge decisions are saved, rerun Stage 10 to clear identity review, then Stage 11 will regroup claims before card synthesis.</p>
  </div>
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
    input, select { min-width: 260px; }
    {{ review_ui_css | safe }}
    {{ pipeline_progress_css | safe }}
  </style>
</head>
<body>
  <div class="page">
  <div class="page-header">
    <h2>THERIAC Conversation Entity Review</h2>
    <p class="subtitle">Decide whether this observed term deserves a canonical entity, a corrected type, or rejection.</p>
  </div>
  {{ run_selector_html | safe }}
  {{ pipeline_progress_html | safe }}
  <div class="review-panel">
    <div class="panel-heading">
      <div>
        <span class="eyebrow">Candidate Entity</span>
        <h3>{{ proposal.candidate_name }}</h3>
      </div>
      <span class="pill">{{ proposal.review_priority or proposal.triage_status or "pending" }}</span>
    </div>
    <div class="meta-grid">
      <div class="meta"><b>Proposed Type</b><span>{{ proposal.proposed_entity_type }}</span></div>
      <div class="meta"><b>Suggested Canonical</b><span>{{ proposal.suggested_canonical_name or proposal.candidate_name }}</span></div>
      <div class="meta"><b>Evidence Count</b><span>{{ proposal.evidence_count }}</span></div>
      <div class="meta"><b>Recency</b><span>{{ proposal.recency_window }}</span></div>
      <div class="meta"><b>Tracks</b><span>{{ proposal.knowledge_tracks|join(", ") }}</span></div>
      <div class="meta"><b>First Seen</b><span>{{ proposal.first_seen_timestamp_utc }}</span></div>
      <div class="meta"><b>Last Seen</b><span>{{ proposal.last_seen_timestamp_utc }}</span></div>
    </div>
    {% if proposal.triage_reason or proposal.type_review_notes %}
      <div class="notice">
        {% if proposal.triage_reason %}<b>Triage:</b> {{ proposal.triage_reason }}{% endif %}
        {% if proposal.type_review_notes %}<br /><b>Type notes:</b> {{ proposal.type_review_notes }}{% endif %}
      </div>
    {% endif %}
    {% if proposal.type_vote_totals %}
      <div class="section">
        <h4>Type Evidence</h4>
        <div class="meta-grid">
          {% for key, value in proposal.type_vote_totals.items() %}
            <div class="meta"><b>{{ key }}</b><span>{{ value }}</span></div>
          {% endfor %}
        </div>
      </div>
    {% endif %}
    {% if alias_rows %}
      <div class="section">
        <h4>Alias Candidates</h4>
        {% for alias in alias_rows %}
          <div class="sample"><p>{{ alias }}</p></div>
        {% endfor %}
      </div>
    {% endif %}
    <div class="section">
      <h4>Evidence Samples</h4>
      {% for sample in proposal.sample_texts[:5] %}
        <div class="sample"><p>{{ sample }}</p></div>
      {% else %}
        <p class="empty">No evidence sample text was included.</p>
      {% endfor %}
    </div>
    <details class="raw-json">
      <summary>Raw candidate JSON</summary>
      <pre>{{ proposal | tojson(indent=2) }}</pre>
    </details>
  </div>
  <form class="decision-form" method="post" action="/conversation_entity_decision">
    <input type="hidden" name="proposal_id" value="{{ proposal.proposal_id }}" />
    <input type="hidden" name="candidate_name" value="{{ proposal.candidate_name }}" />
    <div class="form-row">
      <label>Canonical Name</label>
      <input type="text" name="canonical_name" value="{{ proposal.suggested_canonical_name or proposal.candidate_name }}" />
    </div>
    <div class="form-row">
      <label>Entity Type</label>
      <select name="entity_type">
        {% for entity_type in entity_types %}
          <option value="{{ entity_type }}" {% if entity_type == proposal.proposed_entity_type %}selected{% endif %}>{{ entity_type }}</option>
        {% endfor %}
      </select>
    </div>
    <div class="form-row">
      <label>Reviewer</label>
      <input type="text" name="reviewer" value="human_reviewer" />
    </div>
    <div class="form-row">
      <label>Rationale</label>
      <textarea name="rationale"></textarea>
    </div>
    <button name="decision" value="approve">Approve Entity</button>
    <button name="decision" value="reject">Reject</button>
    <button name="decision" value="defer">Defer</button>
    <button name="decision" value="needs_more_context">Needs More Context</button>
  </form>
  <hr />
  <p class="status">After conversation entity decisions are saved, rerun the pipeline from Stage 07 or rerun the full pipeline so approved entities can be grouped and drafted into claims.</p>
  </div>
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
    .panel { border: 1px solid #ddd; padding: 12px; border-radius: 6px; max-width: 860px; }
    code { background: #f6f8fa; padding: 2px 4px; border-radius: 4px; }
    {{ review_ui_css | safe }}
    {{ pipeline_progress_css | safe }}
  </style>
</head>
<body>
  <div class="page">
  <div class="page-header">
    <h2>THERIAC Review</h2>
    <p class="subtitle">No pending review item is ready yet for this run.</p>
  </div>
  {{ run_selector_html | safe }}
  <div class="review-panel">
    <p>{{ bootstrap_reason }}</p>
    <p>Expected claim draft path: <code>{{ patches_path }}</code></p>
    <p>Full pipeline target: <code>{{ artifacts_root }}</code></p>
    <p>DOCX: <code>{{ docx_hint }}</code></p>
    <p>Conversations: <code>{{ conversations_root }}</code></p>
    <form method="post" action="/run_full_pipeline">
      <button id="run-pipeline-btn" type="submit" {% if pipeline_status == "running" %}disabled{% endif %}>Run / Resume Full Pipeline</button>
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
  </div>
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
    {{ review_ui_css | safe }}
    {{ pipeline_progress_css | safe }}
  </style>
</head>
<body>
  <div class="page">
  <div class="page-header">
    <h2>THERIAC Review</h2>
  </div>
  {{ run_selector_html | safe }}
  <div class="status">{{ message }}</div>
  </div>
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
    card_architecture_decisions_path: Path | None = None,
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
    if card_architecture_decisions_path is not None and not card_architecture_decisions_path.exists():
        write_json(card_architecture_decisions_path, {"decisions": []})


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
        return None, "No synthesized card drafts file was found yet. Run Stage 11 after reviewing claims and identity merges."
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


def source_snippet_previews_for_claim(root: Path, claim: dict[str, Any], limit: int = 4) -> list[dict[str, str]]:
    source_ids = [str(item) for item in claim.get("source_snippet_ids", []) or [] if str(item).strip()]
    if not source_ids:
        return []
    source_path = root / "03_relevance" / "snippets_candidates.jsonl"
    if not source_path.exists():
        return []
    wanted = set(source_ids[:limit])
    previews: list[dict[str, str]] = []
    for row in read_jsonl(source_path):
        snippet_id = str(row.get("snippet_id", ""))
        if snippet_id not in wanted:
            continue
        text = str(row.get("patch_item_text") or row.get("display_text_normalized") or row.get("conversation_patch_summary") or "")
        topic = str(row.get("conversation_topic_label") or row.get("conversation_patch_topic_label") or "")
        previews.append(
            {
                "snippet_id": snippet_id,
                "topic": topic,
                "text": re.sub(r"\s+", " ", text).strip()[:1200],
            }
        )
        if len(previews) >= limit:
            break
    return previews


def card_review_sections(card: dict[str, Any]) -> list[dict[str, str]]:
    details = card.get("details") if isinstance(card.get("details"), dict) else {}
    sections = details.get("sections") if isinstance(details.get("sections"), dict) else {}
    order = [
        ("background", "Background"),
        ("role_in_story", "Role In Story"),
        ("relationships", "Relationships"),
        ("timeline", "Timeline"),
        ("inspirations", "Inspirations"),
        ("open_questions", "Open Questions"),
    ]
    blocks: list[dict[str, str]] = []
    for key, title in order:
        text = str(sections.get(key, "")).strip()
        if text:
            blocks.append({"key": key, "title": title, "text": text})
    for key, value in sections.items():
        if key in {item[0] for item in order}:
            continue
        text = str(value).strip()
        if text:
            blocks.append({"key": str(key), "title": str(key).replace("_", " ").title(), "text": text})
    return blocks


def card_word_counts(card: dict[str, Any]) -> dict[str, Any]:
    details = card.get("details") if isinstance(card.get("details"), dict) else {}
    counts = details.get("section_word_counts")
    return counts if isinstance(counts, dict) else {}


def conversation_entity_alias_rows(proposal: dict[str, Any]) -> list[str]:
    rows: list[str] = []
    for alias in proposal.get("alias_candidates", []) or []:
        if isinstance(alias, dict):
            candidate = alias.get("candidate_name") or alias.get("alias") or alias.get("name")
            target = alias.get("canonical_name") or alias.get("target_name") or proposal.get("suggested_canonical_name") or proposal.get("candidate_name")
            confidence = alias.get("confidence")
            suffix = f" ({confidence})" if confidence is not None else ""
            if candidate:
                rows.append(f"{candidate} -> {target}{suffix}")
        elif str(alias).strip():
            rows.append(str(alias).strip())
    return rows[:8]


def _read_json_or_default(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        return read_json(path)
    except Exception:
        return default


def app_state_path(repo_root: Path) -> Path:
    return repo_root / "artifacts" / APP_STATE_FILENAME


def load_last_open_artifacts_root(repo_root: Path) -> Path | None:
    payload = _read_json_or_default(app_state_path(repo_root), {})
    if not isinstance(payload, dict):
        return None
    raw = str(payload.get("last_open_artifacts_root", "")).strip()
    if not raw:
        return None
    path = Path(raw)
    if not path.is_absolute():
        path = repo_root / path
    try:
        resolved = path.resolve()
    except OSError:
        return None
    if not resolved.exists() or not resolved.is_dir():
        return None
    return resolved


def save_last_open_artifacts_root(repo_root: Path, artifacts_root: Path) -> None:
    try:
        resolved = artifacts_root.resolve()
    except OSError:
        return
    if not resolved.exists() or not resolved.is_dir():
        return
    path = app_state_path(repo_root)
    payload = _read_json_or_default(path, {})
    if not isinstance(payload, dict):
        payload = {}
    payload["last_open_artifacts_root"] = str(resolved)
    payload["updated_at_utc"] = now_utc_iso()
    write_json(path, payload)


def _decision_ids(path: Path, id_fields: list[str]) -> set[str]:
    payload = _read_json_or_default(path, {"decisions": []})
    ids: set[str] = set()
    for decision in payload.get("decisions", []):
        for field in id_fields:
            value = str(decision.get(field, "")).strip()
            if value:
                ids.add(value)
    return ids


def _human_decision_ids(path: Path, id_fields: list[str]) -> set[str]:
    payload = _read_json_or_default(path, {"decisions": []})
    ids: set[str] = set()
    for decision in payload.get("decisions", []) if isinstance(payload, dict) else []:
        if not isinstance(decision, dict):
            continue
        reviewer = str(decision.get("reviewer", "")).strip().lower()
        is_human = bool(decision.get("human_override")) or (
            bool(reviewer) and "auto_review" not in reviewer and "gemini_auto" not in reviewer
        )
        if not is_human:
            continue
        for field in id_fields:
            value = str(decision.get(field, "")).strip()
            if value:
                ids.add(value)
    return ids


def _claim_attention_by_id(root: Path) -> dict[str, dict[str, Any]]:
    payload = _read_json_or_default(root / "07_review" / "claim_auto_review_attention.json", {"items": []})
    by_id: dict[str, dict[str, Any]] = {}
    for item in payload.get("items", []) if isinstance(payload, dict) else []:
        if not isinstance(item, dict):
            continue
        claim_id = str(item.get("claim_id", "")).strip()
        if claim_id:
            by_id[claim_id] = item
    return by_id


def _pending_claim_attention_ids(root: Path) -> set[str]:
    attention_by_id = _claim_attention_by_id(root)
    human_reviewed = _human_decision_ids(root / "07_review" / "claim_review_decisions.json", ["claim_id"])
    return {claim_id for claim_id in attention_by_id if claim_id not in human_reviewed}


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
    claim_attention_ids = _pending_claim_attention_ids(root)
    pending_claims = [
        claim
        for claim in claims
        if str(claim.get("claim_id", "")).strip()
        and (
            str(claim.get("claim_id", "")) not in claim_decisions
            or str(claim.get("claim_id", "")) in claim_attention_ids
        )
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

    try:
        pending_architecture = pending_card_architecture_actions(
            root / "07_review" / "card_architecture_proposals.json",
            root / "07_review" / "card_architecture_decisions.json",
        )
    except Exception:
        pending_architecture = []

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
        "card_architecture": len(pending_architecture),
        "cards": len(pending_cards),
    }


def pending_review_total(counts: dict[str, int]) -> int:
    return sum(int(value) for value in counts.values())


def pending_review_summary(counts: dict[str, int]) -> str:
    labels = [
        ("conversation_entities", "conversation entities"),
        ("claims", "claims"),
        ("identity_merges", "identity clusters"),
        ("card_architecture", "card architecture actions"),
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
    ignore_pending: bool = False,
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
    if ignore_pending:
        args.append("--ignore-pending")
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
        save_last_open_artifacts_root(repo_root, artifacts_root)

    save_last_open_artifacts_root(repo_root, artifacts_root)

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
            "review_ui_css": REVIEW_UI_CSS,
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
                alias_rows=conversation_entity_alias_rows(pending_entities[0]),
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
        claim_attention = _claim_attention_by_id(artifacts_root)
        claim_attention_ids = _pending_claim_attention_ids(artifacts_root)
        pending = [
            {**p, "auto_review_attention": claim_attention.get(str(p.get("claim_id", "")), {})}
            for p in patches
            if p["claim_id"] not in decided or str(p.get("claim_id", "")) in claim_attention_ids
        ]
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
                card_sections=card_review_sections(pending_cards[0]),
                word_counts=card_word_counts(pending_cards[0]),
                **common_template_vars(pipeline_snapshot),
            )
        return render_template_string(
            HTML,
            claim=pending[0],
            claim_evidence=source_snippet_previews_for_claim(artifacts_root, pending[0]),
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
        canonical_name = request.form.get("canonical_name", "").strip()
        if canonical_name:
            payload["canonical_name"] = canonical_name
            proposals_data = read_json(identity_merge_proposals_path) if identity_merge_proposals_path.exists() else {"proposals": []}
            for proposal in proposals_data.get("proposals", []):
                if str(proposal.get("proposal_id", "")) != str(payload["proposal_id"]):
                    continue
                for member in proposal.get("member_entities", []) or []:
                    names = [str(member.get("canonical_name", "")), *[str(alias) for alias in member.get("aliases", []) or []]]
                    if any(normalized_name_key(name) == normalized_name_key(canonical_name) for name in names if name.strip()):
                        payload["canonical_entity_id"] = str(member.get("entity_id", ""))
                        break
                break
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

    repo_root = _project_root()
    initial_artifacts_root = args.artifacts_root
    if initial_artifacts_root is None and args.patches is None and args.decisions is None and args.directives is None:
        initial_artifacts_root = load_last_open_artifacts_root(repo_root)

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
        initial_artifacts_root,
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
        repo_root,
    )
    app.run(host=args.host, port=args.port, debug=False)


if __name__ == "__main__":
    main()
