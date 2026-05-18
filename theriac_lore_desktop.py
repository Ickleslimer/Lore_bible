from __future__ import annotations

import argparse
import json
import os
import queue
import re
import subprocess
import sys
import threading
import urllib.error
import urllib.request
from pathlib import Path
from tkinter import messagebox, scrolledtext
import tkinter as tk
from tkinter import ttk
from typing import Any

from pipeline.common import now_utc_iso, read_json, safe_uuid, write_json
from pipeline.ui_review_app import (
    NEW_RUN_SELECTOR_VALUE,
    PIPELINE_STAGES,
    _decision_ids,
    _display_path,
    _ensure_review_files,
    _load_card_drafts_or_reason,
    _load_patches_or_reason,
    _pipeline_worker_command,
    _read_json_or_default,
    _resolve_docx,
    _resolve_input_paths,
    discover_review_runs,
    is_pipeline_progress_log_line,
    new_run_artifacts_root,
    pending_review_counts_for_root,
    pending_review_summary,
    pending_review_total,
    pipeline_progress_artifact_snapshot,
    pipeline_progress_from_logs,
)

ENTITY_REVIEW_TYPES = ["term", "theme", "quest", "event", "character", "faction", "organization", "location", "timeline_node"]


def looks_like_project_root(path: Path) -> bool:
    return (
        (path / "config" / "pipeline_config.json").exists()
        and (path / "theriac-coda---lore-bible.docx").exists()
    )


def find_project_root(explicit_root: Path | None = None) -> Path:
    if explicit_root is not None:
        return explicit_root.resolve()
    env_root = os.environ.get("THERIAC_LORE_ROOT")
    if env_root:
        return Path(env_root).resolve()
    starts = [Path.cwd()]
    if getattr(sys, "frozen", False):
        starts.append(Path(sys.executable).resolve().parent)
    else:
        starts.append(Path(__file__).resolve().parent)
    for start in starts:
        for candidate in (start, *start.parents):
            if looks_like_project_root(candidate):
                return candidate
    return Path.cwd().resolve()


def load_project_env(repo_root: Path) -> None:
    env_path = repo_root / ".env"
    if not env_path.exists():
        return
    try:
        lines = env_path.read_text(encoding="utf-8", errors="replace").splitlines()
    except Exception:
        return
    for line in lines:
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        if "=" in stripped:
            key, value = stripped.split("=", 1)
        elif ":" in stripped:
            key, value = stripped.split(":", 1)
        else:
            continue
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and value and not os.environ.get(key):
            os.environ[key] = value


TERMINAL_GEMINI_BATCH_STATES = {
    "BATCH_STATE_SUCCEEDED",
    "BATCH_STATE_FAILED",
    "BATCH_STATE_CANCELLED",
    "BATCH_STATE_EXPIRED",
    "JOB_STATE_SUCCEEDED",
    "JOB_STATE_FAILED",
    "JOB_STATE_CANCELLED",
    "JOB_STATE_EXPIRED",
}

REVIEW_REQUIRED_MARKERS = (
    "Pipeline paused for review",
    "requiring review",
    "conversation entity proposal",
    "identity merge proposal",
)

RUN_FROM_STAGE05_STAGE_INDEX_MAP = {
    1: 5,
    2: 6,
    3: 7,
    4: 8,
    5: 9,
}


def gemini_api_key_from_env() -> str:
    for key_name in ("GEMINI_API_KEY", "GOOGLE_API_KEY", "GOOGLE_AI_API_KEY"):
        value = os.environ.get(key_name, "").strip().strip('"').strip("'")
        if value:
            return value
    return ""


def cancel_gemini_batch(job_name: str) -> str:
    api_key = gemini_api_key_from_env()
    if not api_key:
        return f"Skipped Gemini batch cancel for {job_name}: no API key in environment."
    url = f"https://generativelanguage.googleapis.com/v1beta/{job_name.lstrip('/')}:cancel"
    req = urllib.request.Request(
        url=url,
        data=b"{}",
        headers={
            "Content-Type": "application/json",
            "x-goog-api-key": api_key,
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=30):
            return f"Cancel request sent for Gemini batch {job_name}."
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace") if hasattr(exc, "read") else ""
        return f"Gemini batch cancel failed for {job_name}: HTTP {exc.code} {exc.reason} {body[:180]}"
    except Exception as exc:
        return f"Gemini batch cancel failed for {job_name}: {exc}"


def cancellable_gemini_batches_for_run(artifacts_root: Path) -> list[str]:
    latest_by_job: dict[str, dict[str, Any]] = {}
    for status_path in artifacts_root.rglob("gemini_batch_status.jsonl"):
        try:
            lines = status_path.read_text(encoding="utf-8", errors="replace").splitlines()
        except Exception:
            continue
        for line in lines:
            try:
                event = json.loads(line)
            except Exception:
                continue
            job_name = str(event.get("job_name", "")).strip()
            if not job_name:
                continue
            previous = latest_by_job.get(job_name)
            if previous is None or float(event.get("timestamp_epoch_s", 0) or 0) >= float(previous.get("timestamp_epoch_s", 0) or 0):
                latest_by_job[job_name] = event
    jobs: list[str] = []
    for job_name, event in latest_by_job.items():
        state = str(event.get("state", "")).strip()
        if state not in TERMINAL_GEMINI_BATCH_STATES:
            jobs.append(job_name)
    return sorted(jobs)


def stop_process_tree_by_pid(pid: int) -> None:
    if os.name == "nt":
        subprocess.run(
            ["taskkill", "/PID", str(pid), "/T", "/F"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
            check=False,
        )
    else:
        subprocess.run(["kill", "-TERM", str(pid)], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=False)


def stop_process_tree(process: subprocess.Popen[str]) -> None:
    if process.poll() is not None:
        return
    stop_process_tree_by_pid(process.pid)


def process_id_exists(pid: int) -> bool:
    if pid <= 0:
        return False
    if os.name == "nt":
        result = subprocess.run(
            [
                "powershell",
                "-NoProfile",
                "-Command",
                f"$p = Get-Process -Id {pid} -ErrorAction SilentlyContinue; if ($p) {{ '1' }}",
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            text=True,
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
            check=False,
        )
        return result.stdout.strip() == "1"
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    return True


def extract_artifacts_root_from_command_line(command_line: str) -> Path | None:
    match = re.search(
        r"--artifacts-root(?:=|\s+)(?:\"([^\"]+)\"|'([^']+)'|(.+?)(?=\s+--[A-Za-z0-9_-]+|\s*$))",
        command_line,
    )
    if not match:
        return None
    value = next((group for group in match.groups() if group), "").strip()
    return Path(value).resolve() if value else None


def classify_pipeline_command(command_line: str) -> str:
    if "--pipeline-worker" in command_line:
        return "full_pipeline"
    if "pipeline.run_from_b4" in command_line or "run_from_b4" in command_line:
        return "run_from_b4"
    return "pipeline"


def discover_running_pipeline_processes() -> list[dict[str, Any]]:
    if os.name != "nt":
        return []
    script = (
        "$rows = Get-CimInstance Win32_Process | "
        "Where-Object { $_.CommandLine -and ($_.CommandLine -match '--artifacts-root') "
        "-and ($_.CommandLine -match 'pipeline\\.run_from_b4|--pipeline-worker') "
        "-and ($_.Name -notmatch 'powershell') }; "
        "$rows | Select-Object ProcessId,Name,CommandLine | ConvertTo-Json -Depth 3"
    )
    result = subprocess.run(
        ["powershell", "-NoProfile", "-Command", script],
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        text=True,
        creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
        check=False,
    )
    if result.returncode != 0 or not result.stdout.strip():
        return []
    try:
        raw = json.loads(result.stdout)
    except json.JSONDecodeError:
        return []
    rows = raw if isinstance(raw, list) else [raw]
    processes: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        command_line = str(row.get("CommandLine", ""))
        artifacts_root = extract_artifacts_root_from_command_line(command_line)
        if artifacts_root is None:
            continue
        processes.append(
            {
                "pid": int(row.get("ProcessId", 0) or 0),
                "name": str(row.get("Name", "")),
                "command_line": command_line,
                "artifacts_root": artifacts_root,
                "kind": classify_pipeline_command(command_line),
            }
        )
    return [process for process in processes if process["pid"] > 0]


def attach_log_paths_for_run(artifacts_root: Path, kind: str) -> list[Path]:
    candidates: list[Path] = []
    patterns = (
        [
            "run_from_stage05_*.err.log",
            "run_from_stage05_*.log",
            "run_from_b4_*.err.log",
            "run_from_b4_*.log",
        ]
        if kind == "run_from_b4"
        else ["*.log", "*.err.log"]
    )
    for pattern in patterns:
        for path in artifacts_root.glob(pattern):
            if path.is_file() and path not in candidates:
                candidates.append(path)
    candidates.sort(key=lambda path: path.stat().st_mtime, reverse=True)
    return candidates[:2]


def artifact_sort_key(root: Path) -> tuple[int, int, float, str]:
    counts = pending_review_counts_for_root(root)
    total = pending_review_total(counts)
    has_gate = int(counts.get("conversation_entities", 0) > 0 or counts.get("identity_merges", 0) > 0)
    latest = 0.0
    for marker in [
        root / "05_alias" / "conversation_entity_proposals.json",
        root / "07_review" / "identity_merge_proposals.json",
        root / "06_drafts" / "card_drafts" / "claim_drafts.json",
        root / "07_review" / "card_drafts.json",
    ]:
        if marker.exists():
            latest = max(latest, marker.stat().st_mtime)
    return (has_gate, total, latest, str(root))


def _as_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    if value in (None, ""):
        return []
    return [value]


def _as_text_list(value: Any) -> list[str]:
    return [str(item).strip() for item in _as_list(value) if str(item).strip()]


def _join_preview(values: list[str], limit: int = 4) -> str:
    clean = [value for value in values if value]
    if len(clean) <= limit:
        return ", ".join(clean)
    return ", ".join(clean[:limit]) + f" +{len(clean) - limit}"


def candidate_inventory_category(item: dict[str, Any]) -> str:
    if item.get("group_kind") == "alias_review_group":
        return "lore"
    tracks = {value.lower() for value in _as_text_list(item.get("knowledge_tracks"))}
    counts = item.get("knowledge_track_counts")
    if isinstance(counts, dict):
        tracks.update(str(key).strip().lower() for key, value in counts.items() if str(key).strip() and int(value or 0) > 0)
    topics = {value.lower() for value in _as_text_list(item.get("candidate_topics"))}
    source_kinds = {value.lower() for value in _as_text_list(item.get("source_kinds"))}
    triage_reason = str(item.get("triage_reason", "")).lower()
    sample_text = "\n".join(_as_text_list(item.get("sample_texts"))).lower()
    meta_signals = {
        "meta" in tracks,
        any(kind.startswith("patch_note_meta") for kind in source_kinds),
        any(topic in topics for topic in {"production", "design", "marketing", "scope", "staffing"}),
        "project/team contributor" in triage_reason,
        "meta inventory" in triage_reason,
        "external-media" in triage_reason,
        "reference/inspiration" in triage_reason,
        any(marker in sample_text for marker in ("for the game", "for theriac", "artist", "animation team", "logo", "project")),
    }
    lore_signals = {
        "lore" in tracks,
        any(topic in topics for topic in {"entity", "quest", "event", "theme", "mechanic"}),
        any(kind.startswith("patch_note_lore") for kind in source_kinds),
    }
    has_meta = any(meta_signals)
    has_lore = any(lore_signals)
    if has_meta and has_lore and "project/team contributor" not in triage_reason:
        return "mixed"
    if has_meta:
        return "meta"
    if has_lore:
        return "lore"
    return "unknown"


def candidate_inventory_bucket_label(bucket: str) -> str:
    return {
        "proposals": "promoted",
        "candidate_inventory": "demoted",
        "suppressed_candidates": "suppressed",
    }.get(bucket, bucket)


def _latest_conversation_entity_decisions(decisions_path: Path | None) -> dict[str, dict[str, Any]]:
    if decisions_path is None:
        return {}
    payload = _read_json_or_default(decisions_path, {"decisions": []})
    latest: dict[str, dict[str, Any]] = {}

    def priority(decision: dict[str, Any]) -> int:
        reviewer = str(decision.get("reviewer", "")).strip().lower()
        if bool(decision.get("human_override")):
            return 2
        if reviewer and "auto_review" not in reviewer and "gemini_auto" not in reviewer:
            return 2
        return 1

    for decision in payload.get("decisions", []) if isinstance(payload, dict) else []:
        if not isinstance(decision, dict):
            continue
        proposal_id = str(decision.get("proposal_id", "")).strip()
        if proposal_id:
            existing = latest.get(proposal_id)
            if existing is None or priority(decision) >= priority(existing):
                latest[proposal_id] = decision
    return latest


def _conversation_entity_decision_for_item(
    item: dict[str, Any],
    decisions_by_id: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    proposal_id = str(item.get("proposal_id", "")).strip()
    if proposal_id and proposal_id in decisions_by_id:
        return decisions_by_id[proposal_id]
    latest_decision = item.get("latest_decision", {})
    return latest_decision if isinstance(latest_decision, dict) else {}


def _alias_group_decision_summary(
    group: dict[str, Any],
    decisions_by_id: dict[str, dict[str, Any]],
) -> tuple[str, dict[str, Any]]:
    child_ids = [str(child_id).strip() for child_id in group.get("child_proposal_ids", []) or [] if str(child_id).strip()]
    child_decisions = [decisions_by_id[child_id] for child_id in child_ids if child_id in decisions_by_id]
    if not child_decisions:
        return "", {}
    counts: dict[str, int] = {}
    for decision in child_decisions:
        label = str(decision.get("decision", "")).strip().lower() or "decided"
        counts[label] = counts.get(label, 0) + 1
    summary = ", ".join(f"{label} {count}/{len(child_ids)}" for label, count in sorted(counts.items()))
    return summary, child_decisions[-1]


def candidate_inventory_browser_rows(proposals_path: Path, decisions_path: Path | None = None) -> list[dict[str, Any]]:
    payload = _read_json_or_default(
        proposals_path,
        {"proposals": [], "alias_review_groups": [], "candidate_inventory": [], "suppressed_candidates": []},
    )
    decisions_by_id = _latest_conversation_entity_decisions(decisions_path)
    rows: list[dict[str, Any]] = []
    grouped_child_ids = {
        str(child_id)
        for group in (payload.get("alias_review_groups", []) if isinstance(payload, dict) else [])
        if isinstance(group, dict)
        for child_id in group.get("child_proposal_ids", []) or []
    }
    proposal_by_id: dict[str, dict[str, Any]] = {}
    if isinstance(payload, dict):
        for source_bucket in ("proposals", "candidate_inventory", "suppressed_candidates"):
            for item in payload.get(source_bucket, []) or []:
                if isinstance(item, dict):
                    proposal_id = str(item.get("proposal_id", "")).strip()
                    if proposal_id:
                        proposal_by_id[proposal_id] = item
    attention_payload = _read_json_or_default(proposals_path.with_name("conversation_entity_auto_review_attention.json"), {"items": []})
    for index, item in enumerate(attention_payload.get("items", []) if isinstance(attention_payload, dict) else []):
        if not isinstance(item, dict):
            continue
        proposal_id = str(item.get("proposal_id", "")).strip()
        proposal = proposal_by_id.get(proposal_id, {})
        raw_name = (
            str(item.get("candidate_name", "")).strip()
            or str(proposal.get("candidate_name", "")).strip()
            or str(item.get("canonical_name", "")).strip()
            or "(unnamed)"
        )
        canonical_name = str(item.get("canonical_name", "")).strip()
        latest_decision = _conversation_entity_decision_for_item(proposal, decisions_by_id)
        if latest_decision:
            canonical_name = str(latest_decision.get("canonical_name") or canonical_name).strip()
        display_name = canonical_name if canonical_name else raw_name
        if canonical_name and canonical_name.lower() != raw_name.lower():
            display_name = f"{canonical_name} (alias: {raw_name})"
        merged_item = {**proposal, "auto_review_attention": item}
        rows.append(
            {
                "row_id": f"auto_review_attention:{proposal_id or index}",
                "bucket": "attention",
                "source_bucket": "auto_review_attention",
                "category": candidate_inventory_category(proposal or item),
                "candidate_name": display_name,
                "raw_candidate_name": raw_name,
                "canonical_name": canonical_name,
                "proposed_entity_type": str(item.get("entity_type", proposal.get("proposed_entity_type", "term")) or "term"),
                "evidence_count": int(proposal.get("evidence_count", item.get("evidence_count", 0)) or 0),
                "topics": _as_text_list(proposal.get("candidate_topics")),
                "tracks": _as_text_list(proposal.get("knowledge_tracks")),
                "triage_reason": str(item.get("human_review_reason", "") or proposal.get("triage_reason", "") or ""),
                "review_priority": "human attention",
                "decision": str(latest_decision.get("decision") or item.get("decision", "") or ""),
                "secondary_entity_types": _as_text_list(item.get("secondary_entity_types")),
                "item": merged_item,
                "latest_decision": latest_decision,
            }
        )
    for index, item in enumerate(payload.get("alias_review_groups", []) if isinstance(payload, dict) else []):
        if not isinstance(item, dict):
            continue
        raw_name = str(item.get("candidate_name", "")).strip() or str(item.get("suggested_canonical_name", "")).strip() or "(alias group)"
        canonical_name = str(item.get("suggested_canonical_name", "")).strip()
        alias_names = [str(alias.get("candidate_name", "")).strip() for alias in item.get("alias_candidates", []) or [] if isinstance(alias, dict)]
        decision_summary, latest_decision = _alias_group_decision_summary(item, decisions_by_id)
        row = {
            "row_id": f"alias_review_groups:{item.get('proposal_id') or index}",
            "bucket": "promoted",
            "source_bucket": "alias_review_groups",
            "category": candidate_inventory_category(item),
            "candidate_name": raw_name,
            "raw_candidate_name": raw_name,
            "canonical_name": canonical_name,
            "proposed_entity_type": str(item.get("proposed_entity_type", "term") or "term"),
            "evidence_count": int(item.get("evidence_count", 0) or 0),
            "topics": ["alias"],
            "tracks": [],
            "triage_reason": str(item.get("triage_reason", "") or f"{len(alias_names)} alias candidates"),
            "review_priority": str(item.get("review_priority", "") or ""),
            "decision": decision_summary,
            "item": item,
            "latest_decision": latest_decision,
        }
        rows.append(row)
    for bucket in ("proposals", "candidate_inventory", "suppressed_candidates"):
        for index, item in enumerate(payload.get(bucket, []) if isinstance(payload, dict) else []):
            if not isinstance(item, dict):
                continue
            if bucket == "proposals" and str(item.get("proposal_id", "")) in grouped_child_ids:
                continue
            raw_name = str(item.get("candidate_name", "")).strip() or str(item.get("normalized_name_key", "")).strip() or "(unnamed)"
            decision = _conversation_entity_decision_for_item(item, decisions_by_id)
            canonical_name = str(decision.get("canonical_name", "")).strip()
            if not canonical_name:
                canonical_name = str(item.get("canonical_name") or item.get("suggested_canonical_name") or "").strip()
            if canonical_name and canonical_name.lower() != raw_name.lower():
                name = f"{canonical_name} (alias: {raw_name})"
            else:
                name = raw_name
            topics = _as_text_list(item.get("candidate_topics"))
            tracks = _as_text_list(item.get("knowledge_tracks"))
            row = {
                "row_id": f"{bucket}:{item.get('proposal_id') or item.get('normalized_name_key') or index}",
                "bucket": candidate_inventory_bucket_label(bucket),
                "source_bucket": bucket,
                "category": candidate_inventory_category(item),
                "candidate_name": name,
                "raw_candidate_name": raw_name,
                "canonical_name": canonical_name,
                "proposed_entity_type": str(item.get("proposed_entity_type", item.get("initial_proposed_entity_type", "term")) or "term"),
                "evidence_count": int(item.get("evidence_count", 0) or 0),
                "topics": topics,
                "tracks": tracks,
                "triage_reason": str(item.get("triage_reason", "") or ""),
                "review_priority": str(item.get("review_priority", "") or ""),
                "decision": str(decision.get("decision", "") or ""),
                "item": item,
                "latest_decision": decision,
            }
            rows.append(row)
    rows.sort(key=lambda row: (row["bucket"], row["category"], str(row["candidate_name"]).lower()))
    return rows


def write_candidate_inventory_override_decision(
    decisions_path: Path,
    row: dict[str, Any],
    decision: str,
    canonical_name: str,
    entity_type: str,
    reviewer: str,
    rationale: str,
    timestamp_utc: str | None = None,
) -> int:
    timestamp = timestamp_utc or now_utc_iso()
    item = row.get("item", {}) if isinstance(row.get("item"), dict) else {}
    data = _read_json_or_default(decisions_path, {"decisions": []})
    decisions = data.setdefault("decisions", [])
    if not isinstance(decisions, list):
        decisions = []
        data["decisions"] = decisions

    base_payload = {
        "decision": decision,
        "canonical_name": canonical_name.strip(),
        "entity_type": entity_type.strip() or "term",
        "reviewer": reviewer,
        "rationale": rationale,
        "timestamp_utc": timestamp,
        "human_override": True,
        "override_source": "candidate_inventory_browser",
    }

    written = 0
    if item.get("group_kind") == "alias_review_group":
        child_ids = {str(child_id).strip() for child_id in item.get("child_proposal_ids", []) or [] if str(child_id).strip()}
        for alias in item.get("alias_candidates", []) or []:
            if not isinstance(alias, dict):
                continue
            proposal_id = str(alias.get("proposal_id", "")).strip()
            if not proposal_id or (child_ids and proposal_id not in child_ids):
                continue
            decisions.append(
                {
                    **base_payload,
                    "proposal_id": proposal_id,
                    "candidate_name": str(alias.get("candidate_name", "") or row.get("raw_candidate_name", "")),
                }
            )
            written += 1
    else:
        proposal_id = str(item.get("proposal_id", "")).strip()
        if proposal_id:
            decisions.append(
                {
                    **base_payload,
                    "proposal_id": proposal_id,
                    "candidate_name": str(item.get("candidate_name", "") or row.get("raw_candidate_name", "")),
                }
            )
            written = 1

    if written:
        write_json(decisions_path, data)
    return written


def candidate_inventory_sort_value(row: dict[str, Any], sort_key: str) -> Any:
    if sort_key == "evidence":
        return int(row.get("evidence_count", 0) or 0)
    if sort_key == "decision":
        return str(row.get("decision", "")).lower()
    if sort_key == "candidate_name":
        return str(row.get("candidate_name", "")).lower()
    if sort_key == "bucket":
        return str(row.get("bucket", "")).lower()
    if sort_key == "category":
        return str(row.get("category", "")).lower()
    if sort_key == "type":
        return str(row.get("proposed_entity_type", "")).lower()
    if sort_key == "topics":
        return ", ".join(_as_text_list(row.get("topics"))).lower()
    if sort_key == "reason":
        return str(row.get("triage_reason", "")).lower()
    return str(row.get("candidate_name", "")).lower()


def sort_candidate_inventory_rows(rows: list[dict[str, Any]], sort_key: str, descending: bool) -> list[dict[str, Any]]:
    sorted_rows = list(rows)
    sorted_rows.sort(key=lambda row: str(row.get("candidate_name", "")).lower())
    sorted_rows.sort(key=lambda row: candidate_inventory_sort_value(row, sort_key), reverse=descending)
    return sorted_rows


def choose_initial_artifacts_root(repo_root: Path, explicit_root: Path | None = None) -> Path:
    if explicit_root is not None:
        return explicit_root.resolve()
    runs = discover_review_runs(repo_root, repo_root / "artifacts")
    pending_runs = [run for run in runs if run["pending_total"] > 0]
    if pending_runs:
        return max((Path(run["artifacts_root"]) for run in pending_runs), key=artifact_sort_key)
    material_runs = [run for run in runs if Path(run["artifacts_root"]).resolve() != (repo_root / "artifacts").resolve()]
    if material_runs:
        return max((Path(run["artifacts_root"]) for run in material_runs), key=artifact_sort_key)
    return (repo_root / "artifacts").resolve()


class ProgressChart(ttk.Frame):
    def __init__(self, parent: tk.Misc) -> None:
        super().__init__(parent)
        self.progress: dict[str, Any] = pipeline_progress_from_logs([], "idle")
        self.canvas = tk.Canvas(self, height=118, bg="#ffffff", highlightthickness=1, highlightbackground="#d8dee4")
        self.canvas.pack(fill=tk.X, expand=True)
        self.canvas.bind("<Configure>", lambda _event: self.redraw())

    def set_progress(self, progress: dict[str, Any]) -> None:
        self.progress = progress
        self.redraw()

    def redraw(self) -> None:
        self.canvas.delete("all")
        stages = self.progress.get("stages", [])
        if not stages:
            stages = [{**stage, "state": "waiting"} for stage in PIPELINE_STAGES]
        width = max(self.canvas.winfo_width(), 640)
        height = max(self.canvas.winfo_height(), 118)
        margin = 58
        y = 42
        span = max(width - margin * 2, 1)
        step = span / max(len(stages) - 1, 1)
        positions = [(margin + index * step, y) for index in range(len(stages))]

        def color_for_state(state: str) -> str:
            return {
                "done": "#238636",
                "current": "#0969da",
                "attention": "#bf8700",
                "failed": "#cf222e",
                "waiting": "#f6f8fa",
            }.get(state, "#f6f8fa")

        for index in range(len(positions) - 1):
            x1, y1 = positions[index]
            x2, y2 = positions[index + 1]
            state = str(stages[index].get("state", "waiting"))
            line_color = "#238636" if state == "done" else "#d8dee4"
            self.canvas.create_line(x1 + 13, y1, x2 - 13, y2, fill=line_color, width=3)

        for (x, y_pos), stage in zip(positions, stages):
            state = str(stage.get("state", "waiting"))
            fill = color_for_state(state)
            outline = fill if state != "waiting" else "#c9d1d9"
            if state in {"current", "attention", "failed"}:
                self.canvas.create_oval(x - 17, y_pos - 17, x + 17, y_pos + 17, fill="#eaeef2", outline="")
            self.canvas.create_oval(x - 12, y_pos - 12, x + 12, y_pos + 12, fill=fill, outline=outline, width=2)
            if state != "waiting":
                self.canvas.create_oval(x - 4, y_pos - 4, x + 4, y_pos + 4, fill="#ffffff", outline="")
            label = str(stage.get("short_label", ""))
            name = str(stage.get("name", ""))
            self.canvas.create_text(x, y_pos + 28, text=label, fill="#24292f", font=("Segoe UI", 9, "bold"))
            self.canvas.create_text(x, y_pos + 47, text=name, fill="#57606a", font=("Segoe UI", 8), width=max(step - 10, 70))

        summary = str(self.progress.get("summary", ""))
        self.canvas.create_text(width - 12, height - 12, text=summary, anchor="se", fill="#57606a", font=("Segoe UI", 8))


class CandidateInventoryWindow(tk.Toplevel):
    def __init__(self, app: "TheriacDesktopApp") -> None:
        super().__init__(app.root)
        self.app = app
        self.rows: list[dict[str, Any]] = []
        self.filtered_rows: list[dict[str, Any]] = []
        self.row_by_iid: dict[str, dict[str, Any]] = {}
        self.sort_key = "evidence"
        self.sort_descending = True
        self.sort_column_keys = {
            "#0": "candidate_name",
            "bucket": "bucket",
            "category": "category",
            "type": "type",
            "evidence": "evidence",
            "decision": "decision",
            "topics": "topics",
            "reason": "reason",
        }
        self.heading_labels = {
            "#0": "Candidate",
            "bucket": "Bucket",
            "category": "Category",
            "type": "Type",
            "evidence": "Evidence",
            "decision": "Decision",
            "topics": "Topics",
            "reason": "Triage Reason",
        }
        self.title("Candidate Inventory Browser")
        self.geometry("1120x680")
        self.minsize(860, 520)
        self.protocol("WM_DELETE_WINDOW", self.close)

        self.columnconfigure(0, weight=1)
        self.rowconfigure(1, weight=1)

        filters = ttk.Frame(self, padding=(10, 10, 10, 4))
        filters.grid(row=0, column=0, sticky="ew")
        filters.columnconfigure(7, weight=1)
        ttk.Label(filters, text="Bucket").grid(row=0, column=0, sticky="w")
        self.bucket_var = tk.StringVar(value="All")
        self.bucket_combo = ttk.Combobox(
            filters,
            textvariable=self.bucket_var,
            state="readonly",
            values=["All", "attention", "promoted", "demoted", "suppressed"],
            width=14,
        )
        self.bucket_combo.grid(row=0, column=1, sticky="w", padx=(6, 14))
        ttk.Label(filters, text="Category").grid(row=0, column=2, sticky="w")
        self.category_var = tk.StringVar(value="All")
        self.category_combo = ttk.Combobox(
            filters,
            textvariable=self.category_var,
            state="readonly",
            values=["All", "lore", "meta", "mixed", "unknown"],
            width=12,
        )
        self.category_combo.grid(row=0, column=3, sticky="w", padx=(6, 14))
        ttk.Label(filters, text="Topic").grid(row=0, column=4, sticky="w")
        self.topic_var = tk.StringVar(value="All")
        self.topic_combo = ttk.Combobox(filters, textvariable=self.topic_var, state="readonly", values=["All"], width=18)
        self.topic_combo.grid(row=0, column=5, sticky="w", padx=(6, 14))
        ttk.Label(filters, text="Search").grid(row=0, column=6, sticky="w")
        self.search_var = tk.StringVar()
        search_entry = ttk.Entry(filters, textvariable=self.search_var)
        search_entry.grid(row=0, column=7, sticky="ew", padx=(6, 8))
        ttk.Button(filters, text="Refresh", command=self.reload).grid(row=0, column=8, sticky="e")
        self.summary_var = tk.StringVar()
        ttk.Label(filters, textvariable=self.summary_var, foreground="#57606a").grid(row=1, column=0, columnspan=9, sticky="w", pady=(8, 0))

        body = ttk.PanedWindow(self, orient=tk.HORIZONTAL)
        body.grid(row=1, column=0, sticky="nsew", padx=10, pady=(4, 10))

        list_frame = ttk.Frame(body)
        list_frame.columnconfigure(0, weight=1)
        list_frame.rowconfigure(0, weight=1)
        columns = ("bucket", "category", "type", "evidence", "decision", "topics", "reason")
        self.tree = ttk.Treeview(list_frame, columns=columns, show="tree headings", selectmode="browse")
        self.configure_tree_headings()
        self.tree.column("#0", width=180, stretch=True)
        self.tree.column("bucket", width=92, anchor="center", stretch=False)
        self.tree.column("category", width=86, anchor="center", stretch=False)
        self.tree.column("type", width=120, anchor="center", stretch=False)
        self.tree.column("evidence", width=74, anchor="center", stretch=False)
        self.tree.column("decision", width=122, anchor="center", stretch=False)
        self.tree.column("topics", width=180, stretch=True)
        self.tree.column("reason", width=280, stretch=True)
        scrollbar = ttk.Scrollbar(list_frame, orient=tk.VERTICAL, command=self.tree.yview)
        self.tree.configure(yscrollcommand=scrollbar.set)
        self.tree.grid(row=0, column=0, sticky="nsew")
        scrollbar.grid(row=0, column=1, sticky="ns")
        body.add(list_frame, weight=3)

        detail_frame = ttk.Frame(body)
        detail_frame.columnconfigure(0, weight=1)
        detail_frame.rowconfigure(0, weight=1)
        self.detail_text = scrolledtext.ScrolledText(detail_frame, wrap=tk.WORD, font=("Consolas", 9))
        self.detail_text.grid(row=0, column=0, sticky="nsew")
        self.detail_text.configure(state="disabled")
        override_frame = ttk.LabelFrame(detail_frame, text="Manual Override", padding=(8, 8, 8, 8))
        override_frame.grid(row=1, column=0, sticky="ew", pady=(8, 0))
        override_frame.columnconfigure(1, weight=1)
        override_frame.columnconfigure(3, weight=1)
        ttk.Label(override_frame, text="Canonical").grid(row=0, column=0, sticky="w")
        self.override_canonical_var = tk.StringVar()
        self.override_canonical_entry = ttk.Entry(override_frame, textvariable=self.override_canonical_var)
        self.override_canonical_entry.grid(row=0, column=1, sticky="ew", padx=(6, 12))
        ttk.Label(override_frame, text="Type").grid(row=0, column=2, sticky="w")
        self.override_type_var = tk.StringVar(value="term")
        self.override_type_combo = ttk.Combobox(override_frame, textvariable=self.override_type_var, values=ENTITY_REVIEW_TYPES, state="readonly", width=16)
        self.override_type_combo.grid(row=0, column=3, sticky="ew", padx=(6, 0))
        ttk.Label(override_frame, text="Rationale").grid(row=1, column=0, sticky="nw", pady=(8, 0))
        self.override_rationale_text = tk.Text(override_frame, height=3, wrap=tk.WORD)
        self.override_rationale_text.grid(row=1, column=1, columnspan=3, sticky="ew", padx=(6, 0), pady=(8, 0))
        button_row = ttk.Frame(override_frame)
        button_row.grid(row=2, column=0, columnspan=4, sticky="ew", pady=(8, 0))
        self.override_buttons = [
            ttk.Button(button_row, text="Approve", command=lambda: self.save_override_decision("approve")),
            ttk.Button(button_row, text="Reject", command=lambda: self.save_override_decision("reject")),
            ttk.Button(button_row, text="Defer", command=lambda: self.save_override_decision("defer")),
            ttk.Button(button_row, text="Needs More Context", command=lambda: self.save_override_decision("needs_more_context")),
        ]
        for index, button in enumerate(self.override_buttons):
            button.grid(row=0, column=index, sticky="w", padx=(0, 6))
        self.override_status_var = tk.StringVar()
        ttk.Label(override_frame, textvariable=self.override_status_var, foreground="#57606a").grid(row=3, column=0, columnspan=4, sticky="w", pady=(8, 0))
        body.add(detail_frame, weight=2)

        self.bucket_combo.bind("<<ComboboxSelected>>", lambda _event: self.apply_filters())
        self.category_combo.bind("<<ComboboxSelected>>", lambda _event: self.apply_filters())
        self.topic_combo.bind("<<ComboboxSelected>>", lambda _event: self.apply_filters())
        self.search_var.trace_add("write", lambda *_args: self.apply_filters())
        self.tree.bind("<<TreeviewSelect>>", lambda _event: self.show_selected_detail())
        self.reload()

    def close(self) -> None:
        self.app.candidate_browser = None
        self.destroy()

    def reload(self) -> None:
        self.rows = candidate_inventory_browser_rows(
            self.app.paths["conversation_entity_proposals"],
            self.app.paths["conversation_entity_decisions"],
        )
        topic_values = ["All"] + sorted({topic for row in self.rows for topic in row.get("topics", [])}, key=str.lower)
        self.topic_combo.configure(values=topic_values)
        if self.topic_var.get() not in topic_values:
            self.topic_var.set("All")
        self.apply_filters()

    def apply_filters(self) -> None:
        bucket = self.bucket_var.get().strip().lower()
        category = self.category_var.get().strip().lower()
        topic = self.topic_var.get().strip().lower()
        query = self.search_var.get().strip().lower()
        filtered: list[dict[str, Any]] = []
        for row in self.rows:
            if bucket != "all" and row["bucket"] != bucket:
                continue
            if category != "all" and row["category"] != category:
                continue
            if topic != "all" and topic not in {value.lower() for value in row.get("topics", [])}:
                continue
            haystack = "\n".join(
                [
                    str(row.get("candidate_name", "")),
                    str(row.get("proposed_entity_type", "")),
                    str(row.get("category", "")),
                    str(row.get("bucket", "")),
                    " ".join(row.get("topics", [])),
                    str(row.get("triage_reason", "")),
                    "\n".join(_as_text_list(row.get("item", {}).get("sample_texts"))),
                ]
            ).lower()
            if query and query not in haystack:
                continue
            filtered.append(row)
        self.filtered_rows = sort_candidate_inventory_rows(filtered, self.sort_key, self.sort_descending)
        self.render_rows()

    def configure_tree_headings(self) -> None:
        for column, label in self.heading_labels.items():
            sort_key = self.sort_column_keys[column]
            suffix = ""
            if self.sort_key == sort_key:
                suffix = " (desc)" if self.sort_descending else " (asc)"
            self.tree.heading(
                column,
                text=label + suffix,
                command=lambda tree_column=column: self.set_sort_column(tree_column),
            )

    def set_sort_column(self, tree_column: str) -> None:
        sort_key = self.sort_column_keys.get(tree_column, "candidate_name")
        if self.sort_key == sort_key:
            self.sort_descending = not self.sort_descending
        else:
            self.sort_key = sort_key
            self.sort_descending = sort_key == "evidence"
        self.apply_filters()

    def render_rows(self) -> None:
        self.configure_tree_headings()
        for item_id in self.tree.get_children():
            self.tree.delete(item_id)
        self.row_by_iid = {}
        for index, row in enumerate(self.filtered_rows):
            iid = f"row_{index}"
            self.row_by_iid[iid] = row
            self.tree.insert(
                "",
                tk.END,
                iid=iid,
                text=row["candidate_name"],
                values=(
                    row["bucket"],
                    row["category"],
                    row["proposed_entity_type"],
                    row["evidence_count"],
                    row.get("decision", ""),
                    _join_preview(row["topics"], 3),
                    row["triage_reason"],
                ),
            )
        totals = {
            "attention": sum(1 for row in self.rows if row["bucket"] == "attention"),
            "promoted": sum(1 for row in self.rows if row["bucket"] == "promoted"),
            "demoted": sum(1 for row in self.rows if row["bucket"] == "demoted"),
            "suppressed": sum(1 for row in self.rows if row["bucket"] == "suppressed"),
        }
        categories = {name: sum(1 for row in self.rows if row["category"] == name) for name in ["lore", "meta", "mixed", "unknown"]}
        self.summary_var.set(
            f"Showing {len(self.filtered_rows)} of {len(self.rows)} candidates. "
            f"Attention: {totals['attention']}. "
            f"Buckets: {totals['promoted']} promoted, {totals['demoted']} demoted, {totals['suppressed']} suppressed. "
            f"Categories: {categories['lore']} lore, {categories['meta']} meta, {categories['mixed']} mixed, {categories['unknown']} unknown. "
            f"Sorted by {self.sort_key} {'descending' if self.sort_descending else 'ascending'}."
        )
        if self.filtered_rows:
            first = self.tree.get_children()[0]
            self.tree.selection_set(first)
            self.tree.focus(first)
            self.show_selected_detail()
        else:
            self.set_detail_text("No candidates match the current filters.")
            self.override_canonical_var.set("")
            self.override_type_var.set("term")
            self.override_rationale_text.configure(state=tk.NORMAL)
            self.override_rationale_text.delete("1.0", tk.END)
            self.override_rationale_text.configure(state=tk.DISABLED)
            self.override_canonical_entry.configure(state=tk.DISABLED)
            self.override_type_combo.configure(state=tk.DISABLED)
            for button in self.override_buttons:
                button.configure(state=tk.DISABLED)
            self.override_status_var.set("")

    def show_selected_detail(self) -> None:
        selected = self.tree.selection()
        if not selected:
            return
        row = self.row_by_iid.get(selected[0])
        if not row:
            return
        item = row["item"]
        parts = [f"Name: {row['candidate_name']}"]
        if row.get("canonical_name") and row.get("canonical_name") != row.get("raw_candidate_name"):
            parts.extend(
                [
                    f"Raw candidate: {row['raw_candidate_name']}",
                    f"Canonical target: {row['canonical_name']}",
                ]
            )
        parts.extend(
            [
            f"Bucket: {row['bucket']}",
            f"Category: {row['category']}",
            f"Type: {row['proposed_entity_type']}",
            f"Decision: {row.get('decision') or '(none)'}",
            f"Evidence count: {row['evidence_count']}",
            f"Topics: {_join_preview(row['topics'], 12) or '(none)'}",
            f"Tracks: {_join_preview(row['tracks'], 12) or '(none)'}",
            f"Priority: {row['review_priority'] or '(none)'}",
            f"Triage reason: {row['triage_reason'] or '(none)'}",
            "",
            "Counts:",
            json.dumps(
                {
                    "knowledge_track_counts": item.get("knowledge_track_counts", {}),
                    "source_kind_counts": item.get("source_kind_counts", {}),
                    "patch_item_type_counts": item.get("patch_item_type_counts", {}),
                    "patch_update_type_counts": item.get("patch_update_type_counts", {}),
                    "patch_relationship_type_counts": item.get("patch_relationship_type_counts", {}),
                },
                ensure_ascii=False,
                indent=2,
            ),
            "",
            "Sample Evidence:",
            ]
        )
        attention = item.get("auto_review_attention", {}) if isinstance(item.get("auto_review_attention"), dict) else {}
        if attention:
            parts[parts.index("Counts:"):parts.index("Counts:")] = [
                f"Best guess: {attention.get('decision', '(none)')}",
                f"Secondary types: {_join_preview(_as_text_list(attention.get('secondary_entity_types')), 8) or '(none)'}",
                f"Attention reason: {attention.get('human_review_reason', '(none)') or '(none)'}",
                f"Auto-review rationale: {attention.get('rationale', '(none)') or '(none)'}",
                "",
            ]
        samples = _as_text_list(item.get("sample_texts"))
        if samples:
            for index, sample in enumerate(samples, start=1):
                parts.append(f"\n[{index}] {sample}")
        else:
            parts.append("(none)")
        parts.extend(
            [
                "",
                "Source Snippets:",
                _join_preview(_as_text_list(item.get("source_snippet_ids")), 20) or "(none)",
                "",
                "Raw Candidate:",
                json.dumps(item, ensure_ascii=False, indent=2),
            ]
        )
        self.set_detail_text("\n".join(parts))
        self.populate_override_controls(row)

    def set_detail_text(self, text: str) -> None:
        self.detail_text.configure(state="normal")
        self.detail_text.delete("1.0", tk.END)
        self.detail_text.insert("1.0", text)
        self.detail_text.configure(state="disabled")

    def selected_row(self) -> dict[str, Any] | None:
        selected = self.tree.selection()
        if not selected:
            return None
        return self.row_by_iid.get(selected[0])

    def populate_override_controls(self, row: dict[str, Any]) -> None:
        canonical = str(row.get("canonical_name") or row.get("raw_candidate_name") or row.get("candidate_name") or "").strip()
        if not canonical:
            canonical = str(row.get("candidate_name", "")).split(" (alias:", 1)[0].strip()
        self.override_canonical_var.set(canonical)
        entity_type = str(row.get("proposed_entity_type", "term") or "term")
        self.override_type_var.set(entity_type if entity_type in ENTITY_REVIEW_TYPES else "term")
        item = row.get("item", {}) if isinstance(row.get("item"), dict) else {}
        can_override = item.get("group_kind") == "alias_review_group" or bool(str(item.get("proposal_id", "")).strip())
        state = tk.NORMAL if can_override else tk.DISABLED
        self.override_canonical_entry.configure(state=state)
        self.override_type_combo.configure(state="readonly" if can_override else tk.DISABLED)
        self.override_rationale_text.configure(state=state)
        for button in self.override_buttons:
            button.configure(state=state)
        if can_override:
            self.override_status_var.set("Override writes a human decision; Stage 07 will prefer it over AI auto-review.")
        else:
            self.override_status_var.set("This row has no proposal id, so it cannot be overridden here.")

    def save_override_decision(self, decision: str) -> None:
        row = self.selected_row()
        if row is None:
            return
        reviewer = self.app.reviewer_var.get().strip() or "human_reviewer"
        canonical_name = self.override_canonical_var.get().strip() or str(row.get("raw_candidate_name") or row.get("candidate_name") or "").strip()
        entity_type = self.override_type_var.get().strip() or "term"
        rationale = self.override_rationale_text.get("1.0", tk.END).strip()
        written = write_candidate_inventory_override_decision(
            self.app.paths["conversation_entity_decisions"],
            row,
            decision,
            canonical_name,
            entity_type,
            reviewer,
            rationale,
        )
        if not written:
            self.override_status_var.set("No decision was written for this row.")
            return
        self.override_status_var.set(f"Saved {decision} override for {written} proposal(s).")
        self.reload()
        self.app.refresh_review_item()


class TheriacDesktopApp:
    def __init__(
        self,
        root: tk.Tk,
        repo_root: Path,
        artifacts_root: Path,
        docx_hint: Path | None,
        conversations_root: Path | None,
    ) -> None:
        self.root = root
        self.repo_root = repo_root
        self.docx_hint = docx_hint
        self.conversations_root = conversations_root or (repo_root / "discord_conversations")
        self.paths: dict[str, Path] = {}
        self.artifacts_root = artifacts_root
        self.pipeline_status = "idle"
        self.pipeline_message = ""
        self.last_exit_code: int | None = None
        self.pipeline_logs: list[str] = []
        self.progress_logs: list[str] = []
        self.log_queue: queue.Queue[tuple[str, Any]] = queue.Queue()
        self.process: subprocess.Popen[str] | None = None
        self.attached_pid: int | None = None
        self.attached_process_kind = ""
        self.attached_command_line = ""
        self.attached_log_paths: list[Path] = []
        self.attached_log_offsets: dict[Path, int] = {}
        self.cancel_requested = False
        self.run_options: list[dict[str, Any]] = []
        self.run_option_by_label: dict[str, Path | str] = {}
        self.new_run_selected = False
        self.current_kind = ""
        self.current_item: dict[str, Any] | None = None
        self.candidate_browser: CandidateInventoryWindow | None = None

        self.root.title("THERIAC Lore Pipeline Review")
        self.root.geometry("1180x820")
        self.root.minsize(940, 680)

        self.resolve_paths(artifacts_root)
        self.build_ui()
        self.refresh_runs()
        self.refresh_review_item()
        self.refresh_progress()
        self.root.after(200, self.drain_log_queue)
        self.root.after(500, lambda: self.attach_to_running_process(silent=True, allow_switch=True))
        self.root.after(2000, self.poll_attached_process)

    def pipeline_active(self) -> bool:
        return self.pipeline_status in {"running", "cancelling", "attached"}

    def resolve_paths(self, artifacts_root: Path) -> None:
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
            resolved_artifacts_root,
        ) = _resolve_input_paths(None, None, None, artifacts_root)
        self.artifacts_root = resolved_artifacts_root.resolve()
        self.paths = {
            "patches": patches_path,
            "decisions": decisions_path,
            "directives": directives_path,
            "card_drafts": card_drafts_path,
            "card_decisions": card_decisions_path,
            "identity_merge_proposals": identity_merge_proposals_path,
            "identity_merge_decisions": identity_merge_decisions_path,
            "conversation_entity_proposals": conversation_entity_proposals_path,
            "conversation_entity_decisions": conversation_entity_decisions_path,
        }
        _ensure_review_files(
            self.paths["decisions"],
            self.paths["directives"],
            self.paths["card_decisions"],
            self.paths["identity_merge_decisions"],
            self.paths["conversation_entity_decisions"],
        )

    def build_ui(self) -> None:
        self.root.columnconfigure(0, weight=1)
        self.root.rowconfigure(3, weight=1)
        self.root.rowconfigure(5, weight=1)

        top = ttk.Frame(self.root, padding=(12, 10, 12, 4))
        top.grid(row=0, column=0, sticky="ew")
        top.columnconfigure(1, weight=1)
        ttk.Label(top, text="Run", font=("Segoe UI", 9, "bold")).grid(row=0, column=0, sticky="w", padx=(0, 8))
        self.run_var = tk.StringVar()
        self.run_combo = ttk.Combobox(top, textvariable=self.run_var, state="readonly")
        self.run_combo.grid(row=0, column=1, sticky="ew")
        ttk.Button(top, text="Switch", command=self.switch_run).grid(row=0, column=2, padx=(8, 0))
        ttk.Button(top, text="Refresh", command=self.refresh_all).grid(row=0, column=3, padx=(8, 0))
        ttk.Button(top, text="Browse Candidates", command=self.open_candidate_inventory_browser).grid(row=0, column=4, padx=(8, 0))
        self.run_meta_var = tk.StringVar()
        ttk.Label(top, textvariable=self.run_meta_var, foreground="#57606a").grid(row=1, column=0, columnspan=5, sticky="w", pady=(6, 0))

        self.progress_chart = ProgressChart(self.root)
        self.progress_chart.grid(row=1, column=0, sticky="ew", padx=12, pady=(4, 8))

        header = ttk.Frame(self.root, padding=(12, 0, 12, 0))
        header.grid(row=2, column=0, sticky="ew")
        header.columnconfigure(0, weight=1)
        self.item_title_var = tk.StringVar(value="Review")
        ttk.Label(header, textvariable=self.item_title_var, font=("Segoe UI", 12, "bold")).grid(row=0, column=0, sticky="w")
        self.pending_summary_var = tk.StringVar()
        ttk.Label(header, textvariable=self.pending_summary_var, foreground="#57606a").grid(row=0, column=1, sticky="e")

        middle = ttk.Frame(self.root, padding=(12, 8, 12, 8))
        middle.grid(row=3, column=0, sticky="nsew")
        middle.columnconfigure(0, weight=3)
        middle.columnconfigure(1, weight=1)
        middle.rowconfigure(0, weight=1)

        self.item_text = scrolledtext.ScrolledText(middle, wrap=tk.WORD, font=("Consolas", 9), height=14)
        self.item_text.grid(row=0, column=0, sticky="nsew", padx=(0, 10))

        controls = ttk.Frame(middle)
        controls.grid(row=0, column=1, sticky="nsew")
        controls.columnconfigure(0, weight=1)
        ttk.Label(controls, text="Reviewer").grid(row=0, column=0, sticky="w")
        self.reviewer_var = tk.StringVar(value="human_reviewer")
        ttk.Entry(controls, textvariable=self.reviewer_var).grid(row=1, column=0, sticky="ew", pady=(2, 10))

        self.extra_frame = ttk.Frame(controls)
        self.extra_frame.grid(row=2, column=0, sticky="ew", pady=(0, 10))
        self.extra_frame.columnconfigure(0, weight=1)

        ttk.Label(controls, text="Rationale").grid(row=3, column=0, sticky="w")
        self.rationale_text = tk.Text(controls, height=7, wrap=tk.WORD)
        self.rationale_text.grid(row=4, column=0, sticky="ew", pady=(2, 10))

        self.button_frame = ttk.Frame(controls)
        self.button_frame.grid(row=5, column=0, sticky="ew")
        for col in range(2):
            self.button_frame.columnconfigure(col, weight=1)
        self.primary_button = ttk.Button(self.button_frame, text="Accept", command=lambda: self.save_decision("accept"))
        self.reject_button = ttk.Button(self.button_frame, text="Reject", command=lambda: self.save_decision("reject"))
        self.defer_button = ttk.Button(self.button_frame, text="Defer", command=lambda: self.save_decision("defer"))
        self.more_context_button = ttk.Button(self.button_frame, text="Needs More Context", command=lambda: self.save_decision("needs_more_context"))
        self.primary_button.grid(row=0, column=0, sticky="ew", padx=(0, 4), pady=2)
        self.reject_button.grid(row=0, column=1, sticky="ew", padx=(4, 0), pady=2)
        self.defer_button.grid(row=1, column=0, sticky="ew", padx=(0, 4), pady=2)
        self.more_context_button.grid(row=1, column=1, sticky="ew", padx=(4, 0), pady=2)

        pipeline = ttk.Frame(self.root, padding=(12, 4, 12, 6))
        pipeline.grid(row=4, column=0, sticky="ew")
        pipeline.columnconfigure(4, weight=1)
        self.run_button = ttk.Button(pipeline, text="Run Full Pipeline", command=self.run_full_pipeline)
        self.run_button.grid(row=0, column=0, sticky="w")
        self.cancel_button = ttk.Button(pipeline, text="Cancel Run", command=self.cancel_current_run)
        self.cancel_button.grid(row=0, column=1, sticky="w", padx=(8, 0))
        self.attach_button = ttk.Button(pipeline, text="Attach Running", command=lambda: self.attach_to_running_process())
        self.attach_button.grid(row=0, column=2, sticky="w", padx=(8, 0))
        self.auto_review_button = ttk.Button(pipeline, text="\u2728 AI Auto-Review", command=self.run_auto_review)
        self.auto_review_button.grid(row=0, column=3, sticky="w", padx=(8, 0))
        self.pipeline_status_var = tk.StringVar(value="Status: idle")
        ttk.Label(pipeline, textvariable=self.pipeline_status_var, foreground="#57606a").grid(row=0, column=4, sticky="w", padx=(10, 0))

        self.logs_text = scrolledtext.ScrolledText(self.root, wrap=tk.WORD, font=("Consolas", 9), height=9)
        self.logs_text.grid(row=5, column=0, sticky="nsew", padx=12, pady=(0, 12))
        self.logs_text.insert("1.0", "(no logs yet)")
        self.logs_text.configure(state="disabled")

    def refresh_all(self) -> None:
        self.refresh_runs()
        self.refresh_review_item()
        self.refresh_progress()
        if self.candidate_browser is not None and self.candidate_browser.winfo_exists():
            self.candidate_browser.reload()

    def refresh_runs(self) -> None:
        self.run_options = discover_review_runs(self.repo_root, self.artifacts_root)
        new_run_label = "New Run - create a fresh timestamped artifact folder"
        values: list[str] = [new_run_label]
        self.run_option_by_label = {new_run_label: NEW_RUN_SELECTOR_VALUE}
        for run in self.run_options:
            label = f"{run['label']} - {run['pending_total']} pending ({run['summary']})"
            values.append(label)
            self.run_option_by_label[label] = Path(run["artifacts_root"])
            if not self.new_run_selected and Path(run["artifacts_root"]).resolve() == self.artifacts_root.resolve():
                self.run_var.set(label)
        if self.new_run_selected:
            self.run_var.set(new_run_label)
        self.run_combo.configure(values=values)
        if self.new_run_selected:
            self.run_meta_var.set("New run selected. Press Run Full Pipeline to create a fresh folder under artifacts/runs.")
            self.pending_summary_var.set("New run")
            self.run_button.configure(text="Run Full Pipeline Into New Folder")
        else:
            counts = pending_review_counts_for_root(self.artifacts_root)
            self.run_meta_var.set(
                f"Active: {_display_path(self.artifacts_root, self.repo_root)}. Pending: {pending_review_summary(counts)}."
            )
            self.pending_summary_var.set(f"{pending_review_total(counts)} pending")
            self.run_button.configure(text="Resume Pipeline")

    def switch_run(self) -> None:
        if self.pipeline_active():
            messagebox.showinfo("Pipeline Running", "Finish or cancel the active pipeline run before switching runs.")
            return
        selected = self.run_var.get()
        target = self.run_option_by_label.get(selected)
        if target is None:
            return
        if target == NEW_RUN_SELECTOR_VALUE:
            self.new_run_selected = True
            self.pipeline_status = "idle"
            self.pipeline_message = "New run selected. Press Run Full Pipeline to create a fresh artifact folder."
            self.pipeline_logs = []
            self.progress_logs = []
            self.last_exit_code = None
            self.refresh_all()
            return
        self.new_run_selected = False
        self.resolve_paths(target)
        self.pipeline_status = "idle"
        self.pipeline_message = f"Selected run: {_display_path(self.artifacts_root, self.repo_root)}"
        self.pipeline_logs = []
        self.progress_logs = []
        self.last_exit_code = None
        self.refresh_all()

    def pending_conversation_entities(self) -> list[dict[str, Any]]:
        payload = _read_json_or_default(self.paths["conversation_entity_proposals"], {"proposals": [], "alias_review_groups": []})
        proposals = payload.get("proposals", [])
        decisions = _decision_ids(self.paths["conversation_entity_decisions"], ["proposal_id"])
        grouped_child_ids: set[str] = set()
        pending_groups: list[dict[str, Any]] = []
        for group in payload.get("alias_review_groups", []) or []:
            if not isinstance(group, dict):
                continue
            pending_child_ids = [
                str(child_id)
                for child_id in group.get("child_proposal_ids", []) or []
                if str(child_id).strip() and str(child_id) not in decisions
            ]
            if pending_child_ids:
                grouped_child_ids.update(pending_child_ids)
                pending_groups.append({**group, "pending_child_proposal_ids": pending_child_ids})
        pending = list(pending_groups)
        pending.extend(
            proposal
            for proposal in proposals
            if str(proposal.get("proposal_id", "")).strip()
            and str(proposal.get("proposal_id", "")) not in decisions
            and str(proposal.get("proposal_id", "")) not in grouped_child_ids
            and str(proposal.get("review_status", "pending")) == "pending"
        )
        return pending

    def pending_claims(self) -> tuple[list[dict[str, Any]] | None, str]:
        claims, reason = _load_patches_or_reason(self.paths["patches"])
        if claims is None:
            return None, reason
        decisions = _decision_ids(self.paths["decisions"], ["claim_id"])
        return [
            claim
            for claim in claims
            if str(claim.get("claim_id", "")).strip() and str(claim.get("claim_id", "")) not in decisions
        ], ""

    def pending_identity_merges(self) -> list[dict[str, Any]]:
        proposals = _read_json_or_default(self.paths["identity_merge_proposals"], {"proposals": []}).get("proposals", [])
        decisions = _decision_ids(self.paths["identity_merge_decisions"], ["proposal_id", "merge_id"])
        return [
            proposal
            for proposal in proposals
            if str(proposal.get("proposal_id", "")).strip()
            and str(proposal.get("proposal_id", "")) not in decisions
            and str(proposal.get("review_status", "pending")) == "pending"
        ]

    def pending_cards(self) -> tuple[list[dict[str, Any]] | None, str]:
        cards, reason = _load_card_drafts_or_reason(self.paths["card_drafts"])
        if cards is None:
            return None, reason
        decisions = _decision_ids(self.paths["card_decisions"], ["card_id", "target_card_id"])
        return [
            card
            for card in cards
            if str(card.get("card_id", "")).strip() and str(card.get("card_id", "")) not in decisions
        ], ""

    def load_current_item(self) -> tuple[str, dict[str, Any] | None, str]:
        if self.new_run_selected:
            return (
                "new_run",
                None,
                "New run selected. Press Run Full Pipeline to create a fresh artifact folder under artifacts/runs.",
            )
        conversation_entities = self.pending_conversation_entities()
        if conversation_entities:
            return "conversation_entity", conversation_entities[0], "Conversation Entity Review"
        claims, claim_reason = self.pending_claims()
        if claims is None:
            return "bootstrap", None, claim_reason
        if claims:
            return "claim", claims[0], "Claim Review"
        identity_merges = self.pending_identity_merges()
        if identity_merges:
            return "identity_merge", identity_merges[0], "Identity Merge Review"
        cards, card_reason = self.pending_cards()
        if cards is None:
            return "message", None, f"All claims reviewed. {card_reason}"
        if cards:
            return "card", cards[0], "Card Review"
        return "message", None, "All claims and synthesized card drafts reviewed."

    def refresh_review_item(self) -> None:
        self.current_kind, self.current_item, title = self.load_current_item()
        self.item_title_var.set(title)
        self.item_text.configure(state="normal")
        self.item_text.delete("1.0", tk.END)
        if self.current_item is None:
            self.item_text.insert("1.0", title)
        else:
            self.item_text.insert("1.0", json.dumps(self.current_item, ensure_ascii=False, indent=2))
        self.item_text.configure(state="disabled")
        self.rationale_text.delete("1.0", tk.END)
        self.configure_decision_controls()
        self.refresh_runs()

    def configure_decision_controls(self) -> None:
        for child in self.extra_frame.winfo_children():
            child.destroy()
        enabled = self.current_item is not None and self.current_kind in {"conversation_entity", "claim", "identity_merge", "card"}
        state = tk.NORMAL if enabled else tk.DISABLED
        for button in [self.primary_button, self.reject_button, self.defer_button, self.more_context_button]:
            button.configure(state=state)

        if self.current_kind == "conversation_entity" and self.current_item is not None:
            is_alias_group = self.current_item.get("group_kind") == "alias_review_group"
            self.primary_button.configure(text="Approve Alias Group" if is_alias_group else "Approve Entity", command=lambda: self.save_decision("approve"))
            ttk.Label(self.extra_frame, text="Canonical Name").grid(row=0, column=0, sticky="w")
            self.canonical_name_var = tk.StringVar(
                value=str(self.current_item.get("suggested_canonical_name") or self.current_item.get("candidate_name", ""))
            )
            ttk.Entry(self.extra_frame, textvariable=self.canonical_name_var).grid(row=1, column=0, sticky="ew", pady=(2, 8))
            ttk.Label(self.extra_frame, text="Entity Type").grid(row=2, column=0, sticky="w")
            self.entity_type_var = tk.StringVar(value=str(self.current_item.get("proposed_entity_type", "term")))
            ttk.Combobox(self.extra_frame, textvariable=self.entity_type_var, values=ENTITY_REVIEW_TYPES, state="readonly").grid(row=3, column=0, sticky="ew", pady=(2, 0))
        elif self.current_kind == "identity_merge":
            self.primary_button.configure(text="Approve Merge", command=lambda: self.save_decision("approve"))
        elif self.current_kind == "card":
            self.primary_button.configure(text="Approve Canonical", command=lambda: self.save_decision("approve"))
            ttk.Label(self.extra_frame, text="Edited Summary (optional)").grid(row=0, column=0, sticky="w")
            self.edited_summary_text = tk.Text(self.extra_frame, height=5, wrap=tk.WORD)
            self.edited_summary_text.grid(row=1, column=0, sticky="ew", pady=(2, 0))
        else:
            self.primary_button.configure(text="Accept", command=lambda: self.save_decision("accept"))

    def save_decision(self, decision: str) -> None:
        if self.current_item is None:
            return
        rationale = self.rationale_text.get("1.0", tk.END).strip()
        reviewer = self.reviewer_var.get().strip() or "human_reviewer"
        timestamp = now_utc_iso()

        if self.current_kind == "claim":
            payload = {
                "claim_id": self.current_item["claim_id"],
                "decision": decision,
                "reviewer": reviewer,
                "rationale": rationale,
                "timestamp_utc": timestamp,
            }
            path = self.paths["decisions"]
        elif self.current_kind == "card":
            payload = {
                "card_id": self.current_item["card_id"],
                "decision": decision,
                "reviewer": reviewer,
                "rationale": rationale,
                "timestamp_utc": timestamp,
            }
            edited_summary = getattr(self, "edited_summary_text", None)
            if edited_summary is not None:
                value = edited_summary.get("1.0", tk.END).strip()
                if value:
                    payload["edited_summary"] = value
            path = self.paths["card_decisions"]
        elif self.current_kind == "identity_merge":
            payload = {
                "proposal_id": self.current_item["proposal_id"],
                "decision": decision,
                "reviewer": reviewer,
                "rationale": rationale,
                "timestamp_utc": timestamp,
            }
            path = self.paths["identity_merge_decisions"]
        elif self.current_kind == "conversation_entity":
            if self.current_item.get("group_kind") == "alias_review_group":
                path = self.paths["conversation_entity_decisions"]
                data = _read_json_or_default(path, {"decisions": []})
                existing = {
                    str(item.get("proposal_id", ""))
                    for item in data.get("decisions", [])
                    if isinstance(item, dict) and str(item.get("proposal_id", "")).strip()
                }
                canonical_name = getattr(self, "canonical_name_var").get().strip()
                entity_type = getattr(self, "entity_type_var").get().strip() or str(self.current_item.get("proposed_entity_type", "term") or "term")
                pending_child_ids = {
                    str(child_id)
                    for child_id in self.current_item.get("pending_child_proposal_ids", []) or self.current_item.get("child_proposal_ids", []) or []
                }
                for alias in self.current_item.get("alias_candidates", []) or []:
                    if not isinstance(alias, dict):
                        continue
                    proposal_id = str(alias.get("proposal_id", "")).strip()
                    if not proposal_id or proposal_id in existing or proposal_id not in pending_child_ids:
                        continue
                    data.setdefault("decisions", []).append(
                        {
                            "proposal_id": proposal_id,
                            "candidate_name": alias.get("candidate_name", ""),
                            "decision": decision,
                            "canonical_name": canonical_name,
                            "entity_type": entity_type,
                            "reviewer": reviewer,
                            "rationale": rationale,
                            "timestamp_utc": timestamp,
                            "human_override": True,
                        }
                    )
                write_json(path, data)
                self.refresh_review_item()
                if self.candidate_browser is not None and self.candidate_browser.winfo_exists():
                    self.candidate_browser.reload()
                return
            payload = {
                "proposal_id": self.current_item["proposal_id"],
                "candidate_name": self.current_item.get("candidate_name", ""),
                "decision": decision,
                "canonical_name": getattr(self, "canonical_name_var").get().strip(),
                "entity_type": getattr(self, "entity_type_var").get().strip() or "term",
                "reviewer": reviewer,
                "rationale": rationale,
                "timestamp_utc": timestamp,
                "human_override": True,
            }
            path = self.paths["conversation_entity_decisions"]
        else:
            return

        data = _read_json_or_default(path, {"decisions": []})
        data.setdefault("decisions", []).append(payload)
        write_json(path, data)
        self.refresh_review_item()
        if self.candidate_browser is not None and self.candidate_browser.winfo_exists():
            self.candidate_browser.reload()

    def open_candidate_inventory_browser(self) -> None:
        if self.new_run_selected:
            messagebox.showinfo("No Run Selected", "Select an existing run before browsing candidate inventory.")
            return
        if self.candidate_browser is not None and self.candidate_browser.winfo_exists():
            self.candidate_browser.lift()
            self.candidate_browser.reload()
            return
        self.candidate_browser = CandidateInventoryWindow(self)

    def set_log_text(self) -> None:
        self.logs_text.configure(state="normal")
        self.logs_text.delete("1.0", tk.END)
        self.logs_text.insert("1.0", "\n".join(self.pipeline_logs) if self.pipeline_logs else "(no logs yet)")
        self.logs_text.see(tk.END)
        self.logs_text.configure(state="disabled")

    def refresh_progress(self) -> None:
        progress_logs = self.progress_logs
        status = self.pipeline_status
        message = self.pipeline_message
        if not self.pipeline_active() and not self.new_run_selected:
            artifact_snapshot = pipeline_progress_artifact_snapshot(self.artifacts_root)
            artifact_logs = artifact_snapshot.get("logs", [])
            if artifact_logs:
                progress_logs = list(artifact_logs) + list(self.progress_logs)
            artifact_status = str(artifact_snapshot.get("status", "idle"))
            if artifact_status == "review_required" or (status == "idle" and artifact_status != "idle"):
                status = artifact_status
                message = str(artifact_snapshot.get("message", "")) or message
        progress = pipeline_progress_from_logs(
            progress_logs,
            status,
            message,
            self.last_exit_code,
        )
        self.progress_chart.set_progress(progress)
        msg = f"Status: {status}"
        if message:
            msg += f" | {message}"
        self.pipeline_status_var.set(msg)
        active = self.pipeline_active()
        self.run_button.configure(state=tk.DISABLED if active else tk.NORMAL)
        self.cancel_button.configure(state=tk.NORMAL if active else tk.DISABLED)
        self.attach_button.configure(state=tk.DISABLED if active else tk.NORMAL)

    def progress_line_for_log(self, line: str) -> str:
        if self.attached_process_kind != "run_from_b4":
            return line
        match = re.search(r"\[(\d+)/5\]\s+(START|DONE)\s+", line)
        if not match:
            return line
        mapped_index = RUN_FROM_STAGE05_STAGE_INDEX_MAP.get(int(match.group(1)))
        if mapped_index is None:
            return line
        return re.sub(r"\[\d+/5\]", f"[{mapped_index}/9]", line, count=1)

    def append_pipeline_log(self, line: str) -> None:
        clean = line.rstrip()
        self.pipeline_logs.append(clean)
        if len(self.pipeline_logs) > 1200:
            del self.pipeline_logs[:-1200]
        progress_line = self.progress_line_for_log(clean)
        if is_pipeline_progress_log_line(progress_line):
            self.progress_logs.append(progress_line)
        self.set_log_text()
        self.refresh_progress()

    def remember_progress_log(self, line: str) -> None:
        if line not in self.progress_logs:
            self.progress_logs.append(line)

    def attach_to_running_process(self, silent: bool = False, allow_switch: bool = True) -> None:
        if self.process is not None or self.attached_pid is not None:
            if not silent:
                messagebox.showinfo("Pipeline Running", "The desktop app is already tracking a pipeline worker.")
            return
        processes = discover_running_pipeline_processes()
        if not processes:
            if not silent:
                messagebox.showinfo("No Running Pipeline", "No running pipeline worker with an artifact folder was found.")
            return

        target: dict[str, Any] | None = None
        if not self.new_run_selected:
            for process in processes:
                if Path(process["artifacts_root"]).resolve() == self.artifacts_root.resolve():
                    target = process
                    break
        if target is None and allow_switch:
            target = processes[0]
        if target is None:
            if not silent:
                messagebox.showinfo("No Matching Pipeline", "A pipeline is running, but not for the selected artifact folder.")
            return

        target_root = Path(target["artifacts_root"]).resolve()
        if target_root != self.artifacts_root.resolve():
            self.new_run_selected = False
            self.resolve_paths(target_root)
            self.refresh_runs()
            self.refresh_review_item()

        self.attached_pid = int(target["pid"])
        self.attached_process_kind = str(target.get("kind", "pipeline"))
        self.attached_command_line = str(target.get("command_line", ""))
        self.attached_log_paths = attach_log_paths_for_run(self.artifacts_root, self.attached_process_kind)
        self.attached_log_offsets = {}
        self.pipeline_status = "running"
        self.pipeline_message = f"Attached to existing worker PID {self.attached_pid}."
        self.last_exit_code = None
        self.cancel_requested = False
        self.pipeline_logs = []
        self.progress_logs = []
        self.append_pipeline_log(f"[desktop] Attached to {self.attached_process_kind} worker PID {self.attached_pid}.")
        self.append_pipeline_log(f"$ {self.attached_command_line.strip()}")
        self.append_existing_attached_log_tail()
        self.sync_attached_artifact_progress()
        self.refresh_progress()

    def append_existing_attached_log_tail(self) -> None:
        for path in self.attached_log_paths:
            try:
                text = path.read_text(encoding="utf-8", errors="replace")
                self.attached_log_offsets[path] = path.stat().st_size
            except Exception:
                continue
            lines = text.splitlines()
            if lines:
                self.append_pipeline_log(f"[desktop] Showing tail of {_display_path(path, self.repo_root)}.")
            for line in lines[-80:]:
                self.append_pipeline_log(line)
        if not self.attached_log_paths:
            self.append_pipeline_log("[desktop] No log file found yet; progress will be inferred from artifacts.")

    def read_new_attached_log_lines(self) -> None:
        for path in self.attached_log_paths:
            try:
                size = path.stat().st_size
                offset = self.attached_log_offsets.get(path, 0)
                if size < offset:
                    offset = 0
                if size == offset:
                    continue
                with path.open("r", encoding="utf-8", errors="replace") as handle:
                    handle.seek(offset)
                    chunk = handle.read()
                    self.attached_log_offsets[path] = handle.tell()
            except Exception:
                continue
            for line in chunk.splitlines():
                self.append_pipeline_log(line)

    def sync_attached_artifact_progress(self) -> None:
        if self.attached_pid is None:
            return
        if self.attached_process_kind == "run_from_b4":
            patch_notes_path = self.artifacts_root / "02_timeline" / "conversation_patch_notes.json"
            payload = _read_json_or_default(patch_notes_path, {})
            conversation_count = int(payload.get("conversation_count", 0) or 0)
            notes_count = int(payload.get("notes_count", len(payload.get("notes", [])) if isinstance(payload.get("notes"), list) else 0) or 0)
            failure_count = int(payload.get("failure_count", 0) or 0)
            status = str(payload.get("status", "")).strip()
            if conversation_count:
                self.remember_progress_log("[5/9] START Stage 05 Conversation Patch Notes")
                if status == "complete":
                    self.remember_progress_log("[5/9] DONE  Stage 05 Conversation Patch Notes")
                downstream_started = any(re.search(r"\[[6-9]/9\]\s+(START|DONE)", line) for line in self.progress_logs)
                if not downstream_started:
                    processed = min(notes_count + failure_count, conversation_count)
                    self.pipeline_message = (
                        f"Attached to PID {self.attached_pid}. Stage 05 patch notes: "
                        f"{processed}/{conversation_count} conversations, notes={notes_count}, failures={failure_count}."
                    )

    def poll_attached_process(self) -> None:
        if self.attached_pid is not None:
            self.read_new_attached_log_lines()
            self.sync_attached_artifact_progress()
            if not process_id_exists(self.attached_pid):
                pid = self.attached_pid
                self.attached_pid = None
                self.attached_log_paths = []
                self.attached_log_offsets = {}
                logs_text = "\n".join(self.pipeline_logs[-250:])
                logs_lower = logs_text.lower()
                self.append_pipeline_log(f"[desktop] Attached worker PID {pid} exited.")
                if self.cancel_requested:
                    self.pipeline_status = "cancelled"
                    self.pipeline_message = f"Attached worker PID {pid} was cancelled."
                    self.cancel_requested = False
                elif any(marker.lower() in logs_lower for marker in REVIEW_REQUIRED_MARKERS):
                    self.pipeline_status = "review_required"
                    self.pipeline_message = "Pipeline paused for review. Review the pending decisions, then rerun from this artifact folder."
                elif "pipeline complete" in logs_lower or (self.artifacts_root / "08_notion" / "notion_import.ndjson").exists():
                    self.pipeline_status = "succeeded"
                    self.pipeline_message = f"Attached run completed. Notion export path: {self.artifacts_root / '08_notion' / 'notion_import.ndjson'}"
                else:
                    self.pipeline_status = "failed"
                    self.pipeline_message = "Attached worker exited before a completion or review marker was detected."
                self.refresh_runs()
                self.refresh_review_item()
            self.refresh_progress()
        self.root.after(2000, self.poll_attached_process)

    def cancel_current_run(self) -> None:
        if not self.pipeline_active():
            return
        self.cancel_requested = True
        self.pipeline_status = "cancelling"
        self.pipeline_message = "Cancel requested. Stopping pipeline worker..."
        self.append_pipeline_log("[desktop] Cancel requested. Stopping pipeline worker...")
        process = self.process
        if process is not None:
            try:
                stop_process_tree(process)
                self.append_pipeline_log(f"[desktop] Stop signal sent to worker PID {process.pid}.")
            except Exception as exc:
                self.append_pipeline_log(f"[desktop] Failed to stop worker process: {exc}")
        elif self.attached_pid is not None:
            try:
                stop_process_tree_by_pid(self.attached_pid)
                self.append_pipeline_log(f"[desktop] Stop signal sent to attached worker PID {self.attached_pid}.")
            except Exception as exc:
                self.append_pipeline_log(f"[desktop] Failed to stop attached worker process: {exc}")
        else:
            self.append_pipeline_log("[desktop] Worker process has not reported a PID yet; it will be stopped as soon as it starts.")

        def cancel_remote_batches() -> None:
            jobs = cancellable_gemini_batches_for_run(self.artifacts_root)
            if not jobs:
                self.log_queue.put(("log", "[desktop] No cancellable Gemini batch jobs recorded for this run."))
                return
            for job_name in jobs:
                self.log_queue.put(("log", f"[desktop] {cancel_gemini_batch(job_name)}"))

        threading.Thread(target=cancel_remote_batches, daemon=True).start()
        self.refresh_progress()

    def run_auto_review(self) -> None:
        """Launch AI auto-review of all pending items in a background thread."""
        if self.pipeline_active():
            messagebox.showinfo("Pipeline Running", "Finish or cancel the active pipeline run before starting auto-review.")
            return
        if self.new_run_selected:
            messagebox.showinfo("No Run Selected", "Select an existing run with pending items before running auto-review.")
            return
        counts = pending_review_counts_for_root(self.artifacts_root)
        total = pending_review_total(counts)
        if total == 0:
            messagebox.showinfo("Nothing to Review", "No pending review items found for the selected run.")
            return
        if not messagebox.askyesno(
            "AI Auto-Review",
            f"This will use the Gemini API to automatically review {total} pending item(s):\n\n"
            f"  • {counts.get('conversation_entities', 0)} conversation entities\n"
            f"  • {counts.get('claims', 0)} claims\n"
            f"  • {counts.get('identity_merges', 0)} identity merges\n"
            f"  • {counts.get('cards', 0)} cards\n\n"
            "Decisions will be saved with reviewer='gemini_auto_review'.\n"
            "You can still override any AI decision manually afterwards.\n\n"
            "Continue?",
        ):
            return

        self.pipeline_status = "running"
        self.pipeline_message = "AI auto-review in progress..."
        self.pipeline_logs = ["[auto-review] Starting AI auto-review..."]
        self.progress_logs = []
        self.last_exit_code = None
        self.set_log_text()
        self.refresh_progress()

        def worker() -> None:
            from pipeline.auto_review import run_auto_review as _run_auto_review

            def progress_cb(msg: str) -> None:
                self.log_queue.put(("log", msg))

            try:
                ar_result = _run_auto_review(
                    self.paths,
                    progress_callback=progress_cb,
                    cancel_check=lambda: getattr(self, "cancel_requested", False),
                )
                self.log_queue.put(("auto_review_done", ar_result.summary()))
            except Exception as exc:
                self.log_queue.put(("error", f"Auto-review failed: {exc}"))

        threading.Thread(target=worker, daemon=True).start()

    def run_full_pipeline(self) -> None:
        if self.pipeline_active():
            return
        docx_path = _resolve_docx(self.repo_root, self.docx_hint)
        if docx_path is None:
            messagebox.showerror("Missing DOCX", "Could not find the lore DOCX. Pass --docx or place it in the project root.")
            return
        if not self.conversations_root.exists():
            messagebox.showerror("Missing Conversations", f"Conversations root was not found:\n{self.conversations_root}")
            return

        resume_existing = not self.new_run_selected
        if self.new_run_selected:
            fresh_root = new_run_artifacts_root(self.repo_root)
            self.resolve_paths(fresh_root)
            self.new_run_selected = False
            self.refresh_runs()

        cmd = _pipeline_worker_command(docx_path, self.conversations_root, self.artifacts_root, resume=resume_existing)
        self.pipeline_status = "running"
        action = "Pipeline resume" if resume_existing else "Pipeline run"
        self.pipeline_message = f"{action} started in {_display_path(self.artifacts_root, self.repo_root)}."
        self.last_exit_code = None
        self.cancel_requested = False
        self.attached_pid = None
        self.attached_process_kind = ""
        self.attached_command_line = ""
        self.attached_log_paths = []
        self.attached_log_offsets = {}
        self.pipeline_logs = [f"$ {' '.join(cmd)}"]
        self.progress_logs = []
        self.set_log_text()
        self.refresh_progress()

        def worker() -> None:
            env = os.environ.copy()
            env["PYTHONUNBUFFERED"] = "1"
            try:
                with subprocess.Popen(
                    cmd,
                    cwd=self.repo_root,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    text=True,
                    bufsize=1,
                    env=env,
                ) as process:
                    self.process = process
                    self.log_queue.put(("process_started", process.pid))
                    if self.cancel_requested:
                        stop_process_tree(process)
                    if process.stdout is not None:
                        for line in process.stdout:
                            self.log_queue.put(("log", line))
                    exit_code = process.wait()
                self.log_queue.put(("done", exit_code))
            except Exception as exc:
                self.log_queue.put(("error", str(exc)))

        threading.Thread(target=worker, daemon=True).start()

    def drain_log_queue(self) -> None:
        try:
            while True:
                event, payload = self.log_queue.get_nowait()
                if event == "log":
                    self.append_pipeline_log(str(payload))
                elif event == "process_started":
                    self.pipeline_message = f"Pipeline worker started with PID {payload}."
                    self.refresh_progress()
                elif event == "auto_review_done":
                    self.pipeline_status = "succeeded"
                    self.pipeline_message = str(payload)
                    self.append_pipeline_log(f"[desktop] {payload}")
                    self.refresh_review_item()
                    self.refresh_progress()
                    if self.candidate_browser is not None and self.candidate_browser.winfo_exists():
                        self.candidate_browser.reload()
                elif event == "done":
                    self.last_exit_code = int(payload)
                    self.process = None
                    if self.cancel_requested:
                        self.pipeline_status = "cancelled"
                        self.pipeline_message = f"Pipeline cancelled by user. Worker exit code: {self.last_exit_code}."
                        self.cancel_requested = False
                    elif self.last_exit_code == 2:
                        self.pipeline_status = "review_required"
                        self.pipeline_message = "Pipeline paused for review. Review the pending decisions, then rerun from the selected artifact folder."
                    else:
                        self.pipeline_status = "succeeded" if self.last_exit_code == 0 else "failed"
                        self.pipeline_message = (
                            f"Pipeline completed successfully. Notion export path: {self.artifacts_root / '08_notion' / 'notion_import.ndjson'}"
                            if self.last_exit_code == 0
                            else f"Pipeline failed with exit code {self.last_exit_code}."
                        )
                    self.refresh_review_item()
                    self.refresh_progress()
                elif event == "error":
                    self.process = None
                    self.pipeline_status = "failed"
                    self.pipeline_message = "Pipeline run failed before completion."
                    self.cancel_requested = False
                    self.append_pipeline_log(f"[desktop] Failed to start or stream pipeline: {payload}")
                    self.refresh_progress()
        except queue.Empty:
            pass
        self.root.after(200, self.drain_log_queue)


def run_smoke_test(repo_root: Path, artifacts_root: Path | None) -> int:
    selected_root = choose_initial_artifacts_root(repo_root, artifacts_root)
    _resolve_input_paths(None, None, None, selected_root)
    counts = pending_review_counts_for_root(selected_root)
    print(
        json.dumps(
            {
                "project_root": str(repo_root),
                "artifacts_root": str(selected_root),
                "pending": counts,
                "pending_total": pending_review_total(counts),
                "has_gemini_key": bool(os.environ.get("GEMINI_API_KEY") or os.environ.get("GOOGLE_API_KEY")),
            },
            indent=2,
        )
    )
    return 0


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the THERIAC desktop review app.")
    parser.add_argument("--project-root", type=Path, required=False)
    parser.add_argument("--artifacts-root", type=Path, required=False)
    parser.add_argument("--docx", type=Path, required=False)
    parser.add_argument("--conversations-root", type=Path, required=False)
    parser.add_argument("--pipeline-worker", action="store_true")
    parser.add_argument("--smoke-test", action="store_true")
    args, _ = parser.parse_known_args(argv)
    return args


def main() -> None:
    args = parse_args(sys.argv[1:])
    repo_root = find_project_root(args.project_root)
    os.chdir(repo_root)
    load_project_env(repo_root)

    if args.pipeline_worker:
        from pipeline.run_pipeline import main as pipeline_main

        forwarded = [arg for arg in sys.argv[1:] if arg != "--pipeline-worker"]
        sys.argv = ["pipeline.run_pipeline", *forwarded]
        pipeline_main()
        return

    if args.smoke_test:
        raise SystemExit(run_smoke_test(repo_root, args.artifacts_root))

    artifacts_root = choose_initial_artifacts_root(repo_root, args.artifacts_root)
    root = tk.Tk()
    try:
        ttk.Style().theme_use("vista")
    except tk.TclError:
        pass
    TheriacDesktopApp(root, repo_root, artifacts_root, args.docx, args.conversations_root)
    root.mainloop()


if __name__ == "__main__":
    main()
