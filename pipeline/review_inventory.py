from __future__ import annotations

import json
import os
import re
import subprocess
import sys
import textwrap
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

from pipeline.common import now_utc_iso, stable_id, write_json
from pipeline.entity_resolution import card_id_for_entity, load_entity_records, normalized_name_key
from pipeline.ui_review_app import (
    _claim_attention_by_id,
    _human_decision_ids,
    _load_patches_or_reason,
    _read_json_or_default,
    discover_review_runs,
    load_last_open_artifacts_root,
    pending_review_counts_for_root,
    pending_review_total,
)

ENTITY_REVIEW_TYPES = ["term", "theme", "quest", "event", "character", "faction", "organization", "location", "timeline_node"]
AUTHOR_CLAIM_TYPES = [
    "relationship",
    "role",
    "background",
    "timeline",
    "inspiration",
    "open_question",
    "alias",
    "lore_fact",
    "meta_note",
    "other",
]
AUTHOR_CLAIM_TRACKS = ["lore", "meta", "both"]


def ctrl_backspace_delete_start(text_before_cursor: str) -> int:
    """Return the character offset where Ctrl+Backspace deletion should start."""
    i = len(text_before_cursor)
    while i > 0 and text_before_cursor[i - 1].isspace():
        i -= 1
    if i <= 0:
        return 0
    if text_before_cursor[i - 1].isalnum() or text_before_cursor[i - 1] in "_'":
        while i > 0 and (text_before_cursor[i - 1].isalnum() or text_before_cursor[i - 1] in "_'"):
            i -= 1
    else:
        while i > 0 and not text_before_cursor[i - 1].isspace() and not (
            text_before_cursor[i - 1].isalnum() or text_before_cursor[i - 1] in "_'"
        ):
            i -= 1
    return i


def ctrl_delete_delete_end(text_after_cursor: str) -> int:
    i = 0
    length = len(text_after_cursor)
    while i < length and text_after_cursor[i].isspace():
        i += 1
    if i >= length:
        return length
    if text_after_cursor[i].isalnum() or text_after_cursor[i] in "_'":
        while i < length and (text_after_cursor[i].isalnum() or text_after_cursor[i] in "_'"):
            i += 1
    else:
        while i < length and not text_after_cursor[i].isspace() and not (
            text_after_cursor[i].isalnum() or text_after_cursor[i] in "_'"
        ):
            i += 1
    return i



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


def _display_value(value: Any, fallback: str = "(none)") -> str:
    if value is None:
        return fallback
    if isinstance(value, float):
        return f"{value:.3f}".rstrip("0").rstrip(".")
    if isinstance(value, (list, tuple, set)):
        return _join_preview([str(item) for item in value if str(item).strip()], 8) or fallback
    if isinstance(value, dict):
        return json.dumps(value, ensure_ascii=False)
    text = str(value).strip()
    return text if text else fallback


def _wrap_block(text: Any, *, width: int = 110, indent: str = "") -> str:
    clean = re.sub(r"\s+", " ", str(text or "")).strip()
    if not clean:
        return indent + "(none)"
    return textwrap.fill(clean, width=width, initial_indent=indent, subsequent_indent=indent)


def _section(title: str, body: list[str]) -> list[str]:
    clean = [line for line in body if str(line).strip()]
    if not clean:
        return []
    return [title, "-" * len(title), *clean, ""]


def _kv(label: str, value: Any) -> str:
    return f"{label}: {_display_value(value)}"


def _source_snippet_previews(root: Path, source_ids: list[str], limit: int = 4) -> list[str]:
    if not source_ids:
        return []
    source_path = root / "03_relevance" / "snippets_candidates.jsonl"
    if not source_path.exists():
        return []
    wanted = {str(item) for item in source_ids[:limit]}
    previews: list[str] = []
    try:
        with source_path.open("r", encoding="utf-8") as handle:
            for line in handle:
                if len(previews) >= len(wanted):
                    break
                if not line.strip():
                    continue
                row = json.loads(line)
                snippet_id = str(row.get("snippet_id", ""))
                if snippet_id not in wanted:
                    continue
                text = (
                    row.get("patch_item_text")
                    or row.get("display_text_normalized")
                    or row.get("conversation_patch_summary")
                    or ""
                )
                topic = row.get("conversation_topic_label") or row.get("conversation_patch_topic_label") or ""
                previews.append(
                    f"[{snippet_id}] {topic}\n"
                    + _wrap_block(text, indent="  ")
                )
    except Exception:
        return previews
    return previews


def format_review_item(kind: str, item: dict[str, Any], artifacts_root: Path) -> str:
    if kind == "claim":
        source_ids = [str(item_id) for item_id in item.get("source_snippet_ids", []) or [] if str(item_id).strip()]
        lines: list[str] = []
        lines.extend(
            _section(
                "Claim",
                [
                    _wrap_block(item.get("claim_text", "")),
                ],
            )
        )
        lines.extend(
            _section(
                "Review Context",
                [
                    _kv("Target", item.get("target_entity_name") or item.get("target_entity_id")),
                    _kv("Claim type", item.get("claim_type")),
                    _kv("Confidence", item.get("confidence")),
                    _kv("Status", item.get("status")),
                    _kv("Claim ID", item.get("claim_id")),
                ],
            )
        )
        warnings = [str(w) for w in item.get("support_warnings", []) or [] if str(w).strip()]
        notes = str(item.get("contradiction_notes", "")).strip()
        lines.extend(
            _section(
                "Cautions",
                [
                    _kv("Support warnings", warnings),
                    _kv("Contradiction notes", notes),
                ],
            )
        )
        attention = item.get("auto_review_attention", {}) if isinstance(item.get("auto_review_attention"), dict) else {}
        if attention:
            lines.extend(
                _section(
                    "Auto-Review Attention",
                    [
                        _kv("Decision", attention.get("decision")),
                        _kv("Reason", attention.get("human_review_reason")),
                        _kv("Duplicate group", attention.get("duplicate_claim_ids", [])),
                        _kv("Source-set group", attention.get("source_set_claim_ids", [])),
                    ],
                )
            )
        evidence = _source_snippet_previews(artifacts_root, source_ids)
        if not evidence:
            evidence = [_kv("Source snippet IDs", source_ids)]
        lines.extend(_section("Evidence Preview", evidence))
        hints = item.get("proposed_relationship_hints", [])
        if hints:
            hint_lines: list[str] = []
            for hint in hints[:4]:
                if isinstance(hint, dict):
                    label = _display_value(hint.get("relation_type"), "relationship_hint")
                    confidence = hint.get("confidence")
                    suffix = f" (confidence {_display_value(confidence)})" if confidence is not None else ""
                    note = _wrap_block(hint.get("note", ""), indent="  ")
                    hint_lines.append(f"{label}{suffix}\n{note}")
                else:
                    hint_lines.append(_wrap_block(hint))
            lines.extend(_section("Relationship Hints", hint_lines))
        return "\n".join(lines).strip()

    if kind == "conversation_entity":
        sample_texts = [_wrap_block(text, indent="  ") for text in item.get("sample_texts", []) or []]
        alias_candidates = []
        for alias in item.get("alias_candidates", []) or []:
            if isinstance(alias, dict):
                alias_candidates.append(
                    f"- {alias.get('candidate_name', '(unnamed)')} -> {item.get('suggested_canonical_name') or item.get('candidate_name')}"
                )
        lines = []
        lines.extend(
            _section(
                "Entity Candidate",
                [
                    _kv("Candidate", item.get("candidate_name")),
                    _kv("Suggested canonical", item.get("suggested_canonical_name") or item.get("candidate_name")),
                    _kv("Proposed type", item.get("proposed_entity_type")),
                    _kv("Evidence count", item.get("evidence_count")),
                    _kv("Priority", item.get("review_priority")),
                    _kv("Proposal ID", item.get("proposal_id")),
                ],
            )
        )
        lines.extend(
            _section(
                "Why It Is Here",
                [
                    _wrap_block(item.get("triage_reason") or item.get("proposal_reason") or ""),
                    _kv("Topics", item.get("candidate_topics", [])),
                    _kv("Tracks", item.get("knowledge_tracks", [])),
                    _kv("Type votes", item.get("type_vote_totals", {})),
                ],
            )
        )
        lines.extend(_section("Alias Candidates", alias_candidates))
        lines.extend(_section("Evidence Samples", sample_texts))
        return "\n".join(lines).strip()

    if kind == "identity_merge":
        member_names = []
        for member in item.get("member_entities", []) or []:
            if isinstance(member, dict):
                label = str(member.get("canonical_name") or member.get("entity_id") or "").strip()
                entity_type = str(member.get("entity_type", "")).strip()
                if label:
                    member_names.append(f"{label} ({entity_type})" if entity_type else label)
        edge_lines = []
        for edge in item.get("member_edges", []) or []:
            if isinstance(edge, dict):
                edge_lines.append(
                    f"{_display_value(edge.get('source_entity_name'))} -> {_display_value(edge.get('target_entity_name'))} "
                    f"via {_join_preview(_as_text_list(edge.get('evidence_claim_ids')), 4) or 'evidence'}"
                )
        lines = []
        lines.extend(
            _section(
                "Identity Cluster",
                [
                    _kv("Suggested canonical", item.get("canonical_name") or item.get("target_entity_name")),
                    _kv("Canonical entity ID", item.get("canonical_entity_id") or item.get("target_entity_id")),
                    _kv("Members", member_names or [
                        f"{_display_value(item.get('source_entity_name') or item.get('source_entity_id'))} -> "
                        f"{_display_value(item.get('target_entity_name') or item.get('target_entity_id'))}"
                    ]),
                    _kv("Aliases", item.get("alias_texts", []) or item.get("alias_text")),
                    _kv("Former names", item.get("former_names", [])),
                    _kv("Working names", item.get("working_names", [])),
                    _kv("Formal names", item.get("formal_names", [])),
                    _kv("Review flags", item.get("cluster_review_flags", [])),
                    _kv("Suggested exclusions", item.get("suggested_split_entity_ids", [])),
                    _kv("Confidence", item.get("confidence")),
                    _kv("Proposal ID", item.get("proposal_id")),
                ],
            )
        )
        lines.extend(_section("Identity Evidence Edges", edge_lines))
        lines.extend(
            _section(
                "Evidence",
                [
                    _kv("Evidence claim IDs", item.get("evidence_claim_ids", [])),
                    _wrap_block(item.get("rationale") or item.get("reason") or ""),
                ],
            )
        )
        return "\n".join(lines).strip()

    if kind == "card_architecture":
        action_type = str(item.get("action_type", "")).strip()
        lines = []
        lines.extend(
            _section(
                "Card Architecture Action",
                [
                    _kv("Action", action_type),
                    _kv("Source", item.get("source_entity_name") or item.get("source_card_id")),
                    _kv("Target", item.get("target_entity_name") or item.get("target_card_id")),
                    _kv("Section", item.get("target_section")),
                    _kv("Confidence", item.get("confidence")),
                    _kv("Action ID", item.get("action_id")),
                    _kv("Request ID", item.get("request_id")),
                ],
            )
        )
        details: list[str] = []
        for key, label in [
            ("rationale", "Rationale"),
            ("instruction_text", "Instruction"),
            ("claim_text", "Author Claim"),
            ("alias_text", "Alias"),
            ("new_canonical_name", "New Name"),
            ("relationship_type", "Relationship"),
            ("clarification_question", "Clarification"),
        ]:
            value = str(item.get(key, "")).strip()
            if value:
                details.append(_kv(label, value))
        if details:
            lines.extend(_section("Proposed Change", details))
        lines.extend(
            _section(
                "Affected Items",
                [
                    _kv("Claim IDs", item.get("claim_ids") or item.get("affected_claim_ids", [])),
                    _kv("Affected cards", item.get("affected_cards", [])),
                    _kv("Validation", item.get("validation_status", "valid")),
                    _kv("Warnings", item.get("validation_warnings", [])),
                    _kv("Errors", item.get("validation_errors", [])),
                ],
            )
        )
        return "\n".join(lines).strip()

    if kind == "card":
        details = item.get("details", {}) if isinstance(item.get("details", {}), dict) else {}
        sections = details.get("sections", {}) if isinstance(details.get("sections", {}), dict) else {}
        lines = []
        lines.extend(
            _section(
                "Card Draft",
                [
                    _kv("Name", item.get("canonical_name")),
                    _kv("Type", item.get("entity_type")),
                    _kv("Status", item.get("status")),
                    _kv("Card ID", item.get("card_id")),
                ],
            )
        )
        lines.extend(_section("Lead Summary", [_wrap_block(item.get("summary", ""))]))
        for section_name in ["background", "role_in_story", "relationships", "timeline", "inspirations", "open_questions"]:
            text = str(sections.get(section_name, "")).strip()
            if text:
                lines.extend(_section(section_name.replace("_", " ").title(), [_wrap_block(text)]))
        relationships = item.get("relationships", []) or []
        timeline = item.get("timeline", []) or []
        if relationships:
            lines.extend(_section("Structured Relationships", [_wrap_block(json.dumps(relationships[:8], ensure_ascii=False))]))
        if timeline:
            lines.extend(_section("Structured Timeline", [_wrap_block(json.dumps(timeline[:8], ensure_ascii=False))]))
        lines.extend(
            _section(
                "Review Context",
                [
                    _kv("Accepted claim IDs", details.get("accepted_claim_ids", [])),
                    _kv("Source evidence", item.get("source_evidence", [])),
                    _kv("Word counts", details.get("section_word_counts", {})),
                ],
            )
        )
        return "\n".join(lines).strip()

    return json.dumps(item, ensure_ascii=False, indent=2)


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


def _latest_claim_decisions(decisions_path: Path | None) -> dict[str, dict[str, Any]]:
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
        claim_id = str(decision.get("claim_id", "")).strip()
        if not claim_id:
            continue
        existing = latest.get(claim_id)
        if existing is None or priority(decision) >= priority(existing):
            latest[claim_id] = decision
    return latest


def _latest_identity_merge_decisions(decisions_path: Path | None) -> dict[str, dict[str, Any]]:
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
        if str(decision.get("decision_scope", "")).strip() == "identity_edge":
            continue
        proposal_id = str(decision.get("proposal_id", "")).strip() or str(decision.get("merge_id", "")).strip()
        if not proposal_id:
            continue
        existing = latest.get(proposal_id)
        if existing is None or priority(decision) >= priority(existing):
            latest[proposal_id] = decision
    return latest


def _latest_identity_edge_decisions(decisions_path: Path | None) -> dict[str, dict[str, Any]]:
    if decisions_path is None:
        return {}
    payload = _read_json_or_default(decisions_path, {"decisions": []})
    latest: dict[str, dict[str, Any]] = {}
    for decision in payload.get("decisions", []) if isinstance(payload, dict) else []:
        if not isinstance(decision, dict):
            continue
        if str(decision.get("decision_scope", "")).strip() != "identity_edge":
            continue
        edge_id = str(decision.get("edge_proposal_id", "")).strip() or str(decision.get("edge_id", "")).strip()
        if edge_id:
            latest[edge_id] = decision
    return latest


def _identity_edge_bucket(decision: dict[str, Any]) -> str:
    action = str(decision.get("decision", "")).strip().lower()
    if action in {"approve", "accept", "keep", "restore"}:
        return "kept"
    if action in {"reject", "refute", "refuted"}:
        return "refuted"
    if action == "defer":
        return "deferred"
    if action == "needs_more_context":
        return "needs context"
    return "pending"


def author_claims_path_for_root(artifacts_root: Path) -> Path:
    return artifacts_root / "07_review" / "author_claims.json"


def _author_claim_normalized_text(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", str(text or "").lower()).strip()


def _normalize_author_claim_track(value: Any, claim_type: str, claim_text: str) -> str:
    track = str(value or "").strip().lower()
    if track not in AUTHOR_CLAIM_TRACKS:
        track = "lore"
    lower = str(claim_text or "").lower()
    meta_markers = [
        "working name",
        "canonical name",
        "later updated",
        "originally developed",
        "developed based",
        "inspired by",
        "inspiration",
        "player's",
        "player-facing",
        "gameplay",
        "game mechanic",
        "generic reference",
        "likely refer",
    ]
    if claim_type in {"meta_note", "open_question", "inspiration"} or any(marker in lower for marker in meta_markers):
        return "meta"
    return track


def _entity_lookup_options(artifacts_root: Path) -> list[dict[str, Any]]:
    entities_path = artifacts_root / "05_alias" / "resolved_entities.json"
    options: list[dict[str, Any]] = []
    for entity in load_entity_records(entities_path):
        canonical_name = str(entity.get("canonical_name", "")).strip()
        if not canonical_name:
            continue
        entity_type = str(entity.get("entity_type", "term") or "term")
        label = f"{canonical_name} ({entity_type})"
        options.append({"label": label, "entity": entity})
    return sorted(options, key=lambda row: str(row["label"]).lower())


def _resolve_author_claim_entity(artifacts_root: Path, target_text: str) -> dict[str, Any] | None:
    query = str(target_text or "").strip()
    if not query:
        return None
    entities = load_entity_records(artifacts_root / "05_alias" / "resolved_entities.json")
    query_key = normalized_name_key(re.sub(r"\s+\([^)]*\)\s*$", "", query).strip())
    for entity in entities:
        entity_id = str(entity.get("entity_id", "")).strip()
        card_id = str(entity.get("card_id", "")).strip()
        canonical_name = str(entity.get("canonical_name", "")).strip()
        if query in {entity_id, card_id}:
            return entity
        if canonical_name and normalized_name_key(canonical_name) == query_key:
            return entity
        for alias in entity.get("aliases", []) or []:
            alias_text = str(alias).strip()
            if alias_text and normalized_name_key(alias_text) == query_key:
                return entity
    return None


def load_author_claims_for_inventory(artifacts_root: Path) -> list[dict[str, Any]]:
    path = author_claims_path_for_root(artifacts_root)
    payload = _read_json_or_default(path, {"claims": []})
    claims: list[dict[str, Any]] = []
    for raw in payload.get("claims", []) if isinstance(payload, dict) else []:
        if not isinstance(raw, dict):
            continue
        claim_text = str(raw.get("claim_text", "")).strip()
        if not claim_text:
            continue
        target_entity_id = str(raw.get("target_entity_id", "")).strip()
        claim_type = str(raw.get("claim_type", "lore_fact") or "lore_fact")
        knowledge_track = _normalize_author_claim_track(raw.get("knowledge_track", ""), claim_type, claim_text)
        claim_id = str(raw.get("claim_id", "")).strip()
        if not claim_id:
            claim_id = stable_id("author_claim", target_entity_id, claim_type, claim_text)
        claims.append(
            {
                **raw,
                "claim_id": claim_id,
                "claim_text": claim_text,
                "claim_type": claim_type,
                "knowledge_track": knowledge_track,
                "source_snippet_ids": _as_text_list(raw.get("source_snippet_ids")),
                "confidence": raw.get("confidence", 1.0),
                "manual_claim": True,
                "author_claim": True,
                "source_priority": "author_claim",
                "status": str(raw.get("status", "accepted") or "accepted"),
                "normalized_claim_text": str(raw.get("normalized_claim_text") or _author_claim_normalized_text(claim_text)),
            }
        )
    return claims


def append_author_claim(
    artifacts_root: Path,
    target_text: str,
    claim_type: str,
    claim_text: str,
    reviewer: str,
    rationale: str,
    knowledge_track: str = "lore",
    timestamp_utc: str | None = None,
) -> dict[str, Any]:
    entity = _resolve_author_claim_entity(artifacts_root, target_text)
    if not entity:
        raise ValueError(f"Could not resolve target entity: {target_text}")
    clean_claim = re.sub(r"\s+", " ", str(claim_text or "")).strip()
    if not clean_claim:
        raise ValueError("Claim text is required.")
    clean_type = str(claim_type or "lore_fact").strip() or "lore_fact"
    clean_track = _normalize_author_claim_track(knowledge_track, clean_type, clean_claim)
    entity_id = str(entity.get("entity_id", "")).strip()
    canonical_name = str(entity.get("canonical_name", "")).strip()
    created_at = timestamp_utc or now_utc_iso()
    claim_id = stable_id("author_claim", entity_id, clean_type, clean_claim)
    row = {
        "claim_id": claim_id,
        "target_entity_id": entity_id,
        "target_card_id": str(entity.get("card_id") or card_id_for_entity(canonical_name)),
        "target_entity_name": canonical_name,
        "knowledge_track": clean_track,
        "claim_text": clean_claim,
        "claim_type": clean_type,
        "source_snippet_ids": [],
        "confidence": 1.0,
        "status": "accepted",
        "contradiction_notes": "",
        "created_at_utc": created_at,
        "reviewer": reviewer or "author",
        "review_rationale": rationale,
        "manual_claim": True,
        "author_claim": True,
        "source_priority": "author_claim",
        "normalized_claim_text": _author_claim_normalized_text(clean_claim),
    }
    path = author_claims_path_for_root(artifacts_root)
    payload = _read_json_or_default(path, {"claims": []})
    claims = payload.setdefault("claims", [])
    if not isinstance(claims, list):
        claims = []
        payload["claims"] = claims
    replaced = False
    for index, existing in enumerate(claims):
        if isinstance(existing, dict) and str(existing.get("claim_id", "")) == claim_id:
            claims[index] = row
            replaced = True
            break
    if not replaced:
        claims.append(row)
    payload["updated_at_utc"] = created_at
    write_json(path, payload)
    return row


def claim_inventory_bucket(claim: dict[str, Any], decision: dict[str, Any], attention: dict[str, Any], human_reviewed: bool) -> str:
    if attention and not human_reviewed:
        return "attention"
    label = str(decision.get("decision", "")).strip().lower()
    if not label and bool(claim.get("manual_claim") or claim.get("author_claim")):
        return "accepted"
    return {
        "accept": "accepted",
        "approve": "accepted",
        "accepted": "accepted",
        "reject": "rejected",
        "rejected": "rejected",
        "defer": "deferred",
        "needs_more_context": "needs context",
    }.get(label, "pending")


def claim_inventory_category(claim: dict[str, Any]) -> str:
    track = str(claim.get("knowledge_track", "")).strip().lower()
    if track in {"lore", "meta"}:
        return track
    if track == "both":
        return "mixed"
    claim_type = str(claim.get("claim_type", "")).strip().lower()
    if claim_type in {"inspiration", "open_question"}:
        return "meta"
    return "unknown"


def claim_inventory_browser_rows(claims_path: Path, decisions_path: Path, artifacts_root: Path) -> list[dict[str, Any]]:
    claims, _reason = _load_patches_or_reason(claims_path)
    if claims is None:
        claims = []
    author_claims = load_author_claims_for_inventory(artifacts_root)
    all_claims = list(claims) + author_claims
    decisions_by_id = _latest_claim_decisions(decisions_path)
    attention_by_id = _claim_attention_by_id(artifacts_root)
    human_decision_ids = _human_decision_ids(decisions_path, ["claim_id"])
    rows: list[dict[str, Any]] = []
    for index, claim in enumerate(all_claims):
        if not isinstance(claim, dict):
            continue
        claim_id = str(claim.get("claim_id", "")).strip()
        decision = decisions_by_id.get(claim_id, {})
        if not decision and bool(claim.get("manual_claim") or claim.get("author_claim")):
            decision = {"decision": "accept", "reviewer": claim.get("reviewer") or "author", "rationale": "Author-supplied claim."}
        attention = attention_by_id.get(claim_id, {})
        warnings = _as_text_list(claim.get("support_warnings"))
        notes = str(claim.get("contradiction_notes", "")).strip()
        attention_reason = str(attention.get("human_review_reason", "")).strip()
        reason = attention_reason or "; ".join(warnings) or notes
        if bool(claim.get("manual_claim") or claim.get("author_claim")) and not reason:
            reason = "Author-supplied claim for Stage 10 card refactoring."
        target = str(claim.get("target_entity_name") or claim.get("target_entity_id") or "(unknown entity)").strip()
        claim_text = str(claim.get("claim_text", "")).strip()
        display_text = claim_text[:120] + ("..." if len(claim_text) > 120 else "")
        rows.append(
            {
                "row_id": f"claim:{claim_id or index}",
                "row_kind": "claim",
                "bucket": claim_inventory_bucket(claim, decision, attention, claim_id in human_decision_ids),
                "source_bucket": "author_claims" if bool(claim.get("manual_claim") or claim.get("author_claim")) else "claims",
                "category": claim_inventory_category(claim),
                "candidate_name": display_text or claim_id or "(claim)",
                "raw_candidate_name": claim_text,
                "canonical_name": target,
                "proposed_entity_type": str(claim.get("claim_type", "lore_fact") or "lore_fact"),
                "evidence_count": len(_as_text_list(claim.get("source_snippet_ids"))),
                "topics": _as_text_list(claim.get("thematic_tags")),
                "tracks": _as_text_list([claim.get("knowledge_track", "")]),
                "triage_reason": reason,
                "review_priority": "human attention" if attention and claim_id not in human_decision_ids else "",
                "decision": str(decision.get("decision", "") or ""),
                "item": {**claim, "latest_decision": decision, "auto_review_attention": attention},
                "latest_decision": decision,
            }
        )
    rows.sort(key=lambda row: (row["bucket"], row["category"], str(row["candidate_name"]).lower()))
    return rows


def write_claim_inventory_override_decision(
    decisions_path: Path,
    row: dict[str, Any],
    decision: str,
    reviewer: str,
    rationale: str,
    timestamp_utc: str | None = None,
) -> int:
    item = row.get("item", {}) if isinstance(row.get("item"), dict) else {}
    claim_id = str(item.get("claim_id", "")).strip()
    if not claim_id:
        return 0
    normalized_decision = "accept" if decision == "approve" else decision
    data = _read_json_or_default(decisions_path, {"decisions": []})
    decisions = data.setdefault("decisions", [])
    if not isinstance(decisions, list):
        decisions = []
        data["decisions"] = decisions
    decisions.append(
        {
            "claim_id": claim_id,
            "decision": normalized_decision,
            "reviewer": reviewer,
            "rationale": rationale,
            "timestamp_utc": timestamp_utc or now_utc_iso(),
            "human_override": True,
            "override_source": "candidate_inventory_claims_tab",
        }
    )
    write_json(decisions_path, data)
    return 1


def identity_merge_inventory_browser_rows(proposals_path: Path, decisions_path: Path | None = None) -> list[dict[str, Any]]:
    proposals_payload = _read_json_or_default(proposals_path, {"proposals": []})
    proposals = proposals_payload.get("proposals", []) if isinstance(proposals_payload, dict) else []
    decisions_by_id = _latest_identity_merge_decisions(decisions_path)
    edge_decisions_by_id = _latest_identity_edge_decisions(decisions_path)

    rows: list[dict[str, Any]] = []
    for index, proposal in enumerate(proposals):
        if not isinstance(proposal, dict):
            continue
        proposal_id = str(proposal.get("proposal_id", "")).strip()
        decision = decisions_by_id.get(proposal_id, {})

        member_names = [
            str(member.get("canonical_name") or member.get("entity_id") or "").strip()
            for member in proposal.get("member_entities", []) or []
            if isinstance(member, dict) and str(member.get("canonical_name") or member.get("entity_id") or "").strip()
        ]
        source = str(proposal.get("source_entity_name") or proposal.get("source_entity_id") or "(unknown)")
        target = str(proposal.get("canonical_name") or proposal.get("target_entity_name") or proposal.get("target_entity_id") or "(unknown)")
        if member_names:
            members_preview = " + ".join(member_names[:4])
            if len(member_names) > 4:
                members_preview += f" +{len(member_names) - 4}"
            display_name = f"{members_preview} -> {target}"
        else:
            display_name = f"{source} -> {target}"

        bucket = str(decision.get("decision") or proposal.get("review_status") or "pending").strip().lower()
        if bucket == "approve":
            bucket = "approved"
        elif bucket == "reject":
            bucket = "rejected"
        elif bucket == "defer":
            bucket = "deferred"
        elif bucket == "needs_more_context":
            bucket = "needs context"

        member_edges: list[dict[str, Any]] = []
        rejected_edge_ids: list[str] = []
        for edge in proposal.get("member_edges", []) or []:
            if not isinstance(edge, dict):
                continue
            edge_id = str(edge.get("proposal_id", "")).strip()
            edge_decision = edge_decisions_by_id.get(edge_id, {})
            edge_bucket = _identity_edge_bucket(edge_decision)
            if edge_bucket == "refuted":
                rejected_edge_ids.append(edge_id)
            member_edges.append({**edge, "latest_edge_decision": edge_decision, "edge_bucket": edge_bucket})

        reason = str(decision.get("rationale") or proposal.get("rationale") or proposal.get("reason") or "").strip()
        flags = _as_text_list(proposal.get("cluster_review_flags"))
        if rejected_edge_ids:
            flags = list(dict.fromkeys([*flags, f"{len(rejected_edge_ids)} refuted connection(s)"]))
        item = {
            **proposal,
            "latest_decision": decision,
            "member_edges": member_edges if member_edges else proposal.get("member_edges", []),
            "edge_decisions": edge_decisions_by_id,
            "rejected_edge_proposal_ids": sorted(set(_as_text_list(proposal.get("rejected_edge_proposal_ids")) + rejected_edge_ids)),
        }

        rows.append(
            {
                "row_id": f"identity_merge:{proposal_id or index}",
                "row_kind": "identity_merge",
                "bucket": bucket,
                "category": "cluster" if proposal.get("proposal_kind") == "identity_cluster" else "merge",
                "candidate_name": display_name,
                "raw_candidate_name": display_name,
                "canonical_name": target,
                "proposed_entity_type": str(proposal.get("merge_type", "identity_cluster") or "identity_cluster"),
                "evidence_count": len(proposal.get("evidence_claim_ids", []) or []),
                "topics": [],
                "tracks": [],
                "triage_reason": reason,
                "review_priority": _join_preview(flags, 4),
                "decision": str(decision.get("decision", "") or ""),
                "item": item,
                "latest_decision": decision,
            }
        )
    rows.sort(key=lambda row: (row["bucket"], str(row["candidate_name"]).lower()))
    return rows


def write_identity_merge_override_decision(
    decisions_path: Path,
    row: dict[str, Any],
    decision: str,
    reviewer: str,
    rationale: str,
    canonical_name: str | None = None,
) -> int:
    item = row.get("item", {}) if isinstance(row.get("item"), dict) else {}
    proposal_id = str(item.get("proposal_id", "")).strip()
    if not proposal_id:
        return 0
    data = _read_json_or_default(decisions_path, {"decisions": []})
    decisions = data.setdefault("decisions", [])
    if not isinstance(decisions, list):
        decisions = []
        data["decisions"] = decisions

    payload = {
        "proposal_id": proposal_id,
        "decision": decision,
        "reviewer": reviewer,
        "rationale": rationale,
        "timestamp_utc": now_utc_iso(),
        "human_override": True,
        "override_source": "candidate_inventory_merges_tab",
    }
    clean_canonical = str(canonical_name or row.get("canonical_name") or item.get("canonical_name") or item.get("target_entity_name") or "").strip()
    if clean_canonical:
        payload["canonical_name"] = clean_canonical
        for member in item.get("member_entities", []) or []:
            if not isinstance(member, dict):
                continue
            names = [str(member.get("canonical_name", "")), *[str(alias) for alias in member.get("aliases", []) or []]]
            if any(normalized_name_key(name) == normalized_name_key(clean_canonical) for name in names if name.strip()):
                payload["canonical_entity_id"] = str(member.get("entity_id", ""))
                break
    decisions.append(payload)
    write_json(decisions_path, data)
    return 1


def write_identity_edge_override_decision(
    decisions_path: Path,
    row: dict[str, Any],
    decision: str,
    reviewer: str,
    rationale: str,
) -> int:
    item = row.get("item", {}) if isinstance(row.get("item"), dict) else {}
    edge_id = str(item.get("proposal_id", "")).strip()
    cluster_id = str(item.get("cluster_id", "")).strip()
    if not edge_id or not cluster_id:
        return 0
    normalized = {
        "approve": "accept",
        "keep": "accept",
        "restore": "accept",
        "reject": "reject",
        "refute": "reject",
        "defer": "defer",
        "needs_more_context": "needs_more_context",
    }.get(str(decision).strip().lower(), str(decision).strip().lower())
    data = _read_json_or_default(decisions_path, {"decisions": []})
    decisions = data.setdefault("decisions", [])
    if not isinstance(decisions, list):
        decisions = []
        data["decisions"] = decisions
    decisions.append(
        {
            "decision_scope": "identity_edge",
            "cluster_id": cluster_id,
            "edge_proposal_id": edge_id,
            "source_entity_id": item.get("source_entity_id", ""),
            "source_entity_name": item.get("source_entity_name", ""),
            "target_entity_id": item.get("target_entity_id", ""),
            "target_entity_name": item.get("target_entity_name", ""),
            "decision": normalized,
            "reviewer": reviewer,
            "rationale": rationale,
            "timestamp_utc": now_utc_iso(),
            "human_override": True,
            "override_source": "candidate_inventory_identity_edge_row",
        }
    )
    write_json(decisions_path, data)
    return 1


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
    last_open = load_last_open_artifacts_root(repo_root)
    if last_open is not None:
        return last_open.resolve()
    runs = discover_review_runs(repo_root, repo_root / "artifacts")
    pending_runs = [run for run in runs if run["pending_total"] > 0]
    if pending_runs:
        return max((Path(run["artifacts_root"]) for run in pending_runs), key=artifact_sort_key)
    material_runs = [run for run in runs if Path(run["artifacts_root"]).resolve() != (repo_root / "artifacts").resolve()]
    if material_runs:
        return max((Path(run["artifacts_root"]) for run in material_runs), key=artifact_sort_key)
    return (repo_root / "artifacts").resolve()


