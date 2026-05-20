from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any

from pipeline.common import now_utc_iso, read_json, write_json
from pipeline.entity_resolution import normalized_name_key
from pipeline.ui_review_app import (
    NEW_RUN_SELECTOR_VALUE,
    _display_path,
    _read_json_or_default,
    discover_review_runs,
    load_last_open_artifacts_root,
    pending_review_counts_for_root,
    pending_review_summary,
    pending_review_total,
    pipeline_progress_artifact_snapshot,
    pipeline_progress_from_logs,
    new_run_artifacts_root,
    save_last_open_artifacts_root,
)


def _looks_like_project_root(path: Path) -> bool:
    return (
        (path / "config" / "pipeline_config.json").exists()
        and (path / "pipeline" / "run_pipeline.py").exists()
    )


def _plain_windows_path(path: Path | str) -> Path:
    raw = str(path)
    if raw.startswith("\\\\?\\UNC\\"):
        raw = "\\" + raw[7:]
    elif raw.startswith("\\\\?\\"):
        raw = raw[4:]
    return Path(raw)


def _resolve_plain(path: Path | str) -> Path:
    return _plain_windows_path(path).expanduser().resolve()


def find_repo_root(explicit: str | None = None) -> Path:
    if explicit:
        path = _plain_windows_path(explicit).expanduser()
        if path.exists():
            return _resolve_plain(path)
    env_root = os.environ.get("THERIAC_LORE_ROOT")
    if env_root:
        path = _plain_windows_path(env_root).expanduser()
        if path.exists():
            return _resolve_plain(path)
    starts = [Path.cwd(), Path(__file__).resolve().parents[1]]
    for start in starts:
        for candidate in (start, *start.parents):
            if _looks_like_project_root(candidate):
                return _resolve_plain(candidate)
    return _resolve_plain(Path.cwd())


def _json_safe(value: Any) -> Any:
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_safe(item) for item in value]
    if isinstance(value, tuple):
        return [_json_safe(item) for item in value]
    return value


def _active_root(repo_root: Path, payload: dict[str, Any]) -> Path:
    raw = str(payload.get("artifacts_root") or "").strip()
    if raw and raw != NEW_RUN_SELECTOR_VALUE:
        path = _plain_windows_path(raw)
        if not path.is_absolute():
            path = repo_root / path
        return _resolve_plain(path)
    last = load_last_open_artifacts_root(repo_root)
    if last is not None:
        return _resolve_plain(last)
    runs_base = repo_root / "artifacts" / "runs"
    if runs_base.exists():
        runs = [path for path in runs_base.iterdir() if path.is_dir()]
        if runs:
            return _resolve_plain(max(runs, key=lambda path: path.stat().st_mtime))
    return repo_root / "artifacts"


def _run_state(repo_root: Path, active_root: Path) -> dict[str, Any]:
    repo_root = _resolve_plain(repo_root)
    active_root = _resolve_plain(active_root)
    counts = pending_review_counts_for_root(active_root) if active_root.exists() else {}
    runs = discover_review_runs(repo_root, active_root) if active_root.exists() else []
    snapshot = pipeline_progress_artifact_snapshot(active_root) if active_root.exists() else {}
    progress = pipeline_progress_from_logs(
        [str(line) for line in snapshot.get("logs", [])] if isinstance(snapshot, dict) else [],
        str(snapshot.get("status", "idle")) if isinstance(snapshot, dict) else "idle",
        str(snapshot.get("message", "")) if isinstance(snapshot, dict) else "",
    )
    return {
        "repo_root": str(repo_root),
        "active_root": str(active_root),
        "active_label": _display_path(active_root, repo_root),
        "counts": counts,
        "pending_total": pending_review_total(counts) if counts else 0,
        "pending_summary": pending_review_summary(counts) if counts else "no review artifacts yet",
        "progress": progress,
        "runs": [
            {
                **run,
                "artifacts_root": str(_resolve_plain(run["artifacts_root"])),
                "label": _display_path(_resolve_plain(run["artifacts_root"]), repo_root),
                "is_active": _resolve_plain(run["artifacts_root"]) == active_root,
                "latest_mtime": float(run.get("latest_mtime", 0) or 0),
            }
            for run in runs
        ],
    }


def _decision_path(root: Path) -> Path:
    path = root / "07_review" / "identity_merge_decisions.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists():
        write_json(path, {"decisions": []})
    return path


def _append_identity_decision(path: Path, payload: dict[str, Any]) -> None:
    data = _read_json_or_default(path, {"decisions": []})
    if not isinstance(data, dict):
        data = {"decisions": []}
    decisions = data.setdefault("decisions", [])
    if not isinstance(decisions, list):
        decisions = []
        data["decisions"] = decisions
    decisions.append(payload)
    write_json(path, data)


def _identity_clusters(root: Path) -> list[dict[str, Any]]:
    from pipeline.review_inventory import identity_merge_inventory_browser_rows

    proposals_path = root / "07_review" / "identity_merge_proposals.json"
    decisions_path = _decision_path(root)
    rows = identity_merge_inventory_browser_rows(proposals_path, decisions_path)
    clusters = [row for row in rows if row.get("row_kind") == "identity_merge"]
    return _json_safe(clusters)


def _claim_rows(root: Path) -> list[dict[str, Any]]:
    from pipeline.review_inventory import claim_inventory_browser_rows, sort_candidate_inventory_rows

    rows = claim_inventory_browser_rows(
        root / "06_drafts" / "card_drafts" / "claim_drafts.json",
        root / "07_review" / "claim_review_decisions.json",
        root,
    )
    return _json_safe(sort_candidate_inventory_rows(rows, "bucket", False))


def _entity_rows(root: Path) -> list[dict[str, Any]]:
    from pipeline.review_inventory import candidate_inventory_browser_rows, sort_candidate_inventory_rows

    rows = candidate_inventory_browser_rows(
        root / "05_alias" / "conversation_entity_proposals.json",
        root / "05_alias" / "conversation_entity_decisions.json",
    )
    return _json_safe(sort_candidate_inventory_rows(rows, "bucket", False))


def handle_state(repo_root: Path, payload: dict[str, Any]) -> dict[str, Any]:
    active = _active_root(repo_root, payload)
    return _run_state(repo_root, active)


def handle_select_run(repo_root: Path, payload: dict[str, Any]) -> dict[str, Any]:
    active = _active_root(repo_root, payload)
    if active.exists():
        save_last_open_artifacts_root(repo_root, active)
    return _run_state(repo_root, active)


def handle_create_run(repo_root: Path, payload: dict[str, Any]) -> dict[str, Any]:
    active = new_run_artifacts_root(repo_root)
    active = _resolve_plain(active)
    save_last_open_artifacts_root(repo_root, active)
    return _run_state(repo_root, active)


def handle_identity_clusters(repo_root: Path, payload: dict[str, Any]) -> dict[str, Any]:
    active = _active_root(repo_root, payload)
    return {
        "active_root": str(active),
        "clusters": _identity_clusters(active) if active.exists() else [],
    }


def handle_claim_inventory(repo_root: Path, payload: dict[str, Any]) -> dict[str, Any]:
    active = _active_root(repo_root, payload)
    rows = _claim_rows(active) if active.exists() else []
    return {"active_root": str(active), "rows": rows, "total": len(rows)}


def handle_entity_inventory(repo_root: Path, payload: dict[str, Any]) -> dict[str, Any]:
    active = _active_root(repo_root, payload)
    rows = _entity_rows(active) if active.exists() else []
    return {"active_root": str(active), "rows": rows, "total": len(rows)}


def handle_identity_cluster_decision(repo_root: Path, payload: dict[str, Any]) -> dict[str, Any]:
    active = _active_root(repo_root, payload)
    proposal_id = str(payload.get("proposal_id") or "").strip()
    if not proposal_id:
        raise ValueError("Missing proposal_id.")
    decision = str(payload.get("decision") or "defer").strip().lower()
    if decision == "accept":
        decision = "approve"
    if decision not in {"approve", "reject", "defer", "needs_more_context"}:
        raise ValueError(f"Unsupported decision: {decision}")
    canonical_name = str(payload.get("canonical_name") or "").strip()
    canonical_entity_id = str(payload.get("canonical_entity_id") or "").strip()
    if canonical_name and not canonical_entity_id:
        for row in _identity_clusters(active):
            item = row.get("item", {}) if isinstance(row, dict) else {}
            if str(item.get("proposal_id") or "") != proposal_id:
                continue
            name_key = normalized_name_key(canonical_name)
            for member in item.get("member_entities", []) or []:
                if not isinstance(member, dict):
                    continue
                names = [str(member.get("canonical_name") or ""), *[str(alias) for alias in member.get("aliases", []) or []]]
                if any(normalized_name_key(name) == name_key for name in names if name.strip()):
                    canonical_entity_id = str(member.get("entity_id") or "")
                    break
            break
    decision_payload: dict[str, Any] = {
        "proposal_id": proposal_id,
        "decision": decision,
        "reviewer": str(payload.get("reviewer") or "tauri_user"),
        "rationale": str(payload.get("rationale") or ""),
        "timestamp_utc": now_utc_iso(),
        "human_override": True,
        "override_source": "tauri_identity_cluster_panel",
    }
    if canonical_name:
        decision_payload["canonical_name"] = canonical_name
    if canonical_entity_id:
        decision_payload["canonical_entity_id"] = canonical_entity_id
    _append_identity_decision(_decision_path(active), decision_payload)
    return handle_identity_clusters(repo_root, {"artifacts_root": str(active)})


def handle_identity_edge_decision(repo_root: Path, payload: dict[str, Any]) -> dict[str, Any]:
    active = _active_root(repo_root, payload)
    edge_id = str(payload.get("edge_proposal_id") or payload.get("edge_id") or "").strip()
    cluster_id = str(payload.get("cluster_id") or payload.get("proposal_id") or "").strip()
    if not edge_id or not cluster_id:
        raise ValueError("Missing edge_proposal_id or cluster_id.")
    decision = str(payload.get("decision") or "defer").strip().lower()
    decision = {
        "approve": "accept",
        "keep": "accept",
        "restore": "accept",
        "refute": "reject",
    }.get(decision, decision)
    if decision not in {"accept", "reject", "defer", "needs_more_context"}:
        raise ValueError(f"Unsupported edge decision: {decision}")
    _append_identity_decision(
        _decision_path(active),
        {
            "decision_scope": "identity_edge",
            "cluster_id": cluster_id,
            "edge_proposal_id": edge_id,
            "source_entity_id": str(payload.get("source_entity_id") or ""),
            "source_entity_name": str(payload.get("source_entity_name") or ""),
            "target_entity_id": str(payload.get("target_entity_id") or ""),
            "target_entity_name": str(payload.get("target_entity_name") or ""),
            "decision": decision,
            "reviewer": str(payload.get("reviewer") or "tauri_user"),
            "rationale": str(payload.get("rationale") or ""),
            "timestamp_utc": now_utc_iso(),
            "human_override": True,
            "override_source": "tauri_identity_edge_panel",
        },
    )
    return handle_identity_clusters(repo_root, {"artifacts_root": str(active)})


def handle_claim_decision(repo_root: Path, payload: dict[str, Any]) -> dict[str, Any]:
    from pipeline.review_inventory import write_claim_inventory_override_decision

    active = _active_root(repo_root, payload)
    row_id = str(payload.get("row_id") or "").strip()
    if not row_id:
        raise ValueError("Missing row_id.")
    rows = _claim_rows(active)
    row = next((item for item in rows if str(item.get("row_id") or "") == row_id), None)
    if row is None:
        raise ValueError(f"Unknown claim row: {row_id}")
    decision = str(payload.get("decision") or "defer").strip().lower()
    if decision not in {"approve", "accept", "reject", "defer", "needs_more_context"}:
        raise ValueError(f"Unsupported decision: {decision}")
    write_claim_inventory_override_decision(
        active / "07_review" / "claim_review_decisions.json",
        row,
        decision,
        str(payload.get("reviewer") or "tauri_user"),
        str(payload.get("rationale") or ""),
    )
    return handle_claim_inventory(repo_root, {"artifacts_root": str(active)})


def handle_entity_decision(repo_root: Path, payload: dict[str, Any]) -> dict[str, Any]:
    from pipeline.review_inventory import write_candidate_inventory_override_decision

    active = _active_root(repo_root, payload)
    row_id = str(payload.get("row_id") or "").strip()
    if not row_id:
        raise ValueError("Missing row_id.")
    rows = _entity_rows(active)
    row = next((item for item in rows if str(item.get("row_id") or "") == row_id), None)
    if row is None:
        raise ValueError(f"Unknown entity row: {row_id}")
    decision = str(payload.get("decision") or "defer").strip().lower()
    if decision not in {"approve", "accept", "reject", "defer", "needs_more_context"}:
        raise ValueError(f"Unsupported decision: {decision}")
    canonical_name = str(payload.get("canonical_name") or row.get("canonical_name") or row.get("raw_candidate_name") or "").strip()
    entity_type = str(payload.get("entity_type") or row.get("proposed_entity_type") or "term").strip()
    write_candidate_inventory_override_decision(
        active / "05_alias" / "conversation_entity_decisions.json",
        row,
        decision,
        canonical_name,
        entity_type,
        str(payload.get("reviewer") or "tauri_user"),
        str(payload.get("rationale") or ""),
    )
    return handle_entity_inventory(repo_root, {"artifacts_root": str(active)})


COMMANDS = {
    "state": handle_state,
    "select_run": handle_select_run,
    "create_run": handle_create_run,
    "identity_clusters": handle_identity_clusters,
    "identity_cluster_decision": handle_identity_cluster_decision,
    "identity_edge_decision": handle_identity_edge_decision,
    "claim_inventory": handle_claim_inventory,
    "claim_decision": handle_claim_decision,
    "entity_inventory": handle_entity_inventory,
    "entity_decision": handle_entity_decision,
}


def handle_request(request: dict[str, Any]) -> dict[str, Any]:
    command = str(request.get("command") or "").strip()
    if command not in COMMANDS:
        raise ValueError(f"Unknown command: {command}")
    payload = request.get("payload") if isinstance(request.get("payload"), dict) else {}
    repo_root = find_repo_root(str(request.get("repo_root") or payload.get("repo_root") or "").strip() or None)
    return COMMANDS[command](repo_root, payload)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--request", help="JSON request payload. If omitted, stdin is used.")
    args = parser.parse_args()
    try:
        raw = args.request if args.request is not None else sys.stdin.read()
        request = json.loads(raw or "{}")
        result = {"ok": True, "result": handle_request(request)}
    except Exception as exc:
        result = {"ok": False, "error": str(exc)}
    sys.stdout.write(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main()
