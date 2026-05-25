# Antigravity pipeline watch workflow

Use this workflow after starting a pipeline run from the Theriac desktop app (Tauri).

## Prerequisites

- Theriac Watch MCP server configured in Antigravity (`%USERPROFILE%\.gemini\config\mcp_config.json`). Canonical repo: **`D:\Workplaces\Enkidu Project\Theriac\Lore_bible`** — see [mcp_config.example.json](mcp_config.example.json) and [../canonical-repo.md](../canonical-repo.md).
- Model Quota panel reviewed if starting a new run (or rely on Cursor preflight).

## Steps

1. Confirm preflight recommendation or explicit user override.
2. Call MCP `theriac_watch_start` with `watcher: antigravity_flash` and `poll_interval_seconds: 300`.
3. Note the returned `job_id`.
4. Start the sentinel in a terminal (keeps supervision if Flash quota dies), unless you used `theriac_pipeline_handoff` / `python scripts/pipeline_handoff.py` (sentinel starts automatically):
   ```bash
   python scripts/pipeline_watch_sentinel.py --loop --interval 60
   ```
5. Every 5 minutes on **Gemini Flash (Medium)**, call `theriac_watch_status` with `checked_by: antigravity_flash`.
6. If `stuck_suspected` is true, read the last worker log lines and ask whether to cancel.
7. On `terminal: true`, read `{run_root}/watch_report.md` and post a short summary.

## Saved prompt example

```
Watch the active Theriac pipeline run until it finishes or needs review.
Poll theriac_watch_status every 5 minutes with checked_by antigravity_flash.
Report only on terminal or stuck_suspected.
```

## Quota exhaustion

If Antigravity stops polling because model quota is exhausted, the sentinel writes `{run_root}/watch_alert.json`. Follow [pipeline-watch-failover.md](pipeline-watch-failover.md).
