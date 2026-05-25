# Antigravity pipeline watch workflow

Use this workflow after starting a pipeline run from the Theriac desktop app (Tauri) or ops-repo handoff.

## Prerequisites

- Theriac Watch MCP server configured in Antigravity from **theriac-pipeline-ops** (`%USERPROFILE%\.gemini\config\mcp_config.json`). See [ops-repo.md](ops-repo.md) and the ops repo `docs/antigravity/mcp_config.example.json`.
- Model Quota panel reviewed if starting a new run (quota capture from ops repo; see [quota-worker.md](quota-worker.md)).

## Steps

1. Confirm preflight recommendation or explicit user override (ops repo).
2. Call MCP `theriac_watch_start` with `watcher: antigravity_flash` and `poll_interval_seconds: 300`.
3. Note the returned `job_id`.
4. Start the sentinel in a terminal (keeps supervision if Flash quota dies), unless you used ops-repo `pipeline_handoff.py` (sentinel starts automatically):
   ```bash
   cd ../theriac-pipeline-ops
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

## OpenRouter-only runs

When the pipeline uses OpenRouter only, skip quota/watch MCP and supervise via worker log + artifacts — [watch-run-stage-07-through-08.md](watch-run-stage-07-through-08.md).
