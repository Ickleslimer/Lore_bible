# Pipeline watch failover

Antigravity has two shared model quota pools (Gemini vs Claude/GPT-OSS). MCP cannot switch models automatically.

## Watcher stale alert

When `watch_alert.json` appears with `reason: watcher_stale`:

1. Read `artifacts/pipeline_watches/{job_id}.json` for `run_root` and `poll_interval_seconds`.
2. Re-run quota capture from **theriac-pipeline-ops** if needed:
   ```bash
   cd ../theriac-pipeline-ops
   python scripts/check_quota.py
   ```
3. Decide:

| Second pool (Claude/GPT) | Action |
|--------------------------|--------|
| Has capacity (roughly 2+ bars) | Open a new Antigravity session on **GPT-OSS** or **Claude Sonnet**; resume `theriac_watch_status` with the same `job_id` and `checked_by: antigravity_gpt_pool`. |
| Also exhausted | Leave **sentinel** running, or cancel the pipeline if unsupervised runs are unacceptable. |

## Cancel pipeline

- **Desktop:** Pipeline tab → Cancel.
- **MCP (ops repo):** `theriac_pipeline_cancel` (uses `pipeline_worker.pid` written by Tauri when the worker starts).
- **Aggressive watch policy:** set `on_watcher_lost: cancel_run` at `theriac_watch_start` (only if you want stale watcher to kill the worker).

## Wait for Gemini reset

Use the refresh timer from the Model Quota screenshot. Pipeline runs may continue with **sentinel-only** supervision (no Flash polls).
