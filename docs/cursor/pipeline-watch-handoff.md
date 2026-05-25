# Cursor pipeline watch handoff

Canonical repo: `D:\Workplaces\Enkidu Project\Theriac\Lore_bible` (see [../canonical-repo.md](../canonical-repo.md)).

## Before recommending a full pipeline run

Quota preflight is optional. Skip it when the user wants to run now.

If using quota: run `python scripts/check_quota.py`, read `latest.png`, call `theriac_quota_preflight`.

## Autonomous start (Cursor / MCP)

Without Tauri UI:

```bash
python scripts/pipeline_handoff.py
```

Or MCP `theriac_pipeline_handoff()` — starts pipeline worker, background sentinel, and watch job in one call.

## After the user starts a run (manual path)

1. `theriac_watch_start` once **or** use `theriac_pipeline_handoff` above.
2. Tell the user to run the Antigravity workflow in `docs/antigravity/pipeline-watch-workflow.md` (sentinel auto-starts with handoff).
3. Do **not** poll in Cursor.

## When the user returns

- Read `{run_root}/watch_report.md` or `{run_root}/watch_alert.json`.
- Optional: `/loop` on `.watch_done` for a single wake when the run completes.
