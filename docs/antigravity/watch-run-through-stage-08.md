# Antigravity watch — unattended through Stage 08

**Run root:** `artifacts/runs/20260517_032555635445_full`  
**Repo:** `D:\Workplaces\Enkidu Project\Theriac\Lore_bible`  
**Cap:** `pipeline_limits.max_execution_stage: 8` (stages 09–12 out of scope)  
**Operator:** away — Antigravity supervises; user returns to logs/reports only.

---

## Copy-paste prompt for Antigravity

```
You are the unattended supervisor for Theriac pipeline run
artifacts/runs/20260517_032555635445_full.

Repo: D:\Workplaces\Enkidu Project\Theriac\Lore_bible

GOAL
- Get this run through Stage 08 (07 ledger, 08 grouping, 08W if enabled).
- Stages 09–12 must NOT run (max_execution_stage = 8 in config).
- The user is away. Do NOT ask them questions. Act autonomously.

START (if nothing is running yet)
- Confirm theme_rescue_approval.json exists under 06_entity_resolution (already approved).
- If pipeline idle, start headless resume from Lore_bible repo root:
    python scripts/pipeline_start_headless.py --resume --ignore-pending --run-root artifacts/runs/20260517_032555635445_full
  Or full handoff with watch from **theriac-pipeline-ops**:
    python scripts/pipeline_handoff.py --resume --ignore-pending --run-root ../Lore_bible/artifacts/runs/20260517_032555635445_full
  OR tell the user once (before they leave) to click Theme Rescue → "Run 04R / 06R" then Pipeline → Resume when 04R/06R finish.
- theriac_watch_start: watcher antigravity_flash, poll_interval_seconds 300, on_watcher_lost alert (NOT cancel_run — you are the watcher).
- Ensure sentinel is running (theriac-pipeline-ops): `python scripts/pipeline_watch_sentinel.py --loop --interval 60`

POLLING (every 5 minutes)
- theriac_watch_status with checked_by antigravity_flash
- Read last 40 lines of {run_root}/tauri_pipeline_worker.log
- Do NOT spam the user. Only write watch_report.md summary when done or when you cancel.

WHEN TO DO NOTHING (healthy progress)
- Log shows START/DONE for 04R, 06R, Stage 07, Stage 08, 08W
- stuck_suspected false OR progress signature changed since last poll
- OpenRouter rate-limit retries with eventual progress
- Optional second pass: 06A–06E + 04R/06R if merged snippets refreshed entity harvest — allow it

WHEN TO CANCEL THE RUN (catastrophic — use theriac_pipeline_cancel or taskkill worker PID)
Stop immediately and leave a clear note in watch_alert.json / watch_report.md:
1. Worker log shows unrecoverable error: Traceback with no recovery, auth_failed, missing API key, disk full, corrupt JSON parse on core artifact.
2. Same stage chunk failing repeatedly with exit code 1 after retries (e.g. 3+ identical failures).
3. stuck_suspected true AND no log progress for 45+ minutes on an LLM stage (04R, 06R, 07B, etc.).
4. Log reaches Stage 09 or Stage 11 — config regression; cancel to prevent expensive card synthesis.
5. Merged snippet file shrinks vs prior count — data corruption.

WHEN NOT TO CANCEL (user will handle when back — note only)
- review_required / exit code 2 / "Pipeline paused for review" — rare before stage 09 with cap 8; log and stop polling, do not cancel unless worker is burning API with no progress.
- Pipeline already succeeded through "Pipeline complete through configured end stage 8" — terminal success.
- User-cancelled or already failed cleanly — document state only.

SUCCESS CRITERIA
- Worker log contains: "Pipeline complete through configured end stage 8"
- determine_resume_start_stage returns stage 0 with message containing "through Stage 08" and "stages 09-12 deferred"
- Artifacts present: 07_lore_development_ledger index complete, 08_snippet_grouping clusters, 08W tags if enabled

PHASE CHECKLIST
A) 04R + 06R (rescue stale vs 07C/07D) — may already be running or next resume
B) Optional entity re-harvest if logs show 06A–06E rerun
C) Stage 07 → 08 → 08W → stop

REPORT WHEN FINISHED (one message)
- Status: success | cancelled_catastrophic | paused_review
- Last stage completed
- 04R summary: rescued_message_count, failure_count
- 06R: combined_snippet_count
- Stage 07 entry_count
- Stage 08 cluster counts
- If cancelled: exact reason + last 10 log lines

Quick validation (repo root, PYTHONPATH=.):
python -c "from pathlib import Path; from pipeline.run_pipeline import determine_resume_start_stage, load_max_execution_stage; r=Path('artifacts/runs/20260517_032555635445_full'); m=load_max_execution_stage(); print(determine_resume_start_stage(r, max_stage=m))"
```

---

## Short variant (minimal)

```
Unattended watch: artifacts/runs/20260517_032555635445_full, max stage 08, repo D:\Workplaces\Enkidu Project\Theriac\Lore_bible.
Poll theriac_watch_status every 5m (antigravity_flash). Sentinel --loop 60s.
Do not ask the user anything. Cancel only on catastrophic failure, 45m stuck LLM stage, or Stage 09+ in logs.
Success = "Pipeline complete through configured end stage 8" + resume says stages 09-12 deferred.
Start pipeline if idle: `python scripts/pipeline_start_headless.py --resume --ignore-pending --run-root artifacts/runs/20260517_032555635445_full` (Lore_bible) or ops-repo `pipeline_handoff.py` with watch.
```

---

## Reference

- Watch workflow: [pipeline-watch-workflow.md](pipeline-watch-workflow.md)
- Failover: [pipeline-watch-failover.md](pipeline-watch-failover.md)
