# Antigravity watch — Stage 04R failed-adjudication retry (+ 06R refresh)

**Run root:** `artifacts/runs/20260517_032555635445_full`  
**Repo:** `D:\Workplaces\Enkidu Project\Theriac\Lore_bible`  
**Scope:** Re-adjudicate **338** candidate windows that fell back to heuristic in the last 04R pass; refresh 06R merge if rescue outputs change.  
**Out of scope:** Full 04R rerun, Stage 07+ (unless the user starts a separate watch afterward).

---

## Copy-paste prompt for Antigravity

```
You are the unattended supervisor for a targeted Theriac Stage 04R retry on run
artifacts/runs/20260517_032555635445_full.

Repo: D:\Workplaces\Enkidu Project\Theriac\Lore_bible

GOAL
- Re-adjudicate only the 338 candidate windows listed in
  04_conversation_segmentation/theme_relevance_rerun_failures.json
  (batch size now max_windows_per_model_call=4).
- Refresh Stage 06R snippet merge when 04R retry completes.
- Do NOT run full pipeline stages 07–12 unless the user explicitly asks later.
- The user is away. Do NOT ask them questions. Act autonomously.

PREFLIGHT (before starting)
- Confirm OpenRouter key is available (04R uses deepseek/deepseek-v4-flash).
- Optional: ops-repo `python scripts/check_quota.py` + `theriac_quota_preflight` if starting a long watch session.
- Read baseline from existing artifacts:
    theme_relevance_rerun_failures.json → 88 failure rows / 338 candidate ids (all missing_or_invalid_model_decision)
    theme_relevance_rerun.json summary.failure_count → 88

START (if nothing is running yet)
From repo root (PYTHONPATH=.):

  cd "D:\Workplaces\Enkidu Project\Theriac\Lore_bible"
  $env:PYTHONPATH="."
  python -m pipeline.retry_04r_failed_adjudication `
    --artifacts-root "artifacts/runs/20260517_032555635445_full" `
    2>&1 | Tee-Object -FilePath "artifacts/runs/20260517_032555635445_full/tauri_pipeline_worker.log" -Append

This runs 04R retry then 06R automatically. Expect ~85 model calls (338 candidates ÷ 4 per batch).

Watch setup (same as other pipeline watches):
- theriac_watch_start: watcher antigravity_flash, poll_interval_seconds 300, on_watcher_lost alert (NOT cancel_run)
- Sentinel (theriac-pipeline-ops): `python scripts/pipeline_watch_sentinel.py --loop --interval 60`
- Add mcp(theriac-watch/*) to Cursor Allow list if MCP prompts repeat (see docs/antigravity/mcp-auto-approve-theriac-watch.md)

POLLING (every 5 minutes)
- theriac_watch_status with checked_by antigravity_flash (if a watch job exists)
- Tail last 40 lines of {run_root}/tauri_pipeline_worker.log
- Artifact spot-check (repo root, PYTHONPATH=.):

python -c "
import json
from pathlib import Path
run = Path('artifacts/runs/20260517_032555635445_full')
fail = json.loads((run/'04_conversation_segmentation/theme_relevance_rerun_failures.json').read_text(encoding='utf-8'))
rerun = json.loads((run/'04_conversation_segmentation/theme_relevance_rerun.json').read_text(encoding='utf-8'))
s = rerun.get('summary', {})
retry = s.get('retry_failed_adjudication') or {}
failed_ids = {cid for row in fail.get('failures', []) for cid in row.get('candidate_ids', [])}
print('failure_rows', len(fail.get('failures', [])))
print('failed_candidate_ids', len(failed_ids))
print('retry_block', retry)
print('failure_count_summary', s.get('failure_count'))
print('rescued_conversations', s.get('rescued_conversation_count'))
print('model_call_count', s.get('model_call_count'))
"

Do NOT spam the user. Update watch_report.md only on terminal success, catastrophic cancel, or hard stuck.

WHEN TO DO NOTHING (healthy progress)
- Log shows: Stage 04R retry: re-adjudicating 338 failed candidate window(s)
- Log shows adjudication chunk lines with failures count trending down
- Log shows: Stage 04R retry complete: attempted=338 resolved=… remaining_failures=…
- Log shows: Stage 06R refresh complete (or Stage 06R complete)
- OpenRouter rate-limit retries with eventual progress
- failure_count / remaining_failed_candidate_count decreasing between polls

WHEN TO CANCEL (catastrophic)
Stop the python process and note reason in watch_alert.json / watch_report.md:
1. Traceback with no recovery, auth_failed, missing OPENROUTER_API_KEY, disk full.
2. Same adjudication error repeating 3+ times with zero progress for 30+ minutes.
3. remaining_failed_candidate_count stays at 338 after retry claims complete (merge bug).
4. combined_snippet_count in theme_rescue_snippet_merge_report drops vs prior 52373 — data corruption.

WHEN NOT TO CANCEL (note only)
- remaining_failed_candidate_count > 0 but much lower than 338 (partial retry success — report counts).
- Process finished with remaining failures < 20 (acceptable tail; user may rerun retry once).
- User already stopped the job cleanly.

SUCCESS CRITERIA
- Worker log contains: Stage 04R retry complete
- summary.retry_failed_adjudication.remaining_failed_candidate_count is 0 (or user-acceptable small tail, e.g. < 10)
- theme_relevance_rerun_failures.json failure rows near 0
- 06R merge report updated (generated_at_utc newer than 04R retry)
- combined_snippet_count still sane (~52k; may shift slightly if model flips rescue/reject decisions)

REPORT WHEN FINISHED (one message)
- Status: success | partial_success | cancelled_catastrophic
- retry_failed_adjudication: attempted / resolved / remaining_failed
- Before vs after: failure_count, rescued_conversation_count, combined_snippet_count
- Any decision flips worth noting (heuristic rescue → model reject or vice versa) — optional sample count only
- If partial: recommend one more retry command or enable hard_retry_enabled

Quick validation:

python -c "
import json
from pathlib import Path
run = Path('artifacts/runs/20260517_032555635445_full')
r = json.loads((run/'04_conversation_segmentation/theme_relevance_rerun.json').read_text(encoding='utf-8'))
m = json.loads((run/'05_snippet_extraction/theme_rescue_snippet_merge_report.json').read_text(encoding='utf-8'))
print(r.get('summary', {}).get('retry_failed_adjudication'))
print('failures', r.get('summary', {}).get('failure_count'))
print('combined', m.get('summary', {}).get('combined_snippet_count'))
"
```

---

## Short variant (minimal)

```
Unattended watch: 04R failed-adjudication retry on artifacts/runs/20260517_032555635445_full.
Repo D:\Workplaces\Enkidu Project\Theriac\Lore_bible.
Start if idle: python -m pipeline.retry_04r_failed_adjudication --artifacts-root artifacts/runs/20260517_032555635445_full (tee log to tauri_pipeline_worker.log).
Poll theriac_watch_status every 5m + tail worker log + failures JSON failure_count.
Success = Stage 04R retry complete + remaining_failed_candidate_count ≈ 0 + 06R merge refreshed.
Cancel only on auth/traceback, 30m zero progress, or snippet count collapse.
Do not ask the user anything.
```

---

## Reference

- Retry implementation: `pipeline/retry_04r_failed_adjudication.py`, `pipeline/stage_04r_theme_relevance_rerun.py` (`run_retry_failed_adjudication`)
- Config: `theme_aware_rerun.max_windows_per_model_call: 4`
- General watch workflow: [pipeline-watch-workflow.md](pipeline-watch-workflow.md)
