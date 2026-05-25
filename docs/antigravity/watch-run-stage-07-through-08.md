# Antigravity watch — Stage 07 through 08 (post–04R retry)

**Run root:** `artifacts/runs/20260517_032555635445_full`  
**Repo:** `D:\Workplaces\Enkidu Project\Theriac\Lore_bible`  
**Cap:** `pipeline_limits.max_execution_stage: 8`  
**Corpus:** merged snippets **50,224** (`snippets_candidates_with_theme_rescue.jsonl`) after 04R retry + 06R refresh.

**Do not use** `check_quota.py`, `theriac_quota_preflight`, `theriac_watch_*`, or `pipeline_watch_sentinel.py` from Lore_bible — they were moved to **theriac-pipeline-ops** and check **Antigravity Gemini quota**, not OpenRouter. See [ops-repo.md](ops-repo.md).

---

## Copy-paste prompt for Antigravity

```
You are the unattended supervisor for Theriac pipeline run
artifacts/runs/20260517_032555635445_full.

Repo: D:\Workplaces\Enkidu Project\Theriac\Lore_bible

GOAL
- Run stages 07 → 08 → 08W on the post–04R-retry corpus (50,224 merged snippets).
- Stage 07 lore development ledger must use real model calls (prompt=prompt fix is in code).
- Stages 09–12 must NOT run (max_execution_stage = 8).
- The user is away. Do NOT ask questions. Act autonomously.

FORBIDDEN IN THIS REPO (wrong tool / wrong provider)
- Do NOT run: python scripts/check_quota.py
- Do NOT call: theriac_quota_preflight, theriac_watch_start, theriac_watch_status
- Do NOT run: scripts/pipeline_watch_sentinel.py or scripts/pipeline_handoff.py (relocated to theriac-pipeline-ops)
- OpenRouter billing is automatic (THERIAC_OPENROUTER_AUTO_TOPUP); absence of Antigravity quota bars is NOT a blocker.

START (if pipeline idle)
From repo root:

  cd "D:\Workplaces\Enkidu Project\Theriac\Lore_bible"
  $env:PYTHONPATH="."
  python scripts/pipeline_start_headless.py `
    --resume `
    --ignore-pending `
    --run-root "artifacts/runs/20260517_032555635445_full" `
    --start-stage 7 `
    2>&1 | Tee-Object -FilePath "artifacts/runs/20260517_032555635445_full/tauri_pipeline_worker.log" -Append

Alternative (foreground, same stages):

  python -m pipeline.run_pipeline `
    --docx "theriac-coda---lore-bible.docx" `
    --conversations-root "discord_conversations" `
    --artifacts-root "artifacts/runs/20260517_032555635445_full" `
    --start-stage 7

POLLING (every 5 minutes)
- Tail last 50 lines of {run_root}/tauri_pipeline_worker.log
- Do NOT use MCP watch tools from Lore_bible (disabled).

Artifact spot-check (PYTHONPATH=.):

python -c "
from pathlib import Path
from pipeline.run_pipeline import determine_resume_start_stage, load_max_execution_stage
run = Path('artifacts/runs/20260517_032555635445_full')
m = load_max_execution_stage()
print('resume', determine_resume_start_stage(run, max_stage=m))
import json
idx = run/'07_lore_development_ledger/lore_development_ledger_index.json'
if idx.exists():
    s = json.loads(idx.read_text(encoding='utf-8'))
    print('stage07', {k: s.get(k) for k in ['status','entry_count','failure_count','segment_count']})
"

Do NOT spam the user. One summary when done or on catastrophic cancel.

WHEN TO DO NOTHING (healthy progress)
- Log: Stage 07 ledger model call N/M (real API, not base_url TypeError)
- entry_count increasing; failure_count not stuck at segment_count with entry_count 0
- Stage 07 complete → Stage 08 grouping progress → 08W
- OpenRouter rate-limit retries with eventual progress

WHEN TO CANCEL (catastrophic)
1. Traceback with no recovery; missing OPENROUTER_API_KEY; auth_failed; disk full
2. Stage 07 repeats: call_model_chat() got multiple values for argument 'base_url' (regression — stop and report)
3. Stage 07: entry_count=0 and failure_count=segment_count after stage claims complete
4. Same stage failing 3+ times with exit 1
5. Log shows Stage 09 or Stage 11 starting (config regression)
6. combined_snippet_count in merge report collapses far below 50000

WHEN NOT TO CANCEL (note only)
- Stage 07 slow (4430 segments — hours of runtime is normal)
- Partial ledger failures with most entries succeeding
- "Pipeline complete through configured end stage 8" — success

SUCCESS CRITERIA
- Log: Pipeline complete through configured end stage 8; stages 09-12 skipped
- Stage 07: entry_count >> 0, failure_count << segment_count (ideally failures near 0)
- Stage 08: snippet_clusters_lore + meta present
- 08W: narrative_work tags if enabled
- determine_resume_start_stage → stage 0 with "through Stage 08" / "09-12 deferred"

REPORT WHEN FINISHED (one message)
- Status: success | partial | cancelled_catastrophic
- Stage 07: entry_count, failure_count, segment_count
- Stage 08: lore_clusters, meta_clusters
- Stage 08W: tagged count (if ran)
- Any cancel reason + last 10 log lines
```

---

## Short variant

```
Watch artifacts/runs/20260517_032555635445_full stages 07–08 only. Repo D:\Workplaces\Enkidu Project\Theriac\Lore_bible.
Do NOT use check_quota or theriac_watch MCP (ops repo only; not OpenRouter).
Start: python scripts/pipeline_start_headless.py --resume --ignore-pending --run-root artifacts/runs/20260517_032555635445_full --start-stage 7
Poll: tail tauri_pipeline_worker.log every 5m + stage07 index entry_count.
Success = complete through stage 8 + ledger entries > 0. Cancel on base_url regression or stage 09+ in log.
```

---

## Reference

- [ops-repo.md](ops-repo.md) — quota/watch tooling location
- Prior 04R retry: [watch-04r-failed-retry.md](watch-04r-failed-retry.md)
