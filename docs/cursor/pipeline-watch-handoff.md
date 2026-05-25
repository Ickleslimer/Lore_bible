# Cursor pipeline watch handoff

Canonical repo: `D:\Workplaces\Enkidu Project\Theriac\Lore_bible` (see [../canonical-repo.md](../canonical-repo.md)).

## Before recommending a full pipeline run

**OpenRouter runs:** log + artifact supervision only — [watch-run-stage-07-through-08.md](../antigravity/watch-run-stage-07-through-08.md). Do not use quota/watch MCP from Lore_bible.

**Antigravity Gemini quota (optional):** run from **theriac-pipeline-ops** — `python scripts/check_quota.py`, then MCP `theriac_quota_preflight`. See [ops-repo.md](../antigravity/ops-repo.md).

## Start pipeline from Lore_bible (no watch)

```bash
python scripts/pipeline_start_headless.py --resume --run-root artifacts/runs/<run_id>
```

Or MCP-free full handoff with watch/sentinel from **ops repo**:

```bash
cd ../theriac-pipeline-ops
python scripts/pipeline_handoff.py --resume --run-root ../Lore_bible/artifacts/runs/<run_id>
```

## After the user starts a run (manual path)

1. **OpenRouter:** poll worker log + stage artifacts per [watch-run-stage-07-through-08.md](../antigravity/watch-run-stage-07-through-08.md).
2. **Antigravity Flash watch (optional):** use ops-repo `theriac_watch_start` or `pipeline_handoff.py`; follow [pipeline-watch-workflow.md](../antigravity/pipeline-watch-workflow.md).
3. Do **not** poll quota/watch MCP from Lore_bible.

## When the user returns

- Read `{run_root}/watch_report.md` or `{run_root}/watch_alert.json`.
- Optional: `/loop` on `.watch_done` for a single wake when the run completes.
