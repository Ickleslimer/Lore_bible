# Ops tooling (theriac-pipeline-ops)

**Quota capture**, **pipeline watch MCP**, and **sentinel** live in a sibling repo so Antigravity supervisors on Lore_bible runs do not call quota tools thinking they check OpenRouter.

| Repo | Path |
|------|------|
| Lore_bible (pipeline) | `D:\Workplaces\Enkidu Project\Theriac\Lore_bible` |
| theriac-pipeline-ops (**private**) | Local: `D:\Workplaces\Enkidu Project\Theriac\theriac-pipeline-ops` or `Documents\Theriac\theriac-pipeline-ops` — Git: https://github.com/Ickleslimer/theriac-pipeline-ops |

## In theriac-pipeline-ops

- `scripts/check_quota.py` — Antigravity **Gemini** Model Quota only
- `scripts/pipeline_watch_sentinel.py`
- `scripts/pipeline_handoff.py` (full handoff with watch)
- `scripts/quota_*.py`
- `mcp_servers/theriac_watch/server.py` — configure MCP in **ops** repo (`docs/antigravity/mcp_config.example.json`)

Set `PYTHONPATH` to the ops repo first, then Lore_bible, when running handoff or watch from a checkout that spans both trees.

## Start pipeline from Lore_bible (no watch)

```powershell
python scripts/pipeline_start_headless.py --resume --run-root artifacts/runs/<run_id> --start-stage 7
```

Or:

```powershell
python -m pipeline.run_pipeline --docx ... --artifacts-root ... --start-stage 7
```

## OpenRouter run supervision

Use **worker log + artifact checks** only. See [watch-run-stage-07-through-08.md](watch-run-stage-07-through-08.md).

**Do not** run quota capture or `theriac_watch_*` MCP from Lore_bible during an OpenRouter pipeline run.
