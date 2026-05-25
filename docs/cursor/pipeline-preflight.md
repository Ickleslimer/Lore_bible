# Pipeline preflight (Cursor agents)

Antigravity **Gemini quota** preflight runs from **theriac-pipeline-ops**, not Lore_bible. It does **not** check OpenRouter credit.

See [ops-repo.md](../antigravity/ops-repo.md).

## Commands (ops repo)

```bash
cd "D:/Workplaces/Enkidu Project/Theriac/theriac-pipeline-ops"
python scripts/check_quota.py
```

On Windows this is **hands-free by default**: the script focuses Antigravity, opens **Settings → Models** (quota bars), and captures the window. Use `--no-auto-navigate` if the Models panel is already open, or set `THERIAC_QUOTA_AUTO_NAVIGATE=0`.

**Separate session (VM or RDP):** run the worker where Antigravity is visible; from Cursor use `THERIAC_QUOTA_WORKER=1` or `python scripts/check_quota.py --worker`. **VM (Home OK):** [docs/antigravity/quota-vm-session.md](../antigravity/quota-vm-session.md). **RDP (Pro+):** [docs/antigravity/quota-rdp-session.md](../antigravity/quota-rdp-session.md).

Snapshots land under the **Lore_bible** tree when `THERIAC_REPO_ROOT` points there:

- `artifacts/quota_snapshots/latest.png`
- `artifacts/quota_snapshots/latest.meta.json`

## Interpretation (ordinal)

| Gemini pool (3 bars) | Typical recommendation |
|----------------------|-------------------------|
| 0–1 filled | `wait_for_gemini_reset` or run with sentinel-only watch |
| 2 filled | Cautious; sentinel required if using Flash |
| 3 filled | `run_pipeline_and_flash_watch` |

| Claude/GPT pool (5 bars) | Role |
|--------------------------|------|
| 2+ when Gemini low | Failover watch target (`failover_to_gpt_pool_watch`) |

## OpenRouter (Lore_bible pipeline runs)

- Auto-topup is enabled for this project.
- Low `limit_remaining` is **informational only** — do not recommend waiting for billing reset.
- `missing_key` / `auth_failed` are separate configuration blockers.

## MCP (ops repo only)

Configure `theriac-watch` / `theriac_quota_preflight` in **theriac-pipeline-ops** MCP config, not Lore_bible.
