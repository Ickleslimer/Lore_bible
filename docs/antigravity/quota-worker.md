# Quota capture worker (session B)

Use this when Antigravity runs in a **different Windows session** (RDP, VM, second user) than Cursor. Both sessions must see the same repo path (e.g. `D:\Workplaces\Enkidu Project\Theriac\Lore_bible`).

## Session B (where Antigravity is visible)

```powershell
cd "D:\Workplaces\Enkidu Project\Theriac\Lore_bible"
python scripts\quota_worker.py --loop
```

Leave this running. It polls every 2s for `artifacts/quota_snapshots/worker/capture.request.json`.

## Session A (Cursor / MCP)

```powershell
set THERIAC_QUOTA_WORKER=1
python scripts\check_quota.py
```

Or one-shot without env:

```powershell
python scripts\check_quota.py --worker
```

Session A writes the request, waits for `capture.response.json`, then reads `artifacts/quota_snapshots/latest.png` as usual.

## Protocol files

| File | Writer | Purpose |
|------|--------|---------|
| `worker/capture.request.json` | Session A | Job payload (`request_id`, `repo_root`, `auto_navigate`) |
| `worker/capture.response.json` | Session B | Result + embedded capture dict |
| `worker/capture.lock.json` | Session B | In-flight guard |

## MCP

`theriac_quota_preflight(run_capture=true)` uses the worker when `THERIAC_QUOTA_WORKER=1` is set in the environment of the MCP server process.

## Timeout

Default wait 120s. Override with `THERIAC_QUOTA_WORKER_TIMEOUT=180`.

## VirtualBox VM (Windows Home)

No RDP host required. See [quota-vm-session.md](quota-vm-session.md) and `python scripts/quota_vm_session.py`.

## On-demand RDP start/stop (Windows Pro+)

For scripted session lifecycle (start RDP → capture → stop), see [quota-rdp-session.md](quota-rdp-session.md) and `python scripts/quota_session.py`.

## Same-machine smoke test

Two terminals on one PC (simulates A/B):

1. Terminal 1: `python scripts\quota_worker.py --loop`
2. Terminal 2: `python scripts\check_quota.py --worker`

Antigravity must be visible in the session running the worker (terminal 1).
