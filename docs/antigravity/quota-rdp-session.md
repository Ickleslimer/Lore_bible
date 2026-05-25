# On-demand RDP quota session

Run Antigravity UI automation in a **separate Windows session** so your main desktop (Cursor) is not disturbed. Start the session when needed and tear it down when done.

## One-time setup

1. **Windows Pro or Enterprise** (incoming RDP is not available on Home).
2. Enable Remote Desktop: Settings → System → Remote Desktop → On.
3. Create a local user, e.g. `TheriacQuota` (password you control).
4. Sign in as that user **once** (RDP to `localhost`), install Antigravity, sign in, open the Lore_bible project.
5. Allow the quota user to use the repo on `D:\Workplaces\Enkidu Project\Theriac\Lore_bible`.

Store credentials for automation (optional):

```powershell
cmdkey /generic:TERMSRV/localhost /user:TheriacQuota /pass:YOUR_PASSWORD
```

Or set env vars when calling scripts (less secure):

```powershell
set THERIAC_QUOTA_RDP_USER=TheriacQuota
set THERIAC_QUOTA_RDP_PASSWORD=...
```

## At-will commands (session A / Cursor)

```powershell
cd "D:\Workplaces\Enkidu Project\Theriac\Lore_bible"

set THERIAC_QUOTA_RDP_USER=TheriacQuota

# Open RDP (minimized), auto-start quota_worker.py --loop in the RDP session
python scripts\quota_session.py start

# One capture via shared folder (worker must be ready)
python scripts\quota_session.py capture

# Capture then log off RDP and stop worker
python scripts\quota_session.py capture --stop-after

# Tear down: shutdown worker + log off RDP session
python scripts\quota_session.py stop

# Check state
python scripts\quota_session.py status
```

## What `start` does

1. Writes `artifacts/quota_snapshots/worker/quota_worker.rdp` with an **alternate shell** that runs `quota_worker.py --loop` on login.
2. Launches `mstsc.exe` to `THERIAC_QUOTA_RDP_HOST` (default `localhost`).
3. Waits for `worker.ready.json` (up to `--wait-ready` seconds).

Automation runs on the RDP desktop; you can minimize the RDP window or work on your console session.

## What `stop` does

1. Writes `shutdown.request.json` so the worker loop exits cleanly.
2. Runs `logoff <session_id>` for the quota user.

## Notes

- First `start` may show an RDP window until login completes; save credentials with `cmdkey` to reduce prompts.
- Same-machine RDP may disconnect the console session on some Windows editions; use a VM or second PC if that is a problem.
- `capture` uses the existing shared-folder protocol (`capture.request.json` / `capture.response.json`).

See also [quota-worker.md](quota-worker.md).
