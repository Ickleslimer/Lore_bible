# Quota capture via VirtualBox VM (Windows Home OK)

Run Antigravity in a **VirtualBox** Windows guest so UI automation never steals focus on your host (Cursor). Uses the same shared-folder worker protocol as [quota-worker.md](quota-worker.md).

## Overview

| Side | Role |
|------|------|
| **Host** (Lenovo / Cursor) | `quota_vm_session.py` — start VM, request capture, read `latest.png` |
| **Guest** (VM) | Antigravity + `quota_worker.py --loop` on a shared repo mount |

```
Host  D:\Workplaces\...\Lore_bible  ←→  shared folder  ←→  Guest  Z:\Lore_bible
         capture.request.json  ─────────────────────────────►  worker polls
         latest.png  ◄────────────────────────────────────────  worker writes
```

## 1. Install VirtualBox (host)

Download [VirtualBox](https://www.oracle.com/virtualization/technologies/vm/downloads/virtualbox-downloads.html) for Windows (works on Home).

Create a VM:

- Name: `TheriacQuota` (or set `THERIAC_QUOTA_VM_NAME`)
- OS: Windows 10/11, 4+ GB RAM, 2 CPUs
- Disk: 40+ GB
- **First setup:** start with GUI, install Windows + Guest Additions

## 2. Shared folder (host → guest)

VirtualBox → VM Settings → Shared Folders:

- Add: `D:\Workplaces\Enkidu Project\Theriac\Lore_bible`
- Mount point: `Lore_bible`
- Auto-mount, Permanent

In the **guest**, Guest Additions map this as `\\VBOXSVR\Lore_bible`. Assign a drive letter, e.g. **`Z:`** so the repo is `Z:\Lore_bible`.

## 3. Guest setup (inside VM, one time)

```powershell
cd Z:\Lore_bible
pip install -r requirements.txt
```

1. Install **Antigravity** in the VM; sign in; open the Lore_bible project.
2. Install Python (same major version as host is easiest).
3. Start worker manually once to verify:

```powershell
python scripts\quota_worker.py --loop --repo-root Z:\Lore_bible
```

4. Auto-start worker on logon (optional):

```powershell
$env:THERIAC_QUOTA_VM_REPO_ROOT = "Z:\Lore_bible"
powershell -ExecutionPolicy Bypass -File scripts\install_quota_vm_guest_startup.ps1
```

Confirm `artifacts\quota_snapshots\worker\worker.ready.json` appears after logon.

## 4. Host environment

Copy [quota-vm.env.example](quota-vm.env.example) values into your shell or a local `quota-vm.env` (not committed):

```powershell
set THERIAC_QUOTA_VM_NAME=TheriacQuota
set THERIAC_QUOTA_VM_REPO_ROOT=Z:\Lore_bible
```

`THERIAC_QUOTA_VM_REPO_ROOT` is the **guest** path written into capture requests so automation runs inside the VM.

## 5. Host commands

```powershell
cd "D:\Workplaces\Enkidu Project\Theriac\Lore_bible"

# Check VM + worker
python scripts\quota_vm_session.py status

# Start VM headless (if not running)
python scripts\quota_vm_session.py start

# Capture (starts VM if needed, waits for worker.ready, then --worker handoff)
python scripts\quota_vm_session.py capture

# Capture and save-state VM when done
python scripts\quota_vm_session.py capture --stop-after

# Stop worker signal + save VM
python scripts\quota_vm_session.py stop

# Print guest checklist
python scripts\quota_vm_session.py print-guest-setup
```

Preflight from Cursor (after VM worker is ready):

```powershell
set THERIAC_QUOTA_WORKER=1
set THERIAC_QUOTA_VM_REPO_ROOT=Z:\Lore_bible
python scripts\check_quota.py --worker
```

## Troubleshooting

| Issue | Fix |
|-------|-----|
| `VBoxManage not found` | Install VirtualBox; or set `THERIAC_VBOX_MANAGE` |
| Worker not ready | Log into VM; run worker or guest startup script |
| Capture fails in VM | Antigravity must be installed **in the guest**; path must be `Z:\Lore_bible` in request (set host `THERIAC_QUOTA_VM_REPO_ROOT`) |
| Black screenshot | AG window minimized in guest — keep AG logged in, not minimized |
| Slow first capture | VM cold start; use `start` ahead of time or leave VM running |

## RDP alternative

If you have Windows Pro, see [quota-rdp-session.md](quota-rdp-session.md) instead of a VM.
