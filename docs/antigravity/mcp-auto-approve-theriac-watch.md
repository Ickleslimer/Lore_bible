# Stop repeated MCP approval prompts for `theriac-watch`

Antigravity defaults **all MCP tools** to **Ask**. Clicking **Yes, and always allow** on the popup often does **not** permanently allow `theriac_watch_status` — especially when:

- A **background task** / subagent calls the tool (your “2 tasks running” banner).
- The grant only lasts for the **rest of the current turn** (see [Antigravity permissions](https://antigravity.google/docs/permissions) — interactive MCP scope expansion).
- The rule was saved to **session** memory because the workspace was **Restricted**.

## Fix (persistent — do this once)

1. Open **Antigravity Settings** (`Ctrl+,` / `Cmd+,`).
2. Go to **Permissions** (or **Agent → Permissions**).
3. In the **Allow** list (not only the popup), add **one** of:
   - `mcp(theriac-watch/*)` — all tools on this server (recommended for pipeline watch)
   - `mcp(theriac-watch/theriac_watch_status)` — single tool only

Use the **exact** server name from `mcp_config.json` (`theriac-watch` with a hyphen, not `theriac_watch`).

4. **Trust the workspace**: Command Palette → `Workspaces: Manage Workspace Trust` → **Trust** the Lore_bible folder.
5. Optional: set `antigravity.commands.approvalScope` to `user` in user `settings.json` so future “always allow” clicks stick globally (see [approval loop fix](https://antigravitylab.net/en/articles/antigravity/antigravity-command-approval-dialog-repeating-fix)).

6. **Reload** if the UI offers it: Command Palette → search **Permission** / **Reload Permission**.

## If prompts still appear on background tasks

Automated **routines / background agents** sometimes ignore Allow lists (known class of bugs in Gemini/Antigravity stacks). Mitigations:

| Approach | What to do |
|----------|------------|
| **A. Allow list above** | Usually enough for the main Agent panel. |
| **B. Fewer MCP polls** | Watch via log + sentinel only; poll MCP once at start and once at end instead of every 5 minutes. |
| **C. Sentinel only** | Run ops-repo `python scripts/pipeline_watch_sentinel.py --loop` and tell Antigravity to `Get-Content …/tauri_pipeline_worker.log -Tail 40` instead of `theriac_watch_status` each poll. |

## Theriac watch tools (for selective Allow lines)

| Tool | Allow entry |
|------|-------------|
| All watch MCP | `mcp(theriac-watch/*)` |
| Status poll only | `mcp(theriac-watch/theriac_watch_status)` |
| Start watch | `mcp(theriac-watch/theriac_watch_start)` |
| Cancel run | `mcp(theriac-watch/theriac_pipeline_cancel)` |

## Config location (server must match)

Windows: `%USERPROFILE%\.gemini\antigravity\mcp_config.json`  
Example in repo: `docs/antigravity/mcp_config.example.json`
