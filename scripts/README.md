# Lore_bible scripts

Operational helpers for the Theriac lore pipeline. Run from the repo root.

## Pipeline

| Script | Purpose |
|--------|---------|
| `pipeline_start_headless.py` | Start/resume pipeline worker only (no watch MCP, no quota capture) |
| `run_stage_08q.py` | Rerun Stage 08Q quest tagging on an existing run |
| `build_wiki_preview.py` | Build static HTML wiki preview from a run's cards |

## Quest / canon bootstrap (local)

These write **gitignored** files under `config/` and `canon/`:

| Script | Purpose |
|--------|---------|
| `import_quest_songs.py` | Import quest rows from Excel into `config/quest_song_seed.json` |
| `sync_quest_seed_metadata.py` | Refresh `quest_id`, pool order, and prerequisite links on seed rows |
| `bootstrap_quest_motifs.py` | Build `canon/quest_motifs.json` from seed `band_hint` values |

Copy `config/*.example.json` to the live filenames before first run. See root [README.md](../README.md).

## Ops tooling (sibling repo)

Quota capture, pipeline watch MCP, sentinel, and full Antigravity handoff live in **theriac-pipeline-ops**, not here:

`D:\Workplaces\Enkidu Project\Theriac\theriac-pipeline-ops`

See [docs/antigravity/ops-repo.md](../docs/antigravity/ops-repo.md).

## Windows setup

| Script | Purpose |
|--------|---------|
| `install_quota_vm_guest_startup.ps1` | Guest VM startup helper (run from ops repo when applicable) |
