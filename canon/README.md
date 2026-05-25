# Canon (local only)

Everything under `canon/` is **gitignored** — review memory, theme profile, quest motifs, narrative work registry, and other persistent pipeline state for your Theriac workspace.

The pipeline creates and updates these files during normal runs. Bootstrap helpers:

- `scripts/bootstrap_quest_motifs.py` → `canon/quest_motifs.json`
- Desktop review app → `canon/review_memory.json`, theme approvals, etc.
- Stage 06C → `canon/theme_profile.json`

Tests use `tests/fixtures/` for sample registry data.
