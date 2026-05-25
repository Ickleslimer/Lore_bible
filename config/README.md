# Pipeline config

Live config files are **gitignored** (Theriac-specific paths, model routing, quest seeds).

| File | Purpose |
|------|---------|
| `pipeline_config.example.json` | Copy to `pipeline_config.json` before first run |
| `quest_song_seed.example.json` | Copy to `quest_song_seed.json`; expand via `scripts/import_quest_songs.py` |

Tests use `tests/fixtures/` instead of these live files.
