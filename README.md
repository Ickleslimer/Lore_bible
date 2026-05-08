# THERIAC Canon-Extraction and Wiki Pipeline

This repo now contains an incremental, stage-based pipeline that:

- bootstraps canon from `theriac-coda---lore-bible.docx`
- ingests and normalizes Discord discrub exports
- enforces cutoff `2025-01-08T01:05:00Z`
- merges all DMs into one global chronology
- extracts THERIAC-relevant snippets and routes `lore` vs `meta`
- resolves aliases and timelines
- creates draft lore/meta patches
- supports UI-first review decisions
- exports approved results to Notion NDJSON shape

## Mixtral Anchor Deciding

Stage C supports `anchor_provider` modes in `config/pipeline_config.json`:

- `heuristic`: keyword-only relevance and anchor candidates
- `mixtral`: model-only classification and anchors
- `hybrid`: heuristic + Mixtral blend (recommended)

Current default is `hybrid`, targeting Ollama-compatible endpoint:

- `mixtral.base_url`: `http://127.0.0.1:11434`
- `mixtral.model`: `mixtral`
- `mixtral.provider`: `auto|mistral_api|ollama`
- `mixtral.api_base_url`: `https://api.mistral.ai/v1`
- `mixtral.api_model`: `mistral-large-latest`
- strict JSON prompt with fallback to heuristics on timeout/invalid output

API key loading:

- reads from environment (`MIXTRAL_API_KEY` or `MISTRAL_API_KEY`)
- also supports `.env` entries in `KEY=value` or `KEY: "value"` format
- in `provider=auto`, API is attempted first when key is present, then Ollama fallback

Stage A also supports Mixtral ontology extraction from lore bible:

- `stage_a_anchor_provider`: `heuristic|mixtral|hybrid`
- `stage_a_mixtral_excerpt_chars`: max lore excerpt size sent to model
- heuristic fallback is always preserved in `hybrid`
- model output can include conservative `aliases` and `relationship_hints` for each seeded card

## Thematic Linking (Historical + Musical)

Stage D performs lightweight thematic signal tagging (configurable in `thematic_linking`):

- `historical_markers`: detects historical/civilizational naming motifs
- `music_markers`: detects music/artist/track motifs
- pattern hints: quoted song-like titles and `by <Artist>` references

Stage F carries these into lore patch drafts as:

- `thematic_tags`
- `proposed_relationship_hints` (soft evidence only, conservative confidence)

Continuity memory is also computed in Stage D:

- `thematic_memory.artists` tracks repeated artist mentions over chronology
- includes co-occurrence counts with character and quest names
- Stage F boosts music-link hint confidence only when repeated continuity evidence exists

Adaptive thematic updates (safe mode):

- Stage A/C can suggest thematic markers through model outputs.
- Suggestions are written to runtime profile only, never to base config:
  - `artifacts/.../learning/thematic_profile_runtime.json`
- Stage D merges base config markers + active runtime markers.
- Bad runs do not destroy `config/pipeline_config.json`.

## Install

```bash
python -m pip install -r requirements.txt
```

## Quickstart

```bash
python -m pipeline.run_small_batch_validation
python -m pipeline.ui_review_app
```

Windows shortcut commands from repo root:

```bash
run_small_batch.bat
start_ui.bat
```

To see verbose stage progress and debug-level internals:

```bash
python -m pipeline.run_small_batch_validation --log-level DEBUG
```

## Run Stages A-F

```bash
python -m pipeline.run_pipeline --docx "theriac-coda---lore-bible.docx" --conversations-root "discord_conversations" --artifacts-root "artifacts"
```

`run_pipeline` also supports `--log-level` (`DEBUG|INFO|WARNING|ERROR`).

## Start UI Review (Stage G1)

```bash
python -m pipeline.ui_review_app
```

This now auto-discovers common paths (`artifacts/...` and `artifacts/small_batch/...`).
If draft patches are missing, the UI shows a bootstrap screen with a `Run Full Pipeline (Stages A-F)` button so you can generate them directly.
The UI now includes a live `Pipeline Run Logs` panel (status + streaming output) while a run is in progress.

Optional explicit forms:

```bash
python -m pipeline.ui_review_app --artifacts-root "artifacts/small_batch"
python -m pipeline.ui_review_app --patches "artifacts/06_drafts/card_drafts/lore_patches.json" --decisions "artifacts/07_review/merge_decisions.json" --directives "artifacts/07_review/author_directives.json"
```

## Apply Decisions (Stage G2)

```bash
python -m pipeline.stage_g_merge_engine --in-seed-json "artifacts/01_bootstrap/canon_seed.json" --in-lore-patches-json "artifacts/06_drafts/card_drafts/lore_patches.json" --in-decisions-json "artifacts/07_review/merge_decisions.json" --in-author-directives-json "artifacts/07_review/author_directives.json" --out-cards-json "artifacts/07_review/canonical_cards.json" --out-merge-log-jsonl "artifacts/07_review/merge_log.jsonl"
```

## Author Directives

`author_directive` is treated as highest source of truth in Stage G merge precedence.

Suggested natural-language patterns:

- `replace summary with: ...`
- `append summary: ...`
- `set status: canonical`
- `add alias: Working Name`
- `remove alias: Obsolete Name`

## Notion Export (Stage H)

```bash
python -m pipeline.stage_h_notion_export --in-cards-json "artifacts/07_review/canonical_cards.json" --in-meta-cards-json "artifacts/06_drafts/card_drafts/meta_cards_draft.json" --in-alias-json "artifacts/05_alias/alias_map.json" --in-snippets-jsonl "artifacts/03_relevance/snippets_candidates.jsonl" --in-profiles-json "artifacts/03_relevance/dm_source_profiles.json" --in-merge-log-jsonl "artifacts/07_review/merge_log.jsonl" --out-ndjson "artifacts/08_notion/notion_import.ndjson"
```

## Small Batch Validation

```bash
python -m pipeline.run_small_batch_validation
```

Defaults:

- `--base-dir artifacts`
- `--conversations-root discord_conversations`
- `--docx theriac-coda---lore-bible.docx` (or a single `.docx` in repo root)

Optional explicit form:

```bash
python -m pipeline.run_small_batch_validation --base-dir "artifacts" --conversations-root "discord_conversations" --docx "theriac-coda---lore-bible.docx" --sample-limit-files 6
```

`run_small_batch_validation` also supports `--log-level` (`DEBUG|INFO|WARNING|ERROR`).
