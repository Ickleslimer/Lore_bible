# Theriac Lore Card Pipeline

This repo contains a stage-based pipeline for turning Theriac Discord exports into reviewed, wiki-style lore cards.

**Naming:** The game/franchise is **Theriac** (title case), not an acronym — use `Theriac` in user-facing copy, model prompts, and wiki UI. Environment variables keep the `THERIAC_` prefix (e.g. `THERIAC_LORE_ROOT`) for backward compatibility. Canonical name constant: [`pipeline/branding.py`](pipeline/branding.py) (`GAME_NAME`).

V2 contract:

- The initial lore bible is ontology scaffolding only.
- Bootstrap output is `entity_seed.json`, not canon.
- Discord snippets become atomic claim drafts, not pasted card prose.
- **Card-first synthesis (default):** Stage 11 drafts from approved lore snippet bundles per entity plus entity development history. Accepted claims are guardrails (author corrections, conflicts, high-risk facts), not a line-by-line wiki authoring checklist. Low-risk lore claims can be auto-accepted at Stage 11 (`claim_auto_accept_report.json`); human claim review remains available for disputes.
- **Protagonist-tier section-chained synthesis:** Dense `character` entities with large snippet clusters (default: ≥80 approved snippets, e.g. Enoch) use a lore-digest pass, per-section writers (peaceful-path **summary** → background → role → relationships → **history_theriac_coda** / optional **history_path_a_side_route** → inspirations), and a merge pass—Fandom-style depth (~1.2k–1.9k words) without one-shot context overload. The **summary lede** reflects the expected Path B / peaceful main route only; Path A (~6h side route) lives in `history_path_a_side_route`, not the lead.
- **Narrative work pages:** Franchise hub cards (phase 1: **Theriac Coda** in `11_work_synthesis/work_cards.json`) are separate from entity lore cards. Stage **08W** tags snippets with `narrative_work_id` (`canon/narrative_works.json`); Stage **11W** synthesizes active work pages. Character `history_*` sections filter evidence by those tags. Overrides: `narrative_work_tag_overrides` in `canon/review_memory.json`.
- Human review approves synthesized card drafts (pass 2) as the main wiki gate; claim review (pass 1) is selective rather than mandatory for every sentence.
- `canonical_cards.json` contains only human-approved cards.
- Persistent corrections live in `canon/review_memory.json` and are reused by later runs.
- Later accepted claims revise the same entity card from the full accepted claim history, rather than appending raw text or leaving the original card untouched.
- Stage 11 can run a card-base architecture agent before drafting, so freeform structural requests become reviewable card moves, demotions, redirects, aliases, directives, and author claims.
- Theme learning updates `canon/theme_profile.json` for relevance priors only; a theme match is not automatic canon promotion.

## Flow

1. Stage 01 Entity Bootstrap: read `theriac-coda---lore-bible.docx` and write ontology seeds to `01_entity_bootstrap/entity_seed.json`.
2. Stage 02 Message Normalization: normalize Discord exports.
3. Stage 03 Timeline Merge: merge normalized exports into a global chronology.
4. Stage 04 Relevant Conversation Segmentation: split 1:1 DMs into model-approved Theriac-relevant conversations, bounded first by 12-hour coarse windows and then by topic shifts.
   - Stage 04 keeps emitted conversation messages non-overlapping. Duplicate or nested model spans are dropped, partial overlaps are trimmed to their non-overlapping tail, and counters are recorded in `conversation_index.json`.
   - Stage 04 also applies a strict relevance gate: the actual emitted message span must mention Theriac or a known entity seed/alias. Model-inferred inspiration without an explicit project/entity tie is dropped and counted in `model_segments_dropped_by_relevance`.
5. Stage 05 Snippet Extraction: extract Theriac-relevant snippets from `messages_relevant_conversations.jsonl`, using `conversation_id` as the context boundary and Stage 04 relevance metadata (`conversation_metadata` anchor provider by default). Artifacts live under `05_snippet_extraction/`.
6. Stage 06 Entity Resolution and Theme Learning (runs as four sub-stages):
   - **06A Candidate Harvest:** resolve seed entities, aliases, acronyms, and duplicate seed names; harvest conversation-born candidates into `06_entity_resolution/entity_candidate_harvest.json`. Text-observed entities land in `resolved_entities.json`; bootstrap-only matches stay under `seed_only_entities`. Unmatched conversation anchors go to `conversation_entity_proposals.json`.
   - **06B Entity Adjudication:** score ambiguous candidates for external-media vs in-world relevance; write `entity_adjudication_recommendations.json` and cache web lookups in `externality_cache.json`.
   - **06C Theme Miner:** learn/update thematic relevance patterns in `canon/theme_profile.json`; write `theme_profile_update_report.json`.
   - **06D Theme Reclassification:** reclassify harvested candidates using the active theme profile; write `theme_candidate_reclassification.json`.
   - **04R / 06R (optional, when `theme_aware_rerun.enabled`):** rescore previously rejected conversation windows and rescan previously accepted Stage 04 segments for theme/quest-song signals; extract rescue snippets and merge into `snippets_candidates_with_theme_rescue.jsonl` for downstream stages.
   - Conversation entity proposals aggregate type evidence across snippets; pending proposals can gate the run before grouping/drafting. Approved/rejected decisions in `conversation_entity_decisions.json` persist to `canon/review_memory.json`.
7. Stage 07 Lore Development Ledger: after entity resolution, emit chronological `New`/`Change` development entries per resolved entity from strict Stage 04 segments plus optional 04R rescue segments. Outputs under `07_lore_development_ledger/` (`lore_development_ledger.jsonl`, `entity_development_history.json`) for Stage 11 card synthesis context (claims remain the human review surface).
8. Stage 08 Snippet Grouping: group snippets (including theme-rescue merge output when present) against resolved and approved conversation-born entities.
8W. Stage 08W Narrative Work Tagging: batch-classify snippets with `narrative_work_id` (meta snippets first) to `08_narrative_work_tagging/snippet_narrative_work_tags.jsonl`.
9. Stage 09 Claim Drafting: run model-required claim extraction to `09_claim_drafting/claim_drafts.json`.
10. Review pass 1: accept/reject/edit conversation entity proposals and atomic claims in the UI. Optionally use Story Questions in the desktop app to ask one high-value author question at a time, ask the configured story model to propose claim decisions and author claims from the answer, approve/discard/critique that proposal, then generate the next question from the reduced unresolved claim list.
11. Stage 10 Identity Cluster Preflight: detect pairwise identity evidence from accepted claims, collapse it into connected entity clusters, and ask the configured model to suggest one canonical wiki-page title per cluster in `10_identity_merge/identity_merge_proposals.json`.
12. Review identity cluster proposals in `10_identity_merge/identity_merge_decisions.json`; approved clusters are expanded into deterministic entity merges and persisted to `canon/review_memory.json`.
13. Stage 11A Card Architecture Agent: read accepted claims, author claims, source snippets, entity development history, existing cards, review memory, and freeform requests from `11_card_synthesis/card_edit_requests.jsonl`; write proposed actions to `11_card_synthesis/card_architecture_proposals.json`.
14. Stage 11B Card Architecture Review/Application: approve or reject proposed actions in the desktop app. Approved demotions, redirects, claim moves, aliases, directives, and author claims are applied as an overlay and recorded in `card_architecture_applied.json`, `card_redirects.json`, and `canon/review_memory.json`.
15. Stage 11W Work Card Synthesis: synthesize active narrative work hub pages (phase 1: `theriac_coda` only) into `11_work_synthesis/work_cards.json`.
16. Stage 11C Card Synthesis: synthesize draft wiki-card revisions from approved lore snippet bundles and accepted claims for each final architecture work item, using per-entity development history, Theriac Coda work-card frame, and per-route snippet tags. Cite `snippet_*` IDs and/or claim IDs in each section's `support_map`.
17. Stage 11 Draft Notion Sync: live-sync synthesized draft cards (and narrative work cards when present) to Notion for comfortable reading while keeping desktop decisions as the source of truth.
18. Review pass 2: approve/edit synthesized card drafts.
19. Stage 11 Canon Merge: write approved revisions to `11_card_synthesis/canonical_cards.json`, carrying forward unchanged canonical cards.
20. Stage 12 Notion Export: export approved canonical cards, work cards, and supporting records to Notion NDJSON.

Each run stores artifacts under numbered folders (`01_entity_bootstrap/` … `12_notion_export/`) inside the run root (typically `artifacts/runs/<timestamp>/`). Legacy folder names are migrated forward on resume via `pipeline/artifact_paths.py`.

## Install

```bash
python -m pip install -r requirements.txt
```

## Canonical repo path

The repo lives at **`D:\Workplaces\Enkidu Project\Theriac\Lore_bible`**. A junction at `C:\Users\mrdyl\Documents\Theriac\Lore_bible` points to the same tree for older IDE workspaces. See [docs/canonical-repo.md](docs/canonical-repo.md).

## Pipeline watch, quota preflight, and failover

Cross-IDE tooling for long pipeline runs:

- **Preflight (Cursor):** Before recommending a full run, capture Antigravity Model Quota (`python scripts/check_quota.py`), read `artifacts/quota_snapshots/latest.png`, and call MCP `theriac_quota_preflight`. OpenRouter balance is informational only (`THERIAC_OPENROUTER_AUTO_TOPUP=1`); Antigravity pool bars drive watch/run advice. See [docs/cursor/pipeline-preflight.md](docs/cursor/pipeline-preflight.md).
- **Quota VM (Windows Home):** VirtualBox guest runs Antigravity + worker; host uses `python scripts/quota_vm_session.py`. See [docs/antigravity/quota-vm-session.md](docs/antigravity/quota-vm-session.md).
- **Watch (Antigravity Flash):** MCP server `theriac-watch` — `theriac_pipeline_handoff` or `theriac_watch_start` / `theriac_watch_status`. See [docs/antigravity/pipeline-watch-workflow.md](docs/antigravity/pipeline-watch-workflow.md).
- **Headless handoff:** `python scripts/pipeline_handoff.py` starts pipeline + sentinel without Tauri.
- **Sentinel (no LLM):** `python scripts/pipeline_watch_sentinel.py --loop` detects stale Flash polls and writes `watch_alert.json`. See [docs/antigravity/pipeline-watch-failover.md](docs/antigravity/pipeline-watch-failover.md).
- **Cursor MCP:** [`.cursor/mcp.json`](.cursor/mcp.json). **Antigravity IDE:** `%USERPROFILE%\.gemini\config\mcp_config.json` (see [docs/antigravity/mcp_config.example.json](docs/antigravity/mcp_config.example.json)).

Handoff summary: [docs/cursor/pipeline-watch-handoff.md](docs/cursor/pipeline-watch-handoff.md).

## Quickstart

Generate claim drafts:

```bash
python -m pipeline.run_pipeline --docx "theriac-coda---lore-bible.docx" --conversations-root "discord_conversations" --artifacts-root "artifacts/runs/<run_folder>"
```

Resume an existing run from the earliest stale stage (respects pending review gates unless `--ignore-pending`):

```bash
python -m pipeline.run_pipeline --docx "theriac-coda---lore-bible.docx" --conversations-root "discord_conversations" --artifacts-root "artifacts/runs/<run_folder>" --resume
```

Resume after completed Stage 04 segmentation, regenerating Stage 05–09 artifacts without rerunning normalization or segmentation:

```bash
python -m pipeline.run_from_stage_05 --artifacts-root "artifacts/runs/<run_folder>"
```

Start the desktop review app:

```bash
theriac-lore-tauri.exe
```

The Tauri/Svelte desktop app is the primary review surface. Tabs: Pipeline, Claims, Entities, Themes, Identity, Relationships, Drafts, Agent, Overview. It includes the run selector for previous CLI-generated batches, pipeline run controls, cancellation, progress tracking, entity/claim inventory, theme learning review, identity-cluster review, relationship graph, and draft-card browsing. Select `New Run` before starting a full run to create a fresh timestamped artifact folder under `artifacts/runs/`. During claim review, use `Story Questions` for the optional guided review session. `Propose Updates` calls the configured story model; `Approve Proposal` only commits the already proposed decisions and does not make another model call. Use `Card Agent` for freeform card-base commissions such as identity merges or moving a standalone draft into another card section. The Agent tab can run the Cardbase Agent on demand without resuming the pipeline; Stage 11 still processes any pending request rows for compatibility before card writing.

The pipeline pauses with exit code `2` when review is required: pending claims before Stage 10, pending identity clusters or card architecture proposals before Stage 11, pending card drafts before Stage 12.

Run the Cardbase Agent from the CLI without resuming the pipeline:

```bash
python -m pipeline.run_cardbase_agent --artifacts-root "artifacts/runs/<run_folder>" --request "Pandora's mother is Izanami"
```

For Notion lore-card preview and publishing, add these to `.env`:

- `NOTION_ACCESS_TOKEN` or `NOTION_API_KEY`: Notion integration token.
- **Draft preview** (format check while reviewing):
  - `NOTION_PAGE_ID` or `NOTION_DRAFT_PARENT_PAGE_ID`: parent page for the draft-card database.
  - Optional `NOTION_DRAFT_CARDS_DATABASE_ID`: pin a specific draft database (recommended if duplicate databases already exist on the parent page).
- **Final / canonical lore** (approved cards only):
  - `NOTION_CANONICAL_PARENT_PAGE_ID` or `NOTION_FINAL_PARENT_PAGE_ID`: separate parent page for the canon database.
  - Optional `NOTION_CANONICAL_CARDS_DATABASE_ID` or `NOTION_FINAL_CARDS_DATABASE_ID`: reuse an existing canon database.

The integration must be shared with both parent pages (or their databases). Stage 11 syncs **draft** cards to the draft parent (`12_notion_export/notion_draft_sync_report.json`). Stage 12 syncs **canonical** cards to the final parent (`12_notion_export/notion_canonical_sync_report.json`). Draft pages include review metadata; canon pages use the same article sections without the draft disclaimer.

Manual resync:

```bash
python -m pipeline.notion_draft_sync --artifacts-root "artifacts/runs/<run_folder>" --target draft
python -m pipeline.notion_draft_sync --artifacts-root "artifacts/runs/<run_folder>" --target canonical
python -m pipeline.notion_draft_sync --artifacts-root "artifacts/runs/<run_folder>" --target both
```

Draft pages are keyed by card ID and run ID. Canon pages are keyed by card ID only (one page per entity across runs).

Notion database resolution order: explicit `NOTION_*_CARDS_DATABASE_ID` env var, then an existing child database on the parent page with the expected title (oldest wins if duplicates exist), then `artifacts/learning/notion_cards_state.json`, and only then create a new database.

### Targeted wiki seed (two-card test run)

To synthesize and export only specific entities (default: **Enoch**, **Krypteia**) from an existing run that already has claim drafts:

```bash
python -m pipeline.targeted_card_run \
  --source-run "artifacts/runs/<full_run_id>" \
  --artifacts-root "artifacts/runs/wiki_seed_enoch_krypteia" \
  --entities "Enoch,Krypteia" \
  --auto-accept-claims \
  --auto-approve-cards
```

- `--source-run`: a prior pipeline run with `06_entity_resolution/resolved_entities.json` and `09_claim_drafting/claim_drafts.json` (claims reviewed, or pass `--auto-accept-claims`).
- `--auto-approve-cards`: writes `canonical_cards.json` without a second desktop review pass.
- Omit `--skip-notion` to push draft + canon pages to the two Notion parent databases.
- Re-run the same command after lore changes; it overwrites the targeted run root.
- Protagonist **character** cards use separate Notion sections **Path B — Main Route (Peaceful)** and **Path A — Side Route (Destructive)** instead of one blended timeline (~1h branch: lab vs execute-lab orders; B is ~40+ h main arc, A is ~6 h side route). Validation rejects obvious cross-path bleed between those sections.

### Static HTML wiki preview (Fandom-style layout)

Local, read-only site for layout trials (not production export). Builds a landing page, per-entity articles with a **top-right infobox**, and a **header search bar with autocomplete**. Includes **narrative work pages** from `11_work_synthesis/work_cards.json` when that file exists on the run.

```bash
python scripts/build_wiki_preview.py \
  --artifacts-root "artifacts/runs/wiki_seed_enoch_krypteia" \
  --entities "Enoch,Krypteia" \
  --out-dir "artifacts/wiki_preview_enoch_krypteia" \
  --open
```

Open `artifacts/wiki_preview_enoch_krypteia/index.html` in a browser (or use `--open`). Search uses `search-index.json` (title + excerpt). Omit `--entities` to include every card in the run. Use `--drafts` to prefer `card_drafts.json` over canonical cards.

Start the new Tauri/Svelte desktop app during development:

```bash
cd desktop-tauri
nvm use
npm install
npm run dev
```

The desktop app is pinned to even-numbered Node LTS lines. Use Node.js 24 LTS by default; Node.js 22 LTS is also supported. Avoid Node.js 23, which is end-of-life and can make npm emit experimental CommonJS/ESM warnings from npm internals.

Build the Tauri/Svelte app:

```bash
build_tauri_app.bat
```

The Tauri app uses a Svelte UI and a small Python JSON bridge (`pipeline.tauri_bridge`) so the pipeline remains the source of truth. Building Tauri on Windows requires the Rust stable toolchain and Microsoft Visual C++ Build Tools (`link.exe`); `build_tauri_app.bat` loads the Visual Studio developer environment automatically when Build Tools are installed. A successful build copies the app executable to `theriac-lore-tauri.exe` in the repository root for easy launching.

After claim review, generate identity merge proposals:

```bash
python -m pipeline.stage_10_identity_merge --in-entities-json "artifacts/06_entity_resolution/resolved_entities.json" --in-claim-drafts-json "artifacts/09_claim_drafting/claim_drafts.json" --in-claim-decisions-json "artifacts/09_claim_drafting/claim_review_decisions.json" --in-review-memory-json "canon/review_memory.json" --out-identity-merge-proposals-json "artifacts/10_identity_merge/identity_merge_proposals.json" --in-identity-merge-decisions-json "artifacts/10_identity_merge/identity_merge_decisions.json" --in-pipeline-config-json "config/pipeline_config.json"
```

If Stage 10 reports pending identity cluster proposals, review `artifacts/10_identity_merge/identity_merge_proposals.json`, optionally edit the canonical name in the GUI, save decisions to `artifacts/10_identity_merge/identity_merge_decisions.json`, and rerun Stage 10. Then synthesize card drafts:

```bash
python -m pipeline.stage_11_card_synthesis --in-entities-json "artifacts/06_entity_resolution/resolved_entities.json" --in-claim-drafts-json "artifacts/09_claim_drafting/claim_drafts.json" --in-claim-decisions-json "artifacts/09_claim_drafting/claim_review_decisions.json" --in-card-review-decisions-json "artifacts/11_card_synthesis/card_review_decisions.json" --in-author-directives-json "artifacts/11_card_synthesis/author_directives.json" --in-review-memory-json "canon/review_memory.json" --out-card-drafts-json "artifacts/11_card_synthesis/card_drafts.json" --out-cards-json "artifacts/11_card_synthesis/canonical_cards.json" --out-merge-log-jsonl "artifacts/11_card_synthesis/merge_log.jsonl" --in-pipeline-config-json "config/pipeline_config.json"
```

If Stage 11 reports pending card architecture proposals, review `artifacts/11_card_synthesis/card_architecture_proposals.json`, save decisions to `artifacts/11_card_synthesis/card_architecture_decisions.json`, and rerun Stage 11. Then approve card drafts in the UI and rerun Stage 11 to promote approved cards to canon.

Export approved canon:

```bash
python -m pipeline.stage_12_notion_export --in-cards-json "artifacts/11_card_synthesis/canonical_cards.json" --in-meta-cards-json "artifacts/09_claim_drafting/meta_cards_draft.json" --in-alias-json "artifacts/06_entity_resolution/alias_map.json" --in-snippets-jsonl "artifacts/05_snippet_extraction/snippets_candidates.jsonl" --in-profiles-json "artifacts/05_snippet_extraction/dm_source_profiles.json" --in-merge-log-jsonl "artifacts/11_card_synthesis/merge_log.jsonl" --out-ndjson "artifacts/12_notion_export/notion_import.ndjson"
```

## Model Requirement

Stages 04, 05, 06A–06D, 04R, 07, 09, 10, and 11 require a valid model response for their model-backed paths. If the configured provider is unavailable or returns invalid JSON, the pipeline records the failed conversation window/note or stops rather than producing low-quality fallback prose.

Configure providers in `config/pipeline_config.json`:

- `anchor_provider`: `heuristic|model|hybrid` for legacy Stage 06 relevance routing when Stage 04 metadata is unavailable.
- `stage_06_anchor_provider`: defaults to `conversation_metadata`, which trusts Stage 04's model-approved conversation segments and avoids per-message model calls.
- `stage_01_anchor_provider`: `heuristic|model|hybrid` for ontology seed extraction.
- `model_provider.provider`: `openrouter|openai_compatible|api`. Legacy values such as `gemini`, `auto`, and `ollama` are treated as `openrouter`.
- `model_provider.api_model`: defaults here to `qwen/qwen3.5-flash-02-23` for low-cost/high-volume work.
- `model_provider.adaptive_min_interval_seconds`: currently `0.5`; the runtime file will increase this automatically if rate limits appear.
- `model_routing.profiles.high_volume`: routes cheap/high-volume work to `qwen/qwen3.5-flash-02-23`.
- `model_routing.profiles.balanced_reasoning`: routes reasoning-sensitive work to `qwen/qwen3.5-flash-02-23`.
- `model_routing.profiles.premium_reasoning`: routes low-volume work to DeepSeek V4 Flash through OpenRouter.
- `model_routing.profiles.deep_reasoning`: routes low-volume reasoning work to DeepSeek V4 Flash through OpenRouter.
- `model_routing.profiles.card_writing`: routes high-volume final card prose to DeepSeek through OpenRouter.
- `model_routing.tasks.stage_04_conversation_segmentation`: currently uses synchronous high-volume profile so segmentation progress remains visible.
- `model_routing.tasks.stage_05_lore_development_ledger`: uses balanced reasoning profile for entity-anchored New/Change development deltas.
- `model_routing.tasks.stage_07a_entity_candidate_harvest`: Qwen 235B for candidate annotation.
- `model_routing.tasks.stage_07b_entity_adjudication_web`: GPT-OSS 120B with OpenRouter web search for externality checks.
- `model_routing.tasks.stage_07c_theme_miner`: DeepSeek V4 Flash for theme profile updates.
- `model_routing.tasks.stage_07d_theme_reclassification`: DeepSeek V4 Flash for theme-aware candidate reclassification.
- `model_routing.tasks.stage_04r_theme_relevance_adjudication`: DeepSeek V4 Flash for theme-rescue window scoring (when theme rerun is enabled).
- `model_routing.tasks.stage_09_claim_drafting`: currently uses synchronous high-volume profile.
- `model_routing.tasks.stage_10_identity_merge_proposals`: currently uses the deep reasoning profile for identity/alias merge proposal detection.
- `model_routing.tasks.stage_10_identity_merge_cluster_judgement`: currently uses the deep reasoning profile to choose canonical names and aliases for collated identity clusters.
- `model_routing.tasks.stage_11_card_architecture_agent`: currently uses the deep reasoning profile for low-volume structural card-base reasoning before final drafting.
- `model_routing.tasks.stage_11_card_synthesis`: currently uses the card writing profile for final card drafting, with validation retries to keep synthesis stateful and evidence-bound.
- `model_routing.tasks.stage_09_story_questions`: currently uses the deep reasoning profile for iterative question generation and answer-application proposals.
- `story_questions`: controls the optional guided claim-review flow, using DeepSeek V4 Flash through OpenRouter by default, and writes `09_claim_drafting/story_question_session.json`, `story_questions.jsonl`, `story_question_answers.jsonl`, `story_question_application_proposals.jsonl`, `story_question_applications.jsonl`, and `story_question_failures.json`.
- `conversation_segmentation.max_gap_hours`: coarse DM window boundary before model topic segmentation; defaults to `12`.
- `conversation_segmentation.self_user_id`: optional account override for 1:1 DM pair detection.
- `theme_aware_rerun.enabled`: when true, runs Stage 04R/06R after Stage 06D to rescue theme-matched snippets from previously rejected windows and rescan previously accepted Stage 04 segments.
- `theme_aware_rerun.rerun_include_previous_accepts`: when true, Stage 04R also scores strict Stage 04 accepted segments for supplemental snippet extraction (e.g. after embed or theme-profile updates).
- `theme_aware_rerun.rerun_only_previous_rejects`: legacy inverse flag; ignored when `rerun_include_previous_accepts` is true.
- `theme_aware_rerun.max_concurrent_adjudication_calls`: parallel OpenRouter adjudication chunks for Stage 04R (default 3).
- `theme_aware_rerun.adjudication_provider_retries` / `adjudication_provider_retry_sleep_seconds`: pacing-aware retry loop for 04R model adjudication.
- `model_routing.tasks.*.max_concurrent_requests`: per-task OpenRouter concurrency cap used by parallel model dispatch helpers (Stage 04 sync, 04R, 07A, 07D).
- `theme_aware_rerun.require_human_approval_for_new_theme_use`: gate new theme labels before they affect rerun scoring.
- `theme_aware_rerun.require_human_approval_to_start_rescue`: when true (default), the pipeline pauses after Stage 06C/06D until you approve rescue in the desktop **Theme Rescue** tab; then 04R and 06R run on resume from Stage 06.
- `thematic_linking`: quest-song markers, historical/music markers, and runtime profile updates for snippet grouping.
- API keys are read from `OPENROUTER_API_KEY`, `OPENROUTER_KEY`, `OPEN_ROUTER_API_KEY`, `MODEL_API_KEY`, `MODEL_PROVIDER_API_KEY`, `OPENAI_COMPATIBLE_API_KEY`, or `.env`.

## Theme Profile

`canon/theme_profile.json` stores learned thematic patterns used as relevance priors during entity adjudication, theme reclassification, and optional conversation rerun. Policy highlights:

- Theme learning scope is `relevance_prior_only`.
- Theme match does not automatically promote a candidate to canon.
- New theme use can require human approval before affecting rerun scoring (`theme_aware_rerun.require_human_approval_for_new_theme_use`).
- Approved theme labels persist in `canon/review_memory.json`.

Review and edit themes in the desktop app's Themes tab.

## Review Memory

`canon/review_memory.json` stores:

- accepted and rejected claims
- approved and rejected conversation entities
- approved aliases and entity merges
- approved theme labels
- approved card prose
- author directives
- approved card architecture actions and redirects
- story-question answers
- style corrections

Rejected claims suppress repeated bad suggestions in future Stage 09 runs. Accepted claims and approved cards are included in Stage 10 synthesis prompts.

Accepted alias claims are also stored here, so future runs can resolve later mentions of the same entity by that alias.

## Small Batch Validation

```bash
python -m pipeline.run_small_batch_validation --log-level DEBUG
```

This runs the full small-batch path and auto-decides claim drafts for smoke testing. It still requires the configured model for claim extraction and card synthesis. Because canonical promotion requires human card approval, small-batch output may contain draft cards but zero canonical cards.

## Tests

```bash
python -m unittest discover
```
