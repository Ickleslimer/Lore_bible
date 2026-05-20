# THERIAC Lore Card Pipeline

This repo contains a stage-based pipeline for turning THERIAC Discord exports into reviewed, wiki-style lore cards.

V2 contract:

- The initial lore bible is ontology scaffolding only.
- Bootstrap output is `entity_seed.json`, not canon.
- Discord snippets become atomic claim drafts, not pasted card prose.
- Human review accepts/rejects claims first, then approves synthesized card drafts.
- `canonical_cards.json` contains only human-approved cards.
- Persistent corrections live in `canon/review_memory.json` and are reused by later runs.
- Later accepted claims revise the same entity card from the full accepted claim history, rather than appending raw text or leaving the original card untouched.
- Stage 10 can run a card-base architecture agent before drafting, so freeform structural requests become reviewable card moves, demotions, redirects, aliases, directives, and author claims.

## Flow

1. Stage 01 Entity Bootstrap: read `theriac-coda---lore-bible.docx` and write ontology seeds to `01_bootstrap/entity_seed.json`.
2. Stage 02 Message Normalization: normalize Discord exports.
3. Stage 03 Timeline Merge: merge normalized exports into a global chronology.
4. Stage 04 Relevant Conversation Segmentation: split 1:1 DMs into model-approved THERIAC-relevant conversations, bounded first by 12-hour coarse windows and then by topic shifts.
   - Stage 04 keeps emitted conversation messages non-overlapping. Duplicate or nested model spans are dropped, partial overlaps are trimmed to their non-overlapping tail, and counters are recorded in `conversation_index.json`.
   - Stage 04 also applies a strict relevance gate: the actual emitted message span must mention THERIAC or a known entity seed/alias. Model-inferred inspiration without an explicit project/entity tie is dropped and counted in `model_segments_dropped_by_relevance`.
5. Stage 05 Conversation Patch Notes: draft chronological conversation patch notes in global timestamp order, writing `conversation_patch_notes.json` and `.jsonl`.
   - Stage 05 treats each 1:1 conversation as a development note for the lore/meta state of the project. It can see a rolling window of earlier patch notes only, so repeated explanations to different team members reinforce earlier developments instead of creating artificial contradictions.
6. Stage 06 Snippet Extraction: extract THERIAC-relevant snippets from `messages_relevant_conversations.jsonl`, using `conversation_id` as the context boundary, Stage 04's relevance metadata, and Stage 05's conversation patch-note context instead of a per-message model pass.
7. Stage 07 Entity Resolution: resolve entities, aliases, acronyms, and duplicate seed names, then promote only entities text-observed in the current snippets into `05_alias/resolved_entities.json`; bootstrap-only matches stay under `seed_only_entities`.
   - Text-observed conversation anchors that do not match the bootstrap ontology are written to `05_alias/conversation_entity_proposals.json`.
   - Conversation entity proposals aggregate type evidence across snippets, so later usage that clashes with the initial designation can revise `proposed_entity_type` and surface type conflicts for review.
   - Pending proposals stop the run before grouping/drafting, so new entities can be reviewed before they become `unmapped`.
   - Approved proposals in `05_alias/conversation_entity_decisions.json` become `conversation_candidate_approved` entities on rerun; approved/rejected decisions are persisted to `canon/review_memory.json` for future runs.
8. Stage 08 Snippet Grouping: group snippets against resolved and approved conversation-born entities.
9. Stage 09 Claim Drafting: run model-required claim extraction to `06_drafts/card_drafts/claim_drafts.json`.
10. Review pass 1: accept/reject/edit conversation entity proposals and atomic claims in the UI. Optionally use Story Questions in the desktop app to ask one high-value author question at a time, ask the configured story model to propose claim decisions and author claims from the answer, approve/discard/critique that proposal, then generate the next question from the reduced unresolved claim list.
11. Stage 10 Identity Cluster Preflight: detect pairwise identity evidence from accepted claims, collapse it into connected entity clusters, and ask the configured model to suggest one canonical wiki-page title per cluster in `07_review/identity_merge_proposals.json`.
12. Review identity cluster proposals in `07_review/identity_merge_decisions.json`; approved clusters are expanded into deterministic entity merges and persisted to `canon/review_memory.json`.
13. Stage 10A Card Architecture Agent: read accepted claims, author claims, source snippets, patch notes, existing cards, review memory, and freeform requests from `07_review/card_edit_requests.jsonl`; write proposed actions to `07_review/card_architecture_proposals.json`.
14. Stage 10B Card Architecture Review/Application: approve or reject proposed actions in the desktop app. Approved demotions, redirects, claim moves, aliases, directives, and author claims are applied as an overlay and recorded in `card_architecture_applied.json`, `card_redirects.json`, and `canon/review_memory.json`.
15. Stage 10C Card Synthesis: synthesize draft wiki-card revisions from all accepted claims for each final architecture work item, regrouping claims under approved entity merges and card architecture decisions.
16. Stage 10 Draft Notion Sync: live-sync synthesized draft cards to a Notion database for comfortable reading while keeping desktop decisions as the source of truth.
17. Review pass 2: approve/edit synthesized card drafts.
18. Stage 10 Canon Merge: write approved revisions to `07_review/canonical_cards.json`, carrying forward unchanged canonical cards.
19. Stage 11 Notion Export: export approved canonical cards and supporting records to Notion NDJSON.

## Install

```bash
python -m pip install -r requirements.txt
```

## Quickstart

Generate claim drafts:

```bash
python -m pipeline.run_pipeline --docx "theriac-coda---lore-bible.docx" --conversations-root "discord_conversations" --artifacts-root "artifacts"
```

Resume an existing run after completed Stage 04 segmentation, regenerating Stage 05-09 artifacts without rerunning normalization or segmentation:

```bash
python -m pipeline.run_from_stage_05 --artifacts-root "artifacts/runs/<run_folder>"
```

Start the desktop review app:

```bash
desktop-tauri\src-tauri\target\release\theriac-lore-tauri.exe
```

The Tauri/Svelte desktop app is the primary review surface. It includes the run selector for previous CLI-generated batches, pipeline run controls, cancellation, progress tracking, identity-cluster review, and candidate browsing. Select `New Run` before starting a full run to create a fresh timestamped artifact folder under `artifacts/runs/`. During claim review, use `Story Questions` for the optional guided review session. `Propose Updates` calls the configured story model; `Approve Proposal` only commits the already proposed decisions and does not make another model call. Use `Card Agent` for freeform card-base commissions such as moving a standalone draft into another card section; resume Stage 11 to generate proposed architecture actions, then approve/reject those actions before card writing.

For Stage 11 draft-card reading in Notion, add these to `.env`:

- `NOTION_ACCESS_TOKEN` or `NOTION_API_KEY`: Notion integration token.
- `NOTION_PAGE_ID` or `NOTION_DRAFT_PARENT_PAGE_ID`: parent page where the pipeline can create the draft-card database.
- Optional `NOTION_DRAFT_CARDS_DATABASE_ID`: reuse a specific existing database instead of creating one.

The Notion integration must be shared with the parent page or existing database. Stage 11 automatically writes `08_notion/notion_draft_sync_report.json`; the desktop app also has a `Sync Drafts to Notion` button for rerunning the live sync without rerunning synthesis. Existing pages are updated in place by card ID and run ID.

Start the new Tauri/Svelte desktop app during development:

```bash
cd desktop-tauri
npm install
npm run dev
```

Build the Tauri/Svelte app:

```bash
build_tauri_app.bat
```

The Tauri app uses a Svelte UI and a small Python JSON bridge (`pipeline.tauri_bridge`) so the pipeline remains the source of truth. Building Tauri on Windows requires the Rust stable toolchain and Microsoft Visual C++ Build Tools (`link.exe`); `build_tauri_app.bat` loads the Visual Studio developer environment automatically when Build Tools are installed.

After claim review, generate identity merge proposals:

```bash
python -m pipeline.stage_10_identity_merge --in-entities-json "artifacts/05_alias/resolved_entities.json" --in-claim-drafts-json "artifacts/06_drafts/card_drafts/claim_drafts.json" --in-claim-decisions-json "artifacts/07_review/claim_review_decisions.json" --in-review-memory-json "canon/review_memory.json" --out-identity-merge-proposals-json "artifacts/07_review/identity_merge_proposals.json" --in-identity-merge-decisions-json "artifacts/07_review/identity_merge_decisions.json" --in-pipeline-config-json "config/pipeline_config.json"
```

If Stage 10 reports pending identity cluster proposals, review `artifacts/07_review/identity_merge_proposals.json`, optionally edit the canonical name in the GUI, save decisions to `artifacts/07_review/identity_merge_decisions.json`, and rerun Stage 10. Then synthesize card drafts:

```bash
python -m pipeline.stage_11_card_synthesis --in-entities-json "artifacts/05_alias/resolved_entities.json" --in-claim-drafts-json "artifacts/06_drafts/card_drafts/claim_drafts.json" --in-claim-decisions-json "artifacts/07_review/claim_review_decisions.json" --in-card-review-decisions-json "artifacts/07_review/card_review_decisions.json" --in-author-directives-json "artifacts/07_review/author_directives.json" --in-review-memory-json "canon/review_memory.json" --out-card-drafts-json "artifacts/07_review/card_drafts.json" --out-cards-json "artifacts/07_review/canonical_cards.json" --out-merge-log-jsonl "artifacts/07_review/merge_log.jsonl" --in-pipeline-config-json "config/pipeline_config.json"
```

If Stage 11 reports pending card architecture proposals, review `artifacts/07_review/card_architecture_proposals.json`, save decisions to `artifacts/07_review/card_architecture_decisions.json`, and rerun Stage 11. Then approve card drafts in the UI and rerun Stage 11 to promote approved cards to canon.

Export approved canon:

```bash
python -m pipeline.stage_12_notion_export --in-cards-json "artifacts/07_review/canonical_cards.json" --in-meta-cards-json "artifacts/06_drafts/card_drafts/meta_cards_draft.json" --in-alias-json "artifacts/05_alias/alias_map.json" --in-snippets-jsonl "artifacts/03_relevance/snippets_candidates.jsonl" --in-profiles-json "artifacts/03_relevance/dm_source_profiles.json" --in-merge-log-jsonl "artifacts/07_review/merge_log.jsonl" --out-ndjson "artifacts/08_notion/notion_import.ndjson"
```

## Model Requirement

Stages 04, 05, 09, 10, and 11 require a valid model response for their model-backed paths. If the configured provider is unavailable or returns invalid JSON, the pipeline records the failed conversation window/note or stops rather than producing low-quality fallback prose.

Configure providers in `config/pipeline_config.json`:

- `anchor_provider`: `heuristic|model|hybrid` for legacy Stage 06 relevance routing when Stage 04 metadata is unavailable.
- `stage_06_anchor_provider`: defaults to `conversation_metadata`, which trusts Stage 04's model-approved conversation segments and avoids per-message model calls.
- `stage_01_anchor_provider`: `heuristic|model|hybrid` for ontology seed extraction.
- `model_provider.provider`: `openrouter|gemini|anthropic|auto|openai_compatible|api|ollama`. `openrouter` and `openai_compatible` dispatch through OpenAI-compatible chat APIs using `model_provider.api_model`.
- `model_provider.api_model`: defaults here to `qwen/qwen3.5-flash-02-23` for low-cost/high-volume batch work.
- `model_provider.adaptive_min_interval_seconds`: currently `0.5`; the runtime file will increase this automatically if rate limits appear.
- `model_routing.profiles.high_volume`: routes cheap/high-volume work to `qwen/qwen3.5-flash-02-23`.
- `model_routing.profiles.balanced_reasoning`: routes reasoning-sensitive work to `qwen/qwen3.5-flash-02-23`.
- `model_routing.profiles.premium_reasoning`: routes low-volume work to the configured premium reasoning model.
- `model_routing.profiles.deep_reasoning`: routes low-volume reasoning work to the configured deep reasoning model.
- `model_routing.tasks.stage_04_conversation_segmentation`: currently uses synchronous high-volume profile so segmentation progress remains visible.
- `model_routing.tasks.stage_05_conversation_patch_notes`: currently uses synchronous high-volume profile for chronological conversation development notes.
- `model_routing.tasks.stage_09_claim_drafting`: currently uses synchronous high-volume profile.
- `model_routing.tasks.stage_10_identity_merge_proposals`: currently uses the deep reasoning profile for identity/alias merge proposal detection.
- `model_routing.tasks.stage_10_identity_merge_cluster_judgement`: currently uses the deep reasoning profile to choose canonical names and aliases for collated identity clusters.
- `model_routing.tasks.stage_11_card_architecture_agent`: currently uses the deep reasoning profile for low-volume structural card-base reasoning before final drafting.
- `model_routing.tasks.stage_11_card_synthesis`: currently uses the deep reasoning profile for final card drafting, with validation retries to keep synthesis stateful and evidence-bound.
- `model_routing.tasks.stage_09_story_questions`: currently uses the deep reasoning profile for iterative question generation and answer-application proposals.
- `story_questions`: controls the optional guided claim-review flow and writes `07_review/story_question_session.json`, `story_questions.jsonl`, `story_question_answers.jsonl`, `story_question_application_proposals.jsonl`, `story_question_applications.jsonl`, and `story_question_failures.json`.
- `conversation_segmentation.max_gap_hours`: coarse DM window boundary before model topic segmentation; defaults to `12`.
- `conversation_segmentation.self_user_id`: optional account override for 1:1 DM pair detection.
- API keys are read from `OPENROUTER_API_KEY`, `OPENROUTER_KEY`, `OPEN_ROUTER_API_KEY`, `GEMINI_API_KEY`, `GOOGLE_API_KEY`, `ANTHROPIC_API_KEY`, `CLAUDE_API_KEY`, `MODEL_API_KEY`, `MODEL_PROVIDER_API_KEY`, `OPENAI_COMPATIBLE_API_KEY`, or `.env`.

## Review Memory

`canon/review_memory.json` stores:

- accepted and rejected claims
- approved aliases and entity merges
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
