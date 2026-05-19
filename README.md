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
11. Stage 10 Identity Merge Preflight: propose identity merges from accepted claims such as "ACHILLES renames itself to RUINR" in `07_review/identity_merge_proposals.json`.
12. Review identity merge proposals in `07_review/identity_merge_decisions.json`; approved merges are persisted to `canon/review_memory.json`.
13. Stage 10 Card Synthesis: synthesize draft wiki-card revisions from all accepted claims for each touched entity, regrouping claims under approved entity merges.
14. Stage 10 Draft Notion Sync: live-sync synthesized draft cards to a Notion database for comfortable reading while keeping desktop decisions as the source of truth.
15. Review pass 2: approve/edit synthesized card drafts.
16. Stage 10 Canon Merge: write approved revisions to `07_review/canonical_cards.json`, carrying forward unchanged canonical cards.
17. Stage 11 Notion Export: export approved canonical cards and supporting records to Notion NDJSON.

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
python -m pipeline.run_from_b4 --artifacts-root "artifacts/runs/<run_folder>"
```

Start the review UI:

```bash
python -m pipeline.ui_review_app --artifacts-root "artifacts"
```

Start the native Windows app:

```bash
dist\TheriacLoreDesktop.exe
```

The desktop app opens as a normal Windows window, includes the run selector for previous CLI-generated batches, and draws the pipeline progress tracker directly on a canvas. Select `New Run` before pressing `Run Full Pipeline` to create a fresh timestamped artifact folder under `artifacts/runs/`. During claim review, use `Story Questions` for the optional guided review session. `Propose Updates` calls the configured story model; `Approve Proposal` only commits the already proposed decisions and does not make another model call.

For Stage 10 draft-card reading in Notion, add these to `.env`:

- `NOTION_ACCESS_TOKEN` or `NOTION_API_KEY`: Notion integration token.
- `NOTION_PAGE_ID` or `NOTION_DRAFT_PARENT_PAGE_ID`: parent page where the pipeline can create the draft-card database.
- Optional `NOTION_DRAFT_CARDS_DATABASE_ID`: reuse a specific existing database instead of creating one.

The Notion integration must be shared with the parent page or existing database. Stage 10 automatically writes `08_notion/notion_draft_sync_report.json`; the desktop app also has a `Sync Drafts to Notion` button for rerunning the live sync without rerunning synthesis. Existing pages are updated in place by card ID and run ID.

Build or rebuild it with:

```bash
build_desktop_app.bat
```

Start the legacy browser-based packaged app:

```bash
dist\TheriacLoreGUI.exe
```

The executable opens the review GUI in your browser, auto-selects the most recent reviewable artifact root, and falls back to the next free port if `8787` is busy. Build or rebuild it with:

```bash
build_gui_exe.bat
```

The GUI includes a run selector for any artifact root under `artifacts/` that still has pending conversation entity, claim, identity merge, or card review decisions. This lets you open older CLI-generated batches without restarting the app.

After claim review, synthesize card drafts:

```bash
python -m pipeline.stage_g_merge_engine --in-entities-json "artifacts/05_alias/resolved_entities.json" --in-claim-drafts-json "artifacts/06_drafts/card_drafts/claim_drafts.json" --in-claim-decisions-json "artifacts/07_review/claim_review_decisions.json" --in-card-review-decisions-json "artifacts/07_review/card_review_decisions.json" --in-author-directives-json "artifacts/07_review/author_directives.json" --in-review-memory-json "canon/review_memory.json" --out-card-drafts-json "artifacts/07_review/card_drafts.json" --out-cards-json "artifacts/07_review/canonical_cards.json" --out-merge-log-jsonl "artifacts/07_review/merge_log.jsonl" --in-pipeline-config-json "config/pipeline_config.json"
```

If Stage 10 reports pending identity merge proposals, review `artifacts/07_review/identity_merge_proposals.json`, save decisions to `artifacts/07_review/identity_merge_decisions.json`, and rerun the same Stage 10 command. Then approve card drafts in the UI and rerun Stage 10 to promote approved cards to canon.

Export approved canon:

```bash
python -m pipeline.stage_h_notion_export --in-cards-json "artifacts/07_review/canonical_cards.json" --in-meta-cards-json "artifacts/06_drafts/card_drafts/meta_cards_draft.json" --in-alias-json "artifacts/05_alias/alias_map.json" --in-snippets-jsonl "artifacts/03_relevance/snippets_candidates.jsonl" --in-profiles-json "artifacts/03_relevance/dm_source_profiles.json" --in-merge-log-jsonl "artifacts/07_review/merge_log.jsonl" --out-ndjson "artifacts/08_notion/notion_import.ndjson"
```

## Model Requirement

Stages 04, 05, 09, and 10 require a valid model response. If the configured provider is unavailable or returns invalid JSON, the pipeline records the failed conversation window/note or stops rather than producing low-quality fallback prose.

Configure providers in `config/pipeline_config.json`:

- `anchor_provider`: `heuristic|mixtral|hybrid` for legacy Stage 06 relevance routing when Stage 04 metadata is unavailable.
- `stage_c_anchor_provider`: defaults to `conversation_metadata`, which trusts Stage 04's model-approved conversation segments and avoids per-message model calls.
- `stage_a_anchor_provider`: `heuristic|mixtral|hybrid` for ontology seed extraction.
- `mixtral.provider`: `openrouter|gemini|anthropic|auto|mistral_api|ollama`. The setting name is historical; `openrouter` dispatches through OpenRouter's OpenAI-compatible chat API using `mixtral.api_model`.
- `mixtral.api_model`: defaults here to `qwen/qwen3.5-flash-02-23` for low-cost/high-volume batch work.
- `mixtral.adaptive_min_interval_seconds`: currently `0.5`; the runtime file will increase this automatically if rate limits appear.
- `model_routing.profiles.flash_lite`: routes cheap/high-volume work to `qwen/qwen3.5-flash-02-23`.
- `model_routing.profiles.flash_regular`: routes reasoning-sensitive Gemini-replacement work to `qwen/qwen3.5-flash-02-23`.
- `model_routing.profiles.claude_opus`: routes low-volume Claude-replacement work to `qwen/qwen3-235b-a22b-2507`.
- `model_routing.tasks.stage_b3_segmentation`: currently uses synchronous Flash-Lite so segmentation progress remains visible.
- `model_routing.tasks.stage_b4_patch_notes`: currently uses synchronous Flash-Lite for chronological conversation development notes.
- `model_routing.tasks.stage_f_claim_extraction`: currently uses synchronous Flash-Lite.
- `model_routing.tasks.stage_g_card_synthesis`: currently uses synchronous Qwen Instruct for final card drafting, with validation retries to keep synthesis stateful and evidence-bound.
- `model_routing.tasks.stage_09q_story_questions`: currently uses Qwen Instruct for iterative question generation and answer-application proposals.
- `story_questions`: controls the optional guided claim-review flow and writes `07_review/story_question_session.json`, `story_questions.jsonl`, `story_question_answers.jsonl`, `story_question_application_proposals.jsonl`, `story_question_applications.jsonl`, and `story_question_failures.json`.
- `conversation_segmentation.max_gap_hours`: coarse DM window boundary before model topic segmentation; defaults to `12`.
- `conversation_segmentation.self_user_id`: optional account override for 1:1 DM pair detection.
- API keys are read from `OPENROUTER_API_KEY`, `OPENROUTER_KEY`, `OPEN_ROUTER_API_KEY`, `GEMINI_API_KEY`, `GOOGLE_API_KEY`, `ANTHROPIC_API_KEY`, `CLAUDE_API_KEY`, `MIXTRAL_API_KEY`, `MISTRAL_API_KEY`, or `.env`.

## Review Memory

`canon/review_memory.json` stores:

- accepted and rejected claims
- approved aliases and entity merges
- approved card prose
- author directives
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
