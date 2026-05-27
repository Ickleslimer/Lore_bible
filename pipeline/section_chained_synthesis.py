"""Section-chained card synthesis: digest → per-section writers → merge."""

from __future__ import annotations

import json
import time
from typing import Any

from pipeline.card_first_review import (
    build_entity_evidence_bundle,
    card_first_synthesis_config,
    normalize_synthesis_support_ids,
    protagonist_tier_config,
    section_word_targets_for_entity,
    tier1_snippet_pool_for_entity,
    valid_support_id_sets,
)
from pipeline.card_sections import (
    CARD_SECTION_KEYS,
    PATH_BRANCH_CONTEXT,
    PEACEFUL_LEDE_RULE,
    card_section_display_title,
    normalize_card_sections,
    section_write_order,
)
from pipeline.narrative_works import (
    filter_snippet_ids_by_narrative_work,
    narrative_work_for_history_section,
)
from pipeline.common import get_logger
from pipeline.model_provider import call_model_chat, get_model_runtime_status, model_call_kwargs


def _stage_11():
    import pipeline.stage_11_card_synthesis as stage_11

    return stage_11

_STORY_EMPHASIS_RULES = """
Story emphasis (protagonist / major character cards):
- Foreground what defines the character in play: personality, suffering, relationships, path splits, augmentation, death—not lab paperwork or one-off administrative beats.
- Incidental facts (e.g. approving another entity's project specs, embezzled funding) may appear at most ONCE in the whole card, usually in background—not in the summary, and not repeated in relationships with similar wording.
- Do not restate the same fact across summary, background, role_in_story, and relationships; choose the single best section.
- Prefer depth on recurring themes over scattering minor plot tokens into every section.
"""


def _protagonist_emphasis_block(word_targets: dict[str, Any]) -> str:
    tier = str((word_targets or {}).get("synthesis_tier", "")).strip().lower()
    if tier in {"protagonist", "developed"}:
        return _STORY_EMPHASIS_RULES
    return ""

SECTION_SNIPPET_HINTS: dict[str, tuple[str, ...]] = {
    "summary": ("character", "protagonist", "lead", "commander", "founder", "engineer", "scientist", "role"),
    "background": (
        "before",
        "childhood",
        "military",
        "cryo",
        "cryogenic",
        "moratorium",
        "history",
        "born",
        "early",
        "pre",
        "backstory",
        "recruited",
    ),
    "role_in_story": (
        "path",
        "ending",
        "quest",
        "mission",
        "player",
        "gameplay",
        "route",
        "choice",
        "olympus",
        "lab",
        "sequence",
    ),
    "relationships": (
        "relationship",
        "wife",
        "husband",
        "son",
        "daughter",
        "friend",
        "ally",
        "enemy",
        "khava",
        "joy",
        "krypteia",
        "loves",
        "conflict",
    ),
    "history_theriac_coda": (
        "path b",
        "peaceful",
        "main route",
        "main path",
        "side with the lab",
        "lab route",
        "40 hour",
        "quest",
        "romance",
        "cryo",
        "theriac",
    ),
    "history_path_a_side_route": (
        "path a",
        "destructive",
        "side route",
        "against the lab",
        "execute",
        "cyberpsych",
        "olympus",
        "assault",
        "ruinr",
        "gore",
        "6 hour",
    ),
    "inspirations": (
        "inspir",
        "biblical",
        "book of enoch",
        "reference",
        "based on",
        "named after",
        "theme",
        "meta",
        "design",
    ),
}


def _shared_context_block(
    entity: dict[str, Any],
    claims: list[dict[str, Any]],
    memory_for_entity: dict[str, Any],
    evidence_bundle: dict[str, Any],
    source_evidence_rows: list[dict[str, Any]],
    entity_development_history_lines: list[str] | None,
    wiki_link_rows: list[dict[str, Any]],
    word_targets: dict[str, Any],
    *,
    narrative_frame: dict[str, Any] | None = None,
) -> str:
    stage_11 = _stage_11()
    development_history_block = "\n".join(entity_development_history_lines or []) or "none"
    synthesis_entity = stage_11.entity_payload_for_card_synthesis(entity)
    prompt_claims = [stage_11.claim_payload_for_card_synthesis(claim, entity) for claim in claims]
    synthesis_memory = stage_11.memory_payload_for_card_synthesis(memory_for_entity, entity)
    frame_block = json.dumps(narrative_frame, ensure_ascii=False, indent=2) if narrative_frame else "none"
    return f"""Entity development history (chronological context only; do not copy verbatim):
{development_history_block}

Narrative work frame (Theriac Coda work page — use for route scope; do not contradict):
{frame_block}

Entity:
{json.dumps(synthesis_entity, ensure_ascii=False, indent=2)}

Approved entity evidence bundle:
{json.dumps(evidence_bundle, ensure_ascii=False, indent=2)}

Accepted claims (guardrails):
{json.dumps(prompt_claims, ensure_ascii=False, indent=2)}

Word target plan:
{json.dumps(word_targets, ensure_ascii=False, indent=2)}

Available wiki link targets:
{json.dumps(wiki_link_rows, ensure_ascii=False, indent=2)}

Relevant review memory:
{json.dumps(synthesis_memory, ensure_ascii=False, indent=2)}
"""


def snippet_ids_for_section(
    section_key: str,
    ranked_snippet_ids: list[str],
    source_snippets_by_id: dict[str, dict[str, Any]],
    *,
    limit: int,
    narrative_work_tags: dict[str, str] | None = None,
) -> list[str]:
    work_id = narrative_work_for_history_section(section_key)
    if work_id and narrative_work_tags:
        ranked_snippet_ids = filter_snippet_ids_by_narrative_work(
            ranked_snippet_ids,
            work_id,
            narrative_work_tags,
            include_untagged=section_key == "history_theriac_coda",
        )
    hints = SECTION_SNIPPET_HINTS.get(section_key, ())
    scored: list[tuple[int, int, str]] = []
    for index, snippet_id in enumerate(ranked_snippet_ids):
        snippet = source_snippets_by_id.get(snippet_id)
        if not isinstance(snippet, dict):
            continue
        text = " ".join(
            [
                str(snippet.get("display_text_normalized", "")),
                str(snippet.get("conversation_patch_summary", "")),
                " ".join(str(item) for item in snippet.get("conversation_patch_lore_developments", []) or []),
            ]
        ).lower()
        score = sum(1 for hint in hints if hint in text)
        scored.append((-score, index, snippet_id))
    scored.sort()
    chosen = [snippet_id for _, _, snippet_id in scored[:limit]]
    if len(chosen) < min(limit, len(ranked_snippet_ids)):
        for snippet_id in ranked_snippet_ids:
            if snippet_id not in chosen:
                chosen.append(snippet_id)
            if len(chosen) >= limit:
                break
    return chosen[:limit]


def build_digest_prompt(
    entity: dict[str, Any],
    claims: list[dict[str, Any]],
    memory_for_entity: dict[str, Any],
    evidence_bundle: dict[str, Any],
    digest_snippet_rows: list[dict[str, Any]],
    entity_development_history_lines: list[str] | None,
    wiki_link_rows: list[dict[str, Any]],
    word_targets: dict[str, Any],
    validation_feedback: str = "",
    *,
    narrative_frame: dict[str, Any] | None = None,
) -> str:
    shared = _shared_context_block(
        entity,
        claims,
        memory_for_entity,
        evidence_bundle,
        digest_snippet_rows,
        entity_development_history_lines,
        wiki_link_rows,
        word_targets,
        narrative_frame=narrative_frame,
    )
    return f"""You are the research pass for a Theriac fandom-style wiki card. Read the evidence and return strict JSON only.
Do not write wiki prose. Produce a structured lore digest and section outline for downstream section writers.

Rules:
- Use approved lore snippets and entity development history as primary evidence; claims are guardrails.
- biography_beats must follow in-world chronology (earliest life/story events first): pre-lab backstory → moratorium/cryo/military era → lab founding and era. Do not start with naming debates or biblical/meta framing.
- path_b_beats: main-route events after the player sides with the lab (Path B, peaceful, ~40+ hours)—chronological.
- path_a_beats: destructive side-route events if the player follows orders against the lab (Path A, ~6 hours)—chronological; keep separate from path_b_beats.
- Do not mix Path A and Path B events in the same beat list.
- Each beat and timeline row must cite snippet_ids and/or claim_ids from the bundle.
- Put naming-origin, biblical parallels, anime-title, Book of Enoch, external media comparisons, and design-process notes in meta_inspirations only—not in biography_beats or timeline_chronology.
- meta_inspirations notes are factual citations only (what was named after what, what media was referenced)—not praise or analysis of whether those choices "work" or feel resonant.
- section_outline.summary must describe diegetic role only (who they are in Theriac, what they do)—no biblical references, no "formerly known as", no inspiration comparisons.
- section_outline.background covers pre-game biography; section_outline.history_theriac_coda and history_path_a_side_route briefs list route-specific beats only.
- section_outline.summary must be peaceful-path (Path B) lede only—no Path A plot beats.
- Rank beats by story centrality; mark bureaucratic side beats (project approvals, funding mechanics) as low priority—omit from section_outline.summary.
{PATH_BRANCH_CONTEXT}
{PEACEFUL_LEDE_RULE}
{_protagonist_emphasis_block(word_targets)}

{shared}

Digest source snippets (ranked tier-1 sample):
{json.dumps(digest_snippet_rows, ensure_ascii=False, indent=2)}

Previous rejection to fix:
{validation_feedback or "none"}

Return JSON:
{{
  "biography_beats": [
    {{"order": 1, "beat": "", "snippet_ids": ["snippet_id"], "claim_ids": ["claim_id"]}}
  ],
  "relationship_beats": [
    {{"name": "", "relation_type": "", "note": "", "snippet_ids": [], "claim_ids": []}}
  ],
  "path_b_beats": [
    {{"order": 1, "beat": "", "snippet_ids": [], "claim_ids": []}}
  ],
  "path_a_beats": [
    {{"order": 1, "beat": "", "snippet_ids": [], "claim_ids": []}}
  ],
  "meta_inspirations": [
    {{"note": "", "snippet_ids": [], "claim_ids": []}}
  ],
  "open_questions": [
    {{"question": "", "snippet_ids": [], "claim_ids": []}}
  ],
  "section_outline": {{
    "summary": "",
    "background": "",
    "role_in_story": "",
    "relationships": "",
    "history_theriac_coda": "",
    "history_path_a_side_route": "",
    "inspirations": ""
  }}
}}
"""


def build_section_writer_prompt(
    section_key: str,
    entity: dict[str, Any],
    claims: list[dict[str, Any]],
    memory_for_entity: dict[str, Any],
    evidence_bundle: dict[str, Any],
    digest: dict[str, Any],
    section_snippet_rows: list[dict[str, Any]],
    prior_sections: dict[str, str],
    entity_development_history_lines: list[str] | None,
    wiki_link_rows: list[dict[str, Any]],
    word_targets: dict[str, Any],
    validation_feedback: str = "",
    *,
    narrative_frame: dict[str, Any] | None = None,
) -> str:
    section_target = (word_targets.get("section_word_targets", {}) or {}).get(section_key, "")
    shared = _shared_context_block(
        entity,
        claims,
        memory_for_entity,
        evidence_bundle,
        section_snippet_rows,
        entity_development_history_lines,
        wiki_link_rows,
        word_targets,
        narrative_frame=narrative_frame,
    )
    prior_block = json.dumps(prior_sections, ensure_ascii=False, indent=2) if prior_sections else "{}"
    history_block = "\n".join(entity_development_history_lines or []) or "none"
    canonical = str(entity.get("canonical_name", "this entity")).strip() or "this entity"
    section_rules = ""
    emphasis = _protagonist_emphasis_block(word_targets)
    if section_key == "summary":
        section_rules = f"""
Summary-specific rules (strict):
- Peaceful-path lede only: who {canonical} is on the expected Path B / main route (lab siding, ~40+ hour arc)—not Path A violence or dual-ending contrast.
- Diegetic lead only: story function and why they matter on the default playthrough.
- Lead with personality, emotional arc drivers, key relationships—not administrative side beats (project approvals, funding paperwork, org bureaucracy).
- Do NOT mention biblical patriarchs, Book of Enoch, anime-title brainstorming, "formerly known as", working titles, or external media (Cyberpunk, Evangelion, etc.).
- Do NOT list inspirations or naming history; one short clause on role is enough.
- Use the canonical name only; no alias glossary.
{PEACEFUL_LEDE_RULE}{emphasis}"""
    elif section_key == "background":
        section_rules = f"""
Background-specific rules:
- Chronological pre-game biography: military/cryo/moratorium → lab era → key relationships as they formed.
- Do not open with naming-origin or biblical theme; if renames matter, one brief clause mid-section only.
- Keep meta/design out of this section.
- Incidental administrative facts belong here at most once, mid-article—not as the opening hook.{emphasis}"""
    elif section_key == "history_theriac_coda":
        section_rules = f"""
Theriac Coda main-route history rules:
- Cover only events on the peaceful/main route after the player sides with the lab (~40+ hours of story).
- Use digest.path_b_beats; chronological prose; no Path A violence, cyberpsychotic assault, or execute-lab orders here.
- This is the expected player route—not a minor footnote.{PATH_BRANCH_CONTEXT}{emphasis}"""
    elif section_key == "history_path_a_side_route":
        section_rules = f"""
Path A side-route history rules (optional section—omit if digest.path_a_beats empty):
- Cover only the shorter destructive branch (~6 hours) when the player follows orders against the lab.
- Use digest.path_a_beats; include augmentation, assault, and death beats if evidenced—do not spill Path B main-quest progression here.
- Same treatment as spin-off history blocks—not part of the summary lede.{PATH_BRANCH_CONTEXT}{emphasis}"""
    elif section_key == "relationships":
        section_rules = f"""
Relationships-specific rules:
- Focus on bonds, dynamics, and story function with named characters—chess mentorship, marriage, lab trust, romance, rivalry.
- Do not repeat background biography beats (military service, moratorium, project approvals) unless essential to explain a relationship.
- Never restate administrative facts already suitable for background (e.g. approving another project's specs).{emphasis}"""
    elif section_key == "role_in_story":
        section_rules = f"""
Role-in-story rules:
- Pre-branch framing only: who {canonical} is in the lab before the choice, how the player encounters them, and the ~1-hour branch (side with lab vs follow orders).
- Do NOT narrate full Path A or Path B walkthroughs here—those belong in history_theriac_coda and history_path_a_side_route.
- At most one short clause that a destructive alternate route exists; no assault/cyberpsychosis detail.
- Do not pad with bureaucratic lore.{PATH_BRANCH_CONTEXT}{emphasis}"""
    elif section_key == "inspirations":
        section_rules = """
Inspirations-specific rules:
- Out-of-world/meta only: biblical naming, Book of Enoch, anime-title notes, external character comparisons, author design process.
- Do not repeat biography or plot beats already in background/role_in_story.
- Neutral behind-the-scenes tone only: report what sources or parallels the evidence names (e.g. "named after the biblical patriarch Enoch").
- Privacy: Do NOT include, summarize, or allude to the author's personal life or personal experiences (anything "on a personal level", "in my life", etc.), even if present in evidence. Avoid first-person autobiographical phrasing (I/my/me).
- Do not praise, evaluate, or explain why a reference "works" for the character—no resonant/fitting/apt/clever/intentional/thematic masterstroke language unless evidence quotes that judgment verbatim.
- Do not write as a critic reviewing the author's prose; cite parallels and naming facts, then stop."""
    extra_arrays = ""
    if section_key == "relationships":
        extra_arrays = """
  "relationships": [
    {"target_entity_name": "", "relation_type": "", "note": "", "support_claim_ids": ["snippet_id or claim_id"]}
  ],"""
    elif section_key == "summary":
        extra_arrays = """
  "wiki_links": [
    {"target_card_id": "", "target_entity_name": "", "relation_type": "", "section": "summary", "support_claim_ids": []}
  ],"""
    return f"""Write ONE section of a Theriac fandom-style wiki card. Return strict JSON only.

Section to write: {section_key}
Per-section word target: {section_target}

Rules:
- Follow the lore digest and section_outline; do not contradict prior sections.
- Write polished article prose for this section only (field `prose`). No bullet glossary.
- Cite snippet_* IDs and/or claim IDs in support_ids for every non-empty prose block.
- Do not list alias harvests or former names in prose (except one brief rename note in background if essential).
- Meta/design/biblical/inspiration content belongs only in inspirations, never in summary.
- Inspirations prose must be factual citation, not editorial commentary on creative choices.
- Avoid repeating facts already stated in other sections; do not use this pass to restate plot beats.
- Avoid speculative filler words unless evidence uses them.
{_protagonist_emphasis_block(word_targets)}
- If evidence does not support this section, return empty prose and empty support_ids.
{section_rules}

Prior sections already written (do not repeat their content):
{prior_block}

Lore digest:
{json.dumps(digest, ensure_ascii=False, indent=2)}

Section-specific source snippets:
{json.dumps(section_snippet_rows, ensure_ascii=False, indent=2)}

{shared}

Previous rejection to fix:
{validation_feedback or "none"}

Return JSON:
{{
  "section_key": "{section_key}",
  "prose": "",
  "support_ids": [],{extra_arrays}
}}
"""


def build_merge_prompt(
    entity: dict[str, Any],
    claims: list[dict[str, Any]],
    memory_for_entity: dict[str, Any],
    evidence_bundle: dict[str, Any],
    digest: dict[str, Any],
    section_drafts: dict[str, dict[str, Any]],
    entity_development_history_lines: list[str] | None,
    wiki_link_rows: list[dict[str, Any]],
    word_targets: dict[str, Any],
    validation_feedback: str = "",
    *,
    narrative_frame: dict[str, Any] | None = None,
) -> str:
    shared = _shared_context_block(
        entity,
        claims,
        memory_for_entity,
        evidence_bundle,
        [],
        entity_development_history_lines,
        wiki_link_rows,
        word_targets,
        narrative_frame=narrative_frame,
    )
    return f"""Merge section drafts into one Theriac wiki card JSON. Return strict JSON only.

Tasks:
- Unify voice and remove repetition across sections (this is mandatory, not optional).
- Do not shorten the article: preserve most of the combined length from section drafts (trim repetition only, do not compress into a summary-style card).
- Deduplicate facts: if multiple drafts mention the same beat (e.g. approving RUINR specs), keep ONE mention in the best section (usually background)—delete the others, especially from summary and relationships.
- Enforce path isolation: Path B events only in sections.history_theriac_coda; Path A events only in sections.history_path_a_side_route; delete cross-route bleed.
- Rewrite summary as peaceful-path (Path B) lede only; strip Path A markers from summary.
{PATH_BRANCH_CONTEXT}
{PEACEFUL_LEDE_RULE}
- Demote incidental administrative beats; never let them dominate the summary or appear with near-identical wording twice.
{_STORY_EMPHASIS_RULES}
- Keep facts from section drafts; do not invent new facts.
- Build support_map from each section's support_ids (snippet_* and claim IDs). Every non-empty section must copy ALL support_ids from the matching section draft into support_map (do not leave background/role_in_story/relationships empty if the draft had support_ids).
- Merge relationships and wiki_links from section drafts without duplicates. Do not add a top-level timeline array (route chronology lives in path sections).
- Leave open_questions empty unless digest or claims explicitly state uncertainty.

Lead/summary hygiene (strict):
- Rewrite summary as a diegetic lede: in-world identity, emotional arc, key relationships, path outcomes at a high level—no project-approval or funding paperwork in the lead.
- Strip from summary: biblical references, Book of Enoch, anime-title/name-origin discussion, "formerly known as", external media comparisons, and design/meta commentary.
- Move any removed naming or inspiration material into sections.inspirations (or trim if already there).
- Rewrite sections.inspirations to neutral behind-the-scenes facts only; strip editorial praise (resonant, fitting, apt, clever choice, underscores, etc.) unless quoted verbatim from evidence.
- Do not let sections.background open with naming/theme; prefer chronological biography if the draft does.
- Never use inference words (possibly, may, might, suggests, implies) unless they appear verbatim in cited evidence.

Lore digest:
{json.dumps(digest, ensure_ascii=False, indent=2)}

Section drafts:
{json.dumps(section_drafts, ensure_ascii=False, indent=2)}

{shared}

Previous rejection to fix:
{validation_feedback or "none"}

Return JSON object:
{{
  "summary": "",
  "sections": {{
    "background": "",
    "role_in_story": "",
    "relationships": "",
    "history_theriac_coda": "",
    "history_path_a_side_route": "",
    "inspirations": "",
    "open_questions": ""
  }},
  "relationships": [],
  "wiki_links": [],
  "resolved_conflicts": [],
  "unresolved_conflicts": [],
  "support_map": {{
    "summary": [],
    "background": [],
    "role_in_story": [],
    "relationships": [],
    "history_theriac_coda": [],
    "history_path_a_side_route": [],
    "inspirations": [],
    "open_questions": [],
    "resolved_conflicts": [],
    "unresolved_conflicts": []
  }}
}}
"""


def _call_model_json(
    prompt: str,
    config: dict[str, Any],
    task_label: str,
    *,
    entity_name: str = "",
) -> dict[str, Any] | None:
    logger = get_logger(__name__)
    label = f"{entity_name}:{task_label}" if entity_name else task_label
    logger.info("Section-chained [%s] model call starting.", label)
    started = time.monotonic()
    call_kwargs = model_call_kwargs(config, "stage_11_card_synthesis")
    response = call_model_chat(prompt=prompt, **call_kwargs)
    elapsed = time.monotonic() - started
    if response is None:
        status = get_model_runtime_status()
        reason = str(status.get("last_model_skip_reason") or "provider_unavailable")
        logger.warning("Section-chained [%s] no model response after %.1fs (%s).", label, elapsed, reason)
        return None
    if not isinstance(response, dict):
        logger.warning("Section-chained [%s] invalid JSON payload after %.1fs.", label, elapsed)
        return None
    logger.info("Section-chained [%s] model call finished in %.1fs.", label, elapsed)
    return response


def _build_source_evidence_rows(
    entity: dict[str, Any],
    claims: list[dict[str, Any]],
    snippet_ids: list[str],
    source_snippets_by_id: dict[str, dict[str, Any]],
    global_alias_pairs: list[tuple[str, str]] | None = None,
) -> list[dict[str, Any]]:
    stage_11 = _stage_11()
    canonical_name = str(entity.get("canonical_name", "")).strip()
    alias_terms = stage_11.entity_alias_terms_for_normalization(entity)
    rows = stage_11.build_synthesis_source_evidence_rows(
        claims,
        source_snippets_by_id,
        entity=entity,
        canonical_name=canonical_name,
        alias_terms=alias_terms,
        global_alias_pairs=global_alias_pairs,
    )
    return stage_11.merge_synthesis_evidence_rows(
        rows,
        source_snippets_by_id,
        snippet_ids,
        entity=entity,
        canonical_name=canonical_name,
        alias_terms=alias_terms,
        global_alias_pairs=global_alias_pairs,
        max_rows=len(snippet_ids),
    )


def _assemble_section_drafts(
    section_results: dict[str, dict[str, Any]],
    *,
    write_order: list[str] | None = None,
) -> dict[str, dict[str, Any]]:
    assembled: dict[str, dict[str, Any]] = {}
    for section_key in write_order or section_write_order():
        payload = section_results.get(section_key)
        if isinstance(payload, dict):
            assembled[section_key] = payload
    return assembled


def _backfill_support_map_from_section_drafts(
    merged: dict[str, Any],
    section_results: dict[str, dict[str, Any]],
) -> None:
    support_map = merged.get("support_map")
    if not isinstance(support_map, dict):
        support_map = {}
        merged["support_map"] = support_map

    for field_name in ["summary", *CARD_SECTION_KEYS]:
        draft = section_results.get(field_name, {})
        if not isinstance(draft, dict):
            continue
        draft_ids = [str(item).strip() for item in draft.get("support_ids", []) or [] if str(item).strip()]
        if not draft_ids:
            continue
        existing = support_map.get(field_name)
        if not isinstance(existing, list) or not existing:
            support_map[field_name] = list(draft_ids)
            continue
        seen = {str(item).strip() for item in existing}
        for token in draft_ids:
            if token not in seen:
                existing.append(token)
                seen.add(token)


def _merge_section_results_into_synthesis(merged: dict[str, Any], section_results: dict[str, dict[str, Any]]) -> None:
    sections = merged.setdefault("sections", {})
    if not isinstance(sections, dict):
        sections = {}
        merged["sections"] = sections
    summary_text = str(merged.get("summary", "")).strip()
    if not summary_text:
        summary_draft = section_results.get("summary", {})
        if isinstance(summary_draft, dict):
            summary_text = str(summary_draft.get("prose", "")).strip()
            merged["summary"] = summary_text
    for section_key in CARD_SECTION_KEYS:
        draft = section_results.get(section_key, {})
        if not isinstance(draft, dict):
            continue
        draft_prose = str(draft.get("prose", "")).strip()
        if draft_prose and not str(sections.get(section_key, "")).strip():
            sections[section_key] = draft_prose
    relationships_draft = section_results.get("relationships", {})
    if isinstance(relationships_draft, dict):
        rel_items = relationships_draft.get("relationships")
        if isinstance(rel_items, list) and rel_items and not merged.get("relationships"):
            merged["relationships"] = rel_items


def synthesize_card_section_chained(
    entity: dict[str, Any],
    claims: list[dict[str, Any]],
    memory_for_entity: dict[str, Any],
    config: dict[str, Any],
    source_snippets_by_id: dict[str, dict[str, Any]] | None = None,
    entities_by_name: dict[str, dict[str, Any]] | None = None,
    entity_development_history_lines: list[str] | None = None,
    lore_clusters: list[dict[str, Any]] | None = None,
    global_alias_pairs: list[tuple[str, str]] | None = None,
    *,
    narrative_work_tags: dict[str, str] | None = None,
    narrative_frame: dict[str, Any] | None = None,
) -> dict[str, Any]:
    logger = get_logger(__name__)
    snippets_by_id = source_snippets_by_id or {}
    cfg = card_first_synthesis_config(config)
    tier_cfg = protagonist_tier_config(cfg)
    claim_snippet_ids = [sid for claim in claims for sid in (claim.get("source_snippet_ids") or [])]

    ranked_pool = tier1_snippet_pool_for_entity(
        entity,
        lore_clusters or [],
        snippets_by_id,
        cfg,
        extra_snippet_ids=claim_snippet_ids,
    )
    evidence_bundle = build_entity_evidence_bundle(
        entity,
        claims,
        lore_clusters or [],
        config,
        source_snippets_by_id=snippets_by_id,
    )
    evidence_bundle["section_chained_synthesis"] = True
    evidence_bundle["tier1_snippet_pool_size"] = len(ranked_pool)

    valid_claim_ids, valid_snippet_ids = valid_support_id_sets(claims, evidence_bundle)
    snippet_count = len(evidence_bundle.get("approved_snippet_ids", []) or [])
    word_targets = section_word_targets_for_entity(claims, snippet_count, config, entity=entity)
    wiki_link_rows = _stage_11().build_available_wiki_link_rows(entity, entities_by_name)

    model_provider_cfg = config.get("model_provider", {}) if isinstance(config, dict) else {}
    validation_retries = max(2, int(model_provider_cfg.get("synthesis_validation_retries", 1)))
    provider_retries = max(validation_retries, int(model_provider_cfg.get("synthesis_provider_retries", 2)))
    validation_retry_sleep_seconds = max(
        0.0,
        float(
            model_provider_cfg.get(
                "synthesis_validation_retry_sleep_seconds",
                model_provider_cfg.get("adaptive_min_interval_seconds", 2.0),
            )
        ),
    )
    provider_retry_sleep_seconds = max(
        validation_retry_sleep_seconds,
        float(
            model_provider_cfg.get(
                "synthesis_provider_retry_sleep_seconds",
                model_provider_cfg.get("rate_limit_cooldown_seconds", 30),
            )
        ),
    )

    validation_feedback = ""
    last_error: RuntimeError | None = None
    provider_failures = 0
    validation_failures = 0
    entity_name = str(entity.get("canonical_name", "")).strip() or "entity"
    history_line_count = len(entity_development_history_lines or [])

    logger.info(
        "Section-chained synthesis starting entity=%s pool=%s claims=%s development_history_lines=%s",
        entity_name,
        len(ranked_pool),
        len(claims),
        history_line_count,
    )

    section_results: dict[str, dict[str, Any]] = {}
    merge_only_retry = False
    digest: dict[str, Any] = {}
    write_order = section_write_order(config)

    while True:
        attempt = provider_failures + validation_failures + 1
        logger.info(
            "Section-chained entity=%s attempt=%s merge_only=%s",
            entity_name,
            attempt,
            merge_only_retry,
        )
        if not merge_only_retry:
            digest_cap = int(tier_cfg.get("digest_snippet_cap", 45) or 45)
            digest_ids = ranked_pool[:digest_cap]
            digest_rows = _build_source_evidence_rows(
                entity, claims, digest_ids, snippets_by_id, global_alias_pairs
            )
            logger.info(
                "Section-chained entity=%s digest input snippets=%s rows=%s",
                entity_name,
                len(digest_ids),
                len(digest_rows),
            )
            digest_prompt = build_digest_prompt(
                entity,
                claims,
                memory_for_entity,
                evidence_bundle,
                digest_rows,
                entity_development_history_lines,
                wiki_link_rows,
                word_targets,
                validation_feedback=validation_feedback,
                narrative_frame=narrative_frame,
            )
            digest = _call_model_json(digest_prompt, config, "digest", entity_name=entity_name) or {}
            if not isinstance(digest, dict) or not digest.get("section_outline"):
                provider_failures += 1
                last_error = RuntimeError("Section-chained synthesis failed: invalid lore digest JSON.")
                if provider_failures > provider_retries:
                    break
                status = get_model_runtime_status()
                reason = str(status.get("last_model_skip_reason") or "provider_unavailable")
                sleep_s = _stage_11().provider_wait_seconds(reason, status, provider_retry_sleep_seconds)
                if sleep_s:
                    time.sleep(sleep_s)
                validation_feedback = (
                    "Digest must include section_outline, biography_beats, and timeline_chronology with cited IDs."
                )
                continue

            logger.info(
                "Section-chained entity=%s digest ok biography_beats=%s timeline_chronology=%s",
                entity_name,
                len(digest.get("biography_beats", []) or []),
                len(digest.get("timeline_chronology", []) or []),
            )

            per_section_limit = int(tier_cfg.get("snippets_per_section", 22) or 22)
            section_results = {}
            prior_prose: dict[str, str] = {}
            section_failures = 0
            for step_index, section_key in enumerate(write_order, start=1):
                section_ids = snippet_ids_for_section(
                    section_key,
                    ranked_pool,
                    snippets_by_id,
                    limit=per_section_limit,
                    narrative_work_tags=narrative_work_tags,
                )
                section_rows = _build_source_evidence_rows(
                    entity, claims, section_ids, snippets_by_id, global_alias_pairs
                )
                logger.info(
                    "Section-chained entity=%s section %s/%s key=%s snippets=%s",
                    entity_name,
                    step_index,
                    len(write_order),
                    section_key,
                    len(section_ids),
                )
                section_prompt = build_section_writer_prompt(
                    section_key,
                    entity,
                    claims,
                    memory_for_entity,
                    evidence_bundle,
                    digest,
                    section_rows,
                    prior_prose,
                    entity_development_history_lines,
                    wiki_link_rows,
                    word_targets,
                    validation_feedback=validation_feedback,
                    narrative_frame=narrative_frame,
                )
                section_payload = _call_model_json(
                    section_prompt,
                    config,
                    f"section:{section_key}",
                    entity_name=entity_name,
                )
                if not isinstance(section_payload, dict):
                    section_failures += 1
                    logger.warning(
                        "Section-chained entity=%s section %s failed (no JSON).",
                        entity_name,
                        section_key,
                    )
                    break
                prose = str(section_payload.get("prose", "")).strip()
                word_count = len(prose.split()) if prose else 0
                logger.info(
                    "Section-chained entity=%s section %s done words=%s support_ids=%s",
                    entity_name,
                    section_key,
                    word_count,
                    len(section_payload.get("support_ids", []) or []),
                )
                if prose:
                    prior_prose[section_key] = prose
                section_results[section_key] = section_payload

            if section_failures:
                provider_failures += 1
                last_error = RuntimeError("Section-chained synthesis failed: section writer returned no JSON.")
                if provider_failures > provider_retries:
                    break
                validation_feedback = "Each section writer must return section_key, prose, and support_ids."
                merge_only_retry = False
                continue

        if not section_results:
            last_error = RuntimeError("Section-chained synthesis failed: no section drafts produced.")
            break

        logger.info("Section-chained entity=%s merge pass starting sections=%s", entity_name, len(section_results))
        merge_prompt = build_merge_prompt(
            entity,
            claims,
            memory_for_entity,
            evidence_bundle,
            digest,
            _assemble_section_drafts(section_results, write_order=write_order),
            entity_development_history_lines,
            wiki_link_rows,
            word_targets,
            validation_feedback=validation_feedback,
            narrative_frame=narrative_frame,
        )
        merged = _call_model_json(merge_prompt, config, "merge", entity_name=entity_name)
        if not isinstance(merged, dict) or not isinstance(merged.get("summary"), str):
            provider_failures += 1
            last_error = RuntimeError("Section-chained synthesis failed: merge pass returned no valid card JSON.")
            if provider_failures > provider_retries:
                break
            validation_feedback = "Merge pass must return summary, sections, and support_map."
            merge_only_retry = False
            continue

        _merge_section_results_into_synthesis(merged, section_results)
        sections = merged.get("sections")
        if isinstance(sections, dict):
            merged["sections"] = normalize_card_sections(sections)
        _backfill_support_map_from_section_drafts(merged, section_results)
        try:
            stage_11 = _stage_11()
            stage_11.sanitize_optional_synthesis_fields(
                merged,
                claims,
                memory_for_entity,
                valid_snippet_ids=valid_snippet_ids,
            )
            normalize_synthesis_support_ids(merged, valid_claim_ids, valid_snippet_ids)
            stage_11.validate_synthesis_support(
                entity,
                claims,
                memory_for_entity,
                merged,
                evidence_bundle=evidence_bundle,
                config=config,
            )
            merged["_lore_digest"] = digest
            merged["_section_chained_synthesis"] = True
            if provider_failures or validation_failures:
                merged["_validation_retry_count"] = provider_failures + validation_failures
            stage_11_mod = _stage_11()
            word_count = stage_11_mod.synthesis_word_count(merged)
            logger.info(
                "Section-chained synthesis completed entity=%s pool=%s sections=%s words=%s retries=%s",
                entity_name,
                len(ranked_pool),
                len(section_results),
                word_count,
                provider_failures + validation_failures,
            )
            from pipeline.prose_alias_registry import apply_prose_normalization_to_synthesis

            apply_prose_normalization_to_synthesis(merged, entity, global_alias_pairs)
            return merged
        except RuntimeError as exc:
            last_error = exc
            validation_failures += 1
            logger.warning(
                "Section-chained entity=%s validation failed (attempt %s): %s",
                entity_name,
                validation_failures,
                exc,
            )
            if validation_failures > validation_retries:
                break
            validation_feedback = str(exc)
            merge_only_retry = bool(section_results) and any(
                token in str(exc).lower()
                for token in (
                    "too short",
                    "too long",
                    "speculative",
                    "support_map",
                    "evidence-bundle support",
                    "verbatim",
                    "path sections",
                    "path isolation",
                    "peaceful-path",
                    "path b lede",
                )
            )
            if merge_only_retry:
                logger.info(
                    "Section-chained entity=%s re-running merge only after validation: %s",
                    entity_name,
                    exc,
                )
            if validation_retry_sleep_seconds:
                time.sleep(validation_retry_sleep_seconds)

    raise last_error or RuntimeError("Section-chained card synthesis failed.")
