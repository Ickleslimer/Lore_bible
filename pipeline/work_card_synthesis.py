"""Section-chained synthesis for narrative work pages."""

from __future__ import annotations

import json
import time
from typing import Any

from pipeline.common import get_logger
from pipeline.model_provider import call_model_chat, get_model_runtime_status, model_call_kwargs
from pipeline.narrative_works import active_works, filter_snippet_ids_by_narrative_work, load_narrative_works, work_by_id
from pipeline.work_card_sections import (
    PEACEFUL_WORK_LEDE_RULE,
    WORK_SECTION_WRITE_ORDER,
    work_word_target_plan,
)


def _snippet_text(snippet: dict[str, Any]) -> str:
    return " ".join(
        [
            str(snippet.get("display_text_normalized", "")),
            str(snippet.get("conversation_patch_summary", "")),
            " ".join(str(item) for item in snippet.get("conversation_patch_lore_developments", []) or []),
        ]
    ).lower()


def _rank_snippets_for_work(
    work_id: str,
    snippets_by_id: dict[str, dict[str, Any]],
    narrative_work_tags: dict[str, str],
    *,
    limit: int,
) -> list[str]:
    ids = list(snippets_by_id.keys())
    filtered = filter_snippet_ids_by_narrative_work(ids, work_id, narrative_work_tags, include_untagged=True)
    work = work_by_id(load_narrative_works(), work_id) or {}
    hints = tuple(str(h).lower() for h in work.get("keyword_hints", []) or [] if str(h).strip())
    scored: list[tuple[int, str]] = []
    for snippet_id in filtered:
        snippet = snippets_by_id.get(snippet_id, {})
        text = _snippet_text(snippet)
        score = sum(1 for hint in hints if hint in text)
        if str(snippet.get("knowledge_track", "")).lower() == "meta":
            score += 1
        scored.append((-score, snippet_id))
    scored.sort()
    ranked = [snippet_id for _, snippet_id in scored]
    return ranked[:limit]


def _snippet_rows(snippet_ids: list[str], snippets_by_id: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for snippet_id in snippet_ids:
        snippet = snippets_by_id.get(snippet_id)
        if not isinstance(snippet, dict):
            continue
        rows.append(
            {
                "snippet_id": snippet_id,
                "knowledge_track": snippet.get("knowledge_track", "lore"),
                "text": _snippet_text(snippet)[:1400],
            }
        )
    return rows


def _section_rules(section_key: str, work: dict[str, Any]) -> str:
    title = str(work.get("title", "this work"))
    if section_key == "summary":
        return f"Peaceful-path lede for {title} (default Path B experience).\n{PEACEFUL_WORK_LEDE_RULE}"
    if section_key == "branching":
        return (
            f"Path B main route and Path A side route for {title}. "
            "Path A detail lives here only—not in summary."
        )
    if section_key == "other_works":
        return "High-level pointers to spin-offs and alt timelines; no full walkthroughs."
    if section_key == "production":
        return "Meta/design/process notes only; neutral tone."
    return ""


def synthesize_work_card(
    work: dict[str, Any],
    snippets_by_id: dict[str, dict[str, Any]],
    narrative_work_tags: dict[str, str],
    config: dict[str, Any],
) -> dict[str, Any]:
    logger = get_logger(__name__)
    work_id = str(work.get("work_id", "")).strip()
    word_targets = work_word_target_plan()
    pool_cap = int((config.get("narrative_works") or {}).get("work_snippet_pool_cap", 80) or 80)
    per_section = int((config.get("narrative_works") or {}).get("snippets_per_section", 18) or 18)
    ranked = _rank_snippets_for_work(work_id, snippets_by_id, narrative_work_tags, limit=pool_cap)

    section_results: dict[str, dict[str, Any]] = {}
    prior_prose: dict[str, str] = {}
    for section_key in WORK_SECTION_WRITE_ORDER:
        section_ids = ranked[:per_section] if section_key == "summary" else ranked[:per_section]
        rows = _snippet_rows(section_ids, snippets_by_id)
        rules = _section_rules(section_key, work)
        target = (word_targets.get("section_word_targets", {}) or {}).get(section_key, "")
        prompt = f"""Write ONE section of a Theriac narrative work wiki page. Return strict JSON only.

Work: {json.dumps(work, ensure_ascii=False)}
Section: {section_key}
Word target: {target}
{rules}

Prior sections:
{json.dumps(prior_prose, ensure_ascii=False, indent=2)}

Evidence snippets:
{json.dumps(rows, ensure_ascii=False, indent=2)}

Return JSON: {{"section_key": "{section_key}", "prose": "", "support_ids": []}}
"""
        response = call_model_chat(prompt=prompt, **model_call_kwargs(config, "stage_11w_work_card_synthesis"))
        if not isinstance(response, dict):
            status = get_model_runtime_status()
            raise RuntimeError(
                f"Work card synthesis failed for {work_id}/{section_key}: "
                f"{status.get('last_model_skip_reason', 'provider_unavailable')}"
            )
        prose = str(response.get("prose", "")).strip()
        if prose:
            prior_prose[section_key] = prose
        section_results[section_key] = response
        time.sleep(0.1)

    sections = {key: str((section_results.get(key) or {}).get("prose", "")).strip() for key in WORK_SECTION_WRITE_ORDER}
    sections = {key: value for key, value in sections.items() if value}
    summary = sections.pop("summary", "") or prior_prose.get("summary", "")
    support_map = {
        key: list((section_results.get(key) or {}).get("support_ids", []) or [])
        for key in WORK_SECTION_WRITE_ORDER
    }
    return {
        "work_id": work_id,
        "title": work.get("title", work_id),
        "kind": work.get("kind", ""),
        "status": "draft",
        "summary": summary,
        "sections": sections,
        "support_map": support_map,
        "synthesis_tier": "narrative_work",
    }


def synthesize_active_work_cards(
    snippets_by_id: dict[str, dict[str, Any]],
    narrative_work_tags: dict[str, str],
    config: dict[str, Any],
) -> list[dict[str, Any]]:
    works = load_narrative_works()
    cards: list[dict[str, Any]] = []
    for work in active_works(works):
        cards.append(synthesize_work_card(work, snippets_by_id, narrative_work_tags, config))
    return cards
