"""Stage 08Q: tag snippets with quest design signals (heuristics + DeepSeek + optional MusicBrainz)."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from pipeline.common import get_logger, now_utc_iso, read_json, read_jsonl, write_json, write_jsonl
from pipeline.entity_resolution import load_entity_records
from pipeline.model_provider import call_model_chat, model_call_kwargs
from pipeline.quest_catalog import examples_by_title_key, load_quest_examples
from pipeline.quest_motifs import load_quest_motifs, write_default_motifs_if_missing
from pipeline.quest_music_lookup import enrich_tag_with_external_lookup
from pipeline.quest_tagging import (
    apply_override_tags,
    build_artist_review_queue,
    heuristic_tags_for_snippet,
    known_titles_from_indexes,
    merge_discovered_quests,
    quest_tagging_config,
    select_snippets_for_tagging,
    snippet_text,
)
from pipeline.quest_tagging_paths import (
    load_motif_artist_bindings,
    load_quest_tag_overrides,
    load_snippet_narrative_work_tags,
)


def _character_names(entity_seed_path: Path | None) -> list[str]:
    if not entity_seed_path or not entity_seed_path.exists():
        return []
    entities = load_entity_records(entity_seed_path)
    return sorted(
        {
            str(e.get("canonical_name", "")).strip()
            for e in entities
            if str(e.get("entity_type", "")).strip().lower() == "character" and str(e.get("canonical_name", "")).strip()
        }
    )


def _build_model_prompt(
    batch: list[dict[str, Any]],
    *,
    motifs: list[dict[str, Any]],
    examples: list[dict[str, Any]],
    character_names: list[str],
) -> str:
    motif_lines = [
        {
            "motif_id": m.get("motif_id"),
            "primary_character": m.get("primary_character"),
            "artist_aliases": (m.get("artist_aliases") or [])[:6],
            "requires_review": m.get("requires_review", False),
        }
        for m in motifs
    ]
    example_titles = [str(ex.get("quest_label", "")) for ex in examples[:40] if ex.get("quest_label")]
    items = [
        {
            "snippet_id": row.get("snippet_id"),
            "knowledge_track": row.get("knowledge_track"),
            "narrative_work_id": row.get("_narrative_work_id"),
            "text": snippet_text(row)[:1400],
            "existing_heuristic_tags": row.get("_heuristic_tags", []),
        }
        for row in batch
    ]
    return f"""Tag Theriac Coda quest-design snippets. Return strict JSON only:
{{"tags": [{{"snippet_id": "", "quest_label": "", "main_character": "", "motif_id": "", "artist_attributions": [], "match_kind": "model_inferred", "confidence": 0.0, "earliest_year_guess": null, "pool_sequence_guess": null, "chronology_confidence": null, "chronology_source": null, "rationale": ""}}]}}

Rules:
- Tag quest SONG TITLES used as in-game quest names, not real-world songs unrelated to Theriac design.
- Use motif registry to infer main_character from artist/band when discussed.
- Omit earliest_year_guess and pool_sequence_guess unless EXPLICITLY stated in snippet text (e.g. "Year 2", "first quest").
- Do not invent artists not supported by snippet text unless marking low confidence.
- Multiple tags per snippet allowed when multiple quests discussed.
- knowledge_track meta is valid for quest naming / design chat.

Motif registry:
{json.dumps(motif_lines, ensure_ascii=False, indent=2)}

Known example titles (non-exhaustive):
{json.dumps(example_titles, ensure_ascii=False)}

Characters:
{json.dumps(character_names[:80], ensure_ascii=False)}

Snippets:
{json.dumps(items, ensure_ascii=False, indent=2)}
"""


def _model_tag_batch(
    batch: list[dict[str, Any]],
    *,
    motifs: list[dict[str, Any]],
    examples: list[dict[str, Any]],
    character_names: list[str],
    config: dict[str, Any],
) -> dict[str, list[dict[str, Any]]]:
    prompt = _build_model_prompt(batch, motifs=motifs, examples=examples, character_names=character_names)
    response = call_model_chat(prompt=prompt, **model_call_kwargs(config, "stage_08q_quest_tagging"))
    if not isinstance(response, dict):
        return {}
    out: dict[str, list[dict[str, Any]]] = {}
    for row in response.get("tags", []) or []:
        if not isinstance(row, dict):
            continue
        snippet_id = str(row.get("snippet_id", "")).strip()
        label = str(row.get("quest_label", "")).strip()
        if snippet_id and label:
            out.setdefault(snippet_id, []).append(row)
    return out


def _normalize_model_tag(snippet: dict[str, Any], raw: dict[str, Any], narrative_work_id: str) -> dict[str, Any]:
    from pipeline.quest_catalog import quest_key_from_label

    label = str(raw.get("quest_label", "")).strip()
    return {
        "snippet_id": str(snippet.get("snippet_id", "")).strip(),
        "knowledge_track": str(snippet.get("knowledge_track", "lore")).strip() or "lore",
        "narrative_work_id": narrative_work_id,
        "quest_label": label,
        "quest_key": quest_key_from_label(label),
        "main_character": str(raw.get("main_character", "")).strip() or None,
        "character_confidence": float(raw.get("character_confidence", raw.get("confidence", 0.65)) or 0.65),
        "motif_id": str(raw.get("motif_id", "")).strip() or None,
        "artist_attributions": [str(a).strip() for a in raw.get("artist_attributions", []) or [] if str(a).strip()],
        "match_kind": str(raw.get("match_kind", "model_inferred")).strip() or "model_inferred",
        "confidence": float(raw.get("confidence", 0.65) or 0.65),
        "earliest_year_guess": raw.get("earliest_year_guess"),
        "pool_sequence_guess": raw.get("pool_sequence_guess"),
        "chronology_confidence": raw.get("chronology_confidence"),
        "chronology_source": raw.get("chronology_source"),
        "external_lookup_used": False,
        "needs_external_lookup": float(raw.get("confidence", 0) or 0) < 0.55,
        "source": "model_batch",
        "rationale": str(raw.get("rationale", "")).strip(),
        "tagged_at_utc": now_utc_iso(),
    }


def _needs_model_batch(tags: list[dict[str, Any]], cfg: dict[str, Any]) -> bool:
    if not tags:
        return True
    threshold = float(cfg.get("min_character_confidence", 0.55) or 0.55)
    return all(float(t.get("character_confidence", 0) or 0) < threshold for t in tags)


def run(
    in_snippets_jsonl: Path,
    out_tags_jsonl: Path,
    in_narrative_work_tags_jsonl: Path | None = None,
    in_pipeline_config_json: Path | None = None,
    in_review_memory_json: Path | None = None,
    in_entity_seed_json: Path | None = None,
    in_discovered_quests_json: Path | None = None,
) -> None:
    logger = get_logger(__name__)
    config = read_json(in_pipeline_config_json) if in_pipeline_config_json and in_pipeline_config_json.exists() else {}
    cfg = quest_tagging_config(config)
    if not cfg.get("enabled", True):
        logger.info("Stage 08Q skipped (quest_tagging.enabled=false)")
        return

    write_default_motifs_if_missing()
    motifs_path = Path(str(cfg.get("motifs_path", "canon/quest_motifs.json")))
    examples_path = Path(str(cfg.get("examples_path", "config/quest_song_seed.json")))
    motifs = load_quest_motifs(motifs_path)
    examples = load_quest_examples(examples_path)
    examples_index = examples_by_title_key(examples)

    discovered_payload = read_json(in_discovered_quests_json) if in_discovered_quests_json and in_discovered_quests_json.exists() else {}
    discovered_index = examples_by_title_key(list(discovered_payload.get("quests", [])) if isinstance(discovered_payload, dict) else [])

    snippets = read_jsonl(in_snippets_jsonl)
    work_tags = load_snippet_narrative_work_tags(in_narrative_work_tags_jsonl)
    overrides = load_quest_tag_overrides(in_review_memory_json)
    artist_bindings = load_motif_artist_bindings(in_review_memory_json)
    character_names = _character_names(in_entity_seed_json)
    known_titles = known_titles_from_indexes(examples_index, discovered_index)

    selected = select_snippets_for_tagging(snippets, work_tags, cfg)
    batch_size = max(1, int(cfg.get("batch_size", 12) or 12))
    model_enabled = bool(cfg.get("model_batch_enabled", True))

    ambiguous_for_model: list[dict[str, Any]] = []
    all_tags: list[dict[str, Any]] = []

    for snippet in selected:
        snippet_id = str(snippet.get("snippet_id", "")).strip()
        if not snippet_id:
            continue
        narrative_work_id = work_tags.get(snippet_id, "theriac_coda")

        if snippet_id in overrides:
            tag_rows = apply_override_tags(snippet, overrides[snippet_id], narrative_work_id)
            for row in tag_rows:
                row["tagged_at_utc"] = now_utc_iso()
            all_tags.extend(tag_rows)
            continue

        heuristic = heuristic_tags_for_snippet(
            snippet,
            narrative_work_id=narrative_work_id,
            examples_index=examples_index,
            discovered_index=discovered_index,
            motifs=motifs,
            character_names=character_names,
            artist_bindings=artist_bindings,
            known_titles=known_titles,
        )

        if model_enabled and _needs_model_batch(heuristic, cfg):
            snippet_copy = dict(snippet)
            snippet_copy["_narrative_work_id"] = narrative_work_id
            snippet_copy["_heuristic_tags"] = [
                {"quest_label": t.get("quest_label"), "main_character": t.get("main_character"), "confidence": t.get("confidence")}
                for t in heuristic
            ]
            ambiguous_for_model.append(snippet_copy)
            if len(ambiguous_for_model) >= batch_size:
                model_tags = _model_tag_batch(
                    ambiguous_for_model,
                    motifs=motifs,
                    examples=examples,
                    character_names=character_names,
                    config=config,
                )
                for amb in ambiguous_for_model:
                    sid = str(amb.get("snippet_id", "")).strip()
                    if sid in model_tags:
                        for raw in model_tags[sid]:
                            all_tags.append(_normalize_model_tag(amb, raw, work_tags.get(sid, "theriac_coda")))
                    elif sid:
                        for row in heuristic_tags_for_snippet(
                            amb,
                            narrative_work_id=work_tags.get(sid, "theriac_coda"),
                            examples_index=examples_index,
                            discovered_index=discovered_index,
                            motifs=motifs,
                            character_names=character_names,
                            artist_bindings=artist_bindings,
                            known_titles=known_titles,
                        ):
                            row["tagged_at_utc"] = now_utc_iso()
                            all_tags.append(row)
                ambiguous_for_model = []
        else:
            for row in heuristic:
                row["tagged_at_utc"] = now_utc_iso()
                all_tags.extend([row])

    if model_enabled and ambiguous_for_model:
        model_tags = _model_tag_batch(
            ambiguous_for_model,
            motifs=motifs,
            examples=examples,
            character_names=character_names,
            config=config,
        )
        for amb in ambiguous_for_model:
            sid = str(amb.get("snippet_id", "")).strip()
            if sid in model_tags:
                for raw in model_tags[sid]:
                    all_tags.append(_normalize_model_tag(amb, raw, work_tags.get(sid, "theriac_coda")))
            else:
                for row in heuristic_tags_for_snippet(
                    amb,
                    narrative_work_id=work_tags.get(sid, "theriac_coda"),
                    examples_index=examples_index,
                    discovered_index=discovered_index,
                    motifs=motifs,
                    character_names=character_names,
                    artist_bindings=artist_bindings,
                    known_titles=known_titles,
                ):
                    row["tagged_at_utc"] = now_utc_iso()
                    all_tags.append(row)

    # External lookup pass
    enriched: list[dict[str, Any]] = []
    for tag in all_tags:
        enriched.append(
            enrich_tag_with_external_lookup(
                tag,
                motifs=motifs,
                artist_bindings=artist_bindings,
                cfg=cfg,
            )
        )
    all_tags = enriched

    out_tags_jsonl.parent.mkdir(parents=True, exist_ok=True)
    write_jsonl(out_tags_jsonl, all_tags)

    discovered_out = out_tags_jsonl.parent / "discovered_quests.json"
    write_json(discovered_out, merge_discovered_quests(discovered_payload if isinstance(discovered_payload, dict) else None, all_tags))

    review_queue_path = out_tags_jsonl.parent / "artist_character_review_queue.jsonl"
    queue = build_artist_review_queue(
        all_tags,
        threshold=float(cfg.get("other_motif_review_threshold", 0.65) or 0.65),
    )
    write_jsonl(review_queue_path, queue)

    summary = {
        "generated_at_utc": now_utc_iso(),
        "snippet_pool": len(selected),
        "tag_count": len(all_tags),
        "by_match_kind": {},
        "by_character": {},
        "by_track": {"lore": 0, "meta": 0},
        "review_queue_count": len(queue),
        "model_batch_enabled": model_enabled,
        "override_count": sum(1 for s in selected if str(s.get("snippet_id", "")) in overrides),
    }
    for tag in all_tags:
        mk = str(tag.get("match_kind", "unknown"))
        summary["by_match_kind"][mk] = summary["by_match_kind"].get(mk, 0) + 1
        char = str(tag.get("main_character", "") or "unknown")
        summary["by_character"][char] = summary["by_character"].get(char, 0) + 1
        track = str(tag.get("knowledge_track", "lore")).lower()
        if track in summary["by_track"]:
            summary["by_track"][track] += 1
    write_json(out_tags_jsonl.parent / "tagging_summary.json", summary)
    logger.info("Stage 08Q complete: tags=%d snippets=%d queue=%d", len(all_tags), len(selected), len(queue))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--in-snippets-jsonl", type=Path, required=True)
    parser.add_argument("--out-tags-jsonl", type=Path, required=True)
    parser.add_argument("--in-narrative-work-tags-jsonl", type=Path, default=None)
    parser.add_argument("--in-pipeline-config-json", type=Path, default=Path("config/pipeline_config.json"))
    parser.add_argument("--in-review-memory-json", type=Path, default=Path("canon/review_memory.json"))
    parser.add_argument("--in-entity-seed-json", type=Path, default=None)
    parser.add_argument("--in-discovered-quests-json", type=Path, default=None)
    args = parser.parse_args()
    run(
        args.in_snippets_jsonl,
        args.out_tags_jsonl,
        args.in_narrative_work_tags_jsonl,
        args.in_pipeline_config_json,
        args.in_review_memory_json,
        args.in_entity_seed_json,
        args.in_discovered_quests_json,
    )


if __name__ == "__main__":
    main()
