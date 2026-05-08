from __future__ import annotations

import argparse
import re
from pathlib import Path
from typing import Any

from pipeline.common import get_logger, now_utc_iso, read_json, read_jsonl, stable_id, write_json, write_jsonl
from pipeline.mixtral_anchor_provider import build_prompt, call_mixtral_chat, load_seed_entities
from pipeline.thematic_profile import update_runtime_profile


LORE_KEYWORDS = {
    "theriac",
    "enoch",
    "krypteia",
    "hectr",
    "ruinr",
    "mycelium wars",
    "olympus",
    "joy",
    "penemue",
    "quest",
    "immortality",
}
META_KEYWORDS = {
    "mechanic",
    "marketing",
    "campaign",
    "fundraising",
    "pitch",
    "steam",
    "devlog",
    "production",
    "roadmap",
    "audience",
}
META_INTENT_REGEXES = [
    re.compile(r"\b(social media|media push|marketing|campaign|pitch(?:ing)?|production|commission|commercial)\b"),
    re.compile(r"\b(work project|deadline|roadmap|devlog|artist|hiatus)\b"),
    re.compile(r"\b(i|we)\b.{0,24}\b(need|should|could|plan|planning|schedule|pause|ship|release)\b"),
]
SENSITIVE_REGEXES = [
    r"\bfuck\b",
    r"\bshit\b",
    r"\bslur\b",
]


def _profile_defaults_from_config(provider_config: dict[str, Any], profile_type: str) -> tuple[str, float, float, int]:
    source_defaults = provider_config.get("source_profile_defaults", {})
    profile_defaults = source_defaults.get(profile_type, {}) if isinstance(source_defaults, dict) else {}

    strictness_level = str(profile_defaults.get("strictness_level", "strict"))
    relevance_min = float(profile_defaults.get("theriac_relevance_min", 0.7))
    meta_split_min = float(profile_defaults.get("meta_lore_split_min", 0.55))
    context_window = int(profile_defaults.get("context_window_messages", 1))

    relevance_min = max(0.0, min(1.0, relevance_min))
    meta_split_min = max(0.0, min(1.0, meta_split_min))
    context_window = max(1, context_window)
    return strictness_level, relevance_min, meta_split_min, context_window


def default_profile(
    thread_id: str,
    partner_id: str,
    partner_label: str,
    provider_config: dict[str, Any],
) -> dict[str, Any]:
    profile_type = "unknown_low_signal"
    strictness_level, relevance_min, meta_split_min, context_window = _profile_defaults_from_config(
        provider_config, profile_type
    )
    return {
        "thread_id": thread_id,
        "partner_id": partner_id,
        "partner_display_name": partner_label,
        "profile_type": profile_type,
        "strictness_level": strictness_level,
        "base_thresholds": {"theriac_relevance_min": relevance_min, "meta_lore_split_min": meta_split_min},
        "context_window_messages": context_window,
        "notes": "Auto-generated default profile.",
        "last_calibrated_at": now_utc_iso(),
        "calibration_examples": [],
    }


def profile_adjustment(profile_type: str) -> float:
    if profile_type == "theriac_dedicated":
        return 0.15
    if profile_type == "mixed_topic":
        return 0.05
    return -0.05


def index_rows_by_thread(rows: list[dict[str, Any]]) -> tuple[dict[str, list[dict[str, Any]]], dict[tuple[str, str], int]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        thread_id = str(row.get("thread_id", ""))
        grouped.setdefault(thread_id, []).append(row)

    for thread_id, items in grouped.items():
        items.sort(key=lambda x: (str(x.get("timestamp_utc", "")), str(x.get("message_id", ""))))
        grouped[thread_id] = items

    positions: dict[tuple[str, str], int] = {}
    for thread_id, items in grouped.items():
        for idx, item in enumerate(items):
            positions[(thread_id, str(item.get("message_id", "")))] = idx
    return grouped, positions


def context_rows_for_message(
    row: dict[str, Any],
    grouped_rows: dict[str, list[dict[str, Any]]],
    row_positions: dict[tuple[str, str], int],
    context_window_messages: int,
) -> list[dict[str, Any]]:
    thread_id = str(row.get("thread_id", ""))
    message_id = str(row.get("message_id", ""))
    thread_rows = grouped_rows.get(thread_id, [row])
    current_idx = row_positions.get((thread_id, message_id), 0)

    window_size = max(1, int(context_window_messages))
    before = (window_size - 1) // 2
    after = (window_size - 1) - before
    start = max(0, current_idx - before)
    end = min(len(thread_rows), current_idx + after + 1)
    window_rows = thread_rows[start:end]
    return window_rows or [row]


def join_window_text(window_rows: list[dict[str, Any]], field: str) -> str:
    parts: list[str] = []
    for item in window_rows:
        value = str(item.get(field, "")).strip()
        if value:
            parts.append(value)
    return "\n".join(parts)


def score_relevance(text: str, profile: dict[str, Any]) -> tuple[float, str]:
    t = text.lower()
    lore_hits = sum(1 for kw in LORE_KEYWORDS if kw in t)
    meta_hits = sum(1 for kw in META_KEYWORDS if kw in t)
    base = min(1.0, (lore_hits * 0.24) + (meta_hits * 0.12))
    score = max(0.0, min(1.0, base + profile_adjustment(profile.get("profile_type", "unknown_low_signal"))))
    reason = f"lore_hits={lore_hits}, meta_hits={meta_hits}, profile={profile.get('profile_type')}"
    return score, reason


def classify_track(text: str) -> tuple[str, list[str]]:
    t = text.lower()
    lore_hits = [kw for kw in LORE_KEYWORDS if kw in t]
    meta_hits = [kw for kw in META_KEYWORDS if kw in t]
    topics: list[str] = []
    if lore_hits:
        topics.extend(["entity", "theme"])
    if meta_hits:
        topics.extend(["marketing", "production"])
    if lore_hits and not meta_hits:
        return "lore", sorted(set(topics))
    if meta_hits and not lore_hits:
        return "meta", sorted(set(topics))
    if lore_hits and meta_hits:
        return "meta", sorted(set(topics))
    return "unknown", []


def heuristic_anchor_candidates(text: str, seed_entities: list[str]) -> list[str]:
    lower = text.lower()
    matches = [name for name in seed_entities if name.lower() in lower]
    return matches[:10]


def meta_intent_hits(text: str) -> int:
    lowered = text.lower()
    return sum(1 for rx in META_INTENT_REGEXES if rx.search(lowered))


def conservative_bootstrap_without_model(
    text: str,
    heuristic_score: float,
    heuristic_reason: str,
    heuristic_track: str,
    heuristic_topics: list[str],
    anchors: list[str],
) -> tuple[float, str, str, list[str], list[str], list[str], list[str]]:
    # Heuristics are only a weak prior when model output is unavailable.
    meta_hits = meta_intent_hits(text)
    anchor_boost = min(0.2, 0.03 * len(anchors))
    prior_strength = max(0.0, min(1.0, heuristic_score + anchor_boost))

    if heuristic_track == "meta" and (meta_hits >= 2 or (meta_hits >= 1 and prior_strength >= 0.55)):
        score = max(0.38, min(0.72, round(0.20 + (0.16 * meta_hits) + (0.44 * prior_strength), 4)))
        reason = f"{heuristic_reason}; provider_unavailable=bootstrap_meta; meta_intent_hits={meta_hits}"
        topics = sorted(set(heuristic_topics + ["marketing", "production"]))
        return score, reason, "meta", topics, anchors, [], []

    if heuristic_track == "lore" and prior_strength >= 0.62 and meta_hits == 0 and len(anchors) >= 1:
        score = max(0.38, min(0.76, round(0.18 + (0.62 * prior_strength), 4)))
        reason = f"{heuristic_reason}; provider_unavailable=bootstrap_lore; anchor_count={len(anchors)}"
        topics = sorted(set(heuristic_topics + ["entity", "theme"]))
        return score, reason, "lore", topics, anchors, [], []

    score = min(0.34, round(0.20 + (0.20 * prior_strength), 4))
    reason = f"{heuristic_reason}; provider_unavailable=bootstrap_unknown; meta_intent_hits={meta_hits}"
    return score, reason, "unknown", heuristic_topics, anchors, [], []


def classify_with_provider(
    text: str,
    profile: dict[str, Any],
    provider_config: dict[str, Any],
    seed_entities: list[str],
) -> tuple[float, str, str, list[str], list[str], list[str], list[str]]:
    logger = get_logger(__name__)
    heuristic_score, heuristic_reason = score_relevance(text, profile)
    heuristic_track, heuristic_topics = classify_track(text)
    anchors = heuristic_anchor_candidates(text, seed_entities)

    mode = str(provider_config.get("anchor_provider", "heuristic")).lower()
    if mode not in {"mixtral", "hybrid"}:
        return heuristic_score, heuristic_reason, heuristic_track, heuristic_topics, anchors, [], []

    mixtral_cfg = provider_config.get("mixtral", {})
    rate_state_path = Path(str(mixtral_cfg.get("rate_state_path", "artifacts/learning/mixtral_rate_runtime.json")))
    prompt = build_prompt(text, profile, seed_entities, anchors)
    model_response = call_mixtral_chat(
        base_url=str(mixtral_cfg.get("base_url", "http://127.0.0.1:11434")),
        model=str(mixtral_cfg.get("model", "mixtral")),
        prompt=prompt,
        temperature=float(mixtral_cfg.get("temperature", 0.0)),
        timeout_seconds=int(mixtral_cfg.get("timeout_seconds", 60)),
        provider=str(mixtral_cfg.get("provider", "auto")),
        api_base_url=str(mixtral_cfg.get("api_base_url", "https://api.mistral.ai/v1")),
        api_model=str(mixtral_cfg.get("api_model", "mistral-large-latest")),
        api_retries=int(mixtral_cfg.get("api_retries", 2)),
        auto_fallback_to_ollama=bool(mixtral_cfg.get("auto_fallback_to_ollama", True)),
        rate_limit_cooldown_seconds=int(mixtral_cfg.get("rate_limit_cooldown_seconds", 90)),
        rate_state_path=rate_state_path,
        min_interval_seconds=float(mixtral_cfg.get("adaptive_min_interval_seconds", 2.0)),
        max_interval_seconds=float(mixtral_cfg.get("adaptive_max_interval_seconds", 120.0)),
        success_decay=float(mixtral_cfg.get("adaptive_success_decay", 0.9)),
        rate_limit_growth=float(mixtral_cfg.get("adaptive_rate_limit_growth", 1.8)),
        ollama_unavailable_cooldown_seconds=int(mixtral_cfg.get("ollama_unavailable_cooldown_seconds", 120)),
    )
    if not isinstance(model_response, dict):
        logger.debug(
            "Stage C provider fallback: mode=%s heuristic_score=%.3f heuristic_track=%s",
            mode,
            heuristic_score,
            heuristic_track,
        )
        return conservative_bootstrap_without_model(
            text,
            heuristic_score,
            heuristic_reason,
            heuristic_track,
            heuristic_topics,
            anchors,
        )

    model_score = float(model_response.get("theriac_relevance", heuristic_score))
    model_score = max(0.0, min(1.0, model_score))
    model_track = str(model_response.get("knowledge_track", heuristic_track))
    if model_track not in {"lore", "meta", "unknown"}:
        model_track = heuristic_track
    raw_candidates = model_response.get("anchor_candidates", [])
    model_anchors: list[str] = []
    if isinstance(raw_candidates, list):
        for item in raw_candidates:
            if isinstance(item, dict):
                name = str(item.get("name", "")).strip()
                if name:
                    model_anchors.append(name)
    merged_anchors = list(dict.fromkeys((model_anchors + anchors)))[:10]
    suggested = model_response.get("suggested_thematic_markers", {}) if isinstance(model_response, dict) else {}
    suggested_hist: list[str] = []
    suggested_music: list[str] = []
    if isinstance(suggested, dict):
        suggested_hist = [str(x).strip().lower() for x in (suggested.get("historical") or []) if str(x).strip()]
        suggested_music = [str(x).strip().lower() for x in (suggested.get("music") or []) if str(x).strip()]
    reason = f"{heuristic_reason}; provider={'mixtral' if mode == 'mixtral' else 'hybrid'}; heuristic_prior_weight=0.15"
    logger.debug(
        "Stage C provider result: mode=%s model_score=%.3f model_track=%s model_anchors=%d merged_anchors=%d",
        mode,
        model_score,
        model_track,
        len(model_anchors),
        len(merged_anchors),
    )

    if mode == "hybrid":
        # In hybrid mode, model output dominates; heuristics are only weak priors.
        blended = round((0.85 * model_score) + (0.15 * heuristic_score), 4)
        topics = []
        if model_track == "meta":
            topics = sorted(set(heuristic_topics + ["marketing", "production"]))
        elif model_track == "lore":
            topics = sorted(set(heuristic_topics + ["entity", "theme"]))
        else:
            topics = heuristic_topics
        return blended, reason, model_track, topics, merged_anchors, sorted(set(suggested_hist)), sorted(set(suggested_music))

    topics = []
    if model_track == "meta":
        topics = sorted(set(heuristic_topics + ["marketing", "production"]))
    elif model_track == "lore":
        topics = sorted(set(heuristic_topics + ["entity", "theme"]))
    else:
        topics = heuristic_topics
    return model_score, reason, model_track, topics, merged_anchors, sorted(set(suggested_hist)), sorted(set(suggested_music))


def detect_sensitive_flags(text: str) -> list[str]:
    lower = text.lower()
    flags: list[str] = []
    for rx in SENSITIVE_REGEXES:
        if re.search(rx, lower):
            flags.append("contains_sensitive_language")
            break
    if any(ord(ch) > 127 for ch in text):
        flags.append("contains_unicode_or_emoji")
    return flags


def run(
    in_jsonl: Path,
    in_profiles_json: Path,
    out_snippets_jsonl: Path,
    out_needs_review_jsonl: Path,
    out_profiles_json: Path,
    in_pipeline_config_json: Path | None = None,
    in_seed_json: Path | None = None,
    thematic_runtime_path: Path | None = None,
) -> None:
    logger = get_logger(__name__)
    rows = read_jsonl(in_jsonl)
    logger.info("Stage C: loaded %d normalized message row(s).", len(rows))
    grouped_rows, row_positions = index_rows_by_thread(rows)
    profile_payload = {"profiles": []}
    if in_profiles_json.exists():
        profile_payload = read_json(in_profiles_json)
    profiles = {
        p["thread_id"]: p for p in profile_payload.get("profiles", []) if isinstance(p, dict) and "thread_id" in p
    }
    provider_config: dict[str, Any] = {"anchor_provider": "heuristic"}
    if in_pipeline_config_json and in_pipeline_config_json.exists():
        provider_config = read_json(in_pipeline_config_json)
    seed_entities = load_seed_entities(in_seed_json)
    logger.info(
        "Stage C: provider=%s, seed_entities=%d, existing_profiles=%d",
        str(provider_config.get("anchor_provider", "heuristic")),
        len(seed_entities),
        len(profiles),
    )

    snippets: list[dict[str, Any]] = []
    review: list[dict[str, Any]] = []
    progress_every = max(1, len(rows) // 10)
    stage_hist_markers: list[str] = []
    stage_music_markers: list[str] = []

    for row_index, row in enumerate(rows, start=1):
        thread_id = row["thread_id"]
        profile = profiles.get(thread_id)
        if profile is None:
            profile = default_profile(thread_id, row["partner_id"], row["partner_label"], provider_config)
            profiles[thread_id] = profile
        context_window = int(profile.get("context_window_messages", 1))
        window_rows = context_rows_for_message(row, grouped_rows, row_positions, context_window)
        context_raw = join_window_text(window_rows, "content_raw")
        context_normalized = join_window_text(window_rows, "content_normalized")
        if not context_normalized:
            context_normalized = row.get("content_normalized", "")
        if not context_raw:
            context_raw = row.get("content_raw", "")

        score, reason, track, topics, anchor_candidates, suggested_hist, suggested_music = classify_with_provider(
            context_normalized,
            profile,
            provider_config,
            seed_entities,
        )
        stage_hist_markers.extend(suggested_hist)
        stage_music_markers.extend(suggested_music)
        threshold = float(profile["base_thresholds"]["theriac_relevance_min"])
        snippet = {
            "snippet_id": stable_id("snippet", row["message_id"], row["thread_id"]),
            "thread_id": row["thread_id"],
            "partner_id": row["partner_id"],
            "partner_label": row["partner_label"],
            "message_ids": [str(item.get("message_id", "")) for item in window_rows if str(item.get("message_id", ""))],
            "timestamp_start_utc": window_rows[0].get("timestamp_utc", row["timestamp_utc"]),
            "timestamp_end_utc": window_rows[-1].get("timestamp_utc", row["timestamp_utc"]),
            "speaker": "unknown",
            "raw_text": context_raw,
            "display_text_normalized": context_normalized,
            "relevance_score": score,
            "relevance_reason": reason,
            "candidate_entities": anchor_candidates,
            "knowledge_track": track,
            "candidate_topics": topics,
            "sensitivity_flags": detect_sensitive_flags(row.get("content_raw", "")),
            "provenance": row.get("provenance", {}),
        }
        if score >= threshold and track != "unknown":
            snippets.append(snippet)
        elif score >= max(0.35, threshold - 0.2):
            review.append(snippet)
        logger.debug(
            "Stage C classify: row=%d/%d message_id=%s score=%.3f threshold=%.3f track=%s anchors=%d",
            row_index,
            len(rows),
            row.get("message_id", ""),
            score,
            threshold,
            track,
            len(anchor_candidates),
        )
        if row_index % progress_every == 0 or row_index == len(rows):
            logger.info(
                "Stage C progress: %d/%d rows, snippets=%d, needs_review=%d",
                row_index,
                len(rows),
                len(snippets),
                len(review),
            )

    write_jsonl(out_snippets_jsonl, snippets)
    write_jsonl(out_needs_review_jsonl, review)
    write_json(out_profiles_json, {"profiles": list(profiles.values())})
    thematic_cfg = provider_config.get("thematic_linking", {})
    runtime_updates_enabled = bool(thematic_cfg.get("runtime_updates_enabled", True))
    if runtime_updates_enabled and thematic_runtime_path is not None:
        update_runtime_profile(
            thematic_runtime_path,
            "stage_c",
            stage_hist_markers,
            stage_music_markers,
            min_support=int(thematic_cfg.get("runtime_min_support", 2)),
        )
    logger.info(
        "Stage C complete: snippets=%d, needs_review=%d, profiles=%d",
        len(snippets),
        len(review),
        len(profiles),
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--in-jsonl", type=Path, required=True)
    parser.add_argument("--in-profiles-json", type=Path, required=True)
    parser.add_argument("--out-snippets-jsonl", type=Path, required=True)
    parser.add_argument("--out-needs-review-jsonl", type=Path, required=True)
    parser.add_argument("--out-profiles-json", type=Path, required=True)
    parser.add_argument("--in-pipeline-config-json", type=Path, required=False, default=None)
    parser.add_argument("--in-seed-json", type=Path, required=False, default=None)
    parser.add_argument("--thematic-runtime-path", type=Path, required=False, default=None)
    args = parser.parse_args()
    run(
        args.in_jsonl,
        args.in_profiles_json,
        args.out_snippets_jsonl,
        args.out_needs_review_jsonl,
        args.out_profiles_json,
        args.in_pipeline_config_json,
        args.in_seed_json,
        args.thematic_runtime_path,
    )


if __name__ == "__main__":
    main()
