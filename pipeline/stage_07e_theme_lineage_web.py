"""Stage 07E: low-volume web pass to attach real-world lineage to mined themes."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any

from pipeline.common import get_logger, now_utc_iso, read_json, write_json
from pipeline.model_provider import call_model_chat, model_call_kwargs
from pipeline.stage_07b_entity_adjudication import DEFAULT_WEB_TOOLS, stage_task_config


TASK_NAME = "stage_07e_theme_lineage_web"
THEME_LINEAGE_REPORT_SCHEMA_VERSION = 1
THEME_LINEAGE_CACHE_SCHEMA_VERSION = 1
DEFAULT_MAX_WEB_THEMES_PER_RUN = 8
SKIP_THEME_DOMAINS = {"aesthetic"}

LINEAGE_RESPONSE_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "lineage": {
            "type": "object",
            "properties": {
                "status": {"type": "string"},
                "lineage_class": {"type": "string"},
                "primary_tradition": {"type": "string"},
                "figures": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "name": {"type": "string"},
                            "role": {"type": "string"},
                        },
                        "required": ["name", "role"],
                        "additionalProperties": True,
                    },
                },
                "works": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "title": {"type": "string"},
                            "relation": {"type": "string"},
                        },
                        "required": ["title", "relation"],
                        "additionalProperties": True,
                    },
                },
                "movements_or_fields": {"type": "array", "items": {"type": "string"}},
                "web_findings": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "query": {"type": "string"},
                            "finding": {"type": "string"},
                            "source_url": {"type": "string"},
                        },
                        "required": ["query", "finding"],
                        "additionalProperties": True,
                    },
                },
                "reasoning_summary": {"type": "string"},
                "human_review_note": {"type": "string"},
            },
            "required": [
                "status",
                "lineage_class",
                "primary_tradition",
                "figures",
                "works",
                "movements_or_fields",
                "web_findings",
                "reasoning_summary",
                "human_review_note",
            ],
            "additionalProperties": True,
        }
    },
    "required": ["lineage"],
    "additionalProperties": False,
}


def run(
    inout_theme_profile_json: Path,
    out_theme_lineage_web_report_json: Path,
    out_theme_lineage_cache_json: Path,
    in_pipeline_config_json: Path | None = None,
) -> None:
    logger = get_logger(__name__)
    profile = load_theme_profile(inout_theme_profile_json)
    provider_config = read_json(in_pipeline_config_json) if in_pipeline_config_json and in_pipeline_config_json.exists() else {}
    task_cfg = stage_task_config(provider_config, TASK_NAME)
    cache = load_lineage_cache(out_theme_lineage_cache_json)
    cache_entries = cache.setdefault("entries", {})

    selected = select_themes_for_lineage_web(profile, task_cfg)
    web_call_count = 0
    cache_hit_count = 0
    failures: list[dict[str, Any]] = []
    applied: list[dict[str, Any]] = []

    if selected and bool(task_cfg.get("enabled", True)):
        kwargs = lineage_model_kwargs(provider_config, task_cfg)
        force_refresh = bool(task_cfg.get("force_refresh", False))
        max_web = max(0, int(task_cfg.get("max_web_themes_per_run", DEFAULT_MAX_WEB_THEMES_PER_RUN) or 0))

        for theme, reasons in selected:
            if web_call_count >= max_web:
                break
            theme_id = str(theme.get("theme_id", "")).strip()
            if not theme_id:
                continue
            cache_key = theme_lineage_cache_key(theme)
            cached = cache_entries.get(theme_id) if isinstance(cache_entries, dict) else None
            if (
                not force_refresh
                and isinstance(cached, dict)
                and cached.get("cache_key") == cache_key
                and isinstance(cached.get("lineage"), dict)
            ):
                theme["real_world_lineage"] = cached["lineage"]
                cache_hit_count += 1
                applied.append({"theme_id": theme_id, "label": theme.get("label", ""), "cache_status": "hit"})
                continue

            prompt = build_lineage_web_prompt(theme, reasons)
            logger.info(
                "Stage 07E lineage web: theme=%s reasons=%s model=%s",
                theme.get("label", theme_id),
                ", ".join(reasons),
                kwargs.get("api_model", ""),
            )
            response = call_model_chat(prompt=prompt, **kwargs)
            web_call_count += 1
            lineage = normalize_lineage_response(response, theme)
            if lineage is None:
                failures.append(
                    {
                        "theme_id": theme_id,
                        "label": theme.get("label", ""),
                        "reason": "invalid_or_empty_lineage_response",
                        "response_keys": sorted(response.keys()) if isinstance(response, dict) else [],
                    }
                )
                continue

            lineage["linked_at_utc"] = now_utc_iso()
            lineage["selection_reasons"] = reasons
            lineage["cache_status"] = "miss"
            theme["real_world_lineage"] = lineage
            cache_entries[theme_id] = {
                "cache_key": cache_key,
                "theme_id": theme_id,
                "label": theme.get("label", ""),
                "updated_at_utc": now_utc_iso(),
                "source_model": kwargs.get("api_model", ""),
                "lineage": lineage,
            }
            applied.append({"theme_id": theme_id, "label": theme.get("label", ""), "cache_status": "miss", "status": lineage.get("status", "")})
    else:
        logger.info(
            "Stage 07E: no lineage web calls (selected=%d enabled=%s).",
            len(selected),
            bool(task_cfg.get("enabled", True)),
        )

    profile["updated_at_utc"] = now_utc_iso()
    write_json(inout_theme_profile_json, profile)
    cache.update(
        {
            "schema_version": THEME_LINEAGE_CACHE_SCHEMA_VERSION,
            "updated_at_utc": now_utc_iso(),
            "stage": "07E_theme_lineage_web",
            "source_task": TASK_NAME,
            "entries": cache_entries,
        }
    )
    write_json(out_theme_lineage_cache_json, cache)
    report = {
        "schema_version": THEME_LINEAGE_REPORT_SCHEMA_VERSION,
        "generated_at_utc": now_utc_iso(),
        "stage": "07E_theme_lineage_web",
        "inputs": {"theme_profile_json": str(inout_theme_profile_json)},
        "policy": {
            "web_detects_real_world_lineage_not_canon": True,
            "canon_gate": "human_review",
            "max_web_themes_per_run": int(task_cfg.get("max_web_themes_per_run", DEFAULT_MAX_WEB_THEMES_PER_RUN) or 0),
        },
        "summary": {
            "active_theme_count": len([t for t in profile.get("themes", []) if str(t.get("status", "")) in {"active", "candidate"}]),
            "selected_theme_count": len(selected),
            "web_call_count": web_call_count,
            "cache_hit_count": cache_hit_count,
            "applied_count": len(applied),
            "failure_count": len(failures),
        },
        "applied": applied,
        "failures": failures,
    }
    write_json(out_theme_lineage_web_report_json, report)
    logger.info(
        "Stage 07E complete: selected=%d web_calls=%d cache_hits=%d applied=%d failures=%d",
        len(selected),
        web_call_count,
        cache_hit_count,
        len(applied),
        len(failures),
    )


def load_theme_profile(path: Path) -> dict[str, Any]:
    if path.exists():
        payload = read_json(path)
        if isinstance(payload, dict):
            payload.setdefault("themes", [])
            return payload
    return {"schema_version": 1, "themes": [], "policy": {}}


def load_lineage_cache(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"schema_version": THEME_LINEAGE_CACHE_SCHEMA_VERSION, "entries": {}}
    try:
        payload = read_json(path)
    except Exception:
        return {"schema_version": THEME_LINEAGE_CACHE_SCHEMA_VERSION, "entries": {}}
    if not isinstance(payload, dict):
        return {"schema_version": THEME_LINEAGE_CACHE_SCHEMA_VERSION, "entries": {}}
    if not isinstance(payload.get("entries"), dict):
        payload["entries"] = {}
    return payload


def lineage_model_kwargs(provider_config: dict[str, Any], task_cfg: dict[str, Any]) -> dict[str, Any]:
    kwargs = model_call_kwargs(provider_config, TASK_NAME)
    if not task_cfg or "provider" not in task_cfg:
        kwargs["provider"] = "openrouter"
    if not task_cfg or "api_base_url" not in task_cfg:
        kwargs["api_base_url"] = "https://openrouter.ai/api/v1"
    if not task_cfg or "api_model" not in task_cfg:
        kwargs["api_model"] = "openai/gpt-oss-120b"
    kwargs["timeout_seconds"] = max(int(kwargs.get("timeout_seconds", 60)), 180)
    kwargs["max_tokens"] = max(int(kwargs.get("max_tokens", 4096)), 3500)
    if not kwargs.get("tools"):
        kwargs["tools"] = DEFAULT_WEB_TOOLS
    kwargs["json_schema"] = LINEAGE_RESPONSE_SCHEMA
    if "rate_state_path" not in task_cfg:
        kwargs["rate_state_path"] = Path("artifacts/learning/openrouter_gpt_oss_120b_stage_07e_theme_lineage_rate_runtime.json")
    return kwargs


def select_themes_for_lineage_web(profile: dict[str, Any], task_cfg: dict[str, Any]) -> list[tuple[dict[str, Any], list[str]]]:
    max_select = max(0, int(task_cfg.get("max_web_themes_per_run", DEFAULT_MAX_WEB_THEMES_PER_RUN) or 0))
    if max_select <= 0:
        return []

    ranked: list[tuple[int, dict[str, Any], list[str]]] = []
    for theme in profile.get("themes", []) or []:
        if not isinstance(theme, dict):
            continue
        ok, reasons = should_check_theme_lineage(theme, task_cfg)
        if ok:
            ranked.append((lineage_priority_score(theme, reasons), theme, reasons))
    ranked.sort(key=lambda item: (-item[0], str(item[1].get("label", "")).lower()))
    return [(theme, reasons) for _score, theme, reasons in ranked[: max_select * 3]]


def should_check_theme_lineage(theme: dict[str, Any], task_cfg: dict[str, Any]) -> tuple[bool, list[str]]:
    reasons: list[str] = []
    status = str(theme.get("status", "")).strip().lower()
    if status not in {"active", "candidate"}:
        return False, reasons
    if str(theme.get("canon_relevance", "")).strip().lower() == "meta_only":
        return False, reasons
    domain = str(theme.get("theme_domain", "")).strip().lower()
    if domain in SKIP_THEME_DOMAINS:
        return False, reasons
    if str(theme.get("real_world_lineage", {}).get("status", "")).strip().lower() == "attributed" and not bool(
        task_cfg.get("recheck_attributed", False)
    ):
        existing = theme.get("real_world_lineage", {})
        if isinstance(existing, dict) and (existing.get("figures") or existing.get("works") or existing.get("movements_or_fields")):
            return False, reasons
    return True, ["theme_externality_check"]


def lineage_priority_score(theme: dict[str, Any], _reasons: list[str]) -> int:
    score = int(round(float(theme.get("confidence", 0.0) or 0.0) * 100))
    score += min(20, len(theme.get("evidence_snippet_ids", []) or []))
    if not theme.get("real_world_lineage"):
        score += 25
    return score


def theme_evidence_blob(theme: dict[str, Any]) -> str:
    parts = [
        str(theme.get("label", "")),
        str(theme.get("description", "")),
        str(theme.get("provenance_summary", "")),
        " ".join(str(x) for x in theme.get("positive_indicators", []) or []),
        " ".join(str(x) for x in theme.get("evidence_entities", []) or []),
        " ".join(str(x) for x in theme.get("pattern_notes", []) or []),
    ]
    return " ".join(parts)


def theme_lineage_cache_key(theme: dict[str, Any]) -> str:
    payload = {
        "label": theme.get("label", ""),
        "description": theme.get("description", ""),
        "positive_indicators": theme.get("positive_indicators", [])[:12],
        "evidence_entities": theme.get("evidence_entities", [])[:12],
    }
    return hashlib.sha256(json.dumps(payload, sort_keys=True, ensure_ascii=False).encode("utf-8")).hexdigest()[:24]


def build_lineage_web_prompt(theme: dict[str, Any], selection_reasons: list[str]) -> str:
    packet = {
        "theme_id": theme.get("theme_id", ""),
        "label": theme.get("label", ""),
        "theme_domain": theme.get("theme_domain", ""),
        "theme_type": theme.get("theme_type", ""),
        "description": theme.get("description", ""),
        "provenance_summary": theme.get("provenance_summary", ""),
        "positive_indicators": theme.get("positive_indicators", [])[:12],
        "evidence_entities": theme.get("evidence_entities", [])[:16],
        "pattern_notes": theme.get("pattern_notes", [])[:8],
    }
    return f"""You are Stage 07E of the Theriac Lore Bible pipeline.
Use OpenRouter web search for a general externality / real-world lineage check on one mined theme.

Critical policy:
- Web search detects likely real-world inspiration or source traditions; it does NOT decide canon and does NOT reject themes.
- Theriac may adopt real theories, myths, technologies, or institutions as in-fiction patterns. Record high-level lineage for wiki editors.
- High-level associations are enough: primary field, movement name, author, or source tradition. Do not require exhaustive citations.
- Use at most a few targeted searches on the theme label plus 1-2 salient phrases from the description or positive indicators.
- If the theme is mostly an in-fiction pattern with no clear external anchor, return status "not_applicable" or "uncertain".
- If ambiguous, return status "uncertain" rather than guessing.

Selection reasons:
{json.dumps(selection_reasons, ensure_ascii=False)}

Theme packet:
{json.dumps(packet, ensure_ascii=False, indent=2)}

Return strict JSON:
{{
  "lineage": {{
    "status": "attributed | uncertain | not_applicable | none_found",
    "lineage_class": "academic_theory | religious_text | mythological_tradition | scientific_field | technology_program | historical_political | mixed | unknown",
    "primary_tradition": "short label for the real-world tradition or field",
    "figures": [{{"name": "Person Name", "role": "author | theorist | historian | other"}}],
    "works": [{{"title": "Work title", "relation": "foundational text | influenced by | popularized | other"}}],
    "movements_or_fields": ["Terror Management Theory"],
    "web_findings": [{{"query": "search query", "finding": "short finding", "source_url": ""}}],
    "reasoning_summary": "How the real-world lineage relates to this theme label without deciding Theriac canon.",
    "human_review_note": "One sentence for a human editor."
  }}
}}
"""


def normalize_lineage_response(response: Any, theme: dict[str, Any]) -> dict[str, Any] | None:
    if not isinstance(response, dict):
        return None
    lineage = response.get("lineage")
    if not isinstance(lineage, dict):
        return None
    status = clean_text(lineage.get("status", "uncertain"), 40) or "uncertain"
    return {
        "status": status,
        "lineage_class": clean_text(lineage.get("lineage_class", "unknown"), 80) or "unknown",
        "primary_tradition": clean_text(lineage.get("primary_tradition", ""), 160),
        "figures": normalize_object_list(lineage.get("figures"), ("name", "role"), 12),
        "works": normalize_object_list(lineage.get("works"), ("title", "relation"), 12),
        "movements_or_fields": normalize_string_list(lineage.get("movements_or_fields"), 20, 160),
        "web_findings": normalize_web_findings(lineage.get("web_findings")),
        "reasoning_summary": clean_text(lineage.get("reasoning_summary", ""), 800),
        "human_review_note": clean_text(lineage.get("human_review_note", ""), 400),
        "theme_id": theme.get("theme_id", ""),
        "theme_label": theme.get("label", ""),
    }


def normalize_string_list(value: Any, limit: int, max_chars: int) -> list[str]:
    if not isinstance(value, list):
        return []
    out: list[str] = []
    for item in value:
        text = clean_text(item, max_chars)
        if text and text not in out:
            out.append(text)
        if len(out) >= limit:
            break
    return out


def normalize_object_list(value: Any, fields: tuple[str, str], limit: int) -> list[dict[str, str]]:
    if not isinstance(value, list):
        return []
    out: list[dict[str, str]] = []
    for item in value:
        if not isinstance(item, dict):
            continue
        row = {field: clean_text(item.get(field, ""), 160) for field in fields}
        if any(row.values()):
            out.append(row)
        if len(out) >= limit:
            break
    return out


def normalize_web_findings(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    out: list[dict[str, Any]] = []
    for item in value:
        if not isinstance(item, dict):
            continue
        finding = clean_text(item.get("finding", ""), 500)
        query = clean_text(item.get("query", ""), 200)
        if not finding:
            continue
        row: dict[str, Any] = {"query": query, "finding": finding}
        url = clean_text(item.get("source_url", ""), 300)
        if url:
            row["source_url"] = url
        out.append(row)
        if len(out) >= 8:
            break
    return out


def clean_text(value: Any, max_chars: int) -> str:
    return " ".join(str(value or "").strip().split())[:max_chars]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--inout-theme-profile-json", type=Path, required=True)
    parser.add_argument("--out-theme-lineage-web-report-json", type=Path, required=True)
    parser.add_argument("--out-theme-lineage-cache-json", type=Path, required=True)
    parser.add_argument("--in-pipeline-config-json", type=Path, required=False, default=None)
    args = parser.parse_args()
    run(
        args.inout_theme_profile_json,
        args.out_theme_lineage_web_report_json,
        args.out_theme_lineage_cache_json,
        args.in_pipeline_config_json,
    )


if __name__ == "__main__":
    main()
