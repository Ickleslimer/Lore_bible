from __future__ import annotations

import json
import os
import re
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Iterable

from pipeline.common import get_logger, read_json, stable_id
from pipeline.entity_resolution import load_entity_names

DEBUG_LOG_PATH = Path("debug-f7d16c.log")
DEBUG_SESSION_ID = "f7d16c"
_RATE_LIMITED_UNTIL_EPOCH_S = 0.0
_NEXT_MODEL_ATTEMPT_EPOCH_S = 0.0
_LAST_PACING_LOG_EPOCH_S = 0.0
_LAST_COOLDOWN_LOG_EPOCH_S = 0.0
_LAST_PROVIDER_RESOLVE_LOG_EPOCH_S = 0.0
_LAST_API_FAILURE_LOG_EPOCH_S = 0.0
_CACHED_API_KEY: str | None = None
_HAS_CACHED_API_KEY = False
_CACHED_OPENROUTER_API_KEY: str | None = None
_HAS_CACHED_OPENROUTER_API_KEY = False
_LAST_MODEL_SKIP_REASON = ""
_LAST_CALL_BILLED_COST_USD = 0.0
_LAST_CALL_API_MODEL = ""
_LAST_CALL_PROVIDER = ""
_PROVIDER_STATE_LOCK = threading.Lock()

PACING_SKIP_REASONS = frozenset({"provider_locked", "adaptive_pacing", "rate_limit_cooldown", "rate_limited_429"})

TASK_ROUTING_CONTROL_KEYS = {
    "max_concurrent_requests",
    "parallel_wave_size",
    "profile",
    "model_profile",
}

OPENROUTER_TRACE_FIELD_LIMIT = 128


def _clean_openrouter_trace_text(value: Any, limit: int = OPENROUTER_TRACE_FIELD_LIMIT) -> str:
    text = re.sub(r"\s+", "-", str(value or "").strip())
    text = re.sub(r"[^A-Za-z0-9_.:-]+", "-", text).strip("-")
    if not text:
        return ""
    return text[: max(1, int(limit))]


def _default_openrouter_session_id(task_name: str) -> str:
    clean_task = _clean_openrouter_trace_text(task_name.replace("_", "-"))
    return _clean_openrouter_trace_text(f"theriac-{clean_task}") or stable_id("or_session", task_name)


def _openrouter_trace_for_task(task_name: str, session_id: str, cfg: dict[str, Any]) -> dict[str, Any]:
    trace: dict[str, Any] = {
        "trace_id": session_id,
        "trace_name": f"Theriac {task_name}",
        "span_name": task_name,
        "generation_name": task_name,
        "pipeline_task": task_name,
    }
    configured_trace = cfg.get("trace")
    if isinstance(configured_trace, dict):
        trace.update(configured_trace)
    for key in ("trace_id", "trace_name", "span_name", "generation_name", "environment", "feature", "version"):
        if key in cfg and cfg.get(key) is not None:
            trace[key] = cfg[key]
    if not trace.get("trace_id"):
        trace["trace_id"] = session_id
    return trace


def get_model_runtime_status() -> dict[str, Any]:
    with _PROVIDER_STATE_LOCK:
        return {
            "last_model_skip_reason": _LAST_MODEL_SKIP_REASON,
            "rate_limited_until_epoch_s": _RATE_LIMITED_UNTIL_EPOCH_S,
            "next_model_attempt_epoch_s": _NEXT_MODEL_ATTEMPT_EPOCH_S,
            "last_call_billed_cost_usd": _LAST_CALL_BILLED_COST_USD,
            "last_call_api_model": _LAST_CALL_API_MODEL,
            "last_call_provider": _LAST_CALL_PROVIDER,
        }


def _extract_billed_cost_usd(body: dict[str, Any]) -> float:
    """Best-effort extraction of OpenRouter/compatible billed cost from response envelope."""
    usage = body.get("usage", {}) if isinstance(body.get("usage", {}), dict) else {}
    candidates = [
        usage.get("total_cost"),
        usage.get("cost"),
        body.get("total_cost"),
        body.get("cost"),
    ]
    for raw in candidates:
        try:
            value = float(raw)
        except (TypeError, ValueError):
            continue
        if value > 0:
            return value
    return 0.0


def provider_wait_seconds(reason: str, status: dict[str, Any], fallback_seconds: float) -> float:
    now_s = time.time()
    next_attempt = float(status.get("next_model_attempt_epoch_s") or 0.0)
    rate_limited_until = float(status.get("rate_limited_until_epoch_s") or 0.0)
    target = 0.0
    if reason in {"provider_locked", "adaptive_pacing"}:
        target = next_attempt
    elif reason in {"rate_limit_cooldown", "rate_limited_429"}:
        target = max(rate_limited_until, next_attempt)
    if target > now_s:
        return max(0.1, target - now_s)
    return max(0.0, float(fallback_seconds))


def model_max_concurrent_requests(provider_config: dict[str, Any] | None, task_name: str, default: int = 1) -> int:
    cfg = model_task_settings(provider_config, task_name)
    return max(1, int(cfg.get("max_concurrent_requests", default) or default))


def _reserve_dispatch_slot(
    *,
    rate_state_path: Path | None,
    min_interval_seconds: float,
    max_interval_seconds: float,
    provider_label: str,
) -> dict[str, Any]:
    global _RATE_LIMITED_UNTIL_EPOCH_S, _NEXT_MODEL_ATTEMPT_EPOCH_S, _LAST_PACING_LOG_EPOCH_S, _LAST_COOLDOWN_LOG_EPOCH_S, _LAST_MODEL_SKIP_REASON
    while True:
        with _PROVIDER_STATE_LOCK:
            now_s = time.time()
            state = _load_rate_state(rate_state_path)
            adaptive_interval = float(state.get("adaptive_min_interval_seconds", min_interval_seconds))
            adaptive_interval = max(float(min_interval_seconds), min(float(max_interval_seconds), adaptive_interval))
            if _RATE_LIMITED_UNTIL_EPOCH_S > now_s:
                remaining = round(_RATE_LIMITED_UNTIL_EPOCH_S - now_s, 2)
                _NEXT_MODEL_ATTEMPT_EPOCH_S = max(_NEXT_MODEL_ATTEMPT_EPOCH_S, _RATE_LIMITED_UNTIL_EPOCH_S)
                _LAST_MODEL_SKIP_REASON = "rate_limit_cooldown"
                if now_s - _LAST_COOLDOWN_LOG_EPOCH_S >= 1.0:
                    _LAST_COOLDOWN_LOG_EPOCH_S = now_s
                    _debug_log(
                        "model-provider-debug",
                        "H9",
                        "model_provider.py:_reserve_dispatch_slot",
                        f"Waiting for {provider_label} rate-limit cooldown",
                        {"cooldown_remaining_seconds": remaining},
                    )
                wait_s = max(0.05, _RATE_LIMITED_UNTIL_EPOCH_S - now_s)
            elif _NEXT_MODEL_ATTEMPT_EPOCH_S > now_s:
                _LAST_MODEL_SKIP_REASON = "provider_locked"
                wait_s = max(0.05, _NEXT_MODEL_ATTEMPT_EPOCH_S - now_s)
            else:
                last_request = float(state.get("last_request_epoch_s", 0.0))
                elapsed_since_last = now_s - last_request if last_request > 0 else 10**9
                if elapsed_since_last < adaptive_interval:
                    remaining = round(adaptive_interval - elapsed_since_last, 2)
                    _NEXT_MODEL_ATTEMPT_EPOCH_S = max(_NEXT_MODEL_ATTEMPT_EPOCH_S, last_request + adaptive_interval)
                    _LAST_MODEL_SKIP_REASON = "adaptive_pacing"
                    if now_s - _LAST_PACING_LOG_EPOCH_S >= 1.0:
                        _LAST_PACING_LOG_EPOCH_S = now_s
                        _debug_log(
                            "model-provider-debug",
                            "H10",
                            "model_provider.py:_reserve_dispatch_slot",
                            f"Waiting for {provider_label} adaptive min-interval pacing",
                            {
                                "remaining_seconds": remaining,
                                "adaptive_interval_seconds": adaptive_interval,
                                "last_request_epoch_s": last_request,
                            },
                        )
                    wait_s = max(0.05, adaptive_interval - elapsed_since_last)
                else:
                    state["last_request_epoch_s"] = now_s
                    state["updated_at_epoch_s"] = now_s
                    _write_rate_state(rate_state_path, state)
                    _NEXT_MODEL_ATTEMPT_EPOCH_S = max(_NEXT_MODEL_ATTEMPT_EPOCH_S, now_s + adaptive_interval)
                    _LAST_MODEL_SKIP_REASON = ""
                    return state
        time.sleep(min(wait_s, 2.0))


# region agent log
def _debug_log(run_id: str, hypothesis_id: str, location: str, message: str, data: dict[str, Any]) -> None:
    try:
        payload = {
            "sessionId": DEBUG_SESSION_ID,
            "runId": run_id,
            "hypothesisId": hypothesis_id,
            "location": location,
            "message": message,
            "data": data,
            "timestamp": int(__import__("time").time() * 1000),
        }
        with DEBUG_LOG_PATH.open("a", encoding="utf-8") as f:
            f.write(json.dumps(payload, ensure_ascii=False) + "\n")
    except Exception:
        pass
# endregion


def _default_rate_state() -> dict[str, Any]:
    return {
        "adaptive_min_interval_seconds": 2.0,
        "last_request_epoch_s": 0.0,
        "success_count": 0,
        "rate_limited_count": 0,
        "updated_at_epoch_s": 0.0,
    }


def _load_rate_state(rate_state_path: Path | None) -> dict[str, Any]:
    if rate_state_path is None or not rate_state_path.exists():
        return _default_rate_state()
    try:
        payload = read_json(rate_state_path)
        if isinstance(payload, dict):
            state = _default_rate_state()
            state.update(payload)
            return state
    except Exception:
        pass
    return _default_rate_state()


def _write_rate_state(rate_state_path: Path | None, state: dict[str, Any]) -> None:
    if rate_state_path is None:
        return
    try:
        rate_state_path.parent.mkdir(parents=True, exist_ok=True)
        with rate_state_path.open("w", encoding="utf-8") as f:
            f.write(json.dumps(state, ensure_ascii=False, indent=2))
    except Exception:
        pass


def model_task_settings(
    provider_config: dict[str, Any] | None,
    task_name: str,
    *,
    profile_override: str | None = None,
) -> dict[str, Any]:
    config = provider_config if isinstance(provider_config, dict) else {}
    base = dict(config.get("model_provider", {}) if isinstance(config.get("model_provider", {}), dict) else {})
    strategy = config.get("model_strategy", {}) if isinstance(config.get("model_strategy", {}), dict) else {}
    if strategy.get("temperature") is not None and "temperature" not in base:
        base["temperature"] = strategy["temperature"]
    routing = config.get("model_routing", {}) if isinstance(config.get("model_routing", {}), dict) else {}
    profiles = routing.get("profiles", {}) if isinstance(routing.get("profiles", {}), dict) else {}
    tasks = routing.get("tasks", {}) if isinstance(routing.get("tasks", {}), dict) else {}
    task_cfg = tasks.get(task_name, {}) if isinstance(tasks.get(task_name, {}), dict) else {}

    profile_name = str(profile_override or "").strip() or str(
        task_cfg.get("profile")
        or task_cfg.get("model_profile")
        or routing.get("default_profile")
        or ""
    ).strip()
    if profile_name and isinstance(profiles.get(profile_name), dict):
        base.update(profiles[profile_name])

    for key, value in task_cfg.items():
        if key in {"profile", "model_profile"}:
            continue
        base[key] = value
    return base


def model_call_kwargs(
    provider_config: dict[str, Any] | None,
    task_name: str,
    *,
    profile_override: str | None = None,
) -> dict[str, Any]:
    cfg = model_task_settings(provider_config, task_name, profile_override=profile_override)
    session_id = _clean_openrouter_trace_text(
        cfg.get("session_id") or cfg.get("openrouter_session_id") or _default_openrouter_session_id(task_name)
    )
    kwargs = {
        "base_url": str(cfg.get("base_url", "http://127.0.0.1:11434")),
        "model": str(cfg.get("model", "llama3.1")),
        "temperature": float(cfg.get("temperature", 0.0)),
        "timeout_seconds": int(cfg.get("timeout_seconds", 60)),
        "provider": str(cfg.get("provider", "auto")),
        "api_base_url": str(cfg.get("api_base_url", "https://openrouter.ai/api/v1")),
        "api_model": str(cfg.get("api_model", "qwen/qwen3.5-flash-02-23")),
        "api_retries": int(cfg.get("api_retries", 2)),
        "rate_limit_cooldown_seconds": int(cfg.get("rate_limit_cooldown_seconds", 90)),
        "rate_state_path": Path(str(cfg.get("rate_state_path", "artifacts/learning/model_provider_rate_runtime.json"))),
        "min_interval_seconds": float(cfg.get("adaptive_min_interval_seconds", 2.0)),
        "max_interval_seconds": float(cfg.get("adaptive_max_interval_seconds", 120.0)),
        "success_decay": float(cfg.get("adaptive_success_decay", 0.9)),
        "rate_limit_growth": float(cfg.get("adaptive_rate_limit_growth", 1.8)),
        "max_tokens": int(cfg.get("max_tokens", 4096)),
        "session_id": session_id,
        "trace": _openrouter_trace_for_task(task_name, session_id, cfg),
    }
    if cfg.get("user"):
        kwargs["user"] = _clean_openrouter_trace_text(cfg.get("user"))
    if isinstance(cfg.get("tools"), list):
        kwargs["tools"] = cfg["tools"]
    if isinstance(cfg.get("json_schema"), dict):
        kwargs["json_schema"] = cfg["json_schema"]
    if isinstance(cfg.get("api_extra_body"), dict):
        kwargs["api_extra_body"] = cfg["api_extra_body"]
    api_key_env = str(cfg.get("api_key_env", "") or "").strip()
    if api_key_env:
        kwargs["api_key_env"] = api_key_env
    if profile_override:
        kwargs["routing_profile"] = profile_override
    return kwargs


def model_parallel_wave_size(provider_config: dict[str, Any] | None, task_name: str, default: int = 75) -> int:
    cfg = model_task_settings(provider_config, task_name)
    return max(1, int(cfg.get("parallel_wave_size", default) or default))


def call_model_chat_with_pacing_retries(
    prompt: str,
    *,
    provider_config: dict[str, Any] | None = None,
    task_name: str,
    max_provider_attempts: int = 4,
    provider_retry_sleep_seconds: float = 2.0,
    **kwargs: Any,
) -> dict[str, Any] | None:
    call_kwargs = dict(kwargs)
    if not call_kwargs:
        call_kwargs = model_call_kwargs(provider_config, task_name)
    attempts = max(1, int(max_provider_attempts))
    for attempt_idx in range(1, attempts + 1):
        payload = call_model_chat(prompt=prompt, **call_kwargs)
        if payload is not None:
            return payload
        status = get_model_runtime_status()
        reason = str(status.get("last_model_skip_reason") or "provider_unavailable")
        if reason in PACING_SKIP_REASONS:
            time.sleep(provider_wait_seconds(reason, status, provider_retry_sleep_seconds))
            continue
        if attempt_idx < attempts:
            time.sleep(max(0.0, float(provider_retry_sleep_seconds)))
    return None


def call_model_chats_parallel(
    jobs: list[dict[str, Any]],
    provider_config: dict[str, Any] | None,
    task_name: str,
    *,
    max_workers: int | None = None,
    max_provider_attempts: int = 4,
    provider_retry_sleep_seconds: float = 2.0,
) -> dict[str, dict[str, Any]]:
    if not jobs:
        return {}

    workers = max(1, int(max_workers or model_max_concurrent_requests(provider_config, task_name, default=1)))
    logger = get_logger(__name__)

    def _run_job(job: dict[str, Any]) -> tuple[str, dict[str, Any]]:
        key = str(job.get("key") or "")
        job_task_name = str(job.get("task_name") or task_name)
        prompt = str(job.get("prompt") or "")
        call_kwargs = dict(job.get("call_kwargs") or model_call_kwargs(provider_config, job_task_name))
        try:
            payload = call_model_chat_with_pacing_retries(
                prompt,
                task_name=job_task_name,
                max_provider_attempts=max_provider_attempts,
                provider_retry_sleep_seconds=provider_retry_sleep_seconds,
                **call_kwargs,
            )
        except Exception as exc:
            return key, {"payload": None, "error": f"model_call_failed: {exc}"}
        if payload is not None:
            return key, {"payload": payload, "error": ""}
        reason = str(get_model_runtime_status().get("last_model_skip_reason") or "model_call_failed")
        return key, {"payload": None, "error": reason}

    if workers == 1:
        return {key: result for key, result in (_run_job(job) for job in jobs) if key}

    results: dict[str, dict[str, Any]] = {}
    results_lock = threading.Lock()
    in_flight = threading.Semaphore(workers)
    logger.info("Dispatching %d model job(s) with max_concurrent_requests=%d for task=%s.", len(jobs), workers, task_name)

    def _guarded_run(job: dict[str, Any]) -> tuple[str, dict[str, Any]]:
        in_flight.acquire()
        try:
            return _run_job(job)
        finally:
            in_flight.release()

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = [executor.submit(_guarded_run, job) for job in jobs]
        for future in as_completed(futures):
            key, result = future.result()
            if key:
                with results_lock:
                    results[key] = result
    return results


def build_prompt(
    snippet_text: str,
    profile: dict[str, Any],
    seed_entities: list[str],
    heuristic_anchor_candidates: list[str],
) -> str:
    seed_preview = seed_entities[:40]
    anchor_preview = heuristic_anchor_candidates[:10]
    return f"""You classify Discord snippets for Theriac canon extraction.
Be conservative and avoid speculation. Return strict JSON only with no markdown.

Thread profile:
- profile_type: {profile.get("profile_type", "unknown_low_signal")}
- strictness_level: {profile.get("strictness_level", "strict")}

Seed entities:
{seed_preview}

Heuristic anchor candidates:
{anchor_preview}
Note: these heuristic candidates are weak priors; ignore them when unsupported by the snippet.

Snippet:
\"\"\"{snippet_text}\"\"\"

Return JSON object with this exact shape:
{{
  "theriac_relevance": 0.0,
  "knowledge_track": "lore|meta|unknown",
  "anchor_candidates": [{{"name":"", "confidence":0.0, "basis":""}}],
  "reasoning_short": "",
  "suggested_thematic_markers": {{
    "historical": ["string"],
    "music": ["string"]
  }}
}}
"""


def _read_env_value_from_file(repo_root: Path, var_names: list[str]) -> str | None:
    env_path = repo_root / ".env"
    if not env_path.exists():
        return None
    try:
        text = env_path.read_text(encoding="utf-8", errors="replace")
    except Exception:
        return None
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        for var_name in var_names:
            pattern = rf"^\s*{re.escape(var_name)}\s*[:=]\s*(.+?)\s*$"
            m = re.match(pattern, stripped)
            if m:
                raw = m.group(1).strip()
                if (raw.startswith('"') and raw.endswith('"')) or (raw.startswith("'") and raw.endswith("'")):
                    raw = raw[1:-1]
                if raw:
                    return raw
    return None


def _resolve_generic_api_key(api_key_env: str | None = None) -> str | None:
    global _CACHED_API_KEY, _HAS_CACHED_API_KEY
    env_candidates = [
        str(api_key_env or "").strip(),
        "NVIDIA_API_KEY",
        "DEEPSEEK_API_KEY",
        "MODEL_API_KEY",
        "MODEL_PROVIDER_API_KEY",
        "OPENAI_COMPATIBLE_API_KEY",
    ]
    env_candidates = [name for name in env_candidates if name]
    if api_key_env:
        for key_name in env_candidates:
            value = os.environ.get(key_name)
            if value and value.strip():
                return value.strip().strip('"').strip("'")
        repo_root = Path(__file__).resolve().parents[1]
        file_value = _read_env_value_from_file(repo_root, env_candidates)
        return file_value

    if _HAS_CACHED_API_KEY:
        return _CACHED_API_KEY
    for key_name in env_candidates:
        value = os.environ.get(key_name)
        if value and value.strip():
            _CACHED_API_KEY = value.strip().strip('"').strip("'")
            _HAS_CACHED_API_KEY = True
            # region agent log
            _debug_log("model-provider-debug", "H1", "model_provider.py:_resolve_generic_api_key", "Resolved API key from process env", {"key_name": key_name, "source": "process_env"})
            # endregion
            return _CACHED_API_KEY
    repo_root = Path(__file__).resolve().parents[1]
    file_value = _read_env_value_from_file(repo_root, env_candidates)
    _CACHED_API_KEY = file_value
    _HAS_CACHED_API_KEY = True
    # region agent log
    _debug_log(
        "model-provider-debug",
        "H1",
        "model_provider.py:_resolve_generic_api_key",
        "Resolved API key from .env probe",
        {"found": bool(file_value), "source": ".env", "repo_root": str(repo_root)},
    )
    # endregion
    return _CACHED_API_KEY


def _resolve_openrouter_api_key() -> str | None:
    global _CACHED_OPENROUTER_API_KEY, _HAS_CACHED_OPENROUTER_API_KEY
    if _HAS_CACHED_OPENROUTER_API_KEY:
        return _CACHED_OPENROUTER_API_KEY

    env_candidates = ["OPENROUTER_API_KEY", "OPENROUTER_KEY", "OPEN_ROUTER_API_KEY"]
    for key_name in env_candidates:
        value = os.environ.get(key_name)
        if value and value.strip():
            _CACHED_OPENROUTER_API_KEY = value.strip().strip('"').strip("'")
            _HAS_CACHED_OPENROUTER_API_KEY = True
            _debug_log(
                "model-provider-debug",
                "H13",
                "model_provider.py:_resolve_openrouter_api_key",
                "Resolved OpenRouter API key from process env",
                {"key_name": key_name, "source": "process_env"},
            )
            return _CACHED_OPENROUTER_API_KEY

    repo_root = Path(__file__).resolve().parents[1]
    file_value = _read_env_value_from_file(repo_root, env_candidates)
    _CACHED_OPENROUTER_API_KEY = file_value
    _HAS_CACHED_OPENROUTER_API_KEY = True
    _debug_log(
        "model-provider-debug",
        "H13",
        "model_provider.py:_resolve_openrouter_api_key",
        "Resolved OpenRouter API key from .env probe",
        {"found": bool(file_value), "source": ".env", "repo_root": str(repo_root)},
    )
    return _CACHED_OPENROUTER_API_KEY


def _coerce_json_root(parsed_content: Any, logger) -> dict[str, Any] | None:
    if isinstance(parsed_content, list):
        return {"_json_root": parsed_content, "_json_root_type": "list"}
    if not isinstance(parsed_content, dict):
        logger.warning("Model message JSON root was %s (expected object).", type(parsed_content).__name__)
        return None
    return parsed_content


def _balanced_json_object_candidates(content: str) -> list[str]:
    candidates: list[str] = []
    start: int | None = None
    depth = 0
    in_string = False
    escaped = False
    for idx, char in enumerate(content):
        if in_string:
            if escaped:
                escaped = False
            elif char == "\\":
                escaped = True
            elif char == '"':
                in_string = False
            continue
        if char == '"':
            in_string = True
            continue
        if char == "{":
            if depth == 0:
                start = idx
            depth += 1
            continue
        if char == "}" and depth:
            depth -= 1
            if depth == 0 and start is not None:
                candidates.append(content[start : idx + 1])
                start = None
    return candidates


def _parse_json_content(content: str, logger) -> dict[str, Any] | None:
    normalized = content.strip()
    attempts: list[tuple[str, str]] = [("raw", normalized)]
    fenced_matches = re.findall(r"```(?:json)?\s*(.*?)\s*```", normalized, flags=re.DOTALL | re.IGNORECASE)
    for fenced in fenced_matches:
        attempts.append(("fenced", fenced.strip()))
    if normalized.startswith("```") and len(fenced_matches) == 1:
        _debug_log(
            "model-provider-debug",
            "H8",
            "model_provider.py:_parse_json_content",
            "Stripped fenced code block from model content",
            {"had_fence": True, "preview": fenced_matches[0].strip()[:120]},
        )
    for candidate in _balanced_json_object_candidates(normalized):
        attempts.append(("balanced_object", candidate.strip()))

    seen: set[str] = set()
    for source, candidate in attempts:
        if not candidate or candidate in seen:
            continue
        seen.add(candidate)
        try:
            parsed_content = json.loads(candidate)
        except json.JSONDecodeError:
            continue
        coerced = _coerce_json_root(parsed_content, logger)
        if coerced is not None:
            if source != "raw":
                _debug_log(
                    "model-provider-debug",
                    "H8",
                    "model_provider.py:_parse_json_content",
                    "Recovered JSON object from wrapped model content",
                    {"source": source, "content_preview": normalized[:180]},
                )
            return coerced

    logger.warning("Model message content was not valid JSON. content_preview=%s", normalized[:300].replace("\n", "\\n"))
    return None


def _call_openai_compatible_chat(
    api_base_url: str,
    api_key: str,
    model: str,
    prompt: str,
    temperature: float,
    timeout_seconds: int,
    retries: int = 2,
    rate_limit_cooldown_seconds: int = 90,
    rate_state_path: Path | None = None,
    min_interval_seconds: float = 2.0,
    max_interval_seconds: float = 120.0,
    success_decay: float = 0.9,
    rate_limit_growth: float = 1.8,
    max_tokens: int | None = None,
    response_format_json: bool = False,
    json_schema: dict[str, Any] | None = None,
    tools: list[dict[str, Any]] | None = None,
    session_id: str | None = None,
    trace: dict[str, Any] | None = None,
    user: str | None = None,
    extra_headers: dict[str, str] | None = None,
    api_extra_body: dict[str, Any] | None = None,
    provider_label: str = "OpenAI-Compatible API",
) -> dict[str, Any] | None:
    logger = get_logger(__name__)
    global _RATE_LIMITED_UNTIL_EPOCH_S, _NEXT_MODEL_ATTEMPT_EPOCH_S, _LAST_MODEL_SKIP_REASON
    global _LAST_CALL_BILLED_COST_USD, _LAST_CALL_API_MODEL, _LAST_CALL_PROVIDER
    state = _reserve_dispatch_slot(
        rate_state_path=rate_state_path,
        min_interval_seconds=min_interval_seconds,
        max_interval_seconds=max_interval_seconds,
        provider_label=provider_label,
    )

    endpoint = f"{api_base_url.rstrip('/')}/chat/completions"
    logger.debug(
        "Calling %s endpoint=%s model=%s timeout=%ss temperature=%.2f prompt_chars=%d",
        provider_label,
        endpoint,
        model,
        timeout_seconds,
        temperature,
        len(prompt),
    )
    # region agent log
    _debug_log(
        "model-provider-debug",
        "H2",
        "model_provider.py:_call_openai_compatible_chat",
        f"Attempting {provider_label} request",
        {"endpoint": endpoint, "model": model, "timeout_seconds": timeout_seconds},
    )
    # endregion
    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": "You are a precise JSON classifier."},
            {"role": "user", "content": prompt},
        ],
        "temperature": temperature,
    }
    clean_session_id = _clean_openrouter_trace_text(session_id)
    if clean_session_id:
        payload["session_id"] = clean_session_id
    if isinstance(trace, dict) and trace:
        payload["trace"] = trace
    clean_user = _clean_openrouter_trace_text(user)
    if clean_user:
        payload["user"] = clean_user
    if max_tokens is not None:
        payload["max_tokens"] = max(256, int(max_tokens))
    if tools:
        payload["tools"] = tools
    if json_schema:
        payload["response_format"] = {
            "type": "json_schema",
            "json_schema": {
                "name": "structured_output",
                "strict": True,
                "schema": json_schema
            }
        }
    elif response_format_json:
        payload["response_format"] = {"type": "json_object"}
    if isinstance(api_extra_body, dict):
        for key, value in api_extra_body.items():
            payload[key] = value
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {api_key}",
    }
    if clean_session_id:
        headers["X-Session-Id"] = clean_session_id
    if extra_headers:
        headers.update({str(key): str(value) for key, value in extra_headers.items() if str(value).strip()})
    req = urllib.request.Request(
        url=endpoint,
        data=json.dumps(payload).encode("utf-8"),
        headers=headers,
        method="POST",
    )
    raw_body: str | None = None
    attempts = max(1, int(retries) + 1)
    last_error: str | None = None
    for attempt_idx in range(1, attempts + 1):
        state["last_request_epoch_s"] = time.time()
        state["updated_at_epoch_s"] = state["last_request_epoch_s"]
        _write_rate_state(rate_state_path, state)
        # region agent log
        _debug_log(
            "model-provider-debug",
            "H7",
            "model_provider.py:_call_openai_compatible_chat",
            f"{provider_label} attempt started",
            {"attempt": attempt_idx, "attempts": attempts, "timeout_seconds": timeout_seconds},
        )
        # endregion
        import socket
        orig_timeout = socket.getdefaulttimeout()
        try:
            socket.setdefaulttimeout(float(timeout_seconds))
            try:
                with urllib.request.urlopen(req, timeout=timeout_seconds) as resp:
                    raw_body = resp.read().decode("utf-8")
                    break
            finally:
                socket.setdefaulttimeout(orig_timeout)
        except urllib.error.HTTPError as exc:
            err_body = ""
            try:
                err_body = exc.read().decode("utf-8", errors="replace")
            except Exception:
                err_body = "(unable to read HTTP error body)"
            logger.warning(
                "%s HTTP error status=%s reason=%s endpoint=%s body_preview=%s",
                provider_label,
                exc.code,
                exc.reason,
                endpoint,
                err_body[:300].replace("\n", "\\n"),
            )
            # region agent log
            _debug_log(
                "model-provider-debug",
                "H2",
                "model_provider.py:_call_openai_compatible_chat",
                f"{provider_label} HTTP error",
                {"status": int(exc.code), "reason": str(exc.reason), "body_preview": err_body[:120], "attempt": attempt_idx},
            )
            # endregion
            if int(exc.code) == 429:
                with _PROVIDER_STATE_LOCK:
                    _RATE_LIMITED_UNTIL_EPOCH_S = time.time() + max(1, int(rate_limit_cooldown_seconds))
                    _NEXT_MODEL_ATTEMPT_EPOCH_S = max(_NEXT_MODEL_ATTEMPT_EPOCH_S, _RATE_LIMITED_UNTIL_EPOCH_S)
                    state["rate_limited_count"] = int(state.get("rate_limited_count", 0)) + 1
                    grown = float(state.get("adaptive_min_interval_seconds", min_interval_seconds)) * float(rate_limit_growth)
                    state["adaptive_min_interval_seconds"] = max(
                        float(min_interval_seconds),
                        min(float(max_interval_seconds), grown),
                    )
                    state["updated_at_epoch_s"] = time.time()
                    _write_rate_state(rate_state_path, state)
                    _LAST_MODEL_SKIP_REASON = "rate_limited_429"
                # region agent log
                _debug_log(
                    "model-provider-debug",
                    "H9",
                    "model_provider.py:_call_openai_compatible_chat",
                    f"Activated rate-limit cooldown after {provider_label} 429",
                    {
                        "cooldown_seconds": int(rate_limit_cooldown_seconds),
                        "rate_limited_until_epoch_s": _RATE_LIMITED_UNTIL_EPOCH_S,
                        "adaptive_min_interval_seconds": state.get("adaptive_min_interval_seconds"),
                    },
                )
                # endregion
            elif int(exc.code) in {502, 503, 504}:
                _LAST_MODEL_SKIP_REASON = f"http_error_{int(exc.code)}"
                if attempt_idx < attempts:
                    retry_sleep_seconds = min(30.0, 5.0 * attempt_idx)
                    time.sleep(retry_sleep_seconds)
                    continue
            else:
                _LAST_MODEL_SKIP_REASON = f"http_error_{int(exc.code)}"
            return None
        except (urllib.error.URLError, TimeoutError) as exc:
            last_error = f"{type(exc).__name__}: {exc}"
            _LAST_MODEL_SKIP_REASON = "connection_error"
            logger.warning("%s connection failure endpoint=%s error=%s", provider_label, endpoint, exc)
            # region agent log
            _debug_log(
                "model-provider-debug",
                "H2",
                "model_provider.py:_call_openai_compatible_chat",
                f"{provider_label} connection failure",
                {"error_type": type(exc).__name__, "error": str(exc), "attempt": attempt_idx},
            )
            # endregion
            if attempt_idx < attempts:
                # Use bounded exponential backoff to avoid tight retry loops.
                retry_sleep_seconds = min(8.0, 0.5 * (2 ** (attempt_idx - 1)))
                time.sleep(retry_sleep_seconds)
        except Exception as exc:
            last_error = f"{type(exc).__name__}: {exc}"
            _LAST_MODEL_SKIP_REASON = "unexpected_error"
            logger.warning("%s request failed unexpectedly endpoint=%s error=%s", provider_label, endpoint, exc)
    if raw_body is None:
        # region agent log
        _debug_log(
            "model-provider-debug",
            "H7",
            "model_provider.py:_call_openai_compatible_chat",
            f"{provider_label} attempts exhausted",
            {"attempts": attempts, "last_error": last_error or "unknown"},
        )
        # endregion
        if not _LAST_MODEL_SKIP_REASON:
            _LAST_MODEL_SKIP_REASON = "attempts_exhausted"
        return None

    try:
        body = json.loads(raw_body)
    except json.JSONDecodeError:
        _LAST_MODEL_SKIP_REASON = "invalid_json"
        logger.warning("%s response was not valid JSON. response_preview=%s", provider_label, raw_body[:300].replace("\n", "\\n"))
        return None
    if not isinstance(body, dict):
        _LAST_MODEL_SKIP_REASON = "invalid_envelope"
        logger.warning("%s response root was %s (expected object).", provider_label, type(body).__name__)
        return None
    choices = body.get("choices", [])
    if not isinstance(choices, list) or not choices:
        _LAST_MODEL_SKIP_REASON = "missing_choices"
        logger.warning("%s response missing choices.", provider_label)
        return None
    first = choices[0] if isinstance(choices[0], dict) else {}
    message = first.get("message", {}) if isinstance(first, dict) else {}
    content = str(message.get("content", "")).strip()
    if not content:
        _LAST_MODEL_SKIP_REASON = "empty_content"
        logger.warning("%s response contained no assistant message content.", provider_label)
        return None
    parsed_content = _parse_json_content(content, logger)
    if parsed_content is None:
        _LAST_MODEL_SKIP_REASON = "content_parse_failed"
        # region agent log
        _debug_log(
            "model-provider-debug",
            "H4",
            "model_provider.py:_call_openai_compatible_chat",
            f"{provider_label} content parse failed",
            {"content_preview": content[:120]},
        )
        # endregion
        return None
    state["success_count"] = int(state.get("success_count", 0)) + 1
    decayed = float(state.get("adaptive_min_interval_seconds", min_interval_seconds)) * float(success_decay)
    state["adaptive_min_interval_seconds"] = max(float(min_interval_seconds), min(float(max_interval_seconds), decayed))
    state["updated_at_epoch_s"] = time.time()
    with _PROVIDER_STATE_LOCK:
        _NEXT_MODEL_ATTEMPT_EPOCH_S = max(
            _NEXT_MODEL_ATTEMPT_EPOCH_S,
            state["last_request_epoch_s"] + state["adaptive_min_interval_seconds"],
        )
        _LAST_MODEL_SKIP_REASON = ""
        _LAST_CALL_BILLED_COST_USD = _extract_billed_cost_usd(body)
        _LAST_CALL_API_MODEL = str(model or "")
        _LAST_CALL_PROVIDER = str(provider_label or "")
    _write_rate_state(rate_state_path, state)
    # region agent log
    _debug_log(
        "model-provider-debug",
        "H10",
        "model_provider.py:_call_openai_compatible_chat",
        f"{provider_label} success updated adaptive pacing state",
        {
            "success_count": state.get("success_count"),
            "adaptive_min_interval_seconds": state.get("adaptive_min_interval_seconds"),
            "rate_state_path": str(rate_state_path) if rate_state_path else None,
        },
    )
    # endregion
    logger.debug("Parsed OpenAI-Compatible API content keys=%s", sorted(parsed_content.keys()))
    return parsed_content


def _call_openrouter_chat(
    api_base_url: str,
    api_key: str,
    model: str,
    prompt: str,
    temperature: float,
    timeout_seconds: int,
    retries: int = 2,
    rate_limit_cooldown_seconds: int = 90,
    rate_state_path: Path | None = None,
    min_interval_seconds: float = 0.5,
    max_interval_seconds: float = 120.0,
    success_decay: float = 0.9,
    rate_limit_growth: float = 1.8,
    max_tokens: int = 4096,
    json_schema: dict[str, Any] | None = None,
    tools: list[dict[str, Any]] | None = None,
    session_id: str | None = None,
    trace: dict[str, Any] | None = None,
    user: str | None = None,
) -> dict[str, Any] | None:
    clean_session_id = _clean_openrouter_trace_text(session_id) or stable_id("or_session", "openrouter", model)
    trace_payload: dict[str, Any] = dict(trace) if isinstance(trace, dict) else {}
    trace_payload.setdefault("trace_id", clean_session_id)
    trace_payload.setdefault("trace_name", "Theriac OpenRouter API")
    trace_payload.setdefault("span_name", "openrouter_chat")
    trace_payload.setdefault("generation_name", model)
    return _call_openai_compatible_chat(
        api_base_url,
        api_key,
        model,
        prompt,
        temperature,
        timeout_seconds,
        retries=retries,
        rate_limit_cooldown_seconds=rate_limit_cooldown_seconds,
        rate_state_path=rate_state_path,
        min_interval_seconds=min_interval_seconds,
        max_interval_seconds=max_interval_seconds,
        success_decay=success_decay,
        rate_limit_growth=rate_limit_growth,
        max_tokens=max_tokens,
        response_format_json=True,
        json_schema=json_schema,
        tools=tools,
        session_id=clean_session_id,
        trace=trace_payload,
        user=user,
        extra_headers={
            "HTTP-Referer": "https://github.com/theriac/lore-bible",
            "X-Title": "Theriac Lore Bible",
        },
        provider_label="OpenRouter API",
    )


def call_model_chat(
    base_url: str,
    model: str,
    prompt: str,
    temperature: float,
    timeout_seconds: int,
    provider: str = "auto",
    api_base_url: str = "https://openrouter.ai/api/v1",
    api_model: str = "qwen/qwen3.5-flash-02-23",
    api_retries: int = 2,
    rate_limit_cooldown_seconds: int = 90,
    rate_state_path: Path | None = None,
    min_interval_seconds: float = 2.0,
    max_interval_seconds: float = 120.0,
    success_decay: float = 0.9,
    rate_limit_growth: float = 1.8,
    max_tokens: int = 4096,
    json_schema: dict[str, Any] | None = None,
    tools: list[dict[str, Any]] | None = None,
    session_id: str | None = None,
    trace: dict[str, Any] | None = None,
    user: str | None = None,
    api_extra_body: dict[str, Any] | None = None,
    api_key_env: str | None = None,
    routing_profile: str | None = None,
) -> dict[str, Any] | None:
    logger = get_logger(__name__)
    global _LAST_PROVIDER_RESOLVE_LOG_EPOCH_S, _LAST_API_FAILURE_LOG_EPOCH_S, _LAST_MODEL_SKIP_REASON
    now_s = time.time()
    resolved_provider = (provider or "openrouter").lower()
    if resolved_provider in {"google", "google_ai", "google_ai_studio", "gemini_api", "gemini"}:
        resolved_provider = "openrouter"
    if resolved_provider in {"open_router", "openrouter_api"}:
        resolved_provider = "openrouter"
    if resolved_provider in {"nvidia", "nim", "nvidia_nim"}:
        resolved_provider = "openai_compatible"
    if resolved_provider == "auto":
        resolved_provider = "openrouter"
    supported_providers = {"openrouter", "openai_compatible", "api"}
    if resolved_provider not in supported_providers:
        logger.warning("Unsupported model provider selected: %s.", resolved_provider)
        _LAST_MODEL_SKIP_REASON = "unsupported_provider"
        return None

    openrouter_key = _resolve_openrouter_api_key() if resolved_provider == "openrouter" else None
    api_key = (
        _resolve_generic_api_key(api_key_env)
        if resolved_provider in {"api", "openai_compatible"}
        else None
    )
    if now_s - _LAST_PROVIDER_RESOLVE_LOG_EPOCH_S >= 1.0:
        _LAST_PROVIDER_RESOLVE_LOG_EPOCH_S = now_s
        # region agent log
        _debug_log(
            "model-provider-debug",
            "H5",
            "model_provider.py:call_model_chat",
            "Resolved provider mode before dispatch",
            {
                "provider": resolved_provider,
                "has_api_key": bool(api_key or openrouter_key),
                "api_model": api_model,
                "routing_profile": routing_profile or "",
            },
        )
        # endregion
    if routing_profile:
        _LAST_CALL_PROVIDER = routing_profile

    if resolved_provider == "openrouter":
        if not openrouter_key:
            logger.warning("OpenRouter provider selected but no OPENROUTER_API_KEY/OPENROUTER_KEY found in environment/.env.")
            _LAST_MODEL_SKIP_REASON = "missing_api_key"
            return None
        openrouter_base_url = api_base_url
        if "openrouter.ai" not in openrouter_base_url:
            openrouter_base_url = "https://openrouter.ai/api/v1"
        openrouter_model = api_model or "qwen/qwen3.5-flash-02-23"
        return _call_openrouter_chat(
            openrouter_base_url,
            openrouter_key,
            openrouter_model,
            prompt,
            temperature,
            timeout_seconds,
            retries=api_retries,
            rate_limit_cooldown_seconds=rate_limit_cooldown_seconds,
            rate_state_path=rate_state_path,
            min_interval_seconds=min_interval_seconds,
            max_interval_seconds=max_interval_seconds,
            success_decay=success_decay,
            rate_limit_growth=rate_limit_growth,
            max_tokens=max_tokens,
            json_schema=json_schema,
            tools=tools,
            session_id=session_id,
            trace=trace,
            user=user,
        )

    if resolved_provider in {"openai_compatible", "api"}:
        if not api_key:
            logger.warning(
                "OpenAI-compatible provider selected but no NVIDIA_API_KEY/MODEL_API_KEY/"
                "MODEL_PROVIDER_API_KEY/OPENAI_COMPATIBLE_API_KEY found in environment/.env."
            )
            _LAST_MODEL_SKIP_REASON = "missing_api_key"
            return None
        provider_label = "NVIDIA NIM API" if "integrate.api.nvidia.com" in str(api_base_url) else "OpenAI-Compatible API"
        return _call_openai_compatible_chat(
            api_base_url,
            api_key,
            api_model,
            prompt,
            temperature,
            timeout_seconds,
            retries=api_retries,
            rate_limit_cooldown_seconds=rate_limit_cooldown_seconds,
            rate_state_path=rate_state_path,
            min_interval_seconds=min_interval_seconds,
            max_interval_seconds=max_interval_seconds,
            success_decay=success_decay,
            rate_limit_growth=rate_limit_growth,
            max_tokens=max_tokens,
            response_format_json=True,
            json_schema=json_schema,
            tools=tools,
            api_extra_body=api_extra_body,
            provider_label=provider_label,
        )

    return None


def load_seed_entities(seed_path: Path | None) -> list[str]:
    return load_entity_names(seed_path)


def build_stage_01_prompt(doc_excerpt: str) -> str:
    return f"""You extract ontology anchors from a lore bible.
Be conservative. Return strict JSON only.

Task:
- Extract entities that may become lore cards later.
- The lore bible is bootstrap scaffolding only; do not write final card prose.
- Do not extract generic headings such as Project Overview, Remaining Questions, Placeholders, History, Key Organizations, or section labels.
- Keep working names only when clearly used as named concepts.
- Infer one entity type per item from:
  character|faction|organization|location|quest|event|timeline_node|term
- Do not emit abstract themes, motifs, aesthetics, philosophies, or psychological ideas as entities; those belong in the theme profile, not the entity graph.
- Include initial aliases only when explicitly indicated in text.
- Include relationship hints only when clearly stated.
- Theriac quest titles may be named after songs. Do not down-rank, block, or reclassify a named quest solely because it matches a song title. If a song-title name is associated with a path, ending, mission, or quest progression, classify it as quest rather than theme.

Lore excerpt:
\"\"\"{doc_excerpt}\"\"\"

Return JSON object:
{{
  "entities": [
    {{
      "canonical_name": "string",
      "entity_type": "character|faction|organization|location|quest|event|timeline_node|term",
      "source_section_hint": "short source heading or context only, not prose",
      "aliases": ["string"],
      "relationship_hints": [
        {{
          "target_name": "string",
          "relation_type": "string",
          "note": "short basis"
        }}
      ]
    }}
  ],
  "suggested_thematic_markers": {{
    "historical": ["string"],
    "music": ["string"]
  }}
}}
"""