from __future__ import annotations

import json
import os
import re
import time
import urllib.error
import urllib.parse
import urllib.request
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
_CACHED_GEMINI_API_KEY: str | None = None
_HAS_CACHED_GEMINI_API_KEY = False
_CACHED_OPENROUTER_API_KEY: str | None = None
_HAS_CACHED_OPENROUTER_API_KEY = False
_LAST_MODEL_SKIP_REASON = ""

TASK_ROUTING_CONTROL_KEYS = {
    "batch_enabled",
    "batch_initial_max_requests",
    "batch_max_requests",
    "batch_status_log_path",
    "batch_status_log_min_interval_seconds",
    "batch_poll_interval_seconds",
    "batch_timeout_seconds",
    "batch_display_name",
    "batch_abort_on_chunk_failure",
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
        "trace_name": f"THERIAC {task_name}",
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
    return {
        "last_model_skip_reason": _LAST_MODEL_SKIP_REASON,
        "rate_limited_until_epoch_s": _RATE_LIMITED_UNTIL_EPOCH_S,
        "next_model_attempt_epoch_s": _NEXT_MODEL_ATTEMPT_EPOCH_S,
    }


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


def model_task_settings(provider_config: dict[str, Any] | None, task_name: str) -> dict[str, Any]:
    config = provider_config if isinstance(provider_config, dict) else {}
    base = dict(config.get("model_provider", {}) if isinstance(config.get("model_provider", {}), dict) else {})
    routing = config.get("model_routing", {}) if isinstance(config.get("model_routing", {}), dict) else {}
    profiles = routing.get("profiles", {}) if isinstance(routing.get("profiles", {}), dict) else {}
    tasks = routing.get("tasks", {}) if isinstance(routing.get("tasks", {}), dict) else {}
    task_cfg = tasks.get(task_name, {}) if isinstance(tasks.get(task_name, {}), dict) else {}

    profile_name = str(
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


def model_call_kwargs(provider_config: dict[str, Any] | None, task_name: str) -> dict[str, Any]:
    cfg = model_task_settings(provider_config, task_name)
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
    return kwargs


def model_batch_enabled(provider_config: dict[str, Any] | None, task_name: str) -> bool:
    cfg = model_task_settings(provider_config, task_name)
    return bool(cfg.get("batch_enabled", False))


def model_batch_max_requests(provider_config: dict[str, Any] | None, task_name: str, default: int = 100) -> int:
    cfg = model_task_settings(provider_config, task_name)
    return max(1, int(cfg.get("batch_max_requests", default)))


def model_batch_initial_max_requests(provider_config: dict[str, Any] | None, task_name: str, default: int | None = None) -> int:
    cfg = model_task_settings(provider_config, task_name)
    fallback = default if default is not None else int(cfg.get("batch_max_requests", 100))
    return max(1, int(cfg.get("batch_initial_max_requests", fallback)))


def build_prompt(
    snippet_text: str,
    profile: dict[str, Any],
    seed_entities: list[str],
    heuristic_anchor_candidates: list[str],
) -> str:
    seed_preview = seed_entities[:40]
    anchor_preview = heuristic_anchor_candidates[:10]
    return f"""You classify Discord snippets for THERIAC canon extraction.
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


def _resolve_generic_api_key() -> str | None:
    global _CACHED_API_KEY, _HAS_CACHED_API_KEY
    if _HAS_CACHED_API_KEY:
        return _CACHED_API_KEY

    env_candidates = ["MODEL_API_KEY", "MODEL_PROVIDER_API_KEY", "OPENAI_COMPATIBLE_API_KEY"]
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


def _resolve_gemini_api_key() -> str | None:
    global _CACHED_GEMINI_API_KEY, _HAS_CACHED_GEMINI_API_KEY
    if _HAS_CACHED_GEMINI_API_KEY:
        return _CACHED_GEMINI_API_KEY

    env_candidates = ["GEMINI_API_KEY", "GOOGLE_API_KEY", "GOOGLE_AI_API_KEY"]
    for key_name in env_candidates:
        value = os.environ.get(key_name)
        if value and value.strip():
            _CACHED_GEMINI_API_KEY = value.strip().strip('"').strip("'")
            _HAS_CACHED_GEMINI_API_KEY = True
            _debug_log(
                "model-provider-debug",
                "H11",
                "model_provider.py:_resolve_gemini_api_key",
                "Resolved Gemini API key from process env",
                {"key_name": key_name, "source": "process_env"},
            )
            return _CACHED_GEMINI_API_KEY

    repo_root = Path(__file__).resolve().parents[1]
    file_value = _read_env_value_from_file(repo_root, env_candidates)
    _CACHED_GEMINI_API_KEY = file_value
    _HAS_CACHED_GEMINI_API_KEY = True
    _debug_log(
        "model-provider-debug",
        "H11",
        "model_provider.py:_resolve_gemini_api_key",
        "Resolved Gemini API key from .env probe",
        {"found": bool(file_value), "source": ".env", "repo_root": str(repo_root)},
    )
    return _CACHED_GEMINI_API_KEY


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
    provider_label: str = "OpenAI-Compatible API",
) -> dict[str, Any] | None:
    logger = get_logger(__name__)
    global _RATE_LIMITED_UNTIL_EPOCH_S, _NEXT_MODEL_ATTEMPT_EPOCH_S, _LAST_PACING_LOG_EPOCH_S, _LAST_COOLDOWN_LOG_EPOCH_S, _LAST_MODEL_SKIP_REASON
    _LAST_MODEL_SKIP_REASON = ""
    now_s = time.time()
    if _NEXT_MODEL_ATTEMPT_EPOCH_S > now_s:
        _LAST_MODEL_SKIP_REASON = "provider_locked"
        return None
    state = _load_rate_state(rate_state_path)
    adaptive_interval = float(state.get("adaptive_min_interval_seconds", min_interval_seconds))
    if adaptive_interval < float(min_interval_seconds):
        adaptive_interval = float(min_interval_seconds)
    if adaptive_interval > float(max_interval_seconds):
        adaptive_interval = float(max_interval_seconds)
    last_request = float(state.get("last_request_epoch_s", 0.0))
    elapsed_since_last = now_s - last_request if last_request > 0 else 10**9
    if elapsed_since_last < adaptive_interval:
        remaining = round(adaptive_interval - elapsed_since_last, 2)
        _NEXT_MODEL_ATTEMPT_EPOCH_S = max(_NEXT_MODEL_ATTEMPT_EPOCH_S, last_request + adaptive_interval)
        _LAST_MODEL_SKIP_REASON = "adaptive_pacing"
        if now_s - _LAST_PACING_LOG_EPOCH_S >= 1.0:
            _LAST_PACING_LOG_EPOCH_S = now_s
            # region agent log
            _debug_log(
                "model-provider-debug",
                "H10",
                "model_provider.py:_call_openai_compatible_chat",
                f"Skipping {provider_label} due to adaptive min-interval pacing",
                {
                    "remaining_seconds": remaining,
                    "adaptive_interval_seconds": adaptive_interval,
                    "last_request_epoch_s": last_request,
                },
            )
            # endregion
        return None

    if _RATE_LIMITED_UNTIL_EPOCH_S > now_s:
        remaining = round(_RATE_LIMITED_UNTIL_EPOCH_S - now_s, 2)
        _NEXT_MODEL_ATTEMPT_EPOCH_S = max(_NEXT_MODEL_ATTEMPT_EPOCH_S, _RATE_LIMITED_UNTIL_EPOCH_S)
        _LAST_MODEL_SKIP_REASON = "rate_limit_cooldown"
        if now_s - _LAST_COOLDOWN_LOG_EPOCH_S >= 1.0:
            _LAST_COOLDOWN_LOG_EPOCH_S = now_s
            # region agent log
            _debug_log(
                "model-provider-debug",
                "H9",
                "model_provider.py:_call_openai_compatible_chat",
                f"Skipping {provider_label} due to active rate-limit cooldown",
                {"cooldown_remaining_seconds": remaining},
            )
            # endregion
        return None

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
                _LAST_MODEL_SKIP_REASON = "rate_limited_429"
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
    _NEXT_MODEL_ATTEMPT_EPOCH_S = max(_NEXT_MODEL_ATTEMPT_EPOCH_S, state["last_request_epoch_s"] + state["adaptive_min_interval_seconds"])
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
    _LAST_MODEL_SKIP_REASON = ""
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
    trace_payload.setdefault("trace_name", "THERIAC OpenRouter API")
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
            "X-Title": "THERIAC Lore Bible",
        },
        provider_label="OpenRouter API",
    )


def _extract_gemini_text(body: dict[str, Any]) -> str:
    candidates = body.get("candidates", [])
    if not isinstance(candidates, list) or not candidates:
        return ""
    first = candidates[0] if isinstance(candidates[0], dict) else {}
    content = first.get("content", {}) if isinstance(first, dict) else {}
    parts = content.get("parts", []) if isinstance(content, dict) else []
    if not isinstance(parts, list):
        return ""
    text_parts = [str(part.get("text", "")) for part in parts if isinstance(part, dict) and str(part.get("text", "")).strip()]
    return "\n".join(text_parts).strip()


def _gemini_generate_content_request(prompt: str, temperature: float) -> dict[str, Any]:
    return {
        "contents": [
            {
                "role": "user",
                "parts": [
                    {
                        "text": "You are a precise JSON classifier. Return strict JSON only with no markdown.\n\n"
                        + prompt
                    }
                ],
            }
        ],
        "generationConfig": {
            "temperature": temperature,
            "responseMimeType": "application/json",
            "candidateCount": 1,
        },
    }


def _clean_gemini_model(model: str) -> str:
    clean_model = model.strip()
    if clean_model.startswith("models/"):
        clean_model = clean_model[len("models/") :]
    return clean_model


def _gemini_request(
    url: str,
    api_key: str,
    payload: dict[str, Any] | None = None,
    timeout_seconds: int = 60,
    method: str = "POST",
) -> dict[str, Any]:
    data = json.dumps(payload).encode("utf-8") if payload is not None else None
    req = urllib.request.Request(
        url=url,
        data=data,
        headers={
            "Content-Type": "application/json",
            "x-goog-api-key": api_key,
        },
        method=method,
    )
    with urllib.request.urlopen(req, timeout=timeout_seconds) as resp:
        raw_body = resp.read().decode("utf-8")
    body = json.loads(raw_body)
    if not isinstance(body, dict):
        raise RuntimeError(f"Gemini response root was {type(body).__name__}; expected object.")
    return body


def _gemini_batch_state(body: dict[str, Any]) -> str:
    metadata = body.get("metadata", {}) if isinstance(body.get("metadata", {}), dict) else {}
    state = str(metadata.get("state") or body.get("state") or "").strip()
    if state:
        return state
    if bool(body.get("done")) and isinstance(body.get("response"), dict):
        return "BATCH_STATE_SUCCEEDED"
    return "BATCH_STATE_UNKNOWN"


def _gemini_batch_stats(body: dict[str, Any]) -> dict[str, Any]:
    metadata = body.get("metadata", {}) if isinstance(body.get("metadata", {}), dict) else {}
    stats = metadata.get("batchStats", {}) if isinstance(metadata.get("batchStats", {}), dict) else {}
    return {
        "request_count": stats.get("requestCount"),
        "pending_request_count": stats.get("pendingRequestCount"),
        "successful_request_count": stats.get("successfulRequestCount"),
        "failed_request_count": stats.get("failedRequestCount"),
        "update_time": metadata.get("updateTime"),
        "create_time": metadata.get("createTime"),
        "end_time": metadata.get("endTime"),
    }


def _append_batch_status_event(path_value: Any, event: dict[str, Any]) -> None:
    if not path_value:
        return
    try:
        path = Path(str(path_value))
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(event, ensure_ascii=False, sort_keys=True) + "\n")
    except Exception:
        return


def _extract_inline_responses(body: dict[str, Any]) -> list[Any]:
    response = body.get("response", {}) if isinstance(body.get("response", {}), dict) else {}
    metadata = body.get("metadata", {}) if isinstance(body.get("metadata", {}), dict) else {}
    dest = body.get("dest", {}) if isinstance(body.get("dest", {}), dict) else {}
    output = metadata.get("output", {}) if isinstance(metadata.get("output", {}), dict) else {}
    for source in (response, output, dest, metadata, body):
        for key in ("inlinedResponses", "inlined_responses", "inlineResponses", "inline_responses"):
            value = source.get(key) if isinstance(source, dict) else None
            if isinstance(value, list):
                return value
            if isinstance(value, dict):
                for nested_key in ("inlinedResponses", "inlined_responses", "inlineResponses", "inline_responses"):
                    nested_value = value.get(nested_key)
                    if isinstance(nested_value, list):
                        return nested_value
    return []


def _inline_response_key(item: Any, fallback_key: str) -> str:
    if not isinstance(item, dict):
        return fallback_key
    candidates = [item]
    inline_response = item.get("inlineResponse") or item.get("inline_response")
    if isinstance(inline_response, dict):
        candidates.append(inline_response)
    for candidate in candidates:
        metadata = candidate.get("metadata", {}) if isinstance(candidate.get("metadata", {}), dict) else {}
        key = metadata.get("key") or candidate.get("key")
        if key:
            return str(key)
    return fallback_key


def _inline_response_payload(item: Any, logger) -> tuple[dict[str, Any] | None, str]:
    if not isinstance(item, dict):
        return None, "inline_response_not_object"
    source = item.get("inlineResponse") or item.get("inline_response") or item
    if not isinstance(source, dict):
        return None, "inline_response_not_object"
    error = source.get("error") or item.get("error")
    if error:
        return None, f"batch_item_error: {json.dumps(error, ensure_ascii=False)[:300]}"
    response = source.get("response") or source.get("inlineResponse") or source.get("inline_response")
    if not isinstance(response, dict):
        return None, "batch_item_missing_response"
    content = _extract_gemini_text(response)
    if not content:
        return None, "batch_item_empty_content"
    parsed = _parse_json_content(content, logger)
    if parsed is None:
        return None, "batch_item_content_parse_failed"
    return parsed, ""


def call_gemini_batch_json(
    provider_config: dict[str, Any] | None,
    task_name: str,
    requests: Iterable[dict[str, str]],
) -> dict[str, dict[str, Any]]:
    logger = get_logger(__name__)
    task_cfg = model_task_settings(provider_config, task_name)
    provider = str(task_cfg.get("provider", "auto")).lower()
    if provider in {"google", "google_ai", "google_ai_studio", "gemini_api"}:
        provider = "gemini"
    if provider != "gemini":
        raise RuntimeError(f"Batch mode currently requires Gemini provider; got provider={provider!r}.")

    api_key = _resolve_gemini_api_key()
    if not api_key:
        raise RuntimeError("Gemini batch mode selected but no GEMINI_API_KEY/GOOGLE_API_KEY found.")

    api_base_url = str(task_cfg.get("api_base_url", "https://generativelanguage.googleapis.com/v1beta"))
    if "generativelanguage.googleapis.com" not in api_base_url:
        api_base_url = "https://generativelanguage.googleapis.com/v1beta"
    api_model = str(task_cfg.get("api_model", "gemini-2.5-flash-lite"))
    clean_model = _clean_gemini_model(api_model)
    model_path = urllib.parse.quote(clean_model, safe="")
    timeout_seconds = int(task_cfg.get("timeout_seconds", 120))
    poll_interval_seconds = max(1.0, float(task_cfg.get("batch_poll_interval_seconds", 30)))
    batch_timeout_seconds = max(poll_interval_seconds, float(task_cfg.get("batch_timeout_seconds", 24 * 60 * 60)))
    status_log_path = task_cfg.get("batch_status_log_path")
    status_log_min_interval_seconds = max(
        poll_interval_seconds,
        float(task_cfg.get("batch_status_log_min_interval_seconds", poll_interval_seconds)),
    )
    temperature = float(task_cfg.get("temperature", 0.0))
    display_name = str(task_cfg.get("batch_display_name", task_name.replace("_", "-")))[:80]

    request_rows = [dict(row) for row in requests]
    if not request_rows:
        return {}
    inline_requests = []
    fallback_keys: list[str] = []
    for idx, row in enumerate(request_rows, start=1):
        key = str(row.get("key") or f"request-{idx}")
        prompt = str(row.get("prompt", ""))
        fallback_keys.append(key)
        inline_requests.append(
            {
                "request": _gemini_generate_content_request(prompt, temperature),
                "metadata": {"key": key},
            }
        )

    endpoint = f"{api_base_url.rstrip('/')}/models/{model_path}:batchGenerateContent"
    submit_payload = {
        "batch": {
            "display_name": display_name,
            "input_config": {
                "requests": {
                    "requests": inline_requests,
                }
            },
        }
    }
    logger.info(
        "Submitting Gemini batch task=%s model=%s requests=%d",
        task_name,
        clean_model,
        len(inline_requests),
    )
    try:
        job = _gemini_request(endpoint, api_key, submit_payload, timeout_seconds=timeout_seconds, method="POST")
    except urllib.error.HTTPError as exc:
        err_body = exc.read().decode("utf-8", errors="replace") if hasattr(exc, "read") else ""
        raise RuntimeError(
            f"Gemini batch submit failed status={exc.code} reason={exc.reason} body={err_body[:300]}"
        ) from exc

    job_name = str(job.get("name") or "")
    if not job_name:
        raise RuntimeError(f"Gemini batch submit response missing job name: {json.dumps(job, ensure_ascii=False)[:300]}")
    submitted_at = time.time()
    logger.info(
        "Gemini batch submitted task=%s job=%s timeout=%.0fs poll_interval=%.0fs",
        task_name,
        job_name,
        batch_timeout_seconds,
        poll_interval_seconds,
    )
    _append_batch_status_event(
        status_log_path,
        {
            "event": "submitted",
            "task": task_name,
            "job_name": job_name,
            "model": clean_model,
            "request_count": len(inline_requests),
            "elapsed_seconds": 0,
            "state": _gemini_batch_state(job),
            "stats": _gemini_batch_stats(job),
            "timestamp_epoch_s": submitted_at,
        },
    )

    status_url = f"{api_base_url.rstrip('/')}/{job_name.lstrip('/')}"
    deadline = time.time() + batch_timeout_seconds
    status_body = job
    last_status_log_at = 0.0
    while True:
        state = _gemini_batch_state(status_body)
        now = time.time()
        if now - last_status_log_at >= status_log_min_interval_seconds:
            stats = _gemini_batch_stats(status_body)
            elapsed = now - submitted_at
            logger.info(
                "Gemini batch polling task=%s job=%s state=%s elapsed=%.0fs requests=%s pending=%s ok=%s failed=%s update=%s",
                task_name,
                job_name,
                state,
                elapsed,
                stats.get("request_count"),
                stats.get("pending_request_count"),
                stats.get("successful_request_count"),
                stats.get("failed_request_count"),
                stats.get("update_time"),
            )
            _append_batch_status_event(
                status_log_path,
                {
                    "event": "poll",
                    "task": task_name,
                    "job_name": job_name,
                    "model": clean_model,
                    "elapsed_seconds": round(elapsed, 3),
                    "state": state,
                    "stats": stats,
                    "timestamp_epoch_s": now,
                },
            )
            last_status_log_at = now
        if state in {
            "BATCH_STATE_SUCCEEDED",
            "BATCH_STATE_FAILED",
            "BATCH_STATE_CANCELLED",
            "BATCH_STATE_EXPIRED",
            "JOB_STATE_SUCCEEDED",
            "JOB_STATE_FAILED",
            "JOB_STATE_CANCELLED",
            "JOB_STATE_EXPIRED",
        }:
            break
        if time.time() >= deadline:
            _append_batch_status_event(
                status_log_path,
                {
                    "event": "timeout",
                    "task": task_name,
                    "job_name": job_name,
                    "model": clean_model,
                    "elapsed_seconds": round(time.time() - submitted_at, 3),
                    "state": state,
                    "stats": _gemini_batch_stats(status_body),
                    "timestamp_epoch_s": time.time(),
                },
            )
            raise RuntimeError(f"Gemini batch job {job_name} timed out in state {state}.")
        time.sleep(poll_interval_seconds)
        try:
            status_body = _gemini_request(status_url, api_key, None, timeout_seconds=timeout_seconds, method="GET")
        except urllib.error.HTTPError as exc:
            err_body = exc.read().decode("utf-8", errors="replace") if hasattr(exc, "read") else ""
            raise RuntimeError(
                f"Gemini batch poll failed status={exc.code} reason={exc.reason} body={err_body[:300]}"
            ) from exc

    state = _gemini_batch_state(status_body)
    final_stats = _gemini_batch_stats(status_body)
    logger.info(
        "Gemini batch finished task=%s job=%s state=%s elapsed=%.0fs requests=%s pending=%s ok=%s failed=%s",
        task_name,
        job_name,
        state,
        time.time() - submitted_at,
        final_stats.get("request_count"),
        final_stats.get("pending_request_count"),
        final_stats.get("successful_request_count"),
        final_stats.get("failed_request_count"),
    )
    _append_batch_status_event(
        status_log_path,
        {
            "event": "finished",
            "task": task_name,
            "job_name": job_name,
            "model": clean_model,
            "elapsed_seconds": round(time.time() - submitted_at, 3),
            "state": state,
            "stats": final_stats,
            "timestamp_epoch_s": time.time(),
        },
    )
    if state not in {"BATCH_STATE_SUCCEEDED", "JOB_STATE_SUCCEEDED"}:
        error = status_body.get("error")
        raise RuntimeError(f"Gemini batch job {job_name} ended with state {state}: {error}")

    inline_responses = _extract_inline_responses(status_body)
    if not inline_responses:
        raise RuntimeError(f"Gemini batch job {job_name} succeeded but returned no inline responses.")

    results: dict[str, dict[str, Any]] = {}
    for idx, item in enumerate(inline_responses):
        fallback_key = fallback_keys[idx] if idx < len(fallback_keys) else f"request-{idx + 1}"
        key = _inline_response_key(item, fallback_key)
        payload, error = _inline_response_payload(item, logger)
        results[key] = {"payload": payload, "error": error, "raw": item}

    for key in fallback_keys:
        results.setdefault(key, {"payload": None, "error": "missing_batch_response", "raw": None})
    return results


def _call_gemini_chat(
    api_base_url: str,
    api_key: str,
    model: str,
    prompt: str,
    temperature: float,
    timeout_seconds: int,
    rate_limit_cooldown_seconds: int = 90,
    rate_state_path: Path | None = None,
    min_interval_seconds: float = 6.0,
    max_interval_seconds: float = 120.0,
    success_decay: float = 0.95,
    rate_limit_growth: float = 1.8,
    max_tokens: int = 4096,
) -> dict[str, Any] | None:
    logger = get_logger(__name__)
    global _RATE_LIMITED_UNTIL_EPOCH_S, _NEXT_MODEL_ATTEMPT_EPOCH_S, _LAST_PACING_LOG_EPOCH_S, _LAST_MODEL_SKIP_REASON
    _LAST_MODEL_SKIP_REASON = ""
    now_s = time.time()
    if _NEXT_MODEL_ATTEMPT_EPOCH_S > now_s:
        _LAST_MODEL_SKIP_REASON = "provider_locked"
        return None
    state = _load_rate_state(rate_state_path)
    adaptive_interval = float(state.get("adaptive_min_interval_seconds", min_interval_seconds))
    adaptive_interval = max(float(min_interval_seconds), min(float(max_interval_seconds), adaptive_interval))
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
                "H11",
                "model_provider.py:_call_gemini_chat",
                "Skipping Gemini API due to adaptive min-interval pacing",
                {
                    "remaining_seconds": remaining,
                    "adaptive_interval_seconds": adaptive_interval,
                    "last_request_epoch_s": last_request,
                },
            )
        return None
    if _RATE_LIMITED_UNTIL_EPOCH_S > now_s:
        _NEXT_MODEL_ATTEMPT_EPOCH_S = max(_NEXT_MODEL_ATTEMPT_EPOCH_S, _RATE_LIMITED_UNTIL_EPOCH_S)
        _LAST_MODEL_SKIP_REASON = "rate_limit_cooldown"
        return None

    clean_model = model.strip()
    if clean_model.startswith("models/"):
        clean_model = clean_model[len("models/") :]
    model_path = urllib.parse.quote(clean_model, safe="")
    endpoint = f"{api_base_url.rstrip('/')}/models/{model_path}:generateContent?key={urllib.parse.quote(api_key, safe='')}"
    logger.debug(
        "Calling Gemini API endpoint=%s model=%s timeout=%ss temperature=%.2f prompt_chars=%d",
        endpoint.split("?key=", 1)[0] + "?key=REDACTED",
        clean_model,
        timeout_seconds,
        temperature,
        len(prompt),
    )
    _debug_log(
        "model-provider-debug",
        "H11",
        "model_provider.py:_call_gemini_chat",
        "Attempting Gemini API request",
        {"model": clean_model, "timeout_seconds": timeout_seconds},
    )
    payload = _gemini_generate_content_request(prompt, temperature)
    req = urllib.request.Request(
        url=endpoint,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        state["last_request_epoch_s"] = time.time()
        state["updated_at_epoch_s"] = state["last_request_epoch_s"]
        _write_rate_state(rate_state_path, state)
        with urllib.request.urlopen(req, timeout=timeout_seconds) as resp:
            raw_body = resp.read().decode("utf-8")
    except urllib.error.HTTPError as exc:
        err_body = ""
        try:
            err_body = exc.read().decode("utf-8", errors="replace")
        except Exception:
            err_body = "(unable to read HTTP error body)"
        logger.warning(
            "Gemini API HTTP error status=%s reason=%s model=%s body_preview=%s",
            exc.code,
            exc.reason,
            clean_model,
            err_body[:300].replace("\n", "\\n"),
        )
        _debug_log(
            "model-provider-debug",
            "H11",
            "model_provider.py:_call_gemini_chat",
            "Gemini API HTTP error",
            {"status": int(exc.code), "reason": str(exc.reason), "body_preview": err_body[:120]},
        )
        if int(exc.code) == 429:
            retry_after = exc.headers.get("Retry-After") if exc.headers else None
            try:
                cooldown = int(float(retry_after)) if retry_after else int(rate_limit_cooldown_seconds)
            except (TypeError, ValueError):
                cooldown = int(rate_limit_cooldown_seconds)
            _RATE_LIMITED_UNTIL_EPOCH_S = time.time() + max(1, cooldown)
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
        else:
            _LAST_MODEL_SKIP_REASON = f"http_error_{int(exc.code)}"
        return None
    except (urllib.error.URLError, TimeoutError) as exc:
        _LAST_MODEL_SKIP_REASON = "connection_error"
        logger.warning("Gemini API connection failure model=%s error=%s", clean_model, exc)
        return None
    except Exception as exc:
        _LAST_MODEL_SKIP_REASON = "unexpected_error"
        logger.warning("Gemini API request failed unexpectedly model=%s error=%s", clean_model, exc)
        return None

    try:
        body = json.loads(raw_body)
    except json.JSONDecodeError:
        _LAST_MODEL_SKIP_REASON = "invalid_json"
        logger.warning("Gemini API response was not valid JSON. response_preview=%s", raw_body[:300].replace("\n", "\\n"))
        return None
    if not isinstance(body, dict):
        _LAST_MODEL_SKIP_REASON = "invalid_envelope"
        logger.warning("Gemini API response root was %s (expected object).", type(body).__name__)
        return None
    content = _extract_gemini_text(body)
    if not content:
        _LAST_MODEL_SKIP_REASON = "empty_content"
        logger.warning("Gemini API response contained no assistant text. response_keys=%s", sorted(body.keys()))
        return None
    parsed_content = _parse_json_content(content, logger)
    if parsed_content is None:
        _LAST_MODEL_SKIP_REASON = "content_parse_failed"
        _debug_log(
            "model-provider-debug",
            "H11",
            "model_provider.py:_call_gemini_chat",
            "Gemini content parse failed",
            {"content_preview": content[:120]},
        )
        return None
    state["success_count"] = int(state.get("success_count", 0)) + 1
    decayed = float(state.get("adaptive_min_interval_seconds", min_interval_seconds)) * float(success_decay)
    state["adaptive_min_interval_seconds"] = max(float(min_interval_seconds), min(float(max_interval_seconds), decayed))
    state["updated_at_epoch_s"] = time.time()
    _NEXT_MODEL_ATTEMPT_EPOCH_S = max(_NEXT_MODEL_ATTEMPT_EPOCH_S, state["last_request_epoch_s"] + state["adaptive_min_interval_seconds"])
    _write_rate_state(rate_state_path, state)
    _LAST_MODEL_SKIP_REASON = ""
    logger.debug("Parsed Gemini API content keys=%s", sorted(parsed_content.keys()))
    return parsed_content


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
) -> dict[str, Any] | None:
    logger = get_logger(__name__)
    global _LAST_PROVIDER_RESOLVE_LOG_EPOCH_S, _LAST_API_FAILURE_LOG_EPOCH_S, _LAST_MODEL_SKIP_REASON
    now_s = time.time()
    resolved_provider = (provider or "auto").lower()
    if resolved_provider in {"google", "google_ai", "google_ai_studio", "gemini_api"}:
        resolved_provider = "gemini"
    if resolved_provider in {"open_router", "openrouter_api"}:
        resolved_provider = "openrouter"
    supported_providers = {"auto", "gemini", "openrouter", "openai_compatible", "api"}
    if resolved_provider not in supported_providers:
        logger.warning("Unsupported model provider selected: %s.", resolved_provider)
        _LAST_MODEL_SKIP_REASON = "unsupported_provider"
        return None

    gemini_key = _resolve_gemini_api_key() if resolved_provider == "gemini" else None
    openrouter_key = _resolve_openrouter_api_key() if resolved_provider == "openrouter" else None
    api_key = _resolve_generic_api_key() if resolved_provider in {"auto", "api", "openai_compatible"} else None
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
                "has_api_key": bool(api_key or gemini_key or openrouter_key),
                "api_model": api_model,
            },
        )
        # endregion

    if resolved_provider == "gemini":
        if not gemini_key:
            logger.warning("Gemini provider selected but no GEMINI_API_KEY/GOOGLE_API_KEY found in environment/.env.")
            _LAST_MODEL_SKIP_REASON = "missing_api_key"
            return None
        gemini_base_url = api_base_url
        if "generativelanguage.googleapis.com" not in gemini_base_url:
            gemini_base_url = "https://generativelanguage.googleapis.com/v1beta"
        gemini_model = api_model if api_model and api_model != "qwen/qwen3.5-flash-02-23" else "gemini-2.5-flash-lite"
        return _call_gemini_chat(
            gemini_base_url,
            gemini_key,
            gemini_model,
            prompt,
            temperature,
            timeout_seconds,
            rate_limit_cooldown_seconds=rate_limit_cooldown_seconds,
            rate_state_path=rate_state_path,
            min_interval_seconds=min_interval_seconds,
            max_interval_seconds=max_interval_seconds,
            success_decay=success_decay,
            rate_limit_growth=rate_limit_growth,
            max_tokens=max_tokens,
        )

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
                "OpenAI-compatible provider selected but no MODEL_API_KEY/MODEL_PROVIDER_API_KEY/OPENAI_COMPATIBLE_API_KEY found in environment/.env."
            )
            return None
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
        )

    # auto: prefer the generic OpenAI-compatible API when its key is available.
    if api_key:
        api_result = _call_openai_compatible_chat(
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
        )
        if api_result is not None:
            return api_result
        if _LAST_MODEL_SKIP_REASON in {"provider_locked", "adaptive_pacing", "rate_limit_cooldown"}:
            # Pacing/cooldown skips are expected; avoid noisy fallback loops.
            return None
        if now_s - _LAST_API_FAILURE_LOG_EPOCH_S >= 2.0:
            logger.warning("OpenAI-compatible API attempt failed in auto mode.")
            _LAST_API_FAILURE_LOG_EPOCH_S = now_s
        return None

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
- THERIAC quest titles may be named after songs. Do not down-rank, block, or reclassify a named quest solely because it matches a song title. If a song-title name is associated with a path, ending, mission, or quest progression, classify it as quest rather than theme.

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