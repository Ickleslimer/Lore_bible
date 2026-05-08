from __future__ import annotations

import json
import os
import re
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

from pipeline.common import get_logger, read_json

DEBUG_LOG_PATH = Path("debug-f7d16c.log")
DEBUG_SESSION_ID = "f7d16c"
_RATE_LIMITED_UNTIL_EPOCH_S = 0.0
_NEXT_MISTRAL_ATTEMPT_EPOCH_S = 0.0
_LAST_PACING_LOG_EPOCH_S = 0.0
_LAST_COOLDOWN_LOG_EPOCH_S = 0.0
_LAST_PROVIDER_RESOLVE_LOG_EPOCH_S = 0.0
_LAST_OLLAMA_SKIP_LOG_EPOCH_S = 0.0
_LAST_API_FAILURE_LOG_EPOCH_S = 0.0
_CACHED_API_KEY: str | None = None
_HAS_CACHED_API_KEY = False
_OLLAMA_UNAVAILABLE_UNTIL_EPOCH_S = 0.0
_LAST_MISTRAL_SKIP_REASON = ""


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


def _resolve_mixtral_api_key() -> str | None:
    global _CACHED_API_KEY, _HAS_CACHED_API_KEY
    if _HAS_CACHED_API_KEY:
        return _CACHED_API_KEY

    env_candidates = ["MIXTRAL_API_KEY", "Mixtral_API_Key", "MISTRAL_API_KEY"]
    for key_name in env_candidates:
        value = os.environ.get(key_name)
        if value and value.strip():
            _CACHED_API_KEY = value.strip().strip('"').strip("'")
            _HAS_CACHED_API_KEY = True
            # region agent log
            _debug_log("mixtral-debug", "H1", "mixtral_anchor_provider.py:_resolve_mixtral_api_key", "Resolved API key from process env", {"key_name": key_name, "source": "process_env"})
            # endregion
            return _CACHED_API_KEY
    repo_root = Path(__file__).resolve().parents[1]
    file_value = _read_env_value_from_file(repo_root, env_candidates)
    _CACHED_API_KEY = file_value
    _HAS_CACHED_API_KEY = True
    # region agent log
    _debug_log(
        "mixtral-debug",
        "H1",
        "mixtral_anchor_provider.py:_resolve_mixtral_api_key",
        "Resolved API key from .env probe",
        {"found": bool(file_value), "source": ".env", "repo_root": str(repo_root)},
    )
    # endregion
    return _CACHED_API_KEY


def _parse_json_content(content: str, logger) -> dict[str, Any] | None:
    normalized = content.strip()
    if normalized.startswith("```"):
        fenced = re.match(r"^```(?:json)?\s*(.*?)\s*```$", normalized, flags=re.DOTALL | re.IGNORECASE)
        if fenced:
            normalized = fenced.group(1).strip()
            # region agent log
            _debug_log(
                "mixtral-debug",
                "H8",
                "mixtral_anchor_provider.py:_parse_json_content",
                "Stripped fenced code block from model content",
                {"had_fence": True, "preview": normalized[:120]},
            )
            # endregion
    try:
        parsed_content = json.loads(normalized)
    except json.JSONDecodeError:
        logger.warning("Model message content was not valid JSON. content_preview=%s", normalized[:300].replace("\n", "\\n"))
        return None
    if not isinstance(parsed_content, dict):
        logger.warning("Model message JSON root was %s (expected object).", type(parsed_content).__name__)
        return None
    return parsed_content


def _call_ollama_chat(
    base_url: str,
    model: str,
    prompt: str,
    temperature: float,
    timeout_seconds: int,
    unavailable_cooldown_seconds: int = 120,
) -> dict[str, Any] | None:
    logger = get_logger(__name__)
    global _OLLAMA_UNAVAILABLE_UNTIL_EPOCH_S
    endpoint = f"{base_url.rstrip('/')}/api/chat"
    logger.debug(
        "Calling Ollama chat provider endpoint=%s model=%s timeout=%ss temperature=%.2f prompt_chars=%d",
        endpoint,
        model,
        timeout_seconds,
        temperature,
        len(prompt),
    )
    # region agent log
    _debug_log(
        "mixtral-debug",
        "H3",
        "mixtral_anchor_provider.py:_call_ollama_chat",
        "Attempting Ollama request",
        {"endpoint": endpoint, "model": model, "timeout_seconds": timeout_seconds},
    )
    # endregion
    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": "You are a precise JSON classifier."},
            {"role": "user", "content": prompt},
        ],
        "stream": False,
        "options": {"temperature": temperature},
    }
    req = urllib.request.Request(
        url=endpoint,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout_seconds) as resp:
            raw_body = resp.read().decode("utf-8")
    except urllib.error.HTTPError as exc:
        err_body = ""
        try:
            err_body = exc.read().decode("utf-8", errors="replace")
        except Exception:
            err_body = "(unable to read HTTP error body)"
        _OLLAMA_UNAVAILABLE_UNTIL_EPOCH_S = time.time() + max(1, int(unavailable_cooldown_seconds))
        logger.warning(
            "Ollama HTTP error status=%s reason=%s endpoint=%s body_preview=%s",
            exc.code,
            exc.reason,
            endpoint,
            err_body[:300].replace("\n", "\\n"),
        )
        # region agent log
        _debug_log(
            "mixtral-debug",
            "H3",
            "mixtral_anchor_provider.py:_call_ollama_chat",
            "Ollama HTTP error",
            {"status": int(exc.code), "reason": str(exc.reason)},
        )
        # endregion
        return None
    except (urllib.error.URLError, TimeoutError) as exc:
        _OLLAMA_UNAVAILABLE_UNTIL_EPOCH_S = time.time() + max(1, int(unavailable_cooldown_seconds))
        logger.warning("Ollama connection failure endpoint=%s error=%s", endpoint, exc)
        # region agent log
        _debug_log(
            "mixtral-debug",
            "H3",
            "mixtral_anchor_provider.py:_call_ollama_chat",
            "Ollama connection failure",
            {"error_type": type(exc).__name__, "error": str(exc)},
        )
        # endregion
        return None
    except Exception as exc:
        _OLLAMA_UNAVAILABLE_UNTIL_EPOCH_S = time.time() + max(1, int(unavailable_cooldown_seconds))
        logger.warning("Ollama request failed unexpectedly endpoint=%s error=%s", endpoint, exc)
        return None

    try:
        body = json.loads(raw_body)
    except json.JSONDecodeError:
        logger.warning("Ollama response was not valid JSON. response_preview=%s", raw_body[:300].replace("\n", "\\n"))
        return None
    except Exception as exc:
        logger.warning("Failed decoding Ollama response JSON: %s", exc)
        return None

    if not isinstance(body, dict):
        logger.warning("Ollama response JSON root was %s (expected object).", type(body).__name__)
        return None
    logger.debug("Ollama response envelope keys=%s", sorted(body.keys()))
    content = (((body.get("message") or {}).get("content")) or "").strip()
    if not content:
        logger.warning("Ollama response contained no assistant message content.")
        return None
    logger.debug("Ollama response content_preview=%s", content[:300].replace("\n", "\\n"))
    parsed_content = _parse_json_content(content, logger)
    if parsed_content is None:
        # region agent log
        _debug_log(
            "mixtral-debug",
            "H4",
            "mixtral_anchor_provider.py:_call_ollama_chat",
            "Ollama content parse failed",
            {"content_preview": content[:120]},
        )
        # endregion
        return None
    _OLLAMA_UNAVAILABLE_UNTIL_EPOCH_S = 0.0
    logger.debug("Parsed Ollama content keys=%s", sorted(parsed_content.keys()))
    return parsed_content


def _call_mistral_chat(
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
) -> dict[str, Any] | None:
    logger = get_logger(__name__)
    global _RATE_LIMITED_UNTIL_EPOCH_S, _NEXT_MISTRAL_ATTEMPT_EPOCH_S, _LAST_PACING_LOG_EPOCH_S, _LAST_COOLDOWN_LOG_EPOCH_S, _LAST_MISTRAL_SKIP_REASON
    _LAST_MISTRAL_SKIP_REASON = ""
    now_s = time.time()
    if _NEXT_MISTRAL_ATTEMPT_EPOCH_S > now_s:
        _LAST_MISTRAL_SKIP_REASON = "provider_locked"
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
        _NEXT_MISTRAL_ATTEMPT_EPOCH_S = max(_NEXT_MISTRAL_ATTEMPT_EPOCH_S, last_request + adaptive_interval)
        _LAST_MISTRAL_SKIP_REASON = "adaptive_pacing"
        if now_s - _LAST_PACING_LOG_EPOCH_S >= 1.0:
            _LAST_PACING_LOG_EPOCH_S = now_s
            # region agent log
            _debug_log(
                "mixtral-debug",
                "H10",
                "mixtral_anchor_provider.py:_call_mistral_chat",
                "Skipping Mistral API due to adaptive min-interval pacing",
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
        _NEXT_MISTRAL_ATTEMPT_EPOCH_S = max(_NEXT_MISTRAL_ATTEMPT_EPOCH_S, _RATE_LIMITED_UNTIL_EPOCH_S)
        _LAST_MISTRAL_SKIP_REASON = "rate_limit_cooldown"
        if now_s - _LAST_COOLDOWN_LOG_EPOCH_S >= 1.0:
            _LAST_COOLDOWN_LOG_EPOCH_S = now_s
            # region agent log
            _debug_log(
                "mixtral-debug",
                "H9",
                "mixtral_anchor_provider.py:_call_mistral_chat",
                "Skipping Mistral API due to active rate-limit cooldown",
                {"cooldown_remaining_seconds": remaining},
            )
            # endregion
        return None

    endpoint = f"{api_base_url.rstrip('/')}/chat/completions"
    logger.debug(
        "Calling Mistral API endpoint=%s model=%s timeout=%ss temperature=%.2f prompt_chars=%d",
        endpoint,
        model,
        timeout_seconds,
        temperature,
        len(prompt),
    )
    # region agent log
    _debug_log(
        "mixtral-debug",
        "H2",
        "mixtral_anchor_provider.py:_call_mistral_chat",
        "Attempting Mistral API request",
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
    req = urllib.request.Request(
        url=endpoint,
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {api_key}",
        },
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
            "mixtral-debug",
            "H7",
            "mixtral_anchor_provider.py:_call_mistral_chat",
            "Mistral API attempt started",
            {"attempt": attempt_idx, "attempts": attempts, "timeout_seconds": timeout_seconds},
        )
        # endregion
        try:
            with urllib.request.urlopen(req, timeout=timeout_seconds) as resp:
                raw_body = resp.read().decode("utf-8")
                break
        except urllib.error.HTTPError as exc:
            err_body = ""
            try:
                err_body = exc.read().decode("utf-8", errors="replace")
            except Exception:
                err_body = "(unable to read HTTP error body)"
            logger.warning(
                "Mistral API HTTP error status=%s reason=%s endpoint=%s body_preview=%s",
                exc.code,
                exc.reason,
                endpoint,
                err_body[:300].replace("\n", "\\n"),
            )
            # region agent log
            _debug_log(
                "mixtral-debug",
                "H2",
                "mixtral_anchor_provider.py:_call_mistral_chat",
                "Mistral API HTTP error",
                {"status": int(exc.code), "reason": str(exc.reason), "body_preview": err_body[:120], "attempt": attempt_idx},
            )
            # endregion
            if int(exc.code) == 429:
                _RATE_LIMITED_UNTIL_EPOCH_S = time.time() + max(1, int(rate_limit_cooldown_seconds))
                _NEXT_MISTRAL_ATTEMPT_EPOCH_S = max(_NEXT_MISTRAL_ATTEMPT_EPOCH_S, _RATE_LIMITED_UNTIL_EPOCH_S)
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
                    "mixtral-debug",
                    "H9",
                    "mixtral_anchor_provider.py:_call_mistral_chat",
                    "Activated rate-limit cooldown after 429",
                    {
                        "cooldown_seconds": int(rate_limit_cooldown_seconds),
                        "rate_limited_until_epoch_s": _RATE_LIMITED_UNTIL_EPOCH_S,
                        "adaptive_min_interval_seconds": state.get("adaptive_min_interval_seconds"),
                    },
                )
                # endregion
                _LAST_MISTRAL_SKIP_REASON = "rate_limited_429"
            else:
                _LAST_MISTRAL_SKIP_REASON = f"http_error_{int(exc.code)}"
            return None
        except (urllib.error.URLError, TimeoutError) as exc:
            last_error = f"{type(exc).__name__}: {exc}"
            _LAST_MISTRAL_SKIP_REASON = "connection_error"
            logger.warning("Mistral API connection failure endpoint=%s error=%s", endpoint, exc)
            # region agent log
            _debug_log(
                "mixtral-debug",
                "H2",
                "mixtral_anchor_provider.py:_call_mistral_chat",
                "Mistral API connection failure",
                {"error_type": type(exc).__name__, "error": str(exc), "attempt": attempt_idx},
            )
            # endregion
            if attempt_idx < attempts:
                # Use bounded exponential backoff to avoid tight retry loops.
                retry_sleep_seconds = min(8.0, 0.5 * (2 ** (attempt_idx - 1)))
                time.sleep(retry_sleep_seconds)
        except Exception as exc:
            last_error = f"{type(exc).__name__}: {exc}"
            _LAST_MISTRAL_SKIP_REASON = "unexpected_error"
            logger.warning("Mistral API request failed unexpectedly endpoint=%s error=%s", endpoint, exc)
    if raw_body is None:
        # region agent log
        _debug_log(
            "mixtral-debug",
            "H7",
            "mixtral_anchor_provider.py:_call_mistral_chat",
            "Mistral API attempts exhausted",
            {"attempts": attempts, "last_error": last_error or "unknown"},
        )
        # endregion
        if not _LAST_MISTRAL_SKIP_REASON:
            _LAST_MISTRAL_SKIP_REASON = "attempts_exhausted"
        return None

    try:
        body = json.loads(raw_body)
    except json.JSONDecodeError:
        _LAST_MISTRAL_SKIP_REASON = "invalid_json"
        logger.warning("Mistral API response was not valid JSON. response_preview=%s", raw_body[:300].replace("\n", "\\n"))
        return None
    if not isinstance(body, dict):
        _LAST_MISTRAL_SKIP_REASON = "invalid_envelope"
        logger.warning("Mistral API response root was %s (expected object).", type(body).__name__)
        return None
    choices = body.get("choices", [])
    if not isinstance(choices, list) or not choices:
        _LAST_MISTRAL_SKIP_REASON = "missing_choices"
        logger.warning("Mistral API response missing choices.")
        return None
    first = choices[0] if isinstance(choices[0], dict) else {}
    message = first.get("message", {}) if isinstance(first, dict) else {}
    content = str(message.get("content", "")).strip()
    if not content:
        _LAST_MISTRAL_SKIP_REASON = "empty_content"
        logger.warning("Mistral API response contained no assistant message content.")
        return None
    parsed_content = _parse_json_content(content, logger)
    if parsed_content is None:
        _LAST_MISTRAL_SKIP_REASON = "content_parse_failed"
        # region agent log
        _debug_log(
            "mixtral-debug",
            "H4",
            "mixtral_anchor_provider.py:_call_mistral_chat",
            "Mistral API content parse failed",
            {"content_preview": content[:120]},
        )
        # endregion
        return None
    state["success_count"] = int(state.get("success_count", 0)) + 1
    decayed = float(state.get("adaptive_min_interval_seconds", min_interval_seconds)) * float(success_decay)
    state["adaptive_min_interval_seconds"] = max(float(min_interval_seconds), min(float(max_interval_seconds), decayed))
    state["updated_at_epoch_s"] = time.time()
    _NEXT_MISTRAL_ATTEMPT_EPOCH_S = max(_NEXT_MISTRAL_ATTEMPT_EPOCH_S, state["last_request_epoch_s"] + state["adaptive_min_interval_seconds"])
    _write_rate_state(rate_state_path, state)
    # region agent log
    _debug_log(
        "mixtral-debug",
        "H10",
        "mixtral_anchor_provider.py:_call_mistral_chat",
        "Mistral API success updated adaptive pacing state",
        {
            "success_count": state.get("success_count"),
            "adaptive_min_interval_seconds": state.get("adaptive_min_interval_seconds"),
            "rate_state_path": str(rate_state_path) if rate_state_path else None,
        },
    )
    # endregion
    _LAST_MISTRAL_SKIP_REASON = ""
    logger.debug("Parsed Mistral API content keys=%s", sorted(parsed_content.keys()))
    return parsed_content


def call_mixtral_chat(
    base_url: str,
    model: str,
    prompt: str,
    temperature: float,
    timeout_seconds: int,
    provider: str = "auto",
    api_base_url: str = "https://api.mistral.ai/v1",
    api_model: str = "mistral-large-latest",
    api_retries: int = 2,
    auto_fallback_to_ollama: bool = True,
    rate_limit_cooldown_seconds: int = 90,
    rate_state_path: Path | None = None,
    min_interval_seconds: float = 2.0,
    max_interval_seconds: float = 120.0,
    success_decay: float = 0.9,
    rate_limit_growth: float = 1.8,
    ollama_unavailable_cooldown_seconds: int = 120,
) -> dict[str, Any] | None:
    logger = get_logger(__name__)
    global _LAST_PROVIDER_RESOLVE_LOG_EPOCH_S, _LAST_OLLAMA_SKIP_LOG_EPOCH_S, _OLLAMA_UNAVAILABLE_UNTIL_EPOCH_S, _LAST_API_FAILURE_LOG_EPOCH_S, _LAST_MISTRAL_SKIP_REASON
    resolved_provider = (provider or "auto").lower()
    now_s = time.time()

    if resolved_provider in {"mistral_api", "api", "auto"}:
        provider_locked = _NEXT_MISTRAL_ATTEMPT_EPOCH_S > now_s
        if provider_locked and (resolved_provider in {"mistral_api", "api"} or not auto_fallback_to_ollama):
            return None

    api_key = _resolve_mixtral_api_key()
    if now_s - _LAST_PROVIDER_RESOLVE_LOG_EPOCH_S >= 1.0:
        _LAST_PROVIDER_RESOLVE_LOG_EPOCH_S = now_s
        # region agent log
        _debug_log(
            "mixtral-debug",
            "H5",
            "mixtral_anchor_provider.py:call_mixtral_chat",
            "Resolved provider mode before dispatch",
            {"provider": resolved_provider, "has_api_key": bool(api_key), "api_model": api_model, "ollama_model": model},
        )
        # endregion

    if resolved_provider in {"mistral_api", "api"}:
        if not api_key:
            logger.warning("Mixtral provider set to API but no API key found in environment/.env.")
            return None
        return _call_mistral_chat(
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
        )

    if resolved_provider == "ollama":
        if _OLLAMA_UNAVAILABLE_UNTIL_EPOCH_S > now_s:
            if now_s - _LAST_OLLAMA_SKIP_LOG_EPOCH_S >= 5.0:
                _LAST_OLLAMA_SKIP_LOG_EPOCH_S = now_s
                remaining = round(_OLLAMA_UNAVAILABLE_UNTIL_EPOCH_S - now_s, 2)
                # region agent log
                _debug_log(
                    "mixtral-debug",
                    "H3",
                    "mixtral_anchor_provider.py:call_mixtral_chat",
                    "Skipping Ollama due to recent unavailability cooldown",
                    {"cooldown_remaining_seconds": remaining},
                )
                # endregion
            return None
        return _call_ollama_chat(
            base_url,
            model,
            prompt,
            temperature,
            timeout_seconds,
            unavailable_cooldown_seconds=ollama_unavailable_cooldown_seconds,
        )

    # auto: prefer API when key is available, fallback to Ollama
    if api_key:
        api_result = _call_mistral_chat(
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
        )
        if api_result is not None:
            return api_result
        if _LAST_MISTRAL_SKIP_REASON in {"provider_locked", "adaptive_pacing", "rate_limit_cooldown"}:
            # Pacing/cooldown skips are expected; avoid noisy fallback loops.
            return None
        if now_s - _LAST_API_FAILURE_LOG_EPOCH_S >= 2.0:
            logger.warning("Mixtral API attempt failed in auto mode.")
            _LAST_API_FAILURE_LOG_EPOCH_S = now_s
        if not auto_fallback_to_ollama:
            return None
        if now_s - _LAST_API_FAILURE_LOG_EPOCH_S >= 2.0:
            logger.warning("Falling back to Ollama as configured.")
            _LAST_API_FAILURE_LOG_EPOCH_S = now_s
    if _OLLAMA_UNAVAILABLE_UNTIL_EPOCH_S > now_s:
        if now_s - _LAST_OLLAMA_SKIP_LOG_EPOCH_S >= 5.0:
            _LAST_OLLAMA_SKIP_LOG_EPOCH_S = now_s
            remaining = round(_OLLAMA_UNAVAILABLE_UNTIL_EPOCH_S - now_s, 2)
            # region agent log
            _debug_log(
                "mixtral-debug",
                "H3",
                "mixtral_anchor_provider.py:call_mixtral_chat",
                "Skipping Ollama due to recent unavailability cooldown",
                {"cooldown_remaining_seconds": remaining},
            )
            # endregion
        return None
    return _call_ollama_chat(
        base_url,
        model,
        prompt,
        temperature,
        timeout_seconds,
        unavailable_cooldown_seconds=ollama_unavailable_cooldown_seconds,
    )


def load_seed_entities(seed_path: Path | None) -> list[str]:
    if not seed_path or not seed_path.exists():
        return []
    payload = read_json(seed_path)
    cards = payload.get("cards", [])
    entities: list[str] = []
    for card in cards:
        if isinstance(card, dict):
            name = str(card.get("canonical_name", "")).strip()
            if name:
                entities.append(name)
    return entities


def build_stage_a_prompt(doc_excerpt: str) -> str:
    return f"""You extract canonical ontology anchors from a lore bible.
Be conservative. Return strict JSON only.

Task:
- Extract entities that should become canonical lore cards.
- Keep working names only when clearly used as named concepts.
- Infer one entity type per item from:
  character|faction|organization|ai_system|quest|event|timeline_node|theme|term
- Include initial aliases only when explicitly indicated in text.
- Include relationship hints only when clearly stated.

Lore excerpt:
\"\"\"{doc_excerpt}\"\"\"

Return JSON object:
{{
  "entities": [
    {{
      "canonical_name": "string",
      "entity_type": "character|faction|organization|ai_system|quest|event|timeline_node|theme|term",
      "summary": "short reason",
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
