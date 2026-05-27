"""Tiered opportunistic provider router for Stage 07 and other model-backed tasks."""
from __future__ import annotations

import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from pipeline.common import get_logger, read_json, write_json
from pipeline.model_provider import (
    call_model_chat,
    get_model_runtime_status,
    model_call_kwargs,
    model_task_settings,
)

OPENROUTER_PROVIDERS = frozenset({"openrouter", "open_router", "openrouter_api"})
HOMOGENEOUS_DEFAULT_FAMILY = "deepseek_v4_flash"

TRANSIENT_SKIP_REASONS = frozenset(
    {
        "rate_limited_429",
        "provider_locked",
        "adaptive_pacing",
        "rate_limit_cooldown",
        "connection_error",
        "attempts_exhausted",
        "http_error_502",
        "http_error_503",
        "http_error_504",
    }
)

HARD_STOP_REASONS = frozenset({"missing_api_key", "unsupported_provider", "auth_failed"})

PROFILE_COOLDOWN_SECONDS = {
    "rate_limited_429": 90.0,
    "http_error_504": 300.0,
    "http_error_502": 60.0,
    "http_error_503": 60.0,
    "connection_error": 45.0,
    "attempts_exhausted": 30.0,
}


@dataclass
class OpportunisticChatResult:
    response: dict[str, Any] | str | None
    routing_profile: str = ""
    provider: str = ""
    api_model: str = ""
    lane_tier: str = ""
    model_family: str = ""
    tier_name: str = ""
    router_attempt_index: int = 0
    skip_reason: str = ""


def _default_router_state() -> dict[str, Any]:
    return {
        "next_profile_index": 0,
        "tier_start_index": {},
        "last_success_profile": "",
        "updated_at_epoch_s": 0.0,
        "profiles": {},
    }


def _default_profile_health() -> dict[str, Any]:
    return {"disabled_until_epoch_s": 0.0, "consecutive_failures": 0}


def _load_router_state(state_path: Path) -> dict[str, Any]:
    if not state_path.exists():
        return _default_router_state()
    try:
        payload = read_json(state_path)
    except Exception:
        return _default_router_state()
    if not isinstance(payload, dict):
        return _default_router_state()
    state = _default_router_state()
    state.update(payload)
    if not isinstance(state.get("profiles"), dict):
        state["profiles"] = {}
    if not isinstance(state.get("tier_start_index"), dict):
        state["tier_start_index"] = {}
    return state


def _write_router_state(state_path: Path, state: dict[str, Any]) -> None:
    state_path.parent.mkdir(parents=True, exist_ok=True)
    state["updated_at_epoch_s"] = time.time()
    write_json(state_path, state)


def _profile_health(state: dict[str, Any], profile_name: str) -> dict[str, Any]:
    profiles = state.setdefault("profiles", {})
    if profile_name not in profiles or not isinstance(profiles[profile_name], dict):
        profiles[profile_name] = _default_profile_health()
    return profiles[profile_name]


def _openrouter_model_is_free(api_model: str) -> bool:
    model = str(api_model or "").strip().lower()
    if not model:
        return False
    if model == "openrouter/free":
        return True
    if model.endswith(":free"):
        return True
    if "/free" in model:
        return True
    return False


def profile_metadata(
    provider_config: dict[str, Any] | None,
    task_name: str,
    profile_name: str,
) -> dict[str, Any]:
    task_cfg = model_task_settings(provider_config, task_name, profile_override=profile_name)
    model_family = str(task_cfg.get("model_family", "") or HOMOGENEOUS_DEFAULT_FAMILY).strip()
    lane_tier = str(task_cfg.get("lane_tier", "") or "").strip().lower()
    if not lane_tier:
        lane_tier = (
            "heterogeneous"
            if model_family and model_family != HOMOGENEOUS_DEFAULT_FAMILY
            else "homogeneous"
        )
    calibration = str(task_cfg.get("calibration_status", "approved") or "approved").strip().lower()
    trust = str(task_cfg.get("trust_level", "primary") or "primary").strip().lower()
    return {
        "profile_name": profile_name,
        "lane_tier": lane_tier or "homogeneous",
        "model_family": model_family or HOMOGENEOUS_DEFAULT_FAMILY,
        "calibration_status": calibration,
        "trust_level": trust,
        "provider": str(task_cfg.get("provider", "")),
        "api_model": str(task_cfg.get("api_model", "")),
        "paid_lane": bool(task_cfg.get("paid_lane", False)),
        "free_lane": bool(task_cfg.get("free_lane", False)),
    }


def _profile_is_free_lane(
    provider_config: dict[str, Any] | None,
    task_name: str,
    profile_name: str,
) -> bool:
    meta = profile_metadata(provider_config, task_name, profile_name)
    if meta["paid_lane"]:
        return False
    if meta["free_lane"]:
        return True
    provider = str(meta["provider"] or "").strip().lower()
    if provider in OPENROUTER_PROVIDERS:
        return _openrouter_model_is_free(str(meta["api_model"]))
    return True


def _profile_is_routable(
    provider_config: dict[str, Any] | None,
    task_name: str,
    profile_name: str,
) -> bool:
    meta = profile_metadata(provider_config, task_name, profile_name)
    if meta["calibration_status"] == "pending" and meta["trust_level"] == "experimental":
        return False
    return True


def resolve_opportunistic_profiles(
    provider_config: dict[str, Any] | None,
    task_name: str,
    profile_names: list[str],
    *,
    free_only: bool,
) -> list[str]:
    resolved: list[str] = []
    logger = get_logger(__name__)
    for profile_name in profile_names:
        if free_only and not _profile_is_free_lane(provider_config, task_name, profile_name):
            logger.info(
                "Opportunistic model route skipping non-free profile=%s (free_only=true).",
                profile_name,
            )
            continue
        if not _profile_is_routable(provider_config, task_name, profile_name):
            logger.info(
                "Opportunistic model route skipping uncalibrated profile=%s.",
                profile_name,
            )
            continue
        resolved.append(profile_name)
    return resolved


def _trust_sort_key(
    provider_config: dict[str, Any] | None,
    task_name: str,
    profile_name: str,
) -> tuple[int, str]:
    meta = profile_metadata(provider_config, task_name, profile_name)
    trust_order = {"primary": 0, "fallback": 1, "experimental": 2}
    return (trust_order.get(meta["trust_level"], 9), profile_name)


def resolve_routing_tiers(
    provider_config: dict[str, Any] | None,
    task_name: str,
    raw: dict[str, Any],
    *,
    free_only: bool,
) -> list[dict[str, Any]]:
    tiers_raw = raw.get("tiers")
    if isinstance(tiers_raw, list) and tiers_raw:
        tiers: list[dict[str, Any]] = []
        for tier in tiers_raw:
            if not isinstance(tier, dict):
                continue
            name = str(tier.get("name", "")).strip() or "tier"
            profiles = resolve_opportunistic_profiles(
                provider_config,
                task_name,
                [str(p).strip() for p in tier.get("profiles", []) if str(p).strip()],
                free_only=free_only,
            )
            profiles = sorted(
                profiles,
                key=lambda p: _trust_sort_key(provider_config, task_name, p),
            )
            if profiles:
                tiers.append({"name": name, "profiles": profiles})
        return tiers

    flat = resolve_opportunistic_profiles(
        provider_config,
        task_name,
        [str(p).strip() for p in raw.get("profiles", []) if str(p).strip()],
        free_only=free_only,
    )
    flat = sorted(flat, key=lambda p: _trust_sort_key(provider_config, task_name, p))
    if not flat:
        return []
    return [{"name": "homogeneous_flash", "profiles": flat}]


def opportunistic_route_config(provider_config: dict[str, Any] | None, ledger_cfg: dict[str, Any]) -> dict[str, Any]:
    raw = ledger_cfg.get("opportunistic_routing", {})
    if not isinstance(raw, dict) or not raw:
        section = (
            provider_config.get("lore_development_ledger", {})
            if isinstance(provider_config, dict)
            else {}
        )
        raw = section.get("opportunistic_routing", {}) if isinstance(section, dict) else {}
    if not isinstance(raw, dict):
        raw = {}
    free_only = bool(raw.get("free_only", True))
    task_name = str(raw.get("task_name", "stage_05_lore_development_ledger")).strip()
    tiers = resolve_routing_tiers(provider_config, task_name, raw, free_only=free_only)
    profile_count = sum(len(t.get("profiles", [])) for t in tiers)
    return {
        "enabled": bool(raw.get("enabled", False)) and profile_count > 0,
        "free_only": free_only,
        "task_name": task_name,
        "tiers": tiers,
        "profiles": [p for t in tiers for p in t.get("profiles", [])],
        "state_path": Path(str(raw.get("state_path", "artifacts/learning/opportunistic_stage_07_router.json"))),
        "attempts_per_profile": max(1, int(raw.get("attempts_per_profile", 1) or 1)),
        "retry_sleep_seconds": max(0.0, float(raw.get("retry_sleep_seconds", 2.0) or 0.0)),
        "fail_fast_reasons": [
            str(reason).strip()
            for reason in raw.get("fail_fast_reasons", ["rate_limited_429"])
            if str(reason).strip()
        ],
        "homogeneous_family": str(raw.get("homogeneous_family", HOMOGENEOUS_DEFAULT_FAMILY)).strip(),
        "profile_cooldown_seconds": raw.get("profile_cooldown_seconds", {}),
    }


def should_try_next_profile(reason: str, *, fail_fast_reasons: list[str]) -> bool:
    reason = str(reason or "").strip()
    if not reason or reason in HARD_STOP_REASONS:
        return False
    if reason in fail_fast_reasons:
        return True
    return reason in TRANSIENT_SKIP_REASONS


def rotated_profiles(profiles: list[str], start_index: int) -> list[str]:
    if not profiles:
        return []
    start = int(start_index) % len(profiles)
    return profiles[start:] + profiles[:start]


def _cooldown_for_reason(reason: str, route_cfg: dict[str, Any]) -> float:
    overrides = route_cfg.get("profile_cooldown_seconds", {})
    if isinstance(overrides, dict) and reason in overrides:
        return max(0.0, float(overrides[reason]))
    return PROFILE_COOLDOWN_SECONDS.get(reason, 30.0)


def _profile_disabled(health: dict[str, Any]) -> bool:
    return float(health.get("disabled_until_epoch_s", 0.0) or 0.0) > time.time()


def _record_profile_failure(
    state: dict[str, Any],
    profile_name: str,
    reason: str,
    route_cfg: dict[str, Any],
) -> None:
    health = _profile_health(state, profile_name)
    health["consecutive_failures"] = int(health.get("consecutive_failures", 0) or 0) + 1
    cooldown = _cooldown_for_reason(reason, route_cfg)
    health["disabled_until_epoch_s"] = time.time() + cooldown


def _record_profile_success(state: dict[str, Any], profile_name: str) -> None:
    health = _profile_health(state, profile_name)
    health["consecutive_failures"] = 0
    health["disabled_until_epoch_s"] = 0.0


def segment_prefers_homogeneous_only(
    prior_entries: list[dict[str, Any]],
    *,
    homogeneous_family: str,
) -> bool:
    """If prior ledger used homogeneous family, avoid heterogeneous unless Tier A exhausted."""
    for entry in prior_entries:
        family = str(entry.get("inference_model_family", "")).strip()
        if family and family == homogeneous_family:
            return True
    return False


def opportunistic_model_chat(
    prompt: str,
    *,
    provider_config: dict[str, Any] | None,
    task_name: str,
    route_cfg: dict[str, Any],
    segment_id: str = "",
    prior_entries: list[dict[str, Any]] | None = None,
) -> OpportunisticChatResult:
    logger = get_logger(__name__)
    tiers = list(route_cfg.get("tiers", []))
    task_name = str(route_cfg.get("task_name", task_name)).strip()
    homogeneous_family = str(route_cfg.get("homogeneous_family", HOMOGENEOUS_DEFAULT_FAMILY))

    if not tiers:
        kwargs = model_call_kwargs(provider_config, task_name)
        response = call_model_chat(prompt=prompt, **kwargs)
        return OpportunisticChatResult(
            response=response,
            routing_profile=str(kwargs.get("routing_profile", "")),
            provider=str(kwargs.get("provider", "")),
            api_model=str(kwargs.get("api_model", "")),
            skip_reason=str(get_model_runtime_status().get("last_model_skip_reason") or ""),
        )

    prefer_homogeneous = segment_prefers_homogeneous_only(
        prior_entries or [],
        homogeneous_family=homogeneous_family,
    )

    state_path = Path(route_cfg["state_path"])
    state = _load_router_state(state_path)
    attempts_per_profile = int(route_cfg.get("attempts_per_profile", 1))
    retry_sleep = float(route_cfg.get("retry_sleep_seconds", 0.0) or 0.0)
    fail_fast = list(route_cfg.get("fail_fast_reasons", []))

    last_reason = ""
    attempt_index = 0

    tier_list = tiers
    if prefer_homogeneous:
        tier_list = [t for t in tiers if t.get("name") != "heterogeneous_free"] or tiers[:1]

    for tier in tier_list:
        tier_name = str(tier.get("name", "")).strip()
        profiles = list(tier.get("profiles", []))
        if not profiles:
            continue

        tier_start = int(state.get("tier_start_index", {}).get(tier_name, 0) or 0)
        for offset, profile_name in enumerate(rotated_profiles(profiles, tier_start)):
            health = _profile_health(state, profile_name)
            if _profile_disabled(health):
                logger.info(
                    "Opportunistic model route skip profile=%s (cooldown until %.0f).",
                    profile_name,
                    float(health.get("disabled_until_epoch_s", 0)),
                )
                continue

            meta = profile_metadata(provider_config, task_name, profile_name)
            call_kwargs = model_call_kwargs(provider_config, task_name, profile_override=profile_name)
            logger.info(
                "Opportunistic model route try tier=%s profile=%s provider=%s model=%s segment_id=%s.",
                tier_name,
                profile_name,
                call_kwargs.get("provider"),
                call_kwargs.get("api_model"),
                segment_id,
            )

            for attempt_idx in range(attempts_per_profile):
                attempt_index += 1
                response = call_model_chat(prompt=prompt, **call_kwargs)
                if response is not None:
                    chosen_index = (tier_start + offset) % len(profiles)
                    state["tier_start_index"][tier_name] = (chosen_index + 1) % len(profiles)
                    state["last_success_profile"] = profile_name
                    _record_profile_success(state, profile_name)
                    _write_router_state(state_path, state)
                    logger.info(
                        "Opportunistic model route success tier=%s profile=%s segment_id=%s.",
                        tier_name,
                        profile_name,
                        segment_id,
                    )
                    return OpportunisticChatResult(
                        response=response,
                        routing_profile=profile_name,
                        provider=str(call_kwargs.get("provider", meta["provider"])),
                        api_model=str(call_kwargs.get("api_model", meta["api_model"])),
                        lane_tier=meta["lane_tier"],
                        model_family=meta["model_family"],
                        tier_name=tier_name,
                        router_attempt_index=attempt_index,
                    )

                status = get_model_runtime_status()
                last_reason = str(status.get("last_model_skip_reason") or "")
                if not should_try_next_profile(last_reason, fail_fast_reasons=fail_fast):
                    _write_router_state(state_path, state)
                    return OpportunisticChatResult(
                        response=None,
                        routing_profile=profile_name,
                        skip_reason=last_reason,
                        tier_name=tier_name,
                        router_attempt_index=attempt_index,
                    )

                _record_profile_failure(state, profile_name, last_reason, route_cfg)
                if last_reason in fail_fast:
                    break
                if attempt_idx + 1 < attempts_per_profile and retry_sleep:
                    time.sleep(retry_sleep)

            logger.warning(
                "Opportunistic model route falling through tier=%s profile=%s reason=%s segment_id=%s.",
                tier_name,
                profile_name,
                last_reason,
                segment_id,
            )

    _write_router_state(state_path, state)
    return OpportunisticChatResult(response=None, skip_reason=last_reason, router_attempt_index=attempt_index)
