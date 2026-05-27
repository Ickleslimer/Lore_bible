from __future__ import annotations

import unittest
from unittest.mock import patch

from pipeline.opportunistic_model_router import (
    OpportunisticChatResult,
    _openrouter_model_is_free,
    opportunistic_route_config,
    profile_metadata,
    resolve_opportunistic_profiles,
    resolve_routing_tiers,
    rotated_profiles,
    segment_prefers_homogeneous_only,
    should_try_next_profile,
)


class OpportunisticModelRouterTests(unittest.TestCase):
    PROVIDER_CONFIG = {
        "model_routing": {
            "profiles": {
                "nim_deepseek_flash": {
                    "provider": "nvidia",
                    "api_model": "deepseek-ai/deepseek-v4-flash",
                    "free_lane": True,
                    "lane_tier": "homogeneous",
                    "model_family": "deepseek_v4_flash",
                    "trust_level": "primary",
                    "calibration_status": "approved",
                },
                "openrouter_free_lane": {
                    "provider": "openrouter",
                    "api_model": "deepseek/deepseek-v4-flash:free",
                    "free_lane": True,
                    "lane_tier": "homogeneous",
                    "model_family": "deepseek_v4_flash",
                },
                "openrouter_paid_lane": {
                    "provider": "openrouter",
                    "api_model": "deepseek/deepseek-v4-flash",
                },
                "openrouter_free_auto": {
                    "provider": "openrouter",
                    "api_model": "openrouter/free",
                    "free_lane": True,
                    "lane_tier": "heterogeneous",
                    "model_family": "openrouter_free_auto",
                    "trust_level": "experimental",
                    "calibration_status": "pending",
                },
                "deepseek_direct_v4_flash": {
                    "provider": "openai_compatible",
                    "api_model": "deepseek-chat",
                    "paid_lane": True,
                },
            },
            "tasks": {"stage_05_lore_development_ledger": {"profile": "nim_deepseek_flash"}},
        }
    }

    def test_rotated_profiles_cycles_start_index(self) -> None:
        profiles = ["a", "b", "c"]
        self.assertEqual(rotated_profiles(profiles, 0), ["a", "b", "c"])
        self.assertEqual(rotated_profiles(profiles, 1), ["b", "c", "a"])

    def test_should_try_next_profile_on_transient_errors(self) -> None:
        self.assertTrue(
            should_try_next_profile("http_error_504", fail_fast_reasons=["rate_limited_429"])
        )
        self.assertFalse(
            should_try_next_profile("missing_api_key", fail_fast_reasons=["rate_limited_429"])
        )

    def test_openrouter_model_is_free(self) -> None:
        self.assertTrue(_openrouter_model_is_free("deepseek/deepseek-v4-flash:free"))
        self.assertTrue(_openrouter_model_is_free("openrouter/free"))
        self.assertFalse(_openrouter_model_is_free("deepseek/deepseek-v4-flash"))

    def test_free_only_allows_or_free_excludes_paid(self) -> None:
        resolved = resolve_opportunistic_profiles(
            self.PROVIDER_CONFIG,
            "stage_05_lore_development_ledger",
            [
                "openrouter_paid_lane",
                "openrouter_free_lane",
                "nim_deepseek_flash",
                "deepseek_direct_v4_flash",
            ],
            free_only=True,
        )
        self.assertEqual(resolved, ["openrouter_free_lane", "nim_deepseek_flash"])

    def test_experimental_pending_profile_excluded(self) -> None:
        resolved = resolve_opportunistic_profiles(
            self.PROVIDER_CONFIG,
            "stage_05_lore_development_ledger",
            ["openrouter_free_auto"],
            free_only=True,
        )
        self.assertEqual(resolved, [])

    def test_resolve_routing_tiers_from_config(self) -> None:
        tiers = resolve_routing_tiers(
            self.PROVIDER_CONFIG,
            "stage_05_lore_development_ledger",
            {
                "tiers": [
                    {"name": "homogeneous_flash", "profiles": ["nim_deepseek_flash", "openrouter_free_lane"]},
                    {"name": "heterogeneous_free", "profiles": ["openrouter_free_auto"]},
                ]
            },
            free_only=True,
        )
        self.assertEqual(len(tiers), 1)
        self.assertEqual(tiers[0]["name"], "homogeneous_flash")
        self.assertIn("nim_deepseek_flash", tiers[0]["profiles"])

    def test_opportunistic_route_config_backward_compat_flat_profiles(self) -> None:
        cfg = opportunistic_route_config(
            self.PROVIDER_CONFIG,
            {
                "opportunistic_routing": {
                    "enabled": True,
                    "profiles": ["nim_deepseek_flash"],
                }
            },
        )
        self.assertTrue(cfg["enabled"])
        self.assertEqual(len(cfg["tiers"]), 1)

    def test_segment_prefers_homogeneous_only(self) -> None:
        prior = [{"inference_model_family": "deepseek_v4_flash", "headline": "x"}]
        self.assertTrue(
            segment_prefers_homogeneous_only(prior, homogeneous_family="deepseek_v4_flash")
        )

    def test_profile_metadata_lane_tier(self) -> None:
        meta = profile_metadata(
            self.PROVIDER_CONFIG,
            "stage_05_lore_development_ledger",
            "openrouter_free_auto",
        )
        self.assertEqual(meta["lane_tier"], "heterogeneous")
        self.assertEqual(meta["model_family"], "openrouter_free_auto")

    @patch("pipeline.opportunistic_model_router.call_model_chat")
    @patch("pipeline.opportunistic_model_router.get_model_runtime_status")
    def test_opportunistic_model_chat_returns_provenance(self, mock_status, mock_chat) -> None:
        from pipeline.opportunistic_model_router import opportunistic_model_chat

        mock_chat.return_value = {"entries": []}
        mock_status.return_value = {"last_model_skip_reason": ""}
        route_cfg = {
            "tiers": [{"name": "homogeneous_flash", "profiles": ["nim_deepseek_flash"]}],
            "task_name": "stage_05_lore_development_ledger",
            "state_path": "artifacts/learning/test_router_state.json",
            "attempts_per_profile": 1,
            "retry_sleep_seconds": 0,
            "fail_fast_reasons": [],
            "homogeneous_family": "deepseek_v4_flash",
            "profile_cooldown_seconds": {},
        }
        result = opportunistic_model_chat(
            "prompt",
            provider_config=self.PROVIDER_CONFIG,
            task_name="stage_05_lore_development_ledger",
            route_cfg=route_cfg,
            segment_id="seg1",
        )
        self.assertIsInstance(result, OpportunisticChatResult)
        self.assertEqual(result.routing_profile, "nim_deepseek_flash")
        self.assertIsNotNone(result.response)


if __name__ == "__main__":
    unittest.main()
