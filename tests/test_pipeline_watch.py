from __future__ import annotations

import json
import os
import sys
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from unittest.mock import patch

from pipeline.common import write_json, write_jsonl
from pipeline.pipeline_watch import (
    check_stale_watchers,
    infer_run_lifecycle,
    infer_worker_terminal,
    recommendation_from_antigravity_assessment,
    start_watch_job,
    watch_status_update,
)
from pipeline.quota_capture import (
    _is_antigravity_window_title,
    navigate_to_model_quota,
    quota_auto_navigate_enabled,
    run_quota_capture,
)
from pipeline.quota_worker import (
    build_capture_request,
    is_worker_ready,
    process_capture_request,
    quota_worker_dir,
    request_worker_shutdown,
    resolve_capture_repo_root,
    run_quota_capture_via_worker,
    shutdown_requested,
    submit_capture_request,
    wait_for_capture_response,
    write_worker_ready,
    clear_worker_ready,
)
from pipeline.ui_review_app import pipeline_progress_from_logs


class PipelineWatchTests(unittest.TestCase):
    def test_infer_worker_terminal_failed(self) -> None:
        lines = ["1779244235 | desktop: Pipeline stopped with exit code 1."]
        terminal, kind, code = infer_worker_terminal(lines)
        self.assertTrue(terminal)
        self.assertEqual(kind, "failed")
        self.assertEqual(code, 1)

    def test_infer_worker_terminal_succeeded(self) -> None:
        lines = ["1779244235 | desktop: Pipeline completed."]
        terminal, kind, code = infer_worker_terminal(lines)
        self.assertTrue(terminal)
        self.assertEqual(kind, "succeeded")
        self.assertEqual(code, 0)

    def test_infer_run_lifecycle_running_from_worker_log(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            log = root / "tauri_pipeline_worker.log"
            log.write_text(
                "\n".join(
                    [
                        "1 | desktop: starting pipeline worker resume=true ignore_pending=false",
                        "2 | INFO | [1/12] START Stage 01 Entity Bootstrap",
                        "3 | INFO | Stage 01 progress: 1/10 batches",
                    ]
                ),
                encoding="utf-8",
            )
            lifecycle = infer_run_lifecycle(root)
            self.assertTrue(lifecycle.get("running"))
            self.assertEqual(lifecycle.get("lifecycle"), "running")

    def test_watch_status_writes_report_on_terminal_review(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            repo = Path(tmp)
            run = repo / "artifacts" / "runs" / "test_run"
            run.mkdir(parents=True)
            write_json(run / "01_bootstrap" / "entity_seed.json", {"entities": []})
            write_jsonl(run / "02_timeline" / "messages_normalized_per_thread.jsonl", [])
            write_jsonl(run / "02_timeline" / "messages_global_timeline.jsonl", [])
            write_json(run / "02_timeline" / "conversation_segments.json", {"segments": []})
            write_jsonl(run / "03_relevance" / "snippets_candidates.jsonl", [])
            write_json(run / "03_relevance" / "dm_source_profiles.json", {"profiles": []})
            write_json(run / "05_alias" / "resolved_entities.json", {"resolved_entities": []})
            write_json(
                run / "05_alias" / "conversation_entity_proposals.json",
                {"proposals": [{"proposal_id": "p1", "review_status": "pending"}]},
            )
            write_json(run / "05_alias" / "conversation_entity_decisions.json", {"decisions": []})

            job = start_watch_job(repo, run_root=run, watcher="sentinel", poll_interval_seconds=60)
            result = watch_status_update(repo, str(job["job_id"]), checked_by="test")
            self.assertTrue(result["terminal"])
            self.assertEqual(result["status"], "review_required")
            report = run / "watch_report.md"
            self.assertTrue(report.exists())
            self.assertTrue((run / ".watch_done").exists())

    def test_stale_watcher_writes_alert(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            repo = Path(tmp)
            run = repo / "artifacts" / "runs" / "active"
            run.mkdir(parents=True)
            log = run / "tauri_pipeline_worker.log"
            log.write_text(
                "\n".join(
                    [
                        "1 | desktop: starting pipeline worker resume=false ignore_pending=false",
                        "2 | INFO | [2/12] START Stage 02 Message Normalization",
                    ]
                ),
                encoding="utf-8",
            )
            stale_time = (datetime.now(timezone.utc) - timedelta(minutes=20)).isoformat()
            job = {
                "job_id": "abc123",
                "run_root": str(run),
                "until": ["succeeded", "failed", "review_required"],
                "watcher": "antigravity_flash",
                "poll_interval_seconds": 300,
                "on_watcher_lost": "alert",
                "created_at": stale_time,
                "last_checked_at": stale_time,
                "last_checked_by": "antigravity_flash",
            }
            watches = repo / "artifacts" / "pipeline_watches"
            watches.mkdir(parents=True, exist_ok=True)
            (watches / "abc123.json").write_text(json.dumps(job), encoding="utf-8")

            alerts = check_stale_watchers(repo, apply_alerts=True, apply_cancel=False)
            self.assertEqual(len(alerts), 1)
            alert_path = run / "watch_alert.json"
            self.assertTrue(alert_path.exists())

    def test_preflight_recommendation_gemini_low(self) -> None:
        payload = recommendation_from_antigravity_assessment(
            {"gemini_bars_filled": 1, "gpt_pool_bars_filled": 0},
            openrouter_health="ok",
        )
        self.assertEqual(payload["recommendation"], "wait_for_gemini_reset")

    def test_preflight_recommendation_gemini_healthy(self) -> None:
        payload = recommendation_from_antigravity_assessment(
            {"gemini_bars_filled": 3, "gpt_pool_bars_filled": 2},
            openrouter_health="ok",
        )
        self.assertEqual(payload["recommendation"], "run_pipeline_and_flash_watch")

    def test_preflight_failover_to_gpt_pool(self) -> None:
        payload = recommendation_from_antigravity_assessment(
            {"gemini_bars_filled": 1, "gpt_pool_bars_filled": 3},
            openrouter_health="ok",
        )
        self.assertEqual(payload["recommendation"], "failover_to_gpt_pool_watch")

    def test_openrouter_low_balance_does_not_change_recommendation(self) -> None:
        with patch(
            "pipeline.pipeline_watch.check_openrouter_health",
            return_value={
                "openrouter_health": "ok",
                "informational_only": True,
                "limit_remaining": 0.01,
            },
        ):
            from pipeline.pipeline_watch import quota_preflight

            with tempfile.TemporaryDirectory() as tmp:
                repo = Path(tmp)
                result = quota_preflight(
                    repo,
                    run_capture=False,
                    antigravity_assessment={"gemini_bars_filled": 3, "gpt_pool_bars_filled": 3},
                    check_openrouter=True,
                )
        self.assertEqual(result["recommendation"], "run_pipeline_and_flash_watch")

    def test_openrouter_auth_failure_blocks(self) -> None:
        payload = recommendation_from_antigravity_assessment(
            {"gemini_bars_filled": 3},
            openrouter_health="auth_failed",
        )
        self.assertEqual(payload["recommendation"], "quota_unknown")
        self.assertIn("auth_failed", payload["reasons"][0])

    def test_antigravity_window_title_filter(self) -> None:
        self.assertFalse(
            _is_antigravity_window_title("Review: Task delegation to antigravity - Lore_bible - Cursor")
        )
        self.assertTrue(_is_antigravity_window_title("Antigravity - Lore_bible"))
        self.assertFalse(_is_antigravity_window_title("Exploring Theriac Lore Project"))

    def test_quota_auto_navigate_env_default(self) -> None:
        with patch.dict("os.environ", {"THERIAC_QUOTA_AUTO_NAVIGATE": "1"}, clear=False):
            self.assertTrue(quota_auto_navigate_enabled(None))
        with patch.dict("os.environ", {"THERIAC_QUOTA_AUTO_NAVIGATE": "0"}, clear=False):
            self.assertFalse(quota_auto_navigate_enabled(None))

    @patch("pipeline.quota_capture.time.sleep")
    def test_navigate_to_model_quota_clicks_settings_then_models(self, _sleep: Any) -> None:
        from unittest.mock import MagicMock

        mock_pyautogui = MagicMock()
        mock_pyautogui.FAILSAFE = True
        mock_pyautogui.PAUSE = 0.12
        window = type(
            "W",
            (),
            {"left": 100, "top": 50, "width": 1000, "height": 800, "activate": lambda self: None},
        )()
        with patch.dict(sys.modules, {"pyautogui": mock_pyautogui}):
            result = navigate_to_model_quota(window)
        self.assertTrue(result["ok"])
        self.assertEqual(result["method"], "settings_gear_then_models")
        self.assertEqual(mock_pyautogui.click.call_count, 2)

    @patch("pipeline.quota_worker.run_quota_capture")
    def test_quota_worker_request_response_roundtrip(self, mock_capture: Any) -> None:
        mock_capture.return_value = {
            "ok": True,
            "image_path": "/tmp/latest.png",
            "meta_path": "/tmp/latest.meta.json",
        }
        with tempfile.TemporaryDirectory() as tmp:
            repo = Path(tmp)
            request = build_capture_request(repo, auto_navigate=False)
            submit_capture_request(repo, request)
            result = process_capture_request(repo)
            self.assertTrue(result.get("processed"))
            self.assertTrue(result.get("ok"))
            response = wait_for_capture_response(
                repo, str(request["request_id"]), timeout_seconds=1.0
            )
            self.assertTrue(response.get("ok"))
            mock_capture.assert_called_once()

    @patch("pipeline.quota_worker.run_quota_capture")
    def test_quota_worker_via_worker_timeout(self, mock_capture: Any) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            repo = Path(tmp)
            request = build_capture_request(repo)
            submit_capture_request(repo, request)
            result = run_quota_capture_via_worker(
                repo, auto_navigate=False, timeout_seconds=0.6
            )
            self.assertFalse(result.get("ok"))
            self.assertIn("did not respond", result.get("error", ""))
            mock_capture.assert_not_called()

    def test_quota_worker_dir_created(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            repo = Path(tmp)
            path = quota_worker_dir(repo)
            self.assertTrue(path.is_dir())

    def test_resolve_capture_repo_root_vm_override(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            host = Path(tmp)
            with patch.dict(
                os.environ,
                {"THERIAC_QUOTA_VM_REPO_ROOT": r"Z:\Lore_bible"},
                clear=False,
            ):
                self.assertEqual(resolve_capture_repo_root(host), Path(r"Z:\Lore_bible"))

    def test_worker_ready_and_shutdown_signals(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            repo = Path(tmp)
            self.assertFalse(is_worker_ready(repo))
            write_worker_ready(repo)
            self.assertTrue(is_worker_ready(repo))
            self.assertFalse(shutdown_requested(repo))
            request_worker_shutdown(repo)
            self.assertTrue(shutdown_requested(repo))
            clear_worker_ready(repo)
            self.assertFalse(is_worker_ready(repo))

    @patch("pipeline.quota_capture._find_antigravity_window", return_value=None)
    def test_quota_capture_fails_without_window(self, _find: Any) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            repo = Path(tmp)
            result = run_quota_capture(repo, auto_navigate=False)
            self.assertFalse(result.get("ok"))
            meta_path = repo / "artifacts" / "quota_snapshots" / "latest.meta.json"
            self.assertTrue(meta_path.exists())

    def test_running_progress_summary(self) -> None:
        progress = pipeline_progress_from_logs(
            [
                "[1/12] START Stage 01 Entity Bootstrap",
                "[1/12] DONE  Stage 01 Entity Bootstrap",
                "[2/12] START Stage 02 Message Normalization",
            ],
            "running",
            "Pipeline run started.",
        )
        self.assertIn("Running stage 2/12", progress["summary"])


class PipelineLaunchTests(unittest.TestCase):
    @patch("pipeline.pipeline_launch.subprocess.Popen")
    @patch("pipeline.pipeline_launch.pipeline_worker_running", return_value=False)
    def test_pipeline_handoff_starts_pipeline_sentinel_watch(
        self, _running: Any, mock_popen: Any
    ) -> None:
        from pipeline.pipeline_launch import pipeline_handoff

        mock_proc = mock_popen.return_value
        mock_proc.pid = 4242
        with tempfile.TemporaryDirectory() as tmp:
            repo = Path(tmp)
            run = repo / "artifacts" / "runs" / "test_run"
            run.mkdir(parents=True)
            (repo / "config").mkdir(parents=True)
            (repo / "config" / "pipeline_config.json").write_text(
                '{"paths":{"docx_lore_bible":"doc.docx","discord_conversations_root":"conv"}}',
                encoding="utf-8",
            )
            (repo / "doc.docx").write_bytes(b"x")
            (repo / "conv").mkdir()
            (repo / "scripts").mkdir()
            (repo / "scripts" / "pipeline_watch_sentinel.py").write_text("# sentinel stub\n", encoding="utf-8")
            from pipeline.ui_review_app import save_last_open_artifacts_root

            save_last_open_artifacts_root(repo, run)
            result = pipeline_handoff(repo, start_pipeline=True, start_sentinel_daemon=True, start_watch=True)
            self.assertTrue(result.get("ok"))
            self.assertEqual(mock_popen.call_count, 2)
            self.assertIn("watch_job", result)
            self.assertIn("job_id", result["watch_job"])

    def test_build_run_pipeline_command_resume(self) -> None:
        from pipeline.pipeline_launch import build_run_pipeline_command

        with tempfile.TemporaryDirectory() as tmp:
            repo = Path(tmp)
            run = repo / "artifacts" / "runs" / "r1"
            run.mkdir(parents=True)
            (repo / "config").mkdir()
            (repo / "config" / "pipeline_config.json").write_text("{}", encoding="utf-8")
            cmd = build_run_pipeline_command(repo, run, resume=True)
            self.assertIn("--resume", cmd)
            self.assertIn(str(run), cmd)


if __name__ == "__main__":
    unittest.main()
