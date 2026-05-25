from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

from pipeline.stage_08q_quest_tagging import run as run_stage_08q

FIXTURES = Path(__file__).resolve().parent / "fixtures"


def test_stage_08q_heuristic_only(tmp_path: Path) -> None:
    snippets = tmp_path / "snippets.jsonl"
    snippets.write_text(
        json.dumps(
            {
                "snippet_id": "s1",
                "knowledge_track": "meta",
                "display_text_normalized": 'Quest "Cochise" by Audioslave for Oyuun is locked.',
            }
        )
        + "\n",
        encoding="utf-8",
    )
    out_tags = tmp_path / "out" / "snippet_quest_tags.jsonl"
    config = {
        "quest_tagging": {
            "enabled": True,
            "include_meta_track": True,
            "model_batch_enabled": False,
            "external_lookup_enabled": False,
            "max_snippets": 50,
            "examples_path": str(FIXTURES / "quest_song_seed.json"),
            "motifs_path": str(FIXTURES / "quest_motifs.json"),
        }
    }
    config_path = tmp_path / "config.json"
    config_path.write_text(json.dumps(config), encoding="utf-8")

    run_stage_08q(
        snippets,
        out_tags,
        in_pipeline_config_json=config_path,
        in_entity_seed_json=None,
    )
    lines = [json.loads(line) for line in out_tags.read_text(encoding="utf-8").splitlines() if line.strip()]
    assert len(lines) >= 1
    assert lines[0]["match_kind"] == "exact_known"
    assert (tmp_path / "out" / "discovered_quests.json").exists()


def test_stage_08q_model_batch_mock(tmp_path: Path) -> None:
    snippets = tmp_path / "snippets.jsonl"
    snippets.write_text(
        json.dumps(
            {
                "snippet_id": "s2",
                "knowledge_track": "meta",
                "display_text_normalized": "Maybe a new Guns N Roses quest for Izanami?",
            }
        )
        + "\n",
        encoding="utf-8",
    )
    out_tags = tmp_path / "tags.jsonl"
    config = {
        "quest_tagging": {
            "enabled": True,
            "include_meta_track": True,
            "model_batch_enabled": True,
            "external_lookup_enabled": False,
            "max_snippets": 50,
            "min_character_confidence": 0.99,
            "examples_path": str(FIXTURES / "quest_song_seed.json"),
            "motifs_path": str(FIXTURES / "quest_motifs.json"),
        },
        "model_routing": {"tasks": {"stage_08q_quest_tagging": {"profile": "high_volume"}}, "profiles": {}},
    }
    config_path = tmp_path / "config.json"
    config_path.write_text(json.dumps(config), encoding="utf-8")

    mock_response = {
        "tags": [
            {
                "snippet_id": "s2",
                "quest_label": "November Rain",
                "main_character": "Izanami",
                "motif_id": "guns_n_roses",
                "artist_attributions": ["Guns N' Roses"],
                "confidence": 0.77,
                "rationale": "Model test",
            }
        ]
    }
    with patch("pipeline.stage_08q_quest_tagging.call_model_chat", return_value=mock_response):
        run_stage_08q(snippets, out_tags, in_pipeline_config_json=config_path)

    rows = [json.loads(line) for line in out_tags.read_text(encoding="utf-8").splitlines() if line.strip()]
    assert any(r.get("quest_label") == "November Rain" for r in rows)
    assert any(r.get("source") == "model_batch" for r in rows)
