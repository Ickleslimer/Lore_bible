"""Shared helpers for Stage 07C/07E theme evidence selection (routing only, not theme definitions)."""

from __future__ import annotations

from typing import Any


def adjudication_supports_theme_evidence(rec: dict[str, Any]) -> bool:
    """Route 07B lore candidates into theme mining without keyword or mythology-only filters."""
    if str(rec.get("recommended_action", "")) in {"demote_meta", "mark_generic", "no_action"}:
        return False
    if str(rec.get("recommended_track", "")) not in {"lore_candidate", "mixed", "unknown"}:
        return False
    if str(rec.get("externality_class", "")) in {
        "external_fictional_ip",
        "real_world_person",
        "real_world_org",
        "generic_phrase",
    }:
        return False
    return True
