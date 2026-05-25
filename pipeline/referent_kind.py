from __future__ import annotations

from typing import Any

from pipeline.entity_resolution import (
    PROTECTED_LORE_ENTITY_KEYS,
    is_protected_lore_entity_key,
    normalized_name_key,
)

REFERENT_KINDS = frozenset(
    {
        "stable_in_world_label",
        "working_shorthand",
        "role_referent",
        "incidental_language",
        "mixed_or_uncertain",
    }
)

ROLE_REFERENT_KEYS = frozenset(
    {
        "player",
        "the player",
        "narrator",
        "the narrator",
    }
)

WORKING_SHORTHAND_KEYS = frozenset(
    {
        "the lab",
        "lab",
    }
)

# Single-token discourse fragments (not codenames). Often harvested from "Book of …", "song names", etc.
FRAGMENTARY_DISCOURSE_SINGLETON_KEYS = frozenset(
    {
        "book",
        "books",
        "name",
        "names",
        "title",
        "titles",
        "chapter",
        "section",
        "verse",
        "song",
        "songs",
        "lyric",
        "lyrics",
        "line",
        "lines",
        "word",
        "words",
        "phrase",
        "phrases",
        "topic",
        "note",
        "notes",
        "question",
        "answer",
        "list",
        "type",
        "types",
        "part",
        "parts",
        "version",
        "ending",
        "beginning",
        "used",
        "suit",
        "wedding",
        "robots",
        "robot",
    }
)

# Never treat these single-token keys as fragmentary (Theriac codenames / role / shorthand).
FRAGMENTARY_SINGLETON_EXCEPTION_KEYS = frozenset(
    {
        "love",
        "loss",
        "fear",
        "greed",
        "altruism",
        "player",
        "lab",
        "theriac",
        "ruinr",
        "hectr",
        "joy",
        "war",
        "hope",
        "rage",
        "spite",
    }
)

REFERENT_KIND_LABELS = {
    "stable_in_world_label": "Stable in-world label",
    "working_shorthand": "Working shorthand",
    "role_referent": "Role referent (unnamed)",
    "incidental_language": "Incidental language",
    "mixed_or_uncertain": "Mixed / uncertain",
}


def normalize_referent_kind(value: Any) -> str:
    clean = str(value or "").strip().lower()
    return clean if clean in REFERENT_KINDS else "mixed_or_uncertain"


def referent_kind_label(kind: str) -> str:
    return REFERENT_KIND_LABELS.get(normalize_referent_kind(kind), kind.replace("_", " ").title())


def is_incidental_language_key(key: str) -> bool:
    from pipeline.stage_07_entity_resolution import is_generic_conversation_entity_name

    normalized = normalized_name_key(key)
    if is_fragmentary_discourse_singleton(normalized):
        return True
    return is_generic_conversation_entity_name(normalized)


def is_fragmentary_discourse_singleton(key: str) -> bool:
    normalized = normalized_name_key(key)
    if not normalized or " " in normalized:
        return False
    if normalized in FRAGMENTARY_SINGLETON_EXCEPTION_KEYS:
        return False
    if is_protected_lore_entity_key(normalized):
        return False
    if normalized in ROLE_REFERENT_KEYS or normalized in WORKING_SHORTHAND_KEYS:
        return False
    return normalized in FRAGMENTARY_DISCOURSE_SINGLETON_KEYS


def adjudication_externality_class(candidate: dict[str, Any]) -> str:
    for container in (candidate, candidate.get("item") if isinstance(candidate.get("item"), dict) else {}):
        if not isinstance(container, dict):
            continue
        recommendation = container.get("adjudication_recommendation")
        if isinstance(recommendation, dict):
            externality = str(recommendation.get("externality_class") or "").strip()
            if externality:
                return externality
    return ""


def infer_referent_kind(candidate: dict[str, Any], key: str | None = None) -> str:
    normalized = normalized_name_key(
        key
        or candidate.get("normalized_name_key")
        or candidate.get("candidate_name")
        or ""
    )
    if not normalized:
        return "mixed_or_uncertain"

    if is_incidental_language_key(normalized):
        return "incidental_language"

    externality = adjudication_externality_class(candidate)
    reserved_singletons = (
        WORKING_SHORTHAND_KEYS
        | ROLE_REFERENT_KEYS
        | PROTECTED_LORE_ENTITY_KEYS
        | FRAGMENTARY_SINGLETON_EXCEPTION_KEYS
    )
    if externality == "generic_phrase" and normalized not in reserved_singletons:
        return "incidental_language"

    if normalized in WORKING_SHORTHAND_KEYS:
        return "working_shorthand"
    if normalized in ROLE_REFERENT_KEYS:
        return "role_referent"
    if is_protected_lore_entity_key(normalized):
        return "stable_in_world_label"

    annotation = candidate.get("model_annotation", {}) if isinstance(candidate.get("model_annotation"), dict) else {}
    for source in (
        candidate.get("referent_kind"),
        candidate.get("model_referent_kind"),
        annotation.get("referent_kind"),
    ):
        if source:
            kind = normalize_referent_kind(source)
            if kind != "mixed_or_uncertain":
                return kind

    denotation = str(
        candidate.get("model_denotation_class")
        or annotation.get("denotation_class")
        or ""
    ).strip()
    if denotation == "likely_generic_phrase":
        return "incidental_language"
    if denotation == "likely_alias":
        return "stable_in_world_label"
    if denotation == "likely_lore_entity" and is_fragmentary_discourse_singleton(normalized):
        return "incidental_language"
    if denotation == "likely_lore_entity":
        flags = candidate.get("signal_flags", {}) if isinstance(candidate.get("signal_flags"), dict) else {}
        if bool(flags.get("music_quest_pattern")) and normalized in WORKING_SHORTHAND_KEYS:
            return "working_shorthand"
        return "stable_in_world_label"
    if denotation in {"likely_meta_reference", "likely_external_reference"}:
        return "mixed_or_uncertain"

    flags = candidate.get("signal_flags", {}) if isinstance(candidate.get("signal_flags"), dict) else {}
    if bool(flags.get("generic_phrase")) or bool(flags.get("stopword_name")):
        return "incidental_language"

    return "mixed_or_uncertain"


def referent_kind_for_candidate(candidate: dict[str, Any]) -> str:
    return infer_referent_kind(candidate)


def attach_referent_kind(candidate: dict[str, Any]) -> dict[str, Any]:
    kind = infer_referent_kind(candidate)
    candidate["referent_kind"] = kind
    candidate["referent_kind_label"] = referent_kind_label(kind)
    annotation = candidate.get("model_annotation")
    if isinstance(annotation, dict):
        annotation["referent_kind"] = kind
    return candidate


def should_suppress_entity_inventory_row(referent_kind: str, *, bucket: str = "") -> bool:
    if normalize_referent_kind(referent_kind) == "incidental_language":
        return True
    if str(bucket or "").strip() in {"generic", "ignore"}:
        return normalize_referent_kind(referent_kind) not in {
            "role_referent",
            "working_shorthand",
            "stable_in_world_label",
        }
    return False


def inventory_bucket_for_referent_kind(
    referent_kind: str,
    *,
    fallback_bucket: str,
) -> str:
    kind = normalize_referent_kind(referent_kind)
    if kind == "incidental_language":
        return "generic"
    if kind == "role_referent":
        return "role"
    if kind == "working_shorthand":
        return "shorthand"
    if kind == "stable_in_world_label" and fallback_bucket in {"generic", "ignore"}:
        return "review"
    return fallback_bucket


def should_clamp_to_generic_phrase(candidate: dict[str, Any], externality_class: str) -> bool:
    kind = referent_kind_for_candidate(candidate)
    if kind in {"role_referent", "working_shorthand", "stable_in_world_label"}:
        return False
    key = normalized_name_key(str(candidate.get("candidate_name") or candidate.get("normalized_name_key") or ""))
    if is_protected_lore_entity_key(key):
        return False
    return externality_class == "generic_phrase" or kind == "incidental_language"
