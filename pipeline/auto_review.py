"""AI-powered auto-review for pending THERIAC lore pipeline items.

Sends each pending review item to OpenRouter with a structured prompt,
parses the model's accept/reject/defer decision, and writes it to the
corresponding decisions file.
"""
from __future__ import annotations

import json
import re
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any, Callable

from pipeline.common import get_logger, now_utc_iso, read_json, read_jsonl, write_json
from pipeline.review_memory import normalize_claim_text

logger = get_logger(__name__)
DEFAULT_AUTO_REVIEW_MODEL = "deepseek/deepseek-v4-flash"


# ---------------------------------------------------------------------------
# OpenRouter helpers (self-contained so auto_review has no coupling to the
# rate-limit / pacing state inside model_provider). The
# _gemini_generate function name is retained for older tests/call sites.
# ---------------------------------------------------------------------------

def _resolve_api_key() -> str | None:
    """Return the first available OpenRouter API key from env or .env file."""
    import os

    for key_name in ("OPENROUTER_API_KEY", "OPENROUTER_KEY", "OPEN_ROUTER_API_KEY"):
        value = os.environ.get(key_name, "").strip().strip('"').strip("'")
        if value:
            return value

    env_path = Path(__file__).resolve().parents[1] / ".env"
    if not env_path.exists():
        return None
    try:
        text = env_path.read_text(encoding="utf-8", errors="replace")
    except Exception:
        return None
    import re
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        for key_name in ("OPENROUTER_API_KEY", "OPENROUTER_KEY", "OPEN_ROUTER_API_KEY"):
            m = re.match(rf"^\s*{re.escape(key_name)}\s*[:=]\s*(.+?)\s*$", stripped)
            if m:
                raw = m.group(1).strip()
                if (raw.startswith('"') and raw.endswith('"')) or (raw.startswith("'") and raw.endswith("'")):
                    raw = raw[1:-1]
                if raw:
                    return raw
    return None


def _gemini_generate(
    api_key: str,
    prompt: str,
    model: str = DEFAULT_AUTO_REVIEW_MODEL,
    temperature: float = 0.0,
    timeout_seconds: int = 120,
) -> dict[str, Any] | None:
    """Call OpenRouter chat completions and return parsed JSON or None."""
    clean_model = model.strip() or DEFAULT_AUTO_REVIEW_MODEL
    url = "https://openrouter.ai/api/v1/chat/completions"
    payload = {
        "model": clean_model,
        "messages": [
            {"role": "system", "content": "You are a precise JSON reviewer. Return strict JSON only with no markdown."},
            {"role": "user", "content": prompt},
        ],
        "temperature": temperature,
        "response_format": {"type": "json_object"},
    }
    req = urllib.request.Request(
        url=url,
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {api_key}",
            "HTTP-Referer": "https://github.com/theriac/lore-bible",
            "X-Title": "THERIAC Lore Bible",
        },
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
            pass
        logger.warning(
            "OpenRouter auto-review HTTP error status=%s reason=%s body=%s",
            exc.code, exc.reason, err_body[:300],
        )
        return None
    except Exception as exc:
        logger.warning("OpenRouter auto-review request failed: %s", exc)
        return None

    try:
        body = json.loads(raw_body)
    except json.JSONDecodeError:
        logger.warning("OpenRouter auto-review response not valid JSON.")
        return None
    if not isinstance(body, dict):
        return None

    # Extract text from response
    choices = body.get("choices", [])
    if not isinstance(choices, list) or not choices:
        return None
    first = choices[0] if isinstance(choices[0], dict) else {}
    message = first.get("message", {}) if isinstance(first, dict) else {}
    if not isinstance(message, dict):
        return None
    text = str(message.get("content", "")).strip()
    if not text:
        return None

    # Strip markdown fencing if present
    import re
    if text.startswith("```"):
        fenced = re.match(r"^```(?:json)?\s*(.*?)\s*```$", text, flags=re.DOTALL | re.IGNORECASE)
        if fenced:
            text = fenced.group(1).strip()

    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        logger.warning("OpenRouter auto-review content not valid JSON: %s", text[:200])
        return None
    if isinstance(parsed, dict):
        return parsed
    if isinstance(parsed, list):
        return {"_items": parsed}
    return None


# ---------------------------------------------------------------------------
# Review prompts
# ---------------------------------------------------------------------------

_CONVERSATION_ENTITY_PROMPT = """\
You are reviewing a proposed conversation entity for the THERIAC lore bible.
Your job is to decide whether this entity should be APPROVED as a real entity
in the THERIAC universe or REJECTED as noise / not a real entity.

Always make a best-guess approve/reject decision. If the evidence is ambiguous,
still choose the more likely decision, but set human_review_recommended=true.

Run context:
```json
{context_json}
```

The entity proposal is:
```json
{item_json}
```

Respond with a JSON object:
{{
  "decision": "approve" or "reject",
  "canonical_name": "<the best canonical name for this entity>",
  "entity_type": "<one of: term, theme, quest, event, character, faction, organization, location, timeline_node>",
  "secondary_entity_types": ["optional secondary type(s), e.g. faction or organization for a location/institution"],
  "human_review_recommended": true or false,
  "human_review_reason": "<short reason if human_review_recommended is true; otherwise empty>",
  "rationale": "<1-2 sentence explanation>"
}}

Guidelines:
- APPROVE only when the supplied evidence explicitly establishes a durable
  THERIAC entity or an explicit alias/codename/title/working name for one.
- REJECT entities that are real-world references, generic English words with
  no special THERIAC meaning, or names of real people who are team members
  rather than characters.
- Recommend human review when the candidate is broad, abstract, low-evidence,
  mostly production/meta, type-ambiguous, or could be a theme/mechanic/category
  rather than a page-worthy entity. Still make the best approve/reject guess.
- Working names are allowed. Approve a placeholder-style name, such as a role
  label, only when the evidence clearly shows it refers to one specific
  recurring in-world entity; keep the working name as canonical for now.
- AI agents, robots, synthetic minds, and named computer personalities should
  be classified as character when they function as in-world actors. Do not
  classify uppercase technical acronyms as entities from capitalization alone.
- Use location for physical places, sites, facilities, labs, roads, bases, and
  buildings. If a lab/facility is both a place and an institution, prefer
  location unless the evidence mainly describes its people or political role.
- For alias groups, approve only if every child alias is explicitly supported
  as an alias/rename/codename/title/working name of the same canonical entity.
  If some child aliases are thin or ambiguous, make the best group decision and
  set human_review_recommended=true.
- Do not infer "character" status from grammar alone. Pronouns in patch-note
  summaries are not enough unless the source text explicitly treats the
  candidate as an individual in-world actor.
- When in doubt, make the best guess and set human_review_recommended=true so
  the human reviewer sees it without blocking the pipeline.
- Use the candidate_name or suggested_canonical_name as the canonical_name
  unless you see a better form.
- Pick the entity_type that best fits the evidence.
"""

_CLAIM_PROMPT = """\
You are reviewing an atomic lore claim extracted from THERIAC Discord
conversations. Decide whether this claim should be ACCEPTED into the lore
bible or REJECTED.

The review item includes the claim, source snippet context, support warnings,
exact duplicate grouping, and other claims that share the same source set:
```json
{item_json}
```

Respond with a JSON object:
{{
  "decision": "accept" or "reject",
  "human_review_recommended": true or false,
  "human_review_reason": "<short reason if human_review_recommended is true; otherwise empty>",
  "rationale": "<1-2 sentence explanation>"
}}

Guidelines:
- ACCEPT claims that describe concrete lore facts, character traits, world
  events, game mechanics, faction relationships, or timeline events.
- REJECT claims that are purely meta/production discussion with no lore
  content, speculative ideas that were explicitly abandoned, unsupported
  repetitions of already-established facts, or real-world personal chat.
- Use source_context as the primary evidence. A claim should be accepted only
  if its source snippets directly support it.
- Treat support_warnings as caution flags. They do not require rejection by
  themselves, but they usually require human_review_recommended=true unless
  the source_context clearly resolves the warning.
- If duplicate_claim_ids contains more than one claim, review the group once.
  If the claim is otherwise valid, accept the representative decision but set
  human_review_recommended=true so the human can decide whether to collapse or
  ignore the duplicates.
- Use source_set_peer_claims to notice over-fragmentation from the same exact
  source set. Do not reject a claim merely because there are peers; reject only
  claims that are redundant, unsupported, or contradicted by the source.
- If contradiction_notes are non-empty, make the best decision and set
  human_review_recommended=true.
- When in doubt, ACCEPT - it's better to have a slightly noisy lore bible
  than to lose genuine lore, but set human_review_recommended=true when the
  uncertainty matters.
"""

_IDENTITY_MERGE_PROMPT = """\
You are reviewing a proposed identity merge for the THERIAC lore bible.
This proposes that two entity names actually refer to the same entity.

The proposal is:
```json
{item_json}
```

Respond with a JSON object:
{{
  "decision": "approve" or "reject",
  "rationale": "<1-2 sentence explanation>"
}}

Guidelines:
- APPROVE merges where the evidence clearly shows both names refer to the
  same in-universe entity (e.g. a rename, an alias, an acronym expansion).
- REJECT merges where the names refer to genuinely distinct entities that
  happen to share a word or partial name.
"""

_CARD_PROMPT = """\
You are reviewing a synthesized wiki-style lore card for the THERIAC lore
bible. Decide whether the card draft is good enough to APPROVE as canonical
or should be REJECTED for revision.

The card draft is:
```json
{item_json}
```

Respond with a JSON object:
{{
  "decision": "approve" or "reject",
  "rationale": "<1-2 sentence explanation>",
  "edited_summary": "<optional: an improved summary if you think the current one needs small fixes, otherwise empty string>"
}}

Guidelines:
- APPROVE cards that present a coherent, well-written summary of the entity
  based on the accepted claims.
- REJECT cards only if the summary is factually incoherent, contradicts
  its own claims list, or is too garbled to be useful.
- Minor style issues are not grounds for rejection; note them in rationale.
"""


# ---------------------------------------------------------------------------
# Batch auto-review
# ---------------------------------------------------------------------------

def _read_json_or_default(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        return read_json(path)
    except Exception:
        return default


def _decision_ids(path: Path, id_fields: list[str]) -> set[str]:
    payload = _read_json_or_default(path, {"decisions": []})
    ids: set[str] = set()
    for decision in payload.get("decisions", []):
        for field in id_fields:
            value = str(decision.get(field, "")).strip()
            if value:
                ids.add(value)
    return ids


def _run_root_from_claims_path(path: Path) -> Path:
    """Best-effort run root detection for 06_drafts/card_drafts/claim_drafts.json."""
    resolved = path.resolve()
    candidates: list[Path] = []
    try:
        candidates.append(resolved.parents[2])
    except IndexError:
        pass
    candidates.extend([resolved.parent, *resolved.parents])
    for candidate in candidates:
        if (candidate / "07_review").exists() or (candidate / "06_drafts").exists():
            return candidate
    return resolved.parent


def _story_answered_claim_ids(root: Path) -> set[str]:
    """Claim ids already covered by answered Story Questions should not be auto-reviewed."""
    session_path = root / "07_review" / "story_question_session.json"
    session = _read_json_or_default(session_path, {})
    if not isinstance(session, dict):
        return set()
    answered_question_ids = {
        str(answer.get("question_id", "")).strip()
        for answer in session.get("answers", []) or []
        if isinstance(answer, dict) and str(answer.get("question_id", "")).strip()
    }
    for application in session.get("applications", []) or []:
        if isinstance(application, dict) and str(application.get("question_id", "")).strip():
            answered_question_ids.add(str(application.get("question_id", "")).strip())
    pending_proposal = session.get("pending_application_proposal")
    if isinstance(pending_proposal, dict) and str(pending_proposal.get("question_id", "")).strip():
        # The author has already answered and is reviewing the application proposal.
        answered_question_ids.add(str(pending_proposal.get("question_id", "")).strip())

    answered_claim_ids: set[str] = set()
    for question in session.get("questions", []) or []:
        if not isinstance(question, dict):
            continue
        question_id = str(question.get("question_id", "")).strip()
        if question_id not in answered_question_ids and str(question.get("status", "")).strip().lower() != "answered":
            continue
        for claim_id in question.get("linked_claim_ids", []) or []:
            claim_id_text = str(claim_id).strip()
            if claim_id_text:
                answered_claim_ids.add(claim_id_text)
    return answered_claim_ids


def _author_claim_normalized_texts(root: Path) -> set[str]:
    """Return normalized author-supplied claim texts for duplicate suppression."""
    payload = _read_json_or_default(root / "07_review" / "author_claims.json", {"claims": []})
    texts: set[str] = set()
    for claim in payload.get("claims", []) if isinstance(payload, dict) else []:
        if not isinstance(claim, dict):
            continue
        normalized = str(claim.get("normalized_claim_text", "")).strip()
        if not normalized:
            normalized = normalize_claim_text(str(claim.get("claim_text", "") or ""))
        if normalized:
            texts.add(normalized)
    return texts


def _claim_normalized_text(claim: dict[str, Any]) -> str:
    normalized = str(claim.get("normalized_claim_text", "")).strip()
    if normalized:
        return normalized
    return normalize_claim_text(str(claim.get("claim_text", "") or ""))


def _has_human_override_for_proposal(payload: dict[str, Any], proposal_id: str) -> bool:
    for decision in payload.get("decisions", []) if isinstance(payload, dict) else []:
        if not isinstance(decision, dict):
            continue
        if str(decision.get("proposal_id", "")).strip() != proposal_id:
            continue
        reviewer = str(decision.get("reviewer", "")).strip().lower()
        if bool(decision.get("human_override")):
            return True
        if reviewer and "auto_review" not in reviewer and "gemini_auto" not in reviewer:
            return True
    return False


def _truncate_item(item: dict[str, Any], max_chars: int = 6000) -> str:
    """Serialise an item to JSON, truncating very large fields."""
    # Drop very large evidence arrays to keep prompt under limits
    trimmed = dict(item)
    for key in ("sample_texts", "source_snippet_ids", "evidence_snippets",
                "accepted_claims", "claims", "claim_ids"):
        value = trimmed.get(key)
        if isinstance(value, list) and len(value) > 8:
            trimmed[key] = value[:8] + [f"... +{len(value) - 8} more"]
    text = json.dumps(trimmed, ensure_ascii=False, indent=2)
    if len(text) > max_chars:
        text = text[:max_chars] + "\n... (truncated)"
    return text


def _conversation_entity_context(proposals_path: Path) -> dict[str, Any]:
    """Return compact run context for entity auto-review prompts."""
    resolved_path = proposals_path.with_name("resolved_entities.json")
    context: dict[str, Any] = {
        "review_policy": (
            "Auto-review is conservative: approve/reject only high-confidence cases; "
            "leave ambiguous, low-evidence, broad, or mixed meta/lore proposals pending for human review."
        ),
        "resolved_entities": [],
        "seed_only_entities_count": 0,
    }
    if not resolved_path.exists():
        return context
    payload = _read_json_or_default(resolved_path, {})
    if not isinstance(payload, dict):
        return context
    for entity in payload.get("resolved_entities", []) or []:
        if not isinstance(entity, dict):
            continue
        context["resolved_entities"].append(
            {
                "canonical_name": entity.get("canonical_name", ""),
                "entity_type": entity.get("entity_type", ""),
                "aliases": entity.get("aliases", []) or [],
            }
        )
    context["seed_only_entities_count"] = len(payload.get("seed_only_entities", []) or [])
    return context


COMMON_ABSTRACT_OR_CATEGORY_KEYS = {
    "aging",
    "antagonist",
    "antagonists",
    "character deaths",
    "combat mech",
    "death",
    "final boss",
    "humanity",
    "immortalist",
    "instrumentality",
    "lab members",
    "main cast",
    "player",
    "player character",
    "protagonist",
    "robots",
    "sin",
    "suit",
    "suits",
    "war",
}
LOW_EVIDENCE_AUTO_REVIEW_MIN = 3
ALIAS_GROUP_CHILD_EVIDENCE_MIN = 2


def _name_key(value: str) -> str:
    import re

    key = re.sub(r"[^a-z0-9]+", " ", str(value).lower())
    return re.sub(r"\s+", " ", key).strip()


def _conversation_entity_attention_reason(item: dict[str, Any], *, is_alias_group: bool) -> str:
    """Return a reason to flag a best-guess decision for human review, or empty."""
    if is_alias_group:
        aliases = [a for a in item.get("alias_candidates", []) or [] if isinstance(a, dict)]
        low_evidence = [
            str(alias.get("candidate_name", "")).strip()
            for alias in aliases
            if int(alias.get("evidence_count", 0) or 0) < ALIAS_GROUP_CHILD_EVIDENCE_MIN
        ]
        if low_evidence:
            return "alias group contains low-evidence child alias(es): " + ", ".join(low_evidence[:6])
        return ""

    evidence_count = int(item.get("evidence_count", 0) or 0)
    if evidence_count < LOW_EVIDENCE_AUTO_REVIEW_MIN:
        return f"only {evidence_count} evidence item(s)"

    key = _name_key(str(item.get("candidate_name", "")))
    if key in COMMON_ABSTRACT_OR_CATEGORY_KEYS:
        return "broad/common category or abstract term needs human interpretation"

    topics = {_name_key(str(topic)) for topic in item.get("candidate_topics", []) or []}
    source_kinds = {_name_key(str(kind)) for kind in item.get("source_kinds", []) or []}
    if "production" in topics or any("meta" in kind for kind in source_kinds):
        return "mixed production/meta evidence needs human review"

    proposed_type = str(item.get("proposed_entity_type", "")).strip().lower()
    type_conflicts = item.get("type_conflicts", [])
    if isinstance(type_conflicts, list) and type_conflicts:
        conflict_types = [
            str(conflict.get("entity_type", "")).strip()
            for conflict in type_conflicts
            if isinstance(conflict, dict) and str(conflict.get("entity_type", "")).strip()
        ]
        if conflict_types:
            return "type-conflicted evidence needs human review: " + ", ".join(conflict_types[:4])
    if proposed_type == "character" and key.endswith("s"):
        return "plural character-like candidate needs human review"

    return ""


def _conversation_entity_review_item(item: dict[str, Any]) -> dict[str, Any]:
    """Trim a proposal to the fields most useful for model review."""
    keep_fields = [
        "proposal_id",
        "group_kind",
        "candidate_name",
        "suggested_canonical_name",
        "proposed_entity_type",
        "evidence_count",
        "confidence",
        "review_priority",
        "triage_reason",
        "candidate_topics",
        "source_kinds",
        "patch_item_types",
        "patch_update_types",
        "patch_relationship_types",
        "type_reconsidered",
        "type_conflicts",
        "type_vote_totals",
        "type_review_notes",
        "alias_review_notes",
        "sample_texts",
    ]
    return {field: item.get(field) for field in keep_fields if field in item}


def _alias_group_review_item(group: dict[str, Any], proposals_by_id: dict[str, dict[str, Any]]) -> dict[str, Any]:
    item = _conversation_entity_review_item(group)
    children: list[dict[str, Any]] = []
    for alias in group.get("alias_candidates", []) or []:
        if not isinstance(alias, dict):
            continue
        proposal = proposals_by_id.get(str(alias.get("proposal_id", "")), {})
        merged = {**alias, **_conversation_entity_review_item(proposal)}
        children.append(merged)
    item["alias_candidates"] = children
    item["child_review_rule"] = (
        "Make a best approve/reject guess for the group. If any child alias is thin or ambiguous, mark human_review_recommended true."
    )
    return item


def _normalise_entity_review_decision(value: str) -> str:
    decision = str(value).strip().lower()
    if decision in {"approve", "accept", "approved"}:
        return "approve"
    if decision in {"reject", "rejected"}:
        return "reject"
    return "approve"


def _coerce_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    return str(value).strip().lower() in {"1", "true", "yes", "y", "recommended"}


def _entity_type_attention_reason(proposal: dict[str, Any], entity_type: str) -> str:
    proposed_type = str(proposal.get("proposed_entity_type", "")).strip().lower()
    decided_type = str(entity_type).strip().lower()
    if proposed_type and decided_type and proposed_type != decided_type:
        return f"type changed from {proposed_type} to {decided_type}"
    return ""


def _postprocess_entity_type(candidate_name: str, entity_type: str, item: dict[str, Any]) -> tuple[str, list[str], str]:
    """Normalize obvious physical-place types while preserving secondary context."""
    key = _name_key(candidate_name)
    decided_type = str(entity_type or item.get("proposed_entity_type") or "term").strip().lower()
    if decided_type == "ai_system":
        decided_type = "character"
    secondary_types: list[str] = []
    reason = ""
    sample_text = "\n".join(str(text) for text in item.get("sample_texts", []) or []).lower()
    key_words = set(key.split())
    name_locationish = key in {"lab", "the lab"} or bool(key_words & {"facility", "clinic", "road", "base", "city"})
    context_locationish = any(word in sample_text for word in ["facility", "building", "construction", "road", "perimeter", "site"])
    locationish = name_locationish or (context_locationish and decided_type in {"organization", "term"})
    if locationish and decided_type in {"organization", "term", "character"}:
        if decided_type == "organization":
            secondary_types.append("organization")
        decided_type = "location"
        reason = "physical-place evidence normalized to location; review if institution/faction is more appropriate"
    return decided_type, secondary_types, reason


def _append_attention_item(
    decisions_path: Path,
    item: dict[str, Any],
    *,
    filename: str = "conversation_entity_auto_review_attention.json",
    id_fields: tuple[str, ...] = ("proposal_id",),
) -> None:
    attention_path = decisions_path.with_name(filename)
    payload = _read_json_or_default(attention_path, {"items": []})
    existing_ids = {
        (field, str(row.get(field, "")).strip())
        for row in payload.get("items", [])
        if isinstance(row, dict)
        for field in id_fields
        if str(row.get(field, "")).strip()
    }
    for field in id_fields:
        value = str(item.get(field, "")).strip()
        if value and (field, value) in existing_ids:
            return
    payload.setdefault("items", []).append(item)
    write_json(attention_path, payload)


class AutoReviewResult:
    """Accumulates statistics from an auto-review run."""
    def __init__(self) -> None:
        self.total = 0
        self.accepted = 0
        self.rejected = 0
        self.failed = 0
        self.skipped = 0
        self.log_lines: list[str] = []

    def summary(self) -> str:
        return (
            f"Auto-review complete: {self.total} items processed, "
            f"{self.accepted} accepted, {self.rejected} rejected, "
            f"{self.failed} API failures, {self.skipped} flagged for human attention."
        )


def _claim_source_ids(claim: dict[str, Any]) -> list[str]:
    out: list[str] = []
    for source_id in claim.get("source_snippet_ids", []) or []:
        sid = str(source_id).strip()
        if sid and sid not in out:
            out.append(sid)
    return out


def _claim_source_set_key(claim: dict[str, Any]) -> str:
    return "|".join(sorted(_claim_source_ids(claim)))


def _claim_duplicate_key(claim: dict[str, Any]) -> tuple[str, str, str]:
    target = str(claim.get("target_entity_id") or claim.get("target_entity_name") or "").strip().lower()
    normalized_claim = str(claim.get("normalized_claim_text") or normalize_claim_text(str(claim.get("claim_text", ""))))
    return target, normalized_claim, _claim_source_set_key(claim)


def _source_snippets_path_for_claims(patches_path: Path) -> Path | None:
    for parent in [patches_path.parent, *patches_path.parents]:
        for candidate in (
            parent / "03_relevance" / "snippets_candidates.jsonl",
            parent / "snippets_candidates.jsonl",
        ):
            if candidate.exists():
                return candidate
    return None


def _load_source_snippets_for_claims(patches_path: Path, claims: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    wanted = {sid for claim in claims for sid in _claim_source_ids(claim)}
    if not wanted:
        return {}
    source_path = _source_snippets_path_for_claims(patches_path)
    if source_path is None:
        return {}
    rows: dict[str, dict[str, Any]] = {}
    try:
        for row in read_jsonl(source_path):
            snippet_id = str(row.get("snippet_id", "")).strip()
            if snippet_id in wanted:
                rows[snippet_id] = row
                if len(rows) >= len(wanted):
                    break
    except Exception:
        return rows
    return rows


def _compact_source_context(claim: dict[str, Any], source_snippets: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    context: list[dict[str, Any]] = []
    for source_id in _claim_source_ids(claim):
        row = source_snippets.get(source_id)
        if not row:
            continue
        text = (
            row.get("patch_item_text")
            or row.get("display_text_normalized")
            or row.get("conversation_patch_summary")
            or row.get("raw_text")
            or ""
        )
        raw_text = str(row.get("raw_text", "")).strip()
        context.append(
            {
                "snippet_id": source_id,
                "conversation_id": row.get("conversation_id", ""),
                "conversation_global_index": row.get("conversation_global_index", ""),
                "timestamp_start_utc": row.get("timestamp_start_utc", ""),
                "topic_label": row.get("conversation_topic_label", ""),
                "topic_summary": str(row.get("conversation_topic_summary", ""))[:700],
                "knowledge_track": row.get("knowledge_track", ""),
                "source_kind": row.get("source_kind", ""),
                "patch_item_type": row.get("patch_item_type", ""),
                "patch_item_text": re.sub(r"\s+", " ", str(text)).strip()[:1400],
                "supporting_messages": raw_text[:1400],
            }
        )
    return context


def _claim_review_item(
    representative: dict[str, Any],
    duplicate_claims: list[dict[str, Any]],
    source_set_claims: list[dict[str, Any]],
    source_snippets: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    duplicate_ids = [str(claim.get("claim_id", "")) for claim in duplicate_claims if str(claim.get("claim_id", "")).strip()]
    duplicate_warnings = list(
        dict.fromkeys(
            str(warning).strip()
            for claim in duplicate_claims
            for warning in claim.get("support_warnings", []) or []
            if str(warning).strip()
        )
    )
    duplicate_contradictions = [
        str(claim.get("contradiction_notes", "")).strip()
        for claim in duplicate_claims
        if str(claim.get("contradiction_notes", "")).strip()
    ]
    source_set_peers = []
    for claim in source_set_claims[:12]:
        claim_id = str(claim.get("claim_id", "")).strip()
        if not claim_id:
            continue
        source_set_peers.append(
            {
                "claim_id": claim_id,
                "target_entity_name": claim.get("target_entity_name", ""),
                "claim_type": claim.get("claim_type", ""),
                "claim_text": claim.get("claim_text", ""),
                "support_warnings": claim.get("support_warnings", []) or [],
                "confidence": claim.get("confidence", ""),
            }
        )
    return {
        "claim_id": representative.get("claim_id", ""),
        "duplicate_claim_ids": duplicate_ids,
        "duplicate_group_size": len(duplicate_ids),
        "source_set_claim_ids": [
            str(claim.get("claim_id", ""))
            for claim in source_set_claims
            if str(claim.get("claim_id", "")).strip()
        ],
        "source_set_group_size": len(source_set_claims),
        "source_set_peer_claims": source_set_peers,
        "target_entity_id": representative.get("target_entity_id", ""),
        "target_entity_name": representative.get("target_entity_name", ""),
        "claim_text": representative.get("claim_text", ""),
        "claim_type": representative.get("claim_type", ""),
        "knowledge_track": representative.get("knowledge_track", ""),
        "confidence": representative.get("confidence", ""),
        "contradiction_notes": "; ".join(dict.fromkeys(duplicate_contradictions)),
        "support_warnings": duplicate_warnings,
        "source_snippet_ids": _claim_source_ids(representative),
        "source_context": _compact_source_context(representative, source_snippets),
        "review_rule": (
            "Make one best accept/reject decision for the representative claim. "
            "The same decision will be copied to exact duplicate_claim_ids. "
            "Use human_review_recommended for duplicates, support warnings, contradictions, or weak source support."
        ),
    }


def _claim_attention_reasons(
    review_item: dict[str, Any],
    response: dict[str, Any],
    decision: str,
) -> list[str]:
    reasons: list[str] = []
    warnings = [str(w).strip() for w in review_item.get("support_warnings", []) or [] if str(w).strip()]
    if warnings:
        reasons.append("support warnings: " + ", ".join(warnings[:4]))
    if str(review_item.get("contradiction_notes", "")).strip():
        reasons.append("claim has contradiction notes")
    if not review_item.get("source_context"):
        reasons.append("source snippet context was unavailable")
    try:
        confidence = float(review_item.get("confidence", 0) or 0)
    except (TypeError, ValueError):
        confidence = 0.0
    if confidence and confidence < 0.65:
        reasons.append(f"low confidence: {confidence:.2f}")
    if int(review_item.get("duplicate_group_size", 0) or 0) > 1:
        reasons.append(f"exact duplicate group: {review_item.get('duplicate_group_size')} claims")
    if int(review_item.get("source_set_group_size", 0) or 0) >= 6:
        reasons.append(f"large source-set group: {review_item.get('source_set_group_size')} claims share the same source set")
    if str(decision).strip().lower() not in {"accept", "reject"}:
        reasons.append("model requested human review or returned a non-final decision")
    if _coerce_bool(response.get("human_review_recommended", False)):
        reason = str(response.get("human_review_reason", "")).strip()
        reasons.append(reason or "model recommended human review")
    return list(dict.fromkeys(reason for reason in reasons if reason))


def run_auto_review(
    paths: dict[str, Path],
    progress_callback: Callable[[str], None] | None = None,
    cancel_check: Callable[[], bool] | None = None,
    model: str = DEFAULT_AUTO_REVIEW_MODEL,
    inter_request_delay: float = 1.0,
) -> AutoReviewResult:
    """Run AI auto-review on all pending items in the given artifact paths.

    Args:
        paths: Dict with keys matching TheriacDesktopApp.paths
               (patches, decisions, card_drafts, card_decisions,
                identity_merge_proposals, identity_merge_decisions,
                conversation_entity_proposals, conversation_entity_decisions).
        progress_callback: Optional function receiving status messages.
        model: OpenRouter model name to use.
        inter_request_delay: Seconds to wait between API calls.

    Returns:
        AutoReviewResult with statistics and log lines.
    """
    result = AutoReviewResult()
    api_key = _resolve_api_key()
    if not api_key:
        result.log_lines.append("[auto-review] ERROR: No OpenRouter API key found.")
        if progress_callback:
            progress_callback("[auto-review] ERROR: No OpenRouter API key found.")
        return result

    def log(msg: str) -> None:
        result.log_lines.append(msg)
        if progress_callback:
            progress_callback(msg)

    log(f"[auto-review] Starting AI auto-review with model={model}...")

    # ---- Conversation entities ----
    if cancel_check and cancel_check():
        log("[auto-review] Cancelled before conversation entities.")
        return result
    _auto_review_conversation_entities(paths, api_key, model, inter_request_delay, result, log, cancel_check)

    # ---- Claims ----
    if cancel_check and cancel_check():
        log("[auto-review] Cancelled before claims.")
        return result
    _auto_review_claims(paths, api_key, model, inter_request_delay, result, log, cancel_check)

    # ---- Identity merges ----
    if cancel_check and cancel_check():
        log("[auto-review] Cancelled before identity merges.")
        return result
    _auto_review_identity_merges(paths, api_key, model, inter_request_delay, result, log, cancel_check)

    # ---- Cards ----
    if cancel_check and cancel_check():
        log("[auto-review] Cancelled before cards.")
        return result
    _auto_review_cards(paths, api_key, model, inter_request_delay, result, log, cancel_check)

    if cancel_check and cancel_check():
        log(f"[auto-review] Cancelled. Partial summary: {result.summary()}")
    else:
        log(f"[auto-review] {result.summary()}")
    return result


def _auto_review_conversation_entities(
    paths: dict[str, Path],
    api_key: str,
    model: str,
    delay: float,
    result: AutoReviewResult,
    log: Callable[[str], None],
    cancel_check: Callable[[], bool] | None = None,
) -> None:
    proposals_path = paths.get("conversation_entity_proposals")
    decisions_path = paths.get("conversation_entity_decisions")
    if not proposals_path or not decisions_path:
        return

    payload = _read_json_or_default(proposals_path, {"proposals": [], "alias_review_groups": []})
    proposals = payload.get("proposals", [])
    proposals_by_id = {
        str(proposal.get("proposal_id", "")): proposal
        for proposal in proposals
        if isinstance(proposal, dict) and str(proposal.get("proposal_id", "")).strip()
    }
    existing_decisions = _decision_ids(decisions_path, ["proposal_id"])
    context_json = _truncate_item(_conversation_entity_context(proposals_path))

    # Collect grouped child IDs
    grouped_child_ids: set[str] = set()
    for group in payload.get("alias_review_groups", []) or []:
        if isinstance(group, dict):
            for child_id in group.get("child_proposal_ids", []) or []:
                grouped_child_ids.add(str(child_id))

    # Process alias review groups first
    for group in payload.get("alias_review_groups", []) or []:
        if cancel_check and cancel_check():
            break
        if not isinstance(group, dict):
            continue
        pending_child_ids = [
            str(child_id)
            for child_id in group.get("child_proposal_ids", []) or []
            if str(child_id).strip() and str(child_id) not in existing_decisions
        ]
        if not pending_child_ids:
            continue

        group_name = str(group.get("suggested_canonical_name") or group.get("candidate_name", "(alias group)"))
        log(f"[auto-review] Reviewing alias group: {group_name} ({len(pending_child_ids)} children)")
        result.total += 1

        review_item = _alias_group_review_item(group, proposals_by_id)
        attention_reasons = [reason for reason in [_conversation_entity_attention_reason(review_item, is_alias_group=True)] if reason]
        if attention_reasons:
            review_item["human_review_hint"] = "; ".join(attention_reasons)

        item_json = _truncate_item(review_item)
        prompt = _CONVERSATION_ENTITY_PROMPT.format(context_json=context_json, item_json=item_json)
        resp = _gemini_generate(api_key, prompt, model=model)

        if resp is None:
            log(f"[auto-review]   FAILED: API returned no valid response for {group_name}")
            result.failed += 1
            time.sleep(delay)
            continue

        raw_decision = str(resp.get("decision", "")).strip().lower()
        decision = _normalise_entity_review_decision(raw_decision)
        canonical_name = str(resp.get("canonical_name", group_name)).strip()
        entity_type, secondary_types, type_reason = _postprocess_entity_type(
            canonical_name or group_name,
            str(resp.get("entity_type", group.get("proposed_entity_type", "term"))).strip(),
            review_item,
        )
        if type_reason:
            attention_reasons.append(type_reason)
        type_attention = _entity_type_attention_reason(group, entity_type)
        if type_attention:
            attention_reasons.append(type_attention)
        response_secondary = resp.get("secondary_entity_types", [])
        if isinstance(response_secondary, list):
            for item in response_secondary:
                item_type = str(item).strip()
                if item_type == "ai_system":
                    item_type = "character"
                if item_type and item_type not in secondary_types:
                    secondary_types.append(item_type)
        if secondary_types:
            attention_reasons.append("multi-type entity; review primary and secondary type assignment")
        rationale = str(resp.get("rationale", "")).strip()
        response_attention = _coerce_bool(resp.get("human_review_recommended", False))
        response_attention_reason = str(resp.get("human_review_reason", "")).strip()
        if raw_decision in {"defer", "deferred", "needs_more_context", "need_more_context", "human_review"}:
            attention_reasons.append("model requested human review; using approve as best guess")
        if response_attention_reason:
            attention_reasons.append(response_attention_reason)
        human_review_recommended = bool(attention_reasons) or response_attention
        human_review_reason = "; ".join(dict.fromkeys(reason for reason in attention_reasons if reason))
        if human_review_recommended:
            result.skipped += 1
        if decision == "approve":
            result.accepted += 1
        else:
            result.rejected += 1
        attention_suffix = " | human review recommended" if human_review_recommended else ""
        log(f"[auto-review]   -> {decision.upper()}: {group_name}{attention_suffix} | {rationale[:80]}")

        data = _read_json_or_default(decisions_path, {"decisions": []})
        timestamp = now_utc_iso()
        for alias in group.get("alias_candidates", []) or []:
            if not isinstance(alias, dict):
                continue
            proposal_id = str(alias.get("proposal_id", "")).strip()
            if not proposal_id or proposal_id in existing_decisions or proposal_id not in set(pending_child_ids):
                continue
            if _has_human_override_for_proposal(data, proposal_id):
                existing_decisions.add(proposal_id)
                continue
            decision_entry = {
                "proposal_id": proposal_id,
                "candidate_name": alias.get("candidate_name", ""),
                "display_name": alias.get("candidate_name", ""),
                "decision": decision,
                "canonical_name": canonical_name,
                "entity_type": entity_type,
                "secondary_entity_types": secondary_types,
                "human_review_recommended": human_review_recommended,
                "human_review_reason": human_review_reason,
                "auto_review_policy": "best_guess_with_attention_queue_v1",
                "reviewer": "openrouter_auto_review",
                "rationale": f"[AI auto-review] {rationale}",
                "timestamp_utc": timestamp,
            }
            data.setdefault("decisions", []).append(decision_entry)
            if human_review_recommended:
                _append_attention_item(decisions_path, {**decision_entry, "group_name": group_name})
            existing_decisions.add(proposal_id)
        write_json(decisions_path, data)
        time.sleep(delay)

    # Process standalone proposals
    pending = [
        p for p in proposals
        if str(p.get("proposal_id", "")).strip()
        and str(p.get("proposal_id", "")) not in existing_decisions
        and str(p.get("proposal_id", "")) not in grouped_child_ids
        and str(p.get("review_status", "pending")) == "pending"
    ]
    if pending:
        log(f"[auto-review] {len(pending)} standalone conversation entity proposals to review.")
    for i, proposal in enumerate(pending, 1):
        if cancel_check and cancel_check():
            break
        name = str(proposal.get("candidate_name", "(unknown)"))
        log(f"[auto-review] [{i}/{len(pending)}] Reviewing entity: {name}")
        result.total += 1

        review_item = _conversation_entity_review_item(proposal)
        attention_reasons = [reason for reason in [_conversation_entity_attention_reason(review_item, is_alias_group=False)] if reason]
        if attention_reasons:
            review_item["human_review_hint"] = "; ".join(attention_reasons)

        item_json = _truncate_item(review_item)
        prompt = _CONVERSATION_ENTITY_PROMPT.format(context_json=context_json, item_json=item_json)
        resp = _gemini_generate(api_key, prompt, model=model)

        if resp is None:
            log(f"[auto-review]   FAILED: API returned no valid response for {name}")
            result.failed += 1
            time.sleep(delay)
            continue

        raw_decision = str(resp.get("decision", "")).strip().lower()
        decision = _normalise_entity_review_decision(raw_decision)
        canonical_name = str(resp.get("canonical_name", name)).strip()
        entity_type, secondary_types, type_reason = _postprocess_entity_type(
            canonical_name or name,
            str(resp.get("entity_type", proposal.get("proposed_entity_type", "term"))).strip(),
            review_item,
        )
        if type_reason:
            attention_reasons.append(type_reason)
        type_attention = _entity_type_attention_reason(proposal, entity_type)
        if type_attention:
            attention_reasons.append(type_attention)
        response_secondary = resp.get("secondary_entity_types", [])
        if isinstance(response_secondary, list):
            for item in response_secondary:
                item_type = str(item).strip()
                if item_type == "ai_system":
                    item_type = "character"
                if item_type and item_type not in secondary_types:
                    secondary_types.append(item_type)
        if secondary_types:
            attention_reasons.append("multi-type entity; review primary and secondary type assignment")
        rationale = str(resp.get("rationale", "")).strip()
        response_attention = _coerce_bool(resp.get("human_review_recommended", False))
        response_attention_reason = str(resp.get("human_review_reason", "")).strip()
        if raw_decision in {"defer", "deferred", "needs_more_context", "need_more_context", "human_review"}:
            attention_reasons.append("model requested human review; using approve as best guess")
        if response_attention_reason:
            attention_reasons.append(response_attention_reason)
        human_review_recommended = bool(attention_reasons) or response_attention
        human_review_reason = "; ".join(dict.fromkeys(reason for reason in attention_reasons if reason))
        if human_review_recommended:
            result.skipped += 1
        if decision == "approve":
            result.accepted += 1
        else:
            result.rejected += 1
        attention_suffix = " | human review recommended" if human_review_recommended else ""
        log(f"[auto-review]   -> {decision.upper()}: {name}{attention_suffix} | {rationale[:80]}")

        data = _read_json_or_default(decisions_path, {"decisions": []})
        if _has_human_override_for_proposal(data, str(proposal["proposal_id"])):
            existing_decisions.add(str(proposal["proposal_id"]))
            log(f"[auto-review]   SKIPPED: human override already exists for {name}")
            time.sleep(delay)
            continue
        decision_entry = {
            "proposal_id": proposal["proposal_id"],
            "candidate_name": proposal.get("candidate_name", ""),
            "display_name": proposal.get("candidate_name", ""),
            "decision": decision,
            "canonical_name": canonical_name,
            "entity_type": entity_type,
            "secondary_entity_types": secondary_types,
            "human_review_recommended": human_review_recommended,
            "human_review_reason": human_review_reason,
            "auto_review_policy": "best_guess_with_attention_queue_v1",
            "reviewer": "openrouter_auto_review",
            "rationale": f"[AI auto-review] {rationale}",
            "timestamp_utc": now_utc_iso(),
        }
        data.setdefault("decisions", []).append(decision_entry)
        if human_review_recommended:
            _append_attention_item(decisions_path, decision_entry)
        existing_decisions.add(str(proposal["proposal_id"]))
        write_json(decisions_path, data)
        time.sleep(delay)


def _auto_review_claims(
    paths: dict[str, Path],
    api_key: str,
    model: str,
    delay: float,
    result: AutoReviewResult,
    log: Callable[[str], None],
    cancel_check: Callable[[], bool] | None = None,
) -> None:
    patches_path = paths.get("patches")
    decisions_path = paths.get("decisions")
    if not patches_path or not decisions_path:
        return
    if not patches_path.exists():
        return

    try:
        payload = read_json(patches_path)
    except Exception:
        return
    claims = payload.get("claims", [])
    if not isinstance(claims, list):
        return

    existing_decisions = _decision_ids(decisions_path, ["claim_id"])
    run_root = _run_root_from_claims_path(patches_path)
    story_answered_claim_ids = _story_answered_claim_ids(run_root)
    author_claim_texts = _author_claim_normalized_texts(run_root)
    pending = [
        c for c in claims
        if str(c.get("claim_id", "")).strip()
        and str(c.get("claim_id", "")) not in existing_decisions
        and str(c.get("claim_id", "")) not in story_answered_claim_ids
        and _claim_normalized_text(c) not in author_claim_texts
    ]
    if not pending:
        return

    skipped_by_story = len(
        [
            c for c in claims
            if str(c.get("claim_id", "")).strip()
            and str(c.get("claim_id", "")) not in existing_decisions
            and str(c.get("claim_id", "")) in story_answered_claim_ids
        ]
    )
    skipped_by_author = len(
        [
            c for c in claims
            if str(c.get("claim_id", "")).strip()
            and str(c.get("claim_id", "")) not in existing_decisions
            and str(c.get("claim_id", "")) not in story_answered_claim_ids
            and _claim_normalized_text(c) in author_claim_texts
        ]
    )
    if skipped_by_story or skipped_by_author:
        log(
            "[auto-review] Skipping claims already resolved by author input: "
            f"{skipped_by_story} story-question linked, {skipped_by_author} direct author duplicate(s)."
        )

    source_snippets = _load_source_snippets_for_claims(patches_path, pending)
    duplicate_groups: dict[tuple[str, str, str], list[dict[str, Any]]] = {}
    source_set_groups: dict[str, list[dict[str, Any]]] = {}
    for claim in pending:
        duplicate_groups.setdefault(_claim_duplicate_key(claim), []).append(claim)
        source_set_groups.setdefault(_claim_source_set_key(claim), []).append(claim)
    grouped_pending = [claims[0] for claims in duplicate_groups.values()]
    duplicate_reduction = len(pending) - len(grouped_pending)

    log(
        f"[auto-review] {len(pending)} claims to review "
        f"({len(grouped_pending)} model group(s), {duplicate_reduction} exact duplicate claim(s) grouped)."
    )
    for i, claim in enumerate(grouped_pending, 1):
        if cancel_check and cancel_check():
            break
        claim_id = str(claim.get("claim_id", ""))
        entity = str(claim.get("target_entity_name", "(unknown)"))
        duplicate_claims = duplicate_groups.get(_claim_duplicate_key(claim), [claim])
        source_set_claims = source_set_groups.get(_claim_source_set_key(claim), [claim])
        log(
            f"[auto-review] [{i}/{len(grouped_pending)}] Reviewing claim for {entity}: "
            f"{claim_id[:20]}... duplicates={len(duplicate_claims)} source_set={len(source_set_claims)}"
        )
        result.total += len(duplicate_claims)

        review_item = _claim_review_item(claim, duplicate_claims, source_set_claims, source_snippets)
        item_json = _truncate_item(review_item, max_chars=12000)
        prompt = _CLAIM_PROMPT.format(item_json=item_json)
        resp = _gemini_generate(api_key, prompt, model=model)

        if resp is None:
            log(f"[auto-review]   FAILED: API returned no valid response")
            result.failed += len(duplicate_claims)
            time.sleep(delay)
            continue

        raw_decision = str(resp.get("decision", "accept")).lower()
        decision = raw_decision if raw_decision in {"accept", "reject"} else "accept"
        rationale = str(resp.get("rationale", "")).strip()
        attention_reasons = _claim_attention_reasons(review_item, resp, raw_decision)
        human_review_recommended = bool(attention_reasons)
        human_review_reason = "; ".join(attention_reasons)

        if decision == "accept":
            result.accepted += len(duplicate_claims)
        else:
            result.rejected += len(duplicate_claims)
        if human_review_recommended:
            result.skipped += len(duplicate_claims)
        attention_suffix = " | human review recommended" if human_review_recommended else ""
        log(f"[auto-review]   -> {decision.upper()}: {entity} | {rationale[:80]}{attention_suffix}")

        data = _read_json_or_default(decisions_path, {"decisions": []})
        for duplicate_claim in duplicate_claims:
            duplicate_claim_id = str(duplicate_claim.get("claim_id", "")).strip()
            if not duplicate_claim_id or duplicate_claim_id in existing_decisions:
                continue
            decision_entry = {
                "claim_id": duplicate_claim_id,
                "decision": decision,
                "reviewer": "openrouter_auto_review",
                "rationale": f"[AI auto-review] {rationale}",
                "timestamp_utc": now_utc_iso(),
                "human_review_recommended": human_review_recommended,
                "human_review_reason": human_review_reason,
                "auto_review_policy": "claim_best_guess_with_attention_queue_v1",
                "representative_claim_id": claim_id,
                "duplicate_group_id": "|".join(_claim_duplicate_key(claim)),
                "duplicate_claim_ids": review_item.get("duplicate_claim_ids", []),
                "source_set_group_id": _claim_source_set_key(claim),
                "source_set_claim_ids": review_item.get("source_set_claim_ids", []),
                "support_warnings": duplicate_claim.get("support_warnings", []) or [],
                "source_snippet_ids": duplicate_claim.get("source_snippet_ids", []) or [],
            }
            data.setdefault("decisions", []).append(decision_entry)
            if human_review_recommended:
                _append_attention_item(
                    decisions_path,
                    {
                        **decision_entry,
                        "target_entity_name": duplicate_claim.get("target_entity_name", ""),
                        "claim_text": duplicate_claim.get("claim_text", ""),
                        "claim_type": duplicate_claim.get("claim_type", ""),
                        "confidence": duplicate_claim.get("confidence", ""),
                    },
                    filename="claim_auto_review_attention.json",
                    id_fields=("claim_id",),
                )
            existing_decisions.add(duplicate_claim_id)
        write_json(decisions_path, data)
        time.sleep(delay)


def _auto_review_identity_merges(
    paths: dict[str, Path],
    api_key: str,
    model: str,
    delay: float,
    result: AutoReviewResult,
    log: Callable[[str], None],
    cancel_check: Callable[[], bool] | None = None,
) -> None:
    proposals_path = paths.get("identity_merge_proposals")
    decisions_path = paths.get("identity_merge_decisions")
    if not proposals_path or not decisions_path:
        return

    proposals = _read_json_or_default(proposals_path, {"proposals": []}).get("proposals", [])
    existing_decisions = _decision_ids(decisions_path, ["proposal_id", "merge_id"])

    pending = [
        p for p in proposals
        if str(p.get("proposal_id", "")).strip()
        and str(p.get("proposal_id", "")) not in existing_decisions
        and str(p.get("review_status", "pending")) == "pending"
    ]
    if not pending:
        return

    log(f"[auto-review] {len(pending)} identity merge proposals to review.")
    for i, proposal in enumerate(pending, 1):
        if cancel_check and cancel_check():
            break
        source = str(proposal.get("source_entity_name", "?"))
        target = str(proposal.get("target_entity_name", "?"))
        log(f"[auto-review] [{i}/{len(pending)}] Reviewing merge: {source} -> {target}")
        result.total += 1

        item_json = _truncate_item(proposal)
        prompt = _IDENTITY_MERGE_PROMPT.format(item_json=item_json)
        resp = _gemini_generate(api_key, prompt, model=model)

        if resp is None:
            log(f"[auto-review]   FAILED: API returned no valid response")
            result.failed += 1
            time.sleep(delay)
            continue

        decision = str(resp.get("decision", "approve")).lower()
        rationale = str(resp.get("rationale", "")).strip()

        if decision == "approve":
            result.accepted += 1
        else:
            result.rejected += 1
        log(f"[auto-review]   -> {decision.upper()}: {source} -> {target} | {rationale[:80]}")

        data = _read_json_or_default(decisions_path, {"decisions": []})
        data.setdefault("decisions", []).append({
            "proposal_id": proposal["proposal_id"],
            "decision": decision,
            "reviewer": "openrouter_auto_review",
            "rationale": f"[AI auto-review] {rationale}",
            "timestamp_utc": now_utc_iso(),
        })
        existing_decisions.add(str(proposal["proposal_id"]))
        write_json(decisions_path, data)
        time.sleep(delay)


def _auto_review_cards(
    paths: dict[str, Path],
    api_key: str,
    model: str,
    delay: float,
    result: AutoReviewResult,
    log: Callable[[str], None],
    cancel_check: Callable[[], bool] | None = None,
) -> None:
    card_drafts_path = paths.get("card_drafts")
    card_decisions_path = paths.get("card_decisions")
    if not card_drafts_path or not card_decisions_path:
        return
    if not card_drafts_path.exists():
        return

    try:
        payload = read_json(card_drafts_path)
    except Exception:
        return
    cards = payload.get("cards", [])
    if not isinstance(cards, list):
        return

    existing_decisions = _decision_ids(card_decisions_path, ["card_id", "target_card_id"])
    pending = [
        c for c in cards
        if str(c.get("card_id", "")).strip()
        and str(c.get("card_id", "")) not in existing_decisions
    ]
    if not pending:
        return

    log(f"[auto-review] {len(pending)} card drafts to review.")
    for i, card in enumerate(pending, 1):
        if cancel_check and cancel_check():
            break
        card_id = str(card.get("card_id", ""))
        name = str(card.get("canonical_name", "(unknown)"))
        log(f"[auto-review] [{i}/{len(pending)}] Reviewing card: {name}")
        result.total += 1

        item_json = _truncate_item(card)
        prompt = _CARD_PROMPT.format(item_json=item_json)
        resp = _gemini_generate(api_key, prompt, model=model)

        if resp is None:
            log(f"[auto-review]   FAILED: API returned no valid response")
            result.failed += 1
            time.sleep(delay)
            continue

        decision = str(resp.get("decision", "approve")).lower()
        rationale = str(resp.get("rationale", "")).strip()
        edited_summary = str(resp.get("edited_summary", "")).strip()

        if decision == "approve":
            result.accepted += 1
        else:
            result.rejected += 1
        log(f"[auto-review]   -> {decision.upper()}: {name} | {rationale[:80]}")

        entry: dict[str, Any] = {
            "card_id": card_id,
            "decision": decision,
            "reviewer": "openrouter_auto_review",
            "rationale": f"[AI auto-review] {rationale}",
            "timestamp_utc": now_utc_iso(),
        }
        if edited_summary:
            entry["edited_summary"] = edited_summary

        data = _read_json_or_default(card_decisions_path, {"decisions": []})
        data.setdefault("decisions", []).append(entry)
        existing_decisions.add(card_id)
        write_json(card_decisions_path, data)
        time.sleep(delay)

