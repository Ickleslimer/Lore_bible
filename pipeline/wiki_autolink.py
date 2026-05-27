"""Build-time first-mention wiki autolinking for static HTML preview pages."""

from __future__ import annotations

import html
import re
from dataclasses import dataclass, field
from typing import Any

from pipeline.prose_alias_registry import prose_canonical_aliases_from_config, sanitize_card_prose_whitespace
from pipeline.quest_catalog import load_quest_catalog, quests_for_work


def asset_prefix(depth: int) -> str:
    return "../" * depth if depth > 0 else "./"


@dataclass(frozen=True)
class AutolinkTarget:
    label: str
    slug: str
    path: str
    page_kind: str


@dataclass
class AutolinkPattern:
    surface: str
    target: AutolinkTarget


@dataclass
class AutolinkIndex:
    patterns: list[AutolinkPattern]
    targets_by_slug: dict[str, AutolinkTarget] = field(default_factory=dict)

    def href_for_name(self, name: str) -> str | None:
        key = name.strip().lower()
        for pattern in self.patterns:
            if pattern.surface.strip().lower() == key:
                return pattern.target.path
        return None

    def target_for_name(self, name: str) -> AutolinkTarget | None:
        key = name.strip().lower()
        for pattern in self.patterns:
            if pattern.surface.strip().lower() == key:
                return pattern.target
        return None


@dataclass
class AutolinkState:
    seen_slugs: set[str] = field(default_factory=set)
    linked: list[dict[str, str]] = field(default_factory=list)


def _word_boundary_match(text: str, start: int, surface: str) -> bool:
    end = start + len(surface)
    if end > len(text):
        return False
    if text[start:end].casefold() != surface.casefold():
        return False
    before_ok = start == 0 or not text[start - 1].isalnum()
    after_ok = end == len(text) or not text[end].isalnum()
    return before_ok and after_ok


def _find_longest_match_at(text: str, pos: int, patterns: list[AutolinkPattern]) -> tuple[int, int, AutolinkPattern, str] | None:
    for pattern in patterns:
        surface = pattern.surface
        if not surface:
            continue
        end = pos + len(surface)
        if _word_boundary_match(text, pos, surface):
            return pos, end, pattern, text[pos:end]
    return None


def build_quest_autolink_targets(seed_path: Any = None) -> list[AutolinkTarget]:
    catalog = load_quest_catalog(seed_path)
    targets: list[AutolinkTarget] = []
    seen_slugs: set[str] = set()
    for quest in quests_for_work(catalog):
        quest_id = str(quest.get("quest_id", "")).strip()
        title = str(quest.get("quest_title", "")).strip()
        if not quest_id or not title:
            continue
        if quest_id in seen_slugs:
            continue
        seen_slugs.add(quest_id)
        targets.append(
            AutolinkTarget(
                label=title,
                slug=quest_id,
                path=f"quests/{quest_id}.html",
                page_kind="quest",
            )
        )
    return targets


def build_autolink_index(
    entries: list[Any],
    *,
    quest_targets: list[AutolinkTarget] | None = None,
    config: dict[str, Any] | None = None,
) -> AutolinkIndex:
    """Merge entity, work, quest, card alias, and config alias surfaces into match patterns."""
    targets_by_slug: dict[str, AutolinkTarget] = {}
    surface_to_target: dict[str, AutolinkTarget] = {}

    def register_surface(surface: str, target: AutolinkTarget) -> None:
        key = surface.strip()
        if not key:
            return
        lower = key.lower()
        existing = surface_to_target.get(lower)
        if existing is not None and existing.slug != target.slug:
            return
        surface_to_target[lower] = target
        targets_by_slug[target.slug] = target

    def add_entry(entry: Any) -> None:
        target = AutolinkTarget(
            label=entry.name,
            slug=entry.slug,
            path=entry.path,
            page_kind=entry.page_kind,
        )
        register_surface(entry.name, target)
        if entry.is_work:
            work_id = str(entry.payload.get("work_id", "")).strip()
            if work_id:
                register_surface(work_id.replace("_", " "), target)
                register_surface(work_id, target)
        else:
            card = entry.payload
            for alias in card.get("aliases", []) or []:
                alias_text = str(alias).strip()
                if alias_text:
                    register_surface(alias_text, target)

    for entry in entries:
        add_entry(entry)

    for quest_target in quest_targets or []:
        targets_by_slug[quest_target.slug] = quest_target
        register_surface(quest_target.label, quest_target)

    catalog = load_quest_catalog()
    for quest in quests_for_work(catalog):
        quest_id = str(quest.get("quest_id", "")).strip()
        title = str(quest.get("quest_title", "")).strip()
        label = str(quest.get("quest_label", "")).strip()
        if not quest_id:
            continue
        qtarget = targets_by_slug.get(quest_id)
        if qtarget is None:
            qtarget = AutolinkTarget(
                label=title or quest_id,
                slug=quest_id,
                path=f"quests/{quest_id}.html",
                page_kind="quest",
            )
            targets_by_slug[quest_id] = qtarget
        if title:
            register_surface(title, qtarget)
        if label and label.lower() != title.lower():
            register_surface(label, qtarget)

    name_to_target: dict[str, AutolinkTarget] = {}
    for entry in entries:
        name_to_target[entry.name.strip().lower()] = AutolinkTarget(
            label=entry.name,
            slug=entry.slug,
            path=entry.path,
            page_kind=entry.page_kind,
        )

    for row in prose_canonical_aliases_from_config(config):
        alias = str(row.get("alias", "")).strip()
        canonical = str(row.get("canonical", "")).strip()
        if not alias or not canonical:
            continue
        resolved = name_to_target.get(canonical.lower()) or surface_to_target.get(canonical.lower())
        if resolved is not None:
            register_surface(alias, resolved)

    patterns = [
        AutolinkPattern(surface=surface, target=target)
        for surface, target in sorted(surface_to_target.items(), key=lambda item: len(item[0]), reverse=True)
    ]
    return AutolinkIndex(patterns=patterns, targets_by_slug=targets_by_slug)


def href_by_name(index: AutolinkIndex) -> dict[str, str]:
    """Lowercase surface label → site-relative path (pages/, works/, quests/)."""
    out: dict[str, str] = {}
    for pattern in index.patterns:
        out[pattern.surface.strip().lower()] = pattern.target.path
    return out


def path_by_slug(index: AutolinkIndex) -> dict[str, str]:
    return {slug: target.path for slug, target in index.targets_by_slug.items()}


def autolink_plain_text(
    text: str,
    index: AutolinkIndex,
    *,
    current_slug: str,
    depth: int,
    state: AutolinkState | None = None,
    section: str = "",
    page_slug: str = "",
) -> str:
    if not text or not index.patterns:
        return html.escape(text)
    link_state = state if state is not None else AutolinkState()
    prefix = asset_prefix(depth)
    out: list[str] = []
    pos = 0
    length = len(text)
    while pos < length:
        match = _find_longest_match_at(text, pos, index.patterns)
        if match is None:
            next_pos = pos + 1
            while next_pos < length:
                if _find_longest_match_at(text, next_pos, index.patterns):
                    break
                next_pos += 1
            out.append(html.escape(text[pos:next_pos]))
            pos = next_pos
            continue
        start, end, pattern, matched = match
        target = pattern.target
        out.append(html.escape(text[pos:start]))
        if target.slug != current_slug and target.slug not in link_state.seen_slugs:
            href = f"{prefix}{target.path}"
            out.append(f'<a href="{html.escape(href)}">{html.escape(matched)}</a>')
            link_state.seen_slugs.add(target.slug)
            link_state.linked.append(
                {
                    "page_slug": page_slug,
                    "section": section,
                    "target_slug": target.slug,
                    "surface_form": matched,
                    "path": target.path,
                }
            )
        else:
            out.append(html.escape(matched))
        pos = end
    return "".join(out)


def autolink_paragraphs(
    text: str,
    index: AutolinkIndex,
    *,
    current_slug: str,
    depth: int,
    state: AutolinkState | None = None,
    section: str = "",
    page_slug: str = "",
) -> str:
    clean = sanitize_card_prose_whitespace(text)
    if not clean:
        return ""
    link_state = state if state is not None else AutolinkState()
    parts = [p.strip() for p in re.split(r"\n\s*\n", clean) if p.strip()]
    blocks: list[str] = []
    for part in parts:
        paragraph = " ".join(part.split())
        body = autolink_plain_text(
            paragraph,
            index,
            current_slug=current_slug,
            depth=depth,
            state=link_state,
            section=section,
            page_slug=page_slug,
        )
        blocks.append(f"<p>{body}</p>")
    return "".join(blocks)


_MULTI_WORD_PROPER = re.compile(r"\b([A-Z][a-z]+(?:\s+[A-Z][a-z0-9'’-]+)+)\b")
_SINGLE_PROPER = re.compile(r"\b([A-Z][a-z]{2,})\b")
_SKIP_WORDS = frozenset(
    {
        "The",
        "This",
        "That",
        "These",
        "Those",
        "When",
        "Where",
        "What",
        "Which",
        "While",
        "After",
        "Before",
        "During",
        "Path",
        "Year",
        "Quest",
        "Main",
        "Route",
        "Pool",
        "Stage",
        "Theriac",
    }
)


def collect_unlinked_candidates(text: str, index: AutolinkIndex) -> list[str]:
    clean = sanitize_card_prose_whitespace(text)
    if not clean or not index.patterns:
        return []
    indexed_lower = {p.surface.strip().lower() for p in index.patterns}
    found: list[str] = []
    seen: set[str] = set()

    def maybe_add(phrase: str) -> None:
        token = phrase.strip()
        if not token or token in _SKIP_WORDS:
            return
        lower = token.lower()
        if lower in indexed_lower or lower in seen:
            return
        seen.add(lower)
        found.append(token)

    for match in _MULTI_WORD_PROPER.finditer(clean):
        maybe_add(match.group(1))
    for match in _SINGLE_PROPER.finditer(clean):
        maybe_add(match.group(1))
    return found


def wiki_site_autolink_config(config: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(config, dict):
        return {"enabled": False, "write_report": True}
    block = config.get("wiki_site")
    if not isinstance(block, dict):
        return {"enabled": False, "write_report": True}
    autolink = block.get("autolink")
    if not isinstance(autolink, dict):
        return {"enabled": False, "write_report": True}
    return {
        "enabled": bool(autolink.get("enabled", False)),
        "write_report": bool(autolink.get("write_report", True)),
    }
