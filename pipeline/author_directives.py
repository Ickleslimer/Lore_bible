from __future__ import annotations

import re
from typing import Any


def parse_author_instruction(instruction_text: str) -> dict[str, Any]:
    """
    Parse light natural-language directives into structured payloads.
    Supported forms (case-insensitive):
    - "replace summary with: ..."
    - "append summary: ..."
    - "set status: canonical"
    - "add alias: Old Name"
    - "remove alias: Old Name"
    """
    text = instruction_text.strip()
    lower = text.lower()

    replace_match = re.search(r"replace\s+summary\s+with\s*:\s*(.+)$", text, flags=re.IGNORECASE | re.DOTALL)
    if replace_match:
        return {"op": "replace_summary", "value": replace_match.group(1).strip()}

    append_match = re.search(r"append\s+summary\s*:\s*(.+)$", text, flags=re.IGNORECASE | re.DOTALL)
    if append_match:
        return {"op": "append_summary", "value": append_match.group(1).strip()}

    status_match = re.search(r"set\s+status\s*:\s*([a-z_]+)", text, flags=re.IGNORECASE)
    if status_match:
        return {"op": "set_status", "value": status_match.group(1).strip().lower()}

    add_alias = re.search(r"add\s+alias\s*:\s*(.+)$", text, flags=re.IGNORECASE | re.DOTALL)
    if add_alias:
        return {"op": "add_alias", "value": add_alias.group(1).strip()}

    remove_alias = re.search(r"remove\s+alias\s*:\s*(.+)$", text, flags=re.IGNORECASE | re.DOTALL)
    if remove_alias:
        return {"op": "remove_alias", "value": remove_alias.group(1).strip()}

    # default fallback: append to summary to avoid destructive interpretation
    return {"op": "append_summary", "value": text}


def apply_directive_to_card(card: dict[str, Any], directive: dict[str, Any]) -> tuple[dict[str, Any], str]:
    payload = directive.get("parsed_payload", {}) or {}
    op = payload.get("op")
    value = payload.get("value", "")
    note = ""

    if op == "replace_summary":
        card["summary"] = value
        note = "summary_replaced"
    elif op == "append_summary":
        card["summary"] = (card.get("summary", "") + " " + value).strip()
        note = "summary_appended"
    elif op == "set_status":
        card["status"] = value
        note = "status_set"
    elif op == "add_alias":
        aliases = set(card.get("aliases", []))
        aliases.add(value)
        card["aliases"] = sorted(aliases)
        note = "alias_added"
    elif op == "remove_alias":
        aliases = [a for a in card.get("aliases", []) if a != value]
        card["aliases"] = aliases
        note = "alias_removed"
    else:
        card["summary"] = (card.get("summary", "") + " " + str(directive.get("instruction_text", ""))).strip()
        note = "summary_appended_fallback"

    return card, note
