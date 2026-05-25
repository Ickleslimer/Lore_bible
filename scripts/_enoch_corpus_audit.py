"""One-off audit: Enoch snippet cluster vs manual character doc beats. Not part of pipeline."""
from __future__ import annotations

import json
import re
from collections import Counter
from pathlib import Path

FULL = Path(__file__).resolve().parents[1] / "artifacts/runs/20260517_032555635445_full"
SEED = Path(__file__).resolve().parents[1] / "artifacts/runs/wiki_seed_enoch_krypteia"
OUT = Path(__file__).resolve().parents[1] / "artifacts/_enoch_audit_report.txt"


def main() -> None:
    clusters = json.loads((FULL / "08_snippet_grouping/snippet_clusters_lore.json").read_text(encoding="utf-8"))[
        "clusters"
    ]
    enoch_ids = next(
        c["snippet_ids"] for c in clusters if str(c.get("cluster_key", "")).strip().lower() == "enoch"
    )
    snip_path = FULL / "05_snippet_extraction/snippets_candidates_with_theme_rescue.jsonl"
    if not snip_path.exists():
        snip_path = FULL / "05_snippet_extraction/snippets_candidates.jsonl"
    by_id: dict[str, dict] = {}
    with snip_path.open(encoding="utf-8") as f:
        for line in f:
            if line.strip():
                row = json.loads(line)
                by_id[row["snippet_id"]] = row
    texts = [(sid, by_id[sid]) for sid in enoch_ids if sid in by_id]

    beats = {
        "identity_faust_name": r"Faust Ersetzen|Professor Enoch",
        "khava": r"Khava",
        "cryonics_vitrif": r"cryoni|vitrif|cryogenic",
        "daughter_family": r"daughter|baby girl",
        "military_officer": r"military|officer corps|basic training",
        "krypteia_moratorium": r"Krypteia|brain rejuvenation|brain aging|GFNS|moratorium",
        "prosthetics_bionics": r"prosthetic|bionic|biopod|cybernetic",
        "lab_theriac_pi": r"Principal Investigator|Theriac project|the lab",
        "joy": r"\bJoy\b",
        "fear_oyuun": r"Fear|Oyuun",
        "olympus_ruinr": r"Olympus|RUINR",
        "destructive_path": r"Destructive Path|psychotic|blind rage",
        "peaceful_path": r"Peaceful Path|acclimat",
        "love_beau": r"\bLove\b|Beau|Ramasinta",
        "altruism": r"Altruism",
        "loss_theme": r"\bLoss\b",
        "enoch_biblical_naming": r"biblical|patriarch|Enoch Theriac|Book of Enoch",
        "clone_bodyoid": r"clone|bodyoid|secondary",
        "incarcerat": r"incarcerat|40 years|sentence",
        "age_117": r"117 years|age.*117",
        "worcester_birth": r"Worcester|February.*1995|1995",
    }
    counts = {k: 0 for k in beats}
    for sid, s in texts:
        t = str(s.get("display_text_normalized", ""))
        for key, pat in beats.items():
            if re.search(pat, t, re.I):
                counts[key] += 1
    lines: list[str] = []
    lines.append(f"Enoch lore cluster: {len(texts)} snippets")
    lines.append(f"knowledge_track: {dict(Counter(str(s.get('knowledge_track', '')) for _, s in texts))}")
    lines.append("")
    lines.append("BEAT COVERAGE (keyword hits in cluster snippets):")
    for key, n in sorted(counts.items(), key=lambda x: -x[1]):
        flag = "YES" if n >= 5 else ("weak" if n else "NO")
        lines.append(f"  {flag:4} {key:28} {n:4}")

    lines.append("")
    lines.append("=== SAMPLE SNIPPETS (thin beats) ===")
    thin = ["cryonics_vitrif", "military_officer", "peaceful_path", "identity_faust_name", "worcester_birth", "age_117"]
    for key in thin:
        lines.append(f"--- {key} ---")
        n = 0
        for sid, s in texts:
            t = str(s.get("display_text_normalized", ""))
            if re.search(beats[key], t, re.I):
                n += 1
                if n <= 2:
                    lines.append(f"  [{sid}] {t[:400]}")
        lines.append(f"  total: {n}")

    dec = json.loads((FULL / "09_claim_drafting/claim_review_decisions.json").read_text(encoding="utf-8"))
    latest = {d["claim_id"]: d for d in dec.get("decisions", []) if d.get("claim_id")}
    claims = json.loads((FULL / "09_claim_drafting/claim_drafts.json").read_text(encoding="utf-8"))["claims"]
    enoch_claims = [c for c in claims if "enoch" in str(c.get("target_entity_name", "")).lower()]
    lines.append("")
    lines.append(f"FULL RUN: {len(enoch_claims)} Enoch claim drafts")
    lines.append(f"  claim_types: {dict(Counter(c.get('claim_type') for c in enoch_claims))}")
    accepted = [c for c in enoch_claims if latest.get(c.get("claim_id"), {}).get("decision") == "accept"]
    rejected = [c for c in enoch_claims if latest.get(c.get("claim_id"), {}).get("decision") == "reject"]
    lines.append(f"  accepted: {len(accepted)} | rejected: {len(rejected)}")
    lines.append("  Accepted claim texts (all):")
    for c in accepted:
        lines.append(f"    [{c.get('claim_type')}] {c.get('claim_text', '')}")
    lines.append("  Rejected:")
    for c in rejected:
        lines.append(f"    [{c.get('claim_type')}] {c.get('claim_text', '')}")

    seed_claims = json.loads((SEED / "09_claim_drafting/claim_drafts.json").read_text(encoding="utf-8"))["claims"]
    lines.append("")
    lines.append(f"WIKI_SEED RUN: {len(seed_claims)} claims (8 accepted in prior card run)")
    for c in seed_claims:
        lines.append(f"    [{c.get('claim_type')}] {c.get('claim_text', '')}")

    seed_cluster_path = SEED / "08_snippet_grouping/snippet_clusters_lore.json"
    lines.append("")
    lines.append(f"WIKI_SEED has snippet_clusters_lore: {seed_cluster_path.exists()}")

    OUT.write_text("\n".join(lines), encoding="utf-8")
    print(OUT)


if __name__ == "__main__":
    main()
