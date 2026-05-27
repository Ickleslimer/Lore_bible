"""Character quest band motifs (artist → primary character)."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

from pipeline.common import read_json, write_json

DEFAULT_MOTIFS_PATH = Path("canon/quest_motifs.json")
DEFAULT_SEED_PATH = Path("config/quest_song_seed.json")

# Default artist alias hints when bootstrapping from seed band_hint ids.
BAND_HINT_ARTIST_ALIASES: dict[str, list[str]] = {
    "guns_n_roses": ["Guns N' Roses", "Guns N Roses", "GNR"],
    "nine_inch_nails": ["Nine Inch Nails", "NIN"],
    "meat_loaf": ["Meat Loaf", "Meatloaf"],
    "motley_crue": ["Mötley Crüe", "Motley Crue", "Motley Crüe"],
    "motorhead": ["Motörhead", "Motorhead"],
    "the_who": ["The Who"],
    "radiohead": ["Radiohead"],
    "audioslave": ["Audioslave"],
    "black_sabbath": ["Black Sabbath"],
    "faith_no_more": ["Faith No More"],
    "other": [],
}


def normalize_artist_key(value: str) -> str:
    text = str(value or "").lower()
    text = text.replace("'", "").replace("'", "")
    text = re.sub(r"[^a-z0-9]+", " ", text).strip()
    return text


def load_quest_motifs(path: Path | None = None) -> list[dict[str, Any]]:
    motifs_path = path or DEFAULT_MOTIFS_PATH
    if not motifs_path.exists():
        return []
    payload = read_json(motifs_path)
    motifs = payload.get("motifs", []) if isinstance(payload, dict) else []
    return [row for row in motifs if isinstance(row, dict) and str(row.get("motif_id", "")).strip()]


def build_artist_index(motifs: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    index: dict[str, dict[str, Any]] = {}
    for motif in motifs:
        motif_id = str(motif.get("motif_id", "")).strip()
        if not motif_id:
            continue
        aliases = [str(motif.get("motif_id", "")).replace("_", " ")]
        aliases.extend(str(item) for item in motif.get("artist_aliases", []) or [] if str(item).strip())
        for alias in aliases:
            key = normalize_artist_key(alias)
            if key:
                index[key] = motif
    return index


def match_artist_to_motif(artist: str, motifs: list[dict[str, Any]] | None = None) -> dict[str, Any] | None:
    registry = motifs if motifs is not None else load_quest_motifs()
    if not artist.strip():
        return None
    index = build_artist_index(registry)
    key = normalize_artist_key(artist)
    if key in index:
        return index[key]
    # Substring fallback for partial matches (e.g. "guns n roses" in longer string).
    for alias_key, motif in sorted(index.items(), key=lambda item: len(item[0]), reverse=True):
        if len(alias_key) >= 4 and (alias_key in key or key in alias_key):
            return motif
    return None


def bootstrap_motifs_from_seed(
    seed_path: Path | None = None,
    *,
    primary_by_band: dict[str, str | None] | None = None,
) -> dict[str, Any]:
    """Build motifs payload from quest_song_seed band_hint → main_character."""
    path = seed_path or DEFAULT_SEED_PATH
    payload = read_json(path) if path.exists() else {}
    seeds = payload.get("quest_song_seeds", []) if isinstance(payload, dict) else []
    chars_by_band: dict[str, set[str]] = {}
    for row in seeds:
        if not isinstance(row, dict):
            continue
        band = str(row.get("band_hint", "")).strip() or "other"
        char = str(row.get("main_character", "")).strip()
        if char:
            chars_by_band.setdefault(band, set()).add(char)

    overrides = primary_by_band or {
        "guns_n_roses": "Izanami",
        "nine_inch_nails": "Enoch",
        "meat_loaf": "Beau",
        "motley_crue": "Ramasinta",
        "motorhead": "Ramasinta",
        "the_who": "Pandora",
        "radiohead": "Oyuun",
        "audioslave": "Oyuun",
        "black_sabbath": "RUINR",
        "faith_no_more": "Altruism",
        "other": None,
    }

    motifs: list[dict[str, Any]] = []
    for band in sorted(chars_by_band.keys()):
        chars = sorted(chars_by_band[band])
        primary = overrides.get(band)
        if primary is None and len(chars) == 1:
            primary = chars[0]
        motifs.append(
            {
                "motif_id": band,
                "primary_character": primary,
                "secondary_characters": [c for c in chars if c != primary],
                "artist_aliases": BAND_HINT_ARTIST_ALIASES.get(band, [band.replace("_", " ").title()]),
                "requires_review": band == "other" or primary is None,
                "notes": f"Bootstrapped from seed; characters seen: {', '.join(chars)}",
            }
        )
    return {"motifs": motifs, "source": str(path)}


def write_default_motifs_if_missing(out_path: Path | None = None) -> Path:
    path = out_path or DEFAULT_MOTIFS_PATH
    if path.exists():
        return path
    payload = bootstrap_motifs_from_seed()
    path.parent.mkdir(parents=True, exist_ok=True)
    write_json(path, payload)
    return path
