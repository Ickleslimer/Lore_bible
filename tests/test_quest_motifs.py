from __future__ import annotations

from pipeline.quest_motifs import bootstrap_motifs_from_seed, match_artist_to_motif, normalize_artist_key


def test_normalize_artist_key() -> None:
    assert normalize_artist_key("Guns N' Roses") == normalize_artist_key("guns n roses")


def test_match_artist_to_motif_gnr() -> None:
    motifs = bootstrap_motifs_from_seed().get("motifs", [])
    motif = match_artist_to_motif("Guns N' Roses", motifs)
    assert motif is not None
    assert motif.get("motif_id") == "guns_n_roses"
    assert motif.get("primary_character") == "Izanami"


def test_match_artist_to_motif_nin() -> None:
    motifs = bootstrap_motifs_from_seed().get("motifs", [])
    motif = match_artist_to_motif("Nine Inch Nails", motifs)
    assert motif is not None
    assert motif.get("primary_character") == "Enoch"
