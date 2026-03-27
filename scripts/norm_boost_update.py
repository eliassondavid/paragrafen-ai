"""
norm_boost.py-uppdatering: authority_level-viktning för retrieval.

Denna fil visar de tillägg som ska göras i den befintliga norm_boost.py.
Implementeras som patch/manual merge mot paragrafen-ai/retrieval/norm_boost.py.
"""

# === TILLÄGG: authority_level viktning ===

AUTHORITY_WEIGHTS = {
    "binding":     1.0,    # HD, HFD — prejudikat
    "guiding":     0.75,   # HovR, KamR (inkl. MiÖD, MmÖD)
    "indicative":  0.35,   # NYTT — curaterad underrättspraxis
    "preparatory": 0.50,   # Förarbeten (modifieras av forarbete_rank)
    "persuasive":  0.25,   # Doktrin, NJA II
}

# === TILLÄGG: source_type-filter ===

VALID_SOURCE_TYPES = {
    "sfs",
    "forarbete",
    "praxis",
    "praxis_curated",   # NYTT — curaterad underrättspraxis
    "doktrin",
}


def boost_score(
    raw_score: float,
    authority_level: str,
    source_type: str,
    forarbete_rank: int | None = None,
) -> float:
    """
    Boostar retrieval-poäng baserat på authority_level.

    raw_score: Cosine similarity (0.0–1.0) från Chroma
    authority_level: binding | guiding | indicative | preparatory | persuasive
    source_type: praxis | praxis_curated | forarbete | sfs | doktrin
    forarbete_rank: Gäller ENBART för source_type 'forarbete' (1=prop, 2=SOU, etc.)

    Returns: Justerad poäng
    """
    weight = AUTHORITY_WEIGHTS.get(authority_level, 0.25)

    # forarbete_rank-justering gäller BARA förarbeten
    if source_type == "forarbete" and forarbete_rank is not None:
        # Lägre rank = högre auktoritet (prop=1, SOU=2, Ds=3)
        rank_factor = max(0.5, 1.0 - (forarbete_rank - 1) * 0.15)
        weight *= rank_factor

    return raw_score * weight
