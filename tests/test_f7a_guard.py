from __future__ import annotations

from pathlib import Path

import pytest

from guard import AreaBlocker, ConfidenceGate


@pytest.fixture
def excluded_areas_path(tmp_path: Path) -> Path:
    config = tmp_path / "excluded_areas.yaml"
    config.write_text(
        """excluded_areas:
  - id: straffrätt
    label: "Straffrätt"
    sfs_patterns: ["1962:700", "2010:1408"]
    message: "Denna tjänst täcker inte straffrättsliga frågor. Kontakta en advokat eller rättshjälpen."
  - id: asyl
    label: "Asylrätt och migration"
    sfs_patterns: ["2005:716", "2016:752"]
    message: "Asylrättsliga frågor kräver juridiskt ombud. Kontakta Advokatjouren eller Rådgivningsbyrån för asylsökande."
  - id: skatterätt
    label: "Skatterätt"
    sfs_patterns: ["1999:1229"]
    message: "För skattefrågor, kontakta Skatteverket eller en skatterådgivare."
  - id: vbu
    label: "Vårdnad, boende och umgänge"
    sfs_patterns: ["1949:381_kap6"]
    message: "Tvister om vårdnad, boende och umgänge kräver juridiskt ombud. Kontakta familjerätten i din kommun."
""",
        encoding="utf-8",
    )
    return config


@pytest.fixture
def blocker(excluded_areas_path: Path) -> AreaBlocker:
    return AreaBlocker(config_path=excluded_areas_path)


def make_chunk(
    authority_level: str,
    source_type: str,
    distance: float | None = None,
) -> dict:
    metadata: dict[str, object] = {
        "authority_level": authority_level,
        "source_type": source_type,
    }
    if distance is not None:
        metadata["distance"] = distance
    return {"id": "chunk-id", "text": "text", "metadata": metadata}


def test_area_blocker_query_hits_straffratt_keyword(blocker: AreaBlocker) -> None:
    blocked, message = blocker.is_blocked("Jag riskerar åtal för stöld, vad gäller?")
    assert blocked is True
    assert message is not None


def test_area_blocker_query_without_excluded_keywords(blocker: AreaBlocker) -> None:
    blocked, message = blocker.is_blocked("Hur begär jag ut en allmän handling?")
    assert (blocked, message) == (False, None)


def test_area_blocker_blocks_exact_sfs(blocker: AreaBlocker) -> None:
    blocked, message = blocker.is_sfs_blocked("1962:700")
    assert blocked is True
    assert message is not None


def test_area_blocker_does_not_block_family_code_without_chapter_suffix(blocker: AreaBlocker) -> None:
    assert blocker.is_sfs_blocked("1949:381") == (False, None)


def test_area_blocker_does_not_block_unknown_sfs(blocker: AreaBlocker) -> None:
    assert blocker.is_sfs_blocked("2000:100") == (False, None)


def test_area_blocker_uses_yaml_message(blocker: AreaBlocker) -> None:
    blocked, message = blocker.is_blocked("Jag behöver hjälp med asylansökan.")
    assert blocked is True
    assert (
        message
        == "Asylrättsliga frågor kräver juridiskt ombud. Kontakta Advokatjouren eller Rådgivningsbyrån för asylsökande."
    )


def test_confidence_gate_empty_chunks_fails() -> None:
    gate = ConfidenceGate()
    result = gate.evaluate([])
    assert result["pass"] is False
    assert result["score"] == 0.0
    assert "no_results" in result["flags"]


def test_confidence_gate_only_persuasive_penalty() -> None:
    gate = ConfidenceGate()
    chunks = [
        make_chunk("persuasive", "doktrin", 0.18),
        make_chunk("persuasive", "forarbete", 0.24),
    ]
    result = gate.evaluate(chunks)
    assert "only_persuasive" in result["flags"]
    assert result["score"] == pytest.approx(0.7)


def test_confidence_gate_mixed_authority_passes() -> None:
    gate = ConfidenceGate()
    chunks = [
        make_chunk("binding", "sfs", 0.35),
        make_chunk("guiding", "praxis", 0.42),
        make_chunk("persuasive", "doktrin", 0.5),
    ]
    result = gate.evaluate(chunks)
    assert result["pass"] is True
    assert "only_persuasive" not in result["flags"]


def test_confidence_gate_sparse_results_flag() -> None:
    gate = ConfidenceGate()
    result = gate.evaluate([make_chunk("binding", "sfs", 0.2)])
    assert "sparse_results" in result["flags"]
