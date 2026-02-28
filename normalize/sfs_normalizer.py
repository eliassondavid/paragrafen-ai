"""
sfs_normalizer.py — Populerar metadata och validerar chunks (PUBLISHED-steg).

Ansvar:
- norm_type-klassificering (grundlag/lag/forordning/foreskrift)
- legal_area (lager 1: departement, lager 2: YAML-manual)
- kortnamn från priority_mapping
- Schema-validering (obligatoriska fält, namespace-regex)
"""

import re
import yaml
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

CONFIG_DIR = Path("config")

# Ladda config-filer
with open(CONFIG_DIR / "department_area_mapping.yaml") as f:
    _DEPT_MAP = yaml.safe_load(f)["mappings"]

with open(CONFIG_DIR / "sfs_priority_mapping.yaml") as f:
    _PRIORITY_MAP = yaml.safe_load(f)["laws"]

with open(CONFIG_DIR / "legal_areas.yaml") as f:
    _LEGAL_AREAS = {a["id"] for a in yaml.safe_load(f)["areas"]}

# Grundlagar (hårdkodade SFS-nummer)
GRUNDLAGAR = {"1974:152", "1974:713", "1949:105", "1991:1469"}

# Norm_type via titelmönster
NORM_TYPE_PATTERNS = [
    (r"(?i)\bförordning\b", "forordning"),
    (r"(?i)\bkungörelse\b", "forordning"),
    (r"(?i)\bföreskrift\b", "foreskrift"),
    (r"(?i)\bstadga\b", "foreskrift"),
    (r"(?i)\bbalk\b", "lag"),
    (r"(?i)\blag\b", "lag"),
]

NAMESPACE_RE = re.compile(r"^sfs::[\d]+:[\w]+_\d+kap_[\w§-]+_chunk_\d{3}$")

REQUIRED_FIELDS = [
    "namespace", "source_id", "source_type", "sfs_nr", "rubrik",
    "authority_level", "norm_type", "numbering_type",
    "chunk_index", "chunk_total", "text",
]


def classify_norm_type(sfs_nr: str, rubrik: str) -> str:
    if sfs_nr in GRUNDLAGAR:
        return "grundlag"
    for pattern, norm_type in NORM_TYPE_PATTERNS:
        if re.search(pattern, rubrik):
            return norm_type
    return "lag"


def classify_legal_area(sfs_nr: str, departement: str) -> tuple[str, str]:
    """
    Returnerar (legal_area_kommaseparerad, confidence).
    Lager 2 > Lager 1.
    """
    # Lager 2: manuell YAML
    if sfs_nr in _PRIORITY_MAP:
        entry = _PRIORITY_MAP[sfs_nr]
        areas = entry.get("legal_area", [])
        valid = [a for a in areas if a in _LEGAL_AREAS]
        if valid:
            return ",".join(valid), "manual"
    
    # Lager 1: departement
    for dept_key, areas in _DEPT_MAP.items():
        if dept_key.lower() in departement.lower():
            valid = [a for a in areas if a in _LEGAL_AREAS]
            if valid:
                return ",".join(valid), "department"
    
    # Fallback
    return "offentlig rätt", "department"


def get_kortnamn(sfs_nr: str) -> str:
    if sfs_nr in _PRIORITY_MAP:
        return _PRIORITY_MAP[sfs_nr].get("kortnamn", "")
    return ""


def get_verified_numbering_type(sfs_nr: str, detected: str) -> str:
    """Returnerar YAML-override om numbering_type_verified=True, annars detected."""
    if sfs_nr in _PRIORITY_MAP:
        entry = _PRIORITY_MAP[sfs_nr]
        if entry.get("numbering_type_verified") and "numbering_type" in entry:
            yaml_type = entry["numbering_type"]
            if yaml_type != detected and sfs_nr not in _warned_sfs_type:
                logger.warning(
                    f"[S7] {sfs_nr}: YAML säger '{yaml_type}', detektion säger '{detected}'. "
                    f"YAML vinner (verified=true)."
                )
                _warned_sfs_type.add(sfs_nr)
            return yaml_type
    return detected


_warned_sfs_type: set[str] = set()  # Förhindra duplicerad loggning per dokument

def normalize_chunks(chunks: list[dict], raw_meta: dict) -> tuple[list[dict], list[str]]:
    """
    Normaliserar en lista chunks, returnerar (normaliserade_chunks, fel_lista).
    
    raw_meta: metadata från Riksdagens API-svar.
    """
    sfs_nr = raw_meta.get("sfs_nr", "")
    rubrik = raw_meta.get("rubrik", "")
    departement = raw_meta.get("departement", "")
    
    norm_type = classify_norm_type(sfs_nr, rubrik)
    legal_area, legal_area_confidence = classify_legal_area(sfs_nr, departement)
    kortnamn = get_kortnamn(sfs_nr)
    
    errors = []
    normalized = []
    
    for chunk in chunks:
        # YAML-override för numbering_type (S7)
        detected_type = chunk.get("numbering_type", "sequential")
        chunk["numbering_type"] = get_verified_numbering_type(sfs_nr, detected_type)
        
        # Populera metadata
        chunk["norm_type"] = norm_type
        chunk["legal_area"] = legal_area
        chunk["legal_area_confidence"] = legal_area_confidence
        if kortnamn:
            chunk["kortnamn"] = kortnamn
        
        # Schema-validering
        chunk_errors = validate_chunk(chunk)
        if chunk_errors:
            errors.extend(chunk_errors)
            continue
        
        normalized.append(chunk)
    
    return normalized, errors


def validate_chunk(chunk: dict) -> list[str]:
    """Returnerar lista med valideringsfel (tom = OK)."""
    errors = []
    
    # Obligatoriska fält
    for field in REQUIRED_FIELDS:
        if field not in chunk or chunk[field] is None:
            errors.append(f"Saknar obligatoriskt fält: {field} i {chunk.get('namespace', '?')}")
    
    # Namespace-format
    ns = chunk.get("namespace", "")
    if not NAMESPACE_RE.match(ns):
        # Tillåt merged paragrafer (t.ex. 1-3§) — relaxa regex
        relaxed = re.match(r"^sfs::\d{4}:\d+_\d+kap_[\w§\-]+_chunk_\d{3}$", ns)
        if not relaxed:
            errors.append(f"Ogiltigt namespace-format: {ns}")
    
    # Text ej tom
    if not chunk.get("text", "").strip():
        errors.append(f"Tom text i chunk: {ns}")
    
    return errors
