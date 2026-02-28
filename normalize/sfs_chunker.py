"""
sfs_chunker.py — Chunk-strategi för SFS-paragrafer (Beslut S1).

Regler:
- 100–800 tokens: 1 chunk = 1 paragraf
- > 800 tokens: split på styckegräns, stycke-fält sätts
- < 100 tokens: merge med nästa paragraf i samma kapitel
- Definitionsparagraf: standalone, aldrig merge
- Övergångsbestämmelse: 1 chunk per block
"""

import json
import uuid
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

MIN_TOKENS = 100
MAX_TOKENS = 800


def _approx_tokens(text: str) -> int:
    """Enkel tokenuppskattning: ord × 1.3 (svenska ord är långa)."""
    return int(len(text.split()) * 1.3)


def _make_namespace(sfs_nr: str, kapitel: str, paragraf: str, numbering_type: str, chunk_idx: int) -> str:
    """Genererar namespace-sträng enligt S3/S7-beslut."""
    if numbering_type == "relative" and kapitel:
        kap_str = f"{kapitel}kap"
    else:
        kap_str = "0kap"
    return f"sfs::{sfs_nr}_{kap_str}_{paragraf}§_chunk_{chunk_idx:03d}"


def chunk_paragraphs(paragraphs: list[dict], sfs_nr: str, meta: dict) -> list[dict]:
    """
    Tar en lista paragrafobjekt, returnerar en lista färdiga chunks med fullständig metadata.
    """
    chunks = []
    i = 0
    
    while i < len(paragraphs):
        para = paragraphs[i]
        text = para["text"].strip()
        tokens = _approx_tokens(text)
        
        # Definitionsparagraf: standalone alltid
        if para.get("is_definition"):
            chunk = _make_chunk(sfs_nr, para, text, "", meta, chunk_idx=0, chunk_total=1)
            chunk["chunk_total"] = 1
            chunks.append(chunk)
            i += 1
            continue
        
        # För liten: försök merge med nästa (om samma kapitel)
        if tokens < MIN_TOKENS and not para.get("is_overgangsbestammelse"):
            merged_text = text
            j = i + 1
            while j < len(paragraphs) and _approx_tokens(merged_text) < MIN_TOKENS:
                next_para = paragraphs[j]
                next_text = next_para["text"].strip()
                would_be = _approx_tokens(merged_text + "\n\n" + next_text)
                if (next_para["kapitel"] == para["kapitel"]
                        and not next_para.get("is_definition")
                        and would_be <= MAX_TOKENS):
                    merged_text += "\n\n" + next_text
                    j += 1
                else:
                    break
            
            if j > i + 1:
                # Merged chunk — använd sista paragrafens nummer som slutnyckel
                chunk = _make_chunk(sfs_nr, para, merged_text, "", meta, chunk_idx=0, chunk_total=1)
                chunk["paragraf"] = f"{para['paragraf']}-{paragraphs[j-1]['paragraf']}"
                chunk["namespace"] = _make_namespace(
                    sfs_nr, para["kapitel"], chunk["paragraf"], para["numbering_type"], 0
                )
                chunks.append(chunk)
                i = j
                continue
        
        # Lagom stor: 1 chunk = 1 paragraf
        if tokens <= MAX_TOKENS:
            chunk = _make_chunk(sfs_nr, para, text, "", meta, chunk_idx=0, chunk_total=1)
            chunks.append(chunk)
            i += 1
            continue
        
        # För stor: split på styckegränser
        stycken = para.get("stycken", [text])
        if not stycken:
            stycken = [text]
        
        current_parts = []
        current_tokens = 0
        chunk_idx = 0
        sub_chunks = []
        
        for stycke in stycken:
            st_tokens = _approx_tokens(stycke)
            if current_tokens + st_tokens > MAX_TOKENS and current_parts:
                sub_chunks.append("\n".join(current_parts))
                current_parts = []
                current_tokens = 0
            current_parts.append(stycke)
            current_tokens += st_tokens
        
        if current_parts:
            sub_chunks.append("\n".join(current_parts))
        
        chunk_total = len(sub_chunks)
        for idx, sub_text in enumerate(sub_chunks):
            stycke_nr = str(idx + 1) if chunk_total > 1 else ""
            chunk = _make_chunk(sfs_nr, para, sub_text, stycke_nr, meta, chunk_idx=idx, chunk_total=chunk_total)
            chunks.append(chunk)
        
        i += 1
    
    return chunks


def _make_chunk(sfs_nr: str, para: dict, text: str, stycke: str, meta: dict, chunk_idx: int, chunk_total: int) -> dict:
    """Bygger ett fullständigt chunk-objekt med all metadata."""
    numbering_type = para.get("numbering_type", "sequential")
    kapitel = para.get("kapitel", "")
    paragraf = para.get("paragraf", "")
    
    ns = _make_namespace(sfs_nr, kapitel, paragraf.replace(" ", ""), numbering_type, chunk_idx)
    
    return {
        # Identifiering
        "namespace": ns,
        "source_id": meta.get("source_id", str(uuid.uuid4())),
        "source_type": "sfs",
        
        # SFS-specifik metadata
        "sfs_nr": sfs_nr,
        "rubrik": meta.get("rubrik", ""),
        "kortnamn": meta.get("kortnamn", ""),
        "kapitel": kapitel,
        "kapitelrubrik": para.get("kapitelrubrik", ""),
        "paragraf": paragraf,
        "stycke": stycke,
        "rubrik_paragraf": para.get("paragraf_rubrik", ""),
        "ikraftträdande": meta.get("ikraftträdande", ""),
        "upphävd": meta.get("upphävd", False),
        "senaste_andring": meta.get("senaste_andring", ""),
        "consolidation_source": "rk",
        "departement": meta.get("departement", ""),
        "utfärdad": meta.get("utfärdad", ""),
        
        # Harmoniserade fält
        "authority_level": "binding",
        "norm_type": meta.get("norm_type", "lag"),
        "legal_area": meta.get("legal_area", ""),
        "legal_area_confidence": meta.get("legal_area_confidence", "department"),
        "numbering_type": numbering_type,
        
        # Typade kanter (JSON-sträng för ChromaDB)
        "references_to": json.dumps(para.get("references_to", []), ensure_ascii=False),
        
        # Flaggor
        "is_overgangsbestammelse": para.get("is_overgangsbestammelse", False),
        "is_definition": para.get("is_definition", False),
        "has_table": para.get("has_table", False),
        
        # Chunk-metadata
        "embedding_model": "",  # sätts vid indexering
        "chunk_index": chunk_idx,
        "chunk_total": chunk_total,
        
        # Provenance
        "riksdagen_dok_id": meta.get("riksdagen_dok_id", ""),
        "indexed_at": "",
        
        # Text
        "text": text.strip(),
    }
