"""
Normaliserare för svensk rättspraxis → §AI kanoniskt chunk-format.

Läser Publication-JSON (raw API-data), strippar HTML, identifierar
sektioner (domskäl/bakgrund/domslut), chunkar med token-gräns,
och producerar normaliserade dokument med §AI-metadata.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import structlog
import tiktoken
from bs4 import BeautifulSoup

from normalize.praxis_models import Publication
from normalize.praxis_naming import (
    MalnummerParser,
    parse_referat_nummer,
    sanitize_malnummer_for_filename,
)

logger = structlog.get_logger()
ALL_COURTS_SENTINEL = "ALL"

AUTHORITY_MAP: dict[str, str] = {
    "HDO": "binding", "HFD": "binding", "REGR": "binding",
    "HON": "guiding", "HGO": "guiding", "HSB": "guiding",
    "HNN": "guiding", "HVS": "guiding", "HSV": "guiding",
    "HYOD": "guiding", "KST": "guiding", "KSU": "guiding",
    "KGG": "guiding", "KJO": "guiding", "MDO": "guiding",
    "MOD": "guiding", "MIOD": "guiding", "MMOD": "guiding",
    "ADO": "guiding", "PMOD": "guiding", "PBR": "guiding", "RHN": "guiding",
}

_HOGSTA_INSTANS_PATTERNS: list[re.Pattern[str]] = [
    re.compile(r"H\u00f6gsta f\u00f6rvaltningsdomstolen\s*\(", re.IGNORECASE),
    re.compile(r"H\u00f6gsta domstolen\s*\(", re.IGNORECASE),
    re.compile(r"Regeringsr\u00e4tten\s*\(", re.IGNORECASE),
    re.compile(r"Arbetsdomstolen\b", re.IGNORECASE),
    re.compile(r"Marknadsdomstolen\b", re.IGNORECASE),
    re.compile(r"Patent- och marknads\u00f6verdomstolen\b", re.IGNORECASE),
    re.compile(r"Mark- och milj\u00f6\u00f6verdomstolen\b", re.IGNORECASE),
    re.compile(r"Migrations\u00f6verdomstolen\b", re.IGNORECASE),
]

SECTION_PATTERNS: dict[str, list[re.Pattern[str]]] = {
    "domslut": [
        re.compile(r"^(H\u00f6gsta f\u00f6rvaltningsdomstolens|H\u00f6gsta domstolens|Regeringsr\u00e4ttens|Arbetsdomstolens|Marknadsdomstolens)\s+avg\u00f6rande", re.IGNORECASE),
        re.compile(r"^(Hovr\u00e4ttens?|Kammarr\u00e4ttens?|Mark- och milj\u00f6\u00f6verdomstolens?|Migrations\u00f6verdomstolens?)\s+(dom|beslut|avg\u00f6rande)slut", re.IGNORECASE),
        re.compile(r"^Domslut\b", re.IGNORECASE),
    ],
    "domsk\u00e4l": [
        re.compile(r"^Sk\u00e4len f\u00f6r (avg\u00f6randet|beslutet|domen)", re.IGNORECASE),
        re.compile(r"^(H\u00f6gsta f\u00f6rvaltningsdomstolens|H\u00f6gsta domstolens|Regeringsr\u00e4ttens)\s+bed\u00f6mning", re.IGNORECASE),
        re.compile(r"^(Hovr\u00e4ttens?|Kammarr\u00e4ttens?)\s+bed\u00f6mning", re.IGNORECASE),
        re.compile(r"^(Domstolens|R\u00e4ttens)\s+bed\u00f6mning", re.IGNORECASE),
        re.compile(r"^DOMSK\u00c4L\b", re.IGNORECASE),
        re.compile(r"^Domsk\u00e4l\b", re.IGNORECASE),
        re.compile(r"^Sk\u00e4l\b", re.IGNORECASE),
        re.compile(r"^Fr\u00e5gan i m\u00e5let\b", re.IGNORECASE),
        re.compile(r"^R\u00e4ttslig reglering\b", re.IGNORECASE),
    ],
    "bakgrund": [
        re.compile(r"^Bakgrund\b", re.IGNORECASE),
        re.compile(r"^BAKGRUND\b"),
        re.compile(r"^I m\u00e5let \u00e4r f\u00f6ljande", re.IGNORECASE),
        re.compile(r"^Av utredningen framg\u00e5r", re.IGNORECASE),
        re.compile(r"^Omst\u00e4ndigheterna i m\u00e5let", re.IGNORECASE),
    ],
    "skiljaktig": [
        re.compile(r"^(Skiljaktig[at]?\s+mening|Reservant)", re.IGNORECASE),
        re.compile(r"^(Justitier\u00e5d)\w*\s+\w+\s+var\s+skiljaktig", re.IGNORECASE),
    ],
}

_TRAILING_META_PATTERN = re.compile(
    r"^M\u00e5l\s+nr\s+.+,\s*f\u00f6redragande\b", re.IGNORECASE
)

SECTION_TOKEN_LIMITS = {"domsk\u00e4l": 800, "bakgrund": 800, "domslut": 400, "sammanfattning": 400, "skiljaktig": 800}
SECTION_OVERLAP = {"domsk\u00e4l": 1, "bakgrund": 1, "domslut": 0, "sammanfattning": 0, "skiljaktig": 1}

_ENCODING = None

def _get_encoding():
    global _ENCODING
    if _ENCODING is None:
        _ENCODING = tiktoken.get_encoding("cl100k_base")
    return _ENCODING

def count_tokens(text: str) -> int:
    return len(_get_encoding().encode(text))


@dataclass
class Paragraph:
    index: int
    text: str
    section: str = "domsk\u00e4l"
    is_meta: bool = False


def strip_html_to_paragraphs(html: str) -> list[Paragraph]:
    if not html or not html.strip():
        return []
    soup = BeautifulSoup(html, "html.parser")
    paragraphs: list[Paragraph] = []
    for i, p_tag in enumerate(soup.find_all("p")):
        text = p_tag.get_text(separator=" ", strip=True)
        if not text:
            continue
        is_meta = bool(_TRAILING_META_PATTERN.match(text))
        paragraphs.append(Paragraph(index=i, text=text, is_meta=is_meta))
    return paragraphs


def _is_hogsta_instans_start(text: str) -> bool:
    for pattern in _HOGSTA_INSTANS_PATTERNS:
        if pattern.search(text):
            if re.search(r"yttrade\s*:", text):
                return True
    return False


def classify_sections(paragraphs: list[Paragraph], domstol_kod: str) -> list[Paragraph]:
    if not paragraphs:
        return paragraphs

    hogsta_start_idx = None
    is_hogsta = domstol_kod in ("HDO", "HFD", "REGR", "ADO")

    if is_hogsta:
        for i, para in enumerate(paragraphs):
            if _is_hogsta_instans_start(para.text):
                hogsta_start_idx = i
                break

    if hogsta_start_idx is not None:
        for i in range(hogsta_start_idx + 1):
            paragraphs[i].section = "bakgrund"

    current_section = "bakgrund" if hogsta_start_idx is not None else "domsk\u00e4l"
    start_idx = (hogsta_start_idx + 1) if hogsta_start_idx is not None else 0

    for i in range(start_idx, len(paragraphs)):
        para = paragraphs[i]
        if para.is_meta:
            continue
        matched = False
        for section_name, patterns in SECTION_PATTERNS.items():
            for pattern in patterns:
                if pattern.search(para.text):
                    current_section = section_name
                    para.section = section_name
                    matched = True
                    break
            if matched:
                break
        if not matched:
            para.section = current_section

    return paragraphs


@dataclass
class Chunk:
    chunk_index: int
    section: str
    text: str
    token_count: int
    paragraph_indices: list[int] = field(default_factory=list)


def chunk_paragraphs(paragraphs: list[Paragraph], sammanfattning: str | None = None) -> list[Chunk]:
    chunks: list[Chunk] = []
    chunk_idx = 0

    if sammanfattning and sammanfattning.strip():
        chunks.append(Chunk(chunk_index=chunk_idx, section="sammanfattning",
                            text=sammanfattning.strip(), token_count=count_tokens(sammanfattning),
                            paragraph_indices=[]))
        chunk_idx += 1

    section_groups: list[tuple[str, list[Paragraph]]] = []
    current_section = None
    current_group: list[Paragraph] = []

    for para in paragraphs:
        if para.is_meta:
            continue
        if para.section != current_section:
            if current_group:
                section_groups.append((current_section, current_group))
            current_section = para.section
            current_group = [para]
        else:
            current_group.append(para)

    if current_group and current_section is not None:
        section_groups.append((current_section, current_group))

    for section, group in section_groups:
        max_tokens = SECTION_TOKEN_LIMITS.get(section, 800)
        overlap = SECTION_OVERLAP.get(section, 0)
        section_chunks = _chunk_section(group, max_tokens, overlap)
        for paras_in_chunk in section_chunks:
            text = "\n\n".join(p.text for p in paras_in_chunk)
            chunks.append(Chunk(chunk_index=chunk_idx, section=section, text=text,
                                token_count=count_tokens(text),
                                paragraph_indices=[p.index for p in paras_in_chunk]))
            chunk_idx += 1

    return chunks


def _split_oversized_paragraph(para: Paragraph, max_tokens: int) -> list[Paragraph]:
    if count_tokens(para.text) <= max_tokens:
        return [para]
    sentences = re.split(r'(?<=\.)\s+(?=[A-Z\u00c5\u00c4\u00d6"])', para.text)
    if len(sentences) <= 1:
        return [para]
    sub_paras: list[Paragraph] = []
    parts: list[str] = []
    current_tokens = 0
    for sentence in sentences:
        s_tokens = count_tokens(sentence)
        if current_tokens > 0 and current_tokens + s_tokens > max_tokens:
            sub_paras.append(Paragraph(index=para.index, text=" ".join(parts),
                                       section=para.section, is_meta=para.is_meta))
            parts = [sentence]
            current_tokens = s_tokens
        else:
            parts.append(sentence)
            current_tokens += s_tokens
    if parts:
        sub_paras.append(Paragraph(index=para.index, text=" ".join(parts),
                                   section=para.section, is_meta=para.is_meta))
    return sub_paras


def _chunk_section(paragraphs: list[Paragraph], max_tokens: int, overlap: int) -> list[list[Paragraph]]:
    if not paragraphs:
        return []
    expanded: list[Paragraph] = []
    for para in paragraphs:
        expanded.extend(_split_oversized_paragraph(para, max_tokens))
    result: list[list[Paragraph]] = []
    start = 0
    while start < len(expanded):
        current_chunk: list[Paragraph] = []
        current_tokens = 0
        end = start
        while end < len(expanded):
            para_tokens = count_tokens(expanded[end].text)
            if current_tokens > 0 and current_tokens + para_tokens > max_tokens:
                break
            current_chunk.append(expanded[end])
            current_tokens += para_tokens
            end += 1
        if not current_chunk:
            current_chunk = [expanded[start]]
            end = start + 1
        result.append(current_chunk)
        start = max(end - overlap, start + 1) if overlap > 0 else end
    return result


def build_citation(referat_nummer: str, domstol_kod: str) -> str:
    if referat_nummer:
        return referat_nummer.strip()
    return f"{domstol_kod} dom/beslut"


def extract_roman_suffix(citation: str) -> str | None:
    match = re.search(r"\s+(III|II|I)\s*$", citation or "")
    if match:
        return match.group(1)
    return None


def extract_references_to(lagrum_lista: list[dict[str, Any]]) -> list[str]:
    refs: list[str] = []
    for item in lagrum_lista:
        sfs = item.get("sfsNummer")
        if sfs:
            refs.append(f"sfs::{sfs}")
    return sorted(set(refs))


def extract_cites_praxis(hanvisade_publiceringar: list[dict[str, Any]]) -> list[str]:
    cites: list[str] = []
    for item in hanvisade_publiceringar:
        text = (item.get("fritext") or "").strip()
        if text:
            cites.append(text)
    return sorted(set(cites))


def extract_legal_area(rattsomraden: list[Any]) -> list[str]:
    areas: list[str] = []
    for item in rattsomraden:
        value = getattr(item, "rattsomradeNamn", None) or getattr(item, "namn", None) or str(item)
        value = value.strip()
        if value:
            areas.append(value)
    return sorted(set(areas))


@dataclass
class NormalizedDocument:
    filename: str
    domstol: str
    year: int
    ref_no: int
    ref_no_padded: str
    malnummer: str
    avgorandedatum: str
    ar_vagledande: bool
    authority_level: str
    citation: str
    legal_area: list[str]
    references_to: list[str]
    cites_praxis: list[str]
    api_id: str
    harvest_source: str
    chunks: list[Chunk]
    source_type: str = "praxis"

    def _build_chunk_namespace(self, chunk_index: int) -> str:
        if self.ref_no == 0:
            mal_safe = sanitize_malnummer_for_filename(self.malnummer)
            return f"praxis::{self.domstol}_{self.year}_mal-{mal_safe}_chunk_{chunk_index:03d}"
        suffix = extract_roman_suffix(self.citation)
        if suffix:
            return (
                f"praxis::{self.domstol}_{self.year}_ref-{self.ref_no_padded}_{suffix}"
                f"_chunk_{chunk_index:03d}"
            )
        return f"praxis::{self.domstol}_{self.year}_ref-{self.ref_no_padded}_chunk_{chunk_index:03d}"

    def to_dict(self) -> dict[str, Any]:
        return {
            "filename": self.filename, "source_type": self.source_type,
            "domstol": self.domstol, "year": self.year,
            "ref_no": self.ref_no, "ref_no_padded": self.ref_no_padded,
            "malnummer": self.malnummer, "avgorandedatum": self.avgorandedatum,
            "ar_vagledande": self.ar_vagledande, "authority_level": self.authority_level,
            "citation": self.citation,
            "legal_area": json.dumps(self.legal_area, ensure_ascii=False),
            "references_to": json.dumps(self.references_to, ensure_ascii=False),
            "cites_praxis": json.dumps(self.cites_praxis, ensure_ascii=False),
            "api_id": self.api_id, "harvest_source": self.harvest_source,
            "chunks": [
                {
                    "chunk_id": self._build_chunk_namespace(c.chunk_index),
                    "namespace": self._build_chunk_namespace(c.chunk_index),
                    "chunk_index": c.chunk_index, "pinpoint": c.section,
                    "chunk_text": c.text, "token_count": c.token_count,
                    "paragraph_indices": c.paragraph_indices,
                }
                for c in self.chunks
            ],
        }


def normalize_publication(pub: Publication, *, filename: str | None = None) -> NormalizedDocument:
    domstol_kod = pub.domstol.domstolKod
    authority = AUTHORITY_MAP.get(domstol_kod)
    if authority is None:
        raise ValueError(f"Ok\u00e4nd domstolskod: {domstol_kod}. Eskalera till \u00f6verprojektet.")

    referat_nummer = pub.referatNummerLista[0] if pub.referatNummerLista else ""
    if referat_nummer:
        year, ref_no = parse_referat_nummer(referat_nummer)
    else:
        year = int(pub.avgorandedatum[:4]) if pub.avgorandedatum else 0
        ref_no = 0

    ref_no_padded = f"{ref_no:03d}"
    raw_malnummer = [str(m) for m in pub.malNummerLista]
    _, malnummer_primart = MalnummerParser.parse_malnummer_lista(raw_malnummer)
    citation = build_citation(referat_nummer, domstol_kod)
    paragraphs = strip_html_to_paragraphs(pub.innehall or "")
    paragraphs = classify_sections(paragraphs, domstol_kod)
    chunks = chunk_paragraphs(paragraphs, sammanfattning=pub.sammanfattning)

    lagrum_dicts = [{"referens": l.referens, "sfsNummer": l.sfsNummer} for l in pub.lagrumLista]
    references_to = extract_references_to(lagrum_dicts)
    hanvisade_dicts = [{"fritext": h.fritext} for h in pub.hanvisadePubliceringarLista]
    cites_praxis = extract_cites_praxis(hanvisade_dicts)
    legal_area = extract_legal_area(pub.rattsomradeLista)

    if filename is None:
        from normalize.praxis_naming import generate_filename
        filename = generate_filename(domstol=domstol_kod, year=year, ref_no=ref_no,
                                     malnummer_primart=malnummer_primart)

    return NormalizedDocument(
        filename=filename, domstol=domstol_kod, year=year, ref_no=ref_no,
        ref_no_padded=ref_no_padded, malnummer=malnummer_primart,
        avgorandedatum=pub.avgorandedatum, ar_vagledande=pub.arVagledande,
        authority_level=authority, citation=citation, legal_area=legal_area,
        references_to=references_to, cites_praxis=cites_praxis,
        api_id=str(pub.id), harvest_source="rattspraxis.etjanst.domstol.se",
        chunks=chunks,
    )


@dataclass
class NormalizationReport:
    total_files: int = 0
    normalized: int = 0
    skipped: int = 0
    errors: int = 0
    total_chunks: int = 0
    skip_reasons: list[dict[str, str]] = field(default_factory=list)
    error_details: list[dict[str, str]] = field(default_factory=list)

    @property
    def error_rate(self) -> float:
        return self.errors / self.total_files if self.total_files else 0.0

    def summary(self) -> str:
        return (f"Normalisering klar: {self.normalized}/{self.total_files} filer, "
                f"{self.total_chunks} chunks, {self.skipped} skippade, {self.errors} fel "
                f"(felkvot: {self.error_rate:.1%})")


def normalize_directory(input_dir: Path, output_dir: Path, *, dry_run: bool = False) -> NormalizationReport:
    report = NormalizationReport()
    json_files = sorted(input_dir.glob("*.json"))
    report.total_files = len(json_files)
    if not json_files:
        return report
    if not dry_run:
        output_dir.mkdir(parents=True, exist_ok=True)
    for json_path in json_files:
        try:
            raw = json.loads(json_path.read_text(encoding="utf-8"))
            pub = Publication(**raw)
            if not pub.innehall or not pub.innehall.strip():
                report.skipped += 1
                report.skip_reasons.append({"file": json_path.name, "reason": "empty_innehall", "api_id": str(pub.id)})
                continue
            normalized = normalize_publication(pub, filename=json_path.name)
            if not normalized.chunks:
                report.skipped += 1
                continue
            if not dry_run:
                out_path = output_dir / json_path.name
                out_path.write_text(json.dumps(normalized.to_dict(), ensure_ascii=False, indent=2), encoding="utf-8")
            report.normalized += 1
            report.total_chunks += len(normalized.chunks)
        except Exception as e:
            report.errors += 1
            report.error_details.append({"file": json_path.name, "error": str(e), "type": type(e).__name__})
    return report


def normalize_all_courts(raw_base: Path, norm_base: Path, *, courts: list[str] | None = None, dry_run: bool = False) -> dict[str, NormalizationReport]:
    reports = {}
    normalized_courts = [court.upper() for court in courts] if courts else None
    if normalized_courts is None or ALL_COURTS_SENTINEL in normalized_courts:
        court_dirs = sorted(d for d in raw_base.iterdir() if d.is_dir() and d.name.isupper())
    else:
        court_dirs = [raw_base / c for c in normalized_courts if (raw_base / c).is_dir()]
    for court_dir in court_dirs:
        report = normalize_directory(input_dir=court_dir, output_dir=norm_base / court_dir.name, dry_run=dry_run)
        reports[court_dir.name] = report
    return reports


def main(argv: list[str] | None = None) -> int:
    import argparse

    parser = argparse.ArgumentParser(description="Normalize praxis raw documents.")
    parser.add_argument("--raw-base", default="data/raw/praxis")
    parser.add_argument("--norm-base", default="data/norm/praxis")
    parser.add_argument("--courts", nargs="*", help="Domstolskoder, t.ex. HFD HDO")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--overwrite", action="store_true")
    args = parser.parse_args(argv)

    if not args.dry_run and not args.overwrite:
        parser.error("--overwrite krävs för att skriva normaliserade filer.")

    reports = normalize_all_courts(
        raw_base=Path(args.raw_base),
        norm_base=Path(args.norm_base),
        courts=args.courts,
        dry_run=args.dry_run,
    )

    for court_code, report in reports.items():
        print(f"{court_code}: {report.summary()}")

    return 1 if any(report.error_rate > 0.01 for report in reports.values()) else 0


if __name__ == "__main__":
    raise SystemExit(main())
