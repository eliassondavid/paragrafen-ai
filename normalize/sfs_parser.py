"""
sfs_parser.py — Parsrar Riksdagens HTML-format till strukturerade paragrafobjekt.

Hanterar tre HTML-strukturer:
  1. <h3 name="K{n}"> + ankare K{n}P{m} — kapitelrelativ (FB, MB, RB)
  2. <h3 name="K{n}"> + ankare K{n}P{m} — löpande/sequential (AvtL, LAS)
  3. Inga kapitel, bara ankare P{n} — kapitellös (FL, korta förordningar)

Beslut S7: detect_numbering_type() avgör typ A vs B automatiskt.
"""

import re
import yaml
import logging
from pathlib import Path
from bs4 import BeautifulSoup, Tag

logger = logging.getLogger(__name__)

# Config
CONFIG_DIR = Path("config")
with open(CONFIG_DIR / "embedding_config.yaml") as f:
    _cfg = yaml.safe_load(f)
SINGLE_CHAPTER_THRESHOLD = _cfg["sfs_parser"]["single_chapter_sequential_threshold"]

# Mönster för definitionsparagrafer (S2)
DEFINITION_PATTERNS = [
    r"(?i)^i\s+denna\s+(lag|förordning|balk|stadga|föreskrift)",
    r"(?i)^med\s+\w+\s+avses\s+i\s+denna",
    r"(?i)^i\s+detta\s+kapitel\s+avses\s+med",
    r"(?i)^följande\s+ord\s+och\s+uttryck\s+har",
    r"(?i)^beteckningarna?\s+i\s+denna",
    r"(?i)^i\s+denna\s+lag\s+används?\s+följande\s+begrepp",
]

# Mönster för hänvisningar (S6)
REFERENCE_PATTERNS = [
    (r"\b(\d{4}:\d+)\b", "cites"),
    (r"(?i)\bändras?\s+.*?(\d{4}:\d+)", "amends"),
    (r"(?i)\bupphävs?\s+.*?(\d{4}:\d+)", "repeals"),
    (r"(?i)\bdefiniera[rs]?\s+.*?(\d{4}:\d+)", "defines"),
    (r"(?i)\bund(?:an)?tag.*?(\d{4}:\d+)", "exempts"),
]


def detect_numbering_type(chapters: dict) -> str:
    """
    Avgör om en lag använder kapitelrelativ (relative) eller löpande (sequential) numrering.
    
    chapters: {kapitel_nr: [paragraf_nr, ...]}  — bara K-ankare (int → list[int])
    
    Regler (S7):
    1. Inga K-ankare → sequential (kapitellös)
    2. K2 börjar INTE på P1 → sequential (global numrering)
    3. K2 börjar på P1 → relative
    4. Bara K1, ≥ threshold paragrafer → sequential
    5. Bara K1, < threshold paragrafer → relative
    """
    if not chapters:
        return "sequential"  # Regel 1: inga kapitel
    
    if len(chapters) >= 2:
        # Regel 2/3: kolla vad K2 (eller lägsta K > 1) börjar på
        sorted_k = sorted(chapters.keys())
        for k_nr in sorted_k:
            if k_nr <= 1:
                continue
            kn_paragraphs = sorted(chapters.get(k_nr, []))
            if kn_paragraphs:
                if kn_paragraphs[0] == 1:
                    return "relative"   # Regel 3
                else:
                    return "sequential"  # Regel 2
    
    # En enda K-grupp, men inte K1 — troligen sequential (EU-numrering etc.)
    if len(chapters) == 1:
        single_k = list(chapters.keys())[0]
        if single_k > 1:
            return "sequential"  # Enda kapitel är inte K1 — global numrering
    
    # Bara K1
    k1_count = len(chapters.get(1, []))
    if k1_count >= SINGLE_CHAPTER_THRESHOLD:
        return "sequential"  # Regel 4
    else:
        return "relative"   # Regel 5


def is_definition_paragraph(text: str) -> bool:
    """Returnerar True om texten ser ut att vara en definitionsparagraf."""
    for pattern in DEFINITION_PATTERNS:
        if re.search(pattern, text[:500]):
            return True
    return False


def is_overgangsbestammelse_section(text: str) -> bool:
    """Returnerar True om texten är en övergångsbestämmelse."""
    return bool(re.search(r"(?i)(övergångsbestämmelse|ikraftträdande|tillämpas?\s+första\s+gången)", text[:300]))


def extract_references(text: str) -> list[dict]:
    """Extraherar hänvisningar från löptext, returnerar typade kanter."""
    refs = []
    seen = set()
    # SFS-nummerformat
    for match in re.finditer(r"\b(\d{4}:\d+)\b", text):
        target_sfs = match.group(1)
        target = f"sfs::{target_sfs}"
        if target not in seen:
            refs.append({"target": target, "relation_type": "cites"})
            seen.add(target)
    return refs


def parse_html(html: str, sfs_nr: str, meta: dict) -> list[dict]:
    """
    Parsrar Riksdagens HTML-dokument till en lista paragrafobjekt.
    
    Returnerar:
    [
      {
        "kapitel": str,         # "" om kapitellös
        "kapitelrubrik": str,
        "paragraf": str,        # "1", "1a", etc.
        "paragraf_rubrik": str,
        "stycken": [str],       # lista med stycketexter
        "text": str,            # sammanfogad text
        "is_definition": bool,
        "is_overgangsbestammelse": bool,
        "has_table": bool,
        "references_to": list,
        "numbering_type": str,  # sätts efter detect_numbering_type()
        "has_kapitel": bool,
      }
    ]
    """
    soup = BeautifulSoup(html, "html.parser")
    
    # Samla kapitelstruktur för numbering_type-detektion
    # chapters: {kapitel_int: [paragraf_int, ...]}
    chapters: dict[int, list[int]] = {}
    
    # Identifiera alla ankare
    # Format 1: <a name="K1P1"> eller <a name="P1">
    for anchor in soup.find_all("a", {"name": True}):
        name = anchor["name"]
        km = re.match(r"K(\d+)P(\d+)", name)
        if km:
            k = int(km.group(1))
            p = int(km.group(2))
            chapters.setdefault(k, []).append(p)
        elif re.match(r"P(\d+)$", name):
            # Kapitellös: para-ankare utan kapitel
            pass
    
    numbering_type = detect_numbering_type(chapters)
    has_kapitel = len(chapters) > 0 and numbering_type == "relative"
    
    paragraphs = []
    current_kapitel = ""
    current_kapitelrubrik = ""
    
    # Traversera dokumentet
    elements = soup.find_all(["h2", "h3", "h4", "p", "div", "table"])
    
    current_para = None
    
    def save_current():
        if current_para and (current_para.get("stycken") or current_para.get("text")):
            text = "\n".join(current_para["stycken"]) if current_para["stycken"] else current_para.get("text", "")
            current_para["text"] = text.strip()
            if current_para["text"]:
                current_para["is_definition"] = is_definition_paragraph(current_para["text"])
                current_para["is_overgangsbestammelse"] = is_overgangsbestammelse_section(current_para["text"])
                current_para["references_to"] = extract_references(current_para["text"])
                current_para["numbering_type"] = numbering_type
                current_para["has_kapitel"] = has_kapitel
                paragraphs.append(dict(current_para))
    
    def make_para(kapitel, kapitelrubrik, paragraf, paragraf_rubrik=""):
        return {
            "kapitel": kapitel,
            "kapitelrubrik": kapitelrubrik,
            "paragraf": paragraf,
            "paragraf_rubrik": paragraf_rubrik,
            "stycken": [],
            "text": "",
            "has_table": False,
            "is_definition": False,
            "is_overgangsbestammelse": False,
            "references_to": [],
            "numbering_type": numbering_type,
            "has_kapitel": has_kapitel,
        }
    
    # Iterera alla element i dokumentordning
    for el in soup.descendants:
        if not isinstance(el, Tag):
            continue
        
        # Kapitelrubrik
        if el.name == "h2":
            text = el.get_text(strip=True)
            if text:
                save_current()
                current_para = None
                # Kolla om det är ett nytt kapitel
                km = re.search(r"(\d+)\s*[Kk]ap", text)
                if km or re.search(r"(?i)kapitel\s+\d+", text):
                    current_kapitelrubrik = text
        
        elif el.name == "h3":
            name_attr = el.get("name", "")
            text = el.get_text(strip=True)
            km = re.match(r"K(\d+)$", name_attr)
            if km:
                save_current()
                current_para = None
                if numbering_type == "relative":
                    current_kapitel = km.group(1)
                # Kapitelrubrik i nästa h4 eller el.text
                current_kapitelrubrik = text if text and not re.match(r"^\d+", text) else current_kapitelrubrik
        
        # Paragraf-element: identifiera via ankare
        elif el.name in ("p", "div"):
            # Kolla om elementet innehåller ett paragraf-ankare
            anchor = el.find("a", {"name": True}) if el.name == "div" else None
            if not anchor:
                # Kolla om förälder har ankaret, eller el självt har id
                pass
            
            # Paragraftext: leta efter §-tecken
            text = el.get_text(separator=" ", strip=True)
            
            # Kolla om det är ett paragraf-start-element
            para_match = re.match(r"^(\d+\s*[a-z]?)\s*§", text)
            if para_match:
                save_current()
                paragraf_raw = para_match.group(1).replace(" ", "")
                # Bestäm kapitel
                if numbering_type == "sequential":
                    kapitel = ""
                else:
                    kapitel = current_kapitel
                current_para = make_para(kapitel, current_kapitelrubrik, paragraf_raw)
                # Resten av texten är första stycket
                rest = text[para_match.end():].strip()
                if rest:
                    current_para["stycken"].append(rest)
            elif current_para is not None:
                # Lägg till som stycke om det innehåller text
                if text and len(text) > 5:
                    current_para["stycken"].append(text)
        
        elif el.name == "table":
            if current_para is not None:
                current_para["has_table"] = True
                # Konvertera tabell till flattext
                rows = []
                for row in el.find_all("tr"):
                    cells = [td.get_text(strip=True) for td in row.find_all(["td", "th"])]
                    rows.append(" | ".join(cells))
                current_para["stycken"].append("\n".join(rows))
    
    save_current()
    
    # Om ingen paragraf hittades med §-mönster — försök alternativ parsing
    if not paragraphs:
        paragraphs = _fallback_parse(soup, numbering_type, has_kapitel, current_kapitel, current_kapitelrubrik)
    
    return paragraphs


def _fallback_parse(soup, numbering_type, has_kapitel, kapitel, kapitelrubrik):
    """Alternativ parsning för dokument med annorlunda struktur."""
    paragraphs = []
    full_text = soup.get_text(separator="\n", strip=True)
    
    # Dela på §-tecken
    sections = re.split(r'\n(\d+\s*[a-z]?)\s*§\s*', full_text)
    
    for i in range(1, len(sections), 2):
        if i + 1 < len(sections):
            paragraf = sections[i].strip().replace(" ", "")
            text = sections[i + 1].strip()
            if text:
                para = {
                    "kapitel": "" if numbering_type == "sequential" else kapitel,
                    "kapitelrubrik": kapitelrubrik,
                    "paragraf": paragraf,
                    "paragraf_rubrik": "",
                    "stycken": [text],
                    "text": text,
                    "has_table": False,
                    "is_definition": is_definition_paragraph(text),
                    "is_overgangsbestammelse": is_overgangsbestammelse_section(text),
                    "references_to": extract_references(text),
                    "numbering_type": numbering_type,
                    "has_kapitel": has_kapitel,
                }
                paragraphs.append(para)
    
    return paragraphs
