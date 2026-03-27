"""
Tester for praxis_normalizer.py.
"""

import json
from pathlib import Path
import pytest
import sys
sys.path.insert(0, str(Path(__file__).parent.parent))

from normalize.praxis_normalizer import (
    AUTHORITY_MAP, Chunk, NormalizedDocument, Paragraph,
    build_citation, chunk_paragraphs, classify_sections,
    count_tokens, extract_cites_praxis, extract_legal_area,
    extract_references_to, extract_roman_suffix, normalize_publication,
    strip_html_to_paragraphs,
)
from normalize.praxis_models import Publication

FIXTURES_DIR = Path(__file__).parent / "fixtures"

@pytest.fixture
def hfd_2013_ref_31():
    path = FIXTURES_DIR / "HFD_2013_ref-031__mal-5766-12.json"
    return Publication(**json.loads(path.read_text(encoding="utf-8")))

@pytest.fixture
def simple_html():
    return (
        "<p>Bakgrund</p>"
        "<p>Den klagande ansökte om bistånd.</p>"
        "<p>Skälen för avgörandet</p>"
        "<p>Domstolen gör följande bedömning.</p>"
        "<p>Av 4 kap. 1 § socialtjänstlagen framgår att...</p>"
        "<p>Högsta förvaltningsdomstolens avgörande</p>"
        "<p>Överklagandet avslås.</p>"
        "<p>Mål nr 1234-22, föredragande Svensson</p>"
    )

@pytest.fixture
def hfd_with_yttrade():
    return (
        "<p>Förvaltningsrätten i Stockholm yttrade: Domstolen avslår.</p>"
        "<p>Kammarrätten i Stockholm yttrade: Avslår.</p>"
        "<p>Klaganden överklagade.</p>"
        "<p>Högsta förvaltningsdomstolen (2023-06-15, Jermsten) yttrade:</p>"
        "<p>Skälen för avgörandet</p>"
        "<p>Bakgrund</p>"
        "<p>Frågan gäller rätt till sjukersättning.</p>"
        "<p>Rättslig reglering</p>"
        "<p>Enligt 33 kap. socialförsäkringsbalken...</p>"
        "<p>Högsta förvaltningsdomstolens bedömning</p>"
        "<p>Domstolen finner att rekvisiten är uppfyllda.</p>"
        "<p>Högsta förvaltningsdomstolens avgörande</p>"
        "<p>Med ändring av kammarrättens dom bifaller domstolen.</p>"
        "<p>Mål nr 5555-22, föredragande Andersson</p>"
    )


class TestStripHtml:
    def test_basic(self, simple_html):
        paras = strip_html_to_paragraphs(simple_html)
        assert len(paras) == 8

    def test_empty(self):
        assert strip_html_to_paragraphs("") == []
        assert strip_html_to_paragraphs(None) == []

    def test_meta_detected(self, simple_html):
        paras = strip_html_to_paragraphs(simple_html)
        assert sum(1 for p in paras if p.is_meta) == 1

    def test_real_file(self, hfd_2013_ref_31):
        paras = strip_html_to_paragraphs(hfd_2013_ref_31.innehall or "")
        assert len(paras) == 30

    def test_empty_p_excluded(self):
        assert len(strip_html_to_paragraphs("<p>A</p><p></p><p>B</p>")) == 2


class TestSectionClassification:
    def test_simple(self, simple_html):
        paras = strip_html_to_paragraphs(simple_html)
        c = classify_sections(paras, "KST")
        assert c[0].section == "bakgrund"
        assert c[3].section == "domskäl"
        assert c[5].section == "domslut"

    def test_hfd_underinstans(self, hfd_with_yttrade):
        paras = strip_html_to_paragraphs(hfd_with_yttrade)
        c = classify_sections(paras, "HFD")
        for i in range(4):
            assert c[i].section == "bakgrund"

    def test_hfd_after_yttrade(self, hfd_with_yttrade):
        paras = strip_html_to_paragraphs(hfd_with_yttrade)
        c = classify_sections(paras, "HFD")
        assert c[4].section == "domskäl"
        assert c[5].section == "bakgrund"
        assert c[9].section == "domskäl"
        assert c[11].section == "domslut"

    def test_real_file(self, hfd_2013_ref_31):
        paras = strip_html_to_paragraphs(hfd_2013_ref_31.innehall or "")
        c = classify_sections(paras, "HFD")
        for i in range(9):
            assert c[i].section == "bakgrund", f"[{i}] was {c[i].section}"
        assert c[9].section == "domskäl"
        assert c[10].section == "bakgrund"
        assert c[20].section == "domskäl"
        assert c[27].section == "domslut"
        assert c[29].is_meta

    def test_kammaratt(self):
        html = "<p>BAKGRUND</p><p>Text.</p><p>DOMSKÄL</p><p>Bedömning.</p>"
        c = classify_sections(strip_html_to_paragraphs(html), "KST")
        assert c[0].section == "bakgrund"
        assert c[2].section == "domskäl"

    def test_hd(self):
        html = ("<p>Högsta domstolen (2023-05-01, justitieråden A) yttrade:</p>"
                "<p>DOMSKÄL</p><p>Fråga.</p><p>Domslut</p><p>Fastställs.</p>")
        c = classify_sections(strip_html_to_paragraphs(html), "HDO")
        assert c[0].section == "bakgrund"
        assert c[1].section == "domskäl"
        assert c[3].section == "domslut"

    def test_empty(self):
        assert classify_sections([], "HFD") == []

    def test_skiljaktig(self):
        html = "<p>Bedömning</p><p>Avslås.</p><p>Skiljaktig mening</p><p>X anser...</p>"
        c = classify_sections(strip_html_to_paragraphs(html), "HFD")
        assert c[2].section == "skiljaktig"


class TestChunking:
    def test_sammanfattning_first(self):
        paras = [Paragraph(0, "Bedömning.", "domskäl")]
        chunks = chunk_paragraphs(paras, sammanfattning="Kort.")
        assert chunks[0].section == "sammanfattning"
        assert chunks[0].chunk_index == 0

    def test_meta_excluded(self):
        paras = [Paragraph(0, "Text.", "domskäl"),
                 Paragraph(1, "Mål nr 1-22, föredragande X", "domskäl", is_meta=True)]
        assert "Mål nr" not in " ".join(c.text for c in chunk_paragraphs(paras))

    def test_real_file(self, hfd_2013_ref_31):
        paras = strip_html_to_paragraphs(hfd_2013_ref_31.innehall or "")
        c = classify_sections(paras, "HFD")
        chunks = chunk_paragraphs(c, sammanfattning=hfd_2013_ref_31.sammanfattning)
        assert len(chunks) >= 5
        assert chunks[0].section == "sammanfattning"
        sections = {ch.section for ch in chunks}
        assert "bakgrund" in sections and "domskäl" in sections and "domslut" in sections

    def test_no_oversized(self, hfd_2013_ref_31):
        paras = strip_html_to_paragraphs(hfd_2013_ref_31.innehall or "")
        c = classify_sections(paras, "HFD")
        for ch in chunk_paragraphs(c, sammanfattning=hfd_2013_ref_31.sammanfattning):
            assert ch.token_count <= 800, f"Chunk {ch.chunk_index}: {ch.token_count} > 800"

    def test_sequential_indices(self):
        paras = [Paragraph(0, "A.", "bakgrund"), Paragraph(1, "B.", "domskäl")]
        for i, ch in enumerate(chunk_paragraphs(paras, sammanfattning="S.")):
            assert ch.chunk_index == i


class TestTokenCounting:
    def test_basic(self):
        assert 0 < count_tokens("Hello world") < 10
    def test_empty(self):
        assert count_tokens("") == 0


class TestMetadata:
    def test_refs(self):
        lagrum = [{"referens": "x", "sfsNummer": "2010:110"}, {"referens": "y", "sfsNummer": None},
                  {"referens": "z", "sfsNummer": "2010:110"}]
        assert extract_references_to(lagrum) == ["sfs::2010:110"]

    def test_cites(self):
        assert len(extract_cites_praxis([{"fritext": "HFD 2020 ref. 45"}, {"fritext": "C-337/98"}])) == 2

    def test_legal_empty(self):
        assert extract_legal_area([]) == ["övrigt"]

    def test_legal_data(self):
        assert extract_legal_area(["Skatterätt"]) == ["Skatterätt"]

    def test_citation(self):
        assert build_citation("HFD 2013 ref. 31", "HFD") == "HFD 2013 ref. 31"
        assert "okänt" in build_citation("", "HFD")

    def test_extract_roman_suffix(self):
        assert extract_roman_suffix("NJA 2022:66 II") == "II"
        assert extract_roman_suffix("MÖD 2023:17 I") == "I"
        assert extract_roman_suffix("RH 2024:50") is None


class TestAuthority:
    def test_binding(self):
        assert AUTHORITY_MAP["HDO"] == "binding"
        assert AUTHORITY_MAP["HFD"] == "binding"
        assert AUTHORITY_MAP["REGR"] == "binding"

    def test_guiding(self):
        for c in ["HON","HGO","HSB","HNN","HVS","HSV","HYOD","KST","KSU","KGG","KJO","MDO","MOD","MIOD","MMOD"]:
            assert AUTHORITY_MAP[c] == "guiding"

    def test_all_spec(self):
        for c in ["HDO","HFD","HON","HGO","HSB","HNN","HVS","HSV","HYOD","KST","KSU","KGG","KJO","MDO","MOD","MIOD","MMOD"]:
            assert c in AUTHORITY_MAP


class TestNamespace:
    def test_format(self, hfd_2013_ref_31):
        d = normalize_publication(hfd_2013_ref_31).to_dict()
        for ch in d["chunks"]:
            assert ch["namespace"].startswith("praxis::HFD_2013_ref-031_chunk_")
            assert ch["chunk_id"] == ch["namespace"]

    def test_padded(self):
        pub = Publication(**{
            "id": "00000000-0000-0000-0000-000000000001", "typ": "REFERAT",
            "domstol": {"domstolKod": "HFD", "domstolNamn": "HFD"},
            "referatNummerLista": ["HFD 2023 ref. 5"], "malNummerLista": ["1234-22"],
            "avgorandedatum": "2023-01-15", "publiceringstid": "2023-02-01T10:00:00",
            "innehall": "<p>Test</p>", "arVagledande": True,
        })
        ns = normalize_publication(pub).to_dict()["chunks"][0]["namespace"]
        assert "ref-005" in ns and "chunk_000" in ns

    def test_format_with_roman_suffix(self):
        pub = Publication(**{
            "id": "00000000-0000-0000-0000-000000000003", "typ": "REFERAT",
            "domstol": {"domstolKod": "MMOD", "domstolNamn": "Mark- och miljööverdomstolen"},
            "referatNummerLista": ["MÖD 2023:17 I"], "malNummerLista": ["P 1234-23"],
            "avgorandedatum": "2023-04-15", "publiceringstid": "2023-04-16T10:00:00",
            "innehall": "<p>Test</p>", "arVagledande": True,
        })
        ns = normalize_publication(pub).to_dict()["chunks"][0]["namespace"]
        assert ns == "praxis::MMOD_2023_ref-017_I_chunk_000"


class TestEndToEnd:
    def test_full(self, hfd_2013_ref_31):
        doc = normalize_publication(hfd_2013_ref_31)
        assert doc.domstol == "HFD" and doc.year == 2013 and doc.ref_no == 31
        assert doc.malnummer == "5766-12" and doc.authority_level == "binding"
        assert "sfs::2007:1091" in doc.references_to
        assert len(doc.cites_praxis) == 3 and len(doc.chunks) >= 5

    def test_serialization(self, hfd_2013_ref_31):
        d = normalize_publication(hfd_2013_ref_31).to_dict()
        assert isinstance(d["legal_area"], str) and isinstance(d["references_to"], str)
        assert isinstance(json.loads(d["legal_area"]), list)
        json.dumps(d, ensure_ascii=False)

    def test_chunk_fields(self, hfd_2013_ref_31):
        d = normalize_publication(hfd_2013_ref_31).to_dict()
        valid_pp = {"sammanfattning", "bakgrund", "domskäl", "domslut", "skiljaktig"}
        for ch in d["chunks"]:
            for f in ["chunk_id", "namespace", "chunk_index", "pinpoint", "chunk_text"]:
                assert f in ch
            assert ch["pinpoint"] in valid_pp

    def test_unknown_court(self):
        pub = Publication(**{
            "id": "00000000-0000-0000-0000-000000000001", "typ": "REFERAT",
            "domstol": {"domstolKod": "OKÄND", "domstolNamn": "T"},
            "referatNummerLista": ["T 2023 ref. 1"], "malNummerLista": ["1-23"],
            "avgorandedatum": "2023-01-01", "publiceringstid": "2023-01-01T00:00:00",
            "innehall": "<p>Test</p>",
        })
        with pytest.raises(ValueError, match="domstolskod"):
            normalize_publication(pub)

    def test_empty_innehall(self):
        pub = Publication(**{
            "id": "00000000-0000-0000-0000-000000000002", "typ": "REFERAT",
            "domstol": {"domstolKod": "HFD", "domstolNamn": "HFD"},
            "referatNummerLista": ["HFD 2023 ref. 1"], "malNummerLista": ["1-23"],
            "avgorandedatum": "2023-01-01", "publiceringstid": "2023-01-01T00:00:00",
            "innehall": "", "sammanfattning": "Sammanfattning.",
        })
        doc = normalize_publication(pub)
        assert len(doc.chunks) == 1 and doc.chunks[0].section == "sammanfattning"


class TestVerificationChecklist:
    def test_namespace(self, hfd_2013_ref_31):
        for ch in normalize_publication(hfd_2013_ref_31).to_dict()["chunks"]:
            assert ch["namespace"].startswith("praxis::HFD_")

    def test_authority(self, hfd_2013_ref_31):
        assert normalize_publication(hfd_2013_ref_31).authority_level == "binding"

    def test_legal_area_json(self, hfd_2013_ref_31):
        d = normalize_publication(hfd_2013_ref_31).to_dict()
        assert isinstance(json.loads(d["legal_area"]), list)

    def test_refs_format(self, hfd_2013_ref_31):
        for ref in normalize_publication(hfd_2013_ref_31).references_to:
            assert ref.startswith("sfs::") and len(ref.replace("sfs::", "").split(":")) == 2

    def test_pinpoint(self, hfd_2013_ref_31):
        valid = {"domskäl", "bakgrund", "domslut", "sammanfattning", "skiljaktig"}
        for ch in normalize_publication(hfd_2013_ref_31).chunks:
            assert ch.section in valid

    def test_smoke(self, hfd_2013_ref_31):
        doc = normalize_publication(hfd_2013_ref_31)
        assert len(doc.chunks) >= 5 and doc.authority_level == "binding"
