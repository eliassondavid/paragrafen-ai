from __future__ import annotations

from normalize.prop_parser import parse_prop_html


def test_prop_parser_maps_sections_from_page_divs() -> None:
    html = """
    <html><body>
      <div id="page_1">Innehållsförteckning</div>
      <div id="page_2">Skälen för regeringens förslag Motivering text.</div>
      <div id="page_3">Mer motivering.</div>
      <div id="page_4">Författningskommentar Kommentar text.</div>
    </body></html>
    """

    sections = parse_prop_html(html, "HC03180")

    assert [section["section"] for section in sections] == ["rationale", "commentary"]
    assert sections[0]["page_start"] == 2
    assert sections[0]["page_end"] == 3


def test_prop_parser_fallback_without_page_divs_sets_unknown_pages() -> None:
    html = "<html><body>Bakgrund Text. Skälen för regeringens förslag Mer text.</body></html>"

    sections = parse_prop_html(html, "HC03180")

    assert sections[0]["page_start"] == 0
    assert sections[0]["page_end"] == 0


def test_prop_parser_splits_commentary_on_paragraph_markers() -> None:
    html = """
    <html><body>
      <div id="page_1">Författningskommentar
        5 kap. 3 § Första stycket.

        5 kap. 4 § Andra stycket.
      </div>
    </body></html>
    """

    sections = parse_prop_html(html, "HC03180")

    assert len(sections) == 2
    assert sections[0]["section_title"] == "Författningskommentar — 5 kap. 3 §"
    assert sections[1]["section_title"] == "Författningskommentar — 5 kap. 4 §"


def test_prop_parser_filters_toc_from_output() -> None:
    html = """
    <html><body>
      <div id="page_1">Innehållsförteckning</div>
      <div id="page_2">Bakgrund Saktext.</div>
    </body></html>
    """

    sections = parse_prop_html(html, "HC03180")

    assert all(section["section"] != "toc" for section in sections)


def test_prop_parser_uses_page_ids_for_page_ranges() -> None:
    html = """
    <html><body>
      <div id="page_10">Bakgrund Saktext.</div>
      <div id="page_11">Fortsatt bakgrund.</div>
      <div id="page_12">Skälen för regeringens förslag Motivering.</div>
    </body></html>
    """

    sections = parse_prop_html(html, "HC03180")

    assert sections[0]["page_start"] == 10
    assert sections[0]["page_end"] == 11
    assert sections[1]["page_start"] == 12
    assert sections[1]["page_end"] == 12


def test_prop_parser_extends_section_over_nonmatching_page_between_matches() -> None:
    html = """
    <html><body>
      <div id="page_1">Bakgrund Inledande text.</div>
      <div id="page_2">Mellanliggande sida utan rubrik.</div>
      <div id="page_3">Skälen för regeringens förslag Motivering.</div>
    </body></html>
    """

    sections = parse_prop_html(html, "HC03180")

    assert sections[0]["section"] == "background"
    assert sections[0]["page_end"] == 2


def test_prop_parser_pdf2htmlex_without_page_ids_falls_back_to_full_text() -> None:
    html = """
    <html>
      <body>
        <!-- APA-123 -->
        <style>
          #page_1 {position:relative; overflow:hidden;}
        </style>
        <div class="page"><span>Första raden.</span><span>Andra raden.</span></div>
      </body>
    </html>
    """

    sections = parse_prop_html(html, "HC03180")

    assert len(sections) == 1
    assert sections[0]["section"] == "other"
    assert sections[0]["section_title"] == "other"
    assert sections[0]["page_start"] == 0
    assert sections[0]["page_end"] == 0
    assert "Första raden." in sections[0]["text"]
    assert "Andra raden." in sections[0]["text"]
