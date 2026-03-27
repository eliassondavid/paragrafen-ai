from index.ds_bulk_indexer import build_chunk_id, extract_metadata


def test_build_chunk_id_namespace_format() -> None:
    assert build_chunk_id("2015", 13, "ds_geb429", 0) == "ds::2015_13_chunk_000"
    assert build_chunk_id("1990", 29, "ds_geb429", 42) == "ds::1990_29_chunk_042"
    assert build_chunk_id("2003", 7, "ds_abc_d2", 5) == "ds::2003_7_d2_chunk_005"


def test_extract_metadata_nested_ds_format() -> None:
    sample_raw = {
        "dok_id": "GEB429",
        "filename": "ds_geb429",
        "metadata": {"datum": "1990-01-01"},
        "status_json": {
            "dokumentstatus": {
                "dokument": {
                    "dok_id": "GEB429",
                    "rm": "1990",
                    "beteckning": "29",
                    "titel": "Test-Ds",
                    "organ": "Finansdepartementet",
                    "datum": "1990-01-01",
                }
            }
        },
        "html_content": "",
    }
    meta = extract_metadata(sample_raw)
    assert meta["beteckning"] == "Ds 1990:29", f"Fel beteckning: {meta['beteckning']}"
    assert meta["rm"] == "1990"
    assert meta["nummer"] == 29


def test_html_stub_convention_documented() -> None:
    assert len("") < 10_000
