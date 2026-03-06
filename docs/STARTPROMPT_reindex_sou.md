# §AI Implementation Task — scripts/reindex_sou.py

## Din roll
Du implementerar ett re-indexeringsskript för §AI (paragrafen.ai), ett gratis juridiskt AI-system för allmänheten i Sverige. Du arbetar i repo-roten `paragrafen-ai/`.

## Uppgift
Implementera `scripts/reindex_sou.py` enligt specifikationen i `REINDEX_SOU_SPEC_v1.2.md` (bifogad nedan eller i docs/).

Implementera även `tests/test_reindex_sou.py` med minst:
- Ett smoke-test för `--dry-run` (kör utan Chroma, mockar collection)
- Ett enhetstest för metadata-rättningslogiken (in: felaktig metadata → ut: korrekt metadata)
- Ett enhetstest för namespace-rättning (flerdels-SOU)

## Spec-dokument
[Klistra in eller bifoga REINDEX_SOU_SPEC_v1.2.md]

## Tekniska krav

### Chroma $or-begränsning — KRITISK
Chroma stödjer INTE `$or` i `where`-filter. Använd alltid två separata anrop:
```python
results_a = collection.get(where={"forarbete_type": {"$eq": "sou"}}, ...)
results_b = collection.get(where={"authority_level": {"$eq": "persuasive"}}, ...)
```
Slå sedan ihop och deduplicera på ID innan vidare bearbetning.

### Listfält — KRITISK
`legal_area` och `references_to` är listfält som Chroma inte stödjer nativt.
De är lagrade som JSON-serialiserade strängar. Rör dem INTE — kopiera dem oförändrade från original_metadata.
Verifiera att de är strängar (inte listor) innan upsert.

### forarbete_rank — aldrig hårdkodat
Läs alltid från `config/forarbete_rank.yaml`:
```python
import yaml
def load_forarbete_rank(config_path="config/forarbete_rank.yaml"):
    with open(config_path) as f:
        cfg = yaml.safe_load(f)
    return {ft: data["rank"] for ft, data in cfg["forarbete_types"].items()}
```

### Namespace-rättning kräver delete + upsert
Chroma-IDs är immutabla. Namespace-rättning för flerdels-SOU kräver:
1. `collection.delete(ids=[old_ids])` — två separata anrop (pga $or-begränsning)
2. `collection.upsert(ids=[new_ids], ...)` med korrekt metadata och bevarade embeddings

### Metadata-rättning (utan namespace-ändring)
Använd `collection.update()` — snabbare, bevarar embeddings automatiskt.

### Error threshold
Om > 1% av chunks misslyckas: logga sammanfattning och avbryt med sys.exit(1).

### Miljövariabel
```python
import os
CHROMA_PATH = os.environ.get("CHROMA_PATH", "data/index/chroma")
```

## CLI-gränssnitt
```
python scripts/reindex_sou.py [--dry-run] [--limit N] [--verbose]
```
`--dry-run` är obligatoriskt att testa innan produktion. Skriptet ska tydligt logga "[DRY-RUN]" på varje operation som SKULLE utföras.

## Returnera
- `scripts/reindex_sou.py` — komplett implementation
- `tests/test_reindex_sou.py` — testsuite
- Kort avvikelserapport om du behövt avvika från spec (annars: "Inga avvikelser")

## Avvikelserapport
Om du stöter på ett problem som kräver arkitekturavvikelse: stanna och rapportera INNAN du fortsätter. Ange vad, varför, och vilket alternativ du föreslår. Överprojektet (Sonnet 4.6) fattar bindande beslut.
