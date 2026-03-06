# §AI — Re-indexering SOU-chunks: Specifikation v1.2
## Datum: 2026-03-06
## Status: FASTSTÄLLD av överprojektet
## Implementeras av: GPT-5.4 (Codex)
## Verifieras av: Sonnet 4.6

---

## 1. Bakgrund och syfte

Befintliga SOU-chunks i ChromaDB (`paragrafen_forarbete_v1`) indexerades med felaktigt schema:

| Fält | Fel värde | Korrekt värde |
|------|-----------|---------------|
| `authority_level` | `"persuasive"` | `"preparatory"` |
| `forarbete_rank` | saknas | `3` (int) |
| `forarbete_type` | saknas eller fel | `"sou"` |
| namespace-format (flerdels-SOU) | `forarbete::sou_2003_33_chunk_000` (index nollställt per del) | `forarbete::sou_2003_33_d2_chunk_042` (kontinuerligt index med del-suffix) |

Syftet med detta skript är att rätta samtliga befintliga SOU-chunks utan att radera och återindexera hela samlingen (för kostsamt — ~802K chunks).

---

## 2. Scope

- **Collection:** `paragrafen_forarbete_v1`
- **Dokument som berörs:** alla chunks med `source_type == "forarbete"` och `forarbete_type == "sou"` (eller där `forarbete_type` saknas men namespace börjar med `forarbete::sou_`)
- **Skriptet:** `scripts/reindex_sou.py`
- **Körläge:** måste stödja `--dry-run` (obligatoriskt CLI-argument)

---

## 3. Vad skriptet ska göra

### 3.1 Steg 1 — Hämta alla berörda chunks

```python
# Hämta chunks via get() med where-filter
# OBS: Chroma stödjer inte $or i where-filter med en enda get()-anrop.
# Lösning: två separata anrop.

results_a = collection.get(
    where={"forarbete_type": {"$eq": "sou"}},
    include=["metadatas", "documents", "embeddings"]
)

results_b = collection.get(
    where={"authority_level": {"$eq": "persuasive"}},
    include=["metadatas", "documents", "embeddings"]
)

# Slå ihop och deduplicera på id
```

### 3.2 Steg 2 — Identifiera chunks som behöver rättas

För varje chunk: kontrollera om något av följande är fel:
- `authority_level != "preparatory"`
- `forarbete_rank` saknas eller `!= 3`
- `forarbete_type` saknas eller `!= "sou"`
- namespace-ID innehåller `_d{N}_chunk_` med nollställt index (se avsnitt 4)

### 3.3 Steg 3 — Bygg korrekt metadata

```python
corrected_metadata = {
    **original_metadata,
    "authority_level": "preparatory",
    "forarbete_rank": 3,          # int, inte sträng
    "forarbete_type": "sou",
}
```

Fält som **inte** ska ändras: `source_type`, `legal_area`, `references_to`, `citation`, `pinpoint`, `text`, `source_url`, `sha256`, `title`, `year`, `department`, `chunk_index`, `source_origin`.

### 3.4 Steg 4 — Rätta namespace för flerdels-SOU

Flerdels-SOU-dokument (t.ex. SOU 2003:33 del 1 och del 2) har indexerats med nollställt chunk-index per del, vilket ger kolliderande IDs. Korrekt format inkluderar `d{N}`-suffix och kontinuerligt index.

**Identifiering av flerdels-problem:**
```python
# Ett flerdels-problem föreligger om två chunks har samma prefix
# men namespace-ID kolliderar (samma chunk_index, olika del)
# Detta kräver en grupperingsanalys före rättning.
```

**Korrekt namespace-format:**
```
Endels-SOU:   forarbete::sou_{år}_{nr}_chunk_{index:03d}
Flerdels-SOU: forarbete::sou_{år}_{nr}_d{del}_chunk_{index:03d}
```

**Exempel:**
```
Fel:     forarbete::sou_2003_33_chunk_000  (del 1, chunk 0)
Fel:     forarbete::sou_2003_33_chunk_000  (del 2, chunk 0) ← kollision
Rätt:    forarbete::sou_2003_33_d1_chunk_000
Rätt:    forarbete::sou_2003_33_d2_chunk_042
```

**Viktigt:** Namespace-rättning kräver delete + upsert (ID kan inte ändras in-place i Chroma). Metadata-rättning (authority_level, forarbete_rank) görs med `collection.update()`.

### 3.5 Steg 5 — Upsert/update

```python
# För chunks som BARA behöver metadata-rättning (authority_level, forarbete_rank):
collection.update(
    ids=[chunk_id],
    metadatas=[corrected_metadata]
)

# För chunks som behöver namespace-rättning (flerdels-kollision):
# 1. Delete gamla ID:n (två separata delete()-anrop pga Chroma $or-begränsning)
collection.delete(ids=[old_id_list_a])
collection.delete(ids=[old_id_list_b])
# 2. Upsert med nya ID:n och korrekt metadata + embeddings
collection.upsert(
    ids=[new_id],
    metadatas=[corrected_metadata],
    documents=[original_text],
    embeddings=[original_embedding]
)
```

### 3.6 Steg 6 — Felhantering och loggning

- Skip-and-log för chunks med tomt `documents`-fält (tom text)
- 1% error threshold: om fler än 1% av chunks misslyckas → avbryt och rapportera
- Logga varje rättad chunk på DEBUG-nivå, summera på INFO-nivå
- Alla fel loggas med chunk-ID och felmeddelande

---

## 4. Metadata-schema — fullständigt korrekt schema för SOU-chunks

```python
{
    # --- Obligatoriska fält ---
    "source_type": "forarbete",           # str, aldrig ändras
    "forarbete_type": "sou",              # str
    "authority_level": "preparatory",     # str — ALLTID detta värde för förarbeten
    "forarbete_rank": 3,                  # int — läses från config/forarbete_rank.yaml
    "legal_area": "[\"arbetsrätt\"]",     # str — JSON-serialiserad lista (json.dumps())
    "references_to": "[]",               # str — JSON-serialiserad lista (json.dumps())
    "citation": "SOU 2015:14",            # str
    "pinpoint": "s. 42",                  # str | None
    "title": "...",                       # str
    "year": 2015,                         # int
    "department": "...",                  # str | None
    "source_url": "https://...",          # str
    "sha256": "...",                      # str
    "chunk_index": 42,                    # int
    "source_origin": "riksdagen",         # str
    # --- Flerdels-SOU specifikt ---
    "del": 1,                             # int | None — None för endels-SOU
}
```

**Kritiskt:** `legal_area` och `references_to` är listfält som måste serialiseras med `json.dumps()` innan de lagras i Chroma (Chroma stödjer inte lista-typer i metadata). Vid läsning deserialiseras med `json.loads()`.

---

## 5. CLI-gränssnitt

```
python scripts/reindex_sou.py [--dry-run] [--limit N] [--verbose]

Argument:
  --dry-run     Kör utan att skriva till Chroma. Loggar vad som SKULLE rättas.
                Obligatoriskt att testa med --dry-run innan produktion.
  --limit N     Begränsa till N chunks (för testning)
  --verbose     DEBUG-loggning
```

**Körordning:**
```bash
# Steg 1: dry-run på begränsat antal
python scripts/reindex_sou.py --dry-run --limit 100 --verbose

# Steg 2: dry-run på hela samlingen
python scripts/reindex_sou.py --dry-run --verbose

# Steg 3: produktion (efter godkänd dry-run)
python scripts/reindex_sou.py --verbose
```

---

## 6. Konfigurationsberoenden

Skriptet läser `forarbete_rank` från `config/forarbete_rank.yaml` — aldrig hårdkodat:

```python
import yaml
from pathlib import Path

def load_forarbete_rank(config_path: str = "config/forarbete_rank.yaml") -> dict:
    with open(config_path) as f:
        cfg = yaml.safe_load(f)
    return {
        ft: data["rank"]
        for ft, data in cfg["forarbete_types"].items()
    }

# Används som:
RANKS = load_forarbete_rank()
sou_rank = RANKS["sou"]  # → 3
```

---

## 7. Chroma-anslutning

```python
import chromadb

client = chromadb.PersistentClient(path="data/index/chroma")
collection = client.get_collection("paragrafen_forarbete_v1")
```

Chroma-sökväg läses från miljövariabel med fallback:
```python
import os
CHROMA_PATH = os.environ.get("CHROMA_PATH", "data/index/chroma")
```

---

## 8. Förväntad output (dry-run)

```
INFO  reindex_sou: Hittade 801 919 chunks totalt
INFO  reindex_sou: Chunks med felaktig authority_level: 801 919
INFO  reindex_sou: Chunks med saknad forarbete_rank: 801 919
INFO  reindex_sou: Chunks med flerdels-kollision: 4 312
INFO  reindex_sou: [DRY-RUN] Skulle uppdatera metadata: 801 919 chunks
INFO  reindex_sou: [DRY-RUN] Skulle rätta namespace: 4 312 chunks
INFO  reindex_sou: [DRY-RUN] Inga skrivoperationer utförda.
```

---

## 9. Verifieringschecklista (Steg 3 — Sonnet 4.6)

Efter implementation, innan produktion:

```
□ --dry-run kör utan fel och producerar rimlig output
□ authority_level == "preparatory" för samtliga rättade chunks
□ forarbete_rank == 3 (int, inte str "3") för samtliga SOU-chunks
□ forarbete_type == "sou" för samtliga berörda chunks
□ legal_area och references_to är JSON-serialiserade strängar (inte listor)
□ Flerdels-SOU: d{N}-suffix i namespace, kontinuerligt index
□ Endels-SOU: inget d{N}-suffix
□ Inga hårdkodade rank-värden — läses från config/forarbete_rank.yaml
□ 1%-error-threshold implementerad
□ Inga nakna exceptions
□ CHROMA_PATH läses från miljövariabel med fallback
□ Tester: minst ett smoke-test för dry-run + ett för metadata-rättning
```

---

## 10. Filer som berörs

| Fil | Åtgärd |
|-----|--------|
| `scripts/reindex_sou.py` | Skapas (ny) |
| `tests/test_reindex_sou.py` | Skapas (ny) |
| `config/forarbete_rank.yaml` | Läses (finns redan) |

Inga ändringar i befintliga moduler.

---

## 11. Avvikelserapport

Om implementationen kräver avvikelse från denna spec: **pausa och rapportera till överprojektet (Sonnet 4.6) innan du fortsätter.** Ange: vad som avviker, varför, och vilket alternativ du föreslår. Överprojektet fattar bindande beslut.

---

*Spec: REINDEX_SOU_SPEC_v1.2.md | §AI paragrafen.ai | 2026-03-06*
*Relaterade dokument: paragrafen-ai-architecture-v05.md (F4), config/forarbete_rank.yaml*
