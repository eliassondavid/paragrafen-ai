# §AI (paragrafen.ai) — Systemarkitektur v0.17
## Senast uppdaterad: 2026-03-19

### Ändringslogg

| Version | Datum | Ändringar |
|---------|-------|-----------|
| v0.13 | 2026-03-15 | U1–U5: Utvidgad förarbetspipeline, typade references_to, checkpoint-modell, PAR, curerade förarbeten |
| v0.14 | 2026-03-15 | V1–V7: Ny master-pipeline-arkitektur för förarbeten. Splittade Chroma-instanser per förarbetstyp. Delete av `paragrafen_forarbete_v1`. Gemensamt ramverk med adapters. Hybrid retrieval (vector + BM25 post-lansering). Uppgradering chunk-strategi (700–1200 tokens, 10–15% överlapp). Tombstone-modell för ersatta dokument. |
| v0.15 | 2026-03-17 | W1–W6: Curerad prop-pipeline genomförd. 19 skippade propositioner kurerade och normaliserade. `build_curated_prop.py` som standardverktyg. Parsningsfix för curerade filer. Anomalier dokumenterade. |
| v0.16 | 2026-03-17 | X1–X5: Tvåfasad indexeringsstrategi. Bulk-indexerare för pre-lansering (fas 1). Adapter-ramverk reserverat för inkrementell drift (fas 2). Deprecering av Pipeline A-filer. Norm-filer deklarerade obsoleta. |
| **v0.17** | **2026-03-19** | **X6–X7, Y1–Y2: count_tokens-fix (tokenizer istf ordräkning). Chunk-parametrar reviderade (350/150/35). Prop bulk-indexering slutförd (1 237 570 chunks). SOU re-fetch genomförd. Coverage-verifiering obligatorisk. Section-taxonomi fastställd.** |

---

## Arkitekturbeslut v0.17 — Chunk-fix och prop-slutförande

---

## X6: count_tokens med riktig tokenizer — BESLUTAT OCH GENOMFÖRT ✅

### Bakgrund

`ForarbeteChunker.count_tokens()` räknade ord (`len(text.split())`) istället för subword-tokens. Embedding-modellen `KBLab/sentence-bert-swedish-cased` har `max_seq_length=384`. Med ordräkning producerade chunkern chunks på ~1560 faktiska tokens som truncerades till 384 vid embedding — 75% av varje chunk påverkade inte sökvektorn.

### Fix

| Komponent | Ändring |
|-----------|---------|
| `pipelines/common/chunk_base.py` | `count_tokens()` använder nu `AutoTokenizer.from_pretrained("KBLab/sentence-bert-swedish-cased")` |
| `pipelines/common/chunk_base.py` | `ChunkConfig` defaults reviderade |

### Reviderade ChunkConfig-parametrar (BINDANDE)

| Parameter | Gammalt (fel) | Nytt (korrekt) | Motivering |
|-----------|--------------|----------------|------------|
| `max_tokens` | 1200 (ord) | **350** (tokens) | Lämnar 34 tokens marginal för CLS+SEP. Hela chunken embedbas. |
| `min_tokens` | 700 (ord) | **150** (tokens) | Undviker micro-chunks. |
| `overlap_tokens` | 120 (ord) | **35** (tokens) | ~10% av max_tokens. |
| `min_chunk_chars` | 50 | **50** (oförändrad) | |

### Verifiering

| Test | Resultat |
|------|----------|
| count_tokens("9 ord") | 12 tokens (ratio 1.33) — korrekt |
| Prop. 2016/17:180 | 554 chunks, median 350, 100% ≤384 |
| Prop dry-run 5 dok | OK: 5, Failed: 0, Chunks: 6 763 |

Beslut V4 (v0.14) uppdateras härmed: chunk-strategi är 150–350 tokens (subword), 10% överlapp. Den tidigare formuleringen "700–1200 tokens" avsåg ord och var felaktig.

---

## X7: Coverage-verifiering obligatorisk — BESLUTAT ✅

### Bakgrund

SOU-fetchern missade ~460 dokument pga prefix-filter-bugg. Identifierades först vid manuell granskning — det fanns inget automatiserat verifieringssteg.

### Regel

Varje fetch-pipeline ska genomföra coverage-verifiering:

1. **Innan fetch:** Räkna `@traffar` från riksdagens API (eller motsvarande)
2. **Efter fetch:** Räkna filer på disk
3. **Jämför** mot förväntade filer (exkl kända undantag: pre-digitala, engelska versioner)
4. **Om diff > 1% av förväntade:** Pausa och eskalera till överprojektet

Gäller: alla förarbetespipelines, SFS-pipeline, praxis-pipeline.

---

## Y1: Prop bulk-indexering — SLUTFÖRD ✅

### Resultat

| Parameter | Värde |
|-----------|-------|
| Filer processerade | 9 778 |
| OK | 9 774 |
| Skippade | 4 |
| Failed | 0 |
| Felkvot | 0.00% |
| **Chunks i Chroma** | **1 237 570** |
| Median tokens/chunk | 314 |
| Max tokens/chunk | 350 |
| Chunks > 384 tokens | 0 |

### Skippade filer (alla godkända)

| Fil | Orsak |
|-----|-------|
| prop_2002-03_101 | Regeringsskrivelse, ingen rubrikstruktur |
| prop_2003-04_181 | Proposition ej utgiven |
| prop_2007-08_104 | Regeringsskrivelse, pdf2htmlEX utan extraherbar text |
| prop_2012-13_1_d20 | Stub-fil — noterad för fas 2-kurering |

### Section-taxonomi (fastställd)

Godkänd kontrollerad vokabulär för propositions-sektioner:

```
summary | legislation | transitional | consultation |
background | rationale | commentary | consequences |
appendix | other | document
```

`document` uppstår när hela dokumentet är under min_tokens och
slås ihop av `ForarbeteChunker` (3 förekomster av 1.2M — negligibelt).

Fördelning (sample 7 775 chunks):

| Sektion | Andel |
|---------|-------|
| appendix | 35.1% |
| consequences | 14.6% |
| background | 14.6% |
| consultation | 9.8% |
| transitional | 8.2% |
| rationale | 7.1% |
| other | 6.9% |
| commentary | 2.1% |
| summary | 1.4% |
| document | <0.1% |
| legislation | <0.1% |

### Pinpoint-täckning

| Typ | Andel |
|-----|-------|
| Med pinpoint (sidnummer) | 42.1% |
| Utan pinpoint | 57.9% |

57.9% utan pinpoint förklaras av äldre propositioner (pre-2000)
konverterade med pdf2htmlEX som saknar `#page_N`-struktur i HTML.
Post-lansering-förbättring: re-OCR eller PDF-pagesource-mappning.

### Coverage-verifiering

| Källa | Antal |
|-------|-------|
| Riksdagens API (totalt) | 31 703 |
| Filer på disk | 9 778 |
| Diff | 21 925 |
| Förklaring | Pre-1970-propositioner utan digital text |

Prop-coverage för digitalt tillgängliga propositioner: **~100%**.

### Godkänt — beslut Y1 och Y2

- **Y1 (section "document"):** Accepterat. 3/1.2M = negligibelt. Korrekt beteende i `ForarbeteChunker`.
- **Y2 (diff 63 chunks logg vs Chroma):** Accepterat. 63/1.2M = 0.005%. Trolig namespace-kollision vid upsert.

---

## Y3: SOU re-fetch — GENOMFÖRD ✅

### Bakgrund

Befintliga SOU-råfiler var fördelade på två mappar med inkonsekvent schema:
- `data/raw/sou/` (~2 967 filer, nyare format med beteckning)
- `data/raw/forarbete/sou/` (~4 443 filer, äldre format utan beteckning)

Totalt ~7 184 unika, men 2 743 saknade beteckning och annan metadata.

### Beslut

Radera alla befintliga SOU-råfiler och re-fetcha via `ingest/sou_fetcher.py`.
13 saknade moderna SOU:er (2008–2012) hämtades manuellt.
3 engelska versioner exkluderades (embedding-modellen är svensk).
`data/raw/forarbete/sou/` raderad permanent.

### Resultat

| Parameter | Värde |
|-----------|-------|
| SOU i API | 4 914 |
| Filer på disk efter re-fetch | 4 468 |
| Varav manuellt hämtade | 13 |
| Diff mot API | 446 |
| Varav C-prefix (pre-1994, LiU post-lansering) | ~443 |
| Varav engelska (exkluderade) | 3 |
| Modern coverage | **99.9%** |

### Beslut om exkluderade kategorier

| Kategori | Beslut |
|----------|--------|
| Engelska parallellversioner (EN-suffix) | EXKLUDERA — embedding-modellen är svensk |
| Lättläst-versioner (LL-suffix) | INKLUDERA — relevant för §AI:s målgrupp |
| Exkluderade rättsområden (migration, skatt) | FETCHA och indexera med `excluded_at_retrieval: True` |
| Statistikbilagor (separata dok_id) | INKLUDERA |

---

## Chroma-arkitektur — KOMPLETT TABELL v0.17

| Instans | Sökväg | Collection | Dokumenttyper | Status | Chunks |
|---------|--------|------------|---------------|--------|--------|
| `paragrafen_prop_v1` | `data/index/chroma/prop/` | `paragrafen_prop_v1` | prop + lagr + ds | **✅ KLAR** | **1 237 570** |
| `paragrafen_sou_v1` | `data/index/chroma/sou/` | `paragrafen_sou_v1` | sou | 🔄 Bulk-indexering startas | ~1M–2.5M estimerat |
| `paragrafen_riksdag_v1` | `data/index/chroma/riksdag/` | `paragrafen_riksdag_v1` | bet + rskr + dir | 🔜 Post-lansering | ~65–120K |
| `paragrafen_nja_ii_v1` | `data/index/chroma/nja_ii/` | `paragrafen_nja_ii_v1` | nja_ii | 🔜 Post-prop+SOU | ~1K |
| `paragrafen_praxis_v1` | `data/index/chroma/praxis/` | `paragrafen_praxis_v1` | praxis | ✅ Klar | 309 351 |
| `paragrafen_doktrin_v1` | `data/index/chroma/doktrin/` | `paragrafen_doktrin_v1` | doktrin | ✅ Klar | 235 024 |
| `paragrafen_sfs_v1` | `data/index/chroma/sfs/` | `paragrafen_sfs_v1` | sfs | ✅ Klar | 139 622 |
| `paragrafen_namnder_v1` | `data/index/chroma/namnder/` | `paragrafen_namnder_v1` | nja_ii, arn | ✅ Klar | 1 813 |
| ~~`paragrafen_forarbete_v1`~~ | ~~`data/index/chroma/forarbete/`~~ | ~~`paragrafen_forarbete_v1`~~ | ~~prop+sou+nja_ii~~ | ❌ DELETAD | ~~1 767 220~~ |

**Totalt indexerat (v0.17):** 1 923 380 chunks (prop + praxis + doktrin + sfs + namnder)
**Totalt efter SOU:** ~2.9M–4.4M chunks estimerat

---

## Implementationsordning (uppdaterad v0.17)

### Pre-lansering: Bulk-indexering

| Prio | Uppgift | Verktyg | Status |
|------|---------|---------|--------|
| ~~1~~ | ~~Prop bulk-indexering~~ | ~~`prop_bulk_indexer.py`~~ | **✅ KLAR** (1 237 570 chunks) |
| 2 | SOU bulk-indexering | `sou_bulk_indexer.py` (Codex via Sonnet 4.6) | 🔄 Codex-prompt skrivs |
| 3 | Deprecera Pipeline A-filer | David lokalt | 🔜 Efter SOU-verifiering |
| 4 | Radera obsoleta norm-filer | David lokalt | 🔜 Efter SOU-verifiering |
| 5 | Guard-modul | GPT-5.4 Codex | 🔜 |
| 6 | RAG-lager + klarspråk | GPT-5.4 Thinking | 🔜 |
| 7 | End-to-end smoke test | Sonnet 4.6 | Pre-lansering |

### Post-lansering: Inkrementell drift

| Prio | Uppgift | Verktyg | Status |
|------|---------|---------|--------|
| 8 | PropAdapter (fetch + inkrementell) | GPT-5.4 Codex | Post-lansering |
| 9 | SouAdapter (fetch + inkrementell) | GPT-5.4 Codex | Post-lansering |
| 10 | BetAdapter | GPT-5.4 Codex | Post-lansering |
| 11 | DsAdapter | GPT-5.4 Codex | Post-lansering |
| 12 | DirAdapter | GPT-5.4 Codex | Post-lansering |
| 13 | RskrAdapter | GPT-5.4 Codex | Post-lansering |
| 14 | NJA II re-ingest | GPT-5.4 Codex | Post-lansering |

---

## Beslutsstatus — komplett tabell v0.17

| ID | Beslut | Status | Datum |
|----|--------|--------|-------|
| **Y3** | **SOU re-fetch genomförd. 4 468 filer med komplett schema. Legacy-mapp raderad.** | **✅ GENOMFÖRD** | **2026-03-19** |
| **Y2** | **Diff 63 chunks (logg vs Chroma) accepterad som negligibel** | **✅ GODKÄND** | **2026-03-19** |
| **Y1** | **Prop bulk-indexering slutförd. 1 237 570 chunks. 0.00% felkvot.** | **✅ SLUTFÖRD** | **2026-03-18** |
| **X7** | **Coverage-verifiering obligatorisk före indexering** | **✅ BESLUTAT** | **2026-03-19** |
| **X6** | **count_tokens med AutoTokenizer. ChunkConfig 350/150/35.** | **✅ GENOMFÖRD** | **2026-03-18** |
| X5 | Content hash (SHA-256) som fas 1 → fas 2-brygga | ✅ BESLUTAT | 2026-03-17 |
| X4 | Norm-filer deklarerade obsoleta | ✅ BESLUTAT | 2026-03-17 |
| X3 | Pipeline A-filer depreceras | ✅ BESLUTAT | 2026-03-17 |
| X2 | `prop_bulk_indexer.py` implementerad | ✅ IMPLEMENTERAD | 2026-03-17 |
| X1 | Tvåfasad indexeringsstrategi: bulk (fas 1) + inkrementell (fas 2) | ✅ BESLUTAT | 2026-03-17 |
| W1–W6 | Se v0.15 | ✅ | 2026-03-17 |
| V1–V7 | Se v0.14 | ✅ | 2026-03-15 |

---

## X1–X5, V1–V7, W1–W6: Oförändrade

*(Se paragrafen-ai-architecture-v16.md respektive v15.md, v14.md)*

---

*Dokument: paragrafen-ai-architecture-v17.md | §AI paragrafen.ai | v0.17 | 2026-03-19*
*Relaterade dokument: AI_WORKFLOW_v1_4.md*
*Ersätter: paragrafen-ai-architecture-v16.md*
