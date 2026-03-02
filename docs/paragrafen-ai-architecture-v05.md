# §AI (paragrafen.ai) — Systemarkitektur v0.5
## Uppdaterad med arkitekturbeslut 2026-02-24 (tillägg: SFS-pipeline S1–S7 + beslut S7 `numbering_type`)

---

## 0. Projektnamn — BESLUTAT

**§AI — paragrafen.ai**

| Egenskap | Värde |
|----------|-------|
| Varumärke / logotyp | **§AI** |
| Domän | `paragrafen.ai` *(verifierat ledigt 2026-02-23)* |
| GitHub-repo | `paragrafen-ai` |
| Tagline | *"Fråga Paragrafen"* |

Styrkor: Inga förväxlingsrisker med statliga system. Alla förstår "paragraf".
§-tecknet ger visuell identitet i logotyp, UI och markdown.

---

## Arkitekturbeslut (från motfrågorna)

| # | Fråga | Beslut | Arkitekturimplikation |
|---|-------|--------|----------------------|
| 1 | Upphovsrätt doktrin | Citat med författarangivelse; gratis tjänst | `author`-fält obligatoriskt i doktrin-schema; citathanteringsmodul |
| 2 | Drift | Lokalt initialt → skalbart | Docker-compose lokalt; inga molnberoenden i kärnan |
| 3 | VektorDB | **Chroma — gemensam instans med namespace-prefix** | Se beslut F1 nedan |
| 4 | Uppdatering | Daglig | Cron daily + RSS-diff |
| 5 | Målgrupp | **Allmänheten (prio 1)** | Klarspråk-lager; förklarande output; disclaimers |
| 6 | Budget | 0 kr → donations | Allt self-hosted/gratis; embedding lokalt |
| 7 | Disclaimer | Accept + per svar + GitHub | Disclaimer-modul; `DISCLAIMER.md` i repo |
| 8 | Embeddings | Ej testat | **Se benchmark-rekommendation nedan** |
| 9 | Loggning | Nej, men modulärt förberett | Logging-interface definierat men disabled |
| 10 | Exkluderade områden | Straffrätt, asyl, skatterätt, VBU-tvister | Blocklist-config + tydligt felmeddelande |
| 11 | Konsolidering | **Riksdagens/RK:s konsoliderade versioner** | Spara `consolidation_source: "rk"` i metadata |
| 12 | Praxis-urval | HD, HFD, KamR (MiÖD, MmÖD), HovR — publicerade | Inget underrättsmaterial; domstolsfilter i ingest |
| 13 | SFS-källa | **Riksdagens API** (primär och enda öppna källan) | `consolidation_source: "rk"`; `rkrattsbaser.gov.se` är ej tillgänglig |
| 14 | SFS §-numrering | `numbering_type: "relative" | "sequential"` — automatisk detektion | Se beslut S7 nedan |

---

## Fas 3-beslut — PRAXIS (2026-02-23) — BESLUTAT

Dessa beslut gäller omedelbart och ger klartecken för PRAXIS att starta Fas 3.

### F1 — ChromaDB-arkitektur — BESLUTAT ✅

**Beslut: Gemensam Chroma-instans med namespace-prefix.**

Namnkonvention: `praxis::HFD_2022_ref-010`, `doktrin::wennergren_2019_s042`, `sfs::1949:381_6kap`, `forarbete::prop_2016-17_180_s045`

Motivering: Cross-collection-queries är centralt för §AI:s värde. En fråga måste kunna korsa domänerna — t.ex. "Vilka HFD-avgöranden citerar prop. 2016/17:180?" — utan att behöva orkestreras mot separata instanser. Gemensam instans med `source_type`-filter i `where`-klausuler ger detta utan arkitekturkomplexitet.

Migration till Qdrant (steg 2) förblir möjlig; namespace-konventionen är instansoberoende.

### F2 — Metadata-harmonisering (Source-schema) — BESLUTAT ✅

**Beslut: Bekräftat schema. `citation`, `pinpoint`, `authority_level`, `legal_area` och `references_to: [source_id]` är rätt abstraktionsnivå.**

Implementationsregler:
- Alla avgöranden tilldelas `source_id` i ingest-steget *innan* citatgrafen byggs
- `hanvisadePubliceringarLista` (UUID-kopplingar i rådata) mappas till `references_to: [source_id]` som del av Fas 3-parsningen
- `authority_level` sätts i domstolsfilter enligt hierarki:
  - `binding` → HD, HFD
  - `guiding` → HovR, KamR (inkl. MiÖD, MmÖD)
- `legal_area` normaliseras mot kontrollerad vokabulär (lista definieras i `config/legal_areas.yaml`)
- Kompletterande textuell extraktion av `hanvisadePubliceringarLista` (fall med tomma listor trots synliga hänvisningar i löptext) genomförs i Fas 3

### F3 — Chunk-granularitet — BESLUTAT ✅

**Beslut: PRAXIS hybridförslag godkänt med metadatatillägg.**

Chunking-regler:
- **DOMSKÄL**: chunkas på styckegränser med 1-styckes överlapp (pinpoint-precision)
- **BAKGRUND** och **DOMSLUT**: egna chunks om ≤ 800 tokens; annars chunkas på styckegränser
- **Styckenummerbevaring**: pinpoint-references kräver att ursprungligt styckenummer bevaras i chunk-metadata

Obligatoriska metadata-fält per chunk:

```python
{
  "namespace":       str,   # t.ex. "praxis::HFD_2022_ref-010_chunk_003"
  "source_id":       str,   # UUID för avgörandet
  "source_type":     str,   # "praxis"
  "section":         str,   # "BAKGRUND" | "DOMSKÄL" | "DOMSLUT"
  "domstol":         str,   # "HFD" | "HD" | "HovR" | "KamR" | ...
  "avgorandedatum":  date,
  "authority_level": str,   # "binding" | "guiding"
  "legal_area":      list,  # normaliserad lista
  "pinpoint":        str,   # styckenummer eller sidreferens
  "embedding_model": str,   # t.ex. "intfloat/multilingual-e5-large"
  "chunk_index":     int,
  "chunk_total":     int
}
```

---

## Fas 3-beslut — DOKTRIN (2026-02-23) — BESLUTAT

Doktrin-projektet harmoniseras med PRAXIS-besluten. Samma Chroma-instans, samma schema-abstraktionsnivå.

### D1 — ChromaDB-namespace för Doktrin — BESLUTAT ✅

Namnkonvention: `doktrin::{author_efternamn}_{year}_s{page_padded}`

Exempel: `doktrin::wennergren_2019_s042`

Doktrin ingår i gemensam Chroma-instans (se F1). `source_type: "doktrin"` används som `where`-filter vid retrieval.

### D2 — Metadata-schema för Doktrin-chunks — BESLUTAT ✅

Obligatoriska metadata-fält per chunk (kompletterar `doktrin_schema.json`):

```python
{
  "namespace":        str,   # t.ex. "doktrin::wennergren_2019_s042_chunk_001"
  "source_id":        str,   # UUID för verket
  "source_type":      str,   # "doktrin"
  "author_name":      str,   # Fullständigt namn
  "work_title":       str,
  "edition":          str,
  "year":             int,
  "isbn":             str,   # om tillgängligt
  "page_start":       int,   # första sida i chunk
  "page_end":         int,   # sista sida i chunk
  "legal_area":       list,  # normaliserad lista (samma vokabulär som praxis)
  "citation_format":  str,   # "Wennergren, Förvaltningsprocesslagen, 3 uppl., s. 42"
  "authority_level":  str,   # alltid "persuasive" för doktrin
  "embedding_model":  str,
  "chunk_index":      int,
  "chunk_total":      int
}
```

### D3 — Chunk-granularitet för Doktrin — BESLUTAT ✅

- Primär chunking: **avsnittsgränser** (rubriknivå 2–3) med 1-styckes överlapp
- Max chunk-storlek: 600 tokens (lägre än praxis pga tätare referensbehov)
- Sidnummer bevaras i `page_start`/`page_end` — kritiskt för citat med sidhänvisning
- `citation_format` populeras automatiskt av `citation_handler.py` vid ingest

### D4 — Authority level för Doktrin — BESLUTAT ✅

Doktrin har alltid `authority_level: "persuasive"` i retrieval-logiken. Viktning i retrieval: praxis (`binding`) > praxis (`guiding`) > doktrin (`persuasive`). Retrieval-logiken i `norm_boost.py` hanterar viktning — inte embedding-steget.

---

## SFS-pipeline — Beslut S1–S7 (2026-02-24) — BESLUTAT

Pipeline S1–S7 implementerade och testade. Initial crawl (~11 400 SFS) körs lokalt. S8 (embedding-benchmark) och S9 (ChromaDB-indexering) återkopplas när crawlen är klar.

### Datakälla — BESLUTAT ✅

**Riksdagens öppna REST-API** (`data.riksdagen.se`) fastställs som primär och enda källa. `rkrattsbaser.gov.se` returnerar 404 per 2026-02-24. `consolidation_source: "rk"` bevaras i metadata. Volym: 11 415 SFS-dokument (verifierat). Uppskattad chunk-volym: ~150 000–200 000.

### Chunk-strategi — Beslut S1 ✅

| Scenario | Åtgärd |
|----------|--------|
| 100–800 tokens | 1 chunk = 1 paragraf |
| > 800 tokens | Split på styckegräns; `stycke`-fält sätts |
| < 100 tokens | Merge med nästa paragraf i samma kapitel |
| Definitionsparagraf (`is_definition: true`) | Standalone — aldrig merge |
| Övergångsbestämmelse | 1 chunk per block |

### Typade kanter — Beslut S6 ✅

`references_to` implementeras som JSON-sträng i ChromaDB Fas 1. Kontrollerad vokabulär: `cites` | `amends` | `repeals` | `defines` | `exempts` | `motivated_by` | `overturned_by` | `upheld_by` | `analogous_to`

### `legal_area`-klassificering — Beslut S5 ✅

Lager 1: `department_area_mapping.yaml` (`"department"`). Lager 2: `sfs_priority_mapping.yaml` (~200 lagar, `"manual"`). Lager 3: Haiku 4.5 API (`"llm"`, Fas 2).

### S7 — Löpande vs. kapitelrelativ §-numrering — BESLUTAT ✅

Riksdagens HTML använder identisk syntax för två juridiskt fundamentalt skilda numreringssystem. Distinktionen är kritisk: HD och doktrin citerar aldrig kapitel i sekventiella lagar.

| `numbering_type` | Lagar | Ankare-mönster | Korrekt citation |
|-----------------|-------|---------------|-----------------|
| `"relative"` | FB, MB, RB, ABL, JB | `K6P11` = kap. 6 § 11 | `FB 6 kap. 11 §` |
| `"sequential"` | AvtL, SkbrL, LAS | `K3P36` = § 36 globalt | `36 § AvtL (1915:218)` |

**Beslut:**
1. `numbering_type: "relative" | "sequential"` — nytt obligatoriskt fält i SFS-chunk-schema
2. Typ `sequential` behandlas som `har_kapitel=False` i namespace och citation-format
3. `detect_numbering_type()` i `sfs_parser.py` är primär källa (automatisk detektion)
4. `sfs_priority_mapping.yaml` utökas med `numbering_type_verified: true` — YAML vinner vid konflikt, avvikelse loggas

**Detektionslogik:**

```python
"""
1. Inga K-ankare               → "sequential"  (kapitellös lag)
2. K2 börjar INTE på P1        → "sequential"  (K2P10 = § 10 globalt)
3. K2 börjar på P1             → "relative"    (K2P1 = § 1 i kap. 2)
4. Bara K1, K1 ≥ threshold §§  → "sequential"  (trolig löpande)
5. Bara K1, K1 < threshold §§  → "relative"    (genuint enkelt kapitel)
"""
# sfs_parser.single_chapter_sequential_threshold: 15  (embedding_config.yaml)
```

**Verifierat:** 9 lagar, 100% träffsäkerhet. AvtL (`36 § AvtL` → `sfs::1915:218_0kap_36§`), FB (`FB 6 kap. 11 §` → `sfs::1949:381_6kap_11§`).

**Namespace-effekt:**

```
AvtL 36 §:      sfs::1915:218_0kap_36§_chunk_000   ← sequential
FB 6 kap. 11§:  sfs::1949:381_6kap_11§_chunk_000   ← relative
FL 22 §:        sfs::2017:900_0kap_22§_chunk_000   ← kapitellös
```

**Citation-format per `numbering_type`:**

| Värde | Format | Exempel |
|-------|--------|---------|
| `"relative"` | `{kortnamn} {kapitel} kap. {paragraf} §` | `FB 6 kap. 11 §` |
| `"sequential"` | `{paragraf} § {kortnamn} ({sfs_nr})` | `36 § AvtL (1915:218)` |

### Komplett metadata-schema för SFS-chunks ✅

```python
SFS_CHUNK_SCHEMA = {
  "namespace":              str,   # "sfs::1949:381_6kap_11§_chunk_000"
  "source_id":              str,   # UUID
  "source_type":            str,   # "sfs"
  "sfs_nr":                 str,   # "1949:381"
  "titel":                  str,
  "kortnamn":               str,   # "FB"
  "norm_type":              str,   # "grundlag"|"lag"|"forordning"|"foreskrift"
  "departement":            str,
  "ikraftträdande":         str,   # ISO-datum
  "upphävd":                bool,
  "consolidation_source":   str,   # "rk"
  "andrattillochmed":       str,
  "kapitel":                str,   # "" om kapitellös/sequential
  "kapitelrubrik":          str,
  "paragraf":               str,   # "36" eller "1a"
  "stycke":                 str,
  "numbering_type":         str,   # "relative"|"sequential"   ← S7
  "has_kapitel":            bool,
  "legal_area":             str,   # kommaseparerad (ChromaDB Fas 1)
  "legal_area_confidence":  str,   # "department"|"manual"|"llm"
  "is_definition":          bool,
  "is_overgangsbestammelse": bool,
  "has_table":              bool,
  "references_to":          str,   # JSON-sträng med typade kanter
  "authority_level":        str,   # "binding" (lag/grundlag) | "guiding" (förordning)
  "priority_weight":        float,
  "embedding_model":        str,
  "chunk_index":            int,
  "chunk_total":            int,
  "ingest_timestamp":       str
}
```

---

## Gemensam retrieval-arkitektur (PRAXIS + DOKTRIN + SFS)

```
Användarfråga
     │
     ▼
area_classifier.py  ──── exkluderat? ──→  referral_messages.py
     │ nej
     ▼
embedder.py  (E5-large / KBLab)
     │
     ▼
Chroma (gemensam instans)
  where: { source_type: { $in: ["praxis", "doktrin", "sfs", "forarbete"] } }
  where: { authority_level: { $in: ["binding", "guiding", "persuasive"] } }
  where: { legal_area: { $contains: <klassificerat_område> } }
     │
     ▼
norm_boost.py  (viktning: binding > guiding > persuasive)
     │
     ▼
Top-K chunks  →  LLM  →  svar med källförteckning
     │
     ▼
disclaimer_injector.py  →  output till användaren
```

---

## Nya arkitekturkomponenter (baserat på tidigare beslut)

### 3. VektorDB

**Steg 1 (0 kr):** **Chroma** — lokal, open source, Python-native, ingen server krävs.
- Persistent storage till disk (SQLite + parquet)
- Stödjer HNSW-index, metadata-filtrering (norm_level, sfs_nr, ikraft)
- Gemensam instans för alla source_types; namespace-prefix per dokument
- Migration till Qdrant/Weaviate möjlig senare (samma embedding-format)

**Steg 2 (vid skalning):** Qdrant (self-hosted Docker) — bättre prestanda vid >100k chunks, stödjer payload-filtrering på normhierarki.

**Varför inte Pinecone/Weaviate Cloud:** Kostar pengar; vendor lock-in; data lämnar Sverige.

### 8. Embedding-rekommendation för svensk juridisk text

| Modell | Kostnad | Sv juridik | Max tokens | Rekommendation |
|--------|---------|------------|------------|----------------|
| **intfloat/multilingual-e5-large** | Gratis (lokal) | Bra multilingual; testad på nordiska språk | 512 | **✅ Steg 1 — börja här** |
| KBLab/sentence-bert-swedish-cased | Gratis (lokal) | Tränad på svensk text (KB-data) | 512 | Testa parallellt; kan vara bättre på juridisk sv |
| text-embedding-3-small (OpenAI) | $0.02/1M tokens | Mycket bra; dyr vid volym | 8191 | Steg 2 vid budget |
| nomic-embed-text-v1.5 | Gratis (lokal) | Bra, Matryoshka-stöd | 8192 | Bra alternativ |

**Rekommenderad testplan:**
1. Kör 200 juridiska frågor mot 500 chunks med E5-large och KBLab
2. Mät recall@10 och precision@5
3. Välj vinnaren som produktionsmodell
4. Logga modell per chunk i metadata (`embedding_model`-fält) — migration-safe

### 10. Exkluderade rättsområden — implementation

```yaml
# config/excluded_areas.yaml
excluded_areas:
  - id: straffrätt
    label: "Straffrätt"
    sfs_patterns: ["1962:700", "2010:1408"]  # BrB, RB-delar
    message: "Denna tjänst täcker inte straffrättsliga frågor. Kontakta en advokat eller rättshjälpen."
  - id: asyl
    label: "Asylrätt och migration"
    sfs_patterns: ["2005:716", "2016:752"]
    message: "Asylrättsliga frågor kräver juridiskt ombud. Kontakta Advokatjouren eller Rådgivningsbyrån för asylsökande."
  - id: skatterätt
    label: "Skatterätt"
    sfs_patterns: ["1999:1229"]
    message: "För skattefrågor, kontakta Skatteverket eller en skatterådgivare."
  - id: vbu
    label: "Vårdnad, boende och umgänge"
    sfs_patterns: ["1949:381_kap6"]
    message: "Tvister om vårdnad, boende och umgänge kräver juridiskt ombud. Kontakta familjerätten i din kommun."

# Soft-block: assistenten svarar inte men ger hänvisning.
# Ska fungera på både fråge-klassificering och SFS-matchning.
```

### 11. Konsolidering — BESLUTAT

**✅ Använd Riksdagens/RK:s konsoliderade versioner som bas — bygg inte egna.**

Motivering:
- RK publicerar konsoliderade texter på svenskforfattningssamling.se — auktoritativ källa
- Att bygga egen konsolideringslogik är komplex och felkänslig
- Kvalitetskostnad: varje felaktig merge = juridisk felkälla → oacceptabel risk för allmänheten
- Spara `consolidation_source: "rk"` i metadata → tydlig provenance
- Om RK:s version saknas (ovanligt): flagga `"consolidation_source": "none"` → visa grundförfattning + lista ändringar separat

### 7. Disclaimer-implementation

```markdown
<!-- DISCLAIMER.md — ska finnas i repo-rot + visas vid accept + per svar -->

# Juridisk ansvarsfriskrivning

## Vid accept (visas en gång)
Denna tjänst tillhandahåller juridisk information baserad på svenska
rättskällor. Den utgör INTE juridisk rådgivning och ersätter INTE
en jurist. Informationen kan vara ofullständig eller inaktuell.
Använd alltid primärkällor för viktiga beslut.

## Per svar (fotnot i varje output)
---
⚠️ *Detta är juridisk information, inte juridisk rådgivning.
Kontrollera alltid mot primärkällan. Uppdaterad per [datum].*
*Källor: [lista med SFS/NJA/prop-ref]*
```

---

## Uppdaterad Project Instruction (220 ord)

```
DU ÄR ÖVERPROJEKTET "§AI (paragrafen.ai)" — en gratis svensk juridisk
AI-assistent riktad till allmänheten.

DELPROJEKT:
• svensk_rattspraxis — praxis (HD, HFD, HovR, KamR inkl MiÖD/MmÖD)
• Doktrin — doktrin (893 verk, citat med författarangivelse)
• overklagandeSkill — överklagandeguider per rättsområde

EXKLUDERADE OMRÅDEN (ge hänvisning, aldrig juridiskt svar):
Straffrätt, asylrätt, skatterätt, vårdnad/boende/umgänge.

NORMHIERARKI:
1) Grundlag 2) Lag 3) Förordning 4) Föreskrifter
5) Prejudikat (HD/HFD) 6) Förarbeten 7) Sedvana/doktrin

METOD: Ordalydelse → lex specialis → normhierarki →
förarbeten/praxis/systematik → doktrin.
Output: Fakta → Rättsregel → Bedömning → Slutsats.

MÅLGRUPP: Allmänheten. Skriv klarspråk. Förklara juridiska termer
vid första användning. Undvik fackjargong utan förklaring.

KÄLLKRAV:
• ≥2 verifierbara källor per påstående (SFS+§, NJA/HFD, prop).
• Doktrin-citat: alltid med författare, verk, sida.
• Ikraftträdandedatum vid författningshänvisning.
• "Källa ej verifierad" om osäker.

KONSOLIDERING: Använd RK:s konsoliderade texter som bas.

DISCLAIMER: Varje svar avslutas med juridisk ansvarsfriskrivning.
Se DISCLAIMER.md.

QA: ≥95% källträff vid stickprov. Hallucination-test varje sprint.
Pipeline-output följer CONTRACTS.md.
```

---

## Uppdaterat folder tree

```
paragrafen-ai/
├── .github/
│   └── workflows/
│       └── daily_update.yaml      # Daglig RSS-diff cron
├── config/
│   ├── excluded_areas.yaml        # Straffrätt, asyl, skatt, VBU
│   ├── court_filter.yaml          # HD, HFD, HovR, KamR
│   ├── legal_areas.yaml           # [NY] Kontrollerad vokabulär för legal_area
│   └── embedding_config.yaml      # Modell, chunk-size, etc.
├── docs/
│   ├── README.md
│   ├── CONTRACTS.md
│   ├── RAG_GUIDE.md
│   ├── QA_PLAYBOOK.md
│   ├── DISCLAIMER.md              # Juridisk friskrivning
│   └── ARCHITECTURE.md            # Detta dokument
├── schemas/
│   ├── data_model_v0.2.json
│   ├── doktrin_schema.json        # author-fält obligatoriskt
│   ├── chunk_metadata_schema.json # [NY] Gemensamt schema för alla chunk-typer
│   └── disclaimer_schema.json
├── sources/
│   ├── domstolsverket/
│   ├── riksdagen/
│   ├── regeringskansliet/
│   ├── myndigheter/
│   └── doktrin/
├── ingest/
│   ├── fetcher.py
│   ├── rss_watcher.py             # Daglig diff
│   ├── court_filter.py            # Filtrerar HD/HFD/HovR/KamR
│   ├── area_classifier.py         # Klassificerar rättsområde
│   ├── praxis_ingest.py
│   ├── forfattning_ingest.py
│   ├── forarbete_ingest.py
│   ├── foreskrift_ingest.py
│   └── doktrin_ingest.py
├── normalize/
│   ├── base_normalizer.py
│   ├── sfs_parser.py
│   ├── praxis_parser.py           # [UPD] HTML→text, sektionsigenkänning, styckenr
│   ├── forarbete_parser.py
│   ├── citation_handler.py        # Doktrin-citat med författare + citation_format
│   ├── legal_area_normalizer.py   # [NY] Normaliserar mot legal_areas.yaml
│   └── klarsprak_layer.py         # Klarspråksanpassning
├── publish/
│   ├── front_matter.py
│   ├── eli_mapper.py
│   ├── consolidator.py            # Hämtar RK:s konsoliderade version
│   ├── link_resolver.py
│   └── disclaimer_injector.py     # Lägger till disclaimer per svar
├── index/
│   ├── chunker.py                 # [UPD] Hybridstrategi: BAKGRUND/DOMSKÄL/DOMSLUT
│   ├── embedder.py                # E5-large / KBLab (konfigurbart)
│   ├── vector_store.py            # Chroma gemensam instans, namespace-prefix
│   └── norm_boost.py              # [UPD] Viktning: binding > guiding > persuasive
├── guard/                         # Säkerhetsmodul
│   ├── area_blocker.py            # Blockerar exkluderade områden
│   ├── referral_messages.py       # Hänvisningsmeddelanden
│   └── confidence_gate.py         # Flaggar osäkra svar
├── logging/                       # Förberett men disabled
│   ├── __init__.py                # Logging-interface (noop impl)
│   ├── README.md                  # "Aktiveras i framtida version"
│   └── schemas/
│       └── log_event_schema.json
├── qa/
│   ├── hallucination_test.py
│   ├── gold_standard.json
│   ├── excluded_area_test.py      # Verifierar att block fungerar
│   ├── klarsprak_test.py          # Läsbarhetskontroll
│   ├── cross_collection_test.py   # [NY] Testar cross-source-queries
│   ├── schema_validator.py
│   └── qa_reporter.py
├── ops/
│   ├── docker-compose.yaml        # Lokal drift; skalbar
│   ├── cron_daily.yaml
│   └── backup.py
├── data/                          # .gitignore; LFS vid behov
│   ├── raw/
│   ├── norm/
│   ├── pub/
│   └── index/                     # Chroma persistent storage (gemensam instans)
├── DISCLAIMER.md                  # Repo-rot: synlig i GitHub
├── LICENSE                        # Projektlicens
└── pyproject.toml
```

---

## Uppdaterad doktrin-schema (citat med författare)

```json
{
  "DoktrinDocument": {
    "required": ["id", "type", "title", "author", "source_url", "sha256"],
    "properties": {
      "author": {
        "type": "object",
        "required": ["name"],
        "properties": {
          "name": { "type": "string", "description": "Fullständigt namn" },
          "role": { "type": "string", "description": "Professor, docent, etc." }
        }
      },
      "work_title": { "type": "string" },
      "edition": { "type": "string" },
      "year": { "type": "integer" },
      "isbn": { "type": "string" },
      "publisher": { "type": "string" },
      "citation_format": {
        "type": "string",
        "description": "Mall: {author.name}, {work_title}, {edition}, s. {page}"
      }
    }
  }
}
```

---

## Beslutsstatus — sammanfattning

| ID | Fråga | Status | Datum |
|----|-------|--------|-------|
| F1 | ChromaDB-arkitektur (gemensam instans, namespace-prefix) | ✅ BESLUTAT | 2026-02-23 |
| F2 | Metadata-harmonisering (Source-schema) | ✅ BESLUTAT | 2026-02-23 |
| F3 | Chunk-granularitet (hybrid, metadata per chunk) | ✅ BESLUTAT | 2026-02-23 |
| D1 | Doktrin: Chroma-namespace | ✅ BESLUTAT | 2026-02-23 |
| D2 | Doktrin: Metadata-schema för chunks | ✅ BESLUTAT | 2026-02-23 |
| D3 | Doktrin: Chunk-granularitet | ✅ BESLUTAT | 2026-02-23 |
| D4 | Doktrin: authority_level = "persuasive" | ✅ BESLUTAT | 2026-02-23 |
| S1 | SFS: chunk-strategi (paragrafgranularitet) | ✅ BESLUTAT | 2026-02-24 |
| S2 | SFS: definitionsparagrafer och legal term resolver | ✅ BESLUTAT | 2026-02-24 |
| S3 | SFS: namespace-format för kapitellösa lagar (`0kap`) | ✅ BESLUTAT | 2026-02-24 |
| S4 | SFS: Riksdagens API som enda primärkälla | ✅ BESLUTAT | 2026-02-24 |
| S5 | SFS: `legal_area`-klassificering (3-lagers) | ✅ BESLUTAT | 2026-02-24 |
| S6 | SFS: typade kanter i `references_to` | ✅ BESLUTAT | 2026-02-24 |
| S7 | SFS: löpande vs. kapitelrelativ §-numrering (`numbering_type`) | ✅ BESLUTAT | 2026-02-24 |

**Fas 3 (PRAXIS) kan starta omedelbart.**
**Doktrin-chunking kan starta parallellt med PRAXIS Fas 3.**
**SFS-pipeline S1–S7 klar. Initial crawl körs lokalt. S8–S9 återkopplas efter crawl.**

---

## Nästa steg — prioriterad ordning

| Prio | Uppgift | Verktyg | Tid | Status |
|------|---------|---------|-----|--------|
| 1 | PRAXIS Fas 3: HTML-chunking av 16 746 avgöranden | Claude Code | 1 vecka | ▶ Pågår |
| 2 | DOKTRIN: chunking-pipeline (avsnittsgränser, 600 tokens) | Claude Code | 1 vecka | ▶ Pågår |
| 3 | SFS S-1: Initial crawl (~11 400 SFS) | Lokalt Python | 3–4h | ⏳ Körs lokalt |
| 4 | SFS S-8: Embedding-benchmark E5-large vs. KBLab | Lokalt Python | 2–3 dagar | ⏳ Väntar på crawl |
| 5 | SFS S-9: `sfs_indexer.py` + Chroma-setup | Claude Code | 2 dagar | ⏳ Väntar på S-8 |
| 6 | Författnings-pipeline (SFS) daglig RSS-diff | Claude Code | ingår i S-1 | ✅ Implementerat |
| 7 | Guard-modul: area_blocker + disclaimer | Claude Code | 2 dagar | — |
| 8 | Förarbeten-pipeline via Riksdagen API | Claude Code | 1 vecka | — |
| 9 | QA: gold standard 50 frågor + cross-collection-test | Claude Projects | Löpande | — |
| 10 | Klarspråks-lager för allmänheten | Claude Projects | Löpande | — |
