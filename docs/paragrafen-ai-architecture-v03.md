# §AI (paragrafen.ai) — Systemarkitektur v0.3
## Uppdaterad med arkitekturbeslut 2026-02-23

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
| 3 | VektorDB | **Ej valt — se rekommendation nedan** | Börja med Chroma (lokal, gratis); migreringsbar |
| 4 | Uppdatering | Daglig | Cron daily + RSS-diff |
| 5 | Målgrupp | **Allmänheten (prio 1)** | Klarspråk-lager; förklarande output; disclaimers |
| 6 | Budget | 0 kr → donations | Allt self-hosted/gratis; embedding lokalt |
| 7 | Disclaimer | Accept + per svar + GitHub | Disclaimer-modul; `DISCLAIMER.md` i repo |
| 8 | Embeddings | Ej testat | **Se benchmark-rekommendation nedan** |
| 9 | Loggning | Nej, men modulärt förberett | Logging-interface definierat men disabled |
| 10 | Exkluderade områden | Straffrätt, asyl, skatterätt, VBU-tvister | Blocklist-config + tydligt felmeddelande |
| 11 | Konsolidering | **Se rekommendation nedan** | |
| 12 | Praxis-urval | HD, HFD, KamR (MiÖD, MmÖD), HovR — publicerade | Inget underrättsmaterial; domstolsfilter i ingest |

---

## Nya arkitekturkomponenter (baserat på beslut)

### 3. VektorDB-rekommendation

**Steg 1 (0 kr):** **Chroma** — lokal, open source, Python-native, ingen server krävs.
- Persistent storage till disk (SQLite + parquet)
- Stödjer HNSW-index, metadata-filtrering (norm_level, sfs_nr, ikraft)
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
4. Logga modell per chunk i metadata (migration-safe)

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

### 11. Konsolidering — rekommendation

**✅ Använd Riksdagens/RK:s konsoliderade versioner som bas — bygg inte egna.**

Motivering:
- RK publicerar konsoliderade texter på svenskforfattningssamling.se — auktoritativ källa
- Att bygga egen konsolideringslogik (merge grundförfattning + alla ändringar) är komplex och felkänslig
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
│   └── embedding_config.yaml      # Modell, chunk-size, etc.
├── docs/
│   ├── README.md
│   ├── CONTRACTS.md
│   ├── RAG_GUIDE.md
│   ├── QA_PLAYBOOK.md
│   ├── DISCLAIMER.md              # [NY] Juridisk friskrivning
│   └── ARCHITECTURE.md            # Detta dokument
├── schemas/
│   ├── data_model_v0.2.json
│   ├── doktrin_schema.json        # [NY] author-fält obligatoriskt
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
│   ├── court_filter.py            # [NY] Filtrerar HD/HFD/HovR/KamR
│   ├── area_classifier.py         # [NY] Klassificerar rättsområde
│   ├── praxis_ingest.py
│   ├── forfattning_ingest.py
│   ├── forarbete_ingest.py
│   ├── foreskrift_ingest.py
│   └── doktrin_ingest.py
├── normalize/
│   ├── base_normalizer.py
│   ├── sfs_parser.py
│   ├── praxis_parser.py
│   ├── forarbete_parser.py
│   ├── citation_handler.py        # [NY] Doktrin-citat med författare
│   └── klarsprak_layer.py         # [NY] Klarspråksanpassning
├── publish/
│   ├── front_matter.py
│   ├── eli_mapper.py
│   ├── consolidator.py            # Hämtar RK:s konsoliderade version
│   ├── link_resolver.py
│   └── disclaimer_injector.py     # [NY] Lägger till disclaimer per svar
├── index/
│   ├── chunker.py
│   ├── embedder.py                # E5-large / KBLab (konfigurbart)
│   ├── vector_store.py            # Chroma (lokal) → Qdrant (skalning)
│   └── norm_boost.py
├── guard/                          # [NY] Säkerhetsmodul
│   ├── area_blocker.py            # Blockerar exkluderade områden
│   ├── referral_messages.py       # Hänvisningsmeddelanden
│   └── confidence_gate.py         # Flaggar osäkra svar
├── logging/                        # [NY] Förberett men disabled
│   ├── __init__.py                # Logging-interface (noop impl)
│   ├── README.md                  # "Aktiveras i framtida version"
│   └── schemas/
│       └── log_event_schema.json
├── qa/
│   ├── hallucination_test.py
│   ├── gold_standard.json
│   ├── excluded_area_test.py      # [NY] Verifierar att block fungerar
│   ├── klarsprak_test.py          # [NY] Läsbarhetskontroll
│   ├── schema_validator.py
│   └── qa_reporter.py
├── ops/
│   ├── docker-compose.yaml        # [NY] Lokal drift; skalbar
│   ├── cron_daily.yaml
│   └── backup.py
├── data/                           # .gitignore; LFS vid behov
│   ├── raw/
│   ├── norm/
│   ├── pub/
│   └── index/                     # Chroma persistent storage
├── DISCLAIMER.md                   # Repo-rot: synlig i GitHub
├── LICENSE                         # [NY] Projektlicens
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

## Nästa steg — prioriterad ordning

| Prio | Uppgift | Verktyg | Tid |
|------|---------|---------|-----|
| 1 | Skapa GitHub-repo med folder tree + config-filer | Claude Code | 1 dag |
| 2 | Embedding-benchmark: E5-large vs KBLab på 200 juridiska frågor | Lokalt Python | 2-3 dagar |
| 3 | Författnings-pipeline (SFS) med daglig RSS-diff | Claude Code | 1 vecka |
| 4 | Chroma-setup + chunking av befintliga 16 000 avgöranden | Claude Code | 1 vecka |
| 5 | Guard-modul: area_blocker + disclaimer | Claude Code | 2 dagar |
| 6 | Förarbeten-pipeline via Riksdagen API | Claude Code | 1 vecka |
| 7 | QA: gold standard 50 frågor + hallucination-test | Claude Projects | Löpande |
| 8 | Klarspråks-lager för allmänheten | Claude Projects | Löpande |
