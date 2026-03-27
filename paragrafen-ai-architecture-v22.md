# §AI (paragrafen.ai) — Systemarkitektur v0.22
## Senast uppdaterad: 2026-03-23

### Ändringslogg

| Version | Datum | Ändringar |
|---------|-------|-----------|
| v0.13 | 2026-03-15 | U1–U5: Utvidgad förarbetspipeline |
| v0.14 | 2026-03-15 | V1–V7: Master-pipeline-arkitektur, splittade Chroma-instanser |
| v0.15 | 2026-03-17 | W1–W6: Curerad prop-pipeline |
| v0.16 | 2026-03-17 | X1–X5: Tvåfasad indexeringsstrategi, bulk-indexerare |
| v0.17 | 2026-03-19 | X6–X7, Y1–Y3: count_tokens-fix, prop slutförd, SOU re-fetch |
| v0.18 | 2026-03-20 | Y4–Y7, Z1–Z4: SOU slutförd, riksdagskedjan planlagd |
| v0.19 | 2026-03-21 | Z5–Z10: Ds, Bet, Dir, Rskr slutförda. Namnder förlorad. MVP-tidslinje. |
| v0.20 | 2026-03-22 | AA1–AA6: ARN återställd (2 809). FK föreskrifter slutförd (2 002). JO arkitekturbeslut. JK-utredning. |
| v0.21 | 2026-03-22 | AB1–AB4: JO slutförd (38 922). JK slutförd (11 802). Socialstyrelsen-pipeline beslutad. Totalantal 4 408 969. |
| **v0.22** | **2026-03-23** | **AC1–AC3: KV upphandlingspipeline beslutad (~18 954 avgöranden). Upphandlingsvertikal (betaltjänst) arkitekturskiss. Ny collection `paragrafen_upphandling_v1`.** |

---

## Arkitekturbeslut v0.22 — Upphandlingspipeline. Upphandlingsvertikal.

---

## AC1: Konkurrensverkets domstolsdatabas — BESLUTAD, implementation påbörjad

### Källa

`information.konkurrensverket.se/domar/` — ~18 954 avgöranden inom
offentlig upphandling från 2016-01-01 och framåt.

ASP-baserad databas. Ärende-URL: `arende.asp?id={sekventiellt_ID}`.
PDF-nedladdning via `/beslut/`-sökväg extraherad från ärendesidan.
Encoding: Windows-1252 (cp1252) — **inte** UTF-8.

### Instanser

| Instans | Antal (approx) | authority_level |
|---------|----------------|----------------|
| Förvaltningsrätt | ~14 000 | `"persuasive"` |
| Kammarrätt | ~4 200 | `"guiding"` |
| HFD | ~150 | `"binding"` |
| EU-domstolen | ~50 | `"binding"` |
| Övriga | ~500 | `"persuasive"` |

### Collection

**`paragrafen_upphandling_v1`** — separat collection.

Motivering:
1. Utökat metadata-schema (leverantör, upphandlande myndighet, ärendetyp, avgörande)
2. Oberoende pipeline-drift och inkrementell uppdatering
3. Separerar framtida betaltjänstens data från §AI:s gratistjänst
4. Undviker namespace-kollisioner med `paragrafen_praxis_v1`

### Namespace-konvention

```
upphandling::{målnummer_normaliserat}_chunk_{index:03d}
```

Vid duplikat-målnummer (samma mål i flera instanser):
```
upphandling::{målnummer}_{instanssuffix}_chunk_{index:03d}
```
Instanssuffix: `fr` | `kr` | `hfd` | `eu`

### Metadata-schema (utökat)

Utöver standardfält (`chunk_id`, `source_type`, `namespace`, `authority_level`,
`legal_area`, `chunk_index`, `chunk_total`, `chunk_text`):

| Fält | Typ | Beskrivning |
|------|-----|-------------|
| `court_level` | str | `forvaltningsratt` / `kammarratt` / `hfd` / `eu_domstol` / `ovrig` |
| `court` | str | Fullständigt domstolsnamn |
| `case_number` | str | Målnummer |
| `decision_date` | str | YYYY-MM-DD |
| `applicant` | str | Leverantör/sökande |
| `contracting_authority` | str | Upphandlande myndighet/enhet |
| `case_type` | str | Ärendetyp (från KV) |
| `outcome` | str | Avgörande (rå text från KV) |
| `outcome_category` | str | Normaliserad: `avslag` / `bifall` / `delvis_bifall` / `avskrivning` / `avvisning` / `oklart` |
| `citation` | str | T.ex. "Förvaltningsrätten i Falun, mål nr 5523-25, 2026-03-11" |
| `short_citation` | str | T.ex. "FörvR Falun 5523-25" |
| `kv_id` | int | Konkurrensverkets interna ärende-ID |
| `source_url` | str | URL till ärendesidan |
| `pdf_url` | str | URL till PDF |

### Pipeline-steg

| Steg | Skript | Beskrivning |
|------|--------|-------------|
| 1 | `kv_discovery.py` | Iterera ID 1–50 000, parsa ärendesida, spara metadata till JSONL |
| 2 | `kv_fetch.py` | Ladda ned PDF:er, extrahera text med pdfplumber |
| 3 | `kv_normalizer.py` | Chunka (600 tokens, sidbaserad), normalisera metadata |
| 4 | `kv_indexer.py` | Embedd + upsert till Chroma |

### Sektionsigenkänning (bonus, ej blockerande)

Upphandlingsavgöranden har standardiserad struktur:

| Sektion | Regex-trigger |
|---------|---------------|
| `bakgrund` | BAKGRUND, YRKANDEN |
| `grunder` | GRUNDER, PARTERNAS INSTÄLLNING |
| `bedomning` | DOMSTOLENS BEDÖMNING, SKÄLEN FÖR AVGÖRANDET |
| `domslut` | BESLUT, DOMSLUT |

### Estimat

| Parameter | Värde |
|-----------|-------|
| Ärenden | ~18 954 |
| Estimerade chunks | ~45 000–60 000 |
| Discovery-tid | ~3,5 timmar (4 parallella) |
| Total pipeline-tid | ~7–8 timmar |

### Inkrementell drift (post-lansering)

Daglig cron kl 06:00: kolla nya ID:n sedan senaste checkpoint.
~5–20 nya ärenden/dag → körtid <5 minuter.

---

## AC2: Upphandlingsvertikal — arkitekturskiss (betaltjänst)

### Koncept

AI-driven stödtjänst för leverantörer som vill begära
överprövning av offentlig upphandling. Modell: overklaga.se
applicerad på upphandlingsrätt.

### Förhållande till §AI

Separat produkt som delar infrastruktur med §AI:
- Samma ChromaDB-instans, men egen collection (`paragrafen_upphandling_v1`)
- Kan läsa från §AI:s SFS-, prop- och doktrin-collections (read-only)
- Egen affärslogik, eget ansvarsregim, egen frontend

### Datakällor

| Källa | Collection | Roll |
|-------|------------|------|
| KV domstolsdatabas | `paragrafen_upphandling_v1` | Primär praxiskälla |
| SFS (LOU, LUF, LUFS, LUK) | `paragrafen_sfs_v1` | Lagtextreferens |
| Propositioner (prop. 2015/16:195 m.fl.) | `paragrafen_prop_v1` | Förarbeten |
| Doktrin (upphandlingsrätt) | `paragrafen_doktrin_v1` | Stödreferens |
| Norstedts upphandlingskommentar | — | Kartläggningsresurs (ej citerbar) |

### Norstedts-strategi (BINDANDE)

Norstedts upphandlingskommentar används **enbart** som kartläggningsresurs
för att extrahera relationer: vilka kammarrättsavgöranden som berör
vilka LOU-paragrafer. Kommentarens text, analyser och formuleringar
reproduceras aldrig. Relationen "KamR mål X berör Y kap. Z § LOU"
är fakta, inte upphovsrättsskyddat verk.

Extraktion via skript som paragraf-för-paragraf identifierar
refererade avgöranden → mappningsfil `lou_paragraf → [målnummer]`.
Mappningsfilen används för metadata-enrichment av befintliga
praxis-chunks.

### Användarflöde (skiss)

```
Leverantör → Laddar upp tilldelningsbeslut + upphandlingsdokument
         → AI identifierar potentiella grunder (principkränkning)
         → Interaktivt formulär (samma frågor som upphandlingsjurist)
         → Genererad ansökan om överprövning med laghänvisningar + praxis
```

### Överprövningsgrunder (taxonomi)

Baseras på 1 kap. 9 § LOU (de grundläggande principerna):

| Princip | Typfall |
|---------|---------|
| Transparensprincipen | Otydliga utvärderingskriterier, bristande motivering |
| Likabehandlingsprincipen | Ojämlik anbudsprövning, ändrade krav under process |
| Proportionalitetsprincipen | Oproportionerliga kvalifikationskrav |
| Icke-diskriminering | Krav som utesluter utländska leverantörer |
| Ömsesidigt erkännande | Certifikat/intyg från annan EU-stat ej accepterade |

### Yrkanden (standardiserade)

| Situation | Yrkande |
|-----------|---------|
| Grundläggande fel i upphandlingen | Upphandlingen ska göras om |
| Vinnande anbud borde förkastas | Rättelse (ny anbudsprövning) |
| Avbrytandebeslut felaktigt | Upphandlingen ska slutföras |

### Status

Arkitekturskiss. Implementation post-MVP.
KV-pipelinen (AC1) är den konkreta förberedelsen.

---

## AC3: Filstruktur — KV-pipeline

```
paragrafen-ai/
├── fetch/
│   ├── kv_discovery.py          # [NY] metadata-harvest via ID-iteration
│   └── kv_fetch.py              # [NY] PDF-nedladdning + textextraktion
├── normalize/
│   └── kv_normalizer.py         # [NY] chunkning + schema-mapping
├── index/
│   └── kv_indexer.py            # [NY] embedding + Chroma-ingest
└── data/
    └── raw/
        └── upphandling/
            ├── metadata.jsonl   # En rad per ärende
            ├── checkpoint.json  # Resume-punkt
            └── decisions/       # Extraherad text per beslut
                └── *.json
```

---

## Chroma-arkitektur — KOMPLETT TABELL v0.22

| Collection | Dokumenttyper | Status | Chunks |
|------------|---------------|--------|--------|
| `paragrafen_prop_v1` | prop + lagr | ✅ Klar | 1 237 570 |
| `paragrafen_sou_v1` | sou | ✅ Klar | 1 127 413 |
| `paragrafen_bet_v1` | bet | ✅ Klar | 1 075 003 |
| `paragrafen_praxis_v1` | praxis | ✅ Klar | 309 351 |
| `paragrafen_doktrin_v1` | doktrin | ✅ Klar | 235 024 |
| `paragrafen_ds_v1` | ds | ✅ Klar | 180 099 |
| `paragrafen_sfs_v1` | sfs | ✅ Klar | 139 622 |
| `paragrafen_riksdag_v1` | dir + rskr | ✅ Klar* | 49 352 |
| `paragrafen_jo_v1` | JO-beslut | ✅ Klar | 38 922 |
| `paragrafen_jk_v1` | JK-beslut | ✅ Klar | 11 802 |
| `paragrafen_namnder_v1` | arn | ✅ Klar | 2 809 |
| `paragrafen_foreskrift_v1` | FK + (Socialstyrelsen) | ✅/🔄 | 2 002 |
| `paragrafen_upphandling_v1` | **KV upphandlingsavgöranden** | **🔄 Pågår** | **~45K–60K est.** |
| **Totalt indexerat** | | | **4 408 969 + ~50K** |

\* Kompletteringsfetch dir (~1 230) + rskr (~30 700) väntar på API.

### Pågående

| Collection | Status | Estimat |
|------------|--------|---------|
| `paragrafen_foreskrift_v1` | +Socialstyrelsen 🔄 | +30K–80K chunks |
| `paragrafen_upphandling_v1` | **Discovery påbörjad 🔄** | **~45K–60K chunks** |

### Post-MVP

| Collection | Dokumenttyper |
|------------|---------------|
| `paragrafen_foreskrift_v1` | +FI (FFFS), +Konsumentverket (KOVFS) |
| `paragrafen_handbok_v1` | Myndighetshandböcker |
| `paragrafen_riktlinje_v1` | Kommunala riktlinjer |

---

## MVP-tidslinje — uppdaterad

| Datum | Aktivitet |
|-------|-----------|
| 21 mars | FK ✅, ARN ✅ |
| 22 mars | JO ✅, JK ✅, Socialstyrelsen fetch+indexering |
| **23 mars** | **KV discovery startar (4 parallella tmux)** |
| **24 mars** | **KV PDF-fetch + textextraktion + indexering** |
| 23–25 mars | Kompletteringsfetch dir/rskr, ev. handböcker, ev. fler myndigheter |
| **25 mars — investerar­möte** | **Mål: ~18 954 upphandlingsavgöranden indexerade** |
| 26–28 mars | Guard-modul + RAG-lager + klarspråk |
| 29 mars | MVP testköring |

---

## Implementationsordning (uppdaterad v0.22)

### Pre-MVP: Kvarvarande

| Prio | Uppgift | Status |
|------|---------|--------|
| ~~1–11~~ | ~~Prop, SOU, Ds, Bet, Dir, Rskr, FK, ARN, JO, JK~~ | **✅ KLAR** |
| 12 | Socialstyrelsen (föreskrifter + publikationer) | 🔄 Pågår |
| 13 | Dir kompletteringsfetch (~1 230) | 🔜 API-beroende |
| 14 | Rskr kompletteringsfetch (~30 700) | 🔜 API-beroende |
| 15 | Handböcker (Socialstyrelsen, FK) | 🔜 |
| 16 | Guard-modul | 🔜 26–28 mars |
| 17 | RAG-lager + klarspråk | 🔜 26–28 mars |
| 18 | End-to-end smoke test | 🔜 29 mars |

### Upphandlingsvertikal (parallellt spår)

| Prio | Uppgift | Status |
|------|---------|--------|
| **U1** | **KV discovery (~18 954 ärenden)** | **🔄 Påbörjad** |
| **U2** | **KV PDF-fetch + textextraktion** | **🔜 Efter discovery** |
| **U3** | **KV normalisering + chunkning** | **🔜 Efter fetch** |
| **U4** | **KV Chroma-indexering** | **🔜 Efter normalisering** |
| U5 | Norstedts-extraktion (paragraf → avgörande-mappning) | 🔜 Post-indexering |
| U6 | Formulärflöde (överprövningsansökan) | 🔜 Post-MVP |
| U7 | Inkrementell daglig pipeline | 🔜 Post-lansering |

### Post-MVP

| Prio | Uppgift |
|------|---------|
| 19 | FI föreskrifter (FFFS) |
| 20 | Konsumentverket föreskrifter (KOVFS) |
| 21 | Kommunala riktlinjer |
| 22 | Bet-parser (sektionsigenkänning) |
| 23 | Bet pre-1990 + curerade centrala bet |
| 24 | LiU SOU-pipeline (1923–1993) |
| 25 | PropAdapter, SouAdapter etc. (inkrementell drift) |
| 26 | Myndighetshandböcker (utökad) |

---

## Beslutsstatus — tillägg v0.22

| ID | Beslut | Status | Datum |
|----|--------|--------|-------|
| **AC3** | **KV filstruktur fastställd. Separat collection `paragrafen_upphandling_v1`.** | **✅ BESLUTAT** | **2026-03-23** |
| **AC2** | **Upphandlingsvertikal arkitekturskiss. Separat produkt, delad infrastruktur. Norstedts enbart kartläggningsresurs.** | **🔜 SKISS** | **2026-03-23** |
| **AC1** | **KV domstolsdatabas pipeline beslutad. ~18 954 avgöranden. Discovery påbörjad.** | **🔄 PÅGÅR** | **2026-03-23** |
| AB4 | Handböcker och riktlinjer planerade | 🔜 PLANERAD | 2026-03-22 |
| AB3 | Socialstyrelsen-pipeline beslutad. ~3 448 publikationer. | 🔄 PÅGÅR | 2026-03-22 |
| AB2 | JK slutförd. 11 802 chunks. Differentierad authority_level. | ✅ SLUTFÖRD | 2026-03-22 |
| AB1 | JO slutförd. 38 922 chunks. 92% sektionsigenkänning. | ✅ SLUTFÖRD | 2026-03-22 |
| AA1–AA6 | Se v0.20 | ✅ | 2026-03-21/22 |
| Z5–Z10 | Se v0.19 | ✅ | 2026-03-21 |
| Y4–Y7, Z1–Z4 | Se v0.18 | ✅ | 2026-03-20 |
| X6–X7, Y1–Y3 | Se v0.17 | ✅ | 2026-03-19 |
| X1–X5 | Se v0.16 | ✅ | 2026-03-17 |
| W1–W6 | Se v0.15 | ✅ | 2026-03-17 |
| V1–V7 | Se v0.14 (V1 reviderad Z4) | ✅ | 2026-03-15 |

---

*Dokument: paragrafen-ai-architecture-v22.md | §AI paragrafen.ai | v0.22 | 2026-03-23*
*Relaterade dokument: AI_WORKFLOW_v1_4.md, PIPELINE_SPEC_kv_upphandling_v1.0.md*
*Ersätter: paragrafen-ai-architecture-v21.md*
