# §AI (paragrafen.ai) — AI-arbetsflöde och modellallokering
## Version 1.1 — 2026-02-24
### Styrprincip: Rätt modell på rätt uppgift — uppdateras löpande mot aktuell modellinformation

---

## 0. Grundprincip

§AI-projektet använder ett multi-modell orkestreringsflöde. Rätt modell väljs per uppgiftstyp baserat på förmåga, kostnad och lämplighet — inte modelltillhörighet. Senaste information om modeller söks aktivt innan allokeringsbeslut fattas.

**Claude Sonnet 4.6** är systemarkitekt och övergripande verifieringspunkt (steg 1 och 3). **Opus 4.6 + ET reserveras** för genuint komplexa beslut med motstridiga krav — inte rutinuppgifter. Implementation och kodning (steg 2) delegeras till den modell som är bäst lämpad per uppgift.

---

## 1. Modellmatris — verifierad 2026-02-24

### Codex-familjen (implementation och kodning)

| Modell | Styrka | Lämpad för i §AI |
|---|---|---|
| **GPT-5.3-Codex** | Bästa agentic coding; kombinerar GPT-5.2-Codex + GPT-5.2 reasoning; 25% snabbare; stark på terminal och computer-use; steerable mid-task | Komplexa pipelines, cross-file-logik, long-horizon tasks |
| **GPT-5.2-Codex** | Context compaction; stark på stora kodförändringar och migrationer; pålitlig för long-running tasks | Stabila, väldefinierade implementationsuppgifter |
| **GPT-5.1-Codex-Max** | Long-running med compaction; kompetent på projektskaliga uppgifter | Reservalternativ vid specifika behov |
| **GPT-5.1-Codex-Mini** | Kostnadseeffektiv; snabb; kompetent för enkla uppgifter | Rutinändringar, enkla skript, satsvis strängformatering |

### ChatGPT-modeller (reasoning och analys)

| Modell | Styrka | Lämpad för i §AI |
|---|---|---|
| **GPT-5.2 Thinking** | Komplex multi-step reasoning; GPQA Diamond 92.4%; expert-level kunskapsarbete | Reasoning-tunga moduler: viktningslogik, normhierarki, guard |
| **GPT-5.2 Instant** | Snabb, generell, stark instruktionstolkning | Enklare analys, dokumentbearbetning |

### Claude (arkitektur och verifiering)

| Modell | Roll | Används när |
|---|---|---|
| **Sonnet 4.6** | Steg 1 (spec) + Steg 3 (verifiering) — standardval | Alltid i steg 1 och 3 |
| **Opus 4.6 + ET** | Blockerande arkitekturproblem | Motstridiga krav; hög osäkerhet; juridisk systembeslut |

### Specialmodeller

| Modell | Roll i §AI |
|---|---|
| **Gemini 2.0/3.1 Pro** | OCR-pipeline för inscannat material (UB Göteborg etc.) |
| **Haiku 4.5** (API, runtime) | `legal_area`-klassificering vid ingest — automatisk, låg kostnad |

---

## 2. Arbetsflödet — tre steg

```
┌─────────────────────────────────────────────────────────────────┐
│  STEG 1 — ARKITEKTUR & PROMPT (Sonnet 4.6)                     │
│                                                                   │
│  • Beslutar arkitektur och schema                                │
│  • Skriver specifikationsdokument (.md)                          │
│  • Upprättar implementationsprompt                               │
│  • Identifierar kantfall och eskaleringsvillkor                  │
│                                                                   │
│  → Opus 4.6 + ET vid blockerande beslut med motstridiga krav    │
└──────────────────────────┬──────────────────────────────────────┘
                           │  Prompt + spec-fil (.md)
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│  STEG 2 — IMPLEMENTATION                                        │
│                                                                   │
│  GPT-5.3-Codex      → komplexa pipelines, long-horizon tasks    │
│  GPT-5.2-Codex      → väldefinierade, stabila implementationer  │
│  GPT-5.1-Codex-Mini → enkla skript, rutinändringar              │
│  GPT-5.2 Thinking   → reasoning-tunga moduler (guard, boost)    │
│  Gemini Vision      → OCR av inscannat material                 │
│  Lokalt i Terminal  → körning, benchmark, testning              │
│                                                                   │
│  Returnerar: kod + testresultat + avvikelserapport               │
└──────────────────────────┬──────────────────────────────────────┘
                           │  Resultat tillbaka
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│  STEG 3 — VERIFIERING & FASTSTÄLLANDE (Sonnet 4.6)             │
│                                                                   │
│  • Kontrollerar mot spec och arkitekturbeslut                    │
│  • Schema-konformitet, namespace, authority_level                │
│  • Rättar eller skickar tillbaka för revision                    │
│  • Fastställer slutlig version                                   │
└─────────────────────────────────────────────────────────────────┘
```

---

## 3. Uppgiftsallokering per delprojekt

### PRAXIS-pipeline

| Uppgift | Modell | Motivering |
|---|---|---|
| HTML-chunking, 16 746 avgöranden | GPT-5.3-Codex | Long-horizon, cross-file, komplex dokumentstruktur |
| Sektionsigenkänning BAKGRUND/DOMSKÄL/DOMSLUT | GPT-5.2-Codex | Väldefinierat mönster |
| Citatgrafbygge (`references_to`) | GPT-5.3-Codex | Komplex cross-reference-logik |
| Schema-verifiering och QA | Sonnet 4.6 (steg 3) | Verifiering |

### DOKTRIN-pipeline

| Uppgift | Modell | Motivering |
|---|---|---|
| Chunking-pipeline, 600 tokens | GPT-5.2-Codex | Väldefinierad, stabil |
| `citation_format`-generering | GPT-5.1-Codex-Mini | Enkel strängformatering |
| OCR av inscannad doktrin | Gemini Vision | Multimodal |
| Verifiering `citation_handler.py` | Sonnet 4.6 (steg 3) | Verifiering |

### FÖRARBETEN-pipeline

| Uppgift | Modell | Motivering |
|---|---|---|
| SOU-fetcher + LiU API | GPT-5.2-Codex | Väldefinierat REST-API |
| Riksdagen HTML-strip + prop-fetcher | GPT-5.2-Codex | Väldefinierat |
| Sektionsigenkänning rubrikmönster | GPT-5.3-Codex | Komplex regex + fallback |
| `legal_area`-klassificering (runtime) | Haiku 4.5 (API) | Låg kostnad, repetitivt |
| Verifiering + cross-collection-test | Sonnet 4.6 (steg 3) | Verifiering |

### SFS-pipeline

| Uppgift | Modell | Motivering |
|---|---|---|
| SFS-fetcher + konsoliderade versioner | GPT-5.2-Codex | Väldefinierat |
| Paragraf-chunking (§-gräns) | GPT-5.3-Codex | Komplex dokumentstruktur |
| Ikraftträdandedatum-hantering | GPT-5.2-Codex | Väldefinierat datumformat |
| Verifiering | Sonnet 4.6 (steg 3) | Verifiering |

### Guard-modul och RAG-lager

| Uppgift | Modell | Motivering |
|---|---|---|
| `norm_boost.py` viktningslogik | GPT-5.2 Thinking | Kräver reasoning om normhierarki och kantfall |
| `area_blocker.py` + `confidence_gate.py` | GPT-5.2-Codex | Väldefinierad logik |
| `disclaimer_injector.py` | GPT-5.1-Codex-Mini | Enkel textinjicering |
| Integrationstester | Sonnet 4.6 (steg 3) | Verifiering |

### Embedding-benchmark

| Uppgift | Modell | Motivering |
|---|---|---|
| Körning: E5-large vs KBLab | Lokalt Python | Ingen AI-kostnad |
| Analys recall@10 / precision@5 | Sonnet 4.6 (steg 3) | Analys + beslut |

---

## 4. Prompt-mall för delegation (Steg 1 → Steg 2)

```markdown
# §AI Implementation Task — {MODUL_NAMN}

## Kontext
Du implementerar en modul för §AI (paragrafen.ai), ett gratis
juridiskt AI-system för allmänheten i Sverige.

## Spec-dokument
[Bifoga relevant .md-fil, t.ex. FÖRARBETEN_PIPELINE.md avsnitt X]

## Uppgift
[Specificera exakt vad som ska implementeras]

## Schema att följa exakt
[Klistra in relevant chunk-schema från spec]

## Krav
- Namespace-konvention: [ange exakt konvention]
- Felhantering: graceful degradation; logga via noop-interface
- Inga hårdkodade värden: konfiguration från YAML-filer
- Returnera: [lista förväntade filer + testresultat]

## Kantfall
[Lista specifika edge cases från spec]

## Avvikelserapport
Om implementationen kräver avvikelse från spec: pausa och
rapportera INNAN du fortsätter. Ange: vad, varför, alternativ.
```

---

## 5. Verifieringschecklista (Steg 3)

```
□ Schema-konformitet: alla obligatoriska fält finns med rätt typ
□ Namespace-format: exakt konvention för source_type
□ authority_level: korrekt per source_type
□ legal_area: normaliserat mot legal_areas.yaml
□ priority_weight: beräknat, inte hårdkodat
□ Exkluderade områden: blockeras vid retrieval, indexeras vid ingest
□ Felhantering: inga nakna exceptions
□ Inga secrets i kod: API-nycklar via os.environ
□ Loggning: noop-interface anropat
□ Tester medföljer: smoke test + minst ett edge case per modul
```

---

## 6. Prioriterad implementationsordning

| Prio | Uppgift | Impl.-modell | Beroende av |
|---|---|---|---|
| 1 | PRAXIS Fas 3: HTML-chunking | GPT-5.3-Codex | F1–F3 ✅ |
| 2 | DOKTRIN: chunking-pipeline | GPT-5.2-Codex | D1–D4 ✅ |
| 3 | Embedding-benchmark | Lokalt Python | — |
| 4 | Chroma-setup | GPT-5.2-Codex | Benchmark |
| 5 | FÖRARBETEN: SOU + prop/Ds | GPT-5.3-Codex + 5.2-Codex | S1–S7 ✅ |
| 6 | SFS-pipeline | GPT-5.3-Codex | Chroma |
| 7 | Guard-modul | GPT-5.2 Thinking + 5.2-Codex | Chroma |
| 8 | RAG-lager + klarspråk | GPT-5.2 Thinking | Alla pipelines |
| 9 | QA: gold standard 50 frågor | Sonnet 4.6 | RAG-lager |

---

## 7. Uppdateringsrutin för modellmatrisen

Modelllandskapet förändras snabbt. Innan ny uppgiftstyp tilldelas modell:

1. Sök aktuell information (`web_search`) om tillgängliga modeller
2. Kontrollera om nya modeller lanserats eller gamla pensionerats
3. Uppdatera matrisen och dokumentera datumet
4. Versionsöka dokumentet

*Senast verifierad: 2026-02-24*

---

*Dokument: AI_WORKFLOW.md | §AI paragrafen.ai | v1.1 | 2026-02-24*
*Relaterade dokument: paragrafen-ai-architecture-v04.md | FÖRARBETEN_PIPELINE.md*
