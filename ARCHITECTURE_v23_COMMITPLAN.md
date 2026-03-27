# §AI Arkitektur v0.23 + Commit-plan + Startpromptar
## Version 0.23 — 2026-03-27
### Upprättad av: Överprojektet §AI (Opus 4.6, systemarkitekt)

---

## DEL 1: GIT COMMIT-PLAN (godkänd av Opus 4.6)

### Princip
Committa i logiska grupper. Inga loggfiler, inga temporära skript,
inga CSV-filer i repot. Använd `.gitignore` för att permanent exkludera.

### Steg 1: Uppdatera .gitignore FÖRST

```bash
cd /Users/davideliasson/Projects/paragrafen-ai

cat >> .gitignore << 'EOF'

# === Loggfiler ===
*.log
logs/

# === Temporära data ===
*.csv
chroma_baseline.txt
chroma_db/
data/chroma/

# === Tillfälliga skript (ej del av pipelinen) ===
diag_normalize.py
patch_sfs_*.py
post_normalize.py
build_curated_prop.py

# === Checkpoints (återskapas vid körning) ===
data/state/checkpoints/

# === OS ===
.DS_Store
__pycache__/
*.pyc
EOF

git add .gitignore
git commit -m "chore: uppdatera .gitignore — exkludera loggar, CSV, temporära skript"
```

### Steg 2: Committa konfigurationsändringar

```bash
git add config/embedding_config.yaml config/forarbete_rank.yaml \
        config/legal_areas.yaml config/sources.yaml \
        config/nja_ii_config.yaml
git commit -m "config: uppdaterad embedding-config (separata instanser), legal_areas, sources, NJA II"
```

### Steg 3: Committa pipeline-moduler (ingest)

```bash
git add ingest/dir_fetcher.py ingest/rskr_fetcher.py \
        ingest/jo_fetcher.py ingest/jo_converter.py \
        ingest/jk_fetcher.py \
        ingest/arn_converter.py \
        ingest/socialstyrelsen_fetcher.py ingest/socialstyrelsen_converter.py \
        ingest/fk_foreskrift_fetcher.py
git commit -m "feat(ingest): dir, rskr, JO, JK, ARN, SoS, FK fetchers och converters"
```

### Steg 4: Committa normaliserare

```bash
git add normalize/doktrin_normalizer.py \
        normalize/praxis_models.py normalize/praxis_naming.py \
        normalize/praxis_normalizer.py \
        normalize/sou_normalizer.py normalize/sou_parser.py \
        normalize/prop_normalizer.py normalize/prop_parser.py \
        normalize/sfs_chunker.py \
        normalize/run_curated_props.py \
        normalize/config/
git commit -m "feat(normalize): doktrin, praxis, SOU, prop, SFS normaliserare"
```

### Steg 5: Committa indexerare

```bash
git add index/__init__.py \
        index/prop_bulk_indexer.py index/sou_bulk_indexer.py \
        index/ds_bulk_indexer.py index/bet_bulk_indexer.py \
        index/dir_bulk_indexer.py index/rskr_bulk_indexer.py \
        index/praxis_indexer.py index/doktrin_indexer.py \
        index/arn_bulk_indexer.py \
        index/jo_bulk_indexer.py index/jk_bulk_indexer.py \
        index/socialstyrelsen_bulk_indexer.py \
        index/fk_foreskrift_bulk_indexer.py \
        index/prop_indexer.py index/sfs_indexer.py index/sou_indexer.py \
        index/vector_store.py
git commit -m "feat(index): alla bulk-indexerare + refaktorerad vector_store"
```

### Steg 6: Committa guard-modul

```bash
git add guard/
git commit -m "feat(guard): area_blocker, norm_boost, confidence_gate, disclaimer_injector"
```

### Steg 7: Committa tester

```bash
git add tests/ conftest.py
git commit -m "test: guard, praxis, doktrin, prop, JO, JK, ARN, SoS tester"
```

### Steg 8: Committa skript och NJA II

```bash
git add scripts/ingest_nja_ii.py scripts/verify_nja_ii_ingest.py \
        scripts/enrich_citations.py scripts/ingest_curated_praxis.py \
        scripts/norm_boost_update.py \
        data/curated/nja_ii/ data/curated/nja_names.json
git commit -m "feat(nja_ii): NJA II-ingest + citation enrichment + curaterat material"
```

### Steg 9: Committa RAG-pipeline + pipelines

```bash
git add rag/ pipelines/
git commit -m "feat(rag): RAG-pipeline grundstruktur + pipeline-orchestrering"
```

### Steg 10: Committa docs + statusrapporter

```bash
git add docs/ scripts/STATUSRAPPORT_UPPDRAG_B.md \
        STATUSRAPPORT_socialstyrelsen_20260322.md \
        paragrafen-ai-architecture-v22.md
git commit -m "docs: arkitektur v22, statusrapporter"
```

### Steg 11: Push

```bash
git push origin main
```

### Filer som INTE ska committas (temporära)

```
diag_normalize.py          — diagnostikskript
patch_sfs_chunker.py       — engångspatch
patch_sfs_pipeline.py      — engångspatch
post_normalize.py           — engångsskript
build_curated_prop.py       — engångsskript
chroma_baseline.txt         — temporär
sou_c_prefix.csv            — temporär
ds_stubs.csv                — temporär
data/skipped_index_review.csv — temporär
```

Dessa fångas av `.gitignore` (steg 1).

### Verifiering efter push

```bash
git status  # Ska visa "clean" eller bara ignorerade filer
git log --oneline -10  # Verifiera commit-historik
```

---

## DEL 2: ARKITEKTURBESLUT v0.23

### AD5: Modultoggel (NYTT)

Demo-appen ska stödja att moduler kan aktiveras/deaktiveras individuellt.

```yaml
# config/modules.yaml
modules:
  allman_assistent:
    enabled: true
    label: "Juridisk assistent"
    icon: "💬"
    rag_module: "allman"

  framtidsfullmakt:
    enabled: true        # Sätt false för att dölja
    label: "Framtidsfullmakt"
    icon: "📄"
    rag_module: "framtidsfullmakt"
    requires_api_key: true   # Kräver LLM för chatbot

  upphandling:
    enabled: false       # Dölj tills KV-data är integrerad
    label: "Upphandling"
    icon: "🏛️"
    rag_module: "upphandling"
    requires_api_key: true
```

Implementation i demo/app.py:

```python
import yaml

def load_enabled_modules():
    with open("config/modules.yaml") as f:
        config = yaml.safe_load(f)
    return {k: v for k, v in config["modules"].items() if v.get("enabled", True)}

modules = load_enabled_modules()

for key, mod in modules.items():
    with cols[i]:
        st.markdown(f"### {mod['icon']} {mod['label']}")
        if mod.get("requires_api_key") and not os.environ.get("ANTHROPIC_API_KEY"):
            st.warning("Kräver API-nyckel")
        if st.button("Starta", key=key):
            st.switch_page(f"pages/{key}.py")
```

Det ger dig full kontroll: visa bara framtidsfullmakt för investerare,
lägg till upphandling när den är redo, etc.

### AD6: Upphandling-pipeline — separat repo, delad Chroma (NYTT)

KV-pipelinen ligger i `/Users/davideliasson/Projects/Upphandling-pipeline`
med 126 340 chunks i `paragrafen_upphandling_v1`. RAG-lagret i paragrafen-ai
behöver kunna läsa denna collection.

Två alternativ:

**Alt A: Symlink.** Skapa symlink från paragrafen-ai till upphandlings-Chroma:
```bash
ln -s /Users/davideliasson/Projects/Upphandling-pipeline/data/chroma \
      /Users/davideliasson/Projects/paragrafen-ai/data/index/chroma/upphandling
```

**Alt B: Konfigurera extern sökväg i rag_config.yaml:**
```yaml
# I config/rag_config.yaml
chroma_paths:
  default: "data/index/chroma"
  upphandling: "/Users/davideliasson/Projects/Upphandling-pipeline/data/chroma"
```

**BESLUT: Alt B.** Explicit konfiguration är tydligare än symlinks.
`ChromaClientPool` utökas med stöd för externa sökvägar:

```python
class ChromaClientPool:
    def __init__(self, config: dict):
        self.default_base = Path(config.get("default", "data/index/chroma"))
        self.overrides = config.get("chroma_paths", {})  # instance → absolut sökväg
    
    def _get_path(self, instance_key: str) -> Path:
        if instance_key in self.overrides:
            return Path(self.overrides[instance_key])
        return self.default_base / instance_key
```

### AD7: Guard API-harmonisering (NYTT)

Två guard-implementationer existerar:
1. `paragrafen-ai/guard/` — 24 befintliga tester passerar
2. `Guard-module/` — 18 tester, annat API

**BESLUT:** Behåll paragrafen-ai:s guard som master.
RAG-lagret (R-7) bygger adapter mot detta API, inte Guard-module:s.
Guard-module-testerna behöver INTE passera i paragrafen-ai — de
tillhör ett annat projekt.

Codex ska ta bort de kopierade testerna från Guard-module
som inte matchar:
```bash
# Ta bort inkompatibla Guard-module-tester
rm -rf /Users/davideliasson/Projects/paragrafen-ai/tests/guard/
# Behåll befintliga tester:
# tests/test_f7a_guard.py, test_f7b_norm_boost.py, test_f7c_disclaimer.py
```

---

## DEL 3: CHROMA-TABELL v0.23 (UPPDATERAD)

| Collection | Status | Chunks | Repo |
|------------|--------|--------|------|
| `paragrafen_prop_v1` | ✅ | 1 237 570 | paragrafen-ai |
| `paragrafen_sou_v1` | ✅ | 1 127 413 | paragrafen-ai |
| `paragrafen_bet_v1` | ✅ | 1 075 003 | paragrafen-ai |
| `paragrafen_praxis_v1` | ✅ | 309 351 | paragrafen-ai |
| `paragrafen_doktrin_v1` | ✅ | 235 024 | paragrafen-ai |
| `paragrafen_ds_v1` | ✅ | 180 099 | paragrafen-ai |
| `paragrafen_sfs_v1` | ✅ | 139 622 | paragrafen-ai |
| **`paragrafen_upphandling_v1`** | **✅** | **126 340** | **Upphandling-pipeline** |
| `paragrafen_riksdag_v1` | ✅ | 49 352 | paragrafen-ai |
| `paragrafen_jo_v1` | ✅ | 38 922 | paragrafen-ai |
| `paragrafen_jk_v1` | ✅ | 11 802 | paragrafen-ai |
| `paragrafen_foreskrift_v1` | ✅ | 6 460 | paragrafen-ai |
| `paragrafen_namnder_v1` | ✅ | 2 809 | paragrafen-ai |
| **Totalt indexerat** | | **4 539 767** | |

### Pågående / planerad

| Collection | Status | Repo |
|------------|--------|------|
| `paragrafen_fmakt_v1` | 🔜 Planerad | paragrafen-ai |
| Dir komplettering (~1 230) | 🔜 API-beroende | paragrafen-ai |
| Rskr komplettering (~30 700) | 🔜 API-beroende | paragrafen-ai |

---

## DEL 4: STARTPROMPTAR FÖR SONNET 4.6

Varje startprompt är redo att klistra in i en ny chatt i
paragrafen.ai-projektet. Sonnet 4.6 leder implementation
och delegerar till Codex.

---

### STARTPROMPT 1: RAG-lager + Demo-UI

```markdown
# §AI Delprojekt: RAG-lager + Demo-UI
## Led av: Sonnet 4.6 | Implementation: GPT-5.4 (Codex)
## Spec: RAG_DEMO_SPEC_v1.0.md

Du leder implementationen av §AI:s RAG-lager och Streamlit-demo.

## Kontext
§AI (paragrafen.ai) är ett gratis juridiskt AI-system med ~4.5M
indexerade chunks i 13 ChromaDB-collections. Du bygger den delade
retrieval-infrastruktur som alla moduler använder.

## Spec-dokument
Se RAG_DEMO_SPEC_v1.0.md (bifogad i projektet).

## Implementationsordning
R-1 → R-12 (se specen). Delegera varje steg till Codex.

## Arkitekturbeslut att följa
- AD5: Modultoggel via config/modules.yaml
- AD6: Upphandling-Chroma extern sökväg i config
- AD7: Guard-adapter mot paragrafen-ai:s befintliga guard API

## Dina ansvarsområden
1. Skriva implementationspromptar för Codex (steg 2)
2. Verifiera Codex resultat (steg 3)
3. Eskalera till Opus 4.6 vid arkitekturfrågor

## Eskalera till Opus 4.6 om:
- Guard-pipelinen kräver betydande omskrivning
- ChromaClientPool har prestandaproblem vid 4+ collections
- LLM-modellval vid runtime (Sonnet vs Haiku vs lokalt)
- Val som påverkar andra moduler

## Första uppgift
Skriv implementationsprompt för R-1 (rag/models.py) och R-2
(rag/chroma_pool.py) och delegera till Codex.
```

---

### STARTPROMPT 2: Framtidsfullmakt

```markdown
# §AI Delprojekt: Framtidsfullmaktsmodul
## Led av: Sonnet 4.6 | Implementation: GPT-5.4 (Codex)
## Spec: FRAMTIDSFULLMAKT_SPEC_v1.0.md

Du leder implementationen av §AI:s framtidsfullmaktsmodul.

## Kontext
Betaltjänst med två funktioner:
A) Upprätta framtidsfullmakt (chatbot + formulär → PDF/docx)
B) Granska befintlig framtidsfullmakt (uppladdning → rapport)

## Beroenden
- RAG-lagret (rag/rag_query.py) måste finnas — om det inte
  är klart ännu, implementera modulen med en mock-RAG.
- Materialet (Davids artiklar etc.) är under insamling.
  Implementera ingest-pipelinen med placeholder-data först.

## Implementationsordning
F-1 → F-10 (se specen). F-1 (materialinventering) väntar på David.
Börja med F-3 (question_flow.py) som är oberoende av material.

## Eskalera till Opus 4.6 om:
- Frågeflödet behöver juridiska bedömningar utöver spec
- Val av LLM-modell för fältigenkänning
- Intäktsmodell/betalningsintegration
```

---

### STARTPROMPT 3: Upphandlingsmodul

```markdown
# §AI Delprojekt: Upphandlingsmodul (fas 1–3)
## Led av: Sonnet 4.6 | Implementation: GPT-5.4 (Codex)
## Spec: UPPHANDLING_SPEC_v1.0.md

Du leder implementationen av §AI:s upphandlingsmodul.

## Kontext
Tre faser: HITTA → ANALYSERA → ÖVERPRÖVA.
KV-databasen finns i separat repo med 126 340 chunks.
Chroma-sökväg: /Users/davideliasson/Projects/Upphandling-pipeline/data/chroma

## Beroenden
- RAG-lagret (rag/rag_query.py) med AD6 (extern Chroma-sökväg)
- KV-data redan indexerad (126 340 chunks)
- TED API kräver ingen autentisering för sökning

## Implementationsordning
U-1 → U-15 (se specen). Börja med U-1 (ted_client.py) som
är oberoende.

## KRITISKT: Dataisolering (AD3)
Användardata MÅSTE isoleras. Se spec avsnitt 3.2.
Implementera UserSession tidigt (U-10 kan göras parallellt).

## Eskalera till Opus 4.6 om:
- e-Avrop blockerar scraping
- TED API:t har ändrat struktur
- Anomalitaxonomi behöver justering efter KV-praxis-analys
```

---

### STARTPROMPT 4: Git städning + commit

```markdown
# §AI Delprojekt: Git commit + städning
## Utför: Codex GPT-5.4 (direkt, ej via Sonnet)

## Kontext
paragrafen-ai repot har 121 dirty entries. Opus 4.6 har
godkänt commit-planen i ARCHITECTURE_v23_COMMITPLAN.md.

## Uppgift
Följ commit-planen exakt i den ordning den anges (steg 1–11).
Steg 1 (.gitignore) MÅSTE komma först.

## Innan du kör:
1. Kontrollera att alla filer i steg 3–10 faktiskt existerar
2. Om en fil inte finns, hoppa över den (logga som SKIP)
3. Committa INTE filer som inte finns i planen

## Efter push:
Rapportera: git log --oneline -12 + git status

## Guard-tester:
Ta bort tests/guard/ (inkompatibla Guard-module-tester).
Behåll tests/test_f7a_guard.py, test_f7b_norm_boost.py,
test_f7c_disclaimer.py — de tillhör paragrafen-ai:s guard.
```

---

## DEL 5: BESLUTSSTATUS v0.23

| ID | Beslut | Status | Datum |
|----|--------|--------|-------|
| **AD7** | **Guard: paragrafen-ai:s guard är master. Guard-module-tester tas bort.** | **✅ BESLUTAT** | **2026-03-27** |
| **AD6** | **KV-Chroma extern sökväg i rag_config.yaml. Ingen symlink.** | **✅ BESLUTAT** | **2026-03-27** |
| **AD5** | **Modultoggel: config/modules.yaml styr vilka moduler som visas i demo.** | **✅ BESLUTAT** | **2026-03-27** |
| AD4 | Versionshantering FFU | ✅ | 2026-03-27 |
| AD3 | Dataisolering: user_id + sessionmappar | ✅ | 2026-03-27 |
| AD2 | Datakällor per upphandlingsfas | ✅ | 2026-03-27 |
| AD1 | Hybridmodell framtidsfullmakt | ✅ | 2026-03-27 |
| AC1–AC3 | Se v0.22 | ✅/🔄 | 2026-03-23 |
| AB1–AB4 | Se v0.21 | ✅/🔜 | 2026-03-22 |

---

*Dokument: ARCHITECTURE_v23_COMMITPLAN.md | §AI paragrafen.ai | v0.23 | 2026-03-27*
*Relaterade: RAG_DEMO_SPEC_v1.0.md, FRAMTIDSFULLMAKT_SPEC_v1.0.md, UPPHANDLING_SPEC_v1.0.md*
