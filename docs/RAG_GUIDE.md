# RAG Guide — §AI

## Retrieval-Augmented Generation för svensk juridik

### Pipeline-översikt

```
Fråga → Klassificering → [Block?] → Embedding → Retrieval → Rerank → LLM → Disclaimer → Svar
```

### Steg 1: Fråge-klassificering
`guard/area_classifier.py` avgör rättsområde.
Om exkluderat område → `guard/area_blocker.py` returnerar hänvisning.

### Steg 2: Embedding
Frågan embedas med samma modell som indexet (se `config/embedding_config.yaml`).

### Steg 3: Retrieval
Chroma-sökning med metadata-filtrering på `norm_level` och `ikraft`.
Top-k = 10 kandidater.

### Steg 4: Normhierarki-boost
`index/norm_boost.py` viktar upp grundlag > lag > förordning > prejudikat.

### Steg 5: LLM-svar
Format: **Fakta → Rättsregel → Bedömning → Slutsats**
Krav: ≥ 2 verifierbara källor per påstående.

### Steg 6: Disclaimer-injektion
`publish/disclaimer_injector.py` lägger till fotnot med datum och källreferenser.

## Embedding-benchmark

Se `config/embedding_config.yaml` för modellval.
Benchmark: 200 juridiska frågor × 500 chunks, mät recall@10 och precision@5.
