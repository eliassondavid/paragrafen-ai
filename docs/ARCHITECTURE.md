# §AI (paragrafen.ai) — Systemarkitektur v0.3
## Uppdaterad med arkitekturbeslut 2026-02-23

Se [paragrafen-ai-architecture-v03.md](../paragrafen-ai-architecture-v03.md) för fullständigt arkitekturdokument.

---

## Snabböversikt

| Komponent | Val | Motivering |
|-----------|-----|-----------|
| VektorDB | Chroma (lokal) → Qdrant (skalning) | Gratis, Python-native, migreringsbar |
| Embeddings | intfloat/multilingual-e5-large | Testad på nordiska språk, gratis, lokal |
| Konsolidering | RK:s konsoliderade texter | Auktoritativ källa, undviker merge-fel |
| Drift | Docker-compose lokalt | Inga molnberoenden i kärnan |
| Uppdatering | Daglig cron + RSS-diff | Automatisk, spårbar |

## Normhierarki

1. Grundlag
2. Lag
3. Förordning
4. Föreskrifter
5. Prejudikat (HD/HFD)
6. Förarbeten
7. Sedvana / doktrin

## Metod

Ordalydelse → lex specialis → normhierarki → förarbeten/praxis/systematik → doktrin

Output: **Fakta → Rättsregel → Bedömning → Slutsats**
