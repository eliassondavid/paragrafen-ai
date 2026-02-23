# Pipeline Contracts — §AI

Alla pipeline-steg måste följa dessa kontrakt. QA verifierar mot dessa.

## Ingest-kontrakt

Varje inläst dokument måste innehålla:
- `id` (UUID)
- `type` (författning | praxis | förarbete | föreskrift | doktrin)
- `source_url` (primärkälla)
- `sha256` (innehållshash)
- `fetched_at` (ISO-timestamp)

## Norm-kontrakt

Normaliserat dokument måste innehålla allt från ingest-kontrakt plus:
- `norm_level` (grundlag|lag|förordning|föreskrift|prejudikat|förarbete|doktrin)
- `ikraft` (ISO-datum)
- `consolidation_source` ("rk" | "none")

## Doktrin-tillägg

Doktrin-dokument kräver dessutom:
- `author.name` (obligatoriskt)
- `work_title`
- `citation_format` (mall för citat med sida)

## Chunk-kontrakt

Varje chunk måste innehålla:
- `chunk_id`
- `doc_id` (referens till källdokument)
- `embedding_model` (modellnamn, för migration)
- `norm_level`
- `sfs_nr` (om tillämpligt)

## QA-krav

- ≥ 95 % källträff vid stickprov
- Hallucination-test varje sprint
- Disclaimer-injector verifieras per svar
