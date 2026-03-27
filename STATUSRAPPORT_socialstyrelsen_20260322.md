# Statusrapport — Socialstyrelsen-pipeline

Datum: 2026-03-22
Mottagare: Sonnet 4.6
Projekt: `paragrafen-ai`

## Genomfört

Följande filer har implementerats:

- `ingest/socialstyrelsen_fetcher.py`
- `ingest/socialstyrelsen_converter.py`
- `index/socialstyrelsen_bulk_indexer.py`

Pipelinen har verifierats stegvis enligt prompten:

1. `python3 ingest/socialstyrelsen_fetcher.py --max-docs 10 --verbose`
2. `python3 ingest/socialstyrelsen_converter.py --max-docs 10 --verbose`
3. `python3 index/socialstyrelsen_bulk_indexer.py --max-docs 5 --dry-run --verbose`

Verifieringsresultat:

- fetcher-test: 10 poster i `data/raw/socialstyrelsen/catalog.json`
- fetcher-test: 10 PDF:er i `data/raw/socialstyrelsen/pdf/`
- converter-test: 10 JSON-filer i `data/raw/socialstyrelsen/json/`
- indexer dry-run: FK-kontroll passerade, collection-count före dry-run var `2002`

## Full körning

Två fullkörningar genomfördes:

### 1. Foreskrifter-only

Körning:

- `python3 ingest/socialstyrelsen_fetcher.py --only-foreskrifter`
- `python3 ingest/socialstyrelsen_converter.py`
- `python3 index/socialstyrelsen_bulk_indexer.py`

Resultat:

- `412` föreskriftsdokument hämtade
- `412` JSON-filer konverterade
- `4458` chunks indexerade
- collection `paragrafen_foreskrift_v1` ökade från `2002` till `6460`

### 2. Hela katalogen

Körning:

- `python3 ingest/socialstyrelsen_fetcher.py`
- `python3 ingest/socialstyrelsen_converter.py`
- `python3 index/socialstyrelsen_bulk_indexer.py`

Slutläge efter full katalogkörning:

- `3808` katalogposter
- `3755` PDF-filer hämtade
- `53` poster i `fetch_errors.jsonl`
- `412` föreskrifts-JSON-filer
- `0` poster i `convert_errors.jsonl`
- collection `paragrafen_foreskrift_v1` kvar på `6460`

## Avvikelse från spec

En begränsad, motiverad avvikelse gjordes för testkörningar med `--max-docs`.

Bakgrund:

- De första 10 publikationerna på socialstyrelsen.se den 2026-03-22 var inte föreskrifter.
- Det gjorde att specens steg 2, `converter --max-docs 10`, annars skulle ge `0` konverterbara dokument.

Åtgärd:

- I fetchern prioriteras `SOSFS` och `HSLF-FS` först endast när `--max-docs` används utan `--only-foreskrifter`.
- Full körning utan `--max-docs` följer fortfarande hela katalogen om `3808` poster.

## Efterarbete / förbättring

Indexeraren var initialt funktionellt korrekt men inte återkörningssäker i sin slutassertion:

- `upsert()` mot redan indexerade chunk-id:n gav `Adderade chunks: 0`
- detta utlöste tidigare `AssertionError`

Detta har nu justerats:

- återkörning med redan befintliga chunk-id:n räknas som giltig idempotent körning
- indexeraren filtrerar nu bort existerande chunk-id:n före embedding
- omkörningar blir därför betydligt snabbare och laddar inte embedding-modellen när allt redan finns

Verifierad omkörning:

- `python3 index/socialstyrelsen_bulk_indexer.py --max-docs 5`
- resultat: `chunks_prepared=43`, `chunks_upserted=0`, `chunks_skipped_existing=43`

## Bedömning

Implementationen är klar och användbar i nuvarande form.

Det som faktiskt finns inläst i `paragrafen_foreskrift_v1` från Socialstyrelsen är:

- alla konverterbara föreskrifter som identifierats via `SOSFS` och `HSLF-FS`
- totalt `412` dokument och `4458` chunks

Det som inte indexeras är övriga publikationer (`doc_type == "other"`), i enlighet med promptens regel att dokumenttypen filtreras i konverteraren.

## Viktiga loggar och artefakter

- `logs/sos_fetch.log`
- `logs/sos_convert.log`
- `logs/sos_index.log`
- `logs/sos_all_fetch.log`
- `logs/sos_all_convert.log`
- `logs/sos_all_index.log`
- `logs/sos_all_index_rerun.log`
- `data/raw/socialstyrelsen/catalog.json`
- `data/raw/socialstyrelsen/fetch_errors.jsonl`
- `data/raw/socialstyrelsen/convert_errors.jsonl`

