# QA Playbook — §AI

## Mål

- ≥ 95 % källträff vid stickprov
- 0 hallucineringar i gold standard
- Alla exkluderade rättsområden blockeras korrekt

## Testsviter

### 1. Gold Standard (50 frågor)
Fil: `qa/gold_standard.json`
Kör: `pytest qa/hallucination_test.py`

### 2. Excluded Area Test
Verifierar att straffrätt, asyl, skatt och VBU blockeras.
Kör: `pytest qa/excluded_area_test.py`

### 3. Klarspråk-test
Läsbarhetskontroll — Läsbarhetsindex (LIX) ≤ 40 för allmänhetsvar.
Kör: `pytest qa/klarsprak_test.py`

### 4. Schema-validering
Validerar alla dokument mot schemas/*.json.
Kör: `pytest qa/schema_validator.py`

## Sprint-rutin

1. Kör alla testsviter
2. Granska qa_reporter output
3. Åtgärda eventuella hallucineringar före release
4. Logga resultat i `qa/reports/`
