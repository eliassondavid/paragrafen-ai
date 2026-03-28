from __future__ import annotations


def _optional_block(label: str, value: str) -> str:
    cleaned = value.strip()
    if not cleaned:
        return ""
    return f"{label}:\n{cleaned}\n\n"


def build_identify_legal_issue_prompt(
    situation: str,
    advice_received: str,
    legal_area: str = "",
    advice_source: str = "",
) -> str:
    return (
        "Identifiera den centrala rättsfrågan i följande situation och råd. "
        "Svara med exakt en mening på klarspråk. "
        "Nämn aldrig namn på juristen, byrån eller organisationen som gav rådet.\n\n"
        f"{_optional_block('RÄTTSOMRÅDE', legal_area)}"
        f"{_optional_block('RÅDKÄLLA (om känd)', advice_source)}"
        f"SITUATION:\n{situation.strip()}\n\n"
        f"RÅDET SOM SKA GRANSKAS:\n{advice_received.strip()}"
    )


def build_led1_prompt(
    situation: str,
    advice_received: str,
    legal_issue: str,
    context: str,
    legal_area: str = "",
    advice_source: str = "",
) -> str:
    return f"""
UPPGIFT: Analysera följande juridiska råd objektivt.
Presentera steelman-argument FÖR och MOT rådet.

{_optional_block("RÄTTSOMRÅDE", legal_area)}{_optional_block("RÅDKÄLLA (om känd)", advice_source)}SITUATION:
{situation.strip()}

RÅDET SOM GAVS:
{advice_received.strip()}

IDENTIFIERAD RÄTTSFRÅGA:
{legal_issue.strip()}

RELEVANTA RÄTTSKÄLLOR:
{context}

SVARA I EXAKT DETTA JSON-FORMAT:
{{
    "legal_analysis": "Övergripande rättslig analys (3-5 meningar)",
    "strengths": [
        {{
            "description": "Vad i rådet som stöds av rättskällorna",
            "legal_ref": "Lagrum eller avgörande",
            "severity": "low | medium | high"
        }}
    ],
    "weaknesses": [
        {{
            "description": "Vad i rådet som saknar stöd eller motsägs",
            "legal_ref": "Lagrum eller avgörande",
            "severity": "low | medium | high"
        }}
    ],
    "gaps": [
        {{
            "description": "Viktig aspekt som rådet inte nämner",
            "legal_ref": "Lagrum eller avgörande",
            "severity": "low | medium | high"
        }}
    ],
    "overall_assessment": "korrekt | delvis_korrekt | tveksamt | felaktigt",
    "confidence": "high | medium | low"
}}

REGLER:
- Basera alla bedömningar på rättskällorna ovan
- Ange alltid källhänvisning för varje påstående
- Var balanserad och respektfull, steelman båda sidor
- Nämn aldrig namn på juristen, byrån eller organisationen som gav rådet
- Om du inte kan bedöma: säg det och sätt confidence till "low"
- Svara bara med JSON
""".strip()


def build_led2_prompt(
    situation: str,
    legal_issue: str,
    context: str,
    legal_area: str = "",
) -> str:
    return f"""
UPPGIFT: Bedöm den sannolika utgången om denna fråga prövades
i svensk domstol eller annan relevant instans.

{_optional_block("RÄTTSOMRÅDE", legal_area)}SITUATION:
{situation.strip()}

RÄTTSFRÅGA:
{legal_issue.strip()}

RELEVANTA RÄTTSKÄLLOR (inklusive underrättspraxis om tillgängligt):
{context}

SVARA I EXAKT DETTA JSON-FORMAT:
{{
    "outcome_prognosis": "Bedömning av sannolik utgång (3-5 meningar)",
    "burden_of_proof": "Vem har bevisbördan och vad krävs?",
    "practical_obstacles": "Tidsåtgång, kostnader, processuella hinder",
    "prognosis_level": "sannolikt_bifall | osäkert | sannolikt_avslag",
    "follow_up_questions": [
        "Fråga att ställa till sin jurist",
        "Fråga två"
    ]
}}

REGLER:
- Basera prognosen på hur rättstillämpare sannolikt bedömer frågan, inte bara på lagtext
- Beakta bevisbörda, beviskrav och processrisk
- Nämn praktiska hinder som kostnad, tid och bevisläge
- Om underlaget inte räcker: säg det tydligt
- Svara bara med JSON
""".strip()
