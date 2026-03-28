"""
Beslutsträd för hyresrättsärenden.

Systematiserar centrala delar av JB 12 kap. och identifierar:
- Vilken grund hyresvärden åberopar
- Om formkraven verkar uppfyllda
- Om rättelsemöjlighet finns
- Vilka tidsfrister som gäller
"""

from __future__ import annotations

from datetime import timedelta

from modules.hyresratt.models import (
    BristInfo,
    ForverkandeGrund,
    HyresgastInfo,
    HyreshojningsInfo,
    Tidsfrist,
    UpsagningsInfo,
    UpsagningsTyp,
)

FORVERKANDEGRUNDER = {
    ForverkandeGrund.OBETALD_HYRA: {
        "lagrum": "JB 12 kap. 42 § 1 st. 1 p.",
        "beskrivning": "Hyresgästen dröjer mer än en vecka med att betala hyran.",
        "rattelse_mojlig": True,
        "rattelse_frist_dagar": 21,
        "rattelse_lagrum": "JB 12 kap. 44 § 4 st.",
        "rattelse_beskrivning": (
            "Betala hela den förfallna hyran inom återvinningsfristen om tre veckor "
            "för att kunna återvinna hyresrätten."
        ),
        "socialnamnden": True,
        "allvarlighet": "hög",
        "vanliga_argument": [
            "Betalningen blev försenad på grund av sjukdom eller bankfel.",
            "Socialnämnden verkar inte ha underrättats på rätt sätt.",
            "Det rör sig inte om ett upprepat mönster av sena betalningar.",
        ],
    },
    ForverkandeGrund.STORNINGAR: {
        "lagrum": "JB 12 kap. 42 § 1 st. 9 p.",
        "beskrivning": "Hyresgästen påstås ha orsakat störningar i boendet.",
        "rattelse_mojlig": True,
        "rattelse_frist_dagar": None,
        "rattelse_lagrum": "JB 12 kap. 42 § 4 st.",
        "rattelse_beskrivning": (
            "Hyresvärden ska normalt först ge en tillsägelse. Om störningarna upphör "
            "är förverkande ofta inte aktuellt."
        ),
        "socialnamnden": True,
        "allvarlighet": "medel",
        "vanliga_argument": [
            "Störningarna är inte tillräckligt allvarliga eller varaktiga.",
            "Hyresvärden har inte visat att tillsägelse har lämnats.",
            "Det rörde sig om en enstaka händelse och problemen har upphört.",
        ],
    },
    ForverkandeGrund.OTILLATEN_ANDRAHAND: {
        "lagrum": "JB 12 kap. 42 § 1 st. 3 p.",
        "beskrivning": "Hyresgästen hyr ut lägenheten i andra hand utan tillstånd.",
        "rattelse_mojlig": True,
        "rattelse_frist_dagar": None,
        "rattelse_lagrum": "JB 12 kap. 43 §",
        "rattelse_beskrivning": (
            "Avsluta andrahandsuthyrningen snarast eller ansök om tillstånd om "
            "beaktansvärda skäl finns."
        ),
        "socialnamnden": False,
        "allvarlighet": "medel",
        "vanliga_argument": [
            "Det finns beaktansvärda skäl för upplåtelsen.",
            "Andrahandsuthyrningen har redan upphört.",
            "Ansökan om tillstånd har eller ska lämnas in till hyresnämnden.",
        ],
    },
    ForverkandeGrund.VANVARD: {
        "lagrum": "JB 12 kap. 42 § 1 st. 8 p.",
        "beskrivning": "Hyresvärden menar att lägenheten vanvårdas.",
        "rattelse_mojlig": True,
        "rattelse_frist_dagar": None,
        "rattelse_lagrum": "JB 12 kap. 43 §",
        "rattelse_beskrivning": "Åtgärda påstådda brister och dokumentera vad som gjorts.",
        "socialnamnden": False,
        "allvarlighet": "medel",
        "vanliga_argument": [
            "Skicket beror helt eller delvis på eftersatt underhåll från hyresvärden.",
            "Påstådda brister har redan åtgärdats.",
        ],
    },
    ForverkandeGrund.BROTTSLIG_VERKSAMHET: {
        "lagrum": "JB 12 kap. 42 § 1 st. 9 p.",
        "beskrivning": "Lägenheten påstås ha använts för brottslig verksamhet.",
        "rattelse_mojlig": False,
        "rattelse_frist_dagar": None,
        "rattelse_lagrum": "",
        "rattelse_beskrivning": "",
        "socialnamnden": True,
        "allvarlighet": "kritisk",
        "vanliga_argument": [
            "Det saknas tillräcklig utredning om vad som faktiskt har hänt.",
            "Hyresgästen kände inte till den påstådda brottsliga verksamheten.",
        ],
    },
    ForverkandeGrund.VAGRAN_TILLTRADE: {
        "lagrum": "JB 12 kap. 42 § 1 st. 6 p.",
        "beskrivning": "Hyresgästen vägrar ge hyresvärden tillträde.",
        "rattelse_mojlig": True,
        "rattelse_frist_dagar": None,
        "rattelse_lagrum": "JB 12 kap. 43 §",
        "rattelse_beskrivning": "Ge tillträde på rimlig tid och dokumentera kommunikationen.",
        "socialnamnden": False,
        "allvarlighet": "låg",
        "vanliga_argument": [
            "Hyresvärden har inte aviserat besöket i rimlig tid.",
            "Tillträde har redan lämnats eller erbjudits.",
        ],
    },
    ForverkandeGrund.OTILLATEN_INNEBOENDE: {
        "lagrum": "JB 12 kap. 41 § och 42 §",
        "beskrivning": "Hyresvärden invänder mot inneboende eller upplåtelseformen.",
        "rattelse_mojlig": True,
        "rattelse_frist_dagar": None,
        "rattelse_lagrum": "JB 12 kap. 43 §",
        "rattelse_beskrivning": (
            "Om upplåtelsen i praktiken motsvarar andrahandsuthyrning bör den avslutas "
            "eller klarläggas omedelbart."
        ),
        "socialnamnden": False,
        "allvarlighet": "låg",
        "vanliga_argument": [
            "Inneboende är normalt tillåtet när hyresgästen själv bor kvar.",
            "Upplåtelsen motsvarar inte andrahandsuthyrning.",
        ],
    },
    ForverkandeGrund.ANNAN: {
        "lagrum": "JB 12 kap. 42-46 §§",
        "beskrivning": "Grunden behöver preciseras närmare innan den kan bedömas.",
        "rattelse_mojlig": False,
        "rattelse_frist_dagar": None,
        "rattelse_lagrum": "",
        "rattelse_beskrivning": "",
        "socialnamnden": False,
        "allvarlighet": "okänd",
        "vanliga_argument": [
            "Hyresvärden behöver ange vilken konkret grund som åberopas.",
            "Bevisning och formkrav måste granskas närmare.",
        ],
    },
}


def analysera_uppsagning(
    uppsagning: UpsagningsInfo,
    hyresgast: HyresgastInfo,
) -> dict:
    """Analysera uppsägning och returnera en regelbaserad bedömning."""

    resultat = {
        "grund_info": None,
        "rattelse_mojlig": False,
        "tidsfrister": [],
        "bedomning": "medel",
        "argument_for_hyresgast": [],
        "varningar": [],
    }

    if hyresgast.bostadstyp.lower() == "lokal":
        resultat["varningar"].append(
            "Lokalhyra följer andra regler än bostadshyra och stöds inte i denna MVP."
        )

    if not uppsagning.skriftlig:
        resultat["varningar"].append(
            "Uppsägningen verkar inte vara skriftlig. Uppsägning av bostadshyra ska "
            "som huvudregel vara skriftlig enligt JB 12 kap. 8 §."
        )
        resultat["bedomning"] = "stark_position"

    if uppsagning.grund and uppsagning.grund in FORVERKANDEGRUNDER:
        grund = FORVERKANDEGRUNDER[uppsagning.grund]
        resultat["grund_info"] = grund
        resultat["rattelse_mojlig"] = bool(grund["rattelse_mojlig"])
        resultat["argument_for_hyresgast"] = list(grund["vanliga_argument"])

        if uppsagning.grund == ForverkandeGrund.OBETALD_HYRA:
            if uppsagning.datum_mottagen:
                deadline = uppsagning.datum_mottagen + timedelta(days=21)
                resultat["tidsfrister"].append(
                    Tidsfrist(
                        namn="Återvinningsfrist",
                        deadline=deadline,
                        atgard="Betala hela den förfallna hyran och spara kvitto.",
                        lagrum="JB 12 kap. 44 § 4 st.",
                        prioritet="kritisk",
                    )
                )
            else:
                resultat["varningar"].append(
                    "Datum för mottagen uppsägning saknas. Återvinningsfristen kan inte "
                    "räknas exakt utan detta datum."
                )

        if uppsagning.grund == ForverkandeGrund.OTILLATEN_INNEBOENDE:
            resultat["bedomning"] = "stark_position"
        elif grund["rattelse_mojlig"]:
            resultat["bedomning"] = "medel"
            if (
                uppsagning.grund == ForverkandeGrund.OBETALD_HYRA
                and hyresgast.antal_forseningar_12man <= 1
            ):
                resultat["bedomning"] = "stark_position"
        else:
            resultat["bedomning"] = "svag_position"

    if uppsagning.typ == UpsagningsTyp.UPPSAGNING_MED_TID:
        resultat["varningar"].append(
            "Vid uppsägning med uppsägningstid har bostadshyresgäster normalt "
            "besittningsskydd enligt JB 12 kap. 46 §."
        )
        resultat["bedomning"] = "stark_position"

    return resultat


def analysera_hyreshojning(info: HyreshojningsInfo) -> dict:
    """Analysera föreslagen hyreshöjning mot bruksvärdesprincipen."""

    hojning_procent = info.hojning_procent
    if info.nuvarande_hyra > 0 and info.foreslagen_hyra > 0:
        hojning_procent = (
            (info.foreslagen_hyra - info.nuvarande_hyra) / info.nuvarande_hyra * 100
        )

    resultat = {
        "hojning_procent": hojning_procent,
        "bedomning": "medel",
        "argument": [],
        "varningar": [],
        "tidsfrister": [],
    }

    if hojning_procent > 10:
        resultat["bedomning"] = "stark_position"
        resultat["argument"].append(
            f"Höjningen på {hojning_procent:.1f}% är betydande och bör jämföras mot "
            "bruksvärdet för likvärdiga lägenheter enligt JB 12 kap. 55 §."
        )
    elif hojning_procent > 5:
        resultat["bedomning"] = "medel"
        resultat["argument"].append(
            "Höjningen kan vara möjlig, men den bör jämföras med hyran för "
            "likvärdiga lägenheter i området."
        )
    else:
        resultat["bedomning"] = "svag_position"
        resultat["argument"].append(
            "Höjningen är relativt begränsad och kan vara svårare att angripa utan "
            "starkt jämförelsematerial."
        )

    if info.forhandlingsordning:
        resultat["varningar"].append(
            "Det finns förhandlingsordning. Kontakta Hyresgästföreningen omgående."
        )

    return resultat


def analysera_brist(info: BristInfo) -> dict:
    """Analysera brist i lägenheten och nästa processteg."""

    resultat = {
        "bedomning": "medel",
        "atgarder": [],
        "varningar": [],
        "tidsfrister": [],
    }

    allvarliga_bristtyper = {"fukt", "mögel", "skadedjur"}
    if info.typ in allvarliga_bristtyper:
        resultat["varningar"].append(
            "Dokumentera bristen med foto, datum och eventuell påverkan på hälsa eller boende."
        )

    if not info.anmald_till_hyresvard:
        resultat["atgarder"].append(
            "Anmäl bristen skriftligt till hyresvärden och sätt en rimlig tidsfrist "
            "på 2-4 veckor."
        )
        resultat["atgarder"].append(
            "Spara e-post, brev, bilder och annan dokumentation som visar när du reklamerat."
        )
        return resultat

    if not info.hyresvard_agerat:
        resultat["bedomning"] = "stark_position"
        resultat["atgarder"].append(
            "Ansök hos hyresnämnden om föreläggande att hyresvärden ska åtgärda bristen."
        )
        resultat["atgarder"].append(
            "Överväg att begära hyresnedsättning för den tid bristen kvarstår enligt JB 12 kap. 16 §."
        )
        if info.anmald_datum:
            resultat["tidsfrister"].append(
                Tidsfrist(
                    namn="Följ upp reklamationen",
                    deadline=info.anmald_datum + timedelta(days=28),
                    atgard="Påminn hyresvärden eller gå vidare till hyresnämnden.",
                    lagrum="JB 12 kap. 11 § och 16 §",
                    prioritet="viktig",
                )
            )
    else:
        resultat["atgarder"].append(
            "Följ upp skriftligt om åtgärden blev tillräcklig och dokumentera kvarstående fel."
        )

    return resultat
