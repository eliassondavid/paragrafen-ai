"""
Genererar juridiska brev för hyresrättssituationer.

Alla brev är mallbaserade för att ge tydliga och förutsebara formuleringar.
"""

from __future__ import annotations

from datetime import date

from modules.hyresratt.models import (
    BristInfo,
    ForverkandeGrund,
    HyresgastInfo,
    HyreshojningsInfo,
    UpsagningsInfo,
)


def _format_sek(amount: float) -> str:
    return f"{amount:,.0f}".replace(",", " ") + " kr"


def _format_received_date(value: date | None) -> str:
    return value.isoformat() if value else "okänt datum"


def generera_bestridandebrev(
    hyresgast: HyresgastInfo,
    uppsagning: UpsagningsInfo,
    argument: list[str],
) -> str:
    """Generera bestridandebrev vid uppsägning."""

    datum = date.today().isoformat()
    mottaget = _format_received_date(uppsagning.datum_mottagen)
    bostad = hyresgast.bostadstyp or "bostaden"

    brev = f"""
{hyresgast.namn}
{datum}

Till hyresvärden

BESTRIDANDE AV UPPSÄGNING

Jag har den {mottaget} tagit emot er uppsägning av mitt hyresavtal avseende {bostad}.

Jag bestrider härmed uppsägningen och meddelar att jag inte avser att flytta från
lägenheten.
"""

    if uppsagning.grund == ForverkandeGrund.OBETALD_HYRA:
        brev += """

Avseende er grund för uppsägningen, utebliven hyresbetalning, vill jag anföra följande:
"""
        for argument_text in argument:
            brev += f"- {argument_text}\n"
        brev += """

Jag avser att betala den utestående hyran inom den lagstadgade återvinningsfristen
om tre veckor enligt JB 12 kap. 44 § 4 st.
"""
    elif uppsagning.grund == ForverkandeGrund.STORNINGAR:
        brev += """

Avseende påstådda störningar vill jag anföra följande:
"""
        for argument_text in argument:
            brev += f"- {argument_text}\n"
        brev += """

Jag bestrider att omständigheterna är sådana att hyresrätten kan anses förverkad.
"""
    elif uppsagning.grund == ForverkandeGrund.OTILLATEN_ANDRAHAND:
        brev += """

Avseende påstådd otillåten andrahandsuthyrning vill jag anföra följande:
"""
        for argument_text in argument:
            brev += f"- {argument_text}\n"
    else:
        brev += """

Jag anser att uppsägningen saknar tillräckligt angiven och giltig grund.
"""
        for argument_text in argument:
            brev += f"- {argument_text}\n"

    brev += f"""

Om ni vidhåller uppsägningen har ni att hänskjuta ärendet till hyresnämnden för
prövning. Jag har rätt att bo kvar till dess att frågan är slutligt avgjord.

Med vänlig hälsning,

{hyresgast.namn}
"""
    return brev.strip()


def generera_hyreshojningssvar(
    hyresgast: HyresgastInfo,
    hojning: HyreshojningsInfo,
    argument: list[str],
) -> str:
    """Generera svar på hyreshöjning."""

    datum = date.today().isoformat()
    hojning_procent = hojning.hojning_procent
    if hojning.nuvarande_hyra > 0 and hojning.foreslagen_hyra > 0:
        hojning_procent = (
            (hojning.foreslagen_hyra - hojning.nuvarande_hyra) / hojning.nuvarande_hyra * 100
        )

    brev = f"""
{hyresgast.namn}
{datum}

Till hyresvärden

SVAR PÅ MEDDELANDE OM HYRESHÖJNING

Jag har tagit del av ert meddelande om att höja hyran från
{_format_sek(hojning.nuvarande_hyra)} till {_format_sek(hojning.foreslagen_hyra)}
per månad, vilket motsvarar en höjning om {hojning_procent:.1f}%.

Jag godtar inte den föreslagna hyreshöjningen.
"""

    for argument_text in argument:
        brev += f"\n- {argument_text}"

    brev += """

Jag begär att hyran fastställs till ett skäligt belopp enligt bruksvärdesystemet i
JB 12 kap. 55 §.

Om vi inte når en överenskommelse får frågan prövas av hyresnämnden.

Med vänlig hälsning,
"""
    brev += f"\n\n{hyresgast.namn}"
    return brev.strip()


def generera_reklamationsbrev(
    hyresgast: HyresgastInfo,
    brist: BristInfo,
) -> str:
    """Generera reklamationsbrev vid brist i lägenheten."""

    datum = date.today().isoformat()

    brev = f"""
{hyresgast.namn}
{datum}

Till hyresvärden

ANMÄLAN OM BRIST I LÄGENHETEN

Jag vill härmed skriftligen anmäla följande brist i min lägenhet:

{brist.beskrivning}

Enligt JB 12 kap. 15 § ska lägenheten under hyrestiden hållas i sådant skick att den
är fullt brukbar för det avsedda ändamålet.

Jag begär att bristen åtgärdas senast inom fyra veckor från mottagandet av detta brev.

Om åtgärd inte vidtas i tid förbehåller jag mig rätten att ansöka hos hyresnämnden om
föreläggande och att begära nedsättning av hyran för den tid bristen kvarstår.

Med vänlig hälsning,

{hyresgast.namn}
"""
    return brev.strip()
