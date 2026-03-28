"""
Arvsregler enligt ärvdabalken, äktenskapsbalken och sambolagen.

Den här modulen hanterar huvudreglerna för de scenarier som kalkylatorn
exponerar i UI:t. Mer komplexa situationer, som istadarätt via avlidet barn
eller avancerad testamentstolkning, flaggas som upplysningar.
"""

from __future__ import annotations

from decimal import Decimal
from functools import lru_cache
from pathlib import Path

import yaml

from modules.arvskalkylator.models import Arvslott, Barn, BarnTyp, to_decimal

RULES_PATH = Path(__file__).resolve().parent / "config" / "arvs_rules.yaml"


@lru_cache(maxsize=1)
def _load_rule_config() -> dict:
    with RULES_PATH.open(encoding="utf-8") as handle:
        return yaml.safe_load(handle) or {}


def get_prisbasbelopp(ar: int | None = None) -> Decimal:
    """Hämta prisbasbelopp från konfigurationsfil."""
    config = _load_rule_config()
    pbb_config = config.get("prisbasbelopp", {})
    aktivt_ar = ar or pbb_config.get("aktivt_ar")
    belopp = (pbb_config.get("belopp") or {}).get(str(aktivt_ar), 0)
    return to_decimal(belopp)


def _andel(belopp: Decimal, total: Decimal) -> Decimal:
    if total == 0:
        return Decimal("0")
    return belopp / total


def berakna_bodelning(
    civilstatus: str,
    tillgangar_avliden: Decimal | int | float | str,
    tillgangar_efterlevande: Decimal | int | float | str,
    aktenskapsforord_andel: Decimal | int | float | str = 0,
) -> tuple[Decimal, Decimal]:
    """
    Beräkna kvarlåtenskap och efterlevandes andel efter bodelning.

    - Gift: giftorättsgods delas lika, enskild egendom undantas.
    - Sambo: huvudflödet räknar konservativt hela nettot som kvarlåtenskap.
    - Övriga civilstånd: ingen bodelning.
    """
    avliden = to_decimal(tillgangar_avliden)
    efterlevande = to_decimal(tillgangar_efterlevande)
    enskild_andel = min(max(to_decimal(aktenskapsforord_andel), Decimal("0")), Decimal("1"))

    if civilstatus == "gift":
        enskild_avliden = avliden * enskild_andel
        enskild_efterlevande = efterlevande * enskild_andel

        giftoratt_avliden = avliden - enskild_avliden
        giftoratt_efterlevande = efterlevande - enskild_efterlevande

        totalt_giftoratt = giftoratt_avliden + giftoratt_efterlevande
        halva = totalt_giftoratt / 2

        kvarlatenskap = halva + enskild_avliden
        efterlevande_bodelning = halva + enskild_efterlevande
        return kvarlatenskap, efterlevande_bodelning

    if civilstatus == "sambo":
        return avliden, Decimal("0")

    return avliden, Decimal("0")


def berakna_arv(
    kvarlatenskap: Decimal | int | float | str,
    civilstatus: str,
    barn: list[Barn],
    foraldrar_lever: bool,
    syskon_antal: int,
    testamente_finns: bool,
) -> tuple[list[Arvslott], Decimal, list[str], list[str]]:
    """Beräkna arvsfördelning för de scenarier kalkylatorn stödjer."""
    total = to_decimal(kvarlatenskap)
    arvslotter: list[Arvslott] = []
    varningar: list[str] = []
    forklaringar: list[str] = []

    if total <= 0:
        varningar.append(
            "Det finns ingen positiv kvarlåtenskap att fördela efter skulder och eventuell bodelning."
        )
        return arvslotter, Decimal("0"), varningar, forklaringar

    if any((not barn_obj.lever) or barn_obj.har_barn or barn_obj.antal_barnbarn for barn_obj in barn):
        varningar.append(
            "Den här versionen hanterar inte fullt ut istadarätt och arv via barnbarn. "
            "Utgå från resultatet som en huvudregel och be om juristgranskning vid sådana fall."
        )

    gemensamma_barn = [barn_obj for barn_obj in barn if barn_obj.typ == BarnTyp.GEMENSAMT and barn_obj.lever]
    sarkullbarn = [
        barn_obj for barn_obj in barn if barn_obj.typ == BarnTyp.SARKULLBARN_EGET and barn_obj.lever
    ]
    alla_barn = gemensamma_barn + sarkullbarn

    if sarkullbarn:
        varningar.append(
            "Särkullbarn har rätt att få ut sitt arv direkt om de inte frivilligt avstår "
            "till förmån för efterlevande make."
        )

    if civilstatus == "gift":
        if alla_barn:
            arvslott_per_barn = total / len(alla_barn)
            resterande = total

            for barn_obj in sarkullbarn:
                belopp = arvslott_per_barn
                resterande -= belopp
                arvslotter.append(
                    Arvslott(
                        namn=barn_obj.namn,
                        roll="särkullbarn",
                        belopp=belopp,
                        andel=_andel(belopp, total),
                        typ="full_äganderätt",
                        lagrum="ÄB 3 kap. 1 § 2 st.",
                        kommentar=(
                            "Särkullbarn får normalt ut sin arvslott direkt. De kan också välja att "
                            "avstå till förmån för efterlevande make enligt ÄB 3 kap. 9 §."
                        ),
                    )
                )

            if gemensamma_barn and resterande > 0:
                arvslotter.append(
                    Arvslott(
                        namn="Efterlevande make/maka",
                        roll="make",
                        belopp=resterande,
                        andel=_andel(resterande, total),
                        typ="fri_förfoganderätt",
                        lagrum="ÄB 3 kap. 1 §",
                        kommentar=(
                            "Efterlevande make ärver gemensamma barns andelar med fri förfoganderätt. "
                            "Barnens rätt blir efterarv när även maken går bort."
                        ),
                    )
                )

                efterarv_per_barn = resterande / len(gemensamma_barn)
                for barn_obj in gemensamma_barn:
                    arvslotter.append(
                        Arvslott(
                            namn=barn_obj.namn,
                            roll="barn",
                            belopp=efterarv_per_barn,
                            andel=_andel(efterarv_per_barn, total),
                            typ="efterarv",
                            lagrum="ÄB 3 kap. 2 §",
                            kommentar=(
                                "Efterarv innebär att barnets rätt aktualiseras när den efterlevande "
                                "maken avlider. Det slutliga utfallet kan därför förändras över tid."
                            ),
                        )
                    )

                forklaringar.append(
                    "Gemensamma barn får normalt vänta på sitt arv tills båda makarna har gått bort. "
                    "Efterlevande make får använda egendomen men kan inte testamentera bort den del som "
                    "gemensamma barns efterarv avser."
                )
            else:
                fyra_prisbasbelopp = get_prisbasbelopp() * 4
                varningar.append(
                    "Efterlevande make kan ha rätt att ur kvarlåtenskapen få ut egendom motsvarande "
                    f"fyra prisbasbelopp ({fyra_prisbasbelopp:,.0f} kr) enligt ÄB 3 kap. 1 § 2 st."
                )
                forklaringar.append(
                    "När det bara finns särkullbarn kan de ta ut sitt arv direkt, men "
                    "basbeloppsregeln kan minska vad som betalas ut om boet är litet."
                )
        else:
            arvslotter.append(
                Arvslott(
                    namn="Efterlevande make/maka",
                    roll="make",
                    belopp=total,
                    andel=Decimal("1"),
                    typ="full_äganderätt",
                    lagrum="ÄB 3 kap. 1 §",
                    kommentar="Utan bröstarvingar ärver efterlevande make hela kvarlåtenskapen.",
                )
            )
            if foraldrar_lever or syskon_antal > 0:
                varningar.append(
                    "Om den avlidne saknar barn kan föräldrar eller syskon ha rätt till efterarv när "
                    "även efterlevande make avlider."
                )

    elif civilstatus == "sambo":
        varningar.append(
            "Sambor ärver inte varandra utan testamente. Den efterlevande sambon får därför normalt "
            "ingen del av kvarlåtenskapen genom arvsordningen."
        )

        if alla_barn:
            arvslott_per_barn = total / len(alla_barn)
            for barn_obj in alla_barn:
                arvslotter.append(
                    Arvslott(
                        namn=barn_obj.namn,
                        roll="barn",
                        belopp=arvslott_per_barn,
                        andel=_andel(arvslott_per_barn, total),
                        typ="full_äganderätt",
                        lagrum="ÄB 2 kap. 1 §",
                        kommentar="Barnen delar lika på kvarlåtenskapen enligt huvudregeln.",
                    )
                )
        elif foraldrar_lever:
            arvslotter.append(
                Arvslott(
                    namn="Föräldrar",
                    roll="förälder",
                    belopp=total,
                    andel=Decimal("1"),
                    typ="full_äganderätt",
                    lagrum="ÄB 2 kap. 2 §",
                    kommentar="Utan bröstarvingar går arvet vidare till föräldrarna.",
                )
            )
        elif syskon_antal > 0:
            per_syskon = total / syskon_antal
            for index in range(syskon_antal):
                arvslotter.append(
                    Arvslott(
                        namn=f"Syskon {index + 1}",
                        roll="syskon",
                        belopp=per_syskon,
                        andel=_andel(per_syskon, total),
                        typ="full_äganderätt",
                        lagrum="ÄB 2 kap. 2 §",
                        kommentar="När föräldrarna inte lever går arvet vidare till syskon.",
                    )
                )
        else:
            arvslotter.append(
                Arvslott(
                    namn="Allmänna arvsfonden",
                    roll="allmänna_arvsfonden",
                    belopp=total,
                    andel=Decimal("1"),
                    typ="full_äganderätt",
                    lagrum="ÄB 5 kap. 1 §",
                    kommentar="När inga legala arvingar finns går kvarlåtenskapen till arvsfonden.",
                )
            )

    else:
        if alla_barn:
            arvslott_per_barn = total / len(alla_barn)
            for barn_obj in alla_barn:
                arvslotter.append(
                    Arvslott(
                        namn=barn_obj.namn,
                        roll="barn",
                        belopp=arvslott_per_barn,
                        andel=_andel(arvslott_per_barn, total),
                        typ="full_äganderätt",
                        lagrum="ÄB 2 kap. 1 §",
                        kommentar="Barnen delar lika på kvarlåtenskapen.",
                    )
                )
        elif foraldrar_lever:
            arvslotter.append(
                Arvslott(
                    namn="Föräldrar",
                    roll="förälder",
                    belopp=total,
                    andel=Decimal("1"),
                    typ="full_äganderätt",
                    lagrum="ÄB 2 kap. 2 §",
                    kommentar="Utan barn går arvet i första hand till föräldrarna.",
                )
            )
        elif syskon_antal > 0:
            per_syskon = total / syskon_antal
            for index in range(syskon_antal):
                arvslotter.append(
                    Arvslott(
                        namn=f"Syskon {index + 1}",
                        roll="syskon",
                        belopp=per_syskon,
                        andel=_andel(per_syskon, total),
                        typ="full_äganderätt",
                        lagrum="ÄB 2 kap. 2 §",
                        kommentar="Om föräldrarna inte lever träder syskon in i andra parentelen.",
                    )
                )
        else:
            arvslotter.append(
                Arvslott(
                    namn="Allmänna arvsfonden",
                    roll="allmänna_arvsfonden",
                    belopp=total,
                    andel=Decimal("1"),
                    typ="full_äganderätt",
                    lagrum="ÄB 5 kap. 1 §",
                    kommentar="När inga arvingar finns går egendomen till Allmänna arvsfonden.",
                )
            )

    laglott = Decimal("0")
    if alla_barn:
        arvslott_per_barn = total / len(alla_barn)
        laglott = arvslott_per_barn / 2
        forklaringar.append(
            f"Laglotten är {laglott:,.0f} kr per barn. Det motsvarar hälften av varje barns arvslott "
            "och kan inte sättas åt sidan genom testamente."
        )

    if testamente_finns:
        varningar.append(
            "Ett testamente har markerats. Kalkylatorn visar därför lagens utgångspunkt och "
            "laglottsskyddet, men inte en fullständig testamentstolkning."
        )

    return arvslotter, laglott, varningar, forklaringar
