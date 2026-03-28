"""Orkestrering av bodelning och arvsfördelning."""

from __future__ import annotations

from modules.arvskalkylator.models import Arvsresultat, FamiljeInput
from modules.arvskalkylator.rules import berakna_arv, berakna_bodelning


class Arvskalkylator:
    """Beräknar arvslotter utifrån familjesituation och tillgångar."""

    def berakna(self, inp: FamiljeInput) -> Arvsresultat:
        tillgangar_efterlevande = (
            inp.tillgangar_efterlevande.netto if inp.tillgangar_efterlevande else 0
        )

        kvarlatenskap, bodelning_efterlevande = berakna_bodelning(
            civilstatus=inp.civilstatus.value,
            tillgangar_avliden=inp.tillgangar_avliden.netto,
            tillgangar_efterlevande=tillgangar_efterlevande,
            aktenskapsforord_andel=(
                inp.aktenskapsforord.enskild_egendom_andel if inp.aktenskapsforord.finns else 0
            ),
        )

        arvslotter, laglott, varningar, forklaringar = berakna_arv(
            kvarlatenskap=kvarlatenskap,
            civilstatus=inp.civilstatus.value,
            barn=inp.barn,
            foraldrar_lever=inp.foraldrar_lever,
            syskon_antal=inp.syskon_antal,
            testamente_finns=inp.testamente.finns,
        )

        if inp.aktenskapsforord.finns:
            varningar.append(
                "Äktenskapsförord påverkar bodelningen. Resultatet bygger på den andel enskild "
                "egendom du har angett och bör granskas om förordet är mer detaljerat än så."
            )

        return Arvsresultat(
            kvarlatenskap=kvarlatenskap,
            bodelning_efterlevande=bodelning_efterlevande,
            arvslotter=arvslotter,
            laglott_per_barn=laglott,
            varningar=varningar,
            forklaringar=forklaringar,
        )
