"""Hjälpfunktioner för tidsfrister i hyresrättsmodulen."""

from __future__ import annotations

from modules.hyresratt.models import Tidsfrist

_PRIORITY_ORDER = {
    "kritisk": 0,
    "viktig": 1,
    "bra_att_veta": 2,
}


def sortera_tidsfrister(tidsfrister: list[Tidsfrist]) -> list[Tidsfrist]:
    """Sortera deadlines efter datum och prioritet."""

    return sorted(
        tidsfrister,
        key=lambda tidsfrist: (
            tidsfrist.deadline,
            _PRIORITY_ORDER.get(tidsfrist.prioritet, 99),
            tidsfrist.namn,
        ),
    )


def status_for_tidsfrist(tidsfrist: Tidsfrist) -> str:
    """Ge en enkel statusetikett för UI."""

    if tidsfrist.dagar_kvar < 0:
        return "passerad"
    if tidsfrist.dagar_kvar <= 3:
        return "akut"
    if tidsfrist.dagar_kvar <= 14:
        return "snart"
    return "planerad"


def som_tidslinje_poster(tidsfrister: list[Tidsfrist]) -> list[dict[str, str | int]]:
    """Serialisera tidsfrister till ett enkelt UI-format."""

    poster: list[dict[str, str | int]] = []
    for tidsfrist in sortera_tidsfrister(tidsfrister):
        poster.append(
            {
                "namn": tidsfrist.namn,
                "deadline": tidsfrist.deadline.isoformat(),
                "atgard": tidsfrist.atgard,
                "lagrum": tidsfrist.lagrum,
                "prioritet": tidsfrist.prioritet,
                "dagar_kvar": tidsfrist.dagar_kvar,
                "status": status_for_tidsfrist(tidsfrist),
            }
        )
    return poster
