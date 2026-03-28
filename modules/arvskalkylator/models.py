"""Datamodeller för arvsberäkning."""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from enum import Enum

Numeric = Decimal | int | float | str


def to_decimal(value: Numeric | None) -> Decimal:
    """Konvertera inkommande tal till Decimal utan binära flyttalsfel."""
    if value in (None, ""):
        return Decimal("0")
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))


class CivilStatus(Enum):
    GIFT = "gift"
    SAMBO = "sambo"
    ENSAMSTAENDE = "ensamstående"
    SKILD = "skild"
    ANKLING = "änkling"


class BarnTyp(Enum):
    GEMENSAMT = "gemensamt"
    SARKULLBARN_EGET = "särkullbarn"


@dataclass(slots=True)
class Barn:
    namn: str
    typ: BarnTyp
    lever: bool = True
    har_barn: bool = False
    antal_barnbarn: int = 0


@dataclass(slots=True)
class Tillgangar:
    """Tillgångar och skulder i boet."""

    bostad_marknadsvarde: Numeric = 0
    bostad_lan: Numeric = 0
    sparande: Numeric = 0
    ovriga_tillgangar: Numeric = 0
    skulder: Numeric = 0

    def __post_init__(self) -> None:
        self.bostad_marknadsvarde = to_decimal(self.bostad_marknadsvarde)
        self.bostad_lan = to_decimal(self.bostad_lan)
        self.sparande = to_decimal(self.sparande)
        self.ovriga_tillgangar = to_decimal(self.ovriga_tillgangar)
        self.skulder = to_decimal(self.skulder)

    @property
    def netto(self) -> Decimal:
        return (
            self.bostad_marknadsvarde
            - self.bostad_lan
            + self.sparande
            + self.ovriga_tillgangar
            - self.skulder
        )


@dataclass(slots=True)
class Testamente:
    """Förenklad testamentsmodell."""

    __test__ = False

    finns: bool = False
    typ: str = ""
    beskrivning: str = ""


@dataclass(slots=True)
class Aktenskapsforord:
    finns: bool = False
    beskrivning: str = ""
    enskild_egendom_andel: Numeric = 0

    def __post_init__(self) -> None:
        self.enskild_egendom_andel = to_decimal(self.enskild_egendom_andel)


@dataclass(slots=True)
class FamiljeInput:
    """Komplett indata till arvsberäkningen."""

    civilstatus: CivilStatus
    barn: list[Barn] = field(default_factory=list)
    tillgangar_avliden: Tillgangar = field(default_factory=Tillgangar)
    tillgangar_efterlevande: Tillgangar | None = None
    testamente: Testamente = field(default_factory=Testamente)
    aktenskapsforord: Aktenskapsforord = field(default_factory=Aktenskapsforord)
    foraldrar_lever: bool = False
    syskon_antal: int = 0


@dataclass(slots=True)
class Arvslott:
    """En arvtagares andel av kvarlåtenskapen."""

    namn: str
    roll: str
    belopp: Numeric
    andel: Numeric
    typ: str
    lagrum: str
    kommentar: str = ""

    def __post_init__(self) -> None:
        self.belopp = to_decimal(self.belopp)
        self.andel = to_decimal(self.andel)


@dataclass(slots=True)
class Arvsresultat:
    """Komplett resultat av arvsberäkningen."""

    kvarlatenskap: Numeric
    bodelning_efterlevande: Numeric
    arvslotter: list[Arvslott]
    laglott_per_barn: Numeric
    varningar: list[str]
    forklaringar: list[str]

    def __post_init__(self) -> None:
        self.kvarlatenskap = to_decimal(self.kvarlatenskap)
        self.bodelning_efterlevande = to_decimal(self.bodelning_efterlevande)
        self.laglott_per_barn = to_decimal(self.laglott_per_barn)
