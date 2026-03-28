"""Dataklasser för hyresrättsmodulen."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from enum import Enum


class ArendeTyp(Enum):
    UPPSAGNING = "uppsägning"
    HYRESHOJNING = "hyreshöjning"
    BRIST = "brist"


class UpsagningsTyp(Enum):
    FORVERKANDE = "förverkande"
    UPPSAGNING_MED_TID = "uppsägning"
    OKAND = "okänd"


class ForverkandeGrund(Enum):
    OBETALD_HYRA = "obetald_hyra"
    OTILLATEN_ANDRAHAND = "otillåten_andrahand"
    STORNINGAR = "störningar"
    VANVARD = "vanvård"
    BROTTSLIG_VERKSAMHET = "brottslig_verksamhet"
    VAGRAN_TILLTRADE = "vägran_tillträde"
    OTILLATEN_INNEBOENDE = "otillåten_inneboende"
    ANNAN = "annan"


@dataclass(slots=True)
class UpsagningsInfo:
    """Information om uppsägningen."""

    datum_mottagen: date | None = None
    typ: UpsagningsTyp = UpsagningsTyp.OKAND
    grund: ForverkandeGrund | None = None
    skriftlig: bool = True
    hyresvardens_motivering: str = ""


@dataclass(slots=True)
class HyresgastInfo:
    """Information om hyresgästen."""

    namn: str = ""
    boendetid_ar: int = 0
    bostadstyp: str = "lägenhet"
    hyra_per_manad: float = 0
    betalar_i_tid: bool = True
    antal_forseningar_12man: int = 0
    har_andrahandshyrning: bool = False
    har_inneboende: bool = False


@dataclass(slots=True)
class HyreshojningsInfo:
    """Information om hyreshöjning."""

    nuvarande_hyra: float = 0
    foreslagen_hyra: float = 0
    hojning_procent: float = 0
    motivering: str = ""
    forhandlingsordning: bool = False


@dataclass(slots=True)
class BristInfo:
    """Information om brist i lägenheten."""

    beskrivning: str = ""
    typ: str = ""
    anmald_till_hyresvard: bool = False
    anmald_datum: date | None = None
    hyresvard_agerat: bool = False


@dataclass(slots=True)
class Tidsfrist:
    """En tidsfrist med deadline och åtgärd."""

    namn: str
    deadline: date
    atgard: str
    lagrum: str
    prioritet: str
    dagar_kvar: int = field(init=False)

    def __post_init__(self) -> None:
        self.dagar_kvar = (self.deadline - date.today()).days


@dataclass(slots=True)
class HyresrattsAnalys:
    """Komplett analys av hyresrättsfråga."""

    arende_typ: ArendeTyp
    sammanfattning: str
    bedomning: str
    rattslig_grund: str
    relevanta_lagrum: list[str]
    tidsfrister: list[Tidsfrist]
    rekommenderade_atgarder: list[str]
    genererat_brev: str | None = None
    brev_typ: str = ""
    relevanta_rattsfall: list[str] = field(default_factory=list)
    kallhanvisningar: list[str] = field(default_factory=list)
    varningar: list[str] = field(default_factory=list)
    hanvisa_till_jurist: bool = False
    jurist_motivering: str = ""
