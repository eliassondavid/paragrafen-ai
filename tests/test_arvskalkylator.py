from __future__ import annotations

from modules.arvskalkylator.calculator import Arvskalkylator
from modules.arvskalkylator.models import (
    Barn,
    BarnTyp,
    CivilStatus,
    FamiljeInput,
    Testamente,
    Tillgangar,
)
from modules.arvskalkylator.rules import get_prisbasbelopp


def test_gift_med_gemensamma_barn() -> None:
    inp = FamiljeInput(
        civilstatus=CivilStatus.GIFT,
        barn=[
            Barn(namn="Barn 1", typ=BarnTyp.GEMENSAMT),
            Barn(namn="Barn 2", typ=BarnTyp.GEMENSAMT),
        ],
        tillgangar_avliden=Tillgangar(sparande=2000000),
        tillgangar_efterlevande=Tillgangar(sparande=2000000),
    )

    res = Arvskalkylator().berakna(inp)

    assert res.kvarlatenskap == 2000000

    make_lotter = [lott for lott in res.arvslotter if lott.roll == "make"]
    assert len(make_lotter) == 1
    assert make_lotter[0].typ == "fri_förfoganderätt"
    assert make_lotter[0].belopp == 2000000

    barn_lotter = [lott for lott in res.arvslotter if lott.roll == "barn"]
    assert len(barn_lotter) == 2
    assert all(lott.typ == "efterarv" for lott in barn_lotter)


def test_gift_med_sarkullbarn() -> None:
    inp = FamiljeInput(
        civilstatus=CivilStatus.GIFT,
        barn=[Barn(namn="Särkullbarn", typ=BarnTyp.SARKULLBARN_EGET)],
        tillgangar_avliden=Tillgangar(sparande=1000000),
        tillgangar_efterlevande=Tillgangar(sparande=1000000),
    )

    res = Arvskalkylator().berakna(inp)

    sarkull_lotter = [lott for lott in res.arvslotter if lott.roll == "särkullbarn"]
    assert len(sarkull_lotter) == 1
    assert sarkull_lotter[0].typ == "full_äganderätt"
    assert any("särkullbarn" in varning.lower() for varning in res.varningar)


def test_sambo_arver_inte() -> None:
    inp = FamiljeInput(
        civilstatus=CivilStatus.SAMBO,
        barn=[Barn(namn="Barn 1", typ=BarnTyp.GEMENSAMT)],
        tillgangar_avliden=Tillgangar(sparande=1000000),
    )

    res = Arvskalkylator().berakna(inp)

    sambo_lotter = [lott for lott in res.arvslotter if lott.roll == "make"]
    assert sambo_lotter == []

    barn_lotter = [lott for lott in res.arvslotter if lott.roll == "barn"]
    assert len(barn_lotter) == 1
    assert any("testamente" in varning.lower() for varning in res.varningar)


def test_ensamstaende_utan_arvingar() -> None:
    inp = FamiljeInput(
        civilstatus=CivilStatus.ENSAMSTAENDE,
        tillgangar_avliden=Tillgangar(sparande=500000),
    )

    res = Arvskalkylator().berakna(inp)

    assert len(res.arvslotter) == 1
    assert res.arvslotter[0].roll == "allmänna_arvsfonden"


def test_laglott_beraknas() -> None:
    inp = FamiljeInput(
        civilstatus=CivilStatus.ENSAMSTAENDE,
        barn=[
            Barn(namn="Barn 1", typ=BarnTyp.GEMENSAMT),
            Barn(namn="Barn 2", typ=BarnTyp.GEMENSAMT),
        ],
        tillgangar_avliden=Tillgangar(sparande=1000000),
    )

    res = Arvskalkylator().berakna(inp)

    assert res.laglott_per_barn == 250000


def test_gift_utan_barn() -> None:
    inp = FamiljeInput(
        civilstatus=CivilStatus.GIFT,
        tillgangar_avliden=Tillgangar(sparande=1000000),
        tillgangar_efterlevande=Tillgangar(sparande=1000000),
    )

    res = Arvskalkylator().berakna(inp)

    make_lotter = [lott for lott in res.arvslotter if lott.roll == "make"]
    assert len(make_lotter) == 1
    assert make_lotter[0].typ == "full_äganderätt"


def test_prisbasbelopp_lasas_fran_konfig() -> None:
    assert get_prisbasbelopp() == 58400


def test_testamente_flaggar_begransning() -> None:
    inp = FamiljeInput(
        civilstatus=CivilStatus.ENSAMSTAENDE,
        barn=[Barn(namn="Barn 1", typ=BarnTyp.GEMENSAMT)],
        tillgangar_avliden=Tillgangar(sparande=750000),
        testamente=Testamente(finns=True),
    )

    res = Arvskalkylator().berakna(inp)

    assert any("testamente" in varning.lower() for varning in res.varningar)
