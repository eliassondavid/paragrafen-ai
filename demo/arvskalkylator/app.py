"""Streamlit-demo för §AI Arvskalkylator."""

from __future__ import annotations

import sys
from decimal import Decimal
from pathlib import Path

import streamlit as st

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from modules.arvskalkylator.calculator import Arvskalkylator  # noqa: E402
from modules.arvskalkylator.models import (  # noqa: E402
    Aktenskapsforord,
    Barn,
    BarnTyp,
    CivilStatus,
    FamiljeInput,
    Testamente,
    Tillgangar,
    to_decimal,
)


def _format_sek(value: Decimal | int | float | str) -> str:
    amount = to_decimal(value)
    if amount == amount.to_integral():
        return f"{amount:,.0f} kr"
    return f"{amount:,.2f} kr"


def _format_percent(value: Decimal | int | float | str) -> str:
    percentage = to_decimal(value) * 100
    if percentage == percentage.to_integral():
        return f"{percentage:,.0f} %"
    return f"{percentage:,.1f} %"


def render_app(*, standalone: bool = True) -> None:
    if standalone:
        st.set_page_config(
            page_title="§AI Arvskalkylator",
            page_icon="📊",
            layout="wide",
        )

    st.markdown(
        """
        <style>
            .stApp {
                background:
                    radial-gradient(circle at top left, rgba(210, 175, 83, 0.18), transparent 30%),
                    linear-gradient(180deg, #fbfaf7 0%, #f4f0e7 100%);
            }
            .arv-card {
                background: rgba(255, 255, 255, 0.88);
                border: 1px solid rgba(27, 42, 74, 0.08);
                border-radius: 18px;
                padding: 1rem 1.1rem;
                box-shadow: 0 16px 40px rgba(27, 42, 74, 0.06);
            }
            .arv-hero {
                background: linear-gradient(135deg, #15233d 0%, #1f3157 100%);
                color: #f8f2e7;
                border-radius: 24px;
                padding: 1.4rem 1.6rem;
                margin-bottom: 1rem;
            }
        </style>
        """,
        unsafe_allow_html=True,
    )

    st.markdown(
        """
        <div class="arv-hero">
            <h1 style="margin:0;">📊 §AI Arvskalkylator</h1>
            <p style="margin:0.55rem 0 0 0;">
                Beräkna hur arvet normalt fördelas enligt svensk lag och få en tydlig bild
                av när särkullbarn, sambo och laglott förändrar utfallet.
            </p>
        </div>
        """,
        unsafe_allow_html=True,
    )
    st.caption(
        "Beräkningen bygger på huvudregler i ärvdabalken, äktenskapsbalken och sambolagen. "
        "Komplexa testaments- eller internationella frågor kräver separat juridisk granskning."
    )

    civilstatus_label = st.selectbox(
        "Civilstånd",
        options=["Gift", "Sambo", "Ensamstående", "Skild", "Änkling/änka"],
        index=0,
    )
    civilstatus_map = {
        "Gift": CivilStatus.GIFT,
        "Sambo": CivilStatus.SAMBO,
        "Ensamstående": CivilStatus.ENSAMSTAENDE,
        "Skild": CivilStatus.SKILD,
        "Änkling/änka": CivilStatus.ANKLING,
    }

    col_left, col_right = st.columns([1.1, 1], gap="large")

    with col_left:
        st.markdown('<div class="arv-card">', unsafe_allow_html=True)
        st.markdown("### Familjesituation")
        antal_barn = int(st.number_input("Antal barn", min_value=0, max_value=10, value=2))

        barn_lista: list[Barn] = []
        for index in range(antal_barn):
            barn_col_1, barn_col_2 = st.columns([1.2, 1])
            with barn_col_1:
                namn = st.text_input(
                    f"Barn {index + 1} namn",
                    value=f"Barn {index + 1}",
                    key=f"barn_namn_{index}",
                )
            with barn_col_2:
                if civilstatus_label in {"Gift", "Sambo"}:
                    barn_typ_label = st.selectbox(
                        f"Barn {index + 1} typ",
                        options=["Gemensamt barn", "Särkullbarn (eget)"],
                        key=f"barn_typ_{index}",
                    )
                    barn_typ = (
                        BarnTyp.GEMENSAMT
                        if barn_typ_label == "Gemensamt barn"
                        else BarnTyp.SARKULLBARN_EGET
                    )
                else:
                    barn_typ = BarnTyp.GEMENSAMT
                    st.text("Barnet behandlas som bröstarvinge")
            barn_lista.append(Barn(namn=namn, typ=barn_typ))

        if antal_barn == 0:
            foraldrar_lever = st.checkbox("Föräldrar lever", value=False)
            syskon_antal = 0
            if not foraldrar_lever:
                syskon_antal = int(
                    st.number_input("Antal syskon", min_value=0, max_value=10, value=0)
                )
        else:
            foraldrar_lever = False
            syskon_antal = 0

        testamente_finns = st.checkbox("Testamente finns", value=False)
        st.markdown("</div>", unsafe_allow_html=True)

    with col_right:
        st.markdown('<div class="arv-card">', unsafe_allow_html=True)
        st.markdown("### Den avlidnes tillgångar")

        bostad = st.number_input(
            "Bostadens marknadsvärde",
            min_value=0,
            value=2500000,
            step=100000,
            format="%d",
        )
        bostad_lan = st.number_input(
            "Bostadslån",
            min_value=0,
            value=1000000,
            step=100000,
            format="%d",
        )
        sparande = st.number_input(
            "Sparande",
            min_value=0,
            value=500000,
            step=50000,
            format="%d",
        )
        ovriga = st.number_input(
            "Övriga tillgångar",
            min_value=0,
            value=200000,
            step=50000,
            format="%d",
        )
        skulder = st.number_input(
            "Övriga skulder",
            min_value=0,
            value=0,
            step=10000,
            format="%d",
        )

        tillgangar_avliden = Tillgangar(
            bostad_marknadsvarde=bostad,
            bostad_lan=bostad_lan,
            sparande=sparande,
            ovriga_tillgangar=ovriga,
            skulder=skulder,
        )
        st.metric("Nettoförmögenhet", _format_sek(tillgangar_avliden.netto))

        tillgangar_efterlevande = None
        if civilstatus_label in {"Gift", "Sambo"}:
            st.markdown("### Efterlevandes tillgångar")
            ef_netto = st.number_input(
                "Efterlevandes nettotillgångar",
                min_value=0,
                value=1500000,
                step=100000,
                format="%d",
            )
            tillgangar_efterlevande = Tillgangar(sparande=ef_netto)

        if civilstatus_label == "Gift":
            forord_finns = st.checkbox("Äktenskapsförord finns", value=False)
            enskild_andel = Decimal("0")
            if forord_finns:
                enskild_andel = Decimal(
                    str(
                        st.slider(
                            "Andel enskild egendom",
                            min_value=0.0,
                            max_value=1.0,
                            value=0.0,
                            step=0.1,
                        )
                    )
                )
            aktenskapsforord = Aktenskapsforord(
                finns=forord_finns,
                enskild_egendom_andel=enskild_andel,
            )
        else:
            aktenskapsforord = Aktenskapsforord()
        st.markdown("</div>", unsafe_allow_html=True)

    if st.button("Beräkna arvsfördelning", type="primary", use_container_width=True):
        kalkylator = Arvskalkylator()
        resultat = kalkylator.berakna(
            FamiljeInput(
                civilstatus=civilstatus_map[civilstatus_label],
                barn=barn_lista,
                tillgangar_avliden=tillgangar_avliden,
                tillgangar_efterlevande=tillgangar_efterlevande,
                testamente=Testamente(finns=testamente_finns),
                aktenskapsforord=aktenskapsforord,
                foraldrar_lever=foraldrar_lever,
                syskon_antal=syskon_antal,
            )
        )

        st.markdown("## Resultat")
        summary_left, summary_right = st.columns(2)
        with summary_left:
            st.metric("Kvarlåtenskap efter bodelning", _format_sek(resultat.kvarlatenskap))
        with summary_right:
            if resultat.bodelning_efterlevande > 0:
                st.metric(
                    "Efterlevandes del i bodelning",
                    _format_sek(resultat.bodelning_efterlevande),
                )

        st.markdown("### Fördelning")
        typ_emoji = {
            "full_äganderätt": "✅",
            "fri_förfoganderätt": "🔄",
            "efterarv": "⏳",
            "laglott": "🔒",
        }
        for lott in resultat.arvslotter:
            with st.expander(
                f"{typ_emoji.get(lott.typ, '📌')} {lott.namn} • {_format_sek(lott.belopp)} "
                f"({_format_percent(lott.andel)})",
                expanded=True,
            ):
                st.markdown(f"**Typ:** {lott.typ.replace('_', ' ').title()}")
                st.markdown(f"**Lagstöd:** {lott.lagrum}")
                if lott.kommentar:
                    st.info(lott.kommentar)

        if resultat.laglott_per_barn > 0:
            st.markdown(
                f"### Laglott: {_format_sek(resultat.laglott_per_barn)} per barn"
            )
            st.caption("Laglotten motsvarar hälften av arvslotten och kan inte testamenteras bort.")

        for varning in resultat.varningar:
            st.warning(varning)

        if resultat.forklaringar:
            st.markdown("### Förklaringar")
            for forklaring in resultat.forklaringar:
                st.markdown(f"- {forklaring}")

        cta_left, cta_right = st.columns(2, gap="large")
        with cta_left:
            st.markdown("### Upprätta testamente")
            st.write("Säkerställ att din vilja gäller och att rätt person får rätt skydd.")
            st.button("Kommer snart", key="cta_testamente", disabled=True, use_container_width=True)
        with cta_right:
            st.markdown("### Granska befintligt testamente")
            st.write("Fånga risker i formuleringar, laglottsfrågor och efterarv innan det blir skarpt.")
            st.button("Kommer snart", key="cta_granska", disabled=True, use_container_width=True)


if __name__ == "__main__":
    render_app(standalone=True)
