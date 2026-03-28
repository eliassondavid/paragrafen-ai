from __future__ import annotations

import sys
from pathlib import Path

import streamlit as st

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from modules.second_opinion.engine import SecondOpinionEngine
from modules.second_opinion.report_builder import Finding, SecondOpinionReport


@st.cache_resource
def _get_engine() -> SecondOpinionEngine:
    return SecondOpinionEngine()


def _finding_title(finding: Finding) -> str:
    text = finding.description.strip() or "Utan rubrik"
    return text if len(text) <= 80 else text[:77].rstrip() + "..."


def _assessment_icon(assessment: str) -> str:
    return {
        "korrekt": "🟢",
        "delvis_korrekt": "🟡",
        "tveksamt": "🟠",
        "felaktigt": "🔴",
    }.get(assessment, "⚪")


def _prognosis_icon(level: str) -> str:
    return {
        "sannolikt_bifall": "🟢",
        "osäkert": "🟡",
        "sannolikt_avslag": "🔴",
    }.get(level, "⚪")


def _render_finding(finding: Finding, source_label: str) -> None:
    with st.expander(_finding_title(finding)):
        if finding.legal_ref:
            st.markdown(f"**{source_label}:** {finding.legal_ref}")
        st.markdown(f"**Allvarlighetsgrad:** {finding.severity}")
        if finding.source_text:
            st.caption(f"Källutdrag: {finding.source_text}")


def _render_report(report: SecondOpinionReport) -> None:
    icon = _assessment_icon(report.overall_assessment)
    st.markdown(f"## {icon} Sammanfattning")
    st.markdown(report.summary)
    st.markdown(f"**Konfidensnivå:** {report.confidence.upper()}")

    st.markdown("---")
    st.markdown("## ⚖️ Led 1: Har du rätt? (Rättsfrågan)")
    st.markdown(report.legal_analysis)

    col_strengths, col_weaknesses = st.columns(2)

    with col_strengths:
        st.markdown("### ✅ Styrkor i rådet")
        if report.strengths:
            for finding in report.strengths:
                _render_finding(finding, "Lagstöd")
        else:
            st.caption("Inga tydliga styrkor kunde identifieras.")

    with col_weaknesses:
        st.markdown("### ⚠️ Svagheter / invändningar")
        if report.weaknesses:
            for finding in report.weaknesses:
                _render_finding(finding, "Källa som motsäger")
        else:
            st.caption("Inga tydliga invändningar kunde identifieras.")

    if report.gaps:
        st.markdown("### 📌 Vad saknas?")
        for finding in report.gaps:
            st.info(f"**{finding.description}**\n\nLagstöd: {finding.legal_ref}")

    st.markdown("---")
    st.markdown("## 🏛️ Led 2: Får du rätt? (Utfallsprognos)")
    prognosis_icon = _prognosis_icon(report.prognosis_level)
    prognosis_label = report.prognosis_level.replace("_", " ").title()
    st.markdown(f"**Prognos:** {prognosis_icon} {prognosis_label}")
    st.markdown(report.outcome_prognosis)

    with st.expander("Bevisbörda"):
        st.markdown(report.burden_of_proof)

    with st.expander("Praktiska hinder"):
        st.markdown(report.practical_obstacles)

    st.markdown("---")
    st.markdown("## 💡 Rekommendation")
    st.markdown(report.recommendation)

    if report.follow_up_questions:
        st.markdown("**Frågor att ställa till din jurist:**")
        for index, question in enumerate(report.follow_up_questions, start=1):
            st.markdown(f"{index}. {question}")

    with st.expander("📚 Källor som använts i analysen"):
        if report.citations:
            for citation in report.citations:
                st.markdown(f"- {citation}")
        else:
            st.caption("Inga källhänvisningar kunde presenteras.")

    st.markdown("---")
    st.caption(
        "§AI Second Opinion är ett AI-verktyg och ersätter inte juridisk rådgivning. "
        "Vid komplexa frågor, kontakta en kvalificerad jurist."
    )


def render_page(*, set_page_config: bool = True) -> None:
    if set_page_config:
        st.set_page_config(
            page_title="§AI Second Opinion",
            page_icon="🔍",
            layout="wide",
        )

    st.title("🔍 §AI Second Opinion")
    st.subheader("Få en oberoende bedömning av juridiskt råd du fått")

    st.markdown(
        """
Har du fått ett juridiskt råd och undrar om det stämmer?
§AI analyserar rådet mot svenska rättskällor och presenterar
en balanserad bedömning med styrkor, svagheter och saknade aspekter.
"""
    )

    st.markdown("---")

    with st.form("second_opinion_form"):
        situation = st.text_area(
            "1. Beskriv din situation",
            placeholder="Vad har hänt? Vad handlar det om?",
            height=150,
        )

        advice = st.text_area(
            "2. Vilket råd fick du?",
            placeholder="Vad sa juristen, myndigheten eller rådgivaren?",
            height=150,
        )

        col_source, col_area = st.columns(2)

        with col_source:
            advice_source = st.selectbox(
                "3. Vem gav rådet? (valfritt)",
                options=[
                    "Vill ej ange",
                    "Advokat/jurist",
                    "Myndighet",
                    "Fackförbund/organisation",
                    "Bank/försäkringsbolag",
                    "Online-tjänst",
                    "Annan",
                ],
            )

        with col_area:
            legal_area = st.selectbox(
                "4. Rättsområde (om du vet)",
                options=[
                    "Osäker/annat",
                    "Hyresrätt",
                    "Arbetsrätt",
                    "Familjerätt/arv",
                    "Konsumenträtt",
                    "Avtalsrätt",
                    "Upphandling",
                    "Förvaltningsrätt",
                    "Fastighetsrätt",
                    "Socialrätt",
                ],
            )

        submitted = st.form_submit_button("🔍 Analysera", type="primary")

    if submitted and situation and advice:
        with st.spinner("Analyserar mot rättskällor..."):
            report = _get_engine().analyze(
                situation=situation,
                advice_received=advice,
                legal_area=legal_area if legal_area != "Osäker/annat" else "",
                advice_source=advice_source if advice_source != "Vill ej ange" else "",
            )

        st.markdown("---")
        _render_report(report)
    elif submitted:
        st.warning("Fyll i både situation och råd för att få en analys.")


if __name__ == "__main__":
    render_page()
