"""Streamlit-demo för §AI Hyresrättsassistenten."""

from __future__ import annotations

import sys
import textwrap
from datetime import date
from pathlib import Path

import streamlit as st

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from modules.hyresratt.engine import HyresrattsEngine  # noqa: E402
from modules.hyresratt.models import (  # noqa: E402
    ArendeTyp,
    BristInfo,
    ForverkandeGrund,
    HyresgastInfo,
    HyreshojningsInfo,
    HyresrattsAnalys,
    UpsagningsInfo,
    UpsagningsTyp,
)
from modules.hyresratt.timeline import som_tidslinje_poster  # noqa: E402


@st.cache_resource
def _get_engine() -> HyresrattsEngine:
    return HyresrattsEngine()


def _inject_css() -> None:
    st.markdown(
        """
        <style>
            .stApp {
                background:
                    radial-gradient(circle at top left, rgba(54, 127, 169, 0.14), transparent 28%),
                    radial-gradient(circle at bottom right, rgba(53, 166, 138, 0.12), transparent 24%),
                    linear-gradient(180deg, #f3f9fb 0%, #eef7f4 100%);
            }
            .hyres-hero {
                background: linear-gradient(135deg, #0f3552 0%, #175877 55%, #1d7e74 100%);
                color: #f7fcfd;
                border-radius: 26px;
                padding: 1.5rem 1.6rem;
                box-shadow: 0 24px 50px rgba(15, 53, 82, 0.18);
                margin-bottom: 1rem;
            }
            .hyres-card {
                background: rgba(255, 255, 255, 0.9);
                border: 1px solid rgba(15, 53, 82, 0.08);
                border-radius: 20px;
                padding: 1rem 1.1rem;
                box-shadow: 0 16px 36px rgba(15, 53, 82, 0.08);
                margin-bottom: 0.9rem;
            }
            .hyres-pill {
                display: inline-block;
                padding: 0.35rem 0.7rem;
                border-radius: 999px;
                font-weight: 700;
                margin-top: 0.35rem;
            }
            .timeline-item {
                border-left: 6px solid #1d7e74;
                background: rgba(255, 255, 255, 0.92);
                padding: 0.9rem 1rem;
                border-radius: 14px;
                margin: 0.7rem 0;
            }
        </style>
        """,
        unsafe_allow_html=True,
    )


def _format_date(value: date | None) -> str:
    return value.isoformat() if value else "Ej angivet"


def _format_sek(amount: float) -> str:
    return f"{amount:,.0f}".replace(",", " ") + " kr"


def _bedomning_meta(bedomning: str) -> tuple[str, str]:
    mapping = {
        "stark_position": ("Stark position", "#daf5ea"),
        "medel": ("Medelstark position", "#fff4cf"),
        "svag_position": ("Svag position", "#ffdfe1"),
    }
    label, color = mapping.get(bedomning, ("Bedömning saknas", "#e8eef2"))
    return label, color


def _status_label(days_left: int) -> str:
    if days_left < 0:
        return "Passerad"
    if days_left == 0:
        return "I dag"
    if days_left == 1:
        return "1 dag kvar"
    return f"{days_left} dagar kvar"


def _pdf_escape(text: str) -> str:
    sanitized = (
        text.replace("–", "-")
        .replace("—", "-")
        .replace("•", "-")
        .replace("\t", "    ")
    )
    sanitized = sanitized.replace("\\", "\\\\").replace("(", "\\(").replace(")", "\\)")
    return sanitized


def _build_pdf(title: str, body: str) -> bytes:
    lines = [title, ""]
    for paragraph in body.splitlines():
        if not paragraph.strip():
            lines.append("")
            continue
        lines.extend(textwrap.wrap(paragraph, width=86) or [""])

    content_lines = ["BT", "/F1 11 Tf", "50 790 Td", "14 TL"]
    for index, line in enumerate(lines):
        escaped = _pdf_escape(line)
        if index == 0:
            content_lines.append(f"({escaped}) Tj")
        else:
            content_lines.append("T*")
            content_lines.append(f"({escaped}) Tj")
    content_lines.append("ET")
    content_stream = "\n".join(content_lines).encode("latin-1", errors="replace")

    objects = [
        b"<< /Type /Catalog /Pages 2 0 R >>",
        b"<< /Type /Pages /Kids [3 0 R] /Count 1 >>",
        (
            b"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 595 842] "
            b"/Contents 4 0 R /Resources << /Font << /F1 5 0 R >> >> >>"
        ),
        b"<< /Length "
        + str(len(content_stream)).encode("ascii")
        + b" >>\nstream\n"
        + content_stream
        + b"\nendstream",
        b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>",
    ]

    pdf = b"%PDF-1.4\n%\xe2\xe3\xcf\xd3\n"
    offsets = [0]
    for obj_num, obj in enumerate(objects, start=1):
        offsets.append(len(pdf))
        pdf += f"{obj_num} 0 obj\n".encode("ascii")
        pdf += obj + b"\nendobj\n"

    xref_start = len(pdf)
    pdf += f"xref\n0 {len(objects) + 1}\n".encode("ascii")
    pdf += b"0000000000 65535 f \n"
    for offset in offsets[1:]:
        pdf += f"{offset:010d} 00000 n \n".encode("ascii")
    pdf += (
        f"trailer << /Size {len(objects) + 1} /Root 1 0 R >>\nstartxref\n{xref_start}\n%%EOF"
    ).encode("ascii")
    return pdf


def _render_timeline(analysis: HyresrattsAnalys) -> None:
    st.markdown("### Tidslinje")
    poster = som_tidslinje_poster(analysis.tidsfrister)
    if not poster:
        st.info("Inga exakta lagstadgade deadlines kunde räknas ut från dina uppgifter ännu.")
        return

    for post in poster:
        status_color = {
            "akut": "#9d1c28",
            "snart": "#986d00",
            "planerad": "#1d7e74",
            "passerad": "#6f7680",
        }.get(str(post["status"]), "#1d7e74")
        st.markdown(
            f"""
            <div class="timeline-item" style="border-left-color:{status_color};">
                <strong>{post["namn"]}</strong><br>
                Deadline: {post["deadline"]} • {_status_label(int(post["dagar_kvar"]))}<br>
                Åtgärd: {post["atgard"]}<br>
                Lagstöd: {post["lagrum"]}
            </div>
            """,
            unsafe_allow_html=True,
        )


def _render_analysis(analysis: HyresrattsAnalys, *, download_name: str) -> None:
    label, color = _bedomning_meta(analysis.bedomning)
    st.markdown(
        f"""
        <div class="hyres-card">
            <h3 style="margin:0;">Sammanfattning</h3>
            <p style="margin:0.5rem 0 0 0;">{analysis.sammanfattning}</p>
            <span class="hyres-pill" style="background:{color};">{label}</span>
        </div>
        """,
        unsafe_allow_html=True,
    )

    st.success("Hyresnämndens prövning är kostnadsfri.")

    if analysis.varningar:
        for varning in analysis.varningar:
            st.warning(varning)

    if analysis.hanvisa_till_jurist:
        st.error(analysis.jurist_motivering or "Den här situationen bör granskas av jurist.")

    details_left, details_right = st.columns([1.1, 1], gap="large")
    with details_left:
        st.markdown("### Nästa steg")
        for atgard in analysis.rekommenderade_atgarder:
            st.markdown(f"- {atgard}")

        _render_timeline(analysis)

    with details_right:
        st.markdown("### Rättslig grund")
        st.markdown(f"**Huvudlagrum:** {analysis.rattslig_grund}")
        for lagrum in analysis.relevanta_lagrum:
            st.markdown(f"- {lagrum}")

        if analysis.kallhanvisningar:
            with st.expander("Rättskällor och hänvisningar"):
                for kallh in analysis.kallhanvisningar:
                    st.markdown(f"- {kallh}")

    if analysis.genererat_brev:
        st.markdown("### Genererat brev")
        st.text_area(
            "Förhandsgranska brevet",
            value=analysis.genererat_brev,
            height=340,
            label_visibility="collapsed",
        )
        download_col_1, download_col_2 = st.columns(2)
        with download_col_1:
            st.download_button(
                "Ladda ned brevet som text",
                data=analysis.genererat_brev,
                file_name=download_name.replace(".pdf", ".txt"),
                mime="text/plain",
                use_container_width=True,
            )
        with download_col_2:
            st.download_button(
                "Ladda ned brevet som PDF",
                data=_build_pdf("§AI Hyresrättsbrev", analysis.genererat_brev),
                file_name=download_name,
                mime="application/pdf",
                use_container_width=True,
            )


def _hyresgast_info(
    *,
    namn: str,
    bostadstyp: str,
    boendetid_ar: int,
    hyra_per_manad: float,
    betalar_i_tid: bool = True,
    antal_forseningar_12man: int = 0,
    har_andrahandshyrning: bool = False,
    har_inneboende: bool = False,
) -> HyresgastInfo:
    return HyresgastInfo(
        namn=namn.strip(),
        bostadstyp=bostadstyp,
        boendetid_ar=boendetid_ar,
        hyra_per_manad=hyra_per_manad,
        betalar_i_tid=betalar_i_tid,
        antal_forseningar_12man=antal_forseningar_12man,
        har_andrahandshyrning=har_andrahandshyrning,
        har_inneboende=har_inneboende,
    )


def _render_uppsagning_tab() -> None:
    grund_options = {
        "Obetald hyra": ForverkandeGrund.OBETALD_HYRA,
        "Störningar": ForverkandeGrund.STORNINGAR,
        "Otillåten andrahandsuthyrning": ForverkandeGrund.OTILLATEN_ANDRAHAND,
        "Vanvård": ForverkandeGrund.VANVARD,
        "Brottslig verksamhet": ForverkandeGrund.BROTTSLIG_VERKSAMHET,
        "Vägran att ge tillträde": ForverkandeGrund.VAGRAN_TILLTRADE,
        "Otillåten inneboende / oklar upplåtelse": ForverkandeGrund.OTILLATEN_INNEBOENDE,
        "Annan eller oklar grund": ForverkandeGrund.ANNAN,
    }
    typ_options = {
        "Förverkande / omedelbar uppsägning": UpsagningsTyp.FORVERKANDE,
        "Uppsägning med uppsägningstid": UpsagningsTyp.UPPSAGNING_MED_TID,
        "Osäkert": UpsagningsTyp.OKAND,
    }

    st.markdown("### Uppsägning")
    with st.form("hyresratt_uppsagning_form"):
        namn = st.text_input("Ditt namn", value="Förnamn Efternamn")
        col_a, col_b = st.columns(2)
        with col_a:
            bostadstyp = st.selectbox("Bostadstyp", ["lägenhet", "rum", "lokal"])
            boendetid_ar = int(
                st.number_input("Hur länge har du bott där? (år)", min_value=0, max_value=80, value=3)
            )
            hyra_per_manad = float(
                st.number_input("Månadshyra", min_value=0, value=8500, step=500)
            )
            datum_mottagen = st.date_input("När tog du emot uppsägningen?", value=date.today())
        with col_b:
            uppsagning_typ_label = st.selectbox("Typ av uppsägning", list(typ_options.keys()))
            grund_label = st.selectbox("Vilken grund anger hyresvärden?", list(grund_options.keys()))
            skriftlig = st.checkbox("Uppsägningen är skriftlig", value=True)
            antal_forseningar = int(
                st.number_input(
                    "Antal sena hyror senaste 12 månaderna",
                    min_value=0,
                    max_value=24,
                    value=0,
                )
            )

        motivering = st.text_area(
            "Hyresvärdens motivering eller egna anteckningar",
            placeholder="Skriv det som står i uppsägningen eller det viktigaste du vet.",
            height=120,
        )
        submitted = st.form_submit_button("Analysera uppsägningen", type="primary")

    if bostadstyp == "lokal":
        st.info("Lokalhyra stöds inte fullt ut i MVP:n. Resultatet bör ses som preliminär orientering.")

    if submitted:
        analys = _get_engine().analysera(
            arende_typ=ArendeTyp.UPPSAGNING,
            uppsagning=UpsagningsInfo(
                datum_mottagen=datum_mottagen,
                typ=typ_options[uppsagning_typ_label],
                grund=grund_options[grund_label],
                skriftlig=skriftlig,
                hyresvardens_motivering=motivering.strip(),
            ),
            hyresgast=_hyresgast_info(
                namn=namn,
                bostadstyp=bostadstyp,
                boendetid_ar=boendetid_ar,
                hyra_per_manad=hyra_per_manad,
                betalar_i_tid=antal_forseningar == 0,
                antal_forseningar_12man=antal_forseningar,
            ),
        )
        st.session_state["hyresratt_uppsagning_resultat"] = analys

    analys = st.session_state.get("hyresratt_uppsagning_resultat")
    if isinstance(analys, HyresrattsAnalys):
        _render_analysis(analys, download_name="bestridandebrev_hyresratt.pdf")


def _render_hyreshojning_tab() -> None:
    st.markdown("### Hyreshöjning")
    with st.form("hyresratt_hyreshojning_form"):
        namn = st.text_input("Ditt namn", value="Förnamn Efternamn", key="hojning_namn")
        col_a, col_b = st.columns(2)
        with col_a:
            nuvarande = float(
                st.number_input("Nuvarande hyra", min_value=0, value=8000, step=500)
            )
            foreslagen = float(
                st.number_input("Föreslagen hyra", min_value=0, value=9000, step=500)
            )
        with col_b:
            bostadstyp = st.selectbox(
                "Bostadstyp",
                ["lägenhet", "rum"],
                key="hojning_bostadstyp",
            )
            forhandlingsordning = st.checkbox(
                "Det finns förhandlingsordning / HGF är inkopplad",
                value=False,
            )
        motivering = st.text_area(
            "Motivering från hyresvärden",
            placeholder="Vad anges som skäl för höjningen?",
            height=120,
            key="hojning_motivering",
        )
        submitted = st.form_submit_button("Analysera hyreshöjningen", type="primary")

    if submitted:
        analys = _get_engine().analysera(
            arende_typ=ArendeTyp.HYRESHOJNING,
            hyreshojning=HyreshojningsInfo(
                nuvarande_hyra=nuvarande,
                foreslagen_hyra=foreslagen,
                motivering=motivering.strip(),
                forhandlingsordning=forhandlingsordning,
            ),
            hyresgast=_hyresgast_info(
                namn=namn,
                bostadstyp=bostadstyp,
                boendetid_ar=0,
                hyra_per_manad=nuvarande,
            ),
        )
        st.session_state["hyresratt_hyreshojning_resultat"] = analys

    analys = st.session_state.get("hyresratt_hyreshojning_resultat")
    if isinstance(analys, HyresrattsAnalys):
        _render_analysis(analys, download_name="svar_hyreshojning.pdf")


def _render_brist_tab() -> None:
    st.markdown("### Brist i lägenheten")
    with st.form("hyresratt_brist_form"):
        namn = st.text_input("Ditt namn", value="Förnamn Efternamn", key="brist_namn")
        beskrivning = st.text_area(
            "Beskriv bristen",
            placeholder="Exempel: Fukt i badrummet, återkommande mögellukt och missfärgning.",
            height=120,
        )
        col_a, col_b = st.columns(2)
        with col_a:
            brist_typ = st.selectbox(
                "Typ av brist",
                ["fukt", "mögel", "trasig_utrustning", "buller", "skadedjur", "annat"],
            )
            anmald = st.checkbox("Bristen har redan anmälts till hyresvärden", value=False)
        with col_b:
            anmald_datum = st.date_input("När anmäldes bristen?", value=date.today())
            hyresvard_agerat = st.checkbox("Hyresvärden har agerat", value=False)
        submitted = st.form_submit_button("Analysera bristen", type="primary")

    if submitted:
        analys = _get_engine().analysera(
            arende_typ=ArendeTyp.BRIST,
            brist=BristInfo(
                beskrivning=beskrivning.strip(),
                typ=brist_typ,
                anmald_till_hyresvard=anmald,
                anmald_datum=anmald_datum if anmald else None,
                hyresvard_agerat=hyresvard_agerat,
            ),
            hyresgast=_hyresgast_info(
                namn=namn,
                bostadstyp="lägenhet",
                boendetid_ar=0,
                hyra_per_manad=0,
            ),
        )
        st.session_state["hyresratt_brist_resultat"] = analys

    analys = st.session_state.get("hyresratt_brist_resultat")
    if isinstance(analys, HyresrattsAnalys):
        _render_analysis(analys, download_name="reklamationsbrev_hyresratt.pdf")


def render_app(*, standalone: bool = True) -> None:
    if standalone:
        st.set_page_config(
            page_title="§AI Hyresrättsassistent",
            page_icon="🏠",
            layout="wide",
        )

    _inject_css()
    st.markdown(
        """
        <div class="hyres-hero">
            <h1 style="margin:0;">🏠 §AI Hyresrättsassistent</h1>
            <p style="margin:0.55rem 0 0 0;">
                Få vägledning vid uppsägning, hyreshöjning eller brister i lägenheten.
                Fokus ligger på tydliga nästa steg, lagstöd och en trygg ton.
            </p>
        </div>
        """,
        unsafe_allow_html=True,
    )
    st.caption(
        "MVP:n är avsedd för bostadshyra. Vid lokalhyra, oklar besittningsskyddsfråga eller "
        "bostadsrättsspecifika frågor bör en jurist granska underlaget."
    )

    summary_left, summary_right, summary_third = st.columns(3)
    with summary_left:
        st.markdown('<div class="hyres-card"><strong>Uppsägning</strong><br>Svara snabbt och få koll på tidsfrister.</div>', unsafe_allow_html=True)
    with summary_right:
        st.markdown('<div class="hyres-card"><strong>Hyreshöjning</strong><br>Jämför mot bruksvärdet och svara skriftligt.</div>', unsafe_allow_html=True)
    with summary_third:
        st.markdown('<div class="hyres-card"><strong>Brist</strong><br>Dokumentera, reklamera och gå vidare vid passivitet.</div>', unsafe_allow_html=True)

    tab_uppsagning, tab_hojning, tab_brist = st.tabs(
        ["Uppsägning", "Hyreshöjning", "Brist i lägenheten"]
    )

    with tab_uppsagning:
        _render_uppsagning_tab()
    with tab_hojning:
        _render_hyreshojning_tab()
    with tab_brist:
        _render_brist_tab()


if __name__ == "__main__":
    render_app(standalone=True)
