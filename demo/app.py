from __future__ import annotations

import os
from pathlib import Path

import streamlit as st
import yaml


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _modules_config_path() -> Path:
    return _repo_root() / "config" / "modules.yaml"


def _load_modules() -> dict:
    with _modules_config_path().open(encoding="utf-8") as handle:
        payload = yaml.safe_load(handle) or {}
    return payload.get("modules", {})


def _resolve_page(module_key: str) -> str | None:
    pages_dir = Path(__file__).resolve().parent / "pages"
    matches = sorted(pages_dir.glob(f"*_{module_key}.py"))
    if not matches:
        return None
    return str(matches[0].relative_to(Path(__file__).resolve().parent))


st.set_page_config(
    page_title="§AI — Juridisk AI-assistent",
    page_icon="⚖️",
    layout="wide",
)
st.title("⚖️ §AI — paragrafen.ai")
st.subheader("Juridisk AI-assistent för allmänheten")

modules = _load_modules()
enabled_modules = [
    (module_key, module_config)
    for module_key, module_config in modules.items()
    if module_config.get("enabled") is True
]

columns = st.columns(len(enabled_modules) or 1)
has_api_key = bool(os.environ.get("ANTHROPIC_API_KEY"))

for column, (module_key, module_config) in zip(columns, enabled_modules):
    with column:
        label = str(module_config.get("label", module_key))
        icon = str(module_config.get("icon", ""))
        page_path = _resolve_page(module_key)

        st.markdown(f"### {icon} {label}".strip())
        if module_config.get("requires_api_key") and not has_api_key:
            st.info("Kräver API-nyckel — kommer snart")
        elif page_path:
            if st.button("Starta", key=f"start_{module_key}", use_container_width=True):
                st.switch_page(page_path)
        else:
            st.info("Sidan är inte tillgänglig ännu.")

st.caption("§AI är ett open source-projekt. Ersätter inte juridisk rådgivning.")

