from __future__ import annotations

from pathlib import Path

import streamlit as st
import yaml

from rag.llm_client import get_llm_client
from rag.prompt_builder import PromptBuilder
from rag.rag_query import RAGQueryEngine


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _modules_config_path() -> Path:
    return _repo_root() / "config" / "modules.yaml"


def _load_modules() -> dict:
    with _modules_config_path().open(encoding="utf-8") as handle:
        payload = yaml.safe_load(handle) or {}
    return payload.get("modules", {})


def _current_module_key() -> str:
    stem = Path(__file__).stem
    return stem.split("_", 1)[1]


def _format_source_line(index: int, hit) -> str:
    authority_level = hit.metadata.get("authority_level", "unknown")
    source_type = hit.metadata.get("source_type", "unknown")
    citation = hit.metadata.get("short_citation") or hit.metadata.get("citation") or "okänd referens"
    return f"[{index}] {authority_level} | {source_type} | {citation} | score: {hit.score:.3f}"


def _is_block_message(text: str) -> bool:
    normalized = text.lower()
    return any(
        marker in normalized
        for marker in ("straffrätt", "skatte", "migrations", "asyl", "kontakta en advokat")
    )


@st.cache_resource
def _get_engine() -> RAGQueryEngine:
    return RAGQueryEngine()


module_key = _current_module_key()
modules = _load_modules()
module_config = modules[module_key]
rag_module = module_config["rag_module"]
label = str(module_config.get("label", module_key))
icon = str(module_config.get("icon", ""))

if "engine" not in st.session_state:
    st.session_state.engine = _get_engine()
if "llm" not in st.session_state:
    st.session_state.llm = get_llm_client()
if "messages" not in st.session_state:
    st.session_state.messages = []

st.title(f"{icon} {label}".strip())

for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        if message.get("warning"):
            st.warning(message["content"], icon="🚫")
        else:
            st.markdown(message["content"])
        if message.get("sources"):
            with st.expander(f"📚 Källor ({len(message['sources'])} st.)"):
                for source in message["sources"]:
                    st.markdown(source)

prompt = st.chat_input("Ställ din juridiska fråga...")

if prompt:
    user_message = {"role": "user", "content": prompt}
    st.session_state.messages.append(user_message)

    with st.chat_message("user"):
        st.markdown(prompt)

    with st.chat_message("assistant"):
        with st.spinner("Söker i rättskällor..."):
            result = st.session_state.engine.query(prompt, module=rag_module, n_results=8)

            if result.hits == []:
                warning = _is_block_message(result.disclaimer)
                if warning:
                    st.warning(result.disclaimer, icon="🚫")
                else:
                    st.markdown(result.disclaimer)
                st.session_state.messages.append(
                    {
                        "role": "assistant",
                        "content": result.disclaimer,
                        "warning": warning,
                        "sources": [],
                    }
                )
            else:
                prompt_builder = PromptBuilder()
                message_history = [
                    {"role": message["role"], "content": message["content"]}
                    for message in st.session_state.messages
                ]
                system_prompt = prompt_builder.build_system_prompt(result, module=rag_module)
                answer = st.session_state.llm.chat(system_prompt, message_history)
                st.markdown(answer)

                source_lines = [
                    _format_source_line(index, hit)
                    for index, hit in enumerate(result.hits, start=1)
                ]
                with st.expander(f"📚 Källor ({len(source_lines)} st.)"):
                    for source_line in source_lines:
                        st.markdown(source_line)

                st.session_state.messages.append(
                    {
                        "role": "assistant",
                        "content": answer,
                        "warning": False,
                        "sources": source_lines,
                    }
                )
