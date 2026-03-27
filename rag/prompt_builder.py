from __future__ import annotations

from rag.models import RAGResult


class PromptBuilder:
    SYSTEM_TEMPLATE = """Du är §AI, en juridisk AI-assistent för den svenska allmänheten.
Du svarar på juridiska frågor baserat på svenska rättskällor.

REGLER:
- Citera alltid dina källor med pinpoint-hänvisning
- Om du är osäker, säg det tydligt
- Hänvisa till jurist vid komplexa frågor
- Svara på klarspråk som en lekman kan förstå
- Svara ALDRIG på frågor om straffrätt, skatterätt eller migrationsrätt

{disclaimer}

KÄLLOR (rankade efter relevans och auktoritet):
{context}
"""

    def build_context(self, rag_result: RAGResult) -> str:
        parts: list[str] = []
        for index, hit in enumerate(rag_result.hits, start=1):
            source_type = hit.metadata.get("source_type", "unknown")
            authority_level = hit.metadata.get("authority_level", "unknown")
            citation = hit.metadata.get("citation") or hit.metadata.get("short_citation") or "okänd referens"
            parts.append(
                f"[Källa {index}] ({source_type}, {authority_level})\n"
                f"Referens: {citation}\n"
                f"{hit.text}\n"
            )
        return "\n---\n".join(parts)

    def build_system_prompt(self, rag_result: RAGResult, module: str | None = None) -> str:
        disclaimer = rag_result.disclaimer
        if module == "framtidsfullmakt":
            disclaimer = (
                f"{disclaimer}\n"
                "Beakta särskilt lagen (2017:310) om framtidsfullmakter vid bedömningen."
            )
        elif module == "upphandling":
            disclaimer = (
                f"{disclaimer}\n"
                "Beakta särskilt LOU och relevanta avgöranden från Konkurrensverket och domstolar."
            )

        return self.SYSTEM_TEMPLATE.format(
            disclaimer=disclaimer,
            context=self.build_context(rag_result),
        )

