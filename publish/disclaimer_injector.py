"""Inject a fixed legal disclaimer into LLM responses."""

import datetime
import logging


class DisclaimerInjector:
    """Append the architecture-decided legal disclaimer to response text."""

    _logger = logging.getLogger("paragrafenai.noop")
    _separator = "\n\n---\n"
    _disclaimer_template = (
        "⚠️ *Detta är juridisk information, inte juridisk rådgivning. "
        "Kontrollera alltid mot primärkällan. Uppdaterad per {date}.*"
    )
    _sources_template = "*Källor: {sources}*"

    def inject(
        self,
        response_text: str,
        sources: list[str] | None = None,
        date: str | None = None,
    ) -> str:
        """
        Lägg till disclaimer i slutet av ett svar.

        Args:
            response_text: LLM-svaret som ska kompletteras.
            sources: Lista med källreferenser.
            date: Datum i ISO-format "YYYY-MM-DD". Om None: använd today().

        Returns:
            response_text + disclaimer-fotnot.
        """
        effective_date = date or datetime.date.today().isoformat()
        disclaimer_lines = [self._disclaimer_template.format(date=effective_date)]

        if sources:
            disclaimer_lines.append(
                self._sources_template.format(sources=" · ".join(sources))
            )

        disclaimer_body = "\n".join(disclaimer_lines)
        disclaimer_block = f"---\n{disclaimer_body}"

        if not response_text:
            return disclaimer_block

        if response_text.rstrip().endswith("---"):
            return f"{response_text.rstrip()}\n{disclaimer_body}"

        return f"{response_text}{self._separator}{disclaimer_body}"
