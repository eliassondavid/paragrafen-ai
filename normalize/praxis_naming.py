"""Filnamnskonvention och parserhjälpare för domstolsdata."""

import re

import structlog

logger = structlog.get_logger()


class MalnummerParser:
    """Parser för målnummer med stöd för flera domstolsformat."""

    SINGLE_PATTERNS = (
        # Prefix + årsuffix (t.ex. T 1234-22, UM 12369-24, PMT 10755-25, ÖÄ 717-20)
        re.compile(r"\b([A-ZÅÄÖ]{1,6}\s+\d{1,6}-\d{2})\b", re.IGNORECASE),
        # Prefix + slash (t.ex. A 153/24)
        re.compile(r"\b([A-ZÅÄÖ]{1,6}\s+\d{1,6}/\d{2})\b", re.IGNORECASE),
        # Standard utan prefix (t.ex. 4033-09)
        re.compile(r"(?<!\d)(\d{1,6}-\d{2})(?!\d)"),
        # Year format (t.ex. 2016-9)
        re.compile(r"\b(\d{4}-\d{1,3})\b"),
        # Short format (t.ex. 10-292)
        re.compile(r"\b(\d{1,3}-\d{1,3})\b"),
    )

    RANGE_PATTERN = re.compile(r"(?<!\d)(\d{1,6})\s*(?:--|–|-)\s*(\d{1,6})\s*-\s*(\d{2})(?!\d)")

    @classmethod
    def normalize_interval_chars(cls, text: str) -> str:
        """Normaliserar olika intervalltecken."""
        if text is None:
            return ""
        text = text.replace("–", "--")
        text = text.replace("—", "--")
        return text

    @classmethod
    def split_list(cls, text: str) -> list[str]:
        """Delar upp lista av målnummer på vanligt förekommande avgränsare."""
        # Ta bort prefix som "Mål nr.", "Mål:", "mål nr"
        text = re.sub(r"(Mål|mål)\s*(nr\.?|:)\s*", "", text, flags=re.IGNORECASE)
        # Byt ut "och" och "samt" mot komma (case-insensitive)
        text = re.sub(r"\s+och\s+", ",", text, flags=re.IGNORECASE)
        text = re.sub(r"\s+samt\s+", ",", text, flags=re.IGNORECASE)
        # Splitta på komma och semikolon
        tokens = re.split(r"[,;]", text)
        return [token.strip() for token in tokens if token.strip()]

    @classmethod
    def parse_single(cls, token: str) -> str | None:
        """Parsning av ett enskilt målnummer-token."""
        for pattern in cls.SINGLE_PATTERNS:
            match = pattern.search(token)
            if match:
                result = match.group(1)
                # Normalisera whitespace i prefix (T   1234-22 → T 1234-22)
                result = re.sub(r"\s+", " ", result).strip()
                return result

        # Sista fallback: behåll token om den innehåller siffror
        token_norm = re.sub(r"\s+", " ", token).strip()
        if any(char.isdigit() for char in token_norm):
            return token_norm
        return None

    @classmethod
    def parse_range(cls, token: str) -> tuple[int, int, str] | None:
        """Parsning av intervall i formen start-slut-år."""
        match = cls.RANGE_PATTERN.search(token)
        if not match:
            return None
        return int(match.group(1)), int(match.group(2)), match.group(3)

    @classmethod
    def expand_range(cls, start: int, end: int, year: str) -> list[str]:
        """Expanderar intervall till en lista av målnummer."""
        if end < start:
            start, end = end, start
        return [f"{number}-{year}" for number in range(start, end + 1)]

    @classmethod
    def parse_malnummer_lista(cls, raw_lista: list[str]) -> tuple[list[str], str]:
        """Parser för `malNummerLista` från API."""
        if not raw_lista:
            logger.warning("parse_malnummer_lista_empty")
            return ([], "UNKNOWN")

        all_malnummer: list[str] = []

        for raw in raw_lista:
            # Skippa None-värden
            if raw is None:
                logger.warning("parse_malnummer_none_in_list")
                continue
                
            normalized = cls.normalize_interval_chars(raw)
            tokens = cls.split_list(normalized)

            for token in tokens:
                range_match = cls.parse_range(token)
                if range_match:
                    start, end, year = range_match
                    all_malnummer.extend(cls.expand_range(start, end, year))
                    continue

                single = cls.parse_single(token)
                if single:
                    all_malnummer.append(single)
                else:
                    logger.warning("parse_malnummer_failed", token=token, original=raw)

        primary = all_malnummer[0] if all_malnummer else "UNKNOWN"
        return (all_malnummer, primary)


def sanitize_malnummer_for_filename(malnummer: str) -> str:
    """Normaliserar målnummer till filnamnssäker komponent."""
    value = malnummer.strip()
    value = value.replace("/", "-")
    value = re.sub(r"\s+", "", value)
    value = value.replace("–", "-")
    value = re.sub(r"[^0-9A-Za-zÅÄÖåäö-]", "", value)
    return value or "UNKNOWN"


def generate_filename(
    domstol: str,
    year: int,
    ref_no: int,
    malnummer_primart: str,
    extension: str = "json",
) -> str:
    """Genererar filnamn i formatet `{DOMSTOL}_{YEAR}_ref-{NNN}__mal-{MALNR}.{ext}`."""
    court = domstol.upper().strip()
    if not court:
        raise ValueError("domstol får inte vara tom")

    ref_no_padded = f"{ref_no:03d}"
    malnr_component = sanitize_malnummer_for_filename(malnummer_primart)
    return f"{court}_{year}_ref-{ref_no_padded}__mal-{malnr_component}.{extension}"


def generate_filename_dom(
    domstol: str,
    year: int,
    malnummer_primart: str,
    extension: str = "json",
) -> str:
    """Genererar filnamn för DOM_ELLER_BESLUT i formatet `{DOMSTOL}_{YEAR}_dom__mal-{MALNR}.{ext}`."""
    court = domstol.upper().strip()
    if not court:
        raise ValueError("domstol får inte vara tom")

    malnr_component = sanitize_malnummer_for_filename(malnummer_primart)
    return f"{court}_{year}_dom__mal-{malnr_component}.{extension}"


def generate_filename_pt(
    domstol: str,
    year: int,
    malnummer_primart: str,
    extension: str = "json",
) -> str:
    """Genererar filnamn för PROVNINGSTILLSTAND i formatet `{DOMSTOL}_{YEAR}_pt__mal-{MALNR}.{ext}`."""
    court = domstol.upper().strip()
    if not court:
        raise ValueError("domstol får inte vara tom")

    malnr_component = sanitize_malnummer_for_filename(malnummer_primart)
    return f"{court}_{year}_pt__mal-{malnr_component}.{extension}"


def generate_filename_notis(
    domstol: str,
    year: int,
    notis_no: int,
    malnummer_primart: str,
    extension: str = "json",
) -> str:
    """Genererar filnamn för NOTIS i formatet `{DOMSTOL}_{YEAR}_not-{NNN}__mal-{MALNR}.{ext}`."""
    court = domstol.upper().strip()
    if not court:
        raise ValueError("domstol får inte vara tom")

    if notis_no <= 0:
        raise ValueError("notis_no måste vara > 0")

    notis_no_padded = f"{notis_no:03d}"
    malnr_component = sanitize_malnummer_for_filename(malnummer_primart)
    return f"{court}_{year}_not-{notis_no_padded}__mal-{malnr_component}.{extension}"


def generate_filename_for_type(
    domstol: str,
    typ: str,
    year: int,
    ref_no: int,
    malnummer_primart: str,
    extension: str = "json",
) -> str:
    """Genererar filnamn enligt avgörandetyp med bakåtkompatibla fallback-regler."""
    typ_norm = typ.upper().strip()

    if typ_norm == "DOM_ELLER_BESLUT":
        return generate_filename_dom(
            domstol=domstol,
            year=year,
            malnummer_primart=malnummer_primart,
            extension=extension,
        )
    if typ_norm == "PROVNINGSTILLSTAND":
        return generate_filename_pt(
            domstol=domstol,
            year=year,
            malnummer_primart=malnummer_primart,
            extension=extension,
        )
    if typ_norm == "NOTIS":
        return generate_filename_notis(
            domstol=domstol,
            year=year,
            notis_no=ref_no,
            malnummer_primart=malnummer_primart,
            extension=extension,
        )

    # Fallback: REFERAT och andra typer
    return generate_filename(
        domstol=domstol,
        year=year,
        ref_no=ref_no,
        malnummer_primart=malnummer_primart,
        extension=extension,
    )


def parse_referat_nummer(referat_nummer: str) -> tuple[int, int]:
    """Parsning av referatnummer till `(year, ref_no)` för flera format."""
    patterns = (
        # HFD/RÅ
        re.compile(r"(?:HFD|RÅ)\s+(\d{4})\s+ref\.\s*(\d+)", re.IGNORECASE),
        # AD
        re.compile(r"[A-ZÅÄÖ]{1,4}\s+(\d{4})\s+nr\s+(\d+)", re.IGNORECASE),
        # RH, RK, MIG, MD, MÖD
        re.compile(r"[A-ZÅÄÖ]{1,4}\s+(\d{4})\s*:\s*(\d+)", re.IGNORECASE),
        # NJA-format
        re.compile(r"[A-ZÅÄÖ]{1,4}\s+(\d{4})\s+s\.\s*(\d+)", re.IGNORECASE),
    )

    for pattern in patterns:
        match = pattern.search(referat_nummer)
        if match:
            return int(match.group(1)), int(match.group(2))

    # Fallback: första årtal + första efterföljande tal
    fallback = re.search(r"(\d{4}).*?(\d{1,4})", referat_nummer)
    if fallback:
        year = int(fallback.group(1))
        ref_no = int(fallback.group(2))
        return year, ref_no

    raise ValueError(f"Kunde inte parsa referatnummer: {referat_nummer}")


def validate_filename(filename: str) -> bool:
    """Validerar filnamn enligt den generaliserade konventionen."""
    pattern = (
        r"^[A-Z]{2,5}_\d{4}_(?:ref-\d{3}|not-\d{3}|dom|pt)"
        r"__mal-[0-9A-Za-zÅÄÖåäö-]+\.(json|pdf)$"
    )
    return bool(re.match(pattern, filename))
