# Copyright 2026 David Eliasson
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


"""
Pydantic-modeller för HFD Rättspraxis API.

Dessa modeller representerar strukturen på API-svar från
rattspraxis.etjanst.domstol.se/api/v1/
"""

from datetime import datetime
from uuid import UUID

from pydantic import BaseModel, Field


class Domstol(BaseModel):
    """Domstolsinformation."""

    domstolKod: str = Field(..., description="Domstolskod, t.ex. 'HFD', 'REGR', 'HDO'")
    domstolNamn: str = Field(..., description="Fullständigt domstolsnamn")


class Lagrum(BaseModel):
    """Lagrum med SFS-nummer."""

    referens: str = Field(..., description="Full lagrumreferens, t.ex. '3 kap. 6 § miljöbalken (1998:808)'")
    sfsNummer: str | None = Field(None, description="SFS-nummer, t.ex. '1998:808'. None för EU-artiklar, föreskrifter etc.")


class Litteratur(BaseModel):
    """Litteraturreferens från API."""

    forfattare: str = Field(..., description="Författare/redaktör")
    titel: str = Field(..., description="Verktitel inkl. eventuell sidangivelse")


class HanvisadPublicering(BaseModel):
    """Hänvisning till annat avgörande."""

    fritext: str = Field(..., description="Referenstext, t.ex. 'HFD 2020 ref. 45'")
    gruppKorrelationsnummer: UUID | None = Field(
        None, description="UUID för att koppla till annat avgörande"
    )


class Bilaga(BaseModel):
    """Bifogad fil (t.ex. PDF av beslut)."""

    fillagringId: str = Field(..., description="ID för nedladdning via /api/v1/bilagor/{id}")
    filstorlek: int | None = Field(None, description="Filstorlek i bytes")
    filtyp: str | None = Field(None, description="MIME-typ")


class Publication(BaseModel):
    """
    Enskild publicering från HFD (referat/notis).

    Denna modell representerar en fullständig post från API:t.
    För intern bearbetning används PublicationProcessed.
    """

    id: UUID = Field(..., description="Primärnyckel från API")
    typ: str = Field(..., description="REFERAT | NOTIS | DOM_ELLER_BESLUT etc.")

    domstol: Domstol
    referatNummerLista: list[str] = Field(
        default_factory=list, description="T.ex. ['HFD 2011 ref. 1']"
    )
    malNummerLista: list[str] = Field(
        default_factory=list, description="T.ex. ['4033-09', '4034-09']"
    )

    avgorandedatum: str = Field(..., description="Avgörandedatum, YYYY-MM-DD")
    publiceringstid: datetime = Field(..., description="När publicerades i API")

    sammanfattning: str | None = Field(None, description="Kort sammanfattning")
    innehall: str | None = Field(None, description="Full HTML av referatet")

    lagrumLista: list[Lagrum] = Field(default_factory=list)
    litteraturLista: list[Litteratur] = Field(default_factory=list)
    forarbeteLista: list[str] = Field(default_factory=list)
    hanvisadePubliceringarLista: list[HanvisadPublicering] = Field(default_factory=list)
    europarattsligaAvgorandenLista: list[str] = Field(default_factory=list)
    nyckelordLista: list[str] = Field(default_factory=list)
    rattsomradeLista: list[str] = Field(default_factory=list)

    bilagaLista: list[Bilaga] = Field(default_factory=list)
    arVagledande: bool = Field(False, description="Är avgörandet vägledande?")
    gruppKorrelationsnummer: UUID | None = Field(
        None, description="Grupperings-ID för relaterade avgöranden"
    )


class SearchFilter(BaseModel):
    """Filter för sökning."""

    avgorandeTypLista: list[str] = Field(
        default_factory=lambda: ["REFERAT"],
        description="REFERAT, NOTIS, DOM_ELLER_BESLUT, PROVNINGSTILLSTAND, FORHANDSAVGORANDE",
    )
    intervall: dict = Field(
        default_factory=lambda: {"fromDatum": "2011-01-01", "toDatum": None},
        description="Datumintervall {fromDatum, toDatum}",
    )
    rattsomradeLista: list[str] = Field(default_factory=list)
    sfsNummerLista: list[str] = Field(default_factory=list)
    arVagledande: bool = Field(False, description="Filtrera på vägledande avgöranden")
    sokordLista: list[str] = Field(default_factory=list)
    domstolKodLista: list[str] = Field(
        default_factory=lambda: ["HFD"], description="T.ex. ['HFD'], ['REGR'], ['HDO']"
    )


class SearchRequest(BaseModel):
    """Sökförfrågan till /api/v1/sok."""

    antalPerSida: int = Field(100, ge=1, le=100, description="Max 100 per sida")
    asc: bool = Field(True, description="true = äldst först, false = nyast först")
    sidIndex: int = Field(0, ge=0, description="0-baserat sidindex")
    filter: SearchFilter = Field(default_factory=SearchFilter)
    sortorder: str = Field("avgorandedatum", description="Sorteringsordning")


class SearchResponse(BaseModel):
    """Svar från /api/v1/sok."""

    publiceringLista: list[Publication] = Field(
        default_factory=list, description="Lista av publikationer"
    )
    total: int = Field(0, description="Totalt antal träffar")


class PublicationProcessed(BaseModel):
    """
    Bearbetad publikation för intern lagring (kanoniskt format).

    Denna struktur används i data/processed/ och matchar metadata-schemat
    från projektspecifikationen (DEL 4).
    """

    schema_version: str = Field("publication_v1", description="Schemaversion")

    identitet: dict = Field(
        ...,
        description="""
        {
            api_id: str,
            domstol: str,
            typ: str,
            referat_nummer: str,
            year: int,
            ref_no: int,
            ref_no_padded: str,
            malnummer_lista: List[str],
            malnummer_primart: str,
            grupp_id: Optional[str]
        }
        """,
    )

    datum: dict = Field(
        ...,
        description="""
        {
            avgorandedatum: str,
            publiceringstid: str
        }
        """,
    )

    innehall: dict = Field(
        ...,
        description="""
        {
            sammanfattning: Optional[str],
            innehall_html: Optional[str],
            innehall_text_length: int,
            ar_vagledande: bool
        }
        """,
    )

    juridik: dict = Field(
        ...,
        description="""
        {
            lagrum: List[dict],
            forarbeten: List[str],
            rattsomrade: List[str],
            nyckelord: List[str],
            eu_rattsmal: List[str],
            hanvisade_avgoranden: List[dict]
        }
        """,
    )

    doktrin: dict = Field(
        ...,
        description="""
        {
            litteratur_api: List[dict],
            litteratur_extraherad: List[dict]
        }
        """,
    )

    filer: dict = Field(
        ...,
        description="""
        {
            filnamn_json: str,
            filnamn_pdf: Optional[str],
            sha256_json: str,
            sha256_pdf: Optional[str]
        }
        """,
    )

    harvest: dict = Field(
        ...,
        description="""
        {
            harvest_timestamp: str,
            api_source: str,
            api_version: str,
            harvester_version: str
        }
        """,
    )


class MasterListEntry(BaseModel):
    """En rad i masterlist.csv."""

    api_id: UUID
    domstol: str
    typ: str
    referat_nummer: str
    year: int
    ref_no: int
    ref_no_padded: str
    malnummer_primart: str
    avgorandedatum: str
    filnamn_json: str
    sha256_json: str
    harvest_timestamp: str
