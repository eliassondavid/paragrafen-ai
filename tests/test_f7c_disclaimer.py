import datetime

from publish.disclaimer_injector import DisclaimerInjector


def test_basic_inject_contains_disclaimer():
    injector = DisclaimerInjector()

    result = injector.inject("Det här är ett svar.", date="2026-02-28")

    assert "Det här är ett svar." in result
    assert "\n\n---\n" in result
    assert (
        "⚠️ *Detta är juridisk information, inte juridisk rådgivning. "
        "Kontrollera alltid mot primärkällan. Uppdaterad per 2026-02-28.*"
    ) in result


def test_sources_are_joined_with_middle_dot_separator():
    injector = DisclaimerInjector()
    sources = [
        "SFS 1949:381 6 kap. 1 §",
        "NJA 2022 s. 123",
        "prop. 2016/17:180 s. 45",
    ]

    result = injector.inject("Svar", sources=sources, date="2026-02-28")

    assert "*Källor: SFS 1949:381 6 kap. 1 § · NJA 2022 s. 123 · prop. 2016/17:180 s. 45*" in result


def test_sources_none_omits_sources_line():
    injector = DisclaimerInjector()

    result = injector.inject("Svar", sources=None, date="2026-02-28")

    assert "*Källor:" not in result


def test_sources_empty_list_omits_sources_line():
    injector = DisclaimerInjector()

    result = injector.inject("Svar", sources=[], date="2026-02-28")

    assert "*Källor:" not in result


def test_date_uses_given_value_or_today(monkeypatch):
    injector = DisclaimerInjector()
    explicit = injector.inject("Svar", date="2025-01-01")
    assert "Uppdaterad per 2025-01-01." in explicit

    class FakeDate(datetime.date):
        @classmethod
        def today(cls):
            return cls(2030, 12, 24)

    monkeypatch.setattr("publish.disclaimer_injector.datetime.date", FakeDate)
    default_date = injector.inject("Svar")
    assert "Uppdaterad per 2030-12-24." in default_date


def test_empty_response_still_returns_disclaimer():
    injector = DisclaimerInjector()

    result = injector.inject("", date="2026-02-28")

    assert result
    assert result.startswith("---\n")
    assert "Uppdaterad per 2026-02-28." in result
