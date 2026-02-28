from normalize.klarsprak_layer import KlarsprakLayer


def test_term_replaced_on_first_occurrence():
    layer = KlarsprakLayer(config_dir="config")
    result = layer.process("Hyresgästen har besittningsskydd.", query="", legal_area=None)
    assert "besittningsskydd (rätten att bo kvar i lägenheten)" in result


def test_term_not_replaced_twice():
    layer = KlarsprakLayer(config_dir="config")
    result = layer.process(
        "Besittningsskydd gäller. Besittningsskydd kan brytas.", query="", legal_area=None
    )
    assert result.count("rätten att bo kvar") == 1


def test_long_sentence_split_at_och():
    long_sentence = (
        "Hyresgästen har rätt att bo kvar i lägenheten, "
        "och detta skydd gäller även om hyresvärden vill säga upp avtalet "
        "av skäl som inte är godkända enligt jordabalken tolfte kapitlet "
        "och frågan samtidigt gäller flera parallella invändningar om "
        "uppsägningstid, kommunicering och proportionalitet i ärendet."
    )
    layer = KlarsprakLayer(config_dir="config")
    result = layer.process(long_sentence, query="", legal_area=None)
    sentences = [s.strip() for s in result.split(".") if s.strip()]
    assert any(len(s.split()) <= 40 for s in sentences)


def test_short_sentence_unchanged():
    short = "Hyresavtalet kan sägas upp med tre månaders varsel."
    layer = KlarsprakLayer(config_dir="config")
    result = layer.process(short, query="", legal_area=None)
    assert "tre månaders varsel" in result


def test_passive_pattern_replaced():
    layer = KlarsprakLayer(config_dir="config")
    result = layer.process("Avtalet kan sägas upp med omedelbar verkan.", query="", legal_area=None)
    assert "kan arbetsgivaren säga upp" in result


def test_unknown_passive_unchanged():
    layer = KlarsprakLayer(config_dir="config")
    text = "Ärendet ska utredas noggrant."
    result = layer.process(text, query="", legal_area=None)
    assert "ska utredas" in result


def test_heading_injected_for_long_answer():
    long_answer = " ".join(["ord"] * 201)
    layer = KlarsprakLayer(config_dir="config")
    result = layer.process(long_answer, query="", legal_area="hyresrätt")
    assert result.startswith("## Vad lagen säger om hyresrätt")


def test_heading_not_injected_for_short_answer():
    short_answer = "Hyresavtalet regleras i jordabalken."
    layer = KlarsprakLayer(config_dir="config")
    result = layer.process(short_answer, query="", legal_area="hyresrätt")
    assert not result.startswith("#")
