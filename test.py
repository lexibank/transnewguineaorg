def test_valid(cldf_dataset, cldf_logger):
    assert cldf_dataset.validate(log=cldf_logger)


def test_forms(cldf_dataset):
    assert len(list(cldf_dataset["FormTable"])) == 146463 

    # check one specific form to make sure columns, values are correct.
    # 49995,karoto,120_father,aɸa
    f = [f for f in cldf_dataset["FormTable"] if f["Local_ID"] == "49995"]
    assert len(f) == 1
    assert f[0]["Parameter_ID"] == "120_father"
    assert f[0]["Language_ID"] == "karoto"
    assert f[0]["Form"] == "aɸa"


def test_languages(cldf_dataset):
    assert len(list(cldf_dataset["LanguageTable"])) == 1017

    # ID,Name,Glottocode,Glottolog_Name,ISO639P3code,Macroarea,Family
    # abaga,Abaga,abag1245,,abg,,
    f = [f for f in cldf_dataset["LanguageTable"] if f["ID"] == "abaga"]
    assert len(f) == 1
    assert f[0]["Name"] == "Abaga"
    assert f[0]["Glottocode"] == "abag1245"
    assert f[0]["ISO639P3code"] == "abg"


def test_parameters(cldf_dataset):
    assert len(list(cldf_dataset["ParameterTable"])) == 1166

    # ID,Name,Concepticon_ID,Concepticon_Gloss
    # 796_tocough,to cough,879,COUGH
    f = [f for f in cldf_dataset["ParameterTable"] if f["ID"] == "796_tocough"]
    assert len(f) == 1
    assert f[0]["Name"] == "to cough"
    assert f[0]["Concepticon_ID"] == "879"
    assert f[0]["Concepticon_Gloss"] == "COUGH"


def test_sources(cldf_dataset):
    f = [f for f in cldf_dataset.sources if f.id == "abbott1985"]
    assert len(f) == 1
