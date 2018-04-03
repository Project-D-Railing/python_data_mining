import user_utilities

def test__get_tts_by_ttsid_query__check_query_result():
    trainnummberful = "RE11350"
    assert_dailytripid = 5212057309500211661
    df = user_utilities.trainnumberfulltodailytripid(trainnummberful)
    assert df["dailytripid"][0] == assert_dailytripid


def test__dailytripidtotrainnumberfull():
    assert_trainnummberfull = "RE11350"
    dailytripid = 5212057309500211661
    df = user_utilities.dailytripidtotrainnumberfull(dailytripid)
    # assert df["trainnummberfull"][0] == assert_trainnummberfull
    pass


