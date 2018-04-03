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
    assert df["trainnumberfull"][0] == assert_trainnummberfull


def test__datetoyymmddhhmm():
    date = "2017-12-11"
    assert_yymmddhhmm = 1712111635
    dailytripid = 5212057309500211661
    df = user_utilities.datetoyymmddhhmm(date, dailytripid)
    assert df["yymmddhhmm"][0] == assert_yymmddhhmm


def test__yymmddhhmmtodate():
    assert_date = "2017-12-11"
    yymmddhhmm = "1712111109"
    df = user_utilities.yymmddhhmmtodate(yymmddhhmm)
    assert assert_date == df["datum"][0]
    pass
