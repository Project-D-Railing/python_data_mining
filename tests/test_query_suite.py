import pytest
import database_connection
import query_suite
import datetime
import pandas as pd


@pytest.fixture(scope="module")
def dbc():
    #setup database connection
    dbc = database_connection.connect_with_config(
        config="app_config.json", property="dbcconfig")
    yield dbc
    #teardown
    dbc.close()


@pytest.fixture()
def qs(dbc):
    #setup query suite pandas
    qs = query_suite.QuerySuite()
    qs.use_dbc(dbc)
    yield qs
    #teardown


def test__get_tts_by_ttsid_query__check_query_result(qs:query_suite.QuerySuite):
    tts = qs._get_tts_by_ttsid_query("-4535481750358179783-1712111144-6")
    tobe = ((5405925, '-4535481750358179783-1712111144-6', -4535481750358179783, 1712111144, 6, 'S', 'p', '800725',
            'S', '6357', 'S6357', '3', 8004667, datetime.timedelta(0, 43080), datetime.timedelta(0, 43440),
            datetime.timedelta(0, 43140), datetime.timedelta(0, 43440), '1', '1', datetime.date(2017, 12, 11),
            '84d1366a', '8d7bc197', 'n'),)
    assert tts == tobe


def test_get_tts_by_ttsid__check_query_result_and_df_structure(qs:query_suite.QuerySuite):
    tts = qs.get_tts_by_ttsid("-4535481750358179783-1712111144-6")
    assert tts["id"][0] == 5405925
    assert tts["ttsid"][0] == '-4535481750358179783-1712111144-6'
    assert tts["dailytripid"][0] == -4535481750358179783
    assert tts["yymmddhhmm"][0] == 1712111144
    assert tts["stopindex"][0] == 6
    assert tts["zugverkehrstyp"][0] == 'S'
    assert tts["zugtyp"][0] == 'p'
    assert tts["zugowner"][0] == '800725'
    assert tts["zugklasse"][0] == 'S'
    assert tts["zugnummer"][0] == '6357'
    assert tts["zugnummerfull"][0] == 'S6357'
    assert tts["linie"][0] == '3'
    assert tts["evanr"][0] == 8004667
    assert tts["arzeitsoll"][0] == datetime.timedelta(0, 43080)
    assert tts["arzeitist"][0] == datetime.timedelta(0, 43440)
    assert tts["dpzeitsoll"][0] == datetime.timedelta(0, 43140)
    assert tts["dpzeitist"][0] == datetime.timedelta(0, 43440)
    assert tts["gleissoll"][0] == '1'
    assert tts["gleisist"][0] == '1'
    assert tts["datum"][0] == datetime.date(2017, 12, 11)
    assert tts["streckengeplanthash"][0] == '84d1366a'
    assert tts["streckenchangedhash"][0] == '8d7bc197'
    assert tts["zugstatus"][0] == 'n'


def test__get_tts_with_stationnames_on_trip_query(qs:query_suite.QuerySuite):
    tts = qs._get_tts_with_stationnames_on_trip_query(dailytripid=-9190105377711815761, yymmddhhmm=1712111109)
    tobe = ((5405936, '-9190105377711815761-1712111109-1', -9190105377711815761, 1712111109, 1, 'N', 'p', 'K4RB',
             'RB', '24373', 'RB24373', '92', 8004674, None, None, datetime.timedelta(0, 40140),
             datetime.timedelta(0, 40140), '1', '1', datetime.date(2017, 12, 11), '822d1456', '2182c154', 'n', 'Olpe'),
            (5407583, '-9190105377711815761-1712111109-3', -9190105377711815761, 1712111109, 3, 'N', 'p', 'K4RB',
             'RB', '24373', 'RB24373', '92', 8005600, datetime.timedelta(0, 40560), datetime.timedelta(0, 40560),
             datetime.timedelta(0, 40560), datetime.timedelta(0, 40560), '2', '2', datetime.date(2017, 12, 11),
             '822d1456', '36d480cf', 'n', 'Sondern'))
    assert tts == tobe


def test_get_tts_with_stationnames_on_trip(qs:query_suite.QuerySuite):
    tts = qs.get_tts_with_stationnames_on_trip(dailytripid=-9190105377711815761, yymmddhhmm=1712111109)
    assert tts["id"][0] == 5405936
    assert tts["ttsid"][0] == '-9190105377711815761-1712111109-1'
    assert tts["dailytripid"][0] == -9190105377711815761
    assert tts["yymmddhhmm"][0] == 1712111109
    assert tts["stopindex"][0] == 1
    assert tts["zugverkehrstyp"][0] == 'N'
    assert tts["zugtyp"][0] == 'p'
    assert tts["zugowner"][0] == 'K4RB'
    assert tts["zugklasse"][0] == 'RB'
    assert tts["zugnummer"][0] == '24373'
    assert tts["zugnummerfull"][0] == 'RB24373'
    assert tts["linie"][0] == '92'
    assert tts["evanr"][0] == 8004674
    assert pd.isnull(tts["arzeitsoll"][0])
    assert pd.isnull(tts["arzeitist"][0])
    assert tts["dpzeitsoll"][0] == datetime.timedelta(0, 40140)
    assert tts["dpzeitist"][0] == datetime.timedelta(0, 40140)
    assert tts["gleissoll"][0] == '1'
    assert tts["gleisist"][0] == '1'
    assert tts["datum"][0] == datetime.date(2017, 12, 11)
    assert tts["streckengeplanthash"][0] == '822d1456'
    assert tts["streckenchangedhash"][0] == '2182c154'
    assert tts["stationname"][0] == 'Olpe'
    assert tts["zugstatus"][0] == 'n'