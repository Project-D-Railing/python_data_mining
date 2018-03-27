import pytest
import pandas as pd
import processing_utils as pu
import datetime


@pytest.fixture()
def testsample():
    # load test sample from serialized query result
    # tts_with_stationname_df = qs.get_tts_with_stationnames_on_trip(
    #    dailytripid=1307784265419680067, yymmddhhmm=1712111209)
    testsample = pd.read_pickle("testsample.p")
    return testsample


def test__calc_staytime_scheduled_df__sample(testsample):
    stop2 = testsample[12:13]
    result = pu.calc_staytime_scheduled_df(stop2)["staytime_scheduled"].iloc[0]
    assert result == datetime.timedelta(minutes=1)


def test__calc_staytime_real_df__sample(testsample):
    stop2 = testsample[12:13]
    result = pu.calc_staytime_real_df(stop2)["staytime_real"].iloc[0]
    assert result == datetime.timedelta(minutes=1)


def test__calc_delay_by_staytime_df__sample(testsample):
    stop2 = testsample[12:13]
    result = pu.calc_delay_by_staytime_df(stop2)["delay_by_staytime"].iloc[0]
    assert result == datetime.timedelta(minutes=0)


def test__calc_traveltime_scheduled_df__sample(testsample):
    stop1 = testsample[11:12]
    stop2 = testsample[12:13]
    result = pu.calc_traveltime_scheduled_df(stop1, stop2)["traveltime_scheduled"].iloc[0]
    assert result == datetime.timedelta(minutes=13)


def test__calc_traveltime_real_df__sample(testsample):
    stop1 = testsample[11:12]
    stop2 = testsample[12:13]
    result = pu.calc_traveltime_real_df(stop1, stop2)["traveltime_real"].iloc[0]
    assert result == datetime.timedelta(minutes=12)


def test__calc_delay_by_traveltime_df__sample(testsample):
    stop1 = testsample[11:12]
    stop2 = testsample[12:13]
    result = pu.calc_delay_by_traveltime_df(stop1, stop2)["delay_by_traveltime"].iloc[0]
    assert result == datetime.timedelta(minutes=-1)


def test__calc_delay_at_arrival_df__sample(testsample):
    stop2 = testsample[12:13]
    result = pu.calc_delay_at_arrival_df(stop2)["delay_at_arrival"].iloc[0]
    assert result == datetime.timedelta(minutes=4)


def test__calc_delay_at_departure_df__sample(testsample):
    stop2 = testsample[12:13]
    result = pu.calc_delay_at_departure_df(stop2)["delay_at_departure"].iloc[0]
    assert result == datetime.timedelta(minutes=4)


def test__calc_traveltime_scheduled_df__first_arg_is_none(testsample):
    stop2 = testsample[0:1]
    result = pu.calc_traveltime_scheduled_df(None, stop2)["traveltime_scheduled"].iloc[0]
    assert pd.isnull (result)


def test__calc_traveltime_scheduled_df__second_arg_is_none(testsample):
    stop1 = testsample[0:1]
    result = pu.calc_traveltime_scheduled_df(stop1, None)["traveltime_scheduled"].iloc[0]
    assert pd.isnull (result)


def test__calc_traveltime_real_df__first_arg_is_none(testsample):
    stop2 = testsample[0:1]
    result = pu.calc_traveltime_real_df(None, stop2)["traveltime_real"].iloc[0]
    assert pd.isnull (result)


def test__calc_traveltime_real_df__second_arg_is_none(testsample):
    stop1 = testsample[0:1]
    result = pu.calc_traveltime_real_df(stop1, None)["traveltime_real"].iloc[0]
    assert pd.isnull (result)


def test__calc_delay_by_traveltime_df__first_arg_is_none(testsample):
    stop2 = testsample[0:1]
    result = pu.calc_delay_by_traveltime_df(None, stop2)["delay_by_traveltime"].iloc[0]
    assert pd.isnull (result)


def test__calc_delay_by_traveltime_df__second_arg_is_none(testsample):
    stop1 = testsample[0:1]
    result = pu.calc_delay_by_traveltime_df(stop1, None)["delay_by_traveltime"].iloc[0]
    assert pd.isnull (result)


def test__calc_staytime_scheduled_df__start_of_trip(testsample):
    stop1 = testsample[0:1]
    result = pu.calc_staytime_scheduled_df(stop1)["staytime_scheduled"].iloc[0]
    assert pd.isnull (result)


def test__calc_staytime_real_df__start_of_trip(testsample):
    stop1 = testsample[0:1]
    result = pu.calc_staytime_real_df(stop1)["staytime_real"].iloc[0]
    assert pd.isnull (result)


def test__calc_delay_by_staytime_df__start_of_trip(testsample):
    stop1 = testsample[0:1]
    result = pu.calc_delay_by_staytime_df(stop1)["delay_by_staytime"].iloc[0]
    assert pd.isnull (result)


def test__calc_delay_at_arrival_df__start_of_trip(testsample):
    stop1 = testsample[0:1]
    result = pu.calc_delay_at_arrival_df(stop1)["delay_at_arrival"].iloc[0]
    assert pd.isnull (result)


def test_calc_delay_at_departure_df__end_of_trip(testsample):
    trip_len = len(testsample)
    stop_end = testsample[trip_len-1:trip_len]
    result = pu.calc_delay_at_departure_df(stop_end)["delay_at_departure"].iloc[0]
    assert pd.isnull (result)