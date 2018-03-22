import datetime
import pandas as pd


def calc_traveltime_df(train_stop_from_df, train_stop_to_df):
    """
    Calculates the differences between 'train_stop_from_df' and 'train_stop_to_df'.
    'train_stop_from_df': pandas dataframe. Input for the train stop the train comes from.
    'train_stop_to_df': pandas dataframe. Input for the train stop the train arrives at.
    Returns a pandas dataframe with columns 'traveltime', 'ttsid_from', 'ttsid_to'.
    """
    arrival = datetime.datetime.combine(train_stop_to_df["datum"][0],
        (datetime.datetime.min + train_stop_to_df["arzeitist"][0]).time())
    departure = datetime.datetime.combine(train_stop_to_df["datum"][0],
        (datetime.datetime.min + train_stop_from_df["dpzeitist"][0]).time())

    traveltime = arrival - departure
    ttsid_from = train_stop_from_df["ttsid"][0]
    ttsid_to = train_stop_to_df["ttsid"][0]
    result = pd.DataFrame(
        data=[[traveltime, ttsid_from, ttsid_to]],
        columns=["traveltime", "ttsid_from", "ttsid_to"])
    return result


def calc_staytime_df(train_stop):
    """
    Calculates the staytime at train stop.
    'train_stop': pandas dataframe. Input for the train stop the train stays at.
    Returns a pandas dataframe with columns 'staytime', 'ttsid'.
    """
    if train_stop["arzeitist"][0] is None or train_stop["dpzeitist"][0] is None:
        staytime = datetime.timedelta(0)
    else:
        arrival = datetime.datetime.combine(train_stop["datum"][0],
            (datetime.datetime.min + train_stop["arzeitist"][0]).time())
        departure = datetime.datetime.combine(train_stop["datum"][0],
            (datetime.datetime.min + train_stop["dpzeitist"][0]).time())
        staytime = departure - arrival

    ttsid = train_stop["ttsid"][0]
    result = pd.DataFrame(data=[[staytime, ttsid]], columns=["staytime", "ttsid"])
    return result