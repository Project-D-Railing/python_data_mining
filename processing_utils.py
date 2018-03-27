import datetime
import pandas as pd


# function from https://gist.github.com/hellpanderrr/599bce82ecc6934aa9e1
def _sort_df(self, df, column_idx, key):
    '''Takes dataframe, column index and custom function for sorting,
    returns dataframe sorted by this column using this function'''
    col = df.ix[:, column_idx]
    temp = pd.DataFrame([])
    temp[0] = col
    temp[1] = df.index
    temp = temp.values.tolist()
    df = df.ix[[i[1] for i in sorted(temp, key=key)]]
    return df


def calc_staytime_scheduled_df(train_stop_df):
    """
    Calculates the scheduled staytime at train stop.
    :param train_stop_df: Pandas dataframe. Input for the train stop the train stays at.
    :return: Returns a pandas dataframe with columns 'staytime_scheduled', 'ttsid'.
    """
    ttsid = train_stop_df["ttsid"].iloc[0]
    arzeitsoll = train_stop_df["arzeitsoll"].iloc[0]
    dpzeitsoll = train_stop_df["dpzeitsoll"].iloc[0]

    if pd.isnull(arzeitsoll) or pd.isnull(dpzeitsoll):
        staytime = pd.NaT
    else:
        staytime = dpzeitsoll - arzeitsoll

    result = pd.DataFrame(data=[[staytime, ttsid]], columns=["staytime_scheduled", "ttsid"])
    return result


def calc_staytime_real_df(train_stop_df):
    """
    Calculates the real staytime at train stop.
    :param train_stop_df: Pandas dataframe. Input for the train stop the train stays at.
    :return: Returns a pandas dataframe with columns 'staytime_real', 'ttsid'.
    """
    ttsid = train_stop_df["ttsid"].iloc[0]
    arzeitist = train_stop_df["arzeitist"].iloc[0]
    dpzeitist = train_stop_df["dpzeitist"    ].iloc[0]

    if pd.isnull(arzeitist) or pd.isnull(dpzeitist):
        staytime = pd.NaT
    else:
        staytime = dpzeitist - arzeitist

    result = pd.DataFrame(data=[[staytime, ttsid]], columns=["staytime_real", "ttsid"])
    return result


def calc_delay_by_staytime_df(train_stop_df):
    """
    Calculates the delay that has been caused by the stay of the train.
    Positive value means, that the stay time caused additional delay.
    Negative value means, that the stay time decreased the delay.
    :param train_stop_df: Pandas dataframe. Input for the train stop the train stays at.
    :return: Returns a pandas dataframe with columns 'delay_by_staytime', 'ttsid'.
    """
    staytime_real = calc_staytime_real_df(train_stop_df)["staytime_real"].iloc[0]
    staytime_scheduled = calc_staytime_scheduled_df(train_stop_df)["staytime_scheduled"].iloc[0]
    ttsid = train_stop_df["ttsid"].iloc[0]

    if pd.isnull(staytime_scheduled) or pd.isnull(staytime_real):
        delay = pd.NaT
    else:
        delay = staytime_real - staytime_scheduled

    result = pd.DataFrame(data=[[delay, ttsid]], columns=["delay_by_staytime", "ttsid"])
    return result


def calc_traveltime_scheduled_df(train_stop_from_df, train_stop_to_df):
    """
    Calculates the scheduled travel time between 'train_stop_from_df' and 'train_stop_to_df'.
    :param train_stop_from_df: Pandas dataframe. Input for the train stop the train comes from.
    :param train_stop_to_df: Pandas dataframe. Input for the train stop the train arrives at.
    :return: Returns a pandas dataframe with columns 'traveltime_scheduled', 'ttsid_from', 'ttsid_to'.
    """
    if train_stop_from_df is not None:
        ttsid_from = train_stop_from_df["ttsid"].iloc[0]
    else:
        ttsid_from = None

    if train_stop_to_df is not None:
        ttsid_to = train_stop_to_df["ttsid"].iloc[0]
    else:
        ttsid_to = None

    if train_stop_from_df is None or train_stop_to_df is None:
        traveltime = pd.NaT             # signal that result is not a time value
    else:
        arzeitsoll_to = train_stop_to_df["arzeitsoll"].iloc[0]
        dpzeitsoll_from = train_stop_from_df["dpzeitsoll"].iloc[0]
        traveltime = arzeitsoll_to - dpzeitsoll_from

    result = pd.DataFrame(
        data=[[traveltime, ttsid_from, ttsid_to]],
        columns=["traveltime_scheduled", "ttsid_from", "ttsid_to"])
    return result


def calc_traveltime_real_df(train_stop_from_df, train_stop_to_df):
    """
    Calculates the real travel time between 'train_stop_from_df' and 'train_stop_to_df'.
    :param train_stop_from_df: Pandas dataframe. Input for the train stop the train comes from.
    :param train_stop_to_df: Pandas dataframe. Input for the train stop the train arrives at.
    :return: Returns a pandas dataframe with columns 'traveltime_real', 'ttsid_from', 'ttsid_to'.
    """
    if train_stop_from_df is None:
        ttsid_from = None
    else:
        ttsid_from = train_stop_from_df["ttsid"].iloc[0]

    if train_stop_to_df is None:
        ttsid_to = None
    else:
        ttsid_to = train_stop_to_df["ttsid"].iloc[0]

    if train_stop_from_df is None or train_stop_to_df is None:
        traveltime = pd.NaT
    else:
        arzeitist_to = train_stop_to_df["arzeitist"].iloc[0]
        dpzeitist_from = train_stop_from_df["dpzeitist"].iloc[0]
        traveltime = arzeitist_to - dpzeitist_from

    result = pd.DataFrame(
        data=[[traveltime, ttsid_from, ttsid_to]],
        columns=["traveltime_real", "ttsid_from", "ttsid_to"])
    return result


def calc_delay_by_traveltime_df(train_stop_from_df, train_stop_to_df):
    """
    Calculates the delay that has been caused by the travel of the train.
    Positive value means, that the travel time caused additional delay.
    Negative value means, that the travel time decreased the delay.
    :param train_stop_from_df: Pandas dataframe. Input for the train stop the train comes from.
    :param train_stop_to_df: Pandas dataframe. Input for the train stop the train arrives at.
    :return: Returns a pandas dataframe with columns 'delay_by_traveltime', 'ttsid_from', 'ttsid_to'.
    """
    if train_stop_from_df is None:
        ttsid_from = None
    else:
        ttsid_from = train_stop_from_df["ttsid"].iloc[0]

    if train_stop_to_df is None:
        ttsid_to = None
    else:
        ttsid_to = train_stop_to_df["ttsid"].iloc[0]

    if train_stop_from_df is None or train_stop_to_df is None:
        delay = pd.NaT
    else:
        traveltime_real = calc_traveltime_real_df(train_stop_from_df, train_stop_to_df)["traveltime_real"].iloc[0]
        traveltime_scheduled = calc_traveltime_scheduled_df(train_stop_from_df, train_stop_to_df)["traveltime_scheduled"].iloc[0]
        delay = traveltime_real - traveltime_scheduled

    result = pd.DataFrame(
        data=[[delay, ttsid_from, ttsid_to]],
        columns=["delay_by_traveltime", "ttsid_from", "ttsid_to"])
    return result


def calc_delay_at_arrival_df(train_stop_df):
    """
    Calculcates the delay at arrival of the train.
    :param train_stop_df: Pandas dataframe. Input for the station the train arrives at.
    :return: Returns a pandas dataframe with columns 'delay_at_arrival', 'ttsid'.
    """
    arzeitsoll = train_stop_df["arzeitsoll"].iloc[0]
    arzeitist = train_stop_df["arzeitist"].iloc[0]
    ttsid = train_stop_df["ttsid"].iloc[0]

    if pd.isnull(arzeitsoll) or pd.isnull(arzeitist):
        delay = pd.NaT
    else:
        delay = arzeitist - arzeitsoll

    result = pd.DataFrame(data=[[delay, ttsid]], columns=["delay_at_arrival", "ttsid"])
    return result


def calc_delay_at_departure_df(train_stop_df):
    """
    Calculates the delay at departure of the train.
    :param train_stop_df: Pandas dataframe. Input for the station the train departures at.
    :return: Returns a pandas dataframe with columns 'delay_at_departure', 'ttsid'.
    """
    dpzeitsoll = train_stop_df["dpzeitsoll"].iloc[0]
    dpzeitist = train_stop_df["dpzeitist"].iloc[0]
    ttsid = train_stop_df["ttsid"].iloc[0]

    if pd.isnull(dpzeitsoll) or pd.isnull(dpzeitist):
        delay = pd.NaT
    else:
        delay = dpzeitist - dpzeitsoll

    result = pd.DataFrame(data=[[delay, ttsid]], columns=["delay_at_departure", "ttsid"])
    return result