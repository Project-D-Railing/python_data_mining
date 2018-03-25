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
    ttsid = train_stop_df["ttsid"][0]
    arzeitsoll = train_stop_df["arzeitsoll"][0]
    dpzeitsoll = train_stop_df["dpzeitsoll"][0]

    if arzeitsoll is None or dpzeitsoll is None:
        staytime = datetime.timedelta(0)
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
    ttsid = train_stop_df["ttsid"][0]
    arzeitist = train_stop_df["arzeitist"][0]
    dpzeitist = train_stop_df["dpzeitist"][0]

    if arzeitist is None or dpzeitist is None:
        staytime = datetime.timedelta(0)
    else:
        staytime = dpzeitist - arzeitist

    result = pd.DataFrame(data=[[staytime, ttsid]], columns=["staytime_real", "ttsid"])
    return result


def calc_traveltime_scheduled_df(train_stop_from_df, train_stop_to_df):
    """
    Calculates the scheduled travel time between 'train_stop_from_df' and 'train_stop_to_df'.
    :param train_stop_from_df: Pandas dataframe. Input for the train stop the train comes from.
    :param train_stop_to_df: Pandas dataframe. Input for the train stop the train arrives at.
    :return: Returns a pandas dataframe with columns 'traveltime_scheduled', 'ttsid_from', 'ttsid_to'.
    """
    arzeitsoll_to = train_stop_to_df["arzeitsoll"][0]
    dpzeitsoll_from = train_stop_from_df["dpzeitsoll"][0]
    ttsid_from = train_stop_from_df["ttsid"][0]
    ttsid_to = train_stop_to_df["ttsid"][0]

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
    arzeitist_to = train_stop_to_df["arzeitist"][0]
    dpzeitist_from = train_stop_from_df["dpzeitist"][0]
    ttsid_from = train_stop_from_df["ttsid"][0]
    ttsid_to = train_stop_to_df["ttsid"][0]

    traveltime = arzeitist_to - dpzeitist_from
    result = pd.DataFrame(
        data=[[traveltime, ttsid_from, ttsid_to]],
        columns=["traveltime_real", "ttsid_from", "ttsid_to"])
    return result


def calc_delay_at_arrival_df(train_stop_df):
    """
    Calculcates the delay at arrival of the train.
    :param train_stop_df: Pandas dataframe. Input for the station the train arrives at.
    :return: Returns a pandas dataframe with columns 'delay_at_arrival', 'ttsid'.
    """
    arzeitsoll = train_stop_df["arzeitsoll"][0]
    arzeitist = train_stop_df["arzeitist"][0]
    ttsid = train_stop_df["ttsid"][0]

    if arzeitsoll is None or arzeitist is None:
        delay = datetime.timedelta(0)
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
    dpzeitsoll = train_stop_df["dpzeitsoll"][0]
    dpzeitist = train_stop_df["dpzeitist"][0]
    ttsid = train_stop_df["ttsid"][0]

    if dpzeitsoll is None or dpzeitist is None:
        delay = datetime.timedelta(0)
    else:
        delay = dpzeitist - dpzeitsoll

    result = pd.DataFrame(data=[[delay, ttsid]], columns=["delay_at_departure", "ttsid"])
    return result


def calc_delay_by_staytime_df(train_stop_df):
    """
    Calculates the delay that has been caused by the stay of the train.
    Positive value means, that the stay time caused additional delay.
    Negative value means, that the stay time decreased the delay.
    :param train_stop_df: Pandas dataframe. Input for the train stop the train stays at.
    :return: Returns a pandas dataframe with columns 'delay_by_staytime', 'ttsid'.
    """
    delay_at_departure = calc_delay_at_departure_df(train_stop_df)["delay_at_departure"][0]
    delay_at_arrival = calc_delay_at_arrival_df(train_stop_df)["delay_at_arrival"][0]
    ttsid = train_stop_df["ttsid"][0]

    delay = delay_at_departure - delay_at_arrival
    result = pd.DataFrame(data=[[delay, ttsid]], columns=["delay_by_staytime", "ttsid"])
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
    traveltime_real = calc_traveltime_real_df(train_stop_from_df, train_stop_to_df)["traveltime_real"][0]
    traveltime_scheduled = calc_traveltime_scheduled_df(train_stop_from_df, train_stop_to_df)["traveltime_scheduled"][0]
    ttsid_from = train_stop_from_df["ttsid"][0]
    ttsid_to = train_stop_to_df["ttsid"][0]

    delay = traveltime_real - traveltime_scheduled
    result = pd.DataFrame(
        data=[[delay, ttsid_from, ttsid_to]],
        columns=["delay_by_traveltime", "ttsid_from", "ttsid_to"])
    return result