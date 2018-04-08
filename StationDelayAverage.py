import query_suite
import pandas as pd
import datetime
import numpy as np
import analyze_train_delay
import time

DEBUG = True


def concat_query_info_to_data_frame(df, info, columnname):
    """
    Takes the query information and concats it to the given result data
    frame as new column with given column name.
    """
    info_series = pd.Series(data=[info] * len(df), name=columnname)
    # axis=1 means to invert axis, such that the series gets concatenated
    # as column and not as line
    result_df = pd.concat([df, info_series], axis=1)
    return result_df

def Station_Time_Average(evanr=8011160):
    start = time.time()

    # setup query suite
    qsp = query_suite.QuerySuite(config="app_config.json", property_name="dbcconfig", limit=5000)

    init = time.time()
    if DEBUG:
        print("init: {}".format(init - start))

    # get stops on trip
    # ttsids = qsp.get_ttsid_on_trip(dailytripid="-5016615278318514860", yymmddhhmm="1712011704")
    ttsids = pd.DataFrame(qsp.get_Station_information(evanr))

    returnvalue = pd.DataFrame()
    arzeitdelaysum = np.array([])
    dpzeitdelaysum = np.array([])
    staytimedelaysum = np.array([])

    for index, row in ttsids.iterrows():
        arzeitsoll = row["arzeitsoll"]
        arzeitist = row["arzeitist"]
        dpzeitist= row["dpzeitist"]
        dpzeitsoll= row["dpzeitsoll"]
        if type(arzeitist) is pd._libs.tslib.Timedelta and type(arzeitsoll) is pd._libs.tslib.Timedelta:
            arzeitdelaysum = np.append(arzeitdelaysum, arzeitsoll - arzeitist)
        if type(dpzeitist) is pd._libs.tslib.Timedelta and type(dpzeitsoll) is pd._libs.tslib.Timedelta:
            dpzeitdelaysum = np.append(arzeitdelaysum, dpzeitsoll - dpzeitist)
        if type(dpzeitist) is pd._libs.tslib.Timedelta and type(arzeitist) is pd._libs.tslib.Timedelta:
            staytimedelaysum = np.append(arzeitdelaysum, dpzeitist - arzeitist)
    pass
    arzeitdelayaverage = arzeitdelaysum.mean()
    dpzeitdelayaverage = dpzeitdelaysum.mean()
    staytimedelayaverage = arzeitdelaysum.mean()
    print("arzeitdelay: {}".format(arzeitdelayaverage))
    print("dpzeitdelay: {}".format(dpzeitdelayaverage))
    print("staytimedelay: {}".format(staytimedelayaverage))

    gether_data = time.time()
    if DEBUG:
        print("gether_data: {}".format(gether_data - start))


    # clean up
    qsp.disconnect()

    end = time.time()
    if DEBUG:
        print("end: {}".format(end - start))
    return returnvalue


# For Testing Reasons
if DEBUG:
    test = Station_Time_Average()
    print(test)
