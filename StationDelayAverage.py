import query_suite
import pandas as pd
import numpy as np
import time
import JsonFileHolder
import processing_utils as pu

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
        print("EVA: {}".format(evanr))
        print("init: {}".format(init - start))

    # get stops on trip
    # ttsids = qsp.get_ttsid_on_trip(dailytripid="-5016615278318514860", yymmddhhmm="1712011704")
    ttsids = pd.DataFrame(qsp.get_Station_information(evanr))

    returnvalue = {}
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
    if len(arzeitdelaysum) is not 0:
        returnvalue["arzeitdelay"] = pu.strfdelta(arzeitdelaysum.mean(), "%s%D %H:%M:%S")
    if len(dpzeitdelaysum) is not 0:
        returnvalue["dpzeitdelay"] = pu.strfdelta(dpzeitdelaysum.mean(), "%s%D %H:%M:%S")
    if len(staytimedelaysum) is not 0:
        returnvalue["staytimedelay"] = pu.strfdelta(staytimedelaysum.mean(), "%s%D %H:%M:%S")

    gether_data = time.time()
    if DEBUG:
        print("gether_data: {}".format(gether_data - start))

    # clean up
    qsp.disconnect()

    end = time.time()
    if DEBUG:
        print("end: {}".format(end - start))
    return returnvalue


def Calc_all_Station_Time():
    start = time.time()

    # setup query suite
    qsp = query_suite.QuerySuite(config="app_config.json", property_name="dbcconfig", limit=5000)
    jsonfile = JsonFileHolder.JsonHolder("jsontestfile.json")
    jsondata = jsonfile.jsondata.keys()
    if len(jsondata) != 0:
        lastkey = max(jsondata)
    init = time.time()
    if DEBUG:
        print("init: {}".format(init - start))
    if lastkey == 0:
        eva_nummer = qsp.get_all_stationnumbers()
    else:
        eva_nummer = qsp.get_all_stationnumbers(lastkey)
    for index, eva in eva_nummer.iterrows():
        jsonfile.writejsondata(eva["EVA_NR"], Station_Time_Average(evanr=eva["EVA_NR"]))


# For Testing Reasons
if DEBUG:
    #test = Station_Time_Average(evanr=8000019)
    test = Calc_all_Station_Time()
    print(test)
    pass
