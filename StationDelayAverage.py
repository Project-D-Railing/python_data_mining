import query_suite
import pandas as pd
from datetime import timedelta
import time
import os
import processing_utils as pu


DEBUG = True

artime = 1
dptime = 2
staytime = 3


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


def Station_Time_Average(evanr=8011160, qsp=None, qspinsert=None):
    start = time.time()
    close = False
    if qsp is None:
        # setup query suite
        qsp = query_suite.QuerySuite(config="app_config.json", property_name="dbcconfig", limit=5000)
        close = True

    if qspinsert is None:
        qspinsert = qsp

    init = time.time()
    if DEBUG:
        print("EVA: {}".format(evanr))
        print("init: {}".format(init - start))

    returnvalue = {}
    artimetdelaysum = 0
    artimeCount = 0
    artimelastValue = 0
    artimenew = True
    dptimedelaysum = 0
    dptimeCount = 0
    dptimelastValue = 0
    dptimenew = True
    staytimedelaysum = 0
    staytimeCount = 0
    staytimelastValue = 0
    staytimenew = True
    states = qspinsert.get_AverageStatelike(evanr)
    for index, row in states.iterrows():
        description = int(row["DescriptionID"])
        if description == artime:
            artimeCount = row["Count"]
            artimelastValue = row["lastValue"]
            artimetdelaysum = row["Average"] * artimeCount
            artimenew = False
        if description == dptime:
            dptimeCount =  row["Count"]
            dptimelastValue = row["lastValue"]
            dptimedelaysum = row["Average"] * dptimeCount
            dptimenew = False
        if description == staytime:
            staytimeCount = row["Count"]
            staytimelastValue = row["lastValue"]
            staytimedelaysum = row["Average"] * staytimeCount
            staytimenew = False
        pass
    lastValue = max(artimelastValue, dptimelastValue, staytimelastValue)
    ttsids = pd.DataFrame(qsp.get_Station_information(evanr, lastValue))

    gether_data = time.time()
    if DEBUG:
        print("gether_data: {}".format(gether_data - start))

    for index, row in ttsids.iterrows():
        arzeitsoll = row["arzeitsoll"]
        arzeitist = row["arzeitist"]
        dpzeitist= row["dpzeitist"]
        dpzeitsoll= row["dpzeitsoll"]
        ID= row["ID"]
        if type(arzeitist) is pd._libs.tslib.Timedelta and type(arzeitsoll) is pd._libs.tslib.Timedelta:
            artimetdelaysum += int((arzeitist - arzeitsoll).total_seconds())
            artimeCount += 1
            artimelastValue = ID
        if type(dpzeitist) is pd._libs.tslib.Timedelta and type(dpzeitsoll) is pd._libs.tslib.Timedelta:
            dptimedelaysum += int((dpzeitist - dpzeitsoll).total_seconds())
            dptimeCount += 1
            dptimelastValue = ID
        if type(dpzeitist) is pd._libs.tslib.Timedelta and type(arzeitist) is pd._libs.tslib.Timedelta:
            staytimedelaysum += int(((arzeitist - dpzeitist) - (arzeitsoll - dpzeitsoll)).total_seconds())
            staytimeCount += 1
            staytimelastValue = ID
    if artimetdelaysum is not 0:
        average = int(artimetdelaysum/artimeCount)
        writetoAverageState(artimeCount, artimelastValue, artimenew, average, evanr, artime, qspinsert)
        returnvalue["arzeitdelay"] = pu.strfdelta(timedelta(seconds=average), "%s%D %H:%M:%S")
    if dptimedelaysum is not 0:
        average = int(dptimedelaysum/dptimeCount)
        writetoAverageState(dptimeCount, dptimelastValue, dptimenew, average, evanr, dptime, qspinsert)
        returnvalue["dpzeitdelay"] = pu.strfdelta(timedelta(seconds=average), "%s%D %H:%M:%S")
    if staytimedelaysum is not 0:
        average = int(staytimedelaysum/staytimeCount)
        writetoAverageState(staytimeCount, staytimelastValue, staytimenew, average, evanr, staytime, qspinsert)
        returnvalue["staytimedelay"] = pu.strfdelta(timedelta(seconds=average), "%s%D %H:%M:%S")

    Calculate_data = time.time()
    if DEBUG:
        print("Calculate_data: {}".format(Calculate_data - start))

    if close:
        # clean up
        qsp.disconnect()
    print(max(artimeCount, dptimeCount, staytimeCount))
    end = time.time()
    if DEBUG:
        print("end: {}".format(end - start))
    print("_______________")
    return returnvalue


def writetoAverageState(artimeCount, artimelastValue, artimenew, average, evanr, Description_id, qspinsert):
    if artimenew:
        qspinsert.set_AverageState(evanr, Description_id, average, artimeCount, artimelastValue)
    else:
        qspinsert.update_AverageState(evanr, Description_id, average, artimeCount, artimelastValue)


def Calc_all_Station_Time():
    start = time.time()
    filename = "testfile"
    if (os.path.exists(filename)):
        return 1
    file = open(filename, 'w')
    file.close()
    # setup query suite
    qsp = query_suite.QuerySuite(config="app_config_Miner.json", property_name="dbcconfig", limit=7000)
    qspinsert = query_suite.QuerySuite(config="app_config_Storage.json", property_name="dbcconfig", limit=7000)
    eva_nummer = qsp.get_all_stationnumbers()
    #if you want to start it reversed
    #eva_nummer = eva_nummer.sort_index(ascending=False, axis=0)
    for index, eva in eva_nummer.iterrows():
        Station_Time_Average(evanr=eva["EVA_NR"], qsp=qsp, qspinsert=qspinsert)
    end = time.time()
    qsp.disconnect()
    qspinsert.disconnect()
    qsp = None
    qspinsert = None
    print("Global endtime: {}".format((end - start)))
    os.remove(filename)
    return 0

# For Testing Reasons
if DEBUG:
    #test = Station_Time_Average(evanr= 8005785)
    test = Calc_all_Station_Time()
    print(test)
    pass
