import query_suite
import pandas as pd
from datetime import timedelta
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
    states = qsp.get_AverageStatelike(evanr)
    for index, row in states.iterrows():
        description = row["Description"][7:]
        if description == "artime":
            artimeCount = row["Count"]
            artimelastValue = row["lastValue"]
            artimetdelaysum = row["Average"] * artimeCount
            artimenew = False
        if description == "dptime":
            dptimeCount =  row["Count"]
            dptimelastValue = row["lastValue"]
            dptimedelaysum = row["Average"] * dptimeCount
            dptimenew = False
        if description == "staytime":
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
        yymmddhhmm= row["yymmddhhmm"]
        if type(arzeitist) is pd._libs.tslib.Timedelta and type(arzeitsoll) is pd._libs.tslib.Timedelta:
            artimetdelaysum += int((arzeitsoll - arzeitist).total_seconds())
            artimeCount += 1
            artimelastValue = yymmddhhmm
        if type(dpzeitist) is pd._libs.tslib.Timedelta and type(dpzeitsoll) is pd._libs.tslib.Timedelta:
            dptimedelaysum += int((dpzeitsoll - dpzeitist).total_seconds())
            dptimeCount += 1
            dptimelastValue = yymmddhhmm
        if type(dpzeitist) is pd._libs.tslib.Timedelta and type(arzeitist) is pd._libs.tslib.Timedelta:
            staytimedelaysum += int((dpzeitist - arzeitist).total_seconds())
            staytimeCount += 1
            staytimelastValue = yymmddhhmm
    if artimetdelaysum is not 0:
        average = int(artimetdelaysum/artimeCount)
        writetoAverageState(artimeCount, artimelastValue, artimenew, average, evanr, "artime", qsp)
        returnvalue["arzeitdelay"] = pu.strfdelta(timedelta(seconds=average), "%s%D %H:%M:%S")
    if dptimedelaysum is not 0:
        average = int(dptimedelaysum/dptimeCount)
        writetoAverageState(dptimeCount, dptimelastValue, dptimenew, average, evanr, "dptime", qsp)
        returnvalue["dpzeitdelay"] = pu.strfdelta(timedelta(seconds=average), "%s%D %H:%M:%S")
    if staytimedelaysum is not 0:
        average = int(staytimedelaysum/staytimeCount)
        writetoAverageState(staytimeCount, staytimelastValue, staytimenew, average, evanr, "staytime", qsp)
        returnvalue["staytimedelay"] = pu.strfdelta(timedelta(seconds=average), "%s%D %H:%M:%S")

    Calculate_data = time.time()
    if DEBUG:
        print("Calculate_data: {}".format(gether_data - start))

    # clean up
    qsp.disconnect()

    end = time.time()
    if DEBUG:
        print("end: {}".format(end - start))
    return returnvalue


def writetoAverageState(artimeCount, artimelastValue, artimenew, average, evanr, sufix, qsp):
    if artimenew:
        qsp.set_AverageState(str(evanr) + sufix, average, artimeCount, artimelastValue)
    else:
        qsp.update_AverageState(str(evanr) + sufix, average, artimeCount, artimelastValue)


def Calc_all_Station_Time():
    start = time.time()

    # setup query suite
    qsp = query_suite.QuerySuite(config="app_config.json", property_name="dbcconfig", limit=5000)
    jsonfile = JsonFileHolder.JsonHolder("jsontestfile.json")
    jsondata = jsonfile.jsondata.keys()
    eva_nummer = qsp.get_all_stationnumbers()
    for index, eva in eva_nummer.iterrows():
        jsonfile.writejsondata(eva["EVA_NR"], Station_Time_Average(evanr=eva["EVA_NR"]))


# For Testing Reasons
if DEBUG:
    test = Station_Time_Average(evanr= 8005785)
    test = Calc_all_Station_Time()
    print(test)
    pass
