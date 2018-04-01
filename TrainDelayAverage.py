import query_suite
import pandas as pd
import numpy as np
import analyze_train_delay
import time

DEBUG = True

def Time_Average(dailytripid="8317284780065095268"):
    start = time.time()

    # setup query suite
    qsp = query_suite.QuerySuite(config="app_config.json", property_name="dbcconfig", limit=5000)

    init = time.time()
    if DEBUG:
        print("init")
        print(init - start)

    # get stops on trip
    # ttsids = qsp.get_ttsid_on_trip(dailytripid="-5016615278318514860", yymmddhhmm="1712011704")
    ttsids = pd.DataFrame(qsp.get_ttsid_like(dailytripid=dailytripid, stopindex=1))

    returnvalue = pd.DataFrame()
    all = []
    for index, row in ttsids.iterrows():
        all.append(analyze_train_delay.analyze(row["dailytripid"], row["yymmddhhmm"]))

    gether_data = time.time()
    if DEBUG:
        print("gether_data")
        print(gether_data - start)

    for index in all[00]["stopindex"]:
        delay_at_arrival = np.array([])
        delay_at_departure = np.array([])
        staytime_scheduled = np.array([])
        staytime_real = np.array([])
        delay_by_staytime = np.array([])
        traveltime_scheduled = np.array([])
        traveltime_real = np.array([])
        delay_by_traveltime = np.array([])
        test = 0

        for temp in all:
            # print(test)
            try:
                delay_at_arrival = np.append(delay_at_arrival, temp["delay_at_arrival"][index])
                delay_at_departure = np.append(delay_at_departure, temp["delay_at_departure"][index])
                delay_by_staytime = np.append(delay_by_staytime, temp["delay_by_staytime"][index])
                delay_by_traveltime = np.append(delay_by_traveltime, temp["delay_by_traveltime"][index])
            except Exception as e:
                if DEBUG:
                    print("train shorter than before")
                e
                pass
            test += 1
        pass

        size = delay_at_arrival.size
        if delay_at_arrival.size == 0:
            pass
        else:
            avr_delay_at_arrival_storage = delay_at_arrival.mean()
            avr_delay_at_departure_storage = delay_at_departure.mean()
            avr_delay_by_staytime = delay_by_staytime.mean()
            avr_delay_by_traveltime = delay_by_traveltime.mean()
            test = pd.DataFrame([[avr_delay_at_arrival_storage, avr_delay_at_departure_storage, avr_delay_by_staytime,
                              avr_delay_by_traveltime], ], columns=["delay_at_arrival", "delay_at_departure",
                                                                "delay_by_staytime", "delay_by_traveltime"])
            returnvalue = returnvalue.append(test, ignore_index=True)
        print(index)


    # print(traveltime_scheduled_accum)
    # print(traveltime_real_accum)
    # print(delay_by_traveltime_accum)

    # clean up
    qsp.disconnect()

    end = time.time()
    if DEBUG:
        print("end")
        print(end - start)
    return returnvalue


# For Testing Reasons
if DEBUG:
    Time_Average()
