import database_connection as db_con
import query_suite_pandas
import processing_utils as pu

import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

#setup database connection
dbc = db_con.connect_with_config(
    config="app_config.json", property="dbcconfig")

#setup query suite
qsp = query_suite_pandas.QuerySuite()
qsp.use_dbc(dbc)
qsp.set_limit(5000)


#get stops on trip
ttsids = qsp.get_ttsid_on_trip(dailytripid="-6843069272511463904", yymmddhhmm="1712011109")
print(ttsids)

#dataframes for accumulating results
train_stop_accum = pd.DataFrame()
stationname_accum = pd.DataFrame()
staytime_scheduled_accum = pd.DataFrame()
staytime_real_accum = pd.DataFrame()
traveltime_real_accum = pd.DataFrame()
traveltime_scheduled_accum = pd.DataFrame()
delay_at_arrival_accum = pd.DataFrame()
delay_at_departure_accum = pd.DataFrame()
delay_by_staytime_accum = pd.DataFrame()
delay_by_traveltime_accum = pd.DataFrame()

#loop over trip
train_stop_old = None
for index, row in ttsids.iterrows():
    train_stop = qsp.get_tts_by_ttsid(row["ttsid"])
    train_stop_accum = train_stop_accum.append(train_stop, ignore_index=True)

    stationname = qsp.get_stationname_by_evanr(train_stop["evanr"][0])
    stationname_accum = stationname_accum.append(stationname, ignore_index=True)

    staytime_scheduled = pu.calc_staytime_scheduled_df(train_stop)
    staytime_scheduled_accum = staytime_scheduled_accum.append(staytime_scheduled, ignore_index=True)

    staytime_real = pu.calc_staytime_real_df(train_stop)
    staytime_real_accum = staytime_real_accum.append(staytime_real, ignore_index=True)

    delay_at_arrival = pu.calc_delay_at_arrival_df(train_stop)
    delay_at_arrival_accum = delay_at_arrival_accum.append(delay_at_arrival, ignore_index=True)

    delay_at_departure = pu.calc_delay_at_departure_df(train_stop)
    delay_at_departure_accum = delay_at_departure_accum.append(delay_at_departure, ignore_index=True)

    delay_by_staytime = pu.calc_delay_by_staytime_df(train_stop)
    delay_by_staytime_accum = delay_by_staytime_accum.append(delay_by_staytime, ignore_index=True)

    traveltime_scheduled = None
    traveltime_real = None
    delay_by_traveltime = None
    if train_stop_old is not None:
        traveltime_scheduled = pu.calc_traveltime_scheduled_df(train_stop_old, train_stop)
        traveltime_scheduled_accum = traveltime_scheduled_accum.append(traveltime_scheduled, ignore_index=True)

        traveltime_real = pu.calc_traveltime_real_df(train_stop_old, train_stop)
        traveltime_real_accum = traveltime_real_accum.append(traveltime_real, ignore_index=True)

        delay_by_traveltime = pu.calc_delay_by_traveltime_df(train_stop_old, train_stop)
        delay_by_traveltime_accum = delay_by_traveltime_accum.append(delay_by_traveltime, ignore_index=True)

    #make step
    train_stop_old = train_stop

    print(train_stop)
    print(stationname)
    print("Staytime Scheduled: ", staytime_scheduled)
    print("Staytime Real: ", staytime_real)
    if (train_stop_old is not None) and (traveltime_real is not None):
        print("Traveltime Scheduled: ", traveltime_scheduled)
        print("Traveltime Real: ", traveltime_real)
    print("Delay at Arrival: ", delay_at_arrival)
    print("Delay at Departure: ", delay_at_departure)
    print("Delay by Staytime: ", delay_by_staytime)
    if (train_stop_old is not None) and (traveltime_real is not None):
        print("Delay by Traveltime: ", delay_by_traveltime)

print(train_stop_accum)
print(stationname_accum)
print(staytime_scheduled_accum)
print(staytime_real_accum)
print(traveltime_scheduled_accum)
print(traveltime_real_accum)
print(delay_at_arrival_accum)
print(delay_at_departure_accum)
print(delay_by_staytime_accum)
print(delay_by_traveltime_accum)

#visualize results
delay_at_arrival_minutes = [t.total_seconds() / 60.0 for t in delay_at_arrival_accum["delay_at_arrival"]]
delay_at_departure_minutes = [t.total_seconds() / 60.0 for t in delay_at_departure_accum["delay_at_departure"]]
delay_by_staytime_minutes = [t.total_seconds() / 60.0 for t in delay_by_staytime_accum["delay_by_staytime"]]
delay_by_traveltime_minutes = [t.total_seconds() / 60.0 for t in delay_by_traveltime_accum["delay_by_traveltime"]]

plt.plot(delay_at_arrival_minutes)
plt.plot(delay_at_departure_minutes)
plt.plot(delay_by_staytime_minutes)
plt.plot(delay_by_traveltime_minutes)
plt.legend(['Verspätung bei Ankuft', "Verspätung bei Abfahrt",
            "Verspätung durch Haltezeit", "Verspätung durch Fahrtzeit"], loc='upper left')
plt.ylabel('Zeit in Minuten')
plt.xticks(np.arange(len(stationname_accum)), stationname_accum["stationname"], rotation=67)
plt.grid(True)
plt.title(train_stop_accum[0:1]["zugnummerfull"][0] + " am " + train_stop_accum[0:1]["datum"][0].strftime("%d.%m.%Y"))
plt.subplots_adjust(bottom=0.3)
plt.show()


#clean up
dbc.close()

