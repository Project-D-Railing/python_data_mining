import database_connection as db_con
import query_suite_pandas
import processing_utils as pu
import pandas as pd

#setup database connection
dbc = db_con.connect_with_config(
    config="app_config.json", property="dbcconfig")

#setup query suite
qsp = query_suite_pandas.QuerySuite()
qsp.use_dbc(dbc)
qsp.set_limit(5000)


#get stops on trip
#
#ttsids = qsp.get_ttsid_on_trip(dailytripid="-5016615278318514860", yymmddhhmm="1712011704")
ttsids = qsp.get_ttsid_on_trip(dailytripid="7983801099953614172", yymmddhhmm="1802031455")
print(ttsids)

#dataframes for accumulating results
staytime_scheduled_accum = pd.DataFrame()
staytime_real_accum = pd.DataFrame()
traveltime_real_accum = pd.DataFrame()
traveltime_scheduled_accum = pd.DataFrame()
delay_at_arrival_accum = pd.DataFrame()
delay_at_depature_accum = pd.DataFrame()
delay_by_staytime_accum = pd.DataFrame()
delay_by_traveltime_accum = pd.DataFrame()

#loop over trip
train_stop_old = None
for index, row in ttsids.iterrows():
    train_stop = qsp.get_tts_by_ttsid(row["ttsid"])
    station_name = qsp.get_stationname_by_evanr(train_stop["evanr"][0])
    print(train_stop)
    print(station_name)

    staytime_scheduled = pu.calc_staytime_scheduled_df(train_stop)
    staytime_scheduled_accum = staytime_scheduled_accum.append(staytime_scheduled)
    staytime_real = pu.calc_staytime_real_df(train_stop)
    staytime_real_accum = staytime_real_accum.append(staytime_real)
    delay_at_arrival = pu.calc_delay_at_arrival_df(train_stop)
    delay_at_arrival_accum = delay_at_arrival_accum.append(delay_at_arrival)
    delay_at_depature = pu.calc_delay_at_departure_df(train_stop)
    delay_at_depature_accum = delay_at_depature_accum.append(delay_at_depature)
    delay_by_staytime = pu.calc_delay_by_staytime_df(train_stop)
    delay_by_staytime_accum = delay_by_staytime_accum.append(delay_by_staytime)

    traveltime_scheduled = None
    traveltime_real = None
    delay_by_traveltime = None
    if train_stop_old is not None:
        traveltime_scheduled = pu.calc_traveltime_scheduled_df(train_stop_old, train_stop)
        traveltime_scheduled_accum = traveltime_scheduled_accum.append(traveltime_scheduled)
        traveltime_real = pu.calc_traveltime_real_df(train_stop_old, train_stop)
        traveltime_real_accum = traveltime_real_accum.append(traveltime_real)
        delay_by_traveltime = pu.calc_delay_by_traveltime_df(train_stop_old, train_stop)
        delay_by_traveltime_accum = delay_by_traveltime_accum.append(delay_by_traveltime)

    #make step
    train_stop_old = train_stop

    print("Staytime Scheduled: ", staytime_scheduled)
    print("Staytime Real: ", staytime_real)
    if (train_stop_old is not None) and (traveltime_real is not None):
        print("Traveltime Scheduled: ", traveltime_scheduled)
        print("Traveltime Real: ", traveltime_real)
    print("Delay at Arrival: ", delay_at_arrival)
    print("Delay at Departure: ", delay_at_depature)
    print("Delay by Staytime: ", delay_by_staytime)
    if (train_stop_old is not None) and (traveltime_real is not None):
        print("Delay by Traveltime: ", delay_by_traveltime)

print(staytime_scheduled_accum)
print(staytime_real_accum)
print(traveltime_scheduled_accum)
print(traveltime_real_accum)
print(delay_at_arrival_accum)
print(delay_at_depature_accum)
print(delay_by_staytime_accum)
print(delay_by_traveltime_accum)


#clean up
dbc.close()

