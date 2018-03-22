import database_connection as db_con
import query_suite_pandas
import processing_utils as pu
import pandas as pd

dbc = db_con.connect_with_config(
    config="app_config.json", property="dbcconfig")

#setup query suite
qsp = query_suite_pandas.QuerySuite()
qsp.use_dbc(dbc)
qsp.set_limit(5000)


ttsids = qsp.get_ttsid_on_trip(dailytripid="-5016615278318514860", yymmddhhmm="1712011704")
print(ttsids)


stay_times = pd.DataFrame()
travel_times = pd.DataFrame()
last_train_stop = None
for index, row in ttsids.iterrows():
    train_stop = qsp.get_tts_by_ttsid(row["ttsid"])
    stay_times.append(pd.DataFrame([[1,2],[3,4]]))
    if last_train_stop is not None:
        travel_time = pu.calc_traveltime_df(last_train_stop, train_stop)
        travel_times = travel_times.append(travel_time)
        print("Travel Time:\t", travel_time)
    last_train_stop = train_stop
    station_name = qsp.get_stationname_by_evanr(train_stop["evanr"][0])
    print(train_stop)
    print(station_name)
    stay_time = pu.calc_staytime_df(train_stop)
    stay_times = stay_times.append(stay_time)
    print("Stay Time: ", stay_time)

print(stay_times)
print(travel_times)

#clean up
dbc.close()

