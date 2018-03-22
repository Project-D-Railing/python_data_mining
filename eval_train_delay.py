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

delay_arrivals = pd.DataFrame();
delay_depatures = pd.DataFrame();

for index, row in ttsids.iterrows():
    train_stop = qsp.get_tts_by_ttsid(row["ttsid"])
    station_name = qsp.get_stationname_by_evanr(train_stop["evanr"][0])
    print(train_stop)
    print(station_name)
    delay_arrival = pu.calc_delay_at_arrival_df(train_stop)
    delay_arrivals = delay_arrivals.append(delay_arrival)
    delay_depature = pu.calc_delay_at_departure_df(train_stop)
    delay_depatures = delay_depatures.append(delay_depature)
    print("Delay at Arrival: ", delay_arrival)
    print("Delay at Departure: ", delay_depature)

print(delay_arrivals)
print(delay_depatures)

#clean up
dbc.close()

