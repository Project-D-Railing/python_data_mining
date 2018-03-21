import database_connection as db_con
import query_suite_pandas
import processing_utils as pu

dbc = db_con.connect_with_config(
    config="app_config.json", property="dbcconfig")

#setup query suite
qsp = query_suite_pandas.QuerySuite()
qsp.use_dbc(dbc)
qsp.set_limit(5000)


ttsids = qsp.get_ttsid_on_trip(dailytripid="-5016615278318514860", yymmddhhmm="1712011704")
print(ttsids)



last_train_stop = None
for index, row in ttsids.iterrows():
    train_stop = qsp.get_tts_by_ttsid(row["ttsid"])
    if last_train_stop is not None:
        print("driven Time:\t", pu.calc_traveltime_df(last_train_stop, train_stop))
    last_train_stop = train_stop
    StationName = qsp.get_stationname_by_evanr(train_stop["evanr"][0])
    #print(train_stop)
    #print(StationName)
    print("Stay Time: ", pu.calc_staytime_df(train_stop))


#clean up
dbc.close()

