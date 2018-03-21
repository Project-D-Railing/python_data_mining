import database_connection as db_con
import query_suite
import math_util


dbc = db_con.connect_with_config(
    config="app_config.json", property="dbcconfig")

    
#setup query suite
qs = query_suite.QuerySuite();
qs.use_dbc(dbc)
qs.set_limit(5000)


#r = qs.get_ttsid_like(dailytripid="-100020256270627274", yymmddhhmm="", stopindex="")
#r = qs.get_tts_by_ttsid("8898709046814622615-1711301719-2")
#r = qs.get_stationname_by_evanr("8000107")
r = qs.get_ttsid_on_trip(
    dailytripid="-5016615278318514860",
    yymmddhhmm="1712011704")
print(r)
letzteZugInfo = None
for x in r:
    #print(x[0])
    zugInfo = qs.get_tts_by_ttsid(x[0])
    if letzteZugInfo is not None:
        print("driven Time:\t", math_util.calculate_driventime(zugInfo, letzteZugInfo))
    letzteZugInfo = zugInfo
    timeTableStopId = qs.select(zugInfo, columns=[1, 9])
    StationName = qs.get_stationname_by_evanr(timeTableStopId[0][1])
    print(timeTableStopId, StationName, "Stay Time: ", math_util.calculate_staytime(zugInfo))



#clean up
dbc.close()

