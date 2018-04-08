import query_suite
import datetime


def trainnumberfulltodailytripid(trainummberfull= "RE11350"):
    qs = query_suite.QuerySuite(config="app_config.json", property_name="dbcconfig", limit=1)
    ttsid = qs.get_dtid_with_trainnumberfull(trainummberfull)
    return ttsid


def dailytripidtotrainnumberfull(dailytripid = 5212057309500211661):
    qs = query_suite.QuerySuite(config="app_config.json", property_name="dbcconfig", limit=1)
    trainnumberfull = qs.get_trainnumberfull_with_dtid(dailytripid)
    return trainnumberfull


def datetoyymmddhhmm(date="2017-12-11", dailytripid=5212057309500211661):
    qs = query_suite.QuerySuite(config="app_config.json", property_name="dbcconfig", limit=1)
    yymmddhhmm = qs.get_yymmddhhmm_with_date_and_dtid(date, dailytripid)
    return yymmddhhmm


def yymmddhhmmtodate(yymmddhhmm="1712111109"):
    qs = query_suite.QuerySuite(config="app_config.json", property_name="dbcconfig", limit=1)
    date = qs.get_date_with_yymmddhhmm(yymmddhhmm)
    return date