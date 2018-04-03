import query_suite


def trainnumberfulltodailytripid(trainummberfull= "RE11350"):
    qs = query_suite.QuerySuite(config="app_config.json", property_name="dbcconfig", limit=1)
    ttsid = qs.get_ttsid_with_trainnumberfull(trainummberfull)
    return ttsid


def dailytripidtotrainnumberfull(dailytripid = 5212057309500211661):
    qs = query_suite.QuerySuite(config="app_config.json", property_name="dbcconfig", limit=1)
    trainnumberfull = qs.get_trainnumberfull_with_ttsid(dailytripid)
    return trainnumberfull
