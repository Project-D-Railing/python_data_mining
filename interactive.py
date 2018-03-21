import json
import pymysql
import pandas as pd

import query_suite
import query_suite_pandas
import math_util


#read config file
CONFIG_FILE_NAME = "app_config.json"

config_file = open(CONFIG_FILE_NAME, "r", encoding='utf-8-sig', newline='\r\n')
configuration = json.loads(config_file.read())
config_file.close()


#get config data
dbcconfig = configuration["dbcconfig"]


#connect to db
try:
    dbc = pymysql.connect(**dbcconfig)
except pymysql.connector.Error as err:
    print(err)

    
#setup query suite
qs = query_suite.QuerySuite()
qs.use_dbc(dbc)
qs.set_limit(5000)

qspd = query_suite_pandas.QuerySuite()
qspd.use_dbc(dbc)
qspd.set_limit(5000)


