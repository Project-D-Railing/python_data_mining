import json
import pymysql


def connect_with_config(config, property):
    """
    Builds and return a connection to a mysql database using a json 
    configuration file for connection details.
    'config': specifies file name of json config file
    'property': holds property name inside the json file that holds the details
        for the database connection.
    """
    #read config file
    config_file = open(config, "r", encoding='utf-8-sig', newline='\r\n')
    configuration = json.loads(config_file.read())
    config_file.close()

    #get config data
    print(configuration)
    dbc_config = configuration[property]

    #connect to db
    try:
        dbc = pymysql.connect(**dbc_config)
    except pymysql.connector.Error as err:
        print(err)
        dbc= None
        
    return dbc