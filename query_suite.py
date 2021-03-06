import re
import pandas as pd
import pymysql
import json

LOG_TO_CONSOLE = False

# labels for table "time table stops" (named "zuege" in database)
TABLE_LABELS_TTS = ["id", "ttsid", "dailytripid", "yymmddhhmm", "stopindex", "zugverkehrstyp", "zugtyp", "zugowner",
                    "zugklasse", "zugnummer", "zugnummerfull", "linie", "evanr", "arzeitsoll",
                    "arzeitist", "dpzeitsoll", "dpzeitist", "gleissoll", "gleisist", "datum",
                    "streckengeplanthash", "streckenchangedhash", "zugstatus"]

TABLE_LABELS_TTS_WITH_STATIONNAME = TABLE_LABELS_TTS + ["stationname"]


class QuerySuite:
    NO_QUERY_LIMIT = "none"
    dbc = None
    limit = 50

    def __init__(self, config, property_name, limit):
        """
        Builds and return a connection to a mysql database using a json
        configuration file for connection details.
        'config': specifies file name of json config file
        'property': holds property name inside the json file that holds the details
            for the database connection.
        """
        # read config file
        config_file = open(config, "r")
        configuration = json.loads(config_file.read())
        config_file.close()

        # get config data
        dbc_config = configuration[property_name]

        # connect to db
        try:
            self.dbc = pymysql.connect(**dbc_config)
        except Exception as err:
            print(err)
            self.dbc = None

        self.set_limit(limit)

    def disconnect(self):
        """
        Sets the data base connection to be used for queries.
        """
        self.dbc.close()
        self.dbc = None
        return self

    def use_dbc(self, dbc):
        """
        Sets the data base connection to be used for queries.
        """
        self.dbc = dbc
        return self

    def set_limit(self, limit):
        """
        Sets the limit for sql queries. set the limit to the NO_QUERY_LIMIT
        constant to not limit the queries.
        """
        self.limit = limit
        return self

    # class helper functions ###################################################
    def _append_limit_to_query(self, query):
        """
        Takes a query as a string and appends a limit clause. If the limit value
        equals the NO_QUERY_LIMIT constant then no limit clause is appended.
        """
        if self.limit != self.NO_QUERY_LIMIT:
            query = query + " LIMIT {}".format(self.limit)
        return query

    def _do_query(self, query):
        """
        Takes a query as a string and handles quering. Returns the query result
        as a tuple structure.
        """
        query = self._append_limit_to_query(query)
        if LOG_TO_CONSOLE:
            print("EXECUTING QUERY: >> " + query + " <<")

        cursor = self.dbc.cursor()
        cursor.execute(query)
        result = cursor.fetchall()
        cursor.close()
        return result

    def _execute_statement(self, query):
        """
        Takes a query as a string and handles quering. Returns the query result
        as a tuple structure.
        """
        if LOG_TO_CONSOLE:
            print("EXECUTING QUERY: >> " + query + " <<")

        cursor = self.dbc.cursor()
        cursor.execute(query)
        result = cursor.fetchall()
        cursor.close()
        self.dbc.commit()
        return result

    def _concat_query_info_to_data_frame(self, df, info, columnname):
        """
        Takes the query information and concats it to the given result data
        frame as new column with given column name.
        """
        info_series = pd.Series(data=[info] * len(df), name=columnname)
        # axis=1 means to invert axis, such that the series gets concatenated
        # as column and not as line
        result_df = pd.concat([df, info_series], axis=1)
        return result_df


    # basic queries ############################################################
    def _get_tts_by_ttsid_query(self, ttsid):
        query = "SELECT * FROM zuege WHERE zuege.zugid = \"{}\"" \
            .format(ttsid)
        result = self._do_query(query)
        return result


    def get_tts_by_ttsid(self, ttsid):
        """
        Retrieves full row of database table by given 'ttsid' (named 'zugid'
        in database).
        """
        result = self._get_tts_by_ttsid_query(ttsid)
        result_df = pd.DataFrame(data=list(result), columns=TABLE_LABELS_TTS)
        return result_df


    def _get_ttsid_like_query(self, dailytripid, yymmddhhmm, stopindex):
        query = "SELECT zuege.zugid, zuege.dailytripid, zuege.yymmddhhmm, zuege.stopid FROM zuege WHERE zuege.zugid like \"{}-{}-{}\"" \
            .format(dailytripid, yymmddhhmm, stopindex)
        result = self._do_query(query)
        return result


    def get_ttsid_like(self, dailytripid="", yymmddhhmm="", stopindex=""):
        """
        Retrieves all time table stop ids (ttsid, named 'zugid' in database)
        that match the specified SQL pattern.
        'dailytripid' specifies pattern for the dailytripid.
        'yymmddhhmm' specifies the pattern for the second part of the zugid.
        'stopindex' specifies the pattern for the third part of the zugid.
        """
        if dailytripid == "":
            dailytripid = "%"
        if yymmddhhmm == "":
            yymmddhhmm = "%"
        if stopindex == "":
            stopindex = "%"

        result = self._get_ttsid_like_query(dailytripid, yymmddhhmm, stopindex)
        result_df = pd.DataFrame(data=list(result), columns=["ttsid", "dailytripid", "yymmddhhmm","stopid"])
        return result_df


    def _get_stationname_by_evanr_query(self, evanr):
        query = "SELECT NAME FROM haltestellen WHERE haltestellen.EVA_NR = \"{}\"" \
            .format(evanr)
        result = self._do_query(query)
        return result


    def get_stationname_by_evanr(self, evanr):
        """
        Retrieves the station name of a given 'evanr'.
        """
        result = self._get_stationname_by_evanr_query(evanr)
        result_df = pd.DataFrame(data=list(result), columns=["stationname"])
        result_df = self._concat_query_info_to_data_frame(
            result_df, evanr, "evanr")
        return result_df


    def _get_tts_with_stationnames_on_trip_query(self, dailytripid, yymmddhhmm):
        query = "SELECT zuege.*, haltestellen.NAME FROM zuege " \
                "INNER JOIN haltestellen ON zuege.evanr = haltestellen.EVA_NR " \
                "WHERE zuege.dailytripid = {} " \
                "AND zuege.yymmddhhmm = {} " \
                "ORDER BY arzeitsoll ASC"
        query = query.format(dailytripid, yymmddhhmm)
        result = self._do_query(query)
        return result


    def get_tts_with_stationnames_on_trip(self, dailytripid, yymmddhhmm):
        """
        Retrieves the time table stop together with the station name.
        :param dailytripid: specifies the dailytripid.
        :param yymmddhhmm: sppecifies the date and time.
        :return: pandas dataframe.
        """
        result = self._get_tts_with_stationnames_on_trip_query(dailytripid, yymmddhhmm)
        result_df = pd.DataFrame(data=list(result), columns=TABLE_LABELS_TTS_WITH_STATIONNAME)
        return result_df

    def get_station_with_tts(self, dailytripid, stopindex):
        """
        Retrieves all time table stop ids (ttsid, named 'zugid' in database)
        that match the specified SQL pattern.
        'dailytripid' specifies pattern for the dailytripid.
        'yymmddhhmm' specifies the pattern for the second part of the zugid.
        'stopindex' specifies the pattern for the third part of the zugid.
        """

        query = "SELECT zuege.zugid, zuege.evanr, zuege.arzeitist, zuege.arzeitsoll, zuege.dpzeitist, zuege.dpzeitsoll, zuege.yymmddhhmm" \
                " FROM `zuege` WHERE zuege.stopid = \"{}\" AND dailytripid = \"{}\" " \
            .format(stopindex, dailytripid)
        result = self._do_query(query)

        result_df = pd.DataFrame(data=list(result),
                columns=["ttsid", "evanr", "arzeitist", "arzeitsoll", "dpzeitist", "dpzeitsoll", "yymmddhhmm"])
        result_df = self._concat_query_info_to_data_frame(result_df, dailytripid, "dailytripid")
        result_df = self._concat_query_info_to_data_frame(result_df, stopindex, "stopindex")
        return result_df

    def get_dtid_with_trainnumberfull(self, trainnummberfull):
        """
        Retrieves the ttsid by given trainnumberfull.
        'trainnummberfull' specifies trainnummberfull.
        """
        query = "SELECT zuege.dailytripid FROM `zuege` WHERE zuege.zugnummerfull = \"{}\"".format(trainnummberfull)
        result = self._do_query(query)

        result_df = pd.DataFrame(data=list(result), columns=["dailytripid"])
        result_df = self._concat_query_info_to_data_frame(result_df, trainnummberfull, "trainnummberfull")
        return result_df

    def get_trainnumberfull_with_dtid(self, tid):
        """
        Retrieves the trainumberfull by given tid .
        'trainnummberfull' specifies trainummberfull.
        """
        query = "SELECT zuege.zugnummerfull FROM `zuege` WHERE zuege.dailytripid = {}".format(tid)
        result = self._do_query(query)

        result_df = pd.DataFrame(data=list(result), columns=["trainnumberfull"])
        result_df = self._concat_query_info_to_data_frame(result_df, tid, "tid")
        return result_df

    def get_yymmddhhmm_with_date_and_dtid(self, date, dailytripid):
        """
        Retrieves the trainnumberfull by given dtid.
        'dailytripid' specifies dtid.
        """
        query = "SELECT zuege.yymmddhhmm FROM `zuege` WHERE zuege.datum = \"{}\" AND zuege.dailytripid = {}"\
            .format(date, dailytripid)
        result = self._do_query(query)

        result_df = pd.DataFrame(data=list(result), columns=["yymmddhhmm"])
        result_df = self._concat_query_info_to_data_frame(result_df, date, "date")
        return result_df

    def get_date_with_yymmddhhmm(self, yymmddhhmm):
        """
        Retrieves the ttsid by given trainnumberfull.
        'trainnummberfull' specifies trainnummberfull.
        """
        limit_storage = self.limit
        self.limit = 5
        query = "SELECT zuege.datum FROM `zuege` WHERE zuege.yymmddhhmm = \"{}\" GROUP BY datum".format(yymmddhhmm)
        result = self._do_query(query)
        self.limit = limit_storage

        result_df = pd.DataFrame(data=list(result), columns=["datum"])
        result_df = self._concat_query_info_to_data_frame(result_df, yymmddhhmm, "yymmddhhmm")
        return result_df

    def get_Station_information(self, evanr, lastValue=""):
        """

        :param evanr:
        :return:
        """
        limit_storage = self.limit
        self.limit = 30000
        if lastValue == "":
            query = "SELECT zuege.arzeitist, zuege.arzeitsoll, zuege.dpzeitist, zuege.dpzeitsoll, Zuege.ID " \
                    "FROM `zuege` " \
                    "WHERE zuege.evanr = \"{}\" ORDER BY `zuege`.`ID` ASC".format(evanr)
        else:
            query = "SELECT zuege.arzeitist, zuege.arzeitsoll, zuege.dpzeitist, zuege.dpzeitsoll, zuege.ID " \
                "FROM `zuege` " \
                "WHERE `zuege`.`evanr` = \"{}\" AND `zuege`.`ID` > {} ORDER BY `zuege`.`ID` ASC"\
            .format(evanr, lastValue)
        result = self._do_query(query)
        self.limit = limit_storage
        result_df = pd.DataFrame(data=list(result), columns=["arzeitist", "arzeitsoll", "dpzeitist", "dpzeitsoll", "ID"])
        result_df = self._concat_query_info_to_data_frame(result_df, evanr, "evanr")
        return result_df

    def get_all_stationnumbers(self, lastkey=""):
        """
        Retrieves the ttsids
        'lastkey' specifies the key were to start.
        """
        limit_storage = self.limit
        self.limit = 7000
        if lastkey == "":
            query = "SELECT `haltestellen`.`EVA_NR` FROM `haltestellen`"
        else:
            query = "SELECT `haltestellen`.`EVA_NR` FROM `haltestellen` WHERE `haltestellen`.`EVA_NR` > {} ".format(lastkey)
        result = self._do_query(query)
        self.limit = limit_storage
        result_df = pd.DataFrame(data=list(result), columns=["EVA_NR"])
        return result_df

    def set_AverageState(self, eva, DescriptionID, Average, Count, lastValue):
        """
        Saves the state from Average calculation to database
        :param Description: identifier of calculation
        :param Average: courrent average
        :param Count: courrent count
        :param lastValue: last Value used
        """
        query = "INSERT INTO `AverageState` (`eva`, `DescriptionID`, `Average`, `Count`, `lastValue`) " \
                "VALUES ({}, {}, {}, {}, {})".format(eva, DescriptionID, Average, Count, lastValue)

        return self._execute_statement(query)

    def update_AverageState(self, eva, DescriptionID, Average, Count, lastValue):
        """
        Saves the state from Average calculation to database
        :param Description: identifier of calculation
        :param Average: courrent average
        :param Count: courrent count
        :param lastValue: last date used
        """
        query = "UPDATE `AverageState` SET `Average` = {}, `Count` = {}, `lastValue` = {} " \
                "WHERE `AverageState`.`eva` = {} AND `AverageState`.`DescriptionID` = {}".format(Average, Count, lastValue, eva, DescriptionID)
        return self._execute_statement(query)

    def get_AverageState(self, Description):
        """
        reads the state from Average calculation to database
        :param Description: identifier of calculation
        :return: state of Average
        """
        query = "SELECT * FROM `AverageState` WHERE Description = \"{}\"".format(Description)
        result_df = pd.DataFrame(data=list(self._do_query(query)),
                                 columns=["Description", "Average", "Count", "lastValue"])
        return result_df

    def get_AverageStatelike(self, eva):
        """
        reads the state from Average calculation to database
        :param eva: identifier of calculation
        :return: state of Average
        """
        query = "SELECT * FROM `AverageState` WHERE eva ={}".format(eva)
        result_df = pd.DataFrame(data=list(self._do_query(query)),
                                 columns=["ID", "eva", "DescriptionID", "Average", "Count", "lastValue"])
        return result_df


    def get_all_yymmddhhmm_of_dailytripid(self, dailytripid):
        """
        Returns all yymmddhhmm attributes that exist for given dailytripid.
        :param dailytripid: dailytrip to find all yymmddhhmm for.
        :return: pandas dataframe;
        """
        query = "SELECT DISTINCT zuege.yymmddhhmm FROM zuege WHERE zuege.dailytripid = \"{}\" " \
                "ORDER BY CONVERT(zuege.yymmddhhmm, UNSIGNED INT)"
        query = query.format(dailytripid)
        result = self._do_query(query)
        result_df = pd.DataFrame(data=list(result), columns=["yymmddhhmm"])
        return result_df
