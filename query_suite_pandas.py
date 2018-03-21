import re
import pandas as pd


LOG_TO_CONSOLE = True


#labels for table "time table stops" (named "zuege" in database)
TABLE_LABELS_TTS = ["id", "ttsid", "zugverkehrstyp", "zugtyp", "zugowner",
    "zugklasse", "zugnummer", "zugnummerfull", "linie", "evanr", "arzeitsoll",
    "arzeitist", "dpzeitsoll", "dpzeitist", "gleissoll", "gleisist", "datum", "streckengeplanthash", "streckenchangedhash", "zugstatus"]


class QuerySuite:
    NO_QUERY_LIMIT = "none"
    dbc = None
    limit = 50
    
    
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
            print("EXECUTING QUERY: >> "+query+" <<")
        
        cursor = self.dbc.cursor()
        cursor.execute(query)
        result = cursor.fetchall()
        cursor.close()
        return result
    
    
    def _concat_query_info_to_data_frame(self, df, info, columnname):
        """
        Takes the query information and concats it to the given result data
        frame as new column with given column name.
        """
        info_series = pd.Series(data=[info]*len(df), name=columnname)
        #axis=1 means to invert axis, such that the series gets concatenated
        #as column and not as line
        result_df = pd.concat([df, info_series], axis=1)
        return result_df
    
    
    # query processing functions ###############################################
    def select(self, data, columns):
        """ 
        Filters and extracts a specified column out if the query result
        args holds the indeces of the columns to extract.
        'data' holds the query result to be filtered.
        """
        result = () #empty tuple
        for row in data:
            newrow = () #empty tuple
            for c in columns:
                newrow = newrow + (row[c],)
            result = result + (newrow,)
        return result      
    
        
    # basic queries ############################################################
    def get_tts_by_ttsid(self, ttsid):
        """
        Retrieves full row of database table by given 'ttsid' (named 'zugid'
        in database).
        """
        query = "SELECT * FROM zuege WHERE zuege.zugid = \"{}\""\
            .format(ttsid)
        result = self._do_query(query)
        
        result_df = pd.DataFrame(data=list(result), columns=TABLE_LABELS_TTS)
        return result_df
    
    
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
        
        query = "SELECT zuege.zugid FROM zuege WHERE zuege.zugid like \"{}-{}-{}\""\
            .format(dailytripid, yymmddhhmm, stopindex)
        result = self._do_query(query)
        
        result_df = pd.DataFrame(data=list(result), columns=["ttsid"])
        result_df = self._concat_query_info_to_data_frame(
            result_df, dailytripid, "dailytripid")
        result_df = self._concat_query_info_to_data_frame(
            result_df, yymmddhhmm, "yymmddhhmm")
        result_df = self._concat_query_info_to_data_frame(
            result_df, stopindex, "stopindex")
        return result_df
       
       
    def get_stationname_by_evanr(self, evanr):
        """
        Retrieves the station name of a given 'evanr'.
        """
        query = "SELECT NAME FROM haltestellen WHERE haltestellen.EVA_NR = \"{}\""\
            .format(evanr)
        result = self._do_query(query)
        
        result_df = pd.DataFrame(data=list(result), columns=["stationnames"])
        result_df = self._concat_query_info_to_data_frame(
            result_df, evanr, "evanr")
        return result_df
        
        
    # adcanced queries #########################################################
    def get_ttsid_on_trip(self, dailytripid, yymmddhhmm):
        """
        Retrieves all time table stop id (ttsid) (named zugid in data base) 
        of the stops that are part of the trip. The trip is determined by 
        matching the ttsid with the given dailytripid an datetime pattern. 
        Stations are sortet in the order of the trip.
        """
        qs = self
        q_ttsid = qs.get_ttsid_like(
            dailytripid=dailytripid, yymmddhhmm=yymmddhhmm)
        
        q_ttsid_sorted = qs.sort_by_stopindex(q_ttsid)
        return q_ttsid_sorted
        
    
    def sort_by_stopindex(self, data, column=0):
        """ 
        Sorts given data by the stop index of the ttsid in ascending order. 
        'column' specifies the column index of the tuples inside the data tuple
        that hold the dailytripid.
        """
        def access_stopindex(x):
            """
            This function helps the sorting function to access the stop index
            inside the ttsid by using regex and convert it to integer.
            """
            dailytripid = x[column]
            stopindex = re.search("-?[0-9]*-[0-9]*-([0-9]+)", dailytripid)
            stopindex = stopindex.group(1)
            return int(stopindex)
            
        return sorted(data, key=access_stopindex)