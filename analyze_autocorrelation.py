import query_suite
import pandas as pd
import matplotlib.pyplot as plt
import analyze_train_delay

def analyze(dailytripid):
    # setup query suite
    qs = query_suite.QuerySuite(config="app_config.json", property_name="dbcconfig", limit=5000)

    # get all yymmddhhmm that match the dailitripid
    trips_df = qs.get_all_yymmddhhmm_of_dailytripid(dailytripid=dailytripid)

    # get all stops of all trips and collect them in accumulator
    stops_accumulator = pd.DataFrame()
    for index, row in trips_df.iterrows():
        stops_on_trip = analyze_train_delay.analyze(dailytripid=dailytripid, yymmddhhmm=row["yymmddhhmm"])
        stops_accumulator = stops_accumulator.append(stops_on_trip, ignore_index=True)
        #analyze_train_delay.visualize(stops_on_trip)
        print("queries left: {}".format(len(trips_df.index)-index))

    # calculate autocorrelations
    WRAP_AROUND_MODE = False
    acf_delay_by_staytime_df = autocorrelation_over_entire_df(
        stops_accumulator, "delay_by_staytime", wrap_around=WRAP_AROUND_MODE)
    acf_delay_by_traveltime_df = autocorrelation_over_entire_df(
        stops_accumulator, "delay_by_traveltime", wrap_around=WRAP_AROUND_MODE)
    acf_delay_at_arrival_df = autocorrelation_over_entire_df(
        stops_accumulator, "delay_at_arrival", wrap_around=WRAP_AROUND_MODE)
    acf_delay_at_departure_df = autocorrelation_over_entire_df(
        stops_accumulator, "delay_at_departure", wrap_around=WRAP_AROUND_MODE)


    # construct result dataframe
    result_df = acf_delay_by_staytime_df
    result_df = pd.concat([result_df, acf_delay_by_traveltime_df["acf_delay_by_traveltime"]], axis=1)
    result_df = pd.concat([result_df, acf_delay_at_arrival_df["acf_delay_at_arrival"]], axis=1)
    result_df = pd.concat([result_df, acf_delay_at_departure_df["acf_delay_at_departure"]], axis=1)
    return {"acf_df":result_df, "dailytripid":dailytripid}


def autocorrelation_over_entire_df(data_df, column, wrap_around=False):
    """
    Calculates autocorrelation over all possible shifts.
    :param data_df: Dataframe that holds the signal to correlate with.
    :param column: Column name that specifies which column of the dataframe should be used as signal.
    :param wrap_around: Defines if correlation wraps around to the beginning of the signal when the shifting goes
        outside of the dataframe bound. Defaults to false.
    :return:
    """
    N = len(data_df.index)
    result_df_columns = ["acf_k", "acf_"+column]
    result_df = pd.DataFrame(columns=result_df_columns)
    for k in range(N):
        corr = autocorrelation_df(data_df, column, k, wrap_around)
        corr_df = pd.DataFrame(data=[[k, corr]], columns=result_df_columns)
        result_df = result_df.append(corr_df, ignore_index=True)
        print("Autocorrelation of column \"{}\" with size N={} and shift k={}: {}".format(column, N, k, corr))
    return result_df


def autocorrelation_df(data_df, column, k, wrap_around=False):
    """
    Calculates autokorrelation with given shift of k.
    :param data_df: Dataframe containing the signal to correlate with.
    :param column: Column name that specifies which column of the dataframe should be used as signal.
    :param k: The shift of the correlation.
    :param wrap_around: Defines if correlation wraps around to the beginning of the signal when the shifting goes
        outside of the dataframe bound. Defaults to false.
    :return: Value of computated corrrelation.
    """
    N = len(data_df.index)
    sum = 0

    for n in range(N):
        value = data_df[column][n]

        if wrap_around:
            # get shifted value and continue with begin of series when shifting goes outside of dataframe bounds
            value_shifted = data_df[column][(n+k)%N]
        else:
            # get shifted value and use NaT when shifting goes outside of dataframe bounds
            value_shifted = data_df[column][n+k] if (n+k < N) else pd.NaT

        # get total seconds of timedeltas and convert to minutes.
        # sanatize by replacing NaT with 0 minutes
        value = 0 if pd.isnull(value) else value.total_seconds()/60
        value_shifted = 0 if pd.isnull(value_shifted) else value_shifted.total_seconds()/60
        # calculate
        mul = value * value_shifted
        sum += mul

    result = sum / N
    return result

def visualize(data):
    # Berechnete Werte der Autokorrelation visualisieren
    title = "Autokorrelation von Verspätungen von dailytripid={}".format(data["dailytripid"])
    legend = ["Verspätung bei Ankunft", "Verspätung bei Abfahrt", "Verspätung druch Haltezeit", "Verspätung durch Fahrtzeit"]

    plt.plot(data["acf_df"]["acf_delay_at_arrival"].values, color="blue")
    plt.plot(data["acf_df"]["acf_delay_at_departure"].values, color="purple")
    plt.plot(data["acf_df"]["acf_delay_by_staytime"].values, color="orange")
    plt.plot(data["acf_df"]["acf_delay_by_traveltime"].values, color="red")
    plt.title(title)
    plt.legend(legend, loc="best")
    plt.show()

#examples
#visualize(analyze(dailytripid=1307784265419680067))
visualize(analyze(dailytripid=2677562958045670522))