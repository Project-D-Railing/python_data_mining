import query_suite
import processing_utils as pu
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np


# sample trip:
# dailytripid = 1307784265419680067
# yymmddhhmm = 1712111209
def analyze(dailytripid, yymmddhhmm):
    # setup query suite
    qs = query_suite.QuerySuite(config="app_config.json", property_name="dbcconfig", limit=5000)

    # get stops on trip
    tts_with_stationname_df = qs.get_tts_with_stationnames_on_trip(
    dailytripid = dailytripid, yymmddhhmm = yymmddhhmm)

    # dataframes for accumulating results
    delay_at_arrival_accum = pd.DataFrame()
    delay_at_departure_accum = pd.DataFrame()
    staytime_scheduled_accum = pd.DataFrame()
    staytime_real_accum = pd.DataFrame()
    delay_by_staytime_accum = pd.DataFrame()
    traveltime_scheduled_accum = pd.DataFrame()
    traveltime_real_accum = pd.DataFrame()
    delay_by_traveltime_accum = pd.DataFrame()

    # loop over trip
    train_stop_old = None
    for index, row in tts_with_stationname_df.iterrows():
        # convert row to data frame
        train_stop = pd.DataFrame()
        train_stop = train_stop.append(row, ignore_index=True)

        delay_at_arrival = pu.calc_delay_at_arrival_df(train_stop)
        delay_at_arrival_accum = delay_at_arrival_accum.append(delay_at_arrival, ignore_index=True)

        delay_at_departure = pu.calc_delay_at_departure_df(train_stop)
        delay_at_departure_accum = delay_at_departure_accum.append(delay_at_departure, ignore_index=True)

        staytime_scheduled = pu.calc_staytime_scheduled_df(train_stop)
        staytime_scheduled_accum = staytime_scheduled_accum.append(staytime_scheduled, ignore_index=True)

        staytime_real = pu.calc_staytime_real_df(train_stop)
        staytime_real_accum = staytime_real_accum.append(staytime_real, ignore_index=True)

        delay_by_staytime = pu.calc_delay_by_staytime_df(train_stop)
        delay_by_staytime_accum = delay_by_staytime_accum.append(delay_by_staytime, ignore_index=True)

        traveltime_scheduled = pu.calc_traveltime_scheduled_df(train_stop_old, train_stop)
        traveltime_scheduled_accum = traveltime_scheduled_accum.append(traveltime_scheduled, ignore_index=True)

        traveltime_real = pu.calc_traveltime_real_df(train_stop_old, train_stop)
        traveltime_real_accum = traveltime_real_accum.append(traveltime_real, ignore_index=True)

        delay_by_traveltime = pu.calc_delay_by_traveltime_df(train_stop_old, train_stop)
        delay_by_traveltime_accum = delay_by_traveltime_accum.append(delay_by_traveltime, ignore_index=True)

        # make step
        train_stop_old = train_stop

    # teardown query suite
    qs.disconnect()

    #construct result data frame
    result_df = tts_with_stationname_df
    result_df = pd.concat([result_df, delay_at_arrival_accum["delay_at_arrival"]], axis=1)
    result_df = pd.concat([result_df, delay_at_departure_accum["delay_at_departure"]], axis=1)
    result_df = pd.concat([result_df, staytime_scheduled_accum["staytime_scheduled"]], axis=1)
    result_df = pd.concat([result_df, staytime_real_accum["staytime_real"]], axis=1)
    result_df = pd.concat([result_df, delay_by_staytime_accum["delay_by_staytime"]], axis=1)
    result_df = pd.concat([result_df, traveltime_scheduled_accum["traveltime_scheduled"]], axis=1)
    result_df = pd.concat([result_df, traveltime_real_accum["traveltime_real"]], axis=1)
    result_df = pd.concat([result_df, delay_by_traveltime_accum["delay_by_traveltime"]], axis=1)

    return result_df


def visualize(data_df):
    # visualize results
    delay_at_arrival_minutes = [t.total_seconds() / 60.0 for t in data_df["delay_at_arrival"]]
    delay_by_staytime_minutes = [t.total_seconds() / 60.0 for t in data_df["delay_by_staytime"]]
    delay_by_traveltime_minutes = [t.total_seconds() / 60.0 for t in data_df["delay_by_traveltime"]]

    zugnummerfull = data_df[0:1]["zugnummerfull"][0]
    datum = data_df[0:1]["datum"][0].strftime("%d.%m.%Y")
    title = zugnummerfull + " am " + datum
    # do not put a label on first horizontal line by naming it _nolegend_
    legend = ["_nolegend_", "Verspätung bei Ankuft", "Verspätung durch Haltezeit", "Verspätung durch Fahrtzeit"]

    fig = plt.figure(figsize=(14, 7))
    ax = fig.add_subplot(111)
    ax.axhline(0, linewidth=1.5, color="black", linestyle=":")
    ax.plot(delay_at_arrival_minutes, linewidth=2, color="blue")
    ax.plot(delay_by_staytime_minutes, linewidth=2, color="orange")
    ax.plot(delay_by_traveltime_minutes, linewidth=2, color="red")
    ax.set_title(title)
    ax.legend(legend, loc="best")
    ax.set_xticks(np.arange(len(data_df)))
    ax.set_xticklabels(data_df["stationname"], rotation=67)
    ax.set_ylabel('Zeit in Minuten')
    ax.yaxis.set_major_locator(plt.MultipleLocator(1.0))
    ax.set_aspect("equal")
    ax.grid(True)
    fig.subplots_adjust(bottom=0.3)
    plt.show()


# example
#visualize(analyze(1307784265419680067, 1712111209))
#visualize(analyze(1307784265419680067, 1712281209))
#visualize(analyze(2677562958045670522, 1712191000))