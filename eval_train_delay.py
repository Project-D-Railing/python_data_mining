import query_suite
import processing_utils as pu
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

# setup query suite
qs = query_suite.QuerySuite(config="app_config.json", property_name="dbcconfig", limit=5000)


# get stops on trip
tts_with_stationname_df = qs.get_tts_with_stationnames_on_trip(
    dailytripid=1307784265419680067, yymmddhhmm=1712111209)

# dataframes for accumulating results
staytime_scheduled_accum = pd.DataFrame()
staytime_real_accum = pd.DataFrame()
traveltime_real_accum = pd.DataFrame()
traveltime_scheduled_accum = pd.DataFrame()
delay_at_arrival_accum = pd.DataFrame()
delay_at_departure_accum = pd.DataFrame()
delay_by_staytime_accum = pd.DataFrame()
delay_by_traveltime_accum = pd.DataFrame()

# loop over trip
train_stop_old = None
for index, row in tts_with_stationname_df.iterrows():
    # convert row to data frame
    train_stop = pd.DataFrame()
    train_stop = train_stop.append(row, ignore_index=True)

    staytime_scheduled = pu.calc_staytime_scheduled_df(train_stop)
    staytime_scheduled_accum = staytime_scheduled_accum.append(staytime_scheduled, ignore_index=True)

    staytime_real = pu.calc_staytime_real_df(train_stop)
    staytime_real_accum = staytime_real_accum.append(staytime_real, ignore_index=True)

    delay_at_arrival = pu.calc_delay_at_arrival_df(train_stop)
    delay_at_arrival_accum = delay_at_arrival_accum.append(delay_at_arrival, ignore_index=True)

    delay_at_departure = pu.calc_delay_at_departure_df(train_stop)
    delay_at_departure_accum = delay_at_departure_accum.append(delay_at_departure, ignore_index=True)

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

# visualize results
delay_at_arrival_minutes = [t.total_seconds() / 60.0 for t in delay_at_arrival_accum["delay_at_arrival"]]
delay_at_departure_minutes = [t.total_seconds() / 60.0 for t in delay_at_departure_accum["delay_at_departure"]]
delay_by_staytime_minutes = [t.total_seconds() / 60.0 for t in delay_by_staytime_accum["delay_by_staytime"]]
delay_by_traveltime_minutes = [t.total_seconds() / 60.0 for t in delay_by_traveltime_accum["delay_by_traveltime"]]

plt.figure(figsize=(14, 7))
plt.plot(delay_at_arrival_minutes, linewidth=2.0, alpha=0.7)
plt.plot(delay_at_departure_minutes, linewidth=2.0, alpha=0.7)
plt.plot(delay_by_staytime_minutes, linewidth=2.0, alpha=0.7)
plt.plot(delay_by_traveltime_minutes, linewidth=2.0, alpha=0.7)
# title: %zugnummerfull% am %dd.mm.yyyy%
plt.title(tts_with_stationname_df[0:1]["zugnummerfull"][0] + " am " +
          tts_with_stationname_df[0:1]["datum"][0].strftime("%d.%m.%Y"))
plt.legend(['Verspätung bei Ankuft', "Verspätung bei Abfahrt", "Verspätung durch Haltezeit",
            "Verspätung durch Fahrtzeit"], loc='upper left')
plt.ylabel('Zeit in Minuten')
plt.xticks(np.arange(len(tts_with_stationname_df)), tts_with_stationname_df["stationname"], rotation=67)
plt.grid(True)
plt.subplots_adjust(bottom=0.3)
plt.show()


# clean up
qs.disconnect()
