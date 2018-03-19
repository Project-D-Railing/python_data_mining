import datetime


def calculate_driventime(trainStopA, trainStopB):
    """calculates the differences between trainstopA and trainstopB"""
    arrival = datetime.datetime.combine(trainStopA[0][16], (datetime.datetime.min + trainStopA[0][11]).time())
    departure = datetime.datetime.combine(trainStopA[0][16], (datetime.datetime.min + trainStopB[0][13]).time())
    timedelta = arrival - departure
    return timedelta


def calculate_staytime(trainStopA):
    """calculates the differences between trainstopA and trainstopB"""
    if trainStopA[0][11] is None or trainStopA[0][13] is None:
        test = datetime.timedelta(0)
        return test
    departure = datetime.datetime.combine(trainStopA[0][16], (datetime.datetime.min + trainStopA[0][13]).time())
    arrival = datetime.datetime.combine(trainStopA[0][16], (datetime.datetime.min + trainStopA[0][11]).time())
    timedelta = departure - arrival
    return timedelta


