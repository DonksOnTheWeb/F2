import json
import datetime
import pandas as pd
from prophet import Prophet
from _maria import getLatestActualData, loadDailyForecast, latestForecastDailyDate
from _tsLog import log

import os

global debug
debug = False
if not debug:
    import warnings

    warnings.filterwarnings("ignore", message="The frame.append method is deprecated ")


class suppress_stdout_stderr(object):
    '''
    A context manager for doing a "deep suppression" of stdout and stderr in
    Python, i.e. will suppress all print, even if the print originates in a
    compiled C/Fortran sub-function.
       This will not suppress raised exceptions, since exceptions are printed
    to stderr just before a script exits, and after the context manager has
    exited (at least, I think that is why it lets exceptions through).

    '''

    def __init__(self):
        # Open a pair of null files
        self.null_fds = [os.open(os.devnull, os.O_RDWR) for x in range(2)]
        # Save the actual stdout (1) and stderr (2) file descriptors.
        self.save_fds = (os.dup(1), os.dup(2))

    def __enter__(self):
        # Assign the null pointers to stdout and stderr.
        os.dup2(self.null_fds[0], 1)
        os.dup2(self.null_fds[1], 2)

    def __exit__(self, *_):
        # Re-assign the real stdout/stderr back to (1) and (2)
        os.dup2(self.save_fds[0], 1)
        os.dup2(self.save_fds[1], 2)
        # Close the null files
        os.close(self.null_fds[0])
        os.close(self.null_fds[1])


def forecast(params):
    df = pd.io.json.json_normalize(params.get('history'))
    return doForecast(df)


def doForecast(history_json):
    df = pd.json_normalize(history_json)
    m = Prophet(uncertainty_samples=0)
    # if ('holiday_locale' in params):
    #    m.add_country_holidays(country_name=params.get('holiday_locale'))
    if debug:
        m.fit(df)
    else:
        with suppress_stdout_stderr():
            m.fit(df)
    future = m.make_future_dataframe(70)
    return str(m.predict(future)[['ds', 'yhat']].to_json(orient='split'))


def fullReForecast():
    date_format = "%Y-%m-%d"
    final = "2000-01-01"
    result = latestForecastDailyDate()

    if result["Result"] == 1:
        data = json.loads(result["Data"])
        for entry in data:
            final = entry["latest"]
    else:
        log("Issue with retrieving last forecast creation date form DB.")
        return None

    lastForecast = datetime.datetime.strptime(final, date_format).date()
    today = datetime.date.today()
    delta = today - lastForecast

    if delta.days == 0:
        log("Already performed a forecast for today.")
    else:
        data = getLatestActualData(None)
        date_format = "%Y-%m-%d"
        new_entries = []
        if data["Result"] == 1:
            holder = {}
            jLookup = {}
            MFC_forecast = {}
            actuals = data["Data"]
            actual = json.loads(actuals)
            for entry in actual:
                MFC = entry["L"]
                Date = entry["Asat"]
                Orders = entry["Act"]
                Jurisdiction = entry["J"]

                if MFC not in jLookup:
                    jLookup[MFC] = Jurisdiction

                if MFC not in holder:
                    MFC_ts = []
                else:
                    MFC_ts = holder[MFC]
                MFC_ts.append({"ds": Date, "y": Orders})
                holder[MFC] = MFC_ts

            failed = []
            try:
                today = datetime.date.today()
                creation_date = datetime.datetime.strftime(today, date_format)
                for MFC in holder:
                    MFC_forecast[MFC] = doForecast(holder[MFC])
                    localForecast = json.loads(MFC_forecast[MFC])
                    for entry in localForecast["data"]:
                        ts = int(entry[0]) / 1000
                        dte = datetime.datetime.fromtimestamp(ts).date()
                        delta = dte - today
                        if delta.days >= 0:
                            dte = datetime.datetime.strftime(dte, date_format)
                            J = jLookup[MFC]
                            fcst = int(entry[1])
                            record = (creation_date, dte, MFC, J, fcst)
                            new_entries.append(record)
            except:
                failed.append(MFC)

            load = loadDailyForecast(new_entries)
            if load["Result"] == 1:
                log("Loaded " + str(load["Data"]) + " records into daily forecast table.")
                if len(failed) > 0:
                    log("Note - Forecast failed for following list: - " + str(failed))

        else:
            log("Failed to get latest actual data - ")
            log(data["Data"])
