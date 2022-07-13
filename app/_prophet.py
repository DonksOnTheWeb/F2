import json
import datetime
import pandas as pd
from prophet import Prophet
from _maria import getAllActuals, loadDailyForecast, latestForecastDailyDate, countryFromMFC, getFilteredActuals
from loghandler import logger
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


def forecast(groupAs, MFCList, Just):
    data = getFilteredActuals(groupAs, MFCList)
    date_format = "%d-%b-%Y"
    retVal = {}
    if data["Result"] == 1:
        ts = []
        final = []
        actuals = data["Data"]
        Just = int(Just)
        actual = json.loads(actuals)
        if Just > 0:
            daysBack = (7 * Just) - datetime.datetime.today().weekday()
            actual = actual[-daysBack:]
        for entry in actual:
            Date = entry["Asat"]
            Orders = entry["Act"]
            ts.append({"ds": Date, "y": Orders})

        try:
            ctry = json.loads(countryFromMFC(MFCList[0])["Data"])[0]['Country']
        except:
            ctry = None
        thisForecast = doForecast(ts, ctry)
        localForecast = json.loads(thisForecast)
        for entry in localForecast["data"]:
            dte = datetime.datetime.fromtimestamp(int(entry[0]) / 1000).date()
            dte = datetime.datetime.strftime(dte, date_format)
            fcst = int(entry[1])
            record = {"Asat": dte, "Forecast": fcst}
            final.append(record)
        retVal["Result"] = 1
        retVal["Data"] = json.dumps(final, default=str)
        return retVal
    else:
        retVal["Result"] = 0
        retVal["Data"] = data["Data"]
        return retVal


def doForecast(history_json, ctry=None):
    df = pd.json_normalize(history_json)
    m = Prophet(uncertainty_samples=0, changepoint_prior_scale=0.8, daily_seasonality=True, weekly_seasonality=True)

    d1 = datetime.datetime.strptime(history_json[0]['ds'], '%Y-%m-%d')
    d2 = datetime.datetime.strptime(history_json[-1]['ds'], '%Y-%m-%d')
    delta = (d2-d1).days
    if delta > 60:
        m = m.add_seasonality(
            name='monthly',
            period=30,
            fourier_order=10)
    if ctry is not None:
        m.add_country_holidays(country_name=ctry)
    if debug:
        m.fit(df)
    else:
        with suppress_stdout_stderr():
            m.fit(df)
    future = m.make_future_dataframe(140)
    return str(m.predict(future)[['ds', 'yhat']].to_json(orient='split'))


def fullReForecast(alwaysForce=0):
    date_format = "%Y-%m-%d"
    final = "2000-01-01"
    result = latestForecastDailyDate()
    if result["Result"] == 1:
        data = json.loads(result["Data"])
        for entry in data:
            if entry["latest"] is not None:
                final = entry["latest"]

        lastForecast = datetime.datetime.strptime(final, date_format).date()
        today = datetime.date.today()
        delta = today - lastForecast

        if delta.days == 0 and alwaysForce == 0:
            logger('I', "Already performed a forecast for today.")
        else:
            data = getAllActuals()
            date_format = "%Y-%m-%d"
            new_entries = []
            if data["Result"] == 1:
                holder = {}
                MFC_forecast = {}
                actuals = data["Data"]
                actual = json.loads(actuals)
                for entry in actual:
                    MFC = entry["Location"]
                    Date = entry["Asat"]
                    Orders = entry["Act"]

                    if MFC not in holder:
                        MFC_ts = []
                    else:
                        MFC_ts = holder[MFC]
                    MFC_ts.append({"ds": Date, "y": Orders})
                    holder[MFC] = MFC_ts

                failed = []

                today = datetime.date.today()
                creation_date = datetime.datetime.strftime(today, date_format)
                mfcCount = len(holder)
                runningCount = 1
                for MFC in holder:
                    try:
                        try:
                            ctry = json.loads(countryFromMFC(MFC)["Data"])[0]['Country']
                        except:
                            ctry = None
                        logger('I', "Forecasting " + MFC + " (" + str(runningCount) + " of " + str(mfcCount) + ")")
                        runningCount = runningCount + 1
                        MFC_forecast[MFC] = doForecast(holder[MFC], ctry)
                        localForecast = json.loads(MFC_forecast[MFC])

                        for entry in localForecast["data"]:
                            ts = int(entry[0]) / 1000
                            dte = datetime.datetime.fromtimestamp(ts).date()
                            dte = datetime.datetime.strftime(dte, date_format)
                            fcst = int(entry[1])
                            record = (creation_date, dte, MFC, fcst)
                            new_entries.append(record)
                    except:
                        failed.append(MFC)

                load = loadDailyForecast(new_entries)
                if load["Result"] == 1:
                    logger('I', "Loaded " + str(load["Data"]) + " records into daily forecast table.")
                    if len(failed) > 0:
                        logger('I', "Note - Forecast failed for following list: - " + str(failed))

            else:
                logger('W', "Failed to get latest actual data - ")
                logger('W', data["Data"])
    else:
        logger('W', '')
        logger('W', str(result["Data"]))
        logger('W', '')
