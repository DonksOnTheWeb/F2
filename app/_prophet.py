import json

import pandas as pd
from prophet import Prophet
from _maria import getLatestActuals
from _tsLog import log

import os
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
    m = Prophet(uncertainty_samples=0)
    # if ('holiday_locale' in params):
    #    m.add_country_holidays(country_name=params.get('holiday_locale'))

    m.fit(df)

    future = m.make_future_dataframe(periods=params.get('periods'))
    return m.predict(future)


def doForecast(history_json):
    df = pd.json_normalize(history_json)
    m = Prophet(uncertainty_samples=0)
    with suppress_stdout_stderr():
        m.fit(df)
    future = m.make_future_dataframe(70)
    return str(m.predict(future)[['ds', 'yhat']].to_json(orient='split'))


def fullReForecast():
    data = getLatestActuals(None)
    if data["Result"] == 1:
        holder = {}
        MFC_forecast = {}
        actuals = data["Data"]
        actual = json.loads(actuals)
        for entry in actual:
            MFC = entry["L"]
            Date = entry["Asat"]
            Orders = entry["Act"]

            if MFC not in holder:
                MFC_ts = []
            else:
                MFC_ts = holder[MFC]
            MFC_ts.append({"ds": Date, "y": Orders})
            holder[MFC] = MFC_ts

        failed = []
        try:
            for MFC in holder:
                MFC_forecast[MFC] = doForecast(holder[MFC])
        except:
            failed.append(MFC)

        log("Forecast failed for following list: - " + str(failed))
        # log(str(MFC_forecast))

    else:
        log("OOps")
        # we have an issue!
