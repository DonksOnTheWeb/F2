import json
import datetime
import pandas as pd
from prophet import Prophet
from prophet.diagnostics import cross_validation
from prophet.diagnostics import performance_metrics
import itertools
import numpy as np
import os
from _loghandler import logger

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


def buildUserHolidays(holJson):
    appendList = []
    for hType in holJson:
        data = pd.DataFrame({
            'holiday': hType,
            'ds': pd.to_datetime(holJson[hType])
        })
        appendList.append(data)

    if len(appendList) > 0:
        appendList = pd.concat(appendList)

    return appendList


def doHPT(MFC, history_json, userHolidays, ctry=None):

    pd_hols = buildUserHolidays(userHolidays[MFC])
    df = pd.json_normalize(history_json)

    param_grid = {
        'uncertainty_samples': [0],
        'changepoint_prior_scale': [0.001, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55, 60],
        'changepoint_range': [0.8, 0.9, 1.0],
        'seasonality_mode': ['additive', 'multiplicative']
    }

    all_params = [dict(zip(param_grid.keys(), v)) for v in itertools.product(*param_grid.values())]
    rmses = []  # Store the RMSEs for each params here

    training = len(df) - 21
    # Use cross validation to evaluate all parameters
    iteration = 0
    for params in all_params:
        iteration = iteration + 1
        if len(pd_hols) > 0:
            m = Prophet(**params, holidays=pd_hols)
        else:
            m = Prophet(**params)
        m.add_country_holidays(ctry)
        with suppress_stdout_stderr():
            m.fit(df)
            df_cv = cross_validation(m, initial=str(training) + ' days', period='7 days', horizon='7 days',
                                     parallel="processes")
            df_p = performance_metrics(df_cv, rolling_window=1)
            rmses.append(df_p['rmse'].values[0])

    best_params = all_params[np.argmin(rmses)]
    retParams = str(best_params).replace("'", "")
    retParams = retParams.replace(": ", "=")
    retParams = retParams.replace("{", "(")
    retParams = retParams.replace("}", ")")
    retParams = retParams.replace("additive", "'additive'")
    retParams = retParams.replace("multiplicative", "'multiplicative'")
    retVal = {'MFC': MFC, 'Best': retParams, 'Ctry': ctry}
    return retVal


def doForecast(MFC, latest, history_json, ctry, paramString, userHolidays):
    pd_hols = buildUserHolidays(userHolidays[MFC])
    if len(pd_hols) > 0:
        paramString = paramString.replace(')', ', holidays=pd_hols)')
    print(paramString)
    df = pd.json_normalize(history_json)
    prophetString = "Prophet" + paramString
    m = eval(prophetString)
    m.add_country_holidays(country_name=ctry)
    with suppress_stdout_stderr():
        m.fit(df)
    future = m.make_future_dataframe(182)
    JSONStr = str(m.predict(future)[['ds', 'yhat']].to_json(orient='split'))
    JsonHolder = json.loads(JSONStr)
    retJson = {}
    retData = {}
    dCount = 1
    for entry in JsonHolder['data']:
        dte = entry[0] / 1000.0
        dte = datetime.datetime.fromtimestamp(dte)
        if dte > latest:
            dte = dte.strftime('%Y-%m-%d')
            retData[dte] = entry[1]
            dCount = dCount + 1
    retJson['Count'] = dCount
    retJson['Ctry'] = ctry
    retJson['MFC'] = MFC
    retJson['Forecast'] = retData
    return retJson
