from flask import Flask, request
from prophet import __version__
from _prophet import forecast, fullReForecast

from _maria import getForecastHistory, getLatestForecastDaily, getActuals, getWorkingForecast, listMFCs
from _maria import deleteOldDailyForecasts, loadMFCList, delMFCList

from _googlePull import gSyncActuals, loadForecastOneOff
from _tsLog import log

app = Flask(__name__)


@app.route("/")
def root():
    html = "Welcome"
    return html.format(version=__version__)


@app.route("/version")
def version():
    return __version__


@app.route("/getForecastHistoryFromDB", methods=['POST'])
def getForecastHistoryFromDB():
    params = request.get_json(silent=True)
    groupAs = params.get('GroupAs')
    MFC = params.get('MFC')
    MFCList = []
    for M in MFC:
        MFCList.append(M)
    result = getForecastHistory(groupAs, MFCList)
    if result["Result"] == 0:
        log(result["Data"])
        result["Data"] = "Fail - check logs"
    return result


@app.route("/listMFCs", methods=['GET'])
def listMFCsFromDB():
    result = listMFCs()
    if result["Result"] == 0:
        log(result["Data"])
        result["Data"] = "Fail - check logs"
    return result


@app.route("/getWorkingForecastFromDB", methods=['POST'])
def getWorkingForecastFromDB():
    params = request.get_json(silent=True)
    groupAs = params.get('GroupAs')
    MFC = params.get('MFC')
    MFCList = []
    for M in MFC:
        MFCList.append(M)
    result = getWorkingForecast(groupAs, MFCList)
    if result["Result"] == 0:
        log(result["Data"])
        result["Data"] = "Fail - check logs"
    return result


@app.route("/getLatestForecastDailyFromDB", methods=['POST'])
def getLatestForecastDailyFromDB():
    params = request.get_json(silent=True)
    groupAs = params.get('GroupAs')
    MFC = params.get('MFC')
    MFCList = []
    for M in MFC:
        MFCList.append(M)
    result = getLatestForecastDaily(groupAs, MFCList)
    if result["Result"] == 0:
        log(result["Data"])
        result["Data"] = "Fail - check logs"
    return result


@app.route("/getActualsFromDB", methods=['POST'])
def getActualsFromDB():
    params = request.get_json(silent=True)
    groupAs = params.get('GroupAs')
    MFC = params.get('MFC')
    MFCList = []
    for M in MFC:
        MFCList.append(M)
    result = getActuals(groupAs, MFCList)
    if result["Result"] == 0:
        log(result["Data"])
        result["Data"] = "Fail - check logs"
    return result


@app.route("/loadMFCs", methods=['POST'])
def loadMFCsToDB():
    params = request.get_json(silent=True)
    MFCs = params.get('MFCs')
    result = delMFCList()
    if result["Result"] == 1:
        result = loadMFCList(MFCs)
        if result["Result"] == 0:
            log(result["Data"])
            result["Data"] = "Fail - check logs"
        return result
    else:
        log(result["Data"])
        result["Data"] = "Fail - check logs"
    return result


@app.route("/makeForecast", methods=['POST'])
def makeForecast():
    params = request.get_json(silent=True)
    result = forecast(params)
    cut_Down = result[['ds', 'yhat']]
    return str(cut_Down.to_json(orient='split'))


log("Server awake - checking actuals...")
gSyncActuals(['UK', 'ES', 'FR'])
log("Clearing old forecasts...")
deleteOldDailyForecasts()
log("Performing full re-forcast...")
fullReForecast()
#log("Performing one-off forecast history load...")
#loadForecastOneOff()
#log("Done")


if __name__ == "__main__":
    app.run()
