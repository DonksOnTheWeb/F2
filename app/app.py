from flask import Flask, request
from prophet import __version__
from _prophet import forecast, fullReForecast

from _maria import getOfficialForecast, getLatestForecastDaily, getActuals, getWorkingForecast, listMFCs
from _maria import deleteOldDailyForecasts, loadMFCList, delMFCList, updateWkg, redetermineTiers, ignoreWeeksOn, ignoreWeeksOff, getIgnoredWeeks

from _googlePull import gSyncActuals, loadForecastOneOff

import logging

from datetime import datetime
now = datetime.now()
dtNow = now.strftime("%d-%b-%Y")
#logging.basicConfig(filename='../logs/' + dtNow + '.log', level=logging.INFO)

logging.basicConfig(
    filename='../logs/' + dtNow + '.log',
    level=logging.INFO,
    format='%(asctime)s.%(msecs)03d %(levelname)s %(module)s - %(funcName)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)


logging.info("STARTING UP THE LOGGER")

app = Flask(__name__)


@app.route("/")
def root():
    html = "Welcome"
    return html.format(version=__version__)


@app.route("/version")
def version():
    return __version__


@app.route("/getOfficialForecastFromDB", methods=['POST'])
def getOfficialForecastFromDB():
    params = request.get_json(silent=True)
    groupAs = params.get('GroupAs')
    MFC = params.get('MFC')
    MFCList = []
    for M in MFC:
        MFCList.append(M)
    result = getOfficialForecast(groupAs, MFCList)
    if result["Result"] == 0:
        logging.warning(result["Data"])
        result["Data"] = "Fail - check logs"
    return result


@app.route("/listMFCs", methods=['GET'])
def listMFCsFromDB():
    result = listMFCs()
    if result["Result"] == 0:
        logging.warning(result["Data"])
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
        logging.warning(result["Data"])
        result["Data"] = "Fail - check logs"
    return result


@app.route("/getWeeks", methods=['POST'])
def getWeeks():
    params = request.get_json(silent=True)
    MFC = params.get('MFC')
    MFCList = []
    for M in MFC:
        MFCList.append(M)
    result = getIgnoredWeeks(MFCList, len(MFCList))

    if result["Result"] == 0:
        logging.warning(result["Data"])
        result["Data"] = "Fail - check logs"
    return result


@app.route("/updateWeeks", methods=['POST'])
def toggleIgnoredWeeks():
    params = request.get_json(silent=True)
    MFC = params.get('MFC')
    MFCList = []
    for M in MFC:
        MFCList.append(M)
    WeekCommencing = params.get('WeekCommencing')
    ToggleIgnoreOn = params.get('ToggleIgnoreOn')
    if ToggleIgnoreOn == 'True':
        result = ignoreWeeksOn(MFCList, WeekCommencing)
    else:
        result = ignoreWeeksOff(MFCList, WeekCommencing)

    if result["Result"] == 0:
        logging.warning(result["Data"])
        result["Data"] = "Fail - check logs"
    return result


@app.route("/updateWorking", methods=['POST'])
def updateWorkingForecast():
    params = request.get_json(silent=True)
    MFC = params.get('MFC')
    MFCList = []
    for M in MFC:
        MFCList.append(M)
    Updates = params.get('Updates')
    result = updateWkg(MFCList, Updates)
    if result["Result"] == 0:
        logging.warning(result["Data"])
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
        logging.warning(result["Data"])
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
        logging.warning(result["Data"])
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
            logging.warning(result["Data"])
            result["Data"] = "Fail - check logs"
        return result
    else:
        logging.warning(result["Data"])
        result["Data"] = "Fail - check logs"
    return result


@app.route("/onTheFlyForecast", methods=['POST'])
def onTheFlyForecast():
    params = request.get_json(silent=True)
    groupAs = params.get('GroupAs')
    MFC = params.get('MFC')
    MFCList = []
    for M in MFC:
        MFCList.append(M)
    result = forecast(groupAs, MFCList)
    if result["Result"] == 0:
        logging.warning(result["Data"])
        result["Data"] = "Fail - check logs"
    return result


@app.route("/makeForecast", methods=['POST'])
def makeForecast():
    params = request.get_json(silent=True)
    result = forecast(params)
    cut_Down = result[['ds', 'yhat']]
    return str(cut_Down.to_json(orient='split'))


logging.info("Server awake - checking actuals...")
gSyncActuals(['UK', 'ES', 'FR'])
logging.info("Clearing old forecasts...")
deleteOldDailyForecasts()
logging.info("Performing full re-forcast...")
fullReForecast()
logging.info("Re-determining Tiers (if Monday)")
redetermineTiers()
logging.info("One-off forecast history load disabled")
#loadForecastOneOff()
logging.info("Done")

if __name__ == "__main__":
    app.run()
