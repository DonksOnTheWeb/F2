from flask import Flask, request
from prophet import __version__
from _prophet import forecast, fullReForecast

from _maria import getOfficialForecast, getLatestForecastDaily, getActuals, getWorkingForecast, listMFCs
from _maria import deleteOldDailyForecasts, loadMFCList, delMFCList, updateWkg, determineTiers, ignoreWeeksOn, ignoreWeeksOff
from _maria import getWeeksMatrix, getIgnoredWeeksForMFCs, copyToWorking, submitForecast, unGrouped

from _googlePull import gSyncActuals, loadForecastOneOff, writeForecastToSheet
from loghandler import logger
import logging

from datetime import datetime

now = datetime.now()
dtNow = now.strftime("%d-%b-%Y")
# logging.basicConfig(filename='../logs/' + dtNow + '.log', level=logging.INFO)

logging.basicConfig(
    filename='../logs/' + dtNow + '.log',
    level=logging.INFO,
    format='%(asctime)s.%(msecs)03d %(levelname)s %(module)s - %(funcName)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)

logger('I', "STARTING UP THE LOGGER")

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
        logger('W', result["Data"])
        result["Data"] = "Fail - check logs"
    return result


@app.route("/listMFCs", methods=['GET'])
def listMFCsFromDB():
    result = listMFCs()
    if result["Result"] == 0:
        logger('W', result["Data"])
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
        logger('W', result["Data"])
        result["Data"] = "Fail - check logs"
    return result


@app.route("/getWeeks", methods=['POST'])
def getWeeks():
    params = request.get_json(silent=True)
    MFC = params.get('MFC')
    MFCList = []
    for M in MFC:
        MFCList.append(M)
    result = getIgnoredWeeksForMFCs(MFCList, len(MFCList))

    if result["Result"] == 0:
        logger('W', result["Data"])
        result["Data"] = "Fail - check logs"
    return result


@app.route("/getWeeksOverview", methods=['GET'])
def getWeeksOverview():
    result = getWeeksMatrix()

    if result["Result"] == 0:
        logger('W', result["Data"])
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
        logger('W', result["Data"])
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
        logger('W', result["Data"])
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
        logger('W', result["Data"])
        result["Data"] = "Fail - check logs"
    return result


@app.route("/submitForecast", methods=['POST'])
def submitForecastToDB():
    params = request.get_json(silent=True)
    ctry = params.get('Country')
    InOff = params.get('IO')
    logger('I', "Submitting " + InOff + "")
    result = submitForecast(ctry, InOff)

    if result["Result"] == 0:
        logger('W', result["Data"])
        result["Data"] = "Fail - check logs"
    else:
        # Upload To Google
        result = writeForecastToSheet(ctry, InOff)

    return result


@app.route("/getUngrouped", methods=['POST'])
def getUngroupedFromDB():
    params = request.get_json(silent=True)
    MFC = params.get('MFC')
    table = params.get('Table')
    MFCList = []
    for M in MFC:
        MFCList.append(M)
    result = unGrouped(MFCList, table)
    if result["Result"] == 0:
        logger('W', result["Data"])
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
        logger('W', result["Data"])
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
            logger('W', result["Data"])
            result["Data"] = "Fail - check logs"
        return result
    else:
        logger('W', result["Data"])
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
        logger('W', result["Data"])
        result["Data"] = "Fail - check logs"
    return result


#@app.route("/makeForecast", methods=['POST'])
#def makeForecast():
#    params = request.get_json(silent=True)
#    result = forecast(params)
#    cut_Down = result[['ds', 'yhat']]
#    return str(cut_Down.to_json(orient='split'))


logger('I', "Server is now awake - checking actuals...")
gSyncActuals(['UK', 'ES', 'FR'])
logger('I', "Clearing old forecasts...")
deleteOldDailyForecasts()
logger('I', "Performing full re-forcast...")
fullReForecast()
if datetime.today().weekday() == 0:
    logger('I', "It's Monday ... Re-determining Tiers")
    determineTiers()
    logger('I', "It's Monday ... Copying last forecasts to Working")
    copyToWorking()

OneOffLoaf = False
if OneOffLoaf:
    logger('I', "One-off forecast history load")
    loadForecastOneOff()

logger('I', "Startup Done")

if __name__ == "__main__":
    app.run()
