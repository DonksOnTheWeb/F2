from flask import Flask, request
from prophet import __version__
from _prophet import forecast, fullReForecast
from _maria import getForecastData, getLatestForecastDailyData, getLatestActualData, deleteOldForecast
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


@app.route("/getForecastFromDB", methods=['GET'])
def getForecastFromDB():
    params = request.get_json(silent=True)
    result = getForecastData()
    if result["Result"] == 0:
        log(result["Data"])
        result["Data"] = "Fail - check logs"
    return result


@app.route("/getLatestForecastDailyFromDB", methods=['POST'])
def getLatestForecastDailyFromDB():
    params = request.get_json(silent=True)
    ctry = params.get('Country')
    result = getLatestForecastDailyData(ctry)
    if result["Result"] == 0:
        log(result["Data"])
        result["Data"] = "Fail - check logs"
    return result


@app.route("/getLatestActualsFromDB", methods=['POST'])
def getLatestActualsFromDB():
    params = request.get_json(silent=True)
    ctry = params.get('Country')
    result = getLatestActualData(ctry, None)
    if result["Result"] == 0:
        log(result["Data"])
        result["Data"] = "Fail - check logs"
    return result


@app.route("/syncActuals", methods=['GET'])
def syncActuals():
    countries = ['UK', 'FR', 'ES']
    result = gSyncActuals(countries)
    if result["Result"] == 0:
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
syncActuals()
log("Clearing old forecasts...")
deleteOldForecast()
log("Performing full re-forcast...")
fullReForecast()

#log("Performing one-off...")
#loadForecastOneOff()
#log("Done")


if __name__ == "__main__":
    app.run()
