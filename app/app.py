from flask import Flask, request
from prophet import __version__
from _prophet import forecast, fullReForecast
from _maria import getForecastData
from _googlePull import gSyncActuals
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
    if result["Result"] == 1:
        return str(result["Data"])
    else:
        return "Fail - check logs"


@app.route("/syncActuals", methods=['GET'])
def syncActuals():
    countries = ['UK', 'FR', 'ES']
    result = gSyncActuals(countries)
    if result["Result"] == 1:
        return str(result["Data"])
    else:
        return "Fail - check logs"


@app.route("/makeForecast", methods=['POST'])
def makeForecast():
    params = request.get_json(silent=True)
    result = forecast(params)
    cut_Down = result[['ds', 'yhat']]
    return str(cut_Down.to_json(orient='split'))

log("Server awake - checking actuals...")
syncActuals()
log("Performing full re-forcast...")
fullReForecast()

if __name__ == "__main__":
    app.run()

