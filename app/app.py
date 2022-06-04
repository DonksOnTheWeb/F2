from flask import Flask, request
from prophet import __version__
from _prophet import forecast
from _maria import getForecastData
from _googlePull import gSyncActuals
import json

app = Flask(__name__)


@app.route("/")
def root():
    html = "Welcome"
    return html.format(version=__version__)


@app.route("/version")
def version():
    return __version__


@app.route("/getForecastFromDB", methods=['POST'])
def getForecastFromDB():
    params = request.get_json(silent=True)
    result = getForecastData(params)
    return result


@app.route("/syncActuals", methods=['GET'])
def syncActuals():
    countries = ['UK', 'FR', 'ES']
    result = gSyncActuals(countries)
    if result["Result"] == 1:
        return "Success"
    else:
        return "Fail - check logs"



@app.route("/forecast", methods=['POST'])
def makeForecast():
    params = request.get_json(silent=True)
    result = forecast(params)
    cut_Down = result[['ds', 'yhat']]
    return cut_Down.to_json(orient='split')




if __name__ == "__main__":
    app.run()

