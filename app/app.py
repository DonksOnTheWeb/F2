from flask import Flask, request
from prophet import __version__
from _prophet import forecast
from _maria import getData, pushData

app = Flask(__name__)


@app.route("/")
def root():
    html = "Welcome"
    return html.format(version=__version__)


@app.route("/getFromDB", methods=['POST'])
def getFromDB():
    params = request.get_json(silent=True)
    result = getData(params)
    return result


@app.route("/pushToDB", methods=['POST'])
def pushToDB():
    params = request.get_json(silent=True)
    result = pushData(params)
    return result


@app.route("/forecast", methods=['POST'])
def makeForecast():
    params = request.get_json(silent=True)
    result = forecast(params)
    cut_Down = result[['ds', 'yhat']]
    return cut_Down.to_json(orient='split')


@app.route("/version")
def version():
    return __version__


if __name__ == "__main__":
    app.run()
