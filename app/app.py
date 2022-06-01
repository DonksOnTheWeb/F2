from flask import Flask, request
from prophet import __version__
from _prophet import forecast

app = Flask(__name__)

@app.route("/")
def root():
    html = "<h3>Welcome</h3>"
    return html.format(version=__version__)

@app.route("/forecast", methods=['POST'])
def makeForecast():
    params = request.get_json(silent=True)
    result = forecast(params)
    cutDown = result[['ds', 'yhat']]
    return cutDown.to_json(orient='split')
    #return result.to_json(orient='split')

@app.route("/version")
def version():
    return __version__

if __name__ == "__main__":
    #app.run(host='0.0.0.0')
    app.run()