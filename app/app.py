from flask import Flask, request
from prophet import __version__
from _prophet import forecast, fullReForecast
import json
from os.path import exists

from apscheduler.schedulers.background import BackgroundScheduler

from _googlePull import buildDictionaries
from _loghandler import logger

from datetime import datetime

now = datetime.now()
dtNow = now.strftime("%d-%b-%Y")
app = Flask(__name__)


@app.route("/")
def root():
    html = "Welcome"
    return html.format(version=__version__)


@app.route("/version")
def version():
    return __version__


@app.route("/reRunAll", methods=['GET'])
def listMFCsFromDB():
    grabActuals
    result = listMFCs()
    if result["Result"] == 0:
        logger('W', result["Data"])
        result["Data"] = "Fail - check logs"
    return result


def hbLogic():
    today = datetime.now().strftime("%d-%b-%Y")
    timestampTrigger = today + ", 00:00:00" #Change back to 06:30
    dailyTriggerTime = datetime.strptime(timestampTrigger,  "%d-%b-%Y, %H:%M:%S")
    if (datetime.now() - dailyTriggerTime).total_seconds() > 0:
        # Check to see if already run today
        proceed = True
        data = {}
        if exists('heartbeat.json'):
            f = open('heartbeat.json')
            data = json.load(f)
            if data['lastDate'] == today:
                proceed = False

        if proceed:
            buildDictionaries()
            logger('I', "Performing full re-forcast...")
            logger('I', "06:30 checks Done")
            data['lastDate'] = today
            with open('heartbeat.json', 'w') as f:
                json.dump(data, f)


logger('I', "The server is now awake.")
OneOffLoad = False
if OneOffLoad:
    logger('I', "One-off forecast history load")
    loadForecastOneOff()
    logger('I', "DONE")

hbLogic()
heartBeat = BackgroundScheduler(daemon=True)
heartBeat.add_job(hbLogic, 'interval', minutes=5)
heartBeat.start()


if __name__ == "__main__":
    app.run()
