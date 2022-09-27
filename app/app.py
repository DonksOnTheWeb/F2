import json
from os.path import exists
from apscheduler.schedulers.background import BackgroundScheduler

from _main import weeklyForecastRoutine

from _loghandler import logger

from datetime import datetime
import time

now = datetime.now()
dtNow = now.strftime("%d-%b-%Y")

kick_off_at = '05:45:00'
full_re_roll = 'Mon'


# *** GITHUB and EXCEPTIONS ***


def hbLogic():
    today = datetime.now().strftime("%d-%b-%Y")
    timestampTrigger = today + ", " + kick_off_at
    dailyTriggerTime = datetime.strptime(timestampTrigger, "%d-%b-%Y, %H:%M:%S")
    proceedOverride = False
    proceed = False
    data = {}
    if not exists('heartbeat.json'):  # New Container
        logger('I', "No heartbeat file.  Will perform full re-roll")
        proceedOverride = True

    if (datetime.now() - dailyTriggerTime).total_seconds() > 0:
        if datetime.now().strftime("%a") == full_re_roll:
            # Check to see if already run today
            proceed = True
            if exists('heartbeat.json'):
                f = open('heartbeat.json')
                data = json.load(f)
                if data['lastDate'] == today:
                    proceed = False

    if proceed or proceedOverride:
        logger('I', "Performing full re-roll of hyper parameters and hourly curves")
        weeklyForecastRoutine()
        logger('I', kick_off_at + " checks Done")

        data['lastDate'] = today
        with open('heartbeat.json', 'w') as f:
            json.dump(data, f)


logger('I', "Application is now awake.")
hbLogic()
heartBeat = BackgroundScheduler(daemon=True)
heartBeat.add_job(hbLogic, 'interval', minutes=5)
heartBeat.start()

while True:
    time.sleep(60)
