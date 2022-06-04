import pytz
import datetime


def log(string):
    timezone = pytz.timezone('Europe/Luxembourg')
    ts = datetime.datetime.now(tz=timezone)
    date_format = "%Y-%m-%d %H:%M:%S"
    print(datetime.datetime.strftime(ts, date_format) + " - " + string)
