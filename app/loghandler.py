import logging
from datetime import datetime


def logger(lvl, msg):
    timestamp = datetime.now().strftime("%m/%d/%Y, %H:%M:%S")
    if lvl == 'W':
        logging.warning(msg)
        print(timestamp + ' : WARN : ' + msg)
    if lvl == 'I':
        logging.info(msg)
        print(timestamp + ' : INFO : ' + msg)