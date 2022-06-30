import logging
from datetime import datetime


def logger(level, msg):
    timestamp = datetime.now().strftime("%m/%d/%Y, %H:%M:%S")
    match level:
        case 'W':
            logging.warning(msg)
            print(timestamp + ' : WARN : ' + msg)
        case 'I':
            logging.info(msg)
            print(timestamp + ' : INFO : ' + msg)