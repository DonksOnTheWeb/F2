from datetime import datetime

def logger(lvl, msg):
    timestamp = datetime.now().strftime("%d-%b-%Y, %H:%M:%S")
    if lvl == 'W':
        print(timestamp + ' : WARN : ' + msg)
    if lvl == 'I':
        print(timestamp + ' : INFO : ' + msg)
