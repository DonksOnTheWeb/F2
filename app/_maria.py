import mariadb
import json
import datetime

def returnConnection():
    f = open('db.json')
    data = json.load(f)

    try:
        conn = mariadb.connect(
            user=data["user"],
            password=data["password"],
            host=data["host"],
            port=data["port"],
            database=data["database"]
        )
        retVal = (1, conn)
    except mariadb.Error as e:
        retVal = (0, str(e))

    return retVal


def deleteOldForecast():
    retVal = {}
    try_Conn = returnConnection()
    last_week = datetime.date.today() - datetime.timedelta(days=7)
    date_format = "%Y-%m-%d"
    last_week = datetime.datetime.strftime(last_week, date_format)

    if try_Conn[0] == 1:
        my_Conn = try_Conn[1]
        cur = my_Conn.cursor(dictionary=True)
        statement = "DELETE from scfd where CreationDate < %s"
        last_week = (last_week, )
        try:
            cur.execute(statement, last_week)
            my_Conn.close()
            retVal["Result"] = 1
            retVal["Data"] = json.dumps("Success", default=str)
        except mariadb.Error as e:
            retVal["Result"] = 0
            retVal["Data"] = str(e)
        return retVal
    else:
        retVal["Result"] = 0
        retVal["Data"] = try_Conn[1]
        return retVal

def getForecastData():
    # j = params.get('j')
    # t = params.get('t')
    retVal = {}
    try_Conn = returnConnection()
    if try_Conn[0] == 1:
        my_Conn = try_Conn[1]
        cur = my_Conn.cursor(dictionary=True)
        statement = "SELECT * from scf"
        try:
            cur.execute(statement)
            result = cur.fetchall()
            my_Conn.close()
            retVal["Result"] = 1
            retVal["Data"] = json.dumps(result, default=str)
        except mariadb.Error as e:
            retVal["Result"] = 0
            retVal["Data"] = str(e)
        return retVal
    else:
        retVal["Result"] = 0
        retVal["Data"] = try_Conn[1]
        return retVal


def latestForecastDailyDate():
    retVal = {}
    try_Conn = returnConnection()
    if try_Conn[0] == 1:
        my_Conn = try_Conn[1]
        cur = my_Conn.cursor(dictionary=True)
        statement = "SELECT max(CreationDate) as latest from scfd"
        try:
            cur.execute(statement)
            result = cur.fetchall()
            my_Conn.close()
            retVal["Result"] = 1
            retVal["Data"] = json.dumps(result, default=str)
        except mariadb.Error as e:
            retVal["Result"] = 0
            retVal["Data"] = str(e)
        return retVal
    else:
        retVal["Result"] = 0
        retVal["Data"] = try_Conn[1]
        return retVal


def getLatestForecastDailyData(ctry):
    retVal = {}
    try_Conn = returnConnection()
    if try_Conn[0] == 1:
        my_Conn = try_Conn[1]
        cur = my_Conn.cursor(dictionary=True)
        statement = "SELECT * from scfd where CreationDate = (select max(CreationDate) from scfd) and J = %s " \
                    "order by L, Asat "
        try:
            country = (ctry,)
            cur.execute(statement, country)
            result = cur.fetchall()
            my_Conn.close()
            retVal["Result"] = 1
            retVal["Data"] = json.dumps(result, default=str)
        except mariadb.Error as e:
            retVal["Result"] = 0
            retVal["Data"] = str(e)
        return retVal
    else:
        retVal["Result"] = 0
        retVal["Data"] = try_Conn[1]
        return retVal


def latestActualDate():
    retVal = {}
    try_Conn = returnConnection()
    if try_Conn[0] == 1:
        my_Conn = try_Conn[1]
        cur = my_Conn.cursor(dictionary=True)
        statement = "SELECT max(asat) as latest, j from sca group by j"
        try:
            cur.execute(statement)
            result = cur.fetchall()
            my_Conn.close()
            retVal["Result"] = 1
            retVal["Data"] = json.dumps(result, default=str)
        except mariadb.Error as e:
            retVal["Result"] = 0
            retVal["Data"] = str(e)
        return retVal
    else:
        retVal["Result"] = 0
        retVal["Data"] = try_Conn[1]
        return retVal


def getLatestActualData(ctry, from_date):
    retVal = {}
    try_Conn = returnConnection()
    if try_Conn[0] == 1:
        my_Conn = try_Conn[1]
        cur = my_Conn.cursor(dictionary=True)
        statement = "SELECT * from sca where J = %s"
        try:
            if from_date is not None:
                statement = statement + " and Asat >= %s"
                params = (ctry, from_date, )
            else:
                params = (ctry, )

            statement = statement + " order by L, Asat"
            if ctry == "All":
                statement = "SELECT * from sca order by L, Asat"
                cur.execute(statement)
            else:
                cur.execute(statement, params)

            result = cur.fetchall()
            my_Conn.close()
            retVal["Result"] = 1
            retVal["Data"] = json.dumps(result, default=str)
        except mariadb.Error as e:
            retVal["Result"] = 0
            retVal["Data"] = str(e)
        return retVal
    else:
        retVal["Result"] = 0
        retVal["Data"] = try_Conn[1]
        return retVal


def loadActuals(new_entries):
    retVal = {}
    records = []
    counter = 0
    total = 0
    try_Conn = returnConnection()
    if try_Conn[0] == 1:
        my_Conn = try_Conn[1]
        cur = my_Conn.cursor()
        insert_query = "INSERT IGNORE INTO sca (Asat, L, J, Act) VALUES (%s, %s, %s, %s)"
        try:
            for entry in new_entries:
                dte = entry[0]
                locid = entry[1]
                j = entry[2]
                act = int(entry[3])
                recordEntry = (dte, locid, j, act)
                records.append(recordEntry)
                counter = counter + 1
                total = total + 1
                if counter == 999:
                    cur.executemany(insert_query, records)
                    counter = 0
                    records = []

            if counter > 0:
                cur.executemany(insert_query, records)

            my_Conn.commit()
            my_Conn.close()
            retVal["Result"] = 1
            retVal["Data"] = total
        except mariadb.Error as e:
            retVal["Result"] = 0
            retVal["Data"] = str(e)
        return retVal
    else:
        retVal["Result"] = 0
        retVal["Data"] = try_Conn[1]
        return retVal


def loadDailyForecast(new_entries):
    retVal = {}
    records = []
    counter = 0
    total = 0
    try_Conn = returnConnection()
    if try_Conn[0] == 1:
        my_Conn = try_Conn[1]
        cur = my_Conn.cursor()
        insert_query = "INSERT IGNORE INTO scfd (CreationDate, Asat, L, J, Forecast) VALUES (%s, %s, %s, %s, %s)"
        try:
            for entry in new_entries:
                creation_dte = entry[0]
                dte = entry[1]
                locid = entry[2]
                j = entry[3]
                forecast = int(entry[4])
                recordEntry = (creation_dte, dte, locid, j, forecast)
                records.append(recordEntry)
                counter = counter + 1
                total = total + 1
                if counter == 999:
                    cur.executemany(insert_query, records)
                    counter = 0
                    records = []

            if counter > 0:
                cur.executemany(insert_query, records)

            my_Conn.commit()
            my_Conn.close()
            retVal["Result"] = 1
            retVal["Data"] = total
        except mariadb.Error as e:
            retVal["Result"] = 0
            retVal["Data"] = str(e)
        return retVal
    else:
        retVal["Result"] = 0
        retVal["Data"] = try_Conn[1]
        return retVal








def loadForecast(new_entries):
    records = []
    counter = 0
    total = 0
    try_Conn = returnConnection()
    if try_Conn[0] == 1:
        my_Conn = try_Conn[1]
        cur = my_Conn.cursor()
        insert_query = "INSERT IGNORE INTO scfh (Asat, L, J, Override) VALUES (%s, %s, %s, %s)"
        for entry in new_entries:
            dte = entry[0]
            locid = entry[1]
            j = 'CTRY'
            print(dte, locid, j, str(entry[2]))
            forecast = int(entry[2])

            print(dte, locid, j, str(forecast))

            recordEntry = (dte, locid, j, forecast)
            records.append(recordEntry)
            counter = counter + 1
            total = total + 1
            if counter == 999:
                cur.executemany(insert_query, records)
                counter = 0
                records = []

        if counter > 0:
            cur.executemany(insert_query, records)

        my_Conn.commit()
        my_Conn.close()