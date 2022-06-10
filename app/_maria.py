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


def latestActualDate():
    retVal = {}
    try_Conn = returnConnection()
    if try_Conn[0] == 1:
        my_Conn = try_Conn[1]
        cur = my_Conn.cursor(dictionary=True)
        statement = "SELECT max(asat) as latest from sca"
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


def cleanseMFCList(MFCList):
    retVal = {}
    try_Conn = returnConnection()
    if try_Conn[0] == 1:
        my_Conn = try_Conn[1]
        cur = my_Conn.cursor(dictionary=True)
        try:
            statement = "SELECT distinct(Location) FROM Locations WHERE Location IN ({0})".format(', '.join(['?'] * len(MFCList)))
            cur.execute(statement, MFCList)
            result = cur.fetchall()
            my_Conn.close()
            retVal["Result"] = 1
            MFCList = []
            for entry in result:
                MFCList.append(entry["Location"])

            retVal["Data"] = ', '.join(f'\'{M}\'' for M in MFCList)
        except mariadb.Error as e:
            retVal["Result"] = 0
            retVal["Data"] = str(e)
        return retVal
    else:
        retVal["Result"] = 0
        retVal["Data"] = try_Conn[1]
        return retVal


def deleteOldDailyForecasts():
    retVal = {}
    try_Conn = returnConnection()
    last_week = datetime.date.today() - datetime.timedelta(days=7)
    date_format = "%Y-%m-%d"
    last_week = datetime.datetime.strftime(last_week, date_format)

    if try_Conn[0] == 1:
        my_Conn = try_Conn[1]
        cur = my_Conn.cursor(dictionary=True)
        statement = "DELETE from scfd where CreationDate < %s"
        last_week = (last_week,)
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


#Called from REST API


def listMFCs():
    retVal = {}
    try_Conn = returnConnection()
    if try_Conn[0] == 1:
        my_Conn = try_Conn[1]
        cur = my_Conn.cursor(dictionary=True)
        statement = "SELECT * from Locations"
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


def getForecastHistory(groupAs, MFCList):
    retVal = {}
    MFCList = cleanseMFCList(MFCList)
    if MFCList["Result"] == 1:
        MFCList = MFCList["Data"]
        try_Conn = returnConnection()
        if try_Conn[0] == 1:
            my_Conn = try_Conn[1]
            cur = my_Conn.cursor(dictionary=True)
            statement = "SELECT Date_format(Asat,'%d-%b-%Y') as Asat," \
                        " DATE_FORMAT(DATE_ADD(Asat, INTERVAL - WEEKDAY(Asat) DAY), '%d-%b-%Y') AS Commencing," \
                        " SUM(Forecast) as Forecast FROM scfh WHERE Location IN (" + MFCList + ") Group By Asat order by scfh.Asat"
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
    else:
        retVal["Result"] = 0
        retVal["Data"] = MFCList["Data"]
        return retVal


def getLatestForecastDaily(groupAs, MFCList):
    retVal = {}
    MFCList = cleanseMFCList(MFCList)
    if MFCList["Result"] == 1:
        MFCList = MFCList["Data"]

        try_Conn = returnConnection()
        if try_Conn[0] == 1:
            my_Conn = try_Conn[1]
            cur = my_Conn.cursor(dictionary=True)
            statement = "SELECT Date_format(Asat,'%d-%b-%Y') as Asat," \
                        " DATE_FORMAT(DATE_ADD(Asat, INTERVAL - WEEKDAY(Asat) DAY), '%d-%b-%Y') AS Commencing," \
                        " sum(Forecast) as Forecast from scfd where CreationDate = (select max(CreationDate) from scfd) and" \
                        " Location in (" + MFCList + ") Group by Asat order by scfd.AsAt"
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
    else:
        retVal["Result"] = 0
        retVal["Data"] = MFCList["Data"]


def getWorkingForecast(groupAs, MFCList):
    retVal = {}
    MFCList = cleanseMFCList(MFCList)
    if MFCList["Result"] == 1:
        MFCList = MFCList["Data"]
        try_Conn = returnConnection()
        if try_Conn[0] == 1:
            my_Conn = try_Conn[1]
            cur = my_Conn.cursor(dictionary=True)
            statement = "WITH window AS(SELECT *, ROW_NUMBER() OVER (PARTITION BY Location ORDER BY IO ASC) as RN from scfw) " \
                        "SELECT Date_format(Asat,'%d-%b-%Y') as Asat," \
                        " DATE_FORMAT(DATE_ADD(Asat, INTERVAL - WEEKDAY(Asat) DAY), '%d-%b-%Y') AS Commencing," \
                        " sum(Baseline) as Baseline, sum(Override) as Override from window where" \
                        " Location in (" + MFCList + ") AND RN = 1 Group by Asat order by window.Asat"
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
    else:
        retVal["Result"] = 0
        retVal["Data"] = MFCList["Data"]

def getActuals(groupAs, MFCList):
    retVal = {}
    MFCList = cleanseMFCList(MFCList)
    if MFCList["Result"] == 1:
        MFCList = MFCList["Data"]
        try_Conn = returnConnection()
        if try_Conn[0] == 1:
            my_Conn = try_Conn[1]
            cur = my_Conn.cursor(dictionary=True)
            statement = "SELECT DATE_FORMAT(db_date,'%d-%b-%Y') AS Asat," \
                        " DATE_FORMAT(DATE_ADD(db_date, INTERVAL - WEEKDAY(db_date) DAY), '%d-%b-%Y') AS Commencing," \
                        " SUM(Act) AS Act FROM DateDimension d " \
                        " LEFT OUTER JOIN sca a ON d.db_date = a.Asat AND Location IN (" + MFCList + ")" \
                        " WHERE db_date >= (SELECT MIN(Asat) FROM sca WHERE Location IN (" + MFCList + "))" \
                        " AND db_date <= (SELECT MAX(Asat) FROM sca WHERE Location IN (" + MFCList + ")) " \
                        " GROUP BY db_date ORDER BY db_date"
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
    else:
        retVal["Result"] = 0
        retVal["Data"] = MFCList["Data"]

def getAllActuals():
    #USed by the fullReForecast triggered every AM
    retVal = {}
    try_Conn = returnConnection()
    if try_Conn[0] == 1:
        my_Conn = try_Conn[1]
        cur = my_Conn.cursor(dictionary=True)
        statement = "SELECT Asat, Location, Act from sca"
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


# delete Functions
def delMFCList():
    retVal = {}
    try_Conn = returnConnection()
    if try_Conn[0] == 1:
        my_Conn = try_Conn[1]
        cur = my_Conn.cursor()
        delete_query = "Delete from Locations"
        try:
            cur.execute(delete_query)
            my_Conn.commit()
            my_Conn.close()
            retVal["Result"] = 1
        except mariadb.Error as e:
            retVal["Result"] = 0
            retVal["Data"] = str(e)
        return retVal
    else:
        retVal["Result"] = 0
        retVal["Data"] = try_Conn[1]
        return retVal


#Load Functions:


def loadMFCList(new_entries):
    retVal = {}
    records = []
    counter = 0
    total = 0
    try_Conn = returnConnection()
    if try_Conn[0] == 1:
        my_Conn = try_Conn[1]
        cur = my_Conn.cursor()
        insert_query = "INSERT IGNORE INTO Locations (Location, Country, City, FriendlyName, ShortName, AirportCode, Region) " \
                       "VALUES (%s, %s, %s, %s,%s, %s, %s)"
        try:
            for entry in new_entries:
                recordEntry = (entry[0], entry[1], entry[2], entry[3], entry[4], entry[5], entry[6])
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
            retVal["Data"] = "Success"
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
        insert_query = "INSERT IGNORE INTO sca (Asat, Location, Act) VALUES (%s, %s, %s)"
        try:
            for entry in new_entries:
                dte = entry[0]
                locid = entry[1]
                act = int(entry[2])
                recordEntry = (dte, locid, act)
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
        insert_query = "INSERT IGNORE INTO scfd (CreationDate, Asat, Location, Forecast) VALUES (%s, %s, %s, %s)"
        try:
            for entry in new_entries:
                creation_dte = entry[0]
                dte = entry[1]
                locid = entry[2]
                forecast = int(entry[3])
                recordEntry = (creation_dte, dte, locid, forecast)
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
        insert_query = "INSERT IGNORE INTO scfh (Asat, L, J, Forecast) VALUES (%s, %s, %s, %s)"
        for entry in new_entries:
            dte = entry[0]
            locid = entry[1]
            j = entry[2]
            forecast = int(entry[3])
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
