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
            try:
                statement = "SELECT Asat, '" + groupAs + "' AS Location, SUM(Forecast) as Forecast FROM scfh WHERE"
                statement = statement + " Location IN (" + MFCList + ") Group By Asat order by Asat"
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
            statement = "SELECT Asat, '" + groupAs + "' as Location, sum(Forecast) as Forecast from scfd where "
            statement = statement + "CreationDate = (select max(CreationDate) from scfd) and Location in "
            statement = statement + "(" + MFCList + ") Group by Asat order by AsAt"
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
            statement = "WITH window AS("
            statement = statement + "SELECT *, ROW_NUMBER() OVER (PARTITION BY Location ORDER BY IO ASC) as RN from scfw)"
            statement = statement + " SELECT Asat, CASE WHEN Asat IS NULL THEN NULL ELSE '" + groupAs + "' END AS Location, sum(Baseline) as Baseline"
            statement = statement + " , sum(Override) as Override from window where Location in (" + MFCList + ") AND RN = 1"
            statement = statement + " Group by Asat order by Asat"
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
            statement = "SELECT Asat, '" + groupAs + "' as Location, sum(Act) as Act from sca where "
            statement = statement + "Location in (" + MFCList + ") group by Asat order by Asat "
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
        insert_query = "INSERT IGNORE INTO locations (location, Country, City, ShortName, AirportCode, CountryGroup, RegionGroup) VALUES " \
                       "(%s, %s, %s, %s,%s, %s, %s)"
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
