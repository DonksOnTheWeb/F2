import mariadb
import json
import datetime
import uuid


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
        statement = "SELECT max(asat) as latest from Actuals"
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
        statement = "SELECT max(CreationDate) as latest from AutoForecast"
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


def countryFromMFC(MFC):
    retVal = {}
    try_Conn = returnConnection()
    if try_Conn[0] == 1:
        my_Conn = try_Conn[1]
        cur = my_Conn.cursor(dictionary=True)
        statement = "SELECT Country FROM Locations WHERE Location = %s"
        try:
            MFC = (MFC,)
            cur.execute(statement, MFC)
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
        statement = "DELETE from AutoForecast where CreationDate < %s"
        last_week = (last_week,)
        try:
            cur.execute(statement, last_week)
            my_Conn.commit()
            my_Conn.close()
            retVal["Result"] = 1
            retVal["Data"] = json.dumps('Success', default=str)
        except mariadb.Error as e:
            retVal["Result"] = 0
            retVal["Data"] = str(e)
        return retVal
    else:
        retVal["Result"] = 0
        retVal["Data"] = try_Conn[1]
        return retVal


def copyToWorking():
    retVal = {}
    try_Conn = returnConnection()

    if try_Conn[0] == 1:
        my_Conn = try_Conn[1]
        cur = my_Conn.cursor(dictionary=True)
        statement = "call copyToWorking()"
        try:
            cur.execute(statement)
            my_Conn.commit()
            my_Conn.close()
            retVal["Result"] = 1
            retVal["Data"] = json.dumps('Success', default=str)
        except mariadb.Error as e:
            retVal["Result"] = 0
            retVal["Data"] = str(e)
        return retVal
    else:
        retVal["Result"] = 0
        retVal["Data"] = try_Conn[1]
        return retVal


def determineTiers():
    retVal = {}
    try_Conn = returnConnection()

    if try_Conn[0] == 1:
        my_Conn = try_Conn[1]
        cur = my_Conn.cursor(dictionary=True)
        statement = "call determineTiers()"
        try:
            cur.execute(statement)
            my_Conn.commit()
            my_Conn.close()
            retVal["Result"] = 1
            retVal["Data"] = json.dumps('Success', default=str)
        except mariadb.Error as e:
            retVal["Result"] = 0
            retVal["Data"] = str(e)
        return retVal
    else:
        retVal["Result"] = 0
        retVal["Data"] = try_Conn[1]
        return retVal


def submitForecast(ctry, InOff):
    retVal = {}
    try_Conn = returnConnection()

    if try_Conn[0] == 1:
        my_Conn = try_Conn[1]
        cur = my_Conn.cursor(dictionary=True)
        statement = "call submitOfficial('" + ctry + "')"
        if InOff == 'I':
            statement = "call submitIntra('" + ctry + "')"
        try:
            cur.execute(statement)
            my_Conn.commit()
            my_Conn.close()
            retVal["Result"] = 1
            retVal["Data"] = json.dumps('Success', default=str)
        except mariadb.Error as e:
            retVal["Result"] = 0
            retVal["Data"] = str(e)
        return retVal
    else:
        retVal["Result"] = 0
        retVal["Data"] = try_Conn[1]
        return retVal


# Called from REST API
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


def getFullForecast(ctry, InOff):  # IS THIS USED???
    retVal = {}
    try_Conn = returnConnection()
    if try_Conn[0] == 1:
        my_Conn = try_Conn[1]
        cur = my_Conn.cursor(dictionary=True)
        statement = "SELECT * from OfficialForecast where Location in (select Location from Locations where Country = '" + ctry + "')" \
                    " AND Asat >= DATE_ADD(DATE(DATE_ADD(NOW(), INTERVAL(-WEEKDAY(NOW())) DAY)) , INTERVAL 7 DAY) order by Location, Asat"
        if InOff == 'I':
            statement = "SELECT * from IntraForecast where Location in (select Location from Locations where Country = '" + ctry + "')" \
                    " AND Asat > DATE(NOW()) order by Location, Asat"
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


def getOfficialForecast(groupAs, MFCList):
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
                        " SUM(Forecast) as Forecast FROM OfficialForecast WHERE Location IN (" + MFCList + ")" \
                        " Group By Asat order by OfficialForecast.Asat"
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


def unGrouped(MFCList, table):
    retVal = {}
    MFCList = cleanseMFCList(MFCList)
    if MFCList["Result"] == 1:
        MFCList = MFCList["Data"]

        try_Conn = returnConnection()
        if try_Conn[0] == 1:
            my_Conn = try_Conn[1]
            cur = my_Conn.cursor(dictionary=True)

            statement = "SELECT Date_Format(STR_TO_DATE(CONCAT(YEARWEEK(Asat, 1),'Monday'), '%x%v %W'), '%d-%b-%Y') AS WeekCommencing, Location, SUM(Act) AS Act" \
                        " FROM Actuals" \
                        " WHERE DATEDIFF(STR_TO_DATE(CONCAT(YEARWEEK(Asat, 1),'Monday'), '%x%v %W'),STR_TO_DATE(CONCAT(YEARWEEK(NOW(), 1),'Monday'), '%x%v %W')) >= -28" \
                        " AND Location IN (" + MFCList + ")" \
                        " GROUP BY STR_TO_DATE(CONCAT(YEARWEEK(Asat, 1),'Monday'), '%x%v %W'), Location" \
                        " ORDER BY Location, STR_TO_DATE(CONCAT(YEARWEEK(Asat, 1),'Monday'), '%x%v %W')"
            if "Forecast" in table:
                statement = "SELECT Date_Format(STR_TO_DATE(CONCAT(YEARWEEK(Asat, 1),'Monday'), '%x%v %W'), '%d-%b-%Y') AS WeekCommencing, Location, SUM(Forecast) AS Forecast"\
                        " FROM " + table +  \
                        " WHERE DATEDIFF(STR_TO_DATE(CONCAT(YEARWEEK(Asat, 1),'Monday'), '%x%v %W'),STR_TO_DATE(CONCAT(YEARWEEK(NOW(), 1),'Monday'), '%x%v %W')) >= 0" \
                        " AND Location IN (" + MFCList + ")" \
                        " GROUP BY STR_TO_DATE(CONCAT(YEARWEEK(Asat, 1),'Monday'), '%x%v %W'), Location" \
                        " ORDER BY Location, STR_TO_DATE(CONCAT(YEARWEEK(Asat, 1),'Monday'), '%x%v %W')"
            print(statement)
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
                        " sum(Forecast) as Forecast from AutoForecast where CreationDate = (select max(CreationDate) from AutoForecast) and" \
                        " Location in (" + MFCList + ") Group by Asat order by AutoForecast.AsAt"
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


def getWorkingForecast(groupAs, MFCList):
    retVal = {}
    MFCList = cleanseMFCList(MFCList)
    if MFCList["Result"] == 1:
        MFCList = MFCList["Data"]
        try_Conn = returnConnection()
        if try_Conn[0] == 1:
            my_Conn = try_Conn[1]
            cur = my_Conn.cursor(dictionary=True)
            # statement = "WITH window AS(SELECT *, ROW_NUMBER() OVER (PARTITION BY Location, Asat ORDER BY IO ASC) as RN from WorkingForecast) " \
            # "SELECT Date_format(Asat,'%d-%b-%Y') as Asat," \
            # " DATE_FORMAT(DATE_ADD(Asat, INTERVAL - WEEKDAY(Asat) DAY), '%d-%b-%Y') AS Commencing," \
            # " sum(Forecast) as Forecast from window where" \
            # " Location in (" + MFCList + ") AND RN = 1 Group by Asat order by window.Asat"
            statement = "SELECT Date_format(Asat,'%d-%b-%Y') as Asat," \
                        " DATE_FORMAT(DATE_ADD(Asat, INTERVAL - WEEKDAY(Asat) DAY), '%d-%b-%Y') AS Commencing," \
                        " sum(Forecast) as Forecast from WorkingForecast where" \
                        " Location in (" + MFCList + ") Group by Asat order by WorkingForecast.Asat"

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


def getFilteredActuals(groupAs, MFCList):
    retVal = {}
    MFCList = cleanseMFCList(MFCList)
    if MFCList["Result"] == 1:
        MFCList = MFCList["Data"]

        try_Conn = returnConnection()
        if try_Conn[0] == 1:
            my_Conn = try_Conn[1]
            cur = my_Conn.cursor(dictionary=True)
            statement = "WITH big AS (SELECT Asat, STR_TO_DATE(CONCAT(YEARWEEK(Asat, 1),'Monday'), '%x%v %W') AS WeekCommencing, Location, Act" \
                        " FROM Actuals a)" \
                        " SELECT b.Asat, sum(b.Act) as Act FROM Big b" \
                        " LEFT JOIN IgnoredWeeks i on i.Location = b.Location and i.WeekCommencing = b.WeekCommencing" \
                        " WHERE i.WeekCommencing is NULL AND b.Location IN (" + MFCList + ")" \
                        " GROUP BY Asat ORDER BY Asat"
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
                        " LEFT OUTER JOIN Actuals a ON d.db_date = a.Asat AND Location IN (" + MFCList + ")" \
                        " WHERE db_date >= (SELECT MIN(Asat) FROM Actuals WHERE Location IN (" + MFCList + "))" \
                        " AND db_date <= (SELECT MAX(Asat) FROM Actuals WHERE Location IN (" + MFCList + ")) " \
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
        return retVal


def getAllActuals():
    retVal = {}
    try_Conn = returnConnection()
    if try_Conn[0] == 1:
        my_Conn = try_Conn[1]
        cur = my_Conn.cursor(dictionary=True)
        statement = "CALL liveActuals()"
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


def updateWkg(MFCList, Updates):
    if 'MFC' in Updates[0]:
        retVal = updateWkgWeekly(Updates)
        return retVal
    else:
        retVal = {}
        records = []
        counter = 0
        MFCList = cleanseMFCList(MFCList)
        if MFCList["Result"] == 1:
            MFCList = list(MFCList["Data"].split(","))
            try_Conn = returnConnection()
            if try_Conn[0] == 1:
                my_Conn = try_Conn[1]
                cur = my_Conn.cursor(dictionary=True)
                insert_query = "INSERT into ForecastLoader (LoaderUUID, Asat, Location, Pcnt) values (%s, %s, %s, %s)"
                uid = str(uuid.uuid4())
                try:
                    for M in MFCList:
                        for U in Updates:
                            print(U)
                            Dte = U['Dte']
                            Pcnt = float(U['Pcnt'])
                            M = M.replace("'", "").strip()

                            recordEntry = (uid, Dte, M, Pcnt)
                            records.append(recordEntry)
                            counter = counter + 1
                            if counter == 999:
                                cur.executemany(insert_query, records)
                                counter = 0
                                records = []

                    if counter > 0:
                        cur.executemany(insert_query, records)

                    my_Conn.commit()
                    statement = "CALL forecastLoader('" + uid + "')"
                    cur.execute(statement)
                    my_Conn.commit()
                    my_Conn.close()
                except mariadb.Error as e:
                    retVal["Result"] = 0
                    retVal["Data"] = str(e)
                    return retVal

                retVal["Result"] = 1
                retVal["Data"] = 'Success'
                return retVal
            else:
                retVal["Result"] = 0
                retVal["Data"] = try_Conn[1]
                return retVal
        else:
            retVal["Result"] = 0
            retVal["Data"] = MFCList["Data"]
            return retVal


def updateWkgWeekly(Updates):
    retVal = {}
    records = []
    counter = 0
    try_Conn = returnConnection()
    if try_Conn[0] == 1:
        my_Conn = try_Conn[1]
        cur = my_Conn.cursor(dictionary=True)
        insert_query = "INSERT into ForecastLoader (LoaderUUID, Asat, Location, Pcnt) values (%s, %s, %s, %s)"
        uid = str(uuid.uuid4())
        try:
            for U in Updates:
                Dte = U['Dte']
                Pcnt = float(U['Pcnt'])
                M = U['MFC']
                recordEntry = (uid, Dte, M, Pcnt)
                records.append(recordEntry)
                counter = counter + 1
                if counter == 999:
                    cur.executemany(insert_query, records)
                    counter = 0
                    records = []

            if counter > 0:
                cur.executemany(insert_query, records)

            my_Conn.commit()
            statement = "CALL forecastLoader('" + uid + "')"
            cur.execute(statement)
            my_Conn.commit()
            my_Conn.close()
        except mariadb.Error as e:
            retVal["Result"] = 0
            retVal["Data"] = str(e)
            return retVal

        retVal["Result"] = 1
        retVal["Data"] = 'Success'
        return retVal
    else:
        retVal["Result"] = 0
        retVal["Data"] = try_Conn[1]
        return retVal



def getWeeksMatrix():
    retVal = {}
    try_Conn = returnConnection()
    if try_Conn[0] == 1:
        my_Conn = try_Conn[1]
        cur = my_Conn.cursor(dictionary=True)
        statement = "SELECT * FROM IgnoredWeeks ORDER BY WeekCommencing, Location"
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


def getIgnoredWeeksForMFCs(MFCList, expected):
    retVal = {}
    MFCList = cleanseMFCList(MFCList)
    if MFCList["Result"] == 1:
        MFCList = MFCList["Data"]
        try_Conn = returnConnection()
        if try_Conn[0] == 1:
            my_Conn = try_Conn[1]
            cur = my_Conn.cursor(dictionary=True)
            statement = "SELECT DATE_FORMAT(WeekCommencing,'%d-%b-%Y') AS WeekCommencing," \
                        " 100 * (COUNT(Location) / " + str(expected) + ") AS Included" \
                        " FROM IgnoredWeeks WHERE Location IN (" + MFCList + ") " \
                        " GROUP BY WeekCommencing ORDER BY WeekCommencing"
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


def ignoreWeeksOn(MFCList, WeekCommencing):
    retVal = {}
    records = []
    counter = 0
    total = 0
    MFCList = cleanseMFCList(MFCList)
    if MFCList["Result"] == 1:
        MFCList = list(MFCList["Data"].split(","))
        try_Conn = returnConnection()
        if try_Conn[0] == 1:
            my_Conn = try_Conn[1]
            cur = my_Conn.cursor()
            wc = WeekCommencing
            upd_query = "INSERT IGNORE INTO IgnoredWeeks (Location, WeekCommencing) VALUES (%s, %s)"
            try:
                for M in MFCList:
                    recordEntry = (M.replace("'", "").strip(), wc)
                    records.append(recordEntry)
                    counter = counter + 1
                    total = total + 1
                    if counter == 999:
                        cur.executemany(upd_query, records)
                        counter = 0
                        records = []

                if counter > 0:
                    cur.executemany(upd_query, records)

                my_Conn.commit()
                my_Conn.close()
                retVal["Result"] = 1
                retVal["Data"] = 'Success'
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


def ignoreWeeksOff(MFCList, WeekCommencing):
    retVal = {}
    MFCList = cleanseMFCList(MFCList)
    if MFCList["Result"] == 1:
        MFCList = MFCList["Data"]
        try_Conn = returnConnection()
        if try_Conn[0] == 1:
            my_Conn = try_Conn[1]
            cur = my_Conn.cursor()
            wc = WeekCommencing
            statement = "DELETE FROM IgnoredWeeks where WeekCommencing = '" + wc + "' AND Location IN (" + MFCList + ")"
            try:
                cur.execute(statement)
                my_Conn.commit()
                my_Conn.close()
                retVal["Result"] = 1
                retVal["Data"] = 'Success'
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


# Load Functions:
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
            retVal["Data"] = 'Success'
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
        insert_query = "INSERT IGNORE INTO Actuals (Asat, Location, Act) VALUES (%s, %s, %s)"
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
        insert_query = "INSERT IGNORE INTO AutoForecast (CreationDate, Asat, Location, Forecast) VALUES (%s, %s, %s, %s)"
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


def loadForecastWrapper(new_entries):
    deleteForecasts()
    loadForecast(new_entries)
    copyOfficial()


def deleteForecasts():
    try_Conn = returnConnection()
    if try_Conn[0] == 1:
        my_Conn = try_Conn[1]
        cur = my_Conn.cursor()
        cur.execute("Delete from OfficialForecast")
        my_Conn.commit()
        cur.execute("Delete from WorkingForecast")
        my_Conn.commit()
        cur.execute("Delete from IntraForecast")
        my_Conn.commit()
        my_Conn.close()


def copyOfficial():
    try_Conn = returnConnection()
    if try_Conn[0] == 1:
        my_Conn = try_Conn[1]
        cur = my_Conn.cursor()
        cur.execute("INSERT INTO WorkingForecast SELECT * FROM OfficialForecast")
        my_Conn.commit()
        my_Conn.close()


def loadForecast(new_entries):
    records = []
    counter = 0
    total = 0
    try_Conn = returnConnection()
    if try_Conn[0] == 1:
        my_Conn = try_Conn[1]
        cur = my_Conn.cursor()
        insert_query = "INSERT IGNORE INTO OfficialForecast (Asat, Location, Forecast) VALUES (%s, %s, %s)"
        for entry in new_entries:
            dte = entry[0]
            locid = entry[1]
            forecast = int(entry[2])
            recordEntry = (dte, locid, forecast)
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
