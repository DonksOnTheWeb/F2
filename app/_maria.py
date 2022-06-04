import mariadb
import json


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


def getLatestActuals(from_date):
    retVal = {}
    try_Conn = returnConnection()
    if try_Conn[0] == 1:
        my_Conn = try_Conn[1]
        cur = my_Conn.cursor(dictionary=True)
        statement = "SELECT * from sca"
        try:
            if from_date is not None:
                statement = statement + " where Asat >= %s"
                AsatDate = (from_date,)
                cur.execute(statement, AsatDate)
            else:
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
