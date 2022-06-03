import mariadb
import json


def returnConnection():
    f = open('creds/data.json')
    data = json.load(f)

    try:
        conn = mariadb.connect(
            user=data["user"],
            password=data["password"],
            host=data["host"],
            port=data["port"],
            database=data["database"]
        )
    except mariadb.Error:
        conn = None

    return conn


def getData(params):
    j = params.get('j')
    t = params.get('t')
    my_Conn = returnConnection()
    cur = my_Conn.cursor(dictionary=True)
    statement = "SELECT * from sca where j = %s"
    if t.upper() == 'F':
        statement = "SELECT * from scf where asat = (select max(asat) from scf where j = %s)"
    jurisdiction = (j.upper(),)
    cur.execute(statement, jurisdiction)
    result = cur.fetchall()

    my_Conn.close()

    return json.dumps(result, default=str)


def latestActual(params):
    my_Conn = returnConnection()
    cur = my_Conn.cursor(dictionary=True)
    statement = "SELECT max(asat),j from sca group by j"
    cur.execute(statement)
    result = cur.fetchall()

    my_Conn.close()

    return json.dumps(result, default=str)


def pushData(params):
    my_Conn = returnConnection()
    cur = my_Conn.cursor()

    records = []
    counter = 0
    insert_query = "INSERT INTO sca (asat, loc, j, act) VALUES (%s, %s, %s, %s) "
    for entry in params.get('data'):
        dte = entry['dte']
        locid = entry['locid']
        j = entry['j']
        act = entry['act']
        recordEntry = (dte, locid, j, act)
        records.append(recordEntry)
        counter = counter + 1
        if counter == 999:
            cur.executemany(insert_query, records)
            counter = 0
            records = []

    if counter > 0:
        cur.executemany(insert_query, records)

    my_Conn.commit()
    my_Conn.close()
    # return json.dumps(result, default=str)


def loadActuals(newEntries):
    my_Conn = returnConnection()
    cur = my_Conn.cursor()

    records = []
    counter = 0
    total = 0
    insert_query = "INSERT INTO sca (asat, loc, j, act) VALUES (%s, %s, %s, %s) "
    for entry in newEntries:
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
    return total