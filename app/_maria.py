import mariadb
import json


def returnConnection():
    try:
        conn = mariadb.connect(
            user="root",
            password="!L3ngthBe4tYouCannot!!",
            host="192.168.1.4",
            port=3306,
            database="scdb"
        )
    except mariadb.Error:
        conn = None

    return conn


def getData(params):
    j = params.get('j')
    t = params.get('t')
    my_Conn = returnConnection()
    cur = my_Conn.cursor()
    statement = "SELECT * from sca where j = %s)"
    if t.upper() == 'F':
        statement = "SELECT * from scf where asat = (select max(asat) from scf where j = %s)"
    jurisdiction = (j.upper(),)
    cur.execute(statement, jurisdiction)
    result = cur.fetchall()

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

    my_Conn.commit()
    print("Done")
    #return json.dumps(result, default=str)
