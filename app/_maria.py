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


def getA(params):

