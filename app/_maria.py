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
    print(j)
    my_Conn = returnConnection()
    cur = my_Conn.cursor()
    statement = "SELECT * from scdata where asat = (select max(asat) from scdata where j = %s)"
    jurisdiction = (j,)
    cur.execute(statement, jurisdiction)
    result = cur.fetchall()

    print(json.dumps(result, default=str))

    return json.dumps(result, default=str)
