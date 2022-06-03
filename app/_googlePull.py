from googleapiclient.discovery import build
from google.oauth2 import service_account
import datetime
import json
from _maria import loadActuals
from os.path import exists


def readFrom(sheetName):
    SERVICE_ACCOUNT_FILE = 'keys.json'
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']

    creds = None
    creds = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)

    # The ID and range of a sample spreadsheet.
    SAMPLE_SPREADSHEET_ID = '1JQH_br1-_wSTQec0hr6OKJb1AKZYdwLf2vKVjpXWjWs'

    service = build('sheets', 'v4', credentials=creds)

    # Call the Sheets API
    sheet = service.spreadsheets()
    result = sheet.values().get(spreadsheetId=SAMPLE_SPREADSHEET_ID, range=sheetName + "!A:C").execute()
    values = result.get('values', [])

    return values


def checkDaily(ctry):

    date_format = "%Y-%m-%d"
    final = "2000-01-01"
    newEntries = []
    data = {}
    if exists('latest.json'):
        f = open('latest.json')
        data = json.load(f)
        if ctry in data:
            final = data[ctry]

    lastEntry = datetime.datetime.strptime(final, date_format).date()
    today = datetime.date.today()
    delta = today - lastEntry
    if delta.days <= 1:
        print("No new records found for " + ctry)
        loaded = -1
    else:
        gData = readFrom(ctry)
        gData = gData[1:]
        for entry in gData:
            dte = datetime.datetime.strptime(entry[1], date_format).date()
            delta = dte - lastEntry
            if delta.days > 0:
                record = (entry[1], entry[0], ctry[0], int(entry[2].replace(',', '')))
                newEntries.append(record)
                final = entry[1]

        data[ctry] = final

        loaded = loadActuals(newEntries)
        print("Loaded " + str(loaded) + " records for " + ctry)
        print(data)

        with open('latest.json', 'w') as outfile:
            json.dump(data, outfile)
    return loaded


def gSyncActuals(countries):
    for ctry in countries:
        checkDaily(ctry)

