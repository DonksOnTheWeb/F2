from googleapiclient.discovery import build
from google.oauth2 import service_account
import datetime
import json
from _maria import loadActuals
from os.path import exists


def readFrom(sheetName):
    SERVICE_ACCOUNT_FILE = 'creds/keys.json'
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
    lastEntry = "2000-01-01"
    newEntries = []

    if exists('../working/latest.json'):
        f = open('../working/latest.json')
        data = json.load(f)
        if ctry in data:
            lastEntry = data[ctry]

    lastEntry = datetime.datetime.strptime(lastEntry, date_format).date()
    today = datetime.date.today()
    delta = today - lastEntry
    if delta.days <= 1:
        print("No new records found for " + ctry)
        loaded = -1
    else:
        gData = readFrom(ctry)
        for entry in gData:
            dte = datetime.datetime.strptime(entry[1], date_format).date()
            delta = dte - lastEntry
            if delta.days > 0:
                record = (entry[0], entry[1], entry[2], entry[3])
                newEntries.append(record)

            data[ctry] = lastEntry

        loaded = loadActuals(newEntries)
        print("Loaded " + loaded + " records for " + ctry)

        with open('../working/latest.json', 'w') as outfile:
            json.dump(data, outfile)
    return loaded


def gSyncActuals(countries):
    for ctry in countries:
        checkDaily(ctry)

