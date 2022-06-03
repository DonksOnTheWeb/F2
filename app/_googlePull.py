from googleapiclient.discovery import build
from google.oauth2 import service_account
import datetime
import json
from _maria import loadActuals


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
    f = open('working/latest.json')
    data = json.load(f)
    date_format = "%Y-%m-%d"
    lastEntry = datetime.datetime.strptime("2000-01-01", date_format)
    newEntries = []
    if ctry in data:
        ctryData = data[ctry]
        lastEntry = ctryData["LastEntry"]

    gData = readFrom(ctry)
    for entry in gData:
        dte = datetime.datetime.strptime(entry[1], date_format)
        delta = dte - lastEntry
        if delta.days > 0:
            record = (entry[0], entry[1], entry[2], entry[3])
            newEntries.append(record)

    loaded = loadActuals(newEntries)
    print("Loaded " + loaded + " records for " + ctry)
    return loaded


def gSyncActuals(countries):
    for ctry in countries:
        checkDaily(ctry)

