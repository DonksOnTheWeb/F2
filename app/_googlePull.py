from googleapiclient.discovery import build
from google.oauth2 import service_account
import datetime
import json

from _maria import loadActuals, latestActual
from _tsLog import log


def readFrom(sheetname):
    SERVICE_ACCOUNT_FILE = 'keys.json'
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']

    creds = None
    creds = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)

    # The ID and range of a sample spreadsheet.
    SAMPLE_SPREADSHEET_ID = '1JQH_br1-_wSTQec0hr6OKJb1AKZYdwLf2vKVjpXWjWs'

    service = build('sheets', 'v4', credentials=creds)

    # Call the Sheets API
    sheet = service.spreadsheets()
    result = sheet.values().get(spreadsheetId=SAMPLE_SPREADSHEET_ID, range=sheetname + "!A:C").execute()
    values = result.get('values', [])

    return values


def checkDaily(ctry):

    ctrySingle = ctry[0]
    date_format = "%Y-%m-%d"
    final = "2000-01-01"
    result = latestActual()
    result = json.loads(result)
    for entry in result:
        if entry["j"] == ctrySingle:
            final = entry["latest"]

    lastEntry = datetime.datetime.strptime(final, date_format).date()
    yesterday = datetime.date.today() - datetime.timedelta(days=1)
    delta = yesterday - lastEntry
    newEntries = []
    haveLoaded = False

    if delta.days == 0:
        finalStr = "Latest entries for " + ctry + " are yesterday.  Not performing a load."
    else:
        log("Last data found as at " + final + ".  Therefore missing " + str(delta.days) + " days of Data.")
        gData = readFrom(ctry)
        gData = gData[1:]
        unique_dates = {}

        for entry in gData:
            dte = datetime.datetime.strptime(entry[1], date_format).date()
            delta = dte - lastEntry
            if delta.days >= 0:
                if entry[1] not in unique_dates:
                    unique_mfcs = []
                else:
                    unique_mfcs = unique_dates[entry[1]]

                if entry[0] not in unique_dates:
                    unique_mfcs.append(entry[0])

                unique_dates[entry[1]] = unique_mfcs

                record = (entry[1], entry[0], ctry[0], int(entry[2].replace(',', '')))
                newEntries.append(record)
                haveLoaded = True

        log("Loaded data for :")
        log(json.dumps(unique_dates, indent=4))

    loaded = loadActuals(newEntries)
    if haveLoaded:
        finalStr = "Loaded " + str(loaded) + " records for " + ctry + ".  This includes re-loading last data date."

    log(finalStr)
    return loaded


def gSyncActuals(countries):
    for ctry in countries:
        checkDaily(ctry)
