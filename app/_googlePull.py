from googleapiclient.discovery import build
from google.oauth2 import service_account
import datetime
import json
from hashlib import sha256

from _maria import loadActuals, latestActualDate, loadForecast
from _tsLog import log


def readFrom(sheetname, cols=None):
    SERVICE_ACCOUNT_FILE = 'keys.json'
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']

    creds = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)

    # The ID and range of a sample spreadsheet.
    SAMPLE_SPREADSHEET_ID = '1JQH_br1-_wSTQec0hr6OKJb1AKZYdwLf2vKVjpXWjWs'

    service = build('sheets', 'v4', credentials=creds)

    # Call the Sheets API

    if cols is None:
        cols = "!A:C"

    sheet = service.spreadsheets()
    result = sheet.values().get(spreadsheetId=SAMPLE_SPREADSHEET_ID, range=sheetname + cols).execute()
    values = result.get('values', [])

    return values


def checkDaily(ctry, lastEntry):
    retVal = {}
    date_format = "%Y-%m-%d"
    new_entries = []
    #Do a try on the readFrom line
    gData = readFrom(ctry)
    gData = gData[1:]
    haveLoaded = False
    finalStr = "No new entries for " + ctry
    latestFileDate = datetime.datetime.strptime("2000-01-01", date_format).date()
    for entry in gData:
        dte = datetime.datetime.strptime(entry[1], date_format).date()
        delta = dte - latestFileDate
        if delta.days >= 0:
            latestFileDate = dte

        delta = dte - lastEntry
        if delta.days >= -5:
            MFC = entry[0]
            record = (entry[1], MFC, int(entry[2].replace(',', '')))
            new_entries.append(record)
            haveLoaded = True

    loaded = loadActuals(new_entries)
    if haveLoaded:
        finalStr = "Loaded " + str(loaded["Data"]) + " records.  Includes attempted re-load of last 5 days"

    log(finalStr)
    log("Latest file date found for " + ctry + " is " + datetime.datetime.strftime(latestFileDate, date_format))
    retVal["Result"] = 1
    retVal["Data"] = loaded
    return retVal


def gSyncActuals(countries):
    date_format = "%Y-%m-%d"
    result = latestActualDate()
    final = "2000-01-01"
    if result["Result"] == 1:
        data = json.loads(result["Data"])[0]
        if data["latest"] is not None:
            final = data["latest"]

        log("Last DB entry is " + final)
        lastEntry = datetime.datetime.strptime(final, date_format).date()

        for ctry in countries:
            localReturn = checkDaily(ctry, lastEntry)
            if localReturn["Result"] == 0:
                log('')
                log(str(localReturn["Data"]))
                log('')
                break
    else:
        log('')
        log(str(result["Data"]))
        log('')


def buildMFCLookup():
    MFCLookup = {}
    MFCLookup = MFCLookupWrapper("UK", MFCLookup)
    MFCLookup = MFCLookupWrapper("FR", MFCLookup)
    MFCLookup = MFCLookupWrapper("ES", MFCLookup)
    return MFCLookup


def MFCLookupWrapper(ctry, MFCLookup):
    gData = readFrom(ctry)
    gData = gData[1:]
    for entry in gData:
        MFC = entry[0]
        if MFC not in MFCLookup:
            MFCLookup[MFC] = ctry
    return MFCLookup


def loadForecastOneOff():
    #MFCLookup = buildMFCLookup()
    gData = readFrom("Forecast", "!A:E")
    gData = gData[1:]
    new_entries = []
    date_format = "%Y-%m-%d"
    for entry in gData:
        MFC = entry[0]
        #J = MFCLookup[MFC]
        dte = datetime.datetime.strptime(entry[1], date_format).date()
        fcst = int(float(entry[4].replace(',', '')))
        record = (dte, MFC, fcst)
        new_entries.append(record)

    loadForecast(new_entries)
    print(len(new_entries))
    log("Loaded forecast data")
