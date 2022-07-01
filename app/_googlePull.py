from googleapiclient.discovery import build
from google.oauth2 import service_account
import datetime
import json
from hashlib import sha256

from _maria import loadActuals, latestActualDate, loadForecastWrapper, getFullForecast
from loghandler import logger


def writeForecastToSheet(ctry, InOff):
    result = getFullForecast(ctry, InOff)
    result = json.loads(result["Data"])
    result = writeTo(ctry, result, InOff)
    return result


def writeTo(ctry, jsonData, inOff):
    retVal = {}
    try:
        SERVICE_ACCOUNT_FILE = 'keys.json'
        SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

        creds = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)

        # The ID and range of a sample spreadsheet.
        spreadsheet_id = '1vDUg9kZD__YDcjkkeeycxeykkD5Oz0nLIRpOc4i2ww8'
        service = build('sheets', 'v4', credentials=creds)

        # Call the Sheets API
        sheet = service.spreadsheets()
        if inOff == 'O':
            clearRange = ctry + "!A:C"
            headersRange = ctry + "!A1:C2"
            dataRange = ctry + '!A3:C' + str(len(jsonData) + 2)
            tsRange = ctry + "!J2:K2"
            values = [
                ['OFFICIAL', '', ''],
                ['Date', 'Location', 'Forecast'],
            ]
        else:
            clearRange = ctry + "!E:G"
            headersRange = ctry + "!E1:G2"
            dataRange = ctry + '!E3:G' + str(len(jsonData) + 2)
            tsRange = ctry + "!J3:K3"
            values = [
                ['INTRA', '', ''],
                ['Date', 'Location', 'Forecast'],
            ]

        # First clear existing
        sheet.values().clear(spreadsheetId=spreadsheet_id, range=clearRange).execute()

        #Now the headers
        data = {'values': values}
        sheet.values().update(spreadsheetId=spreadsheet_id, body=data, range=headersRange, valueInputOption='USER_ENTERED').execute()

        # Now the full data
        values = []
        for entry in jsonData:
            values.append([entry['Asat'], entry['Location'], entry['Forecast']])

        data = {'values': values}
        sheet.values().update(spreadsheetId=spreadsheet_id, body=data, range=dataRange, valueInputOption='USER_ENTERED').execute()
        # Then the timestamp
        timestamp = datetime.datetime.now().strftime("%d-%b-%Y, %H:%M:%S")
        values = [
            [timestamp, len(jsonData)]
        ]
        data = {'values': values}
        sheet.values().update(spreadsheetId=spreadsheet_id, body=data, range=tsRange, valueInputOption='USER_ENTERED').execute()
        retVal["Result"] = 1
        retVal["Data"] = "Success"
    except:
        retVal["Result"] = 0
        retVal["Data"] = "Error in WriteTo gsheets function"
    return retVal


def readFrom(sheetname, cols=None):
    SERVICE_ACCOUNT_FILE = 'keys.json'
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']

    creds = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)

    # The ID and range of a sample spreadsheet.
    spreadsheet_id = '1JQH_br1-_wSTQec0hr6OKJb1AKZYdwLf2vKVjpXWjWs'

    service = build('sheets', 'v4', credentials=creds)

    # Call the Sheets API

    if cols is None:
        cols = "!A:C"

    sheet = service.spreadsheets()
    result = sheet.values().get(spreadsheetId=spreadsheet_id, range=sheetname + cols).execute()
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

    logger('I', finalStr)
    logger('I', "Latest file date found for " + ctry + " is " + datetime.datetime.strftime(latestFileDate, date_format))
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

        logger('I', "Last DB entry is " + final)
        lastEntry = datetime.datetime.strptime(final, date_format).date()

        for ctry in countries:
            localReturn = checkDaily(ctry, lastEntry)
            if localReturn["Result"] == 0:
                logger('W', '')
                logger('W', str(localReturn["Data"]))
                logger('W', '')
                break
    else:
        logger('W', '')
        logger('W', str(result["Data"]))
        logger('W', '')


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
    gData = readFrom("Forecast", "!A:E")
    gData = gData[1:]
    new_entries = []
    date_format = "%Y-%m-%d"
    for entry in gData:
        MFC = entry[0]
        dte = datetime.datetime.strptime(entry[1], date_format).date()
        fcst = int(float(entry[4].replace(',', '')))
        record = (dte, MFC, fcst)
        new_entries.append(record)

    loadForecastWrapper(new_entries)
    logger('I', "Loaded forecast data - " + str(len(new_entries)) + " entries.")

