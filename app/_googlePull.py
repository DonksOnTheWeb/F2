from googleapiclient.discovery import build
from google.oauth2 import service_account
import datetime
import json
from _loghandler import logger


def buildDictionaries():
    howFarBack = 4

    # We first create a dict of regions and MFCs and opening times
    liveMFCs = readFromGeneric('Live', '1GmOojxN2v0vjJKT_g3SfSv6EgLJ4eMLoKo08LlKTXFk', '!A:J')
    GeoTags = {}
    OpeningCol = {}
    Opening = {}
    line = 0
    dayNames = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']
    for entry in liveMFCs:
        MFC = entry[0]
        GeoTag = entry[1]
        if line == 0:
            itr = 0
            for col in entry:
                if col in dayNames:
                    OpeningCol[col] = itr
                itr = itr + 1
            line = line + 1
        else:
            GeoTags[MFC] = GeoTag
            openingHours = {}
            for dn in dayNames:
                openingHours[dn] = entry[OpeningCol[dn]]
            Opening[MFC] = openingHours

    # We then read all the Actual Data as a list of [Dte, MFC, Orders]
    actualOrders = readFromGeneric('Orders_Daily_FR_UK_ES.csv', '1Uykg22mWkNwj2v-jgYJN0WbtDAKQx9ZkF4aoRXxb6VU', '!B:D')

    # Iterating this data, we create a dictionary of dates and orders for each MFC
    Orders = {}
    actualOrders = actualOrders[1:]
    for entry in actualOrders:
        dte = entry[0]
        MFC = entry[1]
        actual = entry[2].replace(',', '').replace('.', '')
        if len(MFC) > 0:
            if MFC in GeoTags:
                if MFC in Orders:
                    Orders[MFC][dte] = actual
                else:
                    Orders[MFC] = {dte: actual}

    # Finally read the hourly data as a list of [Date, MFC, Hour, Orders]
    hourlyOrders = readFromGeneric('Hourly Dump (FR + UK).csv', '1TjJpezxYxcet7VTWWFm-irtugOIfe7mjymUG7XId7rM', '!A:D')

    # Iterating this data to create an average hourly curve for each MFC for each Day
    hourly = {}
    hourly_G = {}
    hourlyOrders = hourlyOrders[1:]
    for entry in hourlyOrders:
        dte = entry[0]
        dte = datetime.datetime.strptime(dte, '%Y-%m-%d')
        now = datetime.datetime.now()
        proceed = False
        if (now.date() - dte.date()).days <= howFarBack * 7:
            proceed = True
        dte = dte.strftime('%a')  # In format Mon, Tue, Wed etc...
        MFC = entry[1]
        GeoTag = GeoTags[MFC]
        hr = entry[2]
        actual = entry[3].replace(',', '').replace('.', '')
        # populate the hourly json
        if proceed:
            if MFC in hourly:
                if dte in hourly[MFC]:
                    hourly[MFC][dte]['Tot'] = hourly[MFC][dte]['Tot'] + int(actual)
                    if hr in hourly[MFC][dte]:
                        hourly[MFC][dte][hr] = hourly[MFC][dte][hr] + int(actual)
                        hourly[MFC][dte][str(hr) + 'Count'] = hourly[MFC][dte][hr] + 1
                    else:
                        hourly[MFC][dte][hr] = int(actual)
                        hourly[MFC][dte][str(hr) + 'Count'] = 1
                else:
                    hourly[MFC][dte] = {hr: int(actual), 'Tot': int(actual), str(hr) + 'Count': 1}
            else:
                hourly[MFC] = {dte: {hr: int(actual), 'Tot': int(actual), str(hr) + 'Count': 1}}

            if GeoTag in hourly_G:
                if dte in hourly_G[GeoTag]:
                    hourly_G[GeoTag][dte]['Tot'] = hourly_G[GeoTag][dte]['Tot'] + int(actual)
                    if hr in hourly_G[GeoTag][dte]:
                        hourly_G[GeoTag][dte][hr] = hourly_G[GeoTag][dte][hr] + int(actual)
                    else:
                        hourly_G[GeoTag][dte][hr] = int(actual)
                else:
                    hourly_G[GeoTag][dte] = {hr: int(actual), 'Tot': int(actual)}
            else:
                hourly_G[GeoTag] = {dte: {hr: int(actual), 'Tot': int(actual)}}

    # Now normalise the curves
    normHourly_G = {}
    for GeoTag in hourly_G:
        for dte in hourly_G[GeoTag]:
            for entry in hourly_G[GeoTag][dte]:
                if 'Tot' not in entry:
                    normsVal = hourly_G[GeoTag][dte][entry] / hourly_G[GeoTag][dte]['Tot']
                    if GeoTag in normHourly_G:
                        if dte in normHourly_G[GeoTag]:
                            normHourly_G[GeoTag][dte][entry] = normsVal
                        else:
                            normHourly_G[GeoTag][dte] = {entry: normsVal}
                    else:
                        normHourly_G[GeoTag] = {dte: {entry: normsVal}}

    normHourly = {}
    for MFC in hourly:
        for dte in hourly[MFC]:
            for entry in hourly[MFC][dte]:
                if 'Count' not in entry and 'Tot' not in entry:
                    normsVal = hourly[MFC][dte][entry] / hourly[MFC][dte]['Tot']
                    if MFC in normHourly:
                        if dte in normHourly[MFC]:
                            normHourly[MFC][dte][entry] = normsVal
                        else:
                            normHourly[MFC][dte] = {entry: normsVal}
                    else:
                        normHourly[MFC] = {dte: {entry: normsVal}}

    # Now finally replace any entries without howFarBack entries with their geotag equiv average.
    for MFC in hourly:
        for dte in hourly[MFC]:
            for entry in hourly[MFC][dte]:
                if 'Count' in entry:
                    if hourly[MFC][dte][entry] < howFarBack:
                        # pull from GeoTag
                        hr = entry.replace('Count', '')
                        GeoTag = GeoTags[MFC]
                        normHourly[MFC][dte][hr] = normHourly_G[GeoTag][dte][hr]
                        print("Replaced " + MFC + " " + dte + " " + hr + " with " + GeoTag)

    # REMEMBER opening hours


def readFromGeneric(sheet_name, sheet_id, cols):
    # sheet_name is the tab name
    # sheet_id is the google hash for the sheet
    # cols is in format !B:D

    SERVICE_ACCOUNT_FILE = 'keys.json'
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']

    creds = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
    service = build('sheets', 'v4', credentials=creds)

    # Call the Sheets API
    sheet = service.spreadsheets()
    result = sheet.values().get(spreadsheetId=sheet_id, range=sheet_name + cols).execute()
    values = result.get('values', [])

    return values


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

        # Now the headers
        data = {'values': values}
        sheet.values().update(spreadsheetId=spreadsheet_id, body=data, range=headersRange,
                              valueInputOption='USER_ENTERED').execute()

        # Now the full data
        values = []
        for entry in jsonData:
            values.append([entry['Asat'], entry['Location'], entry['Forecast']])

        data = {'values': values}
        sheet.values().update(spreadsheetId=spreadsheet_id, body=data, range=dataRange,
                              valueInputOption='USER_ENTERED').execute()
        # Then the timestamp
        timestamp = datetime.datetime.now().strftime("%d-%b-%Y, %H:%M:%S")
        values = [
            [timestamp, len(jsonData)]
        ]
        data = {'values': values}
        sheet.values().update(spreadsheetId=spreadsheet_id, body=data, range=tsRange,
                              valueInputOption='USER_ENTERED').execute()
        retVal["Result"] = 1
        retVal["Data"] = "Success"
    except:
        retVal["Result"] = 0
        retVal["Data"] = "Error in WriteTo gsheets function"
    return retVal


def readFrom(sheetname):
    SERVICE_ACCOUNT_FILE = 'keys.json'
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']

    creds = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)

    # The ID and range of a sample spreadsheet.
    spreadsheet_id = '1Uykg22mWkNwj2v-jgYJN0WbtDAKQx9ZkF4aoRXxb6VU'

    service = build('sheets', 'v4', credentials=creds)

    # Call the Sheets API
    cols = "!B:D"

    sheet = service.spreadsheets()
    result = sheet.values().get(spreadsheetId=spreadsheet_id, range=sheetname + cols).execute()
    values = result.get('values', [])

    return values


def readFromOld(sheetname, cols=None):
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


def checkDaily(lastEntry):
    retVal = {}
    date_format = "%Y-%m-%d"
    new_entries = []
    gData = readFrom('Orders_Daily_FR_UK_ES.csv')
    gData = gData[1:]
    haveLoaded = False
    finalStr = "No new entries found"
    latestFileDate = datetime.datetime.strptime("2000-01-01", date_format).date()
    for entry in gData:
        if len(entry[0]) > 1:
            dte = datetime.datetime.strptime(entry[0], date_format).date()
            delta = dte - latestFileDate
            if delta.days >= 0:
                latestFileDate = dte

            delta = dte - lastEntry
            if delta.days >= -5:
                MFC = entry[1]
                record = (entry[0], MFC, int(entry[2].replace(',', '')))
                new_entries.append(record)
                haveLoaded = True

    loaded = loadActuals(new_entries)
    if haveLoaded:
        finalStr = "Loaded " + str(loaded["Data"]) + " records.  Includes attempted re-load of last 5 days"

    logger('I', finalStr)
    logger('I', "Latest file date found for is " + datetime.datetime.strftime(latestFileDate, date_format))
    retVal["Result"] = 1
    retVal["Data"] = loaded
    return retVal


def checkDailyOLD(ctry, lastEntry):
    retVal = {}
    date_format = "%Y-%m-%d"
    new_entries = []
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

        localReturn = checkDaily(lastEntry)
        if localReturn["Result"] == 0:
            logger('W', '')
            logger('W', str(localReturn["Data"]))
            logger('W', '')
    else:
        logger('W', '')
        logger('W', str(result["Data"]))
        logger('W', '')


def gSyncActualsOld(countries):
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
    gData = readFromOld("Forecast", "!A:E")
    gData = gData[1:]
    new_entries = []
    date_format = "%Y-%m-%d"
    for entry in gData:
        MFC = entry[0]
        try:
            dte = datetime.datetime.strptime(entry[1], date_format).date()
        except:
            dte = datetime.datetime.strptime(entry[1], "%m/%d/%Y").date()
        fcst = int(float(entry[4].replace(',', '')))
        record = (dte, MFC, fcst)
        new_entries.append(record)

    loadForecastWrapper(new_entries)
    logger('I', "Loaded forecast data - " + str(len(new_entries)) + " entries.")
