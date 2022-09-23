from googleapiclient.discovery import build
from google.oauth2 import service_account
import datetime


def writeParams(MFC, Params, Ctry, Number):
    SERVICE_ACCOUNT_FILE = 'keys.json'
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

    creds = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)

    # The ID and range of a sample spreadsheet.
    spreadsheet_id = '1GmOojxN2v0vjJKT_g3SfSv6EgLJ4eMLoKo08LlKTXFk'
    service = build('sheets', 'v4', credentials=creds)

    # Call the Sheets API
    sheet = service.spreadsheets()

    # Clear existing if first run
    if Number == 1:
        clearRange = "Prophet Params!A:D"
        sheet.values().clear(spreadsheetId=spreadsheet_id, range=clearRange).execute()

    # Now write out vals
    outRange = "Prophet Params!A" + str(Number) + ":D" + str(Number)
    timestamp = datetime.datetime.now().strftime("%d-%b-%Y, %H:%M:%S")
    values = [
        [MFC, Ctry, Params, timestamp]
    ]
    data = {'values': values}
    sheet.values().update(spreadsheetId=spreadsheet_id, body=data, range=outRange,
                          valueInputOption='USER_ENTERED').execute()


def writeHourly(jsonData, tab):
    SERVICE_ACCOUNT_FILE = 'keys.json'
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

    creds = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)

    # The ID and range of a sample spreadsheet.
    spreadsheet_id = '1GmOojxN2v0vjJKT_g3SfSv6EgLJ4eMLoKo08LlKTXFk'
    service = build('sheets', 'v4', credentials=creds)

    # Call the Sheets API
    sheet = service.spreadsheets()
    clearRange = tab + "!A:D"
    headersRange = tab + "!A1:F1"
    tsRange = tab + "!G1"
    values = [
        ['Location', 'Day', 'Hour', 'Split', '', 'Last Updated:'],
    ]

    # First clear existing
    sheet.values().clear(spreadsheetId=spreadsheet_id, range=clearRange).execute()

    # Now the headers
    data = {'values': values}
    sheet.values().update(spreadsheetId=spreadsheet_id, body=data, range=headersRange,
                          valueInputOption='USER_ENTERED').execute()

    # Now the full data
    values = []
    outCount = 0
    for MFC in jsonData:
        for days in ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']:
            for hour in range(0, 24):
                if str(hour) in jsonData[MFC][days]:
                    values.append([MFC, days, hour, jsonData[MFC][days][str(hour)]])
                else:
                    values.append([MFC, days, hour, 0])
                outCount = outCount + 1

    dataRange = tab + "!A2:D" + str(outCount + 1)

    tryGrowSheet(sheet, tab, spreadsheet_id, outCount + 1)

    data = {'values': values}
    sheet.values().update(spreadsheetId=spreadsheet_id, body=data, range=dataRange,
                          valueInputOption='USER_ENTERED').execute()

    # Then the timestamp
    timestamp = datetime.datetime.now().strftime("%d-%b-%Y, %H:%M:%S")
    values = [
        [timestamp]
    ]
    data = {'values': values}
    sheet.values().update(spreadsheetId=spreadsheet_id, body=data, range=tsRange,
                          valueInputOption='USER_ENTERED').execute()


def writeForecast(MFC, Forecast, tab, outRow, ignoreHour):
    SERVICE_ACCOUNT_FILE = 'keys.json'
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

    creds = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)

    # The ID and range of a sample spreadsheet.
    spreadsheet_id = '1GmOojxN2v0vjJKT_g3SfSv6EgLJ4eMLoKo08LlKTXFk'
    service = build('sheets', 'v4', credentials=creds)

    # Call the Sheets API
    sheet = service.spreadsheets()

    # Clear existing if first run
    if outRow == 2:
        if ignoreHour:
            clearRange = tab + "!A:C"
            sheet.values().clear(spreadsheetId=spreadsheet_id, range=clearRange).execute()
            # Now the headers
            headersRange = tab + "!A1:C1"
            values = [
                ['MFC', 'Date', 'Forecast'],
            ]
            data = {'values': values}
            sheet.values().update(spreadsheetId=spreadsheet_id, body=data, range=headersRange,
                                  valueInputOption='USER_ENTERED').execute()
        else:
            clearRange = tab + "!A:D"
            sheet.values().clear(spreadsheetId=spreadsheet_id, range=clearRange).execute()
            # Now the headers
            headersRange = tab + "!A1:D1"
            values = [
                ['MFC', 'Date', 'Hour', 'Forecast'],
            ]
            data = {'values': values}
            sheet.values().update(spreadsheetId=spreadsheet_id, body=data, range=headersRange,
                                  valueInputOption='USER_ENTERED').execute()

    # Now write out vals
    values = []
    if ignoreHour:
        for entry in Forecast:
            values.append([MFC, entry['Date'], entry['Forecast']])
        data = {'values': values}
        outRange = tab + "!A" + str(outRow) + ":C" + str(outRow + len(values))

        tryGrowSheet(sheet, tab, spreadsheet_id, outRow + len(values))

        sheet.values().update(spreadsheetId=spreadsheet_id, body=data, range=outRange,
                              valueInputOption='USER_ENTERED').execute()
    else:
        for entry in Forecast:
            values.append([MFC, entry['Date'], entry['Hour'], entry['Forecast']])
        data = {'values': values}
        outRange = tab + "!A" + str(outRow) + ":D" + str(outRow + len(values))

        tryGrowSheet(sheet, tab, spreadsheet_id, outRow + len(values))

        sheet.values().update(spreadsheetId=spreadsheet_id, body=data, range=outRange,
                              valueInputOption='USER_ENTERED').execute()


def tryGrowSheet(sheet, sheetName, spreadsheet_id, endR):
    res = sheet.values().get(spreadsheetId=spreadsheet_id, range=sheetName, ).execute()
    r = res['range'].split(':')[1]
    maxR = ''
    for c in r:
        if c.isdigit():
            maxR = maxR + c

    maxR = int(maxR)

    if endR > maxR:
        toAdd = (endR - maxR) + 1
        request = {
            'requests': [
                {
                    'appendDimension': {
                        'sheetId': sheetName,
                        'dimension': 'ROWS',
                        'length': int(toAdd)
                    }
                }
            ]
        }
        #print('Growing sheet')
        #sheet.batchUpdate(spreadsheetId=spreadsheet_id, body=request).execute()
