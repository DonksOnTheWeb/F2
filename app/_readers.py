from googleapiclient.discovery import build
from google.oauth2 import service_account


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


