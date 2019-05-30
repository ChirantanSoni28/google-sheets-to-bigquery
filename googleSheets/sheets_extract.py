from google_api_client import GoogleAuthentication
import pandas as pd

class sheetsConnector:

    def __init__(self, spreadsheetId, range):
        self.spreadsheet = spreadsheetId
        self.sheet = range


    def extract(self):
        scope = ['https://www.googleapis.com/auth/drive', 'https://www.googleapis.com/auth/spreadsheets']
        client = GoogleAuthentication("sheets", "v4", scope, "/Chirantan/Projects/google-sheets-to-bigquery","credentials.json").refresh_token_service_object("google-sheets-to-bigqueryrefresh_token.txt")

        spreadsheet = client.spreadsheets()

        sheet = spreadsheet.values().get(spreadsheetId=self.spreadsheet, range=self.sheet).execute()

        data = sheet.get('values', [])

        if not data:
            print("There is no data on the given, Spreadsheet and Range")
        else:

            dataframe = pd.DataFrame(data[1:],columns=data[0])

            return dataframe


y = sheetsConnector("1XpnBmbzZ-8mfZHVyCwySwfdNmXHqMwRKAaqbD7nu-lE", "GoogleAnalytics_Sessions").extract()
