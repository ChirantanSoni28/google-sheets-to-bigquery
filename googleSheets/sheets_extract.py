import pandas as pd
import sys
from google_api_client import GoogleAuthentication


class sheetsClient:

    def __init__(self,spreadsheetId):

        self.spreadsheet = spreadsheetId
        self.dataframes = {}
        self.sheet = range
        self.scope = ['https://www.googleapis.com/auth/drive', 'https://www.googleapis.com/auth/spreadsheets']
        self.client = GoogleAuthentication("sheets", "v4", self.scope, "/Chirantan/Projects/google-sheets-to-bigquery",
                                           "credentials.json").refresh_token_service_object(
            "google-sheets-to-bigqueryrefresh_token.txt")

    def SpreadSheets(self):

        spreadsheet_meta_data = self.client.spreadsheets()
        spreadsheet_details = []
        meta_data = spreadsheet_meta_data.get(spreadsheetId=self.spreadsheet).execute()

        spreadsheet_title = meta_data['properties']['title']

        sheets = meta_data['sheets']

        for sheet in sheets:
            for k, v in sheet.items():
                if k == 'properties' and sheet['properties']['title'] != 'SupermetricsQueries':
                    spreadsheet_details.append((sheet['properties']['title'], sheet['properties']['sheetId']))

        return spreadsheet_details

    def Sheets(self):

        meta_data = sheetsClient(self.spreadsheet).SpreadSheets()

        spreadsheet_data = self.client.spreadsheets()

        for sheet_name, sheet_id in meta_data:
            sheet = spreadsheet_data.values().get(spreadsheetId=self.spreadsheet, range=sheet_name).execute()
            data = sheet.get('values', [])
            dataframe = pd.DataFrame(data[1:])


            self.dataframes[sheet_name] = dataframe

        return self.dataframes


# y = sheetsClient("1XpnBmbzZ-8mfZHVyCwySwfdNmXHqMwRKAaqbD7nu-lE").Sheets()
# print(y['Facebook Ads'].dtypes)