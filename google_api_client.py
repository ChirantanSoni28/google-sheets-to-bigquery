from google_auth_oauthlib.flow import InstalledAppFlow
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
import json


class GoogleAuthentication:

    def __init__(self, api_name, api_version, scope, credentials_directory, client_secret_json):
        self.api_name = api_name
        self.api_version = api_version
        self.scope = scope
        self.client_secret_json = client_secret_json
        self.credentials_directory = credentials_directory

    def google_authorization_access(self):
        with open(self.credentials_directory + "/" + self.client_secret_json) as secrets_file:
            client_secrets = json.loads(secrets_file.read())

        client_config = client_secrets

        flow = InstalledAppFlow.from_client_config(client_config, self.scope)
        credentials = flow.run_console()

        refresh_token = credentials.refresh_token

        with open(self.credentials_directory + "refresh_token.txt", "w") as token_file:
            token_file.write(refresh_token)

        service = build(self.api_name, self.api_version, credentials=credentials)

        return service

    def refresh_token_service_object(self, refresh_token_file):
        with open(self.credentials_directory + "/" + refresh_token_file) as token_file, open(
                self.credentials_directory + "/" + self.client_secret_json) as secrets_file:
            refresh_token = token_file.read()
            client_secrets = json.loads(secrets_file.read())

        credentials = Credentials(
            None,
            refresh_token=refresh_token,
            token_uri="https://accounts.google.com/o/oauth2/token",
            client_id=client_secrets["installed"]["client_id"],
            client_secret=client_secrets["installed"]["client_secret"]
        )

        service_object = build(self.api_name, self.api_version, credentials=credentials)

        return service_object


# scope = ['https://www.googleapis.com/auth/drive', 'https://www.googleapis.com/auth/spreadsheets']
#
# x = GoogleAuthentication("sheets", "v4", scope, "/Chirantan/Projects/google-sheets-to-bigquery",
#                          "credentials.json").google_authorization_access()

