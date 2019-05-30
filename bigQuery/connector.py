from google.oauth2 import service_account
from google.cloud import bigquery
from googleSheets.sheets_extract import sheetsConnector


class connector:

    def __init__(self, projectId):


        self.projectId = projectId
        self.service_account_json = "/Chirantan/Projects/google-sheets-to-bigquery/psd-data-warehouse-649a8f4a3f43.json"
        self.credentials = service_account.Credentials.from_service_account_file(self.service_account_json)
        self.bqClient = bigquery.client.Client(credentials=self.credentials, project=self.projectId)
        self.table_id = ["g_sessions"]

    def connect(self):

        datasets = list(self.bqClient.list_datasets())

        print(datasets[0].dataset_id)

        dataset_ref = self.bqClient.dataset(datasets[0].dataset_id)

        # table_id = self.projectId + "." + datasets[0] + "." + self.table_id[0]

        # table = bigquery.Table(table_id)

        # table = self.bqClient.create_table(table)

        table_ref = dataset_ref.table(self.table_id[0])

        dataframe = sheetsConnector("1XpnBmbzZ-8mfZHVyCwySwfdNmXHqMwRKAaqbD7nu-lE", "GoogleAnalytics_Sessions").extract()

        data = self.bqClient.load_table_from_dataframe(dataframe, table_ref,location="US" )

        data.result()

        assert data.state == "Done"
        table = self.bqClient.get_table(table_ref)
        assert table.num_rows < 0


        # for dataset in datasets:
        #     print(dataset.dataset_id)

        # project = self.bqClient.project
        #
        # if datasets:
        #     print("Datasets in the Project{}".format(project))
        #     for dataset in datasets:
        #
        #         print("\{}").format(dataset.dataset_id)





x = connector("psd-data-warehouse").connect()

