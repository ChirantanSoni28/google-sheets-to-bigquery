from bigQuery.bigquery_load import bigQuery
import datetime
from google.oauth2 import service_account
from google.cloud import bigquery


class autoLoad:

    def __init__(self,projectId):

        self.projectId = projectId
        self.bqClient = bigQuery(self.projectId).bqClient
        self.datasets = bigQuery(self.projectId).datasets
        self.dataset_ref = bigQuery(self.projectId).dataset_ref
        self.tables_schema = bigQuery(self.projectId).getSchema()

    def dataLoader(self):


        for table, schema in self.tables_schema.items():

            if not schema:
                bigQuery(self.projectId).loadData(table,schema)

            else:
                bigQuery(self.projectId).deleteData(table)
                bigQuery(self.projectId).loadData(table, schema)






autoLoad("psd-data-warehouse").dataLoader()