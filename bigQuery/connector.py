from google.oauth2 import service_account
from google.cloud import bigquery
from googleSheets.sheets_extract import sheetsClient



class bigQuery:

    def __init__(self, projectId):


        self.projectId = projectId
        self.service_account_json = "/Chirantan/Projects/google-sheets-to-bigquery/psd-data-warehouse-649a8f4a3f43.json"
        self.credentials = service_account.Credentials.from_service_account_file(self.service_account_json)
        self.bqClient = bigquery.client.Client(credentials=self.credentials, project=projectId)
        self. datasets = list(self.bqClient.list_datasets())
        self.dataset_ref = self.bqClient.dataset(self.datasets[0].dataset_id)
        self.table_names = ["facebook_ads","google_ads","bing_ads","google_analytics_sessions","google_analytics_pages"]
        self.dataframe = sheetsClient("1XpnBmbzZ-8mfZHVyCwySwfdNmXHqMwRKAaqbD7nu-lE").Sheets()


    def createSchema(self):

        datasets_schema =  {"facebook_ads_schema" : [
            bigquery.SchemaField("report_date","STRING"), bigquery.SchemaField("campaign_name", "STRING"), bigquery.SchemaField("ad_set_name", "STRING"), bigquery.SchemaField("ad_name", "STRING"),
            bigquery.SchemaField("placement", "STRING"), bigquery.SchemaField("device_platform", "STRING"), bigquery.SchemaField("impressions", "INTEGER"), bigquery.SchemaField("clicks", "INTEGER"),
            bigquery.SchemaField("cost", "FLOAT")
            ], "google_ads_schema" : [
            bigquery.SchemaField("report_date","STRING"), bigquery.SchemaField("campaign_name", "STRING"), bigquery.SchemaField("ad_group_name", "STRING"), bigquery.SchemaField("keyword", "STRING"),
            bigquery.SchemaField("network", "STRING"), bigquery.SchemaField("device", "STRING"), bigquery.SchemaField("impressions", "INTEGER"), bigquery.SchemaField("clicks", "INTEGER"),
            bigquery.SchemaField("cost", "FLOAT")
            ], "bing_ads_schema" : [
            bigquery.SchemaField("report_date", "STRING"), bigquery.SchemaField("campaign_name", "STRING"),
            bigquery.SchemaField("ad_group_name", "STRING"), bigquery.SchemaField("keyword", "STRING"),
            bigquery.SchemaField("device_type", "STRING"),bigquery.SchemaField("ad_distribution", "STRING"),
            bigquery.SchemaField("impressions", "INTEGER"), bigquery.SchemaField("clicks", "INTEGER"),
            bigquery.SchemaField("cost", "FLOAT")
        ], "google_analytics_sessions_schema" : [
            bigquery.SchemaField("report_date", "STRING"), bigquery.SchemaField("source", "STRING"),
            bigquery.SchemaField("medium", "STRING"), bigquery.SchemaField("campaign", "STRING"),
            bigquery.SchemaField("ad_content", "STRING"), bigquery.SchemaField("keyword", "STRING"),
            bigquery.SchemaField("channel_grouping", "STRING"), bigquery.SchemaField("device_category", "STRING"),
            bigquery.SchemaField("landing_page", "STRING"), bigquery.SchemaField("new_users", "INTEGER"),
            bigquery.SchemaField("sessions", "INTEGER"), bigquery.SchemaField("bounces", "INTEGER"),
            bigquery.SchemaField("page_views", "INTEGER"), bigquery.SchemaField("goal_7_completions", "INTEGER"),
            bigquery.SchemaField("goal_8_completions", "INTEGER"), bigquery.SchemaField("goal_9_completions", "INTEGER")
        ], "google_analytics_pages_schema" : [
            bigquery.SchemaField("report_date", "STRING"), bigquery.SchemaField("source", "STRING"),
            bigquery.SchemaField("medium", "STRING"), bigquery.SchemaField("campaign", "STRING"),
            bigquery.SchemaField("ad_content", "STRING"), bigquery.SchemaField("keyword", "STRING"),
            bigquery.SchemaField("channel_grouping", "STRING"), bigquery.SchemaField("device_category", "STRING"),
            bigquery.SchemaField("landing_page", "STRING"), bigquery.SchemaField("page_views", "INTEGER"),
            bigquery.SchemaField("unique_page_views", "INTEGER"), bigquery.SchemaField("entrances", "INTEGER"),
            bigquery.SchemaField("exits", "INTEGER"), bigquery.SchemaField("bounces", "INTEGER"),
            bigquery.SchemaField("total_time_on_page", "INTEGER")
        ] }

        datasets = list(self.bqClient.list_datasets())

        table_id = []

        for table in self.table_names:

            table_id.append(self.projectId + "." + datasets[0].dataset_id + "." + table)



        for k, v in datasets_schema.items():

            if k == "facebook_ads_schema":

                table = bigquery.Table(table_id[0], schema=v)
                table = self.bqClient.create_table(table)
                print("Table Created {}.{}.{}".format(table.project,table.dataset_id,table.table_id))


            elif k == "google_ads_schema":

                table = bigquery.Table(table_id[1], schema=v)
                table = self.bqClient.create_table(table)
                print("Table Created {}.{}.{}".format(table.project, table.dataset_id, table.table_id))

            elif k == "bing_ads_schema":

                table = bigquery.Table(table_id[2], schema=v)
                table = self.bqClient.create_table(table)
                print("Table Created {}.{}.{}".format(table.project, table.dataset_id, table.table_id))

            elif k == "google_analytics_sessions_schema":

                table = bigquery.Table(table_id[3], schema=v)
                table = self.bqClient.create_table(table)
                print("Table Created {}.{}.{}".format(table.project, table.dataset_id, table.table_id))

            elif k == "google_analytics_pages_schema":

                table = bigquery.Table(table_id[4], schema=v)
                table = self.bqClient.create_table(table)
                print("Table Created {}.{}.{}".format(table.project, table.dataset_id, table.table_id))

    def getSchema(self):

        tables_schema = {}

        table_list_ref = list(self.bqClient.list_tables(self.datasets[0].dataset_id))

        for table in table_list_ref:
            tables_schema[table.table_id] = []
            table_ref = self.dataset_ref.table(table.table_id)
            table = self.bqClient.get_table(table_ref)
            schema = table.schema
            for schema_field in schema:
                tables_schema[table.table_id].append(schema_field.name)


        return tables_schema


    def loadData(self):


        table_list_ref = list(self.bqClient.list_tables(self.datasets[0].dataset_id))

        tables_schema = bigQuery(self.projectId).getSchema()



        for tables in table_list_ref:
            for table,schema in tables_schema.items():
                for platform,data in self.dataframe.items():

                    if platform == 'Facebook Ads' and tables.table_id == self.table_names[0] and table == self.table_names[0]:
                        self.dataframe[platform].columns = schema
                        dataframe = self.dataframe[platform].astype(
                            {"report_date": 'object', "campaign_name": 'object', "ad_set_name": 'object',
                             "ad_name": 'object', "placement": 'object', "device_platform": 'object',
                             "impressions": 'int64', "clicks": 'int64', "cost": 'float64'})
                        table_ref = self.dataset_ref.table(self.table_names[0])
                        data = self.bqClient.load_table_from_dataframe(dataframe, table_ref,location="US" )
                        data.result()
                        print("Data has been loaded on for, {}".format(platform))

                    elif platform == 'Google Ads' and tables.table_id == self.table_names[1] and table == self.table_names[1]:
                        self.dataframe[platform].columns = schema
                        dataframe = self.dataframe[platform].astype(
                            {"report_date": 'object', "campaign_name": 'object', "ad_group_name": 'object',
                             "keyword": 'object', "network": 'object', "device": 'object',
                             "impressions": 'int64', "clicks": 'int64', "cost": 'float64'})
                        table_ref = self.dataset_ref.table(self.table_names[1])
                        data = self.bqClient.load_table_from_dataframe(dataframe, table_ref,location="US")
                        data.result()
                        print("Data has been loaded on for, {}".format(platform))

                    elif platform == 'Bing Ads' and tables.table_id == self.table_names[2] and table == self.table_names[2]:
                        self.dataframe[platform].columns = schema
                        dataframe = self.dataframe[platform].astype({"report_date": 'object', "campaign_name": 'object', "ad_group_name": 'object', "keyword": 'object', "device_type": 'object', "ad_distribution": 'object',"impressions": 'int64', "clicks": 'int64', "cost": 'float64'})
                        table_ref = self.dataset_ref.table(self.table_names[2])
                        data = self.bqClient.load_table_from_dataframe(dataframe, table_ref,location="US")
                        data.result()
                        print("Data has been loaded on for, {}".format(platform))

                    elif platform == 'GoogleAnalytics_Sessions' and tables.table_id == self.table_names[3] and table == self.table_names[3]:
                        self.dataframe[platform].columns = schema
                        dataframe = self.dataframe[platform].astype(
                            {"report_date": 'object', "source": 'object', "medium": 'object', "campaign": 'object',
                             "ad_content": 'object', "keyword": 'object', "channel_grouping": 'object', "device_category": 'object', "landing_page": 'object',
                             "new_users": 'int64', "sessions": 'int64', "bounces": 'int64',
                             "page_views": 'int64', "goal_7_completions": 'int64',"goal_8_completions": 'int64',"goal_9_completions": 'int64'})
                        table_ref = self.dataset_ref.table(self.table_names[3])
                        data = self.bqClient.load_table_from_dataframe(dataframe, table_ref,location="US")
                        data.result()
                        print("Data has been loaded on for, {}".format(platform))

                    elif platform == 'GoogleAnalytics_Pages' and tables.table_id == self.table_names[4] and table == self.table_names[4]:
                        self.dataframe[platform].columns = schema
                        dataframe = self.dataframe[platform].astype(
                            {"report_date": 'object', "source": 'object', "medium": 'object', "campaign": 'object',
                             "ad_content": 'object', "keyword": 'object', "channel_grouping": 'object',
                             "device_category": 'object', "landing_page": 'object',
                             "page_views": 'int64', "unique_page_views": 'int64', "entrances": 'int64',
                             "exits": 'int64', "bounces": 'int64', "total_time_on_page": 'int64'})
                        table_ref = self.dataset_ref.table(self.table_names[4])
                        data = self.bqClient.load_table_from_dataframe(dataframe, table_ref,location="US")
                        data.result()
                        print("Data has been loaded on for, {}".format(platform))










x = bigQuery("psd-data-warehouse").loadData()
# print(x)
