import datetime
import json
import logging

import googleapiclient.discovery
from apiclient.errors import Error
from oauth2client.client import GoogleCredentials

from src.commons.big_query.big_query_table import BigQueryTable
from src.commons.decorators.retry import retry
from src.commons.error_reporting import ErrorReporting


class DataStreamer(object):

    def __init__(self, project_id, dataset_id, table_id):
        self.big_query_table = BigQueryTable(project_id, dataset_id, table_id)
        self.service = googleapiclient.discovery.build(
            'bigquery',
            'v2',
            credentials=self._create_credentials(),
            http=self._create_http()
        )

    @staticmethod
    def _create_credentials():
        return GoogleCredentials.get_application_default()

    @staticmethod
    def _create_http():
        return None

    def stream_stats(self, rows):
        insert_all_data = {
            'rows': [{
                'json': data
            } for data in rows]
        }
        logging.info("Streaming data to table %s", self.big_query_table)
        insert_all_response = self._stream_metadata(insert_all_data)
        if 'insertErrors' in insert_all_response:
            logging.debug("Sent json: \n%s", json.dumps(insert_all_data))
            error_message = "Error during streaming metadata to BigQuery: \n{}"\
                .format(json.dumps(insert_all_response['insertErrors']))
            logging.error(error_message)
            ErrorReporting().report(error_message)
        else:
            logging.debug("Stats have been sent successfully to %s table",
                          self.big_query_table)

    @retry(Error, tries=2, delay=2, backoff=2)
    def _stream_metadata(self, insert_all_data):
        partition = datetime.datetime.now().strftime("%Y%m%d")
        return self.service.tabledata().insertAll(
            projectId=self.big_query_table.get_project_id(),
            datasetId=self.big_query_table.get_dataset_id(),
            tableId='{}${}'.format(self.big_query_table.get_table_id(),
                                   partition),
            body=insert_all_data).execute(num_retries=3)
