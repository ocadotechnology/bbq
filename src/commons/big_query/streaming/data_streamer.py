import datetime
import json
import logging
import uuid

from apiclient.errors import Error
import httplib2
import googleapiclient.discovery
from oauth2client.client import GoogleCredentials

from src.commons.big_query.big_query_table import BigQueryTable
from src.commons.decorators.retry import retry


class DataStreamer(object):

    @staticmethod
    def _create_http():
        return httplib2.Http(timeout=60)

    def __init__(self, project_id, dataset_id, table_id):
        self.big_query_table = BigQueryTable(project_id, dataset_id, table_id)
        self.http = self._create_http()
        self.service = googleapiclient.discovery.build(
            'bigquery',
            'v2',
            credentials=GoogleCredentials.get_application_default(),
            http=self.http
        )

    def stream_stats(self, rows, insert_id=None):
        if insert_id is None: insert_id = uuid.uuid4()
        insert_all_data = {
            'rows': [{
                'json': data,
                'insertId': str(insert_id)
            } for data in rows]
        }
        logging.info("Streaming data to table %s (insertId:%s)", self.big_query_table, insert_id)
        insert_all_response = self._stream_metadata(insert_all_data)
        if 'insertErrors' in insert_all_response:
            logging.debug("Sent json: \n%s", json.dumps(insert_all_data))
            logging.error("Error during streaming metadata to BigQuery: \n%s",
                          json.dumps(insert_all_response['insertErrors']))
        else:
            logging.debug("Stats have been sent successfully to %s table",
                          self.big_query_table)

    @retry(Error, tries=2, delay=2, backoff=2)
    def _stream_metadata(self, insert_all_data):
        partition = datetime.datetime.now().strftime("%Y%m%d")
        return self.service.tabledata().insertAll(
            projectId=self.big_query_table.get_project_id(),
            datasetId=self.big_query_table.get_dataset_id(),
            tableId='{}${}'.format(self.big_query_table.get_table_id(), partition),
            body=insert_all_data).execute(num_retries=3)