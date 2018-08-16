import logging

import webapp2

from src.retention.policy.fifo_10_up_to_7_months import Fifo10UpTo7Months
from src.retention.table_retention import TableRetention
from src.commons.table_reference import TableReference


class TableRetentionHandler(webapp2.RequestHandler):
    def __init__(self, request=None, response=None):
        super(TableRetentionHandler, self).__init__(request, response)
        self.table_retention = TableRetention(Fifo10UpTo7Months())

    def get(self):
        project_id = self.request.get('projectId')
        dataset_id = self.request.get('datasetId')
        table_id = self.request.get('tableId')
        table_key = self.request.get('tableKey')
        partition_id = self.request.get('partitionId', None)

        table_reference = TableReference(project_id, dataset_id, table_id,
                                         partition_id)

        logging.info("Retention will run for table '%s'", table_reference)

        self.table_retention.perform_retention(table_reference, table_key)
