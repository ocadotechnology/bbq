import logging
import time

from src.commons.table_reference import TableReference


class SLIViewQuerier(object):

    def __init__(self, big_query, query_specification):
        self.query_specification = query_specification
        self.big_query = big_query

    def query(self):
        self.snapshot_time = time.time()
        query = self.query_specification.query_string()
        logging.info("Executing query: %s", query)
        query_results = self.big_query.execute_query(query)
        return self.query_specification.format_query_results(query_results, self.snapshot_time), self.snapshot_time

    @staticmethod
    def sli_entry_to_table_reference(table):
        return TableReference(project_id=table['projectId'],
                              dataset_id=table['datasetId'],
                              table_id=table['tableId'],
                              partition_id=table['partitionId'])
    