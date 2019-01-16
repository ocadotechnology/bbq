import logging
import time


class SLIViewQuerier(object):

    def __init__(self, big_query, query_specification):
        self.big_query = big_query
        self.query_specification = query_specification

    def query(self):
        self.snapshot_time = time.time()
        query = self.query_specification.query_string()
        logging.info("Executing query: %s", query)
        query_results = self.big_query.execute_query(query)
        return self.query_specification.format_query_results(query_results, self.snapshot_time), self.snapshot_time
