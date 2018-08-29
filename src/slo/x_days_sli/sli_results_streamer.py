import logging

from src.commons.big_query.big_query import BigQuery


class SLIResultsStreamer(object):
    def __init__(self):
        self.big_query = BigQuery()

    def stream(self, sli_results):
        logging.info("TODO here results should be streamed into BQ")
        logging.info("SLI results: %s", sli_results)
        pass
