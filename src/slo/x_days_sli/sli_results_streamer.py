import logging


class SLIResultsStreamer(object):

    def __init__(self, big_query):
        self.big_query = big_query

    def stream(self, sli_results):
        logging.info("TODO here results should be streamed into BQ")
        logging.info("SLI results: %s", sli_results)
        pass
