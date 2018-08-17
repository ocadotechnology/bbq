import logging

from src.slo.x_days_sli.sli_view_querier import SLIViewQuerier


class XDaysSLIService(object):


    def __init__(self):
        self.querier = SLIViewQuerier()

    def calculate_sli(self):

        query_results = self.querier.query(3)
        logging.info(query_results)
        return query_results


