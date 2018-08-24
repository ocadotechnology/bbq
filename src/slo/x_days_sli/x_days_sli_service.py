import logging

from src.slo.x_days_sli.sli_results_streamer import SLIResultsStreamer
from src.slo.x_days_sli.sli_view_querier import SLIViewQuerier


class XDaysSLIService(object):

    def __init__(self,x_days):
        self.querier = SLIViewQuerier()
        self.streamer = SLIResultsStreamer()
        self.x_days = x_days

    def recalculate_sli(self):
        logging.info("Recalculating %s days SLI has been started.", self.x_days)
        self.streamer.stream(self.querier.query(self.x_days))
