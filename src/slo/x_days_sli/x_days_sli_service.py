import logging

from src.slo.x_days_sli.sli_table_exists_filter import SLITableExistsFilter
from src.slo.x_days_sli.sli_results_streamer import SLIResultsStreamer
from src.slo.x_days_sli.sli_view_querier import SLIViewQuerier


class XDaysSLIService(object):

    def __init__(self,x_days):
        self.querier = SLIViewQuerier()
        self.streamer = SLIResultsStreamer()
        self.filter = SLITableExistsFilter()
        self.x_days = x_days

    def recalculate_sli(self):
        logging.info("Recalculating %s days SLI has been started.", self.x_days)

        all_tables = self.querier.query(self.x_days)
        filtered_tables = [table for table in all_tables
                           if self.filter.exists(table)]

        logging.info("%s days SLI tables filtered from %s to %s", self.x_days,
                     len(all_tables), len(filtered_tables))
        self.streamer.stream(filtered_tables)
