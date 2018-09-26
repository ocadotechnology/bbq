import logging

from src.commons.big_query.big_query import BigQuery
from src.slo.x_days_sli.sli_table_exists_predicate import SLITableExistsPredicate
from src.slo.x_days_sli.sli_results_streamer import SLIResultsStreamer
from src.slo.x_days_sli.sli_table_recreation_predicate import \
  SLITableRecreationPredicate
from src.slo.x_days_sli.sli_view_querier import SLIViewQuerier


class XDaysSLIService(object):

    def __init__(self, x_days):
        self.x_days = x_days
        big_query = BigQuery()
        self.querier = SLIViewQuerier(big_query)
        self.streamer = SLIResultsStreamer()
        self.table_existence_predicate = SLITableExistsPredicate(big_query)
        self.table_recreation_predicate = SLITableRecreationPredicate(big_query)

    def recalculate_sli(self):
        logging.info("Recalculating %s days SLI has been started.", self.x_days)

        all_tables = self.querier.query(self.x_days)
        filtered_tables = [table for table in all_tables
                           if self.__should_stay_as_sli_violation(table)]

        logging.info("%s days SLI tables filtered from %s to %s", self.x_days,
                     len(all_tables), len(filtered_tables))
        self.streamer.stream(filtered_tables)

    def __should_stay_as_sli_violation(self, table):
        try:
            if not self.table_existence_predicate.exists(table):
                return False
            return not self.table_recreation_predicate.is_recreated(table)
        except Exception:
            logging.exception("An error occurred while filtering table %s, "
                              "still it will be streamed", table)
            return True
