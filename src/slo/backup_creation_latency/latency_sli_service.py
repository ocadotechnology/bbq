import logging

from src.commons.big_query.big_query import BigQuery
from src.slo.backup_creation_latency.sli_table_exists_predicate import SLITableExistsPredicate
from src.slo.sli_results_streamer import SLIResultsStreamer
from src.slo.backup_creation_latency.sli_table_recreation_predicate import \
  SLITableRecreationPredicate
from src.slo.sli_view_querier import SLIViewQuerier


class LatencySliService(object):

    def __init__(self, x_days):
        self.x_days = x_days
        big_query = BigQuery()
        self.querier = SLIViewQuerier(big_query)
        self.streamer = SLIResultsStreamer(table_id="SLI_backup_creation_latency")
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
            if self.__is_snapshot_marker_row(table):
                return True
            if not self.table_existence_predicate.exists(table):
                return False
            return not self.table_recreation_predicate.is_recreated(table)
        except Exception:
            logging.exception("An error occurred while filtering table %s, "
                              "still it will be streamed", table)
            return True

    @staticmethod
    def __is_snapshot_marker_row(table):
        return table['projectId'] == 'SNAPSHOT_MARKER'
