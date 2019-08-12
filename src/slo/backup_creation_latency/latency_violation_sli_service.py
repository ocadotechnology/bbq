import logging

from src.commons.big_query.big_query import BigQuery
from src.slo.backup_creation_latency.latency_query_specification import \
    LatencyQuerySpecification
from src.slo.backup_creation_latency.predicate.sli_table_emptiness_predicate import \
    SLITableEmptinessPredicate
from src.slo.backup_creation_latency.predicate.sli_table_has_any_backup_predicate import \
    SLITableHasAnyBackupPredicate
from src.slo.backup_creation_latency.predicate.sli_table_recreation_predicate import \
    SLITableRecreationPredicate
from src.slo.predicate.sli_table_exists_predicate import \
    SLITableExistsPredicate
from src.slo.sli_results_streamer import SLIResultsStreamer


class LatencyViolationSliService(object):

    def __init__(self, x_days):
        self.x_days = x_days
        big_query = BigQuery()
        self.streamer = SLIResultsStreamer(
            table_id="SLI_backup_creation_latency"
        )
        self.table_existence_predicate = SLITableExistsPredicate(big_query, LatencyQuerySpecification)
        self.table_recreation_predicate = SLITableRecreationPredicate(big_query)
        self.table_emptiness_predicate = SLITableEmptinessPredicate(big_query)
        self.table_has_any_backup_predicate = SLITableHasAnyBackupPredicate()

    def check_and_stream_violation(self, json_table):
        if self.__should_stay_as_sli_violation(json_table):
            filtered_table = [json_table]
            self.streamer.stream(filtered_table)

    def __should_stay_as_sli_violation(self, table):
        try:
            if not self.table_existence_predicate.exists(table):
                return False
            if self.table_recreation_predicate.is_recreated(table):
                return False
            if self.table_has_any_backup_predicate.has_any_backup(table) and \
                    self.table_emptiness_predicate.is_empty(table):
                return False
            return True
        except Exception:
            logging.exception("An error occurred while filtering table %s, "
                              "still it will be streamed", table)
            return True
