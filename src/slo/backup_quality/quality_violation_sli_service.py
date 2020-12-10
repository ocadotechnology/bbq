import logging

from src.commons.big_query.big_query import BigQuery
from src.slo.backup_quality.predicate.sli_backup_table_not_seen_by_census_predicate import \
    SLIBackupTableNotSeenByCensusPredicate
from src.slo.predicate.sli_table_exists_predicate import \
    SLITableExistsPredicate
from src.slo.backup_quality.quality_query_specification import \
    QualityQuerySpecification
from src.slo.backup_quality.predicate.sli_table_newer_modification_predicate import \
    SLITableNewerModificationPredicate
from src.slo.sli_results_streamer import SLIResultsStreamer
from src.slo.sli_view_querier import SLIViewQuerier


class QualityViolationSliService(object):

    def __init__(self):
        big_query = BigQuery()
        self.querier = SLIViewQuerier(
            big_query, QualityQuerySpecification()
        )
        self.streamer = SLIResultsStreamer(
            table_id="SLI_backup_quality"
        )
        self.table_newer_modification_predicate = SLITableNewerModificationPredicate(big_query)
        self.table_existence_predicate = SLITableExistsPredicate(big_query, QualityQuerySpecification)
        self.backup_table_not_seen_by_census_predicate = SLIBackupTableNotSeenByCensusPredicate(big_query, QualityQuerySpecification)

    def check_and_stream_violation(self, json_table):
        if self.__should_stay_as_sli_violation(json_table):
            filtered_table = [json_table]
            self.streamer.stream(filtered_table)

    def __should_stay_as_sli_violation(self, table):
        try:
            if not self.table_existence_predicate.exists(table):
                return False
            if self.table_newer_modification_predicate.is_modified_since_last_census_snapshot(table):
                return False
            if self.backup_table_not_seen_by_census_predicate.is_not_seen_by_census(table):
                return False
            return True
        except Exception:
            logging.exception("An error occurred while filtering table %s, "
                              "still it will be streamed", table)
            return True
