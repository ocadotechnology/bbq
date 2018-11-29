import logging
import json

from src.commons.big_query.big_query import BigQuery
from src.commons.tasks import Tasks
from google.appengine.api.taskqueue import Task
from src.slo.predicate.sli_table_exists_predicate import \
    SLITableExistsPredicate
from src.slo.backup_quality.quality_query_specification import \
    QualityQuerySpecification
from src.slo.backup_quality.predicate.sli_table_newer_modification_predicate import \
    SLITableNewerModificationPredicate
from src.slo.sli_results_streamer import SLIResultsStreamer
from src.slo.sli_view_querier import SLIViewQuerier


class QualitySliService(object):

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

    def recalculate_sli(self):
        logging.info("Recalculating quality SLI has been started.")

        all_tables, snapshot_time = self.querier.query()
        tasks = []
        for table in all_tables:
            tasks.append(
                Task(
                    method='POST',
                    url='/sli/quality/violation',
                    payload=json.dumps({'table': table, 'snapshot_time': snapshot_time}),
                    headers={'Content-Type': 'application/json'}
                )
            )
        if tasks:
            Tasks.schedule('sli-table-quality-violations', tasks)

        self.streamer.stream([], snapshot_marker=self.__create_snapshot_marker_row(snapshot_time))
        logging.info("Snapshot marker sent %s", snapshot_time)

    def check_and_stream_violation(self, json_data):
        if self.__should_stay_as_sli_violation(json_data['table']):
            filtered_table = [json_data['table']]
            self.streamer.stream(filtered_table)

    def __should_stay_as_sli_violation(self, table):
        try:
            if not self.table_existence_predicate.exists(table):
                return False
            return not self.table_newer_modification_predicate.is_modified_since_last_census_snapshot(table)
        except Exception:
            logging.exception("An error occurred while filtering table %s, "
                              "still it will be streamed", table)
            return True

    def __create_snapshot_marker_row(self, snapshot_time):
        return {"snapshotTime": snapshot_time,
                "projectId": 'SNAPSHOT_MARKER',
                "datasetId": 'SNAPSHOT_MARKER',
                "tableId": 'SNAPSHOT_MARKER',
                "partitionId": 'SNAPSHOT_MARKER',
                "backupDatasetId": 'SNAPSHOT_MARKER',
                "backupTableId": 'SNAPSHOT_MARKER',
                "lastModifiedTime": float(0),
                "backupLastModifiedTime": float(0),
                "backupEntityLastModifiedTime": float(0),
                "numBytes": 0,
                "backupNumBytes": 0,
                "backupEntityNumBytes": 0,
                "numRows": 0,
                "backupNumRows": 0
                }
