import logging
import json

from src.commons.big_query.big_query import BigQuery
from src.commons.tasks import Tasks
from google.appengine.api.taskqueue import Task
from src.slo.backup_quality.quality_query_specification import \
    QualityQuerySpecification
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

    def recalculate_sli(self):
        logging.info("Recalculating quality SLI has been started.")

        all_tables, snapshot_time = self.querier.query()

        self.streamer.stream([self.__create_snapshot_marker_row(snapshot_time)])
        logging.info("Snapshot marker sent %s", snapshot_time)

        tasks = []
        for table in all_tables:
            tasks.append(
                Task(
                    method='POST',
                    url='/sli/quality/violation',
                    payload=json.dumps({'table': table}),
                    headers={'Content-Type': 'application/json'}
                )
            )
        if tasks:
            Tasks.schedule('sli-table-quality-violations', tasks)

    @staticmethod
    def __create_snapshot_marker_row(snapshot_time):
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
