import logging
import json

from src.commons.big_query.big_query import BigQuery
from src.slo.backup_creation_latency.latency_query_specification import \
    LatencyQuerySpecification
from src.slo.predicate.sli_table_exists_predicate import SLITableExistsPredicate
from src.slo.sli_results_streamer import SLIResultsStreamer
from src.slo.backup_creation_latency.predicate.sli_table_recreation_predicate import \
  SLITableRecreationPredicate
from src.slo.sli_view_querier import SLIViewQuerier
from src.commons.tasks import Tasks
from google.appengine.api.taskqueue import Task


class LatencySliService(object):

    def __init__(self, x_days):
        self.x_days = x_days
        big_query = BigQuery()
        self.querier = SLIViewQuerier(
            big_query,
            LatencyQuerySpecification(self.x_days)
        )
        self.streamer = SLIResultsStreamer(
            table_id="SLI_backup_creation_latency"
        )
        self.table_existence_predicate = SLITableExistsPredicate(big_query, LatencyQuerySpecification)
        self.table_recreation_predicate = SLITableRecreationPredicate(big_query)

    def recalculate_sli(self):
        logging.info("Recalculating %s days SLI has been started.", self.x_days)

        all_tables, snapshot_time = self.querier.query()

        self.streamer.stream([self.__create_snapshot_marker_row(snapshot_time, self.x_days)])
        logging.info("Snapshot marker sent %s for sli latency days: %s", snapshot_time, self.x_days)

        tasks = []
        for table in all_tables:
            tasks.append(
                Task(
                    method='POST',
                    url='/sli/latency/violation',
                    payload=json.dumps({'table': table, 'x_days': self.x_days}),
                    headers={'Content-Type': 'application/json'}
                )
            )
        if tasks:
            Tasks.schedule('sli-table-latency-violations', tasks)

    @staticmethod
    def __create_snapshot_marker_row(snapshot_time, x_days):
        return {"snapshotTime": snapshot_time,
                "projectId": 'SNAPSHOT_MARKER',
                "datasetId": 'SNAPSHOT_MARKER',
                "tableId": 'SNAPSHOT_MARKER',
                "partitionId": 'SNAPSHOT_MARKER',
                "creationTime": float(0),
                "lastModifiedTime": float(0),
                "backupCreated": float(0),
                "backupLastModified": float(0),
                "xDays": x_days}
