import unittest

from mock import patch

from src.slo.backup_creation_latency.latency_violation_sli_service import LatencyViolationSliService
from src.slo.backup_creation_latency.predicate.sli_table_emptiness_predicate import \
  SLITableEmptinessPredicate
from src.slo.backup_creation_latency.predicate.sli_table_has_any_backup_predicate import \
  SLITableHasAnyBackupPredicate
from src.slo.backup_creation_latency.predicate.sli_table_recreation_predicate import SLITableRecreationPredicate
from src.slo.predicate.sli_table_exists_predicate import \
    SLITableExistsPredicate
from src.slo.sli_results_streamer import SLIResultsStreamer


class TestLatencyViolationSliService(unittest.TestCase):

    def setUp(self):
        patch('googleapiclient.discovery.build').start()
        patch('oauth2client.client.GoogleCredentials.get_application_default') \
            .start()

    def tearDown(self):
        patch.stopall()

    @patch.object(SLITableEmptinessPredicate, 'is_empty')
    @patch.object(SLITableHasAnyBackupPredicate, 'has_any_backup')
    @patch.object(SLITableRecreationPredicate, 'is_recreated')
    @patch.object(SLITableExistsPredicate, 'exists')
    @patch.object(SLIResultsStreamer, 'stream')
    def test_table_that_exist_and_is_not_recreated_and_has_no_backup_and_is_empty_should_not_be_filtered_out(
            self, stream, exists, is_recreated, has_any_backup, is_empty):
        # given
        exists.return_value = True
        is_recreated.return_value = False
        has_any_backup.return_value = False
        is_empty.return_value = True

        payload = {"projectId": "p1", "datasetId": "d1",
                   "tableId": "t1",
                   "partitionId": "part1"}
        # when
        LatencyViolationSliService(4).check_and_stream_violation(payload)
        # then
        stream.assert_called_with([{"projectId": "p1", "datasetId": "d1",
                                    "tableId": "t1", "partitionId": "part1"}])

    @patch.object(SLITableEmptinessPredicate, 'is_empty')
    @patch.object(SLITableHasAnyBackupPredicate, 'has_any_backup')
    @patch.object(SLITableRecreationPredicate, 'is_recreated')
    @patch.object(SLITableExistsPredicate, 'exists')
    @patch.object(SLIResultsStreamer, 'stream')
    def test_table_that_exist_and_is_not_recreated_and_has_some_backup_and_is_empty_should_be_filtered_out(
          self, stream, exists, is_recreated, has_any_backup, is_empty):
        # given
        exists.return_value = True
        is_recreated.return_value = False
        has_any_backup.return_value = True
        is_empty.return_value = True

        payload = {"projectId": "p1", "datasetId": "d1",
                   "tableId": "t1",
                   "partitionId": "part1"}
        # when
        LatencyViolationSliService(4).check_and_stream_violation(payload)
        # then
        stream.assert_not_called()

    @patch.object(SLITableExistsPredicate, 'exists')
    @patch.object(SLIResultsStreamer, 'stream')
    def test_table_that_caused_exception_in_exist_filter_should_not_be_filtered_out(
            self, stream, exists):
        # given
        exists.side_effect = Exception("An error")
        payload = {"projectId": "p1", "datasetId": "d1",
                   "tableId": "t1",
                   "partitionId": "part1"}
        # when
        LatencyViolationSliService(4).check_and_stream_violation(payload)

        # then
        stream.assert_called_with([{"projectId": "p1", "datasetId": "d1",
                                    "tableId": "t1", "partitionId": "part1"}])

    @patch.object(SLITableEmptinessPredicate, 'is_empty')
    @patch.object(SLITableHasAnyBackupPredicate, 'has_any_backup')
    @patch.object(SLITableRecreationPredicate, 'is_recreated')
    @patch.object(SLITableExistsPredicate, 'exists')
    @patch.object(SLIResultsStreamer, 'stream')
    def test_table_that_caused_exception_in_recreation_filter_should_not_be_filtered_out(
            self, stream, exists, is_recreated, has_any_backup, is_empty):

        # given
        exists.return_value = True
        is_recreated.side_effect = Exception("An error")
        has_any_backup.return_value = True
        is_empty.return_value = False
        payload = {"projectId": "p1", "datasetId": "d1",
                   "tableId": "t1",
                   "partitionId": "part1"}

        # when
        LatencyViolationSliService(4).check_and_stream_violation(payload)

        # then
        stream.assert_called_with([{"projectId": "p1", "datasetId": "d1",
                                    "tableId": "t1", "partitionId": "part1"}])

    @patch.object(SLITableEmptinessPredicate, 'is_empty')
    @patch.object(SLITableHasAnyBackupPredicate, 'has_any_backup')
    @patch.object(SLITableRecreationPredicate, 'is_recreated')
    @patch.object(SLITableExistsPredicate, 'exists')
    @patch.object(SLIResultsStreamer, 'stream')
    def test_table_that_caused_exception_in_has_any_backup_filter_should_not_be_filtered_out(
          self, stream, exists, is_recreated, has_any_backup, is_empty):

        # given
        exists.return_value = True
        is_recreated.side_effect = False
        has_any_backup.return_value = Exception("An error")
        is_empty.return_value = False
        payload = {"projectId": "p1", "datasetId": "d1",
                   "tableId": "t1",
                   "partitionId": "part1"}

        # when
        LatencyViolationSliService(4).check_and_stream_violation(payload)

        # then
        stream.assert_called_with([{"projectId": "p1", "datasetId": "d1",
                                    "tableId": "t1", "partitionId": "part1"}])


    @patch.object(SLITableEmptinessPredicate, 'is_empty')
    @patch.object(SLITableHasAnyBackupPredicate, 'has_any_backup')
    @patch.object(SLITableRecreationPredicate, 'is_recreated')
    @patch.object(SLITableExistsPredicate, 'exists')
    @patch.object(SLIResultsStreamer, 'stream')
    def test_table_that_caused_exception_in_is_empty_filter_should_not_be_filtered_out(
        self, stream, exists, is_recreated, has_any_backup, is_empty):

        # given
        exists.return_value = True
        is_recreated.side_effect = False
        has_any_backup.return_value = True
        is_empty.return_value = Exception("An error")

        payload = {"projectId": "p1", "datasetId": "d1",
                   "tableId": "t1",
                   "partitionId": "part1"}

        # when
        LatencyViolationSliService(4).check_and_stream_violation(payload)

        # then
        stream.assert_called_with([{"projectId": "p1", "datasetId": "d1",
                                    "tableId": "t1", "partitionId": "part1"}])

    def __create_snapshot_marker_row(self, snapshot_time, x_days):
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
