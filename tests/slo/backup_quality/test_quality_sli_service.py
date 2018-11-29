import unittest

from mock import patch

from src.slo.predicate.sli_table_exists_predicate import \
    SLITableExistsPredicate
from src.slo.backup_quality.quality_sli_service import QualitySliService
from src.slo.backup_quality.predicate.sli_table_newer_modification_predicate import \
    SLITableNewerModificationPredicate
from src.slo.sli_results_streamer import SLIResultsStreamer
from src.slo.sli_view_querier import SLIViewQuerier
from src.commons.test_utils import utils
from google.appengine.ext import testbed


class TestQualitySliService(unittest.TestCase):

    def setUp(self):
        patch('googleapiclient.discovery.build').start()
        patch('oauth2client.client.GoogleCredentials.get_application_default') \
            .start()
        self.testbed = testbed.Testbed()
        self.testbed.activate()
        self.taskqueue_stub = utils.init_testbed_queue_stub(self.testbed)

    def tearDown(self):
        patch.stopall()
        self.testbed.deactivate()

    @patch.object(SLIViewQuerier, 'query',
                  return_value=([
                                  {"projectId": "p1", "datasetId": "d1",
                                   "tableId": "t1",
                                   "partitionId": "part1"}
                                ], 21342134324))
    @patch.object(SLIResultsStreamer, 'stream')
    @patch.object(SLITableExistsPredicate, 'exists')
    @patch.object(SLITableNewerModificationPredicate, 'is_modified_since_last_census_snapshot')
    def test_recalculate_sli_should_start_new_task_with_data_from_query(
            self, is_modified_since_last_census_snapshot, exists, stream, _):
        # given
        exists.return_value = True
        is_modified_since_last_census_snapshot.return_value = True
        # when
        QualitySliService().recalculate_sli()
        # then
        tasks = self.taskqueue_stub.get_filtered_tasks(
            queue_names='sli-table-quality-violations')

        self.assertEqual(len(tasks), 1)
        self.assertIn('{"table": {"projectId": "p1", "partitionId": "part1", "tableId": "t1", "datasetId": "d1"}, '
                      '"snapshot_time": 21342134324}', tasks[0].payload)
        stream.assert_called_with([], snapshot_marker=self.__create_snapshot_marker_row(21342134324))

    @patch.object(SLIViewQuerier, 'query',
                  return_value=([], 21342134324))
    @patch.object(SLIResultsStreamer, 'stream')
    def test_recalculate_sli_should_always_stream_snapshot_marker(
            self, stream, _):
        # when
        QualitySliService().recalculate_sli()

        # then
        stream.assert_called_with([], snapshot_marker=self.__create_snapshot_marker_row(21342134324))

    @patch.object(SLITableNewerModificationPredicate, 'is_modified_since_last_census_snapshot')
    @patch.object(SLIResultsStreamer, 'stream')
    @patch.object(SLITableExistsPredicate, 'exists')
    def test_check_and_stream_violation_that_is_modified_since_last_census_snapshot_should_be_filtered_out(
            self, exists, stream, is_modified_since_last_census_snapshot):
        # given
        exists.return_value = True
        is_modified_since_last_census_snapshot.return_value = True
        payload = {"table": [{"projectId": "p1", "datasetId": "d1",
                                   "tableId": "t1",
                                   "partitionId": "part1"}],
                   "snapshot_time": 21342134324}

        # when
        QualitySliService().check_and_stream_violation(payload)

        # then
        stream.assert_not_called()

    @patch.object(SLITableNewerModificationPredicate, 'is_modified_since_last_census_snapshot')
    @patch.object(SLIResultsStreamer, 'stream')
    @patch.object(SLITableExistsPredicate, 'exists')
    def test_check_and_stream_violation_that_is_not_modified_since_last_census_snapshot_should_not_be_filtered_out(
            self, exists, stream, is_modified_since_last_census_snapshot):
        # given
        exists.return_value = True
        is_modified_since_last_census_snapshot.return_value = False
        payload = {"table": {"projectId": "p1", "datasetId": "d1",
                                    "tableId": "t1", "partitionId": "part1"}, "snapshot_time": 21342134324}

        # when
        QualitySliService().check_and_stream_violation(payload)

        # then
        stream.assert_called_with([{"projectId": "p1", "datasetId": "d1",
                                    "tableId": "t1", "partitionId": "part1"}])

    @patch.object(SLIResultsStreamer, 'stream')
    @patch.object(SLITableExistsPredicate, 'exists')
    def test_check_and_stream_violation_table_that_not_exists_should_be_filtered_out(
            self, exists, stream):
        # given
        exists.return_value = False
        payload = {"table": [{"projectId": "p1", "datasetId": "d1",
                                    "tableId": "t1", "partitionId": "part1"}], "snapshot_time": 21342134324}

        # when
        QualitySliService().check_and_stream_violation(payload)

        # then
        stream.assert_not_called()

    @patch.object(SLIResultsStreamer, 'stream')
    @patch.object(SLITableExistsPredicate, 'exists')
    @patch.object(SLITableNewerModificationPredicate, 'is_modified_since_last_census_snapshot')
    def test_check_and_stream_violation_table_that_caused_exception_in_modification_predicate_should_not_be_filtered_out(
            self, is_modified_since_last_census_snapshot, exists, stream):
        # given
        exists.return_value = True
        is_modified_since_last_census_snapshot.side_effect = Exception("An error")
        payload = {"table": {"projectId": "p1", "datasetId": "d1",
                                    "tableId": "t1", "partitionId": "part1"}, "snapshot_time": 21342134324}

        # when
        QualitySliService().check_and_stream_violation(payload)

        # then
        stream.assert_called_with([{"projectId": "p1", "datasetId": "d1",
                                    "tableId": "t1", "partitionId": "part1"}])

    @patch.object(SLIResultsStreamer, 'stream')
    @patch.object(SLITableExistsPredicate, 'exists')
    def test_check_and_stream_violation_table_that_caused_exception_in_exists_predicate_should_not_be_filtered_out(
            self, exists, stream):
        # given
        exists.return_value = Exception("An error")
        payload = {"table": {"projectId": "p1", "datasetId": "d1",
                                    "tableId": "t1", "partitionId": "part1"}, "snapshot_time": 21342134324}
        # when
        QualitySliService().check_and_stream_violation(payload)

        # then
        stream.assert_called_with([{"projectId": "p1", "datasetId": "d1",
                                    "tableId": "t1", "partitionId": "part1"}])

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
