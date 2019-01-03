import unittest

from mock import patch

from src.slo.backup_quality.predicate.sli_table_newer_modification_predicate import \
    SLITableNewerModificationPredicate
from src.slo.backup_quality.quality_violation_sli_service import QualityViolationSliService
from src.slo.predicate.sli_table_exists_predicate import \
    SLITableExistsPredicate
from src.slo.sli_results_streamer import SLIResultsStreamer


class TestQualityViolationSliService(unittest.TestCase):

    def setUp(self):
        patch('googleapiclient.discovery.build').start()
        patch('oauth2client.client.GoogleCredentials.get_application_default') \
            .start()

    def tearDown(self):
        patch.stopall()

    @patch.object(SLITableNewerModificationPredicate, 'is_modified_since_last_census_snapshot')
    @patch.object(SLIResultsStreamer, 'stream')
    @patch.object(SLITableExistsPredicate, 'exists')
    def test_check_and_stream_violation_that_is_modified_since_last_census_snapshot_should_be_filtered_out(
            self, table_exists, stream, is_modified_since_last_census_snapshot):
        # given
        table_exists.return_value = True
        is_modified_since_last_census_snapshot.return_value = True
        payload = {"projectId": "p1", "datasetId": "d1",
                   "tableId": "t1", "partitionId": "part1"}

        # when
        QualityViolationSliService().check_and_stream_violation(payload)

        # then
        stream.assert_not_called()

    @patch.object(SLITableNewerModificationPredicate, 'is_modified_since_last_census_snapshot')
    @patch.object(SLIResultsStreamer, 'stream')
    @patch.object(SLITableExistsPredicate, 'exists')
    def test_check_and_stream_violation_that_is_not_modified_since_last_census_snapshot_should_not_be_filtered_out(
            self, table_exists, stream, is_modified_since_last_census_snapshot):
        # given
        table_exists.return_value = True
        is_modified_since_last_census_snapshot.return_value = False
        payload = {"projectId": "p1", "datasetId": "d1",
                   "tableId": "t1", "partitionId": "part1"}

        # when
        QualityViolationSliService().check_and_stream_violation(payload)

        # then
        stream.assert_called_with([{"projectId": "p1", "datasetId": "d1",
                                    "tableId": "t1", "partitionId": "part1"}])

    @patch.object(SLIResultsStreamer, 'stream')
    @patch.object(SLITableExistsPredicate, 'exists')
    def test_check_and_stream_violation_table_that_not_exists_should_be_filtered_out(
            self,  table_exists, stream):
        # given
        table_exists.return_value = False
        payload = {"projectId": "p1", "datasetId": "d1",
                   "tableId": "t1", "partitionId": "part1"}

        # when
        QualityViolationSliService().check_and_stream_violation(payload)

        # then
        stream.assert_not_called()

    @patch.object(SLIResultsStreamer, 'stream')
    @patch.object(SLITableExistsPredicate, 'exists')
    @patch.object(SLITableNewerModificationPredicate, 'is_modified_since_last_census_snapshot')
    def test_check_and_stream_violation_table_that_caused_exception_in_modification_predicate_should_not_be_filtered_out(
            self, is_modified_since_last_census_snapshot, table_exists, stream):
        # given
        table_exists.return_value = True
        is_modified_since_last_census_snapshot.side_effect = Exception("An error")
        payload = {"projectId": "p1", "datasetId": "d1",
                   "tableId": "t1", "partitionId": "part1"}

        # when
        QualityViolationSliService().check_and_stream_violation(payload)

        # then
        stream.assert_called_with([{"projectId": "p1", "datasetId": "d1",
                                    "tableId": "t1", "partitionId": "part1"}])

    @patch.object(SLIResultsStreamer, 'stream')
    @patch.object(SLITableExistsPredicate, 'exists')
    def test_check_and_stream_violation_table_that_caused_exception_in_exists_predicate_should_not_be_filtered_out(
            self, exists, stream):
        # given
        exists.return_value = Exception("An error")
        payload = {"projectId": "p1", "datasetId": "d1",
                   "tableId": "t1", "partitionId": "part1"}
        # when
        QualityViolationSliService().check_and_stream_violation(payload)

        # then
        stream.assert_called_with([{"projectId": "p1", "datasetId": "d1",
                                    "tableId": "t1", "partitionId": "part1"}])
