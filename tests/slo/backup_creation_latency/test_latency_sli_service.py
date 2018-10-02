import unittest

from mock import patch

from src.slo.sli_results_streamer import SLIResultsStreamer
from src.slo.backup_creation_latency.sli_table_exists_predicate import SLITableExistsPredicate
from src.slo.backup_creation_latency.sli_table_recreation_predicate import \
    SLITableRecreationPredicate
from src.slo.sli_view_querier import SLIViewQuerier
from src.slo.backup_creation_latency.latency_sli_service import LatencySliService


class TestLatencySliService(unittest.TestCase):

    def setUp(self):
        patch('googleapiclient.discovery.build').start()
        patch('oauth2client.client.GoogleCredentials.get_application_default') \
            .start()

    def tearDown(self):
        patch.stopall()

    @patch.object(SLIViewQuerier, 'query', return_value=[
        {"projectId": "p1", "datasetId": "d1", "tableId": "t1",
         "partitionId": "part1"}
    ])
    @patch.object(SLITableExistsPredicate, 'exists')
    @patch.object(SLIResultsStreamer, 'stream')
    def test_table_that_not_exist_should_be_filtered_out(self,
        stream, exists, _):
        # given
        exists.return_value = False
        # when
        LatencySliService(3).recalculate_sli()
        # then
        stream.assert_called_with([])

    @patch.object(SLIViewQuerier, 'query', return_value=[
        {"projectId": "p1", "datasetId": "d1", "tableId": "t1",
         "partitionId": "part1"}
    ])
    @patch.object(SLITableRecreationPredicate, 'is_recreated')
    @patch.object(SLITableExistsPredicate, 'exists')
    @patch.object(SLIResultsStreamer, 'stream')
    def test_table_that_exist_and_is_recreated_should_be_filtered_out(self,
        stream, exists, is_recreated, _):
        # given
        exists.return_value = True
        is_recreated.return_value = True
        # when
        LatencySliService(3).recalculate_sli()
        # then
        stream.assert_called_with([])

    @patch.object(SLIViewQuerier, 'query', return_value=[
        {"projectId": "p1", "datasetId": "d1", "tableId": "t1",
         "partitionId": "part1"}
    ])
    @patch.object(SLITableRecreationPredicate, 'is_recreated')
    @patch.object(SLITableExistsPredicate, 'exists')
    @patch.object(SLIResultsStreamer, 'stream')
    def test_table_that_exist_and_is_not_recreated_should_not_be_filtered_out(
            self, stream, exists, is_recreated, _):
        # given
        exists.return_value = True
        is_recreated.return_value = False
        # when
        LatencySliService(3).recalculate_sli()
        # then
        stream.assert_called_with([{"projectId": "p1", "datasetId": "d1",
                                    "tableId": "t1", "partitionId": "part1"}])

    @patch.object(SLIViewQuerier, 'query', return_value=[
        {"projectId": "p1", "datasetId": "d1", "tableId": "t1",
         "partitionId": "part1"}
    ])
    @patch.object(SLITableExistsPredicate, 'exists')
    @patch.object(SLIResultsStreamer, 'stream')
    def test_table_that_caused_exception_in_exist_filter_should_not_be_filtered_out(
            self, stream, exists, _1, ):

        # given
        exists.side_effect = Exception("An error")
        # when
        LatencySliService(3).recalculate_sli()

        # then
        stream.assert_called_with([{"projectId": "p1", "datasetId": "d1",
                                    "tableId": "t1", "partitionId": "part1"}])

    @patch.object(SLIViewQuerier, 'query', return_value=[
        {"projectId": "p1", "datasetId": "d1", "tableId": "t1",
         "partitionId": "part1"}
    ])
    @patch.object(SLITableRecreationPredicate, 'is_recreated')
    @patch.object(SLITableExistsPredicate, 'exists')
    @patch.object(SLIResultsStreamer, 'stream')
    def test_table_that_caused_exception_in_recreation_filter_should_not_be_filtered_out(
            self, stream, exists, is_recreated, _1):

        # given
        exists.return_value = True
        is_recreated.side_effect = Exception("An error")

        # when
        LatencySliService(3).recalculate_sli()

        # then
        stream.assert_called_with([{"projectId": "p1", "datasetId": "d1",
                                    "tableId": "t1", "partitionId": "part1"}])