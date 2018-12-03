import unittest

from mock import patch

from src.slo.sli_results_streamer import SLIResultsStreamer
from src.slo.predicate.sli_table_exists_predicate import SLITableExistsPredicate
from src.slo.backup_creation_latency.predicate.sli_table_recreation_predicate import \
    SLITableRecreationPredicate
from src.slo.sli_view_querier import SLIViewQuerier
from src.slo.backup_creation_latency.latency_sli_service import LatencySliService
from src.commons.test_utils import utils
from google.appengine.ext import testbed


class TestLatencySliService(unittest.TestCase):

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

    @patch.object(SLIViewQuerier, 'query', return_value=([
        {"projectId": "p1", "datasetId": "d1", "tableId": "t1",
         "partitionId": "part1"}
    ], 21342134324))
    @patch.object(SLITableExistsPredicate, 'exists')
    @patch.object(SLIResultsStreamer, 'stream')
    def test_table_that_not_exist_should_be_filtered_out(self,
        stream, exists, _):
        # given
        exists.return_value = False
        # when
        LatencySliService(3).recalculate_sli()
        # then
        stream.assert_called_with([self.__create_snapshot_marker_row(21342134324, 3)])

    def __create_snapshot_marker_row(self, snapshotTime, x_days):
        return {"snapshotTime": snapshotTime,
                "projectId": 'SNAPSHOT_MARKER',
                "datasetId": 'SNAPSHOT_MARKER',
                "tableId": 'SNAPSHOT_MARKER',
                "partitionId": 'SNAPSHOT_MARKER',
                "creationTime": float(0),
                "lastModifiedTime": float(0),
                "backupCreated": float(0),
                "backupLastModified": float(0),
                "xDays": x_days}

    @patch.object(SLIViewQuerier, 'query', return_value=([
        {"projectId": "p1", "datasetId": "d1", "tableId": "t1",
         "partitionId": "part1"}
    ], 21342134324))
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
        stream.assert_called_with([self.__create_snapshot_marker_row(21342134324, 3)])


