import unittest

from mock import patch

from src.slo.predicate.sli_table_exists_predicate import \
  SLITableExistsPredicate
from src.slo.backup_quality.quality_sli_service import QualitySliService
from src.slo.backup_quality.predicate.sli_table_newer_modification_predicate import \
  SLITableNewerModificationPredicate
from src.slo.sli_results_streamer import SLIResultsStreamer
from src.slo.sli_view_querier import SLIViewQuerier


class TestQualitySliService(unittest.TestCase):

    def setUp(self):
        patch('googleapiclient.discovery.build').start()
        patch('oauth2client.client.GoogleCredentials.get_application_default') \
          .start()

    def tearDown(self):
        patch.stopall()

    @patch.object(SLIViewQuerier, 'query',
                  return_value=([
                                  {"projectId": "p1", "datasetId": "d1",
                                   "tableId": "t1",
                                   "partitionId": "part1"}
                                ], 21342134324))
    @patch.object(SLITableExistsPredicate, 'exists')
    @patch.object(SLITableNewerModificationPredicate, 'is_modified_since_last_census_snapshot')
    @patch.object(SLIResultsStreamer, 'stream')
    def test_table_that_is_modfied_since_last_census_snapshot_should_be_filtered_out(self,
        stream, is_modified_since_last_census_snapshot, exists ,_):
        # given
        exists.return_value = True
        is_modified_since_last_census_snapshot.return_value = True
        # when
        QualitySliService().recalculate_sli()
        # then
        stream.assert_called_with([], snapshot_marker=self.__create_snapshot_marker_row(21342134324))

    @patch.object(SLIViewQuerier, 'query',
                  return_value=([
                                  {"projectId": "p1", "datasetId": "d1",
                                   "tableId": "t1",
                                   "partitionId": "part1"}
                                ], 21342134324))
    @patch.object(SLITableExistsPredicate, 'exists')
    @patch.object(SLITableNewerModificationPredicate, 'is_modified_since_last_census_snapshot')
    @patch.object(SLIResultsStreamer, 'stream')
    def test_table_that_is_not_modfied_since_last_census_snapshot_should_not_be_filtered_out(self,
        stream, is_modified_since_last_census_snapshot, exists, _):
        # given
        exists.return_value = True
        is_modified_since_last_census_snapshot.return_value = False
        # when
        QualitySliService().recalculate_sli()
        # then
        stream.assert_called_with([{"projectId": "p1", "datasetId": "d1",
                                    "tableId": "t1", "partitionId": "part1"}],
                                  snapshot_marker=self.__create_snapshot_marker_row(21342134324))

    @patch.object(SLIViewQuerier, 'query',
                  return_value=([
                                  {"projectId": "p1", "datasetId": "d1",
                                   "tableId": "t1",
                                   "partitionId": "part1"}
                                ], 21342134324))
    @patch.object(SLITableExistsPredicate, 'exists')
    @patch.object(SLIResultsStreamer, 'stream')
    def test_table_that_not_exists_should_be_be_filtered_out(self,
        stream, exists, _):
        # given
        exists.return_value = False
        # when
        QualitySliService().recalculate_sli()
        # then
        stream.assert_called_with([],
                                  snapshot_marker=self.__create_snapshot_marker_row(21342134324))

    @patch.object(SLIViewQuerier, 'query',
                  return_value=([
                                  {"projectId": "p1", "datasetId": "d1",
                                   "tableId": "t1",
                                   "partitionId": "part1"}
                                ], 21342134324))
    @patch.object(SLITableExistsPredicate, 'exists')
    @patch.object(SLITableNewerModificationPredicate, 'is_modified_since_last_census_snapshot')
    @patch.object(SLIResultsStreamer, 'stream')
    def test_table_that_caused_exception_in_modification_predicate_should_not_be_filtered_out(
        self, stream, is_modified_since_last_census_snapshot,exists, _):

        # given
        exists.return_value = True
        is_modified_since_last_census_snapshot.side_effect = Exception("An error")

        # when
        QualitySliService().recalculate_sli()

        # then
        stream.assert_called_with([{"projectId": "p1", "datasetId": "d1",
                                    "tableId": "t1", "partitionId": "part1"}],
                                  snapshot_marker=self.__create_snapshot_marker_row(21342134324))

    @patch.object(SLIViewQuerier, 'query',
                      return_value=([
                                      {"projectId": "p1", "datasetId": "d1",
                                       "tableId": "t1",
                                       "partitionId": "part1"}
                                    ], 21342134324))
    @patch.object(SLITableExistsPredicate, 'exists')
    @patch.object(SLIResultsStreamer, 'stream')
    def test_table_that_caused_exception_in_exists_predicate_should_not_be_filtered_out(
        self, stream, exists, _):

      # given
      exists.return_value = Exception("An error")

      # when
      QualitySliService().recalculate_sli()

      # then
      stream.assert_called_with([{"projectId": "p1", "datasetId": "d1",
                                  "tableId": "t1", "partitionId": "part1"}],
                                snapshot_marker=self.__create_snapshot_marker_row(21342134324))

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

