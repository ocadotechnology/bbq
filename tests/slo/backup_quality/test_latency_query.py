import unittest

from mock import Mock, MagicMock, patch

from src.commons.big_query.big_query import BigQuery
from src.slo.backup_quality.quality_query_specification import \
  QualityQuerySpecification
from src.slo.sli_view_querier import SLIViewQuerier


class TestQualityQuery(unittest.TestCase):

    @patch('src.commons.big_query.big_query.BigQuery.__init__',
           Mock(return_value=None))
    @patch('time.time', MagicMock(return_value=1535624154.94896))
    @patch.object(BigQuery, 'execute_query')
    def test_should_parse_query_response(self, execute_query):
        # given
        query_results = [
            {
                "f": [{"v": "project1"},
                      {"v": "dataset1"},
                      {"v": "table1"},
                      {"v": "20180421"},
                      {"v": "backup_dataset1"},
                      {"v": "backup_table1"},
                      {"v": "1.632668961889E9"},
                      {"v": "1.533016802E9"},
                      {"v": "1.532668961889E9"},
                      {"v": 50},
                      {"v": 50},
                      {"v": 50},
                      {"v": 5},
                      {"v": 5}]
            }, {
                "f": [{"v": "project2"},
                      {"v": "dataset2"},
                      {"v": "table2"},
                      {"v": "20180422"},
                      {"v": "backup_dataset2"},
                      {"v": "backup_table2"},
                      {"v": "1.632668961889E9"},
                      {"v": "1.533016802E9"},
                      {"v": "1.632668961889E9"},
                      {"v": 60},
                      {"v": 60},
                      {"v": 60},
                      {"v": 6},
                      {"v": 6}]
            }
        ]
        execute_query.return_value = query_results

        expected_output = [
            {
                'snapshotTime': 1535624154.94896,
                'projectId': 'project1',
                'datasetId': 'dataset1',
                'tableId': 'table1',
                'partitionId': '20180421',
                'backupDatasetId': 'backup_dataset1',
                'backupTableId': 'backup_table1',
                'lastModifiedTime': 1632668961.889,
                'backupLastModifiedTime': 1533016802,
                'backupEntityLastModifiedTime': 1532668961.889,
                'numBytes': 50,
                'backupNumBytes': 50,
                'backupEntityNumBytes': 50,
                'numRows': 5,
                'backupNumRows': 5},
            {
                'snapshotTime': 1535624154.94896,
                'projectId': 'project2',
                'datasetId': 'dataset2',
                'tableId': 'table2',
                'partitionId': '20180422',
                'backupDatasetId': 'backup_dataset2',
                'backupTableId': 'backup_table2',
                'lastModifiedTime': 1632668961.889,
                'backupLastModifiedTime': 1533016802,
                'backupEntityLastModifiedTime': 1632668961.889,
                'numBytes': 60,
                'backupNumBytes': 60,
                'backupEntityNumBytes': 60,
                'numRows': 6,
                'backupNumRows': 6}

        ]

        expected_snapshot_time = 1535624154.94896

        # when
        results = SLIViewQuerier(BigQuery(), QualityQuerySpecification()).query()

        # then
        self.assertEqual(2, len(results))
        self.assertEqual(expected_output, results[0])
        self.assertEqual(expected_snapshot_time, results[1])
