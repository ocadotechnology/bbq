import unittest

from mock import Mock, MagicMock, patch

from src.commons.big_query.big_query import BigQuery
from src.slo.backup_creation_latency.latency_query_specification import \
    LatencyQuerySpecification
from src.slo.sli_view_querier import SLIViewQuerier


class TestLatencyQuery(unittest.TestCase):

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
                      {"v": "UNKNOWN_EVENT_v1"},
                      {"v": "20180726"},
                      {"v": "1.524727885462E9"},
                      {"v": "1.533016802E9"},
                      {"v": "1.532668961889E9"},
                      {"v": "1.53262467938E9"}]
            }, {
                "f": [{"v": "project2"},
                      {"v": "dataset2"},
                      {"v": "UNKNOWN_EVENT_v1"},
                      {"v": "20180726"},
                      {"v": "1.524725829453E9"},
                      {"v": "1.533016812798E9"},
                      {"v": "1.532675823532E9"},
                      {"v": "1.532641088365E9"}
                      ]
            }
        ]
        execute_query.return_value = query_results

        expected_output = [
            {
                'snapshotTime': 1535624154.94896,
                'projectId': 'project1',
                'datasetId': 'dataset1',
                'tableId': 'UNKNOWN_EVENT_v1',
                'partitionId': '20180726',
                'lastModifiedTime': 1533016802,
                'creationTime': 1524727885.4619998,
                'backupCreated': 1532668961.889,
                'backupLastModified': 1532624679.38,
                'xDays': '3'},
            {
                'snapshotTime': 1535624154.94896,
                'projectId': 'project2',
                'datasetId': 'dataset2',
                'tableId': 'UNKNOWN_EVENT_v1',
                'partitionId': '20180726',
                'creationTime': 1524725829.453,
                'lastModifiedTime': 1533016812.7980002,
                'backupCreated': 1532675823.532,
                'backupLastModified': 1532641088.365,
                'xDays': '3'
            }
        ]

        expected_snapshot_time=1535624154.94896

        # when
        results = SLIViewQuerier(BigQuery(), LatencyQuerySpecification("3")).query()

        # then
        self.assertEqual(2, len(results))
        self.assertEqual(expected_output, results[0])
        self.assertEqual(expected_snapshot_time, results[1])
