import unittest

from mock import Mock, MagicMock, patch

from src.commons.big_query.big_query import BigQuery
from src.slo.x_days_sli.sli_view_querier import SLIViewQuerier


class TestSLIViewQuerier(unittest.TestCase):

    @patch('src.commons.big_query.big_query.BigQuery.__init__',
           Mock(return_value=None))
    @patch('time.time', MagicMock(return_value=1535624154.94896))
    @patch.object(BigQuery, 'execute_query')
    def test_should_parse_query_response(self, execute_query):
        # given
        query_results = [
            {
                "f": [{"v": "dev-cymes-osp-eu-dataplatform"},
                      {"v": "invalid_dev_atm_use1_om_events_raw"},
                      {"v": "UNKNOWN_EVENT_v1"},
                      {"v": "20180726"},
                      {"v": "1.524727885462E9"},
                      {"v": "1.533016802E9"},
                      {"v": "1.532668961889E9"},
                      {"v": "1.53262467938E9"}]
            }, {
                "f": [{"v": "dev-cymes-osp-eu-dataplatform"},
                      {"v": "invalid_dev_atm_use1_storepick_flux_events_raw"},
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
                'projectId': 'dev-cymes-osp-eu-dataplatform',
                'datasetId': 'invalid_dev_atm_use1_om_events_raw',
                'tableId': 'UNKNOWN_EVENT_v1',
                'partitionId': '20180726',
                'lastModifiedTime': 1533016.802,
                'creationTime': 1524727.8854619998,
                'backupCreated': 1532668.961889,
                'backupLastModified': 1532624.67938,
                'xDays': '3'},
            {
                'snapshotTime': 1535624154.94896,
                'projectId': 'dev-cymes-osp-eu-dataplatform',
                'datasetId': 'invalid_dev_atm_use1_storepick_flux_events_raw',
                'tableId': 'UNKNOWN_EVENT_v1',
                'partitionId': '20180726',
                'creationTime': 1524725.829453,
                'lastModifiedTime': 1533016.8127980002,
                'backupCreated': 1532675.823532,
                'backupLastModified': 1532641.088365,
                'xDays': '3'
            }
        ]

        # when
        results = SLIViewQuerier(BigQuery()).query("3")

        # then
        self.assertEqual(2, len(results))
        self.assertEqual(expected_output, results)
