import unittest

from mock import Mock, patch

from src.commons.big_query.big_query import BigQuery
from src.slo.backup_creation_latency.predicate.sli_table_recreation_predicate import \
    SLITableRecreationPredicate


class TestSLITableRecreationPredicate(unittest.TestCase):

    @patch('src.commons.big_query.big_query.BigQuery.__init__',
           Mock(return_value=None))
    @patch('src.commons.big_query.big_query.BigQuery.get_table',
           Mock(return_value={'projectId': 'p', 'creationTime': '1618522714837',
                              'schema': {'fields': []}}))
    def test_should_return_true_if_get_table_has_newer_creation_time_than_census(self):
        # given
        sli_table = {
            "snapshotTime": None,
            "projectId": 'p',
            "datasetId": 'd',
            "tableId": 'd',
            "partitionId": None,
            "creationTime": float('1.518522714837E9'),
            "lastModifiedTime": None,
            "backupCreated": None,
            "backupLastModified": None,
            "xDays": 4
        }

        # when
        is_recreated = SLITableRecreationPredicate(BigQuery()).is_recreated(sli_table)

        # then
        self.assertTrue(is_recreated)

    @patch('src.commons.big_query.big_query.BigQuery.__init__',
           Mock(return_value=None))
    @patch('src.commons.big_query.big_query.BigQuery.get_table',
           Mock(return_value={'projectId': 'p', 'creationTime': '1518522714837',
                              'schema': {'fields': []}}))
    def test_should_return_false_if_get_table_has_the_same_creation_time_than_census(self):
        # given
        sli_table = {
            "snapshotTime": None,
            "projectId": 'p',
            "datasetId": 'd',
            "tableId": 'd',
            "partitionId": None,
            "creationTime": float('1.518522714837E9'),
            "lastModifiedTime": None,
            "backupCreated": None,
            "backupLastModified": None,
            "xDays": 4
        }

        # when
        is_recreated = SLITableRecreationPredicate(BigQuery()).is_recreated(sli_table)

        # then
        self.assertFalse(is_recreated)

    @patch('src.commons.big_query.big_query.BigQuery.__init__',
           Mock(return_value=None))
    @patch('src.commons.big_query.big_query.BigQuery.get_table',
           Mock(return_value={'projectId': 'p', 'creationTime': '1418522714837',
                              'schema': {'fields': []}}))
    def test_should_return_false_if_get_table_has_the_older_creation_time_than_census(self):
        # given
        sli_table = {
            "snapshotTime": None,
            "projectId": 'p',
            "datasetId": 'd',
            "tableId": 'd',
            "partitionId": None,
            "creationTime": float('1.518522714837E9'),
            "lastModifiedTime": None,
            "backupCreated": None,
            "backupLastModified": None,
            "xDays": 4
        }

        # when
        is_recreated = SLITableRecreationPredicate(BigQuery()).is_recreated(sli_table)

        # then
        self.assertFalse(is_recreated)
