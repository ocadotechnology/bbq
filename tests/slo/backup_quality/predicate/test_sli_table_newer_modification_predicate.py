import unittest

from mock import Mock, patch

from src.commons.big_query.big_query import BigQuery
from src.slo.backup_quality.predicate.sli_table_newer_modification_predicate import \
    SLITableNewerModificationPredicate


class TestSLITableNewerModificationPredicate(unittest.TestCase):

    @patch('src.commons.big_query.big_query.BigQuery.__init__',
           Mock(return_value=None))
    @patch('src.commons.big_query.big_query.BigQuery.get_table',
           Mock(return_value={'projectId': 'p', 'lastModifiedTime': '1618522714837',
                              'schema': {'fields': []}}))
    def test_should_return_true_if_get_table_has_newer_modification_time_than_census(self):
        # given
        sli_table = {
            "snapshotTime": None,
            "projectId": 'p',
            "datasetId": 'd',
            "tableId": 'd',
            "partitionId": None,
            "creationTime": None,
            "lastModifiedTime": float('1.518522714837E9'),
            "backupCreated": None,
            "backupLastModified": None,
            "xDays": 4
        }

        # when
        is_modified = SLITableNewerModificationPredicate(BigQuery()).is_modified_since_last_census_snapshot(sli_table)

        # then
        self.assertTrue(is_modified)

    @patch('src.commons.big_query.big_query.BigQuery.__init__',
           Mock(return_value=None))
    @patch('src.commons.big_query.big_query.BigQuery.get_table',
           Mock(return_value={'projectId': 'p', 'lastModifiedTime': '1518522714837',
                              'schema': {'fields': []}}))
    def test_should_return_false_if_get_table_has_the_same_modification_time_than_census(self):
        # given
        sli_table = {
            "snapshotTime": None,
            "projectId": 'p',
            "datasetId": 'd',
            "tableId": 'd',
            "partitionId": None,
            "creationTime": None,
            "lastModifiedTime": float('1.518522714837E9'),
            "backupCreated": None,
            "backupLastModified": None,
            "xDays": 4
        }

        # when
        is_modified = SLITableNewerModificationPredicate(BigQuery()).is_modified_since_last_census_snapshot(sli_table)

        # then
        self.assertFalse(is_modified)

    @patch('src.commons.big_query.big_query.BigQuery.__init__',
           Mock(return_value=None))
    @patch('src.commons.big_query.big_query.BigQuery.get_table',
           Mock(return_value={'projectId': 'p', 'lastModifiedTime': '1418522714837',
                              'schema': {'fields': []}}))
    def test_should_return_false_if_get_table_has_the_older_modification_time_than_census(self):
        # given
        sli_table = {
            "snapshotTime": None,
            "projectId": 'p',
            "datasetId": 'd',
            "tableId": 'd',
            "partitionId": None,
            "creationTime": None,
            "lastModifiedTime": float('1.518522714837E9'),
            "backupCreated": None,
            "backupLastModified": None,
            "xDays": 4
        }

        # when
        is_modified = SLITableNewerModificationPredicate(BigQuery()).is_modified_since_last_census_snapshot(sli_table)

        # then
        self.assertFalse(is_modified)
