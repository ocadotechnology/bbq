import unittest

from mock import Mock, patch

from src.commons.big_query.big_query import BigQuery
from src.slo.backup_creation_latency.predicate.sli_table_has_any_backup_predicate import \
    SLITableHasAnyBackupPredicate


class TestSLITableEmptinessPredicate(unittest.TestCase):

    @patch('src.commons.big_query.big_query.BigQuery.__init__',
           Mock(return_value=None))
    @patch('src.commons.big_query.big_query.BigQuery.get_table',
           Mock(return_value={'projectId': 'p', 'creationTime': '1618522714837',
                              'numRows': '0', 'schema': {'fields': []}}))
    def test_should_return_true_if_has_some_backup_created_in_the_past(self):
        # given
        sli_table = {
            "snapshotTime": None,
            "projectId": 'p',
            "datasetId": 'd',
            "tableId": 'd',
            "partitionId": None,
            "creationTime": float('1.518522714837E9'),
            "lastModifiedTime": float('1.518522714837E9'),
            "backupCreated": float('1.418522714837E9'),
            "backupLastModified": float('1.418522714837E9'),
            "xDays": 4
        }

        # when
        has_any_backup = SLITableHasAnyBackupPredicate().has_any_backup(sli_table)

        # then
        self.assertTrue(has_any_backup)

    @patch('src.commons.big_query.big_query.BigQuery.__init__',
           Mock(return_value=None))
    @patch('src.commons.big_query.big_query.BigQuery.get_table',
           Mock(return_value={'projectId': 'p', 'creationTime': '1518522714837',
                              'numRows': '0', 'schema': {'fields': []}}))
    def test_should_return_false_if_has_no_backup_created_in_the_past(self):
        # given
        sli_table = {
            "snapshotTime": None,
            "projectId": 'p',
            "datasetId": 'd',
            "tableId": 'd',
            "partitionId": None,
            "creationTime": float('1.518522714837E9'),
            "lastModifiedTime": float('1.518522714837E9'),
            "backupCreated": float('0'),
            "backupLastModified": float('0'),
            "xDays": 4
        }

        # when
        has_any_backup = SLITableHasAnyBackupPredicate().has_any_backup(sli_table)

        # then
        self.assertFalse(has_any_backup)
