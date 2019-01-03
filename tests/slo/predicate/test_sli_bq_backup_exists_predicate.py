import unittest

from src.slo.backup_quality.predicate.sli_bq_backup_exists_predicate import SLIBQBackupExistsPredicate


class TestSLITableExistsPredicate(unittest.TestCase):

    def test_should_return_false_for_not_existing_backup_table(self):
        # given
        sli_table = self.__create_sli_table_without_backup_table()

        # when
        exists = SLIBQBackupExistsPredicate().exists(sli_table)

        # then
        self.assertFalse(exists)

    def test_should_return_true_for_existing_backup_table(self):
        # given
        sli_table = self.__create_sli_table_with_backup_table()

        # when
        exists = SLIBQBackupExistsPredicate().exists(sli_table)

        # then
        self.assertTrue(exists)

    def __create_sli_table_without_backup_table(self):
        return {
            "snapshotTime": None,
            "projectId": 'p',
            "datasetId": 'd',
            "tableId": 'd',
            "partitionId": '20180808',
            "creationTime": '1500000000000',
            "lastModifiedTime": None,
            "backupCreated": None,
            "backupLastModified": None,
            "backupDatasetId": 'b_d',
            "backupTableId": 'b_t',
            "backupNumBytes": None,
            "backupNumRows": None,
            "xDays": 4
        }

    def __create_sli_table_with_backup_table(self):
        return {
            "snapshotTime": None,
            "projectId": 'p',
            "datasetId": 'd',
            "tableId": 'd',
            "partitionId": '20180808',
            "creationTime": '1500000000000',
            "lastModifiedTime": None,
            "backupCreated": None,
            "backupLastModified": None,
            "backupDatasetId": 'b_d',
            "backupTableId": 'b_t',
            "backupNumBytes": 123456789,
            "backupNumRows": 123456789,
            "xDays": 4
        }
