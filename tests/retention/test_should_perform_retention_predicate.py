import unittest

from mock import patch
from src.backup.datastore.Backup import Backup
from src.retention.should_perform_retention_predicate import \
    ShouldPerformRetentionPredicate
from src.commons.error_reporting import ErrorReporting


class TestShouldPerformRetentionPredicate(unittest.TestCase):

    def setUp(self):
        patch(
            'src.commons.config.environment.Environment.version_id',
            return_value='dummy_version'
        ).start()
        patch('googleapiclient.discovery.build').start()
        patch('oauth2client.client.GoogleCredentials.get_application_default') \
            .start()

    def tearDown(self):
        patch.stopall()

    def test_should_return_true_for_valid_backup_list(self):
        # given
        backups = self.__create_valid_backups()

        # when
        result = ShouldPerformRetentionPredicate.test(backups)

        # then
        self.assertEqual(True, result)

    def test_should_return_false_for_empty_list(self):
        # given
        empty_list = []

        # when
        result = ShouldPerformRetentionPredicate.test(empty_list)

        # then
        self.assertEqual(False, result)

    @patch.object(ErrorReporting, '_create_http')
    @patch.object(ErrorReporting, 'report')
    def test_should_return_false_and_trigger_error_reporting_if_there_are_multiple_backups_referencing_same_table_in_bq(
            self, report, _):
        # given
        backups = \
            self.__create_backups_with_part_of_referencing_same_table_in_bq()

        # when
        result = ShouldPerformRetentionPredicate.test(backups)

        # then
        self.assertEqual(False, result)
        report.assert_called_once()

    @staticmethod
    def __create_valid_backups():
        backup_1 = Backup(table_id='table_id_1', dataset_id='dataset_id_1')
        backup_2 = Backup(table_id='table_id_2', dataset_id='dataset_id_1')
        backup_3 = Backup(table_id='table_id_3', dataset_id='dataset_id_1')
        backup_4 = Backup(table_id='table_id_4', dataset_id='dataset_id_1')

        return [backup_1, backup_2, backup_3, backup_4]

    @staticmethod
    def __create_backups_with_part_of_referencing_same_table_in_bq():
        backup_1 = Backup(table_id='table_id_1', dataset_id='dataset_id_1')
        backup_2 = Backup(table_id='table_id_2', dataset_id='dataset_id_1')
        backup_3 = Backup(table_id='table_id_3', dataset_id='dataset_id_1')
        backup_4 = Backup(table_id='table_id_3', dataset_id='dataset_id_1')

        return [backup_4, backup_2, backup_3, backup_1]
