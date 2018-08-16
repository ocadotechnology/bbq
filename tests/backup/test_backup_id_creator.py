import unittest
from datetime import datetime

from src.backup.backup_id_creator import BackupIdCreator
from src.commons.exceptions import ParameterValidationException


class TestBackupIdCreator(unittest.TestCase):
    def test_create_happy_path(self):
        # given
        project_id = 'test-project'
        dataset_id = 'test_dataset'
        table_id = 'test_table'
        now = datetime(1901, 12, 22, 5, 15, 15)

        # when
        result = BackupIdCreator.create(project_id, dataset_id, table_id, now)

        # then
        expected_result = '19011222_051515_test_project_test_dataset_test_table'

        self.assertEquals(result, expected_result)

    def test_create_with_partition(self):
        # given
        project_id = 'test-project'
        dataset_id = 'test_dataset'
        table_id = 'test_table'
        partition_id = '20170324'
        now = datetime(1901, 12, 22, 5, 15, 15)

        # when
        result = BackupIdCreator.create(project_id, dataset_id, table_id, now,
                                        partition_id)

        # then
        expected_result = '19011222_051515_test_project_test_dataset_' \
                          'test_table_partition_20170324'

        self.assertEquals(result, expected_result)

    def test_create_with_exceeded_name_length_should_return_reduced_one_to_1024_chars_with_hash(self):# nopep8 pylint: disable=C0301
        # given
        dataset_id = 'test_dataset'
        table_id = 'test_table'
        now = datetime(1901, 12, 22, 5, 15, 15)
        project_id = 'test-project'
        for x in range(0, 1024):
            project_id += '_test'

        # when
        result = BackupIdCreator.create(project_id, dataset_id, table_id, now)

        # then
        expected_result = '19011222_051515_test_project_test_test_test_test_' \
                          'test_test_test_test_test_test_test_test_test_test_' \
                          'test_test_test_test_test_test_test_test_test_test_' \
                          'test_test_test_test_test_test_test_test_test_test_' \
                          'test_test_test_test_test_test_test_test_test_test_' \
                          'test_test_test_test_test_test_test_test_test_test_' \
                          'test_test_test_test_test_test_test_test_test_test_' \
                          'test_test_test_test_test_test_test_test_test_test_' \
                          'test_test_test_test_test_test_test_test_test_test_' \
                          'test_test_test_test_test_test_test_test_test_test_' \
                          'test_test_test_test_test_test_test_test_test_test_' \
                          'test_test_test_test_test_test_test_test_test_test_' \
                          'test_test_test_test_test_test_test_test_test_test_' \
                          'test_test_test_test_test_test_test_test_test_test_' \
                          'test_test_test_test_test_test_test_test_test_test_' \
                          'test_test_test_test_test_test_test_test_test_test_' \
                          'test_test_test_test_test_test_test_test_test_test_' \
                          'test_test_test_test_test_test_test_test_test_test_' \
                          'test_test_test_test_test_test_test_test_test_test_' \
                          'test_test_test_test_test_test_test_test_test_test_' \
                          't__3470290951459458967'

        self.assertEquals(result, expected_result)
        self.assertLessEqual(len(result), 1024)

    def test_create_without_project_should_throw_error(self):
        # given
        project_id = None
        dataset_id = 'test_dataset'
        table_id = 'test_table'
        timestamp = datetime(1901, 12, 22, 5, 15, 15)

        # when then
        self.assertRaises(ParameterValidationException, BackupIdCreator.create,
                          project_id, dataset_id, table_id, timestamp)

    def test_create_without_dataset_should_throw_error(self):
        # given
        project_id = 'test-project'
        dataset_id = None
        table_id = 'test_table'
        timestamp = datetime(1901, 12, 22, 5, 15, 15)

        # when then
        self.assertRaises(ParameterValidationException, BackupIdCreator.create,
                          project_id, dataset_id, table_id, timestamp)

    def test_create_without_table_should_throw_error(self):
        # given
        project_id = 'test-project'
        dataset_id = 'test_dataset'
        table_id = None
        timestamp = datetime(1901, 12, 22, 5, 15, 15)

        # when then
        self.assertRaises(ParameterValidationException, BackupIdCreator.create,
                          project_id, dataset_id, table_id, timestamp)

    def test_create_without_timestamp_should_throw_error(self):
        # given
        project_id = 'test-project'
        dataset_id = 'test_dataset'
        table_id = 'test_table'
        timestamp = None

        # when then
        self.assertRaises(ParameterValidationException, BackupIdCreator.create,
                          project_id, dataset_id, table_id, timestamp)
