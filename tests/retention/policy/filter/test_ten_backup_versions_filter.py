import unittest
from random import shuffle

from nose_parameterized import parameterized
from src.retention.policy.filter.ten_backup_versions_filter import \
    TenBackupVersionsFilter
from src.commons.table_reference import TableReference
from tests.utils import backup_utils


class TestTenBackupVersionsFilter(unittest.TestCase):
    def setUp(self):
        self.under_test = TenBackupVersionsFilter()

    @parameterized.expand([[0], [1], [2], [5], [7], [10]])
    def test_should_not_filter_out_if_there_is_10_or_less_table_backups(
            self,
            count
    ):
        # given
        reference = TableReference('example-project-id', 'example-dataset-id',
                                   'example-table-id')
        backups = backup_utils.create_backup_daily_sequence(count)

        # when
        backups_to_retain = self.under_test.filter(list(backups), reference)
        # then
        self.assertListEqual(backups_to_retain, backups)

    @parameterized.expand([[0], [1], [2], [5], [7], [10]])
    def test_should_not_filter_out_if_there_is_10_or_less_partition_backups(self, count): # nopep8 pylint: disable=C0301
        # given
        reference = TableReference(
            'example-project-id',
            'example-dataset-id',
            'example-table-id', '20170601'
        )
        backups = backup_utils.create_backup_daily_sequence(count)

        # when
        backups_to_retain = self.under_test.filter(list(backups), reference)
        # then
        self.assertListEqual(backups_to_retain, backups)

    @parameterized.expand(
        [[TableReference('example-project-id', 'example-dataset-id',
                         'example-table-id')],
         [TableReference('example-project-id',
                             'example-dataset-id',
                             'example-table-id',
                             '20170601')]])
    def test_should_filter_out_backups_above_10_version(self, reference):
        # given
        backups = backup_utils.create_backup_daily_sequence(14)
        expected_retained_backups = backups[:10]
        shuffle(backups)

        # when
        backups_to_retain = self.under_test.filter(backups, reference)
        # then
        self.assertListEqual(backups_to_retain, expected_retained_backups)
